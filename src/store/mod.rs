pub mod digest;
pub mod locks;
pub mod messaging;
pub mod notes;
pub mod presence;
pub mod quota;
pub mod tasks;

pub mod attachments;

use sqlx::PgPool;
use uuid::Uuid;

use crate::{auth::AuthCtx, error::BusError, error::BusResult, model::WhoAmI};

/// Channel names are normalised so `#dev`, `dev` and `DEV` all address the same
/// channel. Keeps the model from creating near-duplicate channels.
pub fn normalize_channel(name: &str) -> String {
    name.trim().trim_start_matches('#').trim().to_lowercase()
}

/// `metadata` is a structured pointer — an id, a flag, a short label — not a
/// document. Bodies, notes and attachments are the places built to carry size.
pub const MAX_METADATA_BYTES: usize = 16 * 1024;

/// Reconstruct `metadata` that arrived as a serialised object.
///
/// Some MCP clients stringify structured arguments, so `{"question": true}`
/// reaches the server as the *text* `"{\"question\": true}"`. Stored
/// verbatim it is unusable: the plugin's Stop drain reads
/// `metadata["question"]` and finds a string, and the capability the skill
/// documents does not work from that client at all.
///
/// Deliberately narrow. Only a string that parses to a JSON **object** is
/// rewritten — the exact failure mode. Any other string, number or boolean is
/// stored as sent, because a caller may legitimately want one and guessing at
/// their intent is how coercion becomes a bug of its own.
pub fn normalize_metadata(metadata: Option<serde_json::Value>) -> Option<serde_json::Value> {
    match metadata {
        Some(serde_json::Value::String(raw)) => match serde_json::from_str(&raw) {
            Ok(serde_json::Value::Object(map)) => Some(serde_json::Value::Object(map)),
            _ => Some(serde_json::Value::String(raw)),
        },
        other => other,
    }
}

/// Reject an oversized `metadata` object before it reaches the database. The
/// error names the field and the limit so an LLM caller can trim and retry.
pub fn check_metadata(field: &str, metadata: Option<&serde_json::Value>) -> BusResult<()> {
    let Some(value) = metadata else {
        return Ok(());
    };
    // Serialising is the only honest measure of what gets stored.
    // If it cannot be serialised it cannot be measured, and an unmeasurable
    // payload must not pass the cap that exists to bound it.
    let size = serde_json::to_vec(value)
        .map(|v| v.len())
        .map_err(|_| BusError::invalid(format!("{field} metadata is not serialisable JSON")))?;
    if size > MAX_METADATA_BYTES {
        return Err(BusError::invalid(format!(
            "{field} metadata is {size} bytes; the limit is {MAX_METADATA_BYTES}. \
             Keep metadata to identifiers and short labels — put the payload in \
             the body, a note, or an attachment."
        )));
    }
    Ok(())
}

/// Trim a free-text field and reject it when it exceeds `max` bytes.
/// Returns the trimmed value so callers store exactly what was validated.
pub fn check_text(field: &str, value: &str, max: usize) -> BusResult<String> {
    // The raw input counts too: trimming first let a small value padded with
    // megabytes of whitespace through, and the server still had to carry it.
    if value.len() > max {
        return Err(BusError::invalid(format!(
            "{field} is {} bytes; the limit is {max}",
            value.len()
        )));
    }
    let trimmed = value.trim();
    if trimmed.len() > max {
        return Err(BusError::invalid(format!(
            "{field} is {} bytes; the limit is {max}",
            trimmed.len()
        )));
    }
    Ok(trimmed.to_owned())
}

/// The caller's session as the API reports it: `None` for the shared session.
///
/// Storage and presentation differ on purpose. `agent_presence` carries the
/// session in its primary key, which cannot hold NULL, so the shared session
/// is `''` there; a caller reading `"session": ""` would reasonably wonder
/// what an empty session is.
///
/// The other session columns are nullable and NULL means something different
/// in each: on `tasks.claimed_session` and `locks.holder_session` it is a
/// claim taken before sessions existed, treated as the shared one; on
/// `messages.recipient_session` it means *every* session of the recipient.
/// Read them with that in mind rather than assuming `''`.
pub fn session_label(auth: &AuthCtx) -> Option<String> {
    (!auth.session.is_empty()).then(|| auth.session.clone())
}

pub async fn agent_id_by_name(pool: &PgPool, team_id: Uuid, name: &str) -> BusResult<Uuid> {
    let name = name.trim();
    let row: Option<(Uuid,)> =
        sqlx::query_as("SELECT id FROM agents WHERE team_id = $1 AND name = $2")
            .bind(team_id)
            .bind(name)
            .fetch_optional(pool)
            .await?;
    row.map(|r| r.0)
        .ok_or_else(|| BusError::not_found(format!("no agent named '{name}' in this team")))
}

pub async fn channel_id_by_name(pool: &PgPool, team_id: Uuid, name: &str) -> BusResult<Uuid> {
    let name = normalize_channel(name);
    let row: Option<(Uuid,)> =
        sqlx::query_as("SELECT id FROM channels WHERE team_id = $1 AND name = $2")
            .bind(team_id)
            .bind(&name)
            .fetch_optional(pool)
            .await?;
    row.map(|r| r.0).ok_or_else(|| {
        BusError::not_found(format!(
            "no channel named '{name}'; call list_channels or create it with create_channel"
        ))
    })
}

pub async fn whoami(pool: &PgPool, auth: &AuthCtx) -> BusResult<WhoAmI> {
    // This session's inbox, with this session's cursor: what another window of
    // yours has waiting is not this window's backlog.
    let (unread,): (i64,) = sqlx::query_as(
        r#"
        SELECT count(*)
        FROM messages m
        LEFT JOIN read_cursors c
               ON c.agent_id = $1 AND c.scope = $3
        WHERE m.recipient_agent_id = $1
          AND m.id > COALESCE(c.last_message_id, 0)
          AND (m.recipient_session IS NULL OR m.recipient_session = $2)
        "#,
    )
    .bind(auth.agent_id)
    .bind(&auth.session)
    .bind(messaging::cursor_scope("inbox", &auth.session))
    .fetch_one(pool)
    .await?;

    // Claims belong to the session that took them, so count this one's.
    let (claimed,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM tasks
          WHERE claimed_by = $1 AND status = 'claimed'
            AND COALESCE(claimed_session, '') = $2",
    )
    .bind(auth.agent_id)
    .bind(&auth.session)
    .fetch_one(pool)
    .await?;

    // Resolved, not configured: what this session would post to right now.
    let default_channel = messaging::default_channel(pool, auth)
        .await?
        .map(|(_, name)| name);

    Ok(WhoAmI {
        agent: auth.agent_name.clone(),
        agent_id: auth.agent_id.to_string(),
        team: auth.team_slug.clone(),
        team_id: auth.team_id.to_string(),
        // Stored as '' and reported as null: the database wants a value in a
        // primary key, the caller wants "you are not in a named session".
        session: session_label(auth),
        default_channel,
        unread_direct_messages: unread,
        open_claimed_tasks: claimed,
    })
}
