use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

use super::{agent_id_by_name, channel_id_by_name, normalize_channel};
use crate::{
    auth::AuthCtx,
    error::{BusError, BusResult},
    model::{ChannelInfo, ChannelList, MessageInfo, MessageList, PostMessageResult, ts},
};

const MAX_LIMIT: i64 = 200;
/// Bodies carry logs, diffs and generated reports, not just chat.
const MAX_BODY_BYTES: usize = 1024 * 1024;
/// A topic is one line describing what belongs in the channel.
const MAX_TOPIC_BYTES: usize = 256;

/// A joined message row as it comes back from Postgres.
#[derive(sqlx::FromRow)]
struct MessageRow {
    id: i64,
    sender: String,
    sender_session: Option<String>,
    announce: bool,
    channel: Option<String>,
    recipient: Option<String>,
    recipient_session: Option<String>,
    body: String,
    reply_to: Option<i64>,
    metadata: serde_json::Value,
    attachments: serde_json::Value,
    created_at: chrono::DateTime<chrono::Utc>,
}

impl From<MessageRow> for MessageInfo {
    fn from(r: MessageRow) -> Self {
        MessageInfo {
            id: r.id,
            from: r.sender,
            from_session: r.sender_session.filter(|s| !s.is_empty()),
            announce: r.announce,
            channel: r.channel,
            to: r.recipient,
            to_session: r.recipient_session.filter(|s| !s.is_empty()),
            body: r.body,
            reply_to: r.reply_to,
            metadata: r.metadata,
            attachments: serde_json::from_value(r.attachments).unwrap_or_default(),
            created_at: ts(r.created_at),
        }
    }
}

const MESSAGE_SELECT: &str = r#"
    SELECT m.id,
           s.name  AS sender,
           m.sender_session,
           m.announce,
           ch.name AS channel,
           r.name  AS recipient,
           m.recipient_session,
           m.body,
           m.reply_to,
           m.metadata,
           COALESCE(
               (SELECT json_agg(json_build_object(
                           'id', a.id, 'filename', a.filename,
                           'content_type', a.content_type, 'size_bytes', a.size_bytes)
                       ORDER BY a.id)
                FROM attachments a WHERE a.message_id = m.id),
               '[]'::json
           ) AS attachments,
           m.created_at
    FROM messages m
    JOIN agents s        ON s.id = m.sender_agent_id
    LEFT JOIN channels ch ON ch.id = m.channel_id
    LEFT JOIN agents r    ON r.id = m.recipient_agent_id
"#;

// ---------------------------------------------------------------- channels --

pub async fn create_channel(
    pool: &PgPool,
    auth: &AuthCtx,
    name: &str,
    topic: Option<String>,
) -> BusResult<ChannelInfo> {
    let name = normalize_channel(name);
    if name.is_empty() {
        return Err(BusError::invalid("channel name cannot be empty"));
    }
    if name.len() > 64 {
        return Err(BusError::invalid(
            "channel name is limited to 64 characters",
        ));
    }
    let topic = match topic.as_deref() {
        Some(t) => Some(super::check_text("channel topic", t, MAX_TOPIC_BYTES)?),
        None => None,
    };

    let row: (Uuid, String, Option<String>, chrono::DateTime<chrono::Utc>) = sqlx::query_as(
        r#"
        INSERT INTO channels (team_id, name, topic, created_by)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (team_id, name) DO UPDATE
            SET topic = COALESCE(EXCLUDED.topic, channels.topic)
        RETURNING id, name, topic, created_at
        "#,
    )
    .bind(auth.team_id)
    .bind(&name)
    .bind(topic)
    .bind(auth.agent_id)
    .fetch_one(pool)
    .await?;

    Ok(ChannelInfo {
        name: row.1,
        topic: row.2,
        message_count: 0,
        created_at: ts(row.3),
    })
}

pub async fn list_channels(pool: &PgPool, auth: &AuthCtx) -> BusResult<ChannelList> {
    let rows: Vec<(String, Option<String>, i64, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
        r#"
        SELECT c.name,
               c.topic,
               (SELECT count(*) FROM messages m WHERE m.channel_id = c.id) AS message_count,
               c.created_at
        FROM channels c
        WHERE c.team_id = $1
        ORDER BY c.name
        "#,
    )
    .bind(auth.team_id)
    .fetch_all(pool)
    .await?;

    Ok(ChannelList {
        channels: rows
            .into_iter()
            .map(|(name, topic, message_count, created_at)| ChannelInfo {
                name,
                topic,
                message_count,
                created_at: ts(created_at),
            })
            .collect(),
    })
}

// ---------------------------------------------------------------- posting --

/// Split `agent[/session]` into its parts.
///
/// No slash means the person: every session of theirs sees it, which is what
/// addressing a teammate has always meant. With a slash, one working context
/// of theirs — including one of your own, which is how a coordinating session
/// hands work to the window that has the repository open.
///
/// The session half is normalised exactly as the `X-Crew-Session` header is,
/// so `joaquin/Market-Data` reaches the session that announced itself as
/// `market-data`.
pub fn parse_address(raw: &str) -> BusResult<(String, Option<String>)> {
    let raw = raw.trim();
    match raw.split_once('/') {
        None => Ok((raw.to_owned(), None)),
        Some((agent, session)) => {
            let agent = agent.trim();
            if agent.is_empty() {
                return Err(BusError::invalid(
                    "an address is 'agent' or 'agent/session'; the agent part is missing",
                ));
            }
            // The same validator the X-Crew-Session header uses. A label a
            // header would reject must not be reachable by addressing it
            // instead — otherwise a caller could store sessions that can never
            // connect, and unbounded strings with them.
            let session = crate::auth::normalize_session(session)
                .map_err(|why| BusError::invalid(format!("the session in '{raw}' {why}")))?;
            if session.is_empty() {
                return Err(BusError::invalid(format!(
                    "'{raw}' has an empty session; write '{agent}' to reach every \
                     session of theirs, or '{agent}/<session>' for one of them"
                )));
            }
            Ok((agent.to_owned(), Some(session)))
        }
    }
}

/// The channel a session works in: the one named after it, if the team has
/// one.
///
/// Resolved by name every time rather than stored. A binding table would be
/// one more thing to keep in sync and to migrate, and it would go stale the
/// moment a channel is renamed; a lookup cannot. A team that does not name
/// channels after repositories simply gets no default and passes `channel`
/// exactly as it does today.
pub async fn default_channel(pool: &PgPool, auth: &AuthCtx) -> BusResult<Option<(Uuid, String)>> {
    if auth.session.is_empty() {
        return Ok(None);
    }
    let name = normalize_channel(&auth.session);
    let row: Option<(Uuid,)> =
        sqlx::query_as("SELECT id FROM channels WHERE team_id = $1 AND name = $2")
            .bind(auth.team_id)
            .bind(&name)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|r| (r.0, name)))
}

/// Read-cursor key for a scope, kept apart per session and per view.
///
/// Two windows of one person track their own reading position; sharing the
/// cursor would mean reading in one marks the other's messages read. The
/// cross-session view keeps its own for the same reason.
///
/// Collision-free by construction: `/` is the one character a session label
/// may not contain, so it is the separator. Concatenating a mode suffix would
/// not be — the per-session cursor for a session called `api+all-sessions`
/// would be byte-identical to the cross-session cursor for `api`, and reading
/// one view would silently skip messages in the other.
///
/// The shared session with the default view keeps the bare `base`, so cursors
/// written before sessions existed are still the ones read.
pub fn cursor_scope(base: &str, session: &str) -> String {
    cursor_scope_for(base, session, false)
}

pub fn cursor_scope_for(base: &str, session: &str, all_sessions: bool) -> String {
    match (session.is_empty(), all_sessions) {
        (true, false) => base.to_owned(),
        (_, false) => format!("{base}/s/{session}"),
        (_, true) => format!("{base}/a/{session}"),
    }
}

/// SQL predicate for "a direct message this session should see": addressed to
/// the session by name, or to the person with no session named.
///
/// `$1` is the agent, `$2` the session label.
pub const DM_FOR_SESSION: &str =
    "m.recipient_agent_id = $1 AND (m.recipient_session IS NULL OR m.recipient_session = $2)";

pub struct PostInput {
    pub channel: Option<String>,
    pub to: Option<String>,
    /// Reach every session of the team even when they are focused on their own
    /// channel. Channel messages only.
    pub announce: bool,
    pub body: String,
    pub reply_to: Option<i64>,
    pub metadata: Option<serde_json::Value>,
    /// Already decoded and size-checked; inserted in the same transaction as
    /// the message so readers never see the message without its files.
    pub attachments: Vec<super::attachments::NewAttachment>,
}

pub async fn post_message(
    pool: &PgPool,
    auth: &AuthCtx,
    input: PostInput,
) -> BusResult<PostMessageResult> {
    let body = input.body.trim().to_owned();
    if body.is_empty() {
        return Err(BusError::invalid("message body cannot be empty"));
    }
    if body.len() > MAX_BODY_BYTES {
        return Err(BusError::invalid(format!(
            "message body is {} bytes; the limit is {MAX_BODY_BYTES}. \
             Attach the file instead of pasting it, or split the message.",
            body.len()
        )));
    }

    // A direct message already arrives unfiltered — the focus rule never
    // applies to it — so the flag would promise something it does not change.
    //
    // Only when `to` is set *without* `channel`: with both set, the mutual
    // exclusion below is the real problem, and reporting this one instead
    // would send the caller to fix the wrong thing.
    if input.announce && input.to.is_some() && input.channel.is_none() {
        return Err(BusError::invalid(
            "`announce` applies to channel messages. A direct message already reaches \
             its recipient whatever they are focused on, so drop the flag, or post to \
             a channel if the whole team needs to see this.",
        ));
    }

    let (channel_id, recipient_id, recipient_session, delivered_to) = match (
        &input.channel,
        &input.to,
    ) {
        (Some(_), Some(_)) => {
            return Err(BusError::invalid(
                "set either `channel` or `to`, not both: a message is either broadcast or direct",
            ));
        }
        (None, None) => match default_channel(pool, auth).await? {
            // The session works in a repository and the team has a channel for
            // it: naming it on every call is friction with one right answer.
            Some((id, _name)) => {
                let names: Vec<(String,)> = sqlx::query_as(
                    "SELECT name FROM agents WHERE team_id = $1 AND disabled_at IS NULL ORDER BY name",
                )
                .bind(auth.team_id)
                .fetch_all(pool)
                .await?;
                (
                    Some(id),
                    None,
                    None,
                    names.into_iter().map(|r| r.0).collect::<Vec<_>>(),
                )
            }
            None if auth.session.is_empty() => {
                return Err(BusError::invalid(
                    "set `channel` to broadcast, or `to` to send a direct message",
                ));
            }
            None => {
                return Err(BusError::invalid(format!(
                    "set `channel` to broadcast, or `to` to send a direct message. \
                     This session is '{}', and there is no channel of that name to \
                     fall back on — create_channel '{}' to make it the default here.",
                    auth.session, auth.session
                )));
            }
        },
        (Some(channel), None) => {
            let id = channel_id_by_name(pool, auth.team_id, channel).await?;
            // Everyone in the team can read a channel, so report the roster.
            let names: Vec<(String,)> = sqlx::query_as(
                "SELECT name FROM agents WHERE team_id = $1 AND disabled_at IS NULL ORDER BY name",
            )
            .bind(auth.team_id)
            .fetch_all(pool)
            .await?;
            (
                Some(id),
                None,
                None,
                names.into_iter().map(|r| r.0).collect::<Vec<_>>(),
            )
        }
        (None, Some(to)) => {
            let (agent, session) = parse_address(to)?;
            let id = agent_id_by_name(pool, auth.team_id, &agent).await?;
            // Talking to the window you are already in is a mistake worth
            // naming: nothing would ever read it, and a note or a channel is
            // what the caller actually wants.
            // Only the exact calling window is refused. Addressing yourself as
            // a *person* is allowed and useful: it reaches your other windows,
            // including ones not open yet — the same reason a message to a
            // session that is currently closed is accepted.
            if id == auth.agent_id && session.as_deref() == Some(auth.session.as_str()) {
                return Err(BusError::invalid(
                    "that address is this session — a message to yourself here would \
                     never be read. Use set_note to leave something durable, post to \
                     a channel, or address another of your sessions as 'you/<session>'.",
                ));
            }
            // Reported back so a mistyped session shows up in the response
            // rather than becoming a message nobody ever opens.
            let label = match &session {
                Some(s) => format!("{agent}/{s}"),
                None => agent.clone(),
            };
            (None, Some(id), session, vec![label])
        }
    };

    // A reply must point at a message in the same team.
    if let Some(reply_to) = input.reply_to {
        let exists: Option<(i64,)> =
            sqlx::query_as("SELECT id FROM messages WHERE id = $1 AND team_id = $2")
                .bind(reply_to)
                .bind(auth.team_id)
                .fetch_optional(pool)
                .await?;
        if exists.is_none() {
            return Err(BusError::not_found(format!("message {reply_to}")));
        }
    }

    let metadata_in = super::normalize_metadata(input.metadata);
    super::check_metadata("message", metadata_in.as_ref())?;
    let metadata = metadata_in.unwrap_or_else(|| serde_json::Value::Object(Default::default()));

    // Message + attachments commit together, so the NOTIFY that wakes
    // teammates only fires once everything is readable.
    let mut tx = pool.begin().await?;

    let (id,): (i64,) = sqlx::query_as(
        r#"
        INSERT INTO messages
            (team_id, channel_id, recipient_agent_id, recipient_session,
             sender_agent_id, sender_session, body, reply_to, metadata, announce)
        VALUES ($1, $2, $3, $8, $4, $9, $5, $6, $7, $10)
        RETURNING id
        "#,
    )
    .bind(auth.team_id)
    .bind(channel_id)
    .bind(recipient_id)
    .bind(auth.agent_id)
    .bind(&body)
    .bind(input.reply_to)
    .bind(&metadata)
    .bind(recipient_session.as_deref())
    // Recorded even on channel messages: a reply needs to know which window
    // asked, or it goes back to whichever one happens to notice.
    .bind(super::session_label(auth))
    .bind(input.announce)
    .fetch_one(&mut *tx)
    .await?;

    for att in &input.attachments {
        super::attachments::insert_for_message(&mut tx, auth.team_id, id, auth.agent_id, att)
            .await?;
    }

    let row: MessageRow =
        sqlx::query_as(AssertSqlSafe(format!("{MESSAGE_SELECT} WHERE m.id = $1")))
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;

    tx.commit().await?;

    Ok(PostMessageResult {
        message: row.into(),
        delivered_to,
    })
}

// ----------------------------------------------------------------- asking --

/// The answer to a question DM: their explicit reply if there is one, else
/// their first direct message to the asker after the question was sent.
pub async fn find_answer(
    pool: &PgPool,
    auth: &AuthCtx,
    target_id: Uuid,
    target_session: Option<&str>,
    question_id: i64,
) -> BusResult<Option<MessageInfo>> {
    // Addressed to this session or to the person: an answer sent to another of
    // the asker's windows is not an answer to the window that is blocked here.
    //
    // And when the question named a session, only that session can answer it.
    // Matching the agent alone let any later message from a *sibling* window
    // of the target satisfy the wait and be returned as the answer from the
    // one that was asked.
    let row: Option<MessageRow> = sqlx::query_as(AssertSqlSafe(format!(
        r#"{MESSAGE_SELECT}
           WHERE m.sender_agent_id = $1
             AND m.recipient_agent_id = $2
             AND m.id > $3
             AND (m.recipient_session IS NULL OR m.recipient_session = $4)
             AND ($5::text IS NULL OR COALESCE(m.sender_session, '') = $5)
           ORDER BY (m.reply_to = $3) DESC NULLS LAST, m.id
           LIMIT 1"#
    )))
    .bind(target_id)
    .bind(auth.agent_id)
    .bind(question_id)
    .bind(&auth.session)
    .bind(target_session)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(Into::into))
}

/// A resumed ask must point at a question the caller actually sent to the
/// target; anything else means a mixed-up id.
pub async fn verify_question(
    pool: &PgPool,
    auth: &AuthCtx,
    target_id: Uuid,
    target_session: Option<&str>,
    question_id: i64,
) -> BusResult<()> {
    // Sent by this session, not merely by this agent: resuming someone else's
    // wait — even your own other window's — would hand them each other's
    // answers.
    // The addressee has to match too, or a question sent to one session could
    // be resumed naming another and collect that one's answer instead.
    let exists: Option<(i64,)> = sqlx::query_as(
        "SELECT id FROM messages
         WHERE id = $1 AND sender_agent_id = $2 AND recipient_agent_id = $3
           AND COALESCE(sender_session, '') = $4
           AND recipient_session IS NOT DISTINCT FROM $5",
    )
    .bind(question_id)
    .bind(auth.agent_id)
    .bind(target_id)
    .bind(&auth.session)
    .bind(target_session)
    .fetch_optional(pool)
    .await?;
    if exists.is_none() {
        return Err(BusError::invalid(format!(
            "message {question_id} is not a question this session sent to that exact \
             address; pass the question_message_id returned by ask_agent in this \
             session, with the same `to`"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------- reading --

/// Normalised read scope plus the cursor key used to remember the read position.
enum Scope {
    All,
    Inbox,
    Channel { id: Uuid, name: String },
}

impl Scope {
    /// Namespaced per session: two windows of one person each track their own
    /// reading position, so catching up in one does not mark the other read.
    fn cursor_key_for(&self, auth: &AuthCtx, all_sessions: bool) -> String {
        let base = match self {
            Scope::All => "all".to_owned(),
            Scope::Inbox => "inbox".to_owned(),
            Scope::Channel { id, .. } => format!("channel:{id}"),
        };
        cursor_scope_for(&base, &auth.session, all_sessions)
    }
    fn label(&self) -> String {
        match self {
            Scope::All => "all".into(),
            Scope::Inbox => "inbox".into(),
            Scope::Channel { name, .. } => format!("#{name}"),
        }
    }
}

async fn resolve_scope(pool: &PgPool, auth: &AuthCtx, raw: &str) -> BusResult<Scope> {
    match raw.trim().to_lowercase().as_str() {
        "" | "all" => Ok(Scope::All),
        "inbox" | "dm" | "direct" => Ok(Scope::Inbox),
        other => {
            let name = normalize_channel(other);
            let id = channel_id_by_name(pool, auth.team_id, &name).await?;
            Ok(Scope::Channel { id, name })
        }
    }
}

pub struct ReadInput {
    pub scope: String,
    pub only_new: bool,
    pub limit: i64,
    /// Include direct messages addressed to your *other* sessions. Off by
    /// default so a window sees its own work, on when you want the whole
    /// picture — the bus never hides a person's own messages from them.
    pub all_sessions: bool,
}

pub async fn read_messages(
    pool: &PgPool,
    auth: &AuthCtx,
    input: ReadInput,
) -> BusResult<MessageList> {
    let scope = resolve_scope(pool, auth, &input.scope).await?;
    let limit = input.limit.clamp(1, MAX_LIMIT);
    // Reading across sessions is a different view of a different set of
    // messages, so it keeps its own cursor rather than moving this session's.
    let cursor_key = scope.cursor_key_for(auth, input.all_sessions);
    // True widens the DM filter to every session of this agent; false keeps it
    // to what is addressed to this one, or to the person.
    let session_filter = input.all_sessions;

    let since: i64 = if input.only_new {
        sqlx::query_as::<_, (i64,)>(
            "SELECT last_message_id FROM read_cursors WHERE agent_id = $1 AND scope = $2",
        )
        .bind(auth.agent_id)
        .bind(&cursor_key)
        .fetch_optional(pool)
        .await?
        .map(|r| r.0)
        .unwrap_or(0)
    } else {
        0
    };

    // Ordered ascending so the agent reads the conversation in chronological
    // order; when not filtering by cursor we take the newest `limit` and then
    // flip them back, so "the last N messages" is what you get.
    let rows: Vec<MessageRow> = match &scope {
        Scope::All => {
            sqlx::query_as(AssertSqlSafe(format!(
                r#"{MESSAGE_SELECT}
                   WHERE m.team_id = $1
                     AND m.id > $2
                     AND (m.channel_id IS NOT NULL
                          OR (m.recipient_agent_id = $3
                              AND ($5::bool
                                   OR m.recipient_session IS NULL
                                   OR m.recipient_session = $6))
                          OR m.sender_agent_id = $3)
                   ORDER BY m.id DESC
                   LIMIT $4"#
            )))
            .bind(auth.team_id)
            .bind(since)
            .bind(auth.agent_id)
            .bind(limit)
            .bind(session_filter)
            .bind(&auth.session)
            .fetch_all(pool)
            .await?
        }
        Scope::Inbox => {
            sqlx::query_as(AssertSqlSafe(format!(
                r#"{MESSAGE_SELECT}
                   WHERE m.recipient_agent_id = $1
                     AND m.id > $2
                     AND ($4::bool
                          OR m.recipient_session IS NULL
                          OR m.recipient_session = $5)
                   ORDER BY m.id DESC
                   LIMIT $3"#
            )))
            .bind(auth.agent_id)
            .bind(since)
            .bind(limit)
            .bind(session_filter)
            .bind(&auth.session)
            .fetch_all(pool)
            .await?
        }
        Scope::Channel { id, .. } => {
            sqlx::query_as(AssertSqlSafe(format!(
                r#"{MESSAGE_SELECT}
                   WHERE m.channel_id = $1 AND m.id > $2
                   ORDER BY m.id DESC
                   LIMIT $3"#
            )))
            .bind(*id)
            .bind(since)
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
    };

    let truncated = rows.len() as i64 == limit;
    let mut messages: Vec<MessageInfo> = rows.into_iter().map(Into::into).collect();
    messages.reverse();

    let new_cursor = messages.iter().map(|m| m.id).max().unwrap_or(since);
    if input.only_new && new_cursor > since {
        sqlx::query(
            r#"
            INSERT INTO read_cursors (agent_id, scope, last_message_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (agent_id, scope) DO UPDATE
                SET last_message_id = GREATEST(read_cursors.last_message_id, EXCLUDED.last_message_id),
                    updated_at = now()
            "#,
        )
        .bind(auth.agent_id)
        .bind(&cursor_key)
        .bind(new_cursor)
        .execute(pool)
        .await?;
    }

    Ok(MessageList {
        messages,
        scope: scope.label(),
        cursor: new_cursor,
        truncated,
    })
}

pub async fn search_messages(
    pool: &PgPool,
    auth: &AuthCtx,
    query: &str,
    limit: i64,
) -> BusResult<MessageList> {
    let query = query.trim();
    if query.is_empty() {
        return Err(BusError::invalid("search query cannot be empty"));
    }
    let limit = limit.clamp(1, MAX_LIMIT);

    let rows: Vec<MessageRow> = sqlx::query_as(AssertSqlSafe(format!(
        r#"{MESSAGE_SELECT}
           WHERE m.team_id = $1
             AND (m.channel_id IS NOT NULL
                  OR m.recipient_agent_id = $2
                  OR m.sender_agent_id = $2)
             AND to_tsvector('simple', m.body) @@ plainto_tsquery('simple', $3)
           ORDER BY m.id DESC
           LIMIT $4"#
    )))
    .bind(auth.team_id)
    .bind(auth.agent_id)
    .bind(query)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let truncated = rows.len() as i64 == limit;
    let messages: Vec<MessageInfo> = rows.into_iter().map(Into::into).collect();
    let cursor = messages.iter().map(|m| m.id).max().unwrap_or(0);

    Ok(MessageList {
        messages,
        scope: format!("search:{query}"),
        cursor,
        truncated,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_address_validates_its_session_like_the_header_does() {
        // A label the X-Crew-Session header would reject must not be reachable
        // by addressing it instead.
        assert!(parse_address(&format!("dani/{}", "x".repeat(1000))).is_err());
        assert!(parse_address("dani/${BUS_SESSION}").is_err());
        assert!(parse_address("dani/api\u{7f}").is_err());
        assert!(parse_address("dani/").is_err());
        assert!(parse_address("/api").is_err());
    }

    #[test]
    fn an_address_normalises_its_session_like_the_header_does() {
        assert_eq!(
            parse_address("dani/ Market-Data ").unwrap(),
            ("dani".to_owned(), Some("market-data".to_owned()))
        );
        assert_eq!(parse_address("dani").unwrap(), ("dani".to_owned(), None));
    }

    #[test]
    fn cursor_keys_cannot_collide_between_a_session_and_a_view() {
        // The trap: concatenating a mode suffix makes the per-session cursor
        // for a session called `api+all-sessions` byte-identical to the
        // cross-session cursor for `api`, so reading one view advances the
        // other and silently skips messages.
        let per_session_of_odd_label = cursor_scope_for("inbox", "api+all-sessions", false);
        let all_sessions_of_api = cursor_scope_for("inbox", "api", true);
        assert_ne!(per_session_of_odd_label, all_sessions_of_api);

        // Every combination stays distinct.
        let keys = [
            cursor_scope_for("inbox", "", false),
            cursor_scope_for("inbox", "", true),
            cursor_scope_for("inbox", "api", false),
            cursor_scope_for("inbox", "api", true),
            per_session_of_odd_label,
            cursor_scope_for("inbox", "api+all-sessions", true),
        ];
        let unique: std::collections::HashSet<&String> = keys.iter().collect();
        assert_eq!(unique.len(), keys.len(), "{keys:?}");
    }

    #[test]
    fn the_shared_session_keeps_the_cursor_it_already_had() {
        // Cursors written before sessions existed must still be the ones read,
        // or every agent re-reads its whole history once on upgrade.
        assert_eq!(cursor_scope_for("inbox", "", false), "inbox");
        assert_eq!(cursor_scope_for("all", "", false), "all");
    }
}
