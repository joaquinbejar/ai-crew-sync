//! Session credentials: registration, resume, renewal and revocation.
//!
//! A session credential is derived from an agent token and proves *which
//! window* is calling. Everything it is comes from its parent: agent, team,
//! and the right to authenticate at all. It cannot mint anything, it expires
//! on its own, and a resume fences the connection it replaces (ADR 0001).
//!
//! The one invariant worth stating plainly: **the caller never says who it
//! is**. `register` reads the agent and the team from the token that
//! presented itself, so a client that asks to register "as someone else"
//! simply registers as itself.

use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    auth::{
        AuthCtx, MAX_SESSION_TTL_SECS, SESSION_TTL_SECS, generate_session_token, hash_token,
        normalize_session, token_prefix,
    },
    error::{BusError, BusResult},
    model::{SessionCredential, SessionIdentity},
};

/// A freshly registered or resumed session. `token` is the secret, returned
/// here and nowhere else.
pub struct Issued {
    pub id: Uuid,
    pub token: String,
    pub label: String,
    pub epoch: i64,
    pub expires_at: chrono::DateTime<chrono::Utc>,
}

fn ttl_of(requested: Option<i64>) -> i64 {
    requested
        .unwrap_or(SESSION_TTL_SECS)
        .clamp(60, MAX_SESSION_TTL_SECS)
}

/// Register a session for `label`.
///
/// Must be called with an **agent token**: a session credential cannot mint
/// another, which is what keeps a leaked window credential from becoming a
/// family of them.
///
/// A label that already has a live session is **refused**. Holding the agent
/// token is not proof of being that window: allowing a silent replacement
/// would let any holder of the token — or a second token of the same agent —
/// take over a running conversation, inherit its cursors and claims, and cut
/// the real window off. Resuming is [`resume`], which requires the session's
/// own credential; taking a dead window back is an explicit `revoke_session`
/// first, which is the owner's audited recovery path.
pub async fn register(
    pool: &PgPool,
    auth: &AuthCtx,
    parent_token: Uuid,
    label: &str,
    ttl_seconds: Option<i64>,
) -> BusResult<Issued> {
    if auth.session_is_authenticated() {
        return Err(BusError::Forbidden(
            "a session credential cannot register another session. Register with the agent \
             token that this window's credential was derived from."
                .to_owned(),
        ));
    }
    let label = normalize_session(label)
        .map_err(|why| BusError::invalid(format!("the session label {why}")))?;
    if label.is_empty() {
        return Err(BusError::invalid(
            "a session label is required: it is the address teammates use to reach this \
             window (agent/session). Use the id your host gives the conversation.",
        ));
    }
    let ttl = ttl_of(ttl_seconds);
    let raw = generate_session_token();
    let prefix = token_prefix(&raw);

    // Serialised with owner recovery on the agent row. Recovery checks that
    // no window is live and then reads private history; without a lock both
    // of them can be true at once — it sees none, and this inserts one.
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT id FROM agents WHERE id = $1 FOR NO KEY UPDATE")
        .bind(auth.agent_id)
        .fetch_one(&mut *tx)
        .await?;
    // The token that made this request, re-read under the same lock a
    // revocation takes. A request authenticated a moment before its token
    // was revoked reaches this point after the revocation committed; a
    // session hung off that token would be a credential authentication
    // refuses on first use, and a row that reads as live for a day.
    let parent_live: Option<(bool,)> =
        sqlx::query_as("SELECT revoked_at IS NULL FROM api_tokens WHERE id = $1 AND agent_id = $2")
            .bind(parent_token)
            .bind(auth.agent_id)
            .fetch_optional(&mut *tx)
            .await?;
    if !matches!(parent_live, Some((true,))) {
        return Err(BusError::Unauthenticated(
            "the token that made this request was revoked while it was in flight; nothing \
             was registered. Issue a new token and register again."
                .to_owned(),
        ));
    }

    // One row per (agent, label), and the row is only *taken over* when the
    // one there is dead: revoked, or past its expiry. A live one belongs to
    // a window that can still speak for itself.
    let row: Option<(Uuid, i64, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
        r#"
        INSERT INTO agent_sessions
            (agent_id, parent_token, label, token_hash, prefix, expires_at)
        VALUES ($1, $2, $3, $4, $5, now() + make_interval(secs => $6))
        ON CONFLICT (agent_id, label) DO UPDATE SET
            parent_token = EXCLUDED.parent_token,
            token_hash   = EXCLUDED.token_hash,
            prefix = EXCLUDED.prefix,
            epoch = agent_sessions.epoch + 1,
            expires_at   = EXCLUDED.expires_at,
            revoked_at   = NULL,
            last_used_at = NULL
        WHERE agent_sessions.revoked_at IS NOT NULL
           OR agent_sessions.expires_at <= now()
           -- A window whose parent token was revoked cannot answer with its
           -- credential any more, whatever its own row says: the label is
           -- free. Revocation also marks the row, so this is the seam belt.
           OR EXISTS (SELECT 1 FROM api_tokens t
                       WHERE t.id = agent_sessions.parent_token
                         AND t.revoked_at IS NOT NULL)
        RETURNING id, epoch, expires_at
        "#,
    )
    .bind(auth.agent_id)
    .bind(parent_token)
    .bind(&label)
    .bind(hash_token(&raw))
    .bind(&prefix)
    .bind(ttl as f64)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;

    let Some((id, epoch, expires_at)) = row else {
        return Err(BusError::conflict(format!(
            "session '{label}' is already registered and still live. Holding the agent \
             token does not make you that window: reconnect it with resume_session, using \
             the session credential the process that owns it holds, or close it first \
             with revoke_session if it is gone for good."
        )));
    };

    Ok(Issued {
        id,
        token: raw,
        label,
        epoch,
        expires_at,
    })
}

/// Resume the caller's own session: rotate the secret and bump the epoch, so
/// the connection this one replaces is fenced at its next request. The proof
/// is the credential itself, which is why this is not something the agent
/// token can do.
pub async fn resume(pool: &PgPool, auth: &AuthCtx, ttl_seconds: Option<i64>) -> BusResult<Issued> {
    let Some(session_id) = auth.session_id else {
        return Err(BusError::Forbidden(
            "resume_session needs the session credential of the window being resumed. \
             With an agent token, register_session opens a new window and refuses a live \
             one."
                .to_owned(),
        ));
    };
    let ttl = ttl_of(ttl_seconds);
    let raw = generate_session_token();
    let prefix = token_prefix(&raw);
    // Same lock order as registration, revocation and recovery, and the
    // rotation is fenced on the epoch this credential authenticated with: a
    // request that was in flight while the window was revoked and its label
    // re-registered must not rotate the row the new window now holds.
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT id FROM agents WHERE id = $1 FOR NO KEY UPDATE")
        .bind(auth.agent_id)
        .fetch_one(&mut *tx)
        .await?;
    let row: Option<(i64, chrono::DateTime<chrono::Utc>, String)> = sqlx::query_as(
        "UPDATE agent_sessions s
            SET token_hash = $2,
                prefix = $3,
                epoch = s.epoch + 1,
                expires_at = now() + make_interval(secs => $4),
                last_used_at = NULL
          WHERE s.id = $1 AND s.revoked_at IS NULL AND s.epoch = $5
            AND NOT EXISTS (SELECT 1 FROM api_tokens t
                             WHERE t.id = s.parent_token AND t.revoked_at IS NOT NULL)
          RETURNING s.epoch, s.expires_at, s.label",
    )
    .bind(session_id)
    .bind(hash_token(&raw))
    .bind(&prefix)
    .bind(ttl as f64)
    .bind(auth.session_epoch.unwrap_or(0))
    .fetch_optional(&mut *tx)
    .await?;
    let Some((epoch, expires_at, label)) = row else {
        return Err(BusError::Unauthenticated(
            "this session credential is no longer the window's: it was revoked, or the \
             window was resumed or re-registered after this connection. Nothing was \
             rotated; register a new session with your agent token."
                .to_owned(),
        ));
    };
    tx.commit().await?;
    Ok(Issued {
        id: session_id,
        token: raw,
        label,
        epoch,
        expires_at,
    })
}

/// Extend the caller's own session without changing its epoch or secret.
/// The proof is the credential itself: only the holder can renew it.
pub async fn renew(pool: &PgPool, auth: &AuthCtx, ttl_seconds: Option<i64>) -> BusResult<Issued> {
    let Some(session_id) = auth.session_id else {
        return Err(BusError::Forbidden(
            "renew_session needs a session credential: it extends the credential that made \
             the call. Register one first with register_session."
                .to_owned(),
        ));
    };
    let ttl = ttl_of(ttl_seconds);
    // Fenced on the epoch too: a stale connection does not extend the
    // window that replaced it.
    let row: Option<(i64, chrono::DateTime<chrono::Utc>, String)> = sqlx::query_as(
        "UPDATE agent_sessions
            SET expires_at = now() + make_interval(secs => $2)
          WHERE id = $1 AND revoked_at IS NULL AND epoch = $3
          RETURNING epoch, expires_at, label",
    )
    .bind(session_id)
    .bind(ttl as f64)
    .bind(auth.session_epoch.unwrap_or(0))
    .fetch_optional(pool)
    .await?;
    let Some((epoch, expires_at, label)) = row else {
        return Err(BusError::not_found(
            "this session has been revoked; register a new one with your agent token",
        ));
    };
    Ok(Issued {
        // Renewal keeps the secret: the caller already holds it, and handing
        // back a new one would fence the very connection that asked.
        id: session_id,
        token: String::new(),
        label,
        epoch,
        expires_at,
    })
}

/// Revoke a session. The caller's own by default; with `label`, another
/// session **of the same agent**, which is how a supervisor window closes one
/// that crashed. Never another agent's, whatever the label says.
pub async fn revoke(pool: &PgPool, auth: &AuthCtx, label: Option<&str>) -> BusResult<String> {
    let target = match label.map(str::trim).filter(|l| !l.is_empty()) {
        Some(l) => normalize_session(l)
            .map_err(|why| BusError::invalid(format!("the session label {why}")))?,
        None => {
            if !auth.session_is_authenticated() {
                return Err(BusError::invalid(
                    "say which session to revoke: this call was made with an agent token, \
                     which is not itself a session",
                ));
            }
            auth.session.clone()
        }
    };
    let row: Option<(Uuid,)> = sqlx::query_as(
        "UPDATE agent_sessions SET revoked_at = now()
          WHERE agent_id = $1 AND label = $2 AND revoked_at IS NULL
          RETURNING id",
    )
    .bind(auth.agent_id)
    .bind(&target)
    .fetch_optional(pool)
    .await?;
    if row.is_none() {
        // Already revoked, expired-and-swept or never existed: the caller
        // wanted it gone and it is gone. Reporting which of the three would
        // tell a caller whether a label exists for an agent it is not.
        let exists: Option<(Uuid,)> =
            sqlx::query_as("SELECT id FROM agent_sessions WHERE agent_id = $1 AND label = $2")
                .bind(auth.agent_id)
                .bind(&target)
                .fetch_optional(pool)
                .await?;
        if exists.is_none() {
            return Err(BusError::not_found(format!(
                "no session '{target}' of yours"
            )));
        }
    }
    Ok(target)
}

/// Re-check, **inside the caller's transaction**, that this connection may
/// still write.
///
/// The middleware's check happens before dispatch, which is not the same
/// thing: a request that was already waiting on a row lock when its window
/// was resumed would wake up and commit into the session that replaced it.
/// Running the check in the same transaction as the mutation serialises the
/// two — the resume either happens before this SELECT, and the write is
/// refused, or after the COMMIT, and the write was legitimate.
///
/// A caller holding a plain agent token has no epoch to fence and passes
/// through untouched; its label was never a claim of exclusivity.
pub async fn guard(tx: &mut sqlx::PgConnection, auth: &AuthCtx) -> BusResult<()> {
    let (Some(session_id), Some(epoch)) = (auth.session_id, auth.session_epoch) else {
        return Ok(());
    };
    // FOR SHARE: concurrent writers of the same session may proceed
    // together, and a resume (which updates the row) waits for them.
    let row: Option<(i64, bool, bool)> = sqlx::query_as(
        "SELECT epoch, (revoked_at IS NOT NULL), (expires_at <= now())
           FROM agent_sessions WHERE id = $1 FOR SHARE",
    )
    .bind(session_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((current, revoked, expired)) = row else {
        return Err(BusError::Unauthenticated(
            "this session no longer exists; register a new one with your agent token".to_owned(),
        ));
    };
    if revoked || expired {
        return Err(BusError::Unauthenticated(
            "this session credential is no longer valid; register a new one with your agent \
             token"
                .to_owned(),
        ));
    }
    if current != epoch {
        return Err(BusError::conflict(format!(
            "this connection is stale: it carries epoch {epoch} and the session is at \
             {current}, so another process resumed this window after you. Nothing was \
             written. Resume the session to take over, or exit."
        )));
    }
    Ok(())
}

/// Refuse a call that wears a live window's label without its credential.
///
/// The label in a header is a name a caller chooses. When that name belongs
/// to a registered window, only that window may act as it — otherwise the
/// parent agent token could read that window's private references, confirm
/// deliveries for it and speak in its threads, with none of the audit trail
/// the documented recovery path carries.
pub async fn require_window(pool: &PgPool, auth: &AuthCtx) -> BusResult<()> {
    if auth.session.is_empty() || auth.session_id.is_some() {
        return Ok(());
    }
    // Registered at all, not registered *and still live*. Revoking or
    // expiring a window must not turn it back into a label anyone holding
    // the agent token can wear: that would make revocation a way in rather
    // than a way out. The audited path for an agent to reach a window that
    // is gone is `recover_conversation_history`, which requires every
    // window to be closed and grants nothing.
    let (registered,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM agent_sessions WHERE agent_id = $1 AND label = $2")
            .bind(auth.agent_id)
            .bind(&auth.session)
            .fetch_one(pool)
            .await?;
    if registered == 0 {
        return Ok(());
    }
    Err(BusError::Forbidden(format!(
        "'{}' is a registered window and this call carries an agent token, not that \
         window's session credential. Revoking or expiring it does not hand the label \
         back: ask that window to make the call, register it again, or use \
         recover_conversation_history, which is the audited way for an agent to reach \
         its own windows' threads.",
        auth.session
    )))
}

/// The identity a session credential proves, for `whoami`.
pub async fn identity(pool: &PgPool, auth: &AuthCtx) -> BusResult<Option<SessionIdentity>> {
    let Some(session_id) = auth.session_id else {
        return Ok(None);
    };
    let row: Option<(
        i64,
        chrono::DateTime<chrono::Utc>,
        chrono::DateTime<chrono::Utc>,
    )> = sqlx::query_as("SELECT epoch, created_at, expires_at FROM agent_sessions WHERE id = $1")
        .bind(session_id)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|(epoch, created_at, expires_at)| SessionIdentity {
        session_id: session_id.to_string(),
        epoch,
        registered_at: crate::model::ts(created_at),
        expires_at: crate::model::ts(expires_at),
        expires_in_seconds: (expires_at - chrono::Utc::now()).num_seconds().max(0),
    }))
}

/// Wire form of a freshly issued credential.
pub fn credential_of(issued: Issued, agent: &str) -> SessionCredential {
    SessionCredential {
        session_token: (!issued.token.is_empty()).then(|| issued.token.clone()),
        session_id: issued.id.to_string(),
        session: issued.label.clone(),
        address: format!("{agent}/{}", issued.label),
        epoch: issued.epoch,
        expires_at: crate::model::ts(issued.expires_at),
        expires_in_seconds: (issued.expires_at - chrono::Utc::now())
            .num_seconds()
            .max(0),
    }
}
