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

/// Register a session for `label`, or resume the one that exists.
///
/// Must be called with an **agent token**: a session credential cannot mint
/// another, which is what keeps a leaked window credential from becoming a
/// family of them. Resuming rotates the secret and bumps the epoch, so the
/// process that was replaced is fenced off at its next request.
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

    // One row per (agent, label). A repeat is a resume: new secret, higher
    // epoch, fresh expiry — the window keeps its identity and its history,
    // and whoever held the previous secret is fenced.
    let row: (Uuid, i64, chrono::DateTime<chrono::Utc>) = sqlx::query_as(
        r#"
        INSERT INTO agent_sessions
            (agent_id, parent_token, label, token_hash, prefix, expires_at)
        VALUES ($1, $2, $3, $4, $5, now() + make_interval(secs => $6))
        ON CONFLICT (agent_id, label) DO UPDATE SET
            parent_token = EXCLUDED.parent_token,
            token_hash   = EXCLUDED.token_hash,
            prefix       = EXCLUDED.prefix,
            epoch        = agent_sessions.epoch + 1,
            expires_at   = EXCLUDED.expires_at,
            revoked_at   = NULL,
            last_used_at = NULL
        RETURNING id, epoch, expires_at
        "#,
    )
    .bind(auth.agent_id)
    .bind(parent_token)
    .bind(&label)
    .bind(hash_token(&raw))
    .bind(&prefix)
    .bind(ttl as f64)
    .fetch_one(pool)
    .await?;

    Ok(Issued {
        id: row.0,
        token: raw,
        label,
        epoch: row.1,
        expires_at: row.2,
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
    let row: Option<(i64, chrono::DateTime<chrono::Utc>, String)> = sqlx::query_as(
        "UPDATE agent_sessions
            SET expires_at = now() + make_interval(secs => $2)
          WHERE id = $1 AND revoked_at IS NULL
          RETURNING epoch, expires_at, label",
    )
    .bind(session_id)
    .bind(ttl as f64)
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
