use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

use crate::{
    auth::AuthCtx,
    error::{BusError, BusResult},
    model::{Ack, LockInfo, LockList, LockResult, ts},
};

const DEFAULT_TTL_SECS: i64 = 300;
const MAX_TTL_SECS: i64 = 86_400;

fn normalize_name(name: &str) -> BusResult<String> {
    let name = name.trim().to_lowercase();
    if name.is_empty() {
        return Err(BusError::invalid("lock name cannot be empty"));
    }
    if name.len() > 128 {
        return Err(BusError::invalid("lock name is limited to 128 characters"));
    }
    Ok(name)
}

#[derive(sqlx::FromRow)]
struct LockRow {
    name: String,
    holder: String,
    holder_session: Option<String>,
    purpose: Option<String>,
    acquired_at: chrono::DateTime<chrono::Utc>,
    expires_at: chrono::DateTime<chrono::Utc>,
}

impl From<LockRow> for LockInfo {
    fn from(r: LockRow) -> Self {
        LockInfo {
            name: r.name,
            holder: r.holder,
            // '' and NULL are both the shared session.
            holder_session: r.holder_session.filter(|s| !s.is_empty()),
            purpose: r.purpose,
            acquired_at: ts(r.acquired_at),
            expires_at: ts(r.expires_at),
        }
    }
}

const LOCK_SELECT: &str = r#"
    SELECT l.name, a.name AS holder, l.holder_session, l.purpose,
           l.acquired_at, l.expires_at
    FROM locks l
    JOIN agents a ON a.id = l.holder_agent_id
"#;

/// Acquire (or, for the current holder, extend) an advisory lock. Expired
/// locks are taken over silently — the previous holder is presumed dead.
pub async fn acquire_lock(
    pool: &PgPool,
    auth: &AuthCtx,
    name: &str,
    ttl_seconds: Option<i64>,
    purpose: Option<String>,
) -> BusResult<LockResult> {
    let name = normalize_name(name)?;
    let ttl = ttl_seconds
        .unwrap_or(DEFAULT_TTL_SECS)
        .clamp(5, MAX_TTL_SECS);

    // Serialised with the mutation: a request already queued when its window
    // was resumed must not commit into the session that replaced it.
    let mut tx = pool.begin().await?;
    super::sessions::guard(&mut tx, auth).await?;

    let acquired: Option<(Uuid,)> = sqlx::query_as(
        r#"
        INSERT INTO locks
            (team_id, name, holder_agent_id, holder_session, purpose, acquired_at, expires_at)
        VALUES ($1, $2, $3, $6, $4, now(), now() + make_interval(secs => $5))
        ON CONFLICT (team_id, name) DO UPDATE SET
            holder_agent_id = EXCLUDED.holder_agent_id,
            holder_session = EXCLUDED.holder_session,
            purpose = COALESCE(EXCLUDED.purpose, locks.purpose),
            acquired_at = CASE
                WHEN locks.holder_agent_id = EXCLUDED.holder_agent_id
                 AND COALESCE(locks.holder_session, '') = EXCLUDED.holder_session
                THEN locks.acquired_at
                ELSE EXCLUDED.acquired_at
            END,
            expires_at = EXCLUDED.expires_at
        -- Extending your own lock is fine; taking it from another session,
        -- even your own, is not. A deploy lock held by your core-manager
        -- window must not be silently inherited by your market-data one.
        WHERE locks.expires_at < now()
           OR (locks.holder_agent_id = EXCLUDED.holder_agent_id
               AND COALESCE(locks.holder_session, '') = EXCLUDED.holder_session)
        RETURNING holder_agent_id
        "#,
    )
    .bind(auth.team_id)
    .bind(&name)
    .bind(auth.agent_id)
    .bind(purpose.as_deref())
    .bind(ttl as f64)
    .bind(&auth.session)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;

    let current: Option<LockRow> = sqlx::query_as(AssertSqlSafe(format!(
        "{LOCK_SELECT} WHERE l.team_id = $1 AND l.name = $2"
    )))
    .bind(auth.team_id)
    .bind(&name)
    .fetch_optional(pool)
    .await?;

    match acquired {
        Some(_) => Ok(LockResult {
            acquired: true,
            lock: current.map(Into::into),
            reason: None,
        }),
        None => {
            let holder = current
                .as_ref()
                .map(|l| l.holder.clone())
                .unwrap_or_else(|| "?".into());
            let until = current
                .as_ref()
                .map(|l| ts(l.expires_at))
                .unwrap_or_else(|| "unknown".into());
            // Naming the session matters most when the holder is you: "held by
            // joaquin" reads as a bug when you are joaquin.
            let session = current
                .as_ref()
                .and_then(|l| l.holder_session.as_deref())
                .filter(|s| !s.is_empty())
                .map(|s| format!(" (session '{s}')"))
                .unwrap_or_default();
            Ok(LockResult {
                acquired: false,
                lock: current.map(Into::into),
                reason: Some(format!("held by {holder}{session} until {until}")),
            })
        }
    }
}

pub async fn release_lock(pool: &PgPool, auth: &AuthCtx, name: &str) -> BusResult<Ack> {
    let name = normalize_name(name)?;

    // Fenced like acquiring. The holder predicate matches the agent and the
    // label, which a resumed window shares with the connection it replaced;
    // only the epoch tells them apart, and a replaced connection must not
    // put back a lock its replacement is holding.
    let mut tx = pool.begin().await?;
    super::sessions::guard(&mut tx, auth).await?;
    let released = sqlx::query(
        "DELETE FROM locks
              WHERE team_id = $1 AND name = $2 AND holder_agent_id = $3
                AND COALESCE(holder_session, '') = $4",
    )
    .bind(auth.team_id)
    .bind(&name)
    .bind(auth.agent_id)
    .bind(&auth.session)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    if released.rows_affected() > 0 {
        return Ok(Ack {
            ok: true,
            detail: format!("released lock '{name}'"),
        });
    }

    // Not ours (or gone): explain rather than silently "ok".
    let current: Option<(String, Option<String>)> = sqlx::query_as(
        r#"
        SELECT a.name, l.holder_session FROM locks l
        JOIN agents a ON a.id = l.holder_agent_id
        WHERE l.team_id = $1 AND l.name = $2 AND l.expires_at > now()
        "#,
    )
    .bind(auth.team_id)
    .bind(&name)
    .fetch_optional(pool)
    .await?;

    match current {
        Some((holder, session)) if holder == auth.agent_name => {
            let theirs = session
                .filter(|s| !s.is_empty())
                .map(|s| format!("'{s}'"))
                .unwrap_or_else(|| "shared".to_owned());
            Err(BusError::conflict(format!(
                "lock '{name}' is held by your own {theirs} session, not by this one — \
                 release it there, or wait for it to expire"
            )))
        }
        Some((holder, session)) => {
            let where_ = session
                .filter(|s| !s.is_empty())
                .map(|s| format!(" (session '{s}')"))
                .unwrap_or_default();
            Err(BusError::conflict(format!(
                "lock '{name}' is held by {holder}{where_}, not by you"
            )))
        }
        None => Ok(Ack {
            ok: false,
            detail: format!("lock '{name}' was not held"),
        }),
    }
}

pub async fn list_locks(pool: &PgPool, auth: &AuthCtx) -> BusResult<LockList> {
    let rows: Vec<LockRow> = sqlx::query_as(AssertSqlSafe(format!(
        "{LOCK_SELECT} WHERE l.team_id = $1 AND l.expires_at > now() ORDER BY l.name"
    )))
    .bind(auth.team_id)
    .fetch_all(pool)
    .await?;

    Ok(LockList {
        locks: rows.into_iter().map(Into::into).collect(),
    })
}
