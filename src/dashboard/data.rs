//! Everything the dashboard reads, and nothing about how it looks.
//!
//! Split out of the page builder so the queries can be read, changed and
//! measured without scrolling past HTML — and so the rendering layer receives
//! named fields instead of seven-element tuples whose meaning lived in the
//! order of a SELECT.

use sqlx::PgPool;
use uuid::Uuid;

#[derive(sqlx::FromRow)]
pub struct Totals {
    pub agents_online: i64,
    pub open_tasks: i64,
    pub claimed_tasks: i64,
    pub messages_24h: i64,
}

#[derive(sqlx::FromRow)]
pub struct AgentRow {
    pub name: String,
    /// Working context of this row. Empty is the shared session; an agent with
    /// several open produces one row each.
    pub session: String,
    pub status: Option<String>,
    pub repo: Option<String>,
    pub branch: Option<String>,
    pub activity: Option<String>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
    pub online: bool,
}

#[derive(sqlx::FromRow)]
pub struct TaskRow {
    pub key: String,
    pub title: String,
    pub status: String,
    pub claimed_by: Option<String>,
    pub result: Option<String>,
    pub blocked: bool,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(sqlx::FromRow)]
pub struct MessageRow {
    pub id: i64,
    pub channel: String,
    pub sender: String,
    /// Posted as an announcement — worth marking, since it is what the team
    /// was interrupted for.
    pub announce: bool,
    pub body: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(sqlx::FromRow)]
pub struct LockRow {
    pub name: String,
    pub holder: String,
    pub purpose: Option<String>,
    pub expires_at: chrono::DateTime<chrono::Utc>,
}

#[derive(sqlx::FromRow)]
pub struct NoteRow {
    pub scope: String,
    pub key: String,
    pub updated_by: Option<String>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// One refresh of the dashboard.
pub struct Snapshot {
    pub team: String,
    pub totals: Totals,
    pub agents: Vec<AgentRow>,
    pub tasks: Vec<TaskRow>,
    pub messages: Vec<MessageRow>,
    pub locks: Vec<LockRow>,
    pub notes: Vec<NoteRow>,
}

/// Read everything the page shows for `team_id`.
///
/// Direct messages are excluded here, at the source, rather than being
/// filtered later: the rendering layer never receives one, so it cannot leak
/// one by accident.
///
/// The seven queries are independent, so they run concurrently on separate
/// pool connections rather than one after another — the page refreshes every
/// 15 seconds, and serialising them made the refresh as slow as their sum.
/// Each sees its own snapshot, which is correct for a dashboard: the numbers
/// are already stale by the time they reach a browser, and no derived value
/// is computed across two of them.
pub async fn load(pool: &PgPool, team_id: Uuid) -> Result<Snapshot, sqlx::Error> {
    let (team, totals, agents, tasks, messages, locks, notes) = tokio::try_join!(
        load_team(pool, team_id),
        load_totals(pool, team_id),
        load_agents(pool, team_id),
        load_tasks(pool, team_id),
        load_messages(pool, team_id),
        load_locks(pool, team_id),
        load_notes(pool, team_id),
    )?;

    Ok(Snapshot {
        team,
        totals,
        agents,
        tasks,
        messages,
        locks,
        notes,
    })
}

async fn load_team(pool: &PgPool, team_id: Uuid) -> Result<String, sqlx::Error> {
    sqlx::query_scalar("SELECT slug FROM teams WHERE id = $1")
        .bind(team_id)
        .fetch_one(pool)
        .await
}

async fn load_totals(pool: &PgPool, team_id: Uuid) -> Result<Totals, sqlx::Error> {
    // The three stat-tile numbers in one round trip instead of three.
    sqlx::query_as(
        r#"
        SELECT
            -- DISTINCT because an agent has one presence row per session:
            -- someone working in three repositories is one teammate online,
            -- not three.
            (SELECT count(DISTINCT a.id) FROM agents a
              JOIN agent_presence p ON p.agent_id = a.id
             WHERE a.team_id = $1 AND p.expires_at > now()) AS agents_online,
            (SELECT count(*) FROM tasks
             WHERE team_id = $1 AND status = 'open') AS open_tasks,
            (SELECT count(*) FROM tasks
             WHERE team_id = $1 AND status = 'claimed') AS claimed_tasks,
            (SELECT count(*) FROM messages
             WHERE team_id = $1 AND channel_id IS NOT NULL
               AND created_at > now() - interval '24 hours') AS messages_24h
        "#,
    )
    .bind(team_id)
    .fetch_one(pool)
    .await
}

async fn load_agents(pool: &PgPool, team_id: Uuid) -> Result<Vec<AgentRow>, sqlx::Error> {
    sqlx::query_as(
        r#"
        -- One row per working context: a teammate with three repositories open
        -- appears three times, each with its own repo, branch and activity.
        -- Collapsing them would put one of the three on screen and drop the
        -- rest, which is the flapping board this whole change exists to fix.
        SELECT a.name,
               COALESCE(p.session, '') AS session,
               p.status,
               p.repo,
               p.branch,
               p.activity,
               p.updated_at,
               COALESCE(p.expires_at > now(), false) AS online
        FROM agents a
        LEFT JOIN agent_presence p ON p.agent_id = a.id
        WHERE a.team_id = $1 AND a.disabled_at IS NULL
        ORDER BY COALESCE(p.expires_at > now(), false) DESC,
                 a.name,
                 p.updated_at DESC NULLS LAST
        "#,
    )
    .bind(team_id)
    .fetch_all(pool)
    .await
}

/// Claimed and open first — the work in flight is what a human came to see.
async fn load_tasks(pool: &PgPool, team_id: Uuid) -> Result<Vec<TaskRow>, sqlx::Error> {
    sqlx::query_as(
        r#"
        SELECT t.key,
               t.title,
               t.status,
               cb.name AS claimed_by,
               t.result,
               EXISTS (
                   SELECT 1 FROM task_deps td
                   JOIN tasks d ON d.id = td.blocked_by_task_id
                   WHERE td.task_id = t.id AND d.status NOT IN ('done', 'cancelled')
               ) AS blocked,
               t.updated_at
        FROM tasks t
        LEFT JOIN agents cb ON cb.id = t.claimed_by
        WHERE t.team_id = $1
        ORDER BY CASE t.status WHEN 'claimed' THEN 0 WHEN 'open' THEN 1 ELSE 2 END,
                 t.updated_at DESC
        LIMIT 30
        "#,
    )
    .bind(team_id)
    .fetch_all(pool)
    .await
}

/// The JOIN on channels is what excludes direct messages: a DM has no
/// channel_id, so it cannot appear here at all.
async fn load_messages(pool: &PgPool, team_id: Uuid) -> Result<Vec<MessageRow>, sqlx::Error> {
    sqlx::query_as(
        r#"
        SELECT m.id, ch.name AS channel, s.name AS sender, m.announce,
               left(m.body, 240) AS body, m.created_at
        FROM messages m
        JOIN channels ch ON ch.id = m.channel_id
        JOIN agents s ON s.id = m.sender_agent_id
        WHERE m.team_id = $1
        ORDER BY m.id DESC
        LIMIT 20
        "#,
    )
    .bind(team_id)
    .fetch_all(pool)
    .await
}

async fn load_locks(pool: &PgPool, team_id: Uuid) -> Result<Vec<LockRow>, sqlx::Error> {
    sqlx::query_as(
        r#"
        SELECT l.name, a.name AS holder, l.purpose, l.expires_at
        FROM locks l
        JOIN agents a ON a.id = l.holder_agent_id
        WHERE l.team_id = $1 AND l.expires_at > now()
        ORDER BY l.name
        "#,
    )
    .bind(team_id)
    .fetch_all(pool)
    .await
}

async fn load_notes(pool: &PgPool, team_id: Uuid) -> Result<Vec<NoteRow>, sqlx::Error> {
    sqlx::query_as(
        r#"
        SELECT n.scope, n.key, a.name AS updated_by, n.updated_at
        FROM notes n
        LEFT JOIN agents a ON a.id = n.updated_by
        WHERE n.team_id = $1
        ORDER BY n.updated_at DESC
        LIMIT 15
        "#,
    )
    .bind(team_id)
    .fetch_all(pool)
    .await
}
