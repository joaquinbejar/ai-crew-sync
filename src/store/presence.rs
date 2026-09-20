use sqlx::PgPool;

use crate::{
    auth::AuthCtx,
    error::{BusError, BusResult},
    model::{AgentInfo, AgentList, AgentSession, SessionEntry, SessionList, ts_opt},
};

const DEFAULT_TTL_SECS: i64 = 600; // 10 minutes
const MAX_TTL_SECS: i64 = 86_400;
/// Repo, branch and activity are a status line, not a log.
const MAX_PRESENCE_FIELD_BYTES: usize = 256;

/// A discovery label (project, role): one lower-case word people type and
/// filter by. Bounded like a session label.
pub const MAX_LABEL_BYTES: usize = 64;

pub struct HeartbeatInput {
    pub status: Option<String>,
    pub repo: Option<String>,
    pub branch: Option<String>,
    pub activity: Option<String>,
    /// Discovery labels. `None` keeps the previous value, `Some("")` clears.
    pub project: Option<String>,
    pub role: Option<String>,
    pub ttl_seconds: Option<i64>,
}

/// Normalise a discovery label the way session labels are: trimmed and
/// lower-cased so `Review` and `review` are one role, ASCII, one word.
/// Empty means "clear" and is passed through.
pub fn normalize_label(field: &str, raw: &str) -> BusResult<String> {
    let label = raw.trim().to_lowercase();
    if label.is_empty() {
        return Ok(label);
    }
    if label.len() > MAX_LABEL_BYTES {
        return Err(BusError::invalid(format!(
            "{field} is {} bytes; the limit is {MAX_LABEL_BYTES}",
            label.len()
        )));
    }
    if !label
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':'))
    {
        return Err(BusError::invalid(format!(
            "{field} may only contain ASCII letters, digits, '-', '_', '.' and ':' \
             (got '{raw}'); it is a label to filter by, not a description — put that \
             in activity"
        )));
    }
    Ok(label)
}

/// The discovery labels a session last published, for `whoami`.
pub async fn labels_of(
    pool: &PgPool,
    auth: &AuthCtx,
) -> BusResult<(Option<String>, Option<String>)> {
    let row: Option<(Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT project, role FROM agent_presence WHERE agent_id = $1 AND session = $2",
    )
    .bind(auth.agent_id)
    .bind(&auth.session)
    .fetch_optional(pool)
    .await?;
    Ok(row.unwrap_or((None, None)))
}

pub async fn heartbeat(
    pool: &PgPool,
    auth: &AuthCtx,
    input: HeartbeatInput,
) -> BusResult<AgentInfo> {
    let status = input
        .status
        .map(|s| s.trim().to_lowercase())
        .unwrap_or_else(|| "active".into());
    if !["active", "idle", "busy", "blocked"].contains(&status.as_str()) {
        return Err(BusError::invalid(
            "status must be one of: active, idle, busy, blocked",
        ));
    }
    let ttl = input
        .ttl_seconds
        .unwrap_or(DEFAULT_TTL_SECS)
        .clamp(30, MAX_TTL_SECS);

    // Presence is a status line, not a log: bounded so a heartbeat loop
    // cannot grow the row without limit.
    let repo = match input.repo.as_deref() {
        Some(v) => Some(super::check_text(
            "presence repo",
            v,
            MAX_PRESENCE_FIELD_BYTES,
        )?),
        None => None,
    };
    let branch = match input.branch.as_deref() {
        Some(v) => Some(super::check_text(
            "presence branch",
            v,
            MAX_PRESENCE_FIELD_BYTES,
        )?),
        None => None,
    };
    let activity = match input.activity.as_deref() {
        Some(v) => Some(super::check_text(
            "presence activity",
            v,
            MAX_PRESENCE_FIELD_BYTES,
        )?),
        None => None,
    };
    let project = match input.project.as_deref() {
        Some(v) => Some(normalize_label("project", v)?),
        None => None,
    };
    let role = match input.role.as_deref() {
        Some(v) => Some(normalize_label("role", v)?),
        None => None,
    };

    // Upsert on (agent_id, session), and report back the row just written.
    // Reading it from list_agents instead would pick whichever session came
    // first alphabetically once an agent has more than one.
    let row: (
        String,
        Option<String>,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<chrono::DateTime<chrono::Utc>>,
        bool,
    ) = sqlx::query_as(
        r#"
        WITH up AS (
            INSERT INTO agent_presence
                (agent_id, session, status, repo, branch, activity, project, role,
                 updated_at, expires_at)
            -- NULLIF on the insert path too: the CASE below only runs on
            -- conflict, so a first heartbeat for a new or freshly swept
            -- session stored '' and reported an empty string where the
            -- update path reports null.
            VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), NULLIF($8, ''), NULLIF($9, ''),
                    now(), now() + make_interval(secs => $7))
            ON CONFLICT (agent_id, session) DO UPDATE SET
                status     = EXCLUDED.status,
                -- keep the previous value when the caller omits a field
                repo       = COALESCE(EXCLUDED.repo, agent_presence.repo),
                branch     = COALESCE(EXCLUDED.branch, agent_presence.branch),
                -- Omitted keeps the previous value; an explicit empty string
                -- clears it. A session that has just started has not done
                -- anything yet, and carrying yesterday's line forward is how
                -- a status board ends up lying with a straight face.
                -- Tested against the parameter, not EXCLUDED: the insert
                -- above NULLIFs it, so EXCLUDED.activity no longer carries
                -- the empty string that means "clear".
                activity   = CASE
                                 WHEN $6 = '' THEN NULL
                                 ELSE COALESCE(EXCLUDED.activity, agent_presence.activity)
                             END,
                -- Same rule for the discovery labels.
                project    = CASE
                                 WHEN $8 = '' THEN NULL
                                 ELSE COALESCE(EXCLUDED.project, agent_presence.project)
                             END,
                role       = CASE
                                 WHEN $9 = '' THEN NULL
                                 ELSE COALESCE(EXCLUDED.role, agent_presence.role)
                             END,
                updated_at = now(),
                expires_at = EXCLUDED.expires_at
            RETURNING agent_id, status, repo, branch, activity, project, role,
                      updated_at, expires_at
        )
        SELECT a.name,
               a.display_name,
               up.status,
               up.repo,
               up.branch,
               up.activity,
               up.project,
               up.role,
               up.updated_at,
               up.expires_at > now() AS online
        FROM up
        JOIN agents a ON a.id = up.agent_id
        "#,
    )
    .bind(auth.agent_id)
    .bind(&auth.session)
    .bind(&status)
    .bind(repo.as_deref())
    .bind(branch.as_deref())
    .bind(activity.as_deref())
    .bind(ttl as f64)
    .bind(project.as_deref())
    .bind(role.as_deref())
    .fetch_one(pool)
    .await?;

    // Sweep this agent's long-dead rows. Nothing else ever deleted a presence
    // row: before sessions that was bounded at one per agent, but a row per
    // distinct session label grows without limit, and a label used once stays
    // for good. An hour past expiry keeps "offline recently" visible while
    // still clearing the orphan a sessionless hook left behind.
    //
    // Best-effort: presence is a status line, and failing to tidy it must not
    // fail the heartbeat that was the actual request.
    let _ = sqlx::query(
        "DELETE FROM agent_presence
          WHERE agent_id = $1 AND session <> $2
            AND expires_at < now() - interval '1 hour'",
    )
    .bind(auth.agent_id)
    .bind(&auth.session)
    .execute(pool)
    .await;

    let (name, display_name, status, repo, branch, activity, project, role, updated_at, online) =
        row;
    Ok(AgentInfo {
        name,
        display_name,
        session: super::session_label(auth),
        status,
        repo,
        branch,
        activity,
        project,
        role,
        last_seen: ts_opt(updated_at),
        online,
        // The heartbeat reports the session it just wrote, not a survey of the
        // agent's other contexts; list_agents is where that belongs.
        sessions: Vec::new(),
    })
}

/// Delete shared-session ('') presence rows that are long past expiry,
/// whoever they belong to. The per-heartbeat sweep above only runs when the
/// row's *owner* comes back, so a row left by an agent that never heartbeats
/// again — the 0.6.0 hooks wrote exactly that kind — would otherwise keep its
/// stale `activity` projecting in `list_agents` and `team_digest` forever.
/// Server maintenance, not a tool: no auth context, all teams on purpose,
/// same one-hour grace as the heartbeat sweep so "offline recently" still
/// reads. Named rows are left alone — they carry real last-seen information.
pub async fn sweep_expired_shared_rows(pool: &PgPool) -> BusResult<u64> {
    let res = sqlx::query(
        "DELETE FROM agent_presence
          WHERE session = '' AND expires_at < now() - interval '1 hour'",
    )
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

pub async fn list_agents(pool: &PgPool, auth: &AuthCtx, online_only: bool) -> BusResult<AgentList> {
    // One row per (agent, session). An agent working in three repositories has
    // three presence rows and is still one person, so the rows are folded back
    // into one entry per agent below.
    let rows: Vec<(
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<chrono::DateTime<chrono::Utc>>,
        bool,
    )> = sqlx::query_as(
        r#"
        SELECT a.name,
               a.display_name,
               p.session,
               p.status,
               p.repo,
               p.branch,
               p.activity,
               p.project,
               p.role,
               p.updated_at,
               COALESCE(p.expires_at > now(), false) AS online
        FROM agents a
        -- Every session, deliberately: the previous change picked a single
        -- presence row per agent so the flat output stayed correct while it
        -- was the only thing available. Now the rows are folded back under
        -- their agent in Rust, so all of them are wanted here.
        LEFT JOIN agent_presence p ON p.agent_id = a.id
        WHERE a.team_id = $1
          AND a.disabled_at IS NULL
          AND (NOT $2::bool OR COALESCE(p.expires_at > now(), false))
        -- Within an agent: live first, then a *named* session over the shared
        -- one, then the most recent. The first row is what the top-level
        -- fields project, and a sessionless row that keeps refreshing would
        -- otherwise be the summary everyone reads while the real sessions sit
        -- unread inside sessions[].
        ORDER BY a.name,
                 COALESCE(p.expires_at > now(), false) DESC,
                 (COALESCE(p.session, '') <> '') DESC,
                 p.updated_at DESC NULLS LAST
        "#,
    )
    .bind(auth.team_id)
    .bind(online_only)
    .fetch_all(pool)
    .await?;

    // Rows arrive grouped by agent and already in the order sessions should be
    // reported in, so one pass is enough.
    let mut agents: Vec<AgentInfo> = Vec::new();
    for (
        name,
        display_name,
        session,
        status,
        repo,
        branch,
        activity,
        project,
        role,
        updated_at,
        online,
    ) in rows
    {
        let entry = AgentSession {
            // '' in the database, null on the wire: the shared session has no
            // name, and reporting one would invent a context that is not there.
            session: session.filter(|s| !s.is_empty()),
            status: if online {
                status.unwrap_or_else(|| "active".into())
            } else {
                "offline".into()
            },
            repo,
            branch,
            activity,
            project,
            role,
            last_seen: ts_opt(updated_at),
            online,
        };

        match agents.last_mut() {
            // The lead session — the first row for this agent — is the one the
            // top-level fields describe.
            Some(agent) if agent.name == name => {
                agent.online |= entry.online;
                agent.sessions.push(entry);
            }
            _ => agents.push(AgentInfo {
                name,
                display_name,
                session: entry.session.clone(),
                status: entry.status.clone(),
                repo: entry.repo.clone(),
                branch: entry.branch.clone(),
                activity: entry.activity.clone(),
                project: entry.project.clone(),
                role: entry.role.clone(),
                last_seen: entry.last_seen.clone(),
                online: entry.online,
                sessions: vec![entry],
            }),
        }
    }

    // With a single session the top-level fields say everything; repeating it
    // as a one-element list is noise, and hiding it keeps the output identical
    // to what every existing client already parses.
    for agent in &mut agents {
        if agent.sessions.len() < 2 {
            agent.sessions.clear();
        }
    }

    // People, not sessions: someone with three live sessions is one teammate
    // online.
    let online_count = agents.iter().filter(|a| a.online).count();
    // Online agents first, as before; the SQL ordered by name so that grouping
    // could be a single pass.
    agents.sort_by(|a, b| b.online.cmp(&a.online).then_with(|| a.name.cmp(&b.name)));
    Ok(AgentList {
        agents,
        online_count,
    })
}

/// Upper bound on a `list_sessions` page; the filters exist so nobody needs it.
pub const MAX_SESSIONS: i64 = 1000;
pub const DEFAULT_SESSIONS: i64 = 200;

pub struct SessionFilter {
    pub project: Option<String>,
    pub role: Option<String>,
    pub online_only: bool,
    pub limit: Option<i64>,
}

/// Every session in the caller's team, one entry each, addressable. The
/// shared session (no label) is listed too: it is where clients that send no
/// header live, and `agent` alone is its address.
pub async fn list_sessions(
    pool: &PgPool,
    auth: &AuthCtx,
    filter: SessionFilter,
) -> BusResult<SessionList> {
    let project = match filter.project.as_deref() {
        Some(v) => Some(normalize_label("project", v)?).filter(|s| !s.is_empty()),
        None => None,
    };
    let role = match filter.role.as_deref() {
        Some(v) => Some(normalize_label("role", v)?).filter(|s| !s.is_empty()),
        None => None,
    };
    let limit = filter
        .limit
        .unwrap_or(DEFAULT_SESSIONS)
        .clamp(1, MAX_SESSIONS);
    let rows: Vec<(
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        chrono::DateTime<chrono::Utc>,
        bool,
    )> = sqlx::query_as(
        r#"
        SELECT a.name,
               p.session,
               p.status,
               p.repo,
               p.branch,
               p.activity,
               p.project,
               p.role,
               p.updated_at,
               (p.expires_at > now()) AS online
        FROM agent_presence p
        JOIN agents a ON a.id = p.agent_id
        WHERE a.team_id = $1
          AND a.disabled_at IS NULL
          AND ($2::text IS NULL OR p.project = $2)
          AND ($3::text IS NULL OR p.role = $3)
          AND (NOT $4::bool OR p.expires_at > now())
        -- Live first, then most recently active, then by address so the
        -- order is stable between calls.
        ORDER BY (p.expires_at > now()) DESC, p.updated_at DESC, a.name, p.session
        LIMIT $5
        "#,
    )
    .bind(auth.team_id)
    .bind(project.as_deref())
    .bind(role.as_deref())
    .bind(filter.online_only)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    let sessions: Vec<SessionEntry> = rows
        .into_iter()
        .map(
            |(
                agent,
                session,
                status,
                repo,
                branch,
                activity,
                project,
                role,
                updated_at,
                online,
            )| {
                let address = if session.is_empty() {
                    agent.clone()
                } else {
                    format!("{agent}/{session}")
                };
                SessionEntry {
                    address,
                    session: (!session.is_empty()).then_some(session),
                    agent,
                    project,
                    role,
                    repo,
                    branch,
                    activity,
                    status: if online { status } else { "offline".into() },
                    online,
                    last_seen: ts_opt(Some(updated_at)),
                }
            },
        )
        .collect();
    Ok(SessionList {
        count: sessions.len(),
        sessions,
        limit,
    })
}
