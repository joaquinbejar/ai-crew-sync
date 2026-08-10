use sqlx::PgPool;

use crate::{
    auth::AuthCtx,
    error::BusResult,
    model::{
        DigestChannel, DigestMessage, DigestNote, DigestResult, DigestTask, LockInfo, ts, ts_opt,
    },
};

/// A team-wide summary of the last `hours` hours: what moved, who did it and
/// where things stand. Everything in it is team-visible (no direct messages).
pub async fn team_digest(
    pool: &PgPool,
    auth: &AuthCtx,
    hours: i64,
    all_channels: bool,
) -> BusResult<DigestResult> {
    let hours = hours.clamp(1, 24 * 14);
    // A session catching up wants its own repository first. Passing NULL keeps
    // the whole-team digest, which is what the shared session always gets.
    let focus = match all_channels {
        true => None,
        false => super::messaging::default_channel(pool, auth)
            .await?
            .map(|(id, _)| id),
    };

    // Channel activity: volume plus the tail of the conversation.
    //
    // One set-based query, not one per channel. The old shape ran a tail
    // SELECT for every active channel, so a team with 40 channels paid 41
    // round trips for one digest — and the cost grew with the team.
    let channel_rows: Vec<(String, i64, serde_json::Value)> = sqlx::query_as(
        r#"
        WITH windowed AS (
            SELECT c.name AS channel,
                   a.name AS sender,
                   left(m.body, 300) AS body,
                   m.created_at,
                   m.id,
                   count(*) OVER (PARTITION BY c.id) AS message_count,
                   row_number() OVER (PARTITION BY c.id ORDER BY m.id DESC) AS recency
            FROM messages m
            JOIN channels c ON c.id = m.channel_id
            JOIN agents a   ON a.id = m.sender_agent_id
            WHERE c.team_id = $1
              AND m.created_at > now() - make_interval(hours => $2)
              -- An announcement belongs in every catch-up: a session focused
              -- on its own repository still needs to know a migration landed.
              AND ($3::uuid IS NULL OR c.id = $3 OR m.announce)
        )
        SELECT channel,
               max(message_count) AS message_count,
               COALESCE(
                   json_agg(json_build_object('from', sender, 'body', body, 'at', created_at)
                            ORDER BY id) FILTER (WHERE recency <= 5),
                   '[]'::json
               ) AS tail
        FROM windowed
        GROUP BY channel
        ORDER BY max(message_count) DESC
        "#,
    )
    .bind(auth.team_id)
    .bind(hours as i32)
    .bind(focus)
    .fetch_all(pool)
    .await?;

    #[derive(serde::Deserialize)]
    struct TailRow {
        from: String,
        body: String,
        at: chrono::DateTime<chrono::Utc>,
    }

    let channels: Vec<DigestChannel> = channel_rows
        .into_iter()
        .map(|(name, message_count, tail)| DigestChannel {
            name: name.clone(),
            message_count,
            last_messages: serde_json::from_value::<Vec<TailRow>>(tail)
                .inspect_err(|e| {
                    // The digest is still useful without one channel's tail,
                    // so this does not fail the call — but a silent empty
                    // tail would look like a quiet channel rather than a bug.
                    tracing::warn!(channel = %name, error = %e, "could not decode a channel tail");
                })
                .unwrap_or_default()
                .into_iter()
                .map(|r| DigestMessage {
                    from: r.from,
                    body: r.body,
                    at: ts(r.at),
                })
                .collect(),
        })
        .collect();

    // Tasks that moved in the window.
    let task_rows: Vec<(
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        chrono::DateTime<chrono::Utc>,
    )> = sqlx::query_as(
        r#"
        SELECT t.key, t.title, t.status, cb.name, t.result, t.updated_at
        FROM tasks t
        LEFT JOIN agents cb ON cb.id = t.claimed_by
        WHERE t.team_id = $1 AND t.updated_at > now() - make_interval(hours => $2)
        ORDER BY t.updated_at DESC
        LIMIT 50
        "#,
    )
    .bind(auth.team_id)
    .bind(hours as i32)
    .fetch_all(pool)
    .await?;

    let tasks_moved: Vec<DigestTask> = task_rows
        .into_iter()
        .map(
            |(key, title, status, claimed_by, result, updated_at)| DigestTask {
                key,
                title,
                status,
                claimed_by,
                result,
                updated_at: ts(updated_at),
            },
        )
        .collect();

    let (open_tasks, claimed_tasks): (i64, i64) = sqlx::query_as(
        r#"
        SELECT count(*) FILTER (WHERE status = 'open'),
               count(*) FILTER (WHERE status = 'claimed')
        FROM tasks WHERE team_id = $1
        "#,
    )
    .bind(auth.team_id)
    .fetch_one(pool)
    .await?;

    // Notes touched in the window.
    let note_rows: Vec<(
        String,
        String,
        Option<String>,
        chrono::DateTime<chrono::Utc>,
    )> = sqlx::query_as(
        r#"
            SELECT n.scope, n.key, a.name, n.updated_at
            FROM notes n
            LEFT JOIN agents a ON a.id = n.updated_by
            WHERE n.team_id = $1 AND n.updated_at > now() - make_interval(hours => $2)
            ORDER BY n.updated_at DESC
            LIMIT 50
            "#,
    )
    .bind(auth.team_id)
    .bind(hours as i32)
    .fetch_all(pool)
    .await?;

    let notes_updated: Vec<DigestNote> = note_rows
        .into_iter()
        .map(|(scope, key, updated_by, updated_at)| DigestNote {
            scope,
            key,
            updated_by,
            updated_at: ts(updated_at),
        })
        .collect();

    // Who was around.
    let agent_rows: Vec<(
        String,
        Option<String>,
        Option<chrono::DateTime<chrono::Utc>>,
        bool,
    )> = sqlx::query_as(
        r#"
            -- One row per person, not per session. Presence is keyed by
            -- (agent_id, session), so joining on the agent alone listed
            -- someone working in three repositories three times under the
            -- same name — a catch-up that reads as three teammates.
            SELECT a.name, p.activity, p.updated_at, COALESCE(p.expires_at > now(), false)
            FROM agents a
            LEFT JOIN LATERAL (
                SELECT pp.activity, pp.updated_at, pp.expires_at
                FROM agent_presence pp
                WHERE pp.agent_id = a.id
                -- Same preference as list_agents: live first, then a named
                -- session over the shared one. The digest is read by every
                -- session at start-up, so projecting the wrong row here shows
                -- the whole team a stale line as somebody's current work.
                ORDER BY (pp.expires_at > now()) DESC,
                         (COALESCE(pp.session, '') <> '') DESC,
                         pp.updated_at DESC
                LIMIT 1
            ) p ON true
            WHERE a.team_id = $1 AND a.disabled_at IS NULL
            ORDER BY p.updated_at DESC NULLS LAST
            "#,
    )
    .bind(auth.team_id)
    .fetch_all(pool)
    .await?;

    let agents_seen = agent_rows
        .into_iter()
        .map(
            |(name, activity, last_seen, online)| crate::model::DigestAgent {
                name,
                activity,
                last_seen: ts_opt(last_seen),
                online,
            },
        )
        .collect();

    // Active locks right now.
    let lock_rows: Vec<(
        String,
        String,
        Option<String>,
        Option<String>,
        chrono::DateTime<chrono::Utc>,
        chrono::DateTime<chrono::Utc>,
    )> = sqlx::query_as(
        r#"
            SELECT l.name, a.name, l.holder_session, l.purpose,
                   l.acquired_at, l.expires_at
            FROM locks l JOIN agents a ON a.id = l.holder_agent_id
            WHERE l.team_id = $1 AND l.expires_at > now()
            ORDER BY l.name
            "#,
    )
    .bind(auth.team_id)
    .fetch_all(pool)
    .await?;

    let active_locks = lock_rows
        .into_iter()
        .map(
            |(name, holder, holder_session, purpose, acquired_at, expires_at)| LockInfo {
                name,
                holder,
                holder_session: holder_session.filter(|s: &String| !s.is_empty()),
                purpose,
                acquired_at: ts(acquired_at),
                expires_at: ts(expires_at),
            },
        )
        .collect();

    Ok(DigestResult {
        hours,
        channels,
        tasks_moved,
        open_tasks,
        claimed_tasks,
        notes_updated,
        agents_seen,
        active_locks,
    })
}
