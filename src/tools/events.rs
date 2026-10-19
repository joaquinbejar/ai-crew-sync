use std::time::Duration;

use rmcp::{
    ErrorData, Json, handler::server::wrapper::Parameters, service::RequestContext, tool,
    tool_router,
};
use schemars::JsonSchema;
use serde::Deserialize;
use sqlx::PgPool;
use uuid::Uuid;

use super::{Bus, auth_of};
use crate::{
    auth::AuthCtx,
    events::BusEvent,
    model::{WaitEvent, WaitResult},
};

const DEFAULT_TIMEOUT_SECS: i64 = 25;
const MAX_TIMEOUT_SECS: i64 = 55;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct WaitArgs {
    /// How long to wait before giving up, in seconds (5-55 on the bus,
    /// default 25; kept under a minute so HTTP intermediaries do not cut the
    /// call). Through `ai-crew-sync mcp proxy` it may be up to 1800: the
    /// proxy keeps waiting on the bus for you, so one call covers a long idle
    /// stretch instead of one model turn per minute.
    #[serde(default)]
    pub timeout_seconds: Option<i64>,
    /// Restrict to certain event kinds: any of "message", "task", "lock",
    /// "note". Omit to wake on anything relevant to you.
    #[serde(default)]
    pub kinds: Option<Vec<String>>,
    /// Wake on channel messages from every channel, not only the one this
    /// session works in. Ignored when your session has no matching channel,
    /// where every channel already wakes you. Direct messages, tasks, locks
    /// and notes always wake you either way.
    #[serde(default)]
    pub all_channels: bool,
    /// `resume_from` from your previous wait_for_updates. Task, lock and note
    /// changes after that position are reported at once, so waiting again
    /// never loses what happened between two calls. Omit on a first wait.
    #[serde(default)]
    pub since: Option<i64>,
}

async fn unread_dms(pool: &PgPool, auth: &AuthCtx) -> Result<i64, sqlx::Error> {
    // This session's inbox: what is addressed to it by name, plus what is
    // addressed to the person. Another window's mail is not this window's
    // backlog, and each keeps its own cursor.
    sqlx::query_scalar(
        r#"
        SELECT count(*)
        FROM messages m
        LEFT JOIN read_cursors c ON c.agent_id = $1 AND c.scope = $3
        WHERE m.recipient_agent_id = $1
          AND m.id > COALESCE(c.last_message_id, 0)
          AND (m.recipient_session IS NULL OR m.recipient_session = $2)
        "#,
    )
    .bind(auth.agent_id)
    .bind(&auth.session)
    .bind(crate::store::messaging::cursor_scope(
        "inbox",
        &auth.session,
    ))
    .fetch_one(pool)
    .await
}

async fn unread_anything(
    pool: &PgPool,
    auth: &AuthCtx,
    focus: Option<Uuid>,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        r#"
        SELECT count(*)
        FROM messages m
        LEFT JOIN read_cursors c ON c.agent_id = $1 AND c.scope = $4
        WHERE m.team_id = $2
          AND m.id > COALESCE(c.last_message_id, 0)
          -- Same rule as the wake filter: only this session's own messages
          -- are excluded, so a sibling window's announcement still counts.
          AND NOT (m.sender_agent_id = $1 AND COALESCE(m.sender_session, '') = $3)
          AND ((m.channel_id IS NOT NULL
                -- Same rule as the wake filter: an announcement counts as
                -- pending whatever this session is focused on. Reporting a
                -- backlog the wait would not wake for is a lie the caller
                -- cannot act on.
                AND ($5::uuid IS NULL OR m.channel_id = $5 OR m.announce))
               OR (m.recipient_agent_id = $1
                   AND (m.recipient_session IS NULL OR m.recipient_session = $3)))
        "#,
    )
    .bind(auth.agent_id)
    .bind(auth.team_id)
    .bind(&auth.session)
    .bind(crate::store::messaging::cursor_scope("all", &auth.session))
    .bind(focus)
    .fetch_one(pool)
    .await
}

/// Is this event inside the session's channel focus?
///
/// Only channel messages are filtered: a direct message, a task, a lock or a
/// note is not tied to a channel, and silencing those would hide work rather
/// than noise.
fn in_focus(event: &BusEvent, focus: Option<Uuid>) -> bool {
    // An announcement is the sender saying "this one is for everyone, even if
    // you are concentrating". Filtering it by channel would silence exactly
    // the message the flag exists to deliver.
    if event.is_announcement() {
        return true;
    }
    match (focus, event.channel_id()) {
        (Some(channel), Some(posted_in)) => channel == posted_in,
        _ => true,
    }
}

/// Turn a raw bus event into a one-line summary, resolving ids to names.
async fn describe(pool: &PgPool, event: &BusEvent) -> Option<WaitEvent> {
    match event.kind() {
        "message" => {
            let id = event.message_id()?;
            let row: (String, Option<String>, Option<String>, String) =
                sqlx::query_as::<_, (String, Option<String>, Option<String>, String)>(
                    r#"
                    SELECT s.name, ch.name, r.name, left(m.body, 200)
                    FROM messages m
                    JOIN agents s ON s.id = m.sender_agent_id
                    LEFT JOIN channels ch ON ch.id = m.channel_id
                    LEFT JOIN agents r ON r.id = m.recipient_agent_id
                    WHERE m.id = $1
                    "#,
                )
                .bind(id)
                .fetch_optional(pool)
                .await
                .ok()
                .flatten()?;
            let (sender, channel, recipient, body) = row;
            let target = channel
                .map(|c| format!("#{c}"))
                .or(recipient.map(|r| format!("@{r}")))
                .unwrap_or_default();
            Some(WaitEvent {
                kind: "message".into(),
                summary: format!("{sender} → {target}: {body}"),
            })
        }
        "task" => {
            let key = event.0.get("key").and_then(|v| v.as_str())?;
            let status = event
                .0
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("?");
            let holder = match event.0.get("claimed_by").and_then(|v| v.as_str()) {
                Some(uuid) => {
                    sqlx::query_scalar::<_, String>("SELECT name FROM agents WHERE id = $1::uuid")
                        .bind(uuid)
                        .fetch_optional(pool)
                        .await
                        .ok()
                        .flatten()
                        .map(|n| format!(" by {n}"))
                        .unwrap_or_default()
                }
                None => String::new(),
            };
            Some(WaitEvent {
                kind: "task".into(),
                summary: format!("task '{key}' is now {status}{holder}"),
            })
        }
        "lock" => {
            let name = event.0.get("name").and_then(|v| v.as_str())?;
            let what = event
                .0
                .get("event")
                .and_then(|v| v.as_str())
                .unwrap_or("changed");
            Some(WaitEvent {
                kind: "lock".into(),
                summary: format!("lock '{name}' {what}"),
            })
        }
        "note" => {
            let scope = event
                .0
                .get("scope")
                .and_then(|v| v.as_str())
                .unwrap_or("global");
            let key = event.0.get("key").and_then(|v| v.as_str())?;
            Some(WaitEvent {
                kind: "note".into(),
                summary: format!("note {scope}/{key} was updated"),
            })
        }
        _ => None,
    }
}

fn suggestion_for(events: &[WaitEvent], unread: i64) -> String {
    if events.iter().any(|e| e.kind == "message") || unread > 0 {
        "Call read_messages to fetch the new messages.".into()
    } else if events.iter().any(|e| e.kind == "task") {
        "Call list_tasks (or get_task) to see what changed.".into()
    } else if events.iter().any(|e| e.kind == "lock") {
        "Call list_locks (or retry acquire_lock) now.".into()
    } else if events.iter().any(|e| e.kind == "note") {
        "Call get_note to read the updated note.".into()
    } else {
        "Nothing happened. Every further wait costs you a model turn: do other work, \
         or end your turn if there is none, rather than waiting again in a loop. If you \
         must keep watching, wait once with a long timeout_seconds (up to 1800 through \
         ai-crew-sync mcp proxy)."
            .into()
    }
}

#[tool_router(router = events_router, vis = "pub")]
impl Bus {
    #[tool(
        description = "Block until something happens on the bus that concerns you (a message \
                       arrives, a task changes state, a lock is released, a note is updated) or \
                       the timeout elapses. Use this instead of polling read_messages in a loop: \
                       call it when you are waiting on teammates and idle. Returns immediately \
                       if you already have unread messages."
    )]
    async fn wait_for_updates(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<WaitArgs>,
    ) -> Result<Json<WaitResult>, ErrorData> {
        let auth = auth_of(&ctx)?;
        let timeout = args
            .timeout_seconds
            .unwrap_or(DEFAULT_TIMEOUT_SECS)
            .clamp(5, MAX_TIMEOUT_SECS);
        let kind_filter: Option<Vec<String>> = args
            .kinds
            .map(|ks| ks.into_iter().map(|k| k.trim().to_lowercase()).collect());
        let wants = |kind: &str| {
            kind_filter
                .as_ref()
                .map(|ks| ks.iter().any(|k| k == kind))
                .unwrap_or(true)
        };

        // The channel this session works in, when it has one and the caller
        // has not asked for the whole team. A window working on market-data
        // should not be woken by core-manager chatter — that is half the point
        // of naming a session after a repository.
        let focus = match args.all_channels {
            true => None,
            false => crate::store::messaging::default_channel(&self.db, &auth)
                .await?
                .map(|(id, _)| id),
        };

        // Subscribe before checking the database so nothing slips between the
        // check and the wait.
        let mut rx = self.hub.subscribe();
        // The log position at subscription: everything up to here is either
        // replayed below or was before the caller's `since`; everything after
        // arrives on `rx`.
        let head: i64 = sqlx::query_scalar("SELECT COALESCE(max(id), 0) FROM bus_event_log")
            .fetch_one(&self.db)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "event log position failed");
                ErrorData::internal_error("database error", None)
            })?;

        // Same rule as the wake filter below: reporting a backlog the wait
        // would not have woken for is a lie the caller cannot act on.
        let pending = unread_anything(&self.db, &auth, focus).await.map_err(|e| {
            tracing::error!(error = %e, "unread check failed");
            ErrorData::internal_error("database error", None)
        })?;
        if pending > 0 && wants("message") {
            let dms = unread_dms(&self.db, &auth).await.unwrap_or(0);
            return Ok(Json(WaitResult {
                woke: true,
                timed_out: false,
                events: vec![WaitEvent {
                    kind: "message".into(),
                    summary: format!("{pending} unread message(s) already waiting"),
                }],
                unread_direct_messages: dms,
                suggestion: "Call read_messages to fetch them.".into(),
                resume_from: head,
            }));
        }

        // What happened since the caller's last wait, before this one
        // subscribed: replayed from the log, under the same filters.
        let mut events: Vec<WaitEvent> = Vec::new();
        let mut position = head;
        if let Some(since) = args.since.filter(|s| (0..head).contains(s)) {
            let missed: Vec<(serde_json::Value,)> = sqlx::query_as(
                "SELECT payload FROM bus_event_log
                  WHERE team_id = $1 AND id > $2 AND id <= $3
                  ORDER BY id LIMIT 200",
            )
            .bind(auth.team_id)
            .bind(since)
            .bind(head)
            .fetch_all(&self.db)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "event replay failed");
                ErrorData::internal_error("database error", None)
            })?;
            for (payload,) in missed {
                let event = crate::events::BusEvent(payload);
                if event.visible_to(auth.team_id, auth.agent_id, &auth.session)
                    && wants(event.kind())
                    && in_focus(&event, focus)
                    && let Some(described) = describe(&self.db, &event).await
                {
                    events.push(described);
                }
            }
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout as u64);

        while events.is_empty() {
            let event = tokio::select! {
                _ = tokio::time::sleep_until(deadline) => break,
                recv = rx.recv() => match recv {
                    Ok(ev) => ev,
                    // Lagged: we missed events; report a generic wake so the
                    // caller re-syncs from the database.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        events.push(WaitEvent {
                            kind: "unknown".into(),
                            summary: "event stream lagged; re-check the bus".into(),
                        });
                        break;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                },
            };

            // Only this team's events move the position: the log is per
            // database, and an id seen on the shared channel says nothing
            // about what this caller has or has not been shown.
            if event.team_id() == Some(auth.team_id)
                && let Some(id) = event.event_id()
            {
                position = position.max(id);
            }
            if !event.visible_to(auth.team_id, auth.agent_id, &auth.session)
                || !wants(event.kind())
                || !in_focus(&event, focus)
            {
                continue;
            }
            // Your own messages are not news to you — but "you" is this
            // session, not the person. Excluding by agent alone meant an
            // announcement from your general window never woke the repository
            // windows it was written for, which is the coordination pattern
            // sessions exist to enable.
            if event.kind() == "message"
                && event.sender_agent_id() == Some(auth.agent_id)
                && event.sender_session().unwrap_or("") == auth.session
            {
                continue;
            }
            if let Some(described) = describe(&self.db, &event).await {
                events.push(described);
                // Grace window: batch events that arrive together.
                let grace = tokio::time::Instant::now() + Duration::from_millis(150);
                while let Ok(Ok(more)) = tokio::time::timeout_at(grace, rx.recv()).await {
                    if more.team_id() == Some(auth.team_id)
                        && let Some(id) = more.event_id()
                    {
                        position = position.max(id);
                    }
                    if more.visible_to(auth.team_id, auth.agent_id, &auth.session)
                        && in_focus(&more, focus)
                        && wants(more.kind())
                        && !(more.kind() == "message"
                            && more.sender_agent_id() == Some(auth.agent_id))
                        && let Some(d) = describe(&self.db, &more).await
                    {
                        events.push(d);
                    }
                }
            }
        }

        let unread = unread_dms(&self.db, &auth).await.unwrap_or(0);
        let woke = !events.is_empty();
        Ok(Json(WaitResult {
            woke,
            timed_out: !woke,
            suggestion: suggestion_for(&events, unread),
            events,
            unread_direct_messages: unread,
            resume_from: position,
        }))
    }
}
