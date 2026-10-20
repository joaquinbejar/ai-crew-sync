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
    store::messaging::{self, Scope},
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
    pub since: Option<String>,
    /// Which messages to wait for, with the same values and the same read
    /// cursor as `read_messages`: "all" (the default), "inbox" (direct
    /// messages to this session or to you), or a channel name. With "all",
    /// channel chatter is still narrowed to this session's channel unless
    /// `all_channels` is set. Use the scope
    /// you will read with: a wait on "inbox" ignores unread channel chatter,
    /// which a default wait reports at once. Only messages are narrowed;
    /// tasks, locks and notes still wake you unless `kinds` leaves them out.
    #[serde(default)]
    pub scope: Option<String>,
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

/// Does this message event belong to the scope the wait watches? The same
/// sets `read_messages` reads, so what wakes the wait is what the matching
/// read returns.
fn in_scope(event: &BusEvent, scope: &Scope, focus: Option<Uuid>, auth: &AuthCtx) -> bool {
    match scope {
        // A direct message counts only when it is addressed to this agent:
        // another window of yours writing to a teammate is visible to you,
        // but it is nothing for you to read, and the backlog count agrees.
        Scope::All => {
            (!event.is_direct_message() || event.recipient_agent_id() == Some(auth.agent_id))
                && in_focus(event, focus)
        }
        // `visible_to` has already kept a direct message to another window
        // out; here only ones addressed to this agent remain.
        Scope::Inbox => {
            event.is_direct_message() && event.recipient_agent_id() == Some(auth.agent_id)
        }
        Scope::Channel { id, .. } => event.channel_id() == Some(*id),
    }
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

fn suggestion_for(events: &[WaitEvent], unread: i64, in_scope: i64, read_with: &str) -> String {
    if events.iter().any(|e| e.kind == "message") || in_scope > 0 {
        format!("Call {read_with} to fetch the new messages.")
    } else if unread > 0 {
        "Call read_messages {\"scope\": \"inbox\"} to fetch your unread direct messages.".into()
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

/// Longest `since` accepted: a snapshot of a busy server is a few hundred
/// bytes; anything longer is not one this bus returned.
const MAX_SINCE_LEN: usize = 4096;
/// Task, lock and note events described in one answer; the rest are
/// summarised so nothing is skipped silently.
const REPLAY_LIMIT: i64 = 50;

/// What the log holds for a caller between two positions.
struct Replay {
    /// The snapshot this read used: the next position.
    position: String,
    /// Up to REPLAY_LIMIT events, oldest first.
    events: Vec<BusEvent>,
    /// How many there were in all.
    total: i64,
}

/// Task, lock and note events of the caller's team, of the wanted kinds,
/// written by transactions `since` could not see and visible now. One
/// statement, so the events and the returned snapshot agree exactly: a
/// transaction still running is neither reported nor skipped, it is simply
/// still invisible in the returned position and comes back next time. With
/// no `since`, only the position is read.
async fn replay(
    pool: &PgPool,
    auth: &AuthCtx,
    since: Option<&str>,
    kinds: Option<&[String]>,
) -> Result<Replay, ErrorData> {
    let row: (String, i64, serde_json::Value) = sqlx::query_as(
        r#"
        WITH ev AS (
            SELECT e.id, e.payload
            FROM bus_event_log e
            WHERE $2::pg_snapshot IS NOT NULL
              AND e.team_id = $1
              AND NOT pg_visible_in_snapshot(e.tx, $2::pg_snapshot)
              AND ($3::text[] IS NULL OR e.payload ->> 'kind' = ANY($3))
        )
        SELECT pg_current_snapshot()::text,
               (SELECT count(*) FROM ev),
               COALESCE((SELECT json_agg(payload ORDER BY id)
                           FROM (SELECT id, payload FROM ev ORDER BY id LIMIT $4) first),
                        '[]'::json)
        "#,
    )
    .bind(auth.team_id)
    .bind(since)
    .bind(kinds)
    .bind(REPLAY_LIMIT)
    .fetch_one(pool)
    .await
    .map_err(|e| {
        if let sqlx::Error::Database(db) = &e
            && db.code().is_some_and(|c| c.starts_with("22"))
        {
            return ErrorData::invalid_params(
                "since is not a resume_from this bus returned. Pass resume_from from the \
                 previous wait_for_updates exactly, or omit since to start fresh.",
                None,
            );
        }
        tracing::error!(error = %e, "event replay failed");
        ErrorData::internal_error("database error", None)
    })?;
    let (position, total, payloads) = row;
    let events = payloads
        .as_array()
        .map(|a| a.iter().cloned().map(BusEvent).collect())
        .unwrap_or_default();
    Ok(Replay {
        position,
        events,
        total,
    })
}

/// Say how many logged changes were left out of the answer, and how to
/// re-sync, rather than drop them.
fn push_overflow(events: &mut Vec<WaitEvent>, replay: &Replay) {
    let shown = replay.events.len() as i64;
    if replay.total > shown {
        events.push(WaitEvent {
            kind: "backlog".into(),
            summary: format!(
                "{} more task, lock or note change(s) since your last wait; call list_tasks, \
                 list_locks or list_notes to see the current state",
                replay.total - shown
            ),
        });
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
        // Which messages wake this wait, resolved like read_messages' scope.
        let scope = match args.scope.as_deref() {
            Some(raw) => messaging::resolve_scope(&self.db, &auth, raw).await?,
            None => Scope::All,
        };
        let focus = match (&scope, args.all_channels) {
            (Scope::All, false) => messaging::default_channel(&self.db, &auth)
                .await?
                .map(|(id, _)| id),
            // An explicit scope is already narrower than any focus.
            _ => None,
        };

        if args
            .since
            .as_deref()
            .is_some_and(|v| v.len() > MAX_SINCE_LEN)
        {
            return Err(ErrorData::invalid_params(
                "since is not a resume_from this bus returned. Pass resume_from from the \
                 previous wait_for_updates exactly, or omit since to start fresh.",
                None,
            ));
        }
        let kind_list: Option<Vec<String>> = kind_filter.clone();

        // Subscribe before checking the database so nothing slips between the
        // check and the wait.
        let mut rx = self.hub.subscribe();

        // Task, lock and note changes the caller has not seen: committed after
        // the snapshot it last read from, and visible now. The answer carries
        // the snapshot this read used, which is the next position.
        let first = replay(&self.db, &auth, args.since.as_deref(), kind_list.as_deref()).await?;
        let mut position = first.position.clone();
        let mut events: Vec<WaitEvent> = Vec::new();
        for event in &first.events {
            if let Some(d) = describe(&self.db, event).await {
                events.push(d);
            }
        }
        push_overflow(&mut events, &first);

        // Same rule as the wake filter below: reporting a backlog the wait
        // would not have woken for is a lie the caller cannot act on.
        let pending = messaging::unread_in_scope(&self.db, &auth, &scope, focus).await?;
        if pending > 0 && wants("message") {
            events.insert(
                0,
                WaitEvent {
                    kind: "message".into(),
                    summary: format!(
                        "{pending} unread message(s) already waiting in {}",
                        scope.label()
                    ),
                },
            );
        }
        if !events.is_empty() {
            return self
                .wait_result(&auth, &scope, focus, events, position)
                .await;
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout as u64);
        // Messages are described as they arrive (their unread state is the
        // record). Task, lock and note events only wake the wait: what is
        // reported for them is read back from the log below, so nothing that
        // committed meanwhile is skipped or counted twice.
        while events.is_empty() {
            let event = tokio::select! {
                _ = tokio::time::sleep_until(deadline) => break,
                recv = rx.recv() => match recv {
                    Ok(ev) => ev,
                    // Lagged: we missed notifications. The log read below
                    // recovers tasks, locks and notes; say so for messages.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        if wants("message") {
                            events.push(WaitEvent {
                                kind: "unknown".into(),
                                summary: "event stream lagged; re-check the bus".into(),
                            });
                        }
                        break;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                },
            };

            if !event.visible_to(auth.team_id, auth.agent_id, &auth.session)
                || !wants(event.kind())
                || (event.kind() == "message" && !in_scope(&event, &scope, focus, &auth))
            {
                continue;
            }
            if event.kind() != "message" {
                // A notification is only a hint: it may be for a change the
                // replay above already reported, or one that committed before
                // this wait's starting position. Ask the log; wake only for
                // something new, and otherwise keep waiting from the newer
                // position.
                let fresh = replay(&self.db, &auth, Some(&position), kind_list.as_deref()).await?;
                position = fresh.position.clone();
                for e in &fresh.events {
                    if let Some(d) = describe(&self.db, e).await {
                        events.push(d);
                    }
                }
                push_overflow(&mut events, &fresh);
                continue;
            }
            // Your own messages are not news to you — but "you" is this
            // session, not the person. Excluding by agent alone meant an
            // announcement from your general window never woke the repository
            // windows it was written for, which is the coordination pattern
            // sessions exist to enable.
            if event.sender_agent_id() == Some(auth.agent_id)
                && event.sender_session().unwrap_or("") == auth.session
            {
                continue;
            }
            if let Some(described) = describe(&self.db, &event).await {
                events.push(described);
                // Grace window: batch messages that arrive together.
                let grace = tokio::time::Instant::now() + Duration::from_millis(150);
                while let Ok(Ok(more)) = tokio::time::timeout_at(grace, rx.recv()).await {
                    if more.kind() == "message"
                        && more.visible_to(auth.team_id, auth.agent_id, &auth.session)
                        && in_scope(&more, &scope, focus, &auth)
                        && wants("message")
                        // Same rule as the first event: only this session's
                        // own messages are not news.
                        && !(more.sender_agent_id() == Some(auth.agent_id)
                            && more.sender_session().unwrap_or("") == auth.session)
                        && let Some(d) = describe(&self.db, &more).await
                    {
                        events.push(d);
                    }
                }
            }
        }

        // Everything logged since the position this wait started from, read
        // in one statement whose snapshot becomes the next position. Run on
        // every exit, a timeout included: a notification can be missed, the
        // log cannot.
        let last = replay(&self.db, &auth, Some(&position), kind_list.as_deref()).await?;
        position = last.position.clone();
        for event in &last.events {
            if let Some(d) = describe(&self.db, event).await {
                events.push(d);
            }
        }
        push_overflow(&mut events, &last);

        self.wait_result(&auth, &scope, focus, events, position)
            .await
    }
}

impl Bus {
    /// The answer to a wait: what woke it, the backlog of the scope it
    /// watched and the read that consumes it, and the counts read back from
    /// the database at the end, so a message that landed meanwhile is counted.
    async fn wait_result(
        &self,
        auth: &AuthCtx,
        scope: &Scope,
        focus: Option<Uuid>,
        events: Vec<WaitEvent>,
        position: String,
    ) -> Result<Json<WaitResult>, ErrorData> {
        // Counts read back after the wait. A failure here must not throw
        // away what the wait observed: report zero and log it; the next wait
        // reads them again.
        let unread = unread_dms(&self.db, auth).await.unwrap_or(0);
        let unread_in_scope = messaging::unread_in_scope(&self.db, auth, scope, focus)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(error = %e, "could not count unread messages after a wait");
                0
            });
        let pending_questions = messaging::pending_questions(&self.db, auth)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(error = %e, "could not count pending questions after a wait");
                0
            });
        let read_with = scope.read_with();
        let woke = !events.is_empty();
        Ok(Json(WaitResult {
            woke,
            timed_out: !woke,
            suggestion: suggestion_for(&events, unread, unread_in_scope, &read_with),
            events,
            unread_direct_messages: unread,
            resume_from: position,
            message_scope: scope.label(),
            unread_in_scope,
            read_with,
            pending_questions,
        }))
    }
}
