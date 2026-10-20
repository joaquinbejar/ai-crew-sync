//! `ai-crew-sync context codex-wake`: wake an idle Codex thread when the
//! bus has something new for its window (#222).
//!
//! A Codex thread whose turn has ended calls no tools, so a message for its
//! window waits until its human types. Codex has no background hook that
//! can start a turn; its app-server can (`turn/start`). This adapter is the
//! bridge, and it is explicit at every step:
//!
//! - It runs only when started, for one thread, named by the thread id.
//! - The thread id must be one the window's own proxy recorded from Codex:
//!   the binding's source is `request-meta` (`_meta.threadId` on the host's
//!   own tool calls). Never a guess from a directory, a role or a name.
//! - It acts as that window with the window's credential, read from the
//!   private binding; the credential never reaches argv, a log, the model or
//!   the app-server.
//! - One adapter per thread. A second one is refused while the first is
//!   alive.
//! - It wakes only for news: a direct message newer than the last it woke
//!   for, or more undelivered conversation messages. What was waiting when
//!   it started does not wake the thread. The mark is written before the
//!   turn is requested, so a crash costs at most one missed wake (the
//!   messages stay unread on the bus), never a duplicate.
//! - The turn it starts carries counts and where to read them, never a
//!   message body: the model reads with its own ACS tools.
//! - It never starts a second turn: an active thread is waited on until it
//!   is idle. A thread that is not loaded is resumed first.
//! - What it did is observable as presence, `wake_result` on the window:
//!   `requested` once Codex accepted the turn, `confirmed` once Codex said
//!   the turn started, `failed` when it gave up. None of these is a receipt:
//!   a started turn is not a read message, let alone an answer.

use std::{path::Path, time::Duration};

use anyhow::{Context, bail};
use serde_json::json;

use crate::{
    codex::{AppServer, thread_status},
    context::{self, Binding},
    hook::{self, Reading, WatchState},
};

/// The label the adapter announces as the window's wake mechanism.
pub const WAKE_LABEL: &str = "codex-adapter";
/// How long an announcement stands; renewed well before it lapses.
const ANNOUNCE_TTL_SECS: u64 = 300;
const ANNOUNCE_EVERY: Duration = Duration::from_secs(120);
/// An adapter record not refreshed for this long belongs to a process that
/// is gone, and may be taken over.
const OWNER_STALE_SECS: i64 = 90;
/// Longest single wait on the bus between two readings.
const WAIT_SECS: u64 = 50;
/// Pause after a wait that came back at once, so an answer that keeps
/// returning immediately cannot become a busy loop.
const FAST_RETURN_PAUSE: Duration = Duration::from_secs(5);
/// Back-off when the bus cannot be reached.
const RETRY_PAUSE: Duration = Duration::from_secs(30);
/// Messages that arrive together wake the thread once.
const SETTLE: Duration = Duration::from_secs(2);
/// Attempts to deliver one wake, and the pause before each retry.
const DELIVERY_BACKOFF: &[Duration] = &[
    Duration::from_secs(2),
    Duration::from_secs(5),
    Duration::from_secs(15),
];
/// Longest wait for an active thread to become idle before a wake is
/// given up as failed.
const QUEUE_MAX: Duration = Duration::from_secs(30 * 60);
/// How long to wait for Codex to say the requested turn started.
const CONFIRM_WITHIN: Duration = Duration::from_secs(30);
/// How often the thread is re-read while waiting on it: notifications only
/// reach the connection that started or resumed a thread.
const POLL_EVERY: Duration = Duration::from_secs(2);

/// The adapter's record next to the binding (0600, in the 0700 directory).
/// Holds no credential.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct AdapterState {
    /// The adapter process that owns this thread.
    token: String,
    pid: u32,
    /// Unix seconds of its last sign of life.
    alive_at: i64,
    /// What it has already woken the thread for.
    mark: WatchState,
}

fn state_path(config_dir: &Path, thread: &str) -> std::path::PathBuf {
    context::binding_path(config_dir, thread).with_extension("codex-wake")
}

fn read_state(path: &Path) -> Option<AdapterState> {
    serde_json::from_str(&std::fs::read_to_string(path).ok()?).ok()
}

fn now_secs() -> i64 {
    chrono::Utc::now().timestamp()
}

/// How a run ended, for the command's exit message.
#[derive(Debug, PartialEq, Eq)]
pub enum Ended {
    /// `--max-secs` passed.
    Deadline,
    /// The window closed: its binding no longer carries a credential.
    WindowClosed,
    /// Another adapter took the thread over after this one went quiet.
    Replaced,
    /// SIGINT or SIGTERM: the thread is released at once, so a new adapter
    /// can start without waiting for this one's record to go stale.
    Stopped,
}

/// Resolves on SIGINT, or SIGTERM where there is one.
async fn stop_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut term) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

pub struct Options {
    /// The Codex thread id, which is also the binding key.
    pub thread: String,
    /// The app-server's Unix socket.
    pub socket: std::path::PathBuf,
    /// Stop after this long; `None` runs until the window closes.
    pub max_secs: Option<u64>,
}

/// The binding of `thread`, refused unless Codex itself named the thread.
fn codex_binding(config_dir: &Path, thread: &str) -> anyhow::Result<Option<Binding>> {
    match hook::resolve_binding(config_dir, thread) {
        hook::Resolution::Authenticated(b) => {
            if b.binding.as_deref() != Some(context::CODEX_THREAD_BINDING) {
                bail!(
                    "the window bound to '{thread}' was not bound by Codex (its source is {}): \
                     the adapter only wakes a thread whose own tool calls named it. Start a \
                     turn in that Codex thread that calls an ai-crew-sync tool (session_status \
                     is enough), then start the adapter again.",
                    b.binding.as_deref().unwrap_or("unrecorded")
                );
            }
            Ok(Some(*b))
        }
        hook::Resolution::Unauthenticated(_) => Ok(None),
        hook::Resolution::Missing => bail!(
            "no ai-crew-sync window is bound to Codex thread '{thread}'. In that thread, call \
             any ai-crew-sync tool once (session_status is enough) so its proxy records the \
             binding, then start the adapter again."
        ),
    }
}

/// Run the adapter for one thread until `max_secs`, or until the window
/// closes.
pub async fn run(config_dir: &Path, opts: &Options) -> anyhow::Result<Ended> {
    if codex_binding(config_dir, &opts.thread)?.is_none() {
        bail!(
            "the window bound to Codex thread '{}' is closed; open the thread again so its \
             proxy resumes the window",
            opts.thread
        );
    }
    let path = state_path(config_dir, &opts.thread);
    let token = uuid::Uuid::new_v4().to_string();
    // Take the thread, unless a live adapter already has it.
    let mut mark = context::with_config_lock(config_dir, || {
        let prior = read_state(&path);
        if let Some(p) = &prior
            && now_secs() - p.alive_at < OWNER_STALE_SECS
        {
            bail!(
                "another ai-crew-sync adapter (pid {}) is already waking this Codex thread; \
                 stop it first, or wait {OWNER_STALE_SECS}s if it has died",
                p.pid
            );
        }
        let mark = prior.map(|p| p.mark).unwrap_or_default();
        write_state(
            &path,
            &AdapterState {
                token: token.clone(),
                pid: std::process::id(),
                alive_at: now_secs(),
                mark: mark.clone(),
            },
        )?;
        Ok(mark)
    })?;

    // The thread must exist where we are going to wake it.
    {
        let mut app = AppServer::connect(&opts.socket, "ai-crew-sync-wake").await?;
        let thread = app
            .request("thread/read", json!({"threadId": opts.thread}))
            .await
            .with_context(|| {
                format!(
                    "the Codex app-server at {} does not know thread '{}'",
                    opts.socket.display(),
                    opts.thread
                )
            })?;
        tracing::info!(
            thread = %opts.thread,
            status = thread_status(&thread["thread"]).unwrap_or("unknown"),
            "codex wake adapter attached"
        );
    }

    let deadline = opts
        .max_secs
        .map(|s| tokio::time::Instant::now() + Duration::from_secs(s));
    let mut announced: Option<tokio::time::Instant> = None;
    let stop = stop_signal();
    tokio::pin!(stop);
    let ended = loop {
        if deadline.is_some_and(|d| tokio::time::Instant::now() >= d) {
            break Ended::Deadline;
        }
        // Still ours, and still alive: refresh the record.
        let owned = context::with_config_lock(config_dir, || match read_state(&path) {
            Some(mut s) if s.token == token => {
                s.alive_at = now_secs();
                write_state(&path, &s)?;
                Ok(true)
            }
            _ => Ok(false),
        })?;
        if !owned {
            break Ended::Replaced;
        }
        let binding = match codex_binding(config_dir, &opts.thread)? {
            Some(b) => b,
            None => break Ended::WindowClosed,
        };
        if announced.is_none_or(|at| at.elapsed() >= ANNOUNCE_EVERY) {
            announce(&binding, Some(ANNOUNCE_TTL_SECS)).await;
            announced = Some(tokio::time::Instant::now());
        }

        let reading = match hook::pending_for_window(&binding).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "could not read the window's inbox; retrying");
                tokio::select! {
                    _ = &mut stop => break Ended::Stopped,
                    _ = tokio::time::sleep(RETRY_PAUSE) => {}
                }
                continue;
            }
        };
        if !mark.primed {
            // What was waiting before the adapter started is not news: the
            // thread's own next turn reports it.
            mark = primed(&token, reading);
            save_mark(config_dir, &path, &token, &mark)?;
        } else {
            let (wake, next) = hook::watch_step(&mark, reading);
            mark = next;
            if wake {
                // Let a burst land, then wake once for all of it.
                tokio::time::sleep(SETTLE).await;
                if let Ok(later) = hook::pending_for_window(&binding).await {
                    mark = hook::watch_step(&mark, later).1;
                }
                // Marked before the turn is requested: a crash between the
                // two costs one missed wake, never a duplicate.
                save_mark(config_dir, &path, &token, &mark)?;
                let notice = hook::wake_text(mark.direct, mark.inbox);
                let result = deliver(&opts.socket, &opts.thread, &notice, &mark).await;
                report(&binding, result).await;
                continue;
            }
            save_mark(config_dir, &path, &token, &mark)?;
        }

        // Wait on the bus for the next message addressed to this window.
        let started = tokio::time::Instant::now();
        let until_deadline = async {
            match deadline {
                Some(d) => tokio::time::sleep_until(d).await,
                None => std::future::pending().await,
            }
        };
        let waited = tokio::select! {
            _ = &mut stop => break Ended::Stopped,
            _ = until_deadline => break Ended::Deadline,
            w = hook::call_as_window(
                &binding,
                "wait_for_updates",
                json!({"timeout_seconds": WAIT_SECS, "kinds": ["message"], "scope": "inbox"}),
            ) => w,
        };
        if started.elapsed() < Duration::from_secs(2) {
            let pause = if waited.is_err() {
                RETRY_PAUSE
            } else {
                FAST_RETURN_PAUSE
            };
            tokio::select! {
                _ = &mut stop => break Ended::Stopped,
                _ = tokio::time::sleep(pause) => {}
            }
        }
    };

    // Withdraw the announcement while still the owner, then release.
    let still_owner = context::with_config_lock(config_dir, || {
        Ok(read_state(&path).is_some_and(|s| s.token == token))
    })?;
    if still_owner {
        if let Ok(Some(binding)) = codex_binding(config_dir, &opts.thread) {
            announce(&binding, None).await;
        }
        // Keep the mark for the next adapter; give up ownership.
        context::with_config_lock(config_dir, || {
            if let Some(mut s) = read_state(&path).filter(|s| s.token == token) {
                s.alive_at = 0;
                write_state(&path, &s)?;
            }
            Ok(())
        })?;
    }
    Ok(ended)
}

fn write_state(path: &Path, state: &AdapterState) -> anyhow::Result<()> {
    context::write_binding_file(path, &serde_json::to_string(state)?)
}

fn primed(token: &str, reading: Reading) -> WatchState {
    WatchState {
        token: token.to_owned(),
        direct: reading.direct,
        newest_direct: reading.newest_direct,
        inbox: reading.inbox.unwrap_or(0),
        primed: true,
    }
}

fn save_mark(config_dir: &Path, path: &Path, token: &str, mark: &WatchState) -> anyhow::Result<()> {
    context::with_config_lock(config_dir, || {
        if let Some(mut s) = read_state(path).filter(|s| s.token == token) {
            s.mark = mark.clone();
            s.alive_at = now_secs();
            write_state(path, &s)?;
        }
        Ok(())
    })
}

/// Announce (`Some(ttl)`) or withdraw (`None`) the adapter as the window's
/// wake mechanism. Descriptive, best effort.
async fn announce(binding: &Binding, ttl: Option<u64>) {
    let args = match ttl {
        Some(secs) => json!({"refresh_only": true, "wake": WAKE_LABEL, "wake_ttl_seconds": secs}),
        None => json!({"refresh_only": true, "wake": ""}),
    };
    if let Err(e) = hook::call_as_window(binding, "heartbeat", args).await {
        tracing::warn!(error = %e, "could not announce the wake mechanism");
    }
}

/// What one wake came to.
#[derive(Debug, PartialEq, Eq)]
pub enum Delivery {
    /// Codex accepted the turn and said it started.
    Confirmed,
    /// Codex accepted the turn; no start was seen in time.
    Requested,
    /// Every attempt failed.
    Failed(String),
}

/// Record the outcome on the window's presence.
async fn report(binding: &Binding, result: Delivery) {
    let value = match &result {
        Delivery::Confirmed => "confirmed",
        Delivery::Requested => "requested",
        Delivery::Failed(why) => {
            tracing::warn!(reason = %why, "codex wake failed");
            "failed"
        }
    };
    if let Err(e) = hook::call_as_window(
        binding,
        "heartbeat",
        json!({"refresh_only": true, "wake_result": value}),
    )
    .await
    {
        tracing::warn!(error = %e, "could not record the wake result");
    }
}

/// Start one turn on the thread, with bounded retries.
async fn deliver(socket: &Path, thread: &str, notice: &str, mark: &WatchState) -> Delivery {
    let mut last = String::new();
    for (attempt, pause) in DELIVERY_BACKOFF.iter().enumerate() {
        match deliver_once(socket, thread, notice, mark).await {
            Ok(d) => return d,
            Err(e) => {
                last = format!("{e:#}");
                tracing::warn!(attempt = attempt + 1, error = %last, "codex wake attempt failed");
                tokio::time::sleep(*pause).await;
            }
        }
    }
    Delivery::Failed(last)
}

async fn deliver_once(
    socket: &Path,
    thread: &str,
    notice: &str,
    mark: &WatchState,
) -> anyhow::Result<Delivery> {
    let mut app = AppServer::connect(socket, "ai-crew-sync-wake").await?;
    let read = app
        .request("thread/read", json!({"threadId": thread}))
        .await?;
    let mut status = thread_status(&read["thread"])
        .unwrap_or("unknown")
        .to_owned();
    if status == "notLoaded" {
        app.request(
            "thread/resume",
            json!({"threadId": thread, "excludeTurns": true}),
        )
        .await?;
        status = "idle".into();
    }
    if status == "systemError" {
        bail!("the Codex thread is in an error state");
    }
    if status == "active" {
        // Never a second turn: wait until the human's (or anyone's) turn is
        // over.
        wait_until_idle(&mut app, thread).await?;
    }
    let started = app
        .request(
            "turn/start",
            json!({
                "threadId": thread,
                "input": [{"type": "text", "text": notice}],
                // Names the news this turn is for, so it can be told apart
                // in the thread's history.
                "clientUserMessageId": format!(
                    "acs-wake-{}-{}", mark.newest_direct, mark.inbox
                ),
            }),
        )
        .await?;
    let Some(turn) = started["turn"]["id"].as_str().map(str::to_owned) else {
        bail!("Codex accepted turn/start without a turn id");
    };
    // Confirmed once Codex says the turn started. A connection that did
    // not start or resume the thread is not sent its notifications
    // (verified against codex-cli 0.163.0), so the turn list is read too.
    let until = tokio::time::Instant::now() + CONFIRM_WITHIN;
    loop {
        let left = until.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            return Ok(Delivery::Requested);
        }
        if let Some(n) = app.next_notification(left.min(POLL_EVERY)).await?
            && n["method"] == "turn/started"
            && n["params"]["threadId"] == thread
            && n["params"]["turn"]["id"] == turn.as_str()
        {
            return Ok(Delivery::Confirmed);
        }
        let turns = app
            .request(
                "thread/turns/list",
                json!({"threadId": thread, "limit": 10}),
            )
            .await?;
        let started = turns["data"].as_array().is_some_and(|ts| {
            ts.iter()
                .any(|t| t["id"] == turn.as_str() && !t["startedAt"].is_null())
        });
        if started {
            return Ok(Delivery::Confirmed);
        }
    }
}

/// Wait for `thread` to become idle, re-reading its status now and then in
/// case a notification was missed.
async fn wait_until_idle(app: &mut AppServer, thread: &str) -> anyhow::Result<()> {
    let until = tokio::time::Instant::now() + QUEUE_MAX;
    loop {
        let left = until.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            bail!("the Codex thread stayed busy for {}s", QUEUE_MAX.as_secs());
        }
        match app.next_notification(left.min(POLL_EVERY)).await? {
            Some(n)
                if n["method"] == "thread/status/changed" && n["params"]["threadId"] == thread =>
            {
                if thread_status(&n["params"]) == Some("idle") {
                    return Ok(());
                }
            }
            Some(_) => {}
            None => {
                let read = app
                    .request("thread/read", json!({"threadId": thread}))
                    .await?;
                if thread_status(&read["thread"]) == Some("idle") {
                    return Ok(());
                }
            }
        }
    }
}
