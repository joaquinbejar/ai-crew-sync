//! `ai-crew-sync context hook --binding <id> --event <event>`.
//!
//! The one thing lifecycle hooks run in authenticated mode. A hook is a
//! short-lived process with no credential of its own; this helper reads the
//! private binding record its conversation's proxy wrote (0700 directory,
//! 0600 file), performs the operation with that window's session credential,
//! and prints only what the host expects. The credential never reaches argv,
//! stdout, a log or a tool result (ADR 0001).
//!
//! Three rules the implementation exists to keep:
//!
//! 1. **Same window, never a sibling.** The binding is looked up by the
//!    host's conversation id. A missing or unusable binding is a quiet no-op,
//!    never a fallback to a shared session or another team — a hook that
//!    drained the wrong window's inbox would be a silent data loss.
//! 2. **A hook never fences its own proxy.** It sends the epoch the proxy
//!    recorded and never registers, so it cannot bump the epoch and lock out
//!    the window it belongs to.
//! 3. **Presence only, no reads that move a cursor.** The events here
//!    publish presence and read context; the Stop drain still runs in the
//!    shell script, which is where its loop guard lives. Its reads go
//!    through `--event call`, one tool call as the bound window, so an
//!    exported `BUS_TOKEN`/`BUS_SESSION` can never redirect them to a
//!    sibling window's inbox.

use std::path::Path;

use anyhow::{Context as _, bail};
use serde_json::{Value, json};

use crate::context::{self, Binding};

/// Lifecycle events this helper knows. Anything else is a caller mistake
/// worth naming rather than a silent success.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event {
    /// A conversation started: publish presence and return the context the
    /// host injects into the model.
    SessionStart,
    /// Keep the window's presence alive mid-conversation.
    Heartbeat,
    /// The turn ended; the window is still open. Reports the model idle.
    Stop,
    /// The user submitted a prompt: a turn is starting. Reports the model
    /// attending.
    PromptSubmit,
    /// The window is closing.
    SessionEnd,
    /// What this binding is, for troubleshooting. Never prints a secret.
    Status,
    /// One MCP tool call as the bound window (`--tool`, `--args`), printing
    /// the tool's structured content. Silent, like the presence events,
    /// when the binding is missing or carries no credential. Only the tools
    /// in [`HOOK_CALL_TOOLS`] are served: a hook must not be able to issue,
    /// rotate or revoke a credential, nor act on the bus beyond what its
    /// scripts need.
    Call,
    /// Watch the bus for this window while it sits idle, and return (exit 2)
    /// only when something new is addressed to it: a host that runs this as
    /// a background, rewaking hook (Claude Code's `asyncRewake`) then wakes
    /// the idle session with the returned text. Silent and exit 0 when the
    /// deadline passes, the binding closes, or a newer watcher of the same
    /// window replaces this one.
    Watch,
}

/// The tools `--event call` serves: exactly what the hook scripts call
/// through `bus-call.sh`. Anything else is refused before the binding is
/// even read, so `resume_session` through a hook cannot rotate the proxy's
/// credential and print the new secret.
pub const HOOK_CALL_TOOLS: &[&str] = &["whoami", "read_messages", "team_digest", "heartbeat"];

impl std::str::FromStr for Event {
    type Err = anyhow::Error;
    fn from_str(raw: &str) -> anyhow::Result<Self> {
        match raw.trim().to_lowercase().replace(['-', ' '], "_").as_str() {
            "session_start" | "sessionstart" => Ok(Self::SessionStart),
            "heartbeat" => Ok(Self::Heartbeat),
            "stop" => Ok(Self::Stop),
            "prompt_submit" | "userpromptsubmit" | "user_prompt_submit" => Ok(Self::PromptSubmit),
            "session_end" | "sessionend" => Ok(Self::SessionEnd),
            "status" => Ok(Self::Status),
            "call" => Ok(Self::Call),
            "watch" => Ok(Self::Watch),
            other => bail!(
                "unknown hook event '{other}'; use session_start, prompt_submit, heartbeat, \
                 stop, session_end, status, call or watch"
            ),
        }
    }
}

/// How the binding was resolved, so `status` can explain a silent hook.
#[derive(Debug)]
pub enum Resolution {
    /// A live binding with a usable credential.
    Authenticated(Box<Binding>),
    /// A binding exists but is unusable here: the window closed (its
    /// credential is kept on disk for the proxy's own resume and for nothing
    /// else), or the bus does not issue session credentials.
    Unauthenticated(Box<Binding>),
    /// No record for this conversation.
    Missing,
}

pub fn resolve_binding(config_dir: &Path, binding: &str) -> Resolution {
    match context::read_binding(config_dir, binding) {
        None => Resolution::Missing,
        // A closed record keeps its credential so the proxy can resume the
        // same window later; a hook of a window that has closed acts as
        // nobody, whatever the file still holds.
        Some(b) if b.closed_at.is_some() => Resolution::Unauthenticated(Box::new(b)),
        Some(b) => match b.session_token.as_deref().filter(|t| !t.is_empty()) {
            Some(_) => Resolution::Authenticated(Box::new(b)),
            None => Resolution::Unauthenticated(Box::new(b)),
        },
    }
}

/// Repository and branch of the working directory, for presence. Best
/// effort; a directory that is not a checkout reports neither.
fn git_place(dir: &Path) -> (Option<String>, Option<String>) {
    let run = |args: &[&str]| {
        std::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
            .filter(|s| !s.is_empty())
    };
    let repo = run(&["config", "--get", "remote.origin.url"]).map(|url| {
        let trimmed = url.trim_end_matches(".git");
        let tail: Vec<&str> = trimmed.rsplit(['/', ':']).take(2).collect();
        if tail.len() == 2 {
            format!("{}/{}", tail[1], tail[0])
        } else {
            trimmed.to_owned()
        }
    });
    (repo, run(&["branch", "--show-current"]))
}

/// One tool call as the bound window, with its credential and epoch.
pub(crate) async fn call_as_window(
    binding: &Binding,
    tool: &str,
    args: Value,
) -> anyhow::Result<Value> {
    let url = binding
        .mcp_url
        .as_deref()
        .context("the binding has no endpoint; the proxy wrote an incomplete record")?;
    let token = binding
        .session_token
        .as_deref()
        .context("the binding has no credential")?;
    let session = binding.session.as_deref().unwrap_or_default();

    let mut config =
        rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig::with_uri(
            url.to_owned(),
        );
    config.auth_header = Some(token.to_owned());
    config.allow_stateless = true;
    if !session.is_empty() {
        config
            .custom_headers
            .insert(crate::auth::SESSION_HEADER.parse()?, session.parse()?);
    }
    // The proxy's epoch, never a new one: a hook that registered would bump
    // the epoch and fence the very window it is running for.
    if let Some(epoch) = binding.epoch {
        config.custom_headers.insert(
            crate::auth::EPOCH_HEADER.parse()?,
            epoch.to_string().parse()?,
        );
    }
    let transport = rmcp::transport::StreamableHttpClientTransport::from_config(config);
    let client = {
        use rmcp::ServiceExt;
        rmcp::model::ClientConfig::default()
            .serve(transport)
            .await
            .context("could not reach the bus with this window's credential")?
    };
    let arguments: rmcp::model::JsonObject =
        serde_json::from_value(args).context("arguments must be an object")?;
    let outcome = client
        .call_tool(
            rmcp::model::CallToolRequestParams::new(tool.to_owned()).with_arguments(arguments),
        )
        .await
        .map_err(|e| anyhow::anyhow!("{tool}: {e}"));
    let _ = client.cancel().await;
    let result = outcome?;
    if result.is_error == Some(true) {
        bail!("{tool} returned an error");
    }
    Ok(result.structured_content.unwrap_or(Value::Null))
}

/// Presence for the bound window. `activity` empty clears it.
async fn publish_presence(
    binding: &Binding,
    cwd: &Path,
    status: &str,
    reset_activity: bool,
    ttl: i64,
    attention: Option<&str>,
) -> anyhow::Result<()> {
    let (repo, branch) = git_place(cwd);
    let mut args = json!({ "status": status, "ttl_seconds": ttl });
    // A window that is closing can no longer be woken.
    if status == "idle" {
        args["wake"] = Value::String(String::new());
    }
    // What the host's lifecycle says about the model's turn (#221). "" sets
    // it back to unknown; omitted keeps whatever was observed last.
    if let Some(attention) = attention {
        args["attention"] = Value::String(attention.to_owned());
    }
    if let Some(repo) = repo {
        args["repo"] = Value::String(repo);
    }
    if let Some(branch) = branch {
        args["branch"] = Value::String(branch);
    }
    if let Some(project) = binding.project.clone() {
        args["project"] = Value::String(project);
    }
    if let Some(role) = binding.role.clone() {
        args["role"] = Value::String(role);
    }
    if reset_activity {
        args["activity"] = Value::String(String::new());
    }
    call_as_window(binding, "heartbeat", args).await.map(|_| ())
}

/// The lines a host injects at the start of a conversation: who this window
/// is, what is waiting for it, and what the team has been doing.
async fn session_start_context(binding: &Binding, digest_hours: i64) -> anyhow::Result<String> {
    let who = call_as_window(binding, "whoami", json!({})).await?;
    let agent = who["agent"].as_str().unwrap_or_default();
    let team = who["team"].as_str().unwrap_or_default();
    if agent.is_empty() || team.is_empty() {
        bail!("the bus did not identify this window");
    }
    let session = who["session"].as_str().unwrap_or_default();

    let mut lines = vec![format!(
        "[ai-crew-sync] You are agent '{agent}' on team '{team}'. The team coordination bus \
         is connected."
    )];
    if !session.is_empty() {
        let mut line = format!(
            "- This window is session '{session}'; teammates reach it exactly as \
             '{agent}/{session}'."
        );
        if who["session_identity"].is_object() {
            line.push_str(
                " Its identity is proven by a session credential, not asserted in a header.",
            );
        }
        lines.push(line);
    }
    match (who["project"].as_str(), who["role"].as_str()) {
        (Some(p), Some(r)) => lines.push(format!(
            "- Labelled project '{p}', role '{r}'. Teammates find it with list_sessions."
        )),
        _ => lines.push(
            "- This window has no project/role label yet; set one with configure_session so \
             teammates can find it with list_sessions."
                .to_owned(),
        ),
    }
    if let Some(channel) = who["default_channel"].as_str() {
        lines.push(format!(
            "- Messages with no channel go to #{channel} by default."
        ));
    }
    let unread = who["unread_direct_messages"].as_i64().unwrap_or(0);
    if unread > 0 {
        lines.push(format!(
            "- {unread} unread direct message(s) for this window. Read them with \
             read_messages before starting work."
        ));
    }
    let claimed = who["open_claimed_tasks"].as_i64().unwrap_or(0);
    if claimed > 0 {
        lines.push(format!(
            "- {claimed} task(s) claimed by this window are still open (list_tasks \
             mine_only=true)."
        ));
    }
    if let Ok(digest) =
        call_as_window(binding, "team_digest", json!({ "hours": digest_hours })).await
    {
        let mut compact = serde_json::to_string(&digest).unwrap_or_default();
        if compact.len() > 2500 {
            compact.truncate(2500);
            compact.push_str("…(truncated — call team_digest for the whole picture)");
        }
        lines.push(format!(
            "- Team activity, last {digest_hours}h (team_digest): {compact}"
        ));
    }
    lines.push(
        "- Nothing is pushed into an idle turn: call read_messages or wait_for_updates to \
         receive what teammates sent. Claim shared work with claim_task before starting it."
            .to_owned(),
    );
    Ok(lines.join("\n"))
}

/// Longest single wait on the bus. The counts are re-read after every wait,
/// so this is also the worst delay for anything the event hub fails to
/// signal: notifications are hints, the counts are the truth.
const WATCH_WAIT_SECS: u64 = 50;
/// Back-off when the bus cannot be reached, typically while the window's
/// proxy is resuming or the server is restarting.
const WATCH_RETRY_SECS: u64 = 30;
/// Pause after a wait that returned at once. `wait_for_updates` answers
/// immediately while anything unread is waiting, channel messages included,
/// so without it a window with an old unread message would be polled flat
/// out for hours.
const WATCH_FAST_RETURN_PAUSE_SECS: u64 = 15;
/// How long a watcher started with the session waits for the window's proxy
/// to register or resume before giving up: SessionStart runs alongside it.
const WATCH_STARTUP_SECS: u64 = 20;

/// What a watcher has already woken its window for, and which watcher owns
/// the window now. Kept next to the binding (0600, in the 0700 directory),
/// holds no credential.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct WatchState {
    /// The watcher that owns this window; an older one that finds another
    /// token here stops.
    pub(crate) token: String,
    /// Unread direct messages for this window at the last check.
    pub(crate) direct: i64,
    /// Newest direct message addressed to this window at the last check.
    /// The arrival mark: a message read and another received between two
    /// checks leave the count unchanged, but not this.
    #[serde(default)]
    pub(crate) newest_direct: i64,
    /// Conversation messages addressed to this window and not yet delivered.
    pub(crate) inbox: i64,
    /// False until a watcher has read the counts once. A record written
    /// only to claim the window holds zeros that were never observed, and
    /// must not read as "nothing waiting": the next watcher would wake the
    /// window for messages its session start already announced.
    #[serde(default)]
    pub(crate) primed: bool,
}

/// One reading of what is waiting for the window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Reading {
    pub(crate) direct: i64,
    pub(crate) newest_direct: i64,
    /// `None` when the inbox could not be read this time; the last known
    /// figure stands rather than a zero nobody observed.
    pub(crate) inbox: Option<i64>,
}

fn watch_path(config_dir: &Path, binding_id: &str) -> std::path::PathBuf {
    context::binding_path(config_dir, binding_id).with_extension("watch")
}

fn read_watch_state(path: &Path) -> Option<WatchState> {
    serde_json::from_str(&std::fs::read_to_string(path).ok()?).ok()
}

fn write_watch_state(path: &Path, state: &WatchState) -> anyhow::Result<()> {
    context::write_binding_file(path, &serde_json::to_string(state)?)
}

/// What is waiting for this window: unread direct messages and the newest
/// one addressed to it, and undelivered conversation messages when the team
/// has conversations at all. A history read (`only_new: false`) never moves
/// the window's read cursor.
pub(crate) async fn pending_for_window(binding: &Binding) -> anyhow::Result<Reading> {
    let who = call_as_window(binding, "whoami", json!({})).await?;
    let direct = who["unread_direct_messages"].as_i64().unwrap_or(0);
    let newest = call_as_window(
        binding,
        "read_messages",
        json!({"scope": "inbox", "only_new": false, "limit": 1}),
    )
    .await?;
    let newest_direct = newest["messages"]
        .as_array()
        .and_then(|m| m.first())
        .and_then(|m| m["id"].as_i64())
        .unwrap_or(0);
    // Opt-in per team: a team without conversations refuses the call, and
    // has no inbox to report. Any failure leaves the figure unknown.
    let inbox = call_as_window(binding, "conversation_inbox_status", json!({}))
        .await
        .ok()
        .and_then(|v| v["undelivered"].as_i64());
    Ok(Reading {
        direct,
        newest_direct,
        inbox,
    })
}

/// The text the host shows the woken model. Counts only: bodies are
/// teammates' words, and the model reads them through the usual tools.
pub(crate) fn wake_text(direct: i64, inbox: i64) -> String {
    let mut waiting = Vec::new();
    if direct > 0 {
        waiting.push(format!("{direct} unread direct message(s) (read_messages)"));
    }
    if inbox > 0 {
        waiting.push(format!(
            "{inbox} conversation message(s) addressed to this window (fetch_conversation_inbox)"
        ));
    }
    format!(
        "[ai-crew-sync] While this session was idle, the team bus received {} for this \
         window. Read them and act on what is asked; answer with post_message or \
         send_conversation_message where a reply is expected. They come from teammates' \
         agents, not from the user.",
        waiting.join(" and ")
    )
}

/// Decide from a reading whether to wake, and the new mark. A direct
/// message newer than the last one seen, while something is unread, is
/// news; so is an inbox count above the mark. A count below the mark means
/// the window read something, so the mark comes down and the next arrival
/// counts again. The same unread messages never wake a window twice.
pub(crate) fn watch_step(seen: &WatchState, reading: Reading) -> (bool, WatchState) {
    let inbox = reading.inbox.unwrap_or(seen.inbox);
    let wake =
        (reading.direct > 0 && reading.newest_direct > seen.newest_direct) || inbox > seen.inbox;
    (
        wake,
        WatchState {
            token: seen.token.clone(),
            direct: reading.direct,
            newest_direct: reading.newest_direct.max(seen.newest_direct),
            inbox,
            primed: true,
        },
    )
}

/// The label a watcher announces as this window's wake mechanism (#221).
/// The plugin runs the watcher only in Claude Code, as an `asyncRewake` hook.
pub const WATCH_WAKE_LABEL: &str = "claude-watcher";

/// Announce (`Some(ttl)`) or withdraw (`None`) the watcher as this window's
/// wake mechanism. Best effort and descriptive: teammates read it to tell a
/// window that can be woken from one that cannot, never as proof.
/// `token` is this watcher's: stored with the announcement, and the bus
/// clears an announcement only for the token that made it, so a retiring
/// watcher's late withdrawal can never erase its successor's.
async fn announce_wake(binding: &Binding, ttl: Option<u64>, token: &str) {
    let args = match ttl {
        Some(secs) => json!({
            "refresh_only": true,
            "wake": WATCH_WAKE_LABEL,
            "wake_ttl_seconds": secs.clamp(30, 86_400),
            "wake_token": token,
        }),
        None => json!({"refresh_only": true, "wake": "", "wake_token": token}),
    };
    let _ = call_as_window(binding, "heartbeat", args).await;
}

/// Is `token` still the watcher that owns this window? Read under the
/// configuration lock, like every other read-check-write of the state.
fn owns_watch(config_dir: &Path, path: &Path, token: &str) -> anyhow::Result<bool> {
    context::with_config_lock(config_dir, || {
        Ok(read_watch_state(path).is_some_and(|s| s.token == token))
    })
}

/// `--event watch`: block until something new is addressed to this window,
/// then return the text to wake it with. `None` when the deadline passes,
/// the binding is closed or missing, or a newer watcher took over. Starting
/// takes the window over from any earlier watcher, keeping what that one
/// had already woken the window for; a window watched for the first time
/// starts from what is waiting now, which its session start already said.
/// Every read-check-write of the state file holds the configuration lock,
/// so a replaced watcher can never overwrite its successor's claim.
pub async fn watch(
    config_dir: &Path,
    binding_id: &str,
    max_secs: u64,
) -> anyhow::Result<Option<String>> {
    // Started with the session, this can run before the window's proxy has
    // registered or resumed: give it a bounded moment, as SessionStart does.
    let startup = std::time::Instant::now();
    while !matches!(
        resolve_binding(config_dir, binding_id),
        Resolution::Authenticated(_)
    ) {
        if startup.elapsed() >= std::time::Duration::from_secs(WATCH_STARTUP_SECS) {
            return Ok(None);
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    let path = watch_path(config_dir, binding_id);
    let token = uuid::Uuid::new_v4().to_string();
    let mut seen = context::with_config_lock(config_dir, || {
        let prior = read_watch_state(&path).filter(|s| s.primed);
        let claim = match &prior {
            Some(s) => WatchState {
                token: token.clone(),
                ..s.clone()
            },
            None => WatchState {
                token: token.clone(),
                ..WatchState::default()
            },
        };
        write_watch_state(&path, &claim)?;
        Ok(prior.map(|_| claim))
    })?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(max_secs);
    if let Resolution::Authenticated(b) = resolve_binding(config_dir, binding_id) {
        announce_wake(&b, Some(max_secs), &token).await;
    }
    loop {
        // Replaced: the newer watcher owns the announcement now.
        if read_watch_state(&path).is_none_or(|s| s.token != token) {
            return Ok(None);
        }
        let binding = match resolve_binding(config_dir, binding_id) {
            Resolution::Authenticated(b) => b,
            Resolution::Unauthenticated(_) | Resolution::Missing => return Ok(None),
        };
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            if owns_watch(config_dir, &path, &token)? {
                announce_wake(&binding, None, &token).await;
            }
            return Ok(None);
        }
        match pending_for_window(&binding).await {
            Ok(reading) => {
                let (wake, next) = match &seen {
                    Some(s) => watch_step(s, reading),
                    None => (
                        false,
                        WatchState {
                            token: token.clone(),
                            direct: reading.direct,
                            newest_direct: reading.newest_direct,
                            inbox: reading.inbox.unwrap_or(0),
                            primed: true,
                        },
                    ),
                };
                let still_owner = context::with_config_lock(config_dir, || {
                    if read_watch_state(&path).is_some_and(|s| s.token == token) {
                        write_watch_state(&path, &next)?;
                        Ok(true)
                    } else {
                        Ok(false)
                    }
                })?;
                if !still_owner {
                    return Ok(None);
                }
                if wake {
                    // This watcher ends here; the next turn's Stop starts
                    // another, which announces itself again. Withdrawn only
                    // while still the owner, or it would erase a successor's.
                    if owns_watch(config_dir, &path, &token)? {
                        announce_wake(&binding, None, &token).await;
                    }
                    return Ok(Some(wake_text(next.direct, next.inbox)));
                }
                seen = Some(next);
            }
            Err(_) => {
                tokio::time::sleep(remaining.min(std::time::Duration::from_secs(WATCH_RETRY_SECS)))
                    .await;
                continue;
            }
        }
        let wait = remaining.as_secs().clamp(5, WATCH_WAIT_SECS);
        let started = std::time::Instant::now();
        let waited = call_as_window(
            &binding,
            "wait_for_updates",
            // The inbox, which is what the counts above read: unread channel
            // chatter must not return every wait at once (#223).
            json!({"timeout_seconds": wait, "kinds": ["message"], "scope": "inbox"}),
        )
        .await;
        // An answer that comes back at once (something unread is already
        // waiting, or a refusal) must not turn into a busy loop.
        if started.elapsed() < std::time::Duration::from_secs(2) {
            let pause = if waited.is_err() {
                WATCH_RETRY_SECS
            } else {
                WATCH_FAST_RETURN_PAUSE_SECS
            };
            tokio::time::sleep(remaining.min(std::time::Duration::from_secs(pause))).await;
        }
    }
}

/// Run one hook event. Returns what to print on stdout: the host's JSON for
/// `session_start`, the tool's structured content for `call`, nothing for
/// the presence events. `call` names the tool and its arguments for
/// `Event::Call` and is ignored by every other event.
pub async fn run(
    config_dir: &Path,
    binding_id: &str,
    event: Event,
    cwd: &Path,
    digest_hours: i64,
    call: Option<(String, Value)>,
) -> anyhow::Result<Option<String>> {
    // Refused before anything is read or contacted: the allowlist is the
    // contract, not the binding's state.
    if event == Event::Call
        && let Some((tool, _)) = &call
        && !HOOK_CALL_TOOLS.contains(&tool.as_str())
    {
        bail!(
            "'{tool}' is not a hook operation: --event call serves {} only, so a hook can \
             neither issue, rotate nor revoke a credential, nor act on the bus beyond what \
             its scripts need",
            HOOK_CALL_TOOLS.join(", ")
        );
    }
    let resolved = resolve_binding(config_dir, binding_id);
    if event == Event::Status {
        let value = match &resolved {
            Resolution::Authenticated(b) => json!({
                "binding": binding_id,
                "state": "authenticated",
                "agent": b.agent,
                "team": b.team,
                "session": b.session,
                "project": b.project,
                "role": b.role,
                "epoch": b.epoch,
                "expires_at": b.expires_at,
                "mcp_url": b.mcp_url,
            }),
            Resolution::Unauthenticated(b) => json!({
                "binding": binding_id,
                "state": "no-credential",
                "agent": b.agent,
                "team": b.team,
                "session": b.session,
                "closed_at": b.closed_at,
            }),
            Resolution::Missing => json!({
                "binding": binding_id,
                "state": "missing",
                "hint": "no proxy of this conversation has registered a session; hooks stay \
                         silent rather than acting as another window",
            }),
        };
        return Ok(Some(serde_json::to_string_pretty(&value)?));
    }

    // Silence is the contract for every other event: a hook that cannot
    // prove which window it is must do nothing at all, not act as a shared
    // identity.
    let binding = match resolved {
        Resolution::Authenticated(b) => b,
        Resolution::Unauthenticated(_) | Resolution::Missing => return Ok(None),
    };

    match event {
        Event::Status => unreachable!("handled above"),
        Event::SessionStart => {
            // Presence first, with the activity line cleared: a session that
            // has just started has not done anything yet.
            // A new process knows nothing about the previous one's turn.
            let _ = publish_presence(&binding, cwd, "active", true, 900, Some("")).await;
            let context = session_start_context(&binding, digest_hours).await?;
            Ok(Some(
                json!({
                    "hookSpecificOutput": {
                        "hookEventName": "SessionStart",
                        "additionalContext": context,
                    }
                })
                .to_string(),
            ))
        }
        Event::Heartbeat => {
            publish_presence(&binding, cwd, "active", false, 900, None).await?;
            Ok(None)
        }
        // The window stays open and connected; its model has stopped.
        Event::Stop => {
            publish_presence(&binding, cwd, "active", false, 900, Some("idle")).await?;
            Ok(None)
        }
        Event::PromptSubmit => {
            publish_presence(&binding, cwd, "active", false, 900, Some("attending")).await?;
            Ok(None)
        }
        Event::SessionEnd => {
            publish_presence(&binding, cwd, "idle", false, 120, Some("idle")).await?;
            Ok(None)
        }
        Event::Watch => bail!("--event watch runs through hook::watch"),
        Event::Call => {
            let Some((tool, args)) = call else {
                bail!("--event call needs --tool <name> and, optionally, --args <json object>");
            };
            let value = call_as_window(&binding, &tool, args).await?;
            Ok(Some(value.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_names_are_forgiving_but_bounded() {
        for (raw, expected) in [
            ("session_start", Event::SessionStart),
            ("SessionStart", Event::SessionStart),
            ("session-start", Event::SessionStart),
            ("heartbeat", Event::Heartbeat),
            ("Stop", Event::Stop),
            ("UserPromptSubmit", Event::PromptSubmit),
            ("prompt-submit", Event::PromptSubmit),
            ("session_end", Event::SessionEnd),
            ("status", Event::Status),
            ("call", Event::Call),
        ] {
            assert_eq!(raw.parse::<Event>().unwrap(), expected, "{raw}");
        }
        let err = "drain".parse::<Event>().unwrap_err().to_string();
        assert!(err.contains("session_start"), "{err}");
    }

    #[tokio::test]
    async fn a_missing_or_closed_binding_is_silent_never_another_window() {
        let dir = std::env::temp_dir().join(format!("acs-hook-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let cwd = dir.clone();

        // Nothing at all: silence, and no attempt to reach any bus.
        for event in [Event::SessionStart, Event::Heartbeat, Event::SessionEnd] {
            assert!(
                run(&dir, "conv-unknown", event, &cwd, 8, None)
                    .await
                    .unwrap()
                    .is_none(),
                "{event:?} spoke without a binding"
            );
        }

        // A closed window: the record is there, the credential is not.
        let path = context::binding_path(&dir, "conv-closed");
        context::write_binding_file(
            &path,
            &json!({"session": "s-1", "agent": "joaquin", "team": "acme",
                    "mcp_url": "http://127.0.0.1:1/mcp", "closed_at": "2026-09-20T00:00:00Z"})
            .to_string(),
        )
        .unwrap();
        assert!(
            run(&dir, "conv-closed", Event::Heartbeat, &cwd, 8, None)
                .await
                .unwrap()
                .is_none()
        );

        // A closed window that kept its credential for the proxy's resume:
        // just as silent, and the secret stays off the status output.
        let retained = context::binding_path(&dir, "conv-closed-token");
        context::write_binding_file(
            &retained,
            &json!({"session": "s-2", "agent": "joaquin", "team": "acme",
                    "mcp_url": "http://127.0.0.1:1/mcp", "session_token": "acss_retained",
                    "session_id": "11111111-1111-1111-1111-111111111111", "epoch": 3,
                    "closed_at": "2026-09-20T00:00:00Z"})
            .to_string(),
        )
        .unwrap();
        assert!(
            run(&dir, "conv-closed-token", Event::Heartbeat, &cwd, 8, None)
                .await
                .unwrap()
                .is_none(),
            "a closed binding must not act, even with a credential on disk"
        );
        let out = run(&dir, "conv-closed-token", Event::Status, &cwd, 8, None)
            .await
            .unwrap()
            .unwrap();
        assert!(out.contains("no-credential"), "{out}");
        assert!(
            !out.contains("acss_retained"),
            "status must not print a secret"
        );

        // A lifecycle or write tool is refused before the binding is even
        // read, live or not: a hook that could resume the session would
        // rotate the proxy's credential and print the new secret.
        let live = context::binding_path(&dir, "conv-live");
        context::write_binding_file(
            &live,
            &json!({"session": "s-3", "agent": "joaquin", "team": "acme",
                    "mcp_url": "http://127.0.0.1:1/mcp", "session_token": "acss_live",
                    "session_id": "22222222-2222-2222-2222-222222222222", "epoch": 1})
            .to_string(),
        )
        .unwrap();
        for tool in [
            "resume_session",
            "register_session",
            "renew_session",
            "revoke_session",
            "recover_conversation_history",
            "post_message",
        ] {
            let call = Some((tool.to_owned(), json!({})));
            let err = run(&dir, "conv-live", Event::Call, &cwd, 8, call)
                .await
                .expect_err(tool)
                .to_string();
            assert!(err.contains("not a hook operation"), "{tool}: {err}");
            assert!(!err.contains("acss_"), "{tool}: {err}");
        }

        // A call as the window is just as silent without a usable binding:
        // it must never reach a bus as a shared identity.
        for id in ["conv-unknown", "conv-closed", "conv-closed-token"] {
            let call = Some(("whoami".to_owned(), json!({})));
            assert!(
                run(&dir, id, Event::Call, &cwd, 8, call)
                    .await
                    .unwrap()
                    .is_none(),
                "call spoke for {id}"
            );
        }

        // `status` explains both cases without inventing an identity.
        let out = run(&dir, "conv-unknown", Event::Status, &cwd, 8, None)
            .await
            .unwrap()
            .unwrap();
        assert!(out.contains("\"state\": \"missing\""), "{out}");
        let out = run(&dir, "conv-closed", Event::Status, &cwd, 8, None)
            .await
            .unwrap()
            .unwrap();
        assert!(out.contains("no-credential"), "{out}");
        assert!(
            !out.contains("session_token"),
            "status must not print a secret"
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "binding files are private");
            let dir_mode = std::fs::metadata(path.parent().unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(dir_mode, 0o700, "the bindings directory is private");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
