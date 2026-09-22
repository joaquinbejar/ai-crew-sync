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
//!    shell script, which is where its loop guard lives.

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
    /// The turn ended; the window is still open.
    Stop,
    /// The window is closing.
    SessionEnd,
    /// What this binding is, for troubleshooting. Never prints a secret.
    Status,
}

impl std::str::FromStr for Event {
    type Err = anyhow::Error;
    fn from_str(raw: &str) -> anyhow::Result<Self> {
        match raw.trim().to_lowercase().replace(['-', ' '], "_").as_str() {
            "session_start" | "sessionstart" => Ok(Self::SessionStart),
            "heartbeat" => Ok(Self::Heartbeat),
            "stop" => Ok(Self::Stop),
            "session_end" | "sessionend" => Ok(Self::SessionEnd),
            "status" => Ok(Self::Status),
            other => bail!(
                "unknown hook event '{other}'; use session_start, heartbeat, stop, \
                 session_end or status"
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
async fn call_as_window(binding: &Binding, tool: &str, args: Value) -> anyhow::Result<Value> {
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
) -> anyhow::Result<()> {
    let (repo, branch) = git_place(cwd);
    let mut args = json!({ "status": status, "ttl_seconds": ttl });
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

/// Run one hook event. Returns what to print on stdout: the host's JSON for
/// `session_start`, nothing for the presence events.
pub async fn run(
    config_dir: &Path,
    binding_id: &str,
    event: Event,
    cwd: &Path,
    digest_hours: i64,
) -> anyhow::Result<Option<String>> {
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
            let _ = publish_presence(&binding, cwd, "active", true, 900).await;
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
        Event::Heartbeat | Event::Stop => {
            publish_presence(&binding, cwd, "active", false, 900).await?;
            Ok(None)
        }
        Event::SessionEnd => {
            publish_presence(&binding, cwd, "idle", false, 120).await?;
            Ok(None)
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
            ("session_end", Event::SessionEnd),
            ("status", Event::Status),
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
                run(&dir, "conv-unknown", event, &cwd, 8)
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
            run(&dir, "conv-closed", Event::Heartbeat, &cwd, 8)
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
            run(&dir, "conv-closed-token", Event::Heartbeat, &cwd, 8)
                .await
                .unwrap()
                .is_none(),
            "a closed binding must not act, even with a credential on disk"
        );
        let out = run(&dir, "conv-closed-token", Event::Status, &cwd, 8)
            .await
            .unwrap()
            .unwrap();
        assert!(out.contains("no-credential"), "{out}");
        assert!(
            !out.contains("acss_retained"),
            "status must not print a secret"
        );

        // `status` explains both cases without inventing an identity.
        let out = run(&dir, "conv-unknown", Event::Status, &cwd, 8)
            .await
            .unwrap()
            .unwrap();
        assert!(out.contains("\"state\": \"missing\""), "{out}");
        let out = run(&dir, "conv-closed", Event::Status, &cwd, 8)
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
