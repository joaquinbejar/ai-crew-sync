//! `ai-crew-sync mcp proxy`: a local stdio MCP server that forwards every
//! tool call to the remote bus as **one** agent in **one** session.
//!
//! Why a proxy at all: the bus is stateless Streamable HTTP and any MCP client
//! can talk to it directly. What a direct connection cannot do is give each
//! *conversation* its own session when several windows open the same
//! repository with the same token — a header set once in a client config is
//! the same header in every window. A stdio server is started by the host
//! once per conversation (that is the MCP norm, and what Claude Code and
//! Codex do), so the process itself is the unit of isolation: it mints the
//! session label, sends it on every forwarded request, and keeps the
//! project/role metadata for exactly that window.
//!
//! The proxy is host-agnostic. What a host provides is used as an
//! enrichment, never required:
//!
//! - **Conversation identity**, in order: `--host-session` / `BUS_HOST_SESSION`
//!   (any host that can set per-window environment), `CLAUDE_CODE_SESSION_ID`
//!   (Claude Code exports it to MCP server processes), the `_meta.threadId`
//!   Codex attaches to every `tools/call`, and otherwise the process itself
//!   — a random id that lives as long as this instance. A known conversation
//!   id derives a **stable** session label, so a resumed conversation
//!   reconnects to the same session and a forked one gets a new one.
//! - **Start-of-session context** goes into the `initialize` result's
//!   `instructions`, which every MCP client hands to the model. No hook
//!   needed.
//! - **Presence** is kept by the proxy itself: a heartbeat on connect (with
//!   repo and branch read from the project directory), a periodic
//!   keep-alive, `idle` on exit.
//!
//! Two local tools, `configure_session` and `session_status`, never reach the
//! remote server's catalogue. They change metadata (project, role, channel)
//! and, within the same team, the profile; a team change needs a new
//! conversation, because switching credentials cannot erase what this
//! conversation has already seen. Every profile change is verified with
//! `whoami` before it is committed, in-flight calls to the old context are
//! cancelled rather than replayed, and claims or locks held by the old
//! identity are reported, never transferred.
//!
//! Stdout carries MCP framing only; everything else goes to stderr.

use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::Context as _;
use rmcp::{
    ErrorData, ServerHandler, ServiceExt,
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, ListToolsResult,
        PaginatedRequestParams, ServerCapabilities, ServerConfig, Tool,
    },
    service::{RequestContext, RoleServer, RunningService},
    transport::{
        StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;

use crate::context::{self, Inputs, Resolved};

/// Prefix of a session label the proxy mints. Opaque on purpose: the label
/// is an address, `project`/`role` are the human-facing part.
pub const SESSION_PREFIX: &str = "s-";
/// Hex characters after the prefix. 128 bits: the label is an address that
/// partitions cursors, claims and locks, so two conversations colliding
/// would merge them silently. The session-label limit is 64 bytes, which
/// leaves room to spare.
const SESSION_HEX: usize = 32;

/// Presence lease the proxy keeps alive, and how often it renews it.
const PRESENCE_TTL_SECS: i64 = 900;
const KEEPALIVE_EVERY: Duration = Duration::from_secs(300);
/// How long a context switch waits for in-flight calls to the old context
/// after cancelling them, before swapping anyway.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
/// How long the exit heartbeat may take; the host is waiting.
const EXIT_TIMEOUT: Duration = Duration::from_secs(3);

pub const CONFIGURE_TOOL: &str = "configure_session";
pub const STATUS_TOOL: &str = "session_status";

type Remote = RunningService<rmcp::RoleClient, rmcp::model::ClientConfig>;

/// Where the conversation id came from, reported by `session_status`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum Binding {
    /// `--host-session` or `BUS_HOST_SESSION`.
    Explicit,
    /// `CLAUDE_CODE_SESSION_ID` in the environment.
    ClaudeCode,
    /// `_meta.threadId` on the first forwarded call.
    RequestMeta,
    /// No conversation id: this process is the conversation.
    Instance,
}

/// Command-line shape of the proxy.
#[derive(Clone, Debug, Default)]
pub struct ProxyOptions {
    pub inputs: Inputs,
    pub project: Option<String>,
    pub role: Option<String>,
    pub channel: Option<String>,
    /// Explicit conversation id.
    pub host_session: Option<String>,
    /// Where the proxy keeps its binding record; the config directory.
    pub state_dir: PathBuf,
}

/// Session label derived from a conversation id: stable for the same id,
/// unrelated to repository, role or pid. Shared with the resolver so a hook
/// of the same conversation lands on the same session without coordinating.
pub use crate::context::session_for_host as session_for;

/// Validate a discovery label the way the bus does, so `configure_session`
/// refuses what `heartbeat` would reject rather than storing it locally and
/// reporting a success the server never saw. Empty clears.
fn check_label(field: &str, raw: &str) -> Result<Option<String>, ErrorData> {
    crate::store::presence::normalize_label(field, raw)
        .map(|v| (!v.is_empty()).then_some(v))
        .map_err(|e| ErrorData::invalid_params(e.to_string(), None))
}

fn random_session() -> String {
    let raw = crate::auth::generate_token();
    format!(
        "{SESSION_PREFIX}{}",
        &raw[crate::auth::TOKEN_PREFIX.len()..crate::auth::TOKEN_PREFIX.len() + SESSION_HEX]
    )
}

/// The credential this window proved itself with, when the bus issues them.
#[derive(Clone, Debug)]
pub struct SessionProof {
    /// The secret. Written only to the 0600 binding file, never to a tool
    /// result, a log or argv.
    pub token: String,
    pub session_id: String,
    pub epoch: i64,
    pub expires_at: String,
}

/// The verified, connected context of this instance.
struct Connected {
    resolved: Resolved,
    agent: String,
    team: String,
    remote: Arc<Remote>,
    tools: Vec<Tool>,
    remote_instructions: Option<String>,
    proof: Option<SessionProof>,
    /// Cancelled when this context is replaced; forwarded calls race it.
    ct: CancellationToken,
}

struct State {
    /// Present once a profile resolved and verified. Absent means the proxy
    /// serves only its local tools and says why in `instructions`.
    connected: Option<Connected>,
    /// Why there is no connection, for the model.
    disconnected_reason: Option<String>,
    session: String,
    binding: Binding,
    host_id: Option<String>,
    project: Option<String>,
    role: Option<String>,
    channel: Option<String>,
    /// Bumped on every context switch.
    generation: u64,
}

/// Counts calls currently forwarded; decremented on drop so a request the
/// host abandons mid-flight still lets a context switch drain.
struct InFlight(Arc<AtomicUsize>);

impl InFlight {
    fn enter(counter: &Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self(counter.clone())
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Clone)]
pub struct Proxy {
    state: Arc<RwLock<State>>,
    in_flight: Arc<AtomicUsize>,
    /// Serialises context switches.
    switch: Arc<Mutex<()>>,
    opts: Arc<ProxyOptions>,
    project_dir: PathBuf,
}

// -------------------------------------------------------------- local tools --

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct ConfigureArgs {
    /// What this window does on the project: implementation, design,
    /// review, … One lower-case word. Changing it keeps the session and its
    /// cursors; it only changes how teammates find you.
    #[serde(default)]
    pub role: Option<String>,
    /// Logical project, usually the repository name. Also the channel this
    /// session posts to by default when one of that name exists.
    #[serde(default)]
    pub project: Option<String>,
    /// Channel to post to by default; overrides the project's.
    #[serde(default)]
    pub channel: Option<String>,
    /// Switch to another locally approved profile. Verified with whoami
    /// before anything changes; must stay within the same team.
    #[serde(default)]
    pub profile: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct Status {
    /// True when calls are being forwarded to the bus.
    pub connected: bool,
    /// Why not, when `connected` is false.
    pub error: Option<String>,
    /// Verified with whoami; never asserted.
    pub agent: Option<String>,
    pub team: Option<String>,
    /// Session label every forwarded call carries.
    pub session: String,
    /// `agent/session`: what a teammate puts in `to` to reach this window.
    pub address: Option<String>,
    pub project: Option<String>,
    pub role: Option<String>,
    pub channel: Option<String>,
    pub profile: Option<String>,
    pub binding: Binding,
    pub project_root: Option<String>,
    pub bus: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ConfigureResult {
    pub status: Status,
    /// What the previous identity still holds, when the profile changed.
    /// Nothing is transferred: these expire by their own leases, or the
    /// previous identity releases them from its own window.
    pub previous: Option<PreviousIdentity>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct PreviousIdentity {
    pub agent: String,
    pub team: String,
    pub session: String,
    pub open_claims: Vec<String>,
    pub held_locks: Vec<String>,
}

fn schema_of<T: JsonSchema>() -> Arc<rmcp::model::JsonObject> {
    let schema = schemars::schema_for!(T);
    match serde_json::to_value(schema) {
        Ok(Value::Object(map)) => Arc::new(map),
        _ => Arc::new(rmcp::model::JsonObject::new()),
    }
}

fn local_tools() -> Vec<Tool> {
    vec![
        Tool::new(
            CONFIGURE_TOOL,
            "Set how THIS window presents itself on the bus: role (implementation, design, \
             review, …), project and default channel. Metadata only — it never changes who \
             you are or your session id, so cursors, claims and locks stay yours. `profile` \
             switches to another locally approved credential of the same team after \
             verifying it; a different team needs a new conversation. Affects this window \
             only.",
            schema_of::<ConfigureArgs>(),
        )
        .with_title("Configure this session")
        .with_output_schema::<ConfigureResult>(),
        Tool::new(
            STATUS_TOOL,
            "Who this window is on the bus (verified agent and team), its session id and \
             address (`agent/session`, what teammates use to reach exactly this window), \
             project, role and default channel. Never returns credentials.",
            schema_of::<EmptyArgs>(),
        )
        .with_title("Session status")
        .with_output_schema::<Status>(),
    ]
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct EmptyArgs {}

// --------------------------------------------------------------- connecting --

async fn connect_remote(
    url: &str,
    credential: &str,
    session: &str,
    epoch: Option<i64>,
) -> anyhow::Result<Remote> {
    let mut config = StreamableHttpClientTransportConfig::with_uri(url.to_owned());
    config.auth_header = Some(credential.to_owned());
    config.allow_stateless = true;
    config.custom_headers.insert(
        crate::auth::SESSION_HEADER.parse()?,
        session
            .parse()
            .context("session label is not a valid header value")?,
    );
    // Fencing is opt-in per connection: with it, a request from a process
    // that has been resumed away is refused instead of writing as the window
    // that replaced it.
    if let Some(epoch) = epoch {
        config.custom_headers.insert(
            crate::auth::EPOCH_HEADER.parse()?,
            epoch
                .to_string()
                .parse()
                .context("epoch is not a valid header value")?,
        );
    }
    let transport = StreamableHttpClientTransport::from_config(config);
    let remote = rmcp::model::ClientConfig::default()
        .serve(transport)
        .await
        .with_context(|| format!("could not connect to {url}"))?;
    Ok(remote)
}

async fn call_remote(remote: &Remote, name: &str, args: Value) -> anyhow::Result<Value> {
    let arguments: rmcp::model::JsonObject =
        serde_json::from_value(args).context("arguments must be an object")?;
    let result = remote
        .call_tool(CallToolRequestParams::new(name.to_owned()).with_arguments(arguments))
        .await
        .map_err(|e| anyhow::anyhow!("{name} failed: {e}"))?;
    if result.is_error == Some(true) {
        anyhow::bail!("{name} returned an error: {:?}", result.content);
    }
    Ok(result.structured_content.unwrap_or(Value::Null))
}

/// Resolve, connect with the session header, and verify who the token is.
/// Register this window for the first time. A server too old to know the
/// tool keeps the label-only connection it has always served; any other
/// failure is fatal, because continuing would forward with the parent token
/// under an asserted label while reporting a proven identity.
async fn register_new(remote: &Remote, session: &str) -> anyhow::Result<Option<SessionProof>> {
    match call_remote(remote, "register_session", json!({ "session": session })).await {
        Ok(v) => match v["session_token"].as_str() {
            Some(token) => Ok(Some(SessionProof {
                token: token.to_owned(),
                session_id: v["session_id"].as_str().unwrap_or_default().to_owned(),
                epoch: v["epoch"].as_i64().unwrap_or(1),
                expires_at: v["expires_at"].as_str().unwrap_or_default().to_owned(),
            })),
            None => anyhow::bail!(
                "the bus accepted register_session but returned no credential; refusing to \
                 continue with an asserted label while reporting a proven identity"
            ),
        },
        Err(e) => {
            let text = e.to_string();
            // Only "this server has no such tool" is a legacy bus. A refusal,
            // a database error or a dropped connection is not, and failing
            // closed is the point.
            if text.contains("not found") || text.contains("-32601") || text.contains("Method") {
                tracing::warn!(
                    "this bus does not issue session credentials; continuing with the label only"
                );
                Ok(None)
            } else {
                Err(anyhow::anyhow!(
                    "could not register this window's session: {text}"
                ))
            }
        }
    }
}

/// Reconnect a window we already hold a credential for. Its own credential
/// is the proof, so no agent token is involved; a rejected resume means the
/// window was revoked or expired and the caller must register afresh.
async fn resume_with(
    url: &str,
    prior: &SessionProof,
    session: &str,
) -> anyhow::Result<Option<SessionProof>> {
    let remote = connect_remote(url, &prior.token, session, Some(prior.epoch))
        .await
        .context("the stored session credential could not open a connection")?;
    let outcome = call_remote(&remote, "resume_session", json!({})).await;
    let _ = remote.cancel().await;
    let v =
        outcome.context("this window's session could not be resumed; it may have been revoked")?;
    let token = v["session_token"]
        .as_str()
        .context("resume_session returned no credential")?;
    Ok(Some(SessionProof {
        token: token.to_owned(),
        session_id: v["session_id"].as_str().unwrap_or_default().to_owned(),
        epoch: v["epoch"].as_i64().unwrap_or(1),
        expires_at: v["expires_at"].as_str().unwrap_or_default().to_owned(),
    }))
}

async fn establish(
    inputs: &Inputs,
    session: &str,
    existing_proof: Option<SessionProof>,
) -> anyhow::Result<(
    Resolved,
    String,
    String,
    Remote,
    Vec<Tool>,
    Option<String>,
    Option<SessionProof>,
)> {
    let resolved = context::resolve(inputs)?;
    // First connection: the agent token, with the label in a header, exactly
    // as any direct client would.
    let remote = connect_remote(&resolved.mcp_url, &resolved.token, session, None).await?;
    let me = match call_remote(&remote, "whoami", json!({})).await {
        Ok(me) => me,
        Err(e) => {
            let raw = e.to_string();
            let _ = remote.cancel().await;
            // The same wording a forwarded 401 gets, so a window started
            // with a rotated token says what to do rather than "the bus did
            // not accept the credential".
            if raw.contains("Auth required") || raw.contains("401") {
                anyhow::bail!(
                    "the bus rejected this window's credential — it has been revoked or \
                     rotated{}. Issue a new token (`ai-crew-sync admin token issue --save`) \
                     or select another approved profile",
                    resolved
                        .profile
                        .as_deref()
                        .map(|p| format!(" (profile '{p}')"))
                        .unwrap_or_default()
                );
            }
            anyhow::bail!("the bus did not accept the credential: {raw}");
        }
    };
    let agent = me["agent"].as_str().unwrap_or_default().to_owned();
    let team = me["team"].as_str().unwrap_or_default().to_owned();
    if let Some((exp_team, exp_agent)) = &resolved.expected
        && (&agent != exp_agent || &team != exp_team)
    {
        let _ = remote.cancel().await;
        anyhow::bail!(
            "profile '{}' expects {exp_agent}@{exp_team} but the token authenticates as \
             {agent}@{team}; fix the profile or its token entry",
            resolved.profile.as_deref().unwrap_or("?")
        );
    }

    // Then upgrade: talk with a credential that *proves* which window this
    // is. Reconnecting an existing window resumes it with the credential
    // already on disk — the bus refuses to hand a live window to whoever
    // holds the agent token — and a window with no stored credential
    // registers a new one.
    let stored = existing_proof;
    let proof = match &stored {
        Some(prior) => resume_with(&resolved.mcp_url, prior, session).await?,
        None => register_new(&remote, session).await?,
    };

    let (remote, tools, instructions) = match &proof {
        Some(proof) => {
            let _ = remote.cancel().await;
            let remote =
                connect_remote(&resolved.mcp_url, &proof.token, session, Some(proof.epoch))
                    .await
                    .context("the session credential could not open a connection")?;
            let tools = remote
                .list_all_tools()
                .await
                .context("could not list the bus's tools")?;
            let instructions = remote.peer_info().and_then(|i| i.instructions.clone());
            (remote, tools, instructions)
        }
        None => {
            let tools = remote
                .list_all_tools()
                .await
                .context("could not list the bus's tools")?;
            let instructions = remote.peer_info().and_then(|i| i.instructions.clone());
            (remote, tools, instructions)
        }
    };
    Ok((resolved, agent, team, remote, tools, instructions, proof))
}

/// Repository and branch of the project directory, for presence. Best
/// effort: a directory that is not a checkout simply reports neither.
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
    let branch = run(&["branch", "--show-current"]);
    (repo, branch)
}

impl Proxy {
    /// Resolve the conversation id, connect and verify. Never fails: a proxy
    /// that cannot connect still serves its local tools and explains itself.
    pub async fn start(opts: ProxyOptions) -> Self {
        let project_dir = opts
            .inputs
            .project_dir
            .clone()
            .or_else(|| std::env::var_os("CLAUDE_PROJECT_DIR").map(PathBuf::from))
            .or_else(|| std::env::current_dir().ok())
            .unwrap_or_else(|| PathBuf::from("."));

        let (host_id, binding) = if let Some(id) = opts
            .host_session
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            (Some(id.to_owned()), Binding::Explicit)
        } else if let Some(id) = std::env::var("CLAUDE_CODE_SESSION_ID")
            .ok()
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
        {
            (Some(id), Binding::ClaudeCode)
        } else {
            (None, Binding::Instance)
        };
        let session = match &host_id {
            Some(id) => session_for(id),
            None => random_session(),
        };

        let proxy = Self {
            state: Arc::new(RwLock::new(State {
                connected: None,
                disconnected_reason: None,
                session,
                binding,
                host_id,
                project: opts.project.clone(),
                role: opts.role.clone(),
                channel: opts.channel.clone(),
                generation: 0,
            })),
            in_flight: Arc::new(AtomicUsize::new(0)),
            switch: Arc::new(Mutex::new(())),
            opts: Arc::new(opts),
            project_dir,
        };
        let inputs = proxy.opts.inputs.clone();
        if let Err(e) = proxy.connect_with(&inputs, true).await {
            tracing::warn!(error = %e, "proxy started without a bus connection");
            proxy.state.write().await.disconnected_reason = Some(format!("{e:#}"));
        }
        proxy
    }

    /// Connect a context and make it current. Verifies before it touches
    /// state; on failure the previous context, if any, stays.
    /// `reuse_proof` is true when this is the *same* window reconnecting, so
    /// the credential on disk is its own and a resume is right. A profile
    /// switch passes false: that credential belongs to the identity being
    /// left behind, and resuming it would keep speaking as them.
    async fn connect_with(
        &self,
        inputs: &Inputs,
        reuse_proof: bool,
    ) -> anyhow::Result<Option<PreviousIdentity>> {
        let _guard = self.switch.lock().await;
        let (session, binding_key) = {
            let st = self.state.read().await;
            (
                st.session.clone(),
                st.host_id.clone().unwrap_or_else(|| st.session.clone()),
            )
        };
        // A credential already on disk for this conversation means this is a
        // reconnect: resume that window with its own proof rather than
        // asking the bus to hand it over. Only when the identity is
        // unchanged — see `reuse_proof`.
        let existing = reuse_proof
            .then(|| context::read_binding(&self.opts.state_dir, &binding_key))
            .flatten()
            .and_then(|b| match (b.session_token, b.session_id, b.epoch) {
                (Some(token), Some(session_id), Some(epoch)) if !token.is_empty() => {
                    Some(SessionProof {
                        token,
                        session_id,
                        epoch,
                        expires_at: b.expires_at.unwrap_or_default(),
                    })
                }
                _ => None,
            });
        let (resolved, agent, team, remote, tools, instructions, proof) =
            establish(inputs, &session, existing).await?;

        // A team switch would let one conversation carry another team's
        // transcript into this one. The credential was verified and is
        // dropped unused.
        {
            let st = self.state.read().await;
            if let Some(old) = &st.connected
                && old.team != team
            {
                let _ = remote.cancel().await;
                anyhow::bail!(
                    "this conversation is bound to team '{}'; the profile '{}' belongs to team \
                     '{team}'. Switching teams inside a conversation is not allowed — the \
                     transcript already holds '{}' material. Start a new conversation with \
                     that profile instead",
                    old.team,
                    resolved.profile.as_deref().unwrap_or("?"),
                    old.team
                );
            }
        }

        // Project and channel defaults from the project file, unless the
        // caller set them explicitly.
        {
            let mut st = self.state.write().await;
            if st.project.is_none() {
                st.project = resolved.project.clone();
            }
            if st.channel.is_none() {
                st.channel = resolved.channel.clone();
            }
        }

        // Barrier: cancel the old context's in-flight calls, wait for them to
        // leave, then swap. Nothing is replayed.
        let previous = {
            let old = {
                let mut st = self.state.write().await;
                st.connected.take()
            };
            match old {
                Some(old) => {
                    old.ct.cancel();
                    let started = std::time::Instant::now();
                    while self.in_flight.load(Ordering::SeqCst) > 0
                        && started.elapsed() < DRAIN_TIMEOUT
                    {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    let held = report_holdings(&old.remote, &old.agent, &old.team, &session).await;
                    // This window is no longer that identity, so its session
                    // is closed rather than left live for nobody: a session
                    // with no process behind it blocks its own label and
                    // keeps a credential valid for a day. Claims and locks
                    // are NOT transferred — they stay with the old identity
                    // and expire on their leases, which is what `held`
                    // reports back to the caller.
                    if old.proof.is_some() {
                        let _ = tokio::time::timeout(
                            EXIT_TIMEOUT,
                            call_remote(&old.remote, "revoke_session", json!({})),
                        )
                        .await;
                    }
                    // The old identity goes quiet in its own name.
                    let _ = tokio::time::timeout(
                        EXIT_TIMEOUT,
                        call_remote(
                            &old.remote,
                            "heartbeat",
                            json!({"status": "idle", "ttl_seconds": 30}),
                        ),
                    )
                    .await;
                    close_remote(old.remote).await;
                    Some(held)
                }
                None => None,
            }
        };

        let connected = Connected {
            resolved,
            agent,
            team,
            remote: Arc::new(remote),
            tools,
            remote_instructions: instructions,
            proof,
            ct: CancellationToken::new(),
        };
        {
            let mut st = self.state.write().await;
            st.connected = Some(connected);
            st.disconnected_reason = None;
            st.generation += 1;
        }
        self.heartbeat("active").await;
        self.write_binding().await;
        Ok(previous)
    }

    /// Presence for this window: status, repo/branch from the checkout, and
    /// the discovery labels. Best effort.
    async fn heartbeat(&self, status: &str) {
        let (remote, project, role) = {
            let st = self.state.read().await;
            let Some(c) = &st.connected else { return };
            (c.remote.clone(), st.project.clone(), st.role.clone())
        };
        let (repo, branch) = git_place(&self.project_dir);
        let mut args = json!({"status": status, "ttl_seconds": PRESENCE_TTL_SECS});
        if let Some(r) = repo {
            args["repo"] = Value::String(r);
        }
        if let Some(b) = branch {
            args["branch"] = Value::String(b);
        }
        // Omitted keeps, "" clears: send exactly what this window knows.
        args["project"] = Value::String(project.unwrap_or_default());
        args["role"] = Value::String(role.unwrap_or_default());
        if let Err(e) = call_remote(&remote, "heartbeat", args).await {
            tracing::warn!(error = %e, "heartbeat failed");
        }
    }

    /// Record this instance's binding so lifecycle hooks of the same
    /// conversation can find the session and labels. Keyed by the
    /// conversation id when there is one (hooks know it), by the session
    /// otherwise (nothing else can look it up, but `session_status` can
    /// still say where it is).
    async fn write_binding(&self) {
        let st = self.state.read().await;
        // Keyed by the conversation id when the host gives one — that is what
        // a hook of the same conversation can look up — and by the session
        // label otherwise, where nothing else can find it anyway.
        let key = st.host_id.clone().unwrap_or_else(|| st.session.clone());
        // The credential goes in here, which is why the file is 0600 inside a
        // 0700 directory and why `context hook` is the only thing that reads
        // it. It never reaches a tool result, a log or argv.
        let record = json!({
            "host_id_present": st.host_id.is_some(),
            "binding": st.binding,
            "session": st.session,
            "project": st.project,
            "role": st.role,
            "channel": st.channel,
            "profile": st.connected.as_ref().and_then(|c| c.resolved.profile.clone()),
            "agent": st.connected.as_ref().map(|c| c.agent.clone()),
            "team": st.connected.as_ref().map(|c| c.team.clone()),
            "mcp_url": st.connected.as_ref().map(|c| c.resolved.mcp_url.clone()),
            "session_token": st.connected.as_ref().and_then(|c| c.proof.as_ref().map(|p| p.token.clone())),
            "session_id": st.connected.as_ref().and_then(|c| c.proof.as_ref().map(|p| p.session_id.clone())),
            // Hooks send this epoch, so they are fenced with their proxy
            // rather than fencing it: a hook never bumps it.
            "epoch": st.connected.as_ref().and_then(|c| c.proof.as_ref().map(|p| p.epoch)),
            "expires_at": st.connected.as_ref().and_then(|c| c.proof.as_ref().map(|p| p.expires_at.clone())),
            "proxy_pid": std::process::id(),
            "updated_at": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        });
        let path = context::binding_path(&self.opts.state_dir, &key);
        if let Err(e) = context::write_binding_file(&path, &record.to_string()) {
            tracing::warn!(error = %e, "could not write the session binding");
        }
    }

    async fn status(&self) -> Status {
        let st = self.state.read().await;
        let c = st.connected.as_ref();
        Status {
            connected: c.is_some(),
            error: st.disconnected_reason.clone(),
            agent: c.map(|c| c.agent.clone()),
            team: c.map(|c| c.team.clone()),
            session: st.session.clone(),
            address: c.map(|c| format!("{}/{}", c.agent, st.session)),
            project: st.project.clone(),
            role: st.role.clone(),
            channel: st.channel.clone(),
            profile: c.and_then(|c| c.resolved.profile.clone()),
            binding: st.binding,
            project_root: c
                .and_then(|c| c.resolved.project_root.as_ref())
                .map(|p| p.display().to_string()),
            bus: c.map(|c| c.resolved.mcp_url.clone()),
        }
    }

    async fn configure(&self, args: ConfigureArgs) -> anyhow::Result<ConfigureResult> {
        // Labels first, before anything is committed anywhere.
        let mut previous = None;
        // Validated like the server validates them, and staged rather than
        // committed: a call that also switches profile must leave the
        // previous context *entirely* intact when the switch fails, labels
        // included.
        let staged_role = match args.role {
            Some(v) => Some(check_label("role", &v).map_err(|e| anyhow::anyhow!("{}", e.message))?),
            None => None,
        };
        let staged_project = match args.project {
            Some(v) => {
                Some(check_label("project", &v).map_err(|e| anyhow::anyhow!("{}", e.message))?)
            }
            None => None,
        };
        let staged_channel = match args.channel {
            Some(v) => {
                Some(check_label("channel", &v).map_err(|e| anyhow::anyhow!("{}", e.message))?)
            }
            None => None,
        };

        if let Some(profile) = args
            .profile
            .map(|p| p.trim().to_owned())
            .filter(|p| !p.is_empty())
        {
            let mut inputs = self.opts.inputs.clone();
            inputs.profile = Some(profile);
            // A profile switch is a switch of credentials: explicit ones from
            // the environment would otherwise win and make the call a no-op.
            inputs.explicit_token = None;
            inputs.explicit_url = None;
            previous = self.connect_with(&inputs, false).await?;
        }
        // Only now, with the switch (if any) verified and committed.
        {
            let mut st = self.state.write().await;
            if let Some(role) = staged_role {
                st.role = role;
            }
            if let Some(project) = staged_project {
                st.project = project;
            }
            if let Some(channel) = staged_channel {
                st.channel = channel;
            }
        }
        self.heartbeat("active").await;
        self.write_binding().await;
        Ok(ConfigureResult {
            status: self.status().await,
            previous,
        })
    }

    /// Bind to the conversation a request says it belongs to. First id seen
    /// becomes the binding when there was none; a different id later means
    /// the host multiplexes conversations over one process, which this proxy
    /// does not support and says so rather than mixing them.
    async fn observe_meta(&self, meta: &rmcp::model::RequestMetaObject) -> Result<(), ErrorData> {
        let thread = meta
            .0
            .0
            .get("threadId")
            .or_else(|| meta.0.0.get("sessionId"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let Some(thread) = thread else { return Ok(()) };
        let (bound, current) = {
            let st = self.state.read().await;
            (st.host_id.clone(), st.binding)
        };
        match bound {
            Some(id) if id == thread => Ok(()),
            Some(_) if current == Binding::RequestMeta => Err(ErrorData::invalid_request(
                "this proxy instance is bound to another conversation; a second one is using \
                 the same MCP process, which is not supported. Configure the host to start \
                 one `ai-crew-sync mcp proxy` per conversation",
                None,
            )),
            // Bound by environment or flag: the request's id is informative
            // only, the operator's binding wins.
            Some(_) => Ok(()),
            None => self.rebind(thread.to_owned()).await.map_err(|e| {
                ErrorData::internal_error(format!("could not bind the conversation: {e:#}"), None)
            }),
        }
    }

    /// Adopt a conversation id discovered on the wire: derive the stable
    /// session and reconnect so every forwarded call, this one included,
    /// carries it.
    async fn rebind(&self, host_id: String) -> anyhow::Result<()> {
        // Staged, not committed: if the new session cannot connect, the
        // previous one must keep serving. Advertising the new identity over
        // the old connection is how a window ends up reporting one session
        // and writing as another.
        let previous = {
            let st = self.state.read().await;
            (st.host_id.clone(), st.binding, st.session.clone())
        };
        {
            let mut st = self.state.write().await;
            st.host_id = Some(host_id.clone());
            st.binding = Binding::RequestMeta;
            st.session = session_for(&host_id);
        }
        let inputs = {
            let st = self.state.read().await;
            match &st.connected {
                Some(c) => {
                    let mut i = self.opts.inputs.clone();
                    i.profile = c.resolved.profile.clone();
                    i
                }
                None => self.opts.inputs.clone(),
            }
        };
        match self.connect_with(&inputs, true).await {
            Ok(_) => Ok(()),
            Err(e) => {
                let mut st = self.state.write().await;
                (st.host_id, st.binding, st.session) = previous;
                st.disconnected_reason = Some(format!("{e:#}"));
                Err(e)
            }
        }
    }

    /// Forward one call to the connected context, racing the host's own
    /// cancellation and the context's replacement.
    /// Spool the references a fetch returned, fsync, then confirm them.
    ///
    /// Everything here is best effort in one direction only: a reference
    /// that cannot be written to disk is **not** confirmed, so the bus keeps
    /// offering it. The model still sees it in this turn — it is in the
    /// result either way — but nothing claims durability that does not
    /// exist.
    async fn spool_and_confirm(
        &self,
        result: CallToolResult,
        host_ct: CancellationToken,
    ) -> CallToolResult {
        let Some(structured) = result.structured_content.clone() else {
            return result;
        };
        let session = self.state.read().await.session.clone();
        let path = crate::spool::spool_path(&self.opts.state_dir, &session);

        let mut entries: Vec<crate::spool::Entry> = structured
            .get("references")
            .and_then(|v| v.as_array())
            .map(|refs| {
                refs.iter()
                    .filter_map(|r| {
                        Some(crate::spool::Entry {
                            delivery_id: r.get("delivery_id")?.as_str()?.to_owned(),
                            message_id: r.get("message_id")?.as_str()?.to_owned(),
                            conversation_id: r
                                .get("conversation_id")
                                .and_then(|v| v.as_str())
                                .unwrap_or_default()
                                .to_owned(),
                            seq: r.get("seq").and_then(|v| v.as_i64()).unwrap_or(0),
                            from_address: r
                                .get("from_address")
                                .and_then(|v| v.as_str())
                                .unwrap_or_default()
                                .to_owned(),
                            created_at: r
                                .get("created_at")
                                .and_then(|v| v.as_str())
                                .unwrap_or_default()
                                .to_owned(),
                            confirmed: false,
                            spooled_at: chrono::Utc::now().to_rfc3339(),
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        // Anything a previous process spooled and never confirmed goes in
        // the same confirmation: that is what an interrupted delivery looks
        // like from here.
        let mut held = crate::spool::read(&path);
        let spooled = match crate::spool::append(&path, &entries) {
            Ok(written) => written,
            Err(e) => {
                tracing::warn!(error = %e, "could not spool inbox references; not confirming");
                return result;
            }
        };
        held.extend(spooled.iter().cloned());
        entries.clear();
        let to_confirm = crate::spool::unconfirmed(&held);
        if to_confirm.is_empty() {
            return result;
        }

        let mut params = rmcp::model::JsonObject::new();
        params.insert(
            "delivery_ids".into(),
            Value::Array(
                to_confirm
                    .iter()
                    .map(|id| Value::String(id.clone()))
                    .collect(),
            ),
        );
        let confirm =
            CallToolRequestParams::new("confirm_inbox_delivery".to_string()).with_arguments(params);
        match self.forward(confirm, host_ct).await {
            Ok(result) => {
                // The bus says how many it actually committed. A stale
                // epoch, or a partial result, answers successfully and
                // writes nothing; marking the spool confirmed anyway would
                // throw away the only evidence that they are still owed.
                let committed = result
                    .structured_content
                    .as_ref()
                    .and_then(|v| v.get("confirmed"))
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                if committed as usize != to_confirm.len() {
                    tracing::warn!(
                        sent = to_confirm.len(),
                        committed,
                        "the bus confirmed fewer references than were sent; keeping the rest \
                         in the spool"
                    );
                    return result;
                }
                for entry in held.iter_mut() {
                    if to_confirm.contains(&entry.delivery_id) {
                        entry.confirmed = true;
                    }
                }
                if let Err(e) = crate::spool::rewrite(&path, &held) {
                    // The bus has the truth; this only costs a repeated
                    // confirmation next time, which is a no-op there.
                    tracing::warn!(error = %e, "could not compact the inbox spool");
                }
            }
            Err(e) => tracing::warn!(error = %e, "could not confirm inbox delivery"),
        }
        result
    }

    async fn forward(
        &self,
        request: CallToolRequestParams,
        host_ct: CancellationToken,
    ) -> Result<CallToolResult, ErrorData> {
        let (remote, ct, generation, _guard) = {
            let st = self.state.read().await;
            let Some(c) = &st.connected else {
                return Err(ErrorData::invalid_request(
                    format!(
                        "not connected to the bus: {}. Call {CONFIGURE_TOOL} with an approved \
                         profile, or fix the local configuration and start a new conversation",
                        st.disconnected_reason
                            .as_deref()
                            .unwrap_or("no profile resolved")
                    ),
                    None,
                ));
            };
            (
                c.remote.clone(),
                c.ct.clone(),
                st.generation,
                InFlight::enter(&self.in_flight),
            )
        };
        let name = request.name.to_string();
        let profile = {
            let st = self.state.read().await;
            st.connected
                .as_ref()
                .and_then(|c| c.resolved.profile.clone())
        };
        let outcome = tokio::select! {
            r = remote.call_tool(request) => r.map_err(|e| {
                let raw = e.to_string();
                // The transport reports a rejected bearer as "Auth required",
                // which says nothing about what to do. This is what a revoked
                // or rotated token looks like from here.
                if raw.contains("Auth required") || raw.contains("401") {
                    self.mark_unauthorized(&profile);
                    ErrorData::invalid_request(
                        format!(
                            "{name}: the bus rejected this window's credential — it has been \
                             revoked or rotated{}. Issue a new token (`ai-crew-sync admin \
                             token issue --save`) and call {CONFIGURE_TOOL} with an approved \
                             profile; nothing was sent",
                            profile
                                .as_deref()
                                .map(|p| format!(" (profile '{p}')"))
                                .unwrap_or_default()
                        ),
                        None,
                    )
                } else {
                    ErrorData::internal_error(format!("{name}: {raw}"), None)
                }
            }),
            _ = ct.cancelled() => Err(ErrorData::invalid_request(
                format!(
                    "{name} was cancelled: this window switched credentials while the call was \
                     in flight (generation {generation}). Nothing was replayed; call again if \
                     it is still wanted, as the new identity"
                ),
                None,
            )),
            _ = host_ct.cancelled() => Err(ErrorData::invalid_request(
                format!("{name} was cancelled by the client"),
                None,
            )),
        };
        outcome
    }

    /// Record that the bus refused this window's credential, so
    /// `session_status` and the next `initialize` say so instead of
    /// claiming a healthy connection. Non-blocking: a busy lock means the
    /// next call reports it.
    fn mark_unauthorized(&self, profile: &Option<String>) {
        if let Ok(mut st) = self.state.try_write() {
            st.disconnected_reason = Some(format!(
                "the bus rejected the credential{} (revoked or rotated)",
                profile
                    .as_deref()
                    .map(|p| format!(" of profile '{p}'"))
                    .unwrap_or_default()
            ));
        }
    }

    /// The channel this window posts to when a message names none: its own
    /// `channel`, else the channel named after its project.
    async fn default_channel(&self) -> Option<String> {
        let st = self.state.read().await;
        st.channel.clone().or_else(|| st.project.clone())
    }

    fn instructions(&self, st: &State) -> String {
        let mut lines = Vec::new();
        match &st.connected {
            Some(c) => {
                lines.push(format!(
                    "[ai-crew-sync] You are agent '{}' on team '{}', in session '{}'. Teammates \
                     reach exactly this window at '{}/{}'.",
                    c.agent, c.team, st.session, c.agent, st.session
                ));
                lines.push(format!(
                    "- project: {}, role: {}, default channel: {}. Change them with \
                     {CONFIGURE_TOOL}; see them with {STATUS_TOOL}. Find teammates' windows \
                     with list_sessions.",
                    st.project.as_deref().unwrap_or("(none — set it)"),
                    st.role.as_deref().unwrap_or("(none — set it)"),
                    st.channel
                        .as_deref()
                        .or(st.project.as_deref())
                        .unwrap_or("(none)"),
                ));
                lines.push(
                    "- Nothing is pushed into an idle turn: call read_messages or wait_for_updates \
                     to receive what teammates sent."
                        .to_owned(),
                );
                if let Some(remote) = &c.remote_instructions {
                    lines.push(String::new());
                    lines.push(remote.clone());
                }
            }
            None => {
                lines.push(format!(
                    "[ai-crew-sync] Not connected to the team bus: {}. Only {CONFIGURE_TOOL} and \
                     {STATUS_TOOL} are available until a locally approved profile connects.",
                    st.disconnected_reason
                        .as_deref()
                        .unwrap_or("no profile resolved")
                ));
            }
        }
        lines.join("\n")
    }

    /// Periodic presence, until cancelled.
    pub async fn keepalive(self, ct: CancellationToken) {
        loop {
            tokio::select! {
                _ = ct.cancelled() => return,
                _ = tokio::time::sleep(KEEPALIVE_EVERY) => self.heartbeat("active").await,
            }
        }
    }

    /// Go quiet on the bus; the host is closing this window.
    pub async fn shutdown(&self) {
        let remote = {
            let st = self.state.read().await;
            st.connected.as_ref().map(|c| c.remote.clone())
        };
        if let Some(remote) = remote {
            let _ = tokio::time::timeout(
                EXIT_TIMEOUT,
                call_remote(
                    &remote,
                    "heartbeat",
                    json!({"status": "idle", "ttl_seconds": 120}),
                ),
            )
            .await;
            close_remote(remote).await;
        }
        // The credential stays on disk so a restart of this conversation can
        // resume the window; the record is only stamped as closed, and only
        // if it still describes this instance.
        self.mark_closed().await;
    }

    /// Mark this window closed, **only if the record still describes this
    /// instance**.
    ///
    /// Two things were wrong with clearing it unconditionally. A successor
    /// proxy for the same conversation may already have replaced the record,
    /// and wiping it would cut the live window's hooks off from their own
    /// credential. And the credential itself has to stay: a restart of this
    /// conversation resumes with it, which is the only way back in — the bus
    /// refuses to hand a live session to whoever holds the agent token, and
    /// rightly so. It is a 0600 file scoped to one window and it expires on
    /// its own.
    async fn mark_closed(&self) {
        let (key, mine) = {
            let st = self.state.read().await;
            let key = st.host_id.clone().unwrap_or_else(|| st.session.clone());
            let mine = st
                .connected
                .as_ref()
                .and_then(|c| c.proof.as_ref().map(|p| (p.session_id.clone(), p.epoch)));
            (key, mine)
        };
        let path = context::binding_path(&self.opts.state_dir, &key);
        let Ok(text) = std::fs::read_to_string(&path) else {
            return;
        };
        let Ok(mut value) = serde_json::from_str::<Value>(&text) else {
            return;
        };
        // Ownership check: a record whose session or epoch has moved on
        // belongs to the instance that replaced us.
        if let Some((session_id, epoch)) = mine {
            let same = value["session_id"].as_str() == Some(session_id.as_str())
                && value["epoch"].as_i64() == Some(epoch);
            if !same {
                tracing::debug!(
                    binding = %key,
                    "another instance owns this binding now; leaving it alone"
                );
                return;
            }
        }
        if let Some(map) = value.as_object_mut() {
            map.insert("closed_at".into(), json!(chrono::Utc::now().to_rfc3339()));
        }
        if let Err(e) = context::write_binding_file(&path, &value.to_string()) {
            tracing::warn!(error = %e, binding = %key, "could not mark the binding closed");
        }
    }
}

/// Close a remote connection we may share with an in-flight call. Sole
/// owner: cancel cleanly. Otherwise the last holder drops it, and dropping a
/// `RunningService` closes it as well.
async fn close_remote(remote: Arc<Remote>) {
    if let Ok(owned) = Arc::try_unwrap(remote) {
        let _ = owned.cancel().await;
    }
}

/// What an identity still holds on the bus, read before it is set aside.
async fn report_holdings(
    remote: &Remote,
    agent: &str,
    team: &str,
    session: &str,
) -> PreviousIdentity {
    let open_claims = call_remote(remote, "list_tasks", json!({"mine_only": true}))
        .await
        .ok()
        .and_then(|v| v["tasks"].as_array().cloned())
        .unwrap_or_default()
        .iter()
        .filter(|t| t["status"] == "claimed")
        .filter_map(|t| t["key"].as_str().map(str::to_owned))
        .collect();
    let held_locks = call_remote(remote, "list_locks", json!({}))
        .await
        .ok()
        .and_then(|v| v["locks"].as_array().cloned())
        .unwrap_or_default()
        .iter()
        // Agent AND session: locks are session-scoped, so a sibling window's
        // lock is not this one's to report as left behind.
        .filter(|l| {
            l["holder"] == agent && l["holder_session"].as_str().unwrap_or_default() == session
        })
        .filter_map(|l| l["name"].as_str().map(str::to_owned))
        .collect();
    PreviousIdentity {
        agent: agent.to_owned(),
        team: team.to_owned(),
        session: session.to_owned(),
        open_claims,
        held_locks,
    }
}

fn tool_error(msg: String) -> CallToolResult {
    CallToolResult::error(vec![rmcp::model::ContentBlock::text(msg)])
}

impl ServerHandler for Proxy {
    fn get_info(&self) -> ServerConfig {
        let mut info = ServerConfig::new(ServerCapabilities::builder().enable_tools().build());
        // `get_info` is synchronous; the state lock is uncontended at
        // initialize time, and a contended read simply yields the
        // disconnected wording until the next call.
        let text = match self.state.try_read() {
            Ok(st) => self.instructions(&st),
            Err(_) => format!("[ai-crew-sync] initialising; call {STATUS_TOOL} for details."),
        };
        info.instructions = Some(text);
        info
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let mut tools = local_tools();
        if let Some(c) = &self.state.read().await.connected {
            tools.extend(c.tools.iter().cloned());
        }
        Ok(ListToolsResult::with_all_items(tools))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        // The transport lifts `_meta` out of the params into the context.
        self.observe_meta(&context.meta).await?;
        match request.name.as_ref() {
            STATUS_TOOL => {
                let status = self.status().await;
                let value = serde_json::to_value(status)
                    .map_err(|e| ErrorData::internal_error(e.to_string(), None))?;
                Ok(CallToolResult::structured(value).into())
            }
            CONFIGURE_TOOL => {
                let args: ConfigureArgs = match request.arguments {
                    Some(map) => serde_json::from_value(Value::Object(map))
                        .map_err(|e| ErrorData::invalid_params(e.to_string(), None))?,
                    None => ConfigureArgs::default(),
                };
                match self.configure(args).await {
                    Ok(result) => {
                        let value = serde_json::to_value(result)
                            .map_err(|e| ErrorData::internal_error(e.to_string(), None))?;
                        Ok(CallToolResult::structured(value).into())
                    }
                    // The model must read this one, so it is a tool error
                    // rather than a protocol error.
                    Err(e) => Ok(tool_error(format!("{e:#}")).into()),
                }
            }
            // Delivery has to mean something. The bus hands over references
            // and records nothing; this writes them to disk, fsyncs, and
            // only then tells the bus they are held. A crash in between
            // costs a redelivery, which is idempotent by design.
            "fetch_conversation_inbox" => {
                let result = self.forward(request, context.ct.clone()).await?;
                Ok(self.spool_and_confirm(result, context.ct).await.into())
            }
            // A message with no channel and no recipient goes to this
            // window's channel. The state was decorative until now: the
            // instructions and session_status promised a default the bus
            // never saw.
            "post_message" => {
                let mut request = request;
                if let Some(channel) = self.default_channel().await {
                    let args = request.arguments.get_or_insert_with(Default::default);
                    let addressed = args.contains_key("channel") || args.contains_key("to");
                    if !addressed {
                        args.insert("channel".into(), Value::String(channel));
                    }
                }
                Ok(self.forward(request, context.ct).await?.into())
            }
            _ => Ok(self.forward(request, context.ct).await?.into()),
        }
    }
}

/// Run the proxy over stdio until the host closes the pipe.
pub async fn run(opts: ProxyOptions) -> anyhow::Result<()> {
    let proxy = Proxy::start(opts).await;
    let ct = CancellationToken::new();
    let keepalive = tokio::spawn(proxy.clone().keepalive(ct.child_token()));

    let running = proxy
        .clone()
        .serve(rmcp::transport::stdio())
        .await
        .context("MCP initialize over stdio failed")?;
    let quit = running.waiting().await;
    tracing::debug!(?quit, "host closed the connection");

    ct.cancel();
    let _ = keepalive.await;
    proxy.shutdown().await;
    Ok(())
}
