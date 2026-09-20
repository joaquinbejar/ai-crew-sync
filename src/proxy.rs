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
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;

use crate::context::{self, Inputs, Resolved};

/// Prefix of a session label the proxy mints. Opaque on purpose: the label
/// is an address, `project`/`role` are the human-facing part.
pub const SESSION_PREFIX: &str = "s-";
/// Hex characters after the prefix: 48 bits, collision-free in any team.
const SESSION_HEX: usize = 12;

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
/// unrelated to repository, role or pid.
pub fn session_for(host_id: &str) -> String {
    let digest = Sha256::digest(host_id.trim().as_bytes());
    format!("{SESSION_PREFIX}{}", &hex::encode(digest)[..SESSION_HEX])
}

fn random_session() -> String {
    let raw = crate::auth::generate_token();
    format!(
        "{SESSION_PREFIX}{}",
        &raw[crate::auth::TOKEN_PREFIX.len()..crate::auth::TOKEN_PREFIX.len() + SESSION_HEX]
    )
}

/// The verified, connected context of this instance.
struct Connected {
    resolved: Resolved,
    agent: String,
    team: String,
    remote: Arc<Remote>,
    tools: Vec<Tool>,
    remote_instructions: Option<String>,
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

async fn connect_remote(resolved: &Resolved, session: &str) -> anyhow::Result<Remote> {
    let mut config = StreamableHttpClientTransportConfig::with_uri(resolved.mcp_url.clone());
    config.auth_header = Some(resolved.token.clone());
    config.allow_stateless = true;
    config.custom_headers.insert(
        crate::auth::SESSION_HEADER.parse()?,
        session
            .parse()
            .context("session label is not a valid header value")?,
    );
    let transport = StreamableHttpClientTransport::from_config(config);
    let remote = rmcp::model::ClientConfig::default()
        .serve(transport)
        .await
        .with_context(|| format!("could not connect to {}", resolved.mcp_url))?;
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
async fn establish(
    inputs: &Inputs,
    session: &str,
) -> anyhow::Result<(Resolved, String, String, Remote, Vec<Tool>, Option<String>)> {
    let resolved = context::resolve(inputs)?;
    let remote = connect_remote(&resolved, session).await?;
    let me = call_remote(&remote, "whoami", json!({}))
        .await
        .context("the bus did not accept the credential")?;
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
    let tools = remote
        .list_all_tools()
        .await
        .context("could not list the bus's tools")?;
    let instructions = remote
        .peer_info()
        .and_then(|info| info.instructions.clone());
    Ok((resolved, agent, team, remote, tools, instructions))
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
        if let Err(e) = proxy.connect_with(&inputs).await {
            tracing::warn!(error = %e, "proxy started without a bus connection");
            proxy.state.write().await.disconnected_reason = Some(format!("{e:#}"));
        }
        proxy
    }

    /// Connect a context and make it current. Verifies before it touches
    /// state; on failure the previous context, if any, stays.
    async fn connect_with(&self, inputs: &Inputs) -> anyhow::Result<Option<PreviousIdentity>> {
        let _guard = self.switch.lock().await;
        let session = self.state.read().await.session.clone();
        let (resolved, agent, team, remote, tools, instructions) =
            establish(inputs, &session).await?;

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
                    // The old identity goes quiet in its own name; its
                    // claims and locks are left to their leases.
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
        let key = st
            .host_id
            .as_deref()
            .map(|id| hex::encode(Sha256::digest(id.as_bytes())))
            .unwrap_or_else(|| st.session.clone());
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
            "proxy_pid": std::process::id(),
            "updated_at": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        });
        let path = self
            .opts
            .state_dir
            .join("sessions")
            .join(format!("{key}.json"));
        if let Err(e) = context::write_private(&path, &record.to_string()) {
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
        let mut previous = None;
        // Metadata first: cheap, local, and valid whether or not a profile
        // change follows.
        {
            let mut st = self.state.write().await;
            if let Some(role) = args.role {
                st.role = Some(role.trim().to_lowercase()).filter(|s| !s.is_empty());
            }
            if let Some(project) = args.project {
                st.project = Some(project.trim().to_lowercase()).filter(|s| !s.is_empty());
            }
            if let Some(channel) = args.channel {
                st.channel = Some(channel.trim().to_lowercase()).filter(|s| !s.is_empty());
            }
        }
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
            previous = self.connect_with(&inputs).await?;
        } else {
            self.heartbeat("active").await;
            self.write_binding().await;
        }
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
        match self.connect_with(&inputs).await {
            Ok(_) => Ok(()),
            Err(e) => {
                self.state.write().await.disconnected_reason = Some(format!("{e:#}"));
                Err(e)
            }
        }
    }

    /// Forward one call to the connected context, racing the host's own
    /// cancellation and the context's replacement.
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
        .filter(|l| l["holder"] == agent)
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
