use rmcp::{
    ErrorData, Json, handler::server::wrapper::Parameters, service::RequestContext, tool,
    tool_router,
};
use schemars::JsonSchema;
use serde::Deserialize;

use super::{Bus, auth_of};
use crate::{
    model::{AgentInfo, AgentList, SessionList},
    store::presence,
};

#[derive(Debug, Deserialize, JsonSchema)]
pub struct HeartbeatArgs {
    /// One of "active", "idle", "busy", "blocked". Defaults to "active".
    #[serde(default)]
    pub status: Option<String>,
    /// Repository you are working in, e.g. "acme/api".
    #[serde(default)]
    pub repo: Option<String>,
    /// Branch you are on.
    #[serde(default)]
    pub branch: Option<String>,
    /// Short description of what you are doing right now, e.g. "rewriting the
    /// token refresh flow". This is what teammates see in list_agents.
    #[serde(default)]
    pub activity: Option<String>,
    /// Discovery label: the logical project this session works on, usually
    /// the repository name (e.g. "market-data"). One lower-case word. Lets
    /// teammates find this window with list_sessions, and names the channel
    /// this session posts to by default. Omit to keep, "" to clear.
    #[serde(default)]
    pub project: Option<String>,
    /// Discovery label: what this session does on that project —
    /// "implementation", "design", "review", … One lower-case word. Several
    /// sessions may share a role; each keeps its own address. Omit to keep,
    /// "" to clear.
    #[serde(default)]
    pub role: Option<String>,
    /// How long this presence stays valid before you are shown as offline.
    /// Defaults to 600 (10 minutes).
    #[serde(default)]
    pub ttl_seconds: Option<i64>,
    /// What you observe about the model's turn in this window: "attending"
    /// (a turn is running), "waiting" (blocked in a bus wait) or "idle" (the
    /// turn ended). Hooks and the proxy report it for you; set it only when
    /// you know. "" clears it to unknown; omit to keep. Teammates read it in
    /// list_sessions to tell a window that will read their message soon
    /// from one that is merely connected.
    #[serde(default)]
    pub attention: Option<String>,
    /// True when this heartbeat accompanies a tool call by the window's
    /// model: stamps `last_tool_at`. Proxies set it; keepalives must not.
    #[serde(default)]
    pub tool_activity: bool,
    /// How this window can be woken while idle, as the mechanism itself
    /// announces it, e.g. "claude-watcher". One lower-case word. "" withdraws
    /// it; omit to keep. Descriptive only, never proof a wake will work.
    #[serde(default)]
    pub wake: Option<String>,
    /// How long the `wake` announcement stands unless renewed (30-86400,
    /// default 3600).
    #[serde(default)]
    pub wake_ttl_seconds: Option<i64>,
    /// How many milliseconds ago `attention` was observed, when it is
    /// reported late. The bus keeps only the newest observation, so a late
    /// report never overwrites a newer one. Omit for "now".
    #[serde(default)]
    pub attention_observed_ms_ago: Option<i64>,
    /// An opaque marker the wake mechanism chooses (at most 128 bytes),
    /// stored with its `wake` announcement; a withdrawal (`wake: ""`) with
    /// the same token clears only its own announcement.
    #[serde(default)]
    pub wake_token: Option<String>,
    /// Renew the presence lease and labels but leave `status` as it is.
    /// For a process that is merely alive (a proxy keepalive), so it never
    /// overwrites what the window's hooks or model said.
    #[serde(default)]
    pub refresh_only: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListSessionsArgs {
    /// Only sessions that published this project label.
    #[serde(default)]
    pub project: Option<String>,
    /// Only sessions that published this role label.
    #[serde(default)]
    pub role: Option<String>,
    /// Only sessions whose presence has not expired.
    #[serde(default)]
    pub online_only: bool,
    /// Most sessions to return (1-1000, default 200). Narrow with `project`
    /// or `role` rather than raising it.
    #[serde(default)]
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListAgentsArgs {
    /// Only return agents whose presence has not expired.
    #[serde(default)]
    pub online_only: bool,
}

#[tool_router(router = presence_router, vis = "pub")]
impl Bus {
    #[tool(
        description = "Publish what you are currently working on so teammates' agents can \
                       see it. Call this when you start a piece of work and whenever the \
                       focus changes. Omitted fields keep their previous value."
    )]
    async fn heartbeat(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<HeartbeatArgs>,
    ) -> Result<Json<AgentInfo>, ErrorData> {
        let auth = auth_of(&ctx)?;
        let input = presence::HeartbeatInput {
            status: args.status,
            repo: args.repo,
            branch: args.branch,
            activity: args.activity,
            project: args.project,
            role: args.role,
            ttl_seconds: args.ttl_seconds,
            attention: args.attention,
            tool_activity: args.tool_activity,
            wake: args.wake,
            wake_ttl_seconds: args.wake_ttl_seconds,
            attention_observed_ms_ago: args.attention_observed_ms_ago,
            wake_token: args.wake_token,
            refresh_only: args.refresh_only,
        };
        Ok(Json(presence::heartbeat(&self.db, &auth, input).await?))
    }

    #[tool(
        description = "See who else is on the bus, whether they are online, and what each \
                       one is working on. Useful before claiming work or asking a question."
    )]
    async fn list_agents(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<ListAgentsArgs>,
    ) -> Result<Json<AgentList>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(
            presence::list_agents(&self.db, &auth, args.online_only).await?,
        ))
    }

    #[tool(
        description = "Find the window to talk to. Lists every session in your team \
                       with its address (`agent/session`), project and role, so you can \
                       reach 'the design window of market-data' rather than guessing. \
                       Filter by project and/or role; several sessions may share both \
                       (two reviewers), and each keeps its own address — pick one, never \
                       broadcast a private instruction to all of them. Use the address in \
                       post_message `to` or ask_agent `to`. CHECK `exact` FIRST: it is \
                       false for the shared session, whose address is the bare agent name \
                       and reaches EVERY window that agent has, so it is never a private \
                       target. Labels are what a session said about itself, not proof of \
                       anything."
    )]
    async fn list_sessions(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<ListSessionsArgs>,
    ) -> Result<Json<SessionList>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(
            presence::list_sessions(
                &self.db,
                &auth,
                presence::SessionFilter {
                    project: args.project,
                    role: args.role,
                    online_only: args.online_only,
                    limit: args.limit,
                },
            )
            .await?,
        ))
    }
}
