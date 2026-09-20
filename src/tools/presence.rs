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
        description = "Find the exact window to talk to. Lists every session in your team \
                       with its address (`agent/session`), project and role, so you can \
                       reach 'the design window of market-data' rather than guessing. \
                       Filter by project and/or role; several sessions may share both \
                       (two reviewers), and each keeps its own address — pick one, never \
                       broadcast a private instruction to all of them. Use the address in \
                       post_message `to` or ask_agent `to`. Labels are what a session said \
                       about itself, not proof of anything."
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
