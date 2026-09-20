pub mod attachments;
pub mod events;
pub mod locks;
pub mod messaging;
pub mod notes;
pub mod presence;
pub mod tasks;

use rmcp::{
    ErrorData, Json, ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{ServerCapabilities, ServerConfig},
    service::RequestContext,
    tool, tool_handler, tool_router,
};
use schemars::JsonSchema;
use serde::Deserialize;
use sqlx::PgPool;

use crate::{
    auth::AuthCtx,
    events::EventHub,
    model::{DigestResult, SessionCredential, WhoAmI},
    store,
};

pub const INSTRUCTIONS: &str = r#"
Shared coordination bus for a team of AI coding agents. Every agent in the
team is connected to the same bus, so anything you write here is visible to your
teammates' agents, and anything they write is visible to you.

Identity is taken from your bearer token; you never pass your own name as an
argument. Call `whoami` once at the start of a session to learn your handle and
see whether anything is waiting for you.

Four capabilities:

- Messaging. `post_message` to a `channel` (broadcast to the whole team) or to a
  single agent via `to` (direct message). `read_messages` returns only what you
  have not seen yet by default, and advances your read cursor.
- Task coordination. Before starting shared work, `claim_task` so two agents do
  not do the same job twice. Claims hold a lease that expires, so call
  `renew_task_lease` on long jobs and `complete_task` or `release_task` when you
  stop. An expired lease can be taken over by anyone. Tasks can depend on other
  tasks (`depends_on` at creation); blocked tasks cannot be claimed until their
  dependencies are done, and `claim_next_task` skips them.
- Locks. `acquire_lock` before touching a contended resource ("deploy:staging",
  "schema:users"); `release_lock` when finished. Lighter than a task, expires on
  its own.
- Presence. `heartbeat` publishes what you are working on (repo, branch, a short
  activity string). `list_agents` shows who else is active right now.
- Shared notes. `set_note` / `get_note` / `search_notes` are the team's durable
  memory: decisions, gotchas, deploy state. Prefer a note over repeating the
  same explanation in chat.
- Waiting. When you are blocked on teammates, call `wait_for_updates` instead of
  polling: it blocks until a relevant message/task/lock/note event arrives or
  the timeout passes. `team_digest` summarises the last hours of team activity —
  useful at session start to catch up.
- Attachments. Small files (diffs, logs, configs — max 256 KiB each) travel
  with messages (`post_message` `attachments`) or tasks (`attach_file`), and
  are fetched with `get_attachment`. Share the artifact itself instead of
  describing it.
- Asking. When you need an answer from a specific teammate to continue,
  `ask_agent` sends them the question and waits for the reply in one call. If
  you receive a question (a direct message marked `"question": true`), answer
  promptly with `post_message` (`to` the asker, `reply_to` the question id) —
  their agent is blocked waiting on you.

Conventions worth following: keep messages short and factual, scope notes by
repository name, and use task keys that a human would recognise.
"#;

/// The MCP server. Cheap to clone: `PgPool` and `EventHub` are `Arc`s
/// internally, and the tool router is rebuilt per session by the transport's
/// service factory.
#[derive(Clone)]
pub struct Bus {
    pub db: PgPool,
    pub hub: EventHub,
    pub tool_router: ToolRouter<Self>,
}

impl Bus {
    pub fn new(db: PgPool, hub: EventHub) -> Self {
        let tool_router = Self::core_router()
            + Self::messaging_router()
            + Self::tasks_router()
            + Self::presence_router()
            + Self::notes_router()
            + Self::locks_router()
            + Self::events_router()
            + Self::attachments_router()
            + Self::sessions_router();
        Self {
            db,
            hub,
            tool_router,
        }
    }
}

/// Pull the authenticated identity out of the HTTP request that carried this
/// tool call. The bearer middleware put it there; if it is missing, something
/// is routing around authentication and we refuse rather than guess.
pub fn auth_of(ctx: &RequestContext<rmcp::RoleServer>) -> Result<AuthCtx, ErrorData> {
    ctx.extensions
        .get::<http::request::Parts>()
        .and_then(|parts| parts.extensions.get::<AuthCtx>())
        .cloned()
        .ok_or_else(|| {
            ErrorData::invalid_request(
                "no authentication context on this request; the server is misconfigured",
                None,
            )
        })
}

#[tool_router(router = core_router, vis = "pub")]
impl Bus {
    #[tool(
        description = "Identify yourself on the bus: your agent handle, your team, \
                       how many unread direct messages you have and how many tasks \
                       you currently hold. Call this first in a session."
    )]
    async fn whoami(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
    ) -> Result<Json<WhoAmI>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(store::whoami(&self.db, &auth).await?))
    }

    #[tool(
        description = "Summarise the team's last hours: channel activity, tasks that moved, \
                       notes touched, who was around, active locks. Call it at session start \
                       to catch up, or to prepare a standup. Direct messages are excluded."
    )]
    async fn team_digest(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<DigestArgs>,
    ) -> Result<Json<DigestResult>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(
            store::digest::team_digest(
                &self.db,
                &auth,
                args.hours.unwrap_or(24),
                args.all_channels,
            )
            .await?,
        ))
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RegisterSessionArgs {
    /// Label for this window — the id your host gives the conversation is
    /// the right choice, because it is stable across a reconnect and new
    /// after a fork. It becomes the `agent/session` address teammates use.
    pub session: String,
    /// How long the credential authenticates for, in seconds (60 to 86400).
    /// Defaults to 24 hours.
    #[serde(default)]
    pub ttl_seconds: Option<i64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RenewSessionArgs {
    /// New lifetime in seconds (60 to 86400). Defaults to 24 hours.
    #[serde(default)]
    pub ttl_seconds: Option<i64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RevokeSessionArgs {
    /// Which session of yours to revoke. Omit to revoke the one making the
    /// call. You can only ever revoke your own agent's sessions.
    #[serde(default)]
    pub session: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DigestArgs {
    /// Window to summarise, in hours (1-336). Defaults to 24.
    #[serde(default)]
    pub hours: Option<i64>,
    /// Summarise every channel instead of only the one this session works in.
    /// Has no effect when your session has no matching channel, where the
    /// digest already covers the whole team.
    #[serde(default)]
    pub all_channels: bool,
}

#[tool_router(router = sessions_router, vis = "pub")]
impl Bus {
    #[tool(
        description = "Register this window as an authenticated session and receive a \
                       credential that PROVES which window it is. Call it once per \
                       conversation with your agent token, then send the returned \
                       session_token as the bearer token instead. The label you pass \
                       becomes your `agent/session` address. Registering the same label \
                       again resumes that session: new secret, higher epoch, same identity \
                       and history — the process it replaces is fenced off. A session \
                       credential cannot register another."
    )]
    async fn register_session(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<RegisterSessionArgs>,
    ) -> Result<Json<SessionCredential>, ErrorData> {
        let auth = auth_of(&ctx)?;
        // The credential that authenticated this request, by id: the tool
        // layer never sees the secret it hangs the new session off.
        let parent = auth.token_id.ok_or_else(|| {
            ErrorData::invalid_request(
                "register_session must be called with an agent token. This call used a \
                 session credential, which cannot register another session — use the agent \
                 token this window's credential was derived from.",
                None,
            )
        })?;
        let issued =
            store::sessions::register(&self.db, &auth, parent, &args.session, args.ttl_seconds)
                .await?;
        Ok(Json(store::sessions::credential_of(
            issued,
            &auth.agent_name,
        )))
    }

    #[tool(
        description = "Extend the session credential that made this call, keeping its \
                       secret and its epoch so the connection is not disturbed. Call it \
                       well before expires_in_seconds runs out."
    )]
    async fn renew_session(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<RenewSessionArgs>,
    ) -> Result<Json<SessionCredential>, ErrorData> {
        let auth = auth_of(&ctx)?;
        let issued = store::sessions::renew(&self.db, &auth, args.ttl_seconds).await?;
        Ok(Json(store::sessions::credential_of(
            issued,
            &auth.agent_name,
        )))
    }

    #[tool(
        description = "Revoke a session credential of yours: the one making the call, or \
                       another of your agent's windows by label (a crashed one, say). The \
                       credential stops authenticating at once; messages, claims and \
                       history filed under that session are untouched."
    )]
    async fn revoke_session(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<RevokeSessionArgs>,
    ) -> Result<Json<serde_json::Value>, ErrorData> {
        let auth = auth_of(&ctx)?;
        let label = store::sessions::revoke(&self.db, &auth, args.session.as_deref()).await?;
        Ok(Json(serde_json::json!({ "revoked_session": label })))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for Bus {
    fn get_info(&self) -> ServerConfig {
        let mut info = ServerConfig::new(ServerCapabilities::builder().enable_tools().build());
        info.instructions = Some(INSTRUCTIONS.trim().to_string());
        info
    }
}
