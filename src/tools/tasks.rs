use rmcp::{
    ErrorData, Json, handler::server::wrapper::Parameters, service::RequestContext, tool,
    tool_router,
};
use schemars::JsonSchema;
use serde::Deserialize;

use super::{Bus, auth_of};
use crate::{
    model::{ClaimResult, TaskDetail, TaskInfo, TaskList},
    store::tasks,
};

fn default_limit() -> i64 {
    50
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CreateTaskArgs {
    /// Stable, human-recognisable identifier, e.g. "refactor-auth" or "api#421".
    /// Must be unique within the team.
    pub key: String,
    /// One-line summary of the work.
    pub title: String,
    /// Optional longer description: acceptance criteria, relevant files, context
    /// a teammate's agent would need to pick this up cold.
    #[serde(default)]
    pub description: Option<String>,
    /// Optional structured payload (any JSON object).
    #[serde(default)]
    #[schemars(schema_with = "crate::model::any_json_schema")]
    pub metadata: Option<serde_json::Value>,
    /// Keys of existing tasks this one depends on. It cannot be claimed until
    /// every dependency is done or cancelled, and claim_next_task skips it.
    #[serde(default)]
    pub depends_on: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListTasksArgs {
    /// Filter by status: "open", "claimed", "done", "cancelled", or "any".
    /// Defaults to all statuses. "open" includes tasks whose claim lapsed;
    /// "claimed" is live claims only.
    #[serde(default)]
    pub status: Option<String>,
    /// Only return tasks this session currently holds a live claim on.
    #[serde(default)]
    pub mine_only: bool,
    /// Maximum tasks to return (1-200).
    #[serde(default = "default_limit")]
    pub limit: i64,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct TaskKeyArgs {
    /// The task key.
    pub key: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ClaimTaskArgs {
    /// The task key to claim.
    pub key: String,
    /// How long your claim should hold before another agent may take over.
    /// Defaults to 900 (15 minutes). Renew it if the work runs longer.
    #[serde(default)]
    pub lease_seconds: Option<i64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ClaimNextArgs {
    /// Lease duration in seconds for the claim. Defaults to 900.
    #[serde(default)]
    pub lease_seconds: Option<i64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CompleteTaskArgs {
    /// The task key.
    pub key: String,
    /// What was done, and anything the next person needs to know. This is what
    /// teammates will read instead of asking you.
    #[serde(default)]
    pub result: Option<String>,
}

#[tool_router(router = tasks_router, vis = "pub")]
impl Bus {
    #[tool(
        description = "Register a unit of shared work so the team can coordinate on it. \
                       Creating a task does not claim it. Use depends_on to chain work \
                       into a pipeline."
    )]
    async fn create_task(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<CreateTaskArgs>,
    ) -> Result<Json<TaskInfo>, ErrorData> {
        let auth = auth_of(&ctx)?;
        let input = tasks::CreateInput {
            key: args.key,
            title: args.title,
            description: args.description,
            metadata: args.metadata,
            depends_on: args.depends_on,
        };
        Ok(Json(tasks::create_task(&self.db, &auth, input).await?))
    }

    #[tool(
        description = "List the team's tasks with who holds each one. Check this before \
                       starting work so you do not duplicate a teammate's effort."
    )]
    async fn list_tasks(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<ListTasksArgs>,
    ) -> Result<Json<TaskList>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(
            tasks::list_tasks(&self.db, &auth, args.status, args.mine_only, args.limit).await?,
        ))
    }

    #[tool(description = "Get one task with its full history of claims and completions.")]
    async fn get_task(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<TaskKeyArgs>,
    ) -> Result<Json<TaskDetail>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(tasks::get_task(&self.db, &auth, &args.key).await?))
    }

    #[tool(
        description = "Take exclusive ownership of a task before working on it. Fails \
                       cleanly (claimed=false) if a teammate holds an unexpired claim. \
                       Re-claiming a task you already hold extends your lease."
    )]
    async fn claim_task(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<ClaimTaskArgs>,
    ) -> Result<Json<ClaimResult>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(
            tasks::claim_task(&self.db, &auth, &args.key, args.lease_seconds).await?,
        ))
    }

    #[tool(
        description = "Claim the oldest available task without naming it. Safe to call \
                       concurrently from several agents: each gets a different task."
    )]
    async fn claim_next_task(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<ClaimNextArgs>,
    ) -> Result<Json<ClaimResult>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(
            tasks::claim_next_task(&self.db, &auth, args.lease_seconds).await?,
        ))
    }

    #[tool(
        description = "Extend the lease on a task you hold. Call this periodically \
                       during long work so the claim does not lapse and get stolen."
    )]
    async fn renew_task_lease(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<ClaimTaskArgs>,
    ) -> Result<Json<TaskInfo>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(
            tasks::renew_lease(&self.db, &auth, &args.key, args.lease_seconds).await?,
        ))
    }

    #[tool(
        description = "Give up a task you claimed without finishing it, returning it to \
                       the open pool for someone else."
    )]
    async fn release_task(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<TaskKeyArgs>,
    ) -> Result<Json<TaskInfo>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(tasks::release_task(&self.db, &auth, &args.key).await?))
    }

    #[tool(
        description = "Mark a task as done and record what was done. Write the result as \
                       if a teammate's agent will read it with no other context."
    )]
    async fn complete_task(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<CompleteTaskArgs>,
    ) -> Result<Json<TaskInfo>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(
            tasks::complete_task(&self.db, &auth, &args.key, args.result).await?,
        ))
    }
}
