use rmcp::{
    ErrorData, Json, handler::server::wrapper::Parameters, service::RequestContext, tool,
    tool_router,
};
use schemars::JsonSchema;
use serde::Deserialize;

use super::{Bus, auth_of};
use crate::{
    model::{ClaimResult, TaskDetail, TaskInfo, TaskPageList},
    store::tasks,
};

fn default_limit() -> i64 {
    50
}

fn default_search_mode() -> String {
    "keywords".to_owned()
}

fn default_search_fields() -> String {
    "both".to_owned()
}

fn default_search_language() -> String {
    "simple".to_owned()
}

fn validate_search_args(args: &ListTasksArgs) -> Result<(), ErrorData> {
    if !args
        .search
        .as_deref()
        .is_some_and(|query| !query.trim().is_empty())
    {
        return Ok(());
    }

    if !matches!(args.search_mode.as_str(), "keywords" | "contains" | "regex") {
        return Err(ErrorData::invalid_params(
            "search_mode must be one of: keywords, contains, regex",
            None,
        ));
    }
    if !matches!(
        args.search_fields.as_str(),
        "title" | "description" | "both"
    ) {
        return Err(ErrorData::invalid_params(
            "search_fields must be one of: title, description, both",
            None,
        ));
    }
    if !matches!(
        args.search_language.as_str(),
        "simple" | "english" | "russian"
    ) {
        return Err(ErrorData::invalid_params(
            "search_language must be one of: simple, english, russian",
            None,
        ));
    }
    Ok(())
}

fn search_query_from_args(args: &ListTasksArgs) -> tasks::TaskSearchQuery {
    tasks::TaskSearchQuery {
        search: args.search.clone(),
        search_mode: Some(args.search_mode.clone()),
        search_fields: Some(args.search_fields.clone()),
        search_language: Some(args.search_language.clone()),
    }
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
    /// Maximum tasks to return (1-10,000 per page).
    #[serde(default = "default_limit")]
    pub limit: i64,
    /// Optional key prefix used to scope the inventory, for example `api#`.
    #[serde(default)]
    pub project_prefix: Option<String>,
    /// Opaque cursor returned by an earlier page for the same team and filters.
    #[serde(default)]
    pub cursor: Option<String>,
    /// Optional title/description search query. Empty or omitted preserves the legacy list.
    #[serde(default)]
    pub search: Option<String>,
    /// Search strategy: `keywords`, `contains`, or `regex`.
    #[serde(default = "default_search_mode")]
    pub search_mode: String,
    /// Fields searched: `title`, `description`, or `both`.
    #[serde(default = "default_search_fields")]
    pub search_fields: String,
    /// PostgreSQL text-search language: `simple`, `english`, or `russian`.
    #[serde(default = "default_search_language")]
    pub search_language: String,
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
    ) -> Result<Json<TaskPageList>, ErrorData> {
        validate_search_args(&args)?;
        let search_query = search_query_from_args(&args);
        let auth = auth_of(&ctx)?;
        let page = tasks::list_tasks_page_with_search(
            &self.db,
            &auth,
            tasks::TaskPageQuery {
                status: args.status,
                mine_only: args.mine_only,
                limit: args.limit,
                project_prefix: args.project_prefix,
                cursor: args.cursor,
            },
            search_query,
        )
        .await?;
        Ok(Json(TaskPageList {
            tasks: page.tasks,
            open: page.open,
            claimed: page.claimed,
            next_cursor: page.next_cursor,
            has_more: page.has_more,
        }))
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

#[cfg(test)]
mod tests {
    use super::{ListTasksArgs, search_query_from_args, validate_search_args};

    #[test]
    fn legacy_list_arguments_default_pagination_filters() {
        let args: ListTasksArgs =
            serde_json::from_str(r#"{"status":"open","mine_only":true,"limit":25}"#)
                .expect("legacy task-list arguments should remain valid");

        assert_eq!(args.status.as_deref(), Some("open"));
        assert!(args.mine_only);
        assert_eq!(args.limit, 25);
        assert_eq!(args.project_prefix, None);
        assert_eq!(args.cursor, None);
        assert_eq!(args.search, None);
        assert_eq!(args.search_mode, "keywords");
        assert_eq!(args.search_fields, "both");
        assert_eq!(args.search_language, "simple");
    }

    #[test]
    fn search_arguments_accept_frozen_options() {
        let args: ListTasksArgs = serde_json::from_str(
            r#"{"search":"lease renewal","search_mode":"contains","search_fields":"title","search_language":"english"}"#,
        )
        .expect("search task-list arguments should deserialize");

        assert_eq!(args.search.as_deref(), Some("lease renewal"));
        assert_eq!(args.search_mode, "contains");
        assert_eq!(args.search_fields, "title");
        assert_eq!(args.search_language, "english");
    }

    #[test]
    fn search_arguments_map_to_store_query_without_dropping_filters() {
        let args: ListTasksArgs = serde_json::from_str(
            r#"{"status":"open","mine_only":true,"limit":7,"project_prefix":"api#","cursor":"next","search":"lease","search_mode":"regex","search_fields":"description","search_language":"russian"}"#,
        )
        .expect("search task-list arguments should deserialize");
        let query = search_query_from_args(&args);

        assert_eq!(query.search.as_deref(), Some("lease"));
        assert_eq!(query.search_mode.as_deref(), Some("regex"));
        assert_eq!(query.search_fields.as_deref(), Some("description"));
        assert_eq!(query.search_language.as_deref(), Some("russian"));
        assert_eq!(args.status.as_deref(), Some("open"));
        assert!(args.mine_only);
        assert_eq!(args.limit, 7);
        assert_eq!(args.project_prefix.as_deref(), Some("api#"));
        assert_eq!(args.cursor.as_deref(), Some("next"));
    }

    #[test]
    fn search_options_reject_unknown_values_only_for_nonempty_search() {
        let mut args: ListTasksArgs =
            serde_json::from_str(r#"{"search":"term","search_mode":"wildcard"}"#)
                .expect("search task-list arguments should deserialize");
        assert!(validate_search_args(&args).is_err());

        args.search = Some("   ".to_owned());
        args.search_mode = "wildcard".to_owned();
        assert!(validate_search_args(&args).is_ok());
    }

    #[test]
    fn paged_list_arguments_accept_prefix_and_cursor() {
        let args: ListTasksArgs =
            serde_json::from_str(r#"{"project_prefix":"api#","cursor":"opaque-token"}"#)
                .expect("paged task-list arguments should deserialize");

        assert_eq!(args.project_prefix.as_deref(), Some("api#"));
        assert_eq!(args.cursor.as_deref(), Some("opaque-token"));
        assert_eq!(args.limit, 50);
        assert!(!args.mine_only);
    }
}
