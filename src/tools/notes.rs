use rmcp::{
    ErrorData, Json, handler::server::wrapper::Parameters, service::RequestContext, tool,
    tool_router,
};
use schemars::JsonSchema;
use serde::Deserialize;

use super::{Bus, auth_of};
use crate::{
    model::{Ack, NoteInfo, NoteList, NoteRef},
    store::notes,
};

fn default_limit() -> i64 {
    50
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SetNoteArgs {
    /// Namespace for the note, typically a repository or project name.
    /// Defaults to "global". At most 64 bytes.
    #[serde(default)]
    pub scope: Option<String>,
    /// Short identifier, e.g. "deploy-runbook" or "why-we-dropped-redis".
    /// At most 256 bytes: a key is a name, the content goes in `value`.
    pub key: String,
    /// The content. Write it for a teammate's agent reading it cold, with no
    /// memory of this conversation. Hard limit 1 MiB.
    pub value: String,
    /// Optional tags for filtering, e.g. ["infra", "postmortem"].
    #[serde(default)]
    pub tags: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetNoteArgs {
    /// Namespace. Defaults to "global".
    #[serde(default)]
    pub scope: Option<String>,
    /// The note key.
    pub key: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListNotesArgs {
    /// Restrict to one namespace. Omit to list every scope.
    #[serde(default)]
    pub scope: Option<String>,
    /// Only return notes carrying this tag.
    #[serde(default)]
    pub tag: Option<String>,
    /// Maximum notes to return (1-200).
    #[serde(default = "default_limit")]
    pub limit: i64,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SearchNotesArgs {
    /// Full-text search terms, matched against note keys and content.
    pub query: String,
    /// Restrict to one namespace.
    #[serde(default)]
    pub scope: Option<String>,
    /// Maximum notes to return (1-200).
    #[serde(default = "default_limit")]
    pub limit: i64,
}

#[tool_router(router = notes_router, vis = "pub")]
impl Bus {
    #[tool(
        description = "Write or overwrite a shared note: a decision, a gotcha, the state \
                       of a deploy. This is the team's durable memory, readable by every \
                       teammate's agent. Previous versions are kept."
    )]
    async fn set_note(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<SetNoteArgs>,
    ) -> Result<Json<NoteInfo>, ErrorData> {
        let auth = auth_of(&ctx)?;
        let input = notes::SetInput {
            scope: args.scope,
            key: args.key,
            value: args.value,
            tags: args.tags,
        };
        Ok(Json(notes::set_note(&self.db, &auth, input).await?))
    }

    #[tool(
        description = "Read one shared note by scope and key. Returns found=false rather \
                       than erroring when the note does not exist."
    )]
    async fn get_note(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<GetNoteArgs>,
    ) -> Result<Json<NoteRef>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(
            notes::get_note(&self.db, &auth, args.scope, &args.key).await?,
        ))
    }

    #[tool(
        description = "List shared notes, optionally filtered by scope or tag. Use this \
                       to discover what the team already wrote down."
    )]
    async fn list_notes(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<ListNotesArgs>,
    ) -> Result<Json<NoteList>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(
            notes::list_notes(&self.db, &auth, args.scope, args.tag, args.limit).await?,
        ))
    }

    #[tool(description = "Full-text search the team's shared notes.")]
    async fn search_notes(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<SearchNotesArgs>,
    ) -> Result<Json<NoteList>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(
            notes::search_notes(&self.db, &auth, &args.query, args.scope, args.limit).await?,
        ))
    }

    #[tool(description = "Delete a shared note. Its revision history goes with it.")]
    async fn delete_note(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<GetNoteArgs>,
    ) -> Result<Json<Ack>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(
            notes::delete_note(&self.db, &auth, args.scope, &args.key).await?,
        ))
    }
}
