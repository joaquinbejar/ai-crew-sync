//! MCP tools for conversations: addressed threads with honest receipts.
//!
//! Thin, like every other handler here. Two things are worth saying about
//! the descriptions, because they are read by models:
//!
//! * they name what a receipt **is not** — reading is not acknowledging, a
//!   resolution does not complete a task, and an unknown presentation is not
//!   a "no";
//! * they never offer a way to speak for someone else. There is no argument
//!   for whose receipt this is, and there is no "send as".

use rmcp::{
    ErrorData, Json, handler::server::wrapper::Parameters, service::RequestContext, tool,
    tool_router,
};
use schemars::JsonSchema;
use serde::Deserialize;
use uuid::Uuid;

use super::{Bus, auth_of};
use crate::{
    model::{
        ConversationInfo, ConversationList, ConversationRead, ConversationUpdates, InboxBatch,
        InboxState, MessageReceipts, ProjectInfo, ProjectList, ReceiptInfo, SentMessage,
        TransferResult,
    },
    store::{conversations as store, inbox},
};

#[derive(Debug, Deserialize, JsonSchema)]
pub struct InboxFetchArgs {
    /// How many references to take at most. Default 20, ceiling 50.
    #[serde(default)]
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct InboxConfirmArgs {
    /// The `delivery_id` of every reference you are now holding durably.
    pub delivery_ids: Vec<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct EmptyInboxArgs {}

fn uuid_arg(field: &str, raw: &str) -> Result<Uuid, ErrorData> {
    raw.trim().parse::<Uuid>().map_err(|_| {
        ErrorData::invalid_params(
            format!("{field} must be the opaque id returned by the bus, not a name"),
            None,
        )
    })
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CreateConversationArgs {
    /// What this thread is about, in a line.
    pub title: String,
    /// Project this thread belongs to, by name or id. Everyone with access
    /// to that project can read it. Required unless `private` is true.
    #[serde(default)]
    pub project: Option<String>,
    /// Make it visible only to its members. Fixed at creation: a thread
    /// cannot be widened afterwards, because people spoke in it on those
    /// terms.
    #[serde(default)]
    pub private: bool,
    /// Windows to invite, as `agent` or `agent/session` (list_sessions gives
    /// you the exact address). They are invited, not enrolled: each accepts
    /// with join_conversation.
    #[serde(default)]
    pub invite: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ConversationIdArgs {
    /// The conversation's opaque id.
    pub conversation_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListConversationsArgs {
    /// Include archived threads.
    #[serde(default)]
    pub include_archived: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct InviteArgs {
    pub conversation_id: String,
    /// `agent` or `agent/session`.
    pub address: String,
    /// `moderator`, `participant` (default) or `observer`. An observer reads
    /// and acknowledges but does not post.
    #[serde(default)]
    pub role: Option<String>,
    /// Let the invitee read the thread from its first message instead of
    /// from this one. Never more than you can read yourself: a moderator
    /// admitted late grants history from where it was admitted, whatever
    /// it asks for, and the audit row records both. A re-admission after a
    /// removal always starts here.
    #[serde(default)]
    pub history_from_start: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RemoveMemberArgs {
    pub conversation_id: String,
    /// `agent` or `agent/session`.
    pub address: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SendConversationMessageArgs {
    pub conversation_id: String,
    pub body: String,
    /// A UUID you generate. Sending the same one twice returns the original
    /// message instead of posting it again, which is what makes a retry
    /// safe. The same id with a different body is refused.
    pub request_id: String,
    /// Message id this replies to.
    #[serde(default)]
    pub reply_to: Option<String>,
    /// Structured pointers — ids, flags, short labels. Not a document.
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ReadConversationArgs {
    pub conversation_id: String,
    /// Return messages after this logical sequence. Use `next_after_seq`
    /// from the previous page.
    #[serde(default)]
    pub after_seq: Option<i64>,
    /// Messages per page (1-200, default 50).
    #[serde(default)]
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct MessageIdArgs {
    pub message_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct AckArgs {
    /// The message you are acknowledging. Explicit: acknowledging happens
    /// one message at a time, never "everything up to here".
    pub message_id: String,
    /// Also record that you acted on it, not only that you read it. This
    /// does not complete a task, merge a PR or close anything by itself.
    #[serde(default)]
    pub resolved: bool,
    /// A line for the sender: what you did, or why you will not.
    #[serde(default)]
    pub note: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct TransferArgs {
    pub conversation_id: String,
    /// Another window **of your own agent**, as `agent/session`. It has to
    /// accept with join_conversation; nothing moves until it does.
    pub to: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct WaitConversationArgs {
    /// How long to block, in seconds (1-300, default 50).
    #[serde(default)]
    pub timeout_seconds: Option<i64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ProjectArgs {
    /// Project name (one lower-case word) or id.
    pub project: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ProjectAccessArgs {
    pub project: String,
    /// Teammate's agent handle.
    pub agent: String,
    /// Grant access, or revoke it with false.
    #[serde(default = "default_true")]
    pub grant: bool,
}

fn default_true() -> bool {
    true
}

#[tool_router(router = conversations_router, vis = "pub")]
impl Bus {
    #[tool(
        description = "Start a thread with an explicit membership, so you can later ask who \
                       has seen it and who acted on it — which a channel cannot answer and \
                       three direct messages cannot converge. Pass `project` for a thread \
                       everyone with access to that project can read, or `private: true` \
                       for one only its members can see; the choice is permanent. Invite \
                       exact windows (`agent/session` from list_sessions); each accepts \
                       with join_conversation."
    )]
    async fn create_conversation(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<CreateConversationArgs>,
    ) -> Result<Json<ConversationInfo>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(
            store::create_conversation(
                &self.db,
                &auth,
                store::CreateInput {
                    title: args.title,
                    project: args.project,
                    private: args.private,
                    invite: args.invite,
                },
            )
            .await?,
        ))
    }

    #[tool(
        description = "Threads you can read: the ones you belong to, plus the project \
                       threads your project access covers. A private thread you are not in \
                       is not listed and does not exist as far as this call is concerned."
    )]
    async fn list_conversations(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<ListConversationsArgs>,
    ) -> Result<Json<ConversationList>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(ConversationList {
            conversations: store::list_conversations(&self.db, &auth, args.include_archived)
                .await?,
        }))
    }

    #[tool(
        description = "Invite a window into a thread. Owners and moderators only. The \
                       invitee is not a member until it accepts, so a thread never \
                       conscripts someone into its receipts. By default they see the thread \
                       from now on; `history_from_start` gives them the thread from where \
                       you can read it yourself, its start only if you can, which is a \
                       decision worth making deliberately. Inviting someone already seated \
                       changes their role at once; an owner's seat is only an owner's to \
                       change."
    )]
    async fn invite_to_conversation(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<InviteArgs>,
    ) -> Result<Json<ConversationInfo>, ErrorData> {
        let auth = auth_of(&ctx)?;
        let id = uuid_arg("conversation_id", &args.conversation_id)?;
        Ok(Json(
            store::invite(
                &self.db,
                &auth,
                id,
                &args.address,
                args.role.as_deref(),
                args.history_from_start,
            )
            .await?,
        ))
    }

    #[tool(
        description = "Accept an invitation addressed to THIS window. A sibling window of \
                       yours cannot accept it for you: the invitation names one \
                       agent/session, and so will the receipts."
    )]
    async fn join_conversation(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<ConversationIdArgs>,
    ) -> Result<Json<ConversationInfo>, ErrorData> {
        let auth = auth_of(&ctx)?;
        let id = uuid_arg("conversation_id", &args.conversation_id)?;
        Ok(Json(store::join(&self.db, &auth, id).await?))
    }

    #[tool(
        description = "Leave a thread. What you already said and already acknowledged stays \
                       exactly as it is; you simply stop being addressed."
    )]
    async fn leave_conversation(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<ConversationIdArgs>,
    ) -> Result<Json<serde_json::Value>, ErrorData> {
        let auth = auth_of(&ctx)?;
        let id = uuid_arg("conversation_id", &args.conversation_id)?;
        store::leave(&self.db, &auth, id).await?;
        Ok(Json(serde_json::json!({ "left": args.conversation_id })))
    }

    #[tool(
        description = "Remove someone else from a thread. Owners and moderators only, and \
                       only an owner can remove an owner. Their history and receipts are \
                       kept — a removal is not a rewrite — and they lose access from now on."
    )]
    async fn remove_conversation_member(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<RemoveMemberArgs>,
    ) -> Result<Json<ConversationInfo>, ErrorData> {
        let auth = auth_of(&ctx)?;
        let id = uuid_arg("conversation_id", &args.conversation_id)?;
        Ok(Json(
            store::remove_member(&self.db, &auth, id, &args.address).await?,
        ))
    }

    #[tool(
        description = "Close a thread to new messages. Its history stays readable to \
                       everyone who could read it. Owners and moderators only."
    )]
    async fn archive_conversation(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<ConversationIdArgs>,
    ) -> Result<Json<ConversationInfo>, ErrorData> {
        let auth = auth_of(&ctx)?;
        let id = uuid_arg("conversation_id", &args.conversation_id)?;
        Ok(Json(
            store::archive_conversation(&self.db, &auth, id).await?,
        ))
    }

    #[tool(
        description = "Hand THIS window's seat to another window of your own agent — when a \
                       conversation moves to a different repository, say. It is a proposal: \
                       the target accepts with join_conversation, and only then is your \
                       seat superseded. Authorship, history boundary and old receipts are \
                       preserved; nothing is acknowledged on your behalf. To bring in \
                       someone else, invite them instead."
    )]
    async fn transfer_membership(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<TransferArgs>,
    ) -> Result<Json<TransferResult>, ErrorData> {
        let auth = auth_of(&ctx)?;
        let id = uuid_arg("conversation_id", &args.conversation_id)?;
        Ok(Json(
            store::transfer_membership(&self.db, &auth, id, &args.to).await?,
        ))
    }

    #[tool(
        description = "Post into a thread. Returns the message id, its logical sequence, \
                       `stored` and `publication` (where the body stands with its backend \
                       right now, NOT that anyone read it) and the exact list of windows it \
                       was addressed to, snapshotted now: someone who joins later never \
                       enters this message's denominator. On a team whose conversations are \
                       published to a broker a fresh send returns `stored: false` with \
                       `publication: \"pending_publication\"`: the message is recorded and \
                       will be stored moments later, so do NOT send it again. Pass a fresh \
                       `request_id` UUID; repeating one returns the original message with its \
                       current state, so a retry can neither double-post nor lose anything."
    )]
    async fn send_conversation_message(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<SendConversationMessageArgs>,
    ) -> Result<Json<SentMessage>, ErrorData> {
        let auth = auth_of(&ctx)?;
        let id = uuid_arg("conversation_id", &args.conversation_id)?;
        let request_id = uuid_arg("request_id", &args.request_id)?;
        let reply_to = match args.reply_to.as_deref() {
            Some(r) => Some(uuid_arg("reply_to", r)?),
            None => None,
        };
        Ok(Json(
            store::send(
                &self.db,
                &auth,
                id,
                store::SendInput {
                    body: args.body,
                    request_id,
                    reply_to,
                    metadata: args.metadata,
                },
            )
            .await?,
        ))
    }

    #[tool(
        description = "Read a thread, oldest first, from where your membership starts. \
                       READING IS NOT ACKNOWLEDGING: no receipt is touched here and no \
                       cursor moves on anyone's behalf. Each message carries your own \
                       receipt so you can see what you have already acknowledged. Page with \
                       `next_after_seq`. Every message has an `unavailable` field: `null` \
                       means `body` is the real text; a reason there means `body` is an \
                       empty placeholder for a body this bus cannot give you — its backend \
                       is unreachable right now, or it was never stored, or it is no longer \
                       held — and NOT an empty message. Never quote or summarise an empty \
                       body without checking `unavailable` first."
    )]
    async fn read_conversation(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<ReadConversationArgs>,
    ) -> Result<Json<ConversationRead>, ErrorData> {
        let auth = auth_of(&ctx)?;
        let id = uuid_arg("conversation_id", &args.conversation_id)?;
        Ok(Json(
            store::read(
                &self.db,
                &self.backends,
                &auth,
                id,
                args.after_seq,
                args.limit,
            )
            .await?,
        ))
    }

    #[tool(
        description = "One message by id, with your own receipt. Access is rechecked now, \
                       so a membership that has ended does not keep reading. `unavailable` \
                       is always present: `null` means `body` is the real text; a reason \
                       there means `body` is an empty placeholder for a body this bus \
                       cannot give you — its backend is unreachable right now, or it was \
                       never stored, or it is no longer held — and NOT an empty message. \
                       Never quote or summarise an empty body without checking \
                       `unavailable` first."
    )]
    async fn get_conversation_message(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<MessageIdArgs>,
    ) -> Result<Json<crate::model::ConversationMessage>, ErrorData> {
        let auth = auth_of(&ctx)?;
        let message_id = uuid_arg("message_id", &args.message_id)?;
        Ok(Json(
            store::get_message(&self.db, &self.backends, &auth, message_id).await?,
        ))
    }

    #[tool(
        description = "Record YOUR OWN observation of one message: acknowledged (you read \
                       it), and with `resolved` that you acted on it. You can only ever \
                       speak for the window making the call — there is no argument for \
                       whose receipt this is. Resolving does not complete a task, merge a \
                       PR or close an issue; it says you consider this message dealt with. \
                       Only a window the message was addressed to has anything to \
                       acknowledge."
    )]
    async fn ack_message(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<AckArgs>,
    ) -> Result<Json<ReceiptInfo>, ErrorData> {
        let auth = auth_of(&ctx)?;
        let message_id = uuid_arg("message_id", &args.message_id)?;
        Ok(Json(
            store::ack(&self.db, &auth, message_id, args.resolved, args.note).await?,
        ))
    }

    #[tool(
        description = "Who a message was addressed to and what each of them has observed. \
                       Five independent facts per recipient: stored, delivered, presented, \
                       acknowledged, resolved. An absent timestamp means NOT OBSERVED, not \
                       'no' — `presented_at` in particular is null wherever the host cannot \
                       confirm the message reached the model. Use it to see who is still \
                       to answer, not to conclude who ignored you."
    )]
    async fn get_message_receipts(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<MessageIdArgs>,
    ) -> Result<Json<MessageReceipts>, ErrorData> {
        let auth = auth_of(&ctx)?;
        let message_id = uuid_arg("message_id", &args.message_id)?;
        Ok(Json(store::receipts(&self.db, &auth, message_id).await?))
    }

    #[tool(
        description = "Take the references waiting for THIS window: which messages exist \
                       for you, not their bodies. Nothing is marked delivered here — you \
                       confirm that separately with confirm_inbox_delivery once you are \
                       holding them, so a crash in between costs a redelivery and not a \
                       message. A reference may arrive twice (`redelivered`); handling it \
                       twice must change nothing. Read the body with \
                       get_conversation_message, which checks your access at that moment."
    )]
    async fn fetch_conversation_inbox(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<InboxFetchArgs>,
    ) -> Result<Json<InboxBatch>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(
            inbox::fetch(&self.db, &self.backends, &auth, args.limit).await?,
        ))
    }

    #[tool(
        description = "Say that you are now holding these references durably. THIS is what \
                       records delivered — the reference reached your process, which is not \
                       the same as a model having seen it (presented), read it \
                       (acknowledged) or acted on it (resolved). Confirming twice is \
                       harmless. Confirm only what you can still find after a restart."
    )]
    async fn confirm_inbox_delivery(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<InboxConfirmArgs>,
    ) -> Result<Json<serde_json::Value>, ErrorData> {
        let auth = auth_of(&ctx)?;
        let done = inbox::confirm(&self.db, &self.backends, &auth, &args.delivery_ids).await?;
        Ok(Json(serde_json::json!({
            "confirmed": done.confirmed.len(),
            "of": args.delivery_ids.len(),
            // Which ids, so a caller that lost the previous answer can tell
            // what it may forget from what it is still owed.
            "confirmed_ids": done.confirmed,
            "already_confirmed": done.already_confirmed,
        })))
    }

    #[tool(
        description = "What is waiting for this window, and where. `undelivered` is the \
                       authoritative count from the bus's own records; the broker numbers \
                       are a cache and may lag or be missing. A missing consumer is not an \
                       empty inbox."
    )]
    async fn conversation_inbox_status(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(_args): Parameters<EmptyInboxArgs>,
    ) -> Result<Json<InboxState>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(inbox::state(&self.db, &self.backends, &auth).await?))
    }

    #[tool(
        description = "Block until a thread you belong to has something you have not \
                       acknowledged, or the timeout passes. Use it instead of polling. It \
                       returns which threads are waiting and how many messages, never the \
                       bodies — read_conversation fetches those, and neither call \
                       acknowledges anything."
    )]
    async fn wait_for_conversation_updates(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<WaitConversationArgs>,
    ) -> Result<Json<ConversationUpdates>, ErrorData> {
        let auth = auth_of(&ctx)?;
        store::require_capability(&self.db, &auth).await?;
        let timeout = args.timeout_seconds.unwrap_or(50).clamp(1, 300);
        let started = std::time::Instant::now();
        // Subscribe before the first check, so a message that lands between
        // the two is not missed; notifications are hints and the database is
        // the truth, exactly as wait_for_updates does it.
        let mut rx = self.hub.subscribe();
        loop {
            let waiting = store::activity(&self.db, &auth).await?;
            if !waiting.is_empty() {
                return Ok(Json(ConversationUpdates {
                    conversations: waiting,
                    waited_seconds: started.elapsed().as_secs() as i64,
                }));
            }
            let left = timeout - started.elapsed().as_secs() as i64;
            if left <= 0 {
                return Ok(Json(ConversationUpdates {
                    conversations: Vec::new(),
                    waited_seconds: started.elapsed().as_secs() as i64,
                }));
            }
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(left.min(5) as u64),
                rx.recv(),
            )
            .await;
        }
    }

    #[tool(
        description = "Create a project: the unit a conversation can be visible to. Access \
                       is an explicit grant, never your working directory or a role label. \
                       You get access to what you create; everyone else needs \
                       grant_project_access."
    )]
    async fn create_project(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<ProjectArgs>,
    ) -> Result<Json<ProjectInfo>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(
            store::create_project(&self.db, &auth, &args.project).await?,
        ))
    }

    #[tool(description = "Projects you have access to, with who else has it.")]
    async fn list_projects(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
    ) -> Result<Json<ProjectList>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(ProjectList {
            projects: store::list_projects(&self.db, &auth).await?,
        }))
    }

    #[tool(
        description = "Grant a teammate access to a project, or revoke it with \
                       `grant: false`. Only someone who already has access can extend it, \
                       so the chain stays inside the project. Revoking takes effect at \
                       once, including for threads they are reading."
    )]
    async fn grant_project_access(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<ProjectAccessArgs>,
    ) -> Result<Json<ProjectInfo>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(
            store::set_project_access(&self.db, &auth, &args.project, &args.agent, args.grant)
                .await?,
        ))
    }

    #[tool(
        description = "Read back a thread's history when EVERY window of your agent is \
                       gone — closed, revoked or expired. The documented exception: a \
                       private thread is private within the team, not from the agent that \
                       was in it. Requires your agent token, not a window's credential; it \
                       is read-only, grants no membership, acknowledges nothing, skips \
                       memberships you were removed from, and is audited."
    )]
    async fn recover_conversation_history(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<ReadConversationArgs>,
    ) -> Result<Json<ConversationRead>, ErrorData> {
        let auth = auth_of(&ctx)?;
        let id = uuid_arg("conversation_id", &args.conversation_id)?;
        Ok(Json(
            store::recover_history(
                &self.db,
                &self.backends,
                &auth,
                id,
                args.after_seq,
                args.limit,
            )
            .await?,
        ))
    }
}

/// Conversations are off until a team enables them, and the catalogue says
/// so: a model should not be offered tools that will refuse. The boundary is
/// still enforced per call — see `store::conversations::require_capability`.
pub async fn enabled_for(db: &sqlx::PgPool, team_id: uuid::Uuid) -> bool {
    matches!(
        sqlx::query_as::<_, (bool,)>("SELECT conversations_enabled FROM teams WHERE id = $1")
            .bind(team_id)
            .fetch_optional(db)
            .await,
        Ok(Some((true,)))
    )
}
