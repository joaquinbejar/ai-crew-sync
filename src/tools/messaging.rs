use std::time::Duration;

use rmcp::{
    ErrorData, Json, handler::server::wrapper::Parameters, service::RequestContext, tool,
    tool_router,
};
use schemars::JsonSchema;
use serde::Deserialize;

use super::{Bus, auth_of};
use crate::{
    model::{AskResult, ChannelInfo, ChannelList, MessageList, PostMessageResult},
    store::{agent_id_by_name, messaging},
};

const ASK_DEFAULT_TIMEOUT_SECS: i64 = 45;
const ASK_MAX_TIMEOUT_SECS: i64 = 55;

fn default_limit() -> i64 {
    50
}
fn default_true() -> bool {
    true
}
fn default_scope() -> String {
    "all".into()
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CreateChannelArgs {
    /// Channel name, e.g. "deploys". A leading '#' is optional and names are
    /// lowercased. Creating a channel that already exists is not an error.
    pub name: String,
    /// Optional one-line description of what belongs in this channel.
    #[serde(default)]
    pub topic: Option<String>,
}

/// Does this agent have a live presence row for some *other* session?
///
/// Used to tell an impossible question from a reasonable one: asking yourself
/// as a person is fine when another of your windows is around to answer, and
/// hopeless when it is not.
async fn has_another_live_session(
    pool: &sqlx::PgPool,
    auth: &crate::auth::AuthCtx,
) -> Result<bool, crate::error::BusError> {
    let (other,): (bool,) = sqlx::query_as(
        "SELECT EXISTS (
             SELECT 1 FROM agent_presence
             WHERE agent_id = $1 AND session <> $2 AND expires_at > now()
         )",
    )
    .bind(auth.agent_id)
    .bind(&auth.session)
    .fetch_one(pool)
    .await?;
    Ok(other)
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct PostMessageArgs {
    /// Channel to broadcast to. Mutually exclusive with `to`.
    #[serde(default)]
    pub channel: Option<String>,
    /// Interrupt the whole team with this one. A channel message normally
    /// only wakes sessions focused on that channel; an announcement wakes
    /// every session, whatever they are working on. Reserve it for what
    /// genuinely blocks others — a deploy, a migration, a breaking change,
    /// "stop pushing to main". Routine progress does not qualify, and a team
    /// that is interrupted for everything stops reading announcements.
    /// Channel messages only: a direct message already arrives unfiltered.
    #[serde(default)]
    pub announce: bool,
    /// Who to send a direct message to. Mutually exclusive with `channel`.
    /// `"dani"` reaches every session that teammate has open; `"dani/api"`
    /// reaches only their `api` working context. Addressing one of your own
    /// sessions is allowed and is how a coordinating window hands work to the
    /// one that has the repository open.
    #[serde(default)]
    pub to: Option<String>,
    /// The message text. Keep it short and factual; teammates' agents read
    /// this. Hard limit 1 MiB — attach a file rather than pasting a huge log.
    pub body: String,
    /// Id of the message this replies to, to keep a thread together.
    #[serde(default)]
    pub reply_to: Option<i64>,
    /// Optional structured payload attached to the message (any JSON object).
    #[serde(default)]
    #[schemars(schema_with = "crate::model::any_json_schema")]
    pub metadata: Option<serde_json::Value>,
    /// Small files to ship with the message (diffs, logs, configs). Max 8
    /// files, 256 KiB each (decoded).
    #[serde(default)]
    pub attachments: Option<Vec<AttachmentInput>>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct AttachmentInput {
    pub filename: String,
    /// MIME type; defaults to application/octet-stream.
    #[serde(default)]
    pub content_type: Option<String>,
    /// File content, base64-encoded.
    pub data_base64: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ReadMessagesArgs {
    /// What to read: "all" (everything visible to you), "inbox" (direct
    /// messages addressed to you), or a channel name such as "deploys".
    #[serde(default = "default_scope")]
    pub scope: String,
    /// When true (the default) return only messages you have not read yet and
    /// advance your read cursor. Set false to re-read recent history.
    #[serde(default = "default_true")]
    pub only_new: bool,
    /// Maximum messages to return (1-200).
    #[serde(default = "default_limit")]
    pub limit: i64,
    /// Also return direct messages addressed to your *other* sessions. Off by
    /// default so each working context sees its own; turn it on to catch up on
    /// everything addressed to you anywhere.
    #[serde(default)]
    pub all_sessions: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct AskAgentArgs {
    /// Who to ask: an agent handle, or `agent/session` for one of their
    /// working contexts. Use list_agents to see who is around and which
    /// sessions they have open.
    pub to: String,
    /// The question, sent as a direct message. Omit when resuming with
    /// `resume_message_id`.
    #[serde(default)]
    pub question: Option<String>,
    /// How long to wait for the answer, in seconds (5-55, default 45).
    /// Kept under a minute so HTTP intermediaries do not cut the call.
    #[serde(default)]
    pub timeout_seconds: Option<i64>,
    /// Keep waiting on an earlier question instead of sending a new one:
    /// pass the `question_message_id` from a timed-out ask_agent call.
    #[serde(default)]
    pub resume_message_id: Option<i64>,
}

/// What to tell an asker whose question was not answered in time (#224):
/// the question is stored either way, and what is known about the recipient
/// decides whether waiting longer is likely to help. Never a promise.
fn timeout_suggestion(
    to: &str,
    timeout: i64,
    question_id: i64,
    recipient: &crate::model::RecipientState,
) -> String {
    let resume = format!(
        "ask_agent again with resume_message_id={question_id} (it does not re-send the question)"
    );
    let stored = format!(
        "{to} has not answered within {timeout}s. Your question (message {question_id}) is \
         stored and stays in their inbox"
    );
    let reach = match (recipient.routing.as_str(), recipient.windows.len()) {
        ("agent", n) if n > 1 => format!(
            " The bare name reaches every window of theirs ({n} known); ask one window \
             (`agent/session`, see list_sessions) to know which will read it."
        ),
        _ => String::new(),
    };
    let online: Vec<&crate::model::RecipientWindow> =
        recipient.windows.iter().filter(|w| w.online).collect();
    if online.is_empty() {
        return format!(
            "{stored}, but no window of theirs is connected: nothing is reading it now. Do \
             other work and check read_messages later, or {resume} once list_sessions shows \
             them online.{reach}"
        );
    }
    if let Some(w) = online
        .iter()
        .find(|w| matches!(w.attention.state.as_str(), "attending" | "waiting"))
    {
        return format!(
            "{stored}. {} is {} (observed {}), so an answer may still come: {resume}.{reach}",
            w.address,
            w.attention.state,
            w.attention.observed_at.as_deref().unwrap_or("recently"),
        );
    }
    if let Some(w) = online.iter().find(|w| w.attention.wake != "unknown") {
        let last = match w.attention.last_wake.as_deref() {
            Some("failed") => " Its last wake attempt failed, so it may need its human.",
            _ => "",
        };
        return format!(
            "{stored}. {} is connected but its model is {}; it announces a wake mechanism \
             ({}), which may start a turn for your question.{last} {resume}, or do other \
             work and check read_messages later.{reach}",
            w.address, w.attention.state, w.attention.wake,
        );
    }
    let w = online[0];
    format!(
        "{stored}, but {} is connected with its model {} and announces no way to be woken: \
         the question will likely wait until its human starts a turn. Do not wait in a \
         loop: do other work (or leave the work as a task) and check read_messages later.\
         {reach}",
        w.address, w.attention.state,
    )
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SearchMessagesArgs {
    /// Full-text search terms.
    pub query: String,
    /// Maximum messages to return (1-200).
    #[serde(default = "default_limit")]
    pub limit: i64,
}

#[tool_router(router = messaging_router, vis = "pub")]
impl Bus {
    #[tool(
        description = "List the team's channels with their topic and message count. \
                       Use this before posting so you pick an existing channel."
    )]
    async fn list_channels(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
    ) -> Result<Json<ChannelList>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(messaging::list_channels(&self.db, &auth).await?))
    }

    #[tool(
        description = "Create a channel (or update its topic if it already exists). \
                       Channels are shared by the whole team."
    )]
    async fn create_channel(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<CreateChannelArgs>,
    ) -> Result<Json<ChannelInfo>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(
            messaging::create_channel(&self.db, &auth, &args.name, args.topic).await?,
        ))
    }

    #[tool(
        description = "Send a message to the team. Set `channel` to broadcast, or `to` \
                       with an agent handle to send a direct message. The sender is \
                       your own identity and cannot be spoofed."
    )]
    async fn post_message(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<PostMessageArgs>,
    ) -> Result<Json<PostMessageResult>, ErrorData> {
        let auth = auth_of(&ctx)?;
        let raw = args.attachments.unwrap_or_default();
        if raw.len() > 8 {
            return Err(
                crate::error::BusError::invalid("a message carries at most 8 attachments").into(),
            );
        }
        let attachments = raw
            .into_iter()
            .map(|a| {
                crate::store::attachments::decode_input(&a.filename, a.content_type, &a.data_base64)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let input = messaging::PostInput {
            channel: args.channel,
            to: args.to,
            announce: args.announce,
            body: args.body,
            reply_to: args.reply_to,
            metadata: args.metadata,
            attachments,
        };
        Ok(Json(messaging::post_message(&self.db, &auth, input).await?))
    }

    #[tool(
        description = "Read messages from the bus. By default returns only what you \
                       have not seen and marks it as read, so calling it repeatedly \
                       gives you an incremental feed of what teammates are saying."
    )]
    async fn read_messages(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<ReadMessagesArgs>,
    ) -> Result<Json<MessageList>, ErrorData> {
        let auth = auth_of(&ctx)?;
        let input = messaging::ReadInput {
            scope: args.scope,
            only_new: args.only_new,
            limit: args.limit,
            all_sessions: args.all_sessions,
        };
        Ok(Json(
            messaging::read_messages(&self.db, &auth, input).await?,
        ))
    }

    #[tool(
        description = "Ask a teammate's agent a question and block until they answer or the \
                       timeout passes. Sends the question as a direct message and waits for \
                       their reply, so one call replaces post_message + wait_for_updates + \
                       read_messages. On timeout, call it again with `resume_message_id` set \
                       to the returned question_message_id to keep waiting without asking \
                       twice. The result's `recipient` says whether the window is connected, \
                       whether its model is attending, idle or unknown, and how it can be \
                       woken; on a timeout the suggestion says whether waiting more is likely \
                       to help. A stored question is not a delivered answer. If you receive a \
                       question yourself, answer with post_message (`to` the asker, \
                       `reply_to` the question id)."
    )]
    async fn ask_agent(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<AskAgentArgs>,
    ) -> Result<Json<AskResult>, ErrorData> {
        let auth = auth_of(&ctx)?;
        let to = args.to.trim().to_owned();
        let timeout = args
            .timeout_seconds
            .unwrap_or(ASK_DEFAULT_TIMEOUT_SECS)
            .clamp(5, ASK_MAX_TIMEOUT_SECS);

        let (target_name, target_session) = messaging::parse_address(&to)?;
        let target_id = agent_id_by_name(&self.db, auth.team_id, &target_name).await?;
        // Asking another of your own sessions is the point of session
        // addressing — a coordinating window handing work to the one with the
        // repository open. Only asking *this* window is impossible: nothing
        // would ever read the question, and the call would block until timeout.
        // Asking another of your own sessions is the point of session
        // addressing. What cannot work is an address only *this* window can
        // read, because the call blocks until something answers it.
        if target_id == auth.agent_id {
            let only_me = match target_session.as_deref() {
                // This exact window: nothing else will ever read it.
                Some(s) => s == auth.session,
                // The person: another live window of yours can answer, so this
                // is only hopeless when there is no other one.
                None => !has_another_live_session(&self.db, &auth).await?,
            };
            if only_me {
                return Err(crate::error::BusError::invalid(
                    "nothing would ever read that question: the address resolves to this \
                     session and no other window of yours is live. Ask a teammate, or \
                     another of your own sessions as 'you/<session>' — list_agents shows \
                     which are open.",
                )
                .into());
            }
        }

        // Subscribe before posting or checking so an answer arriving in
        // between still wakes us.
        let mut rx = self.hub.subscribe();

        let question_id = match args.resume_message_id {
            Some(id) => {
                messaging::verify_question(
                    &self.db,
                    &auth,
                    target_id,
                    target_session.as_deref(),
                    id,
                )
                .await?;
                id
            }
            None => {
                let question = args
                    .question
                    .as_deref()
                    .map(str::trim)
                    .filter(|q| !q.is_empty())
                    .ok_or_else(|| {
                        crate::error::BusError::invalid(
                            "provide `question`, or `resume_message_id` to keep waiting on \
                             an earlier one",
                        )
                    })?;
                let posted = messaging::post_message(
                    &self.db,
                    &auth,
                    messaging::PostInput {
                        channel: None,
                        to: Some(to.clone()),
                        // A question is a direct message; it already reaches
                        // the addressee whatever they are focused on.
                        announce: false,
                        body: question.to_owned(),
                        reply_to: None,
                        metadata: Some(serde_json::json!({ "question": true })),
                        attachments: Vec::new(),
                    },
                )
                .await?;
                posted.message.id
            }
        };

        let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout as u64);
        loop {
            // Check the database first: covers resumed asks whose answer
            // already landed, and events lost while lagging.
            if let Some(answer) = messaging::find_answer(
                &self.db,
                &auth,
                target_id,
                target_session.as_deref(),
                question_id,
            )
            .await?
            {
                let recipient = crate::store::presence::recipient_state(
                    &self.db,
                    target_id,
                    &target_name,
                    target_session.as_deref(),
                )
                .await?;
                return Ok(Json(AskResult {
                    answered: true,
                    to,
                    question_message_id: question_id,
                    answer: Some(answer),
                    suggestion: "The answer also sits unread in your inbox; read_messages \
                                 will mark it read."
                        .into(),
                    question_stored: true,
                    recipient,
                }));
            }

            // Wait for a direct message from the target (or the deadline).
            let woke = loop {
                tokio::select! {
                    _ = tokio::time::sleep_until(deadline) => break false,
                    recv = rx.recv() => match recv {
                        Ok(ev) => {
                            // Later than the question, so asking another of your
                            // own sessions does not wake on your own question.
                            // Addressed here or to the person, so a message to a
                            // sibling window does not either.
                            let for_us = ev.recipient_session().is_none_or(|s| s == auth.session);
                            if ev.kind() == "message"
                                && ev.sender_agent_id() == Some(target_id)
                                && ev.recipient_agent_id() == Some(auth.agent_id)
                                && ev.message_id().is_some_and(|id| id > question_id)
                                && for_us
                            {
                                break true;
                            }
                        }
                        // Lagged: events were dropped; resync from the database.
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => break true,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break false,
                    }
                }
            };

            if !woke {
                // One last look: the answer may have raced the deadline.
                let answer = messaging::find_answer(
                    &self.db,
                    &auth,
                    target_id,
                    target_session.as_deref(),
                    question_id,
                )
                .await?;
                let answered = answer.is_some();
                let recipient = crate::store::presence::recipient_state(
                    &self.db,
                    target_id,
                    &target_name,
                    target_session.as_deref(),
                )
                .await?;
                return Ok(Json(AskResult {
                    answered,
                    suggestion: if answered {
                        "The answer also sits unread in your inbox; read_messages will \
                         mark it read."
                            .into()
                    } else {
                        timeout_suggestion(&to, timeout, question_id, &recipient)
                    },
                    to,
                    question_message_id: question_id,
                    answer,
                    question_stored: true,
                    recipient,
                }));
            }
        }
    }

    #[tool(
        description = "Full-text search across the team's channel history and your own \
                       direct messages. Does not affect your read cursor."
    )]
    async fn search_messages(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<SearchMessagesArgs>,
    ) -> Result<Json<MessageList>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(
            messaging::search_messages(&self.db, &auth, &args.query, args.limit).await?,
        ))
    }
}
