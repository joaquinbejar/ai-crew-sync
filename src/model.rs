//! Wire types returned by the MCP tools.
//!
//! Timestamps are RFC 3339 strings rather than typed datetimes: the consumer is
//! a language model, and a plain string is both unambiguous and free of extra
//! schema dependencies.

use schemars::JsonSchema;
use serde::Serialize;

/// `serde_json::Value` fields would produce a boolean `true` schema, which
/// some MCP clients' validators reject; an empty object schema means the same
/// ("anything") and passes everywhere.
pub fn any_json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({})
}

pub fn ts(dt: chrono::DateTime<chrono::Utc>) -> String {
    dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

pub fn ts_opt(dt: Option<chrono::DateTime<chrono::Utc>>) -> Option<String> {
    dt.map(ts)
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct WhoAmI {
    /// Your agent handle. Other agents address you by this name.
    pub agent: String,
    pub agent_id: String,
    pub team: String,
    pub team_id: String,
    /// Which of your concurrent working contexts this connection is, taken
    /// from the `X-Crew-Session` header — usually the repository you are in.
    /// `null` means the shared session: you sent no header, and your presence,
    /// task claims and locks are not separated from your other sessions.
    pub session: Option<String>,
    /// Channel this session posts to when `post_message` is called with
    /// neither `channel` nor `to` — the one named after your session, if the
    /// team has one. `null` means there is none, so you must always say where
    /// a message goes.
    pub default_channel: Option<String>,
    /// Number of unread direct messages waiting for you.
    pub unread_direct_messages: i64,
    /// Tasks currently claimed by you and not yet completed.
    pub open_claimed_tasks: i64,
}

// ------------------------------------------------------------------ agents --

#[derive(Debug, Serialize, JsonSchema)]
pub struct AgentInfo {
    pub name: String,
    pub display_name: Option<String>,
    /// Which working context the fields below describe — usually a repository
    /// name. Absent for the shared session, used by clients that send no
    /// `X-Crew-Session` header, so a roster of teammates who use no sessions
    /// serialises exactly as it did before sessions existed.
    ///
    /// The summary describes the agent's live named session when there is one,
    /// falling back to the shared row only when there is nothing else. Read
    /// `sessions` when you need all of them; this is one of several.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub session: Option<String>,
    /// One of `active`, `idle`, `offline`. `offline` means the presence lease
    /// expired, i.e. the agent has not sent a heartbeat recently.
    pub status: String,
    pub repo: Option<String>,
    pub branch: Option<String>,
    /// Free-text description of what this agent is currently doing.
    pub activity: Option<String>,
    pub last_seen: Option<String>,
    /// True when *any* of this agent's sessions has a live presence lease.
    pub online: bool,
    /// Every working context this agent has open, most recently active first.
    /// Absent when there is only one — the fields above already describe it.
    /// A teammate with several entries here is working in several repositories
    /// at once, and each one claims tasks and holds locks independently.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub sessions: Vec<AgentSession>,
}

/// One working context of an agent: what that session is doing right now.
#[derive(Debug, Serialize, JsonSchema)]
pub struct AgentSession {
    /// Absent for the shared session.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub session: Option<String>,
    pub status: String,
    pub repo: Option<String>,
    pub branch: Option<String>,
    pub activity: Option<String>,
    pub last_seen: Option<String>,
    pub online: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct AgentList {
    pub agents: Vec<AgentInfo>,
    /// Agents with at least one live session — people, not sessions.
    pub online_count: usize,
}

// -------------------------------------------------------------- messaging --

#[derive(Debug, Serialize, JsonSchema)]
pub struct ChannelInfo {
    pub name: String,
    pub topic: Option<String>,
    pub message_count: i64,
    pub created_at: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ChannelList {
    pub channels: Vec<ChannelInfo>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct MessageInfo {
    pub id: i64,
    pub from: String,
    /// Which of the sender's working contexts wrote this; `null` is their
    /// shared session. Reply to `from/from_session` to reach the window that
    /// is waiting, rather than whichever one notices first.
    pub from_session: Option<String>,
    /// True when this was posted as an announcement: something the sender
    /// judged worth interrupting the whole team for, so it reaches every
    /// session regardless of which channel they are focused on.
    pub announce: bool,
    /// Channel name for channel messages; `null` for direct messages.
    pub channel: Option<String>,
    /// Recipient handle for direct messages; `null` for channel messages.
    pub to: Option<String>,
    /// Set when this direct message was addressed to one working context of
    /// the recipient rather than to the person. `null` means every session of
    /// theirs sees it.
    pub to_session: Option<String>,
    pub body: String,
    pub reply_to: Option<i64>,
    #[schemars(schema_with = "any_json_schema")]
    pub metadata: serde_json::Value,
    /// Files attached to this message; fetch content with get_attachment.
    pub attachments: Vec<AttachmentMeta>,
    pub created_at: String,
}

#[derive(Debug, Serialize, serde::Deserialize, JsonSchema)]
pub struct AttachmentMeta {
    /// Pass this id to get_attachment to download the content.
    pub id: i64,
    pub filename: String,
    pub content_type: String,
    pub size_bytes: i64,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct AttachmentContent {
    pub id: i64,
    pub filename: String,
    pub content_type: String,
    pub size_bytes: i64,
    pub uploaded_by: String,
    pub created_at: String,
    /// The file content, base64-encoded.
    pub data_base64: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct PostMessageResult {
    pub message: MessageInfo,
    /// Handles that can now see this message.
    pub delivered_to: Vec<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct MessageList {
    pub messages: Vec<MessageInfo>,
    /// The scope that was actually read, after normalisation.
    pub scope: String,
    /// Read cursor position after this call. Messages at or below this id will
    /// not be returned again when `only_new` is true.
    pub cursor: i64,
    /// True when the result hit `limit` and older/newer messages remain.
    pub truncated: bool,
}

// ------------------------------------------------------------------ tasks --

#[derive(Debug, Serialize, JsonSchema)]
pub struct TaskInfo {
    pub key: String,
    pub title: String,
    pub description: Option<String>,
    /// One of `open`, `claimed`, `done`, `cancelled`.
    pub status: String,
    /// Keys of tasks this one depends on.
    pub depends_on: Vec<String>,
    /// True while any dependency is not yet done/cancelled. Blocked tasks
    /// cannot be claimed.
    pub blocked: bool,
    pub claimed_by: Option<String>,
    /// Which of `claimed_by`'s working contexts holds the claim; `null` is
    /// their shared session. A claim belongs to a session, not to a person —
    /// your own other session cannot renew, release or steal this one.
    pub claimed_session: Option<String>,
    pub claimed_at: Option<String>,
    /// When the current claim expires. After this instant another agent may
    /// steal the task, so renew the lease if you are still working on it.
    pub lease_expires_at: Option<String>,
    /// Seconds left on the claim, so you can decide whether waiting is
    /// reasonable without doing the arithmetic. Zero means it has lapsed.
    pub lease_seconds_remaining: Option<i64>,
    /// True when the claim has already lapsed.
    pub lease_expired: bool,
    pub result: Option<String>,
    #[schemars(schema_with = "any_json_schema")]
    pub metadata: serde_json::Value,
    /// Files attached to this task; fetch content with get_attachment.
    pub attachments: Vec<AttachmentMeta>,
    pub created_by: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct TaskList {
    pub tasks: Vec<TaskInfo>,
    pub open: i64,
    pub claimed: i64,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ClaimResult {
    pub claimed: bool,
    pub task: Option<TaskInfo>,
    /// Present when `claimed` is false: why the claim did not succeed.
    pub reason: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct TaskEventInfo {
    pub event: String,
    pub agent: Option<String>,
    pub detail: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct TaskDetail {
    pub task: TaskInfo,
    pub history: Vec<TaskEventInfo>,
}

// ------------------------------------------------------------------ notes --

#[derive(Debug, Serialize, JsonSchema)]
pub struct NoteInfo {
    pub scope: String,
    pub key: String,
    pub value: String,
    pub tags: Vec<String>,
    pub updated_by: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct NoteList {
    pub notes: Vec<NoteInfo>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct NoteRef {
    pub scope: String,
    pub key: String,
    pub found: bool,
    pub note: Option<NoteInfo>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct Ack {
    pub ok: bool,
    pub detail: String,
}

// ------------------------------------------------------------------ locks --

#[derive(Debug, Serialize, JsonSchema)]
pub struct LockInfo {
    pub name: String,
    pub holder: String,
    /// Which of the holder's working contexts took it; `null` is their shared
    /// session. A lock belongs to a session — your own other session cannot
    /// release it or take it over while it is live.
    pub holder_session: Option<String>,
    pub purpose: Option<String>,
    pub acquired_at: String,
    /// When the lock lapses on its own if not renewed.
    pub expires_at: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct LockList {
    pub locks: Vec<LockInfo>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct LockResult {
    pub acquired: bool,
    pub lock: Option<LockInfo>,
    /// Present when `acquired` is false: who holds it and until when.
    pub reason: Option<String>,
}

// ----------------------------------------------------------------- events --

#[derive(Debug, Serialize, JsonSchema)]
pub struct WaitEvent {
    /// One of `message`, `task`, `lock`, `note`.
    pub kind: String,
    /// Human-readable one-liner of what happened.
    pub summary: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct WaitResult {
    /// True when something happened; false when the timeout elapsed quietly.
    pub woke: bool,
    pub timed_out: bool,
    pub events: Vec<WaitEvent>,
    /// Unread direct messages after the wait — if > 0, call read_messages.
    pub unread_direct_messages: i64,
    /// What to do next, e.g. which tool to call to fetch the details.
    pub suggestion: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct AskResult {
    /// True when the teammate answered before the timeout.
    pub answered: bool,
    /// The agent the question was addressed to.
    pub to: String,
    /// Id of the question message. On timeout, pass it back as
    /// `resume_message_id` to keep waiting without re-sending the question.
    pub question_message_id: i64,
    /// The answer: their reply to the question, or failing that their first
    /// direct message to you after it.
    pub answer: Option<MessageInfo>,
    /// What to do next.
    pub suggestion: String,
}

// ----------------------------------------------------------------- digest --

#[derive(Debug, Serialize, JsonSchema)]
pub struct DigestMessage {
    pub from: String,
    pub body: String,
    pub at: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct DigestChannel {
    pub name: String,
    pub message_count: i64,
    pub last_messages: Vec<DigestMessage>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct DigestTask {
    pub key: String,
    pub title: String,
    pub status: String,
    pub claimed_by: Option<String>,
    pub result: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct DigestNote {
    pub scope: String,
    pub key: String,
    pub updated_by: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct DigestAgent {
    pub name: String,
    pub activity: Option<String>,
    pub last_seen: Option<String>,
    pub online: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct DigestResult {
    /// Window covered, in hours.
    pub hours: i64,
    pub channels: Vec<DigestChannel>,
    /// Tasks whose state changed inside the window, newest first.
    pub tasks_moved: Vec<DigestTask>,
    pub open_tasks: i64,
    pub claimed_tasks: i64,
    pub notes_updated: Vec<DigestNote>,
    pub agents_seen: Vec<DigestAgent>,
    pub active_locks: Vec<LockInfo>,
}
