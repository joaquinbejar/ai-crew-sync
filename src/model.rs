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
    /// Discovery labels this session last published with `heartbeat`
    /// (`project`, `role`). Absent until set, so a client that never labels
    /// its windows sees exactly the response it saw before.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub project: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub role: Option<String>,
    /// Set when this connection authenticated with a session credential:
    /// the session label above is then *proven*, not merely asserted in a
    /// header. `null` means a plain agent token with a header label, which
    /// is still how every existing client connects.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub session_identity: Option<SessionIdentity>,
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
    /// Which row the summary describes, in order: a **live** session before a
    /// dead one, a **named** session before the shared one, then the most
    /// recently updated. Live comes first deliberately — a named session that
    /// died days ago should not outrank a shared row that is active now — so
    /// the shared row can win while every named session is offline. Read
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
    /// Discovery labels the session set about itself (see `list_sessions`).
    /// Absent when never set.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub project: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub role: Option<String>,
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

/// A session credential as the caller receives it. `session_token` is the
/// secret and appears exactly once, on registration.
#[derive(Debug, Serialize, JsonSchema)]
pub struct SessionCredential {
    /// The credential. Store it in a private file (0600) and send it as the
    /// bearer token from now on; it is not shown again. Absent on a renewal,
    /// which extends the credential you already hold.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub session_token: Option<String>,
    pub session_id: String,
    /// The label this credential authenticates as.
    pub session: String,
    /// What a teammate puts in `to` to reach exactly this window.
    pub address: String,
    /// Connection epoch. Send it as `X-Crew-Epoch` to be fenced off cleanly
    /// if another process resumes this window after you.
    pub epoch: i64,
    pub expires_at: String,
    pub expires_in_seconds: i64,
}

/// What a session credential proves, reported by `whoami`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct SessionIdentity {
    pub session_id: String,
    pub epoch: i64,
    pub registered_at: String,
    pub expires_at: String,
    pub expires_in_seconds: i64,
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
    /// Discovery labels this session set about itself. Absent when never set.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub project: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub role: Option<String>,
    pub last_seen: Option<String>,
    pub online: bool,
}

/// One session as `list_sessions` reports it: addressable, with its
/// discovery labels. Two sessions may share every label and still be two
/// entries, because the address differs.
#[derive(Debug, Serialize, JsonSchema)]
pub struct SessionEntry {
    pub agent: String,
    /// Absent for the shared session.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub session: Option<String>,
    /// What to put in `to` (or `ask_agent`'s `to`) to reach this session.
    /// `agent/session` for a named one; the bare `agent` for the shared
    /// session — and see `exact` before treating that as private.
    pub address: String,
    /// True when `address` reaches **this window and no other**. False for
    /// the shared session, whose address is the bare agent name: that is a
    /// broadcast to every window of that agent, named ones included, so it
    /// is the wrong place to send a private instruction. There is no address
    /// that reaches the shared session alone.
    pub exact: bool,
    pub project: Option<String>,
    pub role: Option<String>,
    pub repo: Option<String>,
    pub branch: Option<String>,
    pub activity: Option<String>,
    /// One of `active`, `idle`, `busy`, `blocked`, `offline`.
    pub status: String,
    pub online: bool,
    pub last_seen: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct SessionList {
    pub sessions: Vec<SessionEntry>,
    /// Sessions returned. When it equals the limit there may be more: narrow
    /// with `project` or `role`.
    pub count: usize,
    pub limit: i64,
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
    /// One of `open`, `claimed`, `done`, `cancelled`. A claim whose lease
    /// lapsed reads as `open`: anyone may take it, the former holder
    /// included (see `lapsed_holder`).
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
    /// reasonable without doing the arithmetic.
    pub lease_seconds_remaining: Option<i64>,
    /// True when the last claim lapsed and nobody has claimed the task
    /// since: it is `open`, and `lapsed_holder` says who let it go.
    pub lease_expired: bool,
    /// Who held the claim that lapsed, while the task stays unclaimed. Not a
    /// holder: nobody has to be asked before claiming it.
    pub lapsed_holder: Option<String>,
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

// ----------------------------------------------------------- conversations --

/// A project: the unit a conversation can be visible to. Access is an
/// explicit grant, never inferred from a directory or a role label.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ProjectInfo {
    pub id: String,
    pub name: String,
    /// Agents with an explicit grant. Only visible to someone who has one.
    pub members: Vec<String>,
    pub archived: bool,
    pub created_at: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ProjectList {
    pub projects: Vec<ProjectInfo>,
}

/// One conversation as a caller sees it.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ConversationInfo {
    pub id: String,
    pub title: String,
    /// `project` (everyone with access to the project can read it) or
    /// `private` (only its members). Fixed at creation.
    pub visibility: String,
    /// Absent for a private conversation.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub project: Option<String>,
    pub created_by: String,
    pub created_at: String,
    pub archived: bool,
    /// Highest logical sequence in the thread. Messages are paged by this.
    pub last_seq: i64,
    /// Your own membership, when you have one.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub membership: Option<MembershipInfo>,
    /// Everyone in the thread. Only returned to a member.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub members: Vec<MembershipInfo>,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct MembershipInfo {
    pub membership_id: String,
    pub agent: String,
    /// Absent for the shared session.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub session: Option<String>,
    /// `agent/session`, or the bare agent for the shared session.
    pub address: String,
    /// `owner`, `moderator`, `participant` or `observer`.
    pub role: String,
    /// `invited`, `active`, `left` or `removed`.
    pub state: String,
    /// Lowest sequence this member may read; `null` means from the start.
    pub history_from_seq: Option<i64>,
    pub invited_at: String,
    pub accepted_at: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ConversationList {
    pub conversations: Vec<ConversationInfo>,
}

/// What a send returns. `stored` is a fact about persistence, not about
/// anyone having read anything.
#[derive(Debug, Serialize, JsonSchema)]
pub struct SentMessage {
    pub message_id: String,
    pub conversation_id: String,
    pub seq: i64,
    /// True when the authoritative backend confirmed persistence. This
    /// transaction committed.
    pub stored: bool,
    /// Who the message was addressed to, snapshotted now. A later join never
    /// enters this list.
    pub recipients: Vec<String>,
    pub created_at: String,
}

/// One message of a thread.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ConversationMessage {
    pub message_id: String,
    pub seq: i64,
    pub from: String,
    /// `agent/session` of the sender, for an exact reply.
    pub from_address: String,
    /// The text the sender wrote, or an EMPTY STRING when `unavailable` is
    /// set. Check `unavailable` before quoting or summarising: an empty
    /// body with a reason there is a body this process cannot serve right
    /// now, not an empty message from your teammate.
    pub body: String,
    pub reply_to: Option<String>,
    pub metadata: serde_json::Value,
    pub created_at: String,
    /// Your own observations on this message, when you are a recipient.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub my_receipt: Option<ReceiptInfo>,
    /// Where this body stands with the backend that holds it: `stored`
    /// normally, and `pending_publication`, `failed` or `tombstoned` when
    /// the body is not here. A thread on the default Postgres backend is
    /// always `stored`.
    pub publication: String,
    /// Always present. `null` when `body` is the real text; otherwise why
    /// the body is not here (its backend cannot be reached, or no longer
    /// holds it), and `body` is an empty placeholder. The message keeps its
    /// place in the sequence, its sender and its receipts either way: a gap
    /// you can see and read about is not the same as a gap.
    #[serde(default)]
    pub unavailable: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ConversationRead {
    pub conversation_id: String,
    pub messages: Vec<ConversationMessage>,
    /// Pass as `after_seq` to continue. Absent when the thread is exhausted.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub next_after_seq: Option<i64>,
    /// Lowest sequence you may read in this thread.
    pub history_from_seq: Option<i64>,
}

/// One reference to a message, as a recipient's inbox hands it over. It
/// carries no body: the body is read separately, with a current access
/// check at that moment.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct InboxReference {
    /// Pass this to `confirm_inbox_delivery` once you hold the reference
    /// durably. Until you do, nothing has been marked delivered.
    pub delivery_id: String,
    pub message_id: String,
    pub conversation_id: String,
    pub seq: i64,
    pub from: String,
    /// `agent/session` of the sender, for an exact reply.
    pub from_address: String,
    pub created_at: String,
    /// True when this reference has been offered before: an earlier
    /// confirmation was lost, or the process holding it went away. Handling
    /// it twice must change nothing.
    pub redelivered: bool,
    /// `broker` when it came from the durable inbox, `bus` when it was
    /// rebuilt from the bus's own records after an expiry, a deleted
    /// consumer, or a team that is not routed to a broker at all.
    pub source: String,
    /// `message` — this message was addressed to you — or `receipt`: a
    /// message *you sent* has a receipt worth reading again. A receipt
    /// reference never means someone read anything; `get_message_receipts`
    /// says what actually happened.
    pub kind: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct InboxBatch {
    pub references: Vec<InboxReference>,
    /// How many of them the broker supplied. The rest were rebuilt from the
    /// bus's own records, which are the authority.
    pub from_broker: i64,
    /// True when the batch filled: call again.
    pub more: bool,
    /// Present when something is worth saying about where these came from —
    /// an unreachable broker, a missing consumer, a team on Postgres.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub note: Option<String>,
}

/// What one window's inbox holds. The two sides are reported separately on
/// purpose: they answer different questions, and averaging them would hide
/// exactly the case worth seeing.
#[derive(Debug, Serialize, JsonSchema)]
pub struct InboxState {
    pub address: String,
    /// Messages addressed to this window that it has never confirmed
    /// holding. The authoritative number.
    pub undelivered: i64,
    /// References handed to a process that has not confirmed them. A
    /// non-zero number here after a crash is expected: they are offered
    /// again.
    pub handed_out_unconfirmed: i64,
    /// What the broker still holds for this window, when there is one.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub broker_pending: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub broker_awaiting_ack: Option<i64>,
    /// False means the durable consumer is gone (expired, or removed).
    /// That is not an empty inbox: the bus's own records still have it.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub broker_consumer_present: Option<bool>,
}

/// Five independent observations. An absent timestamp means *not observed*,
/// never "assumed": a cursor moving is not a person reading, and a host that
/// cannot confirm injection leaves `presented_at` null.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct ReceiptInfo {
    pub agent: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub session: Option<String>,
    pub address: String,
    pub stored_at: Option<String>,
    pub delivered_at: Option<String>,
    /// Null when the host cannot confirm the message reached the model. That
    /// is unknown, not "no".
    pub presented_at: Option<String>,
    pub acknowledged_at: Option<String>,
    /// The recipient said it acted on this. It does not complete a task or
    /// merge anything by itself.
    pub resolved_at: Option<String>,
    pub note: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct MessageReceipts {
    pub message_id: String,
    pub seq: i64,
    /// One entry per recipient at acceptance time.
    pub receipts: Vec<ReceiptInfo>,
    pub acknowledged: usize,
    pub resolved: usize,
    pub total: usize,
}

/// What `wait_for_conversation_updates` reports.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ConversationUpdates {
    pub conversations: Vec<ConversationActivity>,
    pub waited_seconds: i64,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ConversationActivity {
    pub conversation_id: String,
    pub title: String,
    /// Highest sequence currently stored in the thread. It is not a read
    /// cursor: it does not move with what you have read or acknowledged.
    pub last_seq: i64,
    /// Messages addressed to you that you have not acknowledged.
    pub unacknowledged: i64,
}

/// Result of a membership transfer proposal or acceptance.
#[derive(Debug, Serialize, JsonSchema)]
pub struct TransferResult {
    pub conversation_id: String,
    /// The membership that will be superseded once the target accepts.
    pub from_address: String,
    pub to_address: String,
    pub state: String,
}
