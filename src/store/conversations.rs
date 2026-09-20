//! Conversations: addressed threads with per-recipient receipts.
//!
//! Every function here answers one of two questions before it does anything
//! else: *may this caller see this thread?* and *may this caller change it?*
//! Both are answered from the credential and from rows, never from a label a
//! caller supplied. A project grant is a row. A private membership is a row.
//! A role a session published for discovery grants nothing at all.
//!
//! Three properties the implementation exists to keep (ADR 0001):
//!
//! 1. **Recipients are snapshotted at acceptance.** `message_recipients` is
//!    written with the message, so a later join never enters an older
//!    message's denominator and a removal never erases a receipt.
//! 2. **Receipts are observations, never inferences.** Reading a thread does
//!    not acknowledge it; a cursor is not a person. `presented_at` stays null
//!    where the host cannot confirm injection, because unknown is not "no".
//! 3. **The exceptional paths are explicit and audited.** A membership
//!    transfer needs the target to accept; owner recovery needs every session
//!    of that agent to be closed, and is read-only.

use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    auth::AuthCtx,
    error::{BusError, BusResult},
    model::{
        ConversationActivity, ConversationInfo, ConversationMessage, ConversationRead,
        MembershipInfo, MessageReceipts, ProjectInfo, ReceiptInfo, SentMessage, TransferResult, ts,
        ts_opt,
    },
    store::backend::MessagingBackend,
};

/// Longest conversation title and project name. Identifiers people type.
pub const MAX_TITLE_BYTES: usize = 200;
/// Longest message body. The same ceiling channel messages have.
pub const MAX_BODY_BYTES: usize = 1024 * 1024;
/// Most messages one read returns.
pub const MAX_PAGE: i64 = 200;
pub const DEFAULT_PAGE: i64 = 50;
/// Members one conversation may hold. A thread is addressed, not broadcast.
pub const MAX_MEMBERS: i64 = 200;

/// Refuse early and clearly when the team has not turned conversations on.
/// The tools are advertised only to teams that have, but a direct call must
/// be refused too: a catalogue is not an authorization boundary.
/// Whether this caller's team has conversations turned on. Used to decide
/// what to advertise; `require_capability` is what decides what to allow.
pub async fn capability_enabled(pool: &PgPool, auth: &AuthCtx) -> BusResult<bool> {
    let enabled: Option<(bool,)> =
        sqlx::query_as("SELECT conversations_enabled FROM teams WHERE id = $1")
            .bind(auth.team_id)
            .fetch_optional(pool)
            .await?;
    Ok(matches!(enabled, Some((true,))))
}

pub async fn require_capability(pool: &PgPool, auth: &AuthCtx) -> BusResult<()> {
    let enabled: Option<(bool,)> =
        sqlx::query_as("SELECT conversations_enabled FROM teams WHERE id = $1")
            .bind(auth.team_id)
            .fetch_optional(pool)
            .await?;
    match enabled {
        Some((true,)) => Ok(()),
        _ => Err(BusError::Forbidden(
            "conversations are not enabled for this team. An operator turns them on with \
             `ai-crew-sync team capability --team <slug> --conversations on`; until then use \
             channels and direct messages."
                .to_owned(),
        )),
    }
}

fn address_of(agent: &str, session: &str) -> String {
    if session.is_empty() {
        agent.to_owned()
    } else {
        format!("{agent}/{session}")
    }
}

fn check_title(field: &str, raw: &str) -> BusResult<String> {
    let value = raw.trim();
    if value.is_empty() {
        return Err(BusError::invalid(format!("{field} cannot be empty")));
    }
    if value.len() > MAX_TITLE_BYTES {
        return Err(BusError::invalid(format!(
            "{field} is {} bytes; the limit is {MAX_TITLE_BYTES}",
            value.len()
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(BusError::invalid(format!(
            "{field} must not contain control characters"
        )));
    }
    Ok(value.to_owned())
}

// ----------------------------------------------------------------- projects --

pub async fn create_project(pool: &PgPool, auth: &AuthCtx, name: &str) -> BusResult<ProjectInfo> {
    require_capability(pool, auth).await?;
    let name = crate::store::presence::normalize_label("project name", name)?;
    if name.is_empty() {
        return Err(BusError::invalid("a project name is required"));
    }
    let mut tx = pool.begin().await?;
    crate::store::sessions::guard(&mut tx, auth).await?;
    let row: Option<(Uuid,)> = sqlx::query_as(
        "INSERT INTO projects (team_id, name, created_by) VALUES ($1, $2, $3)
         ON CONFLICT (team_id, name) DO NOTHING RETURNING id",
    )
    .bind(auth.team_id)
    .bind(&name)
    .bind(auth.agent_id)
    .fetch_optional(&mut *tx)
    .await?;
    let id = match row {
        Some((id,)) => {
            // The creator has access to what they created; everyone else
            // needs a grant.
            sqlx::query(
                "INSERT INTO project_agent_access (project_id, agent_id, team_id, granted_by)
                 VALUES ($1, $2, $3, $2) ON CONFLICT DO NOTHING",
            )
            .bind(id)
            .bind(auth.agent_id)
            .bind(auth.team_id)
            .execute(&mut *tx)
            .await?;
            audit(
                &mut tx,
                auth,
                None,
                "project.create",
                Some(id),
                serde_json::json!({ "name": name }),
            )
            .await?;
            id
        }
        None => {
            // Already exists. Say so rather than silently adopting it: a
            // caller that expected to create it should know it did not.
            let (id,): (Uuid,) =
                sqlx::query_as("SELECT id FROM projects WHERE team_id = $1 AND name = $2")
                    .bind(auth.team_id)
                    .bind(&name)
                    .fetch_one(&mut *tx)
                    .await?;
            id
        }
    };
    tx.commit().await?;
    project_info(pool, auth, id).await
}

/// A project as someone with access sees it. Refused to anyone without.
pub async fn project_info(pool: &PgPool, auth: &AuthCtx, id: Uuid) -> BusResult<ProjectInfo> {
    let row: Option<(
        String,
        Option<chrono::DateTime<chrono::Utc>>,
        chrono::DateTime<chrono::Utc>,
    )> = sqlx::query_as(
        "SELECT p.name, p.archived_at, p.created_at
               FROM projects p
              WHERE p.id = $1 AND p.team_id = $2
                AND EXISTS (SELECT 1 FROM project_agent_access a
                             WHERE a.project_id = p.id AND a.agent_id = $3)",
    )
    .bind(id)
    .bind(auth.team_id)
    .bind(auth.agent_id)
    .fetch_optional(pool)
    .await?;
    let Some((name, archived_at, created_at)) = row else {
        return Err(BusError::not_found(
            "no such project, or you have no access to it",
        ));
    };
    let members: Vec<(String,)> = sqlx::query_as(
        "SELECT ag.name FROM project_agent_access a
           JOIN agents ag ON ag.id = a.agent_id
          WHERE a.project_id = $1 ORDER BY ag.name",
    )
    .bind(id)
    .fetch_all(pool)
    .await?;
    Ok(ProjectInfo {
        id: id.to_string(),
        name,
        members: members.into_iter().map(|m| m.0).collect(),
        archived: archived_at.is_some(),
        created_at: ts(created_at),
    })
}

/// Grant or revoke a teammate's access to a project. Only someone who
/// already has access may extend it, which keeps the grant chain inside the
/// project rather than making it an administrative power.
pub async fn set_project_access(
    pool: &PgPool,
    auth: &AuthCtx,
    project: &str,
    agent: &str,
    grant: bool,
) -> BusResult<ProjectInfo> {
    require_capability(pool, auth).await?;
    let project_id = project_id_for(pool, auth, project).await?;
    let target = crate::store::agent_id_by_name(pool, auth.team_id, agent).await?;
    let mut tx = pool.begin().await?;
    crate::store::sessions::guard(&mut tx, auth).await?;
    // The caller's own grant is re-read and locked *inside* this
    // transaction. Checked only before it, a revoke can commit in between
    // and the revoked caller still hands access to somebody else — while
    // the tool promises a revocation takes effect immediately.
    let still_mine: Option<(Uuid,)> = sqlx::query_as(
        "SELECT project_id FROM project_agent_access
          WHERE project_id = $1 AND agent_id = $2 FOR UPDATE",
    )
    .bind(project_id)
    .bind(auth.agent_id)
    .fetch_optional(&mut *tx)
    .await?;
    if still_mine.is_none() {
        return Err(BusError::Forbidden(
            "your access to this project has been revoked, so you cannot change anyone              else's. Nothing was written."
                .to_owned(),
        ));
    }
    if grant {
        sqlx::query(
            "INSERT INTO project_agent_access (project_id, agent_id, team_id, granted_by)
             VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
        )
        .bind(project_id)
        .bind(target)
        .bind(auth.team_id)
        .bind(auth.agent_id)
        .execute(&mut *tx)
        .await?;
    } else {
        if target == auth.agent_id {
            return Err(BusError::invalid(
                "you cannot revoke your own access; ask another member to do it",
            ));
        }
        sqlx::query("DELETE FROM project_agent_access WHERE project_id = $1 AND agent_id = $2")
            .bind(project_id)
            .bind(target)
            .execute(&mut *tx)
            .await?;
    }
    audit(
        &mut tx,
        auth,
        None,
        if grant {
            "project.grant"
        } else {
            "project.revoke"
        },
        Some(target),
        serde_json::json!({ "agent": agent, "project": project }),
    )
    .await?;
    tx.commit().await?;
    project_info(pool, auth, project_id).await
}

/// Resolve a project the caller has access to, by name or id.
async fn project_id_for(pool: &PgPool, auth: &AuthCtx, project: &str) -> BusResult<Uuid> {
    let by_id = project.parse::<Uuid>().ok();
    let row: Option<(Uuid,)> = sqlx::query_as(
        "SELECT p.id FROM projects p
          WHERE p.team_id = $1
            AND ($2::uuid IS NOT NULL AND p.id = $2 OR p.name = $3)
            AND EXISTS (SELECT 1 FROM project_agent_access a
                         WHERE a.project_id = p.id AND a.agent_id = $4)",
    )
    .bind(auth.team_id)
    .bind(by_id)
    .bind(project.trim().to_lowercase())
    .bind(auth.agent_id)
    .fetch_optional(pool)
    .await?;
    row.map(|r| r.0).ok_or_else(|| {
        BusError::not_found(format!(
            "no project '{project}' you have access to. Create it with create_project, or ask \
             a member to grant you access."
        ))
    })
}

pub async fn list_projects(pool: &PgPool, auth: &AuthCtx) -> BusResult<Vec<ProjectInfo>> {
    require_capability(pool, auth).await?;
    let ids: Vec<(Uuid,)> = sqlx::query_as(
        "SELECT p.id FROM projects p
           JOIN project_agent_access a ON a.project_id = p.id AND a.agent_id = $2
          WHERE p.team_id = $1 ORDER BY p.name",
    )
    .bind(auth.team_id)
    .bind(auth.agent_id)
    .fetch_all(pool)
    .await?;
    let mut out = Vec::with_capacity(ids.len());
    for (id,) in ids {
        out.push(project_info(pool, auth, id).await?);
    }
    Ok(out)
}

// ------------------------------------------------------------------ access --

/// What a caller may do in a conversation, resolved from rows alone.
#[derive(Clone, Debug)]
pub struct Access {
    pub conversation_id: Uuid,
    pub visibility: String,
    pub archived: bool,
    pub last_seq: i64,
    pub title: String,
    /// Present when the caller is (or was) a member of this thread.
    pub membership: Option<Membership>,
    /// True when the caller can read by project access rather than
    /// membership.
    pub by_project: bool,
}

#[derive(Clone, Debug)]
pub struct Membership {
    pub id: Uuid,
    pub role: String,
    pub state: String,
    pub history_from_seq: Option<i64>,
}

impl Access {
    pub fn can_read(&self) -> bool {
        self.by_project
            || matches!(
                self.membership.as_ref().map(|m| m.state.as_str()),
                Some("active") | Some("invited")
            )
    }
    /// Reading the thread's *contents*, which an invitation does not grant.
    ///
    /// An invitee can see that a thread exists and who invited them — that
    /// is what they are deciding about — and nothing that was said in it
    /// until they accept. Otherwise an invitation would be a way to read a
    /// private thread without ever joining it, which is the opposite of
    /// what "a thread cannot conscript a window" means.
    pub fn can_read_messages(&self) -> bool {
        self.by_project
            || matches!(
                self.membership.as_ref().map(|m| m.state.as_str()),
                Some("active")
            )
    }
    /// Observers read and acknowledge; they do not write.
    pub fn can_send(&self) -> bool {
        matches!(
            self.membership
                .as_ref()
                .map(|m| (m.state.as_str(), m.role.as_str())),
            Some(("active", "owner"))
                | Some(("active", "moderator"))
                | Some(("active", "participant"))
        )
    }
    pub fn can_moderate(&self) -> bool {
        matches!(
            self.membership
                .as_ref()
                .map(|m| (m.state.as_str(), m.role.as_str())),
            Some(("active", "owner")) | Some(("active", "moderator"))
        )
    }
    pub fn is_owner(&self) -> bool {
        matches!(
            self.membership
                .as_ref()
                .map(|m| (m.state.as_str(), m.role.as_str())),
            Some(("active", "owner"))
        )
    }
}

/// Resolve what this caller may do with `conversation`, by id.
pub async fn access(pool: &PgPool, auth: &AuthCtx, conversation: Uuid) -> BusResult<Access> {
    let row: Option<(
        String,
        String,
        Option<chrono::DateTime<chrono::Utc>>,
        i64,
        Option<Uuid>,
        bool,
    )> = sqlx::query_as(
        "SELECT c.visibility, c.title, c.archived_at, c.last_seq, c.project_id,
                COALESCE(
                  -- Only a project-visible thread is readable by project
                  -- grant. A private thread that also names a project is
                  -- still private: the grant governs the project, not
                  -- everything that mentions it.
                  c.visibility = 'project' AND c.project_id IS NOT NULL AND EXISTS (
                    SELECT 1 FROM project_agent_access a
                     WHERE a.project_id = c.project_id AND a.agent_id = $3), false)
           FROM conversations c
          WHERE c.id = $1 AND c.team_id = $2",
    )
    .bind(conversation)
    .bind(auth.team_id)
    .bind(auth.agent_id)
    .fetch_optional(pool)
    .await?;
    let Some((visibility, title, archived_at, last_seq, _project, by_project)) = row else {
        // Same answer for "not in this team" and "does not exist": a caller
        // must not learn that another team has a conversation with this id.
        return Err(BusError::not_found("no such conversation"));
    };
    // A seat taken by a registered window belongs to that window, and the
    // label in a header is a name rather than a proof. The parent agent
    // token cannot sit in its own window's chair — that is what the audited
    // `recover_conversation_history` exists for. A legacy seat (session_id
    // NULL) keeps matching by label, as it always did.
    let membership: Option<(Uuid, String, String, Option<i64>)> = sqlx::query_as(
        "SELECT id, role, state, history_from_seq FROM conversation_memberships
          WHERE conversation_id = $1 AND agent_id = $2 AND session = $3
            AND (session_id IS NULL OR $4::uuid IS NOT NULL)",
    )
    .bind(conversation)
    .bind(auth.agent_id)
    .bind(&auth.session)
    .bind(auth.session_id)
    .fetch_optional(pool)
    .await?;
    Ok(Access {
        conversation_id: conversation,
        visibility,
        title,
        archived: archived_at.is_some(),
        last_seq,
        by_project,
        membership: membership.map(|(id, role, state, history_from_seq)| Membership {
            id,
            role,
            state,
            history_from_seq,
        }),
    })
}

/// Resolve and require read access in one step.
/// What a retry is compared against. The body is staged in this row only
/// until its backend confirms it; the digest stays.
fn body_digest(body: &str) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(body.as_bytes()))
}

/// Refuse a content read to a seat that has not been accepted.
fn require_accepted(a: &Access) -> BusResult<()> {
    if a.can_read_messages() {
        return Ok(());
    }
    Err(BusError::Forbidden(
        "you have been invited to this conversation and have not accepted. Call \
         join_conversation first; an invitation is not membership, and it does not read \
         what was said before you answered it."
            .to_owned(),
    ))
}

async fn readable(pool: &PgPool, auth: &AuthCtx, conversation: Uuid) -> BusResult<Access> {
    let a = access(pool, auth, conversation).await?;
    if !a.can_read() {
        // A private conversation must not confirm its own existence to a
        // non-member.
        return Err(BusError::not_found("no such conversation"));
    }
    Ok(a)
}

async fn audit(
    tx: &mut sqlx::PgConnection,
    auth: &AuthCtx,
    conversation: Option<Uuid>,
    action: &str,
    subject: Option<Uuid>,
    detail: serde_json::Value,
) -> BusResult<()> {
    sqlx::query(
        "INSERT INTO conversation_audit
            (team_id, conversation_id, actor_agent, actor_session, action, subject, detail)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(auth.team_id)
    .bind(conversation)
    .bind(auth.agent_id)
    .bind(&auth.session)
    .bind(action)
    .bind(subject)
    .bind(detail)
    .execute(tx)
    .await?;
    Ok(())
}

// ------------------------------------------------------------ conversations --

pub struct CreateInput {
    pub title: String,
    pub project: Option<String>,
    pub private: bool,
    pub invite: Vec<String>,
}

pub async fn create_conversation(
    pool: &PgPool,
    auth: &AuthCtx,
    input: CreateInput,
) -> BusResult<ConversationInfo> {
    require_capability(pool, auth).await?;
    let title = check_title("title", &input.title)?;
    let project_id = match (&input.project, input.private) {
        (Some(_), true) => {
            // Both would mean "members only" and "everyone with the project
            // grant" at once, and one of the two would be a lie to whoever
            // spoke in it.
            return Err(BusError::invalid(
                "a conversation is either private or visible to a project, not both. Drop                  `project` for a members-only thread, or `private` for a project one.",
            ));
        }
        (Some(p), false) => Some(project_id_for(pool, auth, p).await?),
        (None, true) => None,
        (None, false) => {
            return Err(BusError::invalid(
                "a project conversation needs `project`; pass `private: true` for a thread \
                 only its members can see",
            ));
        }
    };
    let visibility = if input.private { "private" } else { "project" };

    // Resolve the invitees before opening the transaction: a name that does
    // not exist should not leave a half-made thread.
    let mut invites = Vec::new();
    for raw in &input.invite {
        let (agent, session) = crate::store::messaging::parse_address(raw)?;
        let agent_id = crate::store::agent_id_by_name(pool, auth.team_id, &agent).await?;
        invites.push((agent_id, session.unwrap_or_default(), raw.clone()));
    }
    if invites.len() as i64 > MAX_MEMBERS {
        return Err(BusError::invalid(format!(
            "a conversation holds at most {MAX_MEMBERS} members"
        )));
    }

    let mut tx = pool.begin().await?;
    crate::store::sessions::guard(&mut tx, auth).await?;
    // The backend is the team's current routing, captured at creation: a
    // thread never changes backend once it holds messages, because half a
    // history in each place is the one shape nobody can read.
    let (id,): (Uuid,) = sqlx::query_as(
        "INSERT INTO conversations
            (team_id, project_id, visibility, title, created_by, created_session,
             backend, publication)
         SELECT $1, $2, $3, $4, $5, $6, t.default_backend,
                CASE WHEN t.default_backend = 'postgres' THEN 'sync' ELSE 'outbox' END
           FROM teams t WHERE t.id = $1
         RETURNING id",
    )
    .bind(auth.team_id)
    .bind(project_id)
    .bind(visibility)
    .bind(&title)
    .bind(auth.agent_id)
    .bind(&auth.session)
    .fetch_one(&mut *tx)
    .await?;

    // The creator owns it, from the beginning.
    sqlx::query(
        "INSERT INTO conversation_memberships
            (conversation_id, agent_id, session, session_id, role, state, history_from_seq,
             invited_by, accepted_at)
         VALUES ($1, $2, $3, $4, 'owner', 'active', NULL, $2, now())",
    )
    .bind(id)
    .bind(auth.agent_id)
    .bind(&auth.session)
    .bind(auth.session_id)
    .execute(&mut *tx)
    .await?;

    for (agent_id, session, raw) in &invites {
        sqlx::query(
            "INSERT INTO conversation_memberships
                (conversation_id, agent_id, session, role, state, history_from_seq, invited_by)
             VALUES ($1, $2, $3, 'participant', 'invited', 0, $4)
             ON CONFLICT (conversation_id, agent_id, session) DO NOTHING",
        )
        .bind(id)
        .bind(agent_id)
        .bind(session)
        .bind(auth.agent_id)
        .execute(&mut *tx)
        .await?;
        audit(
            &mut tx,
            auth,
            Some(id),
            "member.invite",
            Some(*agent_id),
            serde_json::json!({ "address": raw }),
        )
        .await?;
    }
    audit(
        &mut tx,
        auth,
        Some(id),
        "conversation.create",
        Some(id),
        serde_json::json!({ "title": title, "visibility": visibility }),
    )
    .await?;
    tx.commit().await?;
    conversation_info(pool, auth, id).await
}

pub async fn conversation_info(
    pool: &PgPool,
    auth: &AuthCtx,
    id: Uuid,
) -> BusResult<ConversationInfo> {
    let a = readable(pool, auth, id).await?;
    let row: (
        String,
        String,
        Option<String>,
        String,
        chrono::DateTime<chrono::Utc>,
        Option<chrono::DateTime<chrono::Utc>>,
        i64,
    ) = sqlx::query_as(
        "SELECT c.title, c.visibility, p.name, ag.name, c.created_at, c.archived_at, c.last_seq
           FROM conversations c
           LEFT JOIN projects p ON p.id = c.project_id
           JOIN agents ag ON ag.id = c.created_by
          WHERE c.id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await?;
    // Who is in the thread is the members' business. A project grant lets
    // you read a project thread; it does not tell you which windows of which
    // people are in it, with their roles and history boundaries.
    let members = if a.membership.is_some() {
        members_of(pool, id).await?
    } else {
        Vec::new()
    };
    let mine = a.membership.as_ref().and_then(|m| {
        members
            .iter()
            .find(|info| info.membership_id == m.id.to_string())
            .cloned()
    });
    Ok(ConversationInfo {
        id: id.to_string(),
        title: row.0,
        visibility: row.1,
        project: row.2,
        created_by: row.3,
        created_at: ts(row.4),
        archived: row.5.is_some(),
        last_seq: row.6,
        membership: mine,
        members,
    })
}

async fn members_of(pool: &PgPool, id: Uuid) -> BusResult<Vec<MembershipInfo>> {
    let rows: Vec<(
        Uuid,
        String,
        String,
        String,
        String,
        Option<i64>,
        chrono::DateTime<chrono::Utc>,
        Option<chrono::DateTime<chrono::Utc>>,
    )> = sqlx::query_as(
        "SELECT m.id, ag.name, m.session, m.role, m.state, m.history_from_seq,
                m.invited_at, m.accepted_at
           FROM conversation_memberships m
           JOIN agents ag ON ag.id = m.agent_id
          WHERE m.conversation_id = $1
          ORDER BY ag.name, m.session",
    )
    .bind(id)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(mid, agent, session, role, state, history_from_seq, invited_at, accepted_at)| {
                MembershipInfo {
                    membership_id: mid.to_string(),
                    address: address_of(&agent, &session),
                    session: (!session.is_empty()).then_some(session),
                    agent,
                    role,
                    state,
                    history_from_seq,
                    invited_at: ts(invited_at),
                    accepted_at: ts_opt(accepted_at),
                }
            },
        )
        .collect())
}

/// Every conversation this caller may read: their own memberships, plus the
/// project threads their grants cover.
pub async fn list_conversations(
    pool: &PgPool,
    auth: &AuthCtx,
    include_archived: bool,
) -> BusResult<Vec<ConversationInfo>> {
    require_capability(pool, auth).await?;
    let ids: Vec<(Uuid,)> = sqlx::query_as(
        "SELECT DISTINCT c.id
           FROM conversations c
           LEFT JOIN conversation_memberships m
                  ON m.conversation_id = c.id AND m.agent_id = $2 AND m.session = $3
          WHERE c.team_id = $1
            AND ($4::bool OR c.archived_at IS NULL)
            AND (
                 m.state IN ('invited', 'active')
                 OR (c.project_id IS NOT NULL AND EXISTS (
                        SELECT 1 FROM project_agent_access a
                         WHERE a.project_id = c.project_id AND a.agent_id = $2))
            )
          ORDER BY c.id",
    )
    .bind(auth.team_id)
    .bind(auth.agent_id)
    .bind(&auth.session)
    .bind(include_archived)
    .fetch_all(pool)
    .await?;
    let mut out = Vec::with_capacity(ids.len());
    for (id,) in ids {
        out.push(conversation_info(pool, auth, id).await?);
    }
    Ok(out)
}

pub async fn archive_conversation(
    pool: &PgPool,
    auth: &AuthCtx,
    id: Uuid,
) -> BusResult<ConversationInfo> {
    require_capability(pool, auth).await?;
    let a = readable(pool, auth, id).await?;
    if !a.can_moderate() {
        return Err(BusError::Forbidden(
            "only an owner or moderator of this conversation can archive it".to_owned(),
        ));
    }
    let mut tx = pool.begin().await?;
    crate::store::sessions::guard(&mut tx, auth).await?;
    sqlx::query(
        "UPDATE conversations SET archived_at = now() WHERE id = $1 AND archived_at IS NULL",
    )
    .bind(id)
    .execute(&mut *tx)
    .await?;
    audit(
        &mut tx,
        auth,
        Some(id),
        "conversation.archive",
        Some(id),
        serde_json::json!({}),
    )
    .await?;
    tx.commit().await?;
    conversation_info(pool, auth, id).await
}

// ------------------------------------------------------------- membership --

/// Invite an address into a conversation. The invitee is not a member until
/// it accepts: a thread cannot conscript a window into its receipts.
pub async fn invite(
    pool: &PgPool,
    auth: &AuthCtx,
    id: Uuid,
    address: &str,
    role: Option<&str>,
    history_from_start: bool,
) -> BusResult<ConversationInfo> {
    require_capability(pool, auth).await?;
    let a = readable(pool, auth, id).await?;
    if !a.can_moderate() {
        return Err(BusError::Forbidden(
            "only an owner or moderator of this conversation can invite".to_owned(),
        ));
    }
    if a.archived {
        return Err(BusError::conflict("this conversation is archived"));
    }
    let role = match role.map(str::trim).filter(|r| !r.is_empty()) {
        None => "participant".to_owned(),
        Some(r) => {
            let r = r.to_lowercase();
            if !["moderator", "participant", "observer"].contains(&r.as_str()) {
                return Err(BusError::invalid(
                    "role must be moderator, participant or observer. An owner is the \
                     creator, or someone a transfer made one.",
                ));
            }
            r
        }
    };
    let (agent, session) = crate::store::messaging::parse_address(address)?;
    let agent_id = crate::store::agent_id_by_name(pool, auth.team_id, &agent).await?;
    let session = session.unwrap_or_default();

    let mut tx = pool.begin().await?;
    crate::store::sessions::guard(&mut tx, auth).await?;
    // Lock the thread, then count. Read outside the transaction, two
    // concurrent invitations both see room and both commit, and the cap is
    // a suggestion.
    let (last_seq,): (i64,) =
        sqlx::query_as("SELECT last_seq FROM conversations WHERE id = $1 FOR UPDATE")
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
    let (count,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM conversation_memberships WHERE conversation_id = $1")
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
    if count >= MAX_MEMBERS {
        return Err(BusError::conflict(format!(
            "this conversation already holds {MAX_MEMBERS} members"
        )));
    }

    // Was this address removed from the thread? Re-admitting is a moderator
    // decision and stays allowed, but it is recorded as one, and it never
    // hands back the history the removal took away — whatever the inviter
    // asks for.
    let removed: Option<(String,)> = sqlx::query_as(
        "SELECT state FROM conversation_memberships
          WHERE conversation_id = $1 AND agent_id = $2 AND session = $3
          FOR UPDATE",
    )
    .bind(id)
    .bind(agent_id)
    .bind(&session)
    .fetch_optional(&mut *tx)
    .await?;
    let readmitting = removed.as_ref().map(|r| r.0.as_str()) == Some("removed");

    // History boundary: from the start only when an inviter with the right
    // to see it says so, otherwise from here on. Recorded either way. A
    // re-admission is always from here on.
    let from_seq: Option<i64> = if history_from_start && !readmitting {
        None
    } else {
        Some(last_seq)
    };
    sqlx::query(
        "INSERT INTO conversation_memberships
            (conversation_id, agent_id, session, role, state, history_from_seq, invited_by)
         VALUES ($1, $2, $3, $4, 'invited', $5, $6)
         ON CONFLICT (conversation_id, agent_id, session) DO UPDATE SET
            role = EXCLUDED.role,
            state = CASE WHEN conversation_memberships.state IN ('left', 'removed')
                         THEN 'invited' ELSE conversation_memberships.state END,
            history_from_seq = CASE WHEN conversation_memberships.state = 'removed'
                                    THEN EXCLUDED.history_from_seq
                                    ELSE conversation_memberships.history_from_seq END,
            -- A seat that is being re-offered is not the seat the old window
            -- held: whoever accepts has to prove it is them again.
            session_id = NULL,
            invited_by = EXCLUDED.invited_by,
            invited_at = now(),
            ended_at = NULL",
    )
    .bind(id)
    .bind(agent_id)
    .bind(&session)
    .bind(&role)
    .bind(from_seq)
    .bind(auth.agent_id)
    .execute(&mut *tx)
    .await?;
    audit(
        &mut tx,
        auth,
        Some(id),
        if readmitting {
            "member.readmit"
        } else {
            "member.invite"
        },
        Some(agent_id),
        serde_json::json!({ "address": address, "role": role, "history_from_seq": from_seq }),
    )
    .await?;
    tx.commit().await?;
    conversation_info(pool, auth, id).await
}

/// Accept an invitation. Only the invited window can: a sibling session of
/// the same agent is a different address and a different membership.
pub async fn join(pool: &PgPool, auth: &AuthCtx, id: Uuid) -> BusResult<ConversationInfo> {
    require_capability(pool, auth).await?;
    // If this label is a registered window, only that window may take the
    // seat. Otherwise the parent agent token could accept an invitation
    // addressed to one of its own windows and then read, send and
    // acknowledge as it — without the audit trail that the documented
    // recovery path carries.
    crate::store::sessions::require_window(pool, auth).await?;
    let mut tx = pool.begin().await?;
    crate::store::sessions::guard(&mut tx, auth).await?;
    let updated: Option<(Uuid,)> = sqlx::query_as(
        "UPDATE conversation_memberships
            SET state = 'active', accepted_at = now(), session_id = $4
          WHERE conversation_id = $1 AND agent_id = $2 AND session = $3
            AND state = 'invited'
          RETURNING id",
    )
    .bind(id)
    .bind(auth.agent_id)
    .bind(&auth.session)
    .bind(auth.session_id)
    .fetch_optional(&mut *tx)
    .await?;
    if updated.is_none() {
        let a = access(pool, auth, id).await?;
        return Err(match a.membership.as_ref().map(|m| m.state.as_str()) {
            Some("active") => BusError::conflict("you are already in this conversation"),
            Some("removed") => {
                BusError::Forbidden("you were removed from this conversation".to_owned())
            }
            _ => BusError::not_found(
                "no invitation for this window. An invitation is addressed to one \
                 agent/session; a sibling window cannot accept it for you.",
            ),
        });
    }
    if let Some((membership_id,)) = updated {
        // A pending transfer from another window of this agent completes
        // here: its seat is superseded rather than duplicated.
        supersede_predecessor(&mut tx, auth, id, membership_id).await?;
    }
    audit(
        &mut tx,
        auth,
        Some(id),
        "member.join",
        None,
        serde_json::json!({}),
    )
    .await?;
    tx.commit().await?;
    conversation_info(pool, auth, id).await
}

/// Leave a conversation. History and receipts stay exactly as they were.
pub async fn leave(pool: &PgPool, auth: &AuthCtx, id: Uuid) -> BusResult<()> {
    require_capability(pool, auth).await?;
    let mut tx = pool.begin().await?;
    crate::store::sessions::guard(&mut tx, auth).await?;
    let row: Option<(Uuid,)> = sqlx::query_as(
        "UPDATE conversation_memberships
            SET state = 'left', ended_at = now()
          WHERE conversation_id = $1 AND agent_id = $2 AND session = $3
            AND state IN ('invited', 'active')
          RETURNING id",
    )
    .bind(id)
    .bind(auth.agent_id)
    .bind(&auth.session)
    .fetch_optional(&mut *tx)
    .await?;
    if row.is_none() {
        return Err(BusError::not_found("you are not in this conversation"));
    }
    audit(
        &mut tx,
        auth,
        Some(id),
        "member.leave",
        None,
        serde_json::json!({}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

/// Remove someone else. A removal is effective immediately and is not
/// undone by a later transfer or recovery.
pub async fn remove_member(
    pool: &PgPool,
    auth: &AuthCtx,
    id: Uuid,
    address: &str,
) -> BusResult<ConversationInfo> {
    require_capability(pool, auth).await?;
    let a = readable(pool, auth, id).await?;
    if !a.can_moderate() {
        return Err(BusError::Forbidden(
            "only an owner or moderator of this conversation can remove a member".to_owned(),
        ));
    }
    let (agent, session) = crate::store::messaging::parse_address(address)?;
    let agent_id = crate::store::agent_id_by_name(pool, auth.team_id, &agent).await?;
    let session = session.unwrap_or_default();
    if agent_id == auth.agent_id && session == auth.session {
        return Err(BusError::invalid(
            "use leave_conversation to remove yourself",
        ));
    }
    let mut tx = pool.begin().await?;
    crate::store::sessions::guard(&mut tx, auth).await?;
    let row: Option<(String,)> = sqlx::query_as(
        "UPDATE conversation_memberships
            SET state = 'removed', ended_at = now()
          WHERE conversation_id = $1 AND agent_id = $2 AND session = $3
            AND state IN ('invited', 'active')
          RETURNING role",
    )
    .bind(id)
    .bind(agent_id)
    .bind(&session)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((role,)) = row else {
        return Err(BusError::not_found(format!(
            "'{address}' is not in this conversation"
        )));
    };
    if role == "owner" && !a.is_owner() {
        return Err(BusError::Forbidden(
            "only an owner can remove another owner".to_owned(),
        ));
    }
    audit(
        &mut tx,
        auth,
        Some(id),
        "member.remove",
        Some(agent_id),
        serde_json::json!({ "address": address }),
    )
    .await?;
    tx.commit().await?;
    conversation_info(pool, auth, id).await
}

// ---------------------------------------------------------------- messages --

pub struct SendInput {
    pub body: String,
    pub request_id: Uuid,
    pub reply_to: Option<Uuid>,
    pub metadata: Option<serde_json::Value>,
}

/// Send into a conversation.
///
/// Body, sequence, recipient snapshot, receipts and audit commit together:
/// `stored` is true because *this transaction committed*, not because anyone
/// has seen anything. A repeat of the same `request_id` returns the original
/// message rather than making a second one, and the same key with a
/// different body is refused instead of silently keeping the first.
pub async fn send(
    pool: &PgPool,
    auth: &AuthCtx,
    id: Uuid,
    input: SendInput,
) -> BusResult<SentMessage> {
    require_capability(pool, auth).await?;
    let a = readable(pool, auth, id).await?;
    if !a.can_send() {
        return Err(BusError::Forbidden(
            "you cannot post in this conversation: accept your invitation first, and note \
             that an observer reads and acknowledges but does not write"
                .to_owned(),
        ));
    }
    if a.archived {
        return Err(BusError::conflict(
            "this conversation is archived; its history stays readable",
        ));
    }
    let body = crate::store::check_text("message body", &input.body, MAX_BODY_BYTES)?;
    if body.is_empty() {
        return Err(BusError::invalid("a message body is required"));
    }
    let metadata = crate::store::normalize_metadata(input.metadata);
    crate::store::check_metadata("message", metadata.as_ref())?;
    let metadata = metadata.unwrap_or_else(|| serde_json::Value::Object(Default::default()));

    let mut tx = pool.begin().await?;
    crate::store::sessions::guard(&mut tx, auth).await?;

    // Lock the thread before deciding anything. Two things depend on it:
    // two retries of one request_id serialize here instead of racing to the
    // unique index, and the authorization below is re-read while it cannot
    // change underneath.
    let (archived_now, paused): (
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
    ) = sqlx::query_as(
        "SELECT archived_at, write_paused_at FROM conversations WHERE id = $1 FOR UPDATE",
    )
    .bind(id)
    .fetch_one(&mut *tx)
    .await?;
    if archived_now.is_some() {
        return Err(BusError::conflict(
            "this conversation is archived; its history stays readable",
        ));
    }
    // A supervised backend move holds this one thread still while it copies
    // the tail. Read under the same lock the move takes: checked outside the
    // transaction, a request that passed a moment earlier lands after the
    // tail was copied and is left behind. Seconds, not minutes, and only
    // this thread.
    if let Some(since) = paused {
        let secs = (chrono::Utc::now() - since).num_seconds().max(0);
        return Err(BusError::conflict(format!(
            "this conversation's storage is being moved by an operator and writes are \
             paused (for {secs}s so far). Reading still works. Try again in a moment; \
             nothing you have sent was lost."
        )));
    }
    // Membership as it is *now*. It was checked before this transaction
    // opened, and a removal that committed in between must take effect on
    // this call rather than the next one.
    let current: Option<(String, String)> = sqlx::query_as(
        "SELECT state, role FROM conversation_memberships
          WHERE conversation_id = $1 AND agent_id = $2 AND session = $3
            AND (session_id IS NULL OR $4::uuid IS NOT NULL)
          FOR SHARE",
    )
    .bind(id)
    .bind(auth.agent_id)
    .bind(&auth.session)
    .bind(auth.session_id)
    .fetch_optional(&mut *tx)
    .await?;
    let can_send_now = matches!(
        current
            .as_ref()
            .map(|(state, role)| (state.as_str(), role.as_str())),
        Some(("active", "owner")) | Some(("active", "moderator")) | Some(("active", "participant"))
    );
    if !can_send_now {
        return Err(BusError::Forbidden(
            "your membership of this conversation is no longer one that can post. Nothing              was written."
                .to_owned(),
        ));
    }

    // Idempotency, under that lock: a retry that raced the original sees it
    // rather than allocating a second sequence.
    #[allow(clippy::type_complexity)]
    let existing: Option<(
        Uuid,
        i64,
        chrono::DateTime<chrono::Utc>,
        String,
        Option<String>,
    )> = sqlx::query_as(
        "SELECT id, seq, created_at, publication_state, body_sha256
           FROM conversation_messages
          WHERE conversation_id = $1 AND request_id = $2",
    )
    .bind(id)
    .bind(input.request_id)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some((mid, seq, created_at, publication_state, digest)) = existing {
        // The digest, not the body. Once a publication releases the staging
        // copy there is no body here to compare with, and a legitimate
        // retry would be refused as a different message.
        if digest.as_deref() != Some(body_digest(&body).as_str()) {
            return Err(BusError::conflict(
                "this request_id already sent a different message. Use a fresh UUID for a \
                 new message; reusing one is how a retry is recognised.",
            ));
        }
        let recipients = recipients_of(&mut tx, mid).await?;
        tx.commit().await?;
        return Ok(SentMessage {
            message_id: mid.to_string(),
            conversation_id: id.to_string(),
            seq,
            // What the original actually is, not what the first call was
            // told. A retry of an accepted-but-unpublished message must not
            // be handed a storage confirmation the first call did not get.
            stored: publication_state == "stored",
            recipients,
            created_at: ts(created_at),
        });
    }

    // The conversation row is the sequence allocator, and locking it is what
    // makes `seq` gap-free rather than merely unique.
    let (seq,): (i64,) = sqlx::query_as(
        "UPDATE conversations SET last_seq = last_seq + 1 WHERE id = $1 RETURNING last_seq",
    )
    .bind(id)
    .fetch_one(&mut *tx)
    .await?;

    if let Some(reply_to) = input.reply_to {
        let ok: Option<(Uuid,)> = sqlx::query_as(
            "SELECT id FROM conversation_messages WHERE id = $1 AND conversation_id = $2",
        )
        .bind(reply_to)
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?;
        if ok.is_none() {
            return Err(BusError::not_found(
                "reply_to is not a message of this conversation",
            ));
        }
    }

    let (message_id,): (Uuid,) = sqlx::query_as(
        "INSERT INTO conversation_messages
            (conversation_id, seq, sender_agent, sender_session, body, reply_to, metadata,
             request_id, body_sha256, backend)
         SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, c.backend
           FROM conversations c WHERE c.id = $1
         RETURNING id",
    )
    .bind(id)
    .bind(seq)
    .bind(auth.agent_id)
    .bind(&auth.session)
    .bind(&body)
    .bind(input.reply_to)
    .bind(&metadata)
    .bind(input.request_id)
    .bind(body_digest(&body))
    .fetch_one(&mut *tx)
    .await?;

    // The snapshot: who this message was addressed to, as of now. Everyone
    // active except the sender — a sender does not acknowledge itself.
    sqlx::query(
        "INSERT INTO message_recipients (message_id, membership_id, agent_id, session)
         SELECT $1, m.id, m.agent_id, m.session
           FROM conversation_memberships m
          WHERE m.conversation_id = $2
            AND m.state = 'active'
            AND NOT (m.agent_id = $3 AND m.session = $4)",
    )
    .bind(message_id)
    .bind(id)
    .bind(auth.agent_id)
    .bind(&auth.session)
    .execute(&mut *tx)
    .await?;

    // Which path this conversation is on. `sync` is the default and the
    // only one with a body that is durable the moment this commits.
    let (publication,): (String,) =
        sqlx::query_as("SELECT publication FROM conversations WHERE id = $1")
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
    let asynchronous = publication == "outbox";

    // One receipt row per recipient. `stored_at` is set here only when the
    // body is durable here: on the outbox path it stays null until the
    // backend confirms, because saying stored before that would be a claim
    // nobody could check.
    sqlx::query(
        "INSERT INTO message_receipts (message_id, membership_id, stored_at)
         SELECT message_id, membership_id,
                CASE WHEN $2 THEN NULL ELSE now() END
           FROM message_recipients WHERE message_id = $1",
    )
    .bind(message_id)
    .bind(asynchronous)
    .execute(&mut *tx)
    .await?;

    if asynchronous {
        let (backend,): (String,) =
            sqlx::query_as("SELECT backend FROM conversations WHERE id = $1")
                .bind(id)
                .fetch_one(&mut *tx)
                .await?;
        crate::store::outbox::enqueue(&mut tx, message_id, id, auth.team_id, &backend, &body)
            .await?;
    }

    audit(
        &mut tx,
        auth,
        Some(id),
        "message.send",
        Some(message_id),
        serde_json::json!({ "seq": seq }),
    )
    .await?;

    let recipients = recipients_of(&mut tx, message_id).await?;
    let created_at: (chrono::DateTime<chrono::Utc>,) =
        sqlx::query_as("SELECT created_at FROM conversation_messages WHERE id = $1")
            .bind(message_id)
            .fetch_one(&mut *tx)
            .await?;
    tx.commit().await?;

    Ok(SentMessage {
        message_id: message_id.to_string(),
        conversation_id: id.to_string(),
        seq,
        // Accepted is not stored. On the synchronous path they coincide
        // because this commit *is* the persistence; on the outbox path the
        // caller is told the truth and can watch it settle.
        stored: !asynchronous,
        recipients,
        created_at: ts(created_at.0),
    })
}

/// What is known about one message's body.
///
/// A body that cannot be served is **not** an error when reading a thread:
/// the message keeps its sequence, its sender and its receipts, and the
/// reason is stated on the message itself. Only a caller who asked for one
/// specific body gets a refusal.
pub enum BodyState {
    Present(String),
    /// `publication` is the honest status — `pending_publication`, `failed`
    /// or `tombstoned` — and `why` says what it means for this caller.
    Missing {
        publication: &'static str,
        why: BusError,
    },
}

/// The columns resolving a body needs. Read once per message, alongside the
/// rest of the row, so a page of history is still one query plus whatever
/// the backend charges for the bodies it actually holds.
struct BodyRow {
    body: String,
    state: String,
    locator: Option<String>,
    /// Where THIS body is authoritative, which during a supervised move is
    /// not necessarily where the conversation is.
    backend: String,
    tombstoned: bool,
    tombstone_reason: Option<String>,
}

/// Turn one row into a body or an explained absence.
///
/// `backend` must be the conversation's own backend: a locator is only
/// meaningful to the adapter that issued it, and `Backends::for_conversation`
/// is how you get the right one.
async fn resolve_body(
    pool: &PgPool,
    backends: &crate::store::routing::Backends,
    team_id: Uuid,
    message_id: Uuid,
    row: BodyRow,
) -> BusResult<BodyState> {
    if row.tombstoned {
        return Ok(BodyState::Missing {
            publication: "tombstoned",
            why: BusError::not_found(format!(
                "this message's body is no longer held by its backend ({}). Its place in \
                 the thread, its recipients and its receipts remain.",
                row.tombstone_reason.as_deref().unwrap_or("retention")
            )),
        });
    }
    // Still in the local row: either Postgres is the backend, or the
    // publication has not been confirmed and the temporary copy released.
    if row.backend == crate::store::backend::PostgresBackend::NAME || !row.body.is_empty() {
        return Ok(BodyState::Present(row.body));
    }
    match row.state.as_str() {
        "pending_publication" => Ok(BodyState::Missing {
            publication: "pending_publication",
            why: BusError::conflict(
                "this message has been accepted but its backend has not confirmed it yet. \
                 It is not lost; read it again in a moment.",
            ),
        }),
        "failed" => Ok(BodyState::Missing {
            publication: "failed",
            why: BusError::not_found(
                "this message was never stored by its backend. Its slot is kept so the gap \
                 is visible rather than silent.",
            ),
        }),
        _ => {
            let Some(locator) = row.locator else {
                return Ok(BodyState::Missing {
                    publication: "failed",
                    why: BusError::not_found(
                        "this message has no body and no locator; there is nothing to read",
                    ),
                });
            };
            let backend = backends.for_message(&row.backend, team_id).await?;
            match backend
                .fetch(&crate::store::backend::Locator(locator), message_id)
                .await?
            {
                Some(body) => Ok(BodyState::Present(body)),
                None => {
                    // Gone from the backend without a tombstone: record one,
                    // so the next reader gets an explanation rather than the
                    // same surprise.
                    crate::store::outbox::tombstone(pool, message_id, "missing from backend")
                        .await?;
                    Ok(BodyState::Missing {
                        publication: "tombstoned",
                        why: BusError::not_found(
                            "this message's body is no longer held by its backend. Its place \
                             in the thread, its recipients and its receipts remain.",
                        ),
                    })
                }
            }
        }
    }
}

/// Fetch one body that may live on another backend.
///
/// Access is the caller's business and was already checked; this is storage,
/// not policy.
pub async fn body_of(
    pool: &PgPool,
    backends: &crate::store::routing::Backends,
    message_id: Uuid,
) -> BusResult<String> {
    let row: Option<(
        String,
        String,
        Option<String>,
        String,
        Uuid,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<String>,
    )> = sqlx::query_as(
        "SELECT m.body, m.publication_state, m.canonical_locator, m.backend, c.team_id,
                m.tombstoned_at, m.tombstone_reason
           FROM conversation_messages m
           JOIN conversations c ON c.id = m.conversation_id
          WHERE m.id = $1",
    )
    .bind(message_id)
    .fetch_optional(pool)
    .await?;
    let Some((body, state, locator, backend, team_id, tombstoned, tombstone_reason)) = row else {
        return Err(BusError::not_found("no such message"));
    };
    let resolved = resolve_body(
        pool,
        backends,
        team_id,
        message_id,
        BodyRow {
            body,
            state,
            locator,
            backend,
            tombstoned: tombstoned.is_some(),
            tombstone_reason,
        },
    )
    .await?;
    match resolved {
        BodyState::Present(body) => Ok(body),
        BodyState::Missing { why, .. } => Err(why),
    }
}

async fn recipients_of(tx: &mut sqlx::PgConnection, message_id: Uuid) -> BusResult<Vec<String>> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT ag.name, r.session FROM message_recipients r
           JOIN agents ag ON ag.id = r.agent_id
          WHERE r.message_id = $1 ORDER BY ag.name, r.session",
    )
    .bind(message_id)
    .fetch_all(tx)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(agent, session)| address_of(&agent, &session))
        .collect())
}

/// Read a thread. Reading is **not** acknowledging: no receipt is touched
/// here, and no cursor is advanced on anyone's behalf.
pub async fn read(
    pool: &PgPool,
    backends: &crate::store::routing::Backends,
    auth: &AuthCtx,
    id: Uuid,
    after_seq: Option<i64>,
    limit: Option<i64>,
) -> BusResult<ConversationRead> {
    require_capability(pool, auth).await?;
    let a = readable(pool, auth, id).await?;
    require_accepted(&a)?;
    let limit = limit.unwrap_or(DEFAULT_PAGE).clamp(1, MAX_PAGE);
    // A member reads from its own boundary; a project reader sees the thread
    // from the start, which is what project visibility means.
    let floor = a
        .membership
        .as_ref()
        .and_then(|m| m.history_from_seq)
        .unwrap_or(0);
    let after = after_seq.unwrap_or(0).max(floor);

    #[allow(clippy::type_complexity)]
    let rows: Vec<(
        Uuid,
        i64,
        String,
        String,
        String,
        Option<Uuid>,
        serde_json::Value,
        chrono::DateTime<chrono::Utc>,
        String,
        Option<String>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<String>,
        String,
    )> = sqlx::query_as(
        // Messages awaiting publication are returned, not hidden. Hiding
        // them let a reader believe the thread ended there; showing each
        // one with its publication state tells the truth and still stops a
        // cursor from walking over a gap it never knew about.
        "SELECT m.id, m.seq, ag.name, m.sender_session, m.body, m.reply_to, m.metadata,
                m.created_at, m.publication_state, m.canonical_locator, m.tombstoned_at,
                m.tombstone_reason, m.backend
           FROM conversation_messages m
           JOIN agents ag ON ag.id = m.sender_agent
          WHERE m.conversation_id = $1 AND m.seq > $2 AND m.deleted_at IS NULL
          ORDER BY m.seq
          LIMIT $3",
    )
    .bind(id)
    .bind(after)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    // The membership that may see these bodies is rechecked *here*, after
    // the rows are in hand and immediately before any body is served: an
    // ACL that changed while a publication was in flight takes effect on
    // this read, not the next one.
    let a = readable(pool, auth, id).await?;
    require_accepted(&a)?;

    // One query for every receipt on the page. A page of 200 messages used
    // to be 200 extra round trips, which is a read that gets slower exactly
    // as a thread gets busier.
    let mut mine: std::collections::HashMap<Uuid, ReceiptInfo> = std::collections::HashMap::new();
    if let Some(m) = &a.membership {
        let ids: Vec<Uuid> = rows.iter().map(|r| r.0).collect();
        for (message_id, receipt) in receipts_for(pool, &ids, m.id).await? {
            mine.insert(message_id, receipt);
        }
    }

    let mut messages = Vec::with_capacity(rows.len());
    for (
        mid,
        seq,
        from,
        from_session,
        body,
        reply_to,
        metadata,
        created_at,
        state,
        locator,
        tombstoned,
        tombstone_reason,
        message_backend,
    ) in rows
    {
        let my_receipt = mine.remove(&mid);
        let publication = state.clone();
        let resolved = resolve_body(
            pool,
            backends,
            auth.team_id,
            mid,
            BodyRow {
                body,
                state,
                locator,
                backend: message_backend,
                tombstoned: tombstoned.is_some(),
                tombstone_reason,
            },
        )
        .await?;
        let (body, publication, unavailable) = match resolved {
            BodyState::Present(body) => (body, publication, None),
            // One body the backend cannot serve does not fail the page. The
            // message keeps its sequence and says what happened to it.
            BodyState::Missing { publication, why } => {
                (String::new(), publication.to_owned(), Some(why.to_string()))
            }
        };
        messages.push(ConversationMessage {
            message_id: mid.to_string(),
            seq,
            from_address: address_of(&from, &from_session),
            from,
            body,
            reply_to: reply_to.map(|r| r.to_string()),
            metadata,
            created_at: ts(created_at),
            my_receipt,
            publication,
            unavailable,
        });
    }
    // The cursor stops at the first message still awaiting publication. A
    // caller following it must not step over a sequence that is about to
    // fill and never come back for it.
    let first_pending = messages
        .iter()
        .find(|m| m.publication == "pending_publication")
        .map(|m| m.seq);
    let next = messages
        .last()
        .map(|m| m.seq)
        .map(|last| match first_pending {
            Some(pending) => last.min(pending - 1),
            None => last,
        })
        .filter(|s| *s < a.last_seq && *s > 0);
    Ok(ConversationRead {
        conversation_id: id.to_string(),
        next_after_seq: next,
        history_from_seq: a.membership.as_ref().and_then(|m| m.history_from_seq),
        messages,
    })
}

/// Every receipt this membership holds on a page of messages, in one query.
#[allow(clippy::type_complexity)]
async fn receipts_for(
    pool: &PgPool,
    message_ids: &[Uuid],
    membership_id: Uuid,
) -> BusResult<Vec<(Uuid, ReceiptInfo)>> {
    if message_ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows: Vec<(
        Uuid,
        String,
        String,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<String>,
    )> = sqlx::query_as(
        "SELECT r.message_id, ag.name, m.session, r.stored_at, r.delivered_at, r.presented_at,
                r.acknowledged_at, r.resolved_at, r.note
           FROM message_receipts r
           JOIN conversation_memberships m ON m.id = r.membership_id
           JOIN agents ag ON ag.id = m.agent_id
          WHERE r.message_id = ANY($1) AND r.membership_id = $2",
    )
    .bind(message_ids)
    .bind(membership_id)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(message_id, agent, session, stored, delivered, presented, acked, resolved, note)| {
                (
                    message_id,
                    ReceiptInfo {
                        address: address_of(&agent, &session),
                        session: (!session.is_empty()).then_some(session),
                        agent,
                        stored_at: ts_opt(stored),
                        delivered_at: ts_opt(delivered),
                        presented_at: ts_opt(presented),
                        acknowledged_at: ts_opt(acked),
                        resolved_at: ts_opt(resolved),
                        note,
                    },
                )
            },
        )
        .collect())
}

async fn receipt_of(
    pool: &PgPool,
    message_id: Uuid,
    membership_id: Uuid,
) -> BusResult<Option<ReceiptInfo>> {
    let row: Option<(
        String,
        String,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<String>,
    )> = sqlx::query_as(
        "SELECT ag.name, m.session, r.stored_at, r.delivered_at, r.presented_at,
                r.acknowledged_at, r.resolved_at, r.note
           FROM message_receipts r
           JOIN conversation_memberships m ON m.id = r.membership_id
           JOIN agents ag ON ag.id = m.agent_id
          WHERE r.message_id = $1 AND r.membership_id = $2",
    )
    .bind(message_id)
    .bind(membership_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(
        |(agent, session, stored, delivered, presented, acked, resolved, note)| ReceiptInfo {
            address: address_of(&agent, &session),
            session: (!session.is_empty()).then_some(session),
            agent,
            stored_at: ts_opt(stored),
            delivered_at: ts_opt(delivered),
            presented_at: ts_opt(presented),
            acknowledged_at: ts_opt(acked),
            resolved_at: ts_opt(resolved),
            note,
        },
    ))
}

/// One message, by id.
pub async fn get_message(
    pool: &PgPool,
    backends: &crate::store::routing::Backends,
    auth: &AuthCtx,
    message_id: Uuid,
) -> BusResult<ConversationMessage> {
    require_capability(pool, auth).await?;
    #[allow(clippy::type_complexity)]
    let row: Option<(
        Uuid,
        i64,
        String,
        String,
        String,
        Option<Uuid>,
        serde_json::Value,
        chrono::DateTime<chrono::Utc>,
        String,
        Option<String>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<String>,
        String,
    )> = sqlx::query_as(
        "SELECT m.conversation_id, m.seq, ag.name, m.sender_session, m.body, m.reply_to,
                    m.metadata, m.created_at, m.publication_state, m.canonical_locator,
                    m.tombstoned_at, m.tombstone_reason, m.backend
               FROM conversation_messages m
               JOIN agents ag ON ag.id = m.sender_agent
              WHERE m.id = $1 AND m.deleted_at IS NULL",
    )
    .bind(message_id)
    .fetch_optional(pool)
    .await?;
    let Some((
        conversation_id,
        seq,
        from,
        from_session,
        body,
        reply_to,
        metadata,
        created_at,
        state,
        locator,
        tombstoned,
        tombstone_reason,
        message_backend,
    )) = row
    else {
        return Err(BusError::not_found("no such message"));
    };
    // Access is rechecked here, now: a membership that ended since the
    // message was sent does not keep reading it.
    let a = readable(pool, auth, conversation_id).await?;
    require_accepted(&a)?;
    if let Some(m) = &a.membership
        && let Some(floor) = m.history_from_seq
        && seq <= floor
    {
        return Err(BusError::Forbidden(
            "this message is before the point your membership starts".to_owned(),
        ));
    }
    let my_receipt = match &a.membership {
        Some(m) => receipt_of(pool, message_id, m.id).await?,
        None => None,
    };
    // The body is fetched only after that recheck passed, and from the
    // backend this message's body is actually on.
    let publication = state.clone();
    let (body, publication, unavailable) = match resolve_body(
        pool,
        backends,
        auth.team_id,
        message_id,
        BodyRow {
            body,
            state,
            locator,
            backend: message_backend,
            tombstoned: tombstoned.is_some(),
            tombstone_reason,
        },
    )
    .await?
    {
        BodyState::Present(body) => (body, publication, None),
        BodyState::Missing { publication, why } => {
            (String::new(), publication.to_owned(), Some(why.to_string()))
        }
    };
    Ok(ConversationMessage {
        message_id: message_id.to_string(),
        seq,
        from_address: address_of(&from, &from_session),
        from,
        body,
        reply_to: reply_to.map(|r| r.to_string()),
        metadata,
        created_at: ts(created_at),
        my_receipt,
        publication,
        unavailable,
    })
}

/// Record this window's own observation of a message. A caller can only
/// speak for itself: there is no parameter for whose receipt this is.
pub async fn ack(
    pool: &PgPool,
    auth: &AuthCtx,
    message_id: Uuid,
    resolved: bool,
    note: Option<String>,
) -> BusResult<ReceiptInfo> {
    require_capability(pool, auth).await?;
    let (conversation_id,): (Uuid,) =
        sqlx::query_as("SELECT conversation_id FROM conversation_messages WHERE id = $1")
            .bind(message_id)
            .fetch_optional(pool)
            .await?
            .ok_or_else(|| BusError::not_found("no such message"))?;
    let a = readable(pool, auth, conversation_id).await?;
    let Some(membership) = a.membership.as_ref().filter(|m| m.state == "active") else {
        return Err(BusError::Forbidden(
            "only an active member of this conversation can acknowledge its messages".to_owned(),
        ));
    };
    let note = match note {
        Some(n) => Some(crate::store::check_text("note", &n, 2000)?),
        None => None,
    };

    let mut tx = pool.begin().await?;
    crate::store::sessions::guard(&mut tx, auth).await?;
    // Only a recipient has a receipt row. Someone who joined after the
    // message was sent was not asked, and saying they acknowledged it would
    // put them in a denominator they were never in.
    // The membership state is re-read inside this update, not trusted from
    // the check above: a removal that committed while this request waited
    // takes effect here.
    let updated: Option<(Uuid,)> = sqlx::query_as(
        "UPDATE message_receipts r
            SET acknowledged_at = COALESCE(acknowledged_at, now()),
                resolved_at = CASE WHEN $3 THEN COALESCE(resolved_at, now()) ELSE resolved_at END,
                note = COALESCE($4, note)
           FROM conversation_memberships m
          WHERE r.message_id = $1 AND r.membership_id = $2
            AND m.id = r.membership_id AND m.state = 'active'
          RETURNING r.membership_id",
    )
    .bind(message_id)
    .bind(membership.id)
    .bind(resolved)
    .bind(note.as_deref())
    .fetch_optional(&mut *tx)
    .await?;
    if updated.is_none() {
        return Err(BusError::Forbidden(
            "this message was not addressed to your window, so there is nothing for you to \
             acknowledge. You can read it, which is not the same observation."
                .to_owned(),
        ));
    }
    audit(
        &mut tx,
        auth,
        Some(conversation_id),
        "message.ack",
        Some(message_id),
        serde_json::json!({ "resolved": resolved }),
    )
    .await?;
    // Tell the sender to look again, on the same transaction as the receipt
    // itself: a notification cannot exist without the receipt it reports,
    // and a receipt cannot be written without queueing the notification.
    // A no-op for a thread on Postgres, where the event hub already does it.
    crate::store::inbox::enqueue_receipt_reference(
        &mut tx,
        message_id,
        membership.id,
        if resolved { "resolved" } else { "acknowledged" },
    )
    .await?;
    tx.commit().await?;
    receipt_of(pool, message_id, membership.id)
        .await?
        .ok_or_else(|| BusError::not_found("receipt vanished"))
}

/// Who was asked, and what each of them has observed.
pub async fn receipts(
    pool: &PgPool,
    auth: &AuthCtx,
    message_id: Uuid,
) -> BusResult<MessageReceipts> {
    require_capability(pool, auth).await?;
    let row: Option<(Uuid, i64)> =
        sqlx::query_as("SELECT conversation_id, seq FROM conversation_messages WHERE id = $1")
            .bind(message_id)
            .fetch_optional(pool)
            .await?;
    let Some((conversation_id, seq)) = row else {
        return Err(BusError::not_found("no such message"));
    };
    let a = readable(pool, auth, conversation_id).await?;
    require_accepted(&a)?;
    // The same boundary `get_message` applies. A receipt carries recipient
    // addresses and a free-text note, which is the discussion itself often
    // enough; a member who may not read the message may not read who
    // answered it either.
    if let Some(m) = &a.membership
        && let Some(floor) = m.history_from_seq
        && seq <= floor
    {
        return Err(BusError::Forbidden(
            "this message is before the point your membership starts, so its receipts are              not yours to read either"
                .to_owned(),
        ));
    }
    let rows: Vec<(
        String,
        String,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<String>,
    )> = sqlx::query_as(
        "SELECT ag.name, m.session, r.stored_at, r.delivered_at, r.presented_at,
                r.acknowledged_at, r.resolved_at, r.note
           FROM message_receipts r
           JOIN conversation_memberships m ON m.id = r.membership_id
           JOIN agents ag ON ag.id = m.agent_id
          WHERE r.message_id = $1
          ORDER BY ag.name, m.session",
    )
    .bind(message_id)
    .fetch_all(pool)
    .await?;
    let receipts: Vec<ReceiptInfo> = rows
        .into_iter()
        .map(
            |(agent, session, stored, delivered, presented, acked, resolved, note)| ReceiptInfo {
                address: address_of(&agent, &session),
                session: (!session.is_empty()).then_some(session),
                agent,
                stored_at: ts_opt(stored),
                delivered_at: ts_opt(delivered),
                presented_at: ts_opt(presented),
                acknowledged_at: ts_opt(acked),
                resolved_at: ts_opt(resolved),
                note,
            },
        )
        .collect();
    Ok(MessageReceipts {
        message_id: message_id.to_string(),
        seq,
        acknowledged: receipts
            .iter()
            .filter(|r| r.acknowledged_at.is_some())
            .count(),
        resolved: receipts.iter().filter(|r| r.resolved_at.is_some()).count(),
        total: receipts.len(),
        receipts,
    })
}

/// Conversations with something waiting for this window.
pub async fn activity(pool: &PgPool, auth: &AuthCtx) -> BusResult<Vec<ConversationActivity>> {
    let rows: Vec<(Uuid, String, i64, i64)> = sqlx::query_as(
        "SELECT c.id, c.title, c.last_seq,
                (SELECT count(*) FROM message_receipts r
                  WHERE r.membership_id = m.id AND r.acknowledged_at IS NULL)
           FROM conversations c
           JOIN conversation_memberships m
                ON m.conversation_id = c.id AND m.agent_id = $2 AND m.session = $3
          WHERE c.team_id = $1 AND c.archived_at IS NULL AND m.state = 'active'
          ORDER BY c.last_seq DESC",
    )
    .bind(auth.team_id)
    .bind(auth.agent_id)
    .bind(&auth.session)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .filter(|(_, _, _, unack)| *unack > 0)
        .map(
            |(id, title, last_seq, unacknowledged)| ConversationActivity {
                conversation_id: id.to_string(),
                title,
                last_seq,
                unacknowledged,
            },
        )
        .collect())
}

// -------------------------------------------- transfer and owner recovery --

/// Hand this window's membership to another session **of the same agent**.
///
/// Two halves, and the second is the target's: a proposal marks the target
/// membership `invited` with the same role and history boundary, and nothing
/// moves until that window accepts with `join_conversation`. Acceptance
/// supersedes the old membership rather than rewriting it: authorship stays,
/// old receipts stay, and the successor is recorded so unfinished work has a
/// link rather than a forged acknowledgement.
pub async fn transfer_membership(
    pool: &PgPool,
    auth: &AuthCtx,
    id: Uuid,
    to: &str,
) -> BusResult<TransferResult> {
    require_capability(pool, auth).await?;
    let a = readable(pool, auth, id).await?;
    let Some(mine) = a.membership.as_ref().filter(|m| m.state == "active") else {
        return Err(BusError::Forbidden(
            "you have no active membership in this conversation to transfer".to_owned(),
        ));
    };
    let (agent, session) = crate::store::messaging::parse_address(to)?;
    let agent_id = crate::store::agent_id_by_name(pool, auth.team_id, &agent).await?;
    // Same agent only. A transfer moves a window's seat between that
    // person's own windows; handing it to someone else is an invite, which
    // the other member has to accept on its own terms and which does not
    // carry this one's history boundary.
    if agent_id != auth.agent_id {
        return Err(BusError::Forbidden(format!(
            "a membership can only be transferred to another window of the same agent. \
             '{to}' is someone else — invite them instead, which gives them their own \
             history boundary and their own receipts."
        )));
    }
    let session = session.unwrap_or_default();
    if session == auth.session {
        return Err(BusError::invalid("that is the window you are calling from"));
    }

    let mut tx = pool.begin().await?;
    crate::store::sessions::guard(&mut tx, auth).await?;
    // The source seat is re-read and locked here: it was active when the
    // request arrived, and leaving or being removed in between must stop
    // the transfer rather than hand over a seat that no longer exists.
    let source: Option<(String,)> =
        sqlx::query_as("SELECT state FROM conversation_memberships WHERE id = $1 FOR UPDATE")
            .bind(mine.id)
            .fetch_optional(&mut *tx)
            .await?;
    if source.as_ref().map(|s| s.0.as_str()) != Some("active") {
        return Err(BusError::conflict(
            "your membership of this conversation is no longer active, so there is nothing              to transfer. Nothing was written.",
        ));
    }
    let moved: Option<(Uuid,)> = sqlx::query_as(
        "INSERT INTO conversation_memberships
            (conversation_id, agent_id, session, role, state, history_from_seq, invited_by,
             transfer_from)
         SELECT $1, $2, $3, m.role, 'invited', m.history_from_seq, $2, m.id
           FROM conversation_memberships m WHERE m.id = $4
         ON CONFLICT (conversation_id, agent_id, session) DO UPDATE SET
            role = EXCLUDED.role,
            state = CASE WHEN conversation_memberships.state IN ('left', 'removed')
                         THEN 'invited' ELSE conversation_memberships.state END,
            history_from_seq = EXCLUDED.history_from_seq,
            transfer_from = EXCLUDED.transfer_from,
            session_id = NULL,
            invited_at = now(),
            ended_at = NULL
         RETURNING id",
    )
    .bind(id)
    .bind(auth.agent_id)
    .bind(&session)
    .bind(mine.id)
    .fetch_optional(&mut *tx)
    .await?;
    if moved.is_none() {
        return Err(BusError::conflict(
            "that window's seat could not be offered the transfer; nothing was written",
        ));
    }
    // The link is recorded now; the supersede happens when the target joins.
    sqlx::query("UPDATE conversation_memberships SET superseded_by = NULL WHERE id = $1")
        .bind(mine.id)
        .execute(&mut *tx)
        .await?;
    audit(
        &mut tx,
        auth,
        Some(id),
        "member.transfer",
        Some(auth.agent_id),
        serde_json::json!({ "from": address_of(&auth.agent_name, &auth.session), "to": to }),
    )
    .await?;
    tx.commit().await?;
    Ok(TransferResult {
        conversation_id: id.to_string(),
        from_address: address_of(&auth.agent_name, &auth.session),
        to_address: to.to_owned(),
        state: "proposed".into(),
    })
}

/// Complete a transfer: the accepting window takes the seat and the old one
/// is superseded. Called from `join` when a proposal is pending.
async fn supersede_predecessor(
    tx: &mut sqlx::PgConnection,
    auth: &AuthCtx,
    conversation: Uuid,
    new_membership: Uuid,
) -> BusResult<()> {
    // Exactly the seat this one was offered, and only if that proposal is
    // still the open one. "Some transfer by this agent exists" would let an
    // unrelated invitation close a live seat, and one transfer close
    // several.
    sqlx::query(
        "UPDATE conversation_memberships p
            SET state = 'left', ended_at = now(), superseded_by = $3
           FROM conversation_memberships n
          WHERE n.id = $3 AND n.transfer_from = p.id
            AND p.conversation_id = $1 AND p.agent_id = $2 AND p.id <> $3
            AND p.state = 'active'
            AND p.superseded_by IS NULL",
    )
    .bind(conversation)
    .bind(auth.agent_id)
    .bind(new_membership)
    .execute(&mut *tx)
    .await?;
    // The proposal is spent either way: accepted once, not standing.
    sqlx::query("UPDATE conversation_memberships SET transfer_from = NULL WHERE id = $1")
        .bind(new_membership)
        .execute(tx)
        .await?;
    Ok(())
}

/// Read-only recovery for an agent whose windows are all gone.
///
/// The exception the ADR documents: privacy inside a team is not isolation
/// from the owning agent. It requires an **agent token** (not a session
/// credential), every session of that agent to be server-confirmed closed,
/// revoked or expired — offline presence is not enough — and it returns
/// history only for memberships that were not revoked. It grants nothing:
/// no posting, no receipts, no new membership. Every use is audited.
pub async fn recover_history(
    pool: &PgPool,
    backends: &crate::store::routing::Backends,
    auth: &AuthCtx,
    id: Uuid,
    after_seq: Option<i64>,
    limit: Option<i64>,
) -> BusResult<ConversationRead> {
    require_capability(pool, auth).await?;
    if auth.session_is_authenticated() {
        return Err(BusError::Forbidden(
            "recovery is an owner-agent operation: call it with your agent token, not with \
             a window's session credential."
                .to_owned(),
        ));
    }
    // Serialised with registration: a window opening right now must either
    // block this recovery or happen after it, never race it.
    let mut tx = pool.begin().await?;
    // Lock the AGENT row, not the session rows. Locking the sessions that
    // exist locks nothing against the one about to be inserted: registration
    // writes a new row, conflicts with no held lock, and can slip in between
    // this check and the read below. Registration takes the same lock, so
    // the two serialise on something that is always there.
    sqlx::query("SELECT id FROM agents WHERE id = $1 FOR UPDATE")
        .bind(auth.agent_id)
        .fetch_one(&mut *tx)
        .await?;
    let live: Vec<(Uuid,)> = sqlx::query_as(
        "SELECT id FROM agent_sessions
          WHERE agent_id = $1 AND revoked_at IS NULL AND expires_at > now()",
    )
    .bind(auth.agent_id)
    .fetch_all(&mut *tx)
    .await?;
    if !live.is_empty() {
        return Err(BusError::conflict(format!(
            "{} of your sessions are still live. Recovery is for windows that are gone: \
             ask that window to read the thread, or revoke_session it first.",
            live.len()
        )));
    }

    // Only memberships that were not removed, and only their own history.
    let rows: Vec<(
        Uuid,
        i64,
        String,
        String,
        String,
        Option<Uuid>,
        serde_json::Value,
        chrono::DateTime<chrono::Utc>,
    )> = sqlx::query_as(
        "SELECT m.id, m.seq, ag.name, m.sender_session, m.body, m.reply_to, m.metadata,
                    m.created_at
               FROM conversation_messages m
               JOIN agents ag ON ag.id = m.sender_agent
               JOIN conversations c ON c.id = m.conversation_id
              WHERE m.conversation_id = $1
                AND c.team_id = $2
                AND m.seq > $5
                AND m.deleted_at IS NULL
                AND EXISTS (
                    SELECT 1 FROM conversation_memberships me
                     WHERE me.conversation_id = m.conversation_id
                       AND me.agent_id = $3
                       AND me.state <> 'removed'
                       -- An invitation is not membership: a window that was
                       -- asked and never answered read nothing, and its
                       -- agent recovers nothing on its behalf. A seat that
                       -- was active once still counts, however it ended.
                       AND me.accepted_at IS NOT NULL
                       AND m.seq > COALESCE(me.history_from_seq, 0))
                -- A project thread additionally needs current project access.
                AND (c.visibility = 'private' OR EXISTS (
                    SELECT 1 FROM project_agent_access a
                     WHERE a.project_id = c.project_id AND a.agent_id = $3))
              ORDER BY m.seq
              LIMIT $4",
    )
    .bind(id)
    .bind(auth.team_id)
    .bind(auth.agent_id)
    .bind(limit.unwrap_or(DEFAULT_PAGE).clamp(1, MAX_PAGE))
    .bind(after_seq.unwrap_or(0))
    .fetch_all(&mut *tx)
    .await?;
    if rows.is_empty() {
        return Err(BusError::not_found(
            "nothing to recover here: no non-revoked membership of yours covers this \
             conversation",
        ));
    }
    audit(
        &mut tx,
        auth,
        Some(id),
        "member.recover",
        Some(auth.agent_id),
        serde_json::json!({ "messages": rows.len(), "read_only": true }),
    )
    .await?;
    tx.commit().await?;

    // A real cursor: recovery can be more than one page, and a caller that
    // is told `None` stops at the page limit believing it has everything.
    let next_after_seq = (rows.len() as i64 == limit.unwrap_or(DEFAULT_PAGE).clamp(1, MAX_PAGE))
        .then(|| rows.last().map(|r| r.1))
        .flatten();
    // Bodies are resolved after the commit, never inside it: the backend
    // may be a broker, and a network round trip does not belong in a
    // transaction that holds session rows.
    let mut messages = Vec::with_capacity(rows.len());
    for (mid, seq, from, from_session, body, reply_to, metadata, created_at) in rows {
        #[allow(clippy::type_complexity)]
        let state: (
            String,
            Option<String>,
            Option<chrono::DateTime<chrono::Utc>>,
            Option<String>,
            String,
        ) = sqlx::query_as(
            "SELECT publication_state, canonical_locator, tombstoned_at, tombstone_reason,
                    backend
               FROM conversation_messages WHERE id = $1",
        )
        .bind(mid)
        .fetch_one(pool)
        .await?;
        let publication = state.0.clone();
        let (body, publication, unavailable) = match resolve_body(
            pool,
            backends,
            auth.team_id,
            mid,
            BodyRow {
                body,
                state: state.0,
                locator: state.1,
                backend: state.4,
                tombstoned: state.2.is_some(),
                tombstone_reason: state.3,
            },
        )
        .await?
        {
            BodyState::Present(body) => (body, publication, None),
            BodyState::Missing { publication, why } => {
                (String::new(), publication.to_owned(), Some(why.to_string()))
            }
        };
        messages.push(ConversationMessage {
            message_id: mid.to_string(),
            seq,
            from_address: address_of(&from, &from_session),
            from,
            body,
            reply_to: reply_to.map(|r| r.to_string()),
            metadata,
            created_at: ts(created_at),
            // Recovery observes nothing: it is a read, and inventing a
            // receipt is the one thing it must not do.
            my_receipt: None,
            publication,
            unavailable,
        });
    }
    Ok(ConversationRead {
        conversation_id: id.to_string(),
        next_after_seq,
        history_from_seq: None,
        messages,
    })
}
