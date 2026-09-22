//! Administration: teams, agents, agent tokens and administrative credentials.
//!
//! Two callers share this module and must behave identically: the operator
//! CLI next to Postgres (`ai-crew-sync team|agent|token …`, [`Actor::Cli`])
//! and the remote administration API (`/admin/*`, [`Actor::Admin`]). Every
//! mutating operation takes the actor so the audit trail records who did what
//! whichever door they came through, and takes the team as a resolved id so a
//! caller that already checked its scope cannot be widened by a name.
//!
//! Secrets exist here for exactly one statement: the `INSERT` that stores
//! their hash. They are returned to the caller once and never logged,
//! audited, or published as an event.

use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    auth::{ADMIN_TOKEN_PREFIX, generate_admin_token, generate_token, hash_token, token_prefix},
    error::{BusError, BusResult},
};

/// Who performs an administrative action, for the audit trail.
#[derive(Clone, Copy, Debug)]
pub enum Actor {
    /// The operator CLI with a database connection. No credential involved.
    Cli,
    /// A remote administrative credential.
    Admin(Uuid),
}

impl Actor {
    fn columns(self) -> (&'static str, Option<Uuid>) {
        match self {
            Actor::Cli => ("cli", None),
            Actor::Admin(id) => ("http", Some(id)),
        }
    }
}

/// Identity resolved from an administrative credential. It names no agent:
/// an administrator cannot post, claim or read anything on the bus.
#[derive(Clone, Debug)]
pub struct AdminCtx {
    pub id: Uuid,
    /// `None` is a global administrator. `Some` administers that team only.
    pub team_id: Option<Uuid>,
    pub team_slug: Option<String>,
}

impl AdminCtx {
    pub fn is_global(&self) -> bool {
        self.team_id.is_none()
    }
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct TeamRow {
    pub id: Uuid,
    pub slug: String,
    pub name: String,
    pub agents: i64,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct AgentRow {
    pub id: Uuid,
    pub name: String,
    pub display_name: Option<String>,
    pub disabled: bool,
    pub active_tokens: i64,
}

/// A freshly minted agent token. `token` is the secret, present in this
/// struct and nowhere else.
#[derive(Clone, Debug, serde::Serialize)]
pub struct IssuedToken {
    pub id: Uuid,
    pub token: String,
    pub prefix: String,
    pub agent: String,
    pub team: String,
    pub label: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct TokenRow {
    pub id: Uuid,
    pub agent: String,
    pub prefix: String,
    pub label: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub last_used_at: Option<chrono::DateTime<chrono::Utc>>,
    pub revoked: bool,
}

/// A freshly minted administrative credential. `token` is the secret.
#[derive(Clone, Debug, serde::Serialize)]
pub struct IssuedAdmin {
    pub id: Uuid,
    pub token: String,
    pub prefix: String,
    /// `None` for a global credential.
    pub team: Option<String>,
    pub label: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct AdminRow {
    pub id: Uuid,
    /// `None` for a global credential.
    pub team: Option<String>,
    pub prefix: String,
    pub label: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub last_used_at: Option<chrono::DateTime<chrono::Utc>>,
    pub revoked: bool,
}

/// Active tokens one agent may hold. A ceiling on what a leaked or looping
/// administrative credential can mint, and a nudge to revoke what is no
/// longer used: one token per repository is the intended shape, not one per
/// session start.
pub const MAX_ACTIVE_TOKENS_PER_AGENT: i64 = 100;

/// Longest name, slug or label accepted. These are identifiers people type,
/// not documents.
pub const MAX_NAME_BYTES: usize = 64;
pub const MAX_LABEL_BYTES: usize = 128;

fn check_name(field: &str, raw: &str) -> BusResult<String> {
    let value = raw.trim().to_lowercase();
    if value.is_empty() {
        return Err(BusError::invalid(format!("{field} cannot be empty")));
    }
    if value.len() > MAX_NAME_BYTES {
        return Err(BusError::invalid(format!(
            "{field} is {} bytes; the limit is {MAX_NAME_BYTES}",
            value.len()
        )));
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    {
        return Err(BusError::invalid(format!(
            "{field} may only contain ASCII letters, digits, '-', '_' and '.'"
        )));
    }
    Ok(value)
}

/// Free text people read back: a label, a display name, a team name. Trimmed,
/// bounded, and free of control characters — it ends up in terminals, in
/// listings and in the audit log.
fn check_display(field: &str, value: Option<String>) -> BusResult<Option<String>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.trim().to_owned();
    if value.is_empty() {
        return Ok(None);
    }
    if value.len() > MAX_LABEL_BYTES {
        return Err(BusError::invalid(format!(
            "{field} is {} bytes; the limit is {MAX_LABEL_BYTES}",
            value.len()
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(BusError::invalid(format!(
            "{field} must not contain control characters"
        )));
    }
    Ok(Some(value))
}

fn check_label(label: Option<String>) -> BusResult<Option<String>> {
    check_display("label", label)
}

/// One audit row, on the same transaction as the mutation it records: the
/// two commit together or not at all, so a mutation can never outlive a lost
/// audit write.
async fn audit(
    conn: &mut sqlx::PgConnection,
    actor: Actor,
    action: &str,
    team_id: Option<Uuid>,
    subject_id: Option<Uuid>,
    detail: serde_json::Value,
) -> BusResult<()> {
    let (source, admin_id) = actor.columns();
    sqlx::query(
        "INSERT INTO admin_audit (actor_source, actor_admin_id, action, team_id, subject_id, detail)
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(source)
    .bind(admin_id)
    .bind(action)
    .bind(team_id)
    .bind(subject_id)
    .bind(detail)
    .execute(conn)
    .await?;
    Ok(())
}

// ------------------------------------------------------------------ teams --

pub async fn team_id_by_slug(pool: &PgPool, slug: &str) -> BusResult<Uuid> {
    let slug = slug.trim().to_lowercase();
    let row: Option<(Uuid,)> = sqlx::query_as("SELECT id FROM teams WHERE slug = $1")
        .bind(&slug)
        .fetch_optional(pool)
        .await?;
    row.map(|r| r.0)
        .ok_or_else(|| BusError::not_found(format!("no team with slug '{slug}'")))
}

/// Create a team, or return the existing one with that slug unchanged.
pub async fn create_team(
    pool: &PgPool,
    actor: Actor,
    slug: &str,
    name: Option<String>,
) -> BusResult<TeamRow> {
    let slug = check_name("team slug", slug)?;
    let name = check_display("team name", name)?.unwrap_or_else(|| slug.clone());
    let mut tx = pool.begin().await?;
    let created: Option<(Uuid,)> = sqlx::query_as(
        "INSERT INTO teams (slug, name) VALUES ($1, $2)
         ON CONFLICT (slug) DO NOTHING RETURNING id",
    )
    .bind(&slug)
    .bind(&name)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some((id,)) = created {
        audit(
            &mut tx,
            actor,
            "team.create",
            Some(id),
            Some(id),
            serde_json::json!({ "slug": slug, "name": name }),
        )
        .await?;
    }
    tx.commit().await?;
    let id = team_id_by_slug(pool, &slug).await?;
    team_by_id(pool, id).await
}

/// One team as the listings show it.
pub async fn team_by_id(pool: &PgPool, id: Uuid) -> BusResult<TeamRow> {
    let row: Option<(String, String, i64)> = sqlx::query_as(
        "SELECT t.slug, t.name, (SELECT count(*) FROM agents a WHERE a.team_id = t.id)
         FROM teams t WHERE t.id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    let Some((slug, name, agents)) = row else {
        return Err(BusError::not_found("no such team"));
    };
    Ok(TeamRow {
        id,
        slug,
        name,
        agents,
    })
}

/// Turn a team's conversation capability on or off. Off is the default and
/// the safe state: installing a release must never expose a new surface.
pub async fn set_conversations(
    pool: &PgPool,
    actor: Actor,
    team_id: Uuid,
    enabled: bool,
) -> BusResult<()> {
    let mut tx = pool.begin().await?;
    let changed: Option<(String,)> = sqlx::query_as(
        "UPDATE teams SET conversations_enabled = $2
          WHERE id = $1 AND conversations_enabled <> $2 RETURNING slug",
    )
    .bind(team_id)
    .bind(enabled)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some((slug,)) = changed {
        audit(
            &mut tx,
            actor,
            "team.capability",
            Some(team_id),
            Some(team_id),
            serde_json::json!({ "slug": slug, "conversations_enabled": enabled }),
        )
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Route a team's **new** conversations to a backend.
///
/// Existing threads are not migrated and never will be by this call: their
/// bodies are where they are, and `conversations.backend` keeps saying so.
/// Refused while the team has publications in flight, because switching
/// away from a backend with work still queued for it leaves messages nobody
/// drains.
pub async fn set_default_backend(
    pool: &PgPool,
    actor: Actor,
    team_id: Uuid,
    backend: &str,
) -> BusResult<()> {
    if !matches!(backend, "postgres" | "jetstream") {
        return Err(crate::error::BusError::invalid(
            "backend must be 'postgres' or 'jetstream'",
        ));
    }
    let (inflight,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM conversation_outbox WHERE team_id = $1 AND state <> 'failed'",
    )
    .bind(team_id)
    .fetch_one(pool)
    .await?;
    if inflight > 0 {
        return Err(crate::error::BusError::conflict(format!(
            "{inflight} message(s) of this team are still awaiting publication. Let them \
             settle before changing the route; `team usage` shows when the queue is empty."
        )));
    }
    let mut tx = pool.begin().await?;
    let changed: Option<(String,)> = sqlx::query_as(
        "UPDATE teams SET default_backend = $2
          WHERE id = $1 AND default_backend <> $2 RETURNING slug",
    )
    .bind(team_id)
    .bind(backend)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some((slug,)) = changed {
        audit(
            &mut tx,
            actor,
            "team.backend",
            Some(team_id),
            Some(team_id),
            serde_json::json!({ "slug": slug, "default_backend": backend }),
        )
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub async fn list_teams(pool: &PgPool) -> BusResult<Vec<TeamRow>> {
    let rows: Vec<(Uuid, String, String, i64)> = sqlx::query_as(
        "SELECT t.id, t.slug, t.name, (SELECT count(*) FROM agents a WHERE a.team_id = t.id)
         FROM teams t ORDER BY t.slug",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(id, slug, name, agents)| TeamRow {
            id,
            slug,
            name,
            agents,
        })
        .collect())
}

// ----------------------------------------------------------------- agents --

/// Create an agent, or re-enable an existing one of that name. A repeated
/// `create` is how an operator brings back a disabled teammate, so it is not
/// an error. Audited as `agent.create` on creation and `agent.enable` on a
/// re-enable; a repeat on an active agent changes nothing and logs nothing
/// (a display-name tweak is cosmetic, not a security event).
pub async fn create_agent(
    pool: &PgPool,
    actor: Actor,
    team_id: Uuid,
    name: &str,
    display_name: Option<String>,
) -> BusResult<AgentRow> {
    let name = check_name("agent name", name)?;
    let display_name = check_display("display name", display_name)?;
    let mut tx = pool.begin().await?;
    // Locked so two concurrent creates of the same name serialise on the
    // row (the unique index serialises the inserts themselves).
    let existing: Option<(Uuid, bool)> = sqlx::query_as(
        "SELECT id, (disabled_at IS NOT NULL) FROM agents
         WHERE team_id = $1 AND name = $2 FOR UPDATE",
    )
    .bind(team_id)
    .bind(&name)
    .fetch_optional(&mut *tx)
    .await?;
    let id = match existing {
        None => {
            let (id,): (Uuid,) = sqlx::query_as(
                "INSERT INTO agents (team_id, name, display_name) VALUES ($1, $2, $3)
                 RETURNING id",
            )
            .bind(team_id)
            .bind(&name)
            .bind(&display_name)
            .fetch_one(&mut *tx)
            .await?;
            audit(
                &mut tx,
                actor,
                "agent.create",
                Some(team_id),
                Some(id),
                serde_json::json!({ "name": name, "display_name": display_name }),
            )
            .await?;
            id
        }
        Some((id, disabled)) => {
            sqlx::query(
                "UPDATE agents SET display_name = COALESCE($2, display_name), disabled_at = NULL
                 WHERE id = $1",
            )
            .bind(id)
            .bind(&display_name)
            .execute(&mut *tx)
            .await?;
            if disabled {
                audit(
                    &mut tx,
                    actor,
                    "agent.enable",
                    Some(team_id),
                    Some(id),
                    serde_json::json!({ "name": name }),
                )
                .await?;
            }
            id
        }
    };
    tx.commit().await?;
    let rows = list_agents(pool, team_id).await?;
    rows.into_iter()
        .find(|a| a.id == id)
        .ok_or_else(|| BusError::not_found("agent vanished after creation"))
}

pub async fn list_agents(pool: &PgPool, team_id: Uuid) -> BusResult<Vec<AgentRow>> {
    let rows: Vec<(Uuid, String, Option<String>, bool, i64)> = sqlx::query_as(
        r#"
        SELECT a.id, a.name, a.display_name,
               (a.disabled_at IS NOT NULL) AS disabled,
               (SELECT count(*) FROM api_tokens t
                 WHERE t.agent_id = a.id AND t.revoked_at IS NULL)
        FROM agents a WHERE a.team_id = $1 ORDER BY a.name
        "#,
    )
    .bind(team_id)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(id, name, display_name, disabled, active_tokens)| AgentRow {
                id,
                name,
                display_name,
                disabled,
                active_tokens,
            },
        )
        .collect())
}

pub async fn disable_agent(
    pool: &PgPool,
    actor: Actor,
    team_id: Uuid,
    name: &str,
) -> BusResult<()> {
    let name = name.trim().to_lowercase();
    let mut tx = pool.begin().await?;
    // Only a real transition is audited; disabling twice is a quiet no-op.
    let row: Option<(Uuid,)> = sqlx::query_as(
        "UPDATE agents SET disabled_at = now()
         WHERE team_id = $1 AND name = $2 AND disabled_at IS NULL RETURNING id",
    )
    .bind(team_id)
    .bind(&name)
    .fetch_optional(&mut *tx)
    .await?;
    match row {
        Some((id,)) => {
            audit(
                &mut tx,
                actor,
                "agent.disable",
                Some(team_id),
                Some(id),
                serde_json::json!({ "name": name }),
            )
            .await?;
        }
        None => {
            let exists: Option<(Uuid,)> =
                sqlx::query_as("SELECT id FROM agents WHERE team_id = $1 AND name = $2")
                    .bind(team_id)
                    .bind(&name)
                    .fetch_optional(&mut *tx)
                    .await?;
            if exists.is_none() {
                return Err(BusError::not_found(format!(
                    "no agent '{name}' in this team"
                )));
            }
        }
    }
    tx.commit().await?;
    Ok(())
}

// ----------------------------------------------------------- agent tokens --

/// Mint a token for `agent` in `team_id`. The token belongs to exactly that
/// agent and team; the label is a display hint and plays no part in identity.
pub async fn issue_token(
    pool: &PgPool,
    actor: Actor,
    team_id: Uuid,
    agent: &str,
    label: Option<String>,
) -> BusResult<IssuedToken> {
    let agent = agent.trim().to_lowercase();
    let label = check_label(label)?;
    let mut tx = pool.begin().await?;
    // The agent row is locked for the rest of the transaction, so the
    // active-token count below cannot be raced past the cap by a concurrent
    // issue for the same agent.
    let row: Option<(Uuid, String, bool)> = sqlx::query_as(
        "SELECT a.id, t.slug, (a.disabled_at IS NOT NULL)
         FROM agents a JOIN teams t ON t.id = a.team_id
         WHERE a.team_id = $1 AND a.name = $2 FOR UPDATE OF a",
    )
    .bind(team_id)
    .bind(&agent)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((agent_id, team_slug, disabled)) = row else {
        return Err(BusError::not_found(format!(
            "no agent '{agent}' in this team — create it first"
        )));
    };
    if disabled {
        return Err(BusError::conflict(format!(
            "agent '{agent}' is disabled; re-create it to enable it before issuing a token"
        )));
    }
    let (active,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM api_tokens WHERE agent_id = $1 AND revoked_at IS NULL",
    )
    .bind(agent_id)
    .fetch_one(&mut *tx)
    .await?;
    if active >= MAX_ACTIVE_TOKENS_PER_AGENT {
        return Err(BusError::conflict(format!(
            "agent '{agent}' already has {active} active tokens; the limit is \
             {MAX_ACTIVE_TOKENS_PER_AGENT}. Revoke the ones no longer in use first"
        )));
    }

    let (_, issued_by) = actor.columns();
    let raw = generate_token();
    let prefix = token_prefix(&raw);
    let (id,): (Uuid,) = sqlx::query_as(
        "INSERT INTO api_tokens (agent_id, token_hash, prefix, label, issued_by_admin)
         VALUES ($1, $2, $3, $4, $5) RETURNING id",
    )
    .bind(agent_id)
    .bind(hash_token(&raw))
    .bind(&prefix)
    .bind(&label)
    .bind(issued_by)
    .fetch_one(&mut *tx)
    .await?;
    audit(
        &mut tx,
        actor,
        "token.issue",
        Some(team_id),
        Some(id),
        serde_json::json!({ "agent": agent, "label": label, "prefix": prefix }),
    )
    .await?;
    tx.commit().await?;
    Ok(IssuedToken {
        id,
        token: raw,
        prefix,
        agent,
        team: team_slug,
        label,
    })
}

pub async fn list_tokens(pool: &PgPool, team_id: Uuid) -> BusResult<Vec<TokenRow>> {
    let rows: Vec<(
        Uuid,
        String,
        String,
        Option<String>,
        chrono::DateTime<chrono::Utc>,
        Option<chrono::DateTime<chrono::Utc>>,
        bool,
    )> = sqlx::query_as(
        r#"
        SELECT t.id, a.name, t.prefix, t.label, t.created_at, t.last_used_at,
               (t.revoked_at IS NOT NULL) AS revoked
        FROM api_tokens t
        JOIN agents a ON a.id = t.agent_id
        WHERE a.team_id = $1
        ORDER BY a.name, t.created_at
        "#,
    )
    .bind(team_id)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(id, agent, prefix, label, created_at, last_used_at, revoked)| TokenRow {
                id,
                agent,
                prefix,
                label,
                created_at,
                last_used_at,
                revoked,
            },
        )
        .collect())
}

/// Revoke an agent token. With `team_id` set, a token outside that team is
/// reported as not found — a team administrator learns nothing about other
/// teams' ids. Revoking an already revoked token is a no-op that succeeds
/// and logs nothing: the UPDATE is conditional on the token being active, so
/// two concurrent revocations produce exactly one transition and one row.
pub async fn revoke_token(
    pool: &PgPool,
    actor: Actor,
    team_id: Option<Uuid>,
    id: Uuid,
) -> BusResult<()> {
    let mut tx = pool.begin().await?;
    // The agent row first: registration, resume and recovery serialise on
    // it, so a window registering under this token right now either lands
    // before the revocation and is swept below, or waits and finds the
    // token gone. Without the lock a session could slip in between.
    //
    // NO KEY UPDATE, not UPDATE: a first heartbeat holds the session row
    // (the epoch guard) and then inserts presence, whose foreign key takes
    // a key share on this agent. FOR UPDATE blocks that share while this
    // transaction waits on the session row, and Postgres breaks the cycle
    // by rolling the revocation back. NO KEY UPDATE serialises the
    // lifecycle paths with each other and lets the key share through.
    sqlx::query(
        "SELECT a.id FROM agents a JOIN api_tokens t ON t.agent_id = a.id
          WHERE t.id = $1 AND ($2::uuid IS NULL OR a.team_id = $2)
          FOR NO KEY UPDATE OF a",
    )
    .bind(id)
    .bind(team_id)
    .execute(&mut *tx)
    .await?;
    let revoked: Option<(Uuid, String, String)> = sqlx::query_as(
        "UPDATE api_tokens t SET revoked_at = now()
         FROM agents a
         WHERE t.id = $1 AND t.agent_id = a.id AND t.revoked_at IS NULL
           AND ($2::uuid IS NULL OR a.team_id = $2)
         RETURNING a.team_id, a.name, t.prefix",
    )
    .bind(id)
    .bind(team_id)
    .fetch_optional(&mut *tx)
    .await?;
    match revoked {
        Some((owner_team, agent, prefix)) => {
            // A session credential is a child of this token: authentication
            // already refuses it once the parent is gone, and the row must
            // say the same, or recovery keeps counting a window that cannot
            // answer and its label stays reserved for a credential that no
            // longer works.
            let sessions: Vec<(String,)> = sqlx::query_as(
                "UPDATE agent_sessions SET revoked_at = now()
                  WHERE parent_token = $1 AND revoked_at IS NULL
                  RETURNING label",
            )
            .bind(id)
            .fetch_all(&mut *tx)
            .await?;
            audit(
                &mut tx,
                actor,
                "token.revoke",
                Some(owner_team),
                Some(id),
                serde_json::json!({
                    "agent": agent,
                    "prefix": prefix,
                    "sessions_revoked": sessions.iter().map(|s| &s.0).collect::<Vec<_>>(),
                }),
            )
            .await?;
        }
        None => {
            let exists: Option<(Uuid,)> = sqlx::query_as(
                "SELECT t.id FROM api_tokens t JOIN agents a ON a.id = t.agent_id
                 WHERE t.id = $1 AND ($2::uuid IS NULL OR a.team_id = $2)",
            )
            .bind(id)
            .bind(team_id)
            .fetch_optional(&mut *tx)
            .await?;
            if exists.is_none() {
                return Err(BusError::not_found(format!("no token with id {id}")));
            }
        }
    }
    tx.commit().await?;
    Ok(())
}

// ------------------------------------------------- administrative credentials --

/// Mint an administrative credential: for one team, or global when `team_id`
/// is `None`. Only the local CLI (bootstrap) and a global administrator may
/// call this; the caller enforces that, this function records it.
pub async fn grant_admin(
    pool: &PgPool,
    actor: Actor,
    team_id: Option<Uuid>,
    label: Option<String>,
) -> BusResult<IssuedAdmin> {
    let label = check_label(label)?;
    let team_slug = match team_id {
        Some(tid) => {
            let row: Option<(String,)> = sqlx::query_as("SELECT slug FROM teams WHERE id = $1")
                .bind(tid)
                .fetch_optional(pool)
                .await?;
            let Some((slug,)) = row else {
                return Err(BusError::not_found("no such team"));
            };
            Some(slug)
        }
        None => None,
    };
    let (_, issued_by) = actor.columns();
    let raw = generate_admin_token();
    let prefix = token_prefix(&raw);
    let mut tx = pool.begin().await?;
    let (id,): (Uuid,) = sqlx::query_as(
        "INSERT INTO admin_tokens (team_id, token_hash, prefix, label, issued_by)
         VALUES ($1, $2, $3, $4, $5) RETURNING id",
    )
    .bind(team_id)
    .bind(hash_token(&raw))
    .bind(&prefix)
    .bind(&label)
    .bind(issued_by)
    .fetch_one(&mut *tx)
    .await?;
    audit(
        &mut tx,
        actor,
        "admin.grant",
        team_id,
        Some(id),
        serde_json::json!({
            "scope": if team_id.is_some() { "team" } else { "global" },
            "label": label,
            "prefix": prefix,
        }),
    )
    .await?;
    tx.commit().await?;
    Ok(IssuedAdmin {
        id,
        token: raw,
        prefix,
        team: team_slug,
        label,
    })
}

/// List administrative credentials. `team_id` `None` lists every credential
/// (global ones included); `Some` lists that team's only.
pub async fn list_admins(pool: &PgPool, team_id: Option<Uuid>) -> BusResult<Vec<AdminRow>> {
    let rows: Vec<(
        Uuid,
        Option<String>,
        String,
        Option<String>,
        chrono::DateTime<chrono::Utc>,
        Option<chrono::DateTime<chrono::Utc>>,
        bool,
    )> = sqlx::query_as(
        r#"
        SELECT c.id, t.slug, c.prefix, c.label, c.created_at, c.last_used_at,
               (c.revoked_at IS NOT NULL) AS revoked
        FROM admin_tokens c
        LEFT JOIN teams t ON t.id = c.team_id
        WHERE ($1::uuid IS NULL OR c.team_id = $1)
        ORDER BY t.slug NULLS FIRST, c.created_at
        "#,
    )
    .bind(team_id)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(id, team, prefix, label, created_at, last_used_at, revoked)| AdminRow {
                id,
                team,
                prefix,
                label,
                created_at,
                last_used_at,
                revoked,
            },
        )
        .collect())
}

/// Revoke an administrative credential. With `scope` set, a credential that
/// is global or belongs to another team is reported as not found. Same
/// atomic shape as [`revoke_token`]: one transition, one audit row.
pub async fn revoke_admin(
    pool: &PgPool,
    actor: Actor,
    scope: Option<Uuid>,
    id: Uuid,
) -> BusResult<()> {
    let mut tx = pool.begin().await?;
    let revoked: Option<(Option<Uuid>, String)> = sqlx::query_as(
        "UPDATE admin_tokens SET revoked_at = now()
         WHERE id = $1 AND revoked_at IS NULL AND ($2::uuid IS NULL OR team_id = $2)
         RETURNING team_id, prefix",
    )
    .bind(id)
    .bind(scope)
    .fetch_optional(&mut *tx)
    .await?;
    match revoked {
        Some((team_id, prefix)) => {
            audit(
                &mut tx,
                actor,
                "admin.revoke",
                team_id,
                Some(id),
                serde_json::json!({ "prefix": prefix }),
            )
            .await?;
        }
        None => {
            let exists: Option<(Uuid,)> = sqlx::query_as(
                "SELECT id FROM admin_tokens
                 WHERE id = $1 AND ($2::uuid IS NULL OR team_id = $2)",
            )
            .bind(id)
            .bind(scope)
            .fetch_optional(&mut *tx)
            .await?;
            if exists.is_none() {
                return Err(BusError::not_found(format!(
                    "no administrative credential with id {id}"
                )));
            }
        }
    }
    tx.commit().await?;
    Ok(())
}

/// Resolve an administrative credential. `Ok(None)` is "not a valid, active
/// credential" — the caller turns that into 401 without saying which.
pub async fn resolve_admin(pool: &PgPool, raw: &str) -> BusResult<Option<AdminCtx>> {
    let raw = raw.trim();
    if !raw.starts_with(ADMIN_TOKEN_PREFIX) {
        return Ok(None);
    }
    let row: Option<(Uuid, Option<Uuid>, Option<String>)> = sqlx::query_as(
        "SELECT c.id, c.team_id, t.slug
         FROM admin_tokens c LEFT JOIN teams t ON t.id = c.team_id
         WHERE c.token_hash = $1 AND c.revoked_at IS NULL",
    )
    .bind(hash_token(raw))
    .fetch_optional(pool)
    .await?;
    let Some((id, team_id, team_slug)) = row else {
        return Ok(None);
    };
    // Best-effort, like agent tokens: usage bookkeeping must not fail a request.
    let _ = sqlx::query("UPDATE admin_tokens SET last_used_at = now() WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await;
    Ok(Some(AdminCtx {
        id,
        team_id,
        team_slug,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_normalised_and_bounded() {
        assert_eq!(check_name("agent name", "  Backend ").unwrap(), "backend");
        assert!(check_name("agent name", "").is_err());
        assert!(check_name("agent name", "with space").is_err());
        assert!(check_name("agent name", "a/b").is_err());
        assert!(check_name("agent name", &"x".repeat(MAX_NAME_BYTES + 1)).is_err());
    }

    #[test]
    fn labels_are_optional_and_bounded() {
        assert_eq!(check_label(None).unwrap(), None);
        assert_eq!(check_label(Some("  ".into())).unwrap(), None);
        assert_eq!(
            check_label(Some(" sesion backend ".into()))
                .unwrap()
                .as_deref(),
            Some("sesion backend")
        );
        assert!(check_label(Some("x".repeat(MAX_LABEL_BYTES + 1))).is_err());
        assert!(check_label(Some("a\nb".into())).is_err());
        assert!(check_display("team name", Some("Acme\x1b[31m".into())).is_err());
    }
}
