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

fn check_label(label: Option<String>) -> BusResult<Option<String>> {
    let Some(label) = label else {
        return Ok(None);
    };
    let label = label.trim().to_owned();
    if label.is_empty() {
        return Ok(None);
    }
    if label.len() > MAX_LABEL_BYTES {
        return Err(BusError::invalid(format!(
            "label is {} bytes; the limit is {MAX_LABEL_BYTES}",
            label.len()
        )));
    }
    if label.chars().any(char::is_control) {
        return Err(BusError::invalid(
            "label must not contain control characters",
        ));
    }
    Ok(Some(label))
}

async fn audit(
    pool: &PgPool,
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
    .execute(pool)
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
    let name = match name.map(|n| n.trim().to_owned()).filter(|n| !n.is_empty()) {
        Some(n) if n.len() > MAX_LABEL_BYTES => {
            return Err(BusError::invalid(format!(
                "team name is {} bytes; the limit is {MAX_LABEL_BYTES}",
                n.len()
            )));
        }
        Some(n) => n,
        None => slug.clone(),
    };
    let created: Option<(Uuid,)> = sqlx::query_as(
        "INSERT INTO teams (slug, name) VALUES ($1, $2)
         ON CONFLICT (slug) DO NOTHING RETURNING id",
    )
    .bind(&slug)
    .bind(&name)
    .fetch_optional(pool)
    .await?;
    if let Some((id,)) = created {
        audit(
            pool,
            actor,
            "team.create",
            Some(id),
            Some(id),
            serde_json::json!({ "slug": slug, "name": name }),
        )
        .await?;
    }
    let id = team_id_by_slug(pool, &slug).await?;
    let (name, agents): (String, i64) = sqlx::query_as(
        "SELECT t.name, (SELECT count(*) FROM agents a WHERE a.team_id = t.id)
         FROM teams t WHERE t.id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await?;
    Ok(TeamRow {
        id,
        slug,
        name,
        agents,
    })
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
/// an error.
pub async fn create_agent(
    pool: &PgPool,
    actor: Actor,
    team_id: Uuid,
    name: &str,
    display_name: Option<String>,
) -> BusResult<AgentRow> {
    let name = check_name("agent name", name)?;
    let display_name = check_label(display_name)?;
    let (id,): (Uuid,) = sqlx::query_as(
        r#"
        INSERT INTO agents (team_id, name, display_name)
        VALUES ($1, $2, $3)
        ON CONFLICT (team_id, name) DO UPDATE
            SET display_name = COALESCE(EXCLUDED.display_name, agents.display_name),
                disabled_at = NULL
        RETURNING id
        "#,
    )
    .bind(team_id)
    .bind(&name)
    .bind(&display_name)
    .fetch_one(pool)
    .await?;
    audit(
        pool,
        actor,
        "agent.create",
        Some(team_id),
        Some(id),
        serde_json::json!({ "name": name, "display_name": display_name }),
    )
    .await?;
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
    let row: Option<(Uuid,)> = sqlx::query_as(
        "UPDATE agents SET disabled_at = now() WHERE team_id = $1 AND name = $2 RETURNING id",
    )
    .bind(team_id)
    .bind(&name)
    .fetch_optional(pool)
    .await?;
    let Some((id,)) = row else {
        return Err(BusError::not_found(format!(
            "no agent '{name}' in this team"
        )));
    };
    audit(
        pool,
        actor,
        "agent.disable",
        Some(team_id),
        Some(id),
        serde_json::json!({ "name": name }),
    )
    .await?;
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
    let row: Option<(Uuid, String, bool)> = sqlx::query_as(
        "SELECT a.id, t.slug, (a.disabled_at IS NOT NULL)
         FROM agents a JOIN teams t ON t.id = a.team_id
         WHERE a.team_id = $1 AND a.name = $2",
    )
    .bind(team_id)
    .bind(&agent)
    .fetch_optional(pool)
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
    .fetch_one(pool)
    .await?;
    audit(
        pool,
        actor,
        "token.issue",
        Some(team_id),
        Some(id),
        serde_json::json!({ "agent": agent, "label": label, "prefix": prefix }),
    )
    .await?;
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
/// teams' ids. Revoking an already revoked token is a no-op that succeeds.
pub async fn revoke_token(
    pool: &PgPool,
    actor: Actor,
    team_id: Option<Uuid>,
    id: Uuid,
) -> BusResult<()> {
    let row: Option<(Uuid, String, String, bool)> = sqlx::query_as(
        "SELECT a.team_id, a.name, t.prefix, (t.revoked_at IS NOT NULL)
         FROM api_tokens t JOIN agents a ON a.id = t.agent_id
         WHERE t.id = $1 AND ($2::uuid IS NULL OR a.team_id = $2)",
    )
    .bind(id)
    .bind(team_id)
    .fetch_optional(pool)
    .await?;
    let Some((owner_team, agent, prefix, already)) = row else {
        return Err(BusError::not_found(format!("no token with id {id}")));
    };
    if already {
        return Ok(());
    }
    sqlx::query("UPDATE api_tokens SET revoked_at = now() WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    audit(
        pool,
        actor,
        "token.revoke",
        Some(owner_team),
        Some(id),
        serde_json::json!({ "agent": agent, "prefix": prefix }),
    )
    .await?;
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
    let (id,): (Uuid,) = sqlx::query_as(
        "INSERT INTO admin_tokens (team_id, token_hash, prefix, label, issued_by)
         VALUES ($1, $2, $3, $4, $5) RETURNING id",
    )
    .bind(team_id)
    .bind(hash_token(&raw))
    .bind(&prefix)
    .bind(&label)
    .bind(issued_by)
    .fetch_one(pool)
    .await?;
    audit(
        pool,
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
/// is global or belongs to another team is reported as not found.
pub async fn revoke_admin(
    pool: &PgPool,
    actor: Actor,
    scope: Option<Uuid>,
    id: Uuid,
) -> BusResult<()> {
    let row: Option<(Option<Uuid>, String, bool)> = sqlx::query_as(
        "SELECT team_id, prefix, (revoked_at IS NOT NULL) FROM admin_tokens
         WHERE id = $1 AND ($2::uuid IS NULL OR team_id = $2)",
    )
    .bind(id)
    .bind(scope)
    .fetch_optional(pool)
    .await?;
    let Some((team_id, prefix, already)) = row else {
        return Err(BusError::not_found(format!(
            "no administrative credential with id {id}"
        )));
    };
    if already {
        return Ok(());
    }
    sqlx::query("UPDATE admin_tokens SET revoked_at = now() WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    audit(
        pool,
        actor,
        "admin.revoke",
        team_id,
        Some(id),
        serde_json::json!({ "prefix": prefix }),
    )
    .await?;
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
    }
}
