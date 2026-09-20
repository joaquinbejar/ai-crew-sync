//! Operator-facing commands. These bypass MCP entirely and talk to Postgres
//! directly. They remain the emergency path and the only way to bootstrap the
//! first administrative credential; day-to-day administration goes through
//! the remote API with `ai-crew-sync admin …`.
//!
//! Every mutation is delegated to [`crate::store::admin`] with
//! [`Actor::Cli`], so the audit trail is the same whichever door was used.

use anyhow::bail;
use sqlx::PgPool;
use uuid::Uuid;

use crate::store::admin::{self as store, Actor};

async fn team_id(pool: &PgPool, slug: &str) -> anyhow::Result<Uuid> {
    Ok(store::team_id_by_slug(pool, slug).await?)
}

pub async fn team_create(pool: &PgPool, slug: &str, name: Option<String>) -> anyhow::Result<()> {
    let team = store::create_team(pool, Actor::Cli, slug, name).await?;
    println!("team '{}' ready", team.slug);
    Ok(())
}

pub async fn team_list(pool: &PgPool) -> anyhow::Result<()> {
    let rows = store::list_teams(pool).await?;
    if rows.is_empty() {
        println!("(no teams yet — create one with `team create --slug <slug>`)");
    }
    for t in rows {
        println!("{:<20} {:<30} {} agent(s)", t.slug, t.name, t.agents);
    }
    Ok(())
}

fn human_bytes(n: i64) -> String {
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// Set or clear a team's attachment quota. `None` clears it (unlimited).
pub async fn team_quota(pool: &PgPool, team: &str, bytes: Option<i64>) -> anyhow::Result<()> {
    let id = team_id(pool, team).await?;
    if let Some(b) = bytes
        && b <= 0
    {
        anyhow::bail!("a quota must be positive; omit --bytes to clear it");
    }
    sqlx::query("UPDATE teams SET attachment_bytes_limit = $1 WHERE id = $2")
        .bind(bytes)
        .bind(id)
        .execute(pool)
        .await?;
    match bytes {
        Some(b) => println!("team '{team}' attachment quota set to {}", human_bytes(b)),
        None => println!("team '{team}' attachment quota cleared (unlimited)"),
    }
    Ok(())
}

/// Report what a team is storing. Counts and bytes only — never content, so
/// this is safe to run for a team you are not on.
pub async fn team_usage(pool: &PgPool, team: &str) -> anyhow::Result<()> {
    let id = team_id(pool, team).await?;
    let u = crate::store::quota::usage(pool, id).await?;

    let quota = match u.attachment_bytes_limit {
        Some(limit) => format!(
            "{} of {} ({:.1}%)",
            human_bytes(u.attachment_bytes),
            human_bytes(limit),
            u.percent_used().unwrap_or(0.0)
        ),
        None => format!("{} (no quota set)", human_bytes(u.attachment_bytes)),
    };

    println!("team '{team}'");
    println!(
        "  attachments     {quota} across {} file(s)",
        u.attachment_count
    );
    println!("  messages        {}", u.messages);
    println!("  note revisions  {}", u.note_revisions);
    println!("  task events     {}", u.task_events);
    if let Some(oldest) = u.oldest_message {
        let days = (chrono::Utc::now() - oldest).num_days();
        println!("  oldest message  {days} day(s) ago");
    }
    if let Some(pct) = u.percent_used()
        && pct >= 80.0
    {
        println!();
        println!("  ⚠ {pct:.0}% of the attachment quota is in use — raise it with");
        println!("    `team quota --team {team} --bytes N`, or free space with `team prune`.");
    }
    Ok(())
}

/// Trim history older than `days`. Dry run by default at the call site.
pub async fn team_prune(pool: &PgPool, team: &str, days: i64, apply: bool) -> anyhow::Result<()> {
    let id = team_id(pool, team).await?;
    let report = crate::store::quota::prune(pool, id, days, !apply).await?;

    let verb = if report.dry_run {
        "would delete"
    } else {
        "deleted"
    };
    println!("team '{team}', anything older than {days} day(s):");
    println!("  {verb} {} message(s)", report.messages);
    println!("  {verb} {} note revision(s)", report.note_revisions);
    println!("  {verb} {} task event(s)", report.task_events);
    println!(
        "  {verb} attachments worth {}",
        human_bytes(report.attachments_freed_bytes)
    );
    if report.dry_run {
        println!();
        println!("dry run — nothing was deleted. Re-run with --apply to do it.");
        println!("Notes and tasks themselves are never pruned, only their history.");
    }
    Ok(())
}

pub async fn agent_add(
    pool: &PgPool,
    team: &str,
    name: &str,
    display_name: Option<String>,
    issue_token: bool,
) -> anyhow::Result<()> {
    let tid = team_id(pool, team).await?;
    let agent = store::create_agent(pool, Actor::Cli, tid, name, display_name).await?;
    println!("agent '{}' ready in team '{team}'", agent.name);

    if issue_token {
        token_issue(pool, team, &agent.name, None).await?;
    }
    Ok(())
}

pub async fn agent_list(pool: &PgPool, team: &str) -> anyhow::Result<()> {
    let tid = team_id(pool, team).await?;
    for a in store::list_agents(pool, tid).await? {
        let flag = if a.disabled { " [disabled]" } else { "" };
        println!(
            "{:<24} {:<28} {} active token(s){flag}",
            a.name,
            a.display_name.unwrap_or_default(),
            a.active_tokens
        );
    }
    Ok(())
}

pub async fn agent_disable(pool: &PgPool, team: &str, name: &str) -> anyhow::Result<()> {
    let tid = team_id(pool, team).await?;
    match store::disable_agent(pool, Actor::Cli, tid, name).await {
        Err(crate::error::BusError::NotFound(_)) => bail!("no agent '{name}' in team '{team}'"),
        other => other?,
    }
    println!("agent '{name}' disabled; its tokens no longer authenticate");
    Ok(())
}

pub async fn token_issue(
    pool: &PgPool,
    team: &str,
    agent: &str,
    label: Option<String>,
) -> anyhow::Result<()> {
    let tid = team_id(pool, team).await?;
    let issued = match store::issue_token(pool, Actor::Cli, tid, agent, label).await {
        Err(crate::error::BusError::NotFound(_)) => {
            bail!("no agent '{agent}' in team '{team}' — add it with `agent add` first")
        }
        other => other?,
    };

    println!();
    println!(
        "Token for {}@{} — shown once, store it now:",
        issued.agent, issued.team
    );
    println!();
    println!("  {}", issued.token);
    println!();
    Ok(())
}

pub async fn token_list(pool: &PgPool, team: &str) -> anyhow::Result<()> {
    let tid = team_id(pool, team).await?;
    for t in store::list_tokens(pool, tid).await? {
        let used = t
            .last_used_at
            .map(|d| d.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
            .unwrap_or_else(|| "never".into());
        let flag = if t.revoked { " [revoked]" } else { "" };
        println!(
            "{}  {:<20} {}…  last used {used}  {}{flag}",
            t.id,
            t.agent,
            t.prefix,
            t.label.unwrap_or_default()
        );
    }
    Ok(())
}

pub async fn token_revoke(pool: &PgPool, id: Uuid) -> anyhow::Result<()> {
    match store::revoke_token(pool, Actor::Cli, None, id).await {
        Err(crate::error::BusError::NotFound(_)) => bail!("no token with id {id}"),
        other => other?,
    }
    println!("token {id} revoked");
    Ok(())
}

// ------------------------------------------------- administrative credentials --

/// Mint a global administrative credential. The one operation that needs a
/// database connection and no prior credential: everything else can be done
/// remotely with the credential this prints.
pub async fn admin_bootstrap(pool: &PgPool, label: Option<String>) -> anyhow::Result<()> {
    let issued = store::grant_admin(pool, Actor::Cli, None, label).await?;
    let active = store::list_admins(pool, None)
        .await?
        .into_iter()
        .filter(|c| c.team.is_none() && !c.revoked)
        .count();

    println!();
    println!("Global administrative credential — shown once, store it now:");
    println!();
    println!("  {}", issued.token);
    println!();
    println!("Use it from your machine with `ai-crew-sync admin login --url <bus>`.");
    println!(
        "{active} global credential(s) are now active; list them with `admin credential list`."
    );
    Ok(())
}

pub async fn admin_credential_list(pool: &PgPool, team: Option<&str>) -> anyhow::Result<()> {
    let tid = match team {
        Some(slug) => Some(team_id(pool, slug).await?),
        None => None,
    };
    let rows = store::list_admins(pool, tid).await?;
    if rows.is_empty() {
        println!("(no administrative credentials — mint the first with `admin bootstrap`)");
    }
    for c in rows {
        print_admin_row(&c);
    }
    Ok(())
}

/// One line per credential, shared with the remote CLI so both listings read
/// the same.
pub fn print_admin_row(c: &store::AdminRow) {
    let used = c
        .last_used_at
        .map(|d| d.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_else(|| "never".into());
    let scope = c.team.as_deref().unwrap_or("(global)");
    let flag = if c.revoked { " [revoked]" } else { "" };
    println!(
        "{}  {scope:<20} {}…  last used {used}  {}{flag}",
        c.id,
        c.prefix,
        c.label.as_deref().unwrap_or_default()
    );
}

pub async fn admin_credential_revoke(pool: &PgPool, id: Uuid) -> anyhow::Result<()> {
    match store::revoke_admin(pool, Actor::Cli, None, id).await {
        Err(crate::error::BusError::NotFound(_)) => {
            bail!("no administrative credential with id {id}")
        }
        other => other?,
    }
    println!("administrative credential {id} revoked");
    Ok(())
}

/// Print the exact `.mcp.json` block a teammate drops into their repo.
pub fn print_mcp_config(url: &str, token: &str, session: Option<&str>) {
    let mut headers = serde_json::Map::new();
    headers.insert("Authorization".into(), format!("Bearer {token}").into());
    // Only when asked for: an empty header would name a session called "",
    // which is the shared one you get by not sending the header at all.
    if let Some(session) = session.map(str::trim).filter(|s| !s.is_empty()) {
        headers.insert(crate::auth::SESSION_HEADER.into(), session.into());
    }
    let cfg = serde_json::json!({
        "mcpServers": {
            "ai-crew-sync": {
                "type": "http",
                "url": url,
                "headers": headers
            }
        }
    });
    println!("{}", serde_json::to_string_pretty(&cfg).unwrap());
}
