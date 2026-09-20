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

/// Turn conversations on or off for a team.
pub async fn team_capability(pool: &PgPool, team: &str, conversations: bool) -> anyhow::Result<()> {
    let id = team_id(pool, team).await?;
    store::set_conversations(pool, Actor::Cli, id, conversations).await?;
    println!(
        "team '{team}': conversations {}",
        if conversations { "enabled" } else { "disabled" }
    );
    if conversations {
        println!(
            "Agents of this team now see create_conversation and the rest. Existing tools \
             are unchanged."
        );
    }
    Ok(())
}

/// Route a team's new conversations to a backend, and say plainly what that
/// does and does not do.
pub async fn team_backend(pool: &PgPool, team: &str, backend: &str) -> anyhow::Result<()> {
    let id = team_id(pool, team).await?;
    store::set_default_backend(pool, Actor::Cli, id, backend).await?;
    println!("team '{team}': new conversations are created on '{backend}'");
    println!(
        "Existing threads keep the backend they were created on. Nothing was migrated, and \
         nothing will be by this command."
    );
    if backend == "jetstream" {
        println!(
            "Check before anyone writes: the stream exists (`ai-crew-sync team stream --team \
             {team} --nats-url ...`) and the server was started with --nats-url. Without \
             both, sends are accepted and stay pending."
        );
    }
    Ok(())
}

/// Create or remove a team's stream. An operator action with its own
/// credential; the server process deliberately cannot do it.
pub async fn team_stream(
    pool: &PgPool,
    team: &str,
    nats_url: &str,
    credentials: Option<String>,
    remove: bool,
) -> anyhow::Result<()> {
    let id = team_id(pool, team).await?;
    let mut config = crate::store::jetstream::Config::new(nats_url.to_owned());
    config.credentials = credentials;
    if remove {
        let (routed,): (String,) =
            sqlx::query_as("SELECT default_backend FROM teams WHERE id = $1")
                .bind(id)
                .fetch_one(pool)
                .await?;
        if routed == "jetstream" {
            anyhow::bail!(
                "team '{team}' still creates its conversations on JetStream. Route it back \
                 with `team capability --backend postgres` first; deleting the stream now \
                 would drop bodies its threads still point at."
            );
        }
        // Routing new threads elsewhere says nothing about the ones already
        // there. A conversation keeps the backend it was created on, so its
        // bodies are still in this stream and deleting it would turn every
        // one of them into a tombstone.
        let (still_there,): (i64,) = sqlx::query_as(
            "SELECT count(*) FROM conversations WHERE team_id = $1 AND backend = 'jetstream'",
        )
        .bind(id)
        .fetch_one(pool)
        .await?;
        if still_there > 0 {
            anyhow::bail!(
                "{still_there} conversation(s) of team '{team}' still keep their bodies in \
                 this stream. Deleting it now would turn every one of those messages into \
                 a tombstone, and no rollback brings them back."
            );
        }
        crate::store::jetstream::JetStreamBackend::deprovision(&config, id).await?;
        println!("team '{team}': stream removed, with every body it held");
        return Ok(());
    }
    let name = crate::store::jetstream::JetStreamBackend::provision(&config, id).await?;
    // Two streams, because bodies and references cannot share a retention
    // policy: history must not be dropped, and references are a cache whose
    // truth is in Postgres.
    let inbox = crate::store::jetstream::JetStreamBackend::provision_inbox(&config, id).await?;
    println!("team '{team}': streams '{name}' (bodies) and '{inbox}' (inbox references) ready");
    println!(
        "This routes nobody. `team capability --team {team} --backend jetstream` is what \
         sends new conversations there."
    );
    Ok(())
}

/// Plan, and optionally run, a supervised move of conversation bodies.
pub async fn conversations_migrate(
    pool: &PgPool,
    team: &str,
    to: &str,
    conversations: &[String],
    nats_url: &str,
    credentials: Option<String>,
    apply: bool,
) -> anyhow::Result<()> {
    use crate::store::migrate::{self, Direction};

    let id = team_id(pool, team).await?;
    let direction = Direction::parse(to)?;
    let only: Vec<Uuid> = conversations
        .iter()
        .map(|c| {
            c.trim()
                .parse::<Uuid>()
                .map_err(|_| anyhow::anyhow!("'{c}' is not a conversation id"))
        })
        .collect::<anyhow::Result<_>>()?;

    let mut config = crate::store::jetstream::Config::new(nats_url.to_owned());
    config.credentials = credentials;
    // Fail here rather than halfway through: a move that cannot reach the
    // broker is a move that should not start.
    let jetstream = crate::store::jetstream::JetStreamBackend::connect(&config, id).await?;

    let plans = migrate::plan(pool, id, direction, &only).await?;
    if plans.is_empty() {
        println!("team '{team}': nothing matches");
        return Ok(());
    }
    println!("team '{team}' → {}", direction.target());
    for p in &plans {
        println!(
            "  {} {:<30} {} message(s), {}{}",
            p.conversation_id,
            p.title.chars().take(30).collect::<String>(),
            p.messages,
            human_bytes(p.bytes),
            match &p.blocked {
                Some(why) => format!("  — skipped: {why}"),
                None => String::new(),
            }
        );
    }
    if !apply {
        println!();
        println!("Dry run. Nothing was moved. Add --apply to run it.");
        println!(
            "This moves message bodies only. Attachments, memberships, receipts and ids \
             stay exactly where and as they are."
        );
        println!(
            "Each thread pauses writes only while its own tail is copied and verified;              reads keep working throughout, and the rest of the bus is untouched."
        );
        return Ok(());
    }

    for p in plans.iter().filter(|p| p.blocked.is_none()) {
        print!("  {} … ", p.conversation_id);
        use std::io::Write as _;
        let _ = std::io::stdout().flush();
        match migrate::run(pool, &jetstream, id, p.conversation_id, direction).await {
            Ok(o) => println!(
                "moved {} message(s), {} already there, {} verified",
                o.copied,
                o.skipped,
                human_bytes(o.bytes)
            ),
            Err(e) => {
                println!("FAILED: {e}");
                println!(
                    "      Nothing was cut over for this thread and its writes are open                      again. Fix the cause and run the same command: what is already                      verified is not copied twice."
                );
            }
        }
    }
    println!();
    println!(
        "Source bodies are kept. `conversations cleanup` drops them later, once you are          sure you will not roll back."
    );
    Ok(())
}

/// Drop source bodies whose move is old enough to trust.
pub async fn conversations_cleanup(
    pool: &PgPool,
    team: &str,
    rollback_window_hours: i64,
    apply: bool,
) -> anyhow::Result<()> {
    let id = team_id(pool, team).await?;
    let (count, bytes) =
        crate::store::migrate::cleanup(pool, id, rollback_window_hours, apply).await?;
    if count == 0 {
        println!(
            "team '{team}': nothing to drop (no move finished more than \
             {rollback_window_hours}h ago)"
        );
        return Ok(());
    }
    if apply {
        println!(
            "team '{team}': dropped {count} source body(ies), {} freed in Postgres",
            human_bytes(bytes)
        );
        println!("Rolling those threads back now needs the broker, not an older image.");
    } else {
        println!(
            "team '{team}': {count} source body(ies) could be dropped, {} in Postgres",
            human_bytes(bytes)
        );
        println!("Dry run. Add --apply to delete them.");
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
    // Publication, for a team routed off Postgres. Shown only when there is
    // something to show: on the default backend the queue is always empty
    // and a permanent "0 pending" line is noise.
    let (backend,): (String,) = sqlx::query_as("SELECT default_backend FROM teams WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await?;
    let outbox = crate::store::outbox::status(pool, id).await?;
    if backend != "postgres" || outbox.pending + outbox.leased + outbox.failed > 0 {
        println!("  backend         {backend}");
        println!(
            "  publication     {} pending, {} in flight, {} failed ({})",
            outbox.pending,
            outbox.leased,
            outbox.failed,
            human_bytes(outbox.pending_bytes)
        );
        if let Some(secs) = outbox.oldest_pending_seconds
            && secs > 300
        {
            println!(
                "  ⚠ the oldest unpublished message is {} minute(s) old. Check the broker \
                 and that a replica is draining (--publication-worker).",
                secs / 60
            );
        }
        if outbox.failed > 0 {
            println!(
                "  ⚠ {} message(s) will not be published. Their slots stay so the gap is \
                 visible; readers are told.",
                outbox.failed
            );
        }
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
    println!("Token for {agent}@{team} — shown once, store it now:");
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

    // The secret first: nothing that can fail stands between the mint and
    // the one time it is shown.
    println!();
    println!("Global administrative credential — shown once, store it now:");
    println!();
    println!("  {}", issued.token);
    println!();
    println!("Use it from your machine with `ai-crew-sync admin login --url <bus>`.");

    // Informational; a failure here must not look like a failed bootstrap.
    match store::list_admins(pool, None).await {
        Ok(rows) => {
            let active = rows
                .iter()
                .filter(|c| c.team.is_none() && !c.revoked)
                .count();
            println!(
                "{active} global credential(s) are now active; list them with `admin credential list`."
            );
        }
        Err(e) => eprintln!("(could not count active credentials: {e})"),
    }
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

/// Print the client configuration for the per-conversation stdio proxy.
///
/// `format` is `json` (the `.mcp.json` shape most MCP clients use) or `toml`
/// (Codex's `~/.codex/config.toml`). Neither carries a credential: the proxy
/// resolves one from the local profiles.
pub fn print_proxy_config(
    format: &str,
    role: Option<&str>,
    project: Option<&str>,
    profile: Option<&str>,
) {
    let mut args: Vec<String> = vec!["mcp".into(), "proxy".into()];
    for (flag, value) in [
        ("--role", role),
        ("--project", project),
        ("--profile", profile),
    ] {
        if let Some(v) = value.map(str::trim).filter(|v| !v.is_empty()) {
            args.push(flag.into());
            args.push(v.into());
        }
    }
    let exe = "ai-crew-sync";
    match format {
        "toml" => {
            println!("# ~/.codex/config.toml (or <repo>/.codex/config.toml in a trusted project)");
            println!("[mcp_servers.ai-crew-sync]");
            println!("command = \"{exe}\"");
            println!(
                "args = [{}]",
                args.iter()
                    .map(|a| format!("\"{a}\""))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            println!();
            println!("# No token here: credentials come from your local profiles");
            println!("# (`ai-crew-sync context profile add`), never from this file.");
        }
        _ => {
            let cfg = serde_json::json!({
                "mcpServers": {
                    "ai-crew-sync": { "command": exe, "args": args }
                }
            });
            println!("{}", serde_json::to_string_pretty(&cfg).unwrap_or_default());
        }
    }
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
