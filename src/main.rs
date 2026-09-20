use ai_crew_sync::{MIGRATOR, admin, admin_cli, client, context, proxy, serve, webhooks};
use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[derive(Parser)]
#[command(
    name = "ai-crew-sync",
    version,
    about = "MCP coordination bus for a team of AI coding agents, backed by Postgres"
)]
struct Cli {
    /// Postgres connection string.
    #[arg(long, env = "DATABASE_URL", global = true)]
    database_url: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Apply pending database migrations and exit.
    Migrate,
    /// Run the MCP server.
    Serve(ServeArgs),
    /// Manage teams.
    #[command(subcommand)]
    Team(TeamCmd),
    /// Manage agents (one per teammate's coding agent).
    #[command(subcommand)]
    Agent(AgentCmd),
    /// Manage bearer tokens.
    #[command(subcommand)]
    Token(TokenCmd),
    /// Manage outgoing webhooks (Slack/Discord/generic).
    #[command(subcommand)]
    Webhook(WebhookCmd),
    /// Administrative credentials: bootstrap the first one next to Postgres,
    /// then administer the bus remotely with it.
    #[command(subcommand)]
    Admin(AdminCmd),
    /// Local connection context: profiles (which bus, as whom) and project
    /// defaults (.acs.toml), so clients need no BUS_TOKEN export.
    #[command(subcommand)]
    Context(ContextCmd),
    /// Local MCP transports.
    #[command(subcommand)]
    Mcp(McpCmd),
    /// Talk to a running bus from the console, as an agent. Everything the MCP
    /// tools can do: send/read messages, claim tasks, notes, presence.
    Client(client::ClientArgs),
    /// Print a ready-to-paste .mcp.json snippet.
    McpConfig {
        /// Public URL of the /mcp endpoint.
        #[arg(long, default_value = "http://localhost:8787/mcp")]
        url: String,
        /// The token issued to this agent.
        #[arg(long)]
        token: String,
        /// Working context for this config — usually the repository name.
        /// Separates this agent's presence, claims and locks from its own
        /// other sessions. Omit for the shared session.
        #[arg(long)]
        session: Option<String>,
    },
}

#[derive(Args)]
struct ServeArgs {
    /// Address to bind.
    #[arg(long, env = "BUS_BIND", default_value = "0.0.0.0:8787")]
    bind: String,

    /// Comma-separated hostnames accepted in the Host header. Use "*" to accept
    /// any host, which is fine behind a proxy that already validates it.
    #[arg(
        long,
        env = "BUS_ALLOWED_HOSTS",
        default_value = "localhost,127.0.0.1,0.0.0.0,[::1]"
    )]
    allowed_hosts: String,

    /// Comma-separated browser origins to accept. Empty disables the check.
    #[arg(long, env = "BUS_ALLOWED_ORIGINS", default_value = "")]
    allowed_origins: String,

    /// Run migrations on startup.
    #[arg(long, env = "BUS_AUTO_MIGRATE", default_value_t = true)]
    auto_migrate: bool,

    /// Largest MCP request body accepted, in bytes. Rejected with 413 before
    /// the JSON is parsed.
    #[arg(long, env = "BUS_MAX_REQUEST_BYTES",
          default_value_t = serve::DEFAULT_MAX_REQUEST_BYTES)]
    max_request_bytes: usize,

    /// Requests per minute per token, enforced in-process. 0 disables it.
    /// With several replicas the effective ceiling is per replica — put a
    /// hard global limit in the reverse proxy.
    #[arg(long, env = "BUS_RATE_LIMIT_PER_MINUTE",
          default_value_t = serve::DEFAULT_RATE_LIMIT_PER_MINUTE)]
    rate_limit_per_minute: u32,

    /// Signs the dashboard's read-only session cookies. Set the same value on
    /// every replica so a session works across all of them; when unset a
    /// random key is generated at startup, so sessions end at restart.
    #[arg(long, env = "BUS_DASHBOARD_SECRET")]
    dashboard_secret: Option<String>,
}

#[derive(Subcommand)]
enum TeamCmd {
    /// Set or clear a team's attachment storage quota.
    Quota {
        #[arg(long)]
        team: String,
        /// Total attachment bytes allowed. Omit to clear (unlimited).
        #[arg(long)]
        bytes: Option<i64>,
    },
    /// Report what a team is storing (counts and bytes; never content).
    Usage {
        #[arg(long)]
        team: String,
    },
    /// Trim history older than N days. Dry run unless --apply is passed.
    Prune {
        #[arg(long)]
        team: String,
        #[arg(long, default_value_t = 90)]
        older_than_days: i64,
        /// Actually delete. Without this the command only reports.
        #[arg(long)]
        apply: bool,
    },
    Create {
        #[arg(long)]
        slug: String,
        #[arg(long)]
        name: Option<String>,
    },
    List,
}

#[derive(Subcommand)]
enum AgentCmd {
    Add {
        #[arg(long)]
        team: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        display_name: Option<String>,
        /// Also mint a token for the new agent.
        #[arg(long, default_value_t = true)]
        with_token: bool,
    },
    List {
        #[arg(long)]
        team: String,
    },
    Disable {
        #[arg(long)]
        team: String,
        #[arg(long)]
        name: String,
    },
}

#[derive(Subcommand)]
enum WebhookCmd {
    Add {
        #[arg(long)]
        team: String,
        /// Destination URL (Slack/Discord webhook URL, or any JSON endpoint).
        #[arg(long)]
        url: String,
        /// Payload format: slack, discord or generic.
        #[arg(long, default_value = "slack")]
        kind: String,
        /// Comma-separated event kinds: message,task,lock,note.
        #[arg(long, default_value = "message,task")]
        events: String,
        /// Only forward messages from this channel (messages only).
        #[arg(long)]
        channel: Option<String>,
    },
    List {
        #[arg(long)]
        team: String,
    },
    Remove {
        #[arg(long)]
        id: Uuid,
    },
}

#[derive(Subcommand)]
enum AdminCmd {
    /// Mint a GLOBAL administrative credential. Needs DATABASE_URL: this is
    /// the one step that runs next to Postgres, once per deployment.
    Bootstrap {
        #[arg(long)]
        label: Option<String>,
    },
    /// Store an administrative credential for this machine. The credential is
    /// read from a hidden prompt (or stdin with --token-stdin), verified
    /// against the bus, and saved with mode 0600.
    Login {
        /// Base URL of the bus, e.g. https://bus.example.com:8443
        #[arg(long)]
        url: String,
        /// Read the credential from stdin instead of prompting (for scripts).
        #[arg(long)]
        token_stdin: bool,
    },
    /// Forget the stored credential (it stays valid on the bus; revoke it
    /// with `admin credential revoke` if it should not).
    Logout,
    /// Which credential is stored, and what it may administer.
    Whoami,
    /// Teams (global credential only).
    #[command(subcommand)]
    Team(AdminTeamCmd),
    /// Mint an administrative credential for a team, or another global one
    /// with --global. Global credential only.
    Grant {
        #[arg(long, conflicts_with = "global")]
        team: Option<String>,
        #[arg(long)]
        global: bool,
        #[arg(long)]
        label: Option<String>,
    },
    /// Administrative credentials (not agent tokens — those are `token …`).
    #[command(subcommand)]
    Credential(AdminCredentialCmd),
    /// Agents of a team.
    #[command(subcommand)]
    Agent(AdminAgentCmd),
    /// Agent tokens of a team.
    #[command(subcommand)]
    Token(AdminTokenCmd),
}

#[derive(Subcommand)]
enum McpCmd {
    /// Serve MCP over stdio for ONE conversation, forwarding every tool to
    /// the bus as one agent in one session. Start it from your MCP client's
    /// config (command: ai-crew-sync, args: [mcp, proxy]); credentials come
    /// from local profiles, never from the client config.
    Proxy {
        #[command(flatten)]
        select: ContextSelect,
        /// Initial project label (defaults to the project's .acs.toml).
        #[arg(long)]
        project: Option<String>,
        /// Initial role label: implementation, design, review, …
        #[arg(long)]
        role: Option<String>,
        /// Initial default channel (defaults to the project's .acs.toml).
        #[arg(long)]
        channel: Option<String>,
        /// Conversation id this process serves, for hosts that can set one
        /// per window. Derives a stable session: the same id reconnects to
        /// the same session. Otherwise CLAUDE_CODE_SESSION_ID or the request
        /// metadata is used, else this process is the conversation.
        #[arg(long, env = "BUS_HOST_SESSION")]
        host_session: Option<String>,
    },
}

/// Selection flags shared by the `context` commands: the same inputs the
/// console client and the proxy resolve with.
#[derive(Args, Clone)]
struct ContextSelect {
    /// Connect with this profile (BUS_PROFILE).
    #[arg(long, env = "BUS_PROFILE")]
    profile: Option<String>,
    /// Directory whose project defaults apply; the current directory by
    /// default (BUS_PROJECT_DIR).
    #[arg(long, env = "BUS_PROJECT_DIR")]
    project_dir: Option<std::path::PathBuf>,
    /// Explicit token (BUS_TOKEN); wins over every profile.
    #[arg(long, env = "BUS_TOKEN", hide_env_values = true)]
    token: Option<String>,
    /// Explicit endpoint (BUS_URL).
    #[arg(long, env = "BUS_URL")]
    url: Option<String>,
    /// Session label (BUS_SESSION).
    #[arg(long, env = "BUS_SESSION")]
    session: Option<String>,
}

impl ContextSelect {
    fn inputs(&self) -> anyhow::Result<context::Inputs> {
        Ok(context::Inputs {
            config_dir: context::config_dir()?,
            explicit_url: self.url.clone(),
            explicit_token: self.token.clone(),
            explicit_session: self.session.clone(),
            profile: self.profile.clone(),
            project_dir: self.project_dir.clone(),
        })
    }
}

#[derive(Subcommand)]
enum ContextCmd {
    /// What would be used right here: endpoint, profile, expected identity,
    /// token entry (prefix only), project. Never prints the secret.
    Show {
        #[command(flatten)]
        select: ContextSelect,
        #[arg(long)]
        json: bool,
    },
    /// Resolve, then ask the bus who the token really is; fails when it is
    /// not the agent and team the profile expects.
    Verify {
        #[command(flatten)]
        select: ContextSelect,
    },
    /// Write the project's defaults (.acs.toml at the project root): which
    /// approved profile and logical project every window here uses unless
    /// it selects otherwise. Never a credential.
    SetProject {
        /// Profile name; must exist locally.
        #[arg(long)]
        profile: String,
        /// Logical project name; also the token-file entry. Defaults to the
        /// directory name.
        #[arg(long)]
        project: Option<String>,
        /// Channel this project's sessions post to by default.
        #[arg(long)]
        channel: Option<String>,
        /// Token-file entry, when it differs from the project name.
        #[arg(long)]
        key: Option<String>,
        /// Project root to write into; the current directory by default.
        #[arg(long)]
        dir: Option<std::path::PathBuf>,
    },
    /// Manage local profiles (~/.config/ai-crew-sync/profiles.toml).
    #[command(subcommand)]
    Profile(ContextProfileCmd),
}

#[derive(Subcommand)]
enum ContextProfileCmd {
    /// Add or replace a profile: endpoint, expected team and agent, and the
    /// tokens-<team> file holding its credentials.
    Add {
        #[arg(long)]
        name: String,
        /// Base URL of the bus, e.g. https://bus.example.com:8443
        #[arg(long)]
        url: String,
        #[arg(long)]
        team: String,
        #[arg(long)]
        agent: String,
        /// Token file name inside the configuration directory; defaults to
        /// tokens-<team>.
        #[arg(long)]
        tokens: Option<String>,
        /// Entry to use when the project names none (then `_base`).
        #[arg(long)]
        key: Option<String>,
        /// Also make it the user default.
        #[arg(long)]
        default: bool,
    },
    List,
    /// Set (or, with --clear, unset) the user default profile.
    Default {
        name: Option<String>,
        #[arg(long)]
        clear: bool,
    },
    Remove {
        name: String,
    },
}

#[derive(Subcommand)]
enum AdminTeamCmd {
    Add {
        #[arg(long)]
        slug: String,
        #[arg(long)]
        name: Option<String>,
    },
    List,
}

#[derive(Subcommand)]
enum AdminCredentialCmd {
    /// List administrative credentials: every team's for a global
    /// credential, its own team's for a team credential.
    List {
        /// Only this team's credentials (global ones are never included).
        #[arg(long)]
        team: Option<String>,
        /// Talk to Postgres (DATABASE_URL) instead of the bus. For emergencies.
        #[arg(long)]
        local: bool,
    },
    /// Revoke an administrative credential. It stops authorising immediately.
    Revoke {
        #[arg(long)]
        id: Uuid,
        /// Talk to Postgres (DATABASE_URL) instead of the bus. For emergencies.
        #[arg(long)]
        local: bool,
    },
}

#[derive(Subcommand)]
enum AdminAgentCmd {
    Add {
        #[arg(long)]
        team: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        display_name: Option<String>,
    },
    List {
        #[arg(long)]
        team: String,
    },
}

#[derive(Subcommand)]
enum AdminTokenCmd {
    /// Mint a token for an agent, verify it authenticates as that agent, and
    /// print it once — or write it to ~/.config/ai-crew-sync/tokens-<team>
    /// with --save --repo <name> (then it is never printed).
    Issue {
        #[arg(long)]
        team: String,
        #[arg(long)]
        agent: String,
        #[arg(long)]
        label: Option<String>,
        /// Save the token as `<repo>=<token>` in tokens-<team> instead of
        /// printing it.
        #[arg(long, requires = "repo")]
        save: bool,
        /// Entry name in tokens-<team>; usually the repository name.
        #[arg(long, requires = "save")]
        repo: Option<String>,
    },
    List {
        #[arg(long)]
        team: String,
    },
    Revoke {
        #[arg(long)]
        team: String,
        #[arg(long)]
        id: Uuid,
    },
}

#[derive(Subcommand)]
enum TokenCmd {
    Issue {
        #[arg(long)]
        team: String,
        #[arg(long)]
        agent: String,
        #[arg(long)]
        label: Option<String>,
    },
    List {
        #[arg(long)]
        team: String,
    },
    Revoke {
        #[arg(long)]
        id: Uuid,
    },
}

fn split_csv(s: &str) -> Vec<String> {
    if s.trim() == "*" {
        return Vec::new();
    }
    s.split(',')
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty())
        .collect()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();
    let cli = Cli::parse();

    // The stdio proxy owns stdout for MCP framing; its logs go to stderr.
    // Everything else keeps logging to stdout as before.
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "ai_crew_sync=info,tower_http=info,warn".into());
    if matches!(cli.command, Command::Mcp(_)) {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }

    // Commands that talk to the bus over HTTP (or to nothing at all) do not
    // need a database connection.
    match cli.command {
        Command::McpConfig {
            url,
            token,
            session,
        } => {
            admin::print_mcp_config(&url, &token, session.as_deref());
            return Ok(());
        }
        Command::Client(args) => return client::run(args).await,
        Command::Admin(cmd) if !admin_needs_database(&cmd) => return run_admin_remote(cmd).await,
        Command::Context(cmd) => return run_context(cmd).await,
        Command::Mcp(McpCmd::Proxy {
            select,
            project,
            role,
            channel,
            host_session,
        }) => {
            let inputs = select.inputs()?;
            let state_dir = inputs.config_dir.clone();
            return proxy::run(proxy::ProxyOptions {
                inputs,
                project,
                role,
                channel,
                host_session,
                state_dir,
            })
            .await;
        }
        _ => {}
    }

    let url = cli
        .database_url
        .clone()
        .context("DATABASE_URL is not set (pass --database-url or set the env var)")?;
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect(&url)
        .await
        .context("could not connect to Postgres")?;

    dispatch(cli.command, pool).await
}

/// Every command that needs a database, once the pool exists.
///
/// Kept out of `main` so process setup — logging, `.env`, argument parsing,
/// the two commands that need no database — reads as its own short function
/// rather than as a preamble to a 100-line match.
async fn dispatch(command: Command, pool: sqlx::PgPool) -> anyhow::Result<()> {
    match command {
        // `main` routes these before opening a pool; they cannot arrive here.
        Command::McpConfig { .. } | Command::Client(_) | Command::Context(_) | Command::Mcp(_) => {
            unreachable!("handled in main")
        }

        Command::Migrate => {
            MIGRATOR.run(&pool).await?;
            println!("migrations applied");
        }

        Command::Serve(args) => {
            if args.auto_migrate {
                MIGRATOR.run(&pool).await?;
                tracing::info!("migrations applied");
            }
            let allowed_hosts = split_csv(&args.allowed_hosts);
            if allowed_hosts.is_empty() {
                tracing::warn!(
                    "Host header validation is disabled; make sure a proxy in front of this \
                     service validates it"
                );
            }
            let dashboard_secret = match args.dashboard_secret {
                Some(s) if !s.trim().is_empty() => s.into_bytes(),
                _ => {
                    tracing::warn!(
                        "BUS_DASHBOARD_SECRET is unset: dashboard sessions end at restart \
                         and are not shared between replicas"
                    );
                    ai_crew_sync::auth::generate_token().into_bytes()
                }
            };
            serve::run(
                pool,
                serve::ServeOptions {
                    bind: args.bind,
                    allowed_hosts,
                    allowed_origins: split_csv(&args.allowed_origins),
                    max_request_bytes: args.max_request_bytes,
                    rate_limit_per_minute: args.rate_limit_per_minute,
                    dashboard_secret,
                },
            )
            .await?;
        }

        Command::Team(cmd) => match cmd {
            TeamCmd::Create { slug, name } => admin::team_create(&pool, &slug, name).await?,
            TeamCmd::List => admin::team_list(&pool).await?,
            TeamCmd::Quota { team, bytes } => admin::team_quota(&pool, &team, bytes).await?,
            TeamCmd::Usage { team } => admin::team_usage(&pool, &team).await?,
            TeamCmd::Prune {
                team,
                older_than_days,
                apply,
            } => admin::team_prune(&pool, &team, older_than_days, apply).await?,
        },

        Command::Agent(cmd) => match cmd {
            AgentCmd::Add {
                team,
                name,
                display_name,
                with_token,
            } => admin::agent_add(&pool, &team, &name, display_name, with_token).await?,
            AgentCmd::List { team } => admin::agent_list(&pool, &team).await?,
            AgentCmd::Disable { team, name } => admin::agent_disable(&pool, &team, &name).await?,
        },

        Command::Token(cmd) => match cmd {
            TokenCmd::Issue { team, agent, label } => {
                admin::token_issue(&pool, &team, &agent, label).await?
            }
            TokenCmd::List { team } => admin::token_list(&pool, &team).await?,
            TokenCmd::Revoke { id } => admin::token_revoke(&pool, id).await?,
        },

        Command::Admin(cmd) => match cmd {
            AdminCmd::Bootstrap { label } => admin::admin_bootstrap(&pool, label).await?,
            AdminCmd::Credential(AdminCredentialCmd::List { team, .. }) => {
                admin::admin_credential_list(&pool, team.as_deref()).await?
            }
            AdminCmd::Credential(AdminCredentialCmd::Revoke { id, .. }) => {
                admin::admin_credential_revoke(&pool, id).await?
            }
            // `main` routes every remote admin command before opening a pool.
            other => unreachable!(
                "remote admin command reached dispatch: {}",
                admin_name(&other)
            ),
        },

        Command::Webhook(cmd) => match cmd {
            WebhookCmd::Add {
                team,
                url,
                kind,
                events,
                channel,
            } => webhooks::webhook_add(&pool, &team, &url, &kind, &events, channel).await?,
            WebhookCmd::List { team } => webhooks::webhook_list(&pool, &team).await?,
            WebhookCmd::Remove { id } => webhooks::webhook_remove(&pool, id).await?,
        },
    }

    Ok(())
}

/// Which `admin` commands run next to Postgres: bootstrap always, and the
/// credential commands only when asked with --local.
fn admin_needs_database(cmd: &AdminCmd) -> bool {
    match cmd {
        AdminCmd::Bootstrap { .. } => true,
        AdminCmd::Credential(AdminCredentialCmd::List { local, .. })
        | AdminCmd::Credential(AdminCredentialCmd::Revoke { local, .. }) => *local,
        _ => false,
    }
}

fn admin_name(cmd: &AdminCmd) -> &'static str {
    match cmd {
        AdminCmd::Bootstrap { .. } => "bootstrap",
        AdminCmd::Login { .. } => "login",
        AdminCmd::Logout => "logout",
        AdminCmd::Whoami => "whoami",
        AdminCmd::Team(_) => "team",
        AdminCmd::Grant { .. } => "grant",
        AdminCmd::Credential(_) => "credential",
        AdminCmd::Agent(_) => "agent",
        AdminCmd::Token(_) => "token",
    }
}

fn rfc3339(v: &serde_json::Value) -> String {
    v.as_str()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_else(|| "never".into())
}

/// The remote `admin …` commands: everything except bootstrap and --local.
async fn run_admin_remote(cmd: AdminCmd) -> anyhow::Result<()> {
    let dir = admin_cli::config_dir()?;

    // Login and logout are the two that do not need a stored credential.
    match cmd {
        AdminCmd::Login { url, token_stdin } => {
            let token = admin_cli::read_credential(token_stdin)?;
            let (me, path) = admin_cli::login(&dir, &url, token).await?;
            println!(
                "logged in as a {} administrator{}; credential stored in {}",
                me["scope"].as_str().unwrap_or("?"),
                me["team"]
                    .as_str()
                    .map(|t| format!(" of team '{t}'"))
                    .unwrap_or_default(),
                path.display()
            );
            return Ok(());
        }
        AdminCmd::Logout => {
            if admin_cli::remove_config(&dir)? {
                println!(
                    "credential forgotten (it is still valid on the bus; revoke it with `admin credential revoke` if it should not be)"
                );
            } else {
                println!("not logged in");
            }
            return Ok(());
        }
        _ => {}
    }

    let api = admin_cli::Api::new(admin_cli::load_config(&dir)?);
    match cmd {
        AdminCmd::Bootstrap { .. } | AdminCmd::Login { .. } | AdminCmd::Logout => {
            unreachable!("handled above")
        }
        AdminCmd::Whoami => {
            let me = api.whoami().await?;
            println!(
                "{} administrator{} ({}) at {}",
                me["scope"].as_str().unwrap_or("?"),
                me["team"]
                    .as_str()
                    .map(|t| format!(" of team '{t}'"))
                    .unwrap_or_default(),
                me["credential_id"].as_str().unwrap_or("?"),
                api.config().url
            );
        }
        AdminCmd::Team(AdminTeamCmd::Add { slug, name }) => {
            let v = api.create_team(&slug, name.as_deref()).await?;
            println!(
                "team '{}' ready",
                v["team"]["slug"].as_str().unwrap_or(&slug)
            );
        }
        AdminCmd::Team(AdminTeamCmd::List) => {
            let v = api.list_teams().await?;
            for t in v["teams"].as_array().into_iter().flatten() {
                println!(
                    "{:<20} {:<30} {} agent(s)",
                    t["slug"].as_str().unwrap_or_default(),
                    t["name"].as_str().unwrap_or_default(),
                    t["agents"]
                );
            }
        }
        AdminCmd::Grant {
            team,
            global,
            label,
        } => {
            if team.is_none() && !global {
                anyhow::bail!("say which credential to mint: --team <slug> or --global");
            }
            let v = api
                .grant_credential(team.as_deref(), label.as_deref())
                .await?;
            let c = &v["credential"];
            println!();
            match c["team"].as_str() {
                Some(t) => {
                    println!("Administrative credential for team '{t}' — shown once, store it now:")
                }
                None => println!("Global administrative credential — shown once, store it now:"),
            }
            println!();
            println!("  {}", c["token"].as_str().unwrap_or_default());
            println!();
            println!(
                "Its holder stores it with `ai-crew-sync admin login --url {}`.",
                api.config().url
            );
        }
        AdminCmd::Credential(AdminCredentialCmd::List { team, .. }) => {
            let v = api.list_credentials().await?;
            // The server already scopes the listing to the credential; --team
            // narrows a global listing further (a team credential's is its
            // own team whatever the flag says, so the flag is checked).
            let wanted = team.as_deref().map(|t| t.trim().to_lowercase());
            let rows: Vec<serde_json::Value> = v["credentials"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|c| match &wanted {
                    Some(t) => c["team"].as_str() == Some(t.as_str()),
                    None => true,
                })
                .collect();
            if rows.is_empty() {
                match wanted {
                    Some(t) => println!(
                        "(no administrative credentials for team '{t}' visible to this credential)"
                    ),
                    None => println!("(no administrative credentials)"),
                }
            }
            for c in rows {
                let flag = if c["revoked"] == true {
                    " [revoked]"
                } else {
                    ""
                };
                println!(
                    "{}  {:<20} {}…  last used {}  {}{flag}",
                    c["id"].as_str().unwrap_or_default(),
                    c["team"].as_str().unwrap_or("(global)"),
                    c["prefix"].as_str().unwrap_or_default(),
                    rfc3339(&c["last_used_at"]),
                    c["label"].as_str().unwrap_or_default()
                );
            }
        }
        AdminCmd::Credential(AdminCredentialCmd::Revoke { id, .. }) => {
            api.revoke_credential(id).await?;
            println!("administrative credential {id} revoked");
        }
        AdminCmd::Agent(AdminAgentCmd::Add {
            team,
            name,
            display_name,
        }) => {
            let v = api
                .create_agent(&team, &name, display_name.as_deref())
                .await?;
            println!(
                "agent '{}' ready in team '{team}'",
                v["agent"]["name"].as_str().unwrap_or(&name)
            );
        }
        AdminCmd::Agent(AdminAgentCmd::List { team }) => {
            let v = api.list_agents(&team).await?;
            for a in v["agents"].as_array().into_iter().flatten() {
                let flag = if a["disabled"] == true {
                    " [disabled]"
                } else {
                    ""
                };
                println!(
                    "{:<24} {:<28} {} active token(s){flag}",
                    a["name"].as_str().unwrap_or_default(),
                    a["display_name"].as_str().unwrap_or_default(),
                    a["active_tokens"]
                );
            }
        }
        AdminCmd::Token(AdminTokenCmd::Issue {
            team,
            agent,
            label,
            save,
            repo,
        }) => {
            let target = match (save, repo) {
                (true, Some(repo)) => Some(admin_cli::SaveTarget {
                    dir: dir.clone(),
                    repo: admin_cli::validate_repo_name(&repo)?,
                }),
                _ => None,
            };
            let issued = api.issue_token(&team, &agent, label.as_deref()).await?;
            let saved =
                admin_cli::finish_issue(&api, &issued, &agent, &team, target.as_ref()).await?;
            match (saved, target) {
                (Some(path), Some(t)) => println!(
                    "token for {}@{} verified and saved to {} as {}=…  (id {})",
                    issued.agent,
                    issued.team,
                    path.display(),
                    t.repo,
                    issued.id
                ),
                _ => {
                    println!();
                    println!(
                        "Token for {}@{} — verified, shown once, store it now (id {}):",
                        issued.agent, issued.team, issued.id
                    );
                    println!();
                    println!("  {}", issued.token);
                    println!();
                }
            }
        }
        AdminCmd::Token(AdminTokenCmd::List { team }) => {
            let v = api.list_tokens(&team).await?;
            for t in v["tokens"].as_array().into_iter().flatten() {
                let flag = if t["revoked"] == true {
                    " [revoked]"
                } else {
                    ""
                };
                println!(
                    "{}  {:<20} {}…  last used {}  {}{flag}",
                    t["id"].as_str().unwrap_or_default(),
                    t["agent"].as_str().unwrap_or_default(),
                    t["prefix"].as_str().unwrap_or_default(),
                    rfc3339(&t["last_used_at"]),
                    t["label"].as_str().unwrap_or_default()
                );
            }
        }
        AdminCmd::Token(AdminTokenCmd::Revoke { team, id }) => {
            api.revoke_token(&team, id).await?;
            println!("token {id} revoked");
        }
    }
    Ok(())
}

/// The `context` commands: local resolution, no database, no bus except
/// `verify`.
async fn run_context(cmd: ContextCmd) -> anyhow::Result<()> {
    match cmd {
        ContextCmd::Show { select, json } => {
            let resolved = context::resolve(&select.inputs()?)?;
            let view = resolved.redacted();
            if json {
                println!("{}", serde_json::to_string_pretty(&view)?);
                return Ok(());
            }
            let s = |k: &str| view[k].as_str().unwrap_or("-").to_owned();
            println!("endpoint        {}", s("mcp_url"));
            println!(
                "credentials     {} ({})",
                s("token_prefix"),
                view["source"].as_str().unwrap_or("?")
            );
            println!("profile         {}", s("profile"));
            match (
                view["expected_agent"].as_str(),
                view["expected_team"].as_str(),
            ) {
                (Some(a), Some(t)) => println!("expected        {a}@{t}"),
                _ => println!("expected        (explicit credentials: not checked)"),
            }
            if let Some(f) = view["tokens_file"].as_str() {
                println!("token entry     {} in {f}", s("token_key"));
            }
            println!("project         {}", s("project"));
            println!("channel         {}", s("channel"));
            println!("project root    {}", s("project_root"));
            println!("session         {}", s("session"));
            println!();
            println!("Run `ai-crew-sync context verify` to confirm the identity with the bus.");
        }
        ContextCmd::Verify { select } => {
            let resolved = context::resolve(&select.inputs()?)?;
            let v = context::verify(&resolved).await?;
            println!(
                "ok: {}@{} at {} ({}{})",
                v.agent,
                v.team,
                resolved.mcp_url,
                match resolved.source {
                    context::Source::Explicit => "explicit credentials".to_owned(),
                    context::Source::ProfileFlag => "profile from --profile".to_owned(),
                    context::Source::ProjectDefault => "project default".to_owned(),
                    context::Source::UserDefault => "user default profile".to_owned(),
                },
                resolved
                    .profile
                    .as_deref()
                    .map(|p| format!(": '{p}'"))
                    .unwrap_or_default()
            );
        }
        ContextCmd::SetProject {
            profile,
            project,
            channel,
            key,
            dir,
        } => {
            let dir = match dir {
                Some(d) => d,
                None => std::env::current_dir()?,
            };
            let dir = dir
                .canonicalize()
                .with_context(|| format!("resolving {}", dir.display()))?;
            let cfg_dir = context::config_dir()?;
            let profiles = context::load_profiles(&cfg_dir)?;
            let profile = context::validate_name("profile name", &profile)?;
            if !profiles.profiles.contains_key(&profile) {
                anyhow::bail!(
                    "profile '{profile}' does not exist locally; add it first with \
                     `ai-crew-sync context profile add --name {profile} …`. A project may \
                     only reference approved profiles"
                );
            }
            let project = match project {
                Some(p) => Some(context::validate_name("project name", &p)?),
                None => dir
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| context::validate_name("project name", n))
                    .transpose()?,
            };
            let cfg = context::ProjectConfig {
                profile: Some(profile),
                project,
                channel: channel
                    .map(|c| c.trim().to_lowercase())
                    .filter(|c| !c.is_empty()),
                key: key
                    .map(|k| context::validate_name("token key", &k))
                    .transpose()?,
            };
            let path =
                context::with_config_lock(&cfg_dir, || context::write_project_file(&dir, &cfg))?;
            println!(
                "project defaults written to {} (profile '{}', project '{}'). Commit it: it holds no secret.",
                path.display(),
                cfg.profile.as_deref().unwrap_or_default(),
                cfg.project.as_deref().unwrap_or("-")
            );
        }
        ContextCmd::Profile(cmd) => {
            let cfg_dir = context::config_dir()?;
            match cmd {
                ContextProfileCmd::Add {
                    name,
                    url,
                    team,
                    agent,
                    tokens,
                    key,
                    default,
                } => {
                    let name = context::validate_name("profile name", &name)?;
                    let url = admin_cli::normalize_base_url(&url)?;
                    let team = context::validate_name("team", &team)?;
                    let agent = context::validate_name("agent", &agent)?;
                    // Validated before it is written, not only when it is
                    // read back: a stored profile that `load_profiles` will
                    // reject is a trap for the next command.
                    let tokens = context::validate_tokens_ref(
                        &tokens.unwrap_or_else(|| format!("tokens-{team}")),
                    )?;
                    let key = key
                        .map(|k| context::validate_name("token key", &k))
                        .transpose()?;
                    let path = context::update_profiles(&cfg_dir, |p| {
                        p.profiles.insert(
                            name.clone(),
                            context::Profile {
                                url: url.clone(),
                                team: team.clone(),
                                agent: agent.clone(),
                                tokens: tokens.clone(),
                                key: key.clone(),
                            },
                        );
                        if default || p.default.is_none() {
                            p.default = Some(name.clone());
                        }
                        Ok(())
                    })?;
                    println!(
                        "profile '{name}' saved in {} (expects {agent}@{team} at {url}, tokens in {tokens})",
                        path.display()
                    );
                    let tokens_path = cfg_dir.join(&tokens);
                    if !tokens_path.exists() {
                        println!(
                            "note: {} does not exist yet; issue a token with `ai-crew-sync admin token issue --team {team} --agent {agent} --save --repo <name>`",
                            tokens_path.display()
                        );
                    }
                }
                ContextProfileCmd::List => {
                    let p = context::load_profiles(&cfg_dir)?;
                    if p.profiles.is_empty() {
                        println!("(no profiles — add one with `ai-crew-sync context profile add`)");
                    }
                    for (name, prof) in &p.profiles {
                        let mark = if p.default.as_deref() == Some(name) {
                            "*"
                        } else {
                            " "
                        };
                        println!(
                            "{mark} {name:<20} {}@{:<20} {}  tokens {}{}",
                            prof.agent,
                            prof.team,
                            prof.url,
                            prof.tokens,
                            prof.key
                                .as_deref()
                                .map(|k| format!(" (key {k})"))
                                .unwrap_or_default()
                        );
                    }
                }
                ContextProfileCmd::Default { name, clear } => {
                    let path = context::update_profiles(&cfg_dir, |p| {
                        if clear {
                            p.default = None;
                            return Ok(());
                        }
                        let Some(name) = name.clone() else {
                            anyhow::bail!("give a profile name, or --clear");
                        };
                        if !p.profiles.contains_key(&name) {
                            anyhow::bail!("no profile named '{name}'");
                        }
                        p.default = Some(name);
                        Ok(())
                    })?;
                    println!("default updated in {}", path.display());
                }
                ContextProfileCmd::Remove { name } => {
                    context::update_profiles(&cfg_dir, |p| {
                        if p.profiles.remove(&name).is_none() {
                            anyhow::bail!("no profile named '{name}'");
                        }
                        if p.default.as_deref() == Some(name.as_str()) {
                            p.default = None;
                        }
                        Ok(())
                    })?;
                    println!("profile '{name}' removed (its token file is untouched)");
                }
            }
        }
    }
    Ok(())
}
