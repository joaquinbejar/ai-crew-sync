//! Console client: everything the MCP tools can do, from a human terminal.
//!
//! Connects to the bus over the same Streamable HTTP transport and the same
//! bearer token a coding agent would use, so a human on the shell is
//! just another agent on the bus:
//!
//! ```text
//! export BUS_URL=https://bus.internal.example/mcp
//! export BUS_TOKEN=acs_...
//! ai-crew-sync client whoami
//! ai-crew-sync client send --channel deploys --body "staging is on 1.4.2"
//! ai-crew-sync client read --scope inbox
//! ai-crew-sync client task claim --key refactor-auth
//! ```

use anyhow::{Context, bail};
use clap::{Args, Subcommand};
use rmcp::{
    ServiceExt,
    model::{CallToolRequestParams, ClientConfig},
    transport::{
        StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use serde_json::{Value, json};

#[derive(Args)]
pub struct ClientArgs {
    /// URL of the bus MCP endpoint, e.g. https://bus.example.com/mcp. Omit
    /// it to take the endpoint from the selected profile.
    #[arg(long, env = "BUS_URL")]
    pub url: Option<String>,

    /// Your agent token (issued with `ai-crew-sync token issue`). Omit it to
    /// use a local profile: `--profile`, the project's .acs.toml, or the
    /// user default (see `ai-crew-sync context show`).
    #[arg(long, env = "BUS_TOKEN", hide_env_values = true)]
    pub token: Option<String>,

    /// Connect with this local profile (from `context profile add`).
    /// Passing it together with --token is a contradiction and is refused,
    /// so it is always clear which identity a window uses.
    #[arg(long, env = "BUS_PROFILE")]
    pub profile: Option<String>,

    /// Directory whose project defaults (.acs.toml) apply; the current
    /// directory by default.
    #[arg(long, env = "BUS_PROJECT_DIR")]
    pub project_dir: Option<std::path::PathBuf>,

    /// Id of the host conversation this call belongs to. It derives the same
    /// bus session the proxy of that conversation uses, so a lifecycle hook
    /// reads and writes that window's context and nobody else's.
    #[arg(long, env = "BUS_HOST_SESSION")]
    pub host_session: Option<String>,

    /// Which working context this is — usually the repository name. Separates
    /// your presence, task claims and locks from your other sessions. Omit it
    /// to share one context with them.
    #[arg(long, env = "BUS_SESSION")]
    pub session: Option<String>,

    /// Print raw JSON instead of the human-readable rendering.
    #[arg(long, global = true)]
    pub json: bool,

    #[command(subcommand)]
    pub command: ClientCmd,
}

#[derive(Subcommand)]
pub enum ClientCmd {
    /// Who am I on the bus, and is anything waiting for me?
    Whoami,
    /// List the tools the server exposes (sanity check).
    Tools,
    /// Send a message: --channel to broadcast, --to for a direct message.
    Send {
        #[arg(long, conflicts_with = "to")]
        channel: Option<String>,
        #[arg(long)]
        to: Option<String>,
        #[arg(long)]
        body: String,
        /// Interrupt every session in the team, not just those watching this
        /// channel. For deploys, migrations and breaking changes.
        #[arg(long)]
        announce: bool,
        #[arg(long)]
        reply_to: Option<i64>,
        /// Attach a file (repeatable). Max 8 files, 256 KiB each.
        #[arg(long)]
        file: Vec<std::path::PathBuf>,
    },
    /// Attach a file to a task.
    Attach {
        /// Task key.
        task: String,
        #[arg(long)]
        file: std::path::PathBuf,
        #[arg(long)]
        content_type: Option<String>,
    },
    /// Download an attachment by id.
    Download {
        id: i64,
        /// Output path; defaults to the attachment's filename.
        #[arg(long)]
        out: Option<std::path::PathBuf>,
    },
    /// Ask a teammate's agent and wait for the answer.
    Ask {
        to: String,
        /// The question. Omit when resuming with --resume-id.
        question: Option<String>,
        #[arg(long)]
        timeout_seconds: Option<i64>,
        /// Keep waiting on an earlier question (its question_message_id).
        #[arg(long)]
        resume_id: Option<i64>,
    },
    /// Read messages ("all", "inbox", or a channel name).
    Read {
        #[arg(long, default_value = "all")]
        scope: String,
        /// Re-read history instead of only unread messages.
        #[arg(long)]
        history: bool,
        #[arg(long, default_value_t = 50)]
        limit: i64,
        /// Include direct messages addressed to your other sessions.
        #[arg(long)]
        all_sessions: bool,
    },
    /// Full-text search messages.
    Search {
        query: String,
        #[arg(long, default_value_t = 50)]
        limit: i64,
    },
    /// List channels.
    Channels,
    /// Create a channel.
    ChannelCreate {
        name: String,
        #[arg(long)]
        topic: Option<String>,
    },
    /// Who is on the bus and what are they doing?
    Agents {
        #[arg(long)]
        online: bool,
    },
    /// Every session in the team with its exact address, project and role.
    Sessions {
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        role: Option<String>,
        #[arg(long)]
        online: bool,
        #[arg(long)]
        limit: Option<i64>,
    },
    /// Publish your own presence.
    Beat {
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        repo: Option<String>,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long)]
        activity: Option<String>,
        /// Discovery labels (see `sessions`). Pass "" to clear.
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        role: Option<String>,
        #[arg(long)]
        ttl_seconds: Option<i64>,
    },
    /// List tasks.
    Tasks {
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        mine: bool,
        /// Restrict the inventory to task keys beginning with this prefix.
        #[arg(long)]
        project_prefix: Option<String>,
        /// Maximum number of tasks returned per page.
        #[arg(long)]
        limit: Option<i64>,
        /// Continue from a cursor returned by an earlier page.
        #[arg(long)]
        cursor: Option<String>,
        /// Search task titles and descriptions.
        #[arg(long)]
        search: Option<String>,
        /// Search mode: keywords, contains, or regex.
        #[arg(long, value_parser = ["keywords", "contains", "regex"])]
        search_mode: Option<String>,
        /// Search fields: title, description, or both.
        #[arg(long, value_parser = ["title", "description", "both"])]
        search_fields: Option<String>,
        /// Full-text search language: simple, english, or russian.
        #[arg(long, value_parser = ["simple", "english", "russian"])]
        search_language: Option<String>,
        /// Fetch every page until the server reports exhaustion.
        #[arg(long)]
        all_pages: bool,
    },
    /// Operate on a single task.
    #[command(subcommand)]
    Task(TaskCmd),
    /// List notes.
    Notes {
        #[arg(long)]
        scope: Option<String>,
        #[arg(long)]
        tag: Option<String>,
    },
    /// Operate on a single note.
    #[command(subcommand)]
    Note(NoteCmd),
    /// Block until something happens on the bus (or the timeout passes).
    Wait {
        #[arg(long)]
        timeout_seconds: Option<i64>,
        /// Restrict to kinds: message, task, lock, note.
        #[arg(long, value_delimiter = ',')]
        kinds: Vec<String>,
        /// Wake on every channel, not only the one this session works in.
        #[arg(long)]
        all_channels: bool,
    },
    /// Advisory locks on shared resources.
    #[command(subcommand)]
    Lock(LockCmd),
    /// Summary of the team's recent activity.
    Digest {
        #[arg(long, default_value_t = 24)]
        hours: i64,
        /// Cover every channel, not only the one this session works in.
        #[arg(long)]
        all_channels: bool,
    },
    /// Escape hatch: call any tool with raw JSON arguments.
    Call {
        tool: String,
        /// JSON object with the arguments, e.g. '{"key": "x"}'.
        #[arg(long, default_value = "{}")]
        args: String,
    },
}

#[derive(Subcommand)]
pub enum LockCmd {
    Acquire {
        name: String,
        #[arg(long)]
        ttl_seconds: Option<i64>,
        #[arg(long)]
        purpose: Option<String>,
    },
    Release {
        name: String,
    },
    List,
}

#[derive(Subcommand)]
pub enum TaskCmd {
    Create {
        key: String,
        #[arg(long)]
        title: String,
        #[arg(long)]
        description: Option<String>,
        /// Comma-separated keys of tasks this one depends on.
        #[arg(long, value_delimiter = ',')]
        depends_on: Vec<String>,
    },
    Show {
        key: String,
    },
    Claim {
        key: String,
        #[arg(long)]
        lease_seconds: Option<i64>,
    },
    /// Claim the oldest available task, whatever it is.
    Next {
        #[arg(long)]
        lease_seconds: Option<i64>,
    },
    Renew {
        key: String,
        #[arg(long)]
        lease_seconds: Option<i64>,
    },
    Release {
        key: String,
    },
    Done {
        key: String,
        #[arg(long)]
        result: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum NoteCmd {
    Get {
        key: String,
        #[arg(long)]
        scope: Option<String>,
    },
    Set {
        key: String,
        #[arg(long)]
        value: String,
        #[arg(long)]
        scope: Option<String>,
        #[arg(long, value_delimiter = ',')]
        tags: Vec<String>,
    },
    Rm {
        key: String,
        #[arg(long)]
        scope: Option<String>,
    },
    Search {
        query: String,
        #[arg(long)]
        scope: Option<String>,
    },
}

/// Strip nulls so optional flags the user did not pass are simply absent.
pub mod mapping;
pub mod render;

use mapping::{Defaults, to_call_with};
use render::render;

pub async fn run(args: ClientArgs) -> anyhow::Result<()> {
    // Where to connect and as whom: explicit flags first, then the local
    // profiles and the project's defaults. `context` owns the order.
    let resolved = crate::context::resolve(&crate::context::Inputs {
        config_dir: crate::context::config_dir()?,
        explicit_url: args.url.clone(),
        url_origin: args
            .url
            .as_deref()
            .map(|v| crate::context::Origin::of("BUS_URL", v)),
        explicit_token: args.token.clone(),
        token_origin: args
            .token
            .as_deref()
            .map(|v| crate::context::Origin::of("BUS_TOKEN", v)),
        explicit_session: args.session.clone(),
        profile: args.profile.clone(),
        project_dir: args.project_dir.clone(),
        host_session: args.host_session.clone(),
    })?;
    // Shadow warnings go to stderr: stdout is the command's parseable answer.
    for w in &resolved.warnings {
        eprintln!("warning: {w}");
    }
    let mut config = StreamableHttpClientTransportConfig::with_uri(resolved.mcp_url.clone());
    config.auth_header = Some(resolved.token.clone());
    config.allow_stateless = true;
    if let Some(session) = resolved
        .session
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let value = session
            .parse()
            .with_context(|| format!("--session '{session}' is not a valid HTTP header value"))?;
        config
            .custom_headers
            .insert(crate::auth::SESSION_HEADER.parse()?, value);
    }
    let transport = StreamableHttpClientTransport::from_config(config);

    let client = ClientConfig::default()
        .serve(transport)
        .await
        .context("could not connect to the bus (check --url and --token)")?;

    let defaults = Defaults {
        channel: resolved.channel.clone(),
    };
    let outcome = run_command(&client, &args, &defaults).await;
    let _ = client.cancel().await;
    outcome
}

async fn run_command(
    client: &rmcp::service::RunningService<rmcp::RoleClient, ClientConfig>,
    args: &ClientArgs,
    defaults: &Defaults,
) -> anyhow::Result<()> {
    // `tools` is the one command that is not a tool call.
    if matches!(args.command, ClientCmd::Tools) {
        let tools = client.list_all_tools().await?;
        if args.json {
            println!("{}", serde_json::to_string_pretty(&tools)?);
        } else {
            for tool in tools {
                println!(
                    "{:<20} {}",
                    tool.name,
                    tool.description.as_deref().unwrap_or_default().trim()
                );
            }
        }
        return Ok(());
    }

    let (tool, call_args) = match &args.command {
        ClientCmd::Call { tool, args: raw } => {
            let parsed: Value = serde_json::from_str(raw)
                .with_context(|| format!("--args is not valid JSON: {raw}"))?;
            if !parsed.is_object() {
                bail!("--args must be a JSON object");
            }
            (tool.clone(), parsed)
        }
        other => {
            // `tools` is handled above and is the only command that maps to
            // nothing; anything else reaching here without a mapping is a
            // missing match arm, and says so instead of panicking.
            let Some((tool, call_args)) = to_call_with(other, defaults)? else {
                bail!("this subcommand has no MCP tool mapping yet");
            };
            (tool.to_string(), call_args)
        }
    };

    let arguments: serde_json::Map<String, Value> =
        serde_json::from_value(call_args).context("arguments did not form a JSON object")?;
    let mut value = if matches!(
        &args.command,
        ClientCmd::Tasks {
            all_pages: true,
            ..
        }
    ) {
        fetch_all_task_pages(client, &tool, arguments).await?
    } else {
        call_tool_value(client, &tool, arguments).await?
    };

    if let ClientCmd::Tasks {
        all_pages: false, ..
    } = &args.command
    {
        let (incomplete, exhausted) = task_page_state(&value);
        add_task_pagination_metadata(&mut value, incomplete, exhausted);
    }

    if args.json {
        println!("{}", serde_json::to_string_pretty(&value)?);
    } else {
        render(&args.command, &value)?;
    }
    Ok(())
}

async fn call_tool_value(
    client: &rmcp::service::RunningService<rmcp::RoleClient, ClientConfig>,
    tool: &str,
    arguments: serde_json::Map<String, Value>,
) -> anyhow::Result<Value> {
    let result = client
        .call_tool(CallToolRequestParams::new(tool.to_owned()).with_arguments(arguments))
        .await
        .map_err(|e| anyhow::anyhow!("{tool} failed: {e}"))?;

    if result.is_error == Some(true) {
        bail!("{tool} returned an error: {:?}", result.content);
    }
    Ok(result
        .structured_content
        .clone()
        .unwrap_or_else(|| json!({ "ok": true })))
}

fn task_page_state(value: &Value) -> (bool, bool) {
    match value.get("has_more").and_then(Value::as_bool) {
        Some(has_more) => (has_more, !has_more),
        None => (true, false),
    }
}

fn add_task_pagination_metadata(value: &mut Value, incomplete: bool, exhausted: bool) {
    if let Value::Object(object) = value {
        object.insert("incomplete".to_owned(), json!(incomplete));
        object.insert("exhausted".to_owned(), json!(exhausted));
        object.insert(
            "pagination".to_owned(),
            json!({ "incomplete": incomplete, "exhausted": exhausted }),
        );
    }
}

async fn fetch_all_task_pages(
    client: &rmcp::service::RunningService<rmcp::RoleClient, ClientConfig>,
    tool: &str,
    mut arguments: serde_json::Map<String, Value>,
) -> anyhow::Result<Value> {
    let mut combined = Value::Null;
    let mut seen_cursors = std::collections::HashSet::new();
    if let Some(cursor) = arguments.get("cursor").and_then(Value::as_str) {
        seen_cursors.insert(cursor.to_owned());
    }
    let mut incomplete = false;
    let mut exhausted = false;

    loop {
        let page = call_tool_value(client, tool, arguments.clone()).await?;
        let (has_more, page_exhausted) = task_page_state(&page);
        let next_cursor = page
            .get("next_cursor")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .filter(|cursor| !cursor.is_empty());

        if combined.is_null() {
            combined = page.clone();
            if let Some(object) = combined.as_object_mut() {
                object.insert("tasks".to_owned(), json!([]));
            }
        }
        if let (Some(target), Some(tasks)) = (
            combined.as_object_mut(),
            page.get("tasks").and_then(Value::as_array),
        ) {
            if let Some(existing) = target.get_mut("tasks").and_then(Value::as_array_mut) {
                existing.extend(tasks.iter().cloned());
            }
        }

        if page_exhausted {
            exhausted = true;
            break;
        }
        let Some(next_cursor) = next_cursor else {
            incomplete = true;
            break;
        };
        if !seen_cursors.insert(next_cursor.clone()) {
            incomplete = true;
            break;
        }
        arguments.insert("cursor".to_owned(), json!(next_cursor));
        if !has_more {
            exhausted = true;
            break;
        }
    }

    if let Some(object) = combined.as_object_mut() {
        object.insert("next_cursor".to_owned(), Value::Null);
        object.insert("has_more".to_owned(), json!(!exhausted));
    }
    add_task_pagination_metadata(&mut combined, incomplete, exhausted);
    Ok(combined)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_page_state_preserves_exhausted_and_incomplete_semantics() {
        assert_eq!(task_page_state(&json!({"has_more": false})), (false, true));
        assert_eq!(task_page_state(&json!({"has_more": true})), (true, false));
        assert_eq!(task_page_state(&json!({"tasks": []})), (true, false));
    }

    #[test]
    fn task_pagination_metadata_is_explicit_in_json() {
        let mut value = json!({"tasks": []});
        add_task_pagination_metadata(&mut value, true, false);
        assert_eq!(value["incomplete"], true);
        assert_eq!(value["exhausted"], false);
        assert_eq!(value["pagination"]["incomplete"], true);
        assert_eq!(value["pagination"]["exhausted"], false);

        add_task_pagination_metadata(&mut value, false, true);
        assert_eq!(value["incomplete"], false);
        assert_eq!(value["exhausted"], true);
    }
}
