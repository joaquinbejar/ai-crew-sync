//! End-to-end tests against a real Postgres and the real HTTP surface.
//!
//! Two MCP clients ("joaquin" and "marta") connect over Streamable HTTP with
//! their own bearer tokens, exactly as two teammates' coding agents
//! would, and are checked for the properties that actually matter: isolation
//! between teams, identity that cannot be spoofed, and task claims that do not
//! hand the same work to two agents.
//!
//! Requires `TEST_DATABASE_URL` (or `DATABASE_URL`); skipped when unset.

use std::sync::Arc;

use ai_crew_sync::{
    MIGRATOR,
    auth::{generate_token, hash_token, token_prefix},
    serve::{ServeOptions, build_router},
};
use rmcp::{
    ServiceExt,
    model::{CallToolRequestParams, ClientConfig},
    service::RunningService,
    transport::{
        StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

fn db_url() -> Option<String> {
    std::env::var("TEST_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .ok()
}

/// Each test gets its own schema so they can run concurrently without
/// tripping over each other's rows.
struct Harness {
    pool: PgPool,
    base: String,
    ct: CancellationToken,
    /// Every axum task started for this harness, including replicas.
    servers: Vec<tokio::task::JoinHandle<()>>,
    /// The schema this harness owns, so replicas can join it.
    schema: String,
    /// The broker this harness's servers can reach, when the test needs
    /// one. `None` is the default installation: Postgres only.
    nats: Option<String>,
}

impl Harness {
    /// Start another bus instance against the SAME database and schema — the
    /// production topology: N processes, one Postgres, each with its own
    /// LISTEN connection and its own in-process event hub.
    async fn add_replica(&mut self) -> String {
        let (base, handle) =
            spawn_server(self.pool.clone(), self.ct.child_token(), self.nats.clone()).await;
        self.servers.push(handle);
        base
    }

    /// Cancel every background task, wait for the servers to actually stop,
    /// then drop the schema and close the pool — a finished test leaves
    /// neither a running task nor a table behind.
    async fn shutdown(mut self) {
        self.ct.cancel();
        let servers = std::mem::take(&mut self.servers);
        for mut handle in servers {
            // Graceful shutdown is wired to the token; the timeout keeps a
            // wedged task from hanging the suite. Dropping the JoinHandle
            // would DETACH the task rather than stop it — the exact leak this
            // harness exists to prevent — so a timeout aborts it explicitly.
            match tokio::time::timeout(std::time::Duration::from_secs(5), &mut handle).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) if e.is_panic() => panic!("a server task panicked: {e}"),
                Ok(Err(_)) => {}
                Err(_) => {
                    handle.abort();
                    panic!("a server task did not stop within 5s of cancellation");
                }
            }
        }
        // Best effort: a failure here must not fail an otherwise green test,
        // and `setup` drops the schema on the way in regardless.
        let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            self.schema
        )))
        .execute(&self.pool)
        .await;
        self.pool.close().await;
    }
}

impl Drop for Harness {
    /// A panicking test never reaches `shutdown`, and a leaked listener would
    /// keep consuming notifications for the rest of the run.
    fn drop(&mut self) {
        self.ct.cancel();
    }
}

/// Bind an ephemeral port and serve the bus on it. Returns the base URL and
/// the server task, which stops when `ct` is cancelled.
async fn spawn_server(
    pool: PgPool,
    ct: CancellationToken,
    nats: Option<String>,
) -> (String, tokio::task::JoinHandle<()>) {
    spawn_server_with_ping(pool, ct, nats, ai_crew_sync::events::DEFAULT_PING_SECS).await
}

async fn spawn_server_with_ping(
    pool: PgPool,
    ct: CancellationToken,
    nats: Option<String>,
    event_ping_secs: u64,
) -> (String, tokio::task::JoinHandle<()>) {
    let app = build_router(
        pool,
        &ServeOptions {
            bind: String::new(),
            allowed_hosts: vec![],
            allowed_origins: vec![],
            max_request_bytes: ai_crew_sync::serve::DEFAULT_MAX_REQUEST_BYTES,
            // Off by default in tests: the suite hammers the server far faster
            // than any real agent, and the limiter has its own tests.
            rate_limit_per_minute: 0,
            dashboard_secret: b"test-dashboard-secret".to_vec(),
            nats_url: nats,
            nats_credentials: None,
            // The publication tests drive the outbox themselves, one step at
            // a time, so the timing under test is the test's and not a
            // background loop's.
            publication_worker: false,
            event_ping_secs,
        },
        ct.clone(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move { ct.cancelled().await })
            .await;
    });
    (format!("http://{addr}"), handle)
}

async fn setup(schema: &str) -> Option<Harness> {
    setup_with(schema, None).await
}

/// Same, with a broker the server can reach: for the tests that route a
/// team's conversations off Postgres.
async fn setup_with_broker(schema: &str) -> Option<Harness> {
    let nats = nats_url();
    setup_with(schema, Some(nats)).await
}

async fn setup_with(schema: &str, nats: Option<String>) -> Option<Harness> {
    // Silent unless RUST_LOG asks. A failing test that hides the server's
    // own explanation of why it failed wastes the run.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let url = db_url()?;
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .after_connect({
            let schema = schema.to_owned();
            move |conn, _| {
                let schema = schema.clone();
                Box::pin(async move {
                    sqlx::query(sqlx::AssertSqlSafe(format!("SET search_path TO {schema}")))
                        .execute(&mut *conn)
                        .await?;
                    Ok(())
                })
            }
        })
        .connect(&url)
        .await
        .expect("connect");

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP SCHEMA IF EXISTS {schema} CASCADE"
    )))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&pool)
        .await
        .unwrap();
    MIGRATOR.run(&pool).await.expect("migrate");

    let ct = CancellationToken::new();
    let (base, handle) = spawn_server(pool.clone(), ct.child_token(), nats.clone()).await;

    Some(Harness {
        pool,
        base,
        ct,
        servers: vec![handle],
        schema: schema.to_owned(),
        nats,
    })
}

/// Create a team + agent + token straight in the database, the way the CLI does.
async fn seed_agent(pool: &PgPool, team: &str, agent: &str) -> String {
    let team_id: (Uuid,) = sqlx::query_as(
        "INSERT INTO teams (slug, name) VALUES ($1, $1)
         ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name RETURNING id",
    )
    .bind(team)
    .fetch_one(pool)
    .await
    .unwrap();

    let agent_id: (Uuid,) =
        sqlx::query_as("INSERT INTO agents (team_id, name) VALUES ($1, $2) RETURNING id")
            .bind(team_id.0)
            .bind(agent)
            .fetch_one(pool)
            .await
            .unwrap();

    let raw = generate_token();
    sqlx::query("INSERT INTO api_tokens (agent_id, token_hash, prefix) VALUES ($1, $2, $3)")
        .bind(agent_id.0)
        .bind(hash_token(&raw))
        .bind(token_prefix(&raw))
        .execute(pool)
        .await
        .unwrap();
    raw
}

type Client = RunningService<rmcp::RoleClient, ClientConfig>;

async fn connect(base: &str, token: &str) -> Client {
    let mut config = StreamableHttpClientTransportConfig::with_uri(format!("{base}/mcp"));
    config.auth_header = Some(token.to_string());
    config.allow_stateless = true;
    let transport = StreamableHttpClientTransport::from_config(config);
    ClientConfig::default()
        .serve(transport)
        .await
        .expect("mcp handshake")
}

/// Connect as a named working context — the same token, a different session.
async fn connect_with_session(base: &str, token: &str, session: &str) -> Client {
    let mut config = StreamableHttpClientTransportConfig::with_uri(format!("{base}/mcp"));
    config.auth_header = Some(token.to_string());
    config.allow_stateless = true;
    config.custom_headers.insert(
        ai_crew_sync::auth::SESSION_HEADER.parse().unwrap(),
        session.parse().unwrap(),
    );
    let transport = StreamableHttpClientTransport::from_config(config);
    ClientConfig::default()
        .serve(transport)
        .await
        .expect("mcp handshake")
}

/// Call a tool and return its structured output.
async fn call(client: &Client, name: &str, args: Value) -> Value {
    let args: serde_json::Map<String, Value> = serde_json::from_value(args).unwrap();
    let result = client
        .call_tool(CallToolRequestParams::new(name.to_string()).with_arguments(args))
        .await
        .unwrap_or_else(|e| panic!("{name} failed: {e}"));
    assert_ne!(
        result.is_error,
        Some(true),
        "{name} returned an error: {result:?}"
    );
    result
        .structured_content
        .clone()
        .unwrap_or_else(|| panic!("{name} returned no structured content: {result:?}"))
}

/// Call a tool expecting the server to reject it.
async fn call_expect_error(client: &Client, name: &str, args: Value) -> String {
    let args: serde_json::Map<String, Value> = serde_json::from_value(args).unwrap();
    match client
        .call_tool(CallToolRequestParams::new(name.to_string()).with_arguments(args))
        .await
    {
        Err(e) => e.to_string(),
        Ok(result) => {
            assert_eq!(
                result.is_error,
                Some(true),
                "{name} unexpectedly succeeded: {result:?}"
            );
            format!("{:?}", result.content)
        }
    }
}

/// Same harness, but with the in-process rate limiter enabled — the default
/// setup disables it so the suite can hammer the server.
async fn setup_rate_limited(schema: &str, per_minute: u32) -> Option<Harness> {
    let url = match db_url() {
        Some(url) => url,
        None => {
            assert!(
                !db_required(),
                "AI_CREW_SYNC_REQUIRE_DB is set but TEST_DATABASE_URL is not: \
                 this test would have silently passed without a database"
            );
            return None;
        }
    };
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .after_connect({
            let schema = schema.to_owned();
            move |conn, _| {
                let schema = schema.clone();
                Box::pin(async move {
                    sqlx::query(sqlx::AssertSqlSafe(format!("SET search_path TO {schema}")))
                        .execute(&mut *conn)
                        .await?;
                    Ok(())
                })
            }
        })
        .connect(&url)
        .await
        .expect("connect");
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP SCHEMA IF EXISTS {schema} CASCADE"
    )))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&pool)
        .await
        .unwrap();
    MIGRATOR.run(&pool).await.expect("migrate");

    let ct = CancellationToken::new();
    let child = ct.child_token();
    let app = build_router(
        pool.clone(),
        &ServeOptions {
            bind: String::new(),
            allowed_hosts: vec![],
            allowed_origins: vec![],
            max_request_bytes: 64 * 1024,
            rate_limit_per_minute: per_minute,
            dashboard_secret: b"test-dashboard-secret".to_vec(),
            nats_url: None,
            nats_credentials: None,
            publication_worker: false,
            event_ping_secs: ai_crew_sync::events::DEFAULT_PING_SECS,
        },
        child.clone(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move { child.cancelled().await })
            .await;
    });
    Some(Harness {
        pool,
        base: format!("http://{addr}"),
        ct,
        servers: vec![handle],
        schema: schema.to_owned(),
        nats: None,
    })
}

/// Skipping is a convenience for a laptop with no database, and a silent
/// green build everywhere else. `make test` and CI set this, so a broken
/// Postgres setup fails the run instead of passing zero tests.
fn db_required() -> bool {
    std::env::var("AI_CREW_SYNC_REQUIRE_DB").is_ok_and(|v| v != "0")
}

/// Same, for the tests that need the JetStream fixture as well. Missing
/// infrastructure fails visibly: `nats_url()` panics rather than skipping.
macro_rules! require_db_broker {
    ($schema:expr) => {
        match setup_with_broker($schema).await {
            Some(h) => h,
            None => {
                assert!(
                    !db_required(),
                    "AI_CREW_SYNC_REQUIRE_DB is set but TEST_DATABASE_URL is not: \
                     the integration suite would have silently passed without \
                     touching a database"
                );
                eprintln!("skipping: TEST_DATABASE_URL not set");
                return;
            }
        }
    };
}

macro_rules! require_db {
    ($schema:expr) => {
        match setup($schema).await {
            Some(h) => h,
            None => {
                assert!(
                    !db_required(),
                    "AI_CREW_SYNC_REQUIRE_DB is set but TEST_DATABASE_URL is not: \
                     the integration suite would have silently passed without \
                     touching a database"
                );
                eprintln!("skipping: TEST_DATABASE_URL not set");
                return;
            }
        }
    };
}

// ---------------------------------------------------------------------------

#[tokio::test]
async fn unauthenticated_requests_are_rejected() {
    let h = require_db!("t_auth");
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/mcp", h.base))
        .header("Accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "no token must be rejected");

    let resp = client
        .post(format!("{}/mcp", h.base))
        .header("Authorization", "Bearer acs_deadbeef")
        .header("Accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "bogus token must be rejected");

    // Health is deliberately open so load balancers can probe it.
    let resp = client
        .get(format!("{}/health", h.base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn revoked_token_stops_working() {
    let h = require_db!("t_revoke");
    let token = seed_agent(&h.pool, "acme", "joaquin").await;
    let client = connect(&h.base, &token).await;
    call(&client, "whoami", json!({})).await;
    let _ = client.cancel().await;

    sqlx::query("UPDATE api_tokens SET revoked_at = now()")
        .execute(&h.pool)
        .await
        .unwrap();

    let resp = reqwest::Client::new()
        .post(format!("{}/mcp", h.base))
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn tools_are_advertised_with_schemas() {
    let h = require_db!("t_tools");
    let token = seed_agent(&h.pool, "acme", "joaquin").await;
    let client = connect(&h.base, &token).await;

    let tools = client.list_all_tools().await.unwrap();
    let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
    for expected in [
        "whoami",
        "post_message",
        "read_messages",
        "list_channels",
        "create_channel",
        "search_messages",
        "ask_agent",
        "attach_file",
        "get_attachment",
        "create_task",
        "claim_task",
        "claim_next_task",
        "complete_task",
        "release_task",
        "renew_task_lease",
        "list_tasks",
        "get_task",
        "heartbeat",
        "list_agents",
        "list_sessions",
        "register_session",
        "resume_session",
        "renew_session",
        "revoke_session",
        "set_note",
        "get_note",
        "list_notes",
        "search_notes",
        "delete_note",
    ] {
        assert!(
            names.contains(&expected),
            "missing tool {expected} in {names:?}"
        );
    }
    for tool in &tools {
        assert!(
            tool.description.as_ref().is_some_and(|d| d.len() > 20),
            "tool {} needs a usable description",
            tool.name
        );
    }

    // A capability that is off is off in the catalogue too. Advertising
    // eighteen tools that every call rejects is a catalogue that lies, and
    // the model reading it spends a turn finding out.
    let optional = [
        "create_conversation",
        "list_conversations",
        "invite_to_conversation",
        "join_conversation",
        "leave_conversation",
        "remove_conversation_member",
        "archive_conversation",
        "transfer_membership",
        "send_conversation_message",
        "read_conversation",
        "get_conversation_message",
        "ack_message",
        "get_message_receipts",
        "fetch_conversation_inbox",
        "confirm_inbox_delivery",
        "conversation_inbox_status",
        "wait_for_conversation_updates",
        "create_project",
        "list_projects",
        "grant_project_access",
        "recover_conversation_history",
    ];
    for hidden in optional {
        assert!(
            !names.contains(&hidden),
            "conversations are off for this team, so {hidden} must not be advertised"
        );
    }
    let refused = call_expect_error(&client, "create_conversation", json!({"title": "no"})).await;
    assert!(
        refused.contains("not enabled"),
        "the per-call check is still the authorization boundary: {refused}"
    );

    enable_conversations(&h.pool, "acme").await;
    let client2 = connect(&h.base, &token).await;
    let names2: Vec<String> = client2
        .list_all_tools()
        .await
        .unwrap()
        .iter()
        .map(|t| t.name.to_string())
        .collect();
    for shown in optional {
        assert!(
            names2.iter().any(|n| n == shown),
            "with the capability on, {shown} is advertised"
        );
    }
    let _ = client2.cancel().await;
    let _ = client.cancel().await;
}

#[tokio::test]
async fn direct_messages_and_channels_flow_between_two_agents() {
    let h = require_db!("t_msg");
    let joaquin_token = seed_agent(&h.pool, "acme", "joaquin").await;
    let marta_token = seed_agent(&h.pool, "acme", "marta").await;
    let joaquin = connect(&h.base, &joaquin_token).await;
    let marta = connect(&h.base, &marta_token).await;

    // Identity comes from the token, not from an argument.
    let me = call(&joaquin, "whoami", json!({})).await;
    assert_eq!(me["agent"], "joaquin");
    assert_eq!(me["team"], "acme");

    // Direct message: only the recipient sees it in their inbox.
    call(
        &joaquin,
        "post_message",
        json!({"to": "marta", "body": "the auth refactor touches your billing module"}),
    )
    .await;

    let inbox = call(&marta, "read_messages", json!({"scope": "inbox"})).await;
    assert_eq!(inbox["messages"].as_array().unwrap().len(), 1);
    assert_eq!(inbox["messages"][0]["from"], "joaquin");
    assert_eq!(inbox["messages"][0]["to"], "marta");

    // The read cursor advanced, so a second read returns nothing new.
    let again = call(&marta, "read_messages", json!({"scope": "inbox"})).await;
    assert_eq!(again["messages"].as_array().unwrap().len(), 0);

    // ...unless we explicitly ask for history.
    let history = call(
        &marta,
        "read_messages",
        json!({"scope": "inbox", "only_new": false}),
    )
    .await;
    assert_eq!(history["messages"].as_array().unwrap().len(), 1);

    // The sender's own inbox stays empty.
    let joaquin_inbox = call(&joaquin, "read_messages", json!({"scope": "inbox"})).await;
    assert_eq!(joaquin_inbox["messages"].as_array().unwrap().len(), 0);

    // Channels are shared by the whole team.
    call(
        &marta,
        "create_channel",
        json!({"name": "#Deploys", "topic": "what is going out"}),
    )
    .await;
    let channels = call(&joaquin, "list_channels", json!({})).await;
    assert_eq!(
        channels["channels"][0]["name"], "deploys",
        "name normalised"
    );

    call(
        &marta,
        "post_message",
        json!({"channel": "deploys", "body": "staging is on 1.4.2"}),
    )
    .await;
    let read = call(&joaquin, "read_messages", json!({"scope": "deploys"})).await;
    assert_eq!(read["messages"][0]["body"], "staging is on 1.4.2");
    assert_eq!(read["messages"][0]["from"], "marta");

    // Full-text search finds it without disturbing cursors.
    let found = call(&joaquin, "search_messages", json!({"query": "staging"})).await;
    assert_eq!(found["messages"].as_array().unwrap().len(), 1);

    // Sending to an unknown agent is a clean, explanatory error.
    let err = call_expect_error(
        &joaquin,
        "post_message",
        json!({"to": "nobody", "body": "hi"}),
    )
    .await;
    assert!(
        err.contains("nobody"),
        "error should name the missing agent: {err}"
    );

    // A message must have exactly one target.
    let err = call_expect_error(&joaquin, "post_message", json!({"body": "hi"})).await;
    assert!(err.to_lowercase().contains("channel"), "got: {err}");

    let _ = joaquin.cancel().await;
    let _ = marta.cancel().await;
}

#[tokio::test]
async fn teams_are_isolated_from_each_other() {
    let h = require_db!("t_isolation");
    let acme = seed_agent(&h.pool, "acme", "joaquin").await;
    let other = seed_agent(&h.pool, "globex", "intruder").await;
    let acme_client = connect(&h.base, &acme).await;
    let other_client = connect(&h.base, &other).await;

    call(&acme_client, "create_channel", json!({"name": "secrets"})).await;
    call(
        &acme_client,
        "post_message",
        json!({"channel": "secrets", "body": "the api key is in vault"}),
    )
    .await;
    call(
        &acme_client,
        "set_note",
        json!({"scope": "api", "key": "vault-path", "value": "secret/prod/api"}),
    )
    .await;
    call(
        &acme_client,
        "create_task",
        json!({"key": "rotate-keys", "title": "rotate the prod keys"}),
    )
    .await;

    // The other team sees none of it.
    let channels = call(&other_client, "list_channels", json!({})).await;
    assert_eq!(channels["channels"].as_array().unwrap().len(), 0);

    let msgs = call(&other_client, "read_messages", json!({"scope": "all"})).await;
    assert_eq!(msgs["messages"].as_array().unwrap().len(), 0);

    let notes = call(&other_client, "list_notes", json!({})).await;
    assert_eq!(notes["notes"].as_array().unwrap().len(), 0);

    let tasks = call(&other_client, "list_tasks", json!({})).await;
    assert_eq!(tasks["tasks"].as_array().unwrap().len(), 0);

    // Not even by name.
    let found = call(&other_client, "search_messages", json!({"query": "vault"})).await;
    assert_eq!(found["messages"].as_array().unwrap().len(), 0);

    let err = call_expect_error(&other_client, "get_task", json!({"key": "rotate-keys"})).await;
    assert!(err.contains("not found"), "got: {err}");

    let agents = call(&other_client, "list_agents", json!({})).await;
    let names: Vec<&str> = agents["agents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["intruder"]);

    let _ = acme_client.cancel().await;
    let _ = other_client.cancel().await;
}

#[tokio::test]
async fn a_claimed_task_cannot_be_claimed_by_someone_else() {
    let h = require_db!("t_claim");
    let a = seed_agent(&h.pool, "acme", "joaquin").await;
    let b = seed_agent(&h.pool, "acme", "marta").await;
    let joaquin = connect(&h.base, &a).await;
    let marta = connect(&h.base, &b).await;

    call(
        &joaquin,
        "create_task",
        json!({"key": "refactor-auth", "title": "rewrite the token refresh flow"}),
    )
    .await;

    let claim = call(
        &joaquin,
        "claim_task",
        json!({"key": "refactor-auth", "lease_seconds": 600}),
    )
    .await;
    assert_eq!(claim["claimed"], true);
    assert_eq!(claim["task"]["claimed_by"], "joaquin");

    // Marta is refused, and told why rather than getting a bare failure.
    let denied = call(&marta, "claim_task", json!({"key": "refactor-auth"})).await;
    assert_eq!(denied["claimed"], false);
    assert!(
        denied["reason"].as_str().unwrap().contains("joaquin"),
        "reason should name the holder: {denied:?}"
    );

    // Re-claiming your own task is idempotent, not an error.
    let again = call(&joaquin, "claim_task", json!({"key": "refactor-auth"})).await;
    assert_eq!(again["claimed"], true);

    // Marta cannot renew or release a lease she does not hold.
    let err = call_expect_error(&marta, "release_task", json!({"key": "refactor-auth"})).await;
    assert!(err.contains("do not hold"), "got: {err}");
    let err = call_expect_error(&marta, "renew_task_lease", json!({"key": "refactor-auth"})).await;
    assert!(err.contains("do not hold"), "got: {err}");

    // Once released, it is up for grabs again.
    call(&joaquin, "release_task", json!({"key": "refactor-auth"})).await;
    let retry = call(&marta, "claim_task", json!({"key": "refactor-auth"})).await;
    assert_eq!(retry["claimed"], true);
    assert_eq!(retry["task"]["claimed_by"], "marta");

    // Completing records the result and closes the task.
    let done = call(
        &marta,
        "complete_task",
        json!({"key": "refactor-auth", "result": "merged in #421"}),
    )
    .await;
    assert_eq!(done["status"], "done");
    assert_eq!(done["result"], "merged in #421");

    let err = call_expect_error(&joaquin, "complete_task", json!({"key": "refactor-auth"})).await;
    assert!(err.contains("already done"), "got: {err}");

    // The history is a full audit trail.
    let detail = call(&joaquin, "get_task", json!({"key": "refactor-auth"})).await;
    let events: Vec<&str> = detail["history"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["event"].as_str().unwrap())
        .collect();
    assert_eq!(
        events,
        vec![
            "created",
            "claimed",
            "claimed",
            "released",
            "claimed",
            "completed"
        ]
    );

    let _ = joaquin.cancel().await;
    let _ = marta.cancel().await;
}

#[tokio::test]
async fn an_expired_lease_can_be_taken_over() {
    let h = require_db!("t_lease");
    let a = seed_agent(&h.pool, "acme", "joaquin").await;
    let b = seed_agent(&h.pool, "acme", "marta").await;
    let joaquin = connect(&h.base, &a).await;
    let marta = connect(&h.base, &b).await;

    call(
        &joaquin,
        "create_task",
        json!({"key": "long-job", "title": "reindex everything"}),
    )
    .await;
    call(&joaquin, "claim_task", json!({"key": "long-job"})).await;

    // Simulate an agent that died mid-task: the lease lapses.
    sqlx::query("UPDATE tasks SET lease_expires_at = now() - interval '1 minute'")
        .execute(&h.pool)
        .await
        .unwrap();

    let listed = call(&marta, "list_tasks", json!({})).await;
    assert_eq!(listed["tasks"][0]["lease_expired"], true);

    let stolen = call(&marta, "claim_task", json!({"key": "long-job"})).await;
    assert_eq!(
        stolen["claimed"], true,
        "an expired lease must be reclaimable"
    );
    assert_eq!(stolen["task"]["claimed_by"], "marta");

    // Renewing pushes the expiry back out.
    let renewed = call(
        &marta,
        "renew_task_lease",
        json!({"key": "long-job", "lease_seconds": 3600}),
    )
    .await;
    assert_eq!(renewed["lease_expired"], false);

    let _ = joaquin.cancel().await;
    let _ = marta.cancel().await;
}

#[tokio::test]
async fn concurrent_claim_next_never_hands_out_the_same_task_twice() {
    let h = require_db!("t_race");
    let mut clients = Vec::new();
    for i in 0..4 {
        let token = seed_agent(&h.pool, "acme", &format!("agent{i}")).await;
        clients.push(Arc::new(connect(&h.base, &token).await));
    }

    // Four tasks, four agents, all grabbing at once.
    for i in 0..4 {
        call(
            &clients[0],
            "create_task",
            json!({"key": format!("job-{i}"), "title": format!("job {i}")}),
        )
        .await;
    }

    let mut handles = Vec::new();
    for client in &clients {
        let client = Arc::clone(client);
        handles.push(tokio::spawn(async move {
            call(&client, "claim_next_task", json!({})).await
        }));
    }
    let mut keys = Vec::new();
    for handle in handles {
        let result = handle.await.unwrap();
        assert_eq!(result["claimed"], true);
        keys.push(result["task"]["key"].as_str().unwrap().to_owned());
    }
    keys.sort();
    keys.dedup();
    assert_eq!(
        keys.len(),
        4,
        "each agent must get a distinct task: {keys:?}"
    );

    // With nothing left, the pool is empty rather than erroring.
    let empty = call(&clients[0], "claim_next_task", json!({})).await;
    assert_eq!(empty["claimed"], false);
    assert!(empty["reason"].as_str().unwrap().contains("no unclaimed"));
}

#[tokio::test]
async fn presence_expires_and_is_visible_to_the_team() {
    let h = require_db!("t_presence");
    let a = seed_agent(&h.pool, "acme", "joaquin").await;
    let b = seed_agent(&h.pool, "acme", "marta").await;
    let joaquin = connect(&h.base, &a).await;
    let marta = connect(&h.base, &b).await;

    call(
        &joaquin,
        "heartbeat",
        json!({"repo": "acme/api", "branch": "feat/auth", "activity": "rewriting token refresh"}),
    )
    .await;

    let seen = call(&marta, "list_agents", json!({"online_only": true})).await;
    assert_eq!(seen["online_count"], 1);
    assert_eq!(seen["agents"][0]["name"], "joaquin");
    assert_eq!(seen["agents"][0]["activity"], "rewriting token refresh");
    assert_eq!(seen["agents"][0]["repo"], "acme/api");

    // A later heartbeat that omits a field keeps the previous value.
    call(&joaquin, "heartbeat", json!({"status": "blocked"})).await;
    let seen = call(&marta, "list_agents", json!({"online_only": true})).await;
    assert_eq!(seen["agents"][0]["status"], "blocked");
    assert_eq!(seen["agents"][0]["repo"], "acme/api", "repo should persist");

    // When the lease lapses the agent reads as offline, not as stale-but-active.
    sqlx::query("UPDATE agent_presence SET expires_at = now() - interval '1 minute'")
        .execute(&h.pool)
        .await
        .unwrap();
    let seen = call(&marta, "list_agents", json!({})).await;
    assert_eq!(seen["online_count"], 0);
    let joaquin_row = seen["agents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["name"] == "joaquin")
        .unwrap();
    assert_eq!(joaquin_row["status"], "offline");

    let err = call_expect_error(&joaquin, "heartbeat", json!({"status": "vibing"})).await;
    assert!(err.contains("active"), "got: {err}");

    let _ = joaquin.cancel().await;
    let _ = marta.cancel().await;
}

#[tokio::test]
async fn notes_are_shared_memory_with_history() {
    let h = require_db!("t_notes");
    let a = seed_agent(&h.pool, "acme", "joaquin").await;
    let b = seed_agent(&h.pool, "acme", "marta").await;
    let joaquin = connect(&h.base, &a).await;
    let marta = connect(&h.base, &b).await;

    call(
        &joaquin,
        "set_note",
        json!({
            "scope": "api",
            "key": "why-no-redis",
            "value": "we dropped redis in march; the cache lives in postgres now",
            "tags": ["Infra", "decision"]
        }),
    )
    .await;

    // Marta reads what Joaquin wrote, tags normalised.
    let note = call(
        &marta,
        "get_note",
        json!({"scope": "api", "key": "why-no-redis"}),
    )
    .await;
    assert_eq!(note["found"], true);
    assert_eq!(note["note"]["updated_by"], "joaquin");
    assert_eq!(note["note"]["tags"][0], "infra");

    // Missing notes report found=false instead of erroring.
    let missing = call(&marta, "get_note", json!({"key": "nope"})).await;
    assert_eq!(missing["found"], false);
    assert!(missing["note"].is_null());

    // Overwrites keep a revision trail.
    call(
        &marta,
        "set_note",
        json!({"scope": "api", "key": "why-no-redis", "value": "correction: valkey, not redis"}),
    )
    .await;
    let note = call(
        &joaquin,
        "get_note",
        json!({"scope": "api", "key": "why-no-redis"}),
    )
    .await;
    assert_eq!(note["note"]["updated_by"], "marta");
    let (revisions,): (i64,) = sqlx::query_as("SELECT count(*) FROM note_revisions")
        .fetch_one(&h.pool)
        .await
        .unwrap();
    assert_eq!(revisions, 2, "both versions retained");

    // Scope and tag filtering.
    call(
        &joaquin,
        "set_note",
        json!({"scope": "web", "key": "build", "value": "vite, not webpack", "tags": ["infra"]}),
    )
    .await;
    let api_only = call(&marta, "list_notes", json!({"scope": "api"})).await;
    assert_eq!(api_only["notes"].as_array().unwrap().len(), 1);
    let all = call(&marta, "list_notes", json!({})).await;
    assert_eq!(all["notes"].as_array().unwrap().len(), 2);
    let tagged = call(&marta, "list_notes", json!({"tag": "infra"})).await;
    assert_eq!(tagged["notes"].as_array().unwrap().len(), 1);

    let found = call(&marta, "search_notes", json!({"query": "valkey"})).await;
    assert_eq!(found["notes"].as_array().unwrap().len(), 1);

    let del = call(
        &marta,
        "delete_note",
        json!({"scope": "api", "key": "why-no-redis"}),
    )
    .await;
    assert_eq!(del["ok"], true);
    let del_again = call(
        &marta,
        "delete_note",
        json!({"scope": "api", "key": "why-no-redis"}),
    )
    .await;
    assert_eq!(del_again["ok"], false, "deleting twice is not an error");

    let _ = joaquin.cancel().await;
    let _ = marta.cancel().await;
}

/// A note's scope and key are names, not documents: they ride in the NOTIFY
/// payload that Postgres caps at 8000 bytes, in every listing and on the
/// dashboard. Unbounded, a 9 KB key failed as an opaque "database error"
/// and a 3 KB one was stored and listed everywhere.
#[tokio::test]
async fn a_note_key_is_a_name_not_a_document() {
    let h = require_db!("t_note_key");
    let token = seed_agent(&h.pool, "acme", "joaquin").await;
    let c = connect(&h.base, &token).await;

    let err = call_expect_error(
        &c,
        "set_note",
        json!({"key": "k".repeat(9000), "value": "x"}),
    )
    .await;
    assert!(err.contains("note key is 9000 bytes"), "{err}");
    assert!(err.contains("256"), "{err}");
    assert!(!err.contains("database error"), "{err}");

    let err = call_expect_error(
        &c,
        "set_note",
        json!({"key": "k".repeat(300), "value": "x"}),
    )
    .await;
    assert!(err.contains("note key is 300 bytes"), "{err}");

    let err = call_expect_error(
        &c,
        "set_note",
        json!({"scope": "s".repeat(65), "key": "fine", "value": "x"}),
    )
    .await;
    assert!(err.contains("note scope is 65 bytes"), "{err}");
    assert!(err.contains("64"), "{err}");

    // The limit is the limit: a 256-byte key is a (long) name.
    let ok = call(
        &c,
        "set_note",
        json!({"scope": "s".repeat(64), "key": "k".repeat(256), "value": "x"}),
    )
    .await;
    assert_eq!(ok["key"].as_str().map(str::len), Some(256), "{ok}");

    let _ = c.cancel().await;
    h.shutdown().await;
}

#[tokio::test]
async fn whoami_reports_pending_work() {
    let h = require_db!("t_whoami");
    let a = seed_agent(&h.pool, "acme", "joaquin").await;
    let b = seed_agent(&h.pool, "acme", "marta").await;
    let joaquin = connect(&h.base, &a).await;
    let marta = connect(&h.base, &b).await;

    call(
        &marta,
        "post_message",
        json!({"to": "joaquin", "body": "ping"}),
    )
    .await;
    call(
        &marta,
        "post_message",
        json!({"to": "joaquin", "body": "ping again"}),
    )
    .await;
    call(
        &joaquin,
        "create_task",
        json!({"key": "t1", "title": "something"}),
    )
    .await;
    call(&joaquin, "claim_task", json!({"key": "t1"})).await;

    let me = call(&joaquin, "whoami", json!({})).await;
    assert_eq!(me["unread_direct_messages"], 2);
    assert_eq!(me["open_claimed_tasks"], 1);

    call(&joaquin, "read_messages", json!({"scope": "inbox"})).await;
    let me = call(&joaquin, "whoami", json!({})).await;
    assert_eq!(me["unread_direct_messages"], 0);

    let _ = joaquin.cancel().await;
    let _ = marta.cancel().await;
}

// ------------------------------------------------------------ v0.2 features --

#[tokio::test]
async fn wait_for_updates_wakes_on_a_teammates_message() {
    let h = require_db!("t_wait");
    let a = seed_agent(&h.pool, "acme", "joaquin").await;
    let b = seed_agent(&h.pool, "acme", "marta").await;
    let joaquin = connect(&h.base, &a).await;
    let marta = connect(&h.base, &b).await;
    call(&joaquin, "create_channel", json!({"name": "dev"})).await;

    // Joaquin blocks waiting; Marta posts shortly after.
    let waiter = {
        let base = h.base.clone();
        let token = a.clone();
        tokio::spawn(async move {
            let client = connect(&base, &token).await;
            let started = std::time::Instant::now();
            let result = call(&client, "wait_for_updates", json!({"timeout_seconds": 20})).await;
            let _ = client.cancel().await;
            (result, started.elapsed())
        })
    };

    tokio::time::sleep(std::time::Duration::from_millis(700)).await;
    call(
        &marta,
        "post_message",
        json!({"channel": "dev", "body": "he subido el fix del parser"}),
    )
    .await;

    let (result, elapsed) = waiter.await.unwrap();
    assert_eq!(result["woke"], true, "must wake on the message: {result:?}");
    assert_eq!(result["timed_out"], false);
    assert!(
        elapsed < std::time::Duration::from_secs(10),
        "woke by event, not by timeout (took {elapsed:?})"
    );
    let summaries = result["events"].as_array().unwrap();
    assert!(
        summaries
            .iter()
            .any(|e| e["summary"].as_str().unwrap().contains("marta")),
        "event should name the sender: {summaries:?}"
    );

    // With unread messages already pending, the wait returns immediately.
    let instant = call(&joaquin, "wait_for_updates", json!({"timeout_seconds": 30})).await;
    assert_eq!(instant["woke"], true);

    let _ = joaquin.cancel().await;
    let _ = marta.cancel().await;
}

#[tokio::test]
async fn ask_agent_returns_the_teammates_answer() {
    let h = require_db!("t_ask");
    let a = seed_agent(&h.pool, "acme", "joaquin").await;
    let b = seed_agent(&h.pool, "acme", "marta").await;
    let joaquin = connect(&h.base, &a).await;
    let marta = connect(&h.base, &b).await;

    // Joaquin asks and blocks; Marta reads the question and replies to it.
    let asker = {
        let base = h.base.clone();
        let token = a.clone();
        tokio::spawn(async move {
            let client = connect(&base, &token).await;
            let started = std::time::Instant::now();
            let result = call(
                &client,
                "ask_agent",
                json!({"to": "marta", "question": "does staging run pg16?", "timeout_seconds": 20}),
            )
            .await;
            let _ = client.cancel().await;
            (result, started.elapsed())
        })
    };

    tokio::time::sleep(std::time::Duration::from_millis(700)).await;
    let inbox = call(&marta, "read_messages", json!({"scope": "inbox"})).await;
    let question = inbox["messages"]
        .as_array()
        .unwrap()
        .last()
        .unwrap()
        .clone();
    assert_eq!(
        question["metadata"]["question"], true,
        "the question DM is marked as such: {question:?}"
    );
    call(
        &marta,
        "post_message",
        json!({"to": "joaquin", "body": "yes, since yesterday", "reply_to": question["id"]}),
    )
    .await;

    let (result, elapsed) = asker.await.unwrap();
    assert_eq!(result["answered"], true, "{result:?}");
    assert_eq!(result["answer"]["from"], "marta");
    assert_eq!(result["answer"]["body"], "yes, since yesterday");
    assert!(
        elapsed < std::time::Duration::from_secs(10),
        "answered by event, not by timeout (took {elapsed:?})"
    );

    // Timeout path: no answer in time, then resume picks up a late answer
    // that was sent without reply_to (lenient matching).
    let timed = call(
        &joaquin,
        "ask_agent",
        json!({"to": "marta", "question": "and prod?", "timeout_seconds": 5}),
    )
    .await;
    assert_eq!(timed["answered"], false, "{timed:?}");
    let qid = timed["question_message_id"].as_i64().unwrap();
    assert!(
        timed["suggestion"]
            .as_str()
            .unwrap()
            .contains(&qid.to_string()),
        "timeout suggestion tells how to resume: {timed:?}"
    );

    call(
        &marta,
        "post_message",
        json!({"to": "joaquin", "body": "prod is still on pg15"}),
    )
    .await;
    let resumed = call(
        &joaquin,
        "ask_agent",
        json!({"to": "marta", "resume_message_id": qid, "timeout_seconds": 5}),
    )
    .await;
    assert_eq!(resumed["answered"], true, "{resumed:?}");
    assert_eq!(resumed["answer"]["body"], "prod is still on pg15");

    // Asking the window you are calling from is refused: nothing would ever
    // read the question, so the call could only ever time out. Asking another
    // of your own sessions is a different thing and is allowed — see
    // one_session_can_ask_another_session_of_the_same_person.
    let err = call_expect_error(
        &joaquin,
        "ask_agent",
        json!({"to": "joaquin", "question": "hi"}),
    )
    .await;
    assert!(err.contains("this session"), "{err}");

    let _ = joaquin.cancel().await;
    let _ = marta.cancel().await;
}

#[tokio::test]
async fn transport_limits_reject_oversized_and_too_frequent_requests() {
    let h = match setup_rate_limited("t_limits", 60).await {
        Some(h) => h,
        None => {
            eprintln!("skipping: TEST_DATABASE_URL not set");
            return;
        }
    };
    let token = seed_agent(&h.pool, "acme", "joaquin").await;
    let http = reqwest::Client::new();
    let mcp = format!("{}/mcp", h.base);

    let call_body = |body: String| {
        let http = http.clone();
        let mcp = mcp.clone();
        let token = token.clone();
        async move {
            http.post(&mcp)
                .header("Authorization", format!("Bearer {token}"))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json, text/event-stream")
                .body(body)
                .send()
                .await
                .unwrap()
        }
    };

    // Over the 64 KiB harness limit → 413 with an actionable body, and the
    // request never reaches the tool layer.
    let huge = "x".repeat(200 * 1024);
    let resp = call_body(format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{{"name":"whoami","arguments":{{"pad":"{huge}"}}}}}}"#
    ))
    .await;
    assert_eq!(resp.status(), 413, "oversized body is rejected");
    let text = resp.text().await.unwrap();
    assert!(
        text.contains("too large") && text.contains("attachments"),
        "413 tells the caller what to do: {text}"
    );
    assert!(
        text.contains("65536"),
        "413 states the limit this server is configured with: {text}"
    );

    // Burst past the bucket → 429 with Retry-After and advice.
    let small = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"whoami","arguments":{}}}"#;
    let mut throttled = None;
    for _ in 0..40 {
        let resp = call_body(small.to_owned()).await;
        if resp.status() == 429 {
            throttled = Some(resp);
            break;
        }
    }
    let resp = throttled.expect("a burst of 40 must exhaust a 60/min bucket");
    let retry_after = resp
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let text = resp.text().await.unwrap();
    assert!(
        retry_after.is_some(),
        "429 carries Retry-After: headers missing"
    );
    assert!(
        text.contains("rate limit") && text.contains("wait_for_updates"),
        "429 points at the non-polling alternative: {text}"
    );

    // A different token has its own budget.
    let other = seed_agent(&h.pool, "acme", "marta").await;
    let resp = http
        .post(&mcp)
        .header("Authorization", format!("Bearer {other}"))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(small)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "another token is unaffected");
}

#[tokio::test]
async fn bounded_fields_reject_oversized_values() {
    let h = require_db!("t_bounds");
    let a = seed_agent(&h.pool, "acme", "joaquin").await;
    let joaquin = connect(&h.base, &a).await;

    let long = "x".repeat(300);
    let err = call_expect_error(
        &joaquin,
        "create_channel",
        json!({"name": "dev", "topic": long.clone()}),
    )
    .await;
    assert!(err.contains("256"), "channel topic bounded: {err}");

    let err = call_expect_error(&joaquin, "heartbeat", json!({"activity": long.clone()})).await;
    assert!(err.contains("256"), "presence activity bounded: {err}");

    let err = call_expect_error(
        &joaquin,
        "set_note",
        json!({"key": "k", "value": "v", "tags": vec!["t"; 20]}),
    )
    .await;
    assert!(err.contains("16"), "tag count bounded: {err}");

    call(&joaquin, "create_task", json!({"key": "dep", "title": "t"})).await;
    let err = call_expect_error(
        &joaquin,
        "create_task",
        json!({"key": "many-deps", "title": "t", "depends_on": vec!["dep"; 40]}),
    )
    .await;
    assert!(err.contains("32"), "dependency count bounded: {err}");

    let _ = joaquin.cancel().await;
}

/// The store layer scopes every query by team and the API tests prove the
/// isolation holds. This one goes underneath both: raw SQL, no helpers, no
/// application code — the database itself must refuse a cross-team reference.
/// The production topology: two bus processes against one database, each with
/// its own LISTEN connection and its own in-process event hub. A wakeup must
/// cross that boundary — an agent long-polling one replica has to hear about a
/// message posted through the other, or `wait_for_updates` is only correct on
/// a single-instance deployment.
#[tokio::test]
async fn a_wait_on_one_replica_wakes_on_the_other_replicas_write() {
    let mut h = require_db!("t_replicas");
    let replica = h.add_replica().await;
    assert_ne!(replica, h.base, "a genuinely separate instance");

    let a = seed_agent(&h.pool, "acme", "joaquin").await;
    let b = seed_agent(&h.pool, "acme", "marta").await;

    // Marta creates the channel through replica two.
    let marta = connect(&replica, &b).await;
    call(&marta, "create_channel", json!({"name": "dev"})).await;

    // Joaquin blocks on replica one.
    let waiter = {
        let base = h.base.clone();
        let token = a.clone();
        tokio::spawn(async move {
            let client = connect(&base, &token).await;
            let started = std::time::Instant::now();
            let result = call(&client, "wait_for_updates", json!({"timeout_seconds": 20})).await;
            let _ = client.cancel().await;
            (result, started.elapsed())
        })
    };

    tokio::time::sleep(std::time::Duration::from_millis(700)).await;
    call(
        &marta,
        "post_message",
        json!({"channel": "dev", "body": "posted through the other replica"}),
    )
    .await;

    let (result, elapsed) = waiter.await.unwrap();
    assert_eq!(
        result["woke"], true,
        "must wake across replicas: {result:?}"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(10),
        "woken by the NOTIFY, not by the timeout (took {elapsed:?})"
    );

    // Reading through either instance returns the same state — no per-process
    // cursor or cache.
    let joaquin = connect(&h.base, &a).await;
    let via_one = call(&joaquin, "read_messages", json!({"scope": "dev"})).await;
    assert_eq!(via_one["messages"].as_array().map(Vec::len), Some(1));

    let _ = joaquin.cancel().await;
    let _ = marta.cancel().await;
    h.shutdown().await;
}

/// The bug this replaces: every replica received the same NOTIFY and every
/// replica POSTed, so a two-replica deployment sent every channel message
/// twice. Enqueueing in a database trigger and claiming with FOR UPDATE SKIP
/// LOCKED makes the count independent of how many processes are running.
#[tokio::test]
async fn webhook_delivery_is_exactly_one_row_per_hook_across_replicas() {
    let mut h = require_db!("t_outbox");
    let _replica = h.add_replica().await;
    let _replica_two = h.add_replica().await;

    let token = seed_agent(&h.pool, "acme", "joaquin").await;
    let client = connect(&h.base, &token).await;
    call(&client, "create_channel", json!({"name": "dev"})).await;

    let team: (Uuid,) = sqlx::query_as("SELECT id FROM teams WHERE slug = 'acme'")
        .fetch_one(&h.pool)
        .await
        .unwrap();
    // A receiver that does not exist: delivery will fail, which is exactly
    // what exercises the retry path. What matters here is the row count.
    sqlx::query(
        "INSERT INTO webhooks (team_id, url, kind, events)
         VALUES ($1, 'http://127.0.0.1:9/hook', 'generic', ARRAY['message','task'])",
    )
    .bind(team.0)
    .execute(&h.pool)
    .await
    .unwrap();

    call(
        &client,
        "post_message",
        json!({"channel": "dev", "body": "one event, three replicas"}),
    )
    .await;

    // Give every replica's dispatcher a chance to have reacted.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    let (rows,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM webhook_deliveries WHERE kind = 'message'")
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert_eq!(
        rows, 1,
        "one channel message must enqueue exactly one delivery, not one per replica"
    );

    // It failed (nothing is listening on port 9) and was rescheduled rather
    // than dropped — the old code logged a warning and forgot the event.
    let (status, attempts, err): (String, i32, Option<String>) = sqlx::query_as(
        "SELECT status, attempts, last_error FROM webhook_deliveries WHERE kind = 'message'",
    )
    .fetch_one(&h.pool)
    .await
    .unwrap();
    assert!(attempts >= 1, "the delivery was attempted");
    assert!(err.is_some(), "the failure was recorded: {err:?}");
    assert!(
        status == "pending" || status == "failed",
        "a failed delivery is retried or parked, never lost (got {status})"
    );

    // A direct message must not enqueue anything at all.
    let _marta = seed_agent(&h.pool, "acme", "marta").await;
    call(
        &client,
        "post_message",
        json!({"to": "marta", "body": "private"}),
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let (total,): (i64,) = sqlx::query_as("SELECT count(*) FROM webhook_deliveries")
        .fetch_one(&h.pool)
        .await
        .unwrap();
    assert_eq!(total, 1, "a DM must never reach the outbox");

    // A task transition enqueues once too, and a lease renewal does not
    // enqueue at all (it is not a state change).
    call(&client, "create_task", json!({"key": "t1", "title": "t"})).await;
    call(&client, "claim_task", json!({"key": "t1"})).await;
    call(
        &client,
        "renew_task_lease",
        json!({"key": "t1", "lease_seconds": 600}),
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let (task_rows,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM webhook_deliveries WHERE kind = 'task'")
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert_eq!(
        task_rows, 2,
        "created + claimed enqueue one each; renewing the lease enqueues none"
    );

    let _ = client.cancel().await;
    h.shutdown().await;
}

/// The digest used to run one tail query per active channel, so a team with
/// 40 channels paid 41 round trips for one digest and the cost grew with the
/// team. This pins the property: the work must not scale with channel count.
///
/// Measured rather than counted, and the measurement is the hard part.
///
/// `pg_stat_statements` would count executions directly, but it needs
/// `shared_preload_libraries`, and a GitHub Actions service container cannot
/// be given a command — so enabling it means running Postgres as a manual
/// step and diverging CI from `make test`. A database-wide counter is noise
/// while the suite runs in parallel, and per-table stats are flushed
/// asynchronously, which trades this test's flakiness for a sleep.
///
/// So: wall clock, with the two things that make wall clock trustworthy.
/// **Minimum** of several runs, because noise only ever adds time — a loaded
/// runner cannot make a query faster than it is. And a **ratio** rather than
/// a constant offset, because a ratio is scale-invariant: a slow machine
/// slows both measurements and the comparison survives.
///
/// The sizes are chosen from measurement, not taste. One MCP round trip costs
/// roughly 9ms here and a database round trip roughly 0.5ms, so at 20 channels
/// an N+1 hides inside the transport: injecting one and running this test at
/// the old sizes **passed**. At 60 channels the extra round trips dominate and
/// the two shapes separate cleanly — measured at 2.0-2.1 for one statement
/// against 4.2-4.3 for a query per channel, which is where the bound below
/// comes from.
#[tokio::test]
async fn the_digest_cost_does_not_grow_with_channel_count() {
    let h = require_db!("t_digest_scale");
    let token = seed_agent(&h.pool, "acme", "joaquin").await;
    let client = connect(&h.base, &token).await;

    // Seeded straight into the database: 60 channels through post_message is
    // hundreds of MCP calls, and the setup is not what is being measured.
    let seed_channels = |from: i32, to: i32| {
        let pool = h.pool.clone();
        async move {
            // Triggers off for the seed. LISTEN/NOTIFY is database-wide, not
            // schema-scoped, so 360 inserts in one transaction flood every
            // other test's event hub at commit and overflow its broadcast
            // buffer — tests that were passing start waking spuriously. The
            // seed is setup, not the behaviour under measurement, so its
            // notifications are noise by definition. SET LOCAL reverts on
            // commit, so no connection goes back to the pool altered.
            let mut tx = pool.begin().await.expect("begin seed");
            sqlx::query("SET LOCAL session_replication_role = replica")
                .execute(&mut *tx)
                .await
                .expect("suppress seed triggers");
            sqlx::query(sqlx::AssertSqlSafe(
                "WITH team AS (SELECT id FROM teams WHERE slug = 'acme'),
                      me AS (SELECT id FROM agents WHERE name = 'joaquin'),
                      ch AS (
                          INSERT INTO channels (team_id, name, created_by)
                          SELECT team.id, 'chan' || g, me.id
                            FROM generate_series($1, $2 - 1) g, team, me
                          RETURNING id, name
                      )
                 INSERT INTO messages (team_id, channel_id, sender_agent_id, body)
                 SELECT team.id, ch.id, me.id, 'message ' || m || ' in ' || ch.name
                   FROM ch, generate_series(0, 5) m, team, me"
                    .to_owned(),
            ))
            .bind(from)
            .bind(to)
            .execute(&mut *tx)
            .await
            .expect("seed channels");
            tx.commit().await.expect("commit seed");
        }
    };

    // The minimum of several runs. Scheduler noise adds time and never
    // subtracts it, so the smallest observation is the closest to the real
    // cost — which is exactly what a shape assertion wants.
    async fn best_of(client: &Client, runs: usize) -> std::time::Duration {
        let mut best = std::time::Duration::MAX;
        for _ in 0..runs {
            let started = std::time::Instant::now();
            call(client, "team_digest", json!({"hours": 24})).await;
            best = best.min(started.elapsed());
        }
        best
    }

    seed_channels(0, 2).await;
    // Warm the connection and the plan cache before the first measurement.
    call(&client, "team_digest", json!({"hours": 24})).await;

    let small = best_of(&client, 5).await;
    let few = call(&client, "team_digest", json!({"hours": 24})).await;
    assert_eq!(few["channels"].as_array().map(Vec::len), Some(2));

    seed_channels(2, 60).await;
    let large = best_of(&client, 5).await;

    let digest = call(&client, "team_digest", json!({"hours": 24})).await;
    let channels = digest["channels"].as_array().expect("channels");
    assert_eq!(channels.len(), 60, "every channel is reported");
    for c in channels {
        let tail = c["last_messages"].as_array().expect("tail");
        assert!(!tail.is_empty() && tail.len() <= 5, "tail is 1..=5: {c:?}");
        assert_eq!(c["message_count"], 6, "counts survive the rewrite: {c:?}");
        // The tail must be chronological, oldest first — the window function
        // orders by id, and reversing it silently would be easy to miss.
        let first = tail[0]["body"].as_str().unwrap_or("");
        assert!(
            first.contains("message 1"),
            "oldest of the tail first: {tail:?}"
        );
    }

    // Thirty times the channels must not cost thirty times the digest. The
    // bound comes from measuring both shapes rather than taste: one statement
    // runs at 2.0-2.1 here — real growth, 360 rows against 12 — and a query
    // per channel at 4.2-4.3. Three sits between them with room on both
    // sides.
    // The bound has an absolute term as well as a ratio. Two channels can
    // measure well under a millisecond, and the ratio of two sub-millisecond
    // durations on a shared CI runner is mostly scheduler noise — which is
    // how this assertion failed on runs where nothing was wrong. A query per
    // channel costs tens of milliseconds at sixty channels, so the slack
    // does not hide the shape it is here to catch.
    let allowed = small.mul_f64(3.0) + std::time::Duration::from_millis(5);
    assert!(
        large < allowed,
        "digest over 60 channels took {large:?} against {small:?} over 2 \
         (allowed {allowed:?}): the cost is scaling with channel count"
    );

    let _ = client.cancel().await;
    h.shutdown().await;
}

#[tokio::test]
async fn attachment_quotas_are_enforced_per_team() {
    use base64::Engine;
    let b64 = |data: &[u8]| base64::engine::general_purpose::STANDARD.encode(data);

    let h = require_db!("t_quota");
    let a = seed_agent(&h.pool, "acme", "joaquin").await;
    let other = seed_agent(&h.pool, "rival", "spy").await;
    let joaquin = connect(&h.base, &a).await;
    let spy = connect(&h.base, &other).await;
    call(&joaquin, "create_channel", json!({"name": "dev"})).await;
    call(&spy, "create_channel", json!({"name": "dev"})).await;

    // 300 KiB of room for acme; rival stays unlimited.
    sqlx::query("UPDATE teams SET attachment_bytes_limit = $1 WHERE slug = 'acme'")
        .bind(300 * 1024i64)
        .execute(&h.pool)
        .await
        .unwrap();

    let file = |n: usize| json!([{"filename": "f.bin", "data_base64": b64(&vec![b'x'; n])}]);

    // Two 128 KiB files fit.
    for _ in 0..2 {
        call(
            &joaquin,
            "post_message",
            json!({"channel": "dev", "body": "chunk", "attachments": file(128 * 1024)}),
        )
        .await;
    }

    // The third does not, and the error says what to do about it.
    let err = call_expect_error(
        &joaquin,
        "post_message",
        json!({"channel": "dev", "body": "chunk", "attachments": file(128 * 1024)}),
    )
    .await;
    assert!(err.contains("quota"), "names the problem: {err}");
    assert!(
        err.contains("307200") && err.contains("raise the quota"),
        "states the limit and the way out: {err}"
    );

    // The rejection is atomic: the message did not land either.
    let msgs = call(
        &joaquin,
        "read_messages",
        json!({"scope": "dev", "only_new": false}),
    )
    .await;
    assert_eq!(
        msgs["messages"].as_array().map(Vec::len),
        Some(2),
        "a quota rejection must not leave the message behind: {msgs:?}"
    );

    // Another team's quota is its own business.
    call(
        &spy,
        "post_message",
        json!({"channel": "dev", "body": "unbounded", "attachments": file(200 * 1024)}),
    )
    .await;

    // Usage counts only this team's bytes.
    let team: (Uuid,) = sqlx::query_as("SELECT id FROM teams WHERE slug = 'acme'")
        .fetch_one(&h.pool)
        .await
        .unwrap();
    let usage = ai_crew_sync::store::quota::usage(&h.pool, team.0)
        .await
        .expect("usage");
    assert_eq!(usage.attachment_count, 2);
    assert_eq!(usage.attachment_bytes, 256 * 1024);
    assert_eq!(usage.attachment_bytes_limit, Some(300 * 1024));

    // Racing uploads cannot both take the last slot: the check and the insert
    // share a transaction that locks the team row.
    sqlx::query("UPDATE teams SET attachment_bytes_limit = $1 WHERE slug = 'acme'")
        .bind(256 * 1024i64 + 100 * 1024)
        .execute(&h.pool)
        .await
        .unwrap();
    let racers: Vec<_> = (0..4)
        .map(|_| {
            let base = h.base.clone();
            let token = a.clone();
            tokio::spawn(async move {
                let c = connect(&base, &token).await;
                let args: serde_json::Map<String, serde_json::Value> =
                    serde_json::from_value(json!({"channel": "dev", "body": "race",
                           "attachments": [{"filename": "r.bin",
                                            "data_base64": base64::engine::general_purpose::STANDARD
                                                .encode(vec![b'y'; 90 * 1024])}]}))
                    .unwrap();
                let ok = c
                    .call_tool(
                        CallToolRequestParams::new("post_message".to_string()).with_arguments(args),
                    )
                    .await
                    .map(|r| r.is_error != Some(true))
                    .unwrap_or(false);
                let _ = c.cancel().await;
                ok
            })
        })
        .collect();
    let mut accepted = 0;
    for r in racers {
        if r.await.unwrap_or(false) {
            accepted += 1;
        }
    }
    assert_eq!(
        accepted, 1,
        "only one of four racing 90 KiB uploads fits in 100 KiB of room"
    );

    let usage = ai_crew_sync::store::quota::usage(&h.pool, team.0)
        .await
        .expect("usage");
    assert!(
        usage.attachment_bytes <= usage.attachment_bytes_limit.unwrap(),
        "the quota was never exceeded: {} > {:?}",
        usage.attachment_bytes,
        usage.attachment_bytes_limit
    );

    let _ = joaquin.cancel().await;
    let _ = spy.cancel().await;
    h.shutdown().await;
}

/// Retention has to be safe to try: a dry run reports exactly what a real run
/// would remove, and removes nothing.
#[tokio::test]
async fn pruning_is_dry_by_default_and_keeps_durable_state() {
    let h = require_db!("t_prune");
    let a = seed_agent(&h.pool, "acme", "joaquin").await;
    let joaquin = connect(&h.base, &a).await;
    call(&joaquin, "create_channel", json!({"name": "dev"})).await;
    call(
        &joaquin,
        "post_message",
        json!({"channel": "dev", "body": "old"}),
    )
    .await;
    call(&joaquin, "set_note", json!({"key": "k", "value": "v1"})).await;
    call(&joaquin, "set_note", json!({"key": "k", "value": "v2"})).await;
    call(&joaquin, "create_task", json!({"key": "t", "title": "t"})).await;

    // Age everything past the window.
    let team: (Uuid,) = sqlx::query_as("SELECT id FROM teams WHERE slug = 'acme'")
        .fetch_one(&h.pool)
        .await
        .unwrap();
    for sql in [
        "UPDATE messages SET created_at = now() - interval '200 days'",
        "UPDATE note_revisions SET created_at = now() - interval '200 days'",
        "UPDATE task_events SET created_at = now() - interval '200 days'",
    ] {
        sqlx::query(sql).execute(&h.pool).await.unwrap();
    }

    let dry = ai_crew_sync::store::quota::prune(&h.pool, team.0, 90, true)
        .await
        .expect("dry run");
    assert!(dry.dry_run);
    assert_eq!(dry.messages, 1, "reports what it would delete");

    // Nothing actually went away.
    let (still,): (i64,) = sqlx::query_as("SELECT count(*) FROM messages")
        .fetch_one(&h.pool)
        .await
        .unwrap();
    assert_eq!(still, 1, "a dry run deletes nothing");

    let applied = ai_crew_sync::store::quota::prune(&h.pool, team.0, 90, false)
        .await
        .expect("apply");
    assert_eq!(
        applied.messages, dry.messages,
        "the dry run's count was the real one"
    );

    // The durable state survives: the note keeps its current value, the task
    // still exists. Only history was trimmed.
    let note = call(&joaquin, "get_note", json!({"key": "k"})).await;
    assert_eq!(
        note["note"]["value"], "v2",
        "notes are not pruned: {note:?}"
    );
    let task = call(&joaquin, "get_task", json!({"key": "t"})).await;
    assert_eq!(task["task"]["key"], "t", "tasks are not pruned: {task:?}");

    // A nonsensical window is refused rather than deleting everything.
    let err = ai_crew_sync::store::quota::prune(&h.pool, team.0, 0, true).await;
    assert!(err.is_err(), "older_than_days must be at least 1");

    // A day count above i32::MAX used to wrap NEGATIVE, which makes
    // `now() - make_interval(days => -N)` a FUTURE instant — so every row
    // matched and "keep almost everything" became "delete everything".
    let err = ai_crew_sync::store::quota::prune(&h.pool, team.0, 2_147_483_648, true).await;
    assert!(err.is_err(), "a day count above i32::MAX must be refused");
    let (survived,): (i64,) = sqlx::query_as("SELECT count(*) FROM notes")
        .fetch_one(&h.pool)
        .await
        .unwrap();
    assert_eq!(survived, 1, "a refused prune deletes nothing");

    let _ = joaquin.cancel().await;
    h.shutdown().await;
}

/// A dropped harness must not leave an axum task, a listener or a dispatcher
/// behind: the suite runs 20+ tests in one process, and leaked listeners would
/// keep consuming notifications for everyone else.
#[tokio::test]
async fn shutdown_stops_the_server_and_its_background_tasks() {
    let h = require_db!("t_shutdown");
    let base = h.base.clone();
    let pool = h.pool.clone();
    let token = seed_agent(&h.pool, "acme", "joaquin").await;

    // Alive before.
    let http = reqwest::Client::new();
    let resp = http.get(format!("{base}/health")).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    h.shutdown().await;

    // After shutdown the socket is closed: the request fails to connect
    // rather than hanging or being served by a task nobody joined.
    let resp = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        http.get(format!("{base}/health")).send(),
    )
    .await
    .expect("the request must not hang after shutdown");
    assert!(
        resp.is_err(),
        "the server should no longer accept connections"
    );

    // The pool is closed too, so a query through it fails rather than
    // silently opening a fresh connection.
    let seeded = sqlx::query("SELECT 1").execute(&pool).await;
    assert!(
        seeded.is_err(),
        "the harness pool must be closed after shutdown"
    );
    assert!(!token.is_empty(), "the agent was seeded before shutdown");
}

#[tokio::test]
async fn the_database_refuses_cross_team_references() {
    let h = require_db!("t_teamfk");

    // Two teams with one agent each, plus a channel and a task per team.
    let team_a: (Uuid,) =
        sqlx::query_as("INSERT INTO teams (slug, name) VALUES ('a', 'A') RETURNING id")
            .fetch_one(&h.pool)
            .await
            .unwrap();
    let team_b: (Uuid,) =
        sqlx::query_as("INSERT INTO teams (slug, name) VALUES ('b', 'B') RETURNING id")
            .fetch_one(&h.pool)
            .await
            .unwrap();
    let agent_a: (Uuid,) =
        sqlx::query_as("INSERT INTO agents (team_id, name) VALUES ($1, 'a') RETURNING id")
            .bind(team_a.0)
            .fetch_one(&h.pool)
            .await
            .unwrap();
    let agent_b: (Uuid,) =
        sqlx::query_as("INSERT INTO agents (team_id, name) VALUES ($1, 'b') RETURNING id")
            .bind(team_b.0)
            .fetch_one(&h.pool)
            .await
            .unwrap();
    let channel_a: (Uuid,) =
        sqlx::query_as("INSERT INTO channels (team_id, name) VALUES ($1, 'dev') RETURNING id")
            .bind(team_a.0)
            .fetch_one(&h.pool)
            .await
            .unwrap();
    let task_a: (Uuid,) = sqlx::query_as(
        "INSERT INTO tasks (team_id, key, title) VALUES ($1, 'ta', 't') RETURNING id",
    )
    .bind(team_a.0)
    .fetch_one(&h.pool)
    .await
    .unwrap();
    let task_b: (Uuid,) = sqlx::query_as(
        "INSERT INTO tasks (team_id, key, title) VALUES ($1, 'tb', 't') RETURNING id",
    )
    .bind(team_b.0)
    .fetch_one(&h.pool)
    .await
    .unwrap();

    // A sender from the other team.
    let err = sqlx::query(
        "INSERT INTO messages (team_id, channel_id, sender_agent_id, body) VALUES ($1,$2,$3,'x')",
    )
    .bind(team_a.0)
    .bind(channel_a.0)
    .bind(agent_b.0)
    .execute(&h.pool)
    .await;
    assert!(err.is_err(), "a sender from another team must be rejected");

    // A channel belonging to the other team.
    let err = sqlx::query(
        "INSERT INTO messages (team_id, sender_agent_id, channel_id, body) VALUES ($1,$2,$3,'x')",
    )
    .bind(team_b.0)
    .bind(agent_b.0)
    .bind(channel_a.0)
    .execute(&h.pool)
    .await;
    assert!(err.is_err(), "another team's channel must be rejected");

    // A direct message addressed across the team boundary.
    let err = sqlx::query(
        "INSERT INTO messages (team_id, sender_agent_id, recipient_agent_id, body)
         VALUES ($1,$2,$3,'x')",
    )
    .bind(team_a.0)
    .bind(agent_a.0)
    .bind(agent_b.0)
    .execute(&h.pool)
    .await;
    assert!(err.is_err(), "a cross-team DM must be rejected");

    // A lock held by the other team's agent.
    let err = sqlx::query(
        "INSERT INTO locks (team_id, name, holder_agent_id, expires_at)
         VALUES ($1,'x',$2, now() + interval '1 hour')",
    )
    .bind(team_a.0)
    .bind(agent_b.0)
    .execute(&h.pool)
    .await;
    assert!(err.is_err(), "a holder from another team must be rejected");

    // A dependency spanning two teams' tasks.
    let err = sqlx::query("INSERT INTO task_deps (task_id, blocked_by_task_id) VALUES ($1,$2)")
        .bind(task_a.0)
        .bind(task_b.0)
        .execute(&h.pool)
        .await;
    assert!(err.is_err(), "a cross-team dependency must be rejected");

    // An attachment on another team's message.
    let msg_a: (i64,) = sqlx::query_as(
        "INSERT INTO messages (team_id, channel_id, sender_agent_id, body)
         VALUES ($1,$2,$3,'legit') RETURNING id",
    )
    .bind(team_a.0)
    .bind(channel_a.0)
    .bind(agent_a.0)
    .fetch_one(&h.pool)
    .await
    .expect("a same-team message is still accepted");

    let err = sqlx::query(
        "INSERT INTO attachments (team_id, message_id, uploader_agent_id, filename, size_bytes, data)
         VALUES ($1,$2,$3,'f',1,'\\x00')",
    )
    .bind(team_b.0)
    .bind(msg_a.0)
    .bind(agent_b.0)
    .execute(&h.pool)
    .await;
    assert!(
        err.is_err(),
        "an attachment on another team's message must be rejected"
    );
}

#[tokio::test]
async fn oversized_fields_are_rejected_with_their_limit() {
    let h = require_db!("t_caps");
    let a = seed_agent(&h.pool, "acme", "joaquin").await;
    let joaquin = connect(&h.base, &a).await;
    call(&joaquin, "create_channel", json!({"name": "dev"})).await;

    // metadata is a pointer payload, not a document: 16 KiB.
    let fat = "x".repeat(20 * 1024);
    let err = call_expect_error(
        &joaquin,
        "post_message",
        json!({"channel": "dev", "body": "hi", "metadata": {"blob": fat}}),
    )
    .await;
    assert!(err.contains("16384"), "names the metadata limit: {err}");

    let err = call_expect_error(
        &joaquin,
        "create_task",
        json!({"key": "fat-meta", "title": "t", "metadata": {"blob": fat}}),
    )
    .await;
    assert!(err.contains("16384"), "same limit on tasks: {err}");

    // Task text fields.
    let long_title = "t".repeat(600);
    let err = call_expect_error(
        &joaquin,
        "create_task",
        json!({"key": "long-title", "title": long_title}),
    )
    .await;
    assert!(err.contains("512"), "names the title limit: {err}");

    let long_text = "d".repeat(70 * 1024);
    let err = call_expect_error(
        &joaquin,
        "create_task",
        json!({"key": "long-desc", "title": "t", "description": long_text.clone()}),
    )
    .await;
    assert!(err.contains("65536"), "names the description limit: {err}");

    call(
        &joaquin,
        "create_task",
        json!({"key": "capped", "title": "fits"}),
    )
    .await;
    call(&joaquin, "claim_task", json!({"key": "capped"})).await;
    let err = call_expect_error(
        &joaquin,
        "complete_task",
        json!({"key": "capped", "result": long_text}),
    )
    .await;
    assert!(err.contains("65536"), "names the result limit: {err}");

    // Nothing oversized was stored: the task is still claimed, not done.
    let task = call(&joaquin, "get_task", json!({"key": "capped"})).await;
    assert_eq!(task["task"]["status"], "claimed", "{task:?}");

    // A 1 MiB body and note are accepted; one byte over is not.
    let one_mib = "b".repeat(1024 * 1024);
    call(
        &joaquin,
        "post_message",
        json!({"channel": "dev", "body": one_mib.clone()}),
    )
    .await;
    let err = call_expect_error(
        &joaquin,
        "post_message",
        json!({"channel": "dev", "body": format!("{one_mib}x")}),
    )
    .await;
    assert!(err.contains("1048576"), "names the body limit: {err}");

    call(
        &joaquin,
        "set_note",
        json!({"key": "big-note", "value": one_mib.clone()}),
    )
    .await;
    let err = call_expect_error(
        &joaquin,
        "set_note",
        json!({"key": "big-note", "value": format!("{one_mib}x")}),
    )
    .await;
    assert!(err.contains("1048576"), "names the note limit: {err}");

    // Whitespace padding must not smuggle a large payload past the cap.
    let padded = format!("{}fits", " ".repeat(700));
    let err = call_expect_error(
        &joaquin,
        "create_task",
        json!({"key": "padded", "title": padded}),
    )
    .await;
    assert!(
        err.contains("512"),
        "raw size counts, not just trimmed: {err}"
    );

    // And values just under the limits still work.
    call(
        &joaquin,
        "post_message",
        json!({"channel": "dev", "body": "ok", "metadata": {"k": "v"}}),
    )
    .await;
    call(
        &joaquin,
        "create_task",
        json!({"key": "ok-task", "title": "t".repeat(512), "description": "d".repeat(1000)}),
    )
    .await;

    let _ = joaquin.cancel().await;
}

#[tokio::test]
async fn attachments_travel_with_messages_and_tasks() {
    use base64::Engine;
    let b64 = |data: &[u8]| base64::engine::general_purpose::STANDARD.encode(data);

    let h = require_db!("t_attach");
    let a = seed_agent(&h.pool, "acme", "joaquin").await;
    let b = seed_agent(&h.pool, "acme", "marta").await;
    let c = seed_agent(&h.pool, "acme", "pedro").await;
    let joaquin = connect(&h.base, &a).await;
    let marta = connect(&h.base, &b).await;
    let pedro = connect(&h.base, &c).await;

    let diff = "diff --git a/src/lib.rs b/src/lib.rs\n-old\n+new\n";

    // A channel message ships with its file in one call.
    call(&joaquin, "create_channel", json!({"name": "dev"})).await;
    let posted = call(
        &joaquin,
        "post_message",
        json!({
            "channel": "dev", "body": "parser fix attached",
            "attachments": [{
                "filename": "fix.diff", "content_type": "text/plain",
                "data_base64": b64(diff.as_bytes())
            }]
        }),
    )
    .await;
    let att = &posted["message"]["attachments"][0];
    assert_eq!(att["filename"], "fix.diff", "{posted:?}");
    let att_id = att["id"].as_i64().unwrap();

    // A teammate sees the attachment listed and downloads identical bytes.
    let read = call(&marta, "read_messages", json!({"scope": "dev"})).await;
    let msg = read["messages"].as_array().unwrap().last().unwrap().clone();
    assert_eq!(msg["attachments"][0]["id"], att_id, "{msg:?}");
    let got = call(&marta, "get_attachment", json!({"id": att_id})).await;
    assert_eq!(got["data_base64"].as_str().unwrap(), b64(diff.as_bytes()));
    assert_eq!(got["uploaded_by"], "joaquin");

    // DM attachments are invisible to anyone but the two parties.
    let dm = call(
        &joaquin,
        "post_message",
        json!({
            "to": "marta", "body": "the failing log",
            "attachments": [{"filename": "secret.log", "data_base64": b64(b"boom")}]
        }),
    )
    .await;
    let dm_att = dm["message"]["attachments"][0]["id"].as_i64().unwrap();
    call(&marta, "get_attachment", json!({"id": dm_att})).await;
    let err = call_expect_error(&pedro, "get_attachment", json!({"id": dm_att})).await;
    assert!(
        err.contains("not found"),
        "third party must not see it: {err}"
    );

    // Tasks carry attachments too, from any teammate.
    call(
        &joaquin,
        "create_task",
        json!({"key": "fix-parser", "title": "Fix the parser"}),
    )
    .await;
    call(
        &marta,
        "attach_file",
        json!({"task": "fix-parser", "filename": "repro.log", "data_base64": b64(b"repro")}),
    )
    .await;
    let task = call(&pedro, "get_task", json!({"key": "fix-parser"})).await;
    assert_eq!(
        task["task"]["attachments"][0]["filename"], "repro.log",
        "{task:?}"
    );

    // The size cap rejects with an actionable message.
    let big = vec![b'x'; 300 * 1024];
    let err = call_expect_error(
        &joaquin,
        "attach_file",
        json!({"task": "fix-parser", "filename": "big.bin", "data_base64": b64(&big)}),
    )
    .await;
    assert!(err.contains("262144"), "must state the limit: {err}");

    let _ = joaquin.cancel().await;
    let _ = marta.cancel().await;
    let _ = pedro.cancel().await;
}

#[tokio::test]
async fn blocked_tasks_wait_for_their_dependencies() {
    let h = require_db!("t_deps");
    let a = seed_agent(&h.pool, "acme", "joaquin").await;
    let b = seed_agent(&h.pool, "acme", "marta").await;
    let joaquin = connect(&h.base, &a).await;
    let marta = connect(&h.base, &b).await;

    call(
        &joaquin,
        "create_task",
        json!({"key": "migrate-schema", "title": "migrate the users schema"}),
    )
    .await;
    let dependent = call(
        &joaquin,
        "create_task",
        json!({
            "key": "update-clients",
            "title": "update the API clients",
            "depends_on": ["migrate-schema"]
        }),
    )
    .await;
    assert_eq!(dependent["blocked"], true);
    assert_eq!(dependent["depends_on"][0], "migrate-schema");

    // A dependency that does not exist is a clean error.
    let err = call_expect_error(
        &joaquin,
        "create_task",
        json!({"key": "x", "title": "x", "depends_on": ["nope"]}),
    )
    .await;
    assert!(err.contains("nope"), "got: {err}");

    // The blocked task cannot be claimed, with an explanatory reason.
    let denied = call(&marta, "claim_task", json!({"key": "update-clients"})).await;
    assert_eq!(denied["claimed"], false);
    assert!(
        denied["reason"]
            .as_str()
            .unwrap()
            .contains("migrate-schema"),
        "reason should name the blocker: {denied:?}"
    );

    // claim_next_task skips it and hands out the dependency instead.
    let next = call(&marta, "claim_next_task", json!({})).await;
    assert_eq!(next["claimed"], true);
    assert_eq!(next["task"]["key"], "migrate-schema");

    // Finishing the dependency unblocks the dependent task.
    call(&marta, "complete_task", json!({"key": "migrate-schema"})).await;
    let now_free = call(&joaquin, "claim_task", json!({"key": "update-clients"})).await;
    assert_eq!(
        now_free["claimed"], true,
        "unblocked after dep done: {now_free:?}"
    );
    assert_eq!(now_free["task"]["blocked"], false);

    let _ = joaquin.cancel().await;
    let _ = marta.cancel().await;
}

#[tokio::test]
async fn locks_are_exclusive_expiring_and_visible() {
    let h = require_db!("t_locks");
    let a = seed_agent(&h.pool, "acme", "joaquin").await;
    let b = seed_agent(&h.pool, "acme", "marta").await;
    let joaquin = connect(&h.base, &a).await;
    let marta = connect(&h.base, &b).await;

    let got = call(
        &joaquin,
        "acquire_lock",
        json!({"name": "Deploy:Staging", "ttl_seconds": 120, "purpose": "rolling out 1.4.2"}),
    )
    .await;
    assert_eq!(got["acquired"], true);
    assert_eq!(got["lock"]["name"], "deploy:staging", "name normalised");

    // Second acquirer is refused and told who holds it.
    let denied = call(&marta, "acquire_lock", json!({"name": "deploy:staging"})).await;
    assert_eq!(denied["acquired"], false);
    assert!(denied["reason"].as_str().unwrap().contains("joaquin"));

    // Re-acquiring your own lock extends it, not an error.
    let extended = call(
        &joaquin,
        "acquire_lock",
        json!({"name": "deploy:staging", "ttl_seconds": 600}),
    )
    .await;
    assert_eq!(extended["acquired"], true);

    // Visible to the whole team.
    let listed = call(&marta, "list_locks", json!({})).await;
    assert_eq!(listed["locks"][0]["holder"], "joaquin");
    assert_eq!(listed["locks"][0]["purpose"], "rolling out 1.4.2");

    // You cannot release someone else's lock.
    let err = call_expect_error(&marta, "release_lock", json!({"name": "deploy:staging"})).await;
    assert!(err.contains("joaquin"), "got: {err}");

    // Release frees it for the next agent.
    call(&joaquin, "release_lock", json!({"name": "deploy:staging"})).await;
    let now = call(&marta, "acquire_lock", json!({"name": "deploy:staging"})).await;
    assert_eq!(now["acquired"], true);

    // Expired locks are silently taken over.
    sqlx::query("UPDATE locks SET expires_at = now() - interval '1 second'")
        .execute(&h.pool)
        .await
        .unwrap();
    let stolen = call(&joaquin, "acquire_lock", json!({"name": "deploy:staging"})).await;
    assert_eq!(stolen["acquired"], true, "expired lock must be stealable");

    let _ = joaquin.cancel().await;
    let _ = marta.cancel().await;
}

#[tokio::test]
async fn team_digest_summarises_recent_activity() {
    let h = require_db!("t_digest");
    let a = seed_agent(&h.pool, "acme", "joaquin").await;
    let b = seed_agent(&h.pool, "acme", "marta").await;
    let joaquin = connect(&h.base, &a).await;
    let marta = connect(&h.base, &b).await;

    call(&joaquin, "create_channel", json!({"name": "deploys"})).await;
    call(
        &joaquin,
        "post_message",
        json!({"channel": "deploys", "body": "staging lleva la 1.4.2"}),
    )
    .await;
    call(
        &marta,
        "create_task",
        json!({"key": "hotfix", "title": "hotfix the parser"}),
    )
    .await;
    call(&marta, "claim_task", json!({"key": "hotfix"})).await;
    call(
        &marta,
        "complete_task",
        json!({"key": "hotfix", "result": "merged in #99"}),
    )
    .await;
    call(
        &joaquin,
        "set_note",
        json!({"scope": "api", "key": "deploy-runbook", "value": "step 1..."}),
    )
    .await;
    call(&marta, "heartbeat", json!({"activity": "reviewing PRs"})).await;
    // A DM that must NOT leak into the digest.
    call(
        &joaquin,
        "post_message",
        json!({"to": "marta", "body": "esto es privado"}),
    )
    .await;

    let digest = call(&joaquin, "team_digest", json!({"hours": 24})).await;
    assert_eq!(digest["channels"][0]["name"], "deploys");
    assert_eq!(digest["channels"][0]["message_count"], 1);
    let tasks: Vec<&str> = digest["tasks_moved"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["key"].as_str().unwrap())
        .collect();
    assert!(tasks.contains(&"hotfix"));
    assert_eq!(digest["notes_updated"][0]["key"], "deploy-runbook");
    assert!(
        digest["agents_seen"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["name"] == "marta" && a["online"] == true)
    );
    let serialized = serde_json::to_string(&digest).unwrap();
    assert!(
        !serialized.contains("privado"),
        "digest must never contain direct messages"
    );

    let _ = joaquin.cancel().await;
    let _ = marta.cancel().await;
}

#[tokio::test]
async fn webhooks_forward_channel_messages_but_never_dms() {
    let h = require_db!("t_webhooks");
    let a = seed_agent(&h.pool, "acme", "joaquin").await;
    let b = seed_agent(&h.pool, "acme", "marta").await;
    let joaquin = connect(&h.base, &a).await;

    // A local catcher standing in for Slack.
    let received: std::sync::Arc<tokio::sync::Mutex<Vec<Value>>> = Default::default();
    let catcher = {
        let received = received.clone();
        let app = axum::Router::new().route(
            "/hook",
            axum::routing::post(move |axum::Json(v): axum::Json<Value>| {
                let received = received.clone();
                async move {
                    received.lock().await.push(v);
                    "ok"
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        addr
    };

    ai_crew_sync::webhooks::webhook_add(
        &h.pool,
        "acme",
        &format!("http://{catcher}/hook"),
        "slack",
        "message,task",
        None,
    )
    .await
    .unwrap();

    call(&joaquin, "create_channel", json!({"name": "deploys"})).await;
    call(
        &joaquin,
        "post_message",
        json!({"channel": "deploys", "body": "canary verde"}),
    )
    .await;
    call(
        &joaquin,
        "post_message",
        json!({"to": "marta", "body": "secreto entre nosotros"}),
    )
    .await;
    call(
        &joaquin,
        "create_task",
        json!({"key": "rotate", "title": "rotate keys"}),
    )
    .await;
    let _ = b; // marta only needs to exist as a DM target

    // Give LISTEN/NOTIFY + dispatch a moment.
    let mut tries = 0;
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let got = received.lock().await;
        if got.len() >= 2 || tries > 20 {
            break;
        }
        drop(got);
        tries += 1;
    }

    let got = received.lock().await;
    let texts: Vec<String> = got
        .iter()
        .map(|v| v["text"].as_str().unwrap_or_default().to_owned())
        .collect();
    assert!(
        texts
            .iter()
            .any(|t| t.contains("#deploys") && t.contains("canary verde")),
        "channel message must be forwarded in Slack format: {texts:?}"
    );
    assert!(
        texts.iter().any(|t| t.contains("rotate")),
        "task event must be forwarded: {texts:?}"
    );
    assert!(
        !texts.iter().any(|t| t.contains("secreto")),
        "a DM must NEVER reach a webhook: {texts:?}"
    );

    let _ = joaquin.cancel().await;
}

#[tokio::test]
async fn dashboard_requires_a_token_and_renders_team_state() {
    let h = require_db!("t_dash");
    let a = seed_agent(&h.pool, "acme", "joaquin").await;
    let joaquin = connect(&h.base, &a).await;
    call(&joaquin, "heartbeat", json!({"activity": "smoke testing"})).await;
    call(&joaquin, "create_channel", json!({"name": "dev"})).await;
    call(
        &joaquin,
        "post_message",
        json!({"channel": "dev", "body": "<script>alert(1)</script> it's here"}),
    )
    .await;

    // Redirects are followed manually so the Set-Cookie exchange is visible.
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let base = &h.base;

    // No credential at all → the sign-in page, not the data.
    let resp = http.get(format!("{base}/dashboard")).send().await.unwrap();
    assert_eq!(resp.status(), 401);
    let body = resp.text().await.unwrap();
    assert!(body.contains("<form"), "offers a form to sign in: {body}");
    assert!(!body.contains("smoke testing"), "leaks no team state");

    // A token in the query string is NOT a credential any more.
    let resp = http
        .get(format!("{base}/dashboard?token={a}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        401,
        "query-string tokens must not authenticate"
    );

    // Exchange the token for a session cookie via the form POST.
    let resp = http
        .post(format!("{base}/dashboard/login"))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(format!("token={a}"))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_redirection(),
        "successful login redirects: {}",
        resp.status()
    );
    let cookie = resp
        .headers()
        .get("set-cookie")
        .and_then(|v| v.to_str().ok())
        .expect("a session cookie")
        .to_owned();
    assert!(cookie.contains("HttpOnly"), "cookie is HttpOnly: {cookie}");
    assert!(
        cookie.contains("SameSite=Strict"),
        "cookie is SameSite=Strict: {cookie}"
    );
    assert!(
        !cookie.contains(&a),
        "the agent token itself must never be the cookie value"
    );

    let grant = cookie.split(';').next().expect("cookie pair").to_owned();

    // A bad token gets the form back, not a cookie.
    let resp = http
        .post(format!("{base}/dashboard/login"))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body("token=acs_bogus")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
    assert!(resp.headers().get("set-cookie").is_none());

    // The cookie renders the page.
    let resp = http
        .get(format!("{base}/dashboard"))
        .header("Cookie", &grant)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("cache-control")
            .and_then(|v| v.to_str().ok()),
        Some("no-store"),
        "team activity is never cached"
    );
    assert_eq!(
        resp.headers()
            .get("referrer-policy")
            .and_then(|v| v.to_str().ok()),
        Some("no-referrer")
    );
    let body = resp.text().await.unwrap();
    assert!(body.contains("joaquin"), "shows the agent");
    assert!(body.contains("smoke testing"), "shows the activity");
    assert!(
        !body.contains("<script>alert(1)</script>"),
        "message bodies must be HTML-escaped"
    );
    assert!(body.contains("&lt;script&gt;"), "escaped form present");
    assert!(!body.contains("it's here"), "single quotes escaped too");
    assert!(body.contains("it&#39;s here"), "escaped quote present");

    // The grant is read-only: it cannot drive the MCP surface.
    let grant_value = grant.split_once('=').expect("cookie pair").1.to_owned();
    for attempt in [
        http.post(format!("{base}/mcp"))
            .header("Cookie", &grant)
            .header("Accept", "application/json, text/event-stream")
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/list"})),
        http.post(format!("{base}/mcp"))
            .header("Authorization", format!("Bearer {grant_value}"))
            .header("Accept", "application/json, text/event-stream")
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/list"})),
    ] {
        let resp = attempt.send().await.unwrap();
        assert_eq!(
            resp.status(),
            401,
            "a dashboard grant must not authenticate an MCP call"
        );
    }

    // The bearer header still works for scripts, without any cookie exchange.
    let resp = http
        .get(format!("{base}/dashboard"))
        .header("Authorization", format!("Bearer {a}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "curl/script access keeps working");

    let _ = joaquin.cancel().await;
}

// ------------------------------------------------------------------ sessions --

#[tokio::test]
async fn one_token_carries_several_sessions_without_splitting_identity() {
    let h = require_db!("t_session_ctx");
    let token = seed_agent(&h.pool, "layerv", "joaquin").await;

    let market = connect_with_session(&h.base, &token, "market-data").await;
    let core = connect_with_session(&h.base, &token, "core-manager").await;
    let shared = connect(&h.base, &token).await;

    let m = call(&market, "whoami", json!({})).await;
    let c = call(&core, "whoami", json!({})).await;
    let s = call(&shared, "whoami", json!({})).await;

    // One person: the session never changes who is speaking.
    assert_eq!(m["agent"], "joaquin");
    assert_eq!(m["agent_id"], c["agent_id"]);
    assert_eq!(m["agent_id"], s["agent_id"]);
    assert_eq!(m["team"], "layerv");

    // Three working contexts.
    assert_eq!(m["session"], "market-data");
    assert_eq!(c["session"], "core-manager");
    assert_eq!(
        s["session"],
        Value::Null,
        "no header must report the shared session as null, not as an empty name"
    );

    // Case and padding must not silently create a second session.
    let same = connect_with_session(&h.base, &token, "  Market-Data ").await;
    assert_eq!(
        call(&same, "whoami", json!({})).await["session"],
        "market-data"
    );

    // Presence is per session now, so list_agents must still report one entry
    // per *person* rather than one per session. Grouping the sessions under
    // their agent is the next change in the stack; until it lands, duplicate
    // rows would read as duplicate teammates.
    call(&market, "heartbeat", json!({"repo": "Layer-V/market-data"})).await;
    call(&core, "heartbeat", json!({"repo": "Layer-V/core-manager"})).await;
    let seen = call(&market, "list_agents", json!({})).await;
    let mine = seen["agents"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|a| a["name"] == "joaquin")
        .count();
    assert_eq!(mine, 1, "one entry per teammate: {seen}");
    assert_eq!(seen["online_count"], 1, "two sessions is still one person");

    for client in [market, core, shared, same] {
        let _ = client.cancel().await;
    }
    h.shutdown().await;
}

#[tokio::test]
async fn a_malformed_session_header_is_rejected_before_the_token_is_used() {
    let h = require_db!("t_session_bad");
    let token = seed_agent(&h.pool, "layerv", "joaquin").await;
    let http = reqwest::Client::new();

    let call_with = |session: String| {
        let http = http.clone();
        let base = h.base.clone();
        let token = token.clone();
        async move {
            http.post(format!("{base}/mcp"))
                .header("Authorization", format!("Bearer {token}"))
                .header(ai_crew_sync::auth::SESSION_HEADER, session)
                .header("Accept", "application/json, text/event-stream")
                .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))
                .send()
                .await
                .unwrap()
        }
    };

    let resp = call_with("x".repeat(ai_crew_sync::auth::MAX_SESSION_BYTES + 1)).await;
    assert_eq!(
        resp.status(),
        400,
        "an over-long session label is a bad request"
    );
    let body = resp.text().await.unwrap();
    assert!(
        body.contains(&ai_crew_sync::auth::MAX_SESSION_BYTES.to_string()),
        "the error must state the limit so the caller can fix it: {body}"
    );

    // '/' separates agent from session when addressing a message.
    let resp = call_with("joaquin/market-data".to_owned()).await;
    assert_eq!(resp.status(), 400);

    // A valid label on the same token still works, so nothing above rejected
    // the token itself.
    let resp = call_with("market-data".to_owned()).await;
    assert_eq!(resp.status(), 200);

    h.shutdown().await;
}

#[tokio::test]
async fn presence_is_tracked_per_session_not_per_person() {
    let h = require_db!("t_presence_sessions");
    let token = seed_agent(&h.pool, "layerv", "joaquin").await;
    let dani = seed_agent(&h.pool, "layerv", "dani").await;

    let market = connect_with_session(&h.base, &token, "market-data").await;
    let core = connect_with_session(&h.base, &token, "core-manager").await;

    call(
        &market,
        "heartbeat",
        json!({"repo": "Layer-V/market-data", "branch": "devops/scanning"}),
    )
    .await;
    call(
        &core,
        "heartbeat",
        json!({"repo": "Layer-V/core-manager", "branch": "issue-151"}),
    )
    .await;

    // Before this change the second heartbeat overwrote the first, and the
    // board showed one repo flapping between the two.
    let seen = call(&market, "list_agents", json!({})).await;
    let joaquin = seen["agents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["name"] == "joaquin")
        .expect("joaquin is on the bus");

    let sessions = joaquin["sessions"].as_array().expect("two contexts listed");
    assert_eq!(
        sessions.len(),
        2,
        "one entry per working context: {joaquin}"
    );
    let mut repos: Vec<&str> = sessions
        .iter()
        .map(|s| s["repo"].as_str().unwrap_or_default())
        .collect();
    repos.sort_unstable();
    assert_eq!(repos, ["Layer-V/core-manager", "Layer-V/market-data"]);

    let mut labels: Vec<&str> = sessions
        .iter()
        .map(|s| s["session"].as_str().unwrap_or_default())
        .collect();
    labels.sort_unstable();
    assert_eq!(labels, ["core-manager", "market-data"]);

    // One person, not two: three live sessions across two people is two online.
    let dani_client = connect(&h.base, &dani).await;
    call(
        &dani_client,
        "heartbeat",
        json!({"repo": "Layer-V/core-manager"}),
    )
    .await;
    let seen = call(&market, "list_agents", json!({})).await;
    assert_eq!(
        seen["online_count"], 2,
        "online_count counts teammates, not sessions: {seen}"
    );

    // An agent with a single shared session keeps the flat shape it had before
    // sessions existed. Asserted on the JSON keys rather than on values,
    // because `value["absent"]` and `value["x"] == null` read the same from a
    // test and very differently from a client.
    let dani_row = seen["agents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["name"] == "dani")
        .unwrap();
    let keys: Vec<&String> = dani_row.as_object().unwrap().keys().collect();
    assert!(
        !keys.iter().any(|k| *k == "session" || *k == "sessions"),
        "the shared session must add no key at all, before or after: {keys:?}"
    );
    assert_eq!(dani_row["repo"], "Layer-V/core-manager");

    // The digest reads presence too, and it is keyed per session now: a person
    // in two repositories must still appear once in the catch-up.
    let digest = call(&market, "team_digest", json!({"hours": 1})).await;
    let joaquins = digest["agents_seen"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|a| a["name"] == "joaquin")
        .count();
    assert_eq!(joaquins, 1, "one line per teammate in a catch-up: {digest}");

    // One session going stale leaves the others alone.
    sqlx::query(
        "UPDATE agent_presence SET expires_at = now() - interval '1 minute' WHERE session = $1",
    )
    .bind("core-manager")
    .execute(&h.pool)
    .await
    .unwrap();
    let seen = call(&market, "list_agents", json!({"online_only": true})).await;
    let joaquin = seen["agents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["name"] == "joaquin")
        .expect("the live session keeps joaquin online");
    assert_eq!(joaquin["repo"], "Layer-V/market-data");

    for client in [market, core, dani_client] {
        let _ = client.cancel().await;
    }
    h.shutdown().await;
}

#[tokio::test]
async fn a_claim_belongs_to_a_session_not_to_a_person() {
    let h = require_db!("t_session_claims");
    let token = seed_agent(&h.pool, "layerv", "joaquin").await;

    let market = connect_with_session(&h.base, &token, "market-data").await;
    let core = connect_with_session(&h.base, &token, "core-manager").await;

    call(
        &market,
        "create_task",
        json!({"key": "market-data#42", "title": "wire the feed"}),
    )
    .await;

    let first = call(&market, "claim_task", json!({"key": "market-data#42"})).await;
    assert_eq!(first["claimed"], true);
    assert_eq!(first["task"]["claimed_session"], "market-data");

    // The bug this fixes: before, `claimed_by = me` alone satisfied the claim
    // predicate, so this second window was told it held the task too and both
    // did the work.
    let second = call(&core, "claim_task", json!({"key": "market-data#42"})).await;
    assert_eq!(
        second["claimed"], false,
        "another session of the same person must not hold the same claim"
    );
    let reason = second["reason"].as_str().unwrap_or_default();
    assert!(
        reason.contains("market-data") && reason.contains("your own"),
        "the refusal must name the holding session: {reason}"
    );

    // Re-claiming from the holding session is still a lease renewal.
    let renewed = call(&market, "claim_task", json!({"key": "market-data#42"})).await;
    assert_eq!(renewed["claimed"], true, "self-renewal must keep working");

    // Neither renew nor release crosses sessions, and both say who holds it.
    for tool in ["renew_task_lease", "release_task"] {
        let err = call_expect_error(&core, tool, json!({"key": "market-data#42"})).await;
        assert!(
            err.contains("market-data"),
            "{tool} must name the holding session: {err}"
        );
    }
    call(
        &market,
        "renew_task_lease",
        json!({"key": "market-data#42"}),
    )
    .await;

    // "mine" means this session's, the same rule whoami/renew/release use.
    let mine = call(&market, "list_tasks", json!({"mine_only": true})).await;
    assert_eq!(mine["tasks"].as_array().unwrap().len(), 1);
    let theirs = call(&core, "list_tasks", json!({"mine_only": true})).await;
    assert_eq!(
        theirs["tasks"].as_array().unwrap().len(),
        0,
        "another window of the same token does not own this claim: {theirs}"
    );

    // Releasing clears the session with the holder it belongs to; a released
    // task reporting claimed_by null next to a session name would describe an
    // active holder that does not exist.
    let released = call(&market, "release_task", json!({"key": "market-data#42"})).await;
    assert_eq!(released["status"], "open");
    assert_eq!(released["claimed_by"], Value::Null);
    assert!(
        released.get("claimed_session").is_none_or(|v| v.is_null()),
        "the released task must name no holding session: {released}"
    );
    call(&market, "claim_task", json!({"key": "market-data#42"})).await;

    // An expired lease is stealable by anyone, including another session.
    sqlx::query("UPDATE tasks SET lease_expires_at = now() - interval '1 minute'")
        .execute(&h.pool)
        .await
        .unwrap();
    let stolen = call(&core, "claim_task", json!({"key": "market-data#42"})).await;
    assert_eq!(stolen["claimed"], true, "an expired lease is up for grabs");
    assert_eq!(stolen["task"]["claimed_session"], "core-manager");

    for client in [market, core] {
        let _ = client.cancel().await;
    }
    h.shutdown().await;
}

#[tokio::test]
async fn a_lock_belongs_to_a_session_not_to_a_person() {
    let h = require_db!("t_session_locks");
    let token = seed_agent(&h.pool, "layerv", "joaquin").await;

    let market = connect_with_session(&h.base, &token, "market-data").await;
    let core = connect_with_session(&h.base, &token, "core-manager").await;

    let taken = call(&market, "acquire_lock", json!({"name": "deploy:staging"})).await;
    assert_eq!(taken["acquired"], true);
    assert_eq!(taken["lock"]["holder_session"], "market-data");

    // Your other window must not inherit a live deploy lock.
    let blocked = call(&core, "acquire_lock", json!({"name": "deploy:staging"})).await;
    assert_eq!(blocked["acquired"], false);
    assert!(
        blocked["reason"]
            .as_str()
            .unwrap_or_default()
            .contains("market-data"),
        "the refusal must name the holding session: {blocked}"
    );

    // Nor release it.
    let err = call_expect_error(&core, "release_lock", json!({"name": "deploy:staging"})).await;
    assert!(err.contains("market-data"), "{err}");

    // Extending from the holding session still works.
    let again = call(&market, "acquire_lock", json!({"name": "deploy:staging"})).await;
    assert_eq!(
        again["acquired"], true,
        "the holder can extend its own lock"
    );

    call(&market, "release_lock", json!({"name": "deploy:staging"})).await;
    let now_free = call(&core, "acquire_lock", json!({"name": "deploy:staging"})).await;
    assert_eq!(now_free["acquired"], true);

    for client in [market, core] {
        let _ = client.cancel().await;
    }
    h.shutdown().await;
}

#[tokio::test]
async fn a_direct_message_can_address_one_session_of_a_person() {
    let h = require_db!("t_session_dms");
    let token = seed_agent(&h.pool, "layerv", "joaquin").await;
    let dani = seed_agent(&h.pool, "layerv", "dani").await;

    let general = connect_with_session(&h.base, &token, "general").await;
    let market = connect_with_session(&h.base, &token, "market-data").await;
    let core = connect_with_session(&h.base, &token, "core-manager").await;
    let dani_client = connect(&h.base, &dani).await;

    // Addressed to one window of one person.
    let sent = call(
        &general,
        "post_message",
        json!({"to": "joaquin/market-data", "body": "rebase onto main first"}),
    )
    .await;
    assert_eq!(sent["delivered_to"][0], "joaquin/market-data");
    assert_eq!(sent["message"]["to_session"], "market-data");
    assert_eq!(
        sent["message"]["from_session"], "general",
        "a reply needs to know which window asked"
    );

    let inbox = call(&market, "read_messages", json!({"scope": "inbox"})).await;
    assert_eq!(inbox["messages"][0]["body"], "rebase onto main first");

    // The sibling window is not the addressee and does not see it by default.
    let other = call(&core, "read_messages", json!({"scope": "inbox"})).await;
    assert_eq!(
        other["messages"].as_array().unwrap().len(),
        0,
        "a sibling session must not receive another's mail: {other}"
    );
    // But a person can always read their own mail when they ask for it.
    let everything = call(
        &core,
        "read_messages",
        json!({"scope": "inbox", "all_sessions": true, "only_new": false}),
    )
    .await;
    assert_eq!(everything["messages"][0]["body"], "rebase onto main first");

    // Addressing the person still reaches every window, as it always has.
    call(
        &dani_client,
        "post_message",
        json!({"to": "joaquin", "body": "standup in 5"}),
    )
    .await;
    for client in [&market, &core] {
        let seen = call(client, "read_messages", json!({"scope": "inbox"})).await;
        let bodies: Vec<&str> = seen["messages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["body"].as_str().unwrap_or_default())
            .collect();
        assert!(
            bodies.contains(&"standup in 5"),
            "a message to the person reaches every session: {bodies:?}"
        );
    }

    // Reading in one window must not mark another window's inbox read.
    let again = call(&market, "read_messages", json!({"scope": "inbox"})).await;
    assert_eq!(
        again["messages"].as_array().unwrap().len(),
        0,
        "this window had already read everything addressed to it"
    );

    // Talking to the window you are in is refused with something to do instead.
    let err = call_expect_error(
        &market,
        "post_message",
        json!({"to": "joaquin/market-data", "body": "note to self"}),
    )
    .await;
    assert!(err.contains("set_note"), "{err}");

    for client in [general, market, core, dani_client] {
        let _ = client.cancel().await;
    }
    h.shutdown().await;
}

#[tokio::test]
async fn one_session_can_ask_another_session_of_the_same_person() {
    let h = require_db!("t_session_ask");
    let token = seed_agent(&h.pool, "layerv", "joaquin").await;

    let general = connect_with_session(&h.base, &token, "general").await;
    let market = connect_with_session(&h.base, &token, "market-data").await;

    // The coordinating window asks the one that has the repository open, and
    // blocks. The answer must come back here, not to some other window.
    let asker = tokio::spawn(async move {
        let answer = call(
            &general,
            "ask_agent",
            json!({"to": "joaquin/market-data", "question": "is the suite green?",
                   "timeout_seconds": 20}),
        )
        .await;
        let _ = general.cancel().await;
        answer
    });

    // The addressed window sees the question in its own inbox and replies.
    let mut question_id = None;
    for _ in 0..40 {
        let inbox = call(&market, "read_messages", json!({"scope": "inbox"})).await;
        if let Some(m) = inbox["messages"].as_array().and_then(|a| a.first()) {
            assert_eq!(m["from_session"], "general");
            assert_eq!(m["metadata"]["question"], true);
            question_id = m["id"].as_i64();
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let question_id = question_id.expect("the question reached the addressed session");

    call(
        &market,
        "post_message",
        json!({"to": "joaquin/general", "body": "green, 34 passing",
               "reply_to": question_id}),
    )
    .await;

    let answer = asker.await.unwrap();
    assert_eq!(answer["answered"], true, "{answer}");
    assert_eq!(answer["answer"]["body"], "green, 34 passing");
    assert_eq!(answer["answer"]["from_session"], "market-data");

    let _ = market.cancel().await;
    h.shutdown().await;
}

#[tokio::test]
async fn a_sibling_session_cannot_answer_for_the_one_that_was_asked() {
    let h = require_db!("t_session_ask_sibling");
    let token = seed_agent(&h.pool, "layerv", "joaquin").await;
    let dani = seed_agent(&h.pool, "layerv", "dani").await;

    let general = connect_with_session(&h.base, &token, "general").await;
    let dani_api = connect_with_session(&h.base, &dani, "api").await;
    let dani_web = connect_with_session(&h.base, &dani, "web").await;

    // Ask one specific window of dani's.
    let asker = tokio::spawn(async move {
        let r = call(
            &general,
            "ask_agent",
            json!({"to": "dani/api", "question": "did the migration land?",
                   "timeout_seconds": 8}),
        )
        .await;
        let _ = general.cancel().await;
        r
    });
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    // A different window of the same person answers. It must NOT satisfy the
    // wait: the question was addressed to `api`, and `web` cannot see what
    // `api` was asked about.
    call(
        &dani_web,
        "post_message",
        json!({"to": "joaquin/general", "body": "no idea, wrong window"}),
    )
    .await;

    let out = asker.await.unwrap();
    assert_eq!(
        out["answered"], false,
        "a sibling session must not answer for the one that was asked: {out}"
    );
    let qid = out["question_message_id"].as_i64().unwrap();

    // The window that was actually asked answers, and resuming finds it.
    call(
        &dani_api,
        "post_message",
        json!({"to": "joaquin/general", "body": "yes, 0009 applied"}),
    )
    .await;
    let general = connect_with_session(&h.base, &token, "general").await;
    let resumed = call(
        &general,
        "ask_agent",
        json!({"to": "dani/api", "resume_message_id": qid, "timeout_seconds": 5}),
    )
    .await;
    assert_eq!(resumed["answered"], true, "{resumed}");
    assert_eq!(resumed["answer"]["body"], "yes, 0009 applied");

    // Resuming that question against a different address is refused, or a
    // timed-out question could collect another session's answer.
    let err = call_expect_error(
        &general,
        "ask_agent",
        json!({"to": "dani/web", "resume_message_id": qid, "timeout_seconds": 5}),
    )
    .await;
    assert!(err.contains("this session sent"), "{err}");

    for client in [general, dani_api, dani_web] {
        let _ = client.cancel().await;
    }
    h.shutdown().await;
}

#[tokio::test]
async fn wait_for_updates_does_not_wake_a_sibling_session() {
    let h = require_db!("t_session_wait");
    let token = seed_agent(&h.pool, "layerv", "joaquin").await;
    let dani = seed_agent(&h.pool, "layerv", "dani").await;

    let market = connect_with_session(&h.base, &token, "market-data").await;
    let core = connect_with_session(&h.base, &token, "core-manager").await;
    let dani_client = connect(&h.base, &dani).await;

    // core-manager blocks. A question for market-data must not wake it, or
    // every window of a person wakes for work meant for one of them.
    let waiter = tokio::spawn(async move {
        let r = call(
            &core,
            "wait_for_updates",
            json!({"timeout_seconds": 6, "kinds": ["message"]}),
        )
        .await;
        let _ = core.cancel().await;
        r
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    call(
        &dani_client,
        "post_message",
        json!({"to": "joaquin/market-data", "body": "only for that window"}),
    )
    .await;

    let woke = waiter.await.unwrap();
    assert_eq!(
        woke["timed_out"], true,
        "a sibling session's mail must not wake this one: {woke}"
    );

    // The addressed window, however, has it waiting immediately.
    let seen = call(
        &market,
        "wait_for_updates",
        json!({"timeout_seconds": 5, "kinds": ["message"]}),
    )
    .await;
    assert_eq!(seen["woke"], true, "{seen}");

    for client in [market, dani_client] {
        let _ = client.cancel().await;
    }
    h.shutdown().await;
}

#[tokio::test]
async fn a_session_posts_to_and_watches_the_channel_named_after_it() {
    let h = require_db!("t_session_channel");
    let token = seed_agent(&h.pool, "layerv", "joaquin").await;
    let dani = seed_agent(&h.pool, "layerv", "dani").await;

    let market = connect_with_session(&h.base, &token, "market-data").await;
    let dani_client = connect(&h.base, &dani).await;

    // No channel of that name yet: the error says what to do about it.
    let err = call_expect_error(&market, "post_message", json!({"body": "hello"})).await;
    assert!(
        err.contains("market-data") && err.contains("create_channel"),
        "the refusal must name the session and the fix: {err}"
    );

    call(&market, "create_channel", json!({"name": "market-data"})).await;
    call(&market, "create_channel", json!({"name": "core-manager"})).await;

    // Now the session has somewhere obvious to post.
    let me = call(&market, "whoami", json!({})).await;
    assert_eq!(me["default_channel"], "market-data");

    let posted = call(&market, "post_message", json!({"body": "feed is wired"})).await;
    assert_eq!(posted["message"]["channel"], "market-data");

    // An explicit channel always wins.
    let elsewhere = call(
        &market,
        "post_message",
        json!({"channel": "core-manager", "body": "fyi"}),
    )
    .await;
    assert_eq!(elsewhere["message"]["channel"], "core-manager");
    // And any channel of the team stays readable.
    let read = call(
        &market,
        "read_messages",
        json!({"scope": "core-manager", "only_new": false}),
    )
    .await;
    assert_eq!(read["messages"][0]["body"], "fyi");

    // The shared session keeps the old contract exactly: no default, and the
    // original error text.
    let shared = connect(&h.base, &token).await;
    let shared_me = call(&shared, "whoami", json!({})).await;
    assert_eq!(shared_me["default_channel"], Value::Null);
    let err = call_expect_error(&shared, "post_message", json!({"body": "hello"})).await;
    assert!(err.contains("set `channel`"), "{err}");

    // The digest follows the same focus: this window's repository by default,
    // the whole team on request.
    let focused = call(&market, "team_digest", json!({"hours": 1})).await;
    let names: Vec<&str> = focused["channels"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(names, ["market-data"], "the session's own channel only");
    let wide = call(
        &market,
        "team_digest",
        json!({"hours": 1, "all_channels": true}),
    )
    .await;
    assert_eq!(wide["channels"].as_array().unwrap().len(), 2);

    // Noise from another repository must not wake this window.
    let waiter = tokio::spawn(async move {
        let r = call(
            &market,
            "wait_for_updates",
            json!({"timeout_seconds": 6, "kinds": ["message"]}),
        )
        .await;
        let _ = market.cancel().await;
        r
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    call(
        &dani_client,
        "post_message",
        json!({"channel": "core-manager", "body": "unrelated work"}),
    )
    .await;
    let woke = waiter.await.unwrap();
    assert_eq!(
        woke["timed_out"], true,
        "another repository's channel must not wake this session: {woke}"
    );

    // ...but the digest can still be asked for the whole team.
    let focused = call(&shared, "team_digest", json!({"hours": 1})).await;
    let names: Vec<&str> = focused["channels"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap_or_default())
        .collect();
    assert!(names.contains(&"core-manager"), "{names:?}");

    for client in [shared, dani_client] {
        let _ = client.cancel().await;
    }
    h.shutdown().await;
}

#[tokio::test]
async fn an_announcement_reaches_a_session_focused_elsewhere() {
    let h = require_db!("t_announce");
    let token = seed_agent(&h.pool, "layerv", "joaquin").await;
    let dani = seed_agent(&h.pool, "layerv", "dani").await;

    let market = connect_with_session(&h.base, &token, "market-data").await;
    let dani_client = connect(&h.base, &dani).await;

    call(&market, "create_channel", json!({"name": "market-data"})).await;
    call(&market, "create_channel", json!({"name": "general"})).await;

    // An ordinary message in another channel must still be ignored — the
    // announcement must not become a hole in the focus rule.
    let quiet = tokio::spawn({
        let market = connect_with_session(&h.base, &token, "market-data").await;
        async move {
            let r = call(
                &market,
                "wait_for_updates",
                json!({"timeout_seconds": 6, "kinds": ["message"]}),
            )
            .await;
            let _ = market.cancel().await;
            r
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    call(
        &dani_client,
        "post_message",
        json!({"channel": "general", "body": "lunch?"}),
    )
    .await;
    assert_eq!(
        quiet.await.unwrap()["timed_out"],
        true,
        "routine chatter elsewhere must still not wake a focused session"
    );

    // The same channel, flagged: this one gets through.
    let waiting = tokio::spawn({
        let market = connect_with_session(&h.base, &token, "market-data").await;
        async move {
            let r = call(
                &market,
                "wait_for_updates",
                json!({"timeout_seconds": 10, "kinds": ["message"]}),
            )
            .await;
            let _ = market.cancel().await;
            r
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let posted = call(
        &dani_client,
        "post_message",
        json!({"channel": "general", "announce": true,
               "body": "migration 0010 lands in 5 min, stop pushing"}),
    )
    .await;
    assert_eq!(posted["message"]["announce"], true);

    let woke = waiting.await.unwrap();
    assert_eq!(
        woke["woke"], true,
        "an announcement must reach a session focused elsewhere: {woke}"
    );

    // One message in one place — not a copy per channel — so replies work.
    let id = posted["message"]["id"].as_i64().unwrap();
    let seen = call(
        &market,
        "read_messages",
        json!({"scope": "general", "only_new": false}),
    )
    .await;
    let hits = seen["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|m| m["id"].as_i64() == Some(id))
        .count();
    assert_eq!(hits, 1, "an announcement is one message, not a copy each");

    // A focused digest carries it too: a catch-up that omits the migration
    // notice is the same failure one step later.
    let digest = call(&market, "team_digest", json!({"hours": 1})).await;
    let bodies: Vec<String> = digest["channels"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|c| c["last_messages"].as_array().cloned().unwrap_or_default())
        .map(|m| m["body"].as_str().unwrap_or_default().to_owned())
        .collect();
    assert!(
        bodies.iter().any(|b| b.contains("stop pushing")),
        "the focused digest must still list announcements: {bodies:?}"
    );

    // The flag is refused on a direct message, which already arrives unfiltered.
    let err = call_expect_error(
        &dani_client,
        "post_message",
        json!({"to": "joaquin", "announce": true, "body": "psst"}),
    )
    .await;
    assert!(err.contains("channel messages"), "{err}");

    for client in [market, dani_client] {
        let _ = client.cancel().await;
    }
    h.shutdown().await;
}

#[tokio::test]
async fn your_own_general_session_can_announce_to_your_other_windows() {
    let h = require_db!("t_announce_self");
    let token = seed_agent(&h.pool, "layerv", "joaquin").await;

    let general = connect_with_session(&h.base, &token, "general").await;
    let market = connect_with_session(&h.base, &token, "market-data").await;
    call(&general, "create_channel", json!({"name": "general"})).await;
    call(&general, "create_channel", json!({"name": "market-data"})).await;

    // Live wake: the coordinating window announces, the repository window is
    // blocked. Same token, so an agent-level "your own messages" guard would
    // discard it and this would time out.
    let waiting = tokio::spawn({
        let market = connect_with_session(&h.base, &token, "market-data").await;
        async move {
            let r = call(
                &market,
                "wait_for_updates",
                json!({"timeout_seconds": 10, "kinds": ["message"]}),
            )
            .await;
            let _ = market.cancel().await;
            r
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    call(
        &general,
        "post_message",
        json!({"channel": "general", "announce": true,
               "body": "0.6.0 goes out in 10, freeze your branches"}),
    )
    .await;
    let woke = waiting.await.unwrap();
    assert_eq!(
        woke["woke"], true,
        "your own general window must be able to reach your other windows: {woke}"
    );

    // Pre-check: the same must be true of the backlog path, which answers
    // before subscribing. Reporting nothing pending here would make the wait
    // look like a hang.
    let pending = call(
        &market,
        "wait_for_updates",
        json!({"timeout_seconds": 5, "kinds": ["message"]}),
    )
    .await;
    assert_eq!(
        pending["woke"], true,
        "the announcement is already waiting for this window: {pending}"
    );

    // The window that sent it is still not woken by its own announcement.
    let quiet = call(
        &general,
        "wait_for_updates",
        json!({"timeout_seconds": 5, "kinds": ["message"]}),
    )
    .await;
    assert_eq!(
        quiet["timed_out"], true,
        "the sending window must not wake on its own message: {quiet}"
    );

    for client in [general, market] {
        let _ = client.cancel().await;
    }
    h.shutdown().await;
}

#[tokio::test]
async fn an_empty_activity_clears_it_and_dead_rows_are_swept() {
    let h = require_db!("t_presence_hygiene");
    let token = seed_agent(&h.pool, "layerv", "joaquin").await;
    let market = connect_with_session(&h.base, &token, "market-data").await;

    call(
        &market,
        "heartbeat",
        json!({"repo": "Layer-V/market-data", "activity": "rewriting the feed"}),
    )
    .await;

    // Omitting the field keeps it: a mid-session ping from the hook must not
    // wipe what the model announced.
    let kept = call(&market, "heartbeat", json!({"branch": "main"})).await;
    assert_eq!(kept["activity"], "rewriting the feed");
    assert_eq!(kept["branch"], "main");

    // An explicit empty string clears it. Without this a session that starts
    // again carries the previous run's line forever, because nothing else ever
    // overwrites an omitted field.
    let cleared = call(&market, "heartbeat", json!({"activity": ""})).await;
    assert_eq!(
        cleared["activity"],
        Value::Null,
        "an empty activity must clear, not store an empty string: {cleared}"
    );
    assert_eq!(
        cleared["repo"], "Layer-V/market-data",
        "clearing the activity must not disturb the other fields"
    );

    // The first heartbeat of a *new* session, which is the path SessionStart
    // actually takes: the clear ran only on conflict, so a fresh row stored an
    // empty string where the update path stored null. The original test only
    // covered clear-after-set and could never have caught it.
    let fresh = connect_with_session(&h.base, &token, "brand-new").await;
    let first = call(&fresh, "heartbeat", json!({"activity": ""})).await;
    assert_eq!(
        first["activity"],
        Value::Null,
        "a new session's first heartbeat must clear, not store '': {first}"
    );
    let _ = fresh.cancel().await;

    // A row from a session that is long gone is swept on the next heartbeat.
    // Nothing else ever deleted one, and a row per distinct label grows without
    // limit once sessions exist.
    sqlx::query(sqlx::AssertSqlSafe(
        "INSERT INTO agent_presence (agent_id, session, status, activity, updated_at, expires_at)
         SELECT id, 'gone', 'active', 'stopping for the day', now() - interval '3 days',
                now() - interval '3 days'
           FROM agents WHERE name = 'joaquin'"
            .to_owned(),
    ))
    .execute(&h.pool)
    .await
    .unwrap();

    let before: (i64,) =
        sqlx::query_as("SELECT count(*) FROM agent_presence WHERE session = 'gone'")
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert_eq!(before.0, 1);

    call(&market, "heartbeat", json!({})).await;

    let after: (i64,) =
        sqlx::query_as("SELECT count(*) FROM agent_presence WHERE session = 'gone'")
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert_eq!(after.0, 0, "a long-dead session row must not live forever");

    // A row that only just expired is kept, so "offline recently" still reads.
    sqlx::query(sqlx::AssertSqlSafe(
        "INSERT INTO agent_presence (agent_id, session, status, updated_at, expires_at)
         SELECT id, 'recent', 'active', now(), now() - interval '1 minute'
           FROM agents WHERE name = 'joaquin'"
            .to_owned(),
    ))
    .execute(&h.pool)
    .await
    .unwrap();
    call(&market, "heartbeat", json!({})).await;
    let recent: (i64,) =
        sqlx::query_as("SELECT count(*) FROM agent_presence WHERE session = 'recent'")
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert_eq!(recent.0, 1, "a just-expired session is still worth showing");

    let _ = market.cancel().await;
    h.shutdown().await;
}

#[tokio::test]
async fn the_summary_projects_a_named_session_over_the_shared_row() {
    let h = require_db!("t_projection");
    let token = seed_agent(&h.pool, "layerv", "dani").await;
    let reader = seed_agent(&h.pool, "layerv", "joaquin").await;

    let shared = connect(&h.base, &token).await;
    let repo = connect_with_session(&h.base, &token, "risk-engine").await;
    let joaquin = connect(&h.base, &reader).await;

    // The shape dani hit: a sessionless row carrying an old activity that
    // keeps refreshing, alongside a real session doing real work.
    call(
        &shared,
        "heartbeat",
        json!({"repo": "Layer-V/old", "activity": "stopping for the day"}),
    )
    .await;
    call(
        &repo,
        "heartbeat",
        json!({"repo": "Layer-V/risk-engine", "activity": "implementing #169"}),
    )
    .await;
    // Refresh the sessionless row last, so "most recently updated" would pick it.
    call(&shared, "heartbeat", json!({})).await;

    let seen = call(&joaquin, "list_agents", json!({})).await;
    let dani = seen["agents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["name"] == "dani")
        .expect("dani is on the bus");

    assert_eq!(
        dani["activity"], "implementing #169",
        "the summary must project the named session, not the shared row: {dani}"
    );
    assert_eq!(dani["repo"], "Layer-V/risk-engine");
    assert_eq!(dani["session"], "risk-engine");
    // Nothing is hidden: both rows are still listed underneath.
    assert_eq!(dani["sessions"].as_array().unwrap().len(), 2);

    // team_digest reads presence too, and every session reads the digest at
    // start-up — the same wrong row there tells the whole team a stale line.
    let digest = call(&joaquin, "team_digest", json!({"hours": 1})).await;
    let line = digest["agents_seen"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["name"] == "dani")
        .expect("dani in the digest")["activity"]
        .clone();
    assert_eq!(line, "implementing #169", "{digest}");

    // With no named session at all, the shared row is still the answer rather
    // than nothing.
    let solo = seed_agent(&h.pool, "layerv", "carlos").await;
    let carlos = connect(&h.base, &solo).await;
    call(&carlos, "heartbeat", json!({"activity": "triaging"})).await;
    let seen = call(&joaquin, "list_agents", json!({})).await;
    let row = seen["agents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["name"] == "carlos")
        .unwrap();
    assert_eq!(row["activity"], "triaging");

    for client in [shared, repo, joaquin, carlos] {
        let _ = client.cancel().await;
    }
    h.shutdown().await;
}

#[tokio::test]
async fn the_sweeper_clears_long_dead_shared_rows() {
    let mut h = require_db!("t_presence_sweep");
    let _dani = seed_agent(&h.pool, "layerv", "dani").await;
    let _joaquin = seed_agent(&h.pool, "layerv", "joaquin").await;
    let reader = seed_agent(&h.pool, "layerv", "carlos").await;

    // The 0.6.0 shape: a shared-session row whose owner never heartbeats
    // again, so the lazy per-heartbeat sweep never reaches it. Inserted raw,
    // like a row surviving an upgrade.
    sqlx::query(sqlx::AssertSqlSafe(
        "INSERT INTO agent_presence (agent_id, session, status, activity, updated_at, expires_at)
         SELECT id, '', 'active', 'stopping for the day', now() - interval '3 days',
                now() - interval '3 days'
           FROM agents WHERE name = 'dani'"
            .to_owned(),
    ))
    .execute(&h.pool)
    .await
    .unwrap();
    // A shared row that only just expired must survive: same "offline
    // recently" grace as the heartbeat sweep.
    sqlx::query(sqlx::AssertSqlSafe(
        "INSERT INTO agent_presence (agent_id, session, status, activity, updated_at, expires_at)
         SELECT id, '', 'active', 'still warm', now(), now() - interval '1 minute'
           FROM agents WHERE name = 'joaquin'"
            .to_owned(),
    ))
    .execute(&h.pool)
    .await
    .unwrap();

    // A fresh server on the same schema is what a deploy is; its sweeper's
    // first pass runs immediately. The pass is asynchronous, so poll.
    let _replica = h.add_replica().await;
    let mut swept = false;
    for _ in 0..50 {
        let left: (i64,) = sqlx::query_as(
            "SELECT count(*) FROM agent_presence WHERE session = '' AND activity = 'stopping for the day'",
        )
        .fetch_one(&h.pool)
        .await
        .unwrap();
        if left.0 == 0 {
            swept = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(
        swept,
        "the long-dead shared row must be gone after a restart"
    );

    let warm: (i64,) =
        sqlx::query_as("SELECT count(*) FROM agent_presence WHERE activity = 'still warm'")
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert_eq!(
        warm.0, 1,
        "a recently expired shared row is still worth showing"
    );

    // What the team actually reads no longer carries the stale line.
    let carlos = connect(&h.base, &reader).await;
    let seen = call(&carlos, "list_agents", json!({})).await;
    let dani = seen["agents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["name"] == "dani")
        .expect("dani is still on the roster");
    assert_eq!(
        dani["activity"],
        Value::Null,
        "the swept row must not project anywhere: {dani}"
    );
    let digest = call(&carlos, "team_digest", json!({"hours": 24})).await;
    assert!(
        !digest.to_string().contains("stopping for the day"),
        "the digest must not resurrect the swept row: {digest}"
    );

    let _ = carlos.cancel().await;
    h.shutdown().await;
}

#[tokio::test]
async fn a_stringified_metadata_object_is_stored_as_an_object() {
    let h = require_db!("t_metadata_shape");
    let a = seed_agent(&h.pool, "layerv", "joaquin").await;
    let b = seed_agent(&h.pool, "layerv", "dani").await;
    let joaquin = connect(&h.base, &a).await;
    let dani = connect(&h.base, &b).await;

    // What some MCP clients actually send: the object serialised. Stored
    // verbatim it is unusable — the Stop drain reads metadata["question"] and
    // finds a string, so the capability the skill documents does not work.
    let sent = call(
        &joaquin,
        "post_message",
        json!({"to": "dani", "body": "is it green?",
               "metadata": "{\"question\": true}"}),
    )
    .await;
    assert_eq!(
        sent["message"]["metadata"]["question"], true,
        "a serialised object must be reconstructed: {}",
        sent["message"]["metadata"]
    );

    // Deliberately narrow: a string that is not an object is what the caller
    // asked for, and rewriting it would be guessing.
    let plain = call(
        &joaquin,
        "post_message",
        json!({"to": "dani", "body": "fyi", "metadata": "just a note"}),
    )
    .await;
    assert_eq!(plain["message"]["metadata"], "just a note");

    // An object still arrives as an object, which was never broken.
    let obj = call(
        &joaquin,
        "post_message",
        json!({"to": "dani", "body": "q", "metadata": {"question": true}}),
    )
    .await;
    assert_eq!(obj["message"]["metadata"]["question"], true);

    // Same normalisation on tasks, which take metadata too.
    let task = call(
        &joaquin,
        "create_task",
        json!({"key": "market-data#7", "title": "wire the feed",
               "metadata": "{\"epic\": \"feeds\"}"}),
    )
    .await;
    assert_eq!(task["metadata"]["epic"], "feeds");

    for client in [joaquin, dani] {
        let _ = client.cancel().await;
    }
    h.shutdown().await;
}

// -------------------------------------------------- administrative credentials --

/// The operator CLI path (bootstrap, agent add, token issue) through the store,
/// exactly as `ai-crew-sync admin bootstrap` / `agent add` / `token issue` run
/// it: credentials resolve, revocation is immediate, the two credential
/// classes never resolve as each other, and the audit trail carries no secret.
#[tokio::test]
async fn administrative_credentials_are_a_separate_class_with_an_audit_trail() {
    use ai_crew_sync::store::admin::{self as store, Actor};

    let h = require_db!("t_admin_store");

    // Bootstrap: a global credential minted with no prior credential.
    let global = store::grant_admin(&h.pool, Actor::Cli, None, Some("laptop".into()))
        .await
        .unwrap();
    assert!(global.token.starts_with("acsa_"), "admin prefix");
    assert!(global.team.is_none(), "bootstrap mints a global credential");
    let ctx = store::resolve_admin(&h.pool, &global.token)
        .await
        .unwrap()
        .expect("fresh credential resolves");
    assert!(ctx.is_global());
    assert_eq!(ctx.id, global.id);

    // The existing operator commands keep working and mint tokens that
    // authenticate on /mcp as exactly the requested agent and team.
    let team = store::create_team(&h.pool, Actor::Cli, "acme", Some("Acme".into()))
        .await
        .unwrap();
    let again = store::create_team(&h.pool, Actor::Cli, "ACME", None)
        .await
        .unwrap();
    assert_eq!(again.id, team.id, "create is idempotent on the slug");
    assert_eq!(again.name, "Acme", "a repeat never renames");
    store::create_agent(&h.pool, Actor::Cli, team.id, "Backend", None)
        .await
        .unwrap();
    // A repeat on an active agent is a no-op and logs nothing; a disable and
    // a re-create are real transitions and log one row each.
    store::create_agent(
        &h.pool,
        Actor::Cli,
        team.id,
        "backend",
        Some("Backend".into()),
    )
    .await
    .unwrap();
    store::disable_agent(&h.pool, Actor::Cli, team.id, "backend")
        .await
        .unwrap();
    store::create_agent(&h.pool, Actor::Cli, team.id, "backend", None)
        .await
        .unwrap();
    assert!(
        store::create_team(&h.pool, Actor::Cli, "evil", Some("Acme\x1b[31m".into()))
            .await
            .is_err(),
        "a team name is display text: no control characters"
    );
    let issued = store::issue_token(
        &h.pool,
        Actor::Cli,
        team.id,
        "backend",
        Some("sesion backend".into()),
    )
    .await
    .unwrap();
    assert_eq!(
        (issued.agent.as_str(), issued.team.as_str()),
        ("backend", "acme")
    );
    let client = connect(&h.base, &issued.token).await;
    let me = call(&client, "whoami", json!({})).await;
    assert_eq!(me["agent"], "backend");
    assert_eq!(me["team"], "acme");
    let _ = client.cancel().await;

    // A team credential lists only its team; the global listing sees both.
    let team_admin = store::grant_admin(&h.pool, Actor::Admin(global.id), Some(team.id), None)
        .await
        .unwrap();
    assert_eq!(team_admin.team.as_deref(), Some("acme"));
    let mine = store::list_admins(&h.pool, Some(team.id)).await.unwrap();
    assert_eq!(
        mine.iter().map(|c| c.id).collect::<Vec<_>>(),
        vec![team_admin.id]
    );
    assert_eq!(store::list_admins(&h.pool, None).await.unwrap().len(), 2);

    // Neither class resolves as the other: same hashing, different tables.
    assert!(
        store::resolve_admin(&h.pool, &issued.token)
            .await
            .unwrap()
            .is_none(),
        "an agent token is not an administrative credential"
    );
    assert!(
        ai_crew_sync::auth::resolve_token(&h.pool, &global.token)
            .await
            .is_err(),
        "an administrative credential is not an agent token"
    );

    // Scoped revocation: a team scope cannot reach a global credential or a
    // token from another team, and reports them as not found.
    let other = store::create_team(&h.pool, Actor::Cli, "other", None)
        .await
        .unwrap();
    store::create_agent(&h.pool, Actor::Cli, other.id, "x", None)
        .await
        .unwrap();
    let foreign = store::issue_token(&h.pool, Actor::Cli, other.id, "x", None)
        .await
        .unwrap();
    assert!(matches!(
        store::revoke_token(
            &h.pool,
            Actor::Admin(team_admin.id),
            Some(team.id),
            foreign.id
        )
        .await,
        Err(ai_crew_sync::error::BusError::NotFound(_))
    ));
    assert!(matches!(
        store::revoke_admin(
            &h.pool,
            Actor::Admin(team_admin.id),
            Some(team.id),
            global.id
        )
        .await,
        Err(ai_crew_sync::error::BusError::NotFound(_))
    ));
    assert!(
        store::resolve_admin(&h.pool, &global.token)
            .await
            .unwrap()
            .is_some(),
        "a failed scoped revoke changes nothing"
    );

    // Revocation is immediate for both classes, and idempotent.
    store::revoke_token(
        &h.pool,
        Actor::Admin(team_admin.id),
        Some(team.id),
        issued.id,
    )
    .await
    .unwrap();
    store::revoke_token(
        &h.pool,
        Actor::Admin(team_admin.id),
        Some(team.id),
        issued.id,
    )
    .await
    .unwrap();
    assert!(
        ai_crew_sync::auth::resolve_token(&h.pool, &issued.token)
            .await
            .is_err()
    );
    store::revoke_admin(&h.pool, Actor::Cli, None, global.id)
        .await
        .unwrap();
    assert!(
        store::resolve_admin(&h.pool, &global.token)
            .await
            .unwrap()
            .is_none()
    );

    // Audit: every mutation logged with its actor, and no secret anywhere.
    let rows: Vec<(String, Option<Uuid>, String, Value)> = sqlx::query_as(
        "SELECT actor_source, actor_admin_id, action, detail FROM admin_audit ORDER BY id",
    )
    .fetch_all(&h.pool)
    .await
    .unwrap();
    let actions: Vec<&str> = rows.iter().map(|r| r.2.as_str()).collect();
    assert_eq!(
        actions,
        vec![
            "admin.grant",
            "team.create",
            "agent.create",
            "agent.disable",
            "agent.enable",
            "token.issue",
            "admin.grant",
            "team.create",
            "agent.create",
            "token.issue",
            "token.revoke",
            "admin.revoke",
        ],
        "one row per real transition, none for the idempotent repeats"
    );
    let by_http: Vec<&(String, Option<Uuid>, String, Value)> =
        rows.iter().filter(|r| r.0 == "http").collect();
    assert_eq!(by_http.len(), 2, "the two actions taken with a credential");
    assert!(
        by_http.iter().all(|r| r.1.is_some()),
        "http rows name their credential"
    );
    assert!(rows.iter().filter(|r| r.0 == "cli").all(|r| r.1.is_none()));
    let dump = serde_json::to_string(&rows.iter().map(|r| &r.3).collect::<Vec<_>>()).unwrap();
    for secret in [
        &global.token,
        &team_admin.token,
        &issued.token,
        &foreign.token,
    ] {
        assert!(
            !dump.contains(secret.as_str()),
            "audit detail carries a secret"
        );
        assert!(
            !dump.contains(&secret[5..]),
            "audit detail carries a secret's body"
        );
    }
    assert!(
        dump.contains(&issued.prefix),
        "the display prefix is what the log keeps"
    );

    h.shutdown().await;
}

/// Revocation is one transition however many callers race for it: the
/// UPDATE is conditional on the row being active, so exactly one caller
/// performs it and exactly one audit row is written; the others succeed as
/// no-ops.
#[tokio::test]
async fn concurrent_revocations_produce_one_transition_and_one_audit_row() {
    use ai_crew_sync::store::admin::{self as store, Actor};

    let h = require_db!("t_admin_revoke_race");
    let team = store::create_team(&h.pool, Actor::Cli, "acme", None)
        .await
        .unwrap();
    store::create_agent(&h.pool, Actor::Cli, team.id, "bot", None)
        .await
        .unwrap();
    let issued = store::issue_token(&h.pool, Actor::Cli, team.id, "bot", None)
        .await
        .unwrap();
    let admin = store::grant_admin(&h.pool, Actor::Cli, Some(team.id), None)
        .await
        .unwrap();

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let pool = h.pool.clone();
        let (tid, token_id, admin_id) = (team.id, issued.id, admin.id);
        tasks.push(tokio::spawn(async move {
            let a = store::revoke_token(&pool, Actor::Admin(admin_id), Some(tid), token_id).await;
            let b = store::revoke_admin(&pool, Actor::Admin(admin_id), Some(tid), admin_id).await;
            (a.is_ok(), b.is_ok())
        }));
    }
    for t in tasks {
        assert_eq!(t.await.unwrap(), (true, true), "every racer succeeds");
    }
    let (revokes,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM admin_audit WHERE action IN ('token.revoke', 'admin.revoke')",
    )
    .fetch_one(&h.pool)
    .await
    .unwrap();
    assert_eq!(revokes, 2, "one row per transition, not per caller");

    h.shutdown().await;
}

// ------------------------------------------------------- /admin HTTP surface --

/// Minimal client for `/admin/*`: a bearer, a method, a path, a JSON body.
struct Admin {
    http: reqwest::Client,
    base: String,
    token: String,
}

impl Admin {
    fn new(base: &str, token: &str) -> Self {
        Self {
            http: reqwest::Client::new(),
            base: base.to_owned(),
            token: token.to_owned(),
        }
    }

    async fn req(&self, method: reqwest::Method, path: &str, body: Option<Value>) -> (u16, Value) {
        let mut r = self
            .http
            .request(method, format!("{}/admin{path}", self.base))
            .header("Authorization", format!("Bearer {}", self.token));
        if let Some(body) = body {
            r = r.json(&body);
        }
        let resp = r.send().await.unwrap();
        let status = resp.status().as_u16();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        (status, body)
    }

    async fn get(&self, path: &str) -> (u16, Value) {
        self.req(reqwest::Method::GET, path, None).await
    }
    async fn post(&self, path: &str, body: Value) -> (u16, Value) {
        self.req(reqwest::Method::POST, path, Some(body)).await
    }
    async fn delete(&self, path: &str) -> (u16, Value) {
        self.req(reqwest::Method::DELETE, path, None).await
    }
}

/// A raw `tools/list` on `/mcp` with a bearer, for status checks only.
async fn mcp_status(base: &str, token: &str) -> u16 {
    reqwest::Client::new()
        .post(format!("{base}/mcp"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

#[tokio::test]
async fn admin_api_refuses_everything_but_an_administrative_credential() {
    let h = require_db!("t_admin_auth");
    let agent_token = seed_agent(&h.pool, "acme", "joaquin").await;

    // No bearer at all.
    let (status, body) = Admin::new(&h.base, "").get("/whoami").await;
    assert_eq!(status, 401);
    assert!(
        body["error"].as_str().unwrap().contains("bootstrap"),
        "{body}"
    );

    // An agent token: refused, and told what to use instead.
    let (status, body) = Admin::new(&h.base, &agent_token).get("/whoami").await;
    assert_eq!(status, 401, "an agent token never administers");
    assert!(
        body["error"].as_str().unwrap().contains("agent token"),
        "{body}"
    );
    // ...on every route, including the ones that mint.
    let agent = Admin::new(&h.base, &agent_token);
    assert_eq!(
        agent
            .post("/teams/acme/tokens", json!({"agent": "joaquin"}))
            .await
            .0,
        401,
        "an agent token cannot mint a token, not even for its own agent"
    );
    assert_eq!(agent.post("/credentials", json!({})).await.0, 401);

    // A well-formed but unknown credential.
    let (status, _) = Admin::new(&h.base, "acsa_deadbeef").get("/whoami").await;
    assert_eq!(status, 401);

    // And an administrative credential is not an agent token on /mcp.
    let global = ai_crew_sync::store::admin::grant_admin(
        &h.pool,
        ai_crew_sync::store::admin::Actor::Cli,
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(mcp_status(&h.base, &global.token).await, 401);
    let (status, body) = Admin::new(&h.base, &global.token).get("/whoami").await;
    assert_eq!(status, 200);
    assert_eq!(body["scope"], "global");
    assert!(body["team"].is_null());

    h.shutdown().await;
}

#[tokio::test]
async fn admin_api_global_credential_runs_the_whole_onboarding_remotely() {
    let h = require_db!("t_admin_global");
    let bootstrap = ai_crew_sync::store::admin::grant_admin(
        &h.pool,
        ai_crew_sync::store::admin::Actor::Cli,
        None,
        Some("laptop".into()),
    )
    .await
    .unwrap();
    let admin = Admin::new(&h.base, &bootstrap.token);

    let (status, body) = admin
        .post("/teams", json!({"slug": "RoundCrew", "name": "RoundCrew"}))
        .await;
    assert_eq!(status, 201, "{body}");
    assert_eq!(body["team"]["slug"], "roundcrew", "slugs are normalised");

    let (status, body) = admin
        .post(
            "/teams/roundcrew/agents",
            json!({"name": "backend", "display_name": "RoundCrew backend"}),
        )
        .await;
    assert_eq!(status, 201, "{body}");
    assert_eq!(body["agent"]["name"], "backend");

    let (status, body) = admin
        .post(
            "/teams/roundcrew/tokens",
            json!({"agent": "backend", "label": "sesion backend"}),
        )
        .await;
    assert_eq!(status, 201, "{body}");
    let token = body["token"]["token"].as_str().unwrap().to_owned();
    let token_id = body["token"]["id"].as_str().unwrap().to_owned();
    assert!(token.starts_with("acs_"));
    assert_eq!(body["token"]["agent"], "backend");
    assert_eq!(body["token"]["team"], "roundcrew");
    assert_eq!(body["token"]["label"], "sesion backend");

    // The property that matters: the token IS that agent in that team on /mcp.
    let client = connect(&h.base, &token).await;
    let me = call(&client, "whoami", json!({})).await;
    assert_eq!(me["agent"], "backend");
    assert_eq!(me["team"], "roundcrew");
    let _ = client.cancel().await;

    // Listings show it without the secret.
    let (_, body) = admin.get("/teams/roundcrew/tokens").await;
    let listed = &body["tokens"][0];
    assert_eq!(listed["id"], token_id);
    assert!(
        listed.get("token").is_none(),
        "a listing never carries a secret"
    );
    assert_eq!(listed["prefix"], &token[..12]);
    let (_, body) = admin.get("/teams").await;
    assert_eq!(body["teams"][0]["agents"], 1);

    // Unknown things are 404 with a hint, bad input 400.
    assert_eq!(
        admin
            .post("/teams/nope/agents", json!({"name": "x"}))
            .await
            .0,
        404
    );
    assert_eq!(
        admin
            .post("/teams/roundcrew/tokens", json!({"agent": "ghost"}))
            .await
            .0,
        404
    );
    let (status, body) = admin
        .post("/teams/roundcrew/agents", json!({"name": "has space"}))
        .await;
    assert_eq!(status, 400, "{body}");
    // Extractor rejections wear the same JSON shape as every other error.
    let resp = reqwest::Client::new()
        .post(format!("{}/admin/teams/roundcrew/agents", h.base))
        .header("Authorization", format!("Bearer {}", bootstrap.token))
        .header("Content-Type", "application/json")
        .body("{not json")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: Value = resp.json().await.expect("a JSON error body");
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("invalid JSON body"),
        "{body}"
    );
    let (status, body) = admin.delete("/teams/roundcrew/tokens/not-a-uuid").await;
    assert_eq!(status, 400, "{body}");
    assert!(
        body["error"].as_str().unwrap().contains("path parameter"),
        "{body}"
    );
    let (status, body) = admin.post("/credentials", json!({"team": ""})).await;
    assert_eq!(
        status, 400,
        "an empty team is a mistake, not a global grant: {body}"
    );

    // Revocation stops the token on /mcp immediately.
    let (status, _) = admin
        .delete(&format!("/teams/roundcrew/tokens/{token_id}"))
        .await;
    assert_eq!(status, 200);
    assert_eq!(mcp_status(&h.base, &token).await, 401);

    // Granting: a team credential, then a second global one; each works on
    // /admin/whoami with the right scope, and revoking cuts it off.
    let (status, body) = admin
        .post(
            "/credentials",
            json!({"team": "roundcrew", "label": "dani"}),
        )
        .await;
    assert_eq!(status, 201, "{body}");
    let team_cred = body["credential"]["token"].as_str().unwrap().to_owned();
    let team_cred_id = body["credential"]["id"].as_str().unwrap().to_owned();
    assert!(team_cred.starts_with("acsa_"));
    let (_, body) = Admin::new(&h.base, &team_cred).get("/whoami").await;
    assert_eq!(body["scope"], "team");
    assert_eq!(body["team"], "roundcrew");

    let (status, body) = admin.post("/credentials", json!({})).await;
    assert_eq!(status, 201, "{body}");
    assert!(body["credential"]["team"].is_null(), "no team means global");

    let (_, body) = admin.get("/credentials").await;
    assert_eq!(body["credentials"].as_array().unwrap().len(), 3);
    assert!(
        body["credentials"]
            .as_array()
            .unwrap()
            .iter()
            .all(|c| c.get("token").is_none()),
        "listings never carry a secret"
    );

    let (status, _) = admin.delete(&format!("/credentials/{team_cred_id}")).await;
    assert_eq!(status, 200);
    assert_eq!(
        Admin::new(&h.base, &team_cred).get("/whoami").await.0,
        401,
        "a revoked credential stops authorising at once"
    );

    // Audit: http rows name the credential that acted, and hold no secret.
    let rows: Vec<(String, Option<Uuid>, String, Value)> = sqlx::query_as(
        "SELECT actor_source, actor_admin_id, action, detail FROM admin_audit
         WHERE actor_source = 'http' ORDER BY id",
    )
    .fetch_all(&h.pool)
    .await
    .unwrap();
    let actions: Vec<&str> = rows.iter().map(|r| r.2.as_str()).collect();
    assert_eq!(
        actions,
        vec![
            "team.create",
            "agent.create",
            "token.issue",
            "token.revoke",
            "admin.grant",
            "admin.grant",
            "admin.revoke",
        ]
    );
    assert!(rows.iter().all(|r| r.1 == Some(bootstrap.id)));
    let dump = serde_json::to_string(&rows.iter().map(|r| &r.3).collect::<Vec<_>>()).unwrap();
    assert!(!dump.contains(&token[5..]));
    assert!(!dump.contains(&team_cred[5..]));

    h.shutdown().await;
}

#[tokio::test]
async fn admin_api_team_credential_cannot_leave_its_team_by_any_route() {
    use ai_crew_sync::store::admin::{self as store, Actor};

    let h = require_db!("t_admin_team");
    let acme = store::create_team(&h.pool, Actor::Cli, "acme", None)
        .await
        .unwrap();
    let other = store::create_team(&h.pool, Actor::Cli, "other", None)
        .await
        .unwrap();
    store::create_agent(&h.pool, Actor::Cli, acme.id, "joaquin", None)
        .await
        .unwrap();
    store::create_agent(&h.pool, Actor::Cli, other.id, "marta", None)
        .await
        .unwrap();
    let foreign = store::issue_token(&h.pool, Actor::Cli, other.id, "marta", None)
        .await
        .unwrap();
    let global = store::grant_admin(&h.pool, Actor::Cli, None, None)
        .await
        .unwrap();
    let scoped = store::grant_admin(&h.pool, Actor::Cli, Some(acme.id), None)
        .await
        .unwrap();
    let dani = Admin::new(&h.base, &scoped.token);

    // Inside its team: everything a global credential can do there.
    let (status, body) = dani
        .post("/teams/acme/agents", json!({"name": "dani-codex"}))
        .await;
    assert_eq!(status, 201, "{body}");
    let (status, body) = dani
        .post(
            "/teams/acme/tokens",
            json!({"agent": "dani-codex", "label": "x"}),
        )
        .await;
    assert_eq!(status, 201, "{body}");
    let mine = body["token"]["token"].as_str().unwrap().to_owned();
    let mine_id = body["token"]["id"].as_str().unwrap().to_owned();
    let client = connect(&h.base, &mine).await;
    let me = call(&client, "whoami", json!({})).await;
    assert_eq!(
        (me["agent"].as_str(), me["team"].as_str()),
        (Some("dani-codex"), Some("acme"))
    );
    let _ = client.cancel().await;

    // By slug: another team is forbidden, existing or not, same answer.
    for path in [
        "/teams/other/agents",
        "/teams/other/tokens",
        "/teams/nope/tokens",
    ] {
        let (status, body) = dani.get(path).await;
        assert_eq!(status, 403, "{path}: {body}");
        assert!(body["error"].as_str().unwrap().contains("'acme'"), "{body}");
    }
    assert_eq!(
        dani.post("/teams/other/tokens", json!({"agent": "marta"}))
            .await
            .0,
        403
    );
    // By UUID: a foreign token under its own team's path is simply not found.
    let (status, _) = dani
        .delete(&format!("/teams/acme/tokens/{}", foreign.id))
        .await;
    assert_eq!(status, 404);
    assert_eq!(
        mcp_status(&h.base, &foreign.token).await,
        200,
        "and untouched"
    );
    // Nor can a global credential revoke it through the wrong team's path.
    let (status, _) = Admin::new(&h.base, &global.token)
        .delete(&format!("/teams/acme/tokens/{}", foreign.id))
        .await;
    assert_eq!(status, 404);

    // Global-only actions: create teams, grant credentials.
    let (status, body) = dani.post("/teams", json!({"slug": "mine"})).await;
    assert_eq!(status, 403, "{body}");
    let (status, body) = dani.post("/credentials", json!({"team": "acme"})).await;
    assert_eq!(status, 403, "{body}");
    let (status, _) = dani.post("/credentials", json!({})).await;
    assert_eq!(status, 403, "nor a global one");
    assert_eq!(
        store::list_admins(&h.pool, None).await.unwrap().len(),
        2,
        "nothing was granted"
    );

    // The team roster it sees is exactly its own team.
    let (_, body) = dani.get("/teams").await;
    assert_eq!(body["teams"].as_array().unwrap().len(), 1);
    assert_eq!(body["teams"][0]["slug"], "acme");
    let (_, body) = dani.get("/credentials").await;
    assert_eq!(body["credentials"].as_array().unwrap().len(), 1);
    assert_eq!(body["credentials"][0]["id"], scoped.id.to_string());

    // It cannot revoke the global credential, and can revoke its own team's
    // tokens and — last — itself.
    let (status, _) = dani.delete(&format!("/credentials/{}", global.id)).await;
    assert_eq!(status, 404);
    assert_eq!(
        Admin::new(&h.base, &global.token).get("/whoami").await.0,
        200
    );
    assert_eq!(
        dani.delete(&format!("/teams/acme/tokens/{mine_id}"))
            .await
            .0,
        200
    );
    assert_eq!(mcp_status(&h.base, &mine).await, 401);
    assert_eq!(
        dani.delete(&format!("/credentials/{}", scoped.id)).await.0,
        200
    );
    assert_eq!(dani.get("/whoami").await.0, 401);

    h.shutdown().await;
}

#[tokio::test]
async fn admin_api_caps_active_tokens_per_agent() {
    use ai_crew_sync::store::admin::{self as store, Actor, MAX_ACTIVE_TOKENS_PER_AGENT};

    let h = require_db!("t_admin_cap");
    let acme = store::create_team(&h.pool, Actor::Cli, "acme", None)
        .await
        .unwrap();
    store::create_agent(&h.pool, Actor::Cli, acme.id, "bot", None)
        .await
        .unwrap();
    for _ in 0..MAX_ACTIVE_TOKENS_PER_AGENT {
        store::issue_token(&h.pool, Actor::Cli, acme.id, "bot", None)
            .await
            .unwrap();
    }
    let global = store::grant_admin(&h.pool, Actor::Cli, None, None)
        .await
        .unwrap();
    let admin = Admin::new(&h.base, &global.token);
    let (status, body) = admin
        .post("/teams/acme/tokens", json!({"agent": "bot"}))
        .await;
    assert_eq!(status, 409, "{body}");
    assert!(body["error"].as_str().unwrap().contains("Revoke"), "{body}");

    // Revoking one frees a slot.
    let (_, body) = admin.get("/teams/acme/tokens").await;
    let some_id = body["tokens"][0]["id"].as_str().unwrap().to_owned();
    assert_eq!(
        admin
            .delete(&format!("/teams/acme/tokens/{some_id}"))
            .await
            .0,
        200
    );
    assert_eq!(
        admin
            .post("/teams/acme/tokens", json!({"agent": "bot"}))
            .await
            .0,
        201
    );

    h.shutdown().await;
}

#[tokio::test]
async fn admin_api_has_its_own_lower_rate_limit() {
    use ai_crew_sync::store::admin::{self as store, Actor};

    // 200/min on MCP means 20/min on /admin, with a burst of 10.
    let Some(h) = setup_rate_limited("t_admin_rl", 200).await else {
        assert!(!db_required());
        return;
    };
    let global = store::grant_admin(&h.pool, Actor::Cli, None, None)
        .await
        .unwrap();
    let admin = Admin::new(&h.base, &global.token);
    let mut throttled = None;
    for _ in 0..30 {
        let (status, body) = admin.get("/whoami").await;
        if status == 429 {
            throttled = Some(body);
            break;
        }
    }
    let body = throttled.expect("the admin bucket must run out well before the MCP one would");
    assert!(
        body["error"].as_str().unwrap().contains("retry in"),
        "{body}"
    );

    h.shutdown().await;
}

// ------------------------------------------------------- admin remote CLI --

/// The remote CLI's library functions against the real server and a
/// temporary configuration directory: the full onboarding flow with
/// verification and `--save`, and the failure paths that must leave no
/// trace.
#[tokio::test]
async fn admin_cli_runs_the_remote_flow_end_to_end() {
    use ai_crew_sync::admin_cli::{self, SaveTarget};
    use ai_crew_sync::store::admin::{self as store, Actor};

    let h = require_db!("t_admin_cli");
    let dir = std::env::temp_dir().join(format!("acs-admin-cli-{}", Uuid::new_v4()));
    let bootstrap = store::grant_admin(&h.pool, Actor::Cli, None, None)
        .await
        .unwrap();

    // A bad credential is refused and nothing is written.
    let err = admin_cli::login(&dir, &h.base, "acsa_nope".into())
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("nothing was saved"), "{err:#}");
    assert!(!dir.join("admin").exists());

    // Login verifies, then persists with 0600; the URL is normalised.
    let (me, path) = admin_cli::login(&dir, &format!("{}/mcp", h.base), bootstrap.token.clone())
        .await
        .unwrap();
    assert_eq!(me["scope"], "global");
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(text.contains(&format!("url={}\n", h.base)), "{text}");
    assert!(text.contains(&format!("token={}\n", bootstrap.token)));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    let cfg = admin_cli::load_config(&dir).unwrap();
    assert_eq!(cfg.url, h.base);
    let api = admin_cli::Api::new(cfg);

    // Team, agent, token — and the token is verified before anything else.
    api.create_team("roundcrew", Some("RoundCrew"))
        .await
        .unwrap();
    api.create_agent("roundcrew", "backend", None)
        .await
        .unwrap();

    // The file already has hand-written entries that must survive.
    let tokens = admin_cli::tokens_file(&dir, "roundcrew");
    std::fs::write(
        &tokens,
        "# roundcrew tokens\n_base=acs_base_keep\nweb=acs_web_keep\n",
    )
    .unwrap();
    let issued = api
        .issue_token("roundcrew", "backend", Some("sesion backend"))
        .await
        .unwrap();
    let saved = admin_cli::finish_issue(
        &api,
        &issued,
        "backend",
        "roundcrew",
        Some(&SaveTarget {
            dir: dir.clone(),
            repo: "backend".into(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(saved.as_deref(), Some(tokens.as_path()));
    let text = std::fs::read_to_string(&tokens).unwrap();
    assert_eq!(
        text,
        format!(
            "# roundcrew tokens\n_base=acs_base_keep\nweb=acs_web_keep\nbackend={}\n",
            issued.token
        ),
        "only the backend= line is added; _base and the rest are untouched"
    );
    // The saved token is that agent on /mcp.
    let client = connect(&h.base, &issued.token).await;
    let me = call(&client, "whoami", json!({})).await;
    assert_eq!(
        (me["agent"].as_str(), me["team"].as_str()),
        (Some("backend"), Some("roundcrew"))
    );
    let _ = client.cancel().await;

    // Re-issuing for the same repo replaces the line and does NOT revoke the
    // previous token.
    let second = api.issue_token("roundcrew", "backend", None).await.unwrap();
    admin_cli::finish_issue(
        &api,
        &second,
        "backend",
        "roundcrew",
        Some(&SaveTarget {
            dir: dir.clone(),
            repo: "backend".into(),
        }),
    )
    .await
    .unwrap();
    let text = std::fs::read_to_string(&tokens).unwrap();
    assert!(text.contains(&format!("backend={}\n", second.token)));
    assert!(!text.contains(&issued.token), "one line per repo");
    assert!(text.starts_with("# roundcrew tokens\n_base=acs_base_keep\n"));
    assert_eq!(
        mcp_status(&h.base, &issued.token).await,
        200,
        "the old token still works"
    );

    // Verification failure: a token that authenticates as someone else is
    // revoked, and the file is left exactly as it was.
    api.create_agent("roundcrew", "web", None).await.unwrap();
    let other = api.issue_token("roundcrew", "web", None).await.unwrap();
    let before = std::fs::read_to_string(&tokens).unwrap();
    let err = admin_cli::finish_issue(
        &api,
        &other,
        "backend",
        "roundcrew",
        Some(&SaveTarget {
            dir: dir.clone(),
            repo: "backend".into(),
        }),
    )
    .await
    .unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("web@roundcrew, not backend@roundcrew"),
        "{msg}"
    );
    assert!(msg.contains("has been revoked"), "{msg}");
    assert_eq!(
        std::fs::read_to_string(&tokens).unwrap(),
        before,
        "file untouched"
    );
    assert_eq!(
        mcp_status(&h.base, &other.token).await,
        401,
        "the mismatched token is dead"
    );

    // A save that cannot be written revokes the verified token too: a token
    // that was never printed and never saved must not stay active.
    let unwritable = dir.join("blocked");
    std::fs::write(&unwritable, "not a directory").unwrap();
    let doomed = api.issue_token("roundcrew", "backend", None).await.unwrap();
    let err = admin_cli::finish_issue(
        &api,
        &doomed,
        "backend",
        "roundcrew",
        Some(&SaveTarget {
            dir: unwritable.clone(),
            repo: "backend".into(),
        }),
    )
    .await
    .unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("could not be saved"), "{msg}");
    assert!(msg.contains("has been revoked"), "{msg}");
    assert_eq!(mcp_status(&h.base, &doomed.token).await, 401);

    // Revoke through the CLI stops the token on /mcp.
    api.revoke_token("roundcrew", second.id).await.unwrap();
    assert_eq!(mcp_status(&h.base, &second.token).await, 401);

    // A team credential logged in on this machine is confined to its team.
    let team_dir = dir.join("dani");
    let granted = api
        .grant_credential(Some("roundcrew"), Some("dani"))
        .await
        .unwrap();
    let team_token = granted["credential"]["token"].as_str().unwrap().to_owned();
    let (me, _) = admin_cli::login(&team_dir, &h.base, team_token)
        .await
        .unwrap();
    assert_eq!(me["team"], "roundcrew");
    let dani = admin_cli::Api::new(admin_cli::load_config(&team_dir).unwrap());
    dani.create_agent("roundcrew", "docs", None).await.unwrap();
    let err = dani.create_agent("other", "docs", None).await.unwrap_err();
    assert!(err.to_string().contains("403"), "{err}");
    assert!(err.to_string().contains("'roundcrew'"), "{err}");
    let err = dani.create_team("mine", None).await.unwrap_err();
    assert!(err.to_string().contains("403"), "{err}");

    // Logout forgets the file and nothing else.
    assert!(admin_cli::remove_config(&team_dir).unwrap());
    assert!(!admin_cli::remove_config(&team_dir).unwrap());
    assert!(admin_cli::load_config(&team_dir).is_err());
    assert!(tokens.exists(), "token files are not login state");

    let _ = std::fs::remove_dir_all(&dir);
    h.shutdown().await;
}

// ------------------------------------------------------------ local context --

/// The resolver against a real bus: a profile plus a token file yields a
/// verified identity with no BUS_TOKEN anywhere; the wrong token behind the
/// right profile, and a revoked one, are refused with a reason.
#[tokio::test]
async fn local_profiles_resolve_and_verify_against_the_bus() {
    use ai_crew_sync::context::{self, Inputs, Profile, Profiles, Source};
    use ai_crew_sync::store::admin::{self as store, Actor};

    let h = require_db!("t_context");
    let dir = std::env::temp_dir().join(format!("acs-context-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();

    let acme = store::create_team(&h.pool, Actor::Cli, "acme", None)
        .await
        .unwrap();
    store::create_agent(&h.pool, Actor::Cli, acme.id, "joaquin", None)
        .await
        .unwrap();
    store::create_agent(&h.pool, Actor::Cli, acme.id, "marta", None)
        .await
        .unwrap();
    let mine = store::issue_token(&h.pool, Actor::Cli, acme.id, "joaquin", None)
        .await
        .unwrap();
    let hers = store::issue_token(&h.pool, Actor::Cli, acme.id, "marta", None)
        .await
        .unwrap();

    context::save_profiles(
        &dir,
        &Profiles {
            default: None,
            profiles: std::collections::BTreeMap::from([(
                "acme".to_owned(),
                Profile {
                    url: h.base.clone(),
                    team: "acme".into(),
                    agent: "joaquin".into(),
                    tokens: "tokens-acme".into(),
                    key: None,
                },
            )]),
        },
    )
    .unwrap();
    std::fs::write(
        dir.join("tokens-acme"),
        format!("_base={}\nstolen={}\n", mine.token, hers.token),
    )
    .unwrap();
    let repo = dir.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(
        repo.join(context::PROJECT_FILE),
        "profile = \"acme\"\nproject = \"api\"\nchannel = \"api\"\n",
    )
    .unwrap();

    // Two windows in the same repository, no environment: both resolve the
    // project's profile, and each may pick a different entry without
    // touching the file the other reads.
    let base = Inputs {
        config_dir: dir.clone(),
        project_dir: Some(repo.clone()),
        ..Default::default()
    };
    let r = context::resolve(&base).unwrap();
    assert_eq!(r.source, Source::ProjectDefault);
    assert_eq!(
        r.token_key.as_deref(),
        Some("_base"),
        "no 'api' entry, so _base"
    );
    assert_eq!(r.project.as_deref(), Some("api"));
    let v = context::verify(&r).await.unwrap();
    assert_eq!((v.agent.as_str(), v.team.as_str()), ("joaquin", "acme"));

    // The right profile over someone else's token: refused, with the entry
    // named, and the project file untouched.
    let before = std::fs::read_to_string(repo.join(context::PROJECT_FILE)).unwrap();
    std::fs::write(
        repo.join(context::PROJECT_FILE),
        "profile = \"acme\"\nproject = \"api\"\nkey = \"stolen\"\n",
    )
    .unwrap();
    let r = context::resolve(&base).unwrap();
    assert_eq!(r.token_key.as_deref(), Some("stolen"));
    let err = format!("{:#}", context::verify(&r).await.unwrap_err());
    assert!(err.contains("expects joaquin@acme"), "{err}");
    assert!(err.contains("marta@acme"), "{err}");
    assert!(err.contains("'stolen'"), "{err}");
    std::fs::write(repo.join(context::PROJECT_FILE), before).unwrap();

    // A revoked token is reported as such, pointing at the entry to replace.
    store::revoke_token(&h.pool, Actor::Cli, None, mine.id)
        .await
        .unwrap();
    let r = context::resolve(&base).unwrap();
    let err = format!("{:#}", context::verify(&r).await.unwrap_err());
    assert!(err.contains("did not accept the token"), "{err}");
    assert!(err.contains("'_base'"), "{err}");
    assert!(err.contains("admin token issue"), "{err}");

    // Explicit credentials still work exactly as before, and are not
    // checked against any profile.
    let fresh = store::issue_token(&h.pool, Actor::Cli, acme.id, "marta", None)
        .await
        .unwrap();
    let explicit = Inputs {
        explicit_url: Some(format!("{}/mcp", h.base)),
        explicit_token: Some(fresh.token.clone()),
        ..base.clone()
    };
    let r = context::resolve(&explicit).unwrap();
    assert_eq!(r.source, Source::Explicit);
    assert!(r.expected.is_none());
    let v = context::verify(&r).await.unwrap();
    assert_eq!(v.agent, "marta");

    // The project's channel is not decoration: a message with neither
    // --channel nor --to goes there.
    let defaults = ai_crew_sync::client::mapping::Defaults {
        channel: r.channel.clone(),
    };
    let (tool, args) = ai_crew_sync::client::mapping::to_call_with(
        &ai_crew_sync::client::ClientCmd::Send {
            channel: None,
            to: None,
            body: "from the project".into(),
            announce: false,
            reply_to: None,
            file: vec![],
        },
        &defaults,
    )
    .unwrap()
    .expect("send maps to a tool");
    assert_eq!(tool, "post_message");
    assert_eq!(args["channel"], "api", "the .acs.toml channel is used");
    // A direct message stays direct, and an explicit channel still wins.
    let (_, args) = ai_crew_sync::client::mapping::to_call_with(
        &ai_crew_sync::client::ClientCmd::Send {
            channel: None,
            to: Some("marta".into()),
            body: "hi".into(),
            announce: false,
            reply_to: None,
            file: vec![],
        },
        &defaults,
    )
    .unwrap()
    .unwrap();
    assert!(args["channel"].is_null(), "a DM is not redirected: {args}");

    // The secret never appears in the redacted view.
    let shown = serde_json::to_string(&r.redacted()).unwrap();
    assert!(!shown.contains(&fresh.token[5..]));
    assert!(shown.contains(&fresh.token[..12]));

    let _ = std::fs::remove_dir_all(&dir);
    h.shutdown().await;
}

// -------------------------------------------------------- session discovery --

/// Five windows on one token and one repository — implementation, design, a
/// Claude reviewer and two Codex reviewers — are discoverable separately by
/// project and role, keep distinct addresses even when they share both, and
/// a message to one of them reaches that one alone.
#[tokio::test]
async fn sessions_are_discoverable_by_project_and_role_and_addressed_exactly() {
    let h = require_db!("t_sessions");
    let token = seed_agent(&h.pool, "acme", "joaquin").await;
    let dani_token = seed_agent(&h.pool, "acme", "dani").await;
    let outsider = seed_agent(&h.pool, "other", "eve").await;

    // Opaque session ids, the way a per-conversation proxy would mint them.
    let windows = [
        ("s-1a2b3c4d", "market-data", "implementation"),
        ("s-5e6f7a8b", "market-data", "design"),
        ("s-9c0d1e2f", "market-data", "review"),
        ("s-3a4b5c6d", "market-data", "review"),
        ("s-7e8f9a0b", "market-data", "review"),
    ];
    let mut clients = Vec::new();
    for (session, project, role) in windows {
        let c = connect_with_session(&h.base, &token, session).await;
        let beat = call(
            &c,
            "heartbeat",
            json!({"project": project, "role": role, "repo": "acme/market-data",
                   "activity": format!("{role} window")}),
        )
        .await;
        assert_eq!(beat["project"], project);
        assert_eq!(beat["role"], role);
        clients.push(c);
    }
    // A labelled window of another person on another project, and one with
    // an already expired lease.
    let dani = connect_with_session(&h.base, &dani_token, "s-dani0001").await;
    call(
        &dani,
        "heartbeat",
        json!({"project": "core-manager", "role": "implementation"}),
    )
    .await;
    let stale = connect_with_session(&h.base, &token, "s-stale001").await;
    call(
        &stale,
        "heartbeat",
        json!({"project": "market-data", "role": "review"}),
    )
    .await;
    sqlx::query("UPDATE agent_presence SET expires_at = now() - interval '1 minute' WHERE session = 's-stale001'")
        .execute(&h.pool)
        .await
        .unwrap();

    // Labels are validated as labels, not descriptions.
    let err = call_expect_error(&clients[0], "heartbeat", json!({"role": "code review!"})).await;
    assert!(err.contains("role"), "{err}");

    // Discovery: by project, by role, by both; every window separately.
    let all = call(
        &clients[0],
        "list_sessions",
        json!({"project": "market-data"}),
    )
    .await;
    let addresses: Vec<&str> = all["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["address"].as_str().unwrap())
        .collect();
    assert_eq!(
        addresses.len(),
        6,
        "five live windows plus the expired one: {addresses:?}"
    );
    for (session, _, _) in windows {
        assert!(addresses.contains(&format!("joaquin/{session}").as_str()));
    }
    assert!(
        !addresses.iter().any(|a| a.starts_with("dani/")),
        "other project"
    );
    let expired = all["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["session"] == "s-stale001")
        .expect("expired sessions are listed unless online_only");
    assert_eq!(expired["online"], false);
    assert_eq!(expired["status"], "offline");

    let reviewers = call(
        &clients[0],
        "list_sessions",
        json!({"project": "market-data", "role": "review", "online_only": true}),
    )
    .await;
    let mut review_addresses: Vec<String> = reviewers["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["address"].as_str().unwrap().to_owned())
        .collect();
    review_addresses.sort();
    assert_eq!(
        review_addresses,
        vec![
            "joaquin/s-3a4b5c6d",
            "joaquin/s-7e8f9a0b",
            "joaquin/s-9c0d1e2f"
        ],
        "three reviewers share a role and keep three addresses; the expired one is gone"
    );
    assert_eq!(reviewers["count"], 3);

    let design = call(&clients[0], "list_sessions", json!({"role": "Design"})).await;
    assert_eq!(
        design["sessions"].as_array().unwrap().len(),
        1,
        "labels normalise"
    );
    assert_eq!(design["sessions"][0]["address"], "joaquin/s-5e6f7a8b");

    // whoami and list_agents carry the labels too.
    let me = call(&clients[1], "whoami", json!({})).await;
    assert_eq!(me["project"], "market-data");
    assert_eq!(me["role"], "design");
    // A window that never labelled itself gets the response it always got:
    // the keys are absent, not null.
    let unlabelled = connect_with_session(&h.base, &token, "s-nolabels").await;
    let plain = call(&unlabelled, "whoami", json!({})).await;
    assert!(plain.get("project").is_none(), "{plain}");
    assert!(plain.get("role").is_none(), "{plain}");
    let _ = unlabelled.cancel().await;
    let roster = call(&clients[0], "list_agents", json!({})).await;
    let joaquin = roster["agents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["name"] == "joaquin")
        .unwrap();
    assert!(
        joaquin["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["role"] == "design"),
        "{joaquin}"
    );

    // Exact addressing: a message to the design window reaches only it, and
    // a sibling's read neither sees it nor moves its cursor.
    let design_addr = design["sessions"][0]["address"].as_str().unwrap();
    call(
        &clients[0],
        "post_message",
        json!({"to": design_addr, "body": "the header is wrong on mobile"}),
    )
    .await;
    let sibling = call(&clients[2], "read_messages", json!({"scope": "inbox"})).await;
    assert!(
        sibling["messages"].as_array().unwrap().is_empty(),
        "a reviewer window must not see the design window's DM: {sibling}"
    );
    let inbox = call(&clients[1], "read_messages", json!({"scope": "inbox"})).await;
    assert_eq!(inbox["messages"].as_array().unwrap().len(), 1);
    assert_eq!(
        inbox["messages"][0]["body"],
        "the header is wrong on mobile"
    );
    assert_eq!(inbox["messages"][0]["to_session"], "s-5e6f7a8b");
    let again = call(&clients[1], "read_messages", json!({"scope": "inbox"})).await;
    assert!(
        again["messages"].as_array().unwrap().is_empty(),
        "read once, cursor moved"
    );
    let me = call(&clients[1], "whoami", json!({})).await;
    assert_eq!(me["unread_direct_messages"], 0);

    // The default channel follows the project label, not the opaque id.
    call(
        &clients[0],
        "create_channel",
        json!({"name": "market-data"}),
    )
    .await;
    let me = call(&clients[0], "whoami", json!({})).await;
    assert_eq!(me["default_channel"], "market-data");
    let posted = call(
        &clients[0],
        "post_message",
        json!({"body": "posted by default"}),
    )
    .await;
    assert_eq!(posted["message"]["channel"], "market-data");
    // Clearing the project clears the default with it.
    call(&clients[0], "heartbeat", json!({"project": ""})).await;
    let me = call(&clients[0], "whoami", json!({})).await;
    assert!(me["default_channel"].is_null(), "{me}");
    assert!(me["project"].is_null());

    // A shared session mixed with named ones: it is listed, because it is
    // real presence, and it is NOT an exact address. Sending to the bare
    // agent name reaches every window of that agent, which is exactly why
    // `exact` is false — a caller that reads it cannot broadcast a private
    // instruction by accident.
    let shared = connect(&h.base, &dani_token).await;
    call(
        &shared,
        "heartbeat",
        json!({"project": "core-manager", "role": "design"}),
    )
    .await;
    let mixed = call(
        &clients[0],
        "list_sessions",
        json!({"project": "core-manager", "online_only": true}),
    )
    .await;
    let rows = mixed["sessions"].as_array().unwrap();
    let shared_row = rows
        .iter()
        .find(|s| s["session"].is_null())
        .expect("the shared session is listed");
    assert_eq!(shared_row["address"], "dani");
    assert_eq!(
        shared_row["exact"], false,
        "a bare agent name is not one window"
    );
    let named_row = rows
        .iter()
        .find(|s| s["session"] == "s-dani0001")
        .expect("the named session is listed");
    assert_eq!(named_row["address"], "dani/s-dani0001");
    assert_eq!(named_row["exact"], true);

    // Prove the warning: a message to the shared row's address lands in the
    // named window's inbox too.
    call(
        &clients[0],
        "post_message",
        json!({"to": shared_row["address"], "body": "for dani"}),
    )
    .await;
    let named_inbox = call(&dani, "read_messages", json!({"scope": "inbox"})).await;
    assert_eq!(
        named_inbox["messages"][0]["body"], "for dani",
        "the bare name reached the named window as well: {named_inbox}"
    );
    // The exact address of a named window reaches only it.
    call(
        &clients[0],
        "post_message",
        json!({"to": "dani/s-dani0001", "body": "only the named one"}),
    )
    .await;
    let shared_inbox = call(&shared, "read_messages", json!({"scope": "inbox"})).await;
    assert!(
        shared_inbox["messages"]
            .as_array()
            .unwrap()
            .iter()
            .all(|m| m["body"] != "only the named one"),
        "an exact address must not reach the shared session: {shared_inbox}"
    );
    let _ = shared.cancel().await;

    // Another team sees none of it.
    let eve = connect_with_session(&h.base, &outsider, "s-eve").await;
    let theirs = call(&eve, "list_sessions", json!({"project": "market-data"})).await;
    assert_eq!(theirs["count"], 0);
    let err = call_expect_error(
        &eve,
        "post_message",
        json!({"to": design_addr, "body": "hi"}),
    )
    .await;
    assert!(err.contains("no agent"), "{err}");

    for c in clients {
        let _ = c.cancel().await;
    }
    for c in [dani, stale, eve] {
        let _ = c.cancel().await;
    }
    h.shutdown().await;
}

// -------------------------------------------------------------- stdio proxy --

/// A local configuration directory with profiles for the given agents, each
/// with a tokens file holding its token under `_base`.
fn proxy_config_dir(base: &str, profiles: &[(&str, &str, &str, &str)]) -> std::path::PathBuf {
    use ai_crew_sync::context::{Profile, Profiles};
    let dir = std::env::temp_dir().join(format!("acs-proxy-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut store = Profiles::default();
    for (name, team, agent, token) in profiles {
        let tokens = format!("tokens-{name}");
        std::fs::write(dir.join(&tokens), format!("_base={token}\n")).unwrap();
        store.profiles.insert(
            (*name).to_owned(),
            Profile {
                url: base.to_owned(),
                team: (*team).to_owned(),
                agent: (*agent).to_owned(),
                tokens,
                key: None,
            },
        );
    }
    ai_crew_sync::context::save_profiles(&dir, &store).unwrap();
    dir
}

/// Start the real proxy binary over stdio, with a scrubbed environment so
/// nothing from the developer's shell (a BUS_TOKEN, a Claude session id)
/// leaks into the test.
async fn spawn_proxy(
    config_dir: &std::path::Path,
    project_dir: &std::path::Path,
    args: &[&str],
    env: &[(&str, &str)],
) -> Client {
    use rmcp::transport::{ConfigureCommandExt, TokioChildProcess};
    let bin = env!("CARGO_BIN_EXE_ai-crew-sync");
    let transport = TokioChildProcess::new(tokio::process::Command::new(bin).configure(|cmd| {
        cmd.env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("HOME", std::env::var("HOME").unwrap_or_default())
            .env("BUS_CONFIG_DIR", config_dir)
            .env("RUST_LOG", "warn")
            .current_dir(project_dir)
            .args(["mcp", "proxy", "--project-dir"])
            .arg(project_dir)
            .args(args);
        for (k, v) in env {
            cmd.env(k, v);
        }
    }))
    .expect("spawn proxy");
    ClientConfig::default()
        .serve(transport)
        .await
        .expect("proxy initialize")
}

#[tokio::test]
async fn proxy_gives_each_conversation_its_own_session_and_forwards_as_the_profile() {
    let h = require_db!("t_proxy_basic");
    let token = seed_agent(&h.pool, "acme", "joaquin").await;
    let dir = proxy_config_dir(&h.base, &[("acme", "acme", "joaquin", &token)]);
    let repo = dir.join("market-data");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(
        repo.join(".acs.toml"),
        "profile = \"acme\"\nproject = \"market-data\"\n",
    )
    .unwrap();

    // Two windows, same repository, same token, no environment at all.
    let a = spawn_proxy(&dir, &repo, &["--role", "implementation"], &[]).await;
    let b = spawn_proxy(&dir, &repo, &["--role", "review"], &[]).await;

    let sa = call(&a, "session_status", json!({})).await;
    let sb = call(&b, "session_status", json!({})).await;
    assert_eq!(sa["connected"], true, "{sa}");
    assert_eq!(sa["agent"], "joaquin");
    assert_eq!(sa["team"], "acme");
    assert_eq!(sa["project"], "market-data", "from .acs.toml");
    assert_eq!(sa["role"], "implementation");
    assert_eq!(sb["role"], "review");
    assert_eq!(sa["binding"], "instance");
    assert_ne!(sa["session"], sb["session"], "one session per process");
    assert!(sa["session"].as_str().unwrap().starts_with("s-"));
    assert_eq!(
        sa["address"],
        format!("joaquin/{}", sa["session"].as_str().unwrap())
    );
    assert!(sa.get("token").is_none() && sa.get("credentials").is_none());

    // Forwarded calls carry the session: whoami through each proxy is the
    // same agent in a different session.
    let wa = call(&a, "whoami", json!({})).await;
    let wb = call(&b, "whoami", json!({})).await;
    assert_eq!(wa["agent"], "joaquin");
    assert_eq!(wa["session"], sa["session"]);
    assert_eq!(wb["session"], sb["session"]);
    assert_eq!(
        wa["role"], "implementation",
        "the proxy's heartbeat published it"
    );

    // The local tools sit beside the remote ones here, and nowhere on the
    // bus itself.
    let names: Vec<String> = a
        .list_all_tools()
        .await
        .unwrap()
        .iter()
        .map(|t| t.name.to_string())
        .collect();
    for expected in [
        "configure_session",
        "session_status",
        "whoami",
        "post_message",
        "list_sessions",
    ] {
        assert!(names.contains(&expected.to_owned()), "{names:?}");
    }
    let direct = connect(&h.base, &token).await;
    let remote_names: Vec<String> = direct
        .list_all_tools()
        .await
        .unwrap()
        .iter()
        .map(|t| t.name.to_string())
        .collect();
    assert!(
        !remote_names
            .iter()
            .any(|n| n == "configure_session" || n == "session_status")
    );

    // Discovery sees both windows with their roles and exact addresses.
    let found = call(
        &direct,
        "list_sessions",
        json!({"project": "market-data", "online_only": true}),
    )
    .await;
    let addresses: Vec<&str> = found["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["address"].as_str().unwrap())
        .collect();
    assert!(
        addresses.contains(&sa["address"].as_str().unwrap()),
        "{addresses:?}"
    );
    assert!(
        addresses.contains(&sb["address"].as_str().unwrap()),
        "{addresses:?}"
    );

    // A DM to window B is read by B only; A's cursor is untouched.
    call(
        &direct,
        "post_message",
        json!({"to": sb["address"], "body": "for the reviewer"}),
    )
    .await;
    let inbox_a = call(&a, "read_messages", json!({"scope": "inbox"})).await;
    assert!(inbox_a["messages"].as_array().unwrap().is_empty());
    let inbox_b = call(&b, "read_messages", json!({"scope": "inbox"})).await;
    assert_eq!(inbox_b["messages"][0]["body"], "for the reviewer");

    // A role change keeps the session and reaches teammates on the next
    // discovery; it touches this window only.
    let changed = call(&b, "configure_session", json!({"role": "design"})).await;
    assert_eq!(changed["status"]["session"], sb["session"]);
    assert_eq!(changed["status"]["role"], "design");
    assert!(changed["previous"].is_null(), "no identity change");
    let design = call(&direct, "list_sessions", json!({"role": "design"})).await;
    assert_eq!(design["count"], 1);
    assert_eq!(design["sessions"][0]["address"], sb["address"]);
    assert_eq!(
        call(&a, "session_status", json!({})).await["role"],
        "implementation"
    );

    for c in [a, b, direct] {
        let _ = c.cancel().await;
    }
    let _ = std::fs::remove_dir_all(&dir);
    h.shutdown().await;
}

#[tokio::test]
async fn proxy_binds_a_conversation_id_to_a_stable_session() {
    use ai_crew_sync::proxy::session_for;

    let h = require_db!("t_proxy_bind");
    let token = seed_agent(&h.pool, "acme", "joaquin").await;
    let dir = proxy_config_dir(&h.base, &[("acme", "acme", "joaquin", &token)]);
    let repo = dir.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(repo.join(".acs.toml"), "profile = \"acme\"\n").unwrap();

    // The same conversation id, twice (a restart, a resume): one session.
    let first = spawn_proxy(&dir, &repo, &["--host-session", "conv-1"], &[]).await;
    let s1 = call(&first, "session_status", json!({})).await;
    let _ = first.cancel().await;
    let again = spawn_proxy(&dir, &repo, &["--host-session", "conv-1"], &[]).await;
    let s1b = call(&again, "session_status", json!({})).await;
    assert_eq!(s1["session"], s1b["session"], "reconnect keeps the session");
    assert_eq!(s1["binding"], "explicit");
    assert_eq!(s1["session"], session_for("conv-1"));
    // A forked conversation has another id and another session.
    let fork = spawn_proxy(&dir, &repo, &[], &[("BUS_HOST_SESSION", "conv-2")]).await;
    let s2 = call(&fork, "session_status", json!({})).await;
    assert_ne!(s2["session"], s1["session"]);
    assert_eq!(s2["session"], session_for("conv-2"));
    // Claude Code's variable binds the same way.
    let claude = spawn_proxy(&dir, &repo, &[], &[("CLAUDE_CODE_SESSION_ID", "conv-1")]).await;
    let s3 = call(&claude, "session_status", json!({})).await;
    assert_eq!(s3["binding"], "claude-code");
    assert_eq!(
        s3["session"], s1["session"],
        "same conversation id, same session"
    );
    // Identity is independent of the binding: still the profile's agent.
    assert_eq!(call(&claude, "whoami", json!({})).await["agent"], "joaquin");
    for c in [again, fork, claude] {
        let _ = c.cancel().await;
    }

    // A host that sends the conversation id in request metadata (Codex):
    // the first id binds, a second one on the same process is refused.
    let meta = spawn_proxy(&dir, &repo, &[], &[]).await;
    let before = call(&meta, "session_status", json!({})).await;
    assert_eq!(before["binding"], "instance");
    let with_thread = |thread: &str| {
        let mut params = CallToolRequestParams::new("whoami");
        let mut m = rmcp::model::JsonObject::new();
        m.insert("threadId".into(), json!(thread));
        params.meta = Some(rmcp::model::RequestMetaObject::from(m));
        params
    };
    let r = meta.call_tool(with_thread("thread-A")).await.unwrap();
    let who = r.structured_content.unwrap();
    assert_eq!(who["session"], session_for("thread-A"));
    let after = call(&meta, "session_status", json!({})).await;
    assert_eq!(after["binding"], "request-meta");
    assert_eq!(after["session"], session_for("thread-A"));
    let err = meta
        .call_tool(with_thread("thread-B"))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("another conversation"), "{err}");
    assert!(
        err.contains("one `ai-crew-sync mcp proxy` per conversation"),
        "{err}"
    );
    // The bound conversation keeps working.
    let r = meta.call_tool(with_thread("thread-A")).await.unwrap();
    assert_eq!(r.is_error, Some(false));
    let _ = meta.cancel().await;

    let _ = std::fs::remove_dir_all(&dir);
    h.shutdown().await;
}

#[tokio::test]
async fn proxy_switches_profiles_only_after_verification_and_never_across_teams() {
    use ai_crew_sync::store::admin::{self as store, Actor};

    let h = require_db!("t_proxy_switch");
    let joaquin = seed_agent(&h.pool, "acme", "joaquin").await;
    let marta = seed_agent(&h.pool, "acme", "marta").await;
    let eve = seed_agent(&h.pool, "other", "eve").await;
    let dir = proxy_config_dir(
        &h.base,
        &[
            ("me", "acme", "joaquin", &joaquin),
            ("marta", "acme", "marta", &marta),
            ("other", "other", "eve", &eve),
            // Claims to be marta, holds eve's token: verification must catch it.
            ("liar", "acme", "marta", &eve),
        ],
    );
    let repo = dir.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(
        repo.join(".acs.toml"),
        "profile = \"me\"\nproject = \"api\"\n",
    )
    .unwrap();

    // This conversation id hashes to the label `s-a7da401d8d70`. The bus
    // quotes it when marta is refused joaquin's lease below, and the proxy
    // once read that "401" as its own credential being rejected.
    let p = spawn_proxy(
        &dir,
        &repo,
        &["--role", "implementation"],
        &[("BUS_HOST_SESSION", "conv-1617")],
    )
    .await;
    let start = call(&p, "session_status", json!({})).await;
    assert_eq!(start["agent"], "joaquin");
    let session = start["session"].as_str().unwrap().to_owned();
    assert_eq!(
        session, "s-a7da401d8d70",
        "the fixture label moved; pick another id"
    );

    // Hold something as joaquin so the switch has something to report.
    call(
        &p,
        "create_task",
        json!({"key": "api#1", "title": "wire it"}),
    )
    .await;
    call(&p, "claim_task", json!({"key": "api#1"})).await;
    call(&p, "acquire_lock", json!({"name": "api:deploy"})).await;

    // A profile that fails verification changes nothing.
    let r = p
        .call_tool(
            CallToolRequestParams::new("configure_session")
                .with_arguments(serde_json::from_value(json!({"profile": "liar"})).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(r.is_error, Some(true), "{r:?}");
    let text = format!("{:?}", r.content);
    assert!(text.contains("expects marta@acme"), "{text}");
    assert_eq!(
        call(&p, "whoami", json!({})).await["agent"],
        "joaquin",
        "still joaquin"
    );

    // Another team: refused, with the reason.
    let r = p
        .call_tool(
            CallToolRequestParams::new("configure_session")
                .with_arguments(serde_json::from_value(json!({"profile": "other"})).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(r.is_error, Some(true));
    let text = format!("{:?}", r.content);
    assert!(text.contains("new conversation"), "{text}");
    assert!(text.contains("team 'acme'"), "{text}");
    assert_eq!(call(&p, "whoami", json!({})).await["agent"], "joaquin");

    // A same-team switch while a long poll is in flight: the poll is
    // cancelled, not replayed; the new identity answers afterwards; the old
    // one's claim and lock are reported, not transferred; role and session
    // stay.
    let waiter = {
        let p2 = p.clone();
        tokio::spawn(async move {
            p2.call_tool(
                CallToolRequestParams::new("wait_for_updates").with_arguments(
                    serde_json::from_value(json!({"timeout_seconds": 20})).unwrap(),
                ),
            )
            .await
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let switched = call(&p, "configure_session", json!({"profile": "marta"})).await;
    let wait_outcome = tokio::time::timeout(std::time::Duration::from_secs(8), waiter)
        .await
        .expect("the in-flight poll must end at the switch, not at its own timeout")
        .unwrap();
    let err = wait_outcome
        .expect_err("cancelled by the context switch")
        .to_string();
    assert!(err.contains("switched credentials"), "{err}");
    assert_eq!(switched["status"]["agent"], "marta");
    assert_eq!(switched["status"]["team"], "acme");
    assert_eq!(
        switched["status"]["session"], session,
        "session survives the switch"
    );
    assert_eq!(switched["status"]["role"], "implementation");
    assert_eq!(switched["previous"]["agent"], "joaquin");
    assert_eq!(switched["previous"]["open_claims"], json!(["api#1"]));
    assert_eq!(switched["previous"]["held_locks"], json!(["api:deploy"]));

    // The window's own channel is applied to a message that names none, so
    // the default the instructions advertise is the one the bus sees.
    call(&p, "create_channel", json!({"name": "api"})).await;
    let posted = call(&p, "post_message", json!({"body": "from the window"})).await;
    assert_eq!(posted["message"]["channel"], "api");
    // An explicit channel and a direct message are untouched.
    call(&p, "create_channel", json!({"name": "other"})).await;
    let elsewhere = call(
        &p,
        "post_message",
        json!({"channel": "other", "body": "explicit"}),
    )
    .await;
    assert_eq!(elsewhere["message"]["channel"], "other");
    let dm = call(
        &p,
        "post_message",
        json!({"to": "joaquin", "body": "direct"}),
    )
    .await;
    assert!(dm["message"]["channel"].is_null(), "{dm}");

    // A label the bus would reject is refused here too, and changes nothing.
    let before_role = call(&p, "session_status", json!({})).await["role"].clone();
    let r = p
        .call_tool(
            CallToolRequestParams::new("configure_session")
                .with_arguments(serde_json::from_value(json!({"role": "code review!"})).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(r.is_error, Some(true), "{r:?}");
    assert_eq!(
        call(&p, "session_status", json!({})).await["role"],
        before_role
    );
    assert_eq!(call(&p, "whoami", json!({})).await["agent"], "marta");
    // Ownership stayed with joaquin: marta holds neither the lease nor the
    // lock, so she can renew and release nothing of his.
    let err = call_expect_error(&p, "renew_task_lease", json!({"key": "api#1"})).await;
    assert!(err.contains("joaquin"), "{err}");
    let err = call_expect_error(&p, "release_lock", json!({"name": "api:deploy"})).await;
    assert!(err.contains("joaquin"), "{err}");

    // A revoked token surfaces as an error on the next forwarded call, and a
    // working profile recovers the window.
    let (marta_id,): (Uuid,) = sqlx::query_as(
        "SELECT t.id FROM api_tokens t JOIN agents a ON a.id = t.agent_id WHERE a.name = 'marta'",
    )
    .fetch_one(&h.pool)
    .await
    .unwrap();
    store::revoke_token(&h.pool, Actor::Cli, None, marta_id)
        .await
        .unwrap();
    let err = p
        .call_tool(CallToolRequestParams::new("whoami"))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("revoked or rotated"), "{err}");
    assert!(err.contains("configure_session"), "{err}");
    assert!(err.contains("profile 'marta'"), "{err}");
    let hurt = call(&p, "session_status", json!({})).await;
    assert!(
        hurt["error"]
            .as_str()
            .unwrap_or_default()
            .contains("rejected"),
        "the window reports its own broken credential: {hurt}"
    );
    let back = call(&p, "configure_session", json!({"profile": "me"})).await;
    assert_eq!(back["status"]["agent"], "joaquin");
    assert_eq!(call(&p, "whoami", json!({})).await["session"], session);

    let _ = p.cancel().await;
    let _ = std::fs::remove_dir_all(&dir);
    h.shutdown().await;
}

// ------------------------------------------------- host integration (#86) --

/// Run a plugin hook script the way a host would: payload on stdin, the
/// repository as cwd, and only the environment a hook actually gets. No
/// BUS_TOKEN, no BUS_SESSION — everything is resolved from the profiles and
/// the conversation id in the payload.
async fn run_hook(
    script: &str,
    config_dir: &std::path::Path,
    project_dir: &std::path::Path,
    payload: &str,
    extra_args: &[&str],
) -> String {
    // The hook talks HTTP to the harness, which runs on this runtime: waiting
    // for the child on this thread deadlocks both. Off to a blocking thread,
    // with a bound so a wedged hook fails the test instead of hanging CI.
    let (script, config_dir, project_dir, payload) = (
        script.to_owned(),
        config_dir.to_path_buf(),
        project_dir.to_path_buf(),
        payload.to_owned(),
    );
    let extra: Vec<String> = extra_args.iter().map(|a| (*a).to_owned()).collect();
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        tokio::task::spawn_blocking(move || {
            run_hook_blocking(&script, &config_dir, &project_dir, &payload, &extra)
        }),
    )
    .await
    .expect("the hook did not finish within 30s")
    .expect("the hook task panicked")
}

fn run_hook_blocking(
    script: &str,
    config_dir: &std::path::Path,
    project_dir: &std::path::Path,
    payload: &str,
    extra_args: &[String],
) -> String {
    use std::io::Write;
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("plugin/scripts")
        .join(script);
    let bin_dir = std::path::Path::new(env!("CARGO_BIN_EXE_ai-crew-sync"))
        .parent()
        .unwrap()
        .to_path_buf();
    let path = format!(
        "{}:{}",
        bin_dir.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let mut child = std::process::Command::new("sh")
        .arg(&script)
        .args(extra_args.iter().map(String::as_str))
        .current_dir(project_dir)
        .env_clear()
        .env("PATH", path)
        .env("HOME", std::env::var("HOME").unwrap_or_default())
        .env("TMPDIR", config_dir)
        .env("BUS_CONFIG_DIR", config_dir)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn hook");
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(payload.as_bytes())
        .unwrap();
    let out = child.wait_with_output().expect("hook finished");
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// The workflow the whole stack exists for: five conversations in one
/// repository, on one token, with no per-window export and no new token —
/// implementation, design, a Claude review and two Codex reviews. They find
/// each other, send targeted corrections, and each window's hooks act on its
/// own session only.
#[tokio::test]
async fn five_conversations_share_a_repo_and_stay_separate() {
    let h = require_db!("t_host_integration");
    let token = seed_agent(&h.pool, "acme", "joaquin").await;
    let dir = proxy_config_dir(&h.base, &[("acme", "acme", "joaquin", &token)]);
    let repo = dir.join("market-data");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(
        repo.join(".acs.toml"),
        "profile = \"acme\"\nproject = \"market-data\"\nchannel = \"market-data\"\n",
    )
    .unwrap();

    // Five windows. Two hosts that expose a conversation id (Claude Code's
    // variable and an explicit flag), and three that expose none.
    let windows = [
        ("impl", "implementation", Some("conv-impl")),
        ("design", "design", Some("conv-design")),
        ("claude-review", "review", None),
        ("codex-review-1", "review", None),
        ("codex-review-2", "review", None),
    ];
    let mut clients = Vec::new();
    let mut ids = Vec::new();
    for (name, role, conv) in windows {
        let env: Vec<(&str, &str)> = match conv {
            Some(c) if name == "impl" => vec![("CLAUDE_CODE_SESSION_ID", c)],
            Some(c) => vec![("BUS_HOST_SESSION", c)],
            None => vec![],
        };
        let c = spawn_proxy(&dir, &repo, &["--role", role], &env).await;
        let st = call(&c, "session_status", json!({})).await;
        assert_eq!(st["connected"], true, "{name}: {st}");
        assert_eq!(st["agent"], "joaquin");
        ids.push((
            name,
            st["session"].as_str().unwrap().to_owned(),
            st["address"].as_str().unwrap().to_owned(),
        ));
        clients.push(c);
    }
    let sessions: std::collections::HashSet<&str> =
        ids.iter().map(|(_, s, _)| s.as_str()).collect();
    assert_eq!(
        sessions.len(),
        5,
        "five conversations, five sessions: {ids:?}"
    );

    // Design discovers the implementation window and sends a correction; the
    // two same-role Codex reviewers stay distinct.
    let design = &clients[1];
    let found = call(
        design,
        "list_sessions",
        json!({"project": "market-data", "role": "implementation", "online_only": true}),
    )
    .await;
    assert_eq!(found["count"], 1);
    let impl_addr = found["sessions"][0]["address"].as_str().unwrap().to_owned();
    assert_eq!(impl_addr, ids[0].2);
    call(
        design,
        "post_message",
        json!({"to": impl_addr, "body": "the empty state needs a spinner"}),
    )
    .await;
    let reviewers = call(
        design,
        "list_sessions",
        json!({"project": "market-data", "role": "review", "online_only": true}),
    )
    .await;
    assert_eq!(
        reviewers["count"], 3,
        "three reviewers share a role, three addresses"
    );

    // The implementation window reads it, replies to the exact sender, and
    // no other window's inbox moved.
    let implementation = &clients[0];
    let inbox = call(implementation, "read_messages", json!({"scope": "inbox"})).await;
    assert_eq!(inbox["messages"].as_array().unwrap().len(), 1);
    let msg = &inbox["messages"][0];
    assert_eq!(msg["body"], "the empty state needs a spinner");
    let from = format!(
        "{}/{}",
        msg["from"].as_str().unwrap(),
        msg["from_session"].as_str().unwrap()
    );
    assert_eq!(from, ids[1].2);
    call(
        implementation,
        "post_message",
        json!({"to": from, "body": "added", "reply_to": msg["id"]}),
    )
    .await;
    let reply = call(design, "read_messages", json!({"scope": "inbox"})).await;
    assert_eq!(reply["messages"][0]["body"], "added");
    for c in clients.iter().skip(2) {
        let quiet = call(c, "read_messages", json!({"scope": "inbox"})).await;
        assert!(
            quiet["messages"].as_array().unwrap().is_empty(),
            "a reviewer saw another window's direct message"
        );
    }

    // The workflow the stack exists for, end to end: design opens a thread
    // with the implementation window and both reviewers, each acknowledges
    // for itself, and the sender can see who acted.
    enable_conversations(&h.pool, "acme").await;
    let convo = call(
        design,
        "create_conversation",
        json!({"title": "the empty state", "private": true,
               "invite": [ids[0].2.clone(), ids[2].2.clone(), ids[3].2.clone()]}),
    )
    .await;
    let cid = convo["id"].as_str().unwrap().to_owned();
    for c in [&clients[0], &clients[2], &clients[3]] {
        call(c, "join_conversation", json!({"conversation_id": cid})).await;
    }
    let asked = call(
        design,
        "send_conversation_message",
        json!({"conversation_id": cid, "body": "the empty state needs a spinner",
               "request_id": Uuid::new_v4().to_string()}),
    )
    .await;
    let asked_id = asked["message_id"].as_str().unwrap().to_owned();
    assert_eq!(asked["recipients"].as_array().unwrap().len(), 3);

    // The fifth window was never invited and sees nothing.
    let err = call_expect_error(
        &clients[4],
        "read_conversation",
        json!({"conversation_id": cid}),
    )
    .await;
    assert!(err.contains("no such conversation"), "{err}");

    // Independent observations: implementation resolves, one reviewer only
    // acknowledges, the other has not answered.
    call(
        &clients[0],
        "ack_message",
        json!({"message_id": asked_id, "resolved": true, "note": "added in 4d21f"}),
    )
    .await;
    call(&clients[2], "ack_message", json!({"message_id": asked_id})).await;
    let receipts = call(
        design,
        "get_message_receipts",
        json!({"message_id": asked_id}),
    )
    .await;
    assert_eq!(receipts["total"], 3);
    assert_eq!(receipts["acknowledged"], 2);
    assert_eq!(receipts["resolved"], 1, "{receipts}");
    let pending = receipts["receipts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["address"] == ids[3].2.as_str())
        .unwrap();
    assert!(
        pending["acknowledged_at"].is_null(),
        "still to answer: {pending}"
    );
    assert!(
        pending["presented_at"].is_null(),
        "unknown presentation stays unknown"
    );

    // A hook of the implementation conversation acts on THAT window: the
    // same conversation id resolves to the same session, so a heartbeat from
    // the hook updates this window's presence and nobody else's.
    run_hook("heartbeat.sh", &dir, &repo, "", &["busy"]).await;
    let after = call(
        design,
        "list_sessions",
        json!({"project": "market-data", "online_only": true}),
    )
    .await;
    assert_eq!(after["count"], 5, "the hook did not create a sixth session");

    // SessionStart for the design conversation injects its own identity,
    // and only after the bus confirmed it.
    let out = run_hook(
        "session-start.sh",
        &dir,
        &repo,
        &json!({"session_id": "conv-design", "cwd": repo.display().to_string()}).to_string(),
        &[],
    )
    .await;
    let injected: Value = serde_json::from_str(out.trim()).expect("hook emitted JSON");
    let context = injected["hookSpecificOutput"]["additionalContext"]
        .as_str()
        .unwrap_or_default();
    assert!(context.contains("agent 'joaquin'"), "{context}");
    assert!(
        context.contains(&ids[1].1),
        "the design window's own session: {context}"
    );
    assert!(context.contains("role 'design'"), "{context}");
    for (name, session, _) in ids.iter().skip(2) {
        assert!(
            !context.contains(session.as_str()),
            "{name}'s session leaked: {context}"
        );
    }

    // A conversation with no bus configured injects nothing at all.
    let empty_cfg = dir.join("no-profiles");
    std::fs::create_dir_all(&empty_cfg).unwrap();
    let out = run_hook(
        "session-start.sh",
        &empty_cfg,
        &repo,
        &json!({"session_id": "conv-nowhere"}).to_string(),
        &[],
    )
    .await;
    assert!(out.trim().is_empty(), "unconfigured session start: {out}");

    // Ending one window leaves the others alone: no idled presence, no
    // drained inbox, no released claim.
    call(
        implementation,
        "create_task",
        json!({"key": "md#1", "title": "spinner"}),
    )
    .await;
    call(implementation, "claim_task", json!({"key": "md#1"})).await;
    let closing = clients.remove(4);
    let _ = closing.cancel().await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let implementation = &clients[0];
    let mine = call(implementation, "list_tasks", json!({"mine_only": true})).await;
    assert_eq!(mine["tasks"][0]["status"], "claimed");
    assert_eq!(mine["tasks"][0]["claimed_session"], ids[0].1);
    let still = call(
        implementation,
        "list_sessions",
        json!({"project": "market-data", "role": "implementation", "online_only": true}),
    )
    .await;
    assert_eq!(still["count"], 1, "the surviving window is still online");
    assert_eq!(
        call(implementation, "whoami", json!({})).await["session"],
        ids[0].1
    );

    for c in clients {
        let _ = c.cancel().await;
    }
    let _ = std::fs::remove_dir_all(&dir);
    h.shutdown().await;
}

// ------------------------------------------------- authenticated sessions --

/// A session credential proves which window is calling: it is derived from an
/// agent token, cannot mint anything, dies with its parent, expires on its
/// own, and a resume fences the connection it replaced.
#[tokio::test]
async fn session_credentials_prove_a_window_and_die_with_their_parent() {
    let h = require_db!("t_sessions_auth");
    let token = seed_agent(&h.pool, "acme", "joaquin").await;
    let other = seed_agent(&h.pool, "acme", "marta").await;

    // Registration takes the agent token and derives everything from it.
    let agent = connect_with_session(&h.base, &token, "conv-a").await;
    let cred = call(
        &agent,
        "register_session",
        json!({"session": "conv-a", "ttl_seconds": 3600}),
    )
    .await;
    let session_token = cred["session_token"].as_str().unwrap().to_owned();
    assert!(session_token.starts_with("acss_"), "{cred}");
    assert_eq!(cred["session"], "conv-a");
    assert_eq!(cred["address"], "joaquin/conv-a");
    assert_eq!(cred["epoch"], 1);
    assert!(cred["expires_in_seconds"].as_i64().unwrap() <= 3600);

    // It authenticates as that agent in that session, and says so.
    let window = connect(&h.base, &session_token).await;
    let me = call(&window, "whoami", json!({})).await;
    assert_eq!(me["agent"], "joaquin");
    assert_eq!(me["team"], "acme");
    assert_eq!(me["session"], "conv-a", "the label is proven, not sent");
    assert_eq!(me["session_identity"]["epoch"], 1);
    assert_eq!(me["session_identity"]["session_id"], cred["session_id"]);
    // A plain agent token has no proven identity.
    assert!(call(&agent, "whoami", json!({})).await["session_identity"].is_null());

    // It cannot mint: not another session, not anything else.
    let err = call_expect_error(&window, "register_session", json!({"session": "conv-b"})).await;
    assert!(err.contains("cannot register another session"), "{err}");

    // A header that disagrees with the proof is refused outright, so a
    // session credential can never be widened into another window.
    let resp = reqwest::Client::new()
        .post(format!("{}/mcp", h.base))
        .header("Authorization", format!("Bearer {session_token}"))
        .header("X-Crew-Session", "someone-elses-window")
        .header("Accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
    let body: Value = resp.json().await.unwrap();
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("authenticates session 'conv-a'"),
        "{body}"
    );
    // The same header, agreeing, is simply redundant.
    assert_eq!(
        mcp_status_with_session(&h.base, &session_token, "conv-a").await,
        200
    );

    // Renewal extends without disturbing the connection: same secret, same
    // epoch, later expiry.
    let renewed = call(&window, "renew_session", json!({"ttl_seconds": 7200})).await;
    assert!(
        renewed["session_token"].is_null(),
        "renewal returns no secret"
    );
    assert_eq!(renewed["epoch"], 1);
    assert!(renewed["expires_in_seconds"].as_i64().unwrap() > 3600);
    assert_eq!(call(&window, "whoami", json!({})).await["agent"], "joaquin");

    // The agent token cannot take a live window: holding it is not proof of
    // being that conversation.
    let err = call_expect_error(&agent, "register_session", json!({"session": "conv-a"})).await;
    assert!(err.contains("already registered and still live"), "{err}");
    assert!(err.contains("resume_session"), "{err}");
    assert_eq!(
        call(&window, "whoami", json!({})).await["session_identity"]["epoch"],
        1,
        "the refused registration changed nothing"
    );

    // A resume rotates the secret and bumps the epoch; the old credential is
    // dead and the old connection is fenced by its stale epoch. The proof is
    // the session's own credential.
    let resumed = call(&window, "resume_session", json!({})).await;
    assert_eq!(resumed["epoch"], 2);
    let resumed_token = resumed["session_token"].as_str().unwrap().to_owned();
    let window_again = connect(&h.base, &resumed_token).await;
    assert_ne!(resumed_token, session_token);
    assert_eq!(
        mcp_status(&h.base, &session_token).await,
        401,
        "old secret is dead"
    );
    assert_eq!(
        mcp_status_with_epoch(&h.base, &resumed_token, 1).await,
        409,
        "a connection carrying the old epoch is stale"
    );
    assert_eq!(mcp_status_with_epoch(&h.base, &resumed_token, 2).await, 200);
    let _ = window.cancel().await;

    // Fencing is not only a pre-dispatch check. The middleware's check runs
    // before the handler, so a request that passed it and then waited on a
    // row lock would otherwise commit into the session that replaced it. The
    // guard re-checks inside the writing transaction, which is where it has
    // to be: here it is exercised directly, with the epoch moved after the
    // transaction opened.
    use ai_crew_sync::store::sessions;
    let stale_ctx = ai_crew_sync::auth::AuthCtx {
        agent_id: sqlx::query_scalar::<_, Uuid>("SELECT id FROM agents WHERE name = 'joaquin'")
            .fetch_one(&h.pool)
            .await
            .unwrap(),
        agent_name: "joaquin".into(),
        team_id: sqlx::query_scalar::<_, Uuid>("SELECT id FROM teams WHERE slug = 'acme'")
            .fetch_one(&h.pool)
            .await
            .unwrap(),
        team_slug: "acme".into(),
        session: "conv-a".into(),
        session_id: Some(
            resumed["session_id"]
                .as_str()
                .unwrap()
                .parse::<Uuid>()
                .unwrap(),
        ),
        // The epoch this connection was admitted with.
        session_epoch: Some(2),
        token_id: None,
    };
    // Same epoch: the write may proceed.
    let mut tx = h.pool.begin().await.unwrap();
    sessions::guard(&mut tx, &stale_ctx)
        .await
        .expect("current epoch passes");
    tx.rollback().await.unwrap();

    // Now the window is resumed by its owner, and the connection admitted at
    // epoch 2 is refused *inside* the transaction rather than committing.
    let bumped = call(&window_again, "resume_session", json!({})).await;
    assert_eq!(bumped["epoch"], 3);
    // The resume rotated the secret again, so later connections use this one.
    let resumed_token = bumped["session_token"].as_str().unwrap().to_owned();
    let _ = window_again.cancel().await;
    let mut tx = h.pool.begin().await.unwrap();
    let err = sessions::guard(&mut tx, &stale_ctx)
        .await
        .expect_err("a replaced connection must not write");
    let err = err.to_string();
    assert!(err.contains("stale"), "{err}");
    assert!(err.contains("Nothing was written"), "{err}");
    tx.rollback().await.unwrap();

    // And a revoked session is refused the same way, not only at the door.
    sqlx::query("UPDATE agent_sessions SET revoked_at = now() WHERE label = 'conv-a'")
        .execute(&h.pool)
        .await
        .unwrap();
    let mut tx = h.pool.begin().await.unwrap();
    let current = ai_crew_sync::auth::AuthCtx {
        session_epoch: Some(3),
        ..stale_ctx.clone()
    };
    let err = sessions::guard(&mut tx, &current)
        .await
        .expect_err("a revoked session must not write")
        .to_string();
    assert!(err.contains("no longer valid"), "{err}");
    tx.rollback().await.unwrap();
    sqlx::query("UPDATE agent_sessions SET revoked_at = NULL WHERE label = 'conv-a'")
        .execute(&h.pool)
        .await
        .unwrap();

    // Two agents, two sessions of the same label: separate rows, separate
    // addresses, no collision.
    let marta = connect(&h.base, &other).await;
    let hers = call(&marta, "register_session", json!({"session": "conv-a"})).await;
    assert_eq!(hers["address"], "marta/conv-a");
    assert_ne!(hers["session_id"], resumed["session_id"]);

    // Revocation: a window can close another window of its own agent, and
    // never one of somebody else's.
    let live = connect(&h.base, &resumed_token).await;
    call(&agent, "register_session", json!({"session": "conv-c"})).await;
    let gone = call(&live, "revoke_session", json!({"session": "conv-c"})).await;
    assert_eq!(gone["revoked_session"], "conv-c");
    let err = call_expect_error(
        &live,
        "revoke_session",
        json!({"session": "conv-a-of-marta"}),
    )
    .await;
    assert!(err.contains("no session"), "{err}");
    assert_eq!(
        call(&marta, "whoami", json!({})).await["agent"],
        "marta",
        "marta's own session is untouched"
    );

    // The parent's revocation is the session's revocation: no sweep needed.
    let (parent_id,): (Uuid,) = sqlx::query_as(
        "SELECT t.id FROM api_tokens t JOIN agents a ON a.id = t.agent_id WHERE a.name = 'joaquin'",
    )
    .fetch_one(&h.pool)
    .await
    .unwrap();
    ai_crew_sync::store::admin::revoke_token(
        &h.pool,
        ai_crew_sync::store::admin::Actor::Cli,
        None,
        parent_id,
    )
    .await
    .unwrap();
    assert_eq!(
        mcp_status(&h.base, &resumed_token).await,
        401,
        "parent revoked"
    );
    assert_eq!(mcp_status(&h.base, &token).await, 401);
    assert_eq!(
        mcp_status(&h.base, &other).await,
        200,
        "another agent is unaffected"
    );

    // An expired credential says what to do rather than just failing.
    let fresh = call(&marta, "register_session", json!({"session": "conv-old"})).await;
    let fresh_token = fresh["session_token"].as_str().unwrap().to_owned();
    sqlx::query("UPDATE agent_sessions SET expires_at = now() - interval '1 minute' WHERE label = 'conv-old'")
        .execute(&h.pool)
        .await
        .unwrap();
    let resp = reqwest::Client::new()
        .post(format!("{}/mcp", h.base))
        .header("Authorization", format!("Bearer {fresh_token}"))
        .header("Accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
    let body: Value = resp.json().await.unwrap();
    assert!(
        body["error"].as_str().unwrap().contains("register_session"),
        "{body}"
    );

    for c in [agent, live, marta] {
        let _ = c.cancel().await;
    }
    h.shutdown().await;
}

/// `tools/list` with a bearer and a session header, for status checks.
async fn mcp_status_with_session(base: &str, token: &str, session: &str) -> u16 {
    reqwest::Client::new()
        .post(format!("{base}/mcp"))
        .header("Authorization", format!("Bearer {token}"))
        .header("X-Crew-Session", session)
        .header("Accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

/// `tools/list` with a bearer and an explicit connection epoch.
async fn mcp_status_with_epoch(base: &str, token: &str, epoch: i64) -> u16 {
    reqwest::Client::new()
        .post(format!("{base}/mcp"))
        .header("Authorization", format!("Bearer {token}"))
        .header("X-Crew-Epoch", epoch.to_string())
        .header("Accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

// ----------------------------------------------------------- conversations --

/// Turn the capability on for a team, the way an operator does.
async fn enable_conversations(pool: &PgPool, team: &str) {
    sqlx::query("UPDATE teams SET conversations_enabled = true WHERE slug = $1")
        .bind(team)
        .execute(pool)
        .await
        .unwrap();
}

/// A fresh request id for each send; reusing one is how a retry is spotted.
fn request_id() -> String {
    Uuid::new_v4().to_string()
}

/// The property the whole feature exists for: two sessions exchange requests
/// privately, each reader has its own receipt, and the sender can see who
/// acknowledged and who resolved. Plus the boundaries: the capability flag,
/// reading not acknowledging, recipients snapshotted at acceptance, and a
/// non-member seeing nothing.
#[tokio::test]
async fn conversations_carry_private_requests_with_per_recipient_receipts() {
    let h = require_db!("t_conversations");
    let token = seed_agent(&h.pool, "acme", "joaquin").await;
    let dani_token = seed_agent(&h.pool, "acme", "dani").await;
    let outsider = seed_agent(&h.pool, "other", "eve").await;

    // Off by default: the tools refuse before an operator enables them.
    let impl_w = connect_with_session(&h.base, &token, "impl").await;
    let err = call_expect_error(
        &impl_w,
        "create_conversation",
        json!({"title": "too early", "private": true}),
    )
    .await;
    assert!(err.contains("not enabled for this team"), "{err}");
    enable_conversations(&h.pool, "acme").await;

    // Three windows of two people, plus a reviewer who is never invited.
    let design = connect_with_session(&h.base, &dani_token, "design").await;
    let review = connect_with_session(&h.base, &dani_token, "review").await;
    let bystander = connect_with_session(&h.base, &token, "bystander").await;

    // A private thread addressed to two exact windows.
    let convo = call(
        &impl_w,
        "create_conversation",
        json!({"title": "the empty state", "private": true,
               "invite": ["dani/design", "dani/review"]}),
    )
    .await;
    let cid = convo["id"].as_str().unwrap().to_owned();
    assert_eq!(convo["visibility"], "private");
    assert_eq!(convo["membership"]["role"], "owner");
    assert_eq!(convo["members"].as_array().unwrap().len(), 3);

    // An invitation is not membership: until it is accepted, that window is
    // not a recipient.
    let first = call(
        &impl_w,
        "send_conversation_message",
        json!({"conversation_id": cid, "body": "what should the empty state say?",
               "request_id": request_id()}),
    )
    .await;
    assert_eq!(first["stored"], true);
    assert_eq!(first["seq"], 1);
    assert!(
        first["recipients"].as_array().unwrap().is_empty(),
        "nobody has accepted yet: {first}"
    );

    // Both accept; only the invited window can, not a sibling.
    let err = call_expect_error(
        &bystander,
        "join_conversation",
        json!({"conversation_id": cid}),
    )
    .await;
    assert!(err.contains("no invitation for this window"), "{err}");
    call(
        &design,
        "join_conversation",
        json!({"conversation_id": cid}),
    )
    .await;
    call(
        &review,
        "join_conversation",
        json!({"conversation_id": cid}),
    )
    .await;

    let second = call(
        &impl_w,
        "send_conversation_message",
        json!({"conversation_id": cid, "body": "second attempt, with a spinner",
               "request_id": request_id()}),
    )
    .await;
    let second_id = second["message_id"].as_str().unwrap().to_owned();
    let mut addressed: Vec<&str> = second["recipients"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r.as_str().unwrap())
        .collect();
    addressed.sort();
    assert_eq!(addressed, vec!["dani/design", "dani/review"]);

    // Reading is not acknowledging.
    let read = call(
        &design,
        "read_conversation",
        json!({"conversation_id": cid}),
    )
    .await;
    assert_eq!(read["messages"].as_array().unwrap().len(), 2);
    assert!(
        read["messages"][1]["my_receipt"]["acknowledged_at"].is_null(),
        "a read must not acknowledge: {read}"
    );
    let receipts = call(
        &impl_w,
        "get_message_receipts",
        json!({"message_id": second_id}),
    )
    .await;
    assert_eq!(receipts["total"], 2);
    assert_eq!(receipts["acknowledged"], 0);
    assert!(
        receipts["receipts"][0]["stored_at"].is_string(),
        "stored is a fact about persistence: {receipts}"
    );
    assert!(
        receipts["receipts"][0]["presented_at"].is_null(),
        "presentation is unknown, not 'no'"
    );

    // Independent receipts: one reader acknowledges, the other resolves.
    call(
        &design,
        "ack_message",
        json!({"message_id": second_id, "note": "looks right"}),
    )
    .await;
    call(
        &review,
        "ack_message",
        json!({"message_id": second_id, "resolved": true, "note": "shipped in 4d21f"}),
    )
    .await;
    let receipts = call(
        &impl_w,
        "get_message_receipts",
        json!({"message_id": second_id}),
    )
    .await;
    assert_eq!(receipts["acknowledged"], 2);
    assert_eq!(
        receipts["resolved"], 1,
        "only one said it acted: {receipts}"
    );
    let design_receipt = receipts["receipts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["address"] == "dani/design")
        .unwrap();
    assert!(design_receipt["resolved_at"].is_null());
    assert_eq!(design_receipt["note"], "looks right");

    // A sender does not acknowledge its own message, and a window that was
    // not addressed has nothing to acknowledge.
    let err = call_expect_error(&impl_w, "ack_message", json!({"message_id": second_id})).await;
    assert!(err.contains("not addressed to your window"), "{err}");

    // Idempotency: the same request id returns the original message.
    let rid = request_id();
    let once = call(
        &impl_w,
        "send_conversation_message",
        json!({"conversation_id": cid, "body": "retry me", "request_id": rid}),
    )
    .await;
    let twice = call(
        &impl_w,
        "send_conversation_message",
        json!({"conversation_id": cid, "body": "retry me", "request_id": rid}),
    )
    .await;
    assert_eq!(once["message_id"], twice["message_id"]);
    assert_eq!(once["seq"], twice["seq"]);
    let err = call_expect_error(
        &impl_w,
        "send_conversation_message",
        json!({"conversation_id": cid, "body": "something else", "request_id": rid}),
    )
    .await;
    assert!(err.contains("already sent a different message"), "{err}");

    // A late joiner never enters an older message's denominator.
    call(
        &impl_w,
        "invite_to_conversation",
        json!({"conversation_id": cid, "address": "joaquin/bystander"}),
    )
    .await;
    call(
        &bystander,
        "join_conversation",
        json!({"conversation_id": cid}),
    )
    .await;
    let receipts = call(
        &impl_w,
        "get_message_receipts",
        json!({"message_id": second_id}),
    )
    .await;
    assert_eq!(
        receipts["total"], 2,
        "the denominator is frozen: {receipts}"
    );
    // And sees only what was said after they joined.
    let theirs = call(
        &bystander,
        "read_conversation",
        json!({"conversation_id": cid}),
    )
    .await;
    assert!(
        theirs["messages"]
            .as_array()
            .unwrap()
            .iter()
            .all(|m| m["body"] != "what should the empty state say?"),
        "a late member must not read earlier history: {theirs}"
    );

    // A removal keeps history and receipts, and stops access at once.
    call(
        &impl_w,
        "remove_conversation_member",
        json!({"conversation_id": cid, "address": "dani/design"}),
    )
    .await;
    let err = call_expect_error(
        &design,
        "read_conversation",
        json!({"conversation_id": cid}),
    )
    .await;
    assert!(err.contains("no such conversation"), "{err}");
    let receipts = call(
        &impl_w,
        "get_message_receipts",
        json!({"message_id": second_id}),
    )
    .await;
    assert_eq!(
        receipts["acknowledged"], 2,
        "a removal is not a rewrite: {receipts}"
    );

    // Another team sees nothing, by id or otherwise.
    let eve = connect(&h.base, &outsider).await;
    enable_conversations(&h.pool, "other").await;
    let err = call_expect_error(&eve, "read_conversation", json!({"conversation_id": cid})).await;
    assert!(err.contains("no such conversation"), "{err}");
    let theirs = call(&eve, "list_conversations", json!({})).await;
    assert!(theirs["conversations"].as_array().unwrap().is_empty());

    for c in [impl_w, design, review, bystander, eve] {
        let _ = c.cancel().await;
    }
    h.shutdown().await;
}

/// The two exceptional paths, and the project boundary. A transfer needs the
/// target to accept and supersedes rather than rewrites; recovery is
/// read-only, needs every window of the agent to be gone, and never invents
/// a receipt. Project access is a grant, not a directory.
#[tokio::test]
async fn transfer_needs_acceptance_and_recovery_is_read_only() {
    let h = require_db!("t_conversations_transfer");
    let token = seed_agent(&h.pool, "acme", "joaquin").await;
    let dani_token = seed_agent(&h.pool, "acme", "dani").await;
    enable_conversations(&h.pool, "acme").await;

    // A project thread: access is a grant, and a teammate without one sees
    // nothing even though the thread is not private.
    let owner = connect_with_session(&h.base, &token, "market-data").await;
    call(&owner, "create_project", json!({"project": "market-data"})).await;
    let convo = call(
        &owner,
        "create_conversation",
        json!({"title": "feed rewrite", "project": "market-data"}),
    )
    .await;
    let cid = convo["id"].as_str().unwrap().to_owned();
    let first = call(
        &owner,
        "send_conversation_message",
        json!({"conversation_id": cid, "body": "starting on the feed", "request_id": request_id()}),
    )
    .await;
    let first_id = first["message_id"].as_str().unwrap().to_owned();

    let dani = connect_with_session(&h.base, &dani_token, "core").await;
    let err = call_expect_error(&dani, "read_conversation", json!({"conversation_id": cid})).await;
    assert!(
        err.contains("no such conversation"),
        "a project grant is required: {err}"
    );
    call(
        &owner,
        "grant_project_access",
        json!({"project": "market-data", "agent": "dani"}),
    )
    .await;
    let seen = call(&dani, "read_conversation", json!({"conversation_id": cid})).await;
    assert_eq!(seen["messages"].as_array().unwrap().len(), 1, "{seen}");
    // Project visibility is reading, not membership: there is nothing for a
    // project reader to acknowledge.
    let err = call_expect_error(&dani, "ack_message", json!({"message_id": first_id})).await;
    assert!(err.contains("active member"), "{err}");
    // And revoking takes effect at once.
    call(
        &owner,
        "grant_project_access",
        json!({"project": "market-data", "agent": "dani", "grant": false}),
    )
    .await;
    let err = call_expect_error(&dani, "read_conversation", json!({"conversation_id": cid})).await;
    assert!(err.contains("no such conversation"), "{err}");

    // Transfer: the owner moves its seat to another window of its own agent.
    let successor = connect_with_session(&h.base, &token, "market-data-2").await;
    let err = call_expect_error(
        &owner,
        "transfer_membership",
        json!({"conversation_id": cid, "to": "dani/core"}),
    )
    .await;
    assert!(
        err.contains("same agent"),
        "a transfer is not an invitation: {err}"
    );

    let proposal = call(
        &owner,
        "transfer_membership",
        json!({"conversation_id": cid, "to": "joaquin/market-data-2"}),
    )
    .await;
    assert_eq!(proposal["state"], "proposed");
    // Nothing has moved yet: the original seat is still active and the
    // successor is only invited.
    let before = call(&owner, "list_conversations", json!({})).await;
    let entry = before["conversations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["id"] == cid.as_str())
        .unwrap();
    assert_eq!(entry["membership"]["state"], "active");
    assert_eq!(
        entry["members"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["address"] == "joaquin/market-data-2")
            .unwrap()["state"],
        "invited"
    );

    call(
        &successor,
        "join_conversation",
        json!({"conversation_id": cid}),
    )
    .await;
    let after = call(&successor, "list_conversations", json!({})).await;
    let mine = after["conversations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["id"] == cid.as_str())
        .expect("the successor holds the seat");
    assert_eq!(
        mine["membership"]["role"], "owner",
        "the role travelled: {mine}"
    );
    assert_eq!(mine["membership"]["state"], "active");
    let superseded = mine["members"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["address"] == "joaquin/market-data")
        .unwrap();
    assert_eq!(
        superseded["state"], "left",
        "the old seat is superseded: {superseded}"
    );
    // Authorship is untouched: the first message is still from the old window.
    let msg = call(
        &successor,
        "get_conversation_message",
        json!({"message_id": first_id}),
    )
    .await;
    assert_eq!(msg["from_address"], "joaquin/market-data");

    // Recovery: refused while a window is live, refused with a session
    // credential, and read-only when every window is gone.
    let cred = call(
        &owner,
        "register_session",
        json!({"session": "market-data"}),
    )
    .await;
    let window = connect(&h.base, cred["session_token"].as_str().unwrap()).await;
    let err = call_expect_error(
        &window,
        "recover_conversation_history",
        json!({"conversation_id": cid}),
    )
    .await;
    assert!(err.contains("agent token"), "{err}");
    let err = call_expect_error(
        &owner,
        "recover_conversation_history",
        json!({"conversation_id": cid}),
    )
    .await;
    assert!(err.contains("still live"), "offline is not enough: {err}");

    call(&window, "revoke_session", json!({})).await;
    let recovered = call(
        &owner,
        "recover_conversation_history",
        json!({"conversation_id": cid}),
    )
    .await;
    assert!(!recovered["messages"].as_array().unwrap().is_empty());
    assert!(
        recovered["messages"][0]["my_receipt"].is_null(),
        "recovery observes nothing: {recovered}"
    );
    // It is audited, and it granted nothing.
    let (recoveries,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM conversation_audit WHERE action = 'member.recover'")
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert_eq!(recoveries, 1);

    for c in [owner, dani, successor] {
        let _ = c.cancel().await;
    }
    h.shutdown().await;
}

/// Three holes the owner reproduced against a real server, and the shape of
/// their fixes: a window's seat needs that window's credential, a private
/// thread is not a project thread, and a receipt is not a way around the
/// history boundary.
#[tokio::test]
async fn a_private_thread_answers_only_to_the_windows_that_are_in_it() {
    let h = require_db!("t_conversation_boundaries");
    let token = seed_agent(&h.pool, "acme", "joaquin").await;
    let dani_token = seed_agent(&h.pool, "acme", "dani").await;
    let marta_token = seed_agent(&h.pool, "acme", "marta").await;
    enable_conversations(&h.pool, "acme").await;

    // A registered window: the credential, not the label, is what proves it.
    let agent = connect(&h.base, &dani_token).await;
    let cred = call(&agent, "register_session", json!({"session": "review"})).await;
    let window = connect(&h.base, cred["session_token"].as_str().unwrap()).await;
    let owner = connect_with_session(&h.base, &token, "impl").await;

    let convo = call(
        &owner,
        "create_conversation",
        json!({"title": "who is in the room", "private": true, "invite": ["dani/review"]}),
    )
    .await;
    let cid = convo["id"].as_str().unwrap().to_owned();

    // The parent agent token cannot take its own window's seat, even wearing
    // the right label. It is told what to do instead.
    let impostor = connect_with_session(&h.base, &dani_token, "review").await;
    let err = call_expect_error(
        &impostor,
        "join_conversation",
        json!({"conversation_id": cid}),
    )
    .await;
    assert!(err.contains("registered window"), "{err}");
    assert!(err.contains("recover_conversation_history"), "{err}");

    // An invitation is not membership, and it does not read what was said
    // before it was answered.
    let waiting = call(&window, "list_conversations", json!({})).await;
    assert!(
        waiting["conversations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["id"] == cid.as_str()),
        "an invitee can see that it was invited: {waiting}"
    );
    let err = call_expect_error(
        &window,
        "read_conversation",
        json!({"conversation_id": cid}),
    )
    .await;
    assert!(err.contains("join_conversation first"), "{err}");

    call(
        &window,
        "join_conversation",
        json!({"conversation_id": cid}),
    )
    .await;
    let sent = call(
        &owner,
        "send_conversation_message",
        json!({"conversation_id": cid, "body": "the private body", "request_id": request_id()}),
    )
    .await;

    // Now the seat is held by a window. The same agent token with the same
    // label is a different caller and sees nothing.
    let err = call_expect_error(
        &impostor,
        "read_conversation",
        json!({"conversation_id": cid}),
    )
    .await;
    assert!(err.contains("no such conversation"), "{err}");
    let err = call_expect_error(
        &impostor,
        "get_conversation_message",
        json!({"message_id": sent["message_id"]}),
    )
    .await;
    assert!(err.contains("no such conversation"), "{err}");
    // The window itself still reads it.
    let read = call(
        &window,
        "read_conversation",
        json!({"conversation_id": cid}),
    )
    .await;
    assert_eq!(read["messages"][0]["body"], "the private body");

    // Inviting an already active member again does not unbind the seat.
    // The state is preserved, so the binding must be too, or a repeated
    // invitation would quietly hand the label back to the parent token.
    call(
        &owner,
        "invite_to_conversation",
        json!({"conversation_id": cid, "address": "dani/review"}),
    )
    .await;
    let err = call_expect_error(
        &impostor,
        "read_conversation",
        json!({"conversation_id": cid}),
    )
    .await;
    assert!(
        err.contains("no such conversation"),
        "a repeated invitation must not downgrade a protected seat: {err}"
    );
    let read = call(
        &window,
        "read_conversation",
        json!({"conversation_id": cid}),
    )
    .await;
    assert_eq!(
        read["messages"][0]["body"], "the private body",
        "and the window still reads it"
    );

    // Private and project are exclusive, and asking for both is refused
    // rather than quietly resolved one way.
    call(&owner, "create_project", json!({"project": "market-data"})).await;
    let err = call_expect_error(
        &owner,
        "create_conversation",
        json!({"title": "both", "private": true, "project": "market-data"}),
    )
    .await;
    assert!(
        err.contains("either private or visible to a project"),
        "{err}"
    );

    // And a project grant does not reach into a private thread that merely
    // names the project.
    call(
        &owner,
        "grant_project_access",
        json!({"project": "market-data", "agent": "marta"}),
    )
    .await;
    let marta = connect_with_session(&h.base, &marta_token, "review").await;
    sqlx::query(
        "UPDATE conversations SET project_id = (SELECT id FROM projects WHERE name = 'market-data')
          WHERE id = $1",
    )
    .bind(cid.parse::<Uuid>().unwrap())
    .execute(&h.pool)
    .await
    .unwrap();
    let err = call_expect_error(&marta, "read_conversation", json!({"conversation_id": cid})).await;
    assert!(
        err.contains("no such conversation"),
        "a private thread stays private however it is labelled: {err}"
    );

    // A late member is refused the earlier message — and its receipts, which
    // carry recipient addresses and a free-text note that is often the
    // discussion itself.
    call(
        &window,
        "ack_message",
        json!({"message_id": sent["message_id"], "resolved": true,
               "note": "the note is part of the conversation"}),
    )
    .await;
    call(
        &owner,
        "invite_to_conversation",
        json!({"conversation_id": cid, "address": "marta/review"}),
    )
    .await;
    call(&marta, "join_conversation", json!({"conversation_id": cid})).await;
    let err = call_expect_error(
        &marta,
        "get_conversation_message",
        json!({"message_id": sent["message_id"]}),
    )
    .await;
    assert!(
        err.contains("before the point your membership starts"),
        "{err}"
    );
    let err = call_expect_error(
        &marta,
        "get_message_receipts",
        json!({"message_id": sent["message_id"]}),
    )
    .await;
    assert!(
        err.contains("not yours to read either"),
        "the receipts of a message you may not read are not a way around it: {err}"
    );
    // The sender still reads them, of course.
    let receipts = call(
        &owner,
        "get_message_receipts",
        json!({"message_id": sent["message_id"]}),
    )
    .await;
    assert_eq!(receipts["acknowledged"], 1);

    // Revoking the window does not hand its label back. Revocation is a way
    // out, not a way in: the parent token is still refused afterwards, and
    // the audited recovery path is what an agent has instead.
    call(&window, "revoke_session", json!({})).await;
    let err = call_expect_error(
        &impostor,
        "read_conversation",
        json!({"conversation_id": cid}),
    )
    .await;
    assert!(err.contains("no such conversation"), "{err}");
    let err = call_expect_error(
        &impostor,
        "join_conversation",
        json!({"conversation_id": cid}),
    )
    .await;
    assert!(err.contains("registered window"), "{err}");
    assert!(err.contains("does not hand the label back"), "{err}");

    for c in [owner, window, agent, impostor, marta] {
        let _ = c.cancel().await;
    }
    h.shutdown().await;
}

// -------------------------------------------------- backend and the outbox --

/// The failure shape every external store introduces, exercised with
/// Postgres as the only backend: acceptance and persistence are two events,
/// the second can fail, time out, or succeed without the caller hearing.
/// Leases, fencing, bounded retries, idempotency and reconciliation, with no
/// broker installed.
#[tokio::test]
async fn the_outbox_survives_failures_between_acceptance_and_confirmation() {
    use ai_crew_sync::store::backend::{Faults, MessagingBackend, PostgresBackend, Published};
    use ai_crew_sync::store::outbox::{self, Settled};

    let h = require_db!("t_backend_outbox");
    let token = seed_agent(&h.pool, "acme", "joaquin").await;
    let dani_token = seed_agent(&h.pool, "acme", "dani").await;
    enable_conversations(&h.pool, "acme").await;
    let owner = connect_with_session(&h.base, &token, "impl").await;
    let dani = connect_with_session(&h.base, &dani_token, "review").await;

    let convo = call(
        &owner,
        "create_conversation",
        json!({"title": "async thread", "private": true, "invite": ["dani/review"]}),
    )
    .await;
    let cid = convo["id"].as_str().unwrap().to_owned();
    call(&dani, "join_conversation", json!({"conversation_id": cid})).await;

    // Opt this conversation into asynchronous publication. The default is
    // synchronous and untouched.
    let cuuid: Uuid = cid.parse().unwrap();
    outbox::set_publication(&h.pool, cuuid, true).await.unwrap();

    // Acceptance is not storage, and the caller is told so.
    let rid = request_id();
    let sent = call(
        &owner,
        "send_conversation_message",
        json!({"conversation_id": cid, "body": "published later", "request_id": rid}),
    )
    .await;
    assert_eq!(sent["stored"], false, "accepted, not yet stored: {sent}");
    let mid: Uuid = sent["message_id"].as_str().unwrap().parse().unwrap();
    let receipts = call(
        &owner,
        "get_message_receipts",
        json!({"message_id": sent["message_id"]}),
    )
    .await;
    assert!(
        receipts["receipts"][0]["stored_at"].is_null(),
        "stored is not claimed before the backend confirmed: {receipts}"
    );
    let status = outbox::status(&h.pool, team_id(&h.pool, "acme").await)
        .await
        .unwrap();
    assert_eq!(status.pending, 1);
    assert!(status.pending_bytes > 0);

    // A retryable failure backs off and keeps the slot.
    let flaky = PostgresBackend::with_faults(
        h.pool.clone(),
        Faults {
            retryable: 1,
            ..Default::default()
        },
    );
    let outcome = outbox::run_once(&h.pool, &flaky, "worker-a").await.unwrap();
    assert!(
        matches!(outcome, Some(Settled::Retrying { .. })),
        "{outcome:?}"
    );
    let (state,): (String,) =
        sqlx::query_as("SELECT publication_state FROM conversation_messages WHERE id = $1")
            .bind(mid)
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert_eq!(
        state, "pending_publication",
        "a retry does not fabricate stored"
    );

    // A client retry of the same request_id gets the original's real
    // state, not a storage confirmation the first call never got.
    let again = call(
        &owner,
        "send_conversation_message",
        json!({"conversation_id": cid, "body": "published later", "request_id": rid}),
    )
    .await;
    assert_eq!(again["message_id"], sent["message_id"]);
    assert_eq!(
        again["stored"], false,
        "the retry reports what the message is, not what a synchronous send would be"
    );

    // And a reader is told what it is looking at. The message keeps its
    // place in the sequence and says it is not stored yet, which is what a
    // cursor needs to not walk over a gap it never knew about.
    let page = call(&dani, "read_conversation", json!({"conversation_id": cid})).await;
    let pending = page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["message_id"] == sent["message_id"])
        .expect("the message is in the thread, not hidden from it");
    assert_eq!(pending["publication"], "pending_publication");
    let one = call(
        &dani,
        "get_conversation_message",
        json!({"message_id": sent["message_id"]}),
    )
    .await;
    assert_eq!(
        one["publication"], "pending_publication",
        "never reported as stored before the backend says so: {one}"
    );

    // Fencing: a worker whose lease expired settles nothing. Take a lease,
    // let another worker take it over, then try to settle the stale one.
    sqlx::query("UPDATE conversation_outbox SET next_attempt_at = now()")
        .execute(&h.pool)
        .await
        .unwrap();
    let stale = outbox::lease(&h.pool, "worker-stale")
        .await
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE conversation_outbox SET lease_expires_at = now() - interval '1 minute'")
        .execute(&h.pool)
        .await
        .unwrap();
    let fresh = outbox::lease(&h.pool, "worker-b").await.unwrap().unwrap();
    assert!(fresh.generation > stale.generation);
    let fenced = outbox::settle(
        &h.pool,
        &stale,
        Published::Confirmed(ai_crew_sync::store::backend::Locator(mid.to_string())),
    )
    .await
    .unwrap();
    assert_eq!(fenced, Settled::Fenced, "a stale worker must write nothing");
    let (state,): (String,) =
        sqlx::query_as("SELECT publication_state FROM conversation_messages WHERE id = $1")
            .bind(mid)
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert_eq!(state, "pending_publication");

    // Uncertain completion: the body was written and the confirmation lost.
    // Reconciliation asks the backend what it actually holds and settles.
    let losing = PostgresBackend::with_faults(
        h.pool.clone(),
        Faults {
            lose_confirmation: true,
            ..Default::default()
        },
    );
    let outcome = losing
        .publish(ai_crew_sync::store::backend::Envelope {
            message_id: fresh.message_id,
            conversation_id: fresh.conversation_id,
            team_id: fresh.team_id,
            body: fresh.payload.clone(),
            publish_key: fresh.publish_key,
        })
        .await;
    assert!(matches!(outcome, Published::Retryable(_)));
    let plain = PostgresBackend::new(h.pool.clone());
    let settled = outbox::reconcile(&h.pool, &plain, &fresh).await.unwrap();
    assert_eq!(
        settled,
        Settled::Stored,
        "the write did land; reconcile found it"
    );

    // Now it is stored, once, and the receipts say when.
    let (state, locator): (String, Option<String>) = sqlx::query_as(
        "SELECT publication_state, canonical_locator FROM conversation_messages WHERE id = $1",
    )
    .bind(mid)
    .fetch_one(&h.pool)
    .await
    .unwrap();
    assert_eq!(state, "stored");
    assert_eq!(locator.as_deref(), Some(mid.to_string().as_str()));
    let receipts = call(
        &owner,
        "get_message_receipts",
        json!({"message_id": sent["message_id"]}),
    )
    .await;
    assert!(
        receipts["receipts"][0]["stored_at"].is_string(),
        "{receipts}"
    );
    assert!(
        outbox::lease(&h.pool, "worker-c").await.unwrap().is_none(),
        "the slot is gone"
    );

    // Idempotency holds across the async path too: the same request id
    // returns the original message rather than queueing a second.
    let rid = request_id();
    let a = call(
        &owner,
        "send_conversation_message",
        json!({"conversation_id": cid, "body": "twice", "request_id": rid}),
    )
    .await;
    let b = call(
        &owner,
        "send_conversation_message",
        json!({"conversation_id": cid, "body": "twice", "request_id": rid}),
    )
    .await;
    assert_eq!(a["message_id"], b["message_id"]);
    assert_eq!(a["seq"], b["seq"]);
    let (slots,): (i64,) = sqlx::query_as("SELECT count(*) FROM conversation_outbox")
        .fetch_one(&h.pool)
        .await
        .unwrap();
    assert_eq!(slots, 1, "one logical message, one slot");

    // A fatal failure is explicit and keeps the slot visible, rather than
    // retrying for ever or pretending it stored.
    let doomed = PostgresBackend::with_faults(
        h.pool.clone(),
        Faults {
            fatal: true,
            ..Default::default()
        },
    );
    sqlx::query("UPDATE conversation_outbox SET next_attempt_at = now()")
        .execute(&h.pool)
        .await
        .unwrap();
    let outcome = outbox::run_once(&h.pool, &doomed, "worker-d")
        .await
        .unwrap();
    assert_eq!(outcome, Some(Settled::Failed));
    let team = team_id(&h.pool, "acme").await;
    let status = outbox::status(&h.pool, team).await.unwrap();
    assert_eq!(status.failed, 1);
    assert_eq!(status.pending, 0);
    let mid2: Uuid = a["message_id"].as_str().unwrap().parse().unwrap();
    let (state,): (String,) =
        sqlx::query_as("SELECT publication_state FROM conversation_messages WHERE id = $1")
            .bind(mid2)
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert_eq!(state, "failed", "an explicit failed slot, not a silent gap");

    // Returning to synchronous mode is refused while work is *outstanding*,
    // because a thread would keep a gap nobody drains. A failed slot is
    // settled — explicit, visible, not coming back — so it does not block.
    let pending = call(
        &owner,
        "send_conversation_message",
        json!({"conversation_id": cid, "body": "still queued", "request_id": request_id()}),
    )
    .await;
    assert_eq!(pending["stored"], false);
    let err = outbox::set_publication(&h.pool, cuuid, false)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("still awaiting publication"), "{err}");
    sqlx::query("UPDATE conversation_outbox SET next_attempt_at = now() WHERE state = 'pending'")
        .execute(&h.pool)
        .await
        .unwrap();
    assert_eq!(
        outbox::run_once(&h.pool, &plain, "worker-e").await.unwrap(),
        Some(Settled::Stored)
    );
    // The failed slot is still there and still does not block the switch.
    assert_eq!(outbox::status(&h.pool, team).await.unwrap().failed, 1);
    outbox::set_publication(&h.pool, cuuid, false)
        .await
        .unwrap();
    let sync_sent = call(
        &owner,
        "send_conversation_message",
        json!({"conversation_id": cid, "body": "back to sync", "request_id": request_id()}),
    )
    .await;
    assert_eq!(sync_sent["stored"], true, "the default path is unchanged");

    for c in [owner, dani] {
        let _ = c.cancel().await;
    }
    h.shutdown().await;
}

/// The team id, for the store-level calls above.
async fn team_id(pool: &PgPool, slug: &str) -> Uuid {
    sqlx::query_scalar::<_, Uuid>("SELECT id FROM teams WHERE slug = $1")
        .bind(slug)
        .fetch_one(pool)
        .await
        .unwrap()
}

// -------------------------------------------------- the JetStream adapter --

/// The broker fixture. Required from phase 4: a skipped broker test proves
/// nothing and reads like a pass, so a missing broker fails visibly.
fn nats_url() -> String {
    match std::env::var("TEST_NATS_URL") {
        Ok(url) if !url.trim().is_empty() => url,
        _ => panic!(
            "TEST_NATS_URL is not set. From phase 4 the real JetStream fixture is required: \
             run `make test`, which starts it, or set TEST_NATS_URL yourself. This is a \
             failure rather than a skip on purpose."
        ),
    }
}

/// The adapter contract against a real broker: publish and await the PubAck,
/// read the body back, refuse another team's locator, deduplicate on the
/// publish key, carry a body at the contract's ceiling, and tell a full
/// stream apart from a timeout.
#[tokio::test]
async fn the_jetstream_adapter_holds_its_contract_against_a_real_broker() {
    use ai_crew_sync::store::backend::{Envelope, Locator, MessagingBackend, Published};
    use ai_crew_sync::store::jetstream::{self, Config, JetStreamBackend};

    // Small ceilings: the fixture's broker has a small store, and the
    // quota behaviour is the same at any size.
    let config = Config::new(nats_url()).with_limits(1_000, 16 * 1024 * 1024);
    let team = Uuid::new_v4();
    let other_team = Uuid::new_v4();
    let conversation = Uuid::new_v4();

    // Provisioning is an operator action, and the runtime path refuses to
    // start against a team that has not had it done.
    let err = match JetStreamBackend::connect(&config, team).await {
        Err(e) => e.to_string(),
        Ok(_) => panic!("connecting to an unprovisioned team must fail"),
    };
    assert!(err.contains("does not exist"), "{err}");
    assert!(err.contains("Provision it first"), "{err}");

    let stream = JetStreamBackend::provision(&config, team).await.unwrap();
    assert_eq!(stream, jetstream::stream_name(team));
    JetStreamBackend::provision(&config, other_team)
        .await
        .unwrap();
    let backend = JetStreamBackend::connect(&config, team).await.unwrap();
    let theirs = JetStreamBackend::connect(&config, other_team)
        .await
        .unwrap();

    // A publish is stored when the PubAck says so, and the locator reads it
    // back.
    let key = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let envelope = Envelope {
        message_id,
        conversation_id: conversation,
        team_id: team,
        body: "the empty state needs a spinner".into(),
        publish_key: key,
    };
    let locator = match backend.publish(envelope.clone()).await {
        Published::Confirmed(l) => l,
        other => panic!("expected a PubAck: {other:?}"),
    };
    assert!(locator.0.starts_with("jetstream:"), "{locator:?}");
    assert_eq!(
        backend
            .fetch(&locator, message_id)
            .await
            .unwrap()
            .as_deref(),
        Some("the empty state needs a spinner")
    );

    // Another team cannot read it even holding the locator. Three
    // independent reasons and any of them is a pass: the locator names a
    // stream that is not theirs, that stream would not hold the sequence
    // anyway, and the envelope's team is checked rather than trusted if it
    // ever got that far.
    match theirs.fetch(&locator, message_id).await {
        Ok(None) => {}
        Err(e) => {
            let why = e.to_string();
            assert!(
                why.contains("another stream") || why.contains("another team"),
                "{why}"
            );
        }
        Ok(Some(body)) => panic!("another team read a body it must not see: {body}"),
    }

    // Idempotency: the same publish key inside the dedup window returns the
    // same sequence rather than storing twice.
    let again = match backend.publish(envelope.clone()).await {
        Published::Confirmed(l) => l,
        other => panic!("{other:?}"),
    };
    assert_eq!(again, locator, "a retry must not duplicate the body");
    // Reconciliation answers the uncertain case without inventing a second
    // logical message, and without writing a probe that would take the key
    // the body needs.
    assert_eq!(
        backend.reconcile(&envelope).await.unwrap(),
        Some(locator.clone()),
        "reconcile must find a key the broker has seen"
    );
    // A key it has never seen lands now, under that same key, and answers
    // with where it went. One logical message, and reconciling twice does
    // not make it two.
    let unseen = Envelope {
        message_id: Uuid::new_v4(),
        conversation_id: conversation,
        team_id: team,
        body: "reconciled into existence".into(),
        publish_key: Uuid::new_v4(),
    };
    let landed = backend.reconcile(&unseen).await.unwrap().unwrap();
    assert_eq!(
        backend
            .fetch(&landed, unseen.message_id)
            .await
            .unwrap()
            .as_deref(),
        Some("reconciled into existence"),
        "the locator must name the body, not an empty probe"
    );
    assert_eq!(
        backend.reconcile(&unseen).await.unwrap(),
        Some(landed),
        "reconciling twice is still one message"
    );

    // An envelope for another team is refused rather than written into this
    // team's stream, where the team it belongs to could never read it.
    let stranger = backend
        .publish(Envelope {
            message_id: Uuid::new_v4(),
            conversation_id: conversation,
            team_id: Uuid::new_v4(),
            body: "not mine".into(),
            publish_key: Uuid::new_v4(),
        })
        .await;
    assert!(matches!(stranger, Published::Fatal(_)), "{stranger:?}");

    // A locator for a different message of the same team is refused too. A
    // sequence in the right stream is not proof that it is the right body.
    let err = backend
        .fetch(&locator, Uuid::new_v4())
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("another message"), "{err}");

    // A locator naming another stream is refused before it reaches the
    // broker, rather than read as that sequence of this one.
    let forged = Locator(format!("jetstream:ACS_T_{}:1", Uuid::new_v4().simple()));
    let err = backend
        .fetch(&forged, Uuid::new_v4())
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("another stream"), "{err}");

    // The 1 MiB body contract still fits once headers and framing are added
    // — but only because the fixture raises the server's own max_payload as
    // well as the stream's ceiling. With the server default of 1 MiB this
    // publish is refused by a couple of hundred bytes of headers, which is
    // why the Makefile passes --max_payload 2MB and the constant says so.
    let big = "x".repeat(1024 * 1024);
    let outcome = backend
        .publish(Envelope {
            message_id: Uuid::new_v4(),
            conversation_id: conversation,
            team_id: team,
            body: big.clone(),
            publish_key: Uuid::new_v4(),
        })
        .await;
    assert!(
        matches!(outcome, Published::Confirmed(_)),
        "a body at the contract's ceiling must fit: {outcome:?}"
    );
    // And one past the broker's limit is fatal, not retried for ever.
    let too_big = "x".repeat(jetstream::MAX_BROKER_MESSAGE_BYTES as usize + 1);
    let outcome = backend
        .publish(Envelope {
            message_id: Uuid::new_v4(),
            conversation_id: conversation,
            team_id: team,
            body: too_big,
            publish_key: Uuid::new_v4(),
        })
        .await;
    assert!(matches!(outcome, Published::Fatal(_)), "{outcome:?}");

    // A sequence this stream does not hold is absent, not an error. A
    // locator naming another stream, or no stream at all, is refused rather
    // than guessed at.
    assert!(
        backend
            .fetch(
                &Locator(format!("jetstream:{}:999999", backend.stream())),
                Uuid::new_v4(),
            )
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        backend
            .fetch(&Locator("jetstream:ACS_T_x:1".into()), Uuid::new_v4())
            .await
            .is_err()
    );
    assert!(
        backend
            .fetch(&Locator("nonsense".into()), Uuid::new_v4())
            .await
            .is_err()
    );

    // A broker that is not there fails retryably rather than hanging.
    let unreachable = Config::new("nats://127.0.0.1:1");
    assert!(
        JetStreamBackend::connect(&unreachable, team).await.is_err(),
        "an unreachable broker must fail, not block"
    );

    // Clean up after ourselves: streams and their bodies are per test.
    JetStreamBackend::deprovision(&config, team).await.unwrap();
    JetStreamBackend::deprovision(&config, other_team)
        .await
        .unwrap();
}

/// The default install does not need a broker: an ordinary team's
/// conversations are Postgres-backed and untouched by anything above.
#[tokio::test]
async fn the_default_backend_stays_postgres_with_no_broker_involved() {
    let h = require_db!("t_default_backend");
    let token = seed_agent(&h.pool, "acme", "joaquin").await;
    enable_conversations(&h.pool, "acme").await;
    let client = connect_with_session(&h.base, &token, "impl").await;
    let convo = call(
        &client,
        "create_conversation",
        json!({"title": "ordinary", "private": true}),
    )
    .await;
    let (backend, publication): (String, String) =
        sqlx::query_as("SELECT backend, publication FROM conversations WHERE id = $1")
            .bind(convo["id"].as_str().unwrap().parse::<Uuid>().unwrap())
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert_eq!(backend, "postgres");
    assert_eq!(publication, "sync");
    let sent = call(
        &client,
        "send_conversation_message",
        json!({"conversation_id": convo["id"], "body": "no broker here",
               "request_id": request_id()}),
    )
    .await;
    assert_eq!(sent["stored"], true);
    let _ = client.cancel().await;
    h.shutdown().await;
}

/// Phase 5: a conversation routed to JetStream publishes through the outbox
/// to a real broker, reports `stored` only on a PubAck, survives a process
/// death mid-publication, reads its history back by locator, and revalidates
/// access at the moment the body is served.
#[tokio::test]
async fn opted_in_conversations_publish_to_jetstream_and_read_back() {
    use ai_crew_sync::store::jetstream::{Config, JetStreamBackend};
    use ai_crew_sync::store::{conversations as convo_store, outbox};

    let h = require_db_broker!("t_jetstream_publication");
    let token = seed_agent(&h.pool, "acme", "joaquin").await;
    let dani_token = seed_agent(&h.pool, "acme", "dani").await;
    enable_conversations(&h.pool, "acme").await;
    let team = team_id(&h.pool, "acme").await;

    // Routing is an operator decision, per team, and it only affects
    // conversations created afterwards.
    let before = connect_with_session(&h.base, &token, "before").await;
    let legacy = call(
        &before,
        "create_conversation",
        json!({"title": "postgres thread", "private": true}),
    )
    .await;
    sqlx::query("UPDATE teams SET default_backend = 'jetstream' WHERE id = $1")
        .bind(team)
        .execute(&h.pool)
        .await
        .unwrap();

    let config = Config::new(nats_url()).with_limits(1_000, 16 * 1024 * 1024);
    JetStreamBackend::provision(&config, team).await.unwrap();
    let backend = JetStreamBackend::connect(&config, team).await.unwrap();
    // The same view of the backends the server has, for the reads this test
    // makes directly against the store.
    let backends =
        ai_crew_sync::store::routing::Backends::with_jetstream(h.pool.clone(), config.clone());

    let owner = connect_with_session(&h.base, &token, "impl").await;
    let dani = connect_with_session(&h.base, &dani_token, "review").await;
    let convo = call(
        &owner,
        "create_conversation",
        json!({"title": "jetstream thread", "private": true, "invite": ["dani/review"]}),
    )
    .await;
    let cid = convo["id"].as_str().unwrap().to_owned();
    let cuuid: Uuid = cid.parse().unwrap();
    call(&dani, "join_conversation", json!({"conversation_id": cid})).await;

    // The earlier conversation kept Postgres; the new one is routed.
    let (old_backend,): (String,) =
        sqlx::query_as("SELECT backend FROM conversations WHERE id = $1")
            .bind(legacy["id"].as_str().unwrap().parse::<Uuid>().unwrap())
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert_eq!(old_backend, "postgres", "existing threads are not migrated");
    let (new_backend, publication): (String, String) =
        sqlx::query_as("SELECT backend, publication FROM conversations WHERE id = $1")
            .bind(cuuid)
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert_eq!(
        (new_backend.as_str(), publication.as_str()),
        ("jetstream", "outbox")
    );

    // Acceptance is not storage, and the receipts do not claim it is.
    let sent = call(
        &owner,
        "send_conversation_message",
        json!({"conversation_id": cid, "body": "through the broker",
               "request_id": request_id()}),
    )
    .await;
    assert_eq!(sent["stored"], false);
    let mid: Uuid = sent["message_id"].as_str().unwrap().parse().unwrap();
    let receipts = call(
        &owner,
        "get_message_receipts",
        json!({"message_id": sent["message_id"]}),
    )
    .await;
    assert!(receipts["receipts"][0]["stored_at"].is_null(), "{receipts}");

    // A worker dies mid-publication: the outcome is unknown, and that is
    // neither stored nor failed until reconciliation says so.
    let lease = outbox::lease(&h.pool, "worker-dead")
        .await
        .unwrap()
        .unwrap();
    let settled = outbox::mark_uncertain(&h.pool, &lease, "the process died")
        .await
        .unwrap();
    assert!(matches!(settled, outbox::Settled::Retrying { .. }));
    let (state, uncertain): (String, Option<chrono::DateTime<chrono::Utc>>) = sqlx::query_as(
        "SELECT publication_state, uncertain_at FROM conversation_messages WHERE id = $1",
    )
    .bind(mid)
    .fetch_one(&h.pool)
    .await
    .unwrap();
    assert_eq!(state, "pending_publication");
    assert!(
        uncertain.is_some(),
        "an unknown outcome is recorded as unknown"
    );

    // Reconciliation asks the broker under the same key instead of
    // guessing: inside its dedup window that answers with the original
    // sequence, outside it the body lands now. One logical message either
    // way, and only now is it stored.
    assert_eq!(
        outbox::resolve_uncertain(&h.pool, &backend, team)
            .await
            .unwrap(),
        1
    );
    let (uncertain,): (Option<chrono::DateTime<chrono::Utc>>,) =
        sqlx::query_as("SELECT uncertain_at FROM conversation_messages WHERE id = $1")
            .bind(mid)
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert!(
        uncertain.is_none(),
        "an answered question is no longer open"
    );
    let (state, locator): (String, Option<String>) = sqlx::query_as(
        "SELECT publication_state, canonical_locator FROM conversation_messages WHERE id = $1",
    )
    .bind(mid)
    .fetch_one(&h.pool)
    .await
    .unwrap();
    assert_eq!(state, "stored");
    assert!(
        locator.as_deref().unwrap().starts_with("jetstream:"),
        "{locator:?}"
    );
    let receipts = call(
        &owner,
        "get_message_receipts",
        json!({"message_id": sent["message_id"]}),
    )
    .await;
    assert!(
        receipts["receipts"][0]["stored_at"].is_string(),
        "stored once the PubAck arrived, not before: {receipts}"
    );

    // History reads through the ordinary tool, from the broker, with the
    // body's standing stated rather than implied.
    let history = call(&dani, "read_conversation", json!({"conversation_id": cid})).await;
    assert_eq!(history["messages"].as_array().unwrap().len(), 1);
    assert_eq!(history["messages"][0]["publication"], "stored");

    // The temporary local copy is released, and history then reads from the
    // broker by locator.
    assert_eq!(
        outbox::release_published_bodies(&h.pool, Some(cuuid), 0)
            .await
            .unwrap(),
        1
    );
    let (local,): (String,) =
        sqlx::query_as("SELECT body FROM conversation_messages WHERE id = $1")
            .bind(mid)
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert!(local.is_empty(), "the body is the broker's now");
    assert_eq!(
        convo_store::body_of(&h.pool, &backends, mid).await.unwrap(),
        "through the broker"
    );

    // One logical message, not two, and its body came from the broker.
    let history = call(&dani, "read_conversation", json!({"conversation_id": cid})).await;
    assert_eq!(
        history["messages"].as_array().unwrap().len(),
        1,
        "a retained body is one message, not a second copy: {history}"
    );
    assert_eq!(history["messages"][0]["body"], "through the broker");

    // A pending publication is neither hidden nor reported as stored, and
    // an earlier pending message does not let a reader walk past it.
    let pending = call(
        &owner,
        "send_conversation_message",
        json!({"conversation_id": cid, "body": "still in flight",
               "request_id": request_id()}),
    )
    .await;
    let _later = call(
        &owner,
        "send_conversation_message",
        json!({"conversation_id": cid, "body": "sent after it",
               "request_id": request_id()}),
    )
    .await;
    let history = call(&dani, "read_conversation", json!({"conversation_id": cid})).await;
    let msgs = history["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 3);
    assert_eq!(msgs[1]["publication"], "pending_publication");
    assert_eq!(
        msgs[1]["body"], "still in flight",
        "the body is still local"
    );
    assert!(msgs[1]["seq"].as_i64().unwrap() < msgs[2]["seq"].as_i64().unwrap());
    // And the cursor stops before it. Following the advertised
    // next_after_seq must not step over a sequence that is about to fill.
    let pending_seq = msgs[1]["seq"].as_i64().unwrap();
    let paged = call(
        &dani,
        "read_conversation",
        json!({"conversation_id": cid, "limit": 2}),
    )
    .await;
    assert!(
        paged["next_after_seq"]
            .as_i64()
            .is_none_or(|n| n < pending_seq),
        "the cursor advanced past a message still in flight: {paged}"
    );

    // Out-of-order completion: the later message is published first. The
    // thread keeps its own order, which is the sequence, not the order the
    // broker happened to confirm in.
    let first = outbox::lease(&h.pool, "worker-a").await.unwrap().unwrap();
    let second = outbox::lease(&h.pool, "worker-b").await.unwrap().unwrap();
    assert_eq!(first.message_id.to_string(), pending["message_id"]);
    assert_eq!(
        outbox::publish_leased(&h.pool, &backend, &second)
            .await
            .unwrap(),
        outbox::Settled::Stored
    );
    assert_eq!(
        outbox::publish_leased(&h.pool, &backend, &first)
            .await
            .unwrap(),
        outbox::Settled::Stored
    );
    outbox::release_published_bodies(&h.pool, Some(cuuid), 0)
        .await
        .unwrap();
    let history = call(&dani, "read_conversation", json!({"conversation_id": cid})).await;
    let bodies: Vec<&str> = history["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["body"].as_str().unwrap())
        .collect();
    assert_eq!(
        bodies,
        ["through the broker", "still in flight", "sent after it"],
        "confirmation order is not thread order"
    );

    // An ACL that changes while bodies are on the broker takes effect on
    // the next read, not the next restart: a removed member stops reading.
    call(
        &owner,
        "remove_conversation_member",
        json!({"conversation_id": cid, "address": "dani/review"}),
    )
    .await;
    let refused =
        call_expect_error(&dani, "read_conversation", json!({"conversation_id": cid})).await;
    assert!(refused.contains("no such conversation"), "{refused}");
    let refused = call_expect_error(
        &dani,
        "get_conversation_message",
        json!({"message_id": sent["message_id"]}),
    )
    .await;
    assert!(refused.contains("no such conversation"), "{refused}");

    // A body the backend no longer holds is an explained absence, and the
    // message keeps its place, its recipients and its receipts.
    outbox::tombstone(&h.pool, mid, "retention").await.unwrap();
    let err = convo_store::body_of(&h.pool, &backends, mid)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("no longer held"), "{err}");
    assert!(err.contains("receipts remain"), "{err}");
    let receipts = call(
        &owner,
        "get_message_receipts",
        json!({"message_id": sent["message_id"]}),
    )
    .await;
    assert_eq!(receipts["total"], 1, "the denominator survives a tombstone");
    let history = call(&owner, "read_conversation", json!({"conversation_id": cid})).await;
    assert_eq!(history["messages"][0]["publication"], "tombstoned");
    assert_eq!(history["messages"][0]["body"], "");
    assert!(
        history["messages"][0]["unavailable"]
            .as_str()
            .unwrap()
            .contains("no longer held"),
        "a gap that is explained is not the same as a gap: {history}"
    );
    assert_eq!(
        history["messages"].as_array().unwrap().len(),
        3,
        "a tombstoned body does not remove the message"
    );

    // The Postgres thread is untouched by any of this.
    let legacy_sent = call(
        &before,
        "send_conversation_message",
        json!({"conversation_id": legacy["id"], "body": "still local",
               "request_id": request_id()}),
    )
    .await;
    assert_eq!(legacy_sent["stored"], true);
    assert_eq!(
        convo_store::body_of(
            &h.pool,
            &backends,
            legacy_sent["message_id"].as_str().unwrap().parse().unwrap()
        )
        .await
        .unwrap(),
        "still local"
    );

    JetStreamBackend::deprovision(&config, team).await.unwrap();
    for c in [owner, dani, before] {
        let _ = c.cancel().await;
    }
    h.shutdown().await;
}

/// Phase 6: every recipient has its own durable inbox. One acknowledgement
/// drains nobody else's, a process that dies between taking a reference and
/// confirming it gets an honest state rather than a fabricated receipt, and
/// a broker that has lost the references does not make the inbox look empty.
#[tokio::test]
async fn each_recipient_holds_its_own_durable_inbox() {
    use ai_crew_sync::store::jetstream::{Config, JetStreamBackend};
    use ai_crew_sync::store::{inbox, outbox};

    let h = require_db_broker!("t_inbox_delivery");
    let owner_token = seed_agent(&h.pool, "acme", "joaquin").await;
    let dani_token = seed_agent(&h.pool, "acme", "dani").await;
    let marta_token = seed_agent(&h.pool, "acme", "marta").await;
    enable_conversations(&h.pool, "acme").await;
    let team = team_id(&h.pool, "acme").await;
    sqlx::query("UPDATE teams SET default_backend = 'jetstream' WHERE id = $1")
        .bind(team)
        .execute(&h.pool)
        .await
        .unwrap();

    let config = Config::new(nats_url()).with_limits(1_000, 16 * 1024 * 1024);
    JetStreamBackend::provision(&config, team).await.unwrap();
    JetStreamBackend::provision_inbox(&config, team)
        .await
        .unwrap();
    let backend = JetStreamBackend::connect(&config, team).await.unwrap();

    let owner = connect_with_session(&h.base, &owner_token, "impl").await;
    let design = connect_with_session(&h.base, &dani_token, "design").await;
    let marta = connect_with_session(&h.base, &marta_token, "review").await;
    // One of the three is a registered window, so the inbox is exercised
    // with a real session credential and not only with a header label.
    let dani_agent = connect(&h.base, &dani_token).await;
    let review_cred = call(
        &dani_agent,
        "register_session",
        json!({"session": "review"}),
    )
    .await;
    let review = connect(&h.base, review_cred["session_token"].as_str().unwrap()).await;

    let convo = call(
        &owner,
        "create_conversation",
        json!({"title": "three inboxes", "private": true,
               "invite": ["dani/review", "dani/design", "marta/review"]}),
    )
    .await;
    let cid = convo["id"].as_str().unwrap().to_owned();
    for client in [&review, &design, &marta] {
        call(client, "join_conversation", json!({"conversation_id": cid})).await;
    }
    let sent = call(
        &owner,
        "send_conversation_message",
        json!({"conversation_id": cid, "body": "three of you, three inboxes",
               "request_id": request_id()}),
    )
    .await;
    assert_eq!(sent["recipients"].as_array().unwrap().len(), 3);

    // Nothing is notified before the body is canonical: a reference to a
    // message nobody stored would be a promise of something unreadable.
    assert_eq!(
        inbox::publish_pending(&h.pool, &backend, team, 100)
            .await
            .unwrap(),
        0,
        "no references before the body is stored"
    );
    assert_eq!(
        outbox::run_once(&h.pool, &backend, "worker").await.unwrap(),
        Some(outbox::Settled::Stored)
    );
    assert_eq!(
        inbox::publish_pending(&h.pool, &backend, team, 100)
            .await
            .unwrap(),
        3,
        "one reference per recipient, and no more"
    );

    // Each window takes its own, and only its own.
    let first = call(&review, "fetch_conversation_inbox", json!({})).await;
    assert_eq!(first["references"].as_array().unwrap().len(), 1);
    assert_eq!(first["from_broker"], 1);
    assert_eq!(first["references"][0]["message_id"], sent["message_id"]);
    assert_eq!(first["references"][0]["kind"], "message");
    assert_eq!(first["references"][0]["redelivered"], false);
    assert!(
        first["references"][0].get("body").is_none(),
        "a reference carries no body: {first}"
    );

    // Taking it is not receiving it. Until the holder says it has it
    // durably, nothing is delivered.
    let receipts = call(
        &owner,
        "get_message_receipts",
        json!({"message_id": sent["message_id"]}),
    )
    .await;
    assert!(
        receipts["receipts"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["delivered_at"].is_null()),
        "{receipts}"
    );

    call(
        &review,
        "confirm_inbox_delivery",
        json!({"delivery_ids": [first["references"][0]["delivery_id"]]}),
    )
    .await;
    let delivered = |receipts: &Value, address: &str| -> bool {
        receipts["receipts"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["address"] == address)
            .map(|r| r["delivered_at"].is_string())
            .unwrap_or(false)
    };
    let receipts = call(
        &owner,
        "get_message_receipts",
        json!({"message_id": sent["message_id"]}),
    )
    .await;
    assert!(delivered(&receipts, "dani/review"), "{receipts}");
    assert!(!delivered(&receipts, "dani/design"), "{receipts}");
    assert!(!delivered(&receipts, "marta/review"), "{receipts}");

    // One window's acknowledgement is its own. The other two are not
    // "assumed yes" and not "no": they have not answered.
    call(
        &review,
        "ack_message",
        json!({"message_id": sent["message_id"]}),
    )
    .await;
    let receipts = call(
        &owner,
        "get_message_receipts",
        json!({"message_id": sent["message_id"]}),
    )
    .await;
    assert_eq!(receipts["acknowledged"], 1);
    assert_eq!(receipts["resolved"], 0);
    assert_eq!(receipts["total"], 3);

    // The parent agent token cannot read its own window's inbox, or record
    // a delivery on its behalf. The label in the header is a name.
    let impostor = connect_with_session(&h.base, &dani_token, "review").await;
    let err = call_expect_error(&impostor, "fetch_conversation_inbox", json!({})).await;
    assert!(err.contains("registered window"), "{err}");
    let err = call_expect_error(
        &impostor,
        "confirm_inbox_delivery",
        json!({"delivery_ids": [first["references"][0]["delivery_id"]]}),
    )
    .await;
    assert!(err.contains("registered window"), "{err}");
    let err = call_expect_error(&impostor, "conversation_inbox_status", json!({})).await;
    assert!(err.contains("registered window"), "{err}");

    // A later observation on the same message is a new notification. The
    // sender is told to look again when the recipient resolves it, not only
    // the first time it acknowledged.
    inbox::publish_pending(&h.pool, &backend, team, 100)
        .await
        .unwrap();
    call(
        &review,
        "ack_message",
        json!({"message_id": sent["message_id"], "resolved": true, "note": "and done"}),
    )
    .await;
    assert_eq!(
        inbox::publish_pending(&h.pool, &backend, team, 100)
            .await
            .unwrap(),
        1,
        "resolving after acknowledging is a second thing to tell the sender"
    );
    let (events,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM inbox_events WHERE kind = 'receipt' AND message_id = $1",
    )
    .bind(
        sent["message_id"]
            .as_str()
            .unwrap()
            .parse::<Uuid>()
            .unwrap(),
    )
    .fetch_one(&h.pool)
    .await
    .unwrap();
    assert_eq!(
        events, 2,
        "acknowledged and resolved are two observations, not one coalesced row"
    );

    // And revoking the window does not open its inbox either. The label was
    // registered once, which is enough: revocation must not turn a
    // protected window into a legacy identity.
    call(&review, "revoke_session", json!({})).await;
    let err = call_expect_error(&impostor, "fetch_conversation_inbox", json!({})).await;
    assert!(err.contains("does not hand the label back"), "{err}");
    let err = call_expect_error(
        &impostor,
        "confirm_inbox_delivery",
        json!({"delivery_ids": [first["references"][0]["delivery_id"]]}),
    )
    .await;
    assert!(err.contains("registered window"), "{err}");

    // A process that takes a reference and dies before confirming: the
    // reference is not lost and no receipt is invented. A second fetch does
    // not hand it over twice while the first hand-out is still in flight,
    // and the state says exactly that.
    let taken = call(&design, "fetch_conversation_inbox", json!({})).await;
    assert_eq!(taken["references"].as_array().unwrap().len(), 1);
    let again = call(&design, "fetch_conversation_inbox", json!({})).await;
    assert_eq!(
        again["references"].as_array().unwrap().len(),
        0,
        "it is already in flight to this window: {again}"
    );
    let state = call(&design, "conversation_inbox_status", json!({})).await;
    assert_eq!(state["address"], "dani/design");
    assert_eq!(state["undelivered"], 1, "the authority still says one");
    assert_eq!(state["handed_out_unconfirmed"], 1);

    // Offered again once that hand-out has gone stale, it is the same
    // reference under the same delivery id. A second row for the same
    // reference used to violate the unique index and fail the whole fetch.
    sqlx::query("UPDATE inbox_deliveries SET handed_at = now() - interval '10 minutes'")
        .execute(&h.pool)
        .await
        .unwrap();
    let redelivered = call(&design, "fetch_conversation_inbox", json!({})).await;
    assert_eq!(redelivered["references"].as_array().unwrap().len(), 1);
    assert_eq!(
        redelivered["references"][0]["delivery_id"], taken["references"][0]["delivery_id"],
        "a redelivery finds the hand-out that is already open: {redelivered}"
    );
    // The confirmation arrives after the restart, with the reference the
    // spool kept. Confirming twice changes nothing.
    let id = taken["references"][0]["delivery_id"].clone();
    let confirmed = call(
        &design,
        "confirm_inbox_delivery",
        json!({"delivery_ids": [id]}),
    )
    .await;
    assert_eq!(confirmed["confirmed"], 1);
    let repeat = call(
        &design,
        "confirm_inbox_delivery",
        json!({"delivery_ids": [id]}),
    )
    .await;
    assert_eq!(repeat["confirmed"], 0, "confirming twice is harmless");
    let receipts = call(
        &owner,
        "get_message_receipts",
        json!({"message_id": sent["message_id"]}),
    )
    .await;
    assert!(delivered(&receipts, "dani/design"), "{receipts}");
    assert!(!delivered(&receipts, "marta/review"));

    // A confirmation from a window that has since been resumed away writes
    // nothing: the reference stays offered to whoever holds the window now.
    // This needs a registered window, because that is what an epoch is.
    let cred = call(&marta, "register_session", json!({"session": "review"})).await;
    let marta_window = connect(&h.base, cred["session_token"].as_str().unwrap()).await;
    let stale = ai_crew_sync::auth::AuthCtx {
        agent_id: sqlx::query_scalar::<_, Uuid>("SELECT id FROM agents WHERE name = 'marta'")
            .fetch_one(&h.pool)
            .await
            .unwrap(),
        agent_name: "marta".into(),
        team_id: team,
        team_slug: "acme".into(),
        session: "review".into(),
        session_id: sqlx::query_scalar::<_, Uuid>(
            "SELECT id FROM agent_sessions WHERE label = 'review' AND agent_id =
                 (SELECT id FROM agents WHERE name = 'marta')",
        )
        .fetch_optional(&h.pool)
        .await
        .unwrap(),
        // The epoch a connection that has since been replaced was admitted
        // with.
        session_epoch: Some(99),
        token_id: None,
    };
    assert!(stale.session_id.is_some(), "the window must be registered");
    let backends =
        ai_crew_sync::store::routing::Backends::with_jetstream(h.pool.clone(), config.clone());
    let marta_batch = call(&marta_window, "fetch_conversation_inbox", json!({})).await;
    let marta_id = marta_batch["references"][0]["delivery_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let err = inbox::confirm(&h.pool, &backends, &stale, std::slice::from_ref(&marta_id))
        .await
        .expect_err("a replaced connection must not confirm")
        .to_string();
    assert!(err.contains("stale"), "{err}");
    let receipts = call(
        &owner,
        "get_message_receipts",
        json!({"message_id": sent["message_id"]}),
    )
    .await;
    assert!(
        !delivered(&receipts, "marta/review"),
        "a fenced confirmation must not write a receipt: {receipts}"
    );

    // The broker loses everything — an expiry, an operator, a deleted
    // consumer. The inbox is not empty: the bus's own records are the
    // authority, and the reference comes back from there.
    JetStreamBackend::deprovision(&config, team).await.unwrap();
    // Immediately after a hand-out the reference is in flight to this
    // window, and a fetch does not hand it over a second time. The state is
    // the honest answer, and it is not "your inbox is empty".
    let inflight = call(&marta_window, "fetch_conversation_inbox", json!({})).await;
    assert_eq!(inflight["references"].as_array().unwrap().len(), 0);
    let state = call(&marta_window, "conversation_inbox_status", json!({})).await;
    assert_eq!(state["undelivered"], 1);
    assert_eq!(state["handed_out_unconfirmed"], 1);
    assert_eq!(
        state["broker_consumer_present"], false,
        "a missing consumer is a fact, not an empty inbox: {state}"
    );

    // Once that hand-out has gone stale — the process holding it never came
    // back — the reference is offered again, from the records that are the
    // authority.
    sqlx::query("UPDATE inbox_deliveries SET handed_at = now() - interval '10 minutes'")
        .execute(&h.pool)
        .await
        .unwrap();
    let rebuilt = call(
        &marta_window,
        "fetch_conversation_inbox",
        json!({"limit": 5}),
    )
    .await;
    let refs = rebuilt["references"].as_array().unwrap();
    assert_eq!(refs.len(), 1, "{rebuilt}");
    assert_eq!(refs[0]["source"], "bus");
    assert_eq!(rebuilt["from_broker"], 0);
    assert!(
        rebuilt["note"].as_str().unwrap().contains("authority"),
        "{rebuilt}"
    );
    call(
        &marta_window,
        "confirm_inbox_delivery",
        json!({"delivery_ids": [refs[0]["delivery_id"]]}),
    )
    .await;
    let receipts = call(
        &owner,
        "get_message_receipts",
        json!({"message_id": sent["message_id"]}),
    )
    .await;
    assert!(delivered(&receipts, "marta/review"), "{receipts}");
    assert_eq!(
        receipts["acknowledged"], 1,
        "delivery is not acknowledgement, on any path"
    );

    // Legacy traffic is untouched by all of this.
    call(&owner, "create_channel", json!({"name": "general"})).await;
    call(
        &owner,
        "post_message",
        json!({"channel": "general", "body": "still here"}),
    )
    .await;
    let read = call(
        &marta_window,
        "read_messages",
        json!({"channel": "general"}),
    )
    .await;
    assert_eq!(read["messages"].as_array().unwrap().len(), 1);

    for c in [
        owner,
        review,
        design,
        marta,
        marta_window,
        dani_agent,
        impostor,
    ] {
        let _ = c.cancel().await;
    }
    h.shutdown().await;
}

/// Phase 7: a supervised move, a rollback, and an interrupted run that
/// resumes. Bodies, ids, authorship, access and every observed receipt come
/// through unchanged, and nothing is cut over until every body has been read
/// back from the target and matched.
#[tokio::test]
async fn a_conversation_moves_between_backends_and_back_without_losing_anything() {
    use ai_crew_sync::store::jetstream::{Config, JetStreamBackend};
    use ai_crew_sync::store::migrate::{self, Direction};
    use ai_crew_sync::store::outbox;

    let h = require_db_broker!("t_conversation_migration");
    let owner_token = seed_agent(&h.pool, "acme", "joaquin").await;
    let dani_token = seed_agent(&h.pool, "acme", "dani").await;
    enable_conversations(&h.pool, "acme").await;
    let team = team_id(&h.pool, "acme").await;

    let config = Config::new(nats_url()).with_limits(1_000, 16 * 1024 * 1024);
    JetStreamBackend::provision(&config, team).await.unwrap();
    JetStreamBackend::provision_inbox(&config, team)
        .await
        .unwrap();
    let jetstream = JetStreamBackend::connect(&config, team).await.unwrap();

    // A thread that lives entirely in Postgres, with real receipts on it.
    let owner = connect_with_session(&h.base, &owner_token, "impl").await;
    let dani = connect_with_session(&h.base, &dani_token, "review").await;
    let convo = call(
        &owner,
        "create_conversation",
        json!({"title": "moved thread", "private": true, "invite": ["dani/review"]}),
    )
    .await;
    let cid = convo["id"].as_str().unwrap().to_owned();
    let cuuid: Uuid = cid.parse().unwrap();
    call(&dani, "join_conversation", json!({"conversation_id": cid})).await;
    let mut sent = Vec::new();
    for body in ["the first one", "the second one", "the third one"] {
        sent.push(
            call(
                &owner,
                "send_conversation_message",
                json!({"conversation_id": cid, "body": body, "request_id": request_id()}),
            )
            .await,
        );
    }
    call(
        &dani,
        "ack_message",
        json!({"message_id": sent[1]["message_id"], "resolved": true, "note": "done"}),
    )
    .await;
    let before = call(&dani, "read_conversation", json!({"conversation_id": cid})).await;
    let receipts_before = call(
        &owner,
        "get_message_receipts",
        json!({"message_id": sent[1]["message_id"]}),
    )
    .await;

    // A plan changes nothing and says what it would cost.
    let plans = migrate::plan(&h.pool, team, Direction::ToJetStream, &[cuuid])
        .await
        .unwrap();
    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].messages, 3);
    assert_eq!(plans[0].current_backend, "postgres");
    assert!(plans[0].blocked.is_none());
    assert!(plans[0].bytes > 0);
    let unchanged = call(&dani, "read_conversation", json!({"conversation_id": cid})).await;
    assert_eq!(
        unchanged["messages"], before["messages"],
        "a plan is a read"
    );

    // The move itself.
    let outcome = migrate::run(&h.pool, &jetstream, team, cuuid, Direction::ToJetStream)
        .await
        .unwrap();
    assert_eq!(outcome.copied, 3);
    assert_eq!(outcome.state, "cut_over");

    // Writes are open again, and the thread reads exactly as it did — same
    // ids, same order, same bodies, same authors.
    let after = call(&dani, "read_conversation", json!({"conversation_id": cid})).await;
    assert_eq!(
        after["messages"], before["messages"],
        "the thread is the thread"
    );
    let receipts_after = call(
        &owner,
        "get_message_receipts",
        json!({"message_id": sent[1]["message_id"]}),
    )
    .await;
    assert_eq!(
        receipts_after, receipts_before,
        "a move observes nothing and invents nothing"
    );
    let (backend, locators): (String, i64) = sqlx::query_as(
        "SELECT (SELECT backend FROM conversations WHERE id = $1),
                count(*) FILTER (WHERE canonical_locator IS NOT NULL)
           FROM conversation_messages WHERE conversation_id = $1",
    )
    .bind(cuuid)
    .fetch_one(&h.pool)
    .await
    .unwrap();
    assert_eq!((backend.as_str(), locators), ("jetstream", 3));

    // The source bodies are still there: until cleanup runs, a rollback has
    // something to roll back to.
    let (still_local,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM conversation_messages WHERE conversation_id = $1 AND body <> ''",
    )
    .bind(cuuid)
    .fetch_one(&h.pool)
    .await
    .unwrap();
    assert_eq!(still_local, 3);
    let (would_drop, _bytes) = migrate::cleanup(&h.pool, team, 168, false).await.unwrap();
    assert_eq!(
        would_drop, 0,
        "nothing is dropped inside the rollback window"
    );
    assert!(
        migrate::cleanup(&h.pool, team, -1, false).await.is_err(),
        "a negative window would point into the future"
    );

    // And the ordinary publication sweep leaves them alone. It releases
    // bodies the outbox staged; a migration source copy is the rollback,
    // and only `conversations cleanup` may drop it.
    assert_eq!(
        outbox::release_published_bodies(&h.pool, None, 0)
            .await
            .unwrap(),
        0,
        "the five minute sweep must not eat the rollback source"
    );
    let (still_local,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM conversation_messages WHERE conversation_id = $1 AND body <> ''",
    )
    .bind(cuuid)
    .fetch_one(&h.pool)
    .await
    .unwrap();
    assert_eq!(still_local, 3);

    // An interrupted run resumes through the same command. The process
    // died holding the pause, and the plan says so instead of skipping the
    // thread and reporting success.
    sqlx::query("UPDATE conversations SET write_paused_at = now() WHERE id = $1")
        .bind(cuuid)
        .execute(&h.pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO conversation_migrations (team_id, conversation_id, direction, state)
         VALUES ($1, $2, 'to_jetstream', 'copying')",
    )
    .bind(team)
    .bind(cuuid)
    .execute(&h.pool)
    .await
    .unwrap();
    let plans = migrate::plan(&h.pool, team, Direction::ToJetStream, &[cuuid])
        .await
        .unwrap();
    assert!(plans[0].blocked.is_none(), "{:?}", plans[0].blocked);
    assert!(plans[0].resuming, "an open run is resumed, not refused");
    let resumed = migrate::run(&h.pool, &jetstream, team, cuuid, Direction::ToJetStream)
        .await
        .unwrap();
    assert_eq!(resumed.state, "cut_over");
    let (paused,): (Option<chrono::DateTime<chrono::Utc>>,) =
        sqlx::query_as("SELECT write_paused_at FROM conversations WHERE id = $1")
            .bind(cuuid)
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert!(paused.is_none(), "resuming finishes and reopens the thread");

    // New messages go where the thread now lives, and are published like
    // any other: acceptance is not storage.
    let fresh = call(
        &owner,
        "send_conversation_message",
        json!({"conversation_id": cid, "body": "written after the move",
               "request_id": request_id()}),
    )
    .await;
    assert_eq!(fresh["stored"], false);
    assert_eq!(
        outbox::run_once(&h.pool, &jetstream, "worker")
            .await
            .unwrap(),
        Some(outbox::Settled::Stored)
    );

    // An interrupted run resumes instead of copying twice. Half the
    // evidence is thrown away and the same move is run again.
    outbox::release_published_bodies(&h.pool, Some(cuuid), 0)
        .await
        .unwrap();
    let back = migrate::run(&h.pool, &jetstream, team, cuuid, Direction::ToPostgres)
        .await
        .unwrap();
    assert_eq!(back.copied, 4, "every body came back off the broker");
    let (backend,): (String,) = sqlx::query_as("SELECT backend FROM conversations WHERE id = $1")
        .bind(cuuid)
        .fetch_one(&h.pool)
        .await
        .unwrap();
    assert_eq!(backend, "postgres");
    let rolled = call(&dani, "read_conversation", json!({"conversation_id": cid})).await;
    let bodies: Vec<&str> = rolled["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["body"].as_str().unwrap())
        .collect();
    assert_eq!(
        bodies,
        [
            "the first one",
            "the second one",
            "the third one",
            "written after the move"
        ],
        "a rollback is a copy back, and it keeps what was written meanwhile"
    );
    let receipts_rolled = call(
        &owner,
        "get_message_receipts",
        json!({"message_id": sent[1]["message_id"]}),
    )
    .await;
    assert_eq!(
        receipts_rolled, receipts_before,
        "receipts survive both ways"
    );

    // A second move, interrupted after two messages and resumed: the run
    // picks up its own evidence rather than starting over.
    let migration = migrate::run(&h.pool, &jetstream, team, cuuid, Direction::ToJetStream)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE conversation_migrations SET state = 'copying', finished_at = NULL WHERE id = $1",
    )
    .bind(migration.migration_id)
    .execute(&h.pool)
    .await
    .unwrap();
    sqlx::query(
        "DELETE FROM conversation_migration_items
          WHERE migration_id = $1 AND message_id IN (
              SELECT id FROM conversation_messages WHERE conversation_id = $2
               ORDER BY seq LIMIT 2)",
    )
    .bind(migration.migration_id)
    .bind(cuuid)
    .execute(&h.pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE conversation_messages SET backend = 'postgres'
          WHERE conversation_id = $1 AND seq <= 2",
    )
    .bind(cuuid)
    .execute(&h.pool)
    .await
    .unwrap();
    let resumed = migrate::run(&h.pool, &jetstream, team, cuuid, Direction::ToJetStream)
        .await
        .unwrap();
    assert_eq!(
        resumed.migration_id, migration.migration_id,
        "the same run continued"
    );
    assert_eq!(resumed.copied, 2, "only what was missing was copied again");
    assert_eq!(resumed.skipped, 2, "what was verified was left alone");
    let final_read = call(&dani, "read_conversation", json!({"conversation_id": cid})).await;
    assert_eq!(
        final_read["messages"].as_array().unwrap().len(),
        4,
        "one logical message each, however many times they were copied"
    );

    // Writes are never left paused, whichever way it went.
    let (paused,): (Option<chrono::DateTime<chrono::Utc>>,) =
        sqlx::query_as("SELECT write_paused_at FROM conversations WHERE id = $1")
            .bind(cuuid)
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert!(paused.is_none());

    JetStreamBackend::deprovision(&config, team).await.unwrap();
    for c in [owner, dani] {
        let _ = c.cancel().await;
    }
    h.shutdown().await;
}

/// The worker path's last attempt. A backend that keeps the body and never
/// confirms it must not end as `failed`: "we did not hear back" and "it is
/// not there" are different facts, and only one of them is a gap.
#[tokio::test]
async fn a_lost_confirmation_is_not_a_failure_when_the_backend_kept_it() {
    use ai_crew_sync::store::backend::{Faults, MessagingBackend, PostgresBackend};
    use ai_crew_sync::store::outbox::{self, Settled};

    let h = require_db!("t_lost_confirmations");
    let token = seed_agent(&h.pool, "acme", "joaquin").await;
    let dani_token = seed_agent(&h.pool, "acme", "dani").await;
    enable_conversations(&h.pool, "acme").await;
    let owner = connect_with_session(&h.base, &token, "impl").await;
    let dani = connect_with_session(&h.base, &dani_token, "review").await;

    let convo = call(
        &owner,
        "create_conversation",
        json!({"title": "kept but unconfirmed", "private": true, "invite": ["dani/review"]}),
    )
    .await;
    let cid = convo["id"].as_str().unwrap().to_owned();
    call(&dani, "join_conversation", json!({"conversation_id": cid})).await;
    let cuuid: Uuid = cid.parse().unwrap();
    outbox::set_publication(&h.pool, cuuid, true).await.unwrap();
    let sent = call(
        &owner,
        "send_conversation_message",
        json!({"conversation_id": cid, "body": "written, never acknowledged",
               "request_id": request_id()}),
    )
    .await;
    let mid: Uuid = sent["message_id"].as_str().unwrap().parse().unwrap();

    // Every attempt writes the body and reports that it did not.
    let losing = PostgresBackend::with_faults(
        h.pool.clone(),
        Faults {
            lose_confirmation: true,
            ..Default::default()
        },
    );
    let mut last = None;
    for attempt in 1..=outbox::MAX_ATTEMPTS {
        sqlx::query("UPDATE conversation_outbox SET next_attempt_at = now()")
            .execute(&h.pool)
            .await
            .unwrap();
        last = outbox::run_once(&h.pool, &losing, "worker").await.unwrap();
        if attempt < outbox::MAX_ATTEMPTS {
            assert!(
                matches!(last, Some(Settled::Retrying { .. })),
                "attempt {attempt} retries: {last:?}"
            );
        }
    }

    // The last attempt asks instead of assuming. The backend has it, so the
    // message is stored and the receipts say when.
    assert_eq!(
        last,
        Some(Settled::Stored),
        "the backend kept the body; giving up would have recorded a gap that is not there"
    );
    let (state, locator): (String, Option<String>) = sqlx::query_as(
        "SELECT publication_state, canonical_locator FROM conversation_messages WHERE id = $1",
    )
    .bind(mid)
    .fetch_one(&h.pool)
    .await
    .unwrap();
    assert_eq!(state, "stored");
    let plain = PostgresBackend::new(h.pool.clone());
    assert_eq!(
        plain
            .fetch(
                &ai_crew_sync::store::backend::Locator(locator.clone().unwrap()),
                mid
            )
            .await
            .unwrap()
            .as_deref(),
        Some("written, never acknowledged"),
        "and it is the body that was sent"
    );
    let receipts = call(
        &owner,
        "get_message_receipts",
        json!({"message_id": sent["message_id"]}),
    )
    .await;
    assert!(
        receipts["receipts"][0]["stored_at"].is_string(),
        "stored once it was established, not before: {receipts}"
    );

    // A backend that genuinely does not have it still fails, visibly. The
    // fix must not turn every give-up into a success.
    let gone = call(
        &owner,
        "send_conversation_message",
        json!({"conversation_id": cid, "body": "never written anywhere",
               "request_id": request_id()}),
    )
    .await;
    let unreachable = PostgresBackend::with_faults(
        h.pool.clone(),
        Faults {
            retryable: outbox::MAX_ATTEMPTS as usize + 1,
            ..Default::default()
        },
    );
    let mut last = None;
    for _ in 1..=outbox::MAX_ATTEMPTS {
        sqlx::query("UPDATE conversation_outbox SET next_attempt_at = now()")
            .execute(&h.pool)
            .await
            .unwrap();
        last = outbox::run_once(&h.pool, &unreachable, "worker")
            .await
            .unwrap();
    }
    assert_eq!(last, Some(Settled::Failed));
    let (state,): (String,) =
        sqlx::query_as("SELECT publication_state FROM conversation_messages WHERE id = $1")
            .bind(
                gone["message_id"]
                    .as_str()
                    .unwrap()
                    .parse::<Uuid>()
                    .unwrap(),
            )
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert_eq!(
        state, "failed",
        "a body nobody has is a gap, and it is shown"
    );

    for c in [owner, dani] {
        let _ = c.cancel().await;
    }
    h.shutdown().await;
}

/// Losing the broker costs the bodies it holds, for as long as it is away,
/// and nothing else. Reproduced on a real deployment by deleting the
/// broker's volume: the thread stopped reading at all.
#[tokio::test]
async fn a_broker_that_is_gone_does_not_take_the_thread_with_it() {
    use ai_crew_sync::store::jetstream::{Config, JetStreamBackend};
    use ai_crew_sync::store::outbox;

    let h = require_db_broker!("t_broker_gone");
    let token = seed_agent(&h.pool, "acme", "joaquin").await;
    let dani_token = seed_agent(&h.pool, "acme", "dani").await;
    enable_conversations(&h.pool, "acme").await;
    let team = team_id(&h.pool, "acme").await;

    let owner = connect_with_session(&h.base, &token, "impl").await;
    let dani = connect_with_session(&h.base, &dani_token, "review").await;

    // One thread that stays on Postgres, and one routed to the broker.
    let local = call(
        &owner,
        "create_conversation",
        json!({"title": "en postgres", "private": true}),
    )
    .await;
    call(
        &owner,
        "send_conversation_message",
        json!({"conversation_id": local["id"], "body": "sigue aquí", "request_id": request_id()}),
    )
    .await;

    sqlx::query("UPDATE teams SET default_backend = 'jetstream' WHERE id = $1")
        .bind(team)
        .execute(&h.pool)
        .await
        .unwrap();
    let config = Config::new(nats_url()).with_limits(1_000, 16 * 1024 * 1024);
    JetStreamBackend::provision(&config, team).await.unwrap();
    let backend = JetStreamBackend::connect(&config, team).await.unwrap();

    let convo = call(
        &owner,
        "create_conversation",
        json!({"title": "en el broker", "private": true, "invite": ["dani/review"]}),
    )
    .await;
    let cid = convo["id"].as_str().unwrap().to_owned();
    call(&dani, "join_conversation", json!({"conversation_id": cid})).await;
    let sent = call(
        &owner,
        "send_conversation_message",
        json!({"conversation_id": cid, "body": "en el broker", "request_id": request_id()}),
    )
    .await;
    assert_eq!(
        outbox::run_once(&h.pool, &backend, "worker").await.unwrap(),
        Some(outbox::Settled::Stored)
    );
    outbox::release_published_bodies(&h.pool, Some(cid.parse().unwrap()), 0)
        .await
        .unwrap();

    // The broker loses everything. Not retention, not a tombstone: gone.
    JetStreamBackend::deprovision(&config, team).await.unwrap();

    // The thread still reads. The message keeps its place, its sender and
    // its sequence, and says why the body is not here.
    let page = call(&dani, "read_conversation", json!({"conversation_id": cid})).await;
    let msgs = page["messages"]
        .as_array()
        .expect("the page is still a page");
    assert_eq!(msgs.len(), 1, "the thread did not vanish: {page}");
    assert_eq!(msgs[0]["message_id"], sent["message_id"]);
    assert_eq!(msgs[0]["seq"], 1);
    assert_eq!(msgs[0]["from_address"], "joaquin/impl");
    assert_eq!(msgs[0]["body"], "");
    assert_eq!(
        msgs[0]["publication"], "stored",
        "it *is* stored; what is missing is this process's way to read it"
    );
    assert!(
        msgs[0]["unavailable"]
            .as_str()
            .unwrap()
            .contains("cannot be reached"),
        "and it says so: {}",
        msgs[0]
    );

    // Its receipts survive, and so does the rest of the bus.
    let receipts = call(
        &owner,
        "get_message_receipts",
        json!({"message_id": sent["message_id"]}),
    )
    .await;
    assert_eq!(receipts["total"], 1, "{receipts}");
    let still = call(
        &owner,
        "read_conversation",
        json!({"conversation_id": local["id"]}),
    )
    .await;
    assert_eq!(
        still["messages"][0]["body"], "sigue aquí",
        "a thread on Postgres is untouched by the broker's trouble"
    );

    // And a send is still accepted: it queues, as it does for any outage.
    let after = call(
        &owner,
        "send_conversation_message",
        json!({"conversation_id": cid, "body": "después", "request_id": request_id()}),
    )
    .await;
    assert_eq!(after["stored"], false, "{after}");

    for c in [owner, dani] {
        let _ = c.cancel().await;
    }
    h.shutdown().await;
}

/// A claim has to mean something on the way out too. Found on a deployment:
/// anyone on the team could mark a task done while its holder was still
/// working, and the holder learned about it from a refused renewal.
#[tokio::test]
async fn completing_a_task_respects_whoever_holds_it() {
    let h = require_db!("t_complete_claim");
    let owner = seed_agent(&h.pool, "acme", "joaquin").await;
    let holder = seed_agent(&h.pool, "acme", "marta").await;
    let other = seed_agent(&h.pool, "acme", "dani").await;
    let o = connect(&h.base, &owner).await;
    let m = connect(&h.base, &holder).await;
    let d = connect(&h.base, &other).await;

    call(
        &o,
        "create_task",
        json!({"key": "suya", "title": "el trabajo de marta"}),
    )
    .await;
    let claimed = call(
        &m,
        "claim_task",
        json!({"key": "suya", "lease_seconds": 300}),
    )
    .await;
    assert_eq!(claimed["claimed"], true);

    // The holder is working and the lease is live.
    let err = call_expect_error(&d, "complete_task", json!({"key": "suya"})).await;
    assert!(err.contains("held by marta"), "{err}");
    assert!(
        err.contains("end work somebody else is doing"),
        "and it says why: {err}"
    );
    let still = call(&o, "get_task", json!({"key": "suya"})).await;
    assert_eq!(
        still["task"]["status"], "claimed",
        "nothing was written: {still}"
    );

    // Its holder finishes it.
    let done = call(
        &m,
        "complete_task",
        json!({"key": "suya", "result": "hecho"}),
    )
    .await;
    assert_eq!(done["status"], "done");

    // An unclaimed task is anyone's to finish: nobody's work is ended.
    call(
        &o,
        "create_task",
        json!({"key": "libre", "title": "de nadie"}),
    )
    .await;
    let done = call(&d, "complete_task", json!({"key": "libre"})).await;
    assert_eq!(done["status"], "done", "{done}");

    // And an expired lease is fair game, which is what a lease is for.
    call(
        &o,
        "create_task",
        json!({"key": "caducada", "title": "abandonada"}),
    )
    .await;
    call(
        &m,
        "claim_task",
        json!({"key": "caducada", "lease_seconds": 60}),
    )
    .await;
    sqlx::query(
        "UPDATE tasks SET lease_expires_at = now() - interval '1 minute' WHERE key = 'caducada'",
    )
    .execute(&h.pool)
    .await
    .unwrap();
    let done = call(&d, "complete_task", json!({"key": "caducada"})).await;
    assert_eq!(
        done["status"], "done",
        "a claim nobody renewed does not hold the task for ever: {done}"
    );

    for c in [o, m, d] {
        let _ = c.cancel().await;
    }
    h.shutdown().await;
}

/// Every task write is fenced by the session epoch, not only the claim.
/// Found in review: a context admitted before a resume could still complete
/// the replacement window's work, because the holder predicate matches the
/// agent and the label, which a resumed window shares.
#[tokio::test]
async fn a_replaced_window_cannot_finish_the_work_of_the_one_that_replaced_it() {
    use ai_crew_sync::store::tasks;

    let h = require_db!("t_task_epoch_fence");
    let token = seed_agent(&h.pool, "acme", "joaquin").await;
    let agent = connect(&h.base, &token).await;
    let cred = call(&agent, "register_session", json!({"session": "obrero"})).await;
    let window = connect(&h.base, cred["session_token"].as_str().unwrap()).await;

    call(
        &window,
        "create_task",
        json!({"key": "suya", "title": "en curso"}),
    )
    .await;
    let claimed = call(
        &window,
        "claim_task",
        json!({"key": "suya", "lease_seconds": 600}),
    )
    .await;
    assert_eq!(claimed["claimed"], true);

    // The context that connection was admitted with, kept while the window
    // is resumed by the process that took over.
    let stale = ai_crew_sync::auth::AuthCtx {
        agent_id: sqlx::query_scalar::<_, Uuid>("SELECT id FROM agents WHERE name = 'joaquin'")
            .fetch_one(&h.pool)
            .await
            .unwrap(),
        agent_name: "joaquin".into(),
        team_id: team_id(&h.pool, "acme").await,
        team_slug: "acme".into(),
        session: "obrero".into(),
        session_id: Some(cred["session_id"].as_str().unwrap().parse().unwrap()),
        session_epoch: Some(cred["epoch"].as_i64().unwrap()),
        token_id: None,
    };
    let resumed = call(&window, "resume_session", json!({})).await;
    assert_eq!(resumed["epoch"], 2);

    // Same agent, same label, replaced connection. Every write is refused.
    for (name, result) in [
        (
            "complete",
            tasks::complete_task(&h.pool, &stale, "suya", Some("no soy yo".into()))
                .await
                .err(),
        ),
        (
            "renew",
            tasks::renew_lease(&h.pool, &stale, "suya", Some(600))
                .await
                .err(),
        ),
        (
            "release",
            tasks::release_task(&h.pool, &stale, "suya").await.err(),
        ),
        (
            "claim",
            tasks::claim_task(&h.pool, &stale, "suya", Some(600))
                .await
                .err(),
        ),
    ] {
        let err = result
            .unwrap_or_else(|| panic!("{name} accepted a replaced connection"))
            .to_string();
        assert!(err.contains("stale"), "{name}: {err}");
        assert!(err.contains("Nothing was written"), "{name}: {err}");
    }

    // The resume rotated the secret, so the live window is the one holding
    // the new credential.
    let live = connect(&h.base, resumed["session_token"].as_str().unwrap()).await;
    let still = call(&live, "get_task", json!({"key": "suya"})).await;
    assert_eq!(
        still["task"]["status"], "claimed",
        "the task is still the live window's: {still}"
    );

    // And that window finishes it.
    let done = call(
        &live,
        "complete_task",
        json!({"key": "suya", "result": "hecho"}),
    )
    .await;
    assert_eq!(done["status"], "done", "{done}");

    for c in [agent, window, live] {
        let _ = c.cancel().await;
    }
    h.shutdown().await;
}

/// A path this server does not serve is a path, not a credential problem.
/// Found on the deployment: a mistyped URL answered "missing bearer token",
/// and a mistyped /admin path told an operator holding a perfectly good
/// credential that it was invalid or revoked.
#[tokio::test]
async fn a_mistyped_path_does_not_blame_the_credential() {
    let h = require_db!("t_unknown_paths");
    let token = seed_agent(&h.pool, "acme", "joaquin").await;
    let client = reqwest::Client::new();

    for (path, bearer) in [
        ("/pepito", None),
        ("/pepito", Some(token.as_str())),
        ("/admin/no-existe", None),
        ("/dashboard/nope", None),
    ] {
        let mut req = client.get(format!("{}{path}", h.base));
        if let Some(b) = bearer {
            req = req.bearer_auth(b);
        }
        let resp = req.send().await.unwrap();
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        assert_eq!(
            status,
            404,
            "{path} (bearer: {}) answered {status}: {body}",
            bearer.is_some()
        );
        assert!(
            !body.contains("bearer") && !body.contains("revoked"),
            "{path} blamed the credential: {body}"
        );
        assert!(
            body.contains("/mcp"),
            "and it says what this server does serve: {body}"
        );
    }

    // A query string can carry a credential, and an error body travels: it
    // is pasted into issues and scraped out of logs. The 404 names the path
    // and nothing else.
    let resp = client
        .get(format!(
            "{}/admin/not-a-route?token=acs_do_not_echo_me&other=keep",
            h.base
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
    let body = resp.text().await.unwrap_or_default();
    assert!(
        !body.contains("do_not_echo_me") && !body.contains("token="),
        "the 404 repeated a query credential: {body}"
    );
    assert!(body.contains("/admin/not-a-route"), "{body}");

    // Anything under /mcp is the MCP endpoint's own namespace, and that one
    // does ask for a token before it says anything at all.
    let resp = client
        .get(format!("{}/mcp/extra", h.base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "under /mcp, authentication comes first");

    // The real endpoints still guard themselves.
    let resp = client
        .post(format!("{}/mcp", h.base))
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "/mcp without a token is still refused");
    let resp = client
        .post(format!("{}/mcp", h.base))
        .bearer_auth("acs_0000000000000000000000000000000000000000000000000000000000000000")
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "/mcp with a bad token is still refused");

    let c = connect(&h.base, &token).await;
    let me = call(&c, "whoami", json!({})).await;
    assert_eq!(me["agent"], "joaquin", "and a good one still works");
    let _ = c.cancel().await;

    h.shutdown().await;
}

/// The rest of a window's own state is fenced too. A replaced connection
/// cannot put back the lock its replacement holds, cannot advance the live
/// window's read cursor past messages it never saw, and cannot pull
/// references off the live window's inbox.
#[tokio::test]
async fn a_replaced_window_cannot_act_as_the_one_that_replaced_it() {
    use ai_crew_sync::store::{locks, messaging};

    let h = require_db!("t_window_state_fence");
    let token = seed_agent(&h.pool, "acme", "joaquin").await;
    let dani_token = seed_agent(&h.pool, "acme", "dani").await;
    let agent = connect(&h.base, &token).await;
    let cred = call(&agent, "register_session", json!({"session": "obrero"})).await;
    let window = connect(&h.base, cred["session_token"].as_str().unwrap()).await;
    let dani = connect(&h.base, &dani_token).await;

    let got = call(
        &window,
        "acquire_lock",
        json!({"name": "deploy", "ttl_seconds": 300}),
    )
    .await;
    assert_eq!(got["acquired"], true);
    // A direct message the live window has not read yet.
    call(
        &dani,
        "post_message",
        json!({"to": "joaquin/obrero", "body": "para la ventana viva"}),
    )
    .await;

    let stale = ai_crew_sync::auth::AuthCtx {
        agent_id: sqlx::query_scalar::<_, Uuid>("SELECT id FROM agents WHERE name = 'joaquin'")
            .fetch_one(&h.pool)
            .await
            .unwrap(),
        agent_name: "joaquin".into(),
        team_id: team_id(&h.pool, "acme").await,
        team_slug: "acme".into(),
        session: "obrero".into(),
        session_id: Some(cred["session_id"].as_str().unwrap().parse().unwrap()),
        session_epoch: Some(cred["epoch"].as_i64().unwrap()),
        token_id: None,
    };
    let resumed = call(&window, "resume_session", json!({})).await;
    assert_eq!(resumed["epoch"], 2);

    let err = locks::release_lock(&h.pool, &stale, "deploy")
        .await
        .expect_err("a replaced connection released the live window's lock")
        .to_string();
    assert!(err.contains("stale"), "{err}");

    let err = messaging::read_messages(
        &h.pool,
        &stale,
        messaging::ReadInput {
            scope: String::new(),
            only_new: true,
            limit: 50,
            all_sessions: false,
        },
    )
    .await
    .expect_err("a replaced connection advanced the live window's cursor")
    .to_string();
    assert!(err.contains("stale"), "{err}");

    // The live window still holds the lock and still sees its message.
    let live = connect(&h.base, resumed["session_token"].as_str().unwrap()).await;
    let locks_now = call(&live, "list_locks", json!({})).await;
    assert!(
        locks_now["locks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|l| l["name"] == "deploy"),
        "the lock is gone: {locks_now}"
    );
    let unread = call(&live, "read_messages", json!({})).await;
    assert!(
        unread["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["body"] == "para la ventana viva"),
        "the live window lost a message to a cursor it never moved: {unread}"
    );

    for c in [agent, window, dani, live] {
        let _ = c.cancel().await;
    }
    h.shutdown().await;
}

/// And the inbox: a replaced connection does not pull the live window's
/// references off the consumer.
#[tokio::test]
async fn a_replaced_window_cannot_fetch_the_inbox_of_the_one_that_replaced_it() {
    use ai_crew_sync::store::jetstream::Config;
    use ai_crew_sync::store::{inbox, routing::Backends};

    let h = require_db_broker!("t_inbox_epoch_fence");
    let token = seed_agent(&h.pool, "acme", "joaquin").await;
    enable_conversations(&h.pool, "acme").await;
    let agent = connect(&h.base, &token).await;
    let cred = call(&agent, "register_session", json!({"session": "lector"})).await;
    let window = connect(&h.base, cred["session_token"].as_str().unwrap()).await;

    let stale = ai_crew_sync::auth::AuthCtx {
        agent_id: sqlx::query_scalar::<_, Uuid>("SELECT id FROM agents WHERE name = 'joaquin'")
            .fetch_one(&h.pool)
            .await
            .unwrap(),
        agent_name: "joaquin".into(),
        team_id: team_id(&h.pool, "acme").await,
        team_slug: "acme".into(),
        session: "lector".into(),
        session_id: Some(cred["session_id"].as_str().unwrap().parse().unwrap()),
        session_epoch: Some(cred["epoch"].as_i64().unwrap()),
        token_id: None,
    };
    let resumed = call(&window, "resume_session", json!({})).await;
    assert_eq!(resumed["epoch"], 2);

    let backends = Backends::with_jetstream(h.pool.clone(), Config::new(nats_url()));
    let err = inbox::fetch(&h.pool, &backends, &stale, Some(5))
        .await
        .expect_err("a replaced connection fetched the live window's inbox")
        .to_string();
    assert!(err.contains("stale"), "{err}");

    for c in [agent, window] {
        let _ = c.cancel().await;
    }
    h.shutdown().await;
}

/// The attachment cap is decided under the parent's row lock. Counted
/// outside the transaction, ten uploads racing for the last slots all saw
/// room and all committed.
#[tokio::test]
async fn the_attachment_cap_holds_under_a_race() {
    use base64::Engine;
    let h = require_db!("t_attachment_cap_race");
    let token = seed_agent(&h.pool, "acme", "joaquin").await;
    let client = Arc::new(connect(&h.base, &token).await);
    call(
        &client,
        "create_task",
        json!({"key": "adjuntos", "title": "muchos"}),
    )
    .await;

    let data = base64::engine::general_purpose::STANDARD.encode(b"x");
    let mut handles = Vec::new();
    for i in 0..12 {
        let c = Arc::clone(&client);
        let data = data.clone();
        handles.push(tokio::spawn(async move {
            let args: serde_json::Map<String, Value> = serde_json::from_value(json!({
                "task": "adjuntos", "filename": format!("f{i}.txt"), "data_base64": data
            }))
            .unwrap();
            c.call_tool(CallToolRequestParams::new("attach_file".to_string()).with_arguments(args))
                .await
                .map(|r| r.is_error != Some(true))
                .unwrap_or(false)
        }));
    }
    let mut ok = 0;
    for h in handles {
        if h.await.unwrap() {
            ok += 1;
        }
    }
    let (stored,): (i64,) = sqlx::query_as("SELECT count(*) FROM attachments")
        .fetch_one(&h.pool)
        .await
        .unwrap();
    assert_eq!(
        stored, 8,
        "the cap is eight, whatever the race: {stored} stored"
    );
    assert_eq!(ok, 8, "and exactly eight callers were told yes");

    let _ = Arc::try_unwrap(client).ok().map(|c| c.cancel());
    h.shutdown().await;
}

/// A live label is refused even when the refusal spells a missing method.
/// The conversation id here hashes to the label `s-32601f03e877`; the bus
/// quotes it when a second process tries to register it without the
/// window's credential, and the proxy once read that "-32601" as "this bus
/// has no register_session" and carried on label-only with the parent
/// token — the one case the conflict exists to stop.
#[tokio::test]
async fn a_live_label_is_refused_even_when_it_spells_a_missing_method() {
    let h = require_db!("t_proxy_32601");
    let token = seed_agent(&h.pool, "acme", "joaquin").await;
    let profiles = [("acme", "acme", "joaquin", token.as_str())];
    // Two config dirs: the second process has no binding to resume from,
    // so it can only ask the bus to register the label afresh.
    let dir_a = proxy_config_dir(&h.base, &profiles);
    let dir_b = proxy_config_dir(&h.base, &profiles);
    let mut repos = Vec::new();
    for dir in [&dir_a, &dir_b] {
        let repo = dir.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        std::fs::write(repo.join(".acs.toml"), "profile = \"acme\"\n").unwrap();
        repos.push(repo);
    }

    let holder = spawn_proxy(&dir_a, &repos[0], &["--host-session", "conv-1714608"], &[]).await;
    let s = call(&holder, "session_status", json!({})).await;
    assert_eq!(
        s["session"], "s-32601f03e877",
        "the fixture id moved; pick another: {s}"
    );
    assert_eq!(s["agent"], "joaquin");

    let intruder = spawn_proxy(&dir_b, &repos[1], &["--host-session", "conv-1714608"], &[]).await;
    let s2 = call(&intruder, "session_status", json!({})).await;
    assert!(
        s2["agent"].is_null(),
        "the second process is not connected: {s2}"
    );
    let reason = s2["error"].as_str().unwrap_or_default();
    assert!(reason.contains("still live"), "{s2}");
    let err = call_expect_error(&intruder, "whoami", json!({})).await;
    assert!(err.contains("not connected"), "{err}");

    // The window that owns the label is untouched.
    assert_eq!(call(&holder, "whoami", json!({})).await["agent"], "joaquin");
    for c in [holder, intruder] {
        let _ = c.cancel().await;
    }
    h.shutdown().await;
}

/// Route a team's new conversations to JetStream the way an operator does:
/// the flag on the team plus provisioned streams. The harness server has the
/// broker configured whenever `require_db_broker!` was used.
async fn route_team_to_jetstream(h: &Harness, team: &str) {
    use ai_crew_sync::store::jetstream::{Config, JetStreamBackend};
    let id = team_id(&h.pool, team).await;
    sqlx::query("UPDATE teams SET default_backend = 'jetstream' WHERE id = $1")
        .bind(id)
        .execute(&h.pool)
        .await
        .unwrap();
    let config = Config::new(nats_url()).with_limits(1_000, 16 * 1024 * 1024);
    JetStreamBackend::provision(&config, id).await.unwrap();
}

/// Re-admitting a removed member is a moderator decision the bus records as
/// 'member.readmit'. The audit table's check never listed that action, so
/// the invitation rolled back with a bare "database error" and the member
/// stayed out (#125). The rule the readmission keeps: nothing said before
/// it comes back, however the inviter asks.
#[tokio::test]
async fn a_removed_member_is_readmitted_without_its_old_history() {
    for backend in ["postgres", "jetstream"] {
        let schema = format!("t_readmit_{backend}");
        let h = require_db_broker!(&schema);
        let token = seed_agent(&h.pool, "acme", "joaquin").await;
        let dani_token = seed_agent(&h.pool, "acme", "dani").await;
        enable_conversations(&h.pool, "acme").await;
        if backend == "jetstream" {
            route_team_to_jetstream(&h, "acme").await;
        }
        let owner = connect_with_session(&h.base, &token, "impl").await;
        let dani = connect_with_session(&h.base, &dani_token, "core").await;

        let convo = call(
            &owner,
            "create_conversation",
            json!({"title": "readmission", "private": true}),
        )
        .await;
        let cid = convo["id"].as_str().unwrap().to_owned();
        let send = |body: &'static str| {
            let owner = &owner;
            let cid = cid.clone();
            async move {
                call(
                    owner,
                    "send_conversation_message",
                    json!({"conversation_id": cid, "body": body, "request_id": request_id()}),
                )
                .await["message_id"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            }
        };
        let before = send("before the removal").await;
        call(
            &owner,
            "invite_to_conversation",
            json!({"conversation_id": cid, "address": "dani/core", "history_from_start": true}),
        )
        .await;
        call(&dani, "join_conversation", json!({"conversation_id": cid})).await;
        let seen = call(
            &dani,
            "get_conversation_message",
            json!({"message_id": before}),
        )
        .await;
        assert_eq!(seen["body"], "before the removal", "[{backend}] {seen}");

        call(
            &owner,
            "remove_conversation_member",
            json!({"conversation_id": cid, "address": "dani/core"}),
        )
        .await;
        let meanwhile = send("said while removed").await;

        // The readmission goes through, whatever history the inviter asks
        // for, and it is recorded as what it is.
        call(
            &owner,
            "invite_to_conversation",
            json!({"conversation_id": cid, "address": "dani/core", "history_from_start": true}),
        )
        .await;
        let cid_uuid: Uuid = cid.parse().unwrap();
        let (state, from_seq): (String, Option<i64>) = sqlx::query_as(
            "SELECT state, history_from_seq FROM conversation_memberships
              WHERE conversation_id = $1 AND session = 'core'",
        )
        .bind(cid_uuid)
        .fetch_one(&h.pool)
        .await
        .unwrap();
        assert_eq!(state, "invited", "[{backend}]");
        assert_eq!(
            from_seq,
            Some(2),
            "[{backend}] a readmission starts here, not at the start"
        );
        let (readmits,): (i64,) = sqlx::query_as(
            "SELECT count(*) FROM conversation_audit
              WHERE conversation_id = $1 AND action = 'member.readmit'",
        )
        .bind(cid_uuid)
        .fetch_one(&h.pool)
        .await
        .unwrap();
        assert_eq!(readmits, 1, "[{backend}] the readmission is audited as one");

        call(&dani, "join_conversation", json!({"conversation_id": cid})).await;
        for hidden in [&before, &meanwhile] {
            let err = call_expect_error(
                &dani,
                "get_conversation_message",
                json!({"message_id": hidden}),
            )
            .await;
            assert!(!err.contains("database error"), "[{backend}] {err}");
        }
        let after = send("after the readmission").await;
        let seen = call(
            &dani,
            "get_conversation_message",
            json!({"message_id": after}),
        )
        .await;
        assert_eq!(seen["body"], "after the readmission", "[{backend}] {seen}");
        let read = call(&dani, "read_conversation", json!({"conversation_id": cid})).await;
        assert_eq!(
            read["messages"].as_array().map(Vec::len),
            Some(1),
            "[{backend}] only what was said after the readmission: {read}"
        );

        for c in [owner, dani] {
            let _ = c.cancel().await;
        }
        h.shutdown().await;
    }
}

/// A moderator admitted without the past cannot hand that past to anyone:
/// not to another agent, not to another window of its own agent. Its grant
/// is clamped to its own floor, for bodies and for receipts alike, and the
/// audit row keeps what was asked. An inviter that can read the start still
/// grants it (#124).
#[tokio::test]
async fn a_late_moderator_cannot_grant_history_it_cannot_read() {
    for backend in ["postgres", "jetstream"] {
        let schema = format!("t_late_moderator_{backend}");
        let h = require_db_broker!(&schema);
        let owner_token = seed_agent(&h.pool, "acme", "joaquin").await;
        let reader_token = seed_agent(&h.pool, "acme", "dani").await;
        let outsider_token = seed_agent(&h.pool, "acme", "marta").await;
        let full_token = seed_agent(&h.pool, "acme", "luis").await;
        enable_conversations(&h.pool, "acme").await;
        if backend == "jetstream" {
            route_team_to_jetstream(&h, "acme").await;
        }
        let owner = connect_with_session(&h.base, &owner_token, "owner").await;
        let reader = connect_with_session(&h.base, &reader_token, "reader").await;
        let outsider = connect_with_session(&h.base, &outsider_token, "outsider").await;
        let sibling = connect_with_session(&h.base, &reader_token, "other").await;
        let full = connect_with_session(&h.base, &full_token, "full").await;

        let convo = call(
            &owner,
            "create_conversation",
            json!({"title": "late moderator", "private": true}),
        )
        .await;
        let cid = convo["id"].as_str().unwrap().to_owned();
        let withheld = call(
            &owner,
            "send_conversation_message",
            json!({"conversation_id": cid, "body": "history withheld from later moderator",
                   "request_id": request_id()}),
        )
        .await["message_id"]
            .as_str()
            .unwrap()
            .to_owned();

        // A moderator, admitted from here on.
        call(
            &owner,
            "invite_to_conversation",
            json!({"conversation_id": cid, "address": "dani/reader", "role": "moderator"}),
        )
        .await;
        call(
            &reader,
            "join_conversation",
            json!({"conversation_id": cid}),
        )
        .await;
        let err = call_expect_error(
            &reader,
            "get_conversation_message",
            json!({"message_id": withheld}),
        )
        .await;
        assert!(!err.contains("database error"), "[{backend}] {err}");

        // It invites another agent and another window of its own agent,
        // asking for the whole history. Both invitations go through, with
        // the moderator's own floor.
        for address in ["marta/outsider", "dani/other"] {
            call(
                &reader,
                "invite_to_conversation",
                json!({"conversation_id": cid, "address": address, "history_from_start": true}),
            )
            .await;
        }
        for (address, window) in [("marta/outsider", &outsider), ("dani/other", &sibling)] {
            call(window, "join_conversation", json!({"conversation_id": cid})).await;
            let err = call_expect_error(
                window,
                "get_conversation_message",
                json!({"message_id": withheld}),
            )
            .await;
            assert!(
                !err.contains("database error"),
                "[{backend}] {address} body: {err}"
            );
            let err = call_expect_error(
                window,
                "get_message_receipts",
                json!({"message_id": withheld}),
            )
            .await;
            assert!(
                !err.contains("database error"),
                "[{backend}] {address} receipts: {err}"
            );
            let read = call(window, "read_conversation", json!({"conversation_id": cid})).await;
            assert_eq!(
                read["messages"].as_array().map(Vec::len),
                Some(0),
                "[{backend}] {address} reads nothing from before the moderator: {read}"
            );
        }
        let cid_uuid: Uuid = cid.parse().unwrap();
        let floors: Vec<(String, Option<i64>)> = sqlx::query_as(
            "SELECT session, history_from_seq FROM conversation_memberships
              WHERE conversation_id = $1 AND session IN ('reader', 'outsider', 'other')
              ORDER BY session",
        )
        .bind(cid_uuid)
        .fetch_all(&h.pool)
        .await
        .unwrap();
        assert!(
            floors.iter().all(|(_, f)| *f == Some(1)),
            "[{backend}] every seat the moderator handed out starts where it did: {floors:?}"
        );
        let (requested,): (i64,) = sqlx::query_as(
            "SELECT count(*) FROM conversation_audit
              WHERE conversation_id = $1 AND action = 'member.invite'
                AND (detail->>'history_from_start_requested')::boolean
                AND (detail->>'history_from_seq')::bigint = 1",
        )
        .bind(cid_uuid)
        .fetch_one(&h.pool)
        .await
        .unwrap();
        assert_eq!(
            requested, 2,
            "[{backend}] the audit keeps what was asked and what was given"
        );

        // The owner can read the start, so the owner can grant it.
        call(
            &owner,
            "invite_to_conversation",
            json!({"conversation_id": cid, "address": "luis/full", "history_from_start": true}),
        )
        .await;
        call(&full, "join_conversation", json!({"conversation_id": cid})).await;
        let seen = call(
            &full,
            "get_conversation_message",
            json!({"message_id": withheld}),
        )
        .await;
        assert_eq!(
            seen["body"], "history withheld from later moderator",
            "[{backend}] {seen}"
        );

        // A seat that had the whole history and left does not get it back
        // from an inviter who cannot read it: the floor is decided now.
        call(&full, "leave_conversation", json!({"conversation_id": cid})).await;
        call(
            &reader,
            "invite_to_conversation",
            json!({"conversation_id": cid, "address": "luis/full", "history_from_start": true}),
        )
        .await;
        call(&full, "join_conversation", json!({"conversation_id": cid})).await;
        let err = call_expect_error(
            &full,
            "get_conversation_message",
            json!({"message_id": withheld}),
        )
        .await;
        assert!(
            !err.contains("database error"),
            "[{backend}] left and back: {err}"
        );
        let (floor,): (Option<i64>,) = sqlx::query_as(
            "SELECT history_from_seq FROM conversation_memberships
              WHERE conversation_id = $1 AND session = 'full'",
        )
        .bind(cid_uuid)
        .fetch_one(&h.pool)
        .await
        .unwrap();
        assert_eq!(
            floor,
            Some(1),
            "[{backend}] the old floor did not come back"
        );

        for c in [owner, reader, outsider, sibling, full] {
            let _ = c.cancel().await;
        }
        h.shutdown().await;
    }
}

/// A transfer to a window that already has a seat changes nothing: it is
/// refused before anything is written, and the target keeps its role, its
/// history boundary and its credential binding while the source stays. A
/// window that holds an invitation, or that was removed, is not a target
/// either. A window that left is, and then the handoff completes (#126).
#[tokio::test]
async fn a_transfer_to_a_window_with_a_seat_changes_nothing() {
    for backend in ["postgres", "jetstream"] {
        let schema = format!("t_transfer_target_{backend}");
        let h = require_db_broker!(&schema);
        let token = seed_agent(&h.pool, "acme", "joaquin").await;
        enable_conversations(&h.pool, "acme").await;
        if backend == "jetstream" {
            route_team_to_jetstream(&h, "acme").await;
        }
        // Two registered windows of the same agent, each with its own
        // credential, plus a third that only ever gets invited.
        let agent = connect(&h.base, &token).await;
        let owner_cred = call(&agent, "register_session", json!({"session": "owner"})).await;
        let owner = connect(&h.base, owner_cred["session_token"].as_str().unwrap()).await;
        let next_cred = call(&agent, "register_session", json!({"session": "next"})).await;
        let next = connect(&h.base, next_cred["session_token"].as_str().unwrap()).await;

        let convo = call(
            &owner,
            "create_conversation",
            json!({"title": "handoff", "private": true}),
        )
        .await;
        let cid = convo["id"].as_str().unwrap().to_owned();
        let cid_uuid: Uuid = cid.parse().unwrap();
        let earlier = call(
            &owner,
            "send_conversation_message",
            json!({"conversation_id": cid, "body": "before the observer arrived",
                   "request_id": request_id()}),
        )
        .await["message_id"]
            .as_str()
            .unwrap()
            .to_owned();
        call(
            &owner,
            "invite_to_conversation",
            json!({"conversation_id": cid, "address": "joaquin/next", "role": "observer"}),
        )
        .await;
        call(&next, "join_conversation", json!({"conversation_id": cid})).await;
        let err = call_expect_error(
            &next,
            "get_conversation_message",
            json!({"message_id": earlier}),
        )
        .await;
        assert!(!err.contains("database error"), "[{backend}] {err}");

        // A message sent while the observer is seated has receipts for the
        // seats of that moment; a refused transfer must leave them as they
        // are, denominator included.
        let seated = call(
            &owner,
            "send_conversation_message",
            json!({"conversation_id": cid, "body": "while the observer is here",
                   "request_id": request_id()}),
        )
        .await["message_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let receipts_before = call(
            &owner,
            "get_message_receipts",
            json!({"message_id": seated}),
        )
        .await;
        assert!(
            receipts_before["total"].as_u64() >= Some(1),
            "[{backend}] {receipts_before}"
        );

        let seat = |session: &'static str| {
            let pool = h.pool.clone();
            async move {
                sqlx::query_as::<_, (String, String, Option<i64>, Option<Uuid>, Option<Uuid>)>(
                    "SELECT role, state, history_from_seq, session_id, superseded_by
                       FROM conversation_memberships
                      WHERE conversation_id = $1 AND session = $2",
                )
                .bind(cid_uuid)
                .bind(session)
                .fetch_one(&pool)
                .await
                .unwrap()
            }
        };
        let next_before = seat("next").await;
        assert_eq!(next_before.0, "observer", "[{backend}]");
        assert_eq!(next_before.2, Some(1), "[{backend}]");
        assert!(
            next_before.3.is_some(),
            "[{backend}] the seat is bound to the window"
        );

        // Already seated: refused, and nothing moved.
        let err = call_expect_error(
            &owner,
            "transfer_membership",
            json!({"conversation_id": cid, "to": "joaquin/next"}),
        )
        .await;
        assert!(err.contains("seat of its own"), "[{backend}] {err}");
        assert!(err.contains("nothing was written"), "[{backend}] {err}");
        assert_eq!(
            seat("next").await,
            next_before,
            "[{backend}] the target is untouched"
        );
        let source = seat("owner").await;
        assert_eq!(
            source.1, "active",
            "[{backend}] the source is not superseded"
        );
        assert!(source.4.is_none(), "[{backend}]");
        let err = call_expect_error(
            &next,
            "get_conversation_message",
            json!({"message_id": earlier}),
        )
        .await;
        assert!(
            !err.contains("database error"),
            "[{backend}] still withheld: {err}"
        );
        let receipts_after = call(
            &owner,
            "get_message_receipts",
            json!({"message_id": seated}),
        )
        .await;
        assert_eq!(
            receipts_after, receipts_before,
            "[{backend}] the receipts and their denominator are untouched"
        );
        let err =
            call_expect_error(&next, "join_conversation", json!({"conversation_id": cid})).await;
        assert!(
            err.contains("already in this conversation"),
            "[{backend}] {err}"
        );

        // Merely invited: refused too.
        call(
            &owner,
            "invite_to_conversation",
            json!({"conversation_id": cid, "address": "joaquin/third"}),
        )
        .await;
        let err = call_expect_error(
            &owner,
            "transfer_membership",
            json!({"conversation_id": cid, "to": "joaquin/third"}),
        )
        .await;
        assert!(
            err.contains("already holds an invitation"),
            "[{backend}] {err}"
        );

        // Removed: a transfer does not undo a removal.
        call(
            &owner,
            "remove_conversation_member",
            json!({"conversation_id": cid, "address": "joaquin/next"}),
        )
        .await;
        let err = call_expect_error(
            &owner,
            "transfer_membership",
            json!({"conversation_id": cid, "to": "joaquin/next"}),
        )
        .await;
        assert!(err.contains("does not undo a removal"), "[{backend}] {err}");
        assert_eq!(seat("next").await.1, "removed", "[{backend}]");

        // Re-admitted, then gone of its own accord: that seat can be offered
        // the handoff, and accepting it completes the transfer.
        call(
            &owner,
            "invite_to_conversation",
            json!({"conversation_id": cid, "address": "joaquin/next"}),
        )
        .await;
        call(&next, "join_conversation", json!({"conversation_id": cid})).await;
        call(&next, "leave_conversation", json!({"conversation_id": cid})).await;
        let proposal = call(
            &owner,
            "transfer_membership",
            json!({"conversation_id": cid, "to": "joaquin/next"}),
        )
        .await;
        assert_eq!(proposal["state"], "proposed", "[{backend}] {proposal}");
        call(&next, "join_conversation", json!({"conversation_id": cid})).await;
        let seen = call(
            &next,
            "get_conversation_message",
            json!({"message_id": earlier}),
        )
        .await;
        assert_eq!(
            seen["body"], "before the observer arrived",
            "[{backend}] the seat's history came with it: {seen}"
        );
        let taken = seat("next").await;
        assert_eq!(
            (taken.0.as_str(), taken.1.as_str(), taken.2),
            ("owner", "active", None),
            "[{backend}]"
        );
        assert!(
            taken.3.is_some(),
            "[{backend}] bound to the window that accepted"
        );
        let source = seat("owner").await;
        assert_eq!(source.1, "left", "[{backend}] the source is superseded");
        assert!(source.4.is_some(), "[{backend}]");

        for c in [agent, owner, next] {
            let _ = c.cancel().await;
        }
        h.shutdown().await;
    }
}

/// A waiter asking for notes, tasks or locks wakes when a teammate writes
/// one. Every wait test before this asked for messages, so the other three
/// kinds were never seen to arrive (#128).
#[tokio::test]
async fn wait_for_updates_wakes_on_notes_tasks_and_locks() {
    let h = require_db!("t_wait_kinds");
    let a = seed_agent(&h.pool, "acme", "joaquin").await;
    let b = seed_agent(&h.pool, "acme", "marta").await;
    let writer = connect(&h.base, &a).await;

    for (kind, tool, args) in [
        (
            "note",
            "set_note",
            json!({"key": "wake-note", "value": "x"}),
        ),
        (
            "task",
            "create_task",
            json!({"key": "wake-task", "title": "t"}),
        ),
        (
            "lock",
            "acquire_lock",
            json!({"name": "wake-lock", "ttl_seconds": 30}),
        ),
    ] {
        let waiter = connect(&h.base, &b).await;
        let wait = tokio::spawn({
            let kind = kind.to_owned();
            async move {
                let r = call(
                    &waiter,
                    "wait_for_updates",
                    json!({"kinds": [kind], "timeout_seconds": 8}),
                )
                .await;
                let _ = waiter.cancel().await;
                r
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;
        call(&writer, tool, args).await;
        let r = wait.await.unwrap();
        assert_eq!(r["woke"], true, "{kind}: {r}");
        assert_eq!(r["events"][0]["kind"], kind, "{kind}: {r}");
    }
    let _ = writer.cancel().await;
    h.shutdown().await;
}

/// A LISTEN connection can die without a word: Swarm's IPVS forgets an idle
/// TCP connection after fifteen minutes and tells neither end, and a bus
/// sat "attached" for hours while no wake arrived (#128). Each replica now
/// pings itself through Postgres; three unanswered pings and the listener is
/// dropped and attached afresh, and `/health` says which state it is in.
#[tokio::test]
async fn a_listener_that_went_deaf_is_noticed_and_reattached() {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut h = require_db!("t_deaf_listener");
    let token = seed_agent(&h.pool, "acme", "joaquin").await;
    let marta = seed_agent(&h.pool, "acme", "marta").await;

    // A TCP proxy in front of Postgres that can turn a connection into a
    // black hole: bytes are read and dropped in both directions, the socket
    // stays open, nobody is told. It remembers which connections said
    // LISTEN, because those are the ones the bus can only wait on.
    let url = db_url().unwrap();
    let at = url.rfind('@').unwrap();
    let slash = at + url[at..].find('/').unwrap();
    let upstream = url[at + 1..slash].to_owned();
    let proxy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxied_url = format!(
        "{}@{}{}",
        &url[..at],
        proxy.local_addr().unwrap(),
        &url[slash..]
    );
    let holes: Arc<Mutex<Vec<Arc<AtomicBool>>>> = Arc::default();
    tokio::spawn({
        let holes = holes.clone();
        async move {
            loop {
                let Ok((client, _)) = proxy.accept().await else {
                    break;
                };
                let Ok(server) = tokio::net::TcpStream::connect(&upstream).await else {
                    continue;
                };
                let hole = Arc::new(AtomicBool::new(false));
                let (mut cr, mut cw) = client.into_split();
                let (mut sr, mut sw) = server.into_split();
                tokio::spawn({
                    let hole = hole.clone();
                    let holes = holes.clone();
                    async move {
                        let mut buf = vec![0u8; 16 * 1024];
                        loop {
                            let n = match cr.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(n) => n,
                            };
                            if buf[..n].windows(6).any(|w| w == b"LISTEN") {
                                holes.lock().unwrap().push(hole.clone());
                            }
                            if hole.load(Ordering::SeqCst) {
                                continue;
                            }
                            if sw.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                    }
                });
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 16 * 1024];
                    loop {
                        let n = match sr.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => n,
                        };
                        if hole.load(Ordering::SeqCst) {
                            continue;
                        }
                        if cw.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                });
            }
        }
    });

    // A second replica whose every connection goes through the proxy, pinging
    // itself every second so the test does not wait minutes.
    let schema = h.schema.clone();
    let proxied = PgPoolOptions::new()
        .max_connections(8)
        .after_connect(move |conn, _| {
            let schema = schema.clone();
            Box::pin(async move {
                sqlx::query(sqlx::AssertSqlSafe(format!("SET search_path TO {schema}")))
                    .execute(&mut *conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&proxied_url)
        .await
        .unwrap();
    let (replica, handle) = spawn_server_with_ping(proxied, h.ct.child_token(), None, 1).await;
    h.servers.push(handle);

    let health = |base: String| async move {
        reqwest::get(format!("{base}/health"))
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap()
    };
    let wait_until = |pred: fn(&serde_json::Value) -> bool, what: &'static str| {
        let replica = replica.clone();
        async move {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
            loop {
                let h = health(replica.clone()).await;
                if pred(&h["events"]) {
                    return h;
                }
                assert!(std::time::Instant::now() < deadline, "{what}: {h}");
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    };
    // The first echo is the proof of readiness the log line never was.
    let ready = wait_until(|e| e["echoes"].as_u64() >= Some(1), "first echo").await;
    assert_eq!(ready["events"]["listener"], "live", "{ready}");
    assert_eq!(ready["events"]["attachments"], 1, "{ready}");
    assert_eq!(ready["status"], "ok", "{ready}");

    // A wake through the proxied replica works while its listener hears.
    let wake = |replica: String, writer_base: String, key: &'static str| {
        let token = token.clone();
        let marta = marta.clone();
        async move {
            let waiter = connect(&replica, &marta).await;
            let wait = tokio::spawn(async move {
                let r = call(
                    &waiter,
                    "wait_for_updates",
                    json!({"kinds": ["note"], "timeout_seconds": 10}),
                )
                .await;
                let _ = waiter.cancel().await;
                r
            });
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            let writer = connect(&writer_base, &token).await;
            call(&writer, "set_note", json!({"key": key, "value": "x"})).await;
            let _ = writer.cancel().await;
            wait.await.unwrap()
        }
    };
    let woke = wake(replica.clone(), h.base.clone(), "before").await;
    assert_eq!(woke["woke"], true, "{woke}");

    // The socket goes dead without a word. Every LISTEN connection the proxy
    // has seen so far is black-holed; the one the replica opens next is not.
    let dead: Vec<_> = holes.lock().unwrap().drain(..).collect();
    assert!(!dead.is_empty(), "the proxy saw the LISTEN connection");
    for hole in &dead {
        hole.store(true, Ordering::SeqCst);
    }

    // The replica notices on its own, and comes back hearing.
    let back = wait_until(
        |e| e["attachments"].as_u64() >= Some(2) && e["listener"] == "live",
        "reattached",
    )
    .await;
    let echoes_at_reattach = back["events"]["echoes"].as_u64().unwrap();
    wait_until(
        |e| e["listener"] == "live" && e["last_echo_seconds"].as_i64() <= Some(2),
        "hearing again",
    )
    .await;
    let after = health(replica.clone()).await;
    assert!(
        after["events"]["echoes"].as_u64() >= Some(echoes_at_reattach),
        "{after}"
    );
    assert_eq!(after["status"], "ok", "{after}");

    // And wakes work again, on the fresh connection.
    let woke = wake(replica.clone(), h.base.clone(), "after").await;
    assert_eq!(woke["woke"], true, "after the reattach: {woke}");

    h.shutdown().await;
}

/// An owner's seat is only an owner's to change. Re-inviting someone already
/// seated changes their role on the spot, and a moderator used that to
/// demote the owner to observer and then remove it, walking around "only an
/// owner can remove another owner" (#133). The refusal leaves the owner's
/// role, floor and binding exactly as they were; an owner re-inviting a
/// moderator still changes its role.
#[tokio::test]
async fn a_moderator_cannot_demote_an_owner_by_reinviting_it() {
    for backend in ["postgres", "jetstream"] {
        let schema = format!("t_owner_seat_{backend}");
        let h = require_db_broker!(&schema);
        let token = seed_agent(&h.pool, "acme", "joaquin").await;
        let reader_token = seed_agent(&h.pool, "acme", "dani").await;
        enable_conversations(&h.pool, "acme").await;
        if backend == "jetstream" {
            route_team_to_jetstream(&h, "acme").await;
        }
        // The owner is a registered window, so its seat carries a binding
        // the refusal has to leave alone.
        let agent = connect(&h.base, &token).await;
        let cred = call(&agent, "register_session", json!({"session": "owner"})).await;
        let owner = connect(&h.base, cred["session_token"].as_str().unwrap()).await;
        let reader = connect_with_session(&h.base, &reader_token, "reader").await;

        let convo = call(
            &owner,
            "create_conversation",
            json!({"title": "whose seat", "private": true}),
        )
        .await;
        let cid = convo["id"].as_str().unwrap().to_owned();
        let cid_uuid: Uuid = cid.parse().unwrap();
        let first = call(
            &owner,
            "send_conversation_message",
            json!({"conversation_id": cid, "body": "the owner's own thread",
                   "request_id": request_id()}),
        )
        .await["message_id"]
            .as_str()
            .unwrap()
            .to_owned();
        call(
            &owner,
            "invite_to_conversation",
            json!({"conversation_id": cid, "address": "dani/reader", "role": "moderator"}),
        )
        .await;
        call(
            &reader,
            "join_conversation",
            json!({"conversation_id": cid}),
        )
        .await;

        let seat = |session: &'static str| {
            let pool = h.pool.clone();
            async move {
                sqlx::query_as::<_, (String, String, Option<i64>, Option<Uuid>)>(
                    "SELECT role, state, history_from_seq, session_id
                       FROM conversation_memberships
                      WHERE conversation_id = $1 AND session = $2",
                )
                .bind(cid_uuid)
                .bind(session)
                .fetch_one(&pool)
                .await
                .unwrap()
            }
        };
        let owner_seat = seat("owner").await;
        assert_eq!(owner_seat.0, "owner", "[{backend}]");
        assert!(
            owner_seat.3.is_some(),
            "[{backend}] the owner's seat is bound"
        );

        let err = call_expect_error(
            &reader,
            "remove_conversation_member",
            json!({"conversation_id": cid, "address": "joaquin/owner"}),
        )
        .await;
        assert!(
            err.contains("only an owner can remove another owner"),
            "[{backend}] {err}"
        );

        // Every role a moderator could hand out, and the default one.
        for role in [
            Some("observer"),
            Some("participant"),
            Some("moderator"),
            None,
        ] {
            let mut args = json!({"conversation_id": cid, "address": "joaquin/owner"});
            if let Some(role) = role {
                args["role"] = json!(role);
            }
            let err = call_expect_error(&reader, "invite_to_conversation", args).await;
            assert!(
                err.contains("only an owner can change or remove another owner"),
                "[{backend}] role {role:?}: {err}"
            );
            assert!(!err.contains("database error"), "[{backend}] {err}");
            assert_eq!(
                seat("owner").await,
                owner_seat,
                "[{backend}] role {role:?} changed the seat"
            );
            let seen = call(
                &owner,
                "get_conversation_message",
                json!({"message_id": first}),
            )
            .await;
            assert_eq!(seen["body"], "the owner's own thread", "[{backend}] {seen}");
        }
        let err = call_expect_error(
            &reader,
            "remove_conversation_member",
            json!({"conversation_id": cid, "address": "joaquin/owner"}),
        )
        .await;
        assert!(
            err.contains("only an owner can remove another owner"),
            "[{backend}] {err}"
        );
        let read = call(&owner, "read_conversation", json!({"conversation_id": cid})).await;
        assert_eq!(
            read["messages"].as_array().map(Vec::len),
            Some(1),
            "[{backend}] {read}"
        );

        // The ordinary path is untouched: an owner re-inviting a moderator
        // changes its role, and nothing else about its seat.
        let reader_seat = seat("reader").await;
        call(
            &owner,
            "invite_to_conversation",
            json!({"conversation_id": cid, "address": "dani/reader", "role": "observer"}),
        )
        .await;
        let demoted = seat("reader").await;
        assert_eq!(demoted.0, "observer", "[{backend}] {demoted:?}");
        assert_eq!(
            (&demoted.1, demoted.2, demoted.3),
            (&reader_seat.1, reader_seat.2, reader_seat.3),
            "[{backend}] state, floor and binding stay"
        );
        let read = call(
            &reader,
            "read_conversation",
            json!({"conversation_id": cid}),
        )
        .await;
        assert!(read["messages"].is_array(), "[{backend}] {read}");

        for c in [agent, owner, reader] {
            let _ = c.cancel().await;
        }
        h.shutdown().await;
    }
}
