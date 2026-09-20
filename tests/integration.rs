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
}

impl Harness {
    /// Start another bus instance against the SAME database and schema — the
    /// production topology: N processes, one Postgres, each with its own
    /// LISTEN connection and its own in-process event hub.
    async fn add_replica(&mut self) -> String {
        let (base, handle) = spawn_server(self.pool.clone(), self.ct.child_token()).await;
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
    let (base, handle) = spawn_server(pool.clone(), ct.child_token()).await;

    Some(Harness {
        pool,
        base,
        ct,
        servers: vec![handle],
        schema: schema.to_owned(),
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
    })
}

/// Skipping is a convenience for a laptop with no database, and a silent
/// green build everywhere else. `make test` and CI set this, so a broken
/// Postgres setup fails the run instead of passing zero tests.
fn db_required() -> bool {
    std::env::var("AI_CREW_SYNC_REQUIRE_DB").is_ok_and(|v| v != "0")
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
    let ratio = large.as_secs_f64() / small.as_secs_f64();
    assert!(
        ratio < 3.0,
        "digest over 60 channels took {large:?} against {small:?} over 2 \
         (ratio {ratio:.2}): the cost is scaling with channel count"
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

    let p = spawn_proxy(&dir, &repo, &["--role", "implementation"], &[]).await;
    let start = call(&p, "session_status", json!({})).await;
    assert_eq!(start["agent"], "joaquin");
    let session = start["session"].as_str().unwrap().to_owned();

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
