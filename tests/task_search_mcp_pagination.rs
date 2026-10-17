#![allow(clippy::too_many_lines)]

#[path = "task_pagination_fixture.rs"]
mod fixture;

use std::collections::HashSet;

use ai_crew_sync::{
    auth,
    serve::{self, ServeOptions},
};
use fixture::{LARGE_FIXTURE_SIZE, cleanup, setup_fixture};
use rmcp::{
    ServiceExt,
    model::{CallToolRequestParams, ClientConfig},
    transport::{
        StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use serde_json::{Map, Value, json};
use sqlx::PgPool;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

struct McpHarness {
    client: rmcp::service::RunningService<rmcp::RoleClient, ClientConfig>,
    cancellation: CancellationToken,
    server: JoinHandle<Result<(), std::io::Error>>,
}

async fn start_mcp(
    pool: PgPool,
    agent_id: Uuid,
    label: &str,
) -> Result<McpHarness, Box<dyn std::error::Error>> {
    let raw_token = auth::generate_token();
    sqlx::query(
        "INSERT INTO api_tokens (agent_id, token_hash, prefix, label) VALUES ($1, $2, $3, $4)",
    )
    .bind(agent_id)
    .bind(auth::hash_token(&raw_token))
    .bind(auth::token_prefix(&raw_token))
    .bind(label)
    .execute(&pool)
    .await?;

    let cancellation = CancellationToken::new();
    let router = serve::build_router(
        pool,
        &ServeOptions {
            bind: "127.0.0.1:0".to_owned(),
            allowed_hosts: Vec::new(),
            allowed_origins: Vec::new(),
            max_request_bytes: serve::DEFAULT_MAX_REQUEST_BYTES,
            rate_limit_per_minute: 0,
            dashboard_secret: b"task-search-mcp-pagination-test-secret".to_vec(),
            nats_url: None,
            nats_credentials: None,
            publication_worker: false,
            event_ping_secs: 3_600,
        },
        cancellation.child_token(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move { axum::serve(listener, router).await });

    let mut config = StreamableHttpClientTransportConfig::with_uri(format!("http://{address}/mcp"));
    config.auth_header = Some(raw_token);
    config.allow_stateless = true;
    config.custom_headers.insert(
        auth::SESSION_HEADER.parse()?,
        format!("{label}-session").parse()?,
    );
    let transport = StreamableHttpClientTransport::from_config(config);
    let client = ClientConfig::default().serve(transport).await?;

    Ok(McpHarness {
        client,
        cancellation,
        server,
    })
}

async fn call_tool(
    client: &rmcp::service::RunningService<rmcp::RoleClient, ClientConfig>,
    arguments: Value,
) -> Result<Value, Box<dyn std::error::Error>> {
    let arguments = arguments
        .as_object()
        .cloned()
        .ok_or_else(|| "MCP arguments must be an object".to_owned())?;
    let result = client
        .call_tool(CallToolRequestParams::new("list_tasks".to_owned()).with_arguments(arguments))
        .await?;
    if result.is_error == Some(true) {
        return Err(format!("list_tasks returned an MCP error: {:?}", result.content).into());
    }
    Ok(result
        .structured_content
        .clone()
        .unwrap_or_else(|| json!({})))
}

fn task_keys(page: &Value) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    Ok(page["tasks"]
        .as_array()
        .ok_or_else(|| "MCP page omitted tasks".to_owned())?
        .iter()
        .map(|task| {
            task["key"]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| "MCP task omitted key".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?)
}

async fn seed_tasks(
    pool: &PgPool,
    team_id: Uuid,
    agent_id: Uuid,
    prefix: &str,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    for ordinal in 0..LARGE_FIXTURE_SIZE {
        sqlx::query(
            "INSERT INTO tasks (team_id, key, title, description, metadata, created_by) VALUES ($1, $2, $3, $4, jsonb_build_object('fixture_ordinal', $5), $6)",
        )
        .bind(team_id)
        .bind(format!("{prefix}:{ordinal:04}"))
        .bind(format!("MCP pagination task {ordinal:04}"))
        .bind(format!("searchable pagination description {ordinal:04}"))
        .bind(ordinal as i32)
        .bind(agent_id)
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query(
        "UPDATE tasks SET updated_at = TIMESTAMPTZ '2026-01-01 00:00:00+00' WHERE team_id = $1 AND key LIKE $2",
    )
    .bind(team_id)
    .bind(format!("{prefix}:%"))
    .execute(&mut *tx)
    .await?;
    tx.commit().await
}

async fn append_tasks(
    pool: &PgPool,
    team_id: Uuid,
    agent_id: Uuid,
    prefix: &str,
) -> Result<(), sqlx::Error> {
    for ordinal in 0..5 {
        sqlx::query(
            "INSERT INTO tasks (team_id, key, title, description, metadata, created_by, updated_at) VALUES ($1, $2, $3, $4, jsonb_build_object('concurrent', true), $5, TIMESTAMPTZ '2026-01-01 00:00:00+00')",
        )
        .bind(team_id)
        .bind(format!("{prefix}:zz-new-{ordinal}"))
        .bind(format!("concurrent appended task {ordinal}"))
        .bind("best effort mutable inventory write")
        .bind(agent_id)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn stop_mcp(harness: McpHarness) {
    harness.cancellation.cancel();
    let _ = harness.client.cancel().await;
    harness.server.abort();
}

#[tokio::test]
#[ignore = "requires CREWSYNC_ENABLE_LARGE_FIXTURE=1 and disposable TEST_DATABASE_URL"]
async fn traverses_2000_tasks_through_real_mcp_cursors_without_duplicates_or_skips() {
    let fixture = setup_fixture().await.expect("safe disposable PostgreSQL");
    let prefix = fixture.task_prefix.clone();
    let result = async {
        seed_tasks(&fixture.pool, fixture.team_id, fixture.agent_id, &prefix).await?;
        let harness = start_mcp(
            fixture.pool.clone(),
            fixture.agent_id,
            "task-search-mcp-pagination",
        )
        .await?;

        let mut cursor = None;
        let mut seen = HashSet::with_capacity(LARGE_FIXTURE_SIZE + 5);
        let mut pages = 0;
        let writer_pool = fixture.pool.clone();
        let writer_team_id = fixture.team_id;
        let writer_agent_id = fixture.agent_id;
        let writer_prefix = prefix.clone();
        let writer = tokio::spawn(async move {
            tokio::task::yield_now().await;
            append_tasks(
                &writer_pool,
                writer_team_id,
                writer_agent_id,
                &writer_prefix,
            )
            .await
        });
        loop {
            let mut arguments = Map::new();
            arguments.insert("limit".to_owned(), json!(200));
            arguments.insert("project_prefix".to_owned(), json!(prefix));
            if let Some(cursor) = cursor.take() {
                arguments.insert("cursor".to_owned(), json!(cursor));
            }
            let page = call_tool(&harness.client, Value::Object(arguments)).await?;
            pages += 1;
            for key in task_keys(&page)? {
                assert!(
                    seen.insert(key),
                    "duplicate task key returned by MCP cursor"
                );
            }
            let has_more = page["has_more"]
                .as_bool()
                .ok_or("MCP page omitted has_more")?;
            let next_cursor = page["next_cursor"].as_str().map(str::to_owned);
            if !has_more {
                assert!(next_cursor.is_none());
                break;
            }
            assert!(next_cursor.is_some());
            cursor = next_cursor;
        }
        writer.await??;
        assert!(pages >= 10);
        assert!(seen.len() >= LARGE_FIXTURE_SIZE);
        for ordinal in 0..LARGE_FIXTURE_SIZE {
            assert!(seen.contains(&format!("{prefix}:{ordinal:04}")));
        }

        let old_request = call_tool(&harness.client, json!({"status": "open", "limit": 3})).await?;
        assert_eq!(old_request["tasks"].as_array().map(Vec::len), Some(3));

        let claimed_key = format!("{prefix}:0000");
        let claimed = harness
            .client
            .call_tool(
                CallToolRequestParams::new("claim_task").with_arguments(Map::from_iter([
                    ("key".to_owned(), json!(claimed_key)),
                    ("lease_seconds".to_owned(), json!(60)),
                ])),
            )
            .await?;
        assert_ne!(claimed.is_error, Some(true));

        let mine = call_tool(
            &harness.client,
            json!({
                "status": "claimed",
                "mine_only": true,
                "project_prefix": prefix,
                "limit": 10
            }),
        )
        .await?;
        let mine_keys = task_keys(&mine)?;
        assert_eq!(mine_keys, vec![claimed_key]);

        let done_key = format!("{prefix}:0001");
        sqlx::query("UPDATE tasks SET status = 'done' WHERE team_id = $1 AND key = $2")
            .bind(fixture.team_id)
            .bind(&done_key)
            .execute(&fixture.pool)
            .await?;
        let done = call_tool(
            &harness.client,
            json!({
                "status": "done",
                "project_prefix": prefix,
                "limit": 10
            }),
        )
        .await?;
        assert_eq!(task_keys(&done)?, vec![done_key]);

        let mismatch = call_tool(
            &harness.client,
            json!({
                "project_prefix": prefix,
                "limit": 2,
                "search": "pagination",
                "search_mode": "contains"
            }),
        )
        .await?;
        let mismatch_cursor = mismatch["next_cursor"].as_str().map(str::to_owned);
        if let Some(mismatch_cursor) = mismatch_cursor {
            let error = call_tool(
                &harness.client,
                json!({
                    "project_prefix": prefix,
                    "limit": 2,
                    "cursor": mismatch_cursor,
                    "search": "different",
                    "search_mode": "contains"
                }),
            )
            .await;
            assert!(error.is_err(), "cursor must be bound to search filters");
        }

        stop_mcp(harness).await;
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    cleanup(fixture).await.expect("fixture cleanup");
    result.expect("real MCP/PostgreSQL pagination E2E");
}
