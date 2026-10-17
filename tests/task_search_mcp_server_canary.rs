#![allow(clippy::too_many_lines)]

#[path = "task_pagination_fixture.rs"]
mod fixture;

use std::time::Duration;

use ai_crew_sync::serve::{self, ServeOptions};
use fixture::{Fixture, cleanup, setup_fixture};
use rmcp::{
    ServiceExt,
    model::{CallToolRequestParams, ClientConfig},
    transport::{
        StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use serde_json::{Value, json};
use tokio::{task::JoinSet, time::timeout};
use tokio_util::sync::CancellationToken;

const SERVER_START_TIMEOUT: Duration = Duration::from_secs(5);
const SERVER_STOP_TIMEOUT: Duration = Duration::from_secs(5);

struct RunningServer {
    address: String,
    cancellation: CancellationToken,
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

async fn seed_canary_task(fixture: &Fixture) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO tasks (team_id, key, title, description, created_by) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(fixture.team_id)
    .bind(format!("{}:canary", fixture.task_prefix))
    .bind("MCP server lifecycle canary")
    .bind("isolated transport fixture")
    .bind(fixture.agent_id)
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

async fn issue_token(fixture: &Fixture) -> Result<String, sqlx::Error> {
    let raw_token = ai_crew_sync::auth::generate_token();
    sqlx::query(
        "INSERT INTO api_tokens (agent_id, token_hash, prefix, label) VALUES ($1, $2, $3, $4)",
    )
    .bind(fixture.agent_id)
    .bind(ai_crew_sync::auth::hash_token(&raw_token))
    .bind(ai_crew_sync::auth::token_prefix(&raw_token))
    .bind("MCP server lifecycle canary")
    .execute(&fixture.pool)
    .await?;
    Ok(raw_token)
}

fn test_options() -> ServeOptions {
    ServeOptions {
        bind: "127.0.0.1:0".to_owned(),
        allowed_hosts: Vec::new(),
        allowed_origins: Vec::new(),
        max_request_bytes: serve::DEFAULT_MAX_REQUEST_BYTES,
        rate_limit_per_minute: 0,
        dashboard_secret: b"task-search-mcp-server-canary-secret".to_vec(),
        nats_url: None,
        nats_credentials: None,
        publication_worker: false,
        event_ping_secs: 3_600,
    }
}

async fn start_server(fixture: &Fixture) -> Result<RunningServer, Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let router = serve::build_router(
        fixture.pool.clone(),
        &test_options(),
        cancellation.child_token(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let shutdown = cancellation.clone();
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
    });
    Ok(RunningServer {
        address,
        cancellation,
        task,
    })
}

async fn wait_until_ready(address: &str) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    timeout(SERVER_START_TIMEOUT, async {
        loop {
            match client.get(format!("http://{address}/health")).send().await {
                Ok(response) if response.status().is_success() => return Ok(()),
                Ok(_) | Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
    })
    .await
    .map_err(|_| "MCP server did not become ready")?
}

async fn connect_client(
    address: &str,
    raw_token: &str,
    session: &str,
) -> Result<rmcp::service::RunningService<rmcp::RoleClient, ClientConfig>, Box<dyn std::error::Error>>
{
    let mut transport =
        StreamableHttpClientTransportConfig::with_uri(format!("http://{address}/mcp"));
    transport.auth_header = Some(raw_token.to_owned());
    transport.allow_stateless = true;
    transport.custom_headers.insert(
        ai_crew_sync::auth::SESSION_HEADER.parse()?,
        session.parse()?,
    );
    Ok(ClientConfig::default()
        .serve(StreamableHttpClientTransport::from_config(transport))
        .await?)
}

async fn call_list_tasks(
    client: &rmcp::service::RunningService<rmcp::RoleClient, ClientConfig>,
    project_prefix: &str,
) -> Result<Value, Box<dyn std::error::Error>> {
    let result = client
        .call_tool(
            CallToolRequestParams::new("list_tasks".to_owned()).with_arguments(
                json!({
                    "project_prefix": project_prefix,
                    "limit": 20,
                    "mine_only": false,
                })
                .as_object()
                .cloned()
                .ok_or("list_tasks arguments must be an object")?,
            ),
        )
        .await?;
    if result.is_error == Some(true) {
        return Err("authenticated list_tasks returned an MCP error".into());
    }
    Ok(result
        .structured_content
        .ok_or("list_tasks returned no structured content")?)
}

async fn stop_server(server: RunningServer) -> Result<(), Box<dyn std::error::Error>> {
    server.cancellation.cancel();
    timeout(SERVER_STOP_TIMEOUT, server.task)
        .await
        .map_err(|_| "MCP server did not shut down")??
        .map_err(|error| format!("MCP server shutdown failed: {error}"))?;
    Ok(())
}

async fn run_canary() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = setup_fixture().await?;
    let result = async {
        seed_canary_task(&fixture).await?;
        let raw_token = issue_token(&fixture).await?;

        let first_server = start_server(&fixture).await?;
        wait_until_ready(&first_server.address).await?;

        let unauthenticated = reqwest::Client::new()
            .post(format!("http://{}/mcp", first_server.address))
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/list",
            }))
            .send()
            .await?;
        assert!(unauthenticated.status().is_client_error());

        let first_client = connect_client(
            &first_server.address,
            &raw_token,
            "task-search-mcp-server-canary-first",
        )
        .await?;
        let tools = first_client.list_all_tools().await?;
        assert!(tools.iter().any(|tool| tool.name == "list_tasks"));

        let first_result = call_list_tasks(&first_client, &fixture.task_prefix).await?;
        assert_eq!(first_result["tasks"].as_array().map(Vec::len), Some(1));
        assert_eq!(
            first_result["tasks"][0]["key"],
            format!("{}:canary", fixture.task_prefix)
        );

        let mut workers = JoinSet::new();
        for ordinal in 0..8 {
            let address = first_server.address.clone();
            let raw_token = raw_token.clone();
            let prefix = fixture.task_prefix.clone();
            workers.spawn(async move {
                let client = connect_client(
                    &address,
                    &raw_token,
                    &format!("task-search-mcp-server-canary-concurrent-{ordinal}"),
                )
                .await
                .map_err(|error| error.to_string())?;
                let result = call_list_tasks(&client, &prefix)
                    .await
                    .map_err(|error| error.to_string())?;
                let task_count = result["tasks"].as_array().map(Vec::len).unwrap_or_default();
                let _ = client.cancel().await;
                if task_count != 1 {
                    return Err(format!("concurrent list_tasks returned {task_count} tasks"));
                }
                Ok::<(), String>(())
            });
        }
        while let Some(worker) = workers.join_next().await {
            worker??;
        }

        let first_address = first_server.address.clone();
        let _ = first_client.cancel().await;
        stop_server(first_server).await?;
        let stopped_request = reqwest::Client::new()
            .get(format!("http://{first_address}/health"))
            .send()
            .await;
        assert!(stopped_request.is_err());

        let second_server = start_server(&fixture).await?;
        wait_until_ready(&second_server.address).await?;
        let second_client = connect_client(
            &second_server.address,
            &raw_token,
            "task-search-mcp-server-canary-restart",
        )
        .await?;
        let restarted_tools = second_client.list_all_tools().await?;
        assert!(restarted_tools.iter().any(|tool| tool.name == "list_tasks"));
        let restarted_result = call_list_tasks(&second_client, &fixture.task_prefix).await?;
        assert_eq!(restarted_result["tasks"].as_array().map(Vec::len), Some(1));
        let _ = second_client.cancel().await;
        stop_server(second_server).await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    cleanup(fixture).await?;
    result
}

#[tokio::test]
#[ignore = "requires CREWSYNC_ENABLE_LARGE_FIXTURE=1 and disposable TEST_DATABASE_URL"]
async fn task_search_mcp_server_canary_covers_lifecycle_auth_concurrency_and_transport_errors() {
    run_canary().await.expect("MCP server lifecycle canary E2E");
}
