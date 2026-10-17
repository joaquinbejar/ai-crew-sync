#![allow(clippy::too_many_lines)]

#[path = "task_pagination_fixture.rs"]
mod fixture;

use ai_crew_sync::serve::{self, ServeOptions};
use fixture::{Fixture, cleanup, setup_fixture};
use rmcp::{
    ServiceExt,
    model::{CallToolRequestParams, ClientConfig},
    transport::{
        StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use serde_json::{Map, Value, json};

fn arguments(value: Value) -> Map<String, Value> {
    value
        .as_object()
        .cloned()
        .expect("MCP tool arguments must be a JSON object")
}

async fn seed_compatibility_tasks(fixture: &Fixture) -> Result<(), sqlx::Error> {
    let mut tx = fixture.pool.begin().await?;
    sqlx::query(
        "INSERT INTO tasks (team_id, key, title, description, created_by) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(fixture.team_id)
    .bind(format!("{}:open", fixture.task_prefix))
    .bind("Legacy MCP task")
    .bind("A task visible to old clients")
    .bind(fixture.agent_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO tasks (team_id, key, title, description, status, claimed_by, claimed_at, lease_expires_at, created_by) VALUES ($1, $2, $3, $4, 'claimed', $5, now(), now() + interval '1 hour', $5)",
    )
    .bind(fixture.team_id)
    .bind(format!("{}:claimed", fixture.task_prefix))
    .bind("Claimed MCP task")
    .bind("A task with an active claim")
    .bind(fixture.other_agent_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await
}

async fn run_schema_compatibility_check() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = setup_fixture().await?;
    let result = async {
        seed_compatibility_tasks(&fixture).await?;

        let raw_token = ai_crew_sync::auth::generate_token();
        sqlx::query(
            "INSERT INTO api_tokens (agent_id, token_hash, prefix, label) VALUES ($1, $2, $3, $4)",
        )
        .bind(fixture.agent_id)
        .bind(ai_crew_sync::auth::hash_token(&raw_token))
        .bind(ai_crew_sync::auth::token_prefix(&raw_token))
        .bind("MCP schema compatibility test")
        .execute(&fixture.pool)
        .await?;

        let cancellation = tokio_util::sync::CancellationToken::new();
        let router = serve::build_router(
            fixture.pool.clone(),
            &ServeOptions {
                bind: "127.0.0.1:0".to_owned(),
                allowed_hosts: Vec::new(),
                allowed_origins: Vec::new(),
                max_request_bytes: serve::DEFAULT_MAX_REQUEST_BYTES,
                rate_limit_per_minute: 0,
                dashboard_secret: b"schema-compatibility-test-secret".to_vec(),
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

        let mut transport_config =
            StreamableHttpClientTransportConfig::with_uri(format!("http://{address}/mcp"));
        transport_config.auth_header = Some(raw_token);
        transport_config.allow_stateless = true;
        let client = ClientConfig::default()
            .serve(StreamableHttpClientTransport::from_config(transport_config))
            .await?;

        let tools = client.list_all_tools().await?;
        let list_tasks = tools
            .iter()
            .find(|tool| tool.name == "list_tasks")
            .expect("the canonical MCP service must expose list_tasks");
        let schema = serde_json::to_value(&list_tasks.input_schema)?;
        let properties = schema
            .get("properties")
            .and_then(Value::as_object)
            .expect("list_tasks input schema must expose properties");
        for field in ["search", "search_mode", "search_fields", "search_language"] {
            assert!(properties.contains_key(field), "schema is missing {field}");
        }
        let required = schema
            .get("required")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for field in ["search", "search_mode", "search_fields", "search_language"] {
            assert!(
                !required.iter().any(|value| value.as_str() == Some(field)),
                "new {field} field must remain optional for old clients"
            );
        }

        let legacy_result = client
            .call_tool(
                CallToolRequestParams::new("list_tasks").with_arguments(arguments(json!({
                    "status": "any",
                    "mine_only": false,
                    "limit": 10,
                    "project_prefix": fixture.task_prefix,
                }))),
            )
            .await?;
        assert_ne!(legacy_result.is_error, Some(true));
        let legacy_json = legacy_result
            .structured_content
            .clone()
            .expect("list_tasks must return structured JSON");
        for field in ["has_more", "next_cursor", "open", "claimed"] {
            assert!(
                legacy_json.get(field).is_some(),
                "response is missing {field}"
            );
        }
        assert_eq!(legacy_json["open"], 1);
        assert_eq!(legacy_json["claimed"], 1);
        assert_eq!(legacy_json["has_more"], false);
        assert!(legacy_json["next_cursor"].is_null());

        let search_result = client
            .call_tool(
                CallToolRequestParams::new("list_tasks").with_arguments(arguments(json!({
                    "search": "legacy",
                    "search_mode": "contains",
                    "search_fields": "title",
                    "search_language": "simple",
                    "limit": 1,
                    "project_prefix": fixture.task_prefix,
                }))),
            )
            .await?;
        assert_ne!(search_result.is_error, Some(true));
        let search_json = search_result
            .structured_content
            .clone()
            .expect("search list_tasks must return structured JSON");
        assert_eq!(
            search_json["tasks"].as_array().map(|tasks| tasks.len()),
            Some(1)
        );
        assert_eq!(search_json["has_more"], false);
        assert!(search_json["next_cursor"].is_null());

        for invalid in [
            json!({ "search": "term", "search_mode": "unsupported" }),
            json!({ "search": "term", "search_fields": "unsupported" }),
            json!({ "search": "term", "search_language": "unsupported" }),
        ] {
            let error = client
                .call_tool(
                    CallToolRequestParams::new("list_tasks").with_arguments(arguments(invalid)),
                )
                .await
                .expect_err("invalid search options must be protocol errors");
            let error_debug = format!("{error:?}");
            assert!(
                error_debug.contains("-32602"),
                "invalid search options must return MCP InvalidParams (-32602): {error_debug}"
            );
        }

        cancellation.cancel();
        server.abort();
        let _ = client.cancel().await;
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    cleanup(fixture).await?;
    result
}

#[tokio::test]
#[ignore = "requires CREWSYNC_ENABLE_LARGE_FIXTURE=1 and disposable TEST_DATABASE_URL"]
async fn mcp_list_tasks_schema_and_legacy_clients_remain_compatible() {
    run_schema_compatibility_check()
        .await
        .expect("real MCP service and disposable PostgreSQL fixture");
}
