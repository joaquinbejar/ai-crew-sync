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
use serde_json::{Value, json};

async fn seed_transport_fixture(fixture: &Fixture) -> Result<(), sqlx::Error> {
    let rows = [
        ("wire deployment task", Some("operator deployment notes")),
        ("needle title task", Some("ordinary task description")),
        ("Russian inventory task", Some("Быстрые задачи проекта")),
        ("regex sentinel task", Some("aaaaab")),
        ("legacy task", None),
    ];

    let mut tx = fixture.pool.begin().await?;
    for (ordinal, (title, description)) in rows.into_iter().enumerate() {
        sqlx::query(
            "INSERT INTO tasks (team_id, key, title, description, created_by) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(fixture.team_id)
        .bind(format!("{}:{ordinal:04}", fixture.task_prefix))
        .bind(title)
        .bind(description)
        .bind(fixture.agent_id)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await
}

async fn call_list_tasks(
    client: &rmcp::service::RunningService<rmcp::RoleClient, ClientConfig>,
    arguments: Value,
) -> Result<Value, Box<dyn std::error::Error>> {
    let arguments = arguments
        .as_object()
        .cloned()
        .ok_or("list_tasks arguments must be an object")?;
    let result = client
        .call_tool(CallToolRequestParams::new("list_tasks".to_owned()).with_arguments(arguments))
        .await?;
    assert_ne!(
        result.is_error,
        Some(true),
        "list_tasks returned an MCP error"
    );
    Ok(result
        .structured_content
        .clone()
        .ok_or("list_tasks did not return structured JSON")?)
}

fn task_keys(value: &Value) -> Vec<String> {
    value["tasks"]
        .as_array()
        .expect("list_tasks response has tasks array")
        .iter()
        .map(|task| {
            task["key"]
                .as_str()
                .expect("task key is a string")
                .to_owned()
        })
        .collect()
}

async fn run_transport_e2e() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = setup_fixture().await?;
    let result = async {
        seed_transport_fixture(&fixture).await?;

        let raw_token = ai_crew_sync::auth::generate_token();
        sqlx::query(
            "INSERT INTO api_tokens (agent_id, token_hash, prefix, label) VALUES ($1, $2, $3, $4)",
        )
        .bind(fixture.agent_id)
        .bind(ai_crew_sync::auth::hash_token(&raw_token))
        .bind(ai_crew_sync::auth::token_prefix(&raw_token))
        .bind("task search MCP transport E2E")
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
                dashboard_secret: b"task-search-mcp-wire-test-secret".to_vec(),
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

        let mut transport =
            StreamableHttpClientTransportConfig::with_uri(format!("http://{address}/mcp"));
        transport.auth_header = Some(raw_token);
        transport.allow_stateless = true;
        transport.custom_headers.insert(
            ai_crew_sync::auth::SESSION_HEADER.parse()?,
            "task-search-mcp-wire".parse()?,
        );
        let client = ClientConfig::default()
            .serve(StreamableHttpClientTransport::from_config(transport))
            .await?;

        let tools = client.list_all_tools().await?;
        let list_tasks_tool = tools
            .iter()
            .find(|tool| tool.name == "list_tasks")
            .expect("tools/list exposes list_tasks");
        let schema = serde_json::to_value(&list_tasks_tool.input_schema)?;
        for field in ["search", "search_mode", "search_fields", "search_language"] {
            assert!(
                schema["properties"].get(field).is_some(),
                "missing {field} schema"
            );
        }

        let prefix = &fixture.task_prefix;
        let legacy = call_list_tasks(
            &client,
            json!({
                "project_prefix": prefix,
                "limit": 20,
                "mine_only": false
            }),
        )
        .await?;
        assert_eq!(task_keys(&legacy).len(), 5);
        assert_eq!(legacy["has_more"], false);

        let keyword_title = call_list_tasks(
            &client,
            json!({
                "project_prefix": prefix,
                "search": "deployment",
                "search_mode": "keywords",
                "search_fields": "title",
                "search_language": "simple",
                "limit": 20
            }),
        )
        .await?;
        assert_eq!(task_keys(&keyword_title).len(), 1);
        assert!(task_keys(&keyword_title)[0].ends_with(":0000"));

        let contains_description = call_list_tasks(
            &client,
            json!({
                "project_prefix": prefix,
                "search": "ordinary task description",
                "search_mode": "contains",
                "search_fields": "description",
                "search_language": "simple",
                "limit": 20
            }),
        )
        .await?;
        assert_eq!(task_keys(&contains_description).len(), 1);
        assert!(task_keys(&contains_description)[0].ends_with(":0001"));

        let regex_description = call_list_tasks(
            &client,
            json!({
                "project_prefix": prefix,
                "search": "^aaaaab$",
                "search_mode": "regex",
                "search_fields": "description",
                "search_language": "simple",
                "limit": 20
            }),
        )
        .await?;
        assert_eq!(task_keys(&regex_description).len(), 1);
        assert!(task_keys(&regex_description)[0].ends_with(":0003"));

        let russian = call_list_tasks(
            &client,
            json!({
                "project_prefix": prefix,
                "search": "быстрые задачи",
                "search_mode": "keywords",
                "search_fields": "both",
                "search_language": "russian",
                "limit": 20
            }),
        )
        .await?;
        assert_eq!(task_keys(&russian).len(), 1);
        assert!(task_keys(&russian)[0].ends_with(":0002"));

        let title_only = call_list_tasks(
            &client,
            json!({
                "project_prefix": prefix,
                "search": "needle",
                "search_mode": "contains",
                "search_fields": "title",
                "search_language": "simple",
                "limit": 20
            }),
        )
        .await?;
        assert_eq!(task_keys(&title_only).len(), 1);
        assert!(task_keys(&title_only)[0].ends_with(":0001"));

        let description_only = call_list_tasks(
            &client,
            json!({
                "project_prefix": prefix,
                "search": "operator",
                "search_mode": "contains",
                "search_fields": "description",
                "search_language": "simple",
                "limit": 20
            }),
        )
        .await?;
        assert_eq!(task_keys(&description_only).len(), 1);
        assert!(task_keys(&description_only)[0].ends_with(":0000"));

        let _ = client.cancel().await;
        cancellation.cancel();
        server.abort();
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    cleanup(fixture).await?;
    result
}

#[tokio::test]
#[ignore = "requires CREWSYNC_ENABLE_LARGE_FIXTURE=1 and disposable TEST_DATABASE_URL"]
async fn task_search_uses_real_mcp_transport_and_preserves_legacy_requests() {
    run_transport_e2e()
        .await
        .expect("task search MCP transport E2E");
}
