#![allow(clippy::too_many_lines)]

#[path = "task_pagination_fixture.rs"]
mod fixture;

use std::process::Stdio;

use ai_crew_sync::{
    auth,
    serve::{self, ServeOptions},
    store::tasks,
};
use fixture::{Fixture, cleanup, setup_fixture};
use serde_json::Value;
use tokio::process::Command;

const TASK_COUNT: usize = 5;

async fn seed_cli_fixture(fixture: &Fixture) -> Result<(), sqlx::Error> {
    let rows = [
        ("Deploy API", "deploy the API service", "open"),
        ("Deploy worker", "deploy the worker service", "open"),
        ("Maintenance", "routine maintenance", "open"),
        ("Regex sentinel", "aaaaab", "open"),
        (
            "Readback target",
            "task used for show and status checks",
            "open",
        ),
    ];

    let mut tx = fixture.pool.begin().await?;
    for (ordinal, (title, description, status)) in rows.into_iter().enumerate() {
        sqlx::query(
            "INSERT INTO tasks (team_id, key, title, description, status, created_by) VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(fixture.team_id)
        .bind(format!("{}:{ordinal:04}", fixture.task_prefix))
        .bind(title)
        .bind(description)
        .bind(status)
        .bind(fixture.agent_id)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await
}

fn client_auth(fixture: &Fixture) -> auth::AuthCtx {
    auth::AuthCtx {
        agent_id: fixture.agent_id,
        agent_name: "fixture-primary".to_owned(),
        team_id: fixture.team_id,
        team_slug: format!("cli-search-{}", fixture.team_id.simple()),
        session: "cli-transport-test".to_owned(),
        session_id: None,
        session_epoch: None,
        token_id: None,
    }
}

async fn issue_token(fixture: &Fixture) -> Result<String, sqlx::Error> {
    let raw_token = auth::generate_token();
    sqlx::query(
        "INSERT INTO api_tokens (agent_id, token_hash, prefix, label) VALUES ($1, $2, $3, $4)",
    )
    .bind(fixture.agent_id)
    .bind(auth::hash_token(&raw_token))
    .bind(auth::token_prefix(&raw_token))
    .bind("task search CLI transport E2E")
    .execute(&fixture.pool)
    .await?;
    Ok(raw_token)
}

async fn start_server(
    fixture: &Fixture,
) -> Result<
    (
        String,
        tokio_util::sync::CancellationToken,
        tokio::task::JoinHandle<Result<(), std::io::Error>>,
    ),
    Box<dyn std::error::Error>,
> {
    let cancellation = tokio_util::sync::CancellationToken::new();
    let router = serve::build_router(
        fixture.pool.clone(),
        &ServeOptions {
            bind: "127.0.0.1:0".to_owned(),
            allowed_hosts: Vec::new(),
            allowed_origins: Vec::new(),
            max_request_bytes: serve::DEFAULT_MAX_REQUEST_BYTES,
            rate_limit_per_minute: 0,
            dashboard_secret: b"task-search-cli-transport-test".to_vec(),
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
    Ok((format!("http://{address}/mcp"), cancellation, server))
}

async fn run_cli(
    url: &str,
    token: &str,
    args: &[&str],
) -> Result<Value, Box<dyn std::error::Error>> {
    let binary = std::env::var_os("CARGO_BIN_EXE_ai-crew-sync")
        .ok_or("Cargo did not provide the ai-crew-sync test binary path")?;
    let output = Command::new(binary)
        .arg("client")
        .arg("--url")
        .arg(url)
        .arg("--token")
        .arg(token)
        .arg("--session")
        .arg("cli-transport-test")
        .arg("--json")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .await?;
    assert!(
        output.status.success(),
        "CLI failed with {}:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(serde_json::from_slice(&output.stdout)?)
}

async fn run_e2e() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = setup_fixture().await?;
    let result =
        async {
            seed_cli_fixture(&fixture).await?;
            let token = issue_token(&fixture).await?;
            let (url, cancellation, server) = start_server(&fixture).await?;

            let search = run_cli(
                &url,
                &token,
                &[
                    "tasks",
                    "--project-prefix",
                    &fixture.task_prefix,
                    "--search",
                    "^Deploy (API|worker)$",
                    "--search-mode",
                    "regex",
                    "--search-fields",
                    "title",
                    "--search-language",
                    "simple",
                    "--limit",
                    "1",
                    "--all-pages",
                ],
            )
            .await?;
            assert_eq!(search["tasks"].as_array().map(Vec::len), Some(2));
            assert_eq!(search["has_more"], false);
            assert_eq!(search["next_cursor"], Value::Null);
            assert_eq!(search["exhausted"], true);
            assert_eq!(search["incomplete"], false);
            assert!(
                search["tasks"].as_array().unwrap().iter().all(|task| {
                    task["title"] == "Deploy API" || task["title"] == "Deploy worker"
                })
            );

            let no_search = run_cli(
                &url,
                &token,
                &[
                    "tasks",
                    "--project-prefix",
                    &fixture.task_prefix,
                    "--limit",
                    "2",
                ],
            )
            .await?;
            assert_eq!(no_search["tasks"].as_array().map(Vec::len), Some(2));
            assert_eq!(no_search["has_more"], true);
            assert_eq!(no_search["incomplete"], true);
            assert_eq!(no_search["exhausted"], false);
            assert!(no_search["next_cursor"].is_string());

            let regex = run_cli(
                &url,
                &token,
                &[
                    "tasks",
                    "--project-prefix",
                    &fixture.task_prefix,
                    "--search",
                    "^a+b$",
                    "--search-mode",
                    "regex",
                    "--search-fields",
                    "description",
                    "--limit",
                    "1",
                ],
            )
            .await?;
            assert_eq!(regex["tasks"].as_array().map(Vec::len), Some(1));
            assert_eq!(regex["tasks"][0]["title"], "Regex sentinel");
            assert_eq!(regex["exhausted"], true);

            let target_key = format!("{}:0004", fixture.task_prefix);
            let primary = client_auth(&fixture);
            tasks::claim_task(&fixture.pool, &primary, &target_key, Some(60)).await?;
            let status = run_cli(
                &url,
                &token,
                &[
                    "tasks",
                    "--project-prefix",
                    &fixture.task_prefix,
                    "--status",
                    "claimed",
                    "--mine",
                ],
            )
            .await?;
            assert_eq!(status["tasks"].as_array().map(Vec::len), Some(1));
            assert_eq!(status["tasks"][0]["key"], target_key);
            assert_eq!(status["claimed"], 1);

            let readback = run_cli(&url, &token, &["task", "show", &target_key]).await?;
            assert_eq!(readback["task"]["key"], target_key);
            assert_eq!(readback["task"]["status"], "claimed");
            assert_eq!(readback["task"]["title"], "Readback target");

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
async fn task_search_cli_uses_real_mcp_transport() {
    run_e2e().await.expect("task search CLI transport E2E");
}

#[test]
fn fixture_task_count_matches_cli_seed() {
    assert_eq!(TASK_COUNT, 5);
}
