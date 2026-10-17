#![allow(clippy::too_many_lines)]

#[path = "task_pagination_fixture.rs"]
mod fixture;

use std::time::{Duration, Instant};

use ai_crew_sync::{
    client::{self, ClientArgs, ClientCmd},
    serve::{self, ServeOptions},
    store::tasks::{self, TaskPageQuery, TaskSearchQuery},
};
use fixture::{Fixture, cleanup, setup_fixture};
use serde_json::json;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

struct TestServer {
    token: String,
    url: String,
    session: String,
    cancellation: CancellationToken,
    task: JoinHandle<()>,
}

impl TestServer {
    async fn start(
        fixture: &Fixture,
        agent_id: Uuid,
        label: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let token = ai_crew_sync::auth::generate_token();
        sqlx::query(
            "INSERT INTO api_tokens (agent_id, token_hash, prefix, label) VALUES ($1, $2, $3, $4)",
        )
        .bind(agent_id)
        .bind(ai_crew_sync::auth::hash_token(&token))
        .bind(ai_crew_sync::auth::token_prefix(&token))
        .bind(label)
        .execute(&fixture.pool)
        .await?;

        let cancellation = CancellationToken::new();
        let router = serve::build_router(
            fixture.pool.clone(),
            &ServeOptions {
                bind: "127.0.0.1:0".to_owned(),
                allowed_hosts: Vec::new(),
                allowed_origins: Vec::new(),
                max_request_bytes: serve::DEFAULT_MAX_REQUEST_BYTES,
                rate_limit_per_minute: 0,
                dashboard_secret: b"task-search-mcp-security-test-secret".to_vec(),
                nats_url: None,
                nats_credentials: None,
                publication_worker: false,
                event_ping_secs: 3_600,
            },
            cancellation.child_token(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });

        Ok(Self {
            token,
            url: format!("http://{address}/mcp"),
            session: format!("task-search-mcp-security-{label}"),
            cancellation,
            task,
        })
    }

    async fn call(&self, command: ClientCmd) -> Result<(), Box<dyn std::error::Error>> {
        client::run(ClientArgs {
            url: Some(self.url.clone()),
            token: Some(self.token.clone()),
            profile: None,
            project_dir: None,
            host_session: None,
            session: Some(self.session.clone()),
            json: true,
            command,
        })
        .await
        .map_err(Into::into)
    }

    async fn stop(self) {
        self.cancellation.cancel();
        self.task.abort();
        let _ = self.task.await;
    }
}

fn tasks_command(
    search: Option<&str>,
    mode: Option<&str>,
    fields: Option<&str>,
    language: Option<&str>,
    project_prefix: Option<&str>,
    cursor: Option<String>,
    limit: i64,
) -> ClientCmd {
    ClientCmd::Tasks {
        status: None,
        mine: false,
        project_prefix: project_prefix.map(str::to_owned),
        limit: Some(limit),
        cursor,
        search: search.map(str::to_owned),
        search_mode: mode.map(str::to_owned),
        search_fields: fields.map(str::to_owned),
        search_language: language.map(str::to_owned),
        all_pages: false,
    }
}

async fn insert_task(
    fixture: &Fixture,
    key: &str,
    title: &str,
    description: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO tasks (team_id, key, title, description, metadata, created_by) VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(fixture.team_id)
    .bind(key)
    .bind(title)
    .bind(description)
    .bind(json!({"security_fixture": true}))
    .bind(fixture.agent_id)
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

async fn create_foreign_team(
    fixture: &Fixture,
) -> Result<(Uuid, Uuid), Box<dyn std::error::Error>> {
    let mut tx = fixture.pool.begin().await?;
    let team_id: Uuid =
        sqlx::query_scalar("INSERT INTO teams (slug, name) VALUES ($1, $2) RETURNING id")
            .bind(format!(
                "task-search-security-foreign-{}",
                Uuid::new_v4().simple()
            ))
            .bind("task search security foreign team")
            .fetch_one(&mut *tx)
            .await?;
    let agent_id: Uuid =
        sqlx::query_scalar("INSERT INTO agents (team_id, name) VALUES ($1, $2) RETURNING id")
            .bind(team_id)
            .bind("foreign-security-agent")
            .fetch_one(&mut *tx)
            .await?;
    sqlx::query(
        "INSERT INTO tasks (team_id, key, title, description, created_by) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(team_id)
    .bind("security:foreign-secret")
    .bind("foreign tenant secret deployment")
    .bind("must never be returned to another team")
    .bind(agent_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok((team_id, agent_id))
}

async fn seed_fixture(fixture: &Fixture) -> Result<(), Box<dyn std::error::Error>> {
    insert_task(
        fixture,
        "security:alpha",
        "Deploy database safely",
        "Operator guide for the running service",
    )
    .await?;
    insert_task(
        fixture,
        "security:beta",
        "Russian inventory task",
        "Быстрые задачи проекта и поиск данных",
    )
    .await?;
    insert_task(
        fixture,
        "security:regex",
        "Regex sentinel",
        &format!("{}!", "a".repeat(12_000)),
    )
    .await?;
    sqlx::query(
        "UPDATE tasks SET updated_at = TIMESTAMPTZ '2026-01-01 00:00:00+00' WHERE team_id = $1 AND key LIKE 'security:%'",
    )
    .bind(fixture.team_id)
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

fn search(mode: &str, fields: &str, language: &str, query: &str) -> TaskSearchQuery {
    TaskSearchQuery {
        search: Some(query.to_owned()),
        search_mode: Some(mode.to_owned()),
        search_fields: Some(fields.to_owned()),
        search_language: Some(language.to_owned()),
    }
}

async fn run_security_matrix() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = setup_fixture().await?;
    let result = async {
        seed_fixture(&fixture).await?;
        let (foreign_team_id, foreign_agent_id) = create_foreign_team(&fixture).await?;
        let primary = TestServer::start(&fixture, fixture.agent_id, "primary").await?;
        let foreign = TestServer::start(&fixture, foreign_agent_id, "foreign").await?;

        primary
            .call(tasks_command(
                Some("database"),
                Some("contains"),
                Some("title"),
                Some("simple"),
                Some("security:"),
                None,
                10,
            ))
            .await?;
        primary
            .call(tasks_command(
                Some("Быстрые задачи"),
                Some("keywords"),
                Some("description"),
                Some("russian"),
                Some("security:"),
                None,
                10,
            ))
            .await?;
        primary
            .call(tasks_command(
                Some("^regex sentinel$"),
                Some("regex"),
                Some("title"),
                Some("simple"),
                Some("security:"),
                None,
                10,
            ))
            .await?;

        for (mode, fields, language) in [
            ("wildcard", "both", "simple"),
            ("contains", "metadata", "simple"),
            ("contains", "both", "german"),
        ] {
            assert!(
                primary
                    .call(tasks_command(
                        Some("x"),
                        Some(mode),
                        Some(fields),
                        Some(language),
                        Some("security:"),
                        None,
                        10,
                    ))
                    .await
                    .is_err(),
                "invalid search option must be rejected through MCP"
            );
        }

        assert!(
            primary
                .call(tasks_command(
                    Some("[unterminated"),
                    Some("regex"),
                    Some("both"),
                    Some("simple"),
                    Some("security:"),
                    None,
                    10,
                ))
                .await
                .is_err(),
            "invalid regex must become a safe MCP error"
        );
        assert!(
            primary
                .call(tasks_command(
                    Some(&"x".repeat(513)),
                    Some("contains"),
                    Some("both"),
                    Some("simple"),
                    Some("security:"),
                    None,
                    10,
                ))
                .await
                .is_err(),
            "oversized search must be rejected before broadening"
        );

        let started = Instant::now();
        assert!(
            primary
                .call(tasks_command(
                    Some("^(a+)+$"),
                    Some("regex"),
                    Some("description"),
                    Some("simple"),
                    Some("security:"),
                    None,
                    10,
                ))
                .await
                .is_ok(),
            "bounded pathological regex must return a safe result"
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "pathological regex exceeded the bounded MCP request budget"
        );

        let page = tasks::list_tasks_page_with_search(
            &fixture.pool,
            &ai_crew_sync::auth::AuthCtx {
                agent_id: fixture.agent_id,
                agent_name: "fixture-primary".to_owned(),
                team_id: fixture.team_id,
                team_slug: format!("search-security-{}", fixture.team_id.simple()),
                session: "cursor-source".to_owned(),
                session_id: None,
                session_epoch: None,
                token_id: None,
            },
            TaskPageQuery {
                status: None,
                mine_only: false,
                limit: 1,
                project_prefix: Some("security:".to_owned()),
                cursor: None,
            },
            search("contains", "both", "simple", "e"),
        )
        .await?;
        let cursor = page.next_cursor.expect("fixture must produce a cursor");

        primary
            .call(tasks_command(
                Some("e"),
                Some("contains"),
                Some("both"),
                Some("simple"),
                Some("security:"),
                Some(cursor.clone()),
                1,
            ))
            .await?;
        for (query, mode, fields, language) in [
            ("database", "contains", "both", "simple"),
            ("task", "regex", "both", "simple"),
            ("task", "contains", "title", "simple"),
            ("task", "contains", "both", "english"),
        ] {
            assert!(
                primary
                    .call(tasks_command(
                        Some(query),
                        Some(mode),
                        Some(fields),
                        Some(language),
                        Some("security:"),
                        Some(cursor.clone()),
                        1,
                    ))
                    .await
                    .is_err(),
                "cursor replay with altered search binding must fail"
            );
        }
        assert!(
            foreign
                .call(tasks_command(
                    Some("e"),
                    Some("contains"),
                    Some("both"),
                    Some("simple"),
                    None,
                    Some(cursor),
                    1,
                ))
                .await
                .is_err(),
            "cursor must not cross tenant boundaries"
        );
        primary.stop().await;
        foreign.stop().await;

        sqlx::query("DELETE FROM teams WHERE id = $1")
            .bind(foreign_team_id)
            .execute(&fixture.pool)
            .await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    cleanup(fixture).await?;
    result
}

#[tokio::test]
#[ignore = "requires CREWSYNC_ENABLE_LARGE_FIXTURE=1 and disposable TEST_DATABASE_URL"]
async fn mcp_task_search_rejects_adversarial_inputs_and_cursor_replay() {
    run_security_matrix()
        .await
        .expect("MCP task-search security E2E");
}
