#![allow(clippy::too_many_lines)]

#[path = "task_pagination_fixture.rs"]
mod fixture;

use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use ai_crew_sync::{
    auth::AuthCtx,
    client::{self, ClientArgs, ClientCmd},
    serve::{self, ServeOptions},
    store::tasks::{self, TaskPageQuery, TaskSearchQuery},
};
use fixture::{Fixture, LARGE_FIXTURE_SIZE, cleanup, setup_fixture};
use serde_json::json;
use uuid::Uuid;

fn auth(fixture: &Fixture, agent_id: Uuid, agent_name: &str, session: &str) -> AuthCtx {
    AuthCtx {
        agent_id,
        agent_name: agent_name.to_owned(),
        team_id: fixture.team_id,
        team_slug: format!("search-fixture-{}", fixture.team_id.simple()),
        session: session.to_owned(),
        session_id: None,
        session_epoch: None,
        token_id: None,
    }
}

fn page_query(
    prefix: Option<&str>,
    status: Option<&str>,
    mine_only: bool,
    limit: i64,
    cursor: Option<String>,
) -> TaskPageQuery {
    TaskPageQuery {
        status: status.map(str::to_owned),
        mine_only,
        limit,
        project_prefix: prefix.map(str::to_owned),
        cursor,
    }
}

fn search(
    query: Option<&str>,
    mode: Option<&str>,
    fields: Option<&str>,
    language: Option<&str>,
) -> TaskSearchQuery {
    TaskSearchQuery {
        search: query.map(str::to_owned),
        search_mode: mode.map(str::to_owned),
        search_fields: fields.map(str::to_owned),
        search_language: language.map(str::to_owned),
    }
}

async fn seed_search_fixture(fixture: &Fixture) -> Result<(), sqlx::Error> {
    let mut tx = fixture.pool.begin().await?;
    for ordinal in 0..LARGE_FIXTURE_SIZE {
        let key = format!("{}:{ordinal:04}", fixture.task_prefix);
        let (title, description) = match ordinal % 8 {
            0 => (
                format!("Running deployment database search {ordinal:04}"),
                Some(format!(
                    "English operator guide for the running service {ordinal:04}"
                )),
            ),
            1 => (
                format!("Russian inventory task {ordinal:04}"),
                Some(format!(
                    "Быстрые задачи проекта и поиск данных {ordinal:04}"
                )),
            ),
            2 => (
                format!("Literal 100%_wildcard marker {ordinal:04}"),
                Some("contains punctuation literally".to_owned()),
            ),
            3 => (
                format!("Regex sentinel item {ordinal:04}"),
                Some("aaaaab".to_owned()),
            ),
            4 => (
                format!("Quoted database search phrase {ordinal:04}"),
                Some("phrase fixture".to_owned()),
            ),
            5 => (
                format!("English running workflow {ordinal:04}"),
                Some("deployment notes".to_owned()),
            ),
            6 => (
                format!("Ordinary project task {ordinal:04}"),
                Some("unrelated body text".to_owned()),
            ),
            _ => (
                format!("Case insensitive deployment {ordinal:04}"),
                Some("regex and keyword coverage".to_owned()),
            ),
        };
        let title = format!("{title} fixture task");
        sqlx::query(
            "INSERT INTO tasks (team_id, key, title, description, metadata, created_by) VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(fixture.team_id)
        .bind(key)
        .bind(title)
        .bind(description)
        .bind(json!({"fixture_ordinal": ordinal}))
        .bind(fixture.agent_id)
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query(
        "UPDATE tasks SET updated_at = TIMESTAMPTZ '2026-01-01 00:00:00+00' WHERE team_id = $1 AND key LIKE $2",
    )
    .bind(fixture.team_id)
    .bind(format!("{}:%", fixture.task_prefix))
    .execute(&mut *tx)
    .await?;
    tx.commit().await
}

async fn add_foreign_team_task(fixture: &Fixture) -> Result<Uuid, sqlx::Error> {
    let mut tx = fixture.pool.begin().await?;
    let team_id: Uuid =
        sqlx::query_scalar("INSERT INTO teams (slug, name) VALUES ($1, $2) RETURNING id")
            .bind(format!("search-foreign-{}", Uuid::new_v4().simple()))
            .bind("foreign search team")
            .fetch_one(&mut *tx)
            .await?;
    let agent_id: Uuid =
        sqlx::query_scalar("INSERT INTO agents (team_id, name) VALUES ($1, $2) RETURNING id")
            .bind(team_id)
            .bind("foreign-agent")
            .fetch_one(&mut *tx)
            .await?;
    sqlx::query(
        "INSERT INTO tasks (team_id, key, title, description, created_by) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(team_id)
    .bind(format!("{}:foreign", fixture.task_prefix))
    .bind("deployment must not cross ACL")
    .bind("foreign description")
    .bind(agent_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(team_id)
}

async fn claim_one(
    fixture: &Fixture,
    auth: &AuthCtx,
) -> Result<String, Box<dyn std::error::Error>> {
    let key = format!("{}:0000", fixture.task_prefix);
    tasks::claim_task(&fixture.pool, auth, &key, Some(60)).await?;
    Ok(key)
}

async fn run_real_client(
    fixture: &Fixture,
    search_query: Option<&str>,
    search_mode: Option<&str>,
    search_fields: Option<&str>,
    search_language: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let raw_token = ai_crew_sync::auth::generate_token();
    sqlx::query(
        "INSERT INTO api_tokens (agent_id, token_hash, prefix, label) VALUES ($1, $2, $3, $4)",
    )
    .bind(fixture.agent_id)
    .bind(ai_crew_sync::auth::hash_token(&raw_token))
    .bind(ai_crew_sync::auth::token_prefix(&raw_token))
    .bind("task search E2E")
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
            dashboard_secret: b"task-search-client-test-secret".to_vec(),
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

    let command = ClientCmd::Tasks {
        status: None,
        mine: false,
        project_prefix: Some(fixture.task_prefix.clone()),
        limit: Some(25),
        cursor: None,
        search: search_query.map(str::to_owned),
        search_mode: search_mode.map(str::to_owned),
        search_fields: search_fields.map(str::to_owned),
        search_language: search_language.map(str::to_owned),
        all_pages: false,
    };
    let result = client::run(ClientArgs {
        url: Some(format!("http://{address}/mcp")),
        token: Some(raw_token),
        profile: None,
        project_dir: None,
        host_session: None,
        session: Some("task-search-client".to_owned()),
        json: true,
        command,
    })
    .await;

    cancellation.cancel();
    server.abort();
    result.map_err(Into::into)
}

async fn run_e2e() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = setup_fixture().await?;
    let primary = auth(
        &fixture,
        fixture.agent_id,
        "fixture-primary",
        "search-worker",
    );
    let secondary = auth(
        &fixture,
        fixture.other_agent_id,
        "fixture-secondary",
        "search-secondary",
    );
    let result = async {
        seed_search_fixture(&fixture).await?;
        let foreign_team_id = add_foreign_team_task(&fixture).await?;
        let claimed_key = claim_one(&fixture, &primary).await?;

        let title_only = tasks::list_tasks_page_with_search(
            &fixture.pool,
            &primary,
            page_query(Some(&fixture.task_prefix), None, false, 50, None),
            search(
                Some("deployment"),
                Some("keywords"),
                Some("title"),
                Some("simple"),
            ),
        )
        .await?;
        assert!(
            title_only
                .tasks
                .iter()
                .all(|task| task.title.to_ascii_lowercase().contains("deployment"))
        );
        assert!(title_only.tasks.len() >= 50);
        assert_eq!(
            title_only.open + title_only.claimed,
            LARGE_FIXTURE_SIZE as i64
        );

        let description_only = tasks::list_tasks_page_with_search(
            &fixture.pool,
            &secondary,
            page_query(Some(&fixture.task_prefix), None, false, 100, None),
            search(
                Some("быстрый"),
                Some("keywords"),
                Some("description"),
                Some("russian"),
            ),
        )
        .await?;
        assert!(!description_only.tasks.is_empty());
        assert!(description_only.tasks.iter().all(|task| {
            task.description
                .as_deref()
                .unwrap_or_default()
                .contains("Быстрые")
        }));

        let english_stemming = tasks::list_tasks_page_with_search(
            &fixture.pool,
            &primary,
            page_query(Some(&fixture.task_prefix), None, false, 100, None),
            search(
                Some("run"),
                Some("keywords"),
                Some("title"),
                Some("english"),
            ),
        )
        .await?;
        assert!(!english_stemming.tasks.is_empty());
        assert!(
            english_stemming
                .tasks
                .iter()
                .all(|task| task.title.to_ascii_lowercase().contains("running"))
        );

        let phrase = tasks::list_tasks_page_with_search(
            &fixture.pool,
            &primary,
            page_query(Some(&fixture.task_prefix), None, false, 100, None),
            search(
                Some("\"database search\""),
                Some("keywords"),
                Some("both"),
                Some("simple"),
            ),
        )
        .await?;
        assert!(!phrase.tasks.is_empty());

        let literal = tasks::list_tasks_page_with_search(
            &fixture.pool,
            &primary,
            page_query(Some(&fixture.task_prefix), None, false, 100, None),
            search(
                Some("100%_wildcard"),
                Some("contains"),
                Some("title"),
                Some("simple"),
            ),
        )
        .await?;
        assert!(!literal.tasks.is_empty());
        assert!(
            literal
                .tasks
                .iter()
                .all(|task| task.title.contains("100%_wildcard"))
        );

        let regex = tasks::list_tasks_page_with_search(
            &fixture.pool,
            &primary,
            page_query(Some(&fixture.task_prefix), None, false, 100, None),
            search(
                Some("DEPLOYMENT"),
                Some("regex"),
                Some("both"),
                Some("simple"),
            ),
        )
        .await?;
        assert!(!regex.tasks.is_empty());
        assert!(regex.tasks.iter().all(|task| {
            task.title.to_ascii_lowercase().contains("deployment")
                || task
                    .description
                    .as_deref()
                    .unwrap_or_default()
                    .to_ascii_lowercase()
                    .contains("deployment")
        }));

        for invalid in [
            search(Some("["), Some("regex"), Some("title"), Some("simple")),
            search(Some("x"), Some("unknown"), Some("title"), Some("simple")),
            search(Some("x"), Some("contains"), Some("unknown"), Some("simple")),
            search(Some("x"), Some("keywords"), Some("title"), Some("unknown")),
            search(
                Some(&"x".repeat(513)),
                Some("contains"),
                Some("title"),
                Some("simple"),
            ),
        ] {
            assert!(
                tasks::list_tasks_page_with_search(
                    &fixture.pool,
                    &primary,
                    page_query(Some(&fixture.task_prefix), None, false, 20, None),
                    invalid,
                )
                .await
                .is_err()
            );
        }

        let pathological_start = Instant::now();
        let pathological = tokio::time::timeout(
            Duration::from_secs(2),
            tasks::list_tasks_page_with_search(
                &fixture.pool,
                &primary,
                page_query(Some(&fixture.task_prefix), None, false, 10, None),
                search(
                    Some("^(a+)+$"),
                    Some("regex"),
                    Some("description"),
                    Some("simple"),
                ),
            ),
        )
        .await??;
        assert!(pathological.tasks.len() <= 10);
        assert!(pathological_start.elapsed() < Duration::from_secs(2));

        let mut cursor = None;
        let mut keys = HashSet::with_capacity(LARGE_FIXTURE_SIZE);
        let mut pages = 0;
        loop {
            let page = tasks::list_tasks_page_with_search(
                &fixture.pool,
                &primary,
                page_query(Some(&fixture.task_prefix), None, false, 200, cursor),
                search(Some("task"), Some("contains"), Some("both"), Some("simple")),
            )
            .await?;
            pages += 1;
            for task in page.tasks {
                assert!(keys.insert(task.key), "duplicate key in search pagination");
            }
            if !page.has_more {
                assert!(page.next_cursor.is_none());
                break;
            }
            cursor = page.next_cursor;
        }
        assert_eq!(keys.len(), LARGE_FIXTURE_SIZE);
        assert_eq!(pages, 10);

        let first = tasks::list_tasks_page_with_search(
            &fixture.pool,
            &primary,
            page_query(Some(&fixture.task_prefix), None, false, 10, None),
            search(
                Some("deployment"),
                Some("contains"),
                Some("both"),
                Some("simple"),
            ),
        )
        .await?;
        let token = first.next_cursor.clone().expect("search page cursor");
        for altered in [
            search(
                Some("database"),
                Some("contains"),
                Some("both"),
                Some("simple"),
            ),
            search(
                Some("deployment"),
                Some("regex"),
                Some("both"),
                Some("simple"),
            ),
            search(
                Some("deployment"),
                Some("contains"),
                Some("title"),
                Some("simple"),
            ),
            search(
                Some("deployment"),
                Some("contains"),
                Some("both"),
                Some("english"),
            ),
        ] {
            assert!(
                tasks::list_tasks_page_with_search(
                    &fixture.pool,
                    &primary,
                    page_query(
                        Some(&fixture.task_prefix),
                        None,
                        false,
                        10,
                        Some(token.clone())
                    ),
                    altered,
                )
                .await
                .is_err()
            );
        }

        let status_page = tasks::list_tasks_page_with_search(
            &fixture.pool,
            &primary,
            page_query(Some(&fixture.task_prefix), Some("claimed"), true, 20, None),
            search(
                Some("deployment"),
                Some("contains"),
                Some("both"),
                Some("simple"),
            ),
        )
        .await?;
        assert_eq!(status_page.tasks.len(), 1);
        assert_eq!(status_page.tasks[0].key, claimed_key);
        assert_eq!(status_page.claimed, 1);

        let empty_search = tasks::list_tasks_page_with_search(
            &fixture.pool,
            &primary,
            page_query(Some(&fixture.task_prefix), None, false, 20, None),
            search(Some("   "), None, None, None),
        )
        .await?;
        let default_page = tasks::list_tasks_page(
            &fixture.pool,
            &primary,
            page_query(Some(&fixture.task_prefix), None, false, 20, None),
        )
        .await?;
        assert_eq!(empty_search.tasks.len(), default_page.tasks.len());
        assert_eq!(empty_search.tasks[0].key, default_page.tasks[0].key);

        run_real_client(&fixture, None, None, None, None).await?;
        run_real_client(
            &fixture,
            Some("deployment"),
            Some("contains"),
            Some("both"),
            Some("simple"),
        )
        .await?;

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
async fn searches_2000_tasks_across_modes_filters_acl_and_clients() {
    run_e2e().await.expect("Postgres task-search E2E");
}
