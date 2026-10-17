#![allow(clippy::too_many_lines)]

#[path = "task_pagination_fixture.rs"]
mod fixture;

use std::{collections::HashSet, sync::Arc};

use ai_crew_sync::{
    auth::AuthCtx,
    client::{self, ClientArgs, ClientCmd},
    serve::{self, ServeOptions},
    store::tasks::{self, CreateInput, TaskPageQuery},
};
use fixture::{Fixture, LARGE_FIXTURE_SIZE, cleanup, setup_fixture};
use serde_json::json;
use tokio::task::JoinSet;
use uuid::Uuid;

fn auth(fixture: &Fixture, agent_id: Uuid, agent_name: &str, session: &str) -> AuthCtx {
    AuthCtx {
        agent_id,
        agent_name: agent_name.to_owned(),
        team_id: fixture.team_id,
        team_slug: format!("pagination-fixture-{}", fixture.team_id.simple()),
        session: session.to_owned(),
        session_id: None,
        session_epoch: None,
        token_id: None,
    }
}

async fn create_in_parallel(
    fixture: &Fixture,
    primary: &AuthCtx,
) -> Result<(), Box<dyn std::error::Error>> {
    let workers = 20;
    let per_worker = LARGE_FIXTURE_SIZE / workers;
    let pool = Arc::new(fixture.pool.clone());
    let auth = Arc::new(primary.clone());
    let mut jobs = JoinSet::new();

    for worker in 0..workers {
        let pool = Arc::clone(&pool);
        let auth = Arc::clone(&auth);
        let prefix = fixture.task_prefix.clone();
        jobs.spawn(async move {
            let first = worker * per_worker;
            let last = first + per_worker;
            for ordinal in first..last {
                tasks::create_task(
                    &pool,
                    &auth,
                    CreateInput {
                        key: format!("{prefix}:{ordinal:04}"),
                        title: format!("pagination task {ordinal:04}"),
                        description: None,
                        metadata: Some(json!({"fixture_ordinal": ordinal})),
                        depends_on: Vec::new(),
                    },
                )
                .await
                .map_err(|error| error.to_string())?;
            }
            Ok::<(), String>(())
        });
    }

    while let Some(result) = jobs.join_next().await {
        result?.map_err(std::io::Error::other)?;
    }
    Ok(())
}

async fn complete_in_parallel(
    fixture: &Fixture,
    primary: &AuthCtx,
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = Arc::new(fixture.pool.clone());
    let auth = Arc::new(primary.clone());
    let mut jobs = JoinSet::new();

    for ordinal in 0..32 {
        let pool = Arc::clone(&pool);
        let auth = Arc::clone(&auth);
        let key = format!("{}:{ordinal:04}", fixture.task_prefix);
        jobs.spawn(async move {
            tasks::claim_task(&pool, &auth, &key, Some(60))
                .await
                .map_err(|error| error.to_string())?;
            tasks::complete_task(
                &pool,
                &auth,
                &key,
                Some("completed by parallel worker".to_owned()),
            )
            .await
            .map_err(|error| error.to_string())?;
            Ok::<(), String>(())
        });
    }

    while let Some(result) = jobs.join_next().await {
        result?.map_err(std::io::Error::other)?;
    }
    Ok(())
}

async fn set_same_timestamp(fixture: &Fixture) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE tasks SET updated_at = TIMESTAMPTZ '2026-01-01 00:00:00+00' WHERE team_id = $1 AND key LIKE $2",
    )
    .bind(fixture.team_id)
    .bind(format!("{}:%", fixture.task_prefix))
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

async fn add_foreign_team_task(fixture: &Fixture) -> Result<Uuid, sqlx::Error> {
    let mut tx = fixture.pool.begin().await?;
    let team_id: Uuid =
        sqlx::query_scalar("INSERT INTO teams (slug, name) VALUES ($1, $2) RETURNING id")
            .bind(format!("pagination-foreign-{}", Uuid::new_v4().simple()))
            .bind("foreign pagination team")
            .fetch_one(&mut *tx)
            .await?;
    let agent_id: Uuid =
        sqlx::query_scalar("INSERT INTO agents (team_id, name) VALUES ($1, $2) RETURNING id")
            .bind(team_id)
            .bind("foreign-agent")
            .fetch_one(&mut *tx)
            .await?;
    sqlx::query("INSERT INTO tasks (team_id, key, title, created_by) VALUES ($1, $2, $3, $4)")
        .bind(team_id)
        .bind(format!("{}:foreign", fixture.task_prefix))
        .bind("must not cross the team ACL")
        .bind(agent_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(team_id)
}

async fn run_real_client(
    fixture: &Fixture,
    project_prefix: &str,
    cursor: Option<String>,
    all_pages: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let raw_token = ai_crew_sync::auth::generate_token();
    sqlx::query(
        "INSERT INTO api_tokens (agent_id, token_hash, prefix, label) VALUES ($1, $2, $3, $4)",
    )
    .bind(fixture.agent_id)
    .bind(ai_crew_sync::auth::hash_token(&raw_token))
    .bind(ai_crew_sync::auth::token_prefix(&raw_token))
    .bind("pagination client E2E")
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
            dashboard_secret: b"pagination-client-test-secret".to_vec(),
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
        project_prefix: Some(project_prefix.to_owned()),
        limit: Some(200),
        cursor,
        search: None,
        search_mode: None,
        search_fields: None,
        search_language: None,
        all_pages,
    };
    let result = client::run(ClientArgs {
        url: Some(format!("http://{address}/mcp")),
        token: Some(raw_token),
        profile: None,
        project_dir: None,
        host_session: None,
        session: Some("pagination-client".to_owned()),
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
        "pagination-worker",
    );
    let secondary = auth(
        &fixture,
        fixture.other_agent_id,
        "fixture-secondary",
        "pagination-secondary",
    );

    let result = async {
        create_in_parallel(&fixture, &primary).await?;
        complete_in_parallel(&fixture, &primary).await?;
        fixture::seed_expiring_lease(&fixture, "lease-expired").await?;
        let foreign_team_id = add_foreign_team_task(&fixture).await?;
        set_same_timestamp(&fixture).await?;

        let mut cursor = None;
        let mut keys = HashSet::with_capacity(LARGE_FIXTURE_SIZE);
        let mut pages = 0;
        loop {
            let page = tasks::list_tasks_page(
                &fixture.pool,
                &primary,
                TaskPageQuery {
                    status: None,
                    mine_only: false,
                    limit: 200,
                    project_prefix: Some(fixture.task_prefix.clone()),
                    cursor,
                },
            )
            .await?;
            pages += 1;
            assert_eq!(page.tasks.len(), 200);
            for task in page.tasks {
                assert!(keys.insert(task.key), "duplicate key in pagination result");
            }
            if !page.has_more {
                assert!(page.next_cursor.is_none());
                break;
            }
            cursor = page.next_cursor;
            assert!(cursor.is_some());
        }
        assert_eq!(pages, 10);
        assert_eq!(keys.len(), LARGE_FIXTURE_SIZE);
        assert!(!keys.iter().any(|key| key.ends_with(":foreign")));

        let done = tasks::list_tasks_page(
            &fixture.pool,
            &primary,
            TaskPageQuery {
                status: Some("done".to_owned()),
                mine_only: false,
                limit: 100,
                project_prefix: Some(fixture.task_prefix.clone()),
                cursor: None,
            },
        )
        .await?;
        assert_eq!(done.tasks.len(), 32);
        assert!(done.tasks.iter().all(|task| task.status == "done"));

        let claimed_key = format!("{}:0500", fixture.task_prefix);
        tasks::claim_task(&fixture.pool, &primary, &claimed_key, Some(60)).await?;
        let mine = tasks::list_tasks_page(
            &fixture.pool,
            &primary,
            TaskPageQuery {
                status: Some("claimed".to_owned()),
                mine_only: true,
                limit: 20,
                project_prefix: Some(fixture.task_prefix.clone()),
                cursor: None,
            },
        )
        .await?;
        assert_eq!(mine.tasks.len(), 1);
        assert_eq!(mine.tasks[0].key, claimed_key);
        assert_eq!(mine.claimed, 1);

        let open = tasks::list_tasks_page(
            &fixture.pool,
            &secondary,
            TaskPageQuery {
                status: Some("open".to_owned()),
                mine_only: false,
                limit: 20,
                project_prefix: Some("lease-".to_owned()),
                cursor: None,
            },
        )
        .await?;
        assert_eq!(open.tasks.len(), 1);
        assert_eq!(open.tasks[0].key, "lease-expired");
        assert!(open.tasks[0].lease_expired);

        let first = tasks::list_tasks_page(
            &fixture.pool,
            &primary,
            TaskPageQuery {
                status: None,
                mine_only: false,
                limit: 10,
                project_prefix: Some(fixture.task_prefix.clone()),
                cursor: None,
            },
        )
        .await?;
        let token = first.next_cursor.expect("first page has cursor");
        run_real_client(&fixture, &fixture.task_prefix, None, true).await?;
        run_real_client(&fixture, &fixture.task_prefix, Some(token.clone()), false).await?;
        let mut tampered = token.clone().into_bytes();
        let last = tampered.len() - 1;
        tampered[last] = if tampered[last] == b'A' { b'B' } else { b'A' };
        let tampered = String::from_utf8(tampered)?;
        assert!(
            tasks::list_tasks_page(
                &fixture.pool,
                &primary,
                TaskPageQuery {
                    status: None,
                    mine_only: false,
                    limit: 10,
                    project_prefix: Some(fixture.task_prefix.clone()),
                    cursor: Some(tampered),
                },
            )
            .await
            .is_err()
        );
        assert!(
            tasks::list_tasks_page(
                &fixture.pool,
                &primary,
                TaskPageQuery {
                    status: Some("done".to_owned()),
                    mine_only: false,
                    limit: 10,
                    project_prefix: Some(fixture.task_prefix.clone()),
                    cursor: Some(token),
                },
            )
            .await
            .is_err()
        );

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
async fn paginates_2000_tasks_with_concurrent_writes_and_acl_guards() {
    run_e2e().await.expect("Postgres pagination E2E");
}
