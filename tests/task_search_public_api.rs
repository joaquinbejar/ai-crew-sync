#![allow(clippy::too_many_lines)]

#[path = "task_pagination_fixture.rs"]
mod fixture;

use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use ai_crew_sync::{
    auth::AuthCtx,
    store::tasks::{self, TaskPageQuery, TaskSearchQuery},
};
use fixture::{Fixture, LARGE_FIXTURE_SIZE, cleanup, setup_fixture};
use serde_json::json;
use uuid::Uuid;

fn auth(fixture: &Fixture, agent_id: Uuid, name: &str, session: &str) -> AuthCtx {
    AuthCtx {
        agent_id,
        agent_name: name.to_owned(),
        team_id: fixture.team_id,
        team_slug: format!("public-search-{}", fixture.team_id.simple()),
        session: session.to_owned(),
        session_id: None,
        session_epoch: None,
        token_id: None,
    }
}

fn page(
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

async fn seed(fixture: &Fixture) -> Result<(), sqlx::Error> {
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
        sqlx::query(
            "INSERT INTO tasks (team_id, key, title, description, metadata, created_by) VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(fixture.team_id)
        .bind(key)
        .bind(format!("{title} fixture task"))
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

async fn add_foreign_task(fixture: &Fixture) -> Result<Uuid, sqlx::Error> {
    let mut tx = fixture.pool.begin().await?;
    let team_id: Uuid =
        sqlx::query_scalar("INSERT INTO teams (slug, name) VALUES ($1, $2) RETURNING id")
            .bind(format!("public-search-foreign-{}", Uuid::new_v4().simple()))
            .bind("public search foreign team")
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
    .bind("deployment must not cross the team ACL")
    .bind("foreign description")
    .bind(agent_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(team_id)
}

async fn insert_long_regex_row(fixture: &Fixture) -> Result<(), sqlx::Error> {
    let long_input = format!("{}!", "a".repeat(12_000));
    sqlx::query(
        "INSERT INTO tasks (team_id, key, title, description, created_by) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(fixture.team_id)
    .bind(format!("{}:regex-timeout", fixture.task_prefix))
    .bind("regex timeout sentinel")
    .bind(long_input)
    .bind(fixture.agent_id)
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

async fn run_public_api_matrix() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = setup_fixture().await?;
    let primary = auth(
        &fixture,
        fixture.agent_id,
        "fixture-primary",
        "public-search-primary",
    );
    let secondary = auth(
        &fixture,
        fixture.other_agent_id,
        "fixture-secondary",
        "public-search-secondary",
    );
    let foreign_team_id = add_foreign_task(&fixture).await?;
    let result = async {
        seed(&fixture).await?;
        insert_long_regex_row(&fixture).await?;

        let title_hits = tasks::list_tasks_page_with_search(
            &fixture.pool,
            &primary,
            page(Some(&fixture.task_prefix), None, false, 50, None),
            search(
                Some("deployment"),
                Some("contains"),
                Some("title"),
                Some("simple"),
            ),
        )
        .await?;
        assert!(!title_hits.tasks.is_empty());
        assert!(title_hits
            .tasks
            .iter()
            .all(|task| task.title.to_ascii_lowercase().contains("deployment")));
        assert_eq!(title_hits.open + title_hits.claimed, (LARGE_FIXTURE_SIZE + 1) as i64);

        let description_hits = tasks::list_tasks_page_with_search(
            &fixture.pool,
            &secondary,
            page(Some(&fixture.task_prefix), None, false, 50, None),
            search(
                Some("быстрый"),
                Some("keywords"),
                Some("description"),
                Some("russian"),
            ),
        )
        .await?;
        assert!(!description_hits.tasks.is_empty());
        assert!(description_hits
            .tasks
            .iter()
            .all(|task| task.description.as_deref().unwrap_or_default().contains("Быстрые")));

        let regex_hits = tasks::list_tasks_page_with_search(
            &fixture.pool,
            &primary,
            page(Some(&fixture.task_prefix), None, false, 50, None),
            search(
                Some("DEPLOYMENT"),
                Some("regex"),
                Some("both"),
                Some("simple"),
            ),
        )
        .await?;
        assert!(!regex_hits.tasks.is_empty());
        assert!(regex_hits.tasks.iter().all(|task| {
            task.title.to_ascii_lowercase().contains("deployment")
                || task
                    .description
                    .as_deref()
                    .unwrap_or_default()
                    .to_ascii_lowercase()
                    .contains("deployment")
        }));

        let literal_hits = tasks::list_tasks_page_with_search(
            &fixture.pool,
            &primary,
            page(Some(&fixture.task_prefix), None, false, 50, None),
            search(
                Some("100%_wildcard"),
                Some("contains"),
                Some("title"),
                Some("simple"),
            ),
        )
        .await?;
        assert!(!literal_hits.tasks.is_empty());
        assert!(literal_hits
            .tasks
            .iter()
            .all(|task| task.title.contains("100%_wildcard")));

        for invalid in [
            search(
                Some("["),
                Some("regex"),
                Some("title"),
                Some("simple"),
            ),
            search(
                Some("term"),
                Some("invalid"),
                Some("title"),
                Some("simple"),
            ),
            search(
                Some("term"),
                Some("contains"),
                Some("invalid"),
                Some("simple"),
            ),
            search(
                Some("term"),
                Some("keywords"),
                Some("title"),
                Some("invalid"),
            ),
            search(
                Some(&"x".repeat(513)),
                Some("contains"),
                Some("title"),
                Some("simple"),
            ),
            search(
                Some("a\0b"),
                Some("regex"),
                Some("title"),
                Some("simple"),
            ),
        ] {
            let error = tasks::list_tasks_page_with_search(
                &fixture.pool,
                &primary,
                page(Some(&fixture.task_prefix), None, false, 20, None),
                invalid,
            )
            .await
            .expect_err("invalid public search input must be rejected");
            let message = error.to_string();
            assert!(message.starts_with("invalid input:"), "unexpected safe classification: {message}");
        }

        let timeout_start = Instant::now();
        let pathological = tokio::time::timeout(
            Duration::from_millis(750),
            tasks::list_tasks_page_with_search(
                &fixture.pool,
                &primary,
                page(Some(&fixture.task_prefix), None, false, 10, None),
                search(
                    Some("^(a+)+$"),
                    Some("regex"),
                    Some("description"),
                    Some("simple"),
                ),
            ),
        )
        .await
        .expect("pathological regex request must not exceed outer safety bound");
        assert!(timeout_start.elapsed() < Duration::from_secs(2));
        match pathological {
            Ok(result) => assert!(result.tasks.len() <= 10),
            Err(error) => assert!(
                error.to_string().contains("250ms"),
                "unsafe timeout mapping: {error}"
            ),
        }

        let first_page = tasks::list_tasks_page_with_search(
            &fixture.pool,
            &primary,
            page(Some(&fixture.task_prefix), None, false, 25, None),
            search(
                Some("task"),
                Some("contains"),
                Some("both"),
                Some("simple"),
            ),
        )
        .await?;
        let cursor = first_page
            .next_cursor
            .clone()
            .expect("bounded search page must expose a cursor");
        let mut seen = first_page
            .tasks
            .iter()
            .map(|task| task.key.clone())
            .collect::<HashSet<_>>();
        let second_page = tasks::list_tasks_page_with_search(
            &fixture.pool,
            &primary,
            page(Some(&fixture.task_prefix), None, false, 25, Some(cursor.clone())),
            search(
                Some("task"),
                Some("contains"),
                Some("both"),
                Some("simple"),
            ),
        )
        .await?;
        for task in &second_page.tasks {
            assert!(seen.insert(task.key.clone()), "cursor returned a duplicate task");
        }
        assert_eq!(seen.len(), 50);

        for altered in [
            search(
                Some("deployment"),
                Some("contains"),
                Some("both"),
                Some("simple"),
            ),
            search(
                Some("task"),
                Some("regex"),
                Some("both"),
                Some("simple"),
            ),
            search(
                Some("task"),
                Some("contains"),
                Some("title"),
                Some("simple"),
            ),
            search(
                Some("task"),
                Some("contains"),
                Some("both"),
                Some("english"),
            ),
        ] {
            assert!(tasks::list_tasks_page_with_search(
                &fixture.pool,
                &primary,
                page(Some(&fixture.task_prefix), None, false, 25, Some(cursor.clone())),
                altered,
            )
            .await
            .is_err(),
                "cursor must bind every search option"
            );
        }

        sqlx::query(
            "INSERT INTO tasks (team_id, key, title, description, updated_at, created_by) VALUES ($1, $2, $3, $4, TIMESTAMPTZ '2026-01-01 00:00:00+00', $5)",
        )
        .bind(fixture.team_id)
        .bind(format!("{}:0025.5", fixture.task_prefix))
        .bind("live cursor task")
        .bind("task inserted after the first page")
        .bind(fixture.agent_id)
        .execute(&fixture.pool)
        .await?;
        let live_page = tasks::list_tasks_page_with_search(
            &fixture.pool,
            &primary,
            page(Some(&fixture.task_prefix), None, false, 25, Some(cursor)),
            search(
                Some("task"),
                Some("contains"),
                Some("both"),
                Some("simple"),
            ),
        )
        .await?;
        assert!(
            live_page
                .tasks
                .iter()
                .any(|task| task.key.ends_with(":0025.5")),
            "cursor is live-inventory, not a snapshot"
        );

        let empty = tasks::list_tasks_page_with_search(
            &fixture.pool,
            &primary,
            page(Some(&fixture.task_prefix), None, false, 30, None),
            search(Some("   "), None, None, None),
        )
        .await?;
        let legacy = tasks::list_tasks_page(
            &fixture.pool,
            &primary,
            page(Some(&fixture.task_prefix), None, false, 30, None),
        )
        .await?;
        assert_eq!(
            empty.tasks.iter().map(|task| &task.key).collect::<Vec<_>>(),
            legacy
                .tasks
                .iter()
                .map(|task| &task.key)
                .collect::<Vec<_>>()
        );
        assert_eq!((empty.open, empty.claimed), (legacy.open, legacy.claimed));

        tasks::claim_task(
            &fixture.pool,
            &primary,
            &format!("{}:0000", fixture.task_prefix),
            Some(60),
        )
        .await?;
        let mine = tasks::list_tasks_page_with_search(
            &fixture.pool,
            &primary,
            page(Some(&fixture.task_prefix), Some("claimed"), true, 10, None),
            search(
                Some("deployment"),
                Some("contains"),
                Some("both"),
                Some("simple"),
            ),
        )
        .await?;
        assert_eq!(mine.tasks.len(), 1);
        assert_eq!(mine.tasks[0].key, format!("{}:0000", fixture.task_prefix));

        let foreign_rows = tasks::list_tasks_page_with_search(
            &fixture.pool,
            &primary,
            page(None, None, false, 100, None),
            search(
                Some("cross the team ACL"),
                Some("contains"),
                Some("both"),
                Some("simple"),
            ),
        )
        .await?;
        assert!(foreign_rows.tasks.is_empty(), "search must remain team-scoped");

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
async fn public_task_search_api_safety_matrix() {
    run_public_api_matrix()
        .await
        .expect("public task search API safety matrix");
}
