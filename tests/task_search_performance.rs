#![allow(clippy::too_many_lines)]

#[path = "task_pagination_fixture.rs"]
mod fixture;

use std::{
    env,
    time::{Duration, Instant},
};

use fixture::{Fixture, LARGE_FIXTURE_SIZE, cleanup, populate_2000_tasks, setup_fixture};
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

const REGEX_TIMEOUT: &str = "250ms";
const OUTER_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Copy)]
enum SearchPlan {
    Keywords,
    Contains,
    Regex,
}

async fn seed_benchmark_rows(fixture: &Fixture) -> Result<(), sqlx::Error> {
    populate_2000_tasks(fixture).await?;
    let mut tx = fixture.pool.begin().await?;
    sqlx::query(
        r#"
        UPDATE tasks
        SET title = CASE
                WHEN (right(key, 4)::int % 4) = 0 THEN 'Running deployment database search ' || right(key, 4)
                WHEN (right(key, 4)::int % 4) = 1 THEN 'Russian inventory task ' || right(key, 4)
                WHEN (right(key, 4)::int % 4) = 2 THEN 'Literal 100%_wildcard marker ' || right(key, 4)
                ELSE 'Regex sentinel item ' || right(key, 4)
            END,
            description = CASE
                WHEN (right(key, 4)::int % 4) = 0 THEN 'English operator guide for the running service ' || right(key, 4)
                WHEN (right(key, 4)::int % 4) = 1 THEN 'Быстрые задачи проекта и поиск данных ' || right(key, 4)
                WHEN (right(key, 4)::int % 4) = 2 THEN 'contains punctuation literally'
                ELSE repeat('a', 8192)
            END
        WHERE team_id = $1 AND key LIKE $2
        "#,
    )
    .bind(fixture.team_id)
    .bind(format!("{}:%", fixture.task_prefix))
    .execute(&mut *tx)
    .await?;
    tx.commit().await
}

async fn run_explain(
    pool: &PgPool,
    team_id: Uuid,
    sql: &'static str,
    term: String,
) -> Result<String, sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT set_config('statement_timeout', $1, true)")
        .bind(REGEX_TIMEOUT)
        .execute(&mut *tx)
        .await?;
    let configured: String = sqlx::query_scalar("SELECT current_setting('statement_timeout')")
        .fetch_one(&mut *tx)
        .await?;
    assert_eq!(configured, REGEX_TIMEOUT);
    let lines: Vec<String> = sqlx::query_scalar(AssertSqlSafe(sql))
        .bind(team_id)
        .bind(term)
        .fetch_all(&mut *tx)
        .await?;
    tx.rollback().await?;
    Ok(lines.join("\n"))
}

async fn explain_analyze(
    pool: &PgPool,
    team_id: Uuid,
    plan: SearchPlan,
    term: &str,
) -> Result<String, sqlx::Error> {
    let sql = match plan {
        SearchPlan::Keywords => {
            r#"
            EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
            SELECT id FROM tasks
            WHERE team_id = $1
              AND (setweight(to_tsvector('simple', coalesce(title, '')), 'A') ||
                   setweight(to_tsvector('simple', coalesce(description, '')), 'B'))
                  @@ websearch_to_tsquery('simple', $2)
            ORDER BY (CASE WHEN status = 'claimed' AND lease_expires_at <= now() THEN 1 WHEN status = 'claimed' THEN 0 WHEN status = 'open' THEN 1 ELSE 2 END), updated_at DESC, key ASC
            LIMIT 50
            "#
        }
        SearchPlan::Contains => {
            r#"
            EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
            SELECT id FROM tasks
            WHERE team_id = $1
              AND (title ILIKE ('%' || $2 || '%') ESCAPE chr(92)
                   OR coalesce(description, '') ILIKE ('%' || $2 || '%') ESCAPE chr(92))
            ORDER BY (CASE WHEN status = 'claimed' AND lease_expires_at <= now() THEN 1 WHEN status = 'claimed' THEN 0 WHEN status = 'open' THEN 1 ELSE 2 END), updated_at DESC, key ASC
            LIMIT 50
            "#
        }
        SearchPlan::Regex => {
            r#"
            EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
            SELECT id FROM tasks
            WHERE team_id = $1
              AND (title ~* $2 OR coalesce(description, '') ~* $2)
            ORDER BY (CASE WHEN status = 'claimed' AND lease_expires_at <= now() THEN 1 WHEN status = 'claimed' THEN 0 WHEN status = 'open' THEN 1 ELSE 2 END), updated_at DESC, key ASC
            LIMIT 50
            "#
        }
    };
    run_explain(pool, team_id, sql, term.to_owned()).await
}

async fn explain_nonindexable_regex(
    pool: &PgPool,
    team_id: Uuid,
    pattern: &str,
) -> Result<String, sqlx::Error> {
    run_explain(
        pool,
        team_id,
        r#"
        EXPLAIN (FORMAT TEXT)
        SELECT id FROM tasks
        WHERE team_id = $1
          AND (title ~* $2 OR coalesce(description, '') ~* $2)
        ORDER BY (CASE WHEN status = 'claimed' AND lease_expires_at <= now() THEN 1 WHEN status = 'claimed' THEN 0 WHEN status = 'open' THEN 1 ELSE 2 END), updated_at DESC, key ASC
        LIMIT 50
        "#,
        pattern.to_owned(),
    )
    .await
}

async fn run_bounded_regex(
    pool: &PgPool,
    team_id: Uuid,
    pattern: String,
) -> Result<Duration, Box<dyn std::error::Error>> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT set_config('statement_timeout', $1, true)")
        .bind(REGEX_TIMEOUT)
        .execute(&mut *tx)
        .await?;
    let configured: String = sqlx::query_scalar("SELECT current_setting('statement_timeout')")
        .fetch_one(&mut *tx)
        .await?;
    assert_eq!(configured, REGEX_TIMEOUT);
    let started = Instant::now();
    let result = tokio::time::timeout(
        OUTER_TIMEOUT,
        sqlx::query_scalar::<_, i64>(AssertSqlSafe(
            r#"
            SELECT count(*)::bigint FROM tasks
            WHERE team_id = $1
              AND (title ~* $2 OR coalesce(description, '') ~* $2)
            "#,
        ))
        .bind(team_id)
        .bind(pattern)
        .fetch_one(&mut *tx),
    )
    .await;
    let elapsed = started.elapsed();
    match result {
        Err(error) => panic!("outer regex timeout exceeded: {error}"),
        Ok(Ok(_)) => {}
        Ok(Err(error)) => {
            let code = error
                .as_database_error()
                .and_then(|database_error| database_error.code());
            assert_eq!(
                code.as_deref(),
                Some("57014"),
                "unexpected regex error: {error}"
            );
        }
    }
    tx.rollback().await?;
    Ok(elapsed)
}

fn assert_tenant_and_timing(plan: &str, label: &str) {
    assert!(
        plan.contains("team_id"),
        "{label} plan omitted tenant predicate:\n{plan}"
    );
    assert!(
        plan.contains("Execution Time") || plan.contains("Seq Scan") || plan.contains("Index Scan"),
        "{label} plan has no execution evidence:\n{plan}"
    );
}

#[tokio::test]
#[ignore = "requires CREWSYNC_ENABLE_LARGE_FIXTURE=1 and disposable TEST_DATABASE_URL"]
async fn captures_search_explain_plans_and_bounded_regex_behavior() {
    assert_eq!(
        env::var("CREWSYNC_ENABLE_LARGE_FIXTURE").as_deref(),
        Ok("1")
    );
    let fixture = setup_fixture()
        .await
        .expect("safe disposable PostgreSQL fixture");
    let result = async {
        seed_benchmark_rows(&fixture).await?;
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM tasks WHERE team_id = $1 AND key LIKE $2")
            .bind(fixture.team_id)
            .bind(format!("{}:%", fixture.task_prefix))
            .fetch_one(&fixture.pool)
            .await?;
        assert_eq!(count, LARGE_FIXTURE_SIZE as i64);

        let keyword_plan = explain_analyze(&fixture.pool, fixture.team_id, SearchPlan::Keywords, "deployment").await?;
        let contains_plan = explain_analyze(&fixture.pool, fixture.team_id, SearchPlan::Contains, "deployment").await?;
        let regex_plan = explain_analyze(&fixture.pool, fixture.team_id, SearchPlan::Regex, "^Regex sentinel").await?;
        let nonindexable_plan = explain_nonindexable_regex(&fixture.pool, fixture.team_id, "^(a+)+b$").await?;
        for (label, plan) in [
            ("keywords", &keyword_plan),
            ("contains", &contains_plan),
            ("regex", &regex_plan),
            ("nonindexable regex", &nonindexable_plan),
        ] {
            assert_tenant_and_timing(plan, label);
            println!("\n--- {label} EXPLAIN evidence ---\n{plan}");
        }
        assert!(!nonindexable_plan.contains("tasks_search_trgm_idx"), "pathological regex unexpectedly used trigram index:\n{nonindexable_plan}");

        let elapsed = run_bounded_regex(&fixture.pool, fixture.team_id, "^(a+)+b$".to_owned()).await?;
        assert!(elapsed < OUTER_TIMEOUT, "regex execution exceeded outer bound: {elapsed:?}");
        println!("\n--- regex timeout evidence ---\nstatement_timeout={REGEX_TIMEOUT}, elapsed={elapsed:?}");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    cleanup(fixture).await.expect("cleanup disposable fixture");
    result.expect("search benchmark");
}
