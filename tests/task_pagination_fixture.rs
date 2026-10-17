//! Safety-gated Postgres fixtures for pagination/concurrency integration tests.
use sqlx::{
    PgPool, Postgres, Transaction,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{env, str::FromStr};
use uuid::Uuid;

pub const LARGE_FIXTURE_SIZE: usize = 2_000;
const ENABLE_LARGE_FIXTURE: &str = "CREWSYNC_ENABLE_LARGE_FIXTURE";
const TEST_DATABASE_URL: &str = "TEST_DATABASE_URL";
const PRODUCTION_DATABASE_URL: &str = "DATABASE_URL";

#[derive(Debug)]
pub struct Fixture {
    pub pool: PgPool,
    pub team_id: Uuid,
    pub agent_id: Uuid,
    pub other_agent_id: Uuid,
    pub task_prefix: String,
}
#[derive(Debug, PartialEq, Eq)]
struct DbTarget {
    host: String,
    port: u16,
    database: String,
}

pub fn large_fixture_enabled() -> bool {
    env::var(ENABLE_LARGE_FIXTURE).as_deref() == Ok("1")
}

fn canonical_target(options: &PgConnectOptions) -> Result<DbTarget, String> {
    let raw_host = options
        .get_host()
        .trim_end_matches('.')
        .to_ascii_lowercase();
    let host = if matches!(raw_host.as_str(), "localhost" | "127.0.0.1" | "::1") {
        "loopback".to_owned()
    } else {
        raw_host
    };
    let database = options
        .get_database()
        .unwrap_or_default()
        .to_ascii_lowercase();
    if database.is_empty() {
        return Err("database name must not be empty".to_owned());
    }
    Ok(DbTarget {
        host,
        port: options.get_port(),
        database,
    })
}

fn parse_target(url: &str, label: &str) -> Result<(PgConnectOptions, DbTarget), String> {
    let options =
        PgConnectOptions::from_str(url).map_err(|error| format!("invalid {label}: {error}"))?;
    let target = canonical_target(&options).map_err(|error| format!("invalid {label}: {error}"))?;
    Ok((options, target))
}

/// Parse authority fields structurally. Query text cannot spoof the host, and
/// production comparison uses canonical host/port/database, not raw strings.
pub fn validate_safe_test_url(
    test_url: &str,
    production_url: Option<&str>,
) -> Result<String, String> {
    if test_url.trim().is_empty() {
        return Err(format!("{TEST_DATABASE_URL} must not be empty"));
    }
    let (options, test_target) = parse_target(test_url, TEST_DATABASE_URL)?;
    let host = options
        .get_host()
        .trim_end_matches('.')
        .to_ascii_lowercase();
    if !matches!(host.as_str(), "localhost" | "127.0.0.1" | "::1") {
        return Err(format!("{TEST_DATABASE_URL} must use exact loopback host"));
    }
    if !(test_target.database.contains("test") || test_target.database.contains("fixture"))
        || test_target.database.contains("prod")
        || test_target.database.contains("live")
    {
        return Err(format!(
            "{TEST_DATABASE_URL} must name a nonproduction test/fixture database"
        ));
    }
    if let Some(production_url) = production_url {
        let (_, production_target) = parse_target(production_url, PRODUCTION_DATABASE_URL)?;
        if test_target == production_target {
            return Err(format!(
                "{TEST_DATABASE_URL} resolves to the configured {PRODUCTION_DATABASE_URL} target"
            ));
        }
    }
    Ok(test_url.to_owned())
}

pub fn require_safe_database_url() -> Result<String, String> {
    if !large_fixture_enabled() {
        return Err(format!(
            "large fixture disabled; set {ENABLE_LARGE_FIXTURE}=1 to opt in"
        ));
    }
    let test_url = env::var(TEST_DATABASE_URL)
        .map_err(|_| format!("{TEST_DATABASE_URL} is required for the fixture"))?;
    let production_url = env::var(PRODUCTION_DATABASE_URL).ok();
    validate_safe_test_url(&test_url, production_url.as_deref())
}

pub async fn connect_test_database() -> Result<PgPool, Box<dyn std::error::Error>> {
    let url = require_safe_database_url().map_err(std::io::Error::other)?;
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await?;
    ai_crew_sync::MIGRATOR.run(&pool).await?;
    Ok(pool)
}

pub async fn setup_fixture() -> Result<Fixture, Box<dyn std::error::Error>> {
    let pool = connect_test_database().await?;
    let suffix = Uuid::new_v4().simple().to_string();
    let team_slug = format!("pagination-fixture-{suffix}");
    let task_prefix = format!("pagination-fixture-{suffix}");
    let mut tx = pool.begin().await?;
    let team_id: Uuid =
        sqlx::query_scalar("INSERT INTO teams (slug, name) VALUES ($1, $2) RETURNING id")
            .bind(&team_slug)
            .bind("pagination fixture")
            .fetch_one(&mut *tx)
            .await?;
    let agent_id: Uuid =
        sqlx::query_scalar("INSERT INTO agents (team_id, name) VALUES ($1, $2) RETURNING id")
            .bind(team_id)
            .bind("fixture-primary")
            .fetch_one(&mut *tx)
            .await?;
    let other_agent_id: Uuid =
        sqlx::query_scalar("INSERT INTO agents (team_id, name) VALUES ($1, $2) RETURNING id")
            .bind(team_id)
            .bind("fixture-secondary")
            .fetch_one(&mut *tx)
            .await?;
    tx.commit().await?;
    Ok(Fixture {
        pool,
        team_id,
        agent_id,
        other_agent_id,
        task_prefix,
    })
}

pub async fn generate_2000_tasks(
    tx: &mut Transaction<'_, Postgres>,
    team_id: Uuid,
    agent_id: Uuid,
    prefix: &str,
) -> Result<Vec<String>, sqlx::Error> {
    let mut keys = Vec::with_capacity(LARGE_FIXTURE_SIZE);
    for ordinal in 0..LARGE_FIXTURE_SIZE {
        let key = format!("{prefix}:{ordinal:04}");
        sqlx::query("INSERT INTO tasks (team_id, key, title, metadata, created_by) VALUES ($1, $2, $3, jsonb_build_object('fixture_ordinal', $4), $5)").bind(team_id).bind(&key).bind(format!("pagination fixture task {ordinal:04}")).bind(ordinal as i32).bind(agent_id).execute(&mut **tx).await?;
        keys.push(key);
    }
    Ok(keys)
}

pub async fn populate_2000_tasks(fixture: &Fixture) -> Result<Vec<String>, sqlx::Error> {
    let mut tx = fixture.pool.begin().await?;
    let keys = generate_2000_tasks(
        &mut tx,
        fixture.team_id,
        fixture.agent_id,
        &fixture.task_prefix,
    )
    .await?;
    tx.commit().await?;
    Ok(keys)
}

pub async fn seed_expiring_lease(fixture: &Fixture, key: &str) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO tasks (team_id, key, title, status, claimed_by, claimed_at, lease_expires_at, created_by) VALUES ($1, $2, $3, 'claimed', $4, now(), now() - interval '1 second', $4)").bind(fixture.team_id).bind(key).bind("expired lease fixture task").bind(fixture.agent_id).execute(&fixture.pool).await?;
    Ok(())
}

pub async fn cleanup(fixture: Fixture) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM teams WHERE id = $1")
        .bind(fixture.team_id)
        .execute(&fixture.pool)
        .await?;
    fixture.pool.close().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn large_fixture_is_disabled_by_default_without_opt_in() {
        if env::var(ENABLE_LARGE_FIXTURE).is_err() {
            assert!(!large_fixture_enabled());
        }
    }
    #[test]
    fn fixture_size_is_exactly_two_thousand() {
        assert_eq!(LARGE_FIXTURE_SIZE, 2_000);
    }
    #[test]
    fn remote_authority_cannot_be_spoofed_by_query_text() {
        assert!(
            validate_safe_test_url(
                "postgres://tester@db.example.org:5432/live?note=@localhost:5432",
                None
            )
            .is_err()
        );
    }
    #[test]
    fn malformed_url_is_rejected_before_connection() {
        assert!(validate_safe_test_url("postgres://[malformed", None).is_err());
    }
    #[test]
    fn canonical_same_target_rejects_alternate_credentials() {
        assert!(
            validate_safe_test_url(
                "postgres://tester@127.0.0.1:5432/crewsync_test",
                Some("postgres://production@localhost:5432/crewsync_test")
            )
            .is_err()
        );
    }
    #[test]
    fn canonical_same_target_rejects_omitted_default_port() {
        assert!(
            validate_safe_test_url(
                "postgres://tester@127.0.0.1:5432/crewsync_test",
                Some("postgres://production@localhost/crewsync_test")
            )
            .is_err()
        );
    }
    #[test]
    fn malformed_production_url_fails_closed() {
        assert!(
            validate_safe_test_url(
                "postgres://tester@127.0.0.1:5432/crewsync_test",
                Some("postgres://[malformed")
            )
            .is_err()
        );
    }
    #[test]
    fn exact_loopback_and_nonproduction_database_are_required() {
        assert!(
            validate_safe_test_url("postgres://tester@127.0.0.1:5432/crewsync_test", None).is_ok()
        );
        assert!(
            validate_safe_test_url("postgres://tester@localhost:5432/crewsync_fixture", None)
                .is_ok()
        );
        assert!(validate_safe_test_url("postgres://tester@localhost:5432/live", None).is_err());
    }
}

#[tokio::test]
#[ignore = "requires CREWSYNC_ENABLE_LARGE_FIXTURE=1 and disposable TEST_DATABASE_URL"]
async fn generates_exactly_2000_isolated_tasks() {
    let fixture = setup_fixture().await.expect("safe test database");
    let keys = populate_2000_tasks(&fixture)
        .await
        .expect("populate fixture");
    assert_eq!(keys.len(), LARGE_FIXTURE_SIZE);
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM tasks WHERE team_id = $1 AND key LIKE $2")
            .bind(fixture.team_id)
            .bind(format!("{}:%", fixture.task_prefix))
            .fetch_one(&fixture.pool)
            .await
            .expect("count fixture tasks");
    assert_eq!(count, LARGE_FIXTURE_SIZE as i64);
    cleanup(fixture).await.expect("cleanup fixture");
}
