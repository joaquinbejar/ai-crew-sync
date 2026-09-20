//! Moving one conversation's bodies between backends, under supervision.
//!
//! Five steps, and the order is the whole safety argument:
//!
//! 1. **Plan.** Count the messages and bytes, check the target is reachable
//!    and provisioned, and report. Nothing is written, and this is what the
//!    command does unless `--apply` is passed.
//! 2. **Copy.** Every message's body is read from where it is, checksummed,
//!    written to the target under a deterministic idempotency key, and
//!    recorded. The thread keeps working throughout: bodies are still read
//!    from their own recorded backend, which has not changed yet.
//! 3. **Pause and copy the tail.** Writes to this one conversation are
//!    refused, with a message saying why, for as long as the tail takes.
//!    Nothing else on the bus is affected.
//! 4. **Verify.** Every body is read back *from the target* and compared to
//!    the checksum taken from the source. A mismatch fails the move with
//!    the thread untouched.
//! 5. **Cut over.** In one transaction: each verified message's
//!    authoritative backend, then the conversation's routing, then the
//!    pause is lifted.
//!
//! What this never does: invent a receipt, change an author, or delete a
//! source body. Attachments are not involved at all — they hang off channel
//! messages and tasks, they live in Postgres, and no backend move touches
//! them. Cleanup is a
//! separate, explicit operator action after the rollback window, because a
//! rollback that has nothing to roll back to is not a rollback.

use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    error::{BusError, BusResult},
    store::{
        backend::{Envelope, Locator, MessagingBackend, PostgresBackend, Published},
        jetstream::JetStreamBackend,
    },
};

/// Where a move is going.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    ToJetStream,
    ToPostgres,
}

impl Direction {
    pub fn parse(raw: &str) -> BusResult<Self> {
        match raw {
            "jetstream" => Ok(Self::ToJetStream),
            "postgres" => Ok(Self::ToPostgres),
            other => Err(BusError::invalid(format!(
                "unknown backend '{other}'; expected 'jetstream' or 'postgres'"
            ))),
        }
    }
    pub fn target(self) -> &'static str {
        match self {
            Self::ToJetStream => JetStreamBackend::NAME,
            Self::ToPostgres => PostgresBackend::NAME,
        }
    }
    pub fn source(self) -> &'static str {
        match self {
            Self::ToJetStream => PostgresBackend::NAME,
            Self::ToPostgres => JetStreamBackend::NAME,
        }
    }
    fn column(self) -> &'static str {
        match self {
            Self::ToJetStream => "to_jetstream",
            Self::ToPostgres => "to_postgres",
        }
    }
}

/// What one conversation would cost to move, and whether it can be.
#[derive(Clone, Debug, serde::Serialize)]
pub struct Plan {
    pub conversation_id: Uuid,
    pub title: String,
    pub current_backend: String,
    pub messages: i64,
    pub bytes: i64,
    /// Messages already on the target. A resumed or partly-run move.
    pub already_there: i64,
    /// Why this conversation cannot be moved, when it cannot.
    pub blocked: Option<String>,
}

/// Plan a move for every conversation of a team, or for a chosen few.
pub async fn plan(
    pool: &PgPool,
    team_id: Uuid,
    direction: Direction,
    only: &[Uuid],
) -> BusResult<Vec<Plan>> {
    let rows: Vec<(Uuid, String, String, Option<chrono::DateTime<chrono::Utc>>)> = sqlx::query_as(
        "SELECT id, title, backend, write_paused_at FROM conversations
          WHERE team_id = $1 AND ($2::uuid[] = '{}' OR id = ANY($2))
          ORDER BY created_at",
    )
    .bind(team_id)
    .bind(only)
    .fetch_all(pool)
    .await?;

    let mut plans = Vec::with_capacity(rows.len());
    for (id, title, backend, paused) in rows {
        let (messages, bytes, already): (i64, i64, i64) = sqlx::query_as(
            "SELECT count(*),
                    COALESCE(sum(length(body)), 0)::bigint,
                    count(*) FILTER (WHERE backend = $2)
               FROM conversation_messages
              WHERE conversation_id = $1 AND deleted_at IS NULL",
        )
        .bind(id)
        .bind(direction.target())
        .fetch_one(pool)
        .await?;
        let blocked = if backend == direction.target() && already == messages {
            Some(format!("already on {}", direction.target()))
        } else if paused.is_some() {
            Some("a move is already in progress on this thread".to_owned())
        } else {
            None
        };
        plans.push(Plan {
            conversation_id: id,
            title,
            current_backend: backend,
            messages,
            bytes,
            already_there: already,
            blocked,
        });
    }
    Ok(plans)
}

/// A move in progress, and its evidence when it finishes.
#[derive(Clone, Debug, serde::Serialize)]
pub struct Outcome {
    pub migration_id: Uuid,
    pub conversation_id: Uuid,
    pub copied: i64,
    pub verified: i64,
    pub skipped: i64,
    pub bytes: i64,
    pub state: String,
}

fn checksum(body: &str) -> String {
    hex::encode(Sha256::digest(body.as_bytes()))
}

/// Open or resume the run for this conversation and direction. Resuming is
/// the normal case after an interruption: what is already verified is not
/// copied again.
async fn open_run(
    pool: &PgPool,
    team_id: Uuid,
    conversation_id: Uuid,
    direction: Direction,
) -> BusResult<Uuid> {
    let existing: Option<(Uuid,)> = sqlx::query_as(
        "SELECT id FROM conversation_migrations
          WHERE conversation_id = $1 AND direction = $2
            AND state IN ('planned', 'copying', 'verified')
          ORDER BY started_at DESC LIMIT 1",
    )
    .bind(conversation_id)
    .bind(direction.column())
    .fetch_optional(pool)
    .await?;
    if let Some((id,)) = existing {
        return Ok(id);
    }
    let (id,): (Uuid,) = sqlx::query_as(
        "INSERT INTO conversation_migrations (team_id, conversation_id, direction, state)
         VALUES ($1, $2, $3, 'copying') RETURNING id",
    )
    .bind(team_id)
    .bind(conversation_id)
    .bind(direction.column())
    .fetch_one(pool)
    .await?;
    Ok(id)
}

/// Copy, verify and cut over one conversation.
///
/// `pause` is what makes the tail safe: while it is set, sends to this one
/// thread are refused with an explanation. It is lifted by the cutover, and
/// by [`abort`] if anything goes wrong.
pub async fn run(
    pool: &PgPool,
    jetstream: &JetStreamBackend,
    team_id: Uuid,
    conversation_id: Uuid,
    direction: Direction,
) -> BusResult<Outcome> {
    let postgres = PostgresBackend::new(pool.clone());
    let migration_id = open_run(pool, team_id, conversation_id, direction).await?;

    // The pause covers copy and verification. It is one conversation, and a
    // sender is told what is happening rather than seeing a mysterious
    // refusal.
    sqlx::query("UPDATE conversations SET write_paused_at = now() WHERE id = $1")
        .bind(conversation_id)
        .execute(pool)
        .await?;

    let outcome = copy_and_verify(
        pool,
        jetstream,
        &postgres,
        team_id,
        conversation_id,
        direction,
        migration_id,
    )
    .await;

    match outcome {
        Ok(outcome) => Ok(outcome),
        Err(e) => {
            // Nothing was cut over, so nothing is half-moved: the bodies are
            // still authoritative where they were, and the thread reopens.
            abort(pool, migration_id, &e.to_string()).await?;
            Err(e)
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn copy_and_verify(
    pool: &PgPool,
    jetstream: &JetStreamBackend,
    postgres: &PostgresBackend,
    team_id: Uuid,
    conversation_id: Uuid,
    direction: Direction,
    migration_id: Uuid,
) -> BusResult<Outcome> {
    let rows: Vec<(Uuid, String, Option<String>, String)> = sqlx::query_as(
        "SELECT id, body, canonical_locator, backend FROM conversation_messages
          WHERE conversation_id = $1 AND deleted_at IS NULL
            AND tombstoned_at IS NULL
          ORDER BY seq",
    )
    .bind(conversation_id)
    .fetch_all(pool)
    .await?;

    let mut copied = 0;
    let mut skipped = 0;
    let mut bytes = 0i64;
    for (message_id, local_body, locator, backend) in &rows {
        if backend == direction.target() {
            skipped += 1;
            continue;
        }
        let done: Option<(String,)> = sqlx::query_as(
            "SELECT state FROM conversation_migration_items
              WHERE migration_id = $1 AND message_id = $2",
        )
        .bind(migration_id)
        .bind(message_id)
        .fetch_optional(pool)
        .await?;
        if done.as_ref().map(|s| s.0.as_str()) == Some("verified") {
            skipped += 1;
            continue;
        }

        // Read from where this body actually is, not from where the thread
        // is going.
        let body = match direction {
            Direction::ToJetStream => local_body.clone(),
            Direction::ToPostgres => {
                let Some(locator) = locator.clone() else {
                    return Err(BusError::invalid(format!(
                        "message {message_id} has no locator on {}; it cannot be copied back",
                        direction.source()
                    )));
                };
                jetstream
                    .fetch(&Locator(locator), *message_id)
                    .await?
                    .ok_or_else(|| {
                    BusError::not_found(format!(
                        "the broker no longer holds the body of {message_id}. A body that \
                             is gone cannot be copied back; tombstone it deliberately or \
                             restore the stream first."
                    ))
                })?
            }
        };
        let sum = checksum(&body);
        bytes += body.len() as i64;

        sqlx::query(
            "INSERT INTO conversation_migration_items
                (migration_id, message_id, checksum, bytes, state)
             VALUES ($1, $2, $3, $4, 'planned')
             ON CONFLICT (migration_id, message_id)
             DO UPDATE SET checksum = EXCLUDED.checksum, bytes = EXCLUDED.bytes",
        )
        .bind(migration_id)
        .bind(message_id)
        .bind(&sum)
        .bind(body.len() as i64)
        .execute(pool)
        .await?;

        // Write to the target. The idempotency key is derived from the
        // message itself, so a resumed run presents the same key and the
        // broker recognises it instead of storing a second copy.
        let target_locator = match direction {
            Direction::ToJetStream => {
                let envelope = Envelope {
                    message_id: *message_id,
                    conversation_id,
                    team_id,
                    body: body.clone(),
                    publish_key: *message_id,
                };
                match jetstream.publish(envelope).await {
                    Published::Confirmed(Locator(l)) => l,
                    Published::Retryable(why) | Published::Fatal(why) => {
                        return Err(BusError::conflict(format!(
                            "copying {message_id} failed: {why}"
                        )));
                    }
                }
            }
            Direction::ToPostgres => {
                sqlx::query("UPDATE conversation_messages SET body = $2 WHERE id = $1")
                    .bind(message_id)
                    .bind(&body)
                    .execute(pool)
                    .await?;
                message_id.to_string()
            }
        };
        sqlx::query(
            "UPDATE conversation_migration_items
                SET state = 'copied', target_locator = $3
              WHERE migration_id = $1 AND message_id = $2",
        )
        .bind(migration_id)
        .bind(message_id)
        .bind(&target_locator)
        .execute(pool)
        .await?;

        // Read it back from the target and compare. A body that does not
        // come back identical fails the move; nothing is cut over.
        let back = match direction {
            Direction::ToJetStream => {
                jetstream
                    .fetch(&Locator(target_locator.clone()), *message_id)
                    .await?
            }
            Direction::ToPostgres => {
                postgres
                    .fetch(&Locator(target_locator.clone()), *message_id)
                    .await?
            }
        };
        let back = back.ok_or_else(|| {
            BusError::conflict(format!(
                "{message_id} was written to {} and could not be read back",
                direction.target()
            ))
        })?;
        if checksum(&back) != sum {
            sqlx::query(
                "UPDATE conversation_migration_items SET state = 'failed', last_error = $3
                  WHERE migration_id = $1 AND message_id = $2",
            )
            .bind(migration_id)
            .bind(message_id)
            .bind("checksum mismatch")
            .execute(pool)
            .await?;
            return Err(BusError::conflict(format!(
                "the body of {message_id} came back different from {}. Nothing was cut over.",
                direction.target()
            )));
        }
        sqlx::query(
            "UPDATE conversation_migration_items SET state = 'verified'
              WHERE migration_id = $1 AND message_id = $2",
        )
        .bind(migration_id)
        .bind(message_id)
        .execute(pool)
        .await?;
        copied += 1;
    }

    // The cutover. One transaction: the messages' authority, the thread's
    // routing, and the pause.
    let mut tx = pool.begin().await?;
    sqlx::query(
        "UPDATE conversation_messages m
            SET backend = $3,
                canonical_locator = i.target_locator
           FROM conversation_migration_items i
          WHERE i.migration_id = $1 AND i.state = 'verified' AND m.id = i.message_id
            AND m.conversation_id = $2",
    )
    .bind(migration_id)
    .bind(conversation_id)
    .bind(direction.target())
    .execute(&mut *tx)
    .await?;
    // A move to Postgres makes the row the storage again, so the temporary
    // locator has no further meaning; a move to JetStream keeps the body
    // locally until the separate cleanup drops it.
    if direction == Direction::ToPostgres {
        sqlx::query(
            "UPDATE conversation_messages SET canonical_locator = NULL, publication_state = 'stored'
              WHERE conversation_id = $1 AND backend = 'postgres'",
        )
        .bind(conversation_id)
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query(
        "UPDATE conversations
            SET backend = $2,
                publication = CASE WHEN $2 = 'postgres' THEN 'sync' ELSE 'outbox' END,
                write_paused_at = NULL
          WHERE id = $1",
    )
    .bind(conversation_id)
    .bind(direction.target())
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE conversation_migrations
            SET state = 'cut_over', finished_at = now(), messages = $2, bytes = $3
          WHERE id = $1",
    )
    .bind(migration_id)
    .bind(copied)
    .bind(bytes)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    Ok(Outcome {
        migration_id,
        conversation_id,
        copied,
        verified: copied,
        skipped,
        bytes,
        state: "cut_over".to_owned(),
    })
}

/// Give up on a move, lift the pause and leave the thread exactly as it was.
pub async fn abort(pool: &PgPool, migration_id: Uuid, why: &str) -> BusResult<()> {
    let row: Option<(Uuid,)> = sqlx::query_as(
        "UPDATE conversation_migrations SET state = 'failed', finished_at = now(),
                last_error = $2
          WHERE id = $1 RETURNING conversation_id",
    )
    .bind(migration_id)
    .bind(why)
    .fetch_optional(pool)
    .await?;
    if let Some((conversation_id,)) = row {
        sqlx::query("UPDATE conversations SET write_paused_at = NULL WHERE id = $1")
            .bind(conversation_id)
            .execute(pool)
            .await?;
    }
    Ok(())
}

/// Drop source bodies a completed move no longer needs.
///
/// Deliberately separate, deliberately explicit, and deliberately late: a
/// rollback with nothing to roll back to is not a rollback. It refuses to
/// touch anything whose move is not `cut_over`, and anything younger than
/// the rollback window an operator states.
pub async fn cleanup(
    pool: &PgPool,
    team_id: Uuid,
    rollback_window_hours: i64,
    apply: bool,
) -> BusResult<(i64, i64)> {
    let (count, bytes): (i64, i64) = sqlx::query_as(
        "SELECT count(*), COALESCE(sum(length(m.body)), 0)::bigint
           FROM conversation_messages m
           JOIN conversations c ON c.id = m.conversation_id
          WHERE c.team_id = $1 AND m.backend = 'jetstream' AND m.body <> ''
            AND m.canonical_locator IS NOT NULL
            AND EXISTS (
                SELECT 1 FROM conversation_migrations g
                 WHERE g.conversation_id = c.id AND g.state = 'cut_over'
                   AND g.finished_at < now() - make_interval(secs => $2))",
    )
    .bind(team_id)
    .bind((rollback_window_hours * 3600) as f64)
    .fetch_one(pool)
    .await?;
    if !apply {
        return Ok((count, bytes));
    }
    sqlx::query(
        "UPDATE conversation_messages m
            SET body = ''
           FROM conversations c
          WHERE c.id = m.conversation_id AND c.team_id = $1
            AND m.backend = 'jetstream' AND m.body <> ''
            AND m.canonical_locator IS NOT NULL
            AND EXISTS (
                SELECT 1 FROM conversation_migrations g
                 WHERE g.conversation_id = c.id AND g.state = 'cut_over'
                   AND g.finished_at < now() - make_interval(secs => $2))",
    )
    .bind(team_id)
    .bind((rollback_window_hours * 3600) as f64)
    .execute(pool)
    .await?;
    Ok((count, bytes))
}
