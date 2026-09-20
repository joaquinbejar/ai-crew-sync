//! The outbox: accept now, publish later, and be honest in between.
//!
//! Leased like every other unit of work on this bus, fenced by a generation
//! so a worker that comes back after its lease expired writes nothing, and
//! bounded in attempts and payload size. The three states a slot can reach
//! are the three a caller can be told about: pending, stored, failed.
//!
//! Network-like work happens outside the database transactions. A worker
//! opens one short transaction to lease, does the publish with nothing held,
//! then opens another to settle. A publish that takes a minute holds no lock
//! for a minute.

use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    error::BusResult,
    store::backend::{Envelope, Locator, MessagingBackend, Published},
};

/// How long a worker holds a slot before it is fair game again.
pub const LEASE_SECS: i64 = 60;
/// Attempts before a slot is marked failed rather than retried forever.
pub const MAX_ATTEMPTS: i32 = 8;
/// Largest payload the outbox will hold. Bodies are already capped at 1 MiB;
/// this is the ceiling on what one pending slot can cost.
pub const MAX_PAYLOAD_BYTES: i32 = 1024 * 1024;

/// A slot a worker holds.
#[derive(Clone, Debug)]
pub struct Lease {
    pub message_id: Uuid,
    pub conversation_id: Uuid,
    pub team_id: Uuid,
    /// The backend this slot was enqueued for. A worker for another one
    /// must not publish it.
    pub backend: String,
    pub payload: String,
    pub publish_key: Uuid,
    /// The generation this lease was taken at. A settle that does not match
    /// is a worker whose lease expired, and it changes nothing.
    pub generation: i64,
    pub attempts: i32,
}

/// Queue a message for publication. Called inside the sending transaction,
/// so acceptance and the outbox slot commit together: there can be no
/// accepted message without a slot, and no slot without a message.
pub async fn enqueue(
    tx: &mut sqlx::PgConnection,
    message_id: Uuid,
    conversation_id: Uuid,
    team_id: Uuid,
    backend: &str,
    payload: &str,
) -> BusResult<Uuid> {
    let bytes = payload.len().min(i32::MAX as usize) as i32;
    if bytes > MAX_PAYLOAD_BYTES {
        return Err(crate::error::BusError::invalid(format!(
            "message body is {bytes} bytes; the outbox holds at most {MAX_PAYLOAD_BYTES}"
        )));
    }
    let publish_key = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO conversation_outbox
            (message_id, conversation_id, team_id, backend, payload, payload_bytes, publish_key)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (message_id) DO NOTHING",
    )
    .bind(message_id)
    .bind(conversation_id)
    .bind(team_id)
    .bind(backend)
    .bind(payload)
    .bind(bytes)
    .bind(publish_key)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE conversation_messages SET publication_state = 'pending_publication'
          WHERE id = $1",
    )
    .bind(message_id)
    .execute(tx)
    .await?;
    Ok(publish_key)
}

/// Take the next due slot, if there is one. `FOR UPDATE SKIP LOCKED`, so N
/// workers never take the same one.
pub async fn lease(pool: &PgPool, worker: &str) -> BusResult<Option<Lease>> {
    #[allow(clippy::type_complexity)]
    let row: Option<(Uuid, Uuid, Uuid, String, String, Uuid, i64, i32)> = sqlx::query_as(
        "UPDATE conversation_outbox o
            SET state = 'leased',
                leased_by = $1,
                lease_expires_at = now() + make_interval(secs => $2),
                generation = o.generation + 1,
                attempts = o.attempts + 1,
                updated_at = now()
          WHERE o.message_id = (
              SELECT message_id FROM conversation_outbox
               WHERE state IN ('pending', 'leased')
                 AND next_attempt_at <= now()
                 AND (lease_expires_at IS NULL OR lease_expires_at < now())
               ORDER BY next_attempt_at, created_at
               FOR UPDATE SKIP LOCKED
               LIMIT 1)
          RETURNING o.message_id, o.conversation_id, o.team_id, o.backend, o.payload,
                    o.publish_key, o.generation, o.attempts",
    )
    .bind(worker)
    .bind(LEASE_SECS as f64)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(
        |(
            message_id,
            conversation_id,
            team_id,
            backend,
            payload,
            publish_key,
            generation,
            attempts,
        )| {
            Lease {
                message_id,
                conversation_id,
                team_id,
                backend,
                payload,
                publish_key,
                generation,
                attempts,
            }
        },
    ))
}

/// Take one specific slot, for reconciliation. The ordinary `lease` picks
/// what is due; this one is told which.
async fn lease_one(pool: &PgPool, worker: &str, message_id: Uuid) -> BusResult<Option<Lease>> {
    #[allow(clippy::type_complexity)]
    let row: Option<(Uuid, Uuid, Uuid, String, String, Uuid, i64, i32)> = sqlx::query_as(
        "UPDATE conversation_outbox o
            SET state = 'leased',
                leased_by = $1,
                lease_expires_at = now() + make_interval(secs => $3),
                generation = o.generation + 1,
                updated_at = now()
          WHERE o.message_id = $2
            AND (o.lease_expires_at IS NULL OR o.lease_expires_at < now())
          RETURNING o.message_id, o.conversation_id, o.team_id, o.backend, o.payload,
                    o.publish_key, o.generation, o.attempts",
    )
    .bind(worker)
    .bind(message_id)
    .bind(LEASE_SECS as f64)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(
        |(
            message_id,
            conversation_id,
            team_id,
            backend,
            payload,
            publish_key,
            generation,
            attempts,
        )| {
            Lease {
                message_id,
                conversation_id,
                team_id,
                backend,
                payload,
                publish_key,
                generation,
                attempts,
            }
        },
    ))
}

/// What settling a lease did. Reported so a worker (and a test) can see
/// whether it was still the holder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Settled {
    /// Confirmed and recorded; the slot is gone and the message is stored.
    Stored,
    /// Not this time; the slot is due again after a backoff.
    Retrying { attempts: i32 },
    /// It will not be published. The slot stays, visible and explicit.
    Failed,
    /// This worker's lease had expired and someone else holds the slot.
    /// Nothing was written: that is the fence doing its job.
    Fenced,
}

/// Record the outcome of one attempt, fenced by the generation the lease was
/// taken at.
pub async fn settle(pool: &PgPool, lease: &Lease, outcome: Published) -> BusResult<Settled> {
    match outcome {
        Published::Confirmed(Locator(locator)) => {
            let mut tx = pool.begin().await?;
            let deleted: Option<(Uuid,)> = sqlx::query_as(
                // Generation *and* an unexpired lease. A worker whose lease
                // ran out no longer owns this slot, whether or not anyone
                // else has picked it up yet, and settling anyway is exactly
                // the write the fence exists to refuse.
                "DELETE FROM conversation_outbox
                  WHERE message_id = $1 AND generation = $2
                    AND lease_expires_at IS NOT NULL AND lease_expires_at > now()
                  RETURNING message_id",
            )
            .bind(lease.message_id)
            .bind(lease.generation)
            .fetch_optional(&mut *tx)
            .await?;
            if deleted.is_none() {
                return Ok(Settled::Fenced);
            }
            sqlx::query(
                "UPDATE conversation_messages
                    SET publication_state = 'stored', canonical_locator = $2
                  WHERE id = $1",
            )
            .bind(lease.message_id)
            .bind(&locator)
            .execute(&mut *tx)
            .await?;
            // The receipts were written at acceptance with `stored_at` null
            // on this path; the body is only now durable, so this is when
            // storage actually happened.
            sqlx::query(
                "UPDATE message_receipts SET stored_at = COALESCE(stored_at, now())
                  WHERE message_id = $1",
            )
            .bind(lease.message_id)
            .execute(&mut *tx)
            .await?;
            // The body is canonical now, so the recipients can be told it
            // exists. Queued on this transaction: there is no stored
            // message without its references, and no reference to a message
            // nobody stored.
            crate::store::inbox::enqueue_message_references(&mut tx, lease.message_id).await?;
            tx.commit().await?;
            Ok(Settled::Stored)
        }
        Published::Retryable(why) => {
            // Exponential-ish, bounded: a slot that keeps failing backs off
            // rather than spinning, and gives up rather than retrying for
            // ever.
            let give_up = lease.attempts >= MAX_ATTEMPTS;
            let backoff = (1_i64 << lease.attempts.min(6)) as f64;
            // The slot and the message's state move together. Two
            // statements and a crash in between leaves a slot marked failed
            // and a message still saying pending, which is a gap nobody can
            // resolve.
            let mut tx = pool.begin().await?;
            let updated: Option<(Uuid,)> = sqlx::query_as(
                "UPDATE conversation_outbox
                    SET state = CASE WHEN $4 THEN 'failed' ELSE 'pending' END,
                        leased_by = NULL,
                        lease_expires_at = NULL,
                        next_attempt_at = now() + make_interval(secs => $3),
                        last_error = $5,
                        updated_at = now()
                  WHERE message_id = $1 AND generation = $2
                    AND lease_expires_at IS NOT NULL AND lease_expires_at > now()
                  RETURNING message_id",
            )
            .bind(lease.message_id)
            .bind(lease.generation)
            .bind(backoff)
            .bind(give_up)
            .bind(&why)
            .fetch_optional(&mut *tx)
            .await?;
            if updated.is_none() {
                return Ok(Settled::Fenced);
            }
            if give_up {
                mark_failed(&mut tx, lease.message_id).await?;
                tx.commit().await?;
                Ok(Settled::Failed)
            } else {
                tx.commit().await?;
                Ok(Settled::Retrying {
                    attempts: lease.attempts,
                })
            }
        }
        Published::Fatal(why) => {
            let mut tx = pool.begin().await?;
            let updated: Option<(Uuid,)> = sqlx::query_as(
                "UPDATE conversation_outbox
                    SET state = 'failed', leased_by = NULL, lease_expires_at = NULL,
                        last_error = $3, updated_at = now()
                  WHERE message_id = $1 AND generation = $2
                    AND lease_expires_at IS NOT NULL AND lease_expires_at > now()
                  RETURNING message_id",
            )
            .bind(lease.message_id)
            .bind(lease.generation)
            .bind(&why)
            .fetch_optional(&mut *tx)
            .await?;
            if updated.is_none() {
                return Ok(Settled::Fenced);
            }
            mark_failed(&mut tx, lease.message_id).await?;
            tx.commit().await?;
            Ok(Settled::Failed)
        }
    }
}

async fn mark_failed(conn: &mut sqlx::PgConnection, message_id: Uuid) -> BusResult<()> {
    sqlx::query("UPDATE conversation_messages SET publication_state = 'failed' WHERE id = $1")
        .bind(message_id)
        .execute(conn)
        .await?;
    Ok(())
}

/// Ask the backend what it actually holds for a slot whose publish died
/// without an answer, and settle accordingly. This is what turns "we do not
/// know" into one of the three states a caller can be told.
pub async fn reconcile<B: MessagingBackend>(
    pool: &PgPool,
    backend: &B,
    lease: &Lease,
) -> BusResult<Settled> {
    match backend.reconcile(&envelope_of(lease)).await? {
        Some(locator) => settle(pool, lease, Published::Confirmed(locator)).await,
        None => {
            settle(
                pool,
                lease,
                Published::Retryable("the backend holds nothing for this key".into()),
            )
            .await
        }
    }
}

/// Publish one leased slot and record what happened.
///
/// Nothing is held while this runs: a publish that takes a minute costs a
/// lease, not a lock.
pub async fn publish_leased<B: MessagingBackend>(
    pool: &PgPool,
    backend: &B,
    lease: &Lease,
) -> BusResult<Settled> {
    let outcome = backend.publish(envelope_of(lease)).await;
    settle(pool, lease, outcome).await
}

fn envelope_of(lease: &Lease) -> Envelope {
    Envelope {
        message_id: lease.message_id,
        conversation_id: lease.conversation_id,
        team_id: lease.team_id,
        body: lease.payload.clone(),
        publish_key: lease.publish_key,
    }
}

/// One worker pass: lease, publish outside any transaction, settle.
pub async fn run_once<B: MessagingBackend>(
    pool: &PgPool,
    backend: &B,
    worker: &str,
) -> BusResult<Option<Settled>> {
    let Some(lease) = lease(pool, worker).await? else {
        return Ok(None);
    };
    // The slot records which backend it was enqueued for. A worker holding
    // a different one would publish the body somewhere nobody is looking
    // and settle it with a locator the reader cannot use.
    if lease.backend != backend.name() {
        let backend_name = backend.name();
        tracing::warn!(
            slot = %lease.message_id,
            wanted = %lease.backend,
            worker = %backend_name,
            "a worker leased a slot for another backend; releasing it"
        );
        release(pool, &lease, "leased by a worker for another backend").await?;
        return Ok(Some(Settled::Fenced));
    }
    Ok(Some(publish_leased(pool, backend, &lease).await?))
}

/// Put a leased slot back without counting it as an attempt.
async fn release(pool: &PgPool, lease: &Lease, why: &str) -> BusResult<()> {
    sqlx::query(
        "UPDATE conversation_outbox
            SET state = 'pending', leased_by = NULL, lease_expires_at = NULL,
                attempts = GREATEST(attempts - 1, 0), last_error = $3, updated_at = now()
          WHERE message_id = $1 AND generation = $2",
    )
    .bind(lease.message_id)
    .bind(lease.generation)
    .bind(why)
    .execute(pool)
    .await?;
    Ok(())
}

/// What is outstanding, for an operator and for the pause/drain controls.
#[derive(Debug, Default, serde::Serialize)]
pub struct Status {
    pub pending: i64,
    pub leased: i64,
    pub failed: i64,
    pub oldest_pending_seconds: Option<i64>,
    pub pending_bytes: i64,
}

pub async fn status(pool: &PgPool, team_id: Uuid) -> BusResult<Status> {
    // extract(epoch …) is NUMERIC in Postgres; cast it rather than decoding
    // a numeric as a float and failing at runtime.
    let row: (i64, i64, i64, Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT
            count(*) FILTER (WHERE state = 'pending'),
            count(*) FILTER (WHERE state = 'leased'),
            count(*) FILTER (WHERE state = 'failed'),
            extract(epoch FROM now() - min(created_at) FILTER (WHERE state <> 'failed'))::bigint,
            sum(payload_bytes) FILTER (WHERE state <> 'failed')::bigint
           FROM conversation_outbox WHERE team_id = $1",
    )
    .bind(team_id)
    .fetch_one(pool)
    .await?;
    Ok(Status {
        pending: row.0,
        leased: row.1,
        failed: row.2,
        oldest_pending_seconds: row.3,
        pending_bytes: row.4.unwrap_or(0),
    })
}

/// Turn asynchronous publication off for a conversation.
///
/// Refused while anything is still pending: returning to the synchronous
/// path with work in flight would leave messages nobody drains, and a caller
/// reading the thread would see a gap that never closes. Drain first, which
/// `status` is how you watch.
pub async fn set_publication(pool: &PgPool, conversation_id: Uuid, outbox: bool) -> BusResult<()> {
    // The conversation row is what a send locks to take its sequence, so
    // taking it here is what stops a send from enqueueing work for a mode
    // that has just been switched off.
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT id FROM conversations WHERE id = $1 FOR UPDATE")
        .bind(conversation_id)
        .fetch_one(&mut *tx)
        .await?;
    if !outbox {
        let (left,): (i64,) = sqlx::query_as(
            "SELECT count(*) FROM conversation_outbox
              WHERE conversation_id = $1 AND state <> 'failed'",
        )
        .bind(conversation_id)
        .fetch_one(&mut *tx)
        .await?;
        if left > 0 {
            return Err(crate::error::BusError::conflict(format!(
                "{left} message(s) of this conversation are still awaiting publication. \
                 Let them settle before returning to synchronous mode, or the thread keeps \
                 a gap nobody will close."
            )));
        }
    }
    sqlx::query("UPDATE conversations SET publication = $2 WHERE id = $1")
        .bind(conversation_id)
        .bind(if outbox { "outbox" } else { "sync" })
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

// ---------------------------------------------- publication, and honesty --

/// Settle a lease whose publish ended without an answer.
///
/// This is the state a two-system write introduces and a one-system write
/// never has: the message is neither stored nor failed, and claiming either
/// would be a guess. The row is marked uncertain, the slot stays, and
/// [`resolve_uncertain`] asks the backend what actually happened.
pub async fn mark_uncertain(pool: &PgPool, lease: &Lease, why: &str) -> BusResult<Settled> {
    let mut tx = pool.begin().await?;
    let held: Option<(Uuid,)> = sqlx::query_as(
        "UPDATE conversation_outbox
            SET state = 'pending', leased_by = NULL, lease_expires_at = NULL,
                next_attempt_at = now() + interval '5 seconds',
                last_error = $3, updated_at = now()
          WHERE message_id = $1 AND generation = $2
          RETURNING message_id",
    )
    .bind(lease.message_id)
    .bind(lease.generation)
    .bind(why)
    .fetch_optional(&mut *tx)
    .await?;
    if held.is_none() {
        return Ok(Settled::Fenced);
    }
    sqlx::query(
        "UPDATE conversation_messages SET uncertain_at = COALESCE(uncertain_at, now())
          WHERE id = $1",
    )
    .bind(lease.message_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Settled::Retrying {
        attempts: lease.attempts,
    })
}

/// Ask the backend what it holds for every uncertain slot of a team and
/// settle each one. Run on reconnect and on a timer; it is the only thing
/// that turns "we do not know" into a fact.
pub async fn resolve_uncertain<B: MessagingBackend>(
    pool: &PgPool,
    backend: &B,
    team_id: Uuid,
) -> BusResult<usize> {
    let rows: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT o.message_id, o.backend
           FROM conversation_outbox o
           JOIN conversation_messages m ON m.id = o.message_id
          WHERE o.team_id = $1 AND m.uncertain_at IS NOT NULL",
    )
    .bind(team_id)
    .fetch_all(pool)
    .await?;
    let mut resolved = 0;
    for (message_id, slot_backend) in rows {
        // Same rule as the drain: only the backend this slot was enqueued
        // for may answer for it.
        if slot_backend != backend.name() {
            continue;
        }
        // Take the slot properly. Settling is fenced on holding an
        // unexpired lease, and an uncertain slot was released when its
        // outcome became unknown; reconciling without leasing it would
        // write nothing and then report success.
        let Some(lease) = lease_one(pool, "reconciler", message_id).await? else {
            continue;
        };
        if let Some(locator) = backend.reconcile(&envelope_of(&lease)).await?
            && settle(
                pool,
                &lease,
                crate::store::backend::Published::Confirmed(locator),
            )
            .await?
                == Settled::Stored
        {
            resolved += 1;
        }
        // Nothing found: the write did not land, so the slot stays pending
        // and the normal retry path takes it. Still uncertain until then,
        // and still reported as such.
    }
    sqlx::query(
        "UPDATE conversation_messages m SET uncertain_at = NULL
           FROM conversations c
          WHERE c.id = m.conversation_id AND c.team_id = $1
            AND m.uncertain_at IS NOT NULL AND m.publication_state = 'stored'",
    )
    .bind(team_id)
    .execute(pool)
    .await?;
    Ok(resolved)
}

/// Drop the temporary body once its backend holds it. Until this runs, the
/// body lives in both places on purpose: losing it to a failed publish would
/// be worse than storing it twice for a moment.
///
/// Only touches messages whose backend is not Postgres — there, the row *is*
/// the storage, and clearing it would delete the history.
/// `conversation` narrows it to one thread; `None` sweeps everything the
/// installation holds. `min_age_secs` is the grace period: a reader that
/// arrives right after the PubAck still gets the local copy rather than a
/// round trip to the broker.
pub async fn release_published_bodies(
    pool: &PgPool,
    conversation: Option<Uuid>,
    min_age_secs: i64,
) -> BusResult<u64> {
    let done = sqlx::query(
        "UPDATE conversation_messages m
            SET body = ''
           FROM conversations c
          WHERE ($1::uuid IS NULL OR m.conversation_id = $1)
            AND c.id = m.conversation_id
            AND c.backend <> 'postgres'
            AND m.publication_state = 'stored'
            AND m.canonical_locator IS NOT NULL
            AND m.body <> ''
            AND m.created_at <= now() - make_interval(secs => $2)
            -- Never a body a supervised move copied. That one is the
            -- rollback source, and it is released by `conversations
            -- cleanup` with the window an operator states, not by a sweep
            -- five minutes later.
            AND NOT EXISTS (
                SELECT 1 FROM conversation_migration_items i
                  JOIN conversation_migrations g ON g.id = i.migration_id
                 WHERE i.message_id = m.id AND g.state = 'cut_over')",
    )
    .bind(conversation)
    .bind(min_age_secs as f64)
    .execute(pool)
    .await?;
    Ok(done.rows_affected())
}

/// Record that a body is no longer retrievable from its backend. The
/// message, its sequence, its recipients and its receipts all stay: a gap
/// that is explained is not the same as a gap.
pub async fn tombstone(pool: &PgPool, message_id: Uuid, reason: &str) -> BusResult<()> {
    sqlx::query(
        "UPDATE conversation_messages
            SET tombstoned_at = COALESCE(tombstoned_at, now()), tombstone_reason = $2
          WHERE id = $1",
    )
    .bind(message_id)
    .bind(reason)
    .execute(pool)
    .await?;
    Ok(())
}
