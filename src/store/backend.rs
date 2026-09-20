//! The messaging backend boundary, and the outbox that exercises it.
//!
//! Phase 2's path is synchronous: body, recipients and receipts commit in one
//! Postgres transaction, so `stored` is true because that transaction
//! committed and there is nothing to reconcile. That remains the default and
//! is not slowed down by anything here.
//!
//! What that path cannot exercise is the shape every external store
//! introduces: acceptance and persistence are two events, and the second can
//! fail, time out, or succeed without the caller learning it did. This module
//! adds that shape with **Postgres as the only implementation**, so the
//! failure handling can be built and tested before there is a broker to
//! blame for it.
//!
//! What the outbox deliberately does *not* do:
//!
//! * it never reports `stored` on acceptance. A message that is accepted and
//!   not yet confirmed says `pending_publication`, which is a different
//!   thing, and a reader sees the gap rather than a lie;
//! * it never skips a pending earlier message when reporting a thread's
//!   state, so a cursor cannot walk past something still in flight;
//! * it never replays a side effect. A retry presents the same `publish_key`
//!   and the adapter is expected to recognise it; a duplicate physical write
//!   resolves to one canonical locator rather than two messages.

use std::time::Duration;

use sqlx::PgPool;
use uuid::Uuid;

use crate::error::{BusError, BusResult};

/// Where a confirmed body lives. Opaque to everything but the adapter that
/// produced it; Postgres uses the message id itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Locator(pub String);

/// What an adapter reports after trying to publish.
#[derive(Clone, Debug)]
pub enum Published {
    /// The backend confirmed it, and this is where it is.
    Confirmed(Locator),
    /// The attempt failed and may succeed later: a timeout, a refused
    /// connection, a full queue. The slot stays and is retried.
    Retryable(String),
    /// It will never succeed: a payload the backend cannot accept, an
    /// authorization failure. The slot is marked failed and stays visible.
    Fatal(String),
}

/// One publication attempt's input.
#[derive(Clone, Debug)]
pub struct Envelope {
    pub message_id: Uuid,
    pub conversation_id: Uuid,
    pub team_id: Uuid,
    pub body: String,
    /// Presented to the backend on every attempt, so an uncertain completion
    /// can be recognised instead of duplicated.
    pub publish_key: Uuid,
}

/// The boundary. One implementation today; a broker adapter slots in behind
/// the same four operations without touching the callers.
///
/// Network-like work happens **here**, outside any database transaction: the
/// outbox opens short transactions to lease and to settle, and never holds
/// one across a publish.
pub trait MessagingBackend: Send + Sync {
    /// Name recorded on the conversation and the outbox row.
    fn name(&self) -> &'static str;

    /// Persist a body. Must be idempotent on `publish_key`: presented the
    /// same key twice, it returns the same locator rather than storing twice.
    fn publish(&self, envelope: Envelope) -> impl std::future::Future<Output = Published> + Send;

    /// Read a body back by locator, for history.
    fn fetch(
        &self,
        locator: &Locator,
    ) -> impl std::future::Future<Output = BusResult<Option<String>>> + Send;

    /// Drop a body the retention policy no longer keeps. Returns how many
    /// were removed.
    fn retain(
        &self,
        before: chrono::DateTime<chrono::Utc>,
    ) -> impl std::future::Future<Output = BusResult<u64>> + Send;

    /// Ask the backend what it actually holds for a key, for the uncertain
    /// case: the publish call died without an answer. `None` means it has
    /// nothing, so a retry is safe.
    fn reconcile(
        &self,
        publish_key: Uuid,
    ) -> impl std::future::Future<Output = BusResult<Option<Locator>>> + Send;
}

/// The Postgres implementation: the body is already in
/// `conversation_messages`, so publishing is confirming what a row holds and
/// the locator is the message id. Trivial on purpose — the point of this
/// phase is the *handling*, not the storage.
#[derive(Clone)]
pub struct PostgresBackend {
    pool: PgPool,
    /// Fault injection for the tests. Production constructs `new`, which
    /// leaves every fault off.
    faults: Faults,
    /// Retryable failures still owed, counted down as they are served. In
    /// an `Arc` because the backend is cloned and the count is one budget,
    /// not one per clone.
    retryable_left: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

/// What to make go wrong, and how often. Off in production by construction.
#[derive(Clone, Debug, Default)]
pub struct Faults {
    /// Fail the next N publishes with a retryable error, and then stop. A
    /// fault that never runs out is a different test — it models a backend
    /// that is down, not a transient failure — and the two must not be the
    /// same knob.
    pub retryable: usize,
    /// Fail the next publish fatally.
    pub fatal: bool,
    /// Write the body, then report a failure — the uncertain completion a
    /// reconcile has to resolve.
    pub lose_confirmation: bool,
    /// Pause inside publish, to widen the window a lease can expire in.
    pub delay: Option<Duration>,
}

impl PostgresBackend {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            faults: Faults::default(),
            retryable_left: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// Only the tests construct this.
    pub fn with_faults(pool: PgPool, faults: Faults) -> Self {
        let left = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(faults.retryable));
        Self {
            pool,
            faults,
            retryable_left: left,
        }
    }

    pub const NAME: &'static str = "postgres";
}

impl MessagingBackend for PostgresBackend {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    async fn publish(&self, envelope: Envelope) -> Published {
        if let Some(delay) = self.faults.delay {
            tokio::time::sleep(delay).await;
        }
        if self.faults.fatal {
            return Published::Fatal("the backend refused this payload".into());
        }
        if self
            .retryable_left
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |left| (left > 0).then(|| left - 1),
            )
            .is_ok()
        {
            return Published::Retryable("the backend was unreachable".into());
        }
        // Idempotent by construction: the row already exists, and confirming
        // it twice is the same write.
        let done = sqlx::query(
            "UPDATE conversation_messages
                SET canonical_locator = $2
              WHERE id = $1 AND (canonical_locator IS NULL OR canonical_locator = $2)",
        )
        .bind(envelope.message_id)
        .bind(envelope.message_id.to_string())
        .execute(&self.pool)
        .await;
        match done {
            Err(e) => {
                tracing::warn!(error = %e, "publish failed");
                Published::Retryable("the backend write failed".into())
            }
            Ok(_) if self.faults.lose_confirmation => {
                // Written, and the caller is told it was not. This is the
                // case reconcile exists for.
                Published::Retryable("the confirmation was lost".into())
            }
            Ok(_) => Published::Confirmed(Locator(envelope.message_id.to_string())),
        }
    }

    async fn fetch(&self, locator: &Locator) -> BusResult<Option<String>> {
        let id: Uuid = locator
            .0
            .parse()
            .map_err(|_| BusError::invalid("not a locator this backend issued"))?;
        let row: Option<(String,)> =
            sqlx::query_as("SELECT body FROM conversation_messages WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|r| r.0))
    }

    async fn retain(&self, _before: chrono::DateTime<chrono::Utc>) -> BusResult<u64> {
        // Postgres holds the bodies in the history table itself, which the
        // existing `team prune` retention already covers. Nothing separate
        // to drop, and pretending otherwise would delete history.
        Ok(0)
    }

    async fn reconcile(&self, publish_key: Uuid) -> BusResult<Option<Locator>> {
        let row: Option<(Uuid,)> = sqlx::query_as(
            "SELECT m.id FROM conversation_messages m
               JOIN conversation_outbox o ON o.message_id = m.id
              WHERE o.publish_key = $1 AND m.canonical_locator IS NOT NULL",
        )
        .bind(publish_key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| Locator(r.0.to_string())))
    }
}
