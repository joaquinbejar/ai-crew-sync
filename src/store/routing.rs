//! Which backend a conversation's bodies live on, and how the process gets
//! a handle to it.
//!
//! Routing is an operator decision, recorded in two places and read from
//! both. `teams.default_backend` says where a team's **new** conversations
//! are created; `conversations.backend` says where an existing one's bodies
//! already are, and it never changes once the thread holds messages. A
//! thread with half its history in each place is the one shape nobody can
//! read, so nothing here can produce it.
//!
//! A default installation resolves everything to Postgres, never opens a
//! socket to a broker, and does not need one to be running. A team that has
//! been routed to JetStream on a server started without `--nats-url` is a
//! configuration mistake, and this module says so rather than quietly
//! falling back to Postgres — a silent fallback would split the history.

use std::{collections::HashMap, sync::Arc};

use sqlx::PgPool;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{
    error::{BusError, BusResult},
    store::{
        backend::{Envelope, Locator, MessagingBackend, PostgresBackend, Published},
        jetstream::{self, JetStreamBackend},
    },
};

/// One of the backends, chosen at runtime.
///
/// An enum rather than a trait object because the boundary's methods are
/// `async fn`s: the dispatch is a match, the cost is nothing, and adding a
/// third backend is adding a variant.
#[derive(Clone)]
pub enum AnyBackend {
    Postgres(PostgresBackend),
    /// Boxed: a JetStream handle carries a connection's worth of state, and
    /// this enum is cloned once per read.
    JetStream(Box<JetStreamBackend>),
}

impl MessagingBackend for AnyBackend {
    fn name(&self) -> &'static str {
        match self {
            Self::Postgres(b) => b.name(),
            Self::JetStream(b) => b.name(),
        }
    }

    async fn publish(&self, envelope: Envelope) -> Published {
        match self {
            Self::Postgres(b) => b.publish(envelope).await,
            Self::JetStream(b) => b.publish(envelope).await,
        }
    }

    async fn fetch(&self, locator: &Locator, message_id: Uuid) -> BusResult<Option<String>> {
        match self {
            Self::Postgres(b) => b.fetch(locator, message_id).await,
            Self::JetStream(b) => b.fetch(locator, message_id).await,
        }
    }

    async fn retain(&self, before: chrono::DateTime<chrono::Utc>) -> BusResult<u64> {
        match self {
            Self::Postgres(b) => b.retain(before).await,
            Self::JetStream(b) => b.retain(before).await,
        }
    }

    async fn reconcile(&self, envelope: &Envelope) -> BusResult<Option<Locator>> {
        match self {
            Self::Postgres(b) => b.reconcile(envelope).await,
            Self::JetStream(b) => b.reconcile(envelope).await,
        }
    }
}

/// The process's view of where bodies go. Cheap to clone, shared by the
/// tool handlers and the outbox worker.
#[derive(Clone)]
pub struct Backends {
    postgres: PostgresBackend,
    /// `None` on a default installation: no broker is configured, and no
    /// team can be routed to one.
    nats: Option<jetstream::Config>,
    /// One connection per team, opened on first use. Adopting a stream
    /// costs a round trip, and a read of a thread should not pay it twice.
    connected: Arc<RwLock<HashMap<Uuid, JetStreamBackend>>>,
}

impl Backends {
    /// What every installation gets unless an operator says otherwise.
    pub fn postgres_only(pool: PgPool) -> Self {
        Self {
            postgres: PostgresBackend::new(pool),
            nats: None,
            connected: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// With a broker available for the teams that have been routed to it.
    /// Configuring one does not route anybody: that is `team capability
    /// --backend`.
    pub fn with_jetstream(pool: PgPool, config: jetstream::Config) -> Self {
        Self {
            postgres: PostgresBackend::new(pool),
            nats: Some(config),
            connected: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn jetstream_configured(&self) -> bool {
        self.nats.is_some()
    }

    /// A handle for a backend named in the database.
    pub async fn named(&self, name: &str, team_id: Uuid) -> BusResult<AnyBackend> {
        match name {
            PostgresBackend::NAME => Ok(AnyBackend::Postgres(self.postgres.clone())),
            JetStreamBackend::NAME => {
                if let Some(existing) = self.connected.read().await.get(&team_id) {
                    return Ok(AnyBackend::JetStream(Box::new(existing.clone())));
                }
                let Some(config) = &self.nats else {
                    return Err(BusError::conflict(
                        "this team's conversations are routed to JetStream, but this server \
                         was started without a broker (`--nats-url`). Its bodies are not \
                         lost; they are on the broker, and this process cannot reach it. \
                         Start the server with the broker configured, or route the team \
                         back to Postgres for *new* threads.",
                    ));
                };
                let backend = JetStreamBackend::connect(config, team_id).await?;
                self.connected
                    .write()
                    .await
                    .insert(team_id, backend.clone());
                Ok(AnyBackend::JetStream(Box::new(backend)))
            }
            other => Err(BusError::conflict(format!(
                "this conversation records backend '{other}', which this build does not \
                 know how to read. It is a newer server's data; upgrade rather than \
                 downgrade."
            ))),
        }
    }

    /// Where an existing conversation's bodies are.
    pub async fn for_conversation(
        &self,
        pool: &PgPool,
        conversation: Uuid,
    ) -> BusResult<AnyBackend> {
        let row: Option<(String, Uuid)> =
            sqlx::query_as("SELECT backend, team_id FROM conversations WHERE id = $1")
                .bind(conversation)
                .fetch_optional(pool)
                .await?;
        let Some((name, team_id)) = row else {
            return Err(BusError::not_found("no such conversation"));
        };
        self.named(&name, team_id).await
    }

    /// Where one message's body is authoritative.
    ///
    /// Per message, not per conversation: during a supervised move, and
    /// after a rollback that had to leave a tombstoned body behind, a
    /// thread's bodies are not all in the same place. The message row says
    /// where each one is, and that is what a read follows.
    pub async fn for_message(&self, backend: &str, team_id: Uuid) -> BusResult<AnyBackend> {
        self.named(backend, team_id).await
    }

    /// Where a team's new conversations will be created.
    pub async fn for_team(&self, pool: &PgPool, team_id: Uuid) -> BusResult<AnyBackend> {
        let (name,): (String,) = sqlx::query_as("SELECT default_backend FROM teams WHERE id = $1")
            .bind(team_id)
            .fetch_one(pool)
            .await?;
        self.named(&name, team_id).await
    }

    /// Whether the broker answers. `None` when none is configured, which
    /// is not a fault: it is the default installation.
    pub async fn broker_reachable(&self) -> Option<bool> {
        let config = self.nats.as_ref()?;
        Some(JetStreamBackend::reachable(config).await)
    }

    /// Every team whose conversations are routed off Postgres. The worker
    /// reconciles these on startup; nobody else has anything to reconcile.
    pub async fn routed_teams(&self, pool: &PgPool) -> BusResult<Vec<Uuid>> {
        // Not only the teams whose *default* is a broker. A team routed
        // back to Postgres still has threads whose bodies are on the
        // broker; skipping it would stop publishing their inbox references
        // and stop reconciling their uncertain publications.
        let rows: Vec<(Uuid,)> = sqlx::query_as(
            "SELECT id FROM teams t
              WHERE t.default_backend <> 'postgres'
                 OR EXISTS (SELECT 1 FROM conversations c
                             WHERE c.team_id = t.id AND c.backend <> 'postgres')",
        )
        .fetch_all(pool)
        .await?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }
}
