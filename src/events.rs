//! In-process event hub fed by Postgres LISTEN/NOTIFY.
//!
//! One background task holds a single LISTEN connection on `bus_events` and
//! fans every payload out to in-process subscribers through a tokio broadcast
//! channel. Consumers: the `wait_for_updates` tool (long-poll) and the webhook
//! dispatcher. Payloads carry ids only; consumers resolve names against the
//! database when they need them.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    },
    time::Duration,
};

use sqlx::{PgPool, postgres::PgListener};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub const PG_CHANNEL: &str = "bus_events";

/// A parsed NOTIFY payload. Kept as loose JSON plus typed accessors so adding
/// fields to the triggers never breaks older consumers.
#[derive(Clone, Debug)]
pub struct BusEvent(pub serde_json::Value);

impl BusEvent {
    pub fn kind(&self) -> &str {
        self.0.get("kind").and_then(|v| v.as_str()).unwrap_or("")
    }

    fn uuid_field(&self, key: &str) -> Option<Uuid> {
        self.0
            .get(key)
            .and_then(|v| v.as_str())
            .and_then(|s| Uuid::parse_str(s).ok())
    }

    pub fn team_id(&self) -> Option<Uuid> {
        self.uuid_field("team_id")
    }

    pub fn recipient_agent_id(&self) -> Option<Uuid> {
        self.uuid_field("recipient_agent_id")
    }

    pub fn sender_agent_id(&self) -> Option<Uuid> {
        self.uuid_field("sender_agent_id")
    }

    pub fn channel_id(&self) -> Option<Uuid> {
        self.uuid_field("channel_id")
    }

    pub fn message_id(&self) -> Option<i64> {
        self.0.get("id").and_then(|v| v.as_i64())
    }

    /// Working context this message was addressed to, if it was addressed to
    /// one. Absent means every session of the recipient.
    pub fn recipient_session(&self) -> Option<&str> {
        self.0.get("recipient_session").and_then(|v| v.as_str())
    }

    /// Working context the message was sent from, if any.
    pub fn sender_session(&self) -> Option<&str> {
        self.0.get("sender_session").and_then(|v| v.as_str())
    }

    /// Was this posted as an announcement — something the sender judged worth
    /// interrupting the whole team for?
    pub fn is_announcement(&self) -> bool {
        self.0
            .get("announce")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    pub fn is_direct_message(&self) -> bool {
        self.kind() == "message" && self.recipient_agent_id().is_some()
    }

    /// Is this event visible to `session` of `agent` of `team`?
    ///
    /// Direct messages are only visible to their recipient (and sender);
    /// everything else is team-wide. A message addressed to one session does
    /// not wake the recipient's other sessions — otherwise every window of a
    /// person would wake for a question meant for one of them. It still
    /// reaches every session of the *sender*, which is how a reply finds the
    /// window that is blocked waiting for it.
    pub fn visible_to(&self, team_id: Uuid, agent_id: Uuid, session: &str) -> bool {
        if self.team_id() != Some(team_id) {
            return false;
        }
        if self.is_direct_message() {
            if self.sender_agent_id() == Some(agent_id) {
                return true;
            }
            if self.recipient_agent_id() != Some(agent_id) {
                return false;
            }
            return match self.recipient_session() {
                Some(addressed) => addressed == session,
                // Addressed to the person: every one of their sessions.
                None => true,
            };
        }
        true
    }
}

/// How often each replica writes a ping through Postgres and expects to hear
/// it back on its own LISTEN connection.
///
/// The echo is the only proof the listener is alive. A connection that only
/// ever reads cannot tell a quiet channel from a peer that silently dropped
/// it: Swarm's IPVS forgets an idle TCP connection after fifteen minutes and
/// tells neither end, and every wake on a deployment stopped while the log
/// still said "attached". A ping is also traffic, so a listener that echoes
/// is one no idle timeout forgets.
pub const DEFAULT_PING_SECS: u64 = 30;

/// Pings sent without an echo before the listener is declared deaf, dropped
/// and attached afresh.
const MISSED_ECHOES: u32 = 3;

/// What the listener knows about itself, for `/health` and for the loop.
pub struct ListenerHealth {
    ping_every: Duration,
    attached: AtomicBool,
    /// Unix seconds of the last own ping heard back; reset on attach.
    last_echo: AtomicI64,
    /// LISTEN connections established so far. A second one means the first
    /// was lost, silently or not.
    attachments: AtomicU64,
    echoes: AtomicU64,
}

fn unix_now() -> i64 {
    chrono::Utc::now().timestamp()
}

impl ListenerHealth {
    fn new(ping_every: Duration) -> Self {
        Self {
            ping_every,
            attached: AtomicBool::new(false),
            last_echo: AtomicI64::new(0),
            attachments: AtomicU64::new(0),
            echoes: AtomicU64::new(0),
        }
    }

    pub fn ping_every(&self) -> Duration {
        self.ping_every
    }

    /// How long without an echo before "live" becomes "silent": the same
    /// span after which the loop gives up on the connection.
    pub fn stale_after(&self) -> Duration {
        self.ping_every * MISSED_ECHOES
    }

    fn attached_now(&self) {
        self.last_echo.store(unix_now(), Ordering::SeqCst);
        self.attached.store(true, Ordering::SeqCst);
        self.attachments.fetch_add(1, Ordering::SeqCst);
    }

    fn echoed(&self) {
        self.last_echo.store(unix_now(), Ordering::SeqCst);
        self.echoes.fetch_add(1, Ordering::SeqCst);
    }

    fn detached(&self) {
        self.attached.store(false, Ordering::SeqCst);
    }

    /// `live`: attached and hearing itself. `silent`: attached, but nothing
    /// heard back within the deadline — what a dead socket looks like until
    /// the loop drops it. `detached`: no LISTEN connection right now.
    pub fn report(&self) -> serde_json::Value {
        let attached = self.attached.load(Ordering::SeqCst);
        let age = unix_now() - self.last_echo.load(Ordering::SeqCst);
        let listener = if !attached {
            "detached"
        } else if age <= self.stale_after().as_secs() as i64 {
            "live"
        } else {
            "silent"
        };
        serde_json::json!({
            "listener": listener,
            "last_echo_seconds": attached.then_some(age),
            "attachments": self.attachments.load(Ordering::SeqCst),
            "echoes": self.echoes.load(Ordering::SeqCst),
            "ping_seconds": self.ping_every.as_secs(),
        })
    }

    pub fn is_live(&self) -> bool {
        self.report()["listener"] == "live"
    }
}

#[derive(Clone)]
pub struct EventHub {
    tx: broadcast::Sender<BusEvent>,
    health: Arc<ListenerHealth>,
}

impl Default for EventHub {
    fn default() -> Self {
        Self::new()
    }
}

impl EventHub {
    pub fn new() -> Self {
        Self::with_ping(Duration::from_secs(DEFAULT_PING_SECS))
    }

    /// A hub whose listener pings itself every `ping_every` (at least one
    /// second: a zero interval would be a busy loop against Postgres).
    pub fn with_ping(ping_every: Duration) -> Self {
        // 256 in-flight events is plenty; laggards get Lagged and resync from
        // the database, which every consumer does anyway.
        let (tx, _) = broadcast::channel(256);
        Self {
            tx,
            health: Arc::new(ListenerHealth::new(ping_every.max(Duration::from_secs(1)))),
        }
    }

    pub fn listener(&self) -> &ListenerHealth {
        &self.health
    }

    pub fn subscribe(&self) -> broadcast::Receiver<BusEvent> {
        self.tx.subscribe()
    }

    pub fn publish(&self, event: BusEvent) {
        // No receivers is fine: nobody is waiting right now.
        let _ = self.tx.send(event);
    }
}

/// Run the LISTEN loop until cancelled. Reconnects with backoff on failure so
/// a Postgres restart degrades to polling latency instead of killing wakeups.
///
/// The connection is also watched from the inside: every `ping_every` this
/// replica notifies the channel with its own id through the pool and expects
/// to read that ping back here. [`MISSED_ECHOES`] pings without an echo mean
/// the socket is dead however open it looks, and the listener is dropped
/// and attached afresh rather than trusted forever.
pub async fn run_pg_listener(pool: PgPool, hub: EventHub, ct: CancellationToken) {
    let health = hub.listener();
    let replica = Uuid::new_v4().to_string();
    loop {
        if ct.is_cancelled() {
            return;
        }
        match PgListener::connect_with(&pool).await {
            Ok(mut listener) => {
                if let Err(e) = listener.listen(PG_CHANNEL).await {
                    tracing::warn!(error = %e, "LISTEN failed; retrying");
                } else {
                    tracing::info!("event listener attached to '{PG_CHANNEL}'");
                    health.attached_now();
                    let mut ticker = tokio::time::interval(health.ping_every());
                    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    // Pings sent on this connection that have not come back.
                    let mut unanswered: u32 = 0;
                    loop {
                        tokio::select! {
                            _ = ct.cancelled() => return,
                            _ = ticker.tick() => {
                                if unanswered >= MISSED_ECHOES {
                                    tracing::warn!(
                                        unanswered,
                                        "event listener has not heard its own ping; the \
                                         connection is dead however open it looks. Reattaching"
                                    );
                                    break;
                                }
                                unanswered += 1;
                                let ping = serde_json::json!({ "kind": "ping", "replica": replica })
                                    .to_string();
                                if let Err(e) = sqlx::query("SELECT pg_notify($1, $2)")
                                    .bind(PG_CHANNEL)
                                    .bind(&ping)
                                    .execute(&pool)
                                    .await
                                {
                                    tracing::warn!(error = %e, "could not ping the event channel");
                                }
                            }
                            recv = listener.try_recv() => match recv {
                                Ok(Some(notification)) => {
                                    match serde_json::from_str(notification.payload()) {
                                        Ok(value) => {
                                            let event = BusEvent(value);
                                            if event.kind() == "ping" {
                                                // Every replica's pings arrive here; only
                                                // this one's say anything about this
                                                // connection. None of them is an event.
                                                if event.0.get("replica").and_then(|v| v.as_str())
                                                    == Some(replica.as_str())
                                                {
                                                    unanswered = 0;
                                                    health.echoed();
                                                }
                                                continue;
                                            }
                                            hub.publish(event)
                                        }
                                        Err(e) => tracing::warn!(
                                            error = %e,
                                            payload = notification.payload(),
                                            "unparseable bus event"
                                        ),
                                    }
                                }
                                // None = connection dropped and was re-established by
                                // sqlx; notifications in between are lost, which
                                // consumers tolerate by re-checking the database. The
                                // next echo says whether the new connection hears.
                                Ok(None) => tracing::debug!("event listener reconnected"),
                                Err(e) => {
                                    tracing::warn!(error = %e, "event listener error");
                                    break;
                                }
                            }
                        }
                    }
                    health.detached();
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "could not attach event listener; retrying");
            }
        }
        tokio::select! {
            _ = ct.cancelled() => return,
            _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
        }
    }
}
