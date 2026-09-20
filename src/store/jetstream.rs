//! The JetStream adapter, behind the phase 3 backend boundary.
//!
//! Enabled only for teams an operator explicitly routes here. A default
//! installation never contacts a broker, never provisions a stream, and does
//! not need NATS to be running: `conversations.backend` is `postgres` unless
//! someone changes it, and ordinary teams stay on the synchronous Postgres
//! path (ADR 0001, phase 4).
//!
//! What this phase is for: proving the adapter contract against a **real**
//! broker — publish, fetch, idempotency, quotas, reconnect and refused
//! authorization — before any body of anyone's is routed through it. The
//! integration fixture is therefore required rather than skipped: a missing
//! broker fails the suite visibly, because a silently skipped broker test
//! proves nothing and reads like a pass.
//!
//! Three shapes worth stating, because they are easy to get wrong:
//!
//! * **NATS is internal.** No subject, stream or consumer name is ever a
//!   client-facing argument, and ACS checks every ACL itself. A credential
//!   that reaches the broker grants nothing on the bus.
//! * **Provisioning and runtime are separate privileges.** Creating streams
//!   is an operator action with its own credential; publishing and fetching
//!   use a narrower one that cannot create or delete.
//! * **The 1 MiB body contract is unchanged.** The broker limit is 2 MiB, so
//!   a body at the contract's ceiling still fits once headers and framing
//!   are added, and the test measures the serialized size rather than
//!   assuming it.

use async_nats::jetstream;
use uuid::Uuid;

use crate::{
    error::{BusError, BusResult},
    store::backend::{Envelope, Locator, MessagingBackend, Published},
};

/// Header carrying the idempotency key. JetStream deduplicates on
/// `Nats-Msg-Id` within its window; the outbox's `publish_key` is what we
/// put there, so a retry inside that window is recognised by the broker and
/// a retry outside it is recognised by our own reconciliation.
pub const MSG_ID_HEADER: &str = "Nats-Msg-Id";

/// Largest message the broker accepts, including headers. Twice the body
/// contract, so a 1 MiB body plus envelope has room.
///
/// **This has to be set in two places.** The stream's `max_message_size` is
/// one; the server's own `max_payload` is the other, and its default is
/// 1 MiB — which a 1 MiB body plus envelope headers exceeds by a couple of
/// hundred bytes. A deployment that raises only the stream limit refuses
/// exactly the messages the body contract allows. Measured against a real
/// broker in the adapter test, not assumed.
pub const MAX_BROKER_MESSAGE_BYTES: i64 = 2 * 1024 * 1024;

/// Bodies a team's stream retains before the oldest are dropped. Bounded on
/// purpose: a stream with no ceiling is an outage waiting for a quiet week.
pub const DEFAULT_MAX_MESSAGES: i64 = 100_000;
pub const DEFAULT_MAX_BYTES: i64 = 2 * 1024 * 1024 * 1024;

/// Stream name for one team. Stable, opaque and derived from the team id, so
/// renaming a team never moves its data and a slug never reaches the broker.
pub fn stream_name(team_id: Uuid) -> String {
    format!("ACS_T_{}", team_id.simple())
}

/// Subject one conversation's bodies are published on.
pub fn subject(team_id: Uuid, conversation_id: Uuid) -> String {
    format!("acs.{}.conv.{}", team_id.simple(), conversation_id.simple())
}

/// Every subject a team's stream owns.
pub fn subject_filter(team_id: Uuid) -> String {
    format!("acs.{}.>", team_id.simple())
}

/// Connection settings. Kept away from the bus's own configuration: this is
/// infrastructure an operator points at, never something a client supplies.
#[derive(Clone, Debug)]
pub struct Config {
    /// e.g. `nats://127.0.0.1:4222`. TLS in production; the fixture runs
    /// plaintext on a private port.
    pub url: String,
    /// Runtime credential: publish and fetch only. Provisioning uses a
    /// different one, which the server process does not hold.
    pub credentials: Option<String>,
    /// Per-team stream ceilings. Quotas are an operator decision — the
    /// right number depends on the disk the broker actually has — so they
    /// are configuration with a documented default, not a constant.
    pub max_messages: i64,
    pub max_bytes: i64,
}

impl Config {
    /// Production defaults: bounded, file-backed, refusing new writes when
    /// full rather than discarding history.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            credentials: None,
            max_messages: DEFAULT_MAX_MESSAGES,
            max_bytes: DEFAULT_MAX_BYTES,
        }
    }

    /// Smaller ceilings, for a fixture whose broker has a small store.
    pub fn with_limits(mut self, max_messages: i64, max_bytes: i64) -> Self {
        self.max_messages = max_messages;
        self.max_bytes = max_bytes;
        self
    }
}

/// A connected adapter for one team.
#[derive(Clone)]
pub struct JetStreamBackend {
    context: jetstream::Context,
    team_id: Uuid,
    stream: String,
}

impl JetStreamBackend {
    pub const NAME: &'static str = "jetstream";

    /// Connect and adopt the team's stream. Does **not** create it: that is
    /// `provision`, an operator action with a different credential.
    pub async fn connect(config: &Config, team_id: Uuid) -> BusResult<Self> {
        let client = connect_client(config).await?;
        let context = jetstream::new(client);
        let stream = stream_name(team_id);
        // Fail here rather than at the first publish: an unprovisioned team
        // routed to JetStream is a configuration mistake, and it should say
        // so at startup.
        context.get_stream(&stream).await.map_err(|e| {
            BusError::invalid(format!(
                "team {team_id} is routed to JetStream but stream '{stream}' does not exist \
                 or this credential cannot see it ({e}). Provision it first; the runtime \
                 credential deliberately cannot create streams."
            ))
        })?;
        Ok(Self {
            context,
            team_id,
            stream,
        })
    }

    /// Create or update a team's stream. Operator action: run with the
    /// provisioning credential, not the one the server runs with.
    pub async fn provision(config: &Config, team_id: Uuid) -> BusResult<String> {
        let client = connect_client(config).await?;
        let context = jetstream::new(client);
        let name = stream_name(team_id);
        context
            .get_or_create_stream(jetstream::stream::Config {
                name: name.clone(),
                subjects: vec![subject_filter(team_id)],
                // File-backed, bounded, and refusing new writes when full
                // rather than silently dropping the oldest history.
                storage: jetstream::stream::StorageType::File,
                retention: jetstream::stream::RetentionPolicy::Limits,
                discard: jetstream::stream::DiscardPolicy::New,
                max_messages: config.max_messages,
                max_bytes: config.max_bytes,
                max_message_size: MAX_BROKER_MESSAGE_BYTES as i32,
                // History reads go straight to the stream rather than
                // through a consumer: a body fetched by locator is a point
                // read, not a subscription, and a consumer per read would
                // be a consumer per read.
                allow_direct: true,
                ..Default::default()
            })
            .await
            .map_err(|e| BusError::invalid(format!("could not provision '{name}': {e}")))?;
        Ok(name)
    }

    /// Remove a team's stream. Operator action, and a destructive one: it
    /// drops every body the stream holds.
    pub async fn deprovision(config: &Config, team_id: Uuid) -> BusResult<()> {
        let client = connect_client(config).await?;
        let context = jetstream::new(client);
        let name = stream_name(team_id);
        match context.delete_stream(&name).await {
            Ok(_) => Ok(()),
            // Already gone is the outcome the caller asked for.
            Err(e) if e.to_string().contains("not found") => Ok(()),
            // Anything else left the data in place, and an operator who is
            // told "removed" would believe otherwise.
            Err(e) => Err(BusError::invalid(format!(
                "could not delete '{name}' ({e}). The stream and its bodies are still there."
            ))),
        }
    }

    pub fn stream(&self) -> &str {
        &self.stream
    }

    /// A sequence in **this** team's stream, or a refusal.
    ///
    /// The stream name is checked, not discarded. A forged
    /// `jetstream:<someone-elses-stream>:<n>` would otherwise be read as
    /// sequence n of this one, and the envelope check would pass whenever
    /// that sequence happened to be ours.
    fn parse_own_locator(&self, raw: &str) -> BusResult<u64> {
        let (stream, sequence) = parse_locator(raw)?;
        if stream != self.stream {
            return Err(BusError::Forbidden(
                "that locator names another stream".to_owned(),
            ));
        }
        Ok(sequence)
    }
}

async fn connect_client(config: &Config) -> BusResult<async_nats::Client> {
    let options = match &config.credentials {
        Some(path) => {
            async_nats::ConnectOptions::with_credentials_file(std::path::PathBuf::from(path))
                .await
                .map_err(|e| BusError::invalid(format!("could not read NATS credentials: {e}")))?
        }
        None => async_nats::ConnectOptions::new(),
    };
    options
        // Reconnect is the client's job and it does it; what matters here is
        // that a publish which cannot reach the broker fails *retryably*
        // rather than blocking a worker for ever.
        .request_timeout(Some(std::time::Duration::from_secs(10)))
        .connect(&config.url)
        .await
        .map_err(|e| BusError::invalid(format!("could not reach NATS at {}: {e}", config.url)))
}

impl MessagingBackend for JetStreamBackend {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    async fn publish(&self, envelope: Envelope) -> Published {
        // The subject comes from this adapter's team and the header from
        // the envelope's. If they disagree, publishing would write one
        // team's body into another's stream, where the team it belongs to
        // could never read it.
        if envelope.team_id != self.team_id {
            return Published::Fatal(format!(
                "this adapter serves team {} and the envelope is for {}",
                self.team_id, envelope.team_id
            ));
        }
        let subject = subject(self.team_id, envelope.conversation_id);
        let mut headers = async_nats::HeaderMap::new();
        // The broker's own deduplication, inside its window; ours covers the
        // rest, which is why the same key is used for both.
        headers.insert(MSG_ID_HEADER, envelope.publish_key.to_string().as_str());
        headers.insert("Acs-Message-Id", envelope.message_id.to_string().as_str());
        headers.insert(
            "Acs-Conversation-Id",
            envelope.conversation_id.to_string().as_str(),
        );
        headers.insert("Acs-Team-Id", envelope.team_id.to_string().as_str());

        let body_len = envelope.body.len() as i64;
        if body_len > MAX_BROKER_MESSAGE_BYTES {
            return Published::Fatal(format!(
                "body is {body_len} bytes; this broker accepts {MAX_BROKER_MESSAGE_BYTES}"
            ));
        }

        let ack = self
            .context
            .publish_with_headers(subject, headers, envelope.body.into())
            .await;
        let ack = match ack {
            Ok(ack) => ack,
            Err(e) => return classify(&e.to_string()),
        };
        // Await the PubAck: a publish that has not been acknowledged is not
        // stored, and reporting it as such is the mistake this whole phase
        // exists to avoid.
        match ack.await {
            Ok(ack) => Published::Confirmed(Locator(format!(
                "jetstream:{}:{}",
                ack.stream, ack.sequence
            ))),
            Err(e) => classify(&e.to_string()),
        }
    }

    async fn fetch(&self, locator: &Locator) -> BusResult<Option<String>> {
        let sequence = self.parse_own_locator(&locator.0)?;
        let stream = self
            .context
            .get_stream(&self.stream)
            .await
            .map_err(|e| BusError::invalid(format!("stream unavailable: {e}")))?;
        match stream.direct_get(sequence).await {
            Ok(message) => {
                // The envelope's team is checked here, not trusted: a
                // locator is opaque, and a caller that guessed one must not
                // read another team's body.
                let ours = message
                    .headers
                    .get("Acs-Team-Id")
                    .map(|v| v.as_str() == self.team_id.to_string())
                    .unwrap_or(false);
                if !ours {
                    return Err(BusError::Forbidden(
                        "that locator belongs to another team".to_owned(),
                    ));
                }
                Ok(Some(String::from_utf8_lossy(&message.payload).into_owned()))
            }
            Err(e) if e.to_string().contains("not found") => Ok(None),
            Err(e) => Err(BusError::invalid(format!("could not read the body: {e}"))),
        }
    }

    async fn retain(&self, _before: chrono::DateTime<chrono::Utc>) -> BusResult<u64> {
        // Retention is the stream's own policy, set at provisioning: limits
        // by count and bytes, discarding new writes when full rather than
        // dropping history behind the operator's back. Nothing to do per
        // call, and deleting messages here would fight that policy.
        Ok(0)
    }

    async fn reconcile(&self, envelope: &Envelope) -> BusResult<Option<Locator>> {
        // Present the real envelope again under the same `Nats-Msg-Id`.
        // Inside the deduplication window the broker recognises it and
        // answers with the original sequence; outside it, the body lands
        // now. Both answers are a canonical locator for one logical
        // message, which is what the caller needs.
        //
        // The alternative — a small probe carrying the key — is worse than
        // useless. Deduplication is per stream, so a probe that could
        // answer at all is a probe that was stored, it consumes the bounded
        // stream, and it takes the key the real body needed. The locator
        // then names an empty message and the body never lands at all.
        match self.publish(envelope.clone()).await {
            Published::Confirmed(locator) => Ok(Some(locator)),
            // Still no answer, or a refusal. Nothing is claimed either way;
            // the slot stays and the ordinary path retries or gives up.
            Published::Retryable(_) | Published::Fatal(_) => Ok(None),
        }
    }
}

/// `jetstream:<stream>:<sequence>`, parsed into its parts.
fn parse_locator(raw: &str) -> BusResult<(&str, u64)> {
    let mut parts = raw.split(':');
    let (Some("jetstream"), Some(stream), Some(sequence), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(BusError::invalid("not a locator this backend issued"));
    };
    let sequence = sequence
        .parse::<u64>()
        .map_err(|_| BusError::invalid("not a locator this backend issued"))?;
    Ok((stream, sequence))
}

/// Tell apart "try again" from "this will never work". Getting this wrong in
/// either direction is expensive: a fatal error retried for ever, or a
/// transient one that loses a message.
fn classify(error: &str) -> Published {
    let lower = error.to_lowercase();
    if lower.contains("maximum messages")
        || lower.contains("maximum bytes")
        || lower.contains("message size exceeds")
        // The server-wide payload ceiling. Retrying a message the broker is
        // configured never to accept is a loop, not a recovery.
        || lower.contains("max payload size exceeded")
        || lower.contains("too large")
        || lower.contains("authorization")
        || lower.contains("permissions violation")
        || lower.contains("no responders")
    {
        Published::Fatal(error.to_owned())
    } else {
        Published::Retryable(error.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_opaque_and_stable() {
        let team = Uuid::nil();
        assert_eq!(stream_name(team), "ACS_T_00000000000000000000000000000000");
        assert!(
            subject(team, Uuid::nil()).starts_with("acs.00000000"),
            "a subject never carries a slug a team could rename"
        );
        assert!(subject_filter(team).ends_with(".>"));
    }

    #[test]
    fn a_locator_round_trips_and_a_forged_one_is_refused() {
        assert_eq!(
            parse_locator("jetstream:ACS_T_x:42").unwrap(),
            ("ACS_T_x", 42)
        );
        assert!(parse_locator("nonsense").is_err());
        assert!(
            parse_locator("ACS_T_x:42").is_err(),
            "the prefix is checked"
        );
        assert!(
            parse_locator("jetstream:ACS_T_x:42:extra").is_err(),
            "a locator has three parts and no more"
        );
    }

    #[test]
    fn full_and_refused_are_fatal_while_a_timeout_is_not() {
        assert!(matches!(
            classify("maximum messages exceeded"),
            Published::Fatal(_)
        ));
        assert!(matches!(
            classify("permissions violation for publish"),
            Published::Fatal(_)
        ));
        assert!(matches!(
            classify("max payload size exceeded: Payload size limit of 1048576 exceeded"),
            Published::Fatal(_)
        ));
        assert!(matches!(classify("timed out"), Published::Retryable(_)));
        assert!(matches!(
            classify("connection reset by peer"),
            Published::Retryable(_)
        ));
    }
}
