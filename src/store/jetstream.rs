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

/// Bodies a team's stream retains before new writes are refused. Bounded on
/// purpose: a stream with no ceiling is an outage waiting for a quiet week.
/// Every stream's `max_bytes` is **reserved** against the broker's
/// `max_file_store` the moment it is created, used or not, so these are
/// the numbers an operator sizes the broker by (`team stream` takes
/// smaller ones).
pub const DEFAULT_MAX_MESSAGES: i64 = 100_000;
pub const DEFAULT_MAX_BYTES: i64 = 2 * 1024 * 1024 * 1024;
/// The inbox stream holds references (a few hundred bytes each, gone after
/// seven days or on acknowledgement), so its reservation is a fraction of
/// the body stream's.
pub const DEFAULT_INBOX_MAX_MESSAGES: i64 = 100_000;
pub const DEFAULT_INBOX_MAX_BYTES: i64 = 256 * 1024 * 1024;

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

/// The inbox stream for one team: references, not bodies.
///
/// A second stream on purpose. Bodies are canonical history and must not be
/// dropped; references are an accelerator whose truth is in Postgres and can
/// be dropped freely. One retention policy cannot be right for both.
pub fn inbox_stream_name(team_id: Uuid) -> String {
    format!("ACS_I_{}", team_id.simple())
}

/// One recipient's own subject. `recipient_key` is opaque and
/// subject-safe by construction: a session id, or `a` plus an agent id.
pub fn inbox_subject(team_id: Uuid, recipient_key: &str) -> String {
    format!("acsi.{}.inbox.{}", team_id.simple(), recipient_key)
}

pub fn inbox_filter(team_id: Uuid) -> String {
    format!("acsi.{}.>", team_id.simple())
}

/// How long an unread reference is kept before Postgres is the only place
/// it exists. Reconciliation rebuilds it from there, so this is a cache
/// horizon and not a data loss window.
pub const INBOX_MAX_AGE_SECS: u64 = 7 * 24 * 3600;
/// Redeliveries before the broker gives up on a reference. Postgres still
/// has it, and a reader is told the difference.
pub const INBOX_MAX_DELIVER: i64 = 5;
/// References one recipient may hold un-acknowledged at once.
pub const INBOX_MAX_ACK_PENDING: i64 = 256;
/// How long a handed-out reference may stay unconfirmed before the broker
/// offers it again.
pub const INBOX_ACK_WAIT_SECS: u64 = 60;

/// One reference as the broker handed it over.
#[derive(Clone, Debug)]
pub struct InboxRef {
    pub payload: String,
    /// Where to acknowledge it once the receipt is committed. Empty when
    /// the broker offered no reply subject, which makes it unackable and
    /// therefore redelivered — visible rather than silently dropped.
    pub ack_subject: String,
    pub stream_seq: u64,
    /// How many times the broker has offered this one. Greater than one is
    /// a redelivery, which is expected and must be idempotent.
    pub deliveries: u64,
}

/// What a recipient's consumer holds, for an honest status.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct InboxStatus {
    pub pending: u64,
    pub awaiting_ack: u64,
    pub redelivered: u64,
    /// False when the consumer does not exist: expired, never created, or
    /// deleted by an operator. Postgres is then the only source, and
    /// reconciliation says so rather than reporting an empty inbox.
    pub present: bool,
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
    /// Per-team stream ceilings for the body stream. Quotas are an operator
    /// decision — the right number depends on the disk the broker actually
    /// has — so they are configuration with a documented default, not a
    /// constant.
    pub max_messages: i64,
    pub max_bytes: i64,
    /// The inbox stream's own ceilings, independent of the body stream's.
    pub inbox_max_messages: i64,
    pub inbox_max_bytes: i64,
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
            inbox_max_messages: DEFAULT_INBOX_MAX_MESSAGES,
            inbox_max_bytes: DEFAULT_INBOX_MAX_BYTES,
        }
    }

    /// Smaller ceilings for both streams, for a fixture whose broker has a
    /// small store.
    pub fn with_limits(mut self, max_messages: i64, max_bytes: i64) -> Self {
        self.max_messages = max_messages;
        self.max_bytes = max_bytes;
        self.inbox_max_messages = max_messages;
        self.inbox_max_bytes = max_bytes;
        self
    }

    /// Ceilings for the inbox stream alone.
    pub fn with_inbox_limits(mut self, max_messages: i64, max_bytes: i64) -> Self {
        self.inbox_max_messages = max_messages;
        self.inbox_max_bytes = max_bytes;
        self
    }

    /// Refuse quotas that could never work before the broker is asked: a
    /// body stream must hold at least one maximum-size message, and a
    /// count of zero would refuse every write.
    pub fn validate_quotas(&self) -> BusResult<()> {
        if self.max_bytes < MAX_BROKER_MESSAGE_BYTES {
            return Err(BusError::invalid(format!(
                "--max-bytes must be at least {} ({}), the largest message the body stream \
                 accepts; {} would refuse every body",
                MAX_BROKER_MESSAGE_BYTES,
                format_size(MAX_BROKER_MESSAGE_BYTES),
                format_size(self.max_bytes)
            )));
        }
        if self.inbox_max_bytes < MIN_INBOX_BYTES {
            return Err(BusError::invalid(format!(
                "--inbox-max-bytes must be at least {} ({}); references are small but a \
                 team sends many",
                MIN_INBOX_BYTES,
                format_size(MIN_INBOX_BYTES)
            )));
        }
        if self.max_messages < 1 || self.inbox_max_messages < 1 {
            return Err(BusError::invalid(
                "--max-messages and --inbox-max-messages must be at least 1",
            ));
        }
        Ok(())
    }
}

/// The smallest inbox reservation `team stream` accepts.
pub const MIN_INBOX_BYTES: i64 = 1024 * 1024;

/// Parse an operator-typed size: plain bytes, or a number with `K`, `M`, `G`
/// (or `KiB`, `MiB`, `GiB`, `KB`, `MB`, `GB`), case-insensitive, all
/// binary multiples. `1GiB`, `512 MiB`, `1048576` all work.
pub fn parse_size(raw: &str) -> Result<i64, String> {
    let text = raw.trim();
    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    let (digits, unit) = text.split_at(split);
    if digits.is_empty() {
        return Err(format!(
            "'{raw}' is not a size; write bytes, or a number with KiB, MiB or GiB"
        ));
    }
    let number: i64 = digits
        .parse()
        .map_err(|_| format!("'{raw}' is too large a number"))?;
    let multiplier: i64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024 * 1024,
        "g" | "gb" | "gib" => 1024 * 1024 * 1024,
        other => {
            return Err(format!(
                "'{raw}': unknown unit '{other}'; use bytes, KiB, MiB or GiB"
            ));
        }
    };
    number
        .checked_mul(multiplier)
        .ok_or_else(|| format!("'{raw}' is too large a size"))
}

/// A size for a human, in the unit that reads best.
pub fn format_size(bytes: i64) -> String {
    const GIB: i64 = 1024 * 1024 * 1024;
    const MIB: i64 = 1024 * 1024;
    const KIB: i64 = 1024;
    if bytes >= GIB && bytes % GIB == 0 {
        format!("{} GiB", bytes / GIB)
    } else if bytes >= MIB && bytes % MIB == 0 {
        format!("{} MiB", bytes / MIB)
    } else if bytes >= KIB && bytes % KIB == 0 {
        format!("{} KiB", bytes / KIB)
    } else if bytes >= GIB {
        format!("{:.1} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else {
        format!("{bytes} B")
    }
}

/// Which of a team's two streams an operation addresses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamKind {
    Bodies,
    Inbox,
}

/// What provisioning found or made: the stream, whether this call created
/// it, the limits it has **now** (an existing stream keeps its own; see
/// [`JetStreamBackend::update_quotas`]) and what it currently holds.
#[derive(Clone, Debug)]
pub struct Provisioned {
    pub name: String,
    pub created: bool,
    pub max_messages: i64,
    pub max_bytes: i64,
    pub messages: u64,
    pub bytes: u64,
}

impl Provisioned {
    /// Whether the stream's limits differ from what the caller asked for.
    pub fn differs_from(&self, max_messages: i64, max_bytes: i64) -> bool {
        self.max_messages != max_messages || self.max_bytes != max_bytes
    }

    /// Whether the stream holds more than its ceiling allows; after an
    /// update this means a publisher got in between the check and the
    /// change.
    pub fn over_ceiling(&self) -> bool {
        (self.messages as i64) > self.max_messages || (self.bytes as i64) > self.max_bytes
    }
}

/// A vetted quota change: the limits to restore on rollback, the limits
/// asked for, and what the stream holds.
#[derive(Clone, Debug)]
pub struct QuotaChange {
    pub name: String,
    pub kind: StreamKind,
    pub current_max_messages: i64,
    pub current_max_bytes: i64,
    pub wanted_max_messages: i64,
    pub wanted_max_bytes: i64,
    pub messages: u64,
    pub bytes: u64,
}

impl QuotaChange {
    /// Bytes the broker must reserve on top of what this stream already
    /// reserves; zero for a decrease.
    pub fn additional_bytes(&self) -> i64 {
        (self.wanted_max_bytes - self.current_max_bytes).max(0)
    }
}

/// The broker's storage account, in bytes.
#[derive(Clone, Copy, Debug)]
pub struct StorageAccount {
    pub used: u64,
    pub reserved: u64,
    pub budget: Option<i64>,
}

impl StorageAccount {
    /// Whether `additional` more reserved bytes fit the budget.
    pub fn fits(&self, additional: i64) -> bool {
        match self.budget {
            Some(budget) => (self.reserved as i64).saturating_add(additional) <= budget,
            None => true,
        }
    }

    /// One sentence with the numbers, for a refusal.
    pub fn describe(&self) -> String {
        let budget = match self.budget {
            Some(m) => format!("the account's storage budget is {} ({})", m, format_size(m)),
            None => "the account has no storage limit of its own, so the ceiling is the \
                     server's max_file_store"
                .to_owned(),
        };
        format!(
            "{budget}; {} ({}) is already reserved by existing streams' max_bytes while only \
             {} ({}) is actually used",
            self.reserved,
            format_size(self.reserved as i64),
            self.used,
            format_size(self.used as i64)
        )
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

    /// The body stream's configuration, as `provision` creates it and
    /// `update_quotas` rewrites it: one place, so the two never drift.
    fn bodies_config(
        team_id: Uuid,
        max_messages: i64,
        max_bytes: i64,
    ) -> jetstream::stream::Config {
        jetstream::stream::Config {
            name: stream_name(team_id),
            subjects: vec![subject_filter(team_id)],
            // File-backed, bounded, and refusing new writes when full
            // rather than silently dropping the oldest history.
            storage: jetstream::stream::StorageType::File,
            retention: jetstream::stream::RetentionPolicy::Limits,
            discard: jetstream::stream::DiscardPolicy::New,
            max_messages,
            max_bytes,
            max_message_size: MAX_BROKER_MESSAGE_BYTES as i32,
            // History reads go straight to the stream rather than through
            // a consumer: a body fetched by locator is a point read, not a
            // subscription, and a consumer per read would be a consumer per
            // read.
            allow_direct: true,
            ..Default::default()
        }
    }

    /// The inbox stream's configuration; see `bodies_config`.
    fn inbox_config(team_id: Uuid, max_messages: i64, max_bytes: i64) -> jetstream::stream::Config {
        jetstream::stream::Config {
            name: inbox_stream_name(team_id),
            subjects: vec![inbox_filter(team_id)],
            storage: jetstream::stream::StorageType::File,
            // A reference is work: it is removed when its recipient
            // acknowledges it. With one exact subject per recipient and one
            // consumer on it, two recipients can never compete for each
            // other's references.
            retention: jetstream::stream::RetentionPolicy::WorkQueue,
            discard: jetstream::stream::DiscardPolicy::Old,
            max_age: std::time::Duration::from_secs(INBOX_MAX_AGE_SECS),
            max_messages,
            max_bytes,
            allow_direct: true,
            ..Default::default()
        }
    }

    fn stream_config(
        config: &Config,
        team_id: Uuid,
        kind: StreamKind,
    ) -> jetstream::stream::Config {
        match kind {
            StreamKind::Bodies => {
                Self::bodies_config(team_id, config.max_messages, config.max_bytes)
            }
            StreamKind::Inbox => {
                Self::inbox_config(team_id, config.inbox_max_messages, config.inbox_max_bytes)
            }
        }
    }

    /// Create a team's body stream, or find it. Operator action: run with
    /// the provisioning credential, not the one the server runs with.
    ///
    /// An existing stream **keeps its limits**, whatever `config` asks for:
    /// a routine retry must never shrink or grow a quota on the quiet.
    /// [`Provisioned::created`] and [`Provisioned::differs_from`] tell the
    /// caller what happened, and [`Self::update_quotas`] is the explicit
    /// way to change them.
    pub async fn provision(config: &Config, team_id: Uuid) -> BusResult<Provisioned> {
        Self::provision_kind(config, team_id, StreamKind::Bodies).await
    }

    /// Create or find a team's inbox stream. Same operator action and same
    /// credential as `provision`; separate so a deployment can see what
    /// each stream costs.
    pub async fn provision_inbox(config: &Config, team_id: Uuid) -> BusResult<Provisioned> {
        Self::provision_kind(config, team_id, StreamKind::Inbox).await
    }

    async fn provision_kind(
        config: &Config,
        team_id: Uuid,
        kind: StreamKind,
    ) -> BusResult<Provisioned> {
        config.validate_quotas()?;
        let client = connect_client(config).await?;
        let context = jetstream::new(client);
        let wanted = Self::stream_config(config, team_id, kind);
        let name = wanted.name.clone();
        let requested_bytes = wanted.max_bytes;
        // Existing: found, limits untouched. Missing: created with the
        // requested limits. `get_or_create_stream` is exactly that.
        let existed = context.get_stream(&name).await.is_ok();
        let mut stream = match context.get_or_create_stream(wanted).await {
            Ok(stream) => stream,
            Err(e) => {
                return Err(
                    provisioning_error(&context, &name, "create", requested_bytes, e).await,
                );
            }
        };
        let info = stream
            .info()
            .await
            .map_err(|e| BusError::invalid(format!("could not read '{name}' back: {e}")))?;
        Ok(Provisioned {
            name,
            created: !existed,
            max_messages: info.config.max_messages,
            max_bytes: info.config.max_bytes,
            messages: info.state.messages,
            bytes: info.state.bytes,
        })
    }

    /// What changing a stream's quotas would do, without doing it: the
    /// limits it has now (what a rollback restores), what it holds, and the
    /// extra bytes the broker would have to reserve. Refused when the
    /// stream already holds more than the new ceiling would allow, since a
    /// ceiling below the current contents would make the next write fail
    /// (bodies) or start dropping (references) at once.
    pub async fn check_update(
        config: &Config,
        team_id: Uuid,
        kind: StreamKind,
    ) -> BusResult<QuotaChange> {
        config.validate_quotas()?;
        let client = connect_client(config).await?;
        let context = jetstream::new(client);
        let wanted = Self::stream_config(config, team_id, kind);
        let name = wanted.name.clone();
        let mut current = context.get_stream(&name).await.map_err(|e| {
            BusError::invalid(format!(
                "stream '{name}' does not exist ({e}); provision it first, quotas are set at \
                 creation and changed here"
            ))
        })?;
        let info = current
            .info()
            .await
            .map_err(|e| BusError::invalid(format!("could not read '{name}': {e}")))?;
        if (info.state.messages as i64) > wanted.max_messages {
            return Err(BusError::invalid(format!(
                "stream '{name}' holds {} messages, more than the requested ceiling of {}; \
                 nothing was changed. Prune first (`team prune`) or ask for a higher count",
                info.state.messages, wanted.max_messages
            )));
        }
        if (info.state.bytes as i64) > wanted.max_bytes {
            return Err(BusError::invalid(format!(
                "stream '{name}' holds {} ({}), more than the requested ceiling of {} ({}); \
                 nothing was changed. Prune first (`team prune`) or ask for a higher quota",
                info.state.bytes,
                format_size(info.state.bytes as i64),
                wanted.max_bytes,
                format_size(wanted.max_bytes)
            )));
        }
        Ok(QuotaChange {
            name,
            kind,
            current_max_messages: info.config.max_messages,
            current_max_bytes: info.config.max_bytes,
            wanted_max_messages: wanted.max_messages,
            wanted_max_bytes: wanted.max_bytes,
            messages: info.state.messages,
            bytes: info.state.bytes,
        })
    }

    /// Change an existing stream's quotas to what `config` asks for: the
    /// explicit counterpart of the retry that keeps them. Runs
    /// [`Self::check_update`] first. The check is a snapshot: a publisher can
    /// add to the stream between it and the update, so the outcome carries
    /// the contents read back afterwards and the caller says so when they
    /// exceed the new ceiling. Neither stream loses data to that race: the
    /// body stream discards **new** writes when full, and an inbox reference
    /// the broker drops is rebuilt from Postgres, which is its authority.
    pub async fn update_quotas(
        config: &Config,
        team_id: Uuid,
        kind: StreamKind,
    ) -> BusResult<Provisioned> {
        let change = Self::check_update(config, team_id, kind).await?;
        Self::apply_update(config, team_id, kind, &change).await
    }

    /// Apply a change that [`Self::check_update`] already vetted.
    pub async fn apply_update(
        config: &Config,
        team_id: Uuid,
        kind: StreamKind,
        change: &QuotaChange,
    ) -> BusResult<Provisioned> {
        let client = connect_client(config).await?;
        let context = jetstream::new(client);
        let wanted = Self::stream_config(config, team_id, kind);
        let name = wanted.name.clone();
        let info = match context.update_stream(wanted).await {
            Ok(info) => info,
            Err(e) => {
                return Err(provisioning_error(
                    &context,
                    &name,
                    "update",
                    change.additional_bytes(),
                    e,
                )
                .await);
            }
        };
        Ok(Provisioned {
            name,
            created: false,
            max_messages: info.config.max_messages,
            max_bytes: info.config.max_bytes,
            messages: info.state.messages,
            bytes: info.state.bytes,
        })
    }

    /// The broker's storage account as it stands: bytes used, bytes
    /// reserved by every stream's `max_bytes`, and the store's budget
    /// (`None` when the account is unlimited). Lets a caller refuse a
    /// change that could not be reserved before touching anything.
    pub async fn storage_account(config: &Config) -> BusResult<StorageAccount> {
        let client = connect_client(config).await?;
        let context = jetstream::new(client);
        let account = context
            .query_account()
            .await
            .map_err(|e| BusError::invalid(format!("could not read the broker's account: {e}")))?;
        Ok(StorageAccount {
            used: account.storage,
            reserved: account.reserved_storage,
            budget: account.limits.max_storage.filter(|m| *m > 0),
        })
    }

    /// Publish one reference to a recipient's own subject.
    ///
    /// Deduplicated on the event id, so a retry of the same reference is
    /// the same notification rather than a second one.
    pub async fn publish_reference(
        &self,
        recipient_key: &str,
        event_id: Uuid,
        payload: &str,
    ) -> Published {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert(MSG_ID_HEADER, event_id.to_string().as_str());
        headers.insert("Acs-Team-Id", self.team_id.to_string().as_str());
        let ack = self
            .context
            .publish_with_headers(
                inbox_subject(self.team_id, recipient_key),
                headers,
                payload.to_owned().into(),
            )
            .await;
        let ack = match ack {
            Ok(ack) => ack,
            Err(e) => return classify(&e.to_string()),
        };
        match ack.await {
            Ok(ack) => Published::Confirmed(Locator(format!(
                "jetstream:{}:{}",
                ack.stream, ack.sequence
            ))),
            Err(e) => classify(&e.to_string()),
        }
    }

    /// Adopt (creating if needed) the durable pull consumer for one
    /// recipient. One consumer, one exact subject, explicit acknowledgement.
    async fn inbox_consumer(
        &self,
        recipient_key: &str,
    ) -> BusResult<jetstream::consumer::Consumer<jetstream::consumer::pull::Config>> {
        let stream = self
            .context
            .get_stream(inbox_stream_name(self.team_id))
            .await
            .map_err(|e| {
                BusError::invalid(format!(
                    "this team's inbox stream is not provisioned or is unreachable ({e}). \
                     Run `ai-crew-sync team stream --team <team> --nats-url <url>`; \
                     references are still in Postgres meanwhile."
                ))
            })?;
        let durable = format!("IN_{recipient_key}");
        stream
            .get_or_create_consumer(
                &durable,
                jetstream::consumer::pull::Config {
                    durable_name: Some(durable.clone()),
                    filter_subject: inbox_subject(self.team_id, recipient_key),
                    ack_policy: jetstream::consumer::AckPolicy::Explicit,
                    ack_wait: std::time::Duration::from_secs(INBOX_ACK_WAIT_SECS),
                    max_deliver: INBOX_MAX_DELIVER,
                    max_ack_pending: INBOX_MAX_ACK_PENDING,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| BusError::invalid(format!("could not open the inbox: {e}")))
    }

    /// Take up to `limit` references without acknowledging any of them.
    /// Acknowledgement happens only once the receipt is committed.
    pub async fn fetch_references(
        &self,
        recipient_key: &str,
        limit: usize,
    ) -> BusResult<Vec<InboxRef>> {
        use futures::StreamExt;
        let consumer = self.inbox_consumer(recipient_key).await?;
        let mut batch = consumer
            .fetch()
            .max_messages(limit)
            .messages()
            .await
            .map_err(|e| BusError::invalid(format!("could not read the inbox: {e}")))?;
        let mut out = Vec::new();
        while let Some(message) = batch.next().await {
            let message =
                message.map_err(|e| BusError::invalid(format!("inbox read failed: {e}")))?;
            let info = message.info().ok();
            out.push(InboxRef {
                payload: String::from_utf8_lossy(&message.payload).into_owned(),
                ack_subject: message
                    .reply
                    .as_ref()
                    .map(|s| s.to_string())
                    .unwrap_or_default(),
                stream_seq: info.as_ref().map(|i| i.stream_sequence).unwrap_or(0),
                deliveries: info.as_ref().map(|i| i.delivered as u64).unwrap_or(1),
            });
        }
        Ok(out)
    }

    /// Acknowledge one reference by the subject it was offered on. Sent
    /// after the receipt has been committed, never before.
    pub async fn ack_reference(&self, ack_subject: &str) -> BusResult<()> {
        if ack_subject.is_empty() {
            return Ok(());
        }
        let client = self.context.client();
        client
            .publish(ack_subject.to_owned(), bytes::Bytes::from_static(b"+ACK"))
            .await
            .map_err(|e| BusError::invalid(format!("could not acknowledge: {e}")))?;
        client
            .flush()
            .await
            .map_err(|e| BusError::invalid(format!("could not acknowledge: {e}")))?;
        Ok(())
    }

    /// What this recipient's consumer holds. A missing consumer is a fact,
    /// not an empty inbox.
    pub async fn inbox_status(&self, recipient_key: &str) -> BusResult<InboxStatus> {
        let stream = match self
            .context
            .get_stream(inbox_stream_name(self.team_id))
            .await
        {
            Ok(stream) => stream,
            // A stream that is not there is a fact. A broker that cannot be
            // reached is a different one, and reporting an empty cache
            // during an outage is how an operator concludes the inbox is
            // drained when it is not.
            Err(e) if e.to_string().contains("not found") => return Ok(InboxStatus::default()),
            Err(e) => {
                return Err(BusError::invalid(format!(
                    "could not read the inbox stream: {e}"
                )));
            }
        };
        let durable = format!("IN_{recipient_key}");
        let mut consumer = match stream
            .get_consumer::<jetstream::consumer::pull::Config>(&durable)
            .await
        {
            Ok(consumer) => consumer,
            Err(e) if e.to_string().contains("not found") => return Ok(InboxStatus::default()),
            Err(e) => {
                return Err(BusError::invalid(format!(
                    "could not read this window's consumer: {e}"
                )));
            }
        };
        let info = consumer
            .info()
            .await
            .map_err(|e| BusError::invalid(format!("could not read the inbox state: {e}")))?;
        Ok(InboxStatus {
            pending: info.num_pending,
            awaiting_ack: info.num_ack_pending as u64,
            redelivered: info.num_redelivered as u64,
            present: true,
        })
    }

    /// Remove a team's stream. Operator action, and a destructive one: it
    /// drops every body the stream holds.
    pub async fn deprovision(config: &Config, team_id: Uuid) -> BusResult<()> {
        let client = connect_client(config).await?;
        let context = jetstream::new(client);
        // Both streams, and both report. An operator told "removed" while
        // authorization was refused believes the data is gone.
        for name in [stream_name(team_id), inbox_stream_name(team_id)] {
            match context.delete_stream(&name).await {
                Ok(_) => {}
                // Already gone is the outcome the caller asked for.
                Err(e) if e.to_string().contains("not found") => {}
                Err(e) => {
                    return Err(BusError::invalid(format!(
                        "could not delete '{name}' ({e}). The stream and its contents are \
                         still there."
                    )));
                }
            }
        }
        Ok(())
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

    /// Can this process reach the broker at all? For health checks, which
    /// is why it opens and drops a connection rather than reusing a
    /// team's: it answers a question about the deployment, not about a
    /// stream.
    pub async fn reachable(config: &Config) -> bool {
        connect_client(config).await.is_ok()
    }
}

/// Turn a create or update failure into something an operator can act on.
/// The one that bites in practice is 10047, "insufficient storage
/// resources": the broker reserves every stream's `max_bytes` against its
/// `max_file_store` when the stream is created or enlarged, so a store can
/// be almost empty and still refuse. Say so, with the numbers when the
/// account answers and with an honest "unavailable" when it does not.
/// `additional_bytes` is what the operation asked the broker to reserve on
/// top of what it already had: the whole quota for a new stream, the
/// difference for an update.
async fn provisioning_error(
    context: &jetstream::Context,
    name: &str,
    operation: &str,
    additional_bytes: i64,
    error: jetstream::context::CreateStreamError,
) -> BusError {
    let exhausted = matches!(
        error.kind(),
        jetstream::context::CreateStreamErrorKind::JetStream(e)
            if e.error_code() == jetstream::ErrorCode::STORAGE_RESOURCES_EXCEEDED
    );
    if !exhausted {
        return BusError::invalid(format!("could not {operation} '{name}': {error}"));
    }
    let account = match context.query_account().await {
        Ok(a) => StorageAccount {
            used: a.storage,
            reserved: a.reserved_storage,
            budget: a.limits.max_storage.filter(|m| *m > 0),
        }
        .describe(),
        Err(e) => format!(
            "the broker's account statistics are unavailable ({e}), so the reserved and used \
             figures cannot be shown; `nats account info` on the broker has them"
        ),
    };
    BusError::invalid(format!(
        "could not {operation} '{name}': the broker cannot reserve {} ({}) more. Reservation, \
         not disk, is what ran out: {account}. Ask for a smaller quota (--max-bytes / \
         --inbox-max-bytes), lower another stream's quota (`team stream --update-quotas`) or \
         remove one, or raise the broker's max_file_store.",
        additional_bytes,
        format_size(additional_bytes)
    ))
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

    async fn fetch(&self, locator: &Locator, message_id: Uuid) -> BusResult<Option<String>> {
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
                // And the message the caller asked for. A sequence in the
                // right stream is not proof that it is the right body: a
                // locator from another conversation of the same team would
                // otherwise return whatever sits at that position.
                let expected = message
                    .headers
                    .get("Acs-Message-Id")
                    .map(|v| v.as_str() == message_id.to_string())
                    .unwrap_or(false);
                if !expected {
                    return Err(BusError::Forbidden(
                        "that locator names another message".to_owned(),
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
    fn sizes_parse_in_binary_units_and_refuse_nonsense() {
        assert_eq!(parse_size("1048576").unwrap(), 1024 * 1024);
        assert_eq!(parse_size("64MiB").unwrap(), 64 * 1024 * 1024);
        assert_eq!(parse_size("64 mib").unwrap(), 64 * 1024 * 1024);
        assert_eq!(parse_size("2G").unwrap(), 2 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("512kb").unwrap(), 512 * 1024);
        assert!(parse_size("").unwrap_err().contains("not a size"));
        assert!(parse_size("MiB").unwrap_err().contains("not a size"));
        assert!(
            parse_size("12 parsecs")
                .unwrap_err()
                .contains("unknown unit")
        );
        assert!(
            parse_size("99999999999999999999")
                .unwrap_err()
                .contains("too large")
        );
        assert_eq!(format_size(2 * 1024 * 1024 * 1024), "2 GiB");
        assert_eq!(format_size(256 * 1024 * 1024), "256 MiB");
        assert_eq!(format_size(1536 * 1024), "1536 KiB");
        assert_eq!(format_size(5_289_810), "5.0 MiB");
    }

    #[test]
    fn quotas_are_validated_before_the_broker_is_asked() {
        let ok = Config::new("nats://x").with_limits(10, 4 * 1024 * 1024);
        assert!(ok.validate_quotas().is_ok());
        let tiny = Config::new("nats://x").with_limits(10, MAX_BROKER_MESSAGE_BYTES - 1);
        let err = tiny.validate_quotas().unwrap_err().to_string();
        assert!(err.contains("--max-bytes must be at least"), "{err}");
        let inbox = Config::new("nats://x").with_inbox_limits(10, MIN_INBOX_BYTES - 1);
        let err = inbox.validate_quotas().unwrap_err().to_string();
        assert!(err.contains("--inbox-max-bytes"), "{err}");
        let none = Config::new("nats://x").with_limits(0, 4 * 1024 * 1024);
        let err = none.validate_quotas().unwrap_err().to_string();
        assert!(err.contains("at least 1"), "{err}");
        let defaults = Config::new("nats://x");
        assert_eq!(defaults.inbox_max_bytes, DEFAULT_INBOX_MAX_BYTES);
        assert!(defaults.inbox_max_bytes < defaults.max_bytes);
    }

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
