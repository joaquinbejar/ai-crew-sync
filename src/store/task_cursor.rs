//! Versioned opaque cursors for task-list keyset pagination.
//!
//! This module is intentionally standalone. Integration with task storage and
//! MCP/CLI layers belongs to the pagination integration barrier.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

const CURSOR_VERSION: u8 = 1;
const MAC_SIZE: usize = 32;
const MAX_SEARCH_BYTES: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCursorFilters {
    pub project_prefix: Option<String>,
    pub status: Option<String>,
    pub mine: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search_fields: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search_language: Option<String>,
}

impl Default for TaskCursorFilters {
    fn default() -> Self {
        Self {
            project_prefix: None,
            status: None,
            mine: false,
            search: None,
            search_mode: None,
            search_fields: None,
            search_language: None,
        }
    }
}

impl TaskCursorFilters {
    /// Return the canonical representation used for signing and replay checks.
    /// Empty search is the legacy no-search state; search option defaults are
    /// materialized only when a non-empty search is present.
    pub fn normalized(&self) -> Result<Self, CursorError> {
        let search = self
            .search
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        let Some(search) = search else {
            if self.search_mode.is_some()
                || self.search_fields.is_some()
                || self.search_language.is_some()
            {
                return Err(CursorError::InvalidSearchOptions);
            }
            return Ok(Self {
                project_prefix: normalize_optional(self.project_prefix.as_deref()),
                status: normalize_optional(self.status.as_deref()),
                mine: self.mine,
                ..Self::default()
            });
        };

        if search.len() > MAX_SEARCH_BYTES {
            return Err(CursorError::InvalidSearchOptions);
        }
        Ok(Self {
            project_prefix: normalize_optional(self.project_prefix.as_deref()),
            status: normalize_optional(self.status.as_deref()),
            mine: self.mine,
            search: Some(search),
            search_mode: Some(normalize_choice(
                self.search_mode.as_deref(),
                "keywords",
                &["keywords", "contains", "regex"],
            )?),
            search_fields: Some(normalize_choice(
                self.search_fields.as_deref(),
                "both",
                &["title", "description", "both"],
            )?),
            search_language: Some(normalize_choice(
                self.search_language.as_deref(),
                "simple",
                &["simple", "english", "russian"],
            )?),
        })
    }
}

fn normalize_optional(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn normalize_choice(
    value: Option<&str>,
    default: &str,
    allowed: &[&str],
) -> Result<String, CursorError> {
    let value = value.unwrap_or(default).trim().to_ascii_lowercase();
    if allowed.contains(&value.as_str()) {
        Ok(value)
    } else {
        Err(CursorError::InvalidSearchOptions)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCursorContext {
    pub team_id: String,
    #[serde(default)]
    pub filters: TaskCursorFilters,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskOrder {
    /// Frozen SQL ordering: legacy_status_rank ASC, updated_at DESC, task_key ASC.
    pub status_rank: i32,
    pub updated_at_micros: i64,
    /// Unique task key tie breaker; never an ambiguous display id.
    pub task_key: String,
}

impl TaskOrder {
    /// Compatibility constructor for callers that do not yet provide a rank.
    /// SQL integrations must use `from_sql` so rank is carried into the cursor.
    pub fn new(updated_at_micros: i64, task_key: impl Into<String>) -> Self {
        Self {
            status_rank: 0,
            updated_at_micros,
            task_key: task_key.into(),
        }
    }

    /// Constructor matching the SQL keyset tuple exactly.
    pub fn from_sql(status_rank: i32, updated_at_micros: i64, task_key: impl Into<String>) -> Self {
        Self {
            status_rank,
            updated_at_micros,
            task_key: task_key.into(),
        }
    }

    pub fn cmp_key(&self, other: &Self) -> std::cmp::Ordering {
        self.status_rank
            .cmp(&other.status_rank)
            .then_with(|| other.updated_at_micros.cmp(&self.updated_at_micros))
            .then_with(|| self.task_key.as_bytes().cmp(other.task_key.as_bytes()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorLifetime {
    pub issued_at: i64,
    pub expires_at: i64,
}

impl CursorLifetime {
    pub fn new(issued_at: i64, expires_at: i64) -> Result<Self, CursorError> {
        if issued_at > expires_at {
            return Err(CursorError::InvalidLifetime);
        }
        Ok(Self {
            issued_at,
            expires_at,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CursorPayload {
    version: u8,
    team_id: String,
    filters: TaskCursorFilters,
    order: TaskOrder,
    lifetime: CursorLifetime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedTaskCursor {
    pub version: u8,
    pub context: TaskCursorContext,
    pub order: TaskOrder,
    pub lifetime: CursorLifetime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CursorError {
    EmptySecret,
    EmptyTeam,
    EmptyTaskKey,
    InvalidFormat,
    InvalidEncoding,
    InvalidSignature,
    MalformedPayload,
    UnsupportedVersion(u8),
    InvalidSearchOptions,
    ContextMismatch,
    InvalidLifetime,
    NotYetValid,
    Expired,
}

impl fmt::Display for CursorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptySecret => f.write_str("cursor signing secret is empty"),
            Self::EmptyTeam => f.write_str("cursor team is empty"),
            Self::EmptyTaskKey => f.write_str("cursor task key is empty"),
            Self::InvalidFormat => f.write_str("cursor must contain payload and signature"),
            Self::InvalidEncoding => f.write_str("cursor contains invalid base64"),
            Self::InvalidSignature => f.write_str("cursor signature is invalid"),
            Self::MalformedPayload => f.write_str("cursor payload is malformed"),
            Self::UnsupportedVersion(v) => write!(f, "unsupported cursor version {v}"),
            Self::InvalidSearchOptions => f.write_str("cursor search options are invalid"),
            Self::ContextMismatch => f.write_str("cursor does not match the requested context"),
            Self::InvalidLifetime => f.write_str("cursor lifetime is invalid"),
            Self::NotYetValid => f.write_str("cursor is not yet valid"),
            Self::Expired => f.write_str("cursor has expired"),
        }
    }
}

impl std::error::Error for CursorError {}

pub fn encode_cursor_with_lifetime(
    secret: &[u8],
    context: &TaskCursorContext,
    order: &TaskOrder,
    lifetime: CursorLifetime,
) -> Result<String, CursorError> {
    validate_inputs(secret, context, order, lifetime)?;
    let filters = context.filters.normalized()?;
    let payload = CursorPayload {
        version: CURSOR_VERSION,
        team_id: context.team_id.clone(),
        filters,
        order: order.clone(),
        lifetime,
    };
    let bytes = serde_json::to_vec(&payload).map_err(|_| CursorError::MalformedPayload)?;
    Ok(format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(&bytes),
        URL_SAFE_NO_PAD.encode(mac(secret, &bytes))
    ))
}

/// Compatibility constructor for callers without a TTL; new integrations use the explicit constructor.
pub fn encode_cursor(
    secret: &[u8],
    context: &TaskCursorContext,
    order: &TaskOrder,
) -> Result<String, CursorError> {
    encode_cursor_with_lifetime(
        secret,
        context,
        order,
        CursorLifetime {
            issued_at: 0,
            expires_at: i64::MAX,
        },
    )
}

/// Decode/authenticate and validate lifetime shape without selecting a clock.
pub fn decode_cursor(
    secret: &[u8],
    token: &str,
    expected: &TaskCursorContext,
) -> Result<DecodedTaskCursor, CursorError> {
    decode_cursor_internal(secret, token, expected)
}

/// Decode at a caller-supplied instant for deterministic not-before/expiry checks.
pub fn decode_cursor_at(
    secret: &[u8],
    token: &str,
    expected: &TaskCursorContext,
    now: i64,
) -> Result<DecodedTaskCursor, CursorError> {
    let decoded = decode_cursor_internal(secret, token, expected)?;
    if now < decoded.lifetime.issued_at {
        return Err(CursorError::NotYetValid);
    }
    if now >= decoded.lifetime.expires_at {
        return Err(CursorError::Expired);
    }
    Ok(decoded)
}

fn decode_cursor_internal(
    secret: &[u8],
    token: &str,
    expected: &TaskCursorContext,
) -> Result<DecodedTaskCursor, CursorError> {
    if secret.is_empty() {
        return Err(CursorError::EmptySecret);
    }
    if expected.team_id.is_empty() {
        return Err(CursorError::EmptyTeam);
    }
    let (payload_text, signature_text) = token.split_once('.').ok_or(CursorError::InvalidFormat)?;
    let bytes = URL_SAFE_NO_PAD
        .decode(payload_text)
        .map_err(|_| CursorError::InvalidEncoding)?;
    let signature = URL_SAFE_NO_PAD
        .decode(signature_text)
        .map_err(|_| CursorError::InvalidEncoding)?;
    if !constant_time_eq(&signature, &mac(secret, &bytes)) {
        return Err(CursorError::InvalidSignature);
    }
    let payload: CursorPayload =
        serde_json::from_slice(&bytes).map_err(|_| CursorError::MalformedPayload)?;
    if payload.version != CURSOR_VERSION {
        return Err(CursorError::UnsupportedVersion(payload.version));
    }
    if payload.team_id.is_empty() || payload.order.task_key.is_empty() {
        return Err(CursorError::MalformedPayload);
    }
    if payload.lifetime.issued_at > payload.lifetime.expires_at {
        return Err(CursorError::InvalidLifetime);
    }
    let payload_filters = payload.filters.normalized().map_err(|error| match error {
        CursorError::InvalidSearchOptions => CursorError::MalformedPayload,
        other => other,
    })?;
    let expected_filters = expected.filters.normalized()?;
    if payload.team_id != expected.team_id || payload_filters != expected_filters {
        return Err(CursorError::ContextMismatch);
    }
    Ok(DecodedTaskCursor {
        version: payload.version,
        context: TaskCursorContext {
            team_id: payload.team_id,
            filters: payload_filters,
        },
        order: payload.order,
        lifetime: payload.lifetime,
    })
}

fn validate_inputs(
    secret: &[u8],
    context: &TaskCursorContext,
    order: &TaskOrder,
    lifetime: CursorLifetime,
) -> Result<(), CursorError> {
    if secret.is_empty() {
        return Err(CursorError::EmptySecret);
    }
    if context.team_id.is_empty() {
        return Err(CursorError::EmptyTeam);
    }
    if order.task_key.is_empty() {
        return Err(CursorError::EmptyTaskKey);
    }
    if lifetime.issued_at > lifetime.expires_at {
        return Err(CursorError::InvalidLifetime);
    }
    Ok(())
}

fn mac(secret: &[u8], payload: &[u8]) -> [u8; MAC_SIZE] {
    let mut key = [0u8; 64];
    if secret.len() > key.len() {
        key[..MAC_SIZE].copy_from_slice(&Sha256::digest(secret));
    } else {
        key[..secret.len()].copy_from_slice(secret);
    }
    let mut inner = Sha256::new();
    for byte in &mut key {
        *byte ^= 0x36;
    }
    inner.update(key);
    inner.update(payload);
    let inner_digest = inner.finalize();
    for byte in &mut key {
        *byte ^= 0x36 ^ 0x5c;
    }
    let mut outer = Sha256::new();
    outer.update(key);
    outer.update(inner_digest);
    outer.finalize().into()
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |difference, (a, b)| difference | (a ^ b))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filters() -> TaskCursorFilters {
        TaskCursorFilters {
            project_prefix: Some("crew".into()),
            status: Some("open".into()),
            mine: true,
            ..TaskCursorFilters::default()
        }
    }

    fn context() -> TaskCursorContext {
        TaskCursorContext {
            team_id: "team-a".into(),
            filters: filters(),
        }
    }

    fn lifetime() -> CursorLifetime {
        CursorLifetime::new(1_000, 2_000).unwrap()
    }

    #[test]
    fn round_trip_preserves_rank_binding_tuple_and_lifetime() {
        let order = TaskOrder::from_sql(3, 42, "team-a:project:task-7");
        let token =
            encode_cursor_with_lifetime(b"server-secret", &context(), &order, lifetime()).unwrap();
        let decoded = decode_cursor_at(b"server-secret", &token, &context(), 1_500).unwrap();
        assert_eq!(decoded.version, CURSOR_VERSION);
        assert_eq!(decoded.context, context());
        assert_eq!(decoded.order, order);
        assert_eq!(decoded.lifetime, lifetime());
    }

    #[test]
    fn absent_search_keeps_legacy_filter_shape_and_defaults() {
        let token =
            encode_cursor(b"secret", &context(), &TaskOrder::new(1, "team-a:task:t")).unwrap();
        let decoded = decode_cursor(b"secret", &token, &context()).unwrap();
        assert_eq!(decoded.context.filters.search, None);
        assert_eq!(decoded.context.filters.search_mode, None);
        assert_eq!(decoded.context.filters.search_fields, None);
        assert_eq!(decoded.context.filters.search_language, None);
    }

    #[test]
    fn search_options_are_canonicalized_and_bound() {
        let mut requested = context();
        requested.filters.search = Some("  Mixed Case  ".into());
        requested.filters.search_mode = Some(" CONTAINS ".into());
        requested.filters.search_fields = Some(" TITLE ".into());
        requested.filters.search_language = Some(" ENGLISH ".into());
        let token =
            encode_cursor(b"secret", &requested, &TaskOrder::new(1, "team-a:task:t")).unwrap();
        let decoded = decode_cursor(b"secret", &token, &requested).unwrap();
        assert_eq!(
            decoded.context.filters.search.as_deref(),
            Some("Mixed Case")
        );
        assert_eq!(
            decoded.context.filters.search_mode.as_deref(),
            Some("contains")
        );
        assert_eq!(
            decoded.context.filters.search_fields.as_deref(),
            Some("title")
        );
        assert_eq!(
            decoded.context.filters.search_language.as_deref(),
            Some("english")
        );

        let mut canonical = context();
        canonical.filters.search = Some("Mixed Case".into());
        canonical.filters.search_mode = Some("contains".into());
        canonical.filters.search_fields = Some("title".into());
        canonical.filters.search_language = Some("english".into());
        assert!(decode_cursor(b"secret", &token, &canonical).is_ok());
    }

    #[test]
    fn every_search_option_mismatch_rejects_replay() {
        let mut requested = context();
        requested.filters.search = Some("alpha beta".into());
        let token =
            encode_cursor(b"secret", &requested, &TaskOrder::new(1, "team-a:task:t")).unwrap();
        for change in [
            (Some("different"), None, None, None),
            (None, Some("contains"), None, None),
            (None, None, Some("title"), None),
            (None, None, None, Some("english")),
        ] {
            let mut replay = requested.clone();
            if let Some(value) = change.0 {
                replay.filters.search = Some(value.into());
            }
            if let Some(value) = change.1 {
                replay.filters.search_mode = Some(value.into());
            }
            if let Some(value) = change.2 {
                replay.filters.search_fields = Some(value.into());
            }
            if let Some(value) = change.3 {
                replay.filters.search_language = Some(value.into());
            }
            assert_eq!(
                decode_cursor(b"secret", &token, &replay),
                Err(CursorError::ContextMismatch)
            );
        }
    }

    #[test]
    fn invalid_search_options_are_rejected_without_broadening() {
        let mut invalid = context();
        invalid.filters.search = Some("alpha".into());
        invalid.filters.search_mode = Some("unknown".into());
        assert_eq!(
            encode_cursor(b"secret", &invalid, &TaskOrder::new(1, "task")),
            Err(CursorError::InvalidSearchOptions)
        );

        let mut dangling = context();
        dangling.filters.search_mode = Some("contains".into());
        assert_eq!(
            encode_cursor(b"secret", &dangling, &TaskOrder::new(1, "task")),
            Err(CursorError::InvalidSearchOptions)
        );
    }

    #[test]
    fn tampering_wrong_secret_team_and_legacy_filters_are_rejected() {
        let token = encode_cursor_with_lifetime(
            b"secret",
            &context(),
            &TaskOrder::from_sql(2, 1, "team-a:task:t"),
            lifetime(),
        )
        .unwrap();
        let mut tampered = token.clone().into_bytes();
        let index = tampered.len() - 1;
        tampered[index] = if tampered[index] == b'A' { b'B' } else { b'A' };
        assert_eq!(
            decode_cursor_at(
                b"secret",
                &String::from_utf8(tampered).unwrap(),
                &context(),
                1_500
            ),
            Err(CursorError::InvalidSignature)
        );
        assert_eq!(
            decode_cursor_at(b"other", &token, &context(), 1_500),
            Err(CursorError::InvalidSignature)
        );
        let mut wrong_team = context();
        wrong_team.team_id = "team-b".into();
        assert_eq!(
            decode_cursor_at(b"secret", &token, &wrong_team, 1_500),
            Err(CursorError::ContextMismatch)
        );
        let mut wrong_filter = context();
        wrong_filter.filters.status = Some("claimed".into());
        assert_eq!(
            decode_cursor_at(b"secret", &token, &wrong_filter, 1_500),
            Err(CursorError::ContextMismatch)
        );
    }

    #[test]
    fn malformed_and_empty_fields_are_rejected() {
        assert_eq!(
            decode_cursor(b"secret", "not-a-cursor", &context()),
            Err(CursorError::InvalidFormat)
        );
        assert_eq!(
            decode_cursor(b"secret", "!!!.!!!", &context()),
            Err(CursorError::InvalidEncoding)
        );
        assert_eq!(
            encode_cursor(b"", &context(), &TaskOrder::from_sql(0, 1, "team-a:task:t")),
            Err(CursorError::EmptySecret)
        );
        assert_eq!(
            encode_cursor(b"secret", &context(), &TaskOrder::from_sql(0, 1, "")),
            Err(CursorError::EmptyTaskKey)
        );
        assert_eq!(
            CursorLifetime::new(2_000, 1_000),
            Err(CursorError::InvalidLifetime)
        );
    }

    #[test]
    fn expiry_is_deterministic_with_strict_boundaries() {
        let token = encode_cursor_with_lifetime(
            b"secret",
            &context(),
            &TaskOrder::from_sql(1, 1, "team-a:task:t"),
            lifetime(),
        )
        .unwrap();
        assert_eq!(
            decode_cursor_at(b"secret", &token, &context(), 999),
            Err(CursorError::NotYetValid)
        );
        assert!(decode_cursor_at(b"secret", &token, &context(), 1_000).is_ok());
        assert!(decode_cursor_at(b"secret", &token, &context(), 1_999).is_ok());
        assert_eq!(
            decode_cursor_at(b"secret", &token, &context(), 2_000),
            Err(CursorError::Expired)
        );
    }

    #[test]
    fn frozen_rank_time_keyset_order_is_deterministic() {
        let rank_first = TaskOrder::from_sql(1, 999, "team-a:task:z");
        let rank_second = TaskOrder::from_sql(2, 1, "team-a:task:a");
        assert!(rank_first.cmp_key(&rank_second).is_lt());
        let newer = TaskOrder::from_sql(1, 20, "team-a:task:z");
        let older = TaskOrder::from_sql(1, 10, "team-a:task:a");
        assert!(newer.cmp_key(&older).is_lt());
        let key_a = TaskOrder::from_sql(1, 10, "team-a:task:a");
        let key_b = TaskOrder::from_sql(1, 10, "team-a:task:b");
        assert!(key_a.cmp_key(&key_b).is_lt());
    }

    #[test]
    fn version_tampering_is_rejected() {
        let token = encode_cursor_with_lifetime(
            b"secret",
            &context(),
            &TaskOrder::from_sql(1, 1, "team-a:task:t"),
            lifetime(),
        )
        .unwrap();
        let (payload, signature) = token.split_once('.').unwrap();
        let mut bytes = URL_SAFE_NO_PAD.decode(payload).unwrap();
        let version_index = bytes.iter().position(|byte| *byte == b'1').unwrap();
        bytes[version_index] = b'2';
        let rewritten = format!("{}.{}", URL_SAFE_NO_PAD.encode(bytes), signature);
        assert_eq!(
            decode_cursor(b"secret", &rewritten, &context()),
            Err(CursorError::InvalidSignature)
        );
    }
}
