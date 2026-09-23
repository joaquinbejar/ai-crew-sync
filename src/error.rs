use rmcp::ErrorData;

/// Errors produced by the store layer.
///
/// These map onto MCP tool errors so that the calling agent gets a useful
/// message instead of an opaque "internal error".
#[derive(Debug, thiserror::Error)]
pub enum BusError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("invalid input: {0}")]
    Invalid(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("unauthenticated: {0}")]
    Unauthenticated(String),

    /// The caller is known but not allowed: a team-scoped administrative
    /// credential reaching for another team, or for a global-only action.
    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
}

impl BusError {
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::NotFound(msg.into())
    }
    pub fn invalid(msg: impl Into<String>) -> Self {
        Self::Invalid(msg.into())
    }
    pub fn conflict(msg: impl Into<String>) -> Self {
        Self::Conflict(msg.into())
    }
    pub fn forbidden(msg: impl Into<String>) -> Self {
        Self::Forbidden(msg.into())
    }
}

/// The one boundary where a store error becomes what a caller reads.
///
/// The rule every message crossing it keeps: it is written for its reader —
/// the model that made the call, or the human that model works for — and
/// says what to do. Text written for someone else (this server's operator,
/// the broker, the transport) is never forwarded into a tool result, not
/// even interpolated as `({e})`: it is rewritten where it crosses, in the
/// caller's terms, and the original goes to `tracing`. `Db` below is the
/// general case; `broker_unreadable_note` in `store/inbox.rs` and
/// `remote_error_text` in `proxy.rs` are the same rule at the two other
/// boundaries a tool result can cross.
impl From<BusError> for ErrorData {
    fn from(err: BusError) -> Self {
        match err {
            BusError::NotFound(m) => ErrorData::invalid_params(format!("not found: {m}"), None),
            BusError::Invalid(m) => ErrorData::invalid_params(m, None),
            BusError::Conflict(m) => ErrorData::invalid_params(format!("conflict: {m}"), None),
            BusError::Unauthenticated(m) => {
                ErrorData::invalid_request(format!("unauthenticated: {m}"), None)
            }
            BusError::Forbidden(m) => ErrorData::invalid_request(format!("forbidden: {m}"), None),
            BusError::Db(e) => {
                // Never leak SQL/connection detail to the model; log it instead.
                tracing::error!(error = %e, "database error");
                ErrorData::internal_error("database error", None)
            }
        }
    }
}

pub type BusResult<T> = Result<T, BusError>;
