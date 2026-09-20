use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use rand::Rng;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

pub const TOKEN_PREFIX: &str = "acs_";

/// Prefix of an administrative credential. Deliberately not an extension of
/// [`TOKEN_PREFIX`]: `acsa_` does not start with `acs_`, so an administrative
/// credential presented to `/mcp` fails the prefix check before any lookup,
/// and an agent token presented to `/admin` does the same. The two classes
/// live in different tables and never resolve as each other.
pub const ADMIN_TOKEN_PREFIX: &str = "acsa_";

/// Header carrying the working context of the caller — in practice one per
/// repository. Set once in an MCP client's configuration, it then rides on
/// every request, which is the only option available: the transport is
/// stateless, so there is nothing to negotiate once and remember.
pub const SESSION_HEADER: &str = "x-crew-session";

/// A session is a label, not a document. Long enough for a repository name.
pub const MAX_SESSION_BYTES: usize = 64;

/// Identity resolved from the bearer token, injected into the HTTP request
/// extensions so tool handlers can read it. Every tool call is scoped to this.
#[derive(Clone, Debug)]
pub struct AuthCtx {
    pub agent_id: Uuid,
    pub agent_name: String,
    pub team_id: Uuid,
    pub team_slug: String,
    /// Which of the agent's concurrent working contexts is calling. Empty is
    /// the shared session: what every client that sends no header gets, and
    /// what every row created before sessions existed carries.
    ///
    /// This is deliberately *not* identity. It arrives from a header rather
    /// than from the token, so it is caller-controlled and must never be used
    /// to decide **which agent** is speaking — only to partition that agent's
    /// own presence, claims and locks.
    pub session: String,
}

/// Read the session label from the request headers.
///
/// Normalised the way channel names are (trimmed, lower-cased) so that
/// `Market-Data` and `market-data` are one session rather than two that
/// silently fail to see each other's claims.
/// Normalise and validate a session label, wherever it arrives from.
///
/// Shared by the `X-Crew-Session` header and by the `agent/session` half of a
/// message address: a label a header would reject must not be reachable by
/// addressing it instead, or a caller could store sessions that can never
/// exist, and unbounded strings with them.
///
/// Normalised the way channel names are (trimmed, lower-cased) so that
/// `Market-Data` and `market-data` are one session rather than two that
/// silently fail to see each other's claims. Returns the reason on rejection
/// so each caller can wrap it in its own error type.
pub fn normalize_session(raw: &str) -> Result<String, String> {
    let label = raw.trim().to_lowercase();
    if label.is_empty() {
        return Ok(String::new());
    }
    if label.len() > MAX_SESSION_BYTES {
        return Err(format!(
            "is {} bytes; the limit is {MAX_SESSION_BYTES}. Use a short label, \
             such as the repository name",
            label.len()
        ));
    }
    if !label.is_ascii() {
        return Err("must be ASCII".to_owned());
    }
    if label.chars().any(char::is_control) {
        return Err("must not contain control characters".to_owned());
    }
    // A client whose config format has no default syntax sends the template
    // itself when the variable is unset. Silently becoming a session named
    // '${bus_session}' would split presence and claims for a reason nobody
    // would think to look for.
    if label.contains(['$', '{', '}']) {
        return Err(
            "looks like an unexpanded variable. Set the variable, or use a form \
                    with a fallback such as ${BUS_SESSION:-} so an unset value sends \
                    nothing at all"
                .to_owned(),
        );
    }
    // Reserved: a direct message addresses `agent/session`, so a session
    // containing a slash would make that address ambiguous — and it is what
    // keeps the read-cursor keys collision-free.
    if label.contains('/') {
        return Err(
            "must not contain '/', which separates agent from session when \
                    addressing a message"
                .to_owned(),
        );
    }
    Ok(label)
}

/// Read the session label from the request headers.
fn session_from_headers(headers: &axum::http::HeaderMap) -> Result<String, AuthError> {
    let Some(value) = headers.get(SESSION_HEADER) else {
        return Ok(String::new());
    };
    let raw = value
        .to_str()
        .map_err(|_| AuthError::BadSession("must be ASCII".to_owned()))?;
    normalize_session(raw).map_err(AuthError::BadSession)
}

/// Generate a fresh opaque token. Returned once, never stored in the clear.
pub fn generate_token() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    format!("{TOKEN_PREFIX}{}", hex::encode(bytes))
}

/// Generate a fresh administrative credential. Same entropy and hashing as
/// an agent token; only the prefix differs.
pub fn generate_admin_token() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    format!("{ADMIN_TOKEN_PREFIX}{}", hex::encode(bytes))
}

pub fn hash_token(raw: &str) -> Vec<u8> {
    Sha256::digest(raw.trim().as_bytes()).to_vec()
}

/// First 12 characters, kept in plaintext purely so humans can tell tokens
/// apart in `ai-crew-sync token list`.
pub fn token_prefix(raw: &str) -> String {
    raw.chars().take(12).collect()
}

struct AuthRow {
    token_id: Uuid,
    agent_id: Uuid,
    agent_name: String,
    agent_disabled: bool,
    team_id: Uuid,
    team_slug: String,
}

pub async fn resolve_token(pool: &PgPool, raw: &str) -> Result<AuthCtx, AuthError> {
    if !raw.starts_with(TOKEN_PREFIX) {
        return Err(AuthError::Invalid);
    }
    let hash = hash_token(raw);

    let row = sqlx::query_as::<
        _,
        (
            Uuid,
            Uuid,
            String,
            Option<chrono::DateTime<chrono::Utc>>,
            Uuid,
            String,
        ),
    >(
        r#"
        SELECT t.id, a.id, a.name, a.disabled_at, tm.id, tm.slug
        FROM api_tokens t
        JOIN agents a ON a.id = t.agent_id
        JOIN teams tm ON tm.id = a.team_id
        WHERE t.token_hash = $1 AND t.revoked_at IS NULL
        "#,
    )
    .bind(&hash)
    .fetch_optional(pool)
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "token lookup failed");
        AuthError::Internal
    })?;

    let Some((token_id, agent_id, agent_name, disabled_at, team_id, team_slug)) = row else {
        return Err(AuthError::Invalid);
    };
    let row = AuthRow {
        token_id,
        agent_id,
        agent_name,
        agent_disabled: disabled_at.is_some(),
        team_id,
        team_slug,
    };

    if row.agent_disabled {
        return Err(AuthError::Disabled);
    }

    // Best-effort: record usage without blocking the request path on failure.
    let _ = sqlx::query("UPDATE api_tokens SET last_used_at = now() WHERE id = $1")
        .bind(row.token_id)
        .execute(pool)
        .await;

    Ok(AuthCtx {
        agent_id: row.agent_id,
        agent_name: row.agent_name,
        team_id: row.team_id,
        team_slug: row.team_slug,
        // Filled in by the middleware from the request headers; the token
        // itself says nothing about which session is using it.
        session: String::new(),
    })
}

#[derive(Debug)]
pub enum AuthError {
    Missing,
    Invalid,
    Disabled,
    Internal,
    /// Too many requests for this token; carries the seconds to wait.
    Throttled(u64),
    /// The `X-Crew-Session` header is present but unusable; carries what is
    /// wrong with it.
    BadSession(String),
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let retry_after = match self {
            AuthError::Throttled(secs) => Some(secs),
            _ => None,
        };
        // Decided before the match below, which consumes `self`.
        let is_auth_challenge = matches!(self, AuthError::Missing | AuthError::Invalid);
        // The consumer is a language model: say what to do, not just what
        // went wrong.
        let (status, msg) = match self {
            AuthError::Missing => (StatusCode::UNAUTHORIZED, "missing bearer token".to_owned()),
            AuthError::Invalid => (
                StatusCode::UNAUTHORIZED,
                "invalid or revoked token".to_owned(),
            ),
            AuthError::Disabled => (StatusCode::FORBIDDEN, "agent is disabled".to_owned()),
            AuthError::Internal => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal error".to_owned(),
            ),
            AuthError::Throttled(secs) => (
                StatusCode::TOO_MANY_REQUESTS,
                format!(
                    "rate limit exceeded for this token; retry in {secs}s. \
                     If you are polling, use wait_for_updates (it blocks until \
                     something happens) instead of calling in a loop."
                ),
            ),
            AuthError::BadSession(why) => (
                StatusCode::BAD_REQUEST,
                format!(
                    "the {SESSION_HEADER} header {why}. It labels which of your \
                     concurrent working contexts is calling — one per repository \
                     is the usual choice. Omit it entirely to use the shared session."
                ),
            ),
        };
        let body = serde_json::json!({ "error": msg });
        let mut resp = (status, axum::Json(body)).into_response();
        if is_auth_challenge {
            resp.headers_mut().insert(
                axum::http::header::WWW_AUTHENTICATE,
                axum::http::HeaderValue::from_static("Bearer"),
            );
        }
        if let Some(secs) = retry_after
            && let Ok(value) = axum::http::HeaderValue::from_str(&secs.to_string())
        {
            resp.headers_mut()
                .insert(axum::http::header::RETRY_AFTER, value);
        }
        resp
    }
}

/// State for [`require_bearer`]: the pool plus the optional rate limiter.
#[derive(Clone)]
pub struct AuthState {
    pub pool: PgPool,
    pub limiter: Option<crate::ratelimit::RateLimiter>,
}

/// Axum middleware: validates the bearer token, charges the per-token rate
/// limit, and inserts the resulting [`AuthCtx`] into the request extensions,
/// where rmcp tool handlers read it.
pub async fn require_bearer(
    State(state): State<AuthState>,
    mut req: Request,
    next: Next,
) -> Result<Response, AuthError> {
    let raw = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            v.strip_prefix("Bearer ")
                .or_else(|| v.strip_prefix("bearer "))
        })
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or(AuthError::Missing)?
        .to_owned();

    // Charge the bucket on the token's hash before touching the database, so
    // a flood of invalid tokens costs no query either.
    if let Some(limiter) = &state.limiter
        && let Err(throttled) = limiter.check(&hex::encode(hash_token(&raw)))
    {
        return Err(AuthError::Throttled(throttled.retry_after_secs));
    }

    // Validated before the token lookup: a malformed header is the caller's
    // mistake either way, and rejecting it costs no query.
    let session = session_from_headers(req.headers())?;

    let mut ctx = resolve_token(&state.pool, &raw).await?;
    ctx.session = session;
    tracing::debug!(
        agent = %ctx.agent_name,
        team = %ctx.team_slug,
        session = %ctx.session,
        "authenticated"
    );
    req.extensions_mut().insert(ctx);
    Ok(next.run(req).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderValue};

    fn headers(value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(SESSION_HEADER, HeaderValue::from_str(value).unwrap());
        h
    }

    fn err(value: &str) -> String {
        match session_from_headers(&headers(value)) {
            Err(AuthError::BadSession(why)) => why,
            other => panic!("expected BadSession, got {other:?}"),
        }
    }

    #[test]
    fn absent_header_is_the_shared_session() {
        assert_eq!(session_from_headers(&HeaderMap::new()).unwrap(), "");
    }

    #[test]
    fn blank_header_is_the_shared_session() {
        // A client interpolating an unset variable sends whitespace, not a
        // missing header. That must not become a session named " ".
        assert_eq!(session_from_headers(&headers("   ")).unwrap(), "");
    }

    #[test]
    fn label_is_normalised_like_a_channel_name() {
        // Otherwise `Market-Data` and `market-data` are two sessions that
        // cannot see each other's claims.
        assert_eq!(
            session_from_headers(&headers("  Market-Data  ")).unwrap(),
            "market-data"
        );
    }

    #[test]
    fn over_long_label_is_rejected_with_the_limit() {
        let why = err(&"a".repeat(MAX_SESSION_BYTES + 1));
        assert!(why.contains(&MAX_SESSION_BYTES.to_string()), "{why}");
    }

    #[test]
    fn label_at_the_limit_is_accepted() {
        let label = "a".repeat(MAX_SESSION_BYTES);
        assert_eq!(session_from_headers(&headers(&label)).unwrap(), label);
    }

    #[test]
    fn an_unexpanded_template_is_rejected_rather_than_becoming_a_session() {
        let why = err("${BUS_SESSION}");
        assert!(why.contains("unexpanded"), "{why}");
    }

    #[test]
    fn slash_is_rejected_because_it_separates_agent_from_session() {
        assert!(err("joaquin/market-data").contains('/'));
    }

    #[test]
    fn internal_control_character_is_rejected() {
        // HTTP permits a tab inside a field value, and trimming only removes
        // the ones at the edges.
        assert!(err("market\tdata").contains("control"));
    }

    #[test]
    fn non_ascii_header_is_rejected() {
        let mut h = HeaderMap::new();
        h.insert(
            SESSION_HEADER,
            HeaderValue::from_bytes(&[0xff, 0xfe]).unwrap(),
        );
        match session_from_headers(&h) {
            Err(AuthError::BadSession(_)) => {}
            other => panic!("expected BadSession, got {other:?}"),
        }
    }
}
