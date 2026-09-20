//! Remote administration: `/admin/*`.
//!
//! A plain JSON API, deliberately **not** MCP tools. Administration must not
//! appear in any agent's tool catalogue, and an agent token must never be
//! able to mint a credential — not even for its own agent. The only bearer
//! this surface accepts is an administrative credential ([`AdminCtx`]),
//! which in turn cannot do anything on `/mcp`.
//!
//! Every permission decision is made here, from the credential alone. The
//! team in a path is a slug the credential must be allowed to administer;
//! the request body, a UUID or a query string can never widen that.
//!
//! Same shape as the MCP surface: stateless, one bearer header, so a bare
//! `curl` always works.

use axum::{
    Json, Router,
    extract::{
        Path, Request, State,
        rejection::{JsonRejection, PathRejection},
    },
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    routing::get,
};
use serde::Deserialize;
use sqlx::PgPool;
use tower_http::limit::RequestBodyLimitLayer;
use uuid::Uuid;

use crate::{
    auth::{ADMIN_TOKEN_PREFIX, TOKEN_PREFIX, hash_token},
    error::BusError,
    ratelimit::RateLimiter,
    store::admin::{self as store, Actor, AdminCtx},
};

/// The `/admin` rate limit is this fraction of the MCP one
/// (`BUS_RATE_LIMIT_PER_MINUTE`): 600/min for agents means 60/min for
/// administration. Administration is a handful of calls a day, so a leaked or
/// looping credential cannot mint at speed, and there is no second knob to
/// forget. Setting the MCP limit to 0 disables both.
pub const ADMIN_RATE_LIMIT_DIVISOR: u32 = 10;

/// Largest `/admin` request body. Names and labels; nothing here is a
/// document.
pub const MAX_ADMIN_REQUEST_BYTES: usize = 16 * 1024;

#[derive(Clone)]
pub struct AdminApiState {
    pub pool: PgPool,
    limiter: Option<RateLimiter>,
}

/// Errors on this surface, rendered as `{"error": "..."}` with a status.
#[derive(Debug)]
pub enum ApiError {
    Unauthorized(String),
    Forbidden(String),
    NotFound(String),
    BadRequest(String),
    Conflict(String),
    Throttled(u64),
    Internal,
}

impl From<BusError> for ApiError {
    fn from(err: BusError) -> Self {
        match err {
            BusError::NotFound(m) => ApiError::NotFound(m),
            BusError::Invalid(m) => ApiError::BadRequest(m),
            BusError::Conflict(m) => ApiError::Conflict(m),
            BusError::Unauthenticated(m) => ApiError::Unauthorized(m),
            BusError::Forbidden(m) => ApiError::Forbidden(m),
            BusError::Db(e) => {
                // Never leak SQL/connection detail; log it instead.
                tracing::error!(error = %e, "database error on /admin");
                ApiError::Internal
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let retry_after = match self {
            ApiError::Throttled(secs) => Some(secs),
            _ => None,
        };
        let is_challenge = matches!(self, ApiError::Unauthorized(_));
        let (status, msg) = match self {
            ApiError::Unauthorized(m) => (StatusCode::UNAUTHORIZED, m),
            ApiError::Forbidden(m) => (StatusCode::FORBIDDEN, m),
            ApiError::NotFound(m) => (StatusCode::NOT_FOUND, m),
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            ApiError::Conflict(m) => (StatusCode::CONFLICT, m),
            ApiError::Throttled(secs) => (
                StatusCode::TOO_MANY_REQUESTS,
                format!("rate limit exceeded for this credential; retry in {secs}s"),
            ),
            ApiError::Internal => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal error".to_owned(),
            ),
        };
        let mut resp = (status, Json(serde_json::json!({ "error": msg }))).into_response();
        if is_challenge {
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

type ApiResult<T> = Result<T, ApiError>;

/// Extractor rejections become the same `{"error": …}` shape as everything
/// else on this surface. Handlers take `Result<Json<T>, JsonRejection>` and
/// `Result<Path<T>, PathRejection>` and unwrap them through these.
impl From<JsonRejection> for ApiError {
    fn from(r: JsonRejection) -> Self {
        ApiError::BadRequest(format!(
            "invalid JSON body: {}. Send an object with Content-Type: application/json",
            r.body_text()
        ))
    }
}

impl From<PathRejection> for ApiError {
    fn from(r: PathRejection) -> Self {
        ApiError::BadRequest(format!("invalid path parameter: {}", r.body_text()))
    }
}

/// What a JSON body or a path yields once its rejection is mapped.
type Body<T> = Result<Json<T>, JsonRejection>;
type Params<T> = Result<Path<T>, PathRejection>;

/// Middleware: only an active administrative credential gets through. An
/// agent token is refused with a message that says what to use instead.
async fn require_admin(
    State(state): State<AdminApiState>,
    mut req: Request,
    next: Next,
) -> ApiResult<Response> {
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
        .ok_or_else(|| {
            ApiError::Unauthorized(
                "missing bearer credential; /admin needs an administrative credential \
                 (acsa_…), minted with `ai-crew-sync admin bootstrap` or `admin grant`"
                    .to_owned(),
            )
        })?
        .to_owned();

    if raw.starts_with(TOKEN_PREFIX) {
        return Err(ApiError::Unauthorized(
            "this is an agent token; agent tokens cannot administer the bus. /admin needs \
             an administrative credential (acsa_…), minted with `ai-crew-sync admin \
             bootstrap` or `admin grant`"
                .to_owned(),
        ));
    }
    if !raw.starts_with(ADMIN_TOKEN_PREFIX) {
        return Err(ApiError::Unauthorized(
            "invalid or revoked administrative credential".to_owned(),
        ));
    }

    // Charged before the lookup, on the hash: a flood of bad credentials
    // costs no query either.
    if let Some(limiter) = &state.limiter
        && let Err(throttled) = limiter.check(&hex::encode(hash_token(&raw)))
    {
        return Err(ApiError::Throttled(throttled.retry_after_secs));
    }

    let ctx = store::resolve_admin(&state.pool, &raw)
        .await?
        .ok_or_else(|| {
            ApiError::Unauthorized("invalid or revoked administrative credential".to_owned())
        })?;
    tracing::debug!(
        credential = %ctx.id,
        team = ctx.team_slug.as_deref().unwrap_or("(global)"),
        "administrator authenticated"
    );
    req.extensions_mut().insert(ctx);
    Ok(next.run(req).await)
}

/// The credential the middleware resolved. Absent only if a route was mounted
/// outside the middleware, which is a programming error worth failing loudly
/// on — as 401, never as an open door.
fn ctx(req_ctx: Option<axum::Extension<AdminCtx>>) -> ApiResult<AdminCtx> {
    req_ctx.map(|e| e.0).ok_or_else(|| {
        tracing::error!("/admin handler reached without an AdminCtx");
        ApiError::Unauthorized("missing administrative credential".to_owned())
    })
}

fn require_global(ctx: &AdminCtx, what: &str) -> ApiResult<()> {
    if ctx.is_global() {
        return Ok(());
    }
    Err(ApiError::Forbidden(format!(
        "{what} needs a global administrative credential; this one administers team '{}' only",
        ctx.team_slug.as_deref().unwrap_or_default()
    )))
}

/// Resolve the team a request names, as the credential is allowed to see it.
/// A team credential only ever gets its own team back, whatever the path
/// says, and learns nothing about whether another slug exists.
async fn scoped_team(pool: &PgPool, ctx: &AdminCtx, slug: &str) -> ApiResult<Uuid> {
    let slug = slug.trim().to_lowercase();
    match (ctx.team_id, ctx.team_slug.as_deref()) {
        (Some(tid), Some(own)) => {
            if own == slug {
                Ok(tid)
            } else {
                Err(ApiError::Forbidden(format!(
                    "this credential administers team '{own}' only"
                )))
            }
        }
        _ => Ok(store::team_id_by_slug(pool, &slug).await?),
    }
}

// --------------------------------------------------------------- handlers --

async fn whoami(req_ctx: Option<axum::Extension<AdminCtx>>) -> ApiResult<Json<serde_json::Value>> {
    let ctx = ctx(req_ctx)?;
    Ok(Json(serde_json::json!({
        "credential_id": ctx.id,
        "scope": if ctx.is_global() { "global" } else { "team" },
        "team": ctx.team_slug,
    })))
}

async fn list_teams(
    State(state): State<AdminApiState>,
    req_ctx: Option<axum::Extension<AdminCtx>>,
) -> ApiResult<Json<serde_json::Value>> {
    let ctx = ctx(req_ctx)?;
    // A team credential sees exactly its own team, never the roster — and
    // never pays for it either: one row by id, not a scan of every team.
    let teams = match ctx.team_id {
        None => store::list_teams(&state.pool).await?,
        Some(tid) => vec![store::team_by_id(&state.pool, tid).await?],
    };
    Ok(Json(serde_json::json!({ "teams": teams })))
}

#[derive(Deserialize)]
struct CreateTeam {
    slug: String,
    name: Option<String>,
}

async fn create_team(
    State(state): State<AdminApiState>,
    req_ctx: Option<axum::Extension<AdminCtx>>,
    body: Body<CreateTeam>,
) -> ApiResult<(StatusCode, Json<serde_json::Value>)> {
    let ctx = ctx(req_ctx)?;
    let Json(body) = body?;
    require_global(&ctx, "creating a team")?;
    let team = store::create_team(&state.pool, Actor::Admin(ctx.id), &body.slug, body.name).await?;
    tracing::info!(credential = %ctx.id, team = %team.slug, "team ready");
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "team": team })),
    ))
}

async fn list_agents(
    State(state): State<AdminApiState>,
    req_ctx: Option<axum::Extension<AdminCtx>>,
    team: Params<String>,
) -> ApiResult<Json<serde_json::Value>> {
    let ctx = ctx(req_ctx)?;
    let Path(team) = team?;
    let tid = scoped_team(&state.pool, &ctx, &team).await?;
    let agents = store::list_agents(&state.pool, tid).await?;
    Ok(Json(serde_json::json!({ "agents": agents })))
}

#[derive(Deserialize)]
struct CreateAgent {
    name: String,
    display_name: Option<String>,
}

async fn create_agent(
    State(state): State<AdminApiState>,
    req_ctx: Option<axum::Extension<AdminCtx>>,
    team: Params<String>,
    body: Body<CreateAgent>,
) -> ApiResult<(StatusCode, Json<serde_json::Value>)> {
    let ctx = ctx(req_ctx)?;
    let Path(team) = team?;
    let Json(body) = body?;
    let tid = scoped_team(&state.pool, &ctx, &team).await?;
    let agent = store::create_agent(
        &state.pool,
        Actor::Admin(ctx.id),
        tid,
        &body.name,
        body.display_name,
    )
    .await?;
    tracing::info!(credential = %ctx.id, team = %team, agent = %agent.name, "agent ready");
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "agent": agent })),
    ))
}

async fn list_tokens(
    State(state): State<AdminApiState>,
    req_ctx: Option<axum::Extension<AdminCtx>>,
    team: Params<String>,
) -> ApiResult<Json<serde_json::Value>> {
    let ctx = ctx(req_ctx)?;
    let Path(team) = team?;
    let tid = scoped_team(&state.pool, &ctx, &team).await?;
    let tokens = store::list_tokens(&state.pool, tid).await?;
    Ok(Json(serde_json::json!({ "tokens": tokens })))
}

#[derive(Deserialize)]
struct IssueToken {
    agent: String,
    label: Option<String>,
}

/// The one response on this surface that carries a secret. It is returned
/// here and nowhere else: not logged, not audited, not published.
async fn issue_token(
    State(state): State<AdminApiState>,
    req_ctx: Option<axum::Extension<AdminCtx>>,
    team: Params<String>,
    body: Body<IssueToken>,
) -> ApiResult<(StatusCode, Json<serde_json::Value>)> {
    let ctx = ctx(req_ctx)?;
    let Path(team) = team?;
    let Json(body) = body?;
    let tid = scoped_team(&state.pool, &ctx, &team).await?;
    let issued = store::issue_token(
        &state.pool,
        Actor::Admin(ctx.id),
        tid,
        &body.agent,
        body.label,
    )
    .await?;
    tracing::info!(
        credential = %ctx.id,
        team = %issued.team,
        agent = %issued.agent,
        token = %issued.id,
        "agent token issued"
    );
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "token": issued })),
    ))
}

async fn revoke_token(
    State(state): State<AdminApiState>,
    req_ctx: Option<axum::Extension<AdminCtx>>,
    params: Params<(String, Uuid)>,
) -> ApiResult<Json<serde_json::Value>> {
    let ctx = ctx(req_ctx)?;
    let Path((team, id)) = params?;
    let tid = scoped_team(&state.pool, &ctx, &team).await?;
    // Always scoped to the resolved team, a global credential included: the
    // path names the team, and a token elsewhere is not found here.
    store::revoke_token(&state.pool, Actor::Admin(ctx.id), Some(tid), id).await?;
    tracing::info!(credential = %ctx.id, team = %team, token = %id, "agent token revoked");
    Ok(Json(serde_json::json!({ "revoked": id })))
}

async fn list_credentials(
    State(state): State<AdminApiState>,
    req_ctx: Option<axum::Extension<AdminCtx>>,
) -> ApiResult<Json<serde_json::Value>> {
    let ctx = ctx(req_ctx)?;
    // A team credential lists its own team's; a global one lists everything.
    let rows = store::list_admins(&state.pool, ctx.team_id).await?;
    Ok(Json(serde_json::json!({ "credentials": rows })))
}

#[derive(Deserialize)]
struct GrantCredential {
    /// Team slug, or absent/null for a global credential.
    team: Option<String>,
    label: Option<String>,
}

/// The other response carrying a secret. Global administrators only: a team
/// credential can neither widen itself nor mint another of its kind.
async fn grant_credential(
    State(state): State<AdminApiState>,
    req_ctx: Option<axum::Extension<AdminCtx>>,
    body: Body<GrantCredential>,
) -> ApiResult<(StatusCode, Json<serde_json::Value>)> {
    let ctx = ctx(req_ctx)?;
    let Json(body) = body?;
    require_global(&ctx, "granting an administrative credential")?;
    // Absent or null means global. An explicit empty string is a mistake,
    // not a request for the widest scope there is.
    let tid = match body.team.as_deref().map(str::trim) {
        None => None,
        Some("") => {
            return Err(ApiError::BadRequest(
                "team is empty; pass a team slug, or omit it for a global credential".to_owned(),
            ));
        }
        Some(slug) => Some(store::team_id_by_slug(&state.pool, slug).await?),
    };
    let issued = store::grant_admin(&state.pool, Actor::Admin(ctx.id), tid, body.label).await?;
    tracing::info!(
        credential = %ctx.id,
        granted = %issued.id,
        team = issued.team.as_deref().unwrap_or("(global)"),
        "administrative credential granted"
    );
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "credential": issued })),
    ))
}

async fn revoke_credential(
    State(state): State<AdminApiState>,
    req_ctx: Option<axum::Extension<AdminCtx>>,
    id: Params<Uuid>,
) -> ApiResult<Json<serde_json::Value>> {
    let ctx = ctx(req_ctx)?;
    let Path(id) = id?;
    // A team credential may revoke within its team (itself included); a
    // global or foreign credential is not found from there.
    store::revoke_admin(&state.pool, Actor::Admin(ctx.id), ctx.team_id, id).await?;
    tracing::info!(credential = %ctx.id, revoked = %id, "administrative credential revoked");
    Ok(Json(serde_json::json!({ "revoked": id })))
}

/// The body-size layer answers with a bare 413 before any extractor runs;
/// say so in the same shape as every other error here. Extractor rejections
/// (malformed JSON, a bad UUID) are mapped in the handlers themselves.
async fn explain_rejections(req: Request, next: Next) -> Response {
    let resp = next.run(req).await;
    match resp.status() {
        StatusCode::PAYLOAD_TOO_LARGE => ApiError::BadRequest(format!(
            "request body is too large; /admin accepts up to {MAX_ADMIN_REQUEST_BYTES} bytes"
        ))
        .into_response(),
        _ => resp,
    }
}

/// The `/admin` router, to be nested at that path. Generic over the outer
/// state so it merges into the main router whatever that carries; it needs
/// none of it.
pub fn router<S: Clone + Send + Sync + 'static>(
    pool: PgPool,
    mcp_rate_limit_per_minute: u32,
) -> Router<S> {
    // Only 0 disables: a small but non-zero MCP limit still leaves /admin
    // limited, at one request a minute if it comes to that.
    let admin_per_minute = match mcp_rate_limit_per_minute {
        0 => 0,
        n => (n / ADMIN_RATE_LIMIT_DIVISOR).max(1),
    };
    let state = AdminApiState {
        pool,
        limiter: RateLimiter::new(admin_per_minute),
    };
    Router::new()
        .route("/whoami", get(whoami))
        .route("/teams", get(list_teams).post(create_team))
        .route("/teams/{team}/agents", get(list_agents).post(create_agent))
        .route("/teams/{team}/tokens", get(list_tokens).post(issue_token))
        .route(
            "/teams/{team}/tokens/{id}",
            axum::routing::delete(revoke_token),
        )
        .route("/credentials", get(list_credentials).post(grant_credential))
        .route(
            "/credentials/{id}",
            axum::routing::delete(revoke_credential),
        )
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            require_admin,
        ))
        .layer(RequestBodyLimitLayer::new(MAX_ADMIN_REQUEST_BYTES))
        .layer(axum::middleware::from_fn(explain_rejections))
        .with_state(state)
}
