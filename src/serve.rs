use std::time::Duration;

use anyhow::Context;
use axum::response::IntoResponse;
use axum::{
    Router,
    extract::State,
    routing::{get, post},
};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tower_http::{limit::RequestBodyLimitLayer, trace::TraceLayer};

use crate::{auth, dashboard, events::EventHub, tools::Bus, webhooks};

pub struct ServeOptions {
    pub bind: String,
    /// Hostnames accepted in the `Host` header. Empty disables the check, which
    /// is what you want behind a proxy that already validates it.
    pub allowed_hosts: Vec<String>,
    pub allowed_origins: Vec<String>,
    /// Largest MCP request body accepted, in bytes.
    pub max_request_bytes: usize,
    /// Requests per minute per token; 0 disables in-process limiting.
    pub rate_limit_per_minute: u32,
    /// Signs the dashboard's read-only session grants. Share it across
    /// replicas so a grant issued by one is accepted by the others.
    pub dashboard_secret: Vec<u8>,
}

/// Headroom over the largest legitimate request: a 1 MiB message body plus
/// eight 256 KiB attachments, which base64 inflates to ~2.7 MiB, plus JSON
/// framing. Anything beyond this is a mistake or an attack, and rejecting it
/// before parsing keeps a bad request from costing a large allocation.
pub const DEFAULT_MAX_REQUEST_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_RATE_LIMIT_PER_MINUTE: u32 = 600;

async fn health(
    State(pool): State<PgPool>,
) -> (axum::http::StatusCode, axum::Json<serde_json::Value>) {
    match sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&pool)
        .await
    {
        Ok(_) => (
            axum::http::StatusCode::OK,
            axum::Json(serde_json::json!({ "status": "ok", "database": "up" })),
        ),
        Err(e) => {
            tracing::error!(error = %e, "health check failed");
            (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                axum::Json(serde_json::json!({ "status": "degraded", "database": "down" })),
            )
        }
    }
}

/// `DefaultBodyLimit` answers with a bare 413. The caller here is a language
/// model, so replace it with a body that says what the limit is and what to
/// do instead.
async fn explain_payload_too_large(
    limit: usize,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let resp = next.run(req).await;
    if resp.status() != axum::http::StatusCode::PAYLOAD_TOO_LARGE {
        return resp;
    }
    (
        axum::http::StatusCode::PAYLOAD_TOO_LARGE,
        axum::Json(serde_json::json!({
            "error": format!(
                "request body is too large; this server accepts up to {limit} bytes. \
                 Send fewer or smaller attachments (256 KiB each, 8 per message), \
                 or split the call into several smaller ones."
            )
        })),
    )
        .into_response()
}

/// How often the shared-row sweep re-runs on a long-lived server. The first
/// pass runs immediately, so every restart — which is every deploy — clears
/// what the lazy per-heartbeat sweep cannot reach: rows whose owner never
/// heartbeats again.
const PRESENCE_SWEEP_INTERVAL: Duration = Duration::from_secs(3600);

async fn run_presence_sweeper(pool: PgPool, ct: CancellationToken) {
    loop {
        // Best-effort like the heartbeat sweep: presence hygiene must never
        // take the server down, and a failed pass just waits for the next.
        match crate::store::presence::sweep_expired_shared_rows(&pool).await {
            Ok(0) => {}
            Ok(n) => tracing::debug!(deleted = n, "swept long-dead shared presence rows"),
            Err(e) => tracing::warn!(error = %e, "presence sweep failed"),
        }
        tokio::select! {
            _ = ct.cancelled() => return,
            _ = tokio::time::sleep(PRESENCE_SWEEP_INTERVAL) => {}
        }
    }
}

pub fn build_router(pool: PgPool, opts: &ServeOptions, ct: CancellationToken) -> Router {
    // One LISTEN connection feeds every in-process consumer: wait_for_updates
    // long-polls and the webhook dispatcher.
    let hub = EventHub::new();
    tokio::spawn(crate::events::run_pg_listener(
        pool.clone(),
        hub.clone(),
        ct.clone(),
    ));
    tokio::spawn(webhooks::run_dispatcher(
        pool.clone(),
        hub.clone(),
        ct.clone(),
    ));
    tokio::spawn(run_presence_sweeper(pool.clone(), ct.clone()));

    let mut config = StreamableHttpServerConfig::default()
        .with_json_response(true)
        // Sessions are gone in the current MCP protocol revision; running
        // stateless means any instance can serve any request, so the service
        // scales horizontally behind a plain load balancer.
        .with_legacy_session_mode(false)
        .with_sse_keep_alive(Some(Duration::from_secs(30)))
        .with_cancellation_token(ct);

    config = if opts.allowed_hosts.is_empty() {
        config.disable_allowed_hosts()
    } else {
        config.with_allowed_hosts(opts.allowed_hosts.clone())
    };
    if !opts.allowed_origins.is_empty() {
        config = config.with_allowed_origins(opts.allowed_origins.clone());
    }

    let mcp: StreamableHttpService<Bus, LocalSessionManager> = StreamableHttpService::new(
        {
            let pool = pool.clone();
            let hub = hub.clone();
            move || Ok(Bus::new(pool.clone(), hub.clone()))
        },
        Default::default(),
        config,
    );

    let limiter = crate::ratelimit::RateLimiter::new(opts.rate_limit_per_minute);
    if limiter.is_none() {
        tracing::warn!("in-process rate limiting is disabled (BUS_RATE_LIMIT_PER_MINUTE=0)");
    }
    let auth_state = auth::AuthState {
        pool: pool.clone(),
        limiter,
    };

    let mcp_routes = Router::new()
        .nest_service("/mcp", mcp)
        // Every /mcp request must carry a valid bearer token; the middleware
        // injects the resolved AuthCtx that tool handlers read, and charges
        // the per-token rate limit.
        .layer(axum::middleware::from_fn_with_state(
            auth_state,
            auth::require_bearer,
        ))
        // Runs before auth: the body is truncated at the limit rather than
        // read whole, and no token lookup happens. This must be tower-http's
        // layer, not axum's DefaultBodyLimit — the MCP service reads the raw
        // body itself, so an extractor-level limit would never fire.
        .layer(RequestBodyLimitLayer::new(opts.max_request_bytes))
        .layer(axum::middleware::from_fn({
            let limit = opts.max_request_bytes;
            move |req, next| explain_payload_too_large(limit, req, next)
        }));

    let dashboard_state = dashboard::DashboardState {
        pool: pool.clone(),
        secret: std::sync::Arc::new(opts.dashboard_secret.clone()),
    };
    let dashboard_routes = Router::new()
        .route("/dashboard", get(dashboard::render))
        .route("/dashboard/login", post(dashboard::login))
        .with_state(dashboard_state);

    Router::new()
        .route("/health", get(health))
        .merge(dashboard_routes)
        .merge(mcp_routes)
        // Record the method and path only. The default span records the whole
        // URI, which is how a credential in a query string reaches the logs —
        // the dashboard no longer accepts one, and the logs no longer invite it.
        .layer(
            TraceLayer::new_for_http().make_span_with(|req: &axum::http::Request<_>| {
                tracing::info_span!(
                    "http",
                    method = %req.method(),
                    path = %req.uri().path(),
                )
            }),
        )
        .with_state(pool)
}

pub async fn run(pool: PgPool, opts: ServeOptions) -> anyhow::Result<()> {
    let ct = CancellationToken::new();
    let app = build_router(pool, &opts, ct.child_token());

    let listener = tokio::net::TcpListener::bind(&opts.bind)
        .await
        .with_context(|| format!("failed to bind {}", opts.bind))?;
    let addr = listener.local_addr()?;
    tracing::info!(%addr, "ai-crew-sync listening; MCP endpoint at /mcp");

    let shutdown = {
        let ct = ct.clone();
        async move {
            let ctrl_c = async {
                tokio::signal::ctrl_c().await.ok();
            };
            #[cfg(unix)]
            let term = async {
                if let Ok(mut s) =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                {
                    s.recv().await;
                }
            };
            #[cfg(not(unix))]
            let term = std::future::pending::<()>();

            tokio::select! {
                _ = ctrl_c => {},
                _ = term => {},
            }
            tracing::info!("shutdown signal received");
            ct.cancel();
        }
    };

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
        .context("server error")?;
    Ok(())
}
