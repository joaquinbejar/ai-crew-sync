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
    /// Broker for teams routed off Postgres, e.g. `nats://127.0.0.1:4222`.
    /// Absent on a default installation, which never opens a socket to one.
    /// Configuring it routes nobody: that is `team capability --backend`.
    ///
    /// Empty counts as absent. A compose file declares every variable it
    /// supports, so one nobody set arrives here as `Some("")`.
    pub nats_url: Option<String>,
    /// Runtime NATS credentials file: publish and fetch only. The
    /// provisioning credential is an operator's and this process never
    /// holds it.
    pub nats_credentials: Option<String>,
    /// Drain the publication outbox in this process. On by default wherever
    /// a broker is configured; an operator running dedicated drainer
    /// replicas turns it off on the ones serving requests, and a test that
    /// drives publication itself turns it off to keep the timing its own.
    /// With no broker configured there is nothing to drain either way.
    pub publication_worker: bool,
    /// Seconds between the pings each replica sends itself through Postgres
    /// to prove its event listener still hears. Three unanswered and the
    /// listener is reattached; `/health` reports the state under `events`.
    pub event_ping_secs: u64,
}

/// Headroom over the largest legitimate request: a 1 MiB message body plus
/// eight 256 KiB attachments, which base64 inflates to ~2.7 MiB, plus JSON
/// framing. Anything beyond this is a mistake or an attack, and rejecting it
/// before parsing keeps a bad request from costing a large allocation.
pub const DEFAULT_MAX_REQUEST_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_RATE_LIMIT_PER_MINUTE: u32 = 600;

#[derive(Clone)]
struct HealthState {
    pool: PgPool,
    backends: crate::store::routing::Backends,
    hub: EventHub,
}

/// Health, aware of what this deployment actually runs.
///
/// The database decides the status code, because without it this process
/// can serve nothing. A broker that is down does **not**: messages are
/// accepted and queue in the outbox, which is the whole point of having
/// one. It is reported, with the backlog, so an operator sees it — a probe
/// that fails the whole service over a full-but-working queue would take
/// the bus down to fix nothing.
///
/// `?broker=check` opens a connection to the broker. Left out of the
/// default path deliberately: probes run often, and a connection per probe
/// is a cost with no reader.
async fn health(
    State(state): State<HealthState>,
    axum::extract::Query(query): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> (axum::http::StatusCode, axum::Json<serde_json::Value>) {
    let db = sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&state.pool)
        .await;
    if let Err(e) = db {
        tracing::error!(error = %e, "health check failed");
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(serde_json::json!({ "status": "degraded", "database": "down" })),
        );
    }
    let mut body = serde_json::json!({ "status": "ok", "database": "up" });
    // The listener is what turns a write into a wake. A replica whose
    // listener went deaf still serves every request and still stores every
    // message, so it does not fail the probe either; it says so, because a
    // successful write is not proof that anyone was told.
    let events = state.hub.listener().report();
    if events["listener"] != "live" {
        body["status"] = serde_json::json!("degraded");
    }
    body["events"] = events;
    if state.backends.jetstream_configured() {
        body["broker"] = serde_json::json!("configured");
        if query.get("broker").is_some_and(|v| v == "check") {
            body["broker"] = match state.backends.broker_reachable().await {
                Some(true) => serde_json::json!("up"),
                _ => serde_json::json!("unreachable"),
            };
        }
        // Cheap, and the number an operator actually pages on: a backlog
        // that stops draining.
        if let Ok(row) = sqlx::query_as::<_, (i64, i64, Option<i64>)>(
            "SELECT count(*) FILTER (WHERE state <> 'failed'),
                    count(*) FILTER (WHERE state = 'failed'),
                    extract(epoch FROM now() - min(created_at)
                            FILTER (WHERE state <> 'failed'))::bigint
               FROM conversation_outbox",
        )
        .fetch_one(&state.pool)
        .await
        {
            body["publication"] = serde_json::json!({
                "pending": row.0,
                "failed": row.1,
                "oldest_pending_seconds": row.2,
            });
        }
    }
    (axum::http::StatusCode::OK, axum::Json(body))
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

/// An empty setting is an unset one.
///
/// A compose file defines every variable it supports, so an option nobody
/// set arrives as `Some("")` rather than `None`. Taken at face value, the
/// bus reported a broker it did not have and started a drainer pointed at
/// an empty URL.
fn configured(value: &Option<String>) -> Option<&str> {
    value.as_deref().map(str::trim).filter(|v| !v.is_empty())
}

/// How long one publish may take before the worker stops waiting for an
/// answer. What follows is **not** a retry: an attempt that ended without an
/// answer is uncertain, and reconciliation asks the backend what actually
/// happened before anything is published again.
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(30);
/// How long an idle worker waits before looking for work again.
const OUTBOX_IDLE: Duration = Duration::from_millis(500);
/// How often uncertain publications are resolved against the backend, on
/// top of the pass every start does.
const RECONCILE_INTERVAL: Duration = Duration::from_secs(30);
/// How long a confirmed body stays duplicated in Postgres before the local
/// copy is dropped. Long enough that a reader arriving just after the
/// PubAck is served locally; short enough that the duplication is temporary.
const BODY_RELEASE_GRACE_SECS: i64 = 300;
const BODY_SWEEP_INTERVAL: Duration = Duration::from_secs(60);
/// How often queued inbox references are published, and how many per pass.
/// Bounded so one team with a backlog cannot starve the others.
const REFERENCE_INTERVAL: Duration = Duration::from_millis(500);
const REFERENCES_PER_PASS: i64 = 200;

/// Drain the publication outbox for the teams routed off Postgres.
///
/// Spawned only when a broker is configured: an installation with no broker
/// has nothing to publish and should not poll for it.
async fn run_outbox_worker(
    pool: PgPool,
    backends: crate::store::routing::Backends,
    ct: CancellationToken,
) {
    let worker = format!("serve-{}", uuid::Uuid::new_v4().simple());
    let mut next_reconcile = tokio::time::Instant::now();
    let mut next_sweep = tokio::time::Instant::now();
    let mut next_references = tokio::time::Instant::now();
    loop {
        if ct.is_cancelled() {
            return;
        }
        let now = tokio::time::Instant::now();
        if now >= next_reconcile {
            next_reconcile = now + RECONCILE_INTERVAL;
            reconcile_routed_teams(&pool, &backends).await;
        }
        if now >= next_sweep {
            next_sweep = now + BODY_SWEEP_INTERVAL;
            match crate::store::outbox::release_published_bodies(
                &pool,
                None,
                BODY_RELEASE_GRACE_SECS,
            )
            .await
            {
                Ok(0) => {}
                Ok(n) => tracing::debug!(released = n, "dropped bodies their backend now holds"),
                Err(e) => tracing::warn!(error = %e, "could not release published bodies"),
            }
        }

        if now >= next_references {
            next_references = now + REFERENCE_INTERVAL;
            publish_references(&pool, &backends).await;
        }

        let leased = match crate::store::outbox::lease(&pool, &worker).await {
            Ok(leased) => leased,
            Err(e) => {
                tracing::warn!(error = %e, "could not lease an outbox slot");
                None
            }
        };
        let Some(lease) = leased else {
            tokio::select! {
                _ = ct.cancelled() => return,
                _ = tokio::time::sleep(OUTBOX_IDLE) => {}
            }
            continue;
        };

        let backend = match backends
            .for_conversation(&pool, lease.conversation_id)
            .await
        {
            Ok(backend) => backend,
            Err(e) => {
                // The slot stays and is retried: a misconfigured route is
                // an operator problem, not a reason to drop a message.
                tracing::error!(error = %e, conversation = %lease.conversation_id,
                    "no backend for this conversation; the message stays pending");
                tokio::time::sleep(OUTBOX_IDLE).await;
                continue;
            }
        };
        let publish = crate::store::outbox::publish_leased(&pool, &backend, &lease);
        match tokio::time::timeout(PUBLISH_TIMEOUT, publish).await {
            Ok(Ok(settled)) => {
                tracing::debug!(message = %lease.message_id, ?settled, "publication settled")
            }
            Ok(Err(e)) => tracing::warn!(error = %e, "could not settle a publication"),
            Err(_) => {
                // Neither stored nor failed, and saying either would be a
                // guess. Reconciliation asks the backend.
                if let Err(e) = crate::store::outbox::mark_uncertain(
                    &pool,
                    &lease,
                    "the publish did not answer within the timeout",
                )
                .await
                {
                    tracing::warn!(error = %e, "could not record an uncertain publication");
                }
            }
        }
    }
}

/// Publish the per-recipient references queued by publication and by
/// receipt changes. Only for teams routed to a broker: a Postgres team's
/// readers are woken by the event hub, as they always were.
async fn publish_references(pool: &PgPool, backends: &crate::store::routing::Backends) {
    let teams = match backends.routed_teams(pool).await {
        Ok(teams) => teams,
        Err(e) => {
            tracing::warn!(error = %e, "could not list routed teams");
            return;
        }
    };
    for team in teams {
        let Ok(crate::store::routing::AnyBackend::JetStream(backend)) =
            backends.for_team(pool, team).await
        else {
            continue;
        };
        match crate::store::inbox::publish_pending(pool, &backend, team, REFERENCES_PER_PASS).await
        {
            Ok(0) => {}
            Ok(n) => tracing::debug!(%team, published = n, "inbox references published"),
            Err(e) => tracing::warn!(error = %e, %team, "could not publish inbox references"),
        }
    }
}

async fn reconcile_routed_teams(pool: &PgPool, backends: &crate::store::routing::Backends) {
    let teams = match backends.routed_teams(pool).await {
        Ok(teams) => teams,
        Err(e) => {
            tracing::warn!(error = %e, "could not list routed teams");
            return;
        }
    };
    for team in teams {
        let backend = match backends.for_team(pool, team).await {
            Ok(backend) => backend,
            Err(e) => {
                tracing::error!(error = %e, %team, "this team is routed to a backend this \
                    process cannot reach");
                continue;
            }
        };
        match crate::store::outbox::resolve_uncertain(pool, &backend, team).await {
            Ok(0) => {}
            Ok(n) => tracing::info!(%team, resolved = n, "resolved uncertain publications"),
            Err(e) => tracing::warn!(error = %e, %team, "reconciliation failed"),
        }
    }
}

pub fn build_router(pool: PgPool, opts: &ServeOptions, ct: CancellationToken) -> Router {
    // One LISTEN connection feeds every in-process consumer: wait_for_updates
    // long-polls and the webhook dispatcher.
    let hub = EventHub::with_ping(std::time::Duration::from_secs(opts.event_ping_secs));
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

    // Where bodies live. Postgres for everybody unless an operator both
    // configured a broker here and routed a team to it; either alone
    // changes nothing.
    let backends = match configured(&opts.nats_url) {
        Some(url) => {
            let mut config = crate::store::jetstream::Config::new(url.to_owned());
            config.credentials = configured(&opts.nats_credentials).map(str::to_owned);
            tracing::info!(%url, "JetStream available for teams routed to it");
            crate::store::routing::Backends::with_jetstream(pool.clone(), config)
        }
        None => crate::store::routing::Backends::postgres_only(pool.clone()),
    };
    if backends.jetstream_configured() && opts.publication_worker {
        tokio::spawn(run_outbox_worker(
            pool.clone(),
            backends.clone(),
            ct.clone(),
        ));
    }

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
            let backends = backends.clone();
            move || {
                Ok(Bus::with_backends(
                    pool.clone(),
                    hub.clone(),
                    backends.clone(),
                ))
            }
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
        //
        // `route_layer`, not `layer`: a layer applies to this router's
        // fallback too, and merging carried that fallback to the whole
        // server. A mistyped path anywhere then answered "missing bearer
        // token" or "invalid or revoked token" — sending whoever typed it
        // to check a credential that was never the problem.
        .route_layer(axum::middleware::from_fn_with_state(
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

    let health_routes = Router::new()
        .route("/health", get(health))
        .with_state(HealthState {
            pool: pool.clone(),
            backends: backends.clone(),
            hub: hub.clone(),
        });

    Router::new()
        // A path this server does not serve is a path, not a credential
        // problem. Said before anything asks for a token, and it names what
        // is actually here.
        // The path only. A query string can carry a credential — the
        // dashboard still accepts one that way — and repeating it in an
        // error body puts it wherever that body is pasted. The tracing
        // layer below drops query strings for the same reason.
        .fallback(|uri: axum::http::Uri| async move {
            let path = uri.path().to_owned();
            (
                axum::http::StatusCode::NOT_FOUND,
                axum::Json(serde_json::json!({
                    "error": format!(
                        "no route for {path} on this server. It serves POST /mcp (the MCP \
                         endpoint), GET /health, GET /dashboard and /admin/* for \
                         administrative credentials."
                    )
                })),
            )
        })
        .merge(health_routes)
        .merge(dashboard_routes)
        .merge(mcp_routes)
        // Administration is its own surface with its own credential class,
        // its own rate limit and no MCP: see `admin_api`.
        .nest(
            "/admin",
            crate::admin_api::router(pool.clone(), opts.rate_limit_per_minute),
        )
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

#[cfg(test)]
mod tests {
    use super::configured;

    #[test]
    fn an_empty_variable_is_not_a_setting() {
        assert_eq!(
            configured(&Some("nats://b:4222".into())),
            Some("nats://b:4222")
        );
        assert_eq!(
            configured(&Some("  nats://b:4222 ".into())),
            Some("nats://b:4222")
        );
        // What a compose file that declares the variable and sets nothing
        // actually delivers.
        assert_eq!(configured(&Some(String::new())), None);
        assert_eq!(configured(&Some("   ".into())), None);
        assert_eq!(configured(&None), None);
    }
}
