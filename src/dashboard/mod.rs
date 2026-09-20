//! Read-only HTML dashboard for humans: who is online, what is claimed, what
//! the channels are saying. Served by the bus itself at `/dashboard`.
//!
//! Auth: an agent token is presented ONCE — as a form POST to
//! `/dashboard/login` or an `Authorization: Bearer` header — and exchanged for
//! a short-lived, read-only, HttpOnly cookie ([`grant`]). Tokens are never
//! accepted in the query string: a full-privilege credential in a URL leaks
//! into browser history, copied links, referrer headers and proxy logs.
//!
//! Design notes (from the dataviz method): this page is stat tiles + tables,
//! not charts, so no categorical palette is involved. Status colors are the
//! reserved status palette and never appear without an icon + text label;
//! values wear text tokens, never status hues. Light and dark surfaces are the
//! validated reference pair.

pub mod data;
pub mod grant;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{Html, IntoResponse, Response},
};
use serde::Deserialize;
use sqlx::PgPool;

use crate::auth::{AuthCtx, resolve_token};

/// Escape user content for HTML. Covers the single quote too: every current
/// interpolation sits in an element body or a double-quoted attribute, but one
/// future single-quoted attribute would otherwise turn a message body or an
/// agent name into markup.
fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn ago(ts: chrono::DateTime<chrono::Utc>) -> String {
    let secs = (chrono::Utc::now() - ts).num_seconds().max(0);
    match secs {
        0..=59 => format!("{secs}s ago"),
        60..=3599 => format!("{}m ago", secs / 60),
        3600..=86399 => format!("{}h ago", secs / 3600),
        _ => format!("{}d ago", secs / 86400),
    }
}

/// Name of the cookie carrying the read-only grant.
const COOKIE: &str = "acs_dashboard";
/// Grants are cheap to re-mint, so keep the window short.
const GRANT_TTL_SECS: i64 = 3600;

/// Signing key for dashboard grants, plus the page's own state.
#[derive(Clone)]
pub struct DashboardState {
    pub pool: PgPool,
    pub secret: std::sync::Arc<Vec<u8>>,
}

#[derive(Deserialize)]
pub struct LoginForm {
    token: Option<String>,
}

fn bearer(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            v.strip_prefix("Bearer ")
                .or_else(|| v.strip_prefix("bearer "))
        })
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
}

fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .filter_map(|pair| pair.trim().split_once('='))
        .find(|(k, _)| *k == name)
        .map(|(_, v)| v.to_owned())
}

/// Never cache a page listing team activity, and never leak the URL onward.
fn harden(mut resp: Response) -> Response {
    let h = resp.headers_mut();
    h.insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    h.insert(
        header::REFERRER_POLICY,
        header::HeaderValue::from_static("no-referrer"),
    );
    resp
}

fn login_page(status: StatusCode, message: &str) -> Response {
    // A form POST keeps the token out of the URL; the browser sends it in the
    // body, and it never appears in history or a referrer.
    let body = format!(
        r#"<!doctype html><meta charset="utf-8"><title>ai-crew-sync</title>
<style>body{{font:15px/1.5 system-ui,sans-serif;max-width:34rem;margin:12vh auto;padding:0 1.5rem}}
input{{width:100%;padding:.6rem;font:inherit;font-family:ui-monospace,monospace}}
button{{margin-top:.75rem;padding:.6rem 1.2rem;font:inherit}}
p{{color:#666}}</style>
<h1>ai-crew-sync</h1>
<p>{}</p>
<form method="post" action="/dashboard/login">
  <label>Agent token<br><input name="token" type="password" autocomplete="off"
         placeholder="acs_…" autofocus></label>
  <button type="submit">Open dashboard</button>
</form>
<p>Read-only. The token is exchanged for a session cookie that expires in an
hour and cannot call MCP tools.</p>"#,
        esc(message)
    );
    harden((status, Html(body)).into_response())
}

/// Exchange an agent token for a read-only grant cookie.
pub async fn login(
    State(state): State<DashboardState>,
    headers: HeaderMap,
    axum::extract::Form(form): axum::extract::Form<LoginForm>,
) -> Response {
    // Scripts and `curl` do not need this exchange at all — they pass the
    // bearer header straight to GET /dashboard.
    let raw = form.token.filter(|t| !t.trim().is_empty());
    let Some(raw) = raw else {
        return login_page(
            StatusCode::UNAUTHORIZED,
            "Paste your agent token to continue.",
        );
    };

    let auth = match resolve_token(&state.pool, &raw).await {
        Ok(a) => a,
        Err(_) => {
            return login_page(
                StatusCode::UNAUTHORIZED,
                "That token is not valid, or it has been revoked.",
            );
        }
    };

    let grant = grant::issue(
        &state.secret,
        auth.team_id,
        chrono::Utc::now().timestamp(),
        GRANT_TTL_SECS,
    );
    // Secure is set when the request reached us over TLS, directly or through
    // a proxy that says so — marking it unconditionally would break plain-HTTP
    // local use, and omitting it always would be wrong in production.
    let secure = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("https"))
        .unwrap_or(false);
    let cookie = format!(
        "{COOKIE}={grant}; Path=/dashboard; HttpOnly; SameSite=Strict; Max-Age={GRANT_TTL_SECS}{}",
        if secure { "; Secure" } else { "" }
    );

    let mut resp = axum::response::Redirect::to("/dashboard").into_response();
    if let Ok(value) = header::HeaderValue::from_str(&cookie) {
        resp.headers_mut().insert(header::SET_COOKIE, value);
    }
    harden(resp)
}

pub async fn render(State(state): State<DashboardState>, headers: HeaderMap) -> Response {
    // A grant cookie, or a bearer header for `curl` and scripts. Never a
    // query parameter.
    let team_id = match cookie_value(&headers, COOKIE)
        .and_then(|g| grant::verify(&state.secret, &g, chrono::Utc::now().timestamp()))
    {
        Some(team) => team,
        None => match bearer(&headers) {
            Some(raw) => match resolve_token(&state.pool, &raw).await {
                Ok(a) => a.team_id,
                Err(_) => {
                    return login_page(StatusCode::UNAUTHORIZED, "That token is not valid.");
                }
            },
            None => {
                return login_page(
                    StatusCode::UNAUTHORIZED,
                    "Sign in with an agent token to view this team's dashboard.",
                );
            }
        },
    };

    // The grant proves team membership and nothing more; the page is built
    // from the team alone, never from an agent identity.
    let auth = AuthCtx {
        agent_id: uuid::Uuid::nil(),
        agent_name: String::new(),
        team_id,
        team_slug: String::new(),
        // The dashboard is a team-wide view, not a working context.
        session: String::new(),
        session_id: None,
        session_epoch: None,
        token_id: None,
    };

    match build_page(&state.pool, &auth).await {
        Ok(page) => harden(Html(page).into_response()),
        Err(e) => {
            tracing::error!(error = %e, "dashboard query failed");
            harden((StatusCode::INTERNAL_SERVER_ERROR, Html("<h1>500</h1>")).into_response())
        }
    }
}

/// Render the page from a snapshot. All the SQL lives in [`data`]; this
/// function only turns rows into HTML.
async fn build_page(pool: &PgPool, auth: &AuthCtx) -> Result<String, sqlx::Error> {
    let data::Snapshot {
        team,
        totals,
        agents,
        tasks,
        messages,
        locks,
        notes,
    } = data::load(pool, auth.team_id).await?;
    let agents_online = totals.agents_online;
    let open_tasks = totals.open_tasks;
    let claimed_tasks = totals.claimed_tasks;
    let messages_24h = totals.messages_24h;

    let mut agent_rows = String::new();
    for a in &agents {
        // Status: icon + label, never color alone.
        let (dot, label) = if a.online {
            match a.status.as_deref() {
                Some("blocked") => (
                    "<span class=\"st st-serious\">●</span> ⛔ blocked",
                    "blocked",
                ),
                Some("busy") => ("<span class=\"st st-warning\">●</span> ⚙ busy", "busy"),
                Some("idle") => ("<span class=\"st st-muted\">●</span> ◌ idle", "idle"),
                _ => ("<span class=\"st st-good\">●</span> ✓ active", "active"),
            }
        } else {
            ("<span class=\"st st-muted\">●</span> ○ offline", "offline")
        };
        let _ = label;
        let place = match (&a.repo, &a.branch) {
            (Some(r), Some(b)) => format!("{}@{}", esc(r), esc(b)),
            (Some(r), None) => esc(r),
            _ => String::new(),
        };
        // The session qualifies the name rather than taking its own column:
        // most teams have one context per person, and an empty column on every
        // row would cost more than it explains.
        let who = if a.session.is_empty() {
            format!("<strong>{}</strong>", esc(&a.name))
        } else {
            format!(
                "<strong>{}</strong> <span class=\"muted\">/{}</span>",
                esc(&a.name),
                esc(&a.session)
            )
        };
        agent_rows.push_str(&format!(
            "<tr><td>{who}</td><td>{}</td><td>{}</td><td>{}</td><td class=\"muted\">{}</td></tr>",
            dot,
            place,
            esc(a.activity.as_deref().unwrap_or("")),
            a.updated_at.map(ago).unwrap_or_default(),
        ));
    }

    let mut task_rows = String::new();
    for t in &tasks {
        let badge = match t.status.as_str() {
            "done" => "<span class=\"st st-good\">●</span> ✓ done".to_string(),
            "claimed" => format!(
                "<span class=\"st st-warning\">●</span> ⚙ claimed by {}",
                esc(t.claimed_by.as_deref().unwrap_or("?"))
            ),
            "cancelled" => "<span class=\"st st-muted\">●</span> ✕ cancelled".to_string(),
            _ if t.blocked => "<span class=\"st st-serious\">●</span> ⛔ blocked".to_string(),
            _ => "<span class=\"st st-muted\">●</span> ◌ open".to_string(),
        };
        let detail = t.result.as_deref().unwrap_or("");
        task_rows.push_str(&format!(
            "<tr><td><code>{}</code></td><td>{}</td><td>{}</td><td>{}</td><td class=\"muted\">{}</td></tr>",
            esc(&t.key),
            esc(&t.title),
            badge,
            esc(detail),
            ago(t.updated_at),
        ));
    }

    let mut msg_rows = String::new();
    for m in messages.iter().rev() {
        // Marked with an icon and a word, never colour alone: an announcement
        // is what the team was interrupted for, and the panel should say so.
        let channel = if m.announce {
            format!(
                "#{} <span class=\"st st-warning\">●</span> announced",
                esc(&m.channel)
            )
        } else {
            format!("#{}", esc(&m.channel))
        };
        msg_rows.push_str(&format!(
            "<tr><td class=\"muted\">{}</td><td>{channel}</td><td><strong>{}</strong></td><td>{}</td></tr>",
            ago(m.created_at),
            esc(&m.sender),
            esc(&m.body),
        ));
    }

    let mut lock_rows = String::new();
    for l in &locks {
        lock_rows.push_str(&format!(
            "<tr><td><code>{}</code></td><td>{}</td><td>{}</td><td class=\"muted\">expires {}</td></tr>",
            esc(&l.name),
            esc(&l.holder),
            esc(l.purpose.as_deref().unwrap_or("")),
            ago(l.expires_at).replace(" ago", ""),
        ));
    }
    if locks.is_empty() {
        lock_rows.push_str("<tr><td colspan=4 class=\"muted\">no locks held</td></tr>");
    }

    let mut note_rows = String::new();
    for n in &notes {
        note_rows.push_str(&format!(
            "<tr><td class=\"muted\">{}</td><td><code>{}/{}</code></td><td>{}</td></tr>",
            ago(n.updated_at),
            esc(&n.scope),
            esc(&n.key),
            esc(n.updated_by.as_deref().unwrap_or("")),
        ));
    }

    Ok(format!(
        r##"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="15">
<title>ai-crew-sync · {team}</title>
<style>
:root {{
  --surface: #fcfcfb; --card: #ffffff; --border: #e4e3df;
  --ink: #0b0b0b; --ink-2: #52514e; --ink-3: #8a8984;
  --good: #0ca30c; --warning: #fab219; --serious: #ec835a; --critical: #d03b3b;
}}
@media (prefers-color-scheme: dark) {{
  :root {{
    --surface: #1a1a19; --card: #232322; --border: #3a3936;
    --ink: #ffffff; --ink-2: #c3c2b7; --ink-3: #8a8984;
  }}
}}
* {{ box-sizing: border-box; }}
body {{
  margin: 0; padding: 24px; background: var(--surface); color: var(--ink);
  font: 14px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif;
}}
h1 {{ font-size: 18px; margin: 0 0 4px; }}
h2 {{ font-size: 13px; margin: 28px 0 8px; color: var(--ink-2);
     text-transform: uppercase; letter-spacing: .06em; }}
.sub {{ color: var(--ink-3); margin-bottom: 20px; }}
.tiles {{ display: flex; gap: 12px; flex-wrap: wrap; }}
.tile {{
  background: var(--card); border: 1px solid var(--border); border-radius: 10px;
  padding: 14px 18px; min-width: 130px;
}}
.tile .n {{ font-size: 26px; font-weight: 650; }}
.tile .l {{ color: var(--ink-2); font-size: 12px; }}
table {{ width: 100%; border-collapse: collapse; background: var(--card);
        border: 1px solid var(--border); border-radius: 10px; overflow: hidden; }}
td {{ padding: 7px 12px; border-top: 1px solid var(--border); vertical-align: top; }}
tr:first-child td {{ border-top: none; }}
code {{ background: transparent; color: inherit; }}
.muted {{ color: var(--ink-3); white-space: nowrap; }}
.st {{ font-size: 10px; vertical-align: 1px; }}
.st-good {{ color: var(--good); }}
.st-warning {{ color: var(--warning); }}
.st-serious {{ color: var(--serious); }}
.st-muted {{ color: var(--ink-3); }}
</style>
</head>
<body>
<h1>ai-crew-sync · {team}</h1>
<div class="sub">viewed as {viewer} · refreshes every 15s · direct messages never shown</div>

<div class="tiles">
  <div class="tile"><div class="n">{agents_online}</div><div class="l">agents online</div></div>
  <div class="tile"><div class="n">{claimed_tasks}</div><div class="l">tasks in progress</div></div>
  <div class="tile"><div class="n">{open_tasks}</div><div class="l">tasks open</div></div>
  <div class="tile"><div class="n">{messages_24h}</div><div class="l">channel msgs · 24h</div></div>
</div>

<h2>Agents</h2>
<table>{agent_rows}</table>

<h2>Tasks</h2>
<table>{task_rows}</table>

<h2>Locks</h2>
<table>{lock_rows}</table>

<h2>Latest channel messages</h2>
<table>{msg_rows}</table>

<h2>Recently updated notes</h2>
<table>{note_rows}</table>
</body>
</html>"##,
        team = esc(&team),
        viewer = esc(&auth.agent_name),
    ))
}
