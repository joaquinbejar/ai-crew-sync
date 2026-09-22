//! Shared coordination bus for a team of AI coding agents (Claude Code, Codex, Cursor — any MCP client).
//!
//! The crate is split so integration tests can drive the real HTTP surface:
//!
//! - [`store`] holds all business logic and every SQL statement,
//! - [`tools`] is a thin MCP layer over it,
//! - [`serve`] wires the transport, authentication and axum together,
//! - [`admin`] is the operator CLI (teams, agents, tokens),
//! - [`admin_api`] is the remote administration surface at `/admin/*`,
//! - [`admin_cli`] drives it from the operator's machine (`admin …`),
//! - [`context`] resolves which bus, token and project a client should use,
//! - [`proxy`] is the per-conversation stdio MCP server (`mcp proxy`),
//! - [`hook`] is what lifecycle hooks run (`context hook`).

// Row tuples for `sqlx::query_as` are spelled out next to the SELECT that
// produces them; naming each one would only add indirection.
#![allow(clippy::type_complexity)]

pub mod admin;
pub mod admin_api;
pub mod admin_cli;
pub mod auth;
pub mod client;
pub mod context;
pub mod dashboard;
pub mod error;
pub mod events;
pub mod hook;
pub mod model;
pub mod proxy;
pub mod ratelimit;
pub mod recipes;
pub mod serve;
pub mod spool;
pub mod store;
pub mod tools;
pub mod webhooks;

/// Embedded schema migrations, applied by `migrate` and on startup.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");
