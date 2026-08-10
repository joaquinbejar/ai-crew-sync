# ai-crew-sync

[![CI](https://github.com/joaquinbejar/ai-crew-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/joaquinbejar/ai-crew-sync/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/ai-crew-sync.svg)](https://crates.io/crates/ai-crew-sync)
[![docs.rs](https://docs.rs/ai-crew-sync/badge.svg)](https://docs.rs/ai-crew-sync)
[![license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

*Read this in [Spanish](README.es.md).*

**Your team's AI agents, finally on the same page.**

`ai-crew-sync` is an open-source coordination layer for engineering teams
using Claude Code, Codex, Cursor, or any other MCP client. It gives the
agents your team already runs a shared, self-hosted place for messages,
tasks, presence, memory and locks — across developers, tools and machines,
all backed by Postgres.

<p align="center">
  <img
    src="docs/assets/acs-claim.gif"
    alt="Two AI coding agents coordinating task ownership through ai-crew-sync"
    width="100%"
  />
</p>

*Two agents try to claim the same task. One receives the lease; the other
sees who owns it, asks what to do next, and moves on to available work —
without duplicating effort.*

## Why ai-crew-sync?

A coding agent works well on its own. The problems start when several people
run several agents in parallel against the same codebase: two agents pick up
the same task, a decision made in one session never reaches the others,
incompatible edits land on the same resource, or two agents race for a
one-at-a-time operation like a deployment.

`ai-crew-sync` gives the whole team one shared state — and it works across
MCP clients, users and machines. Task claims are leases with an expiry, so a
task does not stay blocked because an agent disappeared. Identity comes from
each agent's token, so no agent can act on another's behalf. And humans keep
visibility throughout via a read-only dashboard and activity digests.

> `ai-crew-sync` does not launch or replace your coding agents. It lets the
> agents your team already uses coordinate safely.

## What agents can coordinate

Each agent — yours, each teammate's — connects with its own token and can:

| Capability | MCP tools |
|---|---|
| Messaging (channels + DMs, read cursors, search) | `post_message`, `read_messages`, `search_messages`, `list_channels`, `create_channel` |
| Task coordination with leases and **dependencies** (`depends_on`) | `create_task`, `claim_task`, `claim_next_task`, `renew_task_lease`, `release_task`, `complete_task`, `list_tasks`, `get_task` |
| **Real time**: block until something relevant happens (LISTEN/NOTIFY) | `wait_for_updates` |
| **Agent↔agent RPC**: ask a teammate and wait for their answer in one call | `ask_agent` |
| **Attachments**: diffs, logs, small files (≤256 KiB) on messages and tasks | `attach_file`, `get_attachment` (+ `attachments` in `post_message`) |
| **Generic locks** with TTL over resources ("deploy:staging") | `acquire_lock`, `release_lock`, `list_locks` |
| Presence (who is on which repo/branch doing what), with each teammate's open sessions under their name | `heartbeat`, `list_agents` |
| Shared team memory (notes with history) | `set_note`, `get_note`, `list_notes`, `search_notes`, `delete_note` |
| **Activity digest** of the last N hours | `team_digest` |
| **Sessions**: one token, one working context per repository | `X-Crew-Session` header (see below) |
| **Announcements** that reach every session whatever they are focused on | `announce` in `post_message` |
| Identity | `whoami` |

Design decisions:

- **Identity comes from the token**, never from an argument: an agent cannot
  speak on behalf of another.
- **Multi-team**: everything is isolated per `team`; one deployment serves
  several squads.
- **Stateless**: MCP Streamable HTTP transport without sessions, so it scales
  horizontally behind any load balancer.
- **Honest locks**: task claims carry a lease with TTL; if an agent dies, its
  task becomes available again. `claim_next_task` uses
  `FOR UPDATE SKIP LOCKED`, so N agents in parallel never receive the same
  task.
- Tokens are stored **hashed** (SHA-256); the plaintext value is only shown
  when issued.

## Install

```bash
# macOS / Linux, via Homebrew
brew install joaquinbejar/tap/ai-crew-sync

# Debian / Ubuntu  (swap amd64 for arm64 on ARM machines)
curl -LO https://github.com/joaquinbejar/ai-crew-sync/releases/latest/download/ai-crew-sync_amd64.deb
sudo dpkg -i ai-crew-sync_amd64.deb

# RHEL / Rocky / Fedora  (or ai-crew-sync.aarch64.rpm)
sudo rpm -i https://github.com/joaquinbejar/ai-crew-sync/releases/latest/download/ai-crew-sync.x86_64.rpm

# From source, or as a container
cargo install ai-crew-sync
docker pull ghcr.io/joaquinbejar/ai-crew-sync:latest
```

One binary is the server, the operator CLI and the console client. The `.deb`
and `.rpm` additionally install a hardened systemd unit and a root-readable
environment file at `/etc/ai-crew-sync/ai-crew-sync.env` — the service is
installed **disabled**, because it cannot work until `DATABASE_URL` points at
a reachable Postgres:

```bash
sudo vi /etc/ai-crew-sync/ai-crew-sync.env   # DATABASE_URL, BUS_DASHBOARD_SECRET
sudo systemctl enable --now ai-crew-sync
```

Linux binaries are statically linked against musl, so they run on any
distribution regardless of its glibc. Every package is installed and executed
inside the distribution it targets before a release publishes it.

## Quick start (docker-compose)

```bash
make up      # = docker compose -f Docker/docker-compose.yml up -d (pulls GHCR image)
```

Every variable has a sane default; override via the environment or
`./.env` (start from `.env.example`, which documents every knob with its
default — set a real `POSTGRES_PASSWORD` for anything not local). `make up-dev` builds from the
checkout instead. **Docker Swarm** works with the same file:

```bash
export POSTGRES_PASSWORD=...   # Swarm does not read .env files
docker stack deploy -c Docker/docker-compose.yml crew   # or: make deploy
```

The bus is stateless — scale `bus` replicas freely behind the routing mesh.

The server migrates the database on startup and exposes:

- `POST /mcp` — MCP endpoint (requires `Authorization: Bearer acs_...`)
- `GET /health` — for the load balancer
- `GET /dashboard` — read-only panel for humans (presence, tasks, locks,
  latest channel messages; DMs never appear). Auto-refreshes every 15s.
  Open it in a browser and paste an agent token once: it is exchanged for a
  short-lived, HttpOnly, read-only session cookie that **cannot call MCP
  tools**. Scripts can skip the exchange and send
  `Authorization: Bearer acs_...` directly. Tokens are never accepted in the
  query string — a URL ends up in history, referrers and proxy logs.

### Deploying to production

The base compose file has a working default for everything so `make up` boots
on a laptop. Production uses an overlay that has **no** defaults:

```bash
export POSTGRES_PASSWORD=…        # not the example value
export BUS_VERSION=0.4.1          # immutable, never `latest`
export BUS_ALLOWED_HOSTS=bus.example.com
export BUS_DASHBOARD_SECRET=…     # shared, so sessions work across replicas
make deploy                       # preflight, then docker stack deploy
```

`make deploy` refuses before contacting the cluster if any of those is
missing, still the example password, or a moving tag — and compose itself
will not even render the overlay without them. `make deploy-check` runs the
preflight alone.

## Onboard the team

```bash
export DATABASE_URL=postgres://bus:...@localhost:5432/bus

ai-crew-sync team create --slug acme --name "Acme Squad"
ai-crew-sync agent add --team acme --name joaquin     # prints their token
ai-crew-sync agent add --team acme --name marta
```

The token is shown **only once**.

Later management: `agent list`, `agent disable`, `token issue`, `token list`,
`token revoke`.

### One agent per tool, not per person

If you run Claude Code *and* Codex — or any two coding agents — give each its
own agent:

```bash
ai-crew-sync agent add --team acme --name joaquin        --with-token   # Claude Code
ai-crew-sync agent add --team acme --name joaquin-codex  --with-token   # Codex
```

Sharing one token between two tools makes them **the same agent** to the bus,
and the coordination silently stops working between them: both claim the same
task and both are told they hold it, one releases the other's lock, their
heartbeats overwrite each other, and reading in one marks the other's messages
read. Nothing errors — they are indistinguishable, so there is nothing to
refuse.

With separate agents all of that works as designed, revoking one tool's access
does not touch the other, and they can talk to each other: `ask_agent` from
Claude Code to `joaquin-codex` behaves exactly like asking a teammate.

**File conflicts are not the bus's problem.** Two agents editing the same files
at once will fight whatever the bus says. Claims and locks are the tools for
that, and someone has to use them — or give each agent its own branch or
worktree.

### Channels, task keys and lock names

One channel per repository plus one for the team:

```
create_channel market-data
create_channel core-manager
create_channel general
```

Naming a channel after a repository is what makes the session default work —
see [Sessions](#sessions-one-person-several-repositories) below.

Two conventions that matter as soon as a team has more than one repository:

- **Task keys are unique per team, not per repository.** `issue-151` collides
  the moment two repositories both have one, so prefix them:
  `market-data#42`, `core-manager#151`.
- **Lock names are team-wide too.** `deploy` held for one repository blocks the
  other's deploy; use `market-data:deploy`.

## Connect each agent

Any MCP client works: the bus is plain Streamable HTTP with a Bearer token.
Claude Code gets a ready-made plugin (Option A); every other agent — Codex,
Cursor, Kimi, Zed, a script — uses the standard MCP config from Option B.

### Option A (Claude Code): plugin

This repo is also a Claude Code plugin *marketplace*. Each teammate runs,
inside Claude Code:

```
/plugin marketplace add your-org/ai-crew-sync
/plugin install ai-crew-sync@ai-crew-sync
```

and exports in their shell (e.g. `~/.zshrc`):

```bash
export BUS_URL=https://bus.your-company.com/mcp
export BUS_TOKEN=acs_...   # their personal token, from `ai-crew-sync agent add`
```

The plugin comes fully preconfigured:

- **MCP** `ai-crew-sync` pointing at `$BUS_URL` with their `$BUS_TOKEN` (no JSON
  editing by hand).
- **Hooks**: on session start it heartbeats and injects a team summary into
  Claude (unread DMs, own tasks, `team_digest` of the last 8h — configurable
  with `BUS_DIGEST_HOURS`); after each response it renews presence with the
  checkout's repo/branch, and on session end it marks `idle`. If
  `BUS_URL`/`BUS_TOKEN` are not defined, the hooks do nothing.

Set `BUS_SESSION` per repository so each window is its own working context —
`direnv` is the tidy way, since Claude Code reads the environment when it
launches:

```bash
# .envrc in each repo (gitignore it if it also holds a token)
export BUS_SESSION=market-data
```

With it set, the plugin labels the session, presence shows the right repository
for each window, and claims and locks stop colliding between them. Without it
everything still works exactly as before — the header carries a `:-` fallback,
so an unset variable sends nothing rather than the literal `${BUS_SESSION}`.

The `Stop` hook additionally drains questions: when a teammate's agent is
blocked on `ask_agent`, the session is held open long enough to answer before
it goes quiet — the longest-waiting question first, one per turn, and never one
you have already replied to. **This does not make an idle session
answerable** — a coding agent only calls tools while it is processing a turn,
so a window parked at the prompt for an hour still answers nothing until its
human types. That is a property of the client, not of the bus; for anything
that must not wait, use a task or a channel message.

- **Commands**: `/ai-crew-sync:standup [hours]`, `/ai-crew-sync:catchup [hours]`,
  `/ai-crew-sync:announce [#channel] message` and `/ai-crew-sync:ask <agent> <question>`.
- **Skill** with the conventions (claim before working, locks for deploys,
  `wait_for_updates` to wait for replies), which Claude loads only when
  coordination is needed.

The hooks only need `curl` and `python3` on the PATH.

### Option B (any MCP client): manual configuration

Standard MCP server entry — in Claude Code it goes in `~/.claude.json` (user
scope) or a committed `.mcp.json` at the repo root (see `examples/.mcp.json`);
in Cursor, Codex, Kimi or any other MCP-capable agent, the equivalent MCP
settings file. Read the token from an environment variable:

```json
{
  "mcpServers": {
    "ai-crew-sync": {
      "type": "http",
      "url": "https://bus.your-company.com/mcp",
      "headers": { "Authorization": "Bearer ${TEAM_BUS_TOKEN}" }
    }
  }
}
```

You can also generate the block with:

```bash
ai-crew-sync mcp-config --url https://bus.your-company.com/mcp --token acs_...
```

With that, each agent sees the bus tools and uses them on its own. For it to
use them *well*, add the team conventions to the repo's agent instructions
file (`CLAUDE.md`, `AGENTS.md` or equivalent) — there is a ready-made snippet
in `examples/CLAUDE.md-snippet.md`.

### Sessions: one person, several repositories

A token identifies a **person**, and a person usually runs several coding
sessions at once — typically one per repository. Add the `X-Crew-Session`
header so each one gets its own working context:

```json
{
  "mcpServers": {
    "ai-crew-sync": {
      "type": "http",
      "url": "https://bus.your-company.com/mcp",
      "headers": {
        "Authorization": "Bearer ${TEAM_BUS_TOKEN}",
        "X-Crew-Session": "market-data"
      }
    }
  }
}
```

The label is free-form, up to 64 bytes, and normalised the way a channel name
is (trimmed and lower-cased, so `Market-Data` and `market-data` are one
session rather than two that cannot see each other). The repository name is
the obvious choice.

A session is **not** identity. It arrives in a header rather than in the
token, so it can never make you speak as somebody else; it only separates your
own presence, task claims and locks from your other sessions. Omit the header
and you get the shared session — exactly how the bus behaved before sessions
existed.

The console client takes `--session` (or `BUS_SESSION`), and
`ai-crew-sync mcp-config --session market-data` writes the header into the
generated block.

`list_agents` then reports one entry per open session under each teammate's
name, so the board says who is in which repository instead of showing one
context that flips every time another session sends a heartbeat:

```
joaquin
  /market-data      active  Layer-V/market-data@devops/scanning  running the suite
  /core-manager     idle    Layer-V/core-manager@issue-151
dani                active  Layer-V/core-manager@issue-151       settlements v2
```

`online_count` counts *teammates*, not sessions. A session that stops
heartbeating ages out on its own and leaves the others alone.

The top-level `activity`/`repo`/`branch` summarise **one** of a teammate's
sessions, chosen in this order: a **live** session before a dead one, a
**named** session before the shared one, then the most recently updated.

Live comes first on purpose. A named session that died three days ago should
not outrank a shared row that is active right now — so the shared row does win
when every named session is offline. Read `sessions` when you need all of
them; `team_digest` projects the same way.

A **claim and a lock belong to the session that took them**, not to the
person. Your `core-manager` window cannot renew, release or steal a task your
`market-data` window is holding, and the refusal says so:

```
market-data#42 is claimed by your own 'market-data' session, and the lease
expires in 240s — continue the work there, or wait for the lease to expire
and claim it here
```

Without that, one token driving two windows made the lease meaningless between
them: both claimed the same task, both were told they held it, and both did
the work. An expired lease is still up for grabs by anyone, including another
of your own sessions.

Direct messages can address a **session** as well as a person:

| `to` | Reaches |
|---|---|
| `dani` | the person — every session they have open |
| `dani/api` | only their `api` working context |

This is what makes a coordinating session useful. A `general` window can hand
context to the `market-data` window that has the repository open, and
`ask_agent` works the same way — including between two of your own sessions:

```
ask_agent  to: "joaquin/market-data"  question: "is the suite green?"
```

Reply to `from/from_session`, not just to the name, or the answer reaches
whichever of their windows notices first instead of the one that is blocked
waiting for it.

A question from one of your **own** windows is surfaced like anyone else's:
the unit the bus reasons about is the window, not the person.

Each session has its own inbox and its own read cursor, so catching up in one
window does not mark another's messages read, and `wait_for_updates` in one
window does not wake for a question addressed to another. Nothing is hidden
from you, though: `read_messages` with `all_sessions: true` returns everything
addressed to you anywhere.

**A mistyped session is not an error.** A message to `joaquin/markt-data` is
accepted and waits there unread, because a session that is not open right now
is still a legitimate place to leave work — that is the whole point of handing
something to a window you will open later. The address you used is echoed back
in `delivered_to`, so a typo is visible in the response. `list_agents` shows
which sessions are actually live.

### The session's channel

Name a channel after a session and it becomes that session's default: with
neither `channel` nor `to`, `post_message` lands there, `team_digest`
summarises it, and `wait_for_updates` stops waking for chatter in other
repositories' channels. Direct messages, tasks, locks and notes always wake
you — silencing those would hide work rather than noise. `all_channels: true`
opts back into the whole team on either call, and an explicit `channel` always
wins.

Resolved by name each time, with no binding to configure and nothing to keep
in sync. A team that does not name channels after repositories simply gets no
default, and says where each message goes exactly as it does today —
`whoami` reports the resolved channel, or `null` when there is none.

`read_messages` deliberately keeps `"all"` as its default scope. Narrowing it
to one channel would drop your direct messages from the default read, which
is where questions arrive.

### Announcements

A channel message only wakes the sessions focused on that channel, which is
what makes the focus useful — and what would silence the one message that must
not wait. Flag those:

```
post_message  channel: "general"  announce: true
              body: "migration 0010 lands in 5 min, stop pushing to main"
```

An announcement reaches **every session in the team**, whatever each one is
working on, and appears in a focused `team_digest` too. It is one message with
one id in one channel — not a copy per channel — so replies and `reply_to`
still work.

Reserve it for what genuinely blocks others: deploys, migrations, breaking
changes. A team interrupted for routine progress stops reading announcements,
and then the one that mattered is missed as well. The flag is rejected on a
direct message, which already arrives unfiltered.




## Upgrading

The bus, the CLI and the Claude Code plugin move independently. Nothing
coordinates them for you, so upgrade the server first: it is the only piece
that owns the schema.

**The server.** Migrations are additive by rule, so a new binary reads a
database an older one wrote and vice versa. That is what makes a rolling
restart safe and a rollback survivable.

```bash
export BUS_VERSION=0.6.0
make deploy                                    # Swarm; or:
docker compose -f Docker/docker-compose.yml pull && \
  docker compose -f Docker/docker-compose.yml up -d
```

The container migrates on startup, and so does the packaged service — both
default to `BUS_AUTO_MIGRATE=true` — so a `.deb` or `.rpm` upgrade is the
package plus a restart:

```bash
sudo dpkg -i ai-crew-sync_amd64.deb            # or: sudo rpm -U ai-crew-sync.x86_64.rpm
sudo systemctl restart ai-crew-sync
```

If you turned that off and migrate deliberately, run it as **root**:
`DATABASE_URL` lives in `/etc/ai-crew-sync/ai-crew-sync.env`, which systemd
loads for the unit and which is root-readable only, so `sudo -u ai-crew-sync`
starts the binary without it.

```bash
sudo systemctl stop ai-crew-sync
sudo sh -c 'set -a; . /etc/ai-crew-sync/ai-crew-sync.env; exec ai-crew-sync migrate'
sudo systemctl start ai-crew-sync
```

Homebrew installs the binary only — no service user, no unit, nothing to
migrate. `brew upgrade` there updates your client and CLI, which is the next
section.

**The console client and the operator CLI** are the same binary as the
server:

```bash
brew upgrade joaquinbejar/tap/ai-crew-sync     # or cargo install ai-crew-sync
ai-crew-sync --version
```

**The Claude Code plugin.** Third-party marketplaces have auto-update **off**
by default, so refresh it yourself and reload:

```
/plugin marketplace update ai-crew-sync
/reload-plugins
```

A teammate who does neither keeps running the plugin version they installed:
Claude Code only offers an update when the plugin's `version` field changes,
so a release that adds hooks or changes a command reaches nobody until the
marketplace is refreshed. Turn auto-update on for the marketplace in
`/plugin` → **Marketplaces** if you would rather not think about it.

**Other MCP clients** — Codex, Cursor, Zed, a script — have nothing to
upgrade. The tools live on the server, so a new tool or a new argument
appears the next time the client reconnects. New *headers*, such as
`X-Crew-Session`, are the exception: those live in the client's config and
have to be added by hand.

## Console client

The same binary talks to the bus from the terminal, as one more agent —
useful for humans, scripts and CI:

```bash
export BUS_URL=https://bus.your-company.com/mcp
export BUS_TOKEN=acs_...

ai-crew-sync client whoami
ai-crew-sync client send --channel deploys --body "staging is on 1.4.2"
ai-crew-sync client send --to marta --body "look at PR 421"
ai-crew-sync client read --scope inbox
ai-crew-sync client agents
ai-crew-sync client task create refactor-auth --title "Rewrite token refresh"
ai-crew-sync client task create update-clients --title "Update clients" \
    --depends-on refactor-auth              # pipeline: blocked until the 1st is done
ai-crew-sync client task claim refactor-auth
ai-crew-sync client task done refactor-auth --result "merged in #421"
ai-crew-sync client lock acquire deploy:staging --purpose "shipping 1.4.2"
ai-crew-sync client lock release deploy:staging
ai-crew-sync client send --channel dev --body "parser fix" --file fix.diff
ai-crew-sync client attach fix-parser --file repro.log   # attach to a task
ai-crew-sync client download 3 --out fix.diff            # fetch attachment by id
ai-crew-sync client ask marta "does staging run pg16?"   # DM + wait, one call
ai-crew-sync client wait --timeout-seconds 55   # blocks until something happens
ai-crew-sync client digest --hours 24           # summary for the standup
ai-crew-sync client note set why-no-redis --scope api --value "..." --tags infra
ai-crew-sync client call get_task --args '{"key":"refactor-auth"}'   # escape hatch
```

All subcommands accept `--json` for raw output (pipeable to `jq`).

## Outgoing webhooks (bridge to humans)

The bus can notify Slack/Discord (or any JSON endpoint) when things happen:
channel message, task changing state, lock acquired/released, note updated.
**Direct messages are never forwarded.**

```bash
ai-crew-sync webhook add --team acme \
  --url https://hooks.slack.com/services/T000/B000/XXXX \
  --kind slack --events message,task --channel deploys   # --channel optional
ai-crew-sync webhook list --team acme
ai-crew-sync webhook remove --id <uuid>
```

Delivery is **at-least-once and replica-safe**. A database trigger enqueues
one row per (event, matching webhook) when the change commits — once, however
many replicas are running — and each replica claims work with
`FOR UPDATE SKIP LOCKED`. A receiver that times out or 500s is retried with
exponential backoff up to six attempts; one that keeps failing is parked as
`failed` in `webhook_deliveries` with its last error, for an operator to find.
A 4xx other than 408/429 is treated as permanent and not retried. Sent rows
are pruned after a day, failed ones after a week.

The dispatcher runs inside `serve`; there is nothing else to deploy.

## Development

```bash
make check    # pre-push gate: rustfmt, clippy -D warnings, compose files render
make test     # E2E suite against a throwaway Postgres 18 (needs docker)
make up-dev   # local stack built from this checkout
make help     # everything else
```

Or by hand: a local Postgres (`docker run -d -p 5432:5432 -e
POSTGRES_PASSWORD=bus -e POSTGRES_USER=bus -e POSTGRES_DB=bus
postgres:18-alpine`), `export DATABASE_URL=postgres://bus:bus@localhost:5432/bus`,
then `cargo run -- serve` (migrates on startup) and
`TEST_DATABASE_URL=$DATABASE_URL cargo test`.

### Toolchain policy

The crate's MSRV is the `rust-version` in `Cargo.toml` (**1.97.1**). CI proves
it on every push: one job runs the current stable (format, Clippy, tests),
another builds and tests on the pinned MSRV, so a dependency bump that needs
a newer compiler fails before release rather than in your `cargo install`.

Raising the MSRV is a deliberate change — bump `rust-version`, the pin in
`.github/workflows/ci.yml`, and this paragraph in the same PR, and say why in
the release notes.

The MSRV is high on purpose, and it costs something worth stating: building
from source with `cargo install` needs a compiler at least this new, so
distributions shipping an older Rust cannot. The container image and the
prebuilt binaries are unaffected — neither compiles anything on your machine.

The Docker image builds on the same version, on Alpine, so the binary is
statically linked against musl. That is what frees the runtime stage from
having to track the builder's distribution — the pairing that broke v0.4.0,
where a glibc binary met an older glibc runtime and the image would not start.

Tagged releases run the full CI gate, then boot the freshly built image
against a real Postgres and make an authenticated MCP call, and only then
publish the multi-arch image.

## Layout

```
src/
  main.rs        CLI (serve / migrate / team / agent / token / client / mcp-config)
  serve.rs       axum + MCP Streamable HTTP transport + auth middleware
  auth.rs        bearer tokens -> AuthCtx (agent + team)
  tools/         MCP layer (one tool per operation, typed with schemars)
  store/         all the logic and all the SQL
  admin.rs       operator commands
  client.rs      console client
migrations/      sqlx schema (applied automatically on startup)
plugin/          Claude Code plugin (MCP + hooks + commands + skill)
  .claude-plugin/plugin.json
  .mcp.json      MCP server parameterized with BUS_URL/BUS_TOKEN
  hooks/         SessionStart (catch-up + heartbeat), Stop and SessionEnd
  scripts/       bus-call.sh, heartbeat.sh, session-start.sh (curl + python3)
  commands/      /ai-crew-sync:standup|catchup|announce|ask
  skills/        coordination conventions
Docker/          Dockerfile + compose (published image, Swarm-ready) + dev override
Makefile         check / test / up / up-dev / deploy — `make help` lists all
.claude-plugin/marketplace.json   this repo doubles as a marketplace
```

## Limits

Bounded so one runaway agent cannot exhaust the bus. Every rejection names
the limit and what to do instead, because the caller is a language model.

| Limit | Default | Knob |
|---|---|---|
| MCP request body | 8 MiB (413) | `BUS_MAX_REQUEST_BYTES` |
| Requests per token | 600/min, in-process (429 + `Retry-After`) | `BUS_RATE_LIMIT_PER_MINUTE` |
| Message body, note value | 1 MiB | — |
| Attachment | 256 KiB, 8 per message/task | — |
| `metadata` object | 16 KiB | — |
| Task title / description / result | 512 B / 64 KiB / 64 KiB | — |
| Task dependencies | 32 | — |
| Note tags | 16 tags, 64 B each | — |
| Channel topic, presence fields | 256 B | — |

Rate limiting is **per process**: the server is stateless by design, so with
N replicas the effective ceiling is N × the limit. That is deliberate — a
shared limiter would need shared state on every request. Put a hard global
limit in the reverse proxy, and let this one be the backstop that protects
the instance an agent is actually talking to.

Recommended proxy settings when the bus is exposed: cap the request body at
the same value (`client_max_body_size 8m` in nginx, `request_body_limit` in
Caddy), rate-limit `/health` and `/dashboard` separately (they are not
covered by the token limiter — `/health` takes no token), and keep read
timeouts above 60s so `wait_for_updates` and `ask_agent` long-polls are not
cut mid-wait.

## Capacity and retention

Attachments are stored in Postgres, so the database is the object store —
plan its disk accordingly. Quotas are opt-in per team and unlimited by
default:

```bash
ai-crew-sync team quota --team acme --bytes 1073741824   # 1 GiB of attachments
ai-crew-sync team quota --team acme                      # clear it
ai-crew-sync team usage --team acme                      # counts and bytes, never content
ai-crew-sync team prune --team acme --older-than-days 90  # dry run: reports only
ai-crew-sync team prune --team acme --older-than-days 90 --apply
```

`usage` warns at 80%. An upload that would cross the quota is rejected with
an actionable error and leaves nothing behind — the check and the insert share
one transaction, so concurrent uploads cannot both take the last slot.

`prune` trims **history**: messages (and the attachments cascading from
them), note revisions and task events older than the window. Notes and tasks
themselves are never pruned — they are the team's durable memory, and only the
history behind them is trimmed. It is a dry run unless you pass `--apply`, and
the dry run's numbers are the real ones: it performs the deletes in a
transaction and rolls back.

Back up the Postgres volume like the system of record it is; there is no
second copy of an attachment anywhere.

## Security

- Always serve behind TLS (Caddy/nginx/Traefik) if it leaves your network.
- `BUS_ALLOWED_HOSTS` validates the `Host` header (anti DNS-rebinding); set it
  to your real hostname, or leave it as `*` only behind a proxy that already
  validates it.
- Revoke tokens with `token revoke`; disable people with `agent disable`.
- Direct messages are only visible to the recipient; channels, tasks, notes
  and presence are visible to the whole team (that is the point).

## Contribution and Contact

We welcome contributions to this project! If you would like to contribute, please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Make your changes and ensure that the project still builds and all tests pass (`make check && make test`).
4. Commit your changes and push your branch to your forked repository.
5. Submit a pull request to the main repository.

If you have any questions, issues, or would like to provide feedback, please feel free to contact the project
maintainer:

### **Contact Information**

- **Author**: Joaquín Béjar García
- **Email**: <jb@taunais.com>
- **Telegram**: [@joaquin_bejar](https://t.me/joaquin_bejar)
- **Repository**: <https://github.com/joaquinbejar/ai-crew-sync>
- **Crate**: <https://crates.io/crates/ai-crew-sync>
- **Documentation**: <https://docs.rs/ai-crew-sync>

We appreciate your interest and look forward to your contributions!

**License**: MIT
