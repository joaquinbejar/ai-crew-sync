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
| Presence (who is on which repo/branch doing what), with each teammate's open sessions under their name; session discovery by project and role | `heartbeat`, `list_agents`, `list_sessions` |
| Authenticated windows: a credential that proves which window is calling, derived from your agent token | `register_session`, `resume_session`, `renew_session`, `revoke_session` |
| Conversations: addressed threads with explicit membership and per-recipient receipts (opt-in per team) | `create_conversation`, `send_conversation_message`, `read_conversation`, `ack_message`, `get_message_receipts`, `fetch_conversation_inbox`, … |
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
make up      # = docker compose -f Docker/docker-compose.yml up -d --no-build (pulls GHCR image)
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
- `GET /health` — for the load balancer; reports the database, the broker
  and whether the event listener still hears itself (`events.listener`)
- `GET /dashboard` — read-only panel for humans (presence, tasks, locks,
  latest channel messages; DMs never appear). Auto-refreshes every 15s.
  Open it in a browser and paste an agent token once: it is exchanged for a
  short-lived, HttpOnly, read-only session cookie that **cannot call MCP
  tools**. Scripts can skip the exchange and send
  `Authorization: Bearer acs_...` directly. Tokens are never accepted in the
  query string — a URL ends up in history, referrers and proxy logs.

### Deploying to production

There is a single compose file, and it has a working default for everything
so `make up` boots on a laptop. What keeps production safe is the preflight
that `make deploy` runs before touching the cluster:

```bash
export POSTGRES_PASSWORD=…        # not the example value
export BUS_VERSION=0.4.1          # immutable, never `latest`
export BUS_ALLOWED_HOSTS=bus.example.com
export BUS_DASHBOARD_SECRET=…     # shared, so sessions work across replicas
make deploy                       # preflight, then docker stack deploy
```

`make deploy` refuses if any of those is missing, still the example password,
or a moving tag. `make deploy-check` runs the preflight alone.

Behind a Traefik v3 proxy (`--providers.swarm`) that already terminates TLS,
set `TRAEFIK_ENABLE=true` and `BUS_PUBLIC_HOST=crew.example.com` (plus
`TRAEFIK_NETWORK`/`TRAEFIK_ENTRYPOINT`/`TRAEFIK_CERTRESOLVER` if they differ
from `edge`/`websecure`/`le`): the bus carries the router labels and joins the
proxy's network, which the proxy's stack must have created as an attachable
overlay.

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

These commands need `DATABASE_URL`: they run next to Postgres, typically
inside the bus container. Do this **once** there to mint a global
administrative credential — the foundation for administering the bus from
your own machine (the remote API and the `admin` client commands that use it
land in the follow-up changes, #81 and #82):

```bash
ai-crew-sync admin bootstrap --label "joaquin laptop"   # prints acsa_… once
```

An administrative credential (prefix `acsa_`) is a different class from an
agent token (`acs_`): it identifies nobody on the bus, cannot post, claim or
read anything, and only manages teams, agents and tokens. Agent tokens, in
turn, can never mint credentials, not even for their own agent. `admin
credential list` / `admin credential revoke --id …` manage this class; every
issue, grant and revocation lands in an audit table that never holds a secret.

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

This repo is also a Claude Code plugin *marketplace*. Each teammate installs
the binary once (Homebrew, `.deb`/`.rpm` or `cargo install`, see **Install**),
tells it which bus they are on, and then installs the plugin inside Claude
Code:

```bash
# --url is the bus's base URL; /mcp is appended for you
ai-crew-sync context profile add --name acme --default \
    --url https://bus.your-company.com:8443 --team roundcrew --agent backend
ai-crew-sync context show            # endpoint, profile, expected identity, token prefix
ai-crew-sync context verify          # asks the bus who the token REALLY is
```

The profile reads its credentials from `~/.config/ai-crew-sync/tokens-roundcrew`,
which the team's administrator fills without ever printing a token
(`admin token issue --save --repo`, see **Remote administration**).

```
/plugin marketplace add your-org/ai-crew-sync
/plugin install ai-crew-sync@ai-crew-sync
```

**The plugin needs `ai-crew-sync` on the PATH.** Its MCP entry is not an HTTP
URL with a token in it: it starts `ai-crew-sync mcp proxy`, one process per
conversation, and that process resolves a credential from your local profiles
and registers the window as an *authenticated session*. Nothing in the plugin
config, and nothing in your shell, has to hold a token. A teammate who would
rather keep the old environment pair can: `mcp proxy` reads `BUS_URL` and
`BUS_TOKEN` too. Mind the precedence, which is the resolver's and not the
plugin's: an exported `BUS_TOKEN` (like `--token`) **wins over** the
repository's `.acs.toml` profile and the user default, and given together
with `--profile` it is an error rather than a silent choice. Migrating from a
shell wrapper that exports the token per directory means unsetting it (or
retiring the wrapper) wherever the profile should decide; while it is
exported, the profile never applies.

The plugin comes fully preconfigured:

- **MCP** `ai-crew-sync` over the per-conversation proxy (no JSON editing by
  hand, no credential in the config). Each Claude Code window becomes its own
  `agent/session` address, proven by a credential of its own; two windows in
  the same repository can no longer be mistaken for each other.
- **Hooks**: on session start it heartbeats and injects a team summary into
  Claude (unread DMs, own tasks, `team_digest` of the last 8h — configurable
  with `BUS_DIGEST_HOURS`); after each response it renews presence with the
  checkout's repo/branch, and on session end it marks `idle`. With no bus
  configured at all, they do nothing.

You no longer set `BUS_SESSION` for this: the proxy derives the session from
the conversation id Claude Code gives it, so a window keeps its identity across
a reconnect, a fork gets a new one, and presence, claims and locks separate
themselves. What is worth setting per repository is the *project* and the
*role*, so teammates can find the right window:

```bash
ai-crew-sync context set-project --profile acme --project market-data --channel market-data
```

That writes `.acs.toml` at the repo root — a profile name and a project label,
never a credential, so it can be committed. Give the window its role from the
client config (`ai-crew-sync proxy-config --role review`) or at runtime with
the proxy's local `configure_session` tool.

The `Stop` hook additionally drains questions: when a teammate's agent is
blocked on `ask_agent`, the session is held open long enough to answer before
it goes quiet — the longest-waiting question first, one per turn, and never one
you have already replied to. **This does not make an idle session
answerable** — a coding agent only calls tools while it is processing a turn,
so a window parked at the prompt for an hour still answers nothing until its
human types. That is a property of the client, not of the bus; for anything
that must not wait, use a task or a channel message.

- **Commands**: `/ai-crew-sync:standup [hours]`, `/ai-crew-sync:catchup [hours]`,
  `/ai-crew-sync:announce [#channel] message`, `/ai-crew-sync:ask <agent> <question>`,
  `/ai-crew-sync:claim <key|next>`, `/ai-crew-sync:done <key> [result]`,
  `/ai-crew-sync:handoff <key> <agent[/session]>`, `/ai-crew-sync:board`,
  `/ai-crew-sync:who`, `/ai-crew-sync:lock <resource>`, `/ai-crew-sync:unlock [resource]`,
  `/ai-crew-sync:note <key> [text]`, `/ai-crew-sync:wait [kinds]`,
  `/ai-crew-sync:thread <addresses> -- <title>`, `/ai-crew-sync:inbox` and
  `/ai-crew-sync:review <agent> <PR>`. Each is a written procedure over the
  bus tools: the same three or four calls in the same order, with the same
  guardrails, from every window.
- **Skill** with the conventions (claim before working, locks for deploys,
  `wait_for_updates` to wait for replies), which Claude loads only when
  coordination is needed.

#### The same procedures for any other host

The commands are generated from `recipes/`, one Markdown file per procedure
written for an agent that has the bus tools and nothing else: no frontmatter,
no host assumptions, `{{input}}` where the caller's arguments go. Codex reads
them from `AGENTS.md` ("for the routine moves follow `recipes/<name>.md`"),
Kimi Code, Grok or any MCP client can be handed one verbatim, and a machine
with the binary and not the repository gets them from
`ai-crew-sync recipes` (list) and `ai-crew-sync recipes catchup` (one).
`make recipes` regenerates the slash commands; `make check` and the unit
tests fail when a command drifts from its recipe, so a procedure is edited in
one place only.

The hooks run in one of two modes, and pick per conversation:

- **Authenticated** — the window has a proxy, so the hooks call
  `ai-crew-sync context hook`, which reads that window's private binding and
  acts as that session. The credential never passes through a script, an
  argument or an environment variable. This needs the binary on the PATH,
  which the plugin already requires.
- **Legacy** — no proxy and no binding, but `BUS_URL`/`BUS_TOKEN` are
  exported. The hooks fall back to plain `curl` + `python3` and the
  `X-Crew-Session` label, exactly as before. Nothing else is assumed to exist.

Neither configured means the hooks do nothing at all, silently.

### Option B (any MCP client): manual configuration

Two shapes, and the difference is what proves which window is calling.

**Through the proxy (recommended).** The client starts the binary instead of
opening an HTTP connection; each conversation gets its own authenticated
session, and no file holds a credential. Generate the block:

```bash
ai-crew-sync proxy-config                       # .mcp.json shape (most clients)
ai-crew-sync proxy-config --format toml         # ~/.codex/config.toml (Codex)
ai-crew-sync proxy-config --role review         # windows here start as reviewers
```

```json
{
  "mcpServers": {
    "ai-crew-sync": {
      "command": "ai-crew-sync",
      "args": ["mcp", "proxy"]
    }
  }
}
```

Ready to copy: `examples/.mcp.json` and `examples/codex-config.toml`.

**Direct HTTP.** No binary needed, and the bus is plain Streamable HTTP with a
Bearer token — this is what a script, a CI job or an MCP client you cannot
give a command to uses. Identity is the token; the window is a `X-Crew-Session`
*label*, which the bus trusts for partitioning presence and claims but never
as proof of who you are:

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

Ready to copy: `examples/.mcp.http.json`. You can also generate the block:

```bash
ai-crew-sync mcp-config --url https://bus.your-company.com/mcp \
    --token acs_... --session market-data
```

With that, each agent sees the bus tools and uses them on its own. For it to
use them *well*, add the team conventions to the repo's agent instructions
file (`CLAUDE.md`, `AGENTS.md` or equivalent) — there is a ready-made snippet
in `examples/CLAUDE.md-snippet.md`.

### Local profiles and project defaults (no `BUS_TOKEN` export)

The console client (and everything built on it) can find its credentials
without a per-shell export or a wrapper function. Two local files do it:

- **Profiles** — `~/.config/ai-crew-sync/profiles.toml`: which bus, expected
  team and agent, and *which token file* holds the credential (the same
  `tokens-<team>` files `admin token issue --save` writes). No secret lives
  in the profile.
- **Project defaults** — `.acs.toml` at the project root, committed with the
  code: names an approved profile and the logical project. Nothing else.

```bash
ai-crew-sync context profile add --name acme --url https://bus.your-company.com:8443 \
    --team acme --agent joaquin --tokens tokens-acme --default
cd ~/Repos/acme/market-data
ai-crew-sync context set-project --profile acme --project market-data --channel market-data
ai-crew-sync context show      # endpoint, profile, token entry (prefix only), project
ai-crew-sync context verify    # asks the bus: must be joaquin@acme, or it fails
ai-crew-sync client whoami     # no BUS_TOKEN needed
```

The token entry is chosen in this order: `key` from `.acs.toml`, the project
name, the profile's `key`, then `_base`. Precedence between sources is fixed
and printed by `context show`:

| Order | Source | Notes |
|---|---|---|
| 1 | `--token` / `BUS_TOKEN` (+ `--url` / `BUS_URL`) | Explicit credentials always win; `.acs.toml` still supplies project and channel. Given together with `--profile`, it is an error rather than a silent choice. |
| 2 | `--profile` / `BUS_PROFILE` | Per-invocation choice; never rewrites project defaults. |
| 3 | `.acs.toml` at the project root | Found from any nested directory; a linked git worktree inherits the main worktree's file. |
| 4 | `default = "…"` in `profiles.toml` | User default. |

A profile that does not exist locally is an **error**, whatever named it: a
repository can suggest a profile, never define one, and `.acs.toml` is refused
outright if it carries `url`, `token` or a tokens path. Endpoints and
credential references come from your own profile store only, so a cloned
repository cannot send your token anywhere. Two windows in the same repository
select profiles independently (`--profile`), sharing nothing mutable. Writes to
the profile store are serialised through a lock and land atomically with mode
`0600`.

### Authenticated sessions: proving which window you are

`X-Crew-Session` is a label the caller chooses. It is enough to keep one
person's windows from overwriting each other's presence and claims, and it is
not proof of anything: whoever holds the agent token can send any label.

`register_session` turns a window into something it can prove. Present your
agent token once per conversation, and the bus returns a credential derived
from it:

```
register_session {"session": "conv-7f2a"}
→ {"session_token": "acss_…", "session_id": "…", "session": "conv-7f2a",
   "address": "joaquin/conv-7f2a", "epoch": 1,
   "expires_at": "…", "expires_in_seconds": 86400}
```

Send that as the bearer token from then on. It authenticates as your agent, in
that one session, and:

- **derives everything from the parent.** Agent and team come from the token
  that registered it, so a client cannot assert either;
- **mints nothing.** Not an agent token, not an administrative credential, not
  another session;
- **expires on its own** (24 hours by default, `ttl_seconds` for less) and
  **dies with its parent**: revoke the token or disable the agent and every
  session under it stops authenticating, with no sweep to wait for;
- **refuses a contradicting header.** A request whose `X-Crew-Session` names a
  different window is rejected, so a proven session can never be widened into
  somebody else's;
- **fences what it replaces.** Registering the same label again is a *resume*:
  new secret, `epoch` bumped, identity and history unchanged. Send the epoch
  as `X-Crew-Epoch` and a process that was resumed away is told so
  (`409`) instead of writing as the window that replaced it.

`renew_session` extends the credential you already hold without touching its
secret or epoch. `revoke_session` closes it, or another window of your own
agent by label. `whoami` reports `session_identity` when the label was proven
and `null` when it is only a header.

Nothing here is required: an agent token with a session header keeps working
for every tool and for a bare `curl`, exactly as before.

### One session per conversation: the stdio proxy

Sessions separate windows, but a header written once in a client config is
the same header in every window of that client. `ai-crew-sync mcp proxy` is a
local MCP server the client starts **once per conversation** — the norm for
stdio servers — so the process itself is the unit of isolation: it mints the
session, sends it on every forwarded call, and keeps that window's project
and role. It works with any MCP client; what a particular host offers is used
as an enrichment, never required.

```json
{
  "mcpServers": {
    "ai-crew-sync": {
      "command": "ai-crew-sync",
      "args": ["mcp", "proxy", "--role", "implementation"]
    }
  }
}
```

No token and no URL in the client config: credentials come from your local
profile and the project's `.acs.toml` (see above). Every remote tool appears
as usual, plus two that never reach the bus:

- `session_status` — verified agent and team, session id, **address**
  (`agent/session`), project, role, channel. Never credentials.
- `configure_session({role?, project?, channel?, profile?})` — this window
  only. Role and project are metadata: the session id, cursors, claims and
  locks are untouched. `profile` switches to another locally approved
  credential **of the same team**, verified with `whoami` before anything
  changes; a failed verification leaves the old context intact, in-flight
  calls to the old identity are cancelled rather than replayed, and what it
  still holds (claims, locks) is reported, never transferred. A different
  team needs a new conversation, because switching credentials cannot erase
  what this conversation has already seen.

The proxy does this for you: it registers the session on connect, forwards
every call with the credential and the epoch, renews the credential half-way
through its lifetime whether the window is busy or idle (`BUS_SESSION_TTL_SECS`
and `BUS_SESSION_RENEW_LEAD_SECS` tune the lifetime it asks for and the lead),
and clears the secret from its private state when the window closes. A
renewal the bus refuses is reported by `session_status` as a rejected
credential, never papered over with another identity. A bus too old to issue
credentials simply keeps the label-only connection.

**Conversation identity**, in order: `--host-session` / `BUS_HOST_SESSION`
(any host that can set a per-window variable), `CLAUDE_CODE_SESSION_ID`
(Claude Code sets it on MCP server processes), the `_meta.threadId` Codex
attaches to every call, otherwise the process itself. With a conversation id
the session label is **stable**, so a resumed conversation reconnects to the
same session and a fork gets a new one; without one, the session lives as
long as the process. If two conversations ever share one process, the proxy
refuses the second rather than mixing them.

Start-of-session context is delivered through the `initialize` result's
`instructions` — every MCP client passes those to the model, so no hooks are
needed. Where a host *does* have lifecycle hooks, they run
`ai-crew-sync context hook --binding <conversation id> --event <event>`: the
helper reads the private record the proxy wrote (0700 directory, 0600 file),
acts as **that** window with its own credential, and prints only what the host
expects. The credential never reaches argv, stdout or a log, a hook never
registers (so it cannot fence its own proxy), and a conversation with no
binding produces no output at all rather than acting as a shared identity.
Like the proxy itself, this authenticated hook mode needs the `ai-crew-sync`
binary on the PATH; the legacy mode still needs only `curl` and `python3`.

Presence is the proxy's own: a heartbeat on connect with the repo and branch
of the project directory, a keep-alive every five minutes, `idle` on exit. Nothing is pushed into an idle turn: incoming messages are read with
`read_messages` or awaited with `wait_for_updates`, as with a direct
connection.

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

#### Finding the right window: `project` and `role`

A session label is good at keeping claims and cursors apart and bad at being
typed by a person, especially once labels are opaque ids minted per
conversation. Two optional **discovery labels** on `heartbeat` fix that:
`project` (the logical project, usually the repository) and `role` (what the
window does there: `implementation`, `design`, `review`, …). Then:

```
list_sessions {"project": "market-data", "role": "review", "online_only": true}
→ {"sessions": [
     {"agent": "joaquin", "session": "s-9c0d1e2f", "address": "joaquin/s-9c0d1e2f",
      "project": "market-data", "role": "review", "status": "active", "online": true, …},
     {"agent": "joaquin", "session": "s-3a4b5c6d", "address": "joaquin/s-3a4b5c6d", …}],
   "count": 2, "limit": 200}
```

`address` is what goes in `to` (or `ask_agent`'s `to`), and **`exact` says
whether it reaches one window**. It is `true` for a named session and `false`
for the shared one, whose address is the bare agent name: that reaches *every*
window of that agent, named ones included, so it is never a private target and
there is no address that reaches the shared session alone. Two reviewers share
a role and keep two exact addresses: discovery returns both and the caller
picks one — nothing is ever routed to "whoever has the role", and a private
instruction is never broadcast to all of them. Labels are what a session said
about itself: not identity (that is the token), not a permission, and freely
shared by several windows. They also drive the default channel: a session that
declared `project = "market-data"` posts to `#market-data` when it names no
channel, whatever its session label is; an opaque label that matches no
channel gets no default rather than a surprising one. Omit a label to keep it,
send `""` to clear it. `whoami` reports both, and `list_agents` shows them
under each session.

Console: `ai-crew-sync client sessions --project market-data --role review
--online`, and `client beat --project market-data --role design`.

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
of your own sessions, and it reads that way: the task is `open` again,
`claimed_by` is null, `lease_expired` is true and `lapsed_holder` names who
let it go. `list_tasks {"status": "open"}` includes it; renewing it is refused,
claim it again instead.

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
export BUS_VERSION=0.7.0
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

**Other MCP clients** — Codex, Cursor, Zed, a script. Tools live on the
server, so a new tool or a new argument appears the next time the client
reconnects, with nothing to upgrade. Two exceptions live in the client's
config and have to be changed by hand: new *headers*, such as
`X-Crew-Session`, and the *proxy* — a client configured with `command:
ai-crew-sync` runs whichever binary is on the PATH, so that one is upgraded
with the binary, not with the server.

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
ai-crew-sync client sessions --project market-data --role review   # exact addresses
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

## Asynchronous publication (opt-in, per conversation)

The default is synchronous: a conversation message's body, recipients and
receipts commit in one Postgres transaction, so `stored: true` means that
transaction committed and there is nothing to reconcile. Nothing below slows
that down or changes it.

A conversation can opt into an **outbox** instead, which is the shape any
external store introduces: acceptance and persistence become two events, and
the second can fail, time out, or succeed without the caller hearing.

```
stored                the backend confirmed, with a canonical locator
pending_publication   accepted, not yet confirmed — and reported as such
failed                it will not be confirmed; the slot stays, explicit
```

A send on that path returns `stored: false`, and the receipts carry no
`stored_at` until the backend confirms. Slots are leased (60 s), fenced by a
generation so a worker that comes back after its lease expired writes
nothing, retried with backoff up to eight attempts, and bounded in payload
size. Network-like work happens outside every database transaction: a
publish that takes a minute costs a lease, not a lock.

An uncertain completion — the write landed and the confirmation was lost —
is resolved by asking the backend what it actually holds for the publish key,
not by guessing. Retries present the same key, so a duplicate physical write
resolves to one canonical locator rather than two messages.

Postgres is the default backend, and the only one an untouched installation
uses. The boundary exists so the failure handling could be built and tested
before there was anything external to blame for it; returning a conversation
to synchronous mode is refused while work is still outstanding, because a
thread would otherwise keep a gap nobody drains.

## JetStream: routing a team's conversation bodies

A JetStream adapter sits behind the backend boundary, and a team can be
routed to it. **A default installation never contacts a broker**:
`teams.default_backend` is `postgres`, ordinary teams stay on the
synchronous path, and NATS does not need to be running. Merging, installing
or upgrading moves nobody's data.

What is in place, proven against a real broker in the test suite:

- One **file-backed, bounded stream per team**, named from the team id so a
  rename never moves data and a slug never reaches the broker. Limits
  retention, `DiscardNew` when full: a full stream refuses new writes instead
  of quietly dropping history.
- **Provisioning is an operator action with its own credential.** The runtime
  credential publishes and fetches and cannot create or delete a stream; a
  team routed to JetStream without provisioning fails at startup with a
  message saying so, rather than at the first message.
- **NATS is internal.** No subject, stream or consumer name is ever a
  client-facing argument, and ACS checks every ACL itself.
- **`stored` still means acknowledged.** The adapter awaits the PubAck; a
  publish that has not been acknowledged is not stored, and is not reported
  as such.
- **Idempotency** on `Nats-Msg-Id`, using the outbox's publish key, so a retry
  inside the deduplication window returns the original sequence. Outside it,
  reconciliation asks the broker what it holds rather than guessing.

### The 1 MiB body contract needs two limits raised, not one

The stream's `max_message_size` is not enough. The **server's** own
`max_payload` defaults to 1 MiB, and a 1 MiB body plus its envelope headers
is about 1,048,800 bytes — refused by a couple of hundred bytes. A deployment
that raises only the stream limit rejects exactly the messages the body
contract allows.

Run the broker with `--max_payload 2MB` (the test fixture does), and a body
at the contract's ceiling is refused by neither. Bodies past the broker's
limit fail **fatally** rather than retrying for ever, along with a full
stream and a refused authorization; a timeout or a dropped connection stays
retryable.

The integration fixture is **required** from this phase: `make test` starts a
real NATS 2.12 with JetStream, and a missing broker fails the suite visibly.
A broker test that skips itself proves nothing and reads like a pass.

### Routing a team, and what routing does not do

Two independent steps, in this order, and neither implies the other:

```bash
# 1. The stream. An operator action with the PROVISIONING credential —
#    the server's own credential deliberately cannot create streams.
ai-crew-sync team stream --team acme --nats-url nats://broker:4222

# 2. The route. From now on, this team's NEW conversations store their
#    bodies on the broker.
ai-crew-sync team capability --team acme --backend jetstream

# And the server has to be able to reach it. In the compose stack that is
# NATS_REPLICAS=1 and BUS_NATS_URL=nats://nats:4222, which starts the broker
# the stack already carries (zero replicas until you ask for it):
ai-crew-sync serve --nats-url nats://broker:4222 \
                   --nats-credentials /etc/ai-crew-sync/runtime.creds
```

**Routing never migrates anything by itself.** A conversation records the
backend it was created on, and every message records where *its* body is;
routing back to Postgres affects new conversations only, and is refused
while anything is still awaiting publication.

Moving an existing thread is a separate, supervised operation:

```bash
ai-crew-sync conversations migrate --team acme --to jetstream \
    --conversation <id> --nats-url nats://broker:4222          # dry run
```

It copies every body, pauses writes **on that one thread** while it takes
the tail, reads every body back from the target and compares checksums, and
only then cuts over — in one transaction. A failure cuts nothing over and
lifts the pause; an interrupted run resumes without copying twice. Ids,
authorship, memberships and every observed receipt are untouched, and no
acknowledgement is ever invented. The rollback is the same command with
`--to postgres`, and the source bodies stay until you explicitly run
`conversations cleanup`.

**`docs/operations/jetstream.md`** has the production topology, the
credentials, the quotas and sizing, the alerts, the restore and node-loss
drills, and the limits — including the one worth knowing first: a body on
the broker is not in any Postgres index, so this build does not search it.

A team routed to JetStream on a server started without `--nats-url` does not
silently fall back — that would split the history. Reads of those threads
say what is wrong and what to do about it.

### What a reader is told while a body is in flight

On this path `stored` is not the same as accepted, and a read says which is
which. Every message carries a `publication`:

| `publication` | What it means |
|---|---|
| `stored` | The broker acknowledged it. The body is durable. |
| `pending_publication` | Accepted, not yet confirmed. The body is still readable from its temporary local copy; the receipts' `stored_at` is null, because it is not stored. |
| `failed` | It will not be published. The message keeps its slot, and its body is still served from the copy that never left Postgres — what is missing is durability on the backend, not the text. |
| `tombstoned` | The backend no longer holds the body (retention, or an operator). The message keeps its sequence, its recipients and its receipts; `unavailable` says why. |

A body the current backend cannot serve does not fail the page: the message
keeps its place and says what happened to it. Thread order is the sequence,
never the order the broker happened to confirm in, and a reader's cursor
cannot walk past a message still in flight.

Access is rechecked at the moment a body is served, not only when the
message was sent: a membership that ended while a publication was in flight
stops reading on the very next call.

### Draining, and running the drainer elsewhere

The process that serves requests also drains the outbox by default. An
attempt that ends without an answer is recorded as **uncertain** — not
stored, not failed, because either would be a guess — and reconciliation
presents the same envelope under the same idempotency key: inside the
broker's dedup window that returns the original sequence, outside it the
body lands then. One logical message either way.

To run dedicated drainer replicas, start the request-serving ones with
`--publication-worker false`.

## A worked example: design, implementation, review

Five conversations in one repository, one agent token, nothing exported.
Each window connects through `ai-crew-sync mcp proxy` with a role, so it has
its own session and its own address:

```
design           → configure_session {"role": "design"}
implementation   → configure_session {"role": "implementation"}
review (Claude)  → configure_session {"role": "review"}
review (Codex) × 2
```

**Design finds the implementation window and asks for a change.** Not by
guessing a name: `list_sessions {"project": "market-data", "role":
"implementation"}` returns one entry with an exact `address`, and a message
to it reaches that window and no sibling.

**A review that needs an answer from several people becomes a thread.**
`create_conversation` with the three addresses, each accepting for itself,
then one message. Later, `get_message_receipts` says implementation
acknowledged *and* resolved, one reviewer acknowledged, and the other has not
answered — three independent facts, none inferred from a cursor.

**Ending a window does not disturb the others.** Its claims stay claimed, its
inbox stays unread, its presence ages out on its own. Reopening the same
conversation resumes the same session; forking gets a new one.

What this does not do: wake an idle window. Nothing is pushed into a model
that is not in a turn — a window reads with `read_messages` or blocks with
`wait_for_updates` while it is working, and the `Stop` hook holds a turn open
long enough to answer a blocking question. Switching credentials also erases
nothing: the transcript is the host's, and every message and receipt stays
under the identity that made it.

`docs/acceptance/host-integration.md` has the verified host facts, the
unsupported topologies and a manual script for real clients, with what was
tested separately from what the automated suite simulates.

## Conversations: who was asked, and who answered

A channel broadcasts and a direct message points at one window. Neither
answers the question a review actually asks: *these three were asked, which
of them has seen it, and which has acted on it?* A channel cannot say, and
three direct messages are three threads that never converge.

A conversation is a thread with an explicit membership, a logical sequence,
and a **recipient snapshot per message**. It is opt-in per team:

```bash
ai-crew-sync team capability --team acme --conversations on
```

```
create_conversation {"title": "the empty state", "private": true,
                     "invite": ["dani/design", "dani/review"]}
send_conversation_message {"conversation_id": "…", "body": "…", "request_id": "<uuid>"}
→ {"seq": 2, "stored": true, "recipients": ["dani/design", "dani/review"], …}
get_message_receipts {"message_id": "…"}
→ {"total": 2, "acknowledged": 2, "resolved": 1, "receipts": [...]}
```

**Five observations, never inferred from each other**: `stored` (the database
committed), `delivered` (a transport handed it over), `presented` (a host
confirmed it reached the model), `acknowledged` (the recipient said it read
it), `resolved` (the recipient said it acted on it). An absent timestamp
means *not observed*, not "no" — `presented_at` is null wherever the host
cannot confirm injection, and that stays honest rather than optimistic.
Reading a thread acknowledges nothing, a cursor is not a person, and
resolving does not complete a task or merge anything.

**Membership is per window** (`agent/session`), and an invitation is not
enrolment: each window accepts with `join_conversation`, so nobody is
conscripted into someone else's receipts. A new member sees the thread from
where they joined unless the inviter deliberately grants the whole history.
Someone who joins later **never enters an older message's denominator**, and
removing someone keeps what they already said and acknowledged.

**Visibility is granted, never inferred.** A `private` thread is visible to
its members only; a project thread is visible to whoever holds an explicit
grant on that project (`create_project`, `grant_project_access`). A working
directory grants nothing, and neither does the `role` label a session
publishes for discovery. The choice is fixed at creation, because people
spoke in the thread on those terms.

**Two exceptional paths, both audited.** `transfer_membership` hands a seat
to another window *of your own agent*, and only once that window accepts;
authorship, history boundary and old receipts are preserved, and nothing is
acknowledged on your behalf. `recover_conversation_history` is the documented
exception that privacy inside a team is not isolation from the agent that was
in the thread: it needs your agent token, every window of that agent to be
closed, revoked or expired — offline is not enough — and it is read-only,
grants no membership, skips memberships you were removed from, and invents no
receipt.

Retries are safe: `send_conversation_message` takes a `request_id` UUID you
generate, and repeating it returns the original message instead of posting
twice. The same id with a different body is refused rather than silently
keeping the first.

### The inbox: what `delivered` is allowed to mean

On a thread routed to a broker, every window has its own durable inbox of
**references** — which messages exist for it, never their bodies. Two
recipients cannot take each other's, and one acknowledgement drains nobody
else's.

```
fetch_conversation_inbox {}
→ {"references": [{"delivery_id": "…", "message_id": "…", "seq": 7,
                   "from_address": "joaquin/impl", "kind": "message",
                   "redelivered": false, "source": "broker"}],
   "from_broker": 1, "more": false}
confirm_inbox_delivery {"delivery_ids": ["…"]}
```

Taking a reference is not receiving it. `delivered_at` is written only when
the holder says, in a separate call, that it is still holding the reference
after a restart — and `ai-crew-sync mcp proxy` makes that true by writing
the references to a 0600 file and **fsyncing before it confirms**. A crash
in between costs a redelivery, which is idempotent; confirming first would
cost the reference itself.

Delivered stays a different fact from presented, acknowledged and resolved.
Nothing here wakes an idle window: no host we support lets a third party
push into a model that is not in a turn, and a broker does not change that.

`conversation_inbox_status` reports both sides separately, because they
answer different questions:

| Field | What it is |
|---|---|
| `undelivered` | The authority: messages addressed to this window that nobody has confirmed holding. |
| `handed_out_unconfirmed` | References given to a process that never confirmed. After a crash this is expected; they are offered again. |
| `broker_pending` / `broker_awaiting_ack` | A cache. It may lag. |
| `broker_consumer_present` | `false` means the durable consumer is gone — expired, or removed. **That is not an empty inbox**: the bus rebuilds the references from its own records. |

## Remote administration

Once a global credential exists (`admin bootstrap`, above), everything else
happens from your own machine — no SSH, no `docker exec`, no database
connection.

### Agent, token, label, session

Five words that are easy to conflate, and the bus treats very differently:

| | What it is | Where it comes from |
|---|---|---|
| **agent** | An identity on the bus: who posts, claims, holds locks. One per coding tool per person (`joaquin`, `joaquin-codex`, `backend`). | `admin agent add` |
| **token** | A credential that *is* one agent. Several tokens can belong to the same agent; revoking one leaves the others working. | `admin token issue` |
| **label** | A note on a token for humans (`"backend repo"`, `"dani laptop"`). Display only: it never decides who the token is. | `--label` |
| **session label** | Which of an agent's windows *claims* to be calling, from the `X-Crew-Session` header. Partitions presence, claims and locks. Trusted for partitioning, never as proof. | `BUS_SESSION` / `--session` |
| **authenticated session** | A credential derived from the agent token that *proves* which window is calling. Required for a seat in a private conversation; a sibling window cannot take it by picking the same label. | `ai-crew-sync mcp proxy` (or `register_session`) |

One token per repository is the shape everything below assumes: the token
says *who*, the session says *where*. The label was enough while "where" only
had to separate presence and claims; once one window can be addressed and
held to a receipt that another must not be able to forge, it stopped being
enough — hence the credential. Both still work, and the label is not going
away.

### Daily administration: `ai-crew-sync admin`

```bash
ai-crew-sync admin login --url https://bus.your-company.com:8443   # prompts for acsa_… (hidden)
ai-crew-sync admin whoami

ai-crew-sync admin team add --slug roundcrew --name "RoundCrew"    # global only
ai-crew-sync admin agent add --team roundcrew --name backend
ai-crew-sync admin token issue --team roundcrew --agent backend --label "backend repo"
ai-crew-sync admin token list --team roundcrew
ai-crew-sync admin token revoke --team roundcrew --id <uuid>

ai-crew-sync admin grant --team roundcrew --label dani    # a credential for roundcrew's admin
ai-crew-sync admin credential list
ai-crew-sync admin credential revoke --id <uuid>
ai-crew-sync admin logout
```

`login` never takes the secret as an argument: it prompts with echo off, or
reads it from stdin with `--token-stdin` for scripts. It calls the bus first
and stores the endpoint and credential only if that succeeds, in
`~/.config/ai-crew-sync/admin` with mode `0600` (`BUS_CONFIG_DIR` moves the
directory; `BUS_ADMIN_URL` + `BUS_ADMIN_TOKEN` bypass the file for CI).

**Every minted token is verified before you see it.** `token issue` presents
the new token to `/mcp`, calls `whoami`, and requires the answer to be exactly
the agent and team you asked for. On any mismatch the token is revoked and
nothing is printed or saved — the label plays no part in this: identity is
what the server says, never what the request said.

The everyday form writes the token straight into the per-team file and never
prints it:

```bash
ai-crew-sync admin token issue --team roundcrew --agent backend \
    --label "backend repo" --save --repo backend
# token for backend@roundcrew verified and saved to ~/.config/ai-crew-sync/tokens-roundcrew as backend=…
```

`tokens-<team>` is one `name=token` line per repository (no quotes, no
spaces), plus `_base=` for the org root. `--save` replaces only the `backend=`
line: every other line survives, `_base` included, the write is atomic and
`0600`, a duplicate `backend=` left by a hand edit collapses into one, and the
previous token for that entry is **not** revoked (revoke it yourself when the
old window is gone). `--repo` is one safe word; the file path comes from the
team slug, never from the flag. A shell function that exports `BUS_TOKEN`
from that file per directory is all the plumbing a machine needs.

`bootstrap` is the only `admin` command that talks to Postgres. `credential
list --local` / `credential revoke --local` do too, for the day the last
global credential is lost; the classic `team`/`agent`/`token` commands stay
as they were.

### The API underneath (`/admin/*`)

The CLI is a thin client over a JSON API on the bus itself, usable with a
bare `curl`. It is deliberately **not** MCP: administration never shows up in
an agent's tool catalogue, and an agent token presented here is refused with
a message saying what to use instead.

```bash
export ADMIN=acsa_...
B=https://bus.your-company.com

curl -s $B/admin/whoami -H "Authorization: Bearer $ADMIN"
curl -s $B/admin/teams -H "Authorization: Bearer $ADMIN" \
     -H "Content-Type: application/json" -d '{"slug":"roundcrew","name":"RoundCrew"}'
curl -s $B/admin/teams/roundcrew/agents -H "Authorization: Bearer $ADMIN" \
     -H "Content-Type: application/json" -d '{"name":"backend"}'
curl -s $B/admin/teams/roundcrew/tokens -H "Authorization: Bearer $ADMIN" \
     -H "Content-Type: application/json" -d '{"agent":"backend","label":"backend repo"}'
     # → {"token":{"id":"…","token":"acs_…","agent":"backend","team":"roundcrew",…}}  (secret shown once)
curl -s $B/admin/teams/roundcrew/tokens -H "Authorization: Bearer $ADMIN"           # list, no secrets
curl -s -X DELETE $B/admin/teams/roundcrew/tokens/<id> -H "Authorization: Bearer $ADMIN"
curl -s $B/admin/credentials -H "Authorization: Bearer $ADMIN" \
     -H "Content-Type: application/json" -d '{"team":"roundcrew","label":"dani"}'   # team administrator
curl -s -X DELETE $B/admin/credentials/<id> -H "Authorization: Bearer $ADMIN"
```

| Route | Global | Team credential |
|-------|--------|-----------------|
| `GET /admin/whoami` | ✓ | ✓ (its scope) |
| `GET/POST /admin/teams` | ✓ | sees its own team only; cannot create |
| `GET/POST /admin/teams/{team}/agents` | ✓ | ✓ inside its team |
| `GET/POST /admin/teams/{team}/tokens` | ✓ | ✓ inside its team |
| `DELETE /admin/teams/{team}/tokens/{id}` | ✓ | ✓ inside its team |
| `GET/POST /admin/credentials` | ✓ | list its own team's; cannot grant |
| `DELETE /admin/credentials/{id}` | ✓ | its own team's only |

Every check runs on the server from the credential alone. A team credential
asking for another team gets `403` whether or not that team exists; a token
id from another team is `404`; the body can never widen the scope. Each
issue, grant and revocation is audited with the credential that acted, and
the only response that ever contains a secret is the one that mints it.

Two ceilings: `/admin` runs at a tenth of `BUS_RATE_LIMIT_PER_MINUTE`, and an
agent may hold at most 100 active tokens (revoke unused ones first).

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
make check    # pre-push gate: rustfmt, clippy -D warnings, compose file renders
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

The crate's MSRV is the `rust-version` in `Cargo.toml` (**1.98.1**). CI proves
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
  main.rs        CLI (serve / migrate / team / agent / token / webhook /
                 conversations / admin / context / mcp / client / *-config)
  serve.rs       axum + MCP Streamable HTTP transport + auth middleware
  auth.rs        bearer tokens -> AuthCtx (agent + team + session)
  context.rs     local resolver: profiles, .acs.toml, host-session binding
  proxy.rs       `mcp proxy` — one stdio MCP server per conversation
  hook.rs        `context hook` — what an authenticated lifecycle hook calls
  tools/         MCP layer (one tool per operation, typed with schemars)
  store/         all the logic and all the SQL; backend.rs + routing.rs pick
                 where a conversation body lives
  admin.rs       operator commands next to Postgres
  admin_api.rs   /admin/* REST API (acsa_ credentials, never MCP)
  admin_cli.rs   `ai-crew-sync admin …` against a remote bus
  client.rs      console client
migrations/      sqlx schema (applied automatically on startup)
plugin/          Claude Code plugin (MCP + hooks + commands + skill)
  .claude-plugin/plugin.json
  .mcp.json      starts `ai-crew-sync mcp proxy`; carries no credential
  hooks/         SessionStart (catch-up + heartbeat), Stop and SessionEnd
  scripts/       bus-call.sh, heartbeat.sh, session-start.sh, stop-drain.sh
                 (authenticated via the binary, or curl + python3 as fallback)
  commands/      /ai-crew-sync:standup|catchup|announce|ask|claim|done|handoff|
                 board|who|lock|unlock|note|wait|thread|inbox|review
  skills/        coordination conventions
Docker/          Dockerfile + the one compose file (published image, local build, Swarm-ready)
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
| Note scope / key | 64 B / 256 B | — |
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

## Architecture decisions

`docs/adr/0001-authenticated-sessions-and-staged-messaging.md` records the
accepted direction, and all seven of its phases are now implemented: session
credentials issued by the server and derived from an agent token, lifecycle
hooks backed by the `ai-crew-sync` binary, conversations and per-recipient
receipts on Postgres, and an optional JetStream route for conversation bodies
and inbox fanout.

Implemented is not the same as active. Conversations are off until an operator
turns them on for a team (`team capability --conversations on`), JetStream is
off until one both provisions a stream and routes the team to it, and a
default installation contacts no broker at all. Installing a release turns on
nothing by itself. The earlier behavior — all state in Postgres,
`X-Crew-Session` as a caller-supplied label, hooks that need only `curl` and
`python3` — keeps working throughout, and is still what a client with no
binary and an exported `BUS_TOKEN` gets.

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

<!-- related-projects:start -->
## Related projects

Repositories by the same author that this project depends on, and repositories that depend on it.

### Used by

| Repository | Description |
|------------|-------------|
| [homebrew-tap](https://github.com/joaquinbejar/homebrew-tap) | Homebrew formulae for joaquinbejar's tools. *(Homebrew formula)* |

<!-- related-projects:end -->
