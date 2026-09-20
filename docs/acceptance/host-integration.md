# Host integration: what was tested, and how to test it yourself

Two kinds of evidence live in this repository, and they are not
interchangeable:

- **Automated**, in `tests/integration.rs`. These run the real binary against
  a real Postgres and a real HTTP/MCP surface. They prove the *bus and proxy*
  behave, including five simultaneous conversations in one repository. They
  do **not** prove anything about Claude Code or Codex: the "host" in those
  tests is the test itself.
- **Manual**, the script below. This is the only evidence that covers the
  actual clients. Run it, and record what you saw — including what did not
  work.

Anything not in one of those two lists is untested, and this document says so
rather than implying coverage.

## Verified host facts

Checked on 2026-09-20 against the versions named. A fact is listed only if it
was observed, not if the documentation merely claims it.

| Host | Version | Fact | How it was checked |
|---|---|---|---|
| Claude Code | 2.1.278 | Sets `CLAUDE_CODE_SESSION_ID` in the environment of stdio MCP server processes | Read the environment of a running MCP child with `ps -wwE` |
| Claude Code | 2.1.278 | Sets `CLAUDE_PROJECT_DIR` in the same environment | Same |
| Claude Code | 2.1.278 | Hook payloads carry `session_id`, `cwd`, `transcript_path`, `hook_event_name` on stdin | Official hooks documentation, and the payload our own hooks receive |
| Claude Code | 2.1.278 | `SessionStart` accepts `hookSpecificOutput.additionalContext` | Our plugin has used it since 0.6 |
| Codex CLI | 0.155.1 | Spawns one stdio MCP server process per thread (conversation); resume spawns a fresh one | `codex-rs` source: thread-owned MCP runtime, per-thread refresh |
| Codex CLI | 0.155.1 | Attaches `_meta.threadId` (equal to the hooks' `session_id`) to every `tools/call` | `codex-rs/core/src/mcp_tool_call.rs` and its tests |
| Codex CLI | 0.155.1 | Does **not** expand `${VAR}` in `[mcp_servers.*]` config values | `codex mcp get` returns the literal `${BUS_URL}` |
| Codex CLI | 0.155.1 | Does **not** implement `roots/list` | Client capabilities in `rmcp_client.rs` advertise only elicitation |
| Codex CLI | 0.155.1 | Hooks: `SessionStart`, `Stop`, `SessionEnd`, `UserPromptSubmit` and others, with the same payload keys and `additionalContext` contract as Claude Code | `codex-rs/hooks/src/schema.rs` and the hooks documentation |
| Both | — | No third-party MCP server can push a message into an idle model turn | Codex logs server notifications without routing them to the model; MCP has no such client obligation |

## Unsupported topologies

State these to users rather than letting them discover them:

- **One MCP process serving several conversations.** The proxy binds to the
  first conversation id it sees and refuses a second, rather than mixing two
  windows' cursors and claims. If a host ever multiplexes this way, that host
  needs an explicit per-conversation binding.
- **Background delivery into an idle window.** Nothing wakes a model that is
  not in a turn. `wait_for_updates` and `wait_for_conversation_updates` block
  *during* a turn; the `Stop` hook holds a turn open long enough to answer a
  blocking question. A window parked at the prompt for an hour answers
  nothing, and no amount of infrastructure changes that. The durable inbox
  is no exception: it makes a reference survive a restart, not a model wake
  up. Nothing on this list becomes supported because a broker is involved.
- **Sharing one agent token between two tools without separate sessions.**
  Two clients that send no session header are the same window to the bus.
  That is what the proxy exists to prevent.

## Manual acceptance script

Run this against a real bus with two real clients. It takes about ten
minutes. Record the result of every step, including failures.

### Setup (once)

```bash
# 1. A profile, so no client config holds a token.
ai-crew-sync context profile add --name acme \
    --url https://bus.your-company.com:8443 --team acme --agent joaquin --default
ai-crew-sync admin token issue --team acme --agent joaquin --save --repo market-data
cd ~/Repos/acme/market-data
ai-crew-sync context set-project --profile acme --project market-data
ai-crew-sync context verify          # must print joaquin@acme

# 2. Conversations, if you are testing phase 2 as well.
#    (operator, next to Postgres)
ai-crew-sync team capability --team acme --conversations on
```

Claude Code, in `.mcp.json` or the plugin:

```json
{ "mcpServers": { "ai-crew-sync": {
    "command": "ai-crew-sync", "args": ["mcp", "proxy", "--role", "implementation"] } } }
```

Codex, in `~/.codex/config.toml` (`ai-crew-sync proxy-config --format toml` prints it):

```toml
[mcp_servers.ai-crew-sync]
command = "ai-crew-sync"
args = ["mcp", "proxy", "--role", "review"]
```

Neither carries a token. `BUS_TOKEN` must be **unset** for this test.

### Steps

| # | Do this | Expect |
|---|---|---|
| 1 | Open five conversations in the same repository: one Claude implementation, one Claude design, one Claude review, two Codex reviews. In each, ask the model to call `session_status`. | Five different `session` values, all `agent` `joaquin`, `connected: true`. No per-window export was needed. |
| 2 | In the design window: `list_sessions` with `project: market-data`. | All five, each with its own `address`. The two Codex reviewers share `role: review` and keep distinct addresses. |
| 3 | From design, send a direct message to the implementation window's exact address. | Only that window's `read_messages` returns it. The other four see nothing. |
| 4 | Ask the implementation window to `configure_session` with a different `role`. | `session_status` shows the new role, the same `session`, and the other windows are unaffected. |
| 5 | Close the design window. Reopen the same conversation (resume). | `session_status` reports the same `session` as before. |
| 6 | Fork the conversation (a new conversation in the same repository). | A different `session`. |
| 7 | With conversations enabled: from design, `create_conversation` inviting the implementation window and both reviewers; each joins; design sends one message. | `recipients` lists exactly the three that joined. |
| 8 | Two of them `ack_message`, one with `resolved: true`. Design calls `get_message_receipts`. | `acknowledged: 2`, `resolved: 1`, and the third shows null timestamps — *not answered*, not "no". `presented_at` is null everywhere. |
| 9 | Revoke the agent token (`ai-crew-sync admin token revoke`). Use any window. | The window reports the credential was revoked or rotated and says what to do. Nothing was sent. |
| 10 | Restore a working token, then kill a proxy process abruptly and reopen that conversation. | It resumes; its claims and locks are still its own; no sibling window was idled or drained. |
| 11 | With a team routed to a broker: send a message to three windows; each calls `fetch_conversation_inbox`. Then `get_message_receipts`. | Each window gets exactly its own reference, with no body. `delivered_at` is set for the windows whose proxy confirmed, and null for any that did not — never for all three because one of them answered. |
| 12 | Kill one window's proxy between the fetch and the confirm (`kill -9` the process), then reopen it and call `fetch_conversation_inbox` again. | Its spool file under the state directory still lists the reference; the reopened window confirms it and `delivered_at` appears then, not before. `conversation_inbox_status` reported `handed_out_unconfirmed: 1` in the meantime — not an empty inbox, and not a receipt. |

### Reporting

Write down, for each step: host and version, what happened, and whether it
matched. Steps that cannot be run (no second client, no operator access) are
**not passes** — mark them untested. A run where step 8 shows
`presented_at` set would be a bug worth filing, not a better result.

## Migrating from `BUS_TOKEN` and shell wrappers

The old setup exported `BUS_URL` and `BUS_TOKEN` per directory, often through
a `claude()` function in `.zshrc`. That keeps working and is still supported.
To move off it:

1. `ai-crew-sync context profile add …` once per bus, and
   `ai-crew-sync admin token issue --save --repo <name>` per repository. The
   token files are the same `tokens-<team>` files the wrapper read.
2. `ai-crew-sync context set-project --profile <name> --project <name>` in
   each repository, and commit `.acs.toml`. It holds no secret.
3. Point the client at `ai-crew-sync mcp proxy` instead of the HTTP endpoint.
4. Stop exporting `BUS_TOKEN`. Leave it exported and it still wins, which is
   the compatibility path, not a mistake.
5. The wrapper can go. Nothing reads the environment any more.

**The proxy introduces a prerequisite**: the `ai-crew-sync` binary on the
PATH, for the client and for authenticated hooks. Legacy hook mode still
needs only `curl` and `python3`, and a direct HTTP MCP client needs neither.

## What switching credentials does not do

`configure_session` can change which profile a window uses, within the same
team. It does **not** erase anything: the conversation's transcript is in the
host, the bus keeps every message and receipt under the identity that made
them, and the claims and locks the previous identity held stay with it until
their leases expire. A cross-team switch is refused outright, because the
transcript already holds the other team's material and no credential change
can un-say it.
