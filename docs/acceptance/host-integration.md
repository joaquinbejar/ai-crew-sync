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
| Claude Code | 2.1.300 | A Stop or SessionStart hook with `async` + `asyncRewake` that exits 2 wakes an idle window and shows its stderr to the model, also after 11 minutes with `timeout: 14400` | #203, measured on a real window |
| Codex CLI | 0.163.0 | A managed app-server daemon serves threads on `$CODEX_HOME/app-server-control/app-server-control.sock`: JSON-RPC 2.0 over WebSocket; `thread/read`, `thread/resume`, `turn/start` and `thread/turns/list` as the adapter uses them | `codex app-server generate-json-schema`, and a real throwaway thread (#222) |
| Codex CLI | 0.163.0 | A connection that did not start or resume a thread receives none of its notifications | Real thread: `turn/started` never reached the adapter's connection (#222) |
| Codex CLI | 0.163.0 | Runs the installed ai-crew-sync plugin's hooks (SessionStart, Stop) for a thread | `hook/started` notifications on a real thread |
| ChatGPT desktop app | — | Runs its own app-server over stdio (`ChatGPT.app/.../codex app-server`, no `--listen`); nothing outside the app can reach its threads | Process listing on 2026-10-20 |
| Antigravity CLI (`agy`) | 1.4.3 | Exports `ANTIGRAVITY_CONVERSATION_ID` to hooks, not to MCP servers; sends the same id as `_meta["antigravity.google/conversation_id"]` on every `tools/call` | A probe plugin dumping environment and requests (#228) |
| Antigravity CLI (`agy`) | 1.4.3 | Hook payloads are camelCase (`conversationId`, `workspacePaths`, `terminationReason`); SessionStart takes `injectSteps`, Stop takes `decision: continue`; hooks run synchronously from the plugin's directory; there are no background (async) hooks | Its bundled hooks documentation, and real runs (#229) |
| Antigravity CLI (`agy`) | 1.4.3 | A globally installed plugin starts its MCP server in the plugin's directory, not the workspace | A wrapper logging the server's working directory (#230) |

## Capability matrix

Checked on 2026-10-20, ai-crew-sync at the 0.7.7 branch, against the host
versions named. "Real" means observed on that host; "automated" means
covered only by `tests/integration.rs` or `plugin/scripts/test-hooks.sh`,
where the host is simulated; "untested" means neither, and is not a pass.

| Capability | Claude Code 2.1.304 (plugin) | Codex 0.163.0 (daemon thread) | Antigravity `agy` 1.4.3 (plugin) |
|---|---|---|---|
| Connected proxy, one authenticated session per conversation | Real (in daily use) | Real (#222 run) | Real (#228 run) |
| Startup catch-up | Real (SessionStart `additionalContext`) | Hooks run (observed); content not inspected: untested | Real (`injectSteps`, #229) |
| Stop drain answers a waiting question | Automated; real untested in this round | Untested (hooks run; whether `block` is honoured was not checked) | Real, once the conversation is bound (#229) |
| Wait during an active turn (`wait_for_updates` up to 1800 s through the proxy) | Automated; real untested in this round | Automated; Codex needs `tool_timeout_sec` (examples/codex-config.toml); real untested | Untested |
| Waking an idle window | Real via the background watcher, `attention.wake: claude-watcher` (#203, measured on 2.1.300; not re-run on 2.1.304) | Real via the opt-in adapter, `context codex-wake`, `attention.wake: codex-adapter` (#222); without it: unsupported | Unsupported: no background hooks (`attention.wake: unknown`) |
| Reconnect / resume keeps the session | Automated | Untested | Real, with a limit: a resumed conversation binds on its first ai-crew-sync tool call (#229) |

What the observations above mean, and what they do not:

- **Stored is not delivered.** A message is stored on the bus whatever the
  recipient is doing, so a timed-out `ask_agent` is not a failed delivery
  (#224).
- **Connected is not attending.** A live proxy proves a process, not a model
  in a turn (#221).
- **Wake requested is not presented.** `last_wake: requested` means the host
  accepted a new turn, and `confirmed` means it reported the turn started.
  Neither means the message was read or answered.
- **An active-turn wait is bounded.** It holds a turn open at most
  `timeout_seconds`, and the host may cut it sooner: Codex at 60 s unless
  `tool_timeout_sec` is raised.

## Unsupported topologies

State these to users rather than letting them discover them:

- **One MCP process serving several conversations.** The proxy binds to the
  first conversation id it sees and refuses a second, rather than mixing two
  windows' cursors and claims. If a host ever multiplexes this way, that host
  needs an explicit per-conversation binding.
- **Background delivery into an idle window, without a wake mechanism.** The
  bus cannot push into a model that is not in a turn. `wait_for_updates` and
  `wait_for_conversation_updates` block *during* a turn; the `Stop` hook holds
  a turn open long enough to answer a blocking question. Only two mechanisms
  start a new turn in an idle window:
  - Claude Code's background watcher (`asyncRewake`);
  - the opt-in Codex adapter (`context codex-wake`), for threads hosted by
    the managed app-server daemon.

  Everywhere else (Antigravity, ChatGPT desktop app threads, Codex without
  the adapter, any other MCP client) a window parked at the prompt answers
  nothing until its human types. `list_sessions` says which case a window is
  in (`attention.wake`). The durable inbox is no exception: it makes a
  reference survive a restart, not a model wake up, and nothing on this list
  becomes supported because a broker is involved.
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

### Waking an idle window (run per host)

The scenario #225 asks for: the sender asks after the recipient's turn has
ended, the recipient stays idle, and each observation is recorded separately.
Use a second agent as the sender (`client ask`, or another window).

| # | Do this | Record |
|---|---|---|
| 13 | Let the recipient finish its turn and leave it idle. From the sender, `list_sessions`. | The recipient's `attention.state` (`idle` where a Stop hook reports it, `attending` turning `unknown` where none does) and `attention.wake`. |
| 14 | Sender: `ask_agent` to the recipient's exact address with a short question, default timeout. | `question_stored`, the `recipient` object and the suggestion. On a timeout: was it the "no way to be woken" or the "wake mechanism" wording, and did it match step 13? |
| 15 | Wait without touching the recipient. | Whether a turn started on its own; `last_wake` (`requested` or `confirmed`) where a wake mechanism exists; whether and when the answer arrived (`answered`, time from the question). |
| 16 | Send two direct messages within one second. | One wake, not two (bursts are coalesced). |
| 17 | Restart the wake mechanism (a new Claude Code turn ends; restart `context codex-wake`) without new messages. | No wake for messages already woken for. |
| 18 | Open a second window of the same agent with the same role; address only the first. | Only the addressed window wakes. |
| 19 | Stop the wake mechanism (Codex: stop the adapter) and ask again. | `attention.wake` back to `unknown`, the "no way to be woken" wording, and no turn. |
| 20 | A message arriving exactly while the recipient's turn ends. | Whether the Stop drain took it (same turn) or the wake did (new turn). Either is fine; losing it is a bug. |

Results on 2026-10-20:
- **Codex 0.163.0 with the adapter:**
  - steps 13–15 woke and answered in 16–18 s, `confirmed`;
  - step 17 caused no repeat wake;
  - a message sent during a busy turn was queued and answered 29 s later.
- **Antigravity:** steps 13–15 behave as unsupported, by design.
- **Claude Code** and every remaining step: untested in this round.

### When a window should have woken and did not

Look from the sender's side first: `list_sessions` (or `client sessions`)
shows the window's `attention`.

- **Claude Code, `wake: unknown`.** The watcher is not running. It starts
  at SessionStart and after every turn, as an `async` + `asyncRewake` hook.
  1. Run `ai-crew-sync context hook --binding <session id> --event status`.
     It must say `authenticated`.
  2. `no-credential` means the window's proxy closed: start a new turn.
  3. `missing` means the plugin's proxy never bound this conversation.
- **Codex, `wake: unknown`.** No adapter is running for that thread. Start
  `ai-crew-sync context codex-wake --thread <thread id>`. If it refuses:
  - "not bound by Codex": the thread has not called an ai-crew-sync tool
    yet;
  - "already waking": another adapter runs, or one died less than 90 s ago;
  - "does not know thread": the thread is not hosted by the managed daemon
    (`codex app-server daemon version`).
- **Codex, `last_wake: failed`.** The adapter reached the daemon but every
  `turn/start` failed. Its stderr has the reason. Check that the thread is
  not in an error state, and restart the adapter.
- **`last_wake: requested`, never `confirmed`.** Codex accepted the turn,
  but the adapter could not see it start. Look at the thread itself before
  doing anything else: no second turn is requested for the same news.
- **Antigravity.** No idle wake exists. A resumed conversation binds on its
  first ai-crew-sync tool call; until then its hooks stay silent.

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
   each repository. It lists `.acs.toml` in `.git/info/exclude`: the file
   names profiles from your own `profiles.toml`, so it stays local unless the
   whole team uses the same profile names.
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
