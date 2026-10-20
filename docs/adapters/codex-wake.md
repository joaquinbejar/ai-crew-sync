# Codex wake adapter: contract, setup and evidence

A Codex thread whose turn has ended calls no tools. A message addressed to
its window is stored on the bus and waits, unread, until something starts a
new turn: usually the thread's human. `ai-crew-sync context codex-wake` is
an opt-in local process that starts that turn through the Codex app-server
when the bus has news for the window. This document is the contract it
keeps, how to run it, and what was verified on a real host.

## Contract

**Scope.** One adapter process serves one Codex thread, named on the command
line by its thread id. It is never started by installing ai-crew-sync, by
the proxy, or by a hook. Nothing changes for Claude Code, for Codex threads
without an adapter, or for any other MCP client.

**Binding.** The thread id must be the id Codex itself gave the window's
proxy: the binding record's source is `request-meta` (`_meta.threadId` on
the host's own tool calls). A window bound any other way (an explicit
`BUS_HOST_SESSION`, `CLAUDE_CODE_SESSION_ID`, or no id at all) is refused,
and so is a thread id no window is bound to. The adapter never guesses a
thread from a directory, a role, a window title or an agent name.

**Identity and credentials.** It acts as that window, with the window's
session credential read from the private binding (0600 file, 0700
directory). The credential never appears in argv, logs, the app-server
connection or the model's context. A closed window ends the adapter.

**What wakes the thread.** A direct message to the window newer than the
last one it woke for, or more undelivered conversation messages addressed to
the window. What was already waiting when the adapter started does not wake
the thread. Messages that arrive together (within two seconds) wake it once.

**What the turn carries.** A notice with counts and the tools to read them
with, never a message body: the model reads and answers with its own ACS
tools and authority. The notice says the messages come from teammates'
agents, not from the user.

**Exactly one turn at a time.** The adapter reads the thread's status first.

- If the thread is active (its human, or anyone, is in a turn), the wake is
  queued until the thread is idle, for up to 30 minutes, and never steered
  into the running turn.
- If the thread is not loaded, it is resumed first.
- If the thread is in an error state, the attempt fails.

There is still a race: a human could start a turn between the adapter's
last status check and its `turn/start`. Codex then steers the request into
that turn rather than starting a second one (the protocol's documented
`turn/start` behaviour on an active thread). That is the only path by which
the adapter's notice can reach a running turn.

**Duplicates and restarts.** The adapter records what it has woken the
thread for before it requests the turn. A crash between the two costs at
most one missed wake: the messages stay unread on the bus and the thread's
next turn sees them. It never costs a duplicate. The record survives
restarts, so a restarted adapter does not wake the thread again for the same
messages.

**One owner.** A second adapter for the same thread is refused while the
first is alive. A record not refreshed for 90 seconds belongs to a process
that is gone and may be taken over. SIGINT or SIGTERM releases the thread at
once.

**Retries.** A failed attempt is retried after 2, 5 and 15 seconds, then
recorded as failed. The next new message tries again.

**What it reports.** On the window's presence (`list_sessions`,
`list_agents`), under `attention`:

| Field | Value |
|---|---|
| `wake` | `codex-adapter` while the adapter runs; renewed every two minutes, it lapses within five if the adapter dies. Withdrawn on a clean stop. |
| `last_wake` | `requested`: Codex accepted the turn. `confirmed`: Codex reported the turn started. `failed`: every attempt failed. |
| `last_wake_at` | When that was recorded. |

None of these is a message receipt. A confirmed wake means a turn started;
it does not mean anything was read, acknowledged or answered. The adapter
writes no conversation receipts.

**Privacy.** Direct messages never reach the app-server, a webhook or the
dashboard. The app-server receives counts and tool names. The adapter
declines every request the app-server sends it (approval prompts included):
it has no authority to approve anything.

## Supported surfaces

| Surface | Status | Evidence |
|---|---|---|
| Threads hosted by the managed Codex app-server daemon (`codex app-server daemon`, socket `$CODEX_HOME/app-server-control/app-server-control.sock`) | Supported. Verified with codex-cli 0.163.0 | Real-host run below |
| Threads of the ChatGPT desktop app | Not supported | The desktop app runs its own app-server over stdio (`ChatGPT.app/.../codex app-server`, no `--listen`); nothing outside that process can reach it. Not tested further. |
| Codex CLI TUI sessions | Untested | Whether a TUI thread is hosted by the daemon was not verified. |
| Any other host | Not applicable | The adapter speaks only the Codex app-server protocol. |

The protocol facts the adapter relies on were checked against codex-cli
0.163.0:

- The daemon socket speaks JSON-RPC 2.0 over WebSocket.
- `thread/read`, `thread/resume`, `turn/start` and `thread/turns/list` take
  the shapes the adapter sends; `codex app-server generate-json-schema`
  generates the schema.
- A connection that did not start or resume a thread receives none of that
  thread's notifications. This is why the adapter confirms a turn by reading
  the turn list rather than waiting for `turn/started`.

## Setup

1. In the Codex thread, make sure ai-crew-sync is connected through the
   proxy (`[mcp_servers.ai-crew-sync]` with `ai-crew-sync mcp proxy`, or the
   plugin) and that one ai-crew-sync tool has been called in that thread.
   `session_status` is enough, and its `binding` must read `request-meta`.
2. Find the thread id. It is the `session_id` Codex passes to hooks, and the
   key the proxy bound; `codex app-server`'s `thread/list` shows it too.
3. Start the adapter where the Codex daemon runs, as the same user:

   ```bash
   ai-crew-sync context codex-wake --thread <thread-id>
   ```

   `--socket` overrides the daemon socket path, and `--max-secs` stops it
   after a while. It logs to stderr.
4. Check it from any window: `list_sessions` shows the thread's window with
   `attention.wake: codex-adapter`.

To disable it, stop the process (Ctrl-C or `kill`). Stopping it withdraws the
announcement. Uninstalling nothing is needed: no configuration changes.

## Evidence

### Automated (`tests/integration.rs`, fake app-server)

These tests prove the adapter's logic against a scripted app-server that
behaves like the real one: same framing, same methods, no notifications for
unsubscribed clients. They do not prove anything about Codex.

- `the_codex_adapter_wakes_an_idle_thread_for_news_only` covers these
  cases:
  - no wake for old unread messages;
  - the wake mechanism is announced;
  - a second adapter is refused;
  - one confirmed wake for one new DM, carrying counts and no body;
  - a busy thread is waited on, with no second turn (verified by breaking
    the wait and watching the test fail);
  - SIGTERM releases the thread and withdraws the announcement;
  - a restart does not wake again;
  - a refused `turn/start` is retried and recorded as `failed`.
- `the_codex_adapter_refuses_what_codex_did_not_bind` covers an explicit
  binding, a thread with no window, and a thread the app-server does not
  know.

### Real host (2026-10-20)

- Codex: codex-cli 0.163.0, managed app-server daemon 0.163.0, macOS.
- ai-crew-sync: this branch, against a throwaway bus on localhost with its
  own team.
- The thread was created for the test through the daemon (`thread/start`,
  `approvalPolicy: never`, read-only sandbox) with an extra MCP server
  running the proxy. A first turn called `session_status`, which bound the
  window `request-meta`. No existing thread was touched.

| Scenario | Result |
|---|---|
| Idle thread, the sender asks a question with `ask_agent` | The adapter started one turn; the model read the inbox and answered "17 + 25 = 42.". 18 s from the question to the answer, no human input. |
| Same, after a restart of the adapter | Answered "6 × 7 = 42." in 16 s; `last_wake: confirmed`. |
| DM arrives while a 30 s turn is running | The adapter waited for the running turn to end, then started the wake turn; the reply came 29 s after the DM. One turn per wake, none steered. |
| Adapter stopped with SIGTERM, restarted, left 60 s | No new turn; the thread kept 6 turns, the same as before the restart. |
| Wake mechanism shown to teammates | `attention.wake: codex-adapter` while running, absent after the stop. |

Not executed on the real host: a failed wake (only through the fake), a
thread that is not loaded, and the race with a human starting a turn.
