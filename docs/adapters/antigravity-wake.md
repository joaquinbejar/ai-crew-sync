# Waking an idle Antigravity conversation: a pattern, not an adapter

Status: **experimental, and not part of the binary.** Antigravity has no
background hooks, so a message addressed to an idle conversation waits until
something starts a turn (`attention.wake: unknown`, see
[host-integration.md](../acceptance/host-integration.md)). Codex has an opt-in
adapter for the same gap ([codex-wake.md](codex-wake.md)). There is no
Antigravity counterpart in `ai-crew-sync`. This page describes the pattern a
local helper can use, what was checked, and what was not, so nobody has to
rediscover it. Discussion: #263.

## The pattern

A local process, run as the user who runs Antigravity:

1. waits on the bus with `ai-crew-sync client wait` under a session label of
   its own, so it never moves the reading cursor of the user's conversations;
2. when something addressed to the agent arrives, delivers a short prompt into
   an Antigravity conversation with the `agentapi` command that ships with
   Antigravity:

   ```
   agentapi new-conversation [--title=<title>] [--model=<model>] <prompt>
   agentapi send-message <conversation_id> <content>
   agentapi get-conversation-metadata <conversation_id>
   ```

   `new-conversation` once, then `send-message` into the same conversation;
   `get-conversation-metadata` tells a vanished conversation from a transient
   error.

## What was checked

On Linux with Antigravity app 2.32.1 and `agy` 1.4.12:

- `~/.gemini/antigravity/bin/agentapi` is a shell wrapper around
  `language_server agentapi`, and `agy agentapi --help` prints the same three
  commands.
- Outside a running Antigravity, `agentapi` fails with
  `{"error": "ANTIGRAVITY_LS_ADDRESS is not set"}`.
- Antigravity writes `~/.gemini/antigravity/daemon/hub_<pid>.json` with the
  keys `csrfToken`, `httpPort`, `httpsPort`, `lsVersion` and `pid`.

Not run on that machine: a live `send-message` into a running app. End to end,
the pattern has been used on Windows with the Antigravity desktop app, where
the helper delivered bus messages into one conversation for several days.

## What is not known

- Whether the id field of the `new-conversation` reply is stable. A helper
  should look for `conversationId`, `cascadeId` or `id`, then for a UUID in
  the text, and open a new conversation when it finds none.
- Whether the hub descriptor layout is something Antigravity intends to keep.
- Behaviour on macOS.

## Things that are not obvious

- `agentapi` needs the language server's address and CSRF token
  (`ANTIGRAVITY_LS_ADDRESS`, `ANTIGRAVITY_CSRF_TOKEN`). After Antigravity
  restarts both change. A long-running helper must re-read the newest hub
  descriptor whose `pid` is alive before each call; one that kept the
  environment from its own start got "address is not set" or HTTP 403.
- On Windows, multi-line prompts break when they pass through `cmd.exe`
  (`agentapi.bat`). Call `language_server.exe agentapi` directly and pass a
  one-line prompt that points at a file holding the message.
- Reuse one conversation. Opening one per message fills the history quickly.
- One reader per agent. `read` moves one cursor per agent, so a polling
  sidecar plus a helper split the unread messages between them.

## Security

Messages come from other agents, and some of them from other people's agents.
An agent that acts on bus messages unattended should:

- authorize the sender from the `from` and `from_session` fields **in code**,
  not in the prompt: the bus sets them from the token, so they cannot be
  forged by the message text;
- treat the body as untrusted input. Hand it over as a file the agent reads,
  never as an instruction in the prompt, and keep the prompt itself free of
  message text;
- never let the helper approve anything on the agent's behalf.

The same applies to any other host's unattended loop, not only Antigravity.
