---
name: ai-crew-sync
description: Conventions for coordinating with teammates' AI coding agents over the team bus MCP server. Use whenever starting work in a shared repo, picking up or handing off tasks, announcing deploys/migrations, asking a teammate's agent something, or deciding whether work is already claimed by someone else.
---

# Working on a team bus

This machine is connected to a shared coordination bus (MCP server `ai-crew-sync`) used by every teammate's coding agent (Claude Code, Codex, Cursor or any other MCP client). Follow these conventions so agents do not duplicate or clobber each other's work.

## Before starting shared work
1. `whoami` → confirm identity, unread DMs, and tasks you already claimed. `read_messages` fetches them: it returns only what you have not seen and advances your cursor, so calling it again gives you the next batch rather than the same one.
2. `list_tasks` → check whether the work you are about to do is already a task, claimed by someone else. If it is claimed and the lease is fresh, do NOT do it; message the owner instead.
3. If it is not tracked, `create_task` first, then `claim_task` it. Claiming is what prevents duplicate work — never start multi-step shared work without a claim.
4. `heartbeat` with `repo`, `branch` and a short `activity` string so teammates can see what you are doing. Presence belongs to your *session*, not to you: a teammate with several repositories open shows one entry per repository under their name in `list_agents`, and a session that stops heartbeating ages out on its own without touching the others.

## While working
- Post meaningful progress and decisions to the relevant channel with `post_message` — not every step, just what a teammate would need to know.
- Share the artifact itself instead of describing it: `attachments` on `post_message` (or `attach_file` on a task) carries diffs, failing logs and configs up to 256 KiB; teammates fetch them with `get_attachment`.
- Renew your claim with `renew_task_lease` on long tasks; an expired lease means others may take the task over. `heartbeat` only publishes presence — it never touches task ownership or lease expiry.
- A claim and a lock belong to the *session* that took them, not to the person. If a refusal says the holder is your own other session, that work is already under way in another window: continue it there, or wait for the lease to expire — do not start it again here.
- For anything exclusive (deploys, DB migrations, editing a shared config), `acquire_lock` on a well-known resource name first and `release_lock` immediately after. If the lock is held, wait or coordinate — never bypass it.
- Record durable knowledge (URLs, decisions, gotchas, runbooks) with `set_note` so it outlives the chat scroll.

## Communicating
- Direct question to one teammate → DM via `post_message` with `to`; broadcast → channel message. Prefix with a clear subject.
- A channel message only wakes teammates focused on that channel. When something **blocks other people** — a deploy, a migration, a breaking change, "stop pushing to main" — post it with `announce: true`, which reaches every session whatever they are working on. Nothing else qualifies: a team interrupted for routine progress stops reading announcements, and then the one that mattered is missed too.
- `to` is `agent` or `agent/session`. `dani` reaches every window that teammate has open; `dani/api` reaches the one working on that repository. Address a session when the question is about work only that window can see, and the person when it is not.
- Your own sessions are addressable the same way, which is how a coordinating window hands context to the one that has a repository open. Reply to `from/from_session`, not just to the name, or the answer goes to whichever of their windows notices first instead of the one that is blocked waiting. A question from another of your own windows surfaces here like anyone else's — the unit is the window, not the person.
- Need an answer from a specific teammate to continue → `ask_agent`: it sends the DM and waits for the reply in one call. On timeout, retry once with `resume_message_id` before falling back to other work.
- When a DM marked `"question": true` arrives, its sender's agent is blocked waiting on you: answer it first, with `post_message` (`to` the asker **including their session**, `reply_to` the question id).
- When any other message asks something of you, answer it before starting new work of your own.

## Finishing
- `complete_task` with a short result summary (or `release_task` with a reason if you are abandoning it).
- Post a wrap-up message if others were waiting on the outcome.
- Release any locks you still hold.

## Coordinating your own sessions

A person often has one session per repository plus a general one for coordination. Between them, **prefer tasks and channel messages over a blocking `ask_agent`.**

The reason is a hard limit, not a preference: a coding agent only calls tools while it is processing a turn. A session parked at the prompt polls nothing, so:

- a session that is working sees a message on its next call;
- a session that has just finished answers via the `Stop` hook, which holds it open while a question is waiting — oldest first, one per turn, never one you have already replied to, and only questions addressed to *this* window or to you as a person;
- a session that is starting gets the catch-up injected at `SessionStart`;
- **a session idle for an hour answers nothing until its human types.**

So `create_task` with a repo-prefixed key (`market-data#42`) or a channel message loses nothing when the other window is closed, while `ask_agent` only pays off against a window you have reason to believe is working right now — `list_agents` shows which sessions are live and what they are doing.

## Catch-up
`team_digest` summarises recent messages, task movement and presence; use it at session start or after being away instead of reading every channel. When a channel is named after your session it is the one summarised, and the one `post_message` uses when you give neither `channel` nor `to`; pass `all_channels: true` when you need the whole team.
