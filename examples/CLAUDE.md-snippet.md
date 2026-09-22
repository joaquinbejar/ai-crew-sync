# Snippet for your repo's CLAUDE.md

Add something like this to the `CLAUDE.md` of every repository on the team, so
the agents use the bus without anyone having to ask them to:

```markdown
## Team bus

This project is connected to the team coordination bus (the `ai-crew-sync` MCP
server). Conventions:

- At the start of a session, call `whoami` and `read_messages` to see if
  teammates left anything relevant.
- Before starting non-trivial shared work, check `list_tasks` and `claim_task`
  so two people do not do the same job. Complete tasks with a useful `result`.
- Publish `heartbeat` with the repo, branch and a one-line activity when you
  start working, and when your focus changes. Add `project` and `role`
  (`implementation`, `design`, `review`, …) so teammates can find this window
  with `list_sessions` and address it exactly as `agent/session`.
- Record durable decisions and gotchas as notes (`set_note`) scoped to this
  repository's name, instead of leaving them only in chat.
- Post to the `deploys` channel before and after touching staging/production,
  and `acquire_lock` on "deploy:<env>" while you do it.
- When you are blocked waiting on a teammate (their task, their lock, their
  answer), call `wait_for_updates` instead of polling or giving up.
- At the start of a session, `team_digest` gives you the last 24h of team
  activity in one call.
- For the routine moves (catch up, claim, hand off, lock, note, wait, ask for
  a review…) follow the matching recipe: `recipes/<name>.md` in the
  ai-crew-sync repository, or `ai-crew-sync recipes <name>`. They are the same
  procedures the Claude Code plugin ships as slash commands.
- If `create_conversation` is in your tool list, the team has conversations
  on: use one when it matters who has seen a message and who acted on it, and
  record your own observation with `ack_message`. Reading is not
  acknowledging, and no tool lets you acknowledge on someone else's behalf.
```
