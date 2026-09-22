---
description: Team standup summary from the bus (messages, tasks, presence)
argument-hint: "[hours, default 24]"
---

Produce a standup-style summary of team activity using the ai-crew-sync MCP tools.

Steps:
1. Call `team_digest` with `hours` = $ARGUMENTS if a number was given, otherwise 24.
2. Call `list_agents` with `online_only` = false to see who is around and what they are working on, and `list_sessions` when you need the exact window behind a name (a teammate with three repositories open shows three).
3. Call `list_tasks` (open + in-progress) to see the state of the shared task queue.

Then write a compact standup report grouped as:
- **Done / shipped** (from digest activity and completed tasks)
- **In progress** (claimed tasks, per agent, with staleness warnings if a lease looks expired)
- **Blocked / needs attention** (blocked tasks, stale locks, unanswered questions in channels)
- **Idle capacity** (online agents with no claimed task)

Keep it short and factual; link task ids and channel names inline so items are actionable.
