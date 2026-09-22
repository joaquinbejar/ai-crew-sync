---
description: Show the shared task board and the resource locks at a glance
argument-hint: "[open | claimed | blocked | done]"
---

Show me the state of the crew bus task queue.

Input: $ARGUMENTS

Steps:
1. Call `list_tasks` with `status` = the input if given, otherwise `any`, and `limit` 200.
2. Call `list_locks`.

Then print a compact board:
- **Claimed**: key, title, holder (`agent/session`), lease left. Flag anything with less than five minutes left.
- **Lapsed**: tasks whose `lease_expired` is true, with `lapsed_holder`: they are open, anyone may take them.
- **Blocked**: tasks with `blocked` true and what they depend on.
- **Open**: key and title, oldest first.
- **Done recently**: key, who, result (one line), at most ten.
- **Locks**: name, holder, purpose, time left.

Keep it to one screen; counts first, then rows. Do not claim, release or change anything from this command.
