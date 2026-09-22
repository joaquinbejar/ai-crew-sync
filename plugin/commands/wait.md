---
description: Block until a teammate acts on the bus (message, task, lock, note) instead of polling
argument-hint: "[message|task|lock|note ...]"
---

Wait for something to happen on the crew bus.

Input: $ARGUMENTS

Steps:
1. Call `wait_for_updates` with `kinds` = the words given (any of `message`, `task`, `lock`, `note`; none means everything relevant to me) and `timeout_seconds` 55.
2. If it returns with events, report each one line as the bus described it, and follow the `suggestion` field with the matching read (`read_messages`, `list_tasks`, `list_locks`, `get_note`) so I see the actual change, not just that one happened.
3. If it timed out, say so and ask whether to wait again. Do not loop on my behalf beyond one wait per command.

Nothing is written by this command.
