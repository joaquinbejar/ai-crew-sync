<!-- slash: description="Block until a teammate acts on the bus (message, task, lock, note) instead of polling" hint="[message|task|lock|note ...] [inbox|<channel>]" -->
Wait for something to happen on the crew bus.

Input: {{input}}

Steps:
1. Call `wait_for_updates` with `kinds` = the words given (any of `message`, `task`, `lock`, `note`; none means everything relevant to me) and `timeout_seconds` 1800 (the proxy keeps waiting on the bus for that long in one call; connected directly over HTTP, use 55). If the input names `inbox` or a channel, pass it as `scope`, so channel chatter I do not read does not end the wait.
2. If it returns with events, report each one line as the bus described it, and follow the `suggestion` field with the matching read (`read_with` for messages, `list_tasks`, `list_locks`, `get_note`) so I see the actual change, not just that one happened.
3. If it timed out, say so and ask whether to wait again. Do not loop on my behalf beyond one wait per command.

Nothing is written by this command.
