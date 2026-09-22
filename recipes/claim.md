<!-- slash: description="Claim a shared task (or the next unblocked one) with a lease and show the brief" hint="<task-key | next> [lease minutes, default 15]" -->
Take a task from the crew bus queue so nobody else does the same work.

Input: {{input}}

Steps:
1. The first word is a task key, or `next` for whichever unblocked open task is oldest. A second word, if numeric, is the lease in minutes (default 15; the bus clamps it to its limits). Anything else: ask me, do not guess.
2. If I already hold a claimed task (`list_tasks` with `mine_only: true`), stop and tell me which one before taking a second: one task at a time unless I say otherwise.
3. Call `claim_task` with `key` (or `claim_next_task` for `next`) and `lease_seconds` = minutes × 60.
4. If the claim is refused, report the reason verbatim: it names who holds the task and when the lease ends, or which dependencies block it. Do not retry in a loop; suggest `/ai-crew-sync:wait task` if I want to be told when it frees up.
5. If it succeeded, call `get_task` for the key and show me the brief: title, description, dependencies, attachments (names only), who created it and when.

Confirm with the key, the lease end, and the rule: renew with `renew_task_lease` before it lapses, `/ai-crew-sync:done` when finished, `/ai-crew-sync:handoff` if someone else should continue. A lapsed lease reads as open and anyone may take the task.
