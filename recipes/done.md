<!-- slash: description="Complete a task you hold and post a one-line result to the team" hint="<task-key> [result]" -->
Finish a task on the crew bus.

Input: {{input}}

Steps:
1. The first word is the task key; the rest is the result. If no result was given, write one line from what I did in this session (what shipped, where: a PR, a commit, a note key). Keep it under 200 characters.
2. Call `complete_task` with `key` and `result`.
3. If it is refused because somebody else holds the task, say who and until when, verbatim, and stop: finishing another window's work is their call, not mine.
4. On success, post the result to the channel with `post_message`: the channel is `general` unless the task's description names one. One line: `done <key>: <result>`. No `announce`.

Confirm with the key, the result as recorded, and the message id.
