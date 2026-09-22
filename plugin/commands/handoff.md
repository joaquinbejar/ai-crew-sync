---
description: Hand a task you hold to a teammate with its state written down first
argument-hint: "<task-key> <agent[/session]> [what is left]"
---

Pass my task to a teammate so the work continues without a meeting.

Input: $ARGUMENTS

Steps:
1. The first word is the task key, the second the teammate (`agent` or `agent/session`); the rest, if any, is what is left to do. Check the teammate exists with `list_agents` (and the session with `list_sessions` when given); if not, ask me instead of guessing.
2. Call `get_task` and confirm I hold it. If I do not, say who does and stop.
3. Write the state down first: call `set_note` with `scope` = the task key, `key` = `handoff`, and a value with three parts: what is done (from this session), what is left (from the input, or ask me), and where things are (branch, PR, files). Show me the value before writing it if anything in it is a guess.
4. Then release: `release_task` with the key.
5. Then point: `post_message` with `to` = the teammate and a body that names the task key, says I released it, and tells them to read the note (`get_note` scope `<key>` key `handoff`) and claim it.

Confirm with the note reference, the release, and the message id. Order matters: note, release, message, so the pointer never arrives before the state.
