---
description: Open a private conversation with specific teammates' windows
argument-hint: "<agent[/session]> [more...] -- <title>"
---

Start an addressed conversation on the crew bus.

Input: $ARGUMENTS

Steps:
1. If `create_conversation` is not in the tool list, tell me the team has conversations off (an operator turns them on with `team capability --conversations on`) and stop.
2. Everything before `--` is a list of addresses (`agent` or `agent/session`); everything after is the title. Check each address with `list_agents` / `list_sessions`; ask me about any it does not know.
3. Call `create_conversation` with `title`, `private: true` and `invite` = the addresses.
4. If I gave an opening message in the same request, send it with `send_conversation_message` and a fresh `request_id` (a new UUID; never reuse one for a different message).

Confirm with the conversation id, who was invited, and the rule: an invitee is not in the thread until it accepts, and the thread starts for them from now unless I said `history_from_start`.
