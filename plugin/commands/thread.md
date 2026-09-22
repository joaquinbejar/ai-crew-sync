---
description: Open a private conversation with specific teammates' windows
argument-hint: "<agent[/session]> [more...] -- <title> [:: <opening message>]"
---

Start an addressed conversation on the crew bus.

Input: $ARGUMENTS

Steps:
1. If `create_conversation` is not in the tool list, tell me the team has conversations off (an operator turns them on with `team capability --conversations on`) and stop.
2. Everything before `--` is a list of addresses (`agent` or `agent/session`); after it comes the title, and after an optional `::` the opening message. Check each address with `list_agents` / `list_sessions`; ask me about any it does not know.
3. Call `create_conversation` with `title`, `private: true` and `invite` = the addresses.
4. If an opening message was given after `::`, send it with `send_conversation_message` on the new conversation id and a fresh `request_id` (a new UUID; never reuse one for a different message). Without `::`, send nothing.

Confirm with the conversation id, who was invited, and the rule: an invitee is not in the thread until it accepts, and the thread starts for them from now unless I said `history_from_start`.
