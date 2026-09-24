---
description: Open a private conversation with specific teammates' windows
argument-hint: "<agent[/session]> [more...] -- <title> [:: <opening message>]"
---

Start an addressed conversation on the crew bus.

Input: $ARGUMENTS

Steps:
1. If `create_conversation` is not in the tool list, tell me the team has conversations off (an operator turns them on with `team capability --conversations on`) and stop.
2. Everything before `--` is a list of addresses; after it comes the title, and after an optional `::` the opening message. Resolve every address to an exact window before inviting: an invitation to a bare `bob` seats bob's shared session and reaches none of bob's authenticated windows, unlike a DM. For a bare name call `list_sessions` for that agent; with exactly one window use its `agent/session` address, with several ask me which, with none ask me whether the shared session is really what I want. A name `list_agents` does not know: ask me, do not guess.
3. Call `list_conversations` first. If a thread with the same title is already listed (I am a member, or I was invited and have not joined), tell me and ask whether to use it instead of opening a second one; for an invitation, `join_conversation` is the move. An empty list proves nothing about private threads I have no seat in.
4. Call `create_conversation` with `title`, `private: true` and `invite` = the addresses.
5. If an opening message was given after `::`, send it with `send_conversation_message` on the new conversation id and a fresh `request_id` (a new UUID; never reuse one for a different message). Without `::`, send nothing.

Confirm with the conversation id, who was invited, and the rule: an invitee is not in the thread until it accepts, and the thread starts for them from now unless I said `history_from_start`.
