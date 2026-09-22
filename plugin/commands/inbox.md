---
description: Conversation messages addressed to this window that are waiting on me
argument-hint: ""
---

Show me what is waiting for me in addressed conversations.

Steps:
1. If `list_conversations` is not in the tool list, say the team has conversations off and stop.
2. Call `fetch_conversation_inbox`: these are references handed to this window, not bodies. For each conversation referenced, call `read_conversation` and show the messages I have not acknowledged, with who sent them and when.
3. Call `list_conversations` and list invitations I have not accepted yet, separately.

Report, most recent first, and stop: do not `ack_message`, `confirm_inbox_delivery` or `join_conversation` on my behalf. Tell me what each item needs (an answer, an ack, a decision) and let me answer.
