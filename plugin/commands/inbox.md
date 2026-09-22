---
description: Conversation messages addressed to this window that are waiting on me
argument-hint: ""
---

Show me what is waiting for me in addressed conversations.

Steps:
1. If `list_conversations` is not in the tool list, say the team has conversations off and stop.
2. Call `list_conversations`. For every conversation where my `membership.state` is `active`, call `read_conversation` and page with `after_seq` = `next_after_seq` until it is absent. Keep the messages where `my_receipt` exists and has no `acknowledged_at`: those are addressed to me and still waiting on my acknowledgement, whatever the inbox has already handed over. Do not use `fetch_conversation_inbox` for this: a delivery reference is confirmed and consumed by the proxy after its first fetch, so a second fetch returns nothing while the acknowledgement is still owed.
3. From the same listing, report separately the conversations where my `membership.state` is `invited`: invitations I have not accepted.

Report, most recent first, with who sent each message and when, and stop: do not `ack_message`, `confirm_inbox_delivery` or `join_conversation` on my behalf. Tell me what each item needs (an answer, an ack, a decision) and let me answer.
