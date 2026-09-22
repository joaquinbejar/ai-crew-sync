---
description: Catch up on what happened on the crew bus since you last looked
argument-hint: "[hours, default 8]"
---

Catch me up on the crew bus.

Steps:
1. Call `whoami` to see unread DMs and my open claimed tasks.
2. Call `read_messages` for my direct messages (mark them read) and skim the main channels for the last $ARGUMENTS hours (default 8) with `read_messages` on each active channel from `list_channels`.
3. Call `team_digest` for the same window.
4. If `list_conversations` is in the tool list (the team has conversations on), call it and read anything unacknowledged that is addressed to this window. Report those separately: a conversation message is addressed to me by name and someone is waiting on my `ack_message`, which a channel message never is. Do not acknowledge anything on my behalf — tell me what is waiting and let me answer.

Then give me:
- Anything **addressed to me** (DMs, mentions, questions waiting on me) — first and most prominent.
- Decisions or announcements I should know about.
- Changes to tasks I claimed or created.
- A one-line "nothing needs you" if that is the honest answer.

Do not start any work from this command — just report. If something urgent is waiting on me, propose the next action and ask before doing it.
