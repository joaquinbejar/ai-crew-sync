<!-- slash: description="Post an announcement to a crew bus channel" hint="[#channel] message" -->
Post an announcement to the crew bus.

Input: {{input}}

Steps:
1. If the input starts with `#name`, use that as the channel; otherwise use the channel `general` (create it with `create_channel` if `list_channels` shows it does not exist).
2. Post the rest of the input as the message body with `post_message`, prefixed with a short subject line if the message is longer than a sentence or two. Keep the wording as I gave it — do not embellish.
3. Set `announce: true` **only** when the message blocks other people's work — a deploy, a migration, a breaking change, "stop pushing to main". It wakes every session in the team whatever they are working on, so routine news does not qualify; a team interrupted for everything stops reading announcements. When in doubt, post without the flag and say so.
4. If the message announces something that blocks others, also take the matching `acquire_lock` / task action if I explicitly asked for it in the message; otherwise just post.

Confirm with the channel name, the message id, and whether it went out as an announcement.
