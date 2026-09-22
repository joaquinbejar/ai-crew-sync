---
description: Read, write or search the team's shared notes (decisions, runbooks, deploy state)
argument-hint: "<key> [text] | find <query> | <scope>/<key> [text]"
---

Use the crew bus shared notes as the team's durable memory.

Input: $ARGUMENTS

Steps:
1. Parse the input:
   - `find <query>`: call `search_notes` with the query and list the matches as `scope/key` with the first line of each value.
   - `<key>` alone (optionally `scope/key`; scope defaults to `global`): call `get_note` and show the value verbatim, with who updated it and when.
   - `<key> <text>`: a write. Call `get_note` first; if the note exists, show me the current value next to the new one and ask before overwriting. Then `set_note` with the text as `value` and the tags I mention, if any.
2. Values are read cold by teammates' agents: when writing, keep it self-contained (what, why, where) and do not paste secrets.

Confirm with `scope/key` and whether it was created or updated.
