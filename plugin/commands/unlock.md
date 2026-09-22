---
description: Release a shared resource you hold, or list the locks
argument-hint: "[resource]"
---

Release a lock on the crew bus.

Input: $ARGUMENTS

Steps:
1. With no input, call `list_locks` and show them: name, holder, purpose, time left. Stop there.
2. Otherwise call `release_lock` with `name` = the input.
3. If it is refused because somebody else holds it, say who, verbatim, and stop: never release another window's lock, even one of my own sessions — that window is still working.

Confirm with the name and that it is free.
