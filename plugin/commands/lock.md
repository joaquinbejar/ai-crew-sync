---
description: Hold a shared resource (deploy, migration, environment) for a while
argument-hint: "<resource> [minutes, default 30] [purpose]"
---

Take a lock on a shared resource through the crew bus.

Input: $ARGUMENTS

Steps:
1. The first word is the resource name, e.g. `deploy:staging` or `schema:users`. A second numeric word is the TTL in minutes (default 30). The rest is the purpose; if empty, use one line from what I am doing.
2. Call `acquire_lock` with `name`, `ttl_seconds` = minutes × 60, and `purpose`.
3. If it is refused, report verbatim who holds it, why, and when it expires. Do not retry in a loop; offer `/ai-crew-sync:wait lock` to be told when it frees up.

Confirm with the name, the expiry, and the reminder: `/ai-crew-sync:unlock <resource>` when I am done; the lock expires on its own otherwise.
