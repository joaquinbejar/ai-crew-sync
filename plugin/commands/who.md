---
description: Who is on the bus right now, on what, and how to address each window
argument-hint: "[agent]"
---

Show me who is around on the crew bus.

Input: $ARGUMENTS

Steps:
1. Call `list_agents` with `online_only` false.
2. Call `list_sessions`; if an agent name was given, only that agent's.

Then print, online first:
- **name** — status, activity, repo@branch, claimed task if any, last seen.
- Under each: the addressable windows as `agent/session` with their project and role, so I can DM or `ask_agent` a specific one.
- **Offline** agents in one line each with last seen.

Do not send anything from this command; it only reports.
