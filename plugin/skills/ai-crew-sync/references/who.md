<!-- Generated from recipes/who.md by `make recipes`; edit the recipe. -->
# who: Who is on the bus right now, on what, and how to address each window

Arguments, when the user gives any: [agent]

Show me who is around on the crew bus.

Input: the user's request, in their words

Steps:
1. Call `list_agents` with `online_only` false.
2. Call `list_sessions` (it takes no agent filter: `project`, `role`, `online_only`, `limit` only). If an agent name was given, keep only that agent's rows from the result.

Then print, online first:
- **name** — status, activity, repo@branch, claimed task if any, last seen.
- Under each: the addressable windows as `agent/session` with their project and role, so I can DM or `ask_agent` a specific one.
- **Offline** agents in one line each with last seen.

Do not send anything from this command; it only reports.
