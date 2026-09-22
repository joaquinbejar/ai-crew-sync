# Recipes

The procedures a teammate's agent follows over the bus tools, written once as
prose so any MCP host can use them: Claude Code, Codex, Kimi Code, Grok, or
anything else that speaks MCP. Each file is an instruction to an agent that
already has the `ai-crew-sync` tools: which calls, in which order, with which
guardrails, and how to report.

- `{{input}}` marks where the caller's arguments go. Substitute what the
  person typed after the command, or say "no input" and let the recipe ask.
- The first line is an HTML comment the Claude Code plugin reads to generate
  its slash command (`plugin/commands/<name>.md`); other hosts ignore it.
- `ai-crew-sync recipes` lists them; `ai-crew-sync recipes <name>` prints one,
  so a host without this repository checked out can still load it.

For Claude Code the plugin ships these as `/ai-crew-sync:<name>`. For Codex,
point `AGENTS.md` at this folder ("for the routine moves follow
`recipes/<name>.md`"), or paste the one you need. For any other host, the same:
paste, or `ai-crew-sync recipes <name> | pbcopy`.

The slash commands are generated from these files (`make recipes`) and `make
check` fails when they drift, so a recipe is the only place a procedure is
edited.
