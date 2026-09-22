---
description: Ask a teammate's agent to review a PR and wait for the verdict
argument-hint: "<agent[/session]> <PR number or URL> [what to look at]"
---

Request a code review from a teammate's agent over the crew bus.

Input: $ARGUMENTS

Steps:
1. The first word is the reviewer (`agent` or `agent/session`), the second the PR (a number in this repository or a URL); the rest, if any, is what to focus on. Check the reviewer with `list_agents`; if the PR is a number, expand it to `owner/repo#N` from the current repository's remote.
2. Call `ask_agent` with `to` = the reviewer and `question` = a structured request: the PR reference, one line on what it changes, what to look at (from the input, or "correctness, security, tests"), and how to answer: a verdict line (`approve` / `changes requested` / `comment`) followed by findings, each with file and line. Use the default timeout.
3. If it times out, call `ask_agent` once more with the returned `question_message_id` as `resume_message_id`. If that times out too, say the request was delivered and the answer will arrive in my inbox; do not keep blocking.
4. Show the verdict and findings verbatim, attributed to the reviewer.

Never merge, approve or change the PR from this command; it asks and reports.
