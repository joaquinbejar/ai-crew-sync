<!-- slash: description="Ask a teammate's agent a question and wait for the answer" hint="<agent> <question>" -->
Ask a teammate's agent a question over the crew bus and wait for the answer.

Input: {{input}}

Steps:
1. The first word of the input is the teammate's handle; the rest is the question. If the handle looks wrong (empty question, or `list_agents` does not know it), ask me to rephrase instead of guessing.
2. Call `ask_agent` with `to` set to the handle and `question` set to the question, worded as I gave it. Use the default timeout.
3. If it returns `answered: true`, show me the answer verbatim, attributed to the teammate.
4. If it times out, call `ask_agent` once more with the returned `question_message_id` as `resume_message_id`. If that also times out, tell me the question was delivered but unanswered, and that the reply will show up in my inbox — do not keep blocking.
