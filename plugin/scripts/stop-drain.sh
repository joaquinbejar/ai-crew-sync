#!/bin/sh
# Stop hook: answer a teammate's blocking question before going quiet.
#
# A coding agent only calls MCP tools while it is processing a turn. A session
# parked at the prompt polls nothing, so a question addressed to it would sit
# unread until its human typed something. This hook closes the most useful part
# of that gap: on the way out, look for a question, and if there is one, hold
# the session open long enough to answer it.
#
# It cannot close the gap entirely — a session that has been idle for an hour
# still answers nothing. That is a property of the client, not of the bus.
#
# Always exits 0 and prints nothing when there is no question, when the bus is
# unreachable, or when the bus is not configured at all.
set -u
DIR="$(cd "$(dirname "$0")" && pwd)"
[ -n "${BUS_URL:-}" ] && [ -n "${BUS_TOKEN:-}" ] || exit 0
command -v python3 >/dev/null 2>&1 || exit 0

# Claude Code delivers the hook payload on stdin; session_id keys the loop
# guard so two sessions in different repositories do not share one.
PAYLOAD="$(cat 2>/dev/null || true)"

# Scope "all" rather than "inbox": it carries this agent's own sent messages
# too, which is the only way to tell a question that has already been answered
# from one still waiting.
#
# only_new is false on purpose: this must not advance the read cursor. Marking
# a question read here would hide it from the read_messages call the model is
# about to make to answer it.
FEED="$("$DIR/bus-call.sh" read_messages \
    '{"scope":"all","only_new":false,"limit":50}' 2>/dev/null || true)"
[ -n "$FEED" ] || exit 0
WHO="$("$DIR/bus-call.sh" whoami 2>/dev/null || true)"

STATE_DIR="${TMPDIR:-/tmp}"
export PAYLOAD FEED WHO STATE_DIR
export BUS_SESSION="${BUS_SESSION:-}"

python3 - <<'PY' 2>/dev/null || true
import json, os, re

def sc(raw):
    try:
        return json.loads(raw)["result"]["structuredContent"]
    except Exception:
        return None

feed = sc(os.environ.get("FEED", ""))
who = sc(os.environ.get("WHO", "")) or {}
me = who.get("agent")
messages = (feed or {}).get("messages")
if not messages or not me:
    raise SystemExit(0)

# Addressed to this window, or to the person. `agent/session` narrows who a
# message is for, so it has to narrow who sees it too — otherwise a question
# for dani/risk-engine interrupts every one of dani's windows.
#
# The server applies the same rule to scope "all" once bus-call.sh sends the
# session header, so this is the second of two guards rather than the only
# one. It is worth having: this hook is what *blocks* a session, and a wrong
# message here is visible to the user immediately.
my_session = os.environ.get("BUS_SESSION", "").strip().lower()

def for_this_window(m):
    addressed = m.get("to_session")
    return addressed is None or addressed == my_session

# A question is a direct message someone else's agent is blocked on: ask_agent
# marks it, and post_message can too.
questions = [
    m for m in messages
    if isinstance(m, dict)
    and m.get("to")
    and m.get("from") != me
    and for_this_window(m)
    and (m.get("metadata") or {}).get("question") is True
    and isinstance(m.get("id"), int)
]
if not questions:
    raise SystemExit(0)

# Answered already: metadata says "this is a question", never "this one is
# still open". Anything this agent has replied to is settled, so a question
# answered during normal work must not be raised again on the way out.
answered = {
    m["reply_to"] for m in messages
    if isinstance(m, dict) and m.get("from") == me and isinstance(m.get("reply_to"), int)
}

# Loop guard. A Stop hook that blocks unconditionally traps the session going
# round forever, so each question is only ever blocked on once — if the model
# does not answer, the hook does not nag. Kept as a set rather than a
# high-water mark: with two questions queued, remembering only the newest id
# would suppress the older one for good and leave its caller blocked.
payload = json.loads(os.environ.get("PAYLOAD") or "{}") if os.environ.get("PAYLOAD") else {}
session_key = re.sub(r"[^A-Za-z0-9_.-]", "_", str(payload.get("session_id") or "default"))[:64]
state = os.path.join(os.environ["STATE_DIR"], f"ai-crew-sync-drain-{session_key}")

try:
    with open(state) as fh:
        seen = {int(line) for line in fh.read().split() if line.strip().isdigit()}
except Exception:
    seen = set()

# Oldest first: the caller who has been blocked longest is the one to unblock.
pending = sorted(q["id"] for q in questions if q["id"] not in answered and q["id"] not in seen)
if not pending:
    raise SystemExit(0)
message_id = pending[0]
latest = next(q for q in questions if q["id"] == message_id)

try:
    with open(state, "w") as fh:
        # Bounded: only recent ids can still be in a 50-message window anyway.
        fh.write("\n".join(str(i) for i in sorted(seen | {message_id})[-200:]))
except Exception:
    # Without a durable marker the guard cannot hold, and blocking anyway
    # risks the loop this exists to prevent.
    raise SystemExit(0)

sender = latest.get("from") or "a teammate"
if latest.get("from_session"):
    sender = f"{sender}/{latest['from_session']}"
body = (latest.get("body") or "").strip()
if len(body) > 1500:
    body = body[:1500] + "\u2026(truncated \u2014 read_messages has the whole thing)"

waiting = len(pending) - 1
more = f" {waiting} other question(s) are also waiting." if waiting else ""

print(json.dumps({
    "decision": "block",
    "reason": f"{sender} is blocked waiting on an answer from you.",
    "hookSpecificOutput": {
        "hookEventName": "Stop",
        "additionalContext": (
            f"[ai-crew-sync] {sender} asked you a question and their agent is "
            f"blocked waiting for the reply:\n\n{body}\n\n"
            f"Answer it now with post_message (to: \"{sender}\", "
            f"reply_to: {message_id}), then finish. Address the sender's "
            "session, not just their name, or the answer reaches a different "
            "window from the one that is waiting. If you genuinely cannot "
            f"answer, say so in the reply rather than leaving them blocked.{more}"
        ),
    },
}))
PY
exit 0
