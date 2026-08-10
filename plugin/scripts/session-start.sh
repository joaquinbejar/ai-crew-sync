#!/bin/sh
# SessionStart hook: announce presence on the bus and inject a compact
# team catch-up (whoami + team_digest) into the session context.
# Always exits 0; produces no output when the bus is unreachable.
set -u
DIR="$(cd "$(dirname "$0")" && pwd)"
[ -n "${BUS_URL:-}" ] && [ -n "${BUS_TOKEN:-}" ] || exit 0

# reset: a new session has not done anything yet, so the previous run's
# activity line must not stand as this one's.
"$DIR/heartbeat.sh" active reset >/dev/null 2>&1 || true
command -v python3 >/dev/null 2>&1 || exit 0

# team_digest takes 1-336; anything else (empty, non-numeric, out of range)
# would build invalid JSON, so fall back to the default rather than send it.
HOURS="${BUS_DIGEST_HOURS:-8}"
case "$HOURS" in
    ''|*[!0-9]*) HOURS=8 ;;
    *) [ "$HOURS" -ge 1 ] && [ "$HOURS" -le 336 ] || HOURS=8 ;;
esac

# Suggested session label when BUS_SESSION is unset: the repository this
# checkout belongs to. Only a suggestion — the header is read from the
# environment when the client launches, so a hook cannot set it.
SUGGESTED="$(git config --get remote.origin.url 2>/dev/null \
  | sed -e 's#\.git$##' -e 's#.*[:/][^/]*/##')"

WHO="$("$DIR/bus-call.sh" whoami 2>/dev/null || true)"
DIG="$("$DIR/bus-call.sh" team_digest "{\"hours\":$HOURS}" 2>/dev/null || true)"
export WHO DIG BUS_DIGEST_HOURS="$HOURS" SUGGESTED BUS_SESSION="${BUS_SESSION:-}"

python3 - <<'PY' 2>/dev/null || true
import json, os

def sc(raw):
    try:
        return json.loads(raw)["result"]["structuredContent"]
    except Exception:
        return None

who = sc(os.environ.get("WHO", ""))
dig = sc(os.environ.get("DIG", ""))
if not who and not dig:
    raise SystemExit(0)

lines = []
if who:
    lines.append(
        f"[ai-crew-sync] You are agent '{who.get('agent')}' on team '{who.get('team')}'. "
        "The team coordination bus (MCP server 'ai-crew-sync') is connected."
    )
    session = who.get("session")
    if session:
        where = f"in the '{session}' session"
        channel = who.get("default_channel")
        if channel:
            where += f", posting to #{channel} by default"
        lines.append(
            f"- You are {where}. Your presence, task claims and locks here are "
            "separate from your other sessions, and teammates can address this "
            f"window directly as '{who.get('agent')}/{session}'."
        )
    else:
        suggested = (os.environ.get("SUGGESTED") or "").strip()
        hint = f" e.g. export BUS_SESSION={suggested}" if suggested else ""
        lines.append(
            "- This is the shared session (no BUS_SESSION set), so presence, task "
            "claims and locks are shared with every other window using this token. "
            f"Set BUS_SESSION per repository to separate them{hint}."
        )
    dm = who.get("unread_direct_messages") or 0
    ct = who.get("open_claimed_tasks") or 0
    if dm:
        lines.append(f"- {dm} unread direct message(s) for you. Read them with read_messages before starting work.")
    if ct:
        lines.append(f"- {ct} task(s) claimed by you are still open (list_tasks mine_only=true).")
if dig:
    hours = os.environ.get("BUS_DIGEST_HOURS", "8")
    compact = json.dumps(dig, ensure_ascii=False, separators=(",", ":"))
    if len(compact) > 2500:
        compact = compact[:2500] + "…(truncated — call team_digest for the full picture)"
    lines.append(f"- Team activity, last {hours}h (team_digest): {compact}")
lines.append(
    "- Conventions: claim_task before working on shared tasks, renew_task_lease on long ones, "
    "post progress to the relevant channel, and ask_agent when you need a teammate's reply. "
    "Address a specific window as 'agent/session' when the question is about work only that "
    "window can see."
)

print(json.dumps({
    "hookSpecificOutput": {
        "hookEventName": "SessionStart",
        "additionalContext": "\n".join(lines),
    }
}))
PY
exit 0
