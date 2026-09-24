#!/bin/sh
# SessionStart hook: announce presence on the bus and inject a compact
# team catch-up (whoami + team_digest) into the session context.
# Always exits 0; produces no output when the bus is unreachable.
set -u
DIR="$(cd "$(dirname "$0")" && pwd)"
command -v python3 >/dev/null 2>&1 || exit 0

# Claude Code delivers the hook payload on stdin. Its session_id is this
# conversation's id, and exporting it as BUS_HOST_SESSION is what makes every
# hook of this window act on the same bus session as its `mcp proxy` — no
# shared per-repository file, no "current session" value to race on.
PAYLOAD="$(cat 2>/dev/null || true)"
HOST_SESSION="$(PAYLOAD="$PAYLOAD" python3 -c 'import json,os,sys
try:
    v = json.loads(os.environ.get("PAYLOAD") or "{}").get("session_id")
except Exception:
    v = None
sys.stdout.write(str(v) if v else "")' 2>/dev/null || true)"
[ -n "$HOST_SESSION" ] && export BUS_HOST_SESSION="$HOST_SESSION"

# Configured either way: local binary with profiles, or the legacy
# BUS_URL/BUS_TOKEN pair. Neither present means this repository has no bus.
if [ -z "${BUS_TOKEN:-}" ] && ! command -v ai-crew-sync >/dev/null 2>&1; then
    exit 0
fi
[ -n "${BUS_TOKEN:-}" ] && [ -z "${BUS_URL:-}" ] && exit 0

# Authenticated mode: this conversation's proxy registered a session, so the
# helper acts as THAT window with its own credential. It prints the host's
# JSON or nothing at all; it never falls back to another identity, and the
# credential never passes through this script.
if [ -n "$HOST_SESSION" ] && command -v ai-crew-sync >/dev/null 2>&1; then
    STATE="$(ai-crew-sync context hook --binding "$HOST_SESSION" --event status 2>/dev/null)"
    case "$STATE" in
        *'"authenticated"'*)
            ai-crew-sync context hook --binding "$HOST_SESSION" --event session_start \
                --digest-hours "${BUS_DIGEST_HOURS:-8}" 2>/dev/null || true
            exit 0
            ;;
        *'"no-credential"'*)
            # A resumed conversation starts a new proxy while this hook runs:
            # the old one stamped the binding closed when it exited, and the
            # new one clears that once it has resumed the session, about a
            # second later. Give it a bounded moment before calling the
            # credential gone (#199).
            waited=0
            while [ "$waited" -lt "${BUS_RESUME_WAIT_SECS:-6}" ]; do
                sleep 1
                waited=$((waited + 1))
                case "$(ai-crew-sync context hook --binding "$HOST_SESSION" --event status 2>/dev/null)" in
                    *'"authenticated"'*)
                        ai-crew-sync context hook --binding "$HOST_SESSION" --event session_start \
                            --digest-hours "${BUS_DIGEST_HOURS:-8}" 2>/dev/null || true
                        exit 0
                        ;;
                esac
            done
            # This window WAS authenticated and its state is still unusable.
            # Falling through would inject a digest read with the parent token
            # under a guessed label, which is the unproven identity this whole
            # path exists to avoid. Say so instead, and stay quiet on the bus.
            printf '%s' '{"hookSpecificOutput":{"hookEventName":"SessionStart","additionalContext":"[ai-crew-sync] This conversation is bound to a bus session whose credential is gone (the window was closed, or its state file was removed). No bus context was loaded and nothing was published. Restart the conversation, or run `ai-crew-sync context hook --binding <session id> --event status` to see the binding."}}'
            exit 0
            ;;
    esac
    # "missing" falls through: this conversation never had a proxy, so the
    # legacy path below is the intended configuration, not a broken one.
fi

# Identity first: nothing is injected into the conversation until the bus has
# confirmed who this window is. A digest from a credential that turns out to
# belong to another agent — or to nobody — is worse than no digest.
WHO="$("$DIR/bus-call.sh" whoami 2>/dev/null || true)"
case "$WHO" in
    *'"agent"'*) ;;
    *) exit 0 ;;
esac

# reset: a new session has not done anything yet, so the previous run's
# activity line must not stand as this one's.
"$DIR/heartbeat.sh" active reset >/dev/null 2>&1 || true

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
        role = who.get("role")
        project = who.get("project")
        if role or project:
            lines.append(
                f"- This window is labelled project '{project or '-'}', role "
                f"'{role or '-'}'. Teammates find it with list_sessions."
            )
        else:
            lines.append(
                "- This window has no project/role label yet: set one so teammates can "
                "find it with list_sessions (configure_session when connected through "
                "`ai-crew-sync mcp proxy`, or heartbeat with project and role)."
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
