#!/bin/sh
# heartbeat.sh [active|idle|busy|blocked|stop|prompt] [reset]
# Presence ping with repo/branch context from the current git checkout.
#
# `stop` and `prompt` are lifecycle observations, not statuses (#221): the
# turn ended (Stop) or one started (UserPromptSubmit). Both publish status
# active, since the window is still open, and report the model's attention
# as idle or attending, so teammates can tell a connected window from one
# whose model is reading.
# Fire-and-forget: always exits 0, never blocks the session.
#
# `reset` clears the activity line. A session that has just started has not
# done anything yet, and omitted fields keep their previous value - so
# without this the last thing the previous run announced stands as the
# current one. Mid-session pings omit it, so whatever the model set through
# the MCP heartbeat tool survives.
set -u
DIR="$(cd "$(dirname "$0")" && pwd)"
# Callers that have the hook payload export BUS_HOST_SESSION first; when a
# hook has none (the Stop keep-alive is invoked without stdin), the resolver
# falls back to BUS_SESSION exactly as before.
STATUS="${1:-active}"
ATTENTION=""
case "$STATUS" in
    stop)   STATUS=active; ATTENTION=idle ;;
    prompt) STATUS=active; ATTENTION=attending ;;
esac
# Clamped before it reaches a payload. The python path would encode a
# surprising value safely, but the fallback below interpolates it straight
# into JSON, where one quote is malformed output rather than a bad status.
case "$STATUS" in
    active|idle|busy|blocked) ;;
    *) STATUS=active ;;
esac
# A window that is closing (SessionEnd sends idle) has no model running and
# nothing to wake it: say so, as the authenticated session_end event does.
[ "$STATUS" = idle ] && [ -z "$ATTENTION" ] && ATTENTION=idle
RESET="${2:-}"

# `--from-payload` in the second slot: the hook was given the host payload on
# stdin, so read the conversation id from it exactly as the other hooks do.
# (Kept distinct from `reset`, which is the legacy second argument.)
# The payload names the host too: Antigravity reads a JSON object from every
# hook's stdout, so under it this hook answers `{}` on its way out.
HOOK_HOST=claude
if [ "$RESET" = "--from-payload" ]; then
    RESET=""
    if command -v python3 >/dev/null 2>&1; then
        . "$DIR/host-payload.sh"
        [ -z "${BUS_HOST_SESSION:-}" ] && [ -n "$HOST_SESSION" ] \
            && export BUS_HOST_SESSION="$HOST_SESSION"
    fi
fi
# Quiet by contract: nothing a host should read, except Antigravity's `{}`.
done_quietly() {
    [ "$HOOK_HOST" = antigravity ] && printf '{}\n'
    exit 0
}

# Authenticated mode: the helper publishes presence as the bound window, with
# repo and branch read from this checkout. Silent when there is no binding.
if [ -n "${BUS_HOST_SESSION:-}" ] && command -v ai-crew-sync >/dev/null 2>&1; then
    case "$STATUS:$ATTENTION" in
        idle:*)      EVENT=session_end ;;
        *:idle)      EVENT=stop ;;
        *:attending) EVENT=prompt_submit ;;
        *)           EVENT=heartbeat ;;
    esac
    case "$(ai-crew-sync context hook --binding "$BUS_HOST_SESSION" --event status 2>/dev/null)" in
        *'"authenticated"'*)
            ai-crew-sync context hook --binding "$BUS_HOST_SESSION" --event "$EVENT" \
                >/dev/null 2>&1 || true
            done_quietly
            ;;
        *'"no-credential"'*)
            # This window WAS authenticated and its state is now unusable.
            # Falling through would publish presence with the parent token
            # under BUS_SESSION: a closed window reporting as the shared
            # legacy identity another window may be using. Stay quiet, as
            # session-start.sh does. A binding that never existed ("missing")
            # still takes the legacy path below.
            done_quietly
            ;;
    esac
fi

TTL=900
[ "$STATUS" = "idle" ] && TTL=120

REPO="$(git config --get remote.origin.url 2>/dev/null \
  | sed -e 's#\.git$##' -e 's#.*[:/]\([^/]*/[^/]*\)$#\1#')"
BRANCH="$(git branch --show-current 2>/dev/null || true)"

# Git allows quotes, backslashes and other JSON-hostile characters in branch
# names and remote URLs, so the payload is encoded by json.dumps rather than
# string-concatenated. Without python3 we still publish presence, just without
# repo/branch — degraded detail beats a malformed request.
if command -v python3 >/dev/null 2>&1; then
    ARGS="$(STATUS="$STATUS" TTL="$TTL" REPO="$REPO" BRANCH="$BRANCH" RESET="$RESET" ATTENTION="$ATTENTION" python3 - <<'PY'
import json, os

args = {"status": os.environ["STATUS"], "ttl_seconds": int(os.environ["TTL"])}
if os.environ.get("ATTENTION"):
    args["attention"] = os.environ["ATTENTION"]
elif os.environ.get("RESET") == "reset":
    # A new session knows nothing about the previous turn.
    args["attention"] = ""
if os.environ.get("STATUS") == "idle":
    # Closing: whatever announced a way to wake this window is gone too.
    args["wake"] = ""
for key in ("repo", "branch"):
    value = os.environ.get(key.upper(), "").strip()
    if value:
        args[key] = value
# Empty string means clear; omitting the key means keep.
if os.environ.get("RESET") == "reset":
    args["activity"] = ""
print(json.dumps(args))
PY
)"
elif [ "$STATUS" = idle ]; then
    ARGS="{\"status\":\"idle\",\"ttl_seconds\":$TTL,\"attention\":\"idle\",\"wake\":\"\"}"
elif [ -n "$ATTENTION" ]; then
    ARGS="{\"status\":\"$STATUS\",\"ttl_seconds\":$TTL,\"attention\":\"$ATTENTION\"}"
elif [ "$RESET" = "reset" ]; then
    ARGS="{\"status\":\"$STATUS\",\"ttl_seconds\":$TTL,\"activity\":\"\",\"attention\":\"\"}"
else
    ARGS="{\"status\":\"$STATUS\",\"ttl_seconds\":$TTL}"
fi

"$DIR/bus-call.sh" heartbeat "$ARGS" >/dev/null 2>&1 || true
done_quietly
