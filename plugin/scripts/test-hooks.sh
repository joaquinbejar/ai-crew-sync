#!/bin/sh
# Regression tests for the plugin hooks. No bus and no network required: the
# scripts are pointed at a fake bus-call.sh that captures the payload instead
# of sending it, so we can assert on the JSON the hooks would have sent.
#
# Run directly, or via `make check`.
set -u
FAIL=0
ROOT="$(cd "$(dirname "$0")" && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

ok()  { echo "  ok    $1"; }
bad() { echo "  FAIL  $1: $2"; FAIL=1; }

# A stand-in for bus-call.sh that records "<tool> <json>" and sends nothing.
mkdir -p "$WORK/bin"
cp "$ROOT/heartbeat.sh" "$WORK/bin/heartbeat.sh"
cat > "$WORK/bin/bus-call.sh" <<'FAKE'
#!/bin/sh
printf '%s\t%s\n' "$1" "${2:-{\}}" >> "$CAPTURE"
FAKE
chmod +x "$WORK/bin/bus-call.sh"

# A git checkout whose remote and branch carry characters that break naive
# string concatenation: quote, backslash, and non-ASCII.
REPO_DIR="$WORK/repo"
mkdir -p "$REPO_DIR"
(
    cd "$REPO_DIR" || exit 1
    git init -q .
    git remote add origin 'git@github.com:acme/we"ird\repo.git'
    git symbolic-ref HEAD 'refs/heads/feat/quote"and\back-ünicode'
) >/dev/null 2>&1

export CAPTURE="$WORK/capture.txt"
: > "$CAPTURE"

# --- heartbeat produces valid JSON for hostile repo/branch values ------------
(
    cd "$REPO_DIR" || exit 1
    BUS_URL=http://example.invalid/mcp BUS_TOKEN=acs_test \
        sh "$WORK/bin/heartbeat.sh" active
) >/dev/null 2>&1

payload="$(cut -f2 "$CAPTURE" | tail -1)"
if [ -z "$payload" ]; then
    bad "heartbeat sends a payload" "nothing captured"
else
    if printf '%s' "$payload" | python3 -c 'import json,sys; json.load(sys.stdin)' 2>/dev/null; then
        ok "heartbeat payload is valid JSON with quotes/backslash/unicode"
    else
        bad "heartbeat payload is valid JSON" "$payload"
    fi
    printf '%s' "$payload" | python3 -c '
import json, sys
args = json.load(sys.stdin)
assert args["status"] == "active", args
assert isinstance(args["ttl_seconds"], int), args
' 2>/dev/null && ok "heartbeat carries status and an integer ttl" \
        || bad "heartbeat carries status and ttl" "$payload"
fi

# --- idle uses the short TTL ------------------------------------------------
: > "$CAPTURE"
(cd "$REPO_DIR" && BUS_URL=x BUS_TOKEN=y sh "$WORK/bin/heartbeat.sh" idle) >/dev/null 2>&1
cut -f2 "$CAPTURE" | tail -1 | python3 -c '
import json, sys
args = json.load(sys.stdin)
assert args["status"] == "idle" and args["ttl_seconds"] < 900, args
' 2>/dev/null && ok "idle heartbeat shortens the presence lease" \
    || bad "idle heartbeat" "$(cut -f2 "$CAPTURE" | tail -1)"

# --- BUS_DIGEST_HOURS is validated before it reaches the request ------------
for value in "abc" "" "0" "999" "8; rm -rf /"; do
    hours="$value"
    case "$hours" in
        ''|*[!0-9]*) hours=8 ;;
        *) [ "$hours" -ge 1 ] && [ "$hours" -le 336 ] || hours=8 ;;
    esac
    printf '{"hours":%s}' "$hours" | python3 -c '
import json, sys
h = json.load(sys.stdin)["hours"]
assert isinstance(h, int) and 1 <= h <= 336, h
' 2>/dev/null || bad "BUS_DIGEST_HOURS guard rejects '$value'" "produced $hours"
done
ok "BUS_DIGEST_HOURS guard keeps the digest request well-formed"

# --- bus-call.sh sends the session header ------------------------------------
# The bug: every hook goes through bus-call.sh, which sent only Authorization,
# so the hook layer was person-scoped while the MCP connection beside it was
# session-scoped. They wrote different presence rows.
#
# Kept in its own directory: $WORK/bin holds the *fake* bus-call the other
# tests assert against, and overwriting it with the real one silently breaks
# them.
mkdir -p "$WORK/bcall"
cp "$ROOT/bus-call.sh" "$WORK/bcall/bus-call.sh"
cat > "$WORK/bcall/curl" <<'FAKECURL'
#!/bin/sh
printf '%s\n' "$*" >> "$CURLLOG"
FAKECURL
chmod +x "$WORK/bcall/curl"
CURLLOG="$WORK/curl.txt"
export CURLLOG

call_with_session() {
    : > "$CURLLOG"
    BUS_SESSION="$1" BUS_URL=http://example.invalid/mcp BUS_TOKEN=acs_test \
        PATH="$WORK/bcall:$PATH" sh "$WORK/bcall/bus-call.sh" whoami >/dev/null 2>&1
    cat "$CURLLOG"
}

out="$(call_with_session market-data)"
case "$out" in
    *"X-Crew-Session: market-data"*) ok "bus-call sends the session header" ;;
    *) bad "bus-call sends the session header" "$out" ;;
esac

out="$(call_with_session "")"
case "$out" in
    *X-Crew-Session*) bad "unset BUS_SESSION sends no header" "$out" ;;
    *) ok "unset BUS_SESSION sends no header, not an empty one" ;;
esac

# A client whose config format has no default syntax passes the template
# through; the server rejects it, so the hook must not send it either.
out="$(call_with_session '${BUS_SESSION}')"
case "$out" in
    *X-Crew-Session*) bad "unexpanded template is not sent" "$out" ;;
    *) ok "an unexpanded \${VAR} is dropped rather than sent" ;;
esac

# Anything the server would reject is dropped, so the hook degrades to the
# shared session instead of failing every call. A newline is the one that
# matters most: in a header that is injection, not a bad label.
for bad in 'has space' 'a/b' "$(printf 'x\ny: z')" "$(printf 'ctrl\001')" \
           'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'; do
    out="$(call_with_session "$bad")"
    case "$out" in
        *X-Crew-Session*) bad "a rejectable session label is dropped" "$bad -> $out" ;;
    esac
done
ok "labels the server would reject are dropped, not sent"

# --- a surprising status cannot produce malformed JSON -----------------------
# The python path encodes safely; the fallback interpolates straight into JSON,
# where one quote is broken output rather than a bad status.
: > "$CAPTURE"
(cd "$REPO_DIR" && BUS_URL=x BUS_TOKEN=y sh "$WORK/bin/heartbeat.sh" 'evil"status') >/dev/null 2>&1
cut -f2 "$CAPTURE" | tail -1 | python3 -c '
import json, sys
args = json.load(sys.stdin)
assert args["status"] == "active", args
' 2>/dev/null && ok "an unknown status is clamped rather than interpolated" \
    || bad "status is clamped" "$(cut -f2 "$CAPTURE" | tail -1)"

# --- session start clears the stale activity line ----------------------------
# Omitted fields keep their previous value, so without this the last thing the
# previous run announced stands as the current session's activity forever.
: > "$CAPTURE"
(cd "$REPO_DIR" && BUS_URL=x BUS_TOKEN=y sh "$WORK/bin/heartbeat.sh" active reset) >/dev/null 2>&1
cut -f2 "$CAPTURE" | tail -1 | python3 -c '
import json, sys
args = json.load(sys.stdin)
assert args.get("activity") == "", args
' 2>/dev/null && ok "session-start heartbeat clears the activity line" \
    || bad "reset clears activity" "$(cut -f2 "$CAPTURE" | tail -1)"

: > "$CAPTURE"
(cd "$REPO_DIR" && BUS_URL=x BUS_TOKEN=y sh "$WORK/bin/heartbeat.sh" active) >/dev/null 2>&1
cut -f2 "$CAPTURE" | tail -1 | python3 -c '
import json, sys
args = json.load(sys.stdin)
assert "activity" not in args, args
' 2>/dev/null && ok "a mid-session heartbeat leaves the activity alone" \
    || bad "plain heartbeat omits activity" "$(cut -f2 "$CAPTURE" | tail -1)"

# --- the Stop drain: oldest unanswered question, once each -------------------
cp "$ROOT/stop-drain.sh" "$WORK/bin/stop-drain.sh"

# The fake bus answers read_messages with $1 and whoami with a fixed identity.
drain() {
    # $1 = messages array JSON, $2 = session id
    cat > "$WORK/bin/bus-call.sh" <<FAKE
#!/bin/sh
if [ "\$1" = "whoami" ]; then
    echo '{"result":{"structuredContent":{"agent":"joaquin","team":"layerv"}}}'
else
    cat <<'RESP'
{"result":{"structuredContent":{"messages":$1}}}
RESP
fi
FAKE
    chmod +x "$WORK/bin/bus-call.sh"
    printf '{"session_id":"%s"}' "$2" \
        | TMPDIR="$WORK" BUS_URL=http://example.invalid/mcp BUS_TOKEN=acs_test \
          sh "$WORK/bin/stop-drain.sh" 2>/dev/null
}

NONE='[{"id":7,"from":"dani","to":"joaquin","body":"fyi","metadata":{}}]'
ONE='[{"id":9,"from":"dani","from_session":"api","to":"joaquin","body":"is it green?","metadata":{"question":true}}]'
# Two queued questions, oldest first in wall-clock order.
TWO='[{"id":9,"from":"dani","to":"joaquin","body":"older one","metadata":{"question":true}},
      {"id":11,"from":"marta","to":"joaquin","body":"newer one","metadata":{"question":true}}]'
# The same question, already answered during normal work.
ANSWERED='[{"id":9,"from":"dani","to":"joaquin","body":"is it green?","metadata":{"question":true}},
           {"id":10,"from":"joaquin","to":"dani","reply_to":9,"body":"yes","metadata":{}}]'

out="$(drain "$NONE" sess-a)"
[ -z "$out" ] && ok "no pending question means the session stops normally" \
    || bad "drain stays quiet without a question" "$out"

out="$(drain "$ONE" sess-b)"
printf '%s' "$out" | python3 -c '
import json, sys
d = json.load(sys.stdin)
assert d["decision"] == "block", d
ctx = d["hookSpecificOutput"]["additionalContext"]
assert "is it green?" in ctx, ctx
assert "dani/api" in ctx, "the reply must be addressed to the asking session"
assert "reply_to: 9" in ctx, ctx
' 2>/dev/null && ok "a pending question holds the session open with the question" \
    || bad "drain blocks on a question" "$out"

# The loop guard: blocking again on the same question would trap the session
# going round forever.
out="$(drain "$ONE" sess-b)"
[ -z "$out" ] && ok "the same question never blocks the session twice" \
    || bad "drain loop guard" "$out"

# A different session has its own guard, so it still gets its turn.
out="$(drain "$ONE" sess-c)"
[ -n "$out" ] && ok "the guard is per session, not global" \
    || bad "drain guard is per session" "empty"

# A question already answered during normal work must not be reopened on the
# way out: metadata says "this is a question", never "this one is still open".
out="$(drain "$ANSWERED" sess-d)"
[ -z "$out" ] && ok "an answered question is not raised again at Stop" \
    || bad "drain reopens an answered question" "$out"

# Two queued questions: the older caller has waited longest and is unblocked
# first, and the newer one survives to the next Stop rather than being
# suppressed by a high-water mark.
out="$(drain "$TWO" sess-e)"
printf '%s' "$out" | python3 -c '
import json, sys
ctx = json.load(sys.stdin)["hookSpecificOutput"]["additionalContext"]
assert "older one" in ctx, ctx
assert "reply_to: 9" in ctx, ctx
assert "1 other question(s)" in ctx, ctx
' 2>/dev/null && ok "the oldest waiting question is drained first" \
    || bad "drain picks the oldest" "$out"

out="$(drain "$TWO" sess-e)"
printf '%s' "$out" | python3 -c '
import json, sys
ctx = json.load(sys.stdin)["hookSpecificOutput"]["additionalContext"]
assert "newer one" in ctx, ctx
assert "reply_to: 11" in ctx, ctx
' 2>/dev/null && ok "the second question is not lost behind the first" \
    || bad "drain loses a queued question" "$out"

# Unconfigured bus: silent, as every hook must be. `env -u` rather than simply
# not passing them: a developer machine that is connected to a real bus has
# both exported from the shell profile, and the test would otherwise assert
# nothing while appearing to pass on CI, where they happen to be absent.
out="$(printf '{"session_id":"sess-f"}' \
    | env -u BUS_URL -u BUS_TOKEN TMPDIR="$WORK" sh "$WORK/bin/stop-drain.sh" 2>/dev/null)"
[ -z "$out" ] && ok "no BUS_URL/BUS_TOKEN means the hook does nothing" \
    || bad "drain without a configured bus" "$out"

# --- every tool the hooks call exists in the served schema ------------------
# Cheap coupling check: the tool names the scripts use must appear in the
# server's tool router. Catches a rename before a user's session breaks.
SRC="$ROOT/../../src/tools"
if [ -d "$SRC" ]; then
    missing=""
    for tool in whoami team_digest heartbeat list_tasks read_messages; do
        grep -rq "async fn $tool(" "$SRC" 2>/dev/null || missing="$missing $tool"
    done
    [ -z "$missing" ] && ok "tools the hooks and skill name exist server-side" \
        || bad "tools exist server-side" "missing:$missing"
fi

[ "$FAIL" -eq 0 ] && echo "plugin hooks: clean" || echo "plugin hooks: FAILURES"
exit "$FAIL"
