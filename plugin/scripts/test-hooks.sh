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
cp "$ROOT/host-payload.sh" "$WORK/bin/host-payload.sh"
cp "$ROOT/python.sh" "$WORK/bin/python.sh"
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

# stop and prompt are lifecycle observations (#221): status stays active and
# the model's attention is reported as idle or attending.
for pair in stop:idle prompt:attending; do
    : > "$CAPTURE"
    (cd "$REPO_DIR" && BUS_URL=x BUS_TOKEN=y sh "$WORK/bin/heartbeat.sh" "${pair%%:*}") >/dev/null 2>&1
    cut -f2 "$CAPTURE" | tail -1 | WANT="${pair#*:}" python3 -c '
import json, os, sys
args = json.load(sys.stdin)
assert args["status"] == "active", args
assert args["attention"] == os.environ["WANT"], args
' 2>/dev/null && ok "${pair%%:*} reports the model ${pair#*:}" \
        || bad "${pair%%:*} attention" "$(cut -f2 "$CAPTURE" | tail -1)"
done

# Legacy SessionEnd (idle) reports the model idle and withdraws any wake;
# SessionStart (reset) clears the previous session's attention (#227 review).
: > "$CAPTURE"
(cd "$REPO_DIR" && BUS_URL=x BUS_TOKEN=y sh "$WORK/bin/heartbeat.sh" idle) >/dev/null 2>&1
cut -f2 "$CAPTURE" | tail -1 | python3 -c '
import json, sys
a = json.load(sys.stdin)
assert a["attention"] == "idle" and a["wake"] == "", a
' 2>/dev/null && ok "legacy SessionEnd reports idle and withdraws the wake" \
    || bad "legacy idle attention" "$(cut -f2 "$CAPTURE" | tail -1)"
: > "$CAPTURE"
(cd "$REPO_DIR" && BUS_URL=x BUS_TOKEN=y sh "$WORK/bin/heartbeat.sh" active reset) >/dev/null 2>&1
cut -f2 "$CAPTURE" | tail -1 | python3 -c '
import json, sys
a = json.load(sys.stdin)
assert a["attention"] == "" and a["activity"] == "", a
' 2>/dev/null && ok "legacy SessionStart clears the previous attention" \
    || bad "legacy reset attention" "$(cut -f2 "$CAPTURE" | tail -1)"

# Antigravity (#229): the Stop heartbeat answers {} and still reports idle.
: > "$CAPTURE"
hb_out="$(cd "$REPO_DIR" && printf '{"conversationId":"agy-hb"}' \
    | env -u BUS_HOST_SESSION BUS_URL=x BUS_TOKEN=y sh "$WORK/bin/heartbeat.sh" stop --from-payload)"
[ "$hb_out" = "{}" ] && ok "under Antigravity the Stop heartbeat answers {}" \
    || bad "antigravity heartbeat output" "$hb_out"
cut -f2 "$CAPTURE" | tail -1 | python3 -c '
import json, sys
assert json.load(sys.stdin)["attention"] == "idle"
' 2>/dev/null && ok "under Antigravity the Stop heartbeat still reports idle" \
    || bad "antigravity heartbeat attention" "$(cut -f2 "$CAPTURE" | tail -1)"

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
          BUS_SESSION="${3:-}" sh "$WORK/bin/stop-drain.sh" 2>/dev/null
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

# Antigravity (#229): its payload names the conversation `conversationId`,
# and its Stop contract is {"decision": "continue", "reason": ...} with the
# whole instruction in the reason; with nothing to say it gets `{}`.
drain_agy() {
    # $1 = messages array JSON, $2 = conversation id
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
    printf '{"conversationId":"%s","terminationReason":"%s"}' "$2" "${3:-NO_TOOL_CALL}" \
        | TMPDIR="$WORK" BUS_URL=http://example.invalid/mcp BUS_TOKEN=acs_test \
          sh "$WORK/bin/stop-drain.sh" 2>/dev/null
}
out="$(drain_agy "$ONE" agy-a)"
printf '%s' "$out" | python3 -c '
import json, sys
d = json.load(sys.stdin)
assert d["decision"] == "continue", d
assert "is it green?" in d["reason"] and "reply_to: 9" in d["reason"], d
assert "hookSpecificOutput" not in d, d
' 2>/dev/null && ok "under Antigravity a pending question continues the loop with the instruction" \
    || bad "antigravity stop drain" "$out"
out="$(drain_agy "$ONE" agy-a)"
[ "$out" = "{}" ] && ok "under Antigravity the loop guard answers {} the second time" \
    || bad "antigravity drain loop guard" "$out"
# A cancelled turn is not forced on, and the question is not used up.
out="$(drain_agy "$ONE" agy-c USER_CANCELED)"
[ "$out" = "{}" ] && ok "under Antigravity a cancelled turn is not held open" \
    || bad "antigravity drain on cancel" "$out"
out="$(drain_agy "$ONE" agy-c)"
case "$out" in
    *'"continue"'*) ok "the question survives a cancel for the next normal stop" ;;
    *) bad "antigravity drain after cancel" "$out" ;;
esac
out="$(drain_agy "$NONE" agy-b)"
[ "$out" = "{}" ] && ok "under Antigravity no question answers {}" \
    || bad "antigravity drain quiet" "$out"

# A question addressed to another window of the same person must not surface
# here. `agent/session` narrows who a message is for, so it has to narrow who
# sees it — otherwise every window of dani's is interrupted by a question for
# one of them.
OTHER='[{"id":21,"from":"marta","to":"joaquin","to_session":"core-manager","body":"for the other window","metadata":{"question":true}}]'
MINE='[{"id":22,"from":"marta","to":"joaquin","to_session":"market-data","body":"for this window","metadata":{"question":true}}]'
PERSON='[{"id":23,"from":"marta","to":"joaquin","body":"for the person","metadata":{"question":true}}]'

out="$(drain "$OTHER" sess-g market-data)"
[ -z "$out" ] && ok "a question for another window does not surface here" \
    || bad "drain is session-scoped" "$out"

out="$(drain "$MINE" sess-h market-data)"
case "$out" in
    *"for this window"*) ok "a question for this window still surfaces" ;;
    *) bad "drain surfaces its own window's question" "$out" ;;
esac

# Person-addressed DMs reach every window, which is what addressing a person
# has always meant.
out="$(drain "$PERSON" sess-i market-data)"
case "$out" in
    *"for the person"*) ok "a person-addressed question reaches every window" ;;
    *) bad "person-addressed question reaches this window" "$out" ;;
esac

# The bug this fixes: two windows of the SAME agent could address each other
# and never reach each other unprompted, because the sender was compared per
# agent. "dani" != "dani" is false, so the message was dropped as my own.
SELF_CROSS='[{"id":31,"from":"joaquin","from_session":"coordination","to":"joaquin","to_session":"market-data","body":"rebase before you push","metadata":{"question":true}}]'
SELF_SAME='[{"id":32,"from":"joaquin","from_session":"market-data","to":"joaquin","to_session":"market-data","body":"note to self","metadata":{"question":true}}]'

out="$(drain "$SELF_CROSS" sess-j market-data)"
case "$out" in
    *"rebase before you push"*) ok "another window of the same person reaches this one" ;;
    *) bad "same-agent cross-session question surfaces" "$out" ;;
esac

out="$(drain "$SELF_SAME" sess-k market-data)"
[ -z "$out" ] && ok "this window's own message is still not raised to itself" \
    || bad "own-window message must stay silent" "$out"

# A single malformed message used to abort the whole comprehension and mute
# every pending question for that turn — silently, and for as long as the bad
# row stayed in the 50-message window.
POISON='[{"id":41,"from":"marta","to":"joaquin","body":"stringified","metadata":"{\"question\": true}"},
         {"id":42,"from":"marta","to":"joaquin","body":"a real question","metadata":{"question":true}},
         {"id":43,"from":"marta","to":"joaquin","body":"junk","metadata":42}]'
out="$(drain "$POISON" sess-l market-data)"
case "$out" in
    *"stringified"*) ok "a stringified metadata object is still read as a question" ;;
    *) bad "one bad message must not mute the hook" "$out" ;;
esac

# ...and the good ones behind it are still reachable on the next turn.
out="$(drain "$POISON" sess-l market-data)"
case "$out" in
    *"a real question"*) ok "the questions behind a malformed one are not lost" ;;
    *) bad "questions behind a bad row survive" "$out" ;;
esac

# A sibling window's reply does not settle a question addressed to THIS one.
# find_answer requires the reply's sender_session to match when ask_agent
# targeted agent/session, so treating a sibling reply as an answer would
# suppress the question in the only window whose reply counts — and leave the
# asker blocked for good.
SIBLING_REPLIED='[{"id":51,"from":"marta","to":"joaquin","to_session":"market-data","body":"still open for me","metadata":{"question":true}},
                  {"id":52,"from":"joaquin","from_session":"core-manager","to":"marta","reply_to":51,"body":"answered from the wrong window","metadata":{}}]'
out="$(drain "$SIBLING_REPLIED" sess-m market-data)"
case "$out" in
    *"still open for me"*) ok "a sibling window's reply does not settle this window's question" ;;
    *) bad "sibling reply must not suppress the question" "$out" ;;
esac

# This window's own reply does settle it.
OWN_REPLIED='[{"id":53,"from":"marta","to":"joaquin","to_session":"market-data","body":"q","metadata":{"question":true}},
              {"id":54,"from":"joaquin","from_session":"market-data","to":"marta","reply_to":53,"body":"done","metadata":{}}]'
out="$(drain "$OWN_REPLIED" sess-n market-data)"
[ -z "$out" ] && ok "this window's own reply settles its question" \
    || bad "own reply settles the question" "$out"

# A person-addressed question is settled by any window of mine: the asker
# accepts a reply from any of them.
PERSON_REPLIED='[{"id":55,"from":"marta","to":"joaquin","body":"anyone?","metadata":{"question":true}},
                 {"id":56,"from":"joaquin","from_session":"core-manager","to":"marta","reply_to":55,"body":"got it","metadata":{}}]'
out="$(drain "$PERSON_REPLIED" sess-o market-data)"
[ -z "$out" ] && ok "any window of mine settles a person-addressed question" \
    || bad "person-addressed question settled by a sibling" "$out"

# Unconfigured bus: silent, as every hook must be. `env -u` rather than simply
# not passing them: a developer machine that is connected to a real bus has
# both exported from the shell profile, and the test would otherwise assert
# nothing while appearing to pass on CI, where they happen to be absent.
out="$(printf '{"session_id":"sess-f"}' \
    | env -u BUS_URL -u BUS_TOKEN TMPDIR="$WORK" sh "$WORK/bin/stop-drain.sh" 2>/dev/null)"
[ -z "$out" ] && ok "no BUS_URL/BUS_TOKEN means the hook does nothing" \
    || bad "drain without a configured bus" "$out"

# --- bus-call.sh: which mode it picks, and what it passes on ----------------
# The real script this time, not the fake: the point is the routing.
cp "$ROOT/bus-call.sh" "$WORK/bin/real-bus-call.sh"
mkdir -p "$WORK/fakebin"
cat > "$WORK/fakebin/ai-crew-sync" <<'FAKE'
#!/bin/sh
# Records the environment the hook path would carry and answers like the
# console client with --json: the tool's structured content.
{
    printf 'host=%s ' "${BUS_HOST_SESSION:-}"
    printf 'session=%s ' "${BUS_SESSION:-}"
    printf 'args=%s
' "$*"
} >> "$CALLS"
echo '{"agent":"joaquin","team":"acme","session":"s-abc123"}'
FAKE
chmod +x "$WORK/fakebin/ai-crew-sync"
export CALLS="$WORK/calls.txt"

# No token exported and the binary present: profiles path, conversation id
# passed through, and the JSON-RPC envelope every caller parses.
: > "$CALLS"
out="$(env -u BUS_TOKEN -u BUS_URL PATH="$WORK/fakebin:$PATH"     BUS_HOST_SESSION=conv-7 sh "$WORK/bin/real-bus-call.sh" whoami 2>/dev/null)"
case "$out" in
    *'"structuredContent"'*'"joaquin"'*) ok "binary path wraps the reply for callers" ;;
    *) bad "binary path reply" "$out" ;;
esac
case "$(cat "$CALLS")" in
    *"host=conv-7"*"client --json call whoami"*) ok "the conversation id reaches the client" ;;
    *) bad "conversation id not passed" "$(cat "$CALLS")" ;;
esac

# An exported BUS_TOKEN keeps the legacy curl path, even with the binary on
# the PATH: an operator's explicit credential is never silently replaced.
: > "$CALLS"
out="$(PATH="$WORK/fakebin:$PATH" BUS_TOKEN=acs_legacy BUS_URL=http://127.0.0.1:1     BUS_TIMEOUT=1 sh "$WORK/bin/real-bus-call.sh" whoami 2>/dev/null || true)"
[ ! -s "$CALLS" ] && ok "an exported token keeps the legacy curl path"     || bad "legacy path bypassed" "$(cat "$CALLS")"

# Neither a token nor the binary: silent, like every other unconfigured case.
out="$(env -u BUS_TOKEN -u BUS_URL PATH="$WORK/empty"     sh "$WORK/bin/real-bus-call.sh" whoami 2>/dev/null || true)"
[ -z "$out" ] && ok "no binary and no token means no call"     || bad "unconfigured bus-call" "$out"

# --- omitted arguments are an empty object, on every sh ------------------------
# The real bus-call.sh, with the recording curl on the PATH: the JSON it sends
# for `whoami` with no arguments, an empty argument and an explicit object
# must parse, and must carry an empty object.
mkdir -p "$WORK/argbin"
cat > "$WORK/argbin/curl" <<'FAKE'
#!/bin/sh
while [ $# -gt 0 ]; do
    if [ "$1" = "--data" ]; then printf '%s' "$2" > "$SENT"; fi
    shift
done
FAKE
chmod +x "$WORK/argbin/curl"
export SENT="$WORK/sent.json"
for variant in omitted empty explicit; do
    : > "$SENT"
    case "$variant" in
        omitted)  (PATH="$WORK/argbin:$PATH" BUS_URL=http://127.0.0.1:1/mcp BUS_TOKEN=acs_x sh "$ROOT/bus-call.sh" whoami) >/dev/null 2>&1 || true ;;
        empty)    (PATH="$WORK/argbin:$PATH" BUS_URL=http://127.0.0.1:1/mcp BUS_TOKEN=acs_x sh "$ROOT/bus-call.sh" whoami "") >/dev/null 2>&1 || true ;;
        explicit) (PATH="$WORK/argbin:$PATH" BUS_URL=http://127.0.0.1:1/mcp BUS_TOKEN=acs_x sh "$ROOT/bus-call.sh" whoami '{}') >/dev/null 2>&1 || true ;;
    esac
    SENT="$SENT" python3 -c '
import json, os
body = json.load(open(os.environ["SENT"]))
assert body["params"]["name"] == "whoami", body
assert body["params"]["arguments"] == {}, body
' 2>/dev/null && ok "bus-call sends an empty object for $variant arguments" \
        || bad "bus-call arguments ($variant)" "$(cat "$SENT")"
done
# The binary mode passes the same empty object on to the client.
: > "$CALLS"
out="$(env -u BUS_TOKEN -u BUS_URL PATH="$WORK/fakebin:$PATH" sh "$WORK/bin/real-bus-call.sh" whoami 2>/dev/null)"
case "$(cat "$CALLS")" in
    *"call whoami --args {}"*) ok "binary mode passes an empty object for omitted arguments" ;;
    *) bad "binary mode arguments" "$(cat "$CALLS")" ;;
esac

# --- lifecycle hooks after a binding lost its credential ---------------------
# The binary answers `context hook --event status` from an env var so each
# binding state can be replayed; every other invocation is recorded. A curl
# on the PATH records too, so a legacy request cannot hide behind the fake
# bus-call.sh: with the real bus-call.sh both would show.
# The drain tests above replaced the recording bus-call.sh with one that
# answers; put the recorder back, or a legacy request would leave no trace.
cat > "$WORK/bin/bus-call.sh" <<'FAKE'
#!/bin/sh
printf '%s\t%s\n' "$1" "${2:-{\}}" >> "$CAPTURE"
FAKE
chmod +x "$WORK/bin/bus-call.sh"
mkdir -p "$WORK/statebin"
cat > "$WORK/statebin/ai-crew-sync" <<'FAKE'
#!/bin/sh
printf 'args=%s\n' "$*" >> "$HOOKCALLS"
case "$*" in
    *"--event status"*) echo "{\"binding\":\"$2\",\"state\":\"${STUB_STATE:-missing}\"}" ;;
esac
FAKE
cat > "$WORK/statebin/curl" <<'FAKE'
#!/bin/sh
printf 'curl %s\n' "$*" >> "$CURLCALLS"
FAKE
chmod +x "$WORK/statebin/ai-crew-sync" "$WORK/statebin/curl"
export HOOKCALLS="$WORK/hookcalls.txt" CURLCALLS="$WORK/curlcalls.txt"

lifecycle() { # <state> <status> <with-token|no-token>
    : > "$CAPTURE"; : > "$HOOKCALLS"; : > "$CURLCALLS"
    if [ "$3" = "with-token" ]; then
        (cd "$REPO_DIR" && printf '{"session_id":"closed-window"}' | env -u BUS_HOST_SESSION \
            STUB_STATE="$1" PATH="$WORK/statebin:$PATH" BUS_URL=http://127.0.0.1:1/mcp \
            BUS_TOKEN=acs_parent_placeholder BUS_SESSION=legacy-shared \
            sh "$WORK/bin/heartbeat.sh" "$2" --from-payload) >/dev/null 2>&1
    else
        (cd "$REPO_DIR" && printf '{"session_id":"closed-window"}' | env -u BUS_HOST_SESSION \
            -u BUS_TOKEN -u BUS_URL -u BUS_SESSION STUB_STATE="$1" PATH="$WORK/statebin:$PATH" \
            sh "$WORK/bin/heartbeat.sh" "$2" --from-payload) >/dev/null 2>&1
    fi
}
for status in active idle; do
    for creds in with-token no-token; do
        lifecycle no-credential "$status" "$creds"
        if [ ! -s "$CAPTURE" ] && [ ! -s "$CURLCALLS" ] \
            && ! grep -q -- "--event heartbeat\|--event session_end" "$HOOKCALLS"; then
            ok "$status heartbeat publishes nothing after no-credential ($creds)"
        else
            bad "$status heartbeat fell back after no-credential ($creds)" \
                "$(cat "$CAPTURE" "$CURLCALLS" "$HOOKCALLS")"
        fi
    done
done
lifecycle authenticated active with-token
grep -q -- "--binding closed-window --event heartbeat" "$HOOKCALLS" && [ ! -s "$CAPTURE" ] \
    && ok "an authenticated binding still heartbeats as its own window" \
    || bad "authenticated heartbeat" "$(cat "$HOOKCALLS" "$CAPTURE")"
lifecycle authenticated idle with-token
grep -q -- "--binding closed-window --event session_end" "$HOOKCALLS" && [ ! -s "$CAPTURE" ] \
    && ok "an authenticated binding still ends its own session" \
    || bad "authenticated session_end" "$(cat "$HOOKCALLS" "$CAPTURE")"
lifecycle missing active with-token
grep -q '^heartbeat' "$CAPTURE" \
    && ok "a binding that never existed keeps the legacy heartbeat" \
    || bad "missing binding lost the legacy path" "$(cat "$CAPTURE" "$HOOKCALLS")"
lifecycle authenticated stop with-token
grep -q -- "--binding closed-window --event stop" "$HOOKCALLS" && [ ! -s "$CAPTURE" ] \
    && ok "an authenticated Stop reports through the stop event" \
    || bad "authenticated stop" "$(cat "$HOOKCALLS" "$CAPTURE")"
lifecycle authenticated prompt with-token
grep -q -- "--binding closed-window --event prompt_submit" "$HOOKCALLS" && [ ! -s "$CAPTURE" ] \
    && ok "an authenticated prompt reports through the prompt_submit event" \
    || bad "authenticated prompt" "$(cat "$HOOKCALLS" "$CAPTURE")"

: > "$CAPTURE"; : > "$HOOKCALLS"; : > "$CURLCALLS"
out="$(cd "$REPO_DIR" && printf '{"session_id":"closed-window"}' | env -u BUS_HOST_SESSION \
    STUB_STATE=no-credential PATH="$WORK/statebin:$PATH" BUS_URL=http://127.0.0.1:1/mcp \
    BUS_TOKEN=acs_parent_placeholder BUS_SESSION=legacy-shared \
    sh "$WORK/bin/stop-drain.sh" 2>/dev/null || true)"
[ -z "$out" ] && [ ! -s "$CAPTURE" ] && [ ! -s "$CURLCALLS" ] \
    && ok "the Stop drain reads nothing after no-credential" \
    || bad "stop drain fell back after no-credential" "$out $(cat "$CAPTURE" "$CURLCALLS")"

# --- an authenticated binding beats an exported token --------------------------
# The stub answers status from STUB_STATE and, for `--event call`, records
# the call and answers like the binary: the tool's structured content. With
# BUS_TOKEN and a conflicting BUS_SESSION exported, the real bus-call.sh must
# still go through the binding's window, and the Stop drain must read that
# window's inbox. The credential is never on the command line.
cat > "$WORK/statebin/ai-crew-sync" <<'FAKE'
#!/bin/sh
printf 'args=%s\n' "$*" >> "$HOOKCALLS"
case "$*" in
    *"--event status"*) echo "{\"binding\":\"$2\",\"state\":\"${STUB_STATE:-missing}\"}" ;;
    *"--event call"*"--tool whoami"*) echo '{"agent":"bob","team":"acme","session":"s-window"}' ;;
    *"--event call"*"--tool read_messages"*) echo '{"messages":[]}' ;;
    *"--event call"*) echo '{}' ;;
esac
FAKE
chmod +x "$WORK/statebin/ai-crew-sync"
: > "$HOOKCALLS"; : > "$CURLCALLS"
out="$(STUB_STATE=authenticated PATH="$WORK/statebin:$PATH" BUS_HOST_SESSION=conv-9 \
    BUS_URL=http://127.0.0.1:1/mcp BUS_TOKEN=acs_parent BUS_SESSION=legacy-other \
    sh "$ROOT/bus-call.sh" whoami 2>/dev/null)"
case "$(cat "$HOOKCALLS")" in
    *"--binding conv-9 --event call --tool whoami --args {}"*) ok "an authenticated binding beats an exported token" ;;
    *) bad "binding not preferred" "$(cat "$HOOKCALLS")" ;;
esac
grep -q "acs_" "$HOOKCALLS" && bad "a credential reached the binary's command line" "$(cat "$HOOKCALLS")" \
    || ok "no credential on the command line"
[ ! -s "$CURLCALLS" ] && ok "no legacy request when the binding is authenticated" || bad "legacy request with a binding" "$(cat "$CURLCALLS")"
case "$out" in
    *'"structuredContent"'*'"bob"'*) ok "the binding path answers in the JSON-RPC envelope" ;;
    *) bad "binding path envelope" "$out" ;;
esac
: > "$HOOKCALLS"; : > "$CURLCALLS"
out="$(STUB_STATE=no-credential PATH="$WORK/statebin:$PATH" BUS_HOST_SESSION=conv-9 \
    BUS_URL=http://127.0.0.1:1/mcp BUS_TOKEN=acs_parent BUS_SESSION=legacy-other \
    sh "$ROOT/bus-call.sh" whoami 2>/dev/null || true)"
[ -z "$out" ] && [ ! -s "$CURLCALLS" ] && ! grep -q -- "--event call" "$HOOKCALLS" \
    && ok "a binding that lost its credential calls nothing, token or not" \
    || bad "no-credential fell back" "$out $(cat "$CURLCALLS" "$HOOKCALLS")"
: > "$HOOKCALLS"; : > "$CURLCALLS"
out="$(STUB_STATE=missing PATH="$WORK/statebin:$PATH" BUS_HOST_SESSION=conv-9 \
    BUS_URL=http://127.0.0.1:1/mcp BUS_TOKEN=acs_parent BUS_SESSION=legacy-other \
    sh "$ROOT/bus-call.sh" whoami 2>/dev/null || true)"
grep -q "^curl" "$CURLCALLS" && ok "a conversation with no binding keeps the legacy path" \
    || bad "missing binding lost legacy" "$(cat "$CURLCALLS" "$HOOKCALLS")"
# The Stop drain, end to end through the real bus-call.sh: both reads go
# through the binding's window and none through the exported session.
cp "$ROOT/bus-call.sh" "$WORK/bin/bus-call.sh"
: > "$HOOKCALLS"; : > "$CURLCALLS"
out="$(cd "$REPO_DIR" && printf '{"session_id":"conv-9"}' | env -u BUS_HOST_SESSION STUB_STATE=authenticated \
    PATH="$WORK/statebin:$PATH" TMPDIR="$WORK" BUS_URL=http://127.0.0.1:1/mcp BUS_TOKEN=acs_parent \
    BUS_SESSION=legacy-other sh "$WORK/bin/stop-drain.sh" 2>/dev/null || true)"
grep -q -- "--event call --tool read_messages" "$HOOKCALLS" && grep -q -- "--event call --tool whoami" "$HOOKCALLS" \
    && [ ! -s "$CURLCALLS" ] \
    && ok "the Stop drain reads the binding's inbox, not the exported session's" \
    || bad "stop drain routing" "$(cat "$HOOKCALLS" "$CURLCALLS")"
# Put the recorder back for whatever runs after this section.
cat > "$WORK/bin/bus-call.sh" <<'FAKE'
#!/bin/sh
printf '%s\t%s\n' "$1" "${2:-{\}}" >> "$CAPTURE"
FAKE
chmod +x "$WORK/bin/bus-call.sh"

# --- every tool the hooks call exists in the served schema ------------------
# Cheap coupling check: the tool names the scripts use must appear in the
# server's tool router. Catches a rename before a user's session breaks.
SRC="$ROOT/../../src/tools"
if [ -d "$SRC" ]; then
    missing=""
    for tool in whoami team_digest heartbeat list_tasks read_messages list_sessions \
                list_agents list_channels create_channel post_message ask_agent \
                claim_task claim_next_task get_task complete_task release_task renew_task_lease \
                acquire_lock release_lock list_locks get_note set_note search_notes \
                wait_for_updates create_conversation send_conversation_message \
                list_conversations fetch_conversation_inbox read_conversation; do
        grep -rq "async fn $tool(" "$SRC" 2>/dev/null || missing="$missing $tool"
    done
    [ -z "$missing" ] && ok "tools the hooks and skill name exist server-side" \
        || bad "tools exist server-side" "missing:$missing"
fi

# --- both hosts' hook files point at scripts that exist (#230) -----------
# Claude Code reads hooks/hooks.json (timeouts in ms, ${CLAUDE_PLUGIN_ROOT}
# paths); Antigravity reads the root hooks.json (named groups, timeouts in
# seconds, commands run from the plugin root). A typo in either is a hook
# that silently never runs.
PLUGIN_DIR="$(cd "$ROOT/.." && pwd)"
PLUGIN_DIR="$PLUGIN_DIR" python3 - <<'PY' && ok "both hosts' hooks files name existing executable scripts" \
    || bad "hook files" "a hook names a missing script or a bad timeout"
import json, os, shlex
root = os.environ["PLUGIN_DIR"]

def scripts_of(command):
    for word in shlex.split(command):
        word = word.replace('"${CLAUDE_PLUGIN_ROOT}"', root).replace("${CLAUDE_PLUGIN_ROOT}", root)
        if word.endswith(".sh"):
            yield word if os.path.isabs(word) else os.path.join(root, word)

claude = json.load(open(os.path.join(root, "hooks", "hooks.json")))["hooks"]
for groups in claude.values():
    for g in groups:
        for h in g["hooks"]:
            assert h["timeout"] >= 1000, h  # milliseconds
            for path in scripts_of(h["command"]):
                assert os.access(path, os.X_OK), path

agy = json.load(open(os.path.join(root, "hooks.json")))
assert set(agy) == {"ai-crew-sync"}, agy.keys()
for event, handlers in agy["ai-crew-sync"].items():
    assert event in {"SessionStart", "PreToolUse", "PostToolUse", "PreInvocation",
                     "PostInvocation", "Stop"}, event
    for h in handlers:
        assert 1 <= h["timeout"] <= 60, h  # seconds
        assert h["command"].startswith("./scripts/"), h
        for path in scripts_of(h["command"]):
            assert os.access(path, os.X_OK), path

mcp = json.load(open(os.path.join(root, "mcp_config.json")))["mcpServers"]["ai-crew-sync"]
assert mcp == {"command": "ai-crew-sync", "args": ["mcp", "proxy"]}, mcp
PY

# Hosts substitute ${CLAUDE_PLUGIN_ROOT} as plain text and run the result
# with `bash -c` (#251). Devin on Windows hands over a root that ends in a
# backslash, which turned the old `"${CLAUDE_PLUGIN_ROOT}"/scripts/...` into
# an escaped quote and a syntax error before any script started. Every
# Claude Code hook must still reach its script with such a root, and with
# one that has a space in it; stubs stand in for the scripts.
PLUGIN_DIR="$PLUGIN_DIR" WORK="$WORK" python3 - <<'PY' && ok "every hook command survives a plugin root ending in a backslash or with a space" \
    || bad "hook quoting" "a hook command does not reach its script for some plugin root"
import json, os, subprocess
manifest = json.load(open(os.path.join(os.environ["PLUGIN_DIR"], "hooks", "hooks.json")))["hooks"]
commands = [h["command"] for groups in manifest.values() for g in groups for h in g["hooks"]]
for name in ("trailing-backslash\\", "with space"):
    root = os.path.join(os.environ["WORK"], "quoting", name)
    os.makedirs(os.path.join(root, "scripts"), exist_ok=True)
    for script in os.listdir(os.path.join(os.environ["PLUGIN_DIR"], "scripts")):
        if script.endswith(".sh"):
            with open(os.path.join(root, "scripts", script), "w") as f:
                f.write('#!/bin/sh\nprintf "ran %s" "$(basename "$0")"\n')
    for command in commands:
        assert command.startswith('sh "${CLAUDE_PLUGIN_ROOT}/scripts/'), command
        script = command.split("/scripts/", 1)[1].split('"', 1)[0]
        run = subprocess.run(["bash", "-c", command.replace("${CLAUDE_PLUGIN_ROOT}", root)],
                             input="{}", capture_output=True, text=True, timeout=10)
        assert run.returncode == 0 and run.stdout == "ran " + script, (name, command, run)
PY

# A python3 that is found but does not run (the Microsoft Store alias on
# Windows exits 49) is not a Python (#242): the probe falls through to
# `python`, and with neither the hooks take their no-Python path.
mkdir -p "$WORK/storepy" "$WORK/storepy2"
printf '#!/bin/sh\nexit 49\n' > "$WORK/storepy/python3"
chmod +x "$WORK/storepy/python3"
for tool in dirname cat; do ln -sf "$(command -v "$tool")" "$WORK/storepy/$tool"; done
cp "$WORK/storepy/python3" "$WORK/storepy2/python3"
ln -sf "$(command -v python3)" "$WORK/storepy2/python"
picked="$(PATH="$WORK/storepy2" ROOT="$ROOT" /bin/sh -c '. "$ROOT/python.sh"; printf %s "$PY"')"
[ "$picked" = python ] && ok "a python3 that does not run falls through to python" \
    || bad "python probe" "the probe picked '$picked' with a broken python3 and a working python"
none="$(PATH="$WORK/storepy" ROOT="$ROOT" /bin/sh -c '. "$ROOT/python.sh"; printf %s "$PY"')"
[ -z "$none" ] && ok "a python3 that does not run is no Python" \
    || bad "python probe" "the probe trusted a python3 that exits 49: '$none'"
out="$(printf '{}' | PATH="$WORK/storepy" ANTIGRAVITY_CONVERSATION_ID=ag-x \
    /bin/sh "$ROOT/session-start.sh" 2>&1)"; rc=$?
[ "$rc" -eq 0 ] && [ "$out" = "{}" ] \
    && ok "SessionStart takes its no-Python path with only a broken python3" \
    || bad "python probe" "SessionStart with only a broken python3: rc=$rc out=$out"
out="$(printf '{}' | PATH="$WORK/storepy" ANTIGRAVITY_CONVERSATION_ID=ag-x \
    /bin/sh "$ROOT/stop-drain.sh" 2>&1)"; rc=$?
[ "$rc" -eq 0 ] && [ "$out" = "{}" ] \
    && ok "the Stop drain takes its no-Python path with only a broken python3" \
    || bad "python probe" "the Stop drain with only a broken python3: rc=$rc out=$out"

[ "$FAIL" -eq 0 ] && echo "plugin hooks: clean" || echo "plugin hooks: FAILURES"
exit "$FAIL"
