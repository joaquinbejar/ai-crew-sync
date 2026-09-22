#!/bin/sh
# bus-call.sh <tool> [json-args]
# One stateless tools/call against the crew bus. Prints the raw JSON-RPC
# response on stdout, so every caller keeps parsing `.result.structuredContent`
# whichever path answered. Silently no-ops when nothing is configured, so the
# plugin never breaks a session that has no bus.
#
# Three modes, in this order:
#
#   0. Authenticated binding. When BUS_HOST_SESSION names a conversation whose
#      proxy registered a session, the call goes through
#      `ai-crew-sync context hook --event call` as THAT window, with the
#      credential and epoch the proxy recorded, whatever else the environment
#      holds: an exported BUS_TOKEN would otherwise route a Stop drain to the
#      shared session's inbox and inject another window's question here. The
#      credential never passes through this script, and the binary serves
#      only the tools these scripts use (whoami, read_messages, team_digest,
#      heartbeat): a hook cannot issue, rotate or revoke a credential. A
#      binding whose credential is gone stays silent, like the lifecycle
#      hooks; a conversation with no binding at all falls through to the
#      modes below.
#
#   1. Local binary + profiles. When `ai-crew-sync` is on the PATH and no
#      BUS_TOKEN is exported, credentials come from the local profiles and the
#      project's .acs.toml, and BUS_HOST_SESSION (the host's conversation id,
#      passed by each hook from its payload) picks the SAME bus session the
#      `mcp proxy` of this conversation uses. That is what keeps a hook from
#      draining a sibling window's messages: both sides derive the session from
#      the conversation id, with no shared mutable file between them.
#
#   2. Legacy curl. BUS_URL + BUS_TOKEN in the environment, session from
#      BUS_SESSION. Unchanged, so existing setups keep working with no binary
#      installed.
set -eu
TOOL="$1"
# A plain assignment, not a default inside a parameter expansion: macOS's
# /bin/sh keeps the backslash that escapes the closing brace there, and
# `{\}` is not JSON. An omitted or empty second argument is an empty object.
ARGS="${2:-}"
[ -n "$ARGS" ] || ARGS='{}'

# ---------------------------------------------------------------- mode 0 --
if [ -n "${BUS_HOST_SESSION:-}" ] && command -v ai-crew-sync >/dev/null 2>&1; then
    case "$(ai-crew-sync context hook --binding "$BUS_HOST_SESSION" --event status 2>/dev/null)" in
        *'"authenticated"'*)
            OUT="$(ai-crew-sync context hook --binding "$BUS_HOST_SESSION" --event call \
                --tool "$TOOL" --args "$ARGS" 2>/dev/null || true)"
            [ -n "$OUT" ] || exit 0
            printf '{"jsonrpc":"2.0","id":1,"result":{"structuredContent":%s}}\n' "$OUT"
            exit 0
            ;;
        *'"no-credential"'*) exit 0 ;;
    esac
fi

# ---------------------------------------------------------------- mode 1 --
if [ -z "${BUS_TOKEN:-}" ] && command -v ai-crew-sync >/dev/null 2>&1; then
    # `client call` maps straight onto tools/call; --json prints the tool's
    # structured content, which is wrapped below into the JSON-RPC envelope
    # every caller already parses.
    OUT="$(ai-crew-sync client --json call "$TOOL" --args "$ARGS" 2>/dev/null || true)"
    [ -n "$OUT" ] || exit 0
    printf '{"jsonrpc":"2.0","id":1,"result":{"structuredContent":%s}}\n' "$OUT"
    exit 0
fi

# ---------------------------------------------------------------- mode 2 --
[ -n "${BUS_URL:-}" ] && [ -n "${BUS_TOKEN:-}" ] || exit 0

# Same source and same fallback as plugin/.mcp.json: unset means the shared
# session, which is a real session and not an error.
#
# Anything the server would reject is dropped rather than sent, so the hook
# falls back to the shared session instead of failing every call: an
# unexpanded ${VAR} from a client without default syntax, a label with a '/'
# (which separates agent from session when addressing), one over the 64-byte
# cap, or one carrying a newline — which in a header is not a bad label but a
# header injection.
#
# The allowed set is deliberately narrower than the server's: this value is
# interpolated into an HTTP header from a shell script, and a repository name
# needs nothing outside it.
SESSION="${BUS_SESSION:-}"
case "$SESSION" in
    *[!A-Za-z0-9._-]*) SESSION="" ;;
esac
[ "${#SESSION}" -gt 64 ] && SESSION=""


if [ -n "$SESSION" ]; then
    exec curl -sf --max-time "${BUS_TIMEOUT:-8}" -X POST "$BUS_URL" \
      -H "Authorization: Bearer $BUS_TOKEN" \
      -H "X-Crew-Session: $SESSION" \
      -H "Content-Type: application/json" \
      -H "Accept: application/json, text/event-stream" \
      --data "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"$TOOL\",\"arguments\":$ARGS}}"
fi

exec curl -sf --max-time "${BUS_TIMEOUT:-8}" -X POST "$BUS_URL" \
  -H "Authorization: Bearer $BUS_TOKEN" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  --data "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"$TOOL\",\"arguments\":$ARGS}}"
