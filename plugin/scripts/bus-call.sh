#!/bin/sh
# bus-call.sh <tool> [json-args]
# One stateless tools/call against the crew bus. Prints the raw JSON-RPC
# response on stdout, so every caller keeps parsing `.result.structuredContent`
# whichever path answered. Silently no-ops when nothing is configured, so the
# plugin never breaks a session that has no bus.
#
# Two modes, in this order:
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
ARGS="${2:-{\}}"

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
