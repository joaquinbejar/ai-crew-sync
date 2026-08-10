#!/bin/sh
# bus-call.sh <tool> [json-args]
# One stateless JSON-RPC tools/call against the crew bus. Prints the raw
# JSON-RPC response on stdout. Silently no-ops if BUS_URL/BUS_TOKEN are unset,
# so the plugin never breaks a session that has no bus configured.
#
# Every hook goes through here, so this is where the session label has to be
# sent. Without it the hooks are person-scoped while the MCP connection beside
# them is session-scoped, and the two write different presence rows.
set -eu
[ -n "${BUS_URL:-}" ] && [ -n "${BUS_TOKEN:-}" ] || exit 0
TOOL="$1"
ARGS="${2:-{\}}"

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
