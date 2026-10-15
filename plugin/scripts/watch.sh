#!/bin/sh
# Background watcher that wakes an idle window when the bus has news for it.
#
# A session parked at the prompt calls no tools, so a message addressed to it
# waits until its human types something. Claude Code runs this hook in the
# background (`async` + `asyncRewake`), from SessionStart and again after
# every turn: `context hook --event watch` waits on the bus as THIS window
# and exits 2 with a short notice only when something new is addressed to it
# (unread direct messages, conversation messages), which wakes the session.
# A newer watcher of the same window replaces the older one, the same unread
# messages never wake it twice, and it gives up quietly after BUS_WATCH_SECS.
#
# Authenticated mode only: it needs the binary and this conversation's
# binding. Without them (legacy mode, no bus, another host) it exits 0 and
# does nothing; a watcher must never act as a guessed identity.
set -u
command -v ai-crew-sync >/dev/null 2>&1 || exit 0
command -v python3 >/dev/null 2>&1 || exit 0
PAYLOAD="$(cat 2>/dev/null || true)"
HOST_SESSION="$(PAYLOAD="$PAYLOAD" python3 -c 'import json,os,sys
try:
    v = json.loads(os.environ.get("PAYLOAD") or "{}").get("session_id")
except Exception:
    v = None
sys.stdout.write(str(v) if v else "")' 2>/dev/null || true)"
[ -n "$HOST_SESSION" ] || exit 0
# The exit status is the signal: 2 wakes the session with stderr as the
# notice, 0 leaves it alone. Nothing goes to stdout.
exec ai-crew-sync context hook --binding "$HOST_SESSION" --event watch >/dev/null
