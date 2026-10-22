# Sourced by the lifecycle hooks (not run on its own): read the host's
# payload once from stdin, and work out which conversation this is and which
# host's contract to answer in.
#
# Sets PAYLOAD, HOST_SESSION (the conversation id, possibly empty) and
# HOOK_HOST (claude | antigravity):
#
# - Claude Code and Codex send `session_id`.
# - Google Antigravity sends camelCase `conversationId` (protojson) and also
#   exports ANTIGRAVITY_CONVERSATION_ID to every hook it runs (verified with
#   agy 1.4.3).
#
# Without Python nothing can be parsed: HOOK_HOST stays claude and
# HOST_SESSION empty, which every hook already treats as "no binding".
PAYLOAD="$(cat 2>/dev/null || true)"
. "$DIR/python.sh"
_acs_ids="$(PAYLOAD="$PAYLOAD" "${PY:-python3}" -c 'import json, os
try:
    p = json.loads(os.environ.get("PAYLOAD") or "{}")
except Exception:
    p = {}
p = p if isinstance(p, dict) else {}

def clean(v):
    v = str(v or "").strip()
    # One line, not an option, or nothing: the id is passed as one argument.
    ok = v and "\n" not in v and "\r" not in v and not v.startswith("-")
    return v if ok else ""

if clean(p.get("conversationId")):
    host, cid = "antigravity", clean(p.get("conversationId"))
elif clean(p.get("session_id")):
    host, cid = "claude", clean(p.get("session_id"))
else:
    cid = clean(os.environ.get("ANTIGRAVITY_CONVERSATION_ID"))
    host = "antigravity" if cid else "claude"
# Antigravity runs hooks from the plugin directory; the workspace is in the
# payload, and presence (repo, branch) has to describe it.
roots = p.get("workspacePaths")
workspace = clean(roots[0]) if isinstance(roots, list) and roots else ""
# One write, so a failure cannot leave half an answer behind.
print(f"{host}\n{cid}\n{workspace}")
' 2>/dev/null || printf 'claude\n\n\n')"
HOOK_HOST="$(printf '%s\n' "$_acs_ids" | sed -n 1p)"
HOST_SESSION="$(printf '%s\n' "$_acs_ids" | sed -n 2p)"
HOOK_WORKSPACE="$(printf '%s\n' "$_acs_ids" | sed -n 3p)"
# The workspace the host is working in, when it says so and it exists: the
# helper reads repo and branch from the current directory.
[ -n "$HOOK_WORKSPACE" ] && [ -d "$HOOK_WORKSPACE" ] && cd "$HOOK_WORKSPACE"
case "$HOOK_HOST" in
    antigravity) ;;
    *) HOOK_HOST=claude ;;
esac
unset _acs_ids

# Antigravity reads a JSON object from every hook's stdout; Claude Code reads
# nothing from most hooks. A hook that has nothing to say ends with this.
acs_quiet_exit() {
    [ "$HOOK_HOST" = antigravity ] && printf '{}\n'
    exit 0
}

# Hand TEXT to the model at session start, in this host's contract.
acs_context() {
    ACS_TEXT="$1" HOOK_HOST="$HOOK_HOST" "${PY:-python3}" -c 'import json, os
text = os.environ["ACS_TEXT"]
if os.environ.get("HOOK_HOST") == "antigravity":
    print(json.dumps({"injectSteps": [{"systemMessage": {"systemMessage": text}}]}))
else:
    print(json.dumps({"hookSpecificOutput": {"hookEventName": "SessionStart", "additionalContext": text}}))'
}
