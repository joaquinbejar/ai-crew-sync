-- Session discovery: find the window you mean by what it does, not by its id.
--
-- With one token and one repository open in five windows (implementation,
-- design, three reviewers), the session label is what keeps their claims,
-- cursors and locks apart — and a label good at that is a poor name for a
-- person to type. Two optional discovery labels ride on the presence row so
-- list_sessions can answer "the design window of market-data" with an exact
-- `agent/session` address:
--
--   project  the logical project the window works on (usually a repository)
--   role     what the window does there: implementation, design, review, …
--
-- Both are metadata a caller sets about itself. Neither is identity (that is
-- still the bearer token alone), neither grants anything, and two windows may
-- share both — the address stays distinct because the session does.
ALTER TABLE agent_presence
    ADD COLUMN project TEXT,
    ADD COLUMN role    TEXT;

COMMENT ON COLUMN agent_presence.project IS
    'Discovery label: the logical project this session works on. Not identity.';
COMMENT ON COLUMN agent_presence.role IS
    'Discovery label: what this session does (implementation, design, review, …). Not identity.';
