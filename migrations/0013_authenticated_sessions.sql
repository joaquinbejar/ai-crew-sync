-- Authenticated sessions: a window proves which window it is.
--
-- `X-Crew-Session` partitions work by a label the caller chooses. That is
-- enough to keep one person's windows from overwriting each other's presence
-- and claims, and it is not enough to own anything: any holder of the agent
-- token can send any label, so a label can never gate access to a private
-- conversation (ADR 0001).
--
-- This adds a second credential *derived from* an agent token, one per host
-- conversation:
--
--   * registration presents the agent token, so agent and team come from it
--     and a client cannot assert either;
--   * the session credential (prefix acss_) authenticates as that agent, in
--     that one session label, and can mint nothing — not an agent token, not
--     an administrative credential, not another session;
--   * it expires on its own (24 hours by default) and dies with its parent:
--     revoke the token or disable the agent and every session under it stops
--     authenticating, with no sweep to run;
--   * `epoch` fences stale connections. A resume bumps it, so a process that
--     was replaced cannot keep writing as the window that replaced it.
--
-- Legacy stays exactly as it was: an agent token with a header label keeps
-- working for every existing tool and for a bare `curl`.

CREATE TABLE agent_sessions (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Denormalised from the parent token so every lookup is one join less
    -- and a team-scoped query never has to trust the caller.
    agent_id      UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    -- The credential this session hangs from. Its revocation is the
    -- session's revocation; there is no independent lifetime.
    parent_token  UUID NOT NULL REFERENCES api_tokens(id) ON DELETE CASCADE,
    -- The session label this credential authenticates as: the same value
    -- `X-Crew-Session` would have carried, now proven rather than asserted.
    label         TEXT NOT NULL,
    -- sha256 of the raw credential; the raw value is returned exactly once
    token_hash    BYTEA NOT NULL UNIQUE,
    prefix        TEXT NOT NULL,
    -- Bumped on every resume. A request carrying an older epoch belongs to a
    -- connection that has been replaced and is refused.
    epoch         BIGINT NOT NULL DEFAULT 1 CHECK (epoch > 0),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at  TIMESTAMPTZ,
    expires_at    TIMESTAMPTZ NOT NULL,
    revoked_at    TIMESTAMPTZ,
    -- One live session per (agent, label): resuming a conversation renews
    -- the row it already has instead of accumulating credentials for the
    -- same window.
    UNIQUE (agent_id, label)
);

CREATE INDEX agent_sessions_parent_idx ON agent_sessions (parent_token);
CREATE INDEX agent_sessions_expiry_idx ON agent_sessions (expires_at)
    WHERE revoked_at IS NULL;

COMMENT ON TABLE agent_sessions IS
    'Session credentials (prefix acss_) derived from an agent token. Expire with their parent; cannot mint anything.';
COMMENT ON COLUMN agent_sessions.epoch IS
    'Connection epoch. A resume bumps it; requests carrying an older one are stale and refused.';
