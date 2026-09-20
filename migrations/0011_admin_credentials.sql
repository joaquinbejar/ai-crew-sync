-- Administrative credentials: administration without a shell on the server.
--
-- Until now every credential was minted by the operator CLI running next to
-- Postgres. That kept minting out of reach of the agents, which is right, but
-- it also kept it out of reach of the people: onboarding a repository meant
-- SSH, docker exec and a connection string.
--
-- This migration adds a SECOND class of credential, kept deliberately apart
-- from agent tokens:
--
--   * an agent token (api_tokens, prefix acs_) identifies one agent on the
--     bus and can never mint anything, not even for its own agent;
--   * an administrative credential (admin_tokens, prefix acsa_) identifies
--     nobody on the bus and cannot post, claim or read; it can only manage
--     teams, agents and tokens — either one team or, when team_id is NULL,
--     all of them.
--
-- Neither table references the other for authentication, so an agent token
-- presented as an administrator is simply an unknown hash, and vice-versa.
-- No existing token gains a privilege it did not have.

CREATE TABLE admin_tokens (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- NULL means a global administrator: creates teams, grants credentials,
    -- administers every team. Non-NULL means an administrator of exactly
    -- that team and nothing else.
    team_id       UUID REFERENCES teams(id) ON DELETE CASCADE,
    -- sha256 of the raw credential; the raw value is shown exactly once
    token_hash    BYTEA NOT NULL UNIQUE,
    -- first characters of the raw credential, for display in listings
    prefix        TEXT NOT NULL,
    label         TEXT,
    -- Which administrative credential minted this one. NULL means the local
    -- bootstrap CLI, the only path that needs no prior credential.
    issued_by     UUID REFERENCES admin_tokens(id) ON DELETE SET NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at  TIMESTAMPTZ,
    revoked_at    TIMESTAMPTZ
);

CREATE INDEX admin_tokens_team_idx ON admin_tokens (team_id);

COMMENT ON TABLE admin_tokens IS
    'Administrative credentials (prefix acsa_). team_id NULL = global administrator.';

-- Which administrative credential minted an agent token. NULL for every
-- token minted before this migration and for the local CLI path.
ALTER TABLE api_tokens
    ADD COLUMN issued_by_admin UUID REFERENCES admin_tokens(id) ON DELETE SET NULL;

-- Audit trail. issued_by on a token says who created it and nothing about
-- who revoked it, granted it, or disabled its agent; a log does. Rows are
-- append-only and never carry a secret: at most the display prefix.
CREATE TABLE admin_audit (
    id               BIGSERIAL PRIMARY KEY,
    at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- 'cli' for the operator CLI next to Postgres (no credential involved),
    -- 'http' for the remote administration API.
    actor_source     TEXT NOT NULL CHECK (actor_source IN ('cli', 'http')),
    -- The credential that acted; NULL for the CLI. Kept after the credential
    -- is revoked, so history survives a rotation.
    actor_admin_id   UUID REFERENCES admin_tokens(id) ON DELETE SET NULL,
    action           TEXT NOT NULL CHECK (action IN (
                        'team.create', 'agent.create', 'agent.enable', 'agent.disable',
                        'token.issue', 'token.revoke',
                        'admin.grant', 'admin.revoke')),
    -- The team the action concerned; NULL for team-less actions such as
    -- granting a global credential. Not a foreign key: a deleted team keeps
    -- its history.
    team_id          UUID,
    -- The row the action created or changed (agent, token or credential id).
    subject_id       UUID,
    -- Names, labels and prefixes only. Never a secret.
    detail           JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX admin_audit_team_idx ON admin_audit (team_id, id DESC);
CREATE INDEX admin_audit_actor_idx ON admin_audit (actor_admin_id, id DESC);

COMMENT ON TABLE admin_audit IS
    'Append-only log of administrative actions. Never contains a secret.';
