-- Conversations: addressed threads with honest, per-recipient receipts.
--
-- Channels broadcast and direct messages point at one agent or one window.
-- Neither answers the question a review actually asks: *these three people
-- were asked, which of them has seen it, and which has acted on it?* A
-- channel cannot say; a DM to three people is three threads that never
-- converge.
--
-- A conversation is a thread with an explicit membership, a logical sequence
-- and a recipient snapshot per message, so "who was asked" is a fact about
-- the message rather than a guess from the membership as it looks now.
--
-- Five things this schema is careful about (ADR 0001):
--
--   * **Access is granted, never inferred.** A project grant is a row, not a
--     working directory or a role label. A private conversation's membership
--     is per *session*, so one window of an agent can be in a thread its
--     sibling cannot read.
--   * **Recipients are snapshotted at acceptance.** Someone who joins later
--     never appears in an older message's denominator, and someone removed
--     keeps their historical receipts.
--   * **Receipts are distinct observations.** Stored, delivered, presented,
--     acknowledged and resolved mean different things and are never inferred
--     from each other. A cursor moving is not a person reading.
--   * **History has boundaries.** A member sees from where they joined,
--     unless they were granted more; the grant is recorded, not implied.
--   * **Everything is audited**, including the two exceptional paths:
--     membership transfer and owner recovery.
--
-- The whole feature sits behind a per-team capability flag and is invisible
-- until an operator turns it on. Nothing here changes an existing table's
-- behaviour.

-- --------------------------------------------------------------- capability --

-- Off by default: installing a release must never expose a new surface.
ALTER TABLE teams
    ADD COLUMN conversations_enabled BOOLEAN NOT NULL DEFAULT false;

COMMENT ON COLUMN teams.conversations_enabled IS
    'Per-team capability flag for conversations. Off by default; the tools refuse while it is.';

-- ----------------------------------------------------------------- projects --

-- A project is the unit a conversation can be *visible to*, and access to it
-- is an explicit grant. Deliberately not the repository name a session
-- publishes for discovery: that is a label a caller chooses, and this decides
-- who can read.
CREATE TABLE projects (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    team_id     UUID NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    created_by  UUID REFERENCES agents(id) ON DELETE SET NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    archived_at TIMESTAMPTZ,
    UNIQUE (team_id, name),
    -- Composite target for the conversation FK below, so a conversation can
    -- never point at a project of another team.
    UNIQUE (id, team_id)
);

CREATE TABLE project_agent_access (
    project_id  UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    agent_id    UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    granted_by  UUID REFERENCES agents(id) ON DELETE SET NULL,
    granted_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, agent_id)
);

-- ------------------------------------------------------------ conversations --

CREATE TABLE conversations (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    team_id     UUID NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    -- NULL for a private conversation: only its members can see it exists.
    project_id  UUID,
    -- 'project' is readable by anyone with access to the project; 'private'
    -- only by its members. Immutable after creation, so a thread cannot be
    -- widened retroactively past what people said in it.
    visibility  TEXT NOT NULL CHECK (visibility IN ('project', 'private')),
    title       TEXT NOT NULL,
    created_by  UUID NOT NULL REFERENCES agents(id) ON DELETE RESTRICT,
    created_session TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    archived_at TIMESTAMPTZ,
    -- Last logical sequence handed out; the next message takes this + 1.
    -- Kept on the row so a send takes one lock and sequence is gap-free per
    -- conversation.
    last_seq    BIGINT NOT NULL DEFAULT 0,
    -- Which backend holds the bodies of this conversation's messages. Only
    -- 'postgres' exists today; later phases add others per conversation, and
    -- a column here is what keeps that a routing decision rather than a
    -- global migration.
    backend     TEXT NOT NULL DEFAULT 'postgres',
    CHECK (visibility = 'private' OR project_id IS NOT NULL),
    FOREIGN KEY (project_id, team_id) REFERENCES projects (id, team_id) ON DELETE CASCADE,
    UNIQUE (id, team_id)
);

CREATE INDEX conversations_team_idx ON conversations (team_id, created_at DESC);
CREATE INDEX conversations_project_idx ON conversations (project_id) WHERE project_id IS NOT NULL;

-- Membership is per (agent, session): a window, not a person. The shared
-- session is '' as everywhere else, so a client that sends no header joins
-- as its agent's shared window.
CREATE TABLE conversation_memberships (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    agent_id        UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    session         TEXT NOT NULL DEFAULT '',
    role            TEXT NOT NULL DEFAULT 'participant'
                    CHECK (role IN ('owner', 'moderator', 'participant', 'observer')),
    state           TEXT NOT NULL DEFAULT 'invited'
                    CHECK (state IN ('invited', 'active', 'left', 'removed')),
    -- Where this member's history starts. NULL means "from the beginning",
    -- which is what the creator gets and what an explicit grant can give.
    history_from_seq BIGINT,
    invited_by      UUID REFERENCES agents(id) ON DELETE SET NULL,
    invited_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    accepted_at     TIMESTAMPTZ,
    ended_at        TIMESTAMPTZ,
    -- Set when this membership was superseded by a transfer, pointing at the
    -- membership that took it over. The old row is kept: its receipts and
    -- authorship are history, not something to rewrite.
    superseded_by   UUID REFERENCES conversation_memberships(id) ON DELETE SET NULL,
    UNIQUE (conversation_id, agent_id, session)
);

CREATE INDEX conversation_memberships_agent_idx
    ON conversation_memberships (agent_id, session)
    WHERE state IN ('invited', 'active');

-- ---------------------------------------------------------------- messages --

CREATE TABLE conversation_messages (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    -- Gap-free per conversation, assigned under the conversation row lock.
    -- Readers page by this, never by timestamp.
    seq             BIGINT NOT NULL,
    sender_agent    UUID NOT NULL REFERENCES agents(id) ON DELETE RESTRICT,
    sender_session  TEXT NOT NULL DEFAULT '',
    -- The body. Phase 2 stores it here; the conversation's `backend` column
    -- is what a later phase routes on.
    body            TEXT NOT NULL,
    reply_to        UUID REFERENCES conversation_messages(id) ON DELETE SET NULL,
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Caller-supplied idempotency key. A retry of the same request must not
    -- make a second logical message.
    request_id      UUID NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    edited_at       TIMESTAMPTZ,
    deleted_at      TIMESTAMPTZ,
    UNIQUE (conversation_id, seq),
    UNIQUE (conversation_id, request_id)
);

CREATE INDEX conversation_messages_thread_idx
    ON conversation_messages (conversation_id, seq DESC);

-- Who this message was addressed to, as the membership looked when it was
-- accepted. This is the denominator every receipt question is answered
-- against, and it never changes afterwards.
CREATE TABLE message_recipients (
    message_id    UUID NOT NULL REFERENCES conversation_messages(id) ON DELETE CASCADE,
    membership_id UUID NOT NULL REFERENCES conversation_memberships(id) ON DELETE CASCADE,
    agent_id      UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    session       TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (message_id, membership_id)
);

CREATE INDEX message_recipients_membership_idx ON message_recipients (membership_id);

-- Five independent observations, each with its own timestamp. A row exists
-- only for what actually happened: an absent column is "not observed", never
-- "assumed".
--
--   stored       the authoritative backend confirmed persistence
--   delivered    a transport reported handing it to a client
--   presented    a host confirmed it reached the model's context — unknown
--                for hosts that cannot say, and left NULL rather than guessed
--   acknowledged the recipient said it read it
--   resolved     the recipient said it acted on it
CREATE TABLE message_receipts (
    message_id      UUID NOT NULL REFERENCES conversation_messages(id) ON DELETE CASCADE,
    membership_id   UUID NOT NULL REFERENCES conversation_memberships(id) ON DELETE CASCADE,
    stored_at       TIMESTAMPTZ,
    delivered_at    TIMESTAMPTZ,
    presented_at    TIMESTAMPTZ,
    acknowledged_at TIMESTAMPTZ,
    resolved_at     TIMESTAMPTZ,
    -- Free text the acknowledging session chose to leave, e.g. what it did.
    note            TEXT,
    PRIMARY KEY (message_id, membership_id)
);

-- ------------------------------------------------------------------- audit --

-- Append-only, and the only record of the two exceptional paths: a
-- membership transfer and an owner recovery. Never holds a body.
CREATE TABLE conversation_audit (
    id              BIGSERIAL PRIMARY KEY,
    at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    team_id         UUID NOT NULL,
    conversation_id UUID,
    actor_agent     UUID,
    actor_session   TEXT,
    action          TEXT NOT NULL CHECK (action IN (
                        'conversation.create', 'conversation.archive',
                        'member.invite', 'member.join', 'member.leave',
                        'member.remove', 'member.transfer', 'member.recover',
                        'message.send', 'message.ack',
                        'project.create', 'project.grant', 'project.revoke')),
    subject         UUID,
    detail          JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX conversation_audit_conversation_idx
    ON conversation_audit (conversation_id, id DESC);
CREATE INDEX conversation_audit_team_idx ON conversation_audit (team_id, id DESC);

COMMENT ON TABLE conversation_audit IS
    'Append-only log of conversation and membership actions. Never contains a message body.';
