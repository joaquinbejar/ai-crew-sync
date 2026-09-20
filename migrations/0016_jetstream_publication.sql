-- Opt-in JetStream publication for new conversations.
--
-- Phase 5 connects the proven outbox path to a real broker, for
-- conversations an operator explicitly routes there. Nothing is migrated:
-- existing conversations keep `backend = 'postgres'` and read exactly as
-- they did, and a default installation still never contacts a broker.
--
-- What this migration adds is the state needed to be *honest* about a
-- two-system write:
--
--   * `uncertain_at` marks a publication whose outcome we do not know — the
--     PubAck never arrived. It is neither stored nor failed, and saying
--     either would be a guess. Reconciliation resolves it.
--   * `tombstone` records a message whose body is gone from its backend
--     (retention, or an operator deletion) while the thread's sequence
--     keeps its place. A gap that is explained is not the same as a gap.
--
-- The body stays in `conversation_messages` until the backend confirms;
-- only then is it dropped and the locator becomes the authority. That
-- temporary duplication is deliberate: losing a body to a failed publish
-- would be worse than storing it twice for a second.

ALTER TABLE conversation_messages
    -- Set when a publish attempt ended without an answer. Cleared by
    -- reconciliation, one way or the other.
    ADD COLUMN uncertain_at TIMESTAMPTZ,
    -- Set when the body is no longer retrievable from its backend. The
    -- message, its sequence, its recipients and its receipts all remain.
    ADD COLUMN tombstoned_at TIMESTAMPTZ,
    ADD COLUMN tombstone_reason TEXT;

COMMENT ON COLUMN conversation_messages.uncertain_at IS
    'A publish whose outcome is unknown. Not stored, not failed: reconciliation decides.';
COMMENT ON COLUMN conversation_messages.tombstoned_at IS
    'The body is gone from its backend; the message keeps its place in the sequence.';

CREATE INDEX conversation_messages_uncertain_idx
    ON conversation_messages (conversation_id)
    WHERE uncertain_at IS NOT NULL;

-- Which backend a team's conversations are created on. Per team, because
-- routing is an operator decision about infrastructure, and per
-- conversation thereafter, because a thread never changes backend once it
-- holds messages.
ALTER TABLE teams
    ADD COLUMN default_backend TEXT NOT NULL DEFAULT 'postgres'
        CHECK (default_backend IN ('postgres', 'jetstream'));

COMMENT ON COLUMN teams.default_backend IS
    'Backend new conversations of this team are created on. Existing ones keep theirs.';

-- Routing a team is its own administrative action, and the audit trail has
-- to be able to say so. 0014 added 'team.capability'; this adds the one
-- `team capability --backend` writes.
ALTER TABLE admin_audit
    DROP CONSTRAINT admin_audit_action_check,
    ADD CONSTRAINT admin_audit_action_check CHECK (action IN (
        'team.create', 'team.capability', 'team.backend', 'agent.create',
        'agent.enable', 'agent.disable', 'token.issue', 'token.revoke',
        'admin.grant', 'admin.revoke'));

-- The body's digest, kept when the body itself is not.
--
-- A retry is recognised by comparing the incoming body with the stored one
-- under the same request_id. Once publication releases the staging copy
-- there is no stored body to compare with, and a legitimate retry would be
-- refused as "a different message". The digest survives the release and
-- answers the same question.
ALTER TABLE conversation_messages ADD COLUMN body_sha256 TEXT;

COMMENT ON COLUMN conversation_messages.body_sha256 IS
    'SHA-256 of the body as sent. Outlives the staging copy, so a retry is still recognised.';
