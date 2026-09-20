-- Durable inbox delivery for conversations routed off Postgres.
--
-- Phase 6 of ADR 0001. Two things change, and neither touches a team that
-- has not been routed to a broker:
--
--   * after a body is canonically published, one **reference** per
--     recipient is queued here and published to that recipient's own
--     subject. A reference carries no body: it names the message, and the
--     body is fetched with a current ACL check at the moment it is read.
--   * a receipt change queues a reference for the sender in the same
--     transaction that writes the receipt, so a notification cannot exist
--     without the receipt it reports, or the other way round.
--
-- Postgres remains the truth. The broker is how a recipient finds out
-- quickly; everything it holds can be rebuilt from `message_receipts`,
-- which is what reconciliation does after an expiry or a deleted consumer.

CREATE TABLE inbox_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    team_id         UUID NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    -- Who this is for, as an opaque key: a registered session's id, or
    -- 'a<agent id>' for a membership that names no session. Deterministic,
    -- so the same recipient always resolves to the same subject.
    recipient_key   TEXT NOT NULL,
    kind            TEXT NOT NULL CHECK (kind IN ('message', 'receipt')),
    message_id      UUID NOT NULL REFERENCES conversation_messages(id) ON DELETE CASCADE,
    membership_id   UUID REFERENCES conversation_memberships(id) ON DELETE CASCADE,
    -- The reference itself. Ids, addresses and a sequence; never a body.
    payload         JSONB NOT NULL,
    state           TEXT NOT NULL DEFAULT 'pending'
                    CHECK (state IN ('pending', 'published', 'failed')),
    attempts        INT NOT NULL DEFAULT 0,
    last_error      TEXT,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- One reference per recipient per message per kind. A retry of the
    -- publication is the same row, never a second notification.
    UNIQUE (kind, message_id, recipient_key)
);

CREATE INDEX inbox_events_due_idx ON inbox_events (next_attempt_at)
    WHERE state = 'pending';

COMMENT ON TABLE inbox_events IS
    'Per-recipient references awaiting publication. Never carries a body.';

-- Handing a reference to a client and committing that it was delivered are
-- two steps on purpose: the client says it has the reference durably, and
-- only then is the receipt written and the broker acknowledged. A crash in
-- between redelivers, and redelivery finds this row.
CREATE TABLE inbox_deliveries (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    team_id       UUID NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    recipient_key TEXT NOT NULL,
    -- The session that pulled it, and the epoch it was at. A confirmation
    -- from a process that has since been resumed away changes nothing.
    session_id    UUID REFERENCES agent_sessions(id) ON DELETE CASCADE,
    epoch         BIGINT,
    message_id    UUID NOT NULL REFERENCES conversation_messages(id) ON DELETE CASCADE,
    membership_id UUID REFERENCES conversation_memberships(id) ON DELETE CASCADE,
    -- Where to acknowledge the broker once the receipt is committed. Empty
    -- for a reference reconstructed from Postgres, which has nothing to
    -- acknowledge.
    ack_subject   TEXT NOT NULL DEFAULT '',
    stream_seq    BIGINT,
    handed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    confirmed_at  TIMESTAMPTZ
);

CREATE INDEX inbox_deliveries_open_idx ON inbox_deliveries (recipient_key, handed_at)
    WHERE confirmed_at IS NULL;
CREATE UNIQUE INDEX inbox_deliveries_seq_idx
    ON inbox_deliveries (recipient_key, stream_seq)
    WHERE stream_seq IS NOT NULL;

COMMENT ON TABLE inbox_deliveries IS
    'References handed to a client, and whether it confirmed holding them durably.';
