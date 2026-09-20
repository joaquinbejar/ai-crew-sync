-- A backend boundary, and an outbox to test failure handling against.
--
-- Phase 2 commits a conversation message and its recipients in one Postgres
-- transaction: `stored` is true because that transaction committed, and
-- there is nothing to reconcile. That path stays, and stays the default.
--
-- What it cannot do is tell us how the system behaves when persistence is
-- somewhere else and the write can fail *after* acceptance — the shape every
-- broker introduces. This migration adds that shape, with Postgres as the
-- only implementation, so the failure modes can be exercised before there is
-- anything external to blame:
--
--   pending_publication → stored      the backend confirmed, with a locator
--   pending_publication → failed      it will not be confirmed; the slot is
--                                     kept, visible and explicit
--
-- A conversation carries its backend (`conversations.backend`, already
-- there) and now a publication mode. Asynchronous publication is a
-- capability someone opts into, never latency imposed on the synchronous
-- path.

-- ------------------------------------------------------------ publication --

ALTER TABLE conversations
    ADD COLUMN publication TEXT NOT NULL DEFAULT 'sync'
        CHECK (publication IN ('sync', 'outbox'));

COMMENT ON COLUMN conversations.publication IS
    'sync = body committed with the message (default). outbox = accepted first, published by a worker.';

-- A message that is not yet stored on its backend, and where it ended up.
ALTER TABLE conversation_messages
    ADD COLUMN publication_state TEXT NOT NULL DEFAULT 'stored'
        CHECK (publication_state IN ('stored', 'pending_publication', 'failed')),
    -- Where the authoritative copy lives, once the backend confirmed it.
    -- Opaque to everything but the adapter that wrote it.
    ADD COLUMN canonical_locator TEXT;

COMMENT ON COLUMN conversation_messages.publication_state IS
    'stored = confirmed by the authoritative backend. pending_publication = accepted, not yet confirmed. failed = will not be.';

CREATE INDEX conversation_messages_pending_idx
    ON conversation_messages (conversation_id, seq)
    WHERE publication_state = 'pending_publication';

-- ---------------------------------------------------------------- outbox --

-- One row per message awaiting publication. Leased, fenced and bounded:
--
--   * `lease_expires_at` + `leased_by` is the lease. A worker that dies
--     releases its work by expiry, like every other lease on this bus.
--   * `generation` fences a worker that comes back after its lease expired:
--     it bumps on every lease, and a stale worker's update matches nothing.
--   * `attempts` and `payload_bytes` are the bounds. A payload that cannot
--     be published is not retried forever and is not unbounded in size.
CREATE TABLE conversation_outbox (
    message_id      UUID PRIMARY KEY REFERENCES conversation_messages(id) ON DELETE CASCADE,
    conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    team_id         UUID NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    backend         TEXT NOT NULL,
    -- The body, held here only until the backend confirms it. Deleted on
    -- success, so the outbox is never a second copy of the history.
    payload         TEXT NOT NULL,
    payload_bytes   INTEGER NOT NULL CHECK (payload_bytes >= 0),
    -- Idempotency key the adapter presents to the backend, so a retry after
    -- an uncertain completion is recognised rather than duplicated.
    publish_key     UUID NOT NULL,
    state           TEXT NOT NULL DEFAULT 'pending'
                    CHECK (state IN ('pending', 'leased', 'failed')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    generation      BIGINT NOT NULL DEFAULT 0,
    leased_by       TEXT,
    lease_expires_at TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The claim query: due work, oldest first, skipping what another worker
-- holds. Same shape as claim_next_task, for the same reason.
CREATE INDEX conversation_outbox_due_idx
    ON conversation_outbox (next_attempt_at, created_at)
    WHERE state IN ('pending', 'leased');

CREATE INDEX conversation_outbox_team_idx ON conversation_outbox (team_id, state);

COMMENT ON TABLE conversation_outbox IS
    'Messages accepted but not yet confirmed by their backend. Leased, fenced by generation, bounded by attempts.';
