-- Moving a conversation between backends, and being able to prove it worked.
--
-- Phase 7 of ADR 0001. This migration adds the bookkeeping a supervised
-- move needs; it moves nothing by itself, and installing it changes no
-- behaviour. A default installation gains three unused columns and two
-- empty tables.
--
-- The important one is `conversation_messages.backend`. Until now the
-- conversation said where its bodies were, which is true at rest and false
-- exactly when it matters: during a move, some bodies are on the broker and
-- some are not. Recording the authoritative backend **per message** is what
-- makes a half-moved thread readable rather than a thread with a hole in
-- it, and it is what lets a rollback be a fact rather than a hope.

ALTER TABLE conversation_messages
    ADD COLUMN backend TEXT NOT NULL DEFAULT 'postgres'
        CHECK (backend IN ('postgres', 'jetstream'));

COMMENT ON COLUMN conversation_messages.backend IS
    'Where THIS body is authoritative. The conversation says where new ones go.';

-- Existing rows: wherever their conversation says they are. This is a
-- backfill of a fact, not a change of one.
UPDATE conversation_messages m
   SET backend = c.backend
  FROM conversations c
 WHERE c.id = m.conversation_id AND c.backend <> 'postgres';

-- A move pauses writes on one conversation for as long as the tail copy
-- takes. Everything else on the bus keeps working: this is one thread, and
-- senders are told plainly what is happening and what to do.
ALTER TABLE conversations ADD COLUMN write_paused_at TIMESTAMPTZ;
COMMENT ON COLUMN conversations.write_paused_at IS
    'Set while a supervised backend move copies this thread''s tail. Sends are refused.';

-- One supervised move. Kept after it finishes: it is the evidence that the
-- move preserved what it claimed to preserve.
CREATE TABLE conversation_migrations (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    team_id         UUID NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    -- 'to_jetstream' or 'to_postgres'. A rollback is a move like any other,
    -- with its own evidence.
    direction       TEXT NOT NULL CHECK (direction IN ('to_jetstream', 'to_postgres')),
    state           TEXT NOT NULL DEFAULT 'planned'
                    CHECK (state IN ('planned', 'copying', 'verified', 'cut_over',
                                     'failed', 'rolled_back')),
    messages        BIGINT NOT NULL DEFAULT 0,
    bytes           BIGINT NOT NULL DEFAULT 0,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    last_error      TEXT
);

CREATE INDEX conversation_migrations_open_idx
    ON conversation_migrations (conversation_id)
    WHERE state IN ('planned', 'copying', 'verified');

-- One message's part in that move, with the checksum that says the body
-- that arrived is the body that left. An interrupted run resumes from here:
-- what is already verified is not copied again.
CREATE TABLE conversation_migration_items (
    migration_id    UUID NOT NULL REFERENCES conversation_migrations(id) ON DELETE CASCADE,
    message_id      UUID NOT NULL REFERENCES conversation_messages(id) ON DELETE CASCADE,
    -- SHA-256 of the body as it was read from the source.
    checksum        TEXT NOT NULL,
    bytes           BIGINT NOT NULL,
    target_locator  TEXT,
    state           TEXT NOT NULL DEFAULT 'planned'
                    CHECK (state IN ('planned', 'copied', 'verified', 'failed')),
    last_error      TEXT,
    PRIMARY KEY (migration_id, message_id)
);

COMMENT ON TABLE conversation_migration_items IS
    'Per-message evidence: what was copied, and that it came back identical.';
