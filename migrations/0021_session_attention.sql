-- Connectivity is not attention (#221).
--
-- A presence row said one thing, `status`, and three writers meant three
-- different things by it: the proxy's keepalive re-asserted "active" every
-- five minutes because its process was alive, the Stop hook said "active"
-- when the model had just finished its turn, and a model could say anything.
-- A window whose turn had ended therefore looked exactly like one reading
-- its messages, and senders waited on answers nobody was going to write.
--
-- These columns keep the observations apart. Each is what one writer saw,
-- with the time it saw it; nothing here is inferred from anything else, and
-- an observation nobody made stays NULL, which the tools report as unknown.
--
-- attention          what the host's model was doing when last observed:
--                    'attending' (a turn is running), 'waiting' (a turn is
--                    blocked inside a bus wait) or 'idle' (the turn ended).
-- attention_at       when that was observed.
-- last_tool_at       the last tool call the window's model made, as its
--                    proxy saw it. Proxy liveness never moves it.
-- last_inbox_read_at the last read of this window's inbox (read_messages
--                    with scope inbox or all). A read is not a reply.
-- wake               a descriptive label of how this window can be woken
--                    while idle ('claude-watcher', 'codex-adapter'), as the
--                    mechanism announced it. Not authority, not proof that a
--                    wake will succeed.
-- wake_until         when that announcement lapses unless renewed.
-- wake_token         an opaque value the announcer chose, so that it can
--                    withdraw its own announcement and never a successor's.

ALTER TABLE agent_presence
    ADD COLUMN attention TEXT
        CHECK (attention IN ('attending', 'waiting', 'idle')),
    ADD COLUMN attention_at TIMESTAMPTZ,
    ADD COLUMN last_tool_at TIMESTAMPTZ,
    ADD COLUMN last_inbox_read_at TIMESTAMPTZ,
    ADD COLUMN wake TEXT,
    ADD COLUMN wake_until TIMESTAMPTZ,
    ADD COLUMN wake_token TEXT;
