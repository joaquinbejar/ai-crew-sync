-- What the last wake attempt for a window came to (#222).
--
-- A wake mechanism (the Codex adapter) records the outcome of each attempt
-- on the window it serves, so a sender can tell a working adapter from one
-- whose last attempt failed:
--
-- wake_result     'requested' (the host accepted a new turn), 'confirmed'
--                 (the host said the turn started) or 'failed' (the
--                 mechanism gave up). None of these is a message receipt: a
--                 started turn has not read anything, let alone answered.
-- wake_result_at  when that was recorded.

ALTER TABLE agent_presence
    ADD COLUMN wake_result TEXT
        CHECK (wake_result IN ('requested', 'confirmed', 'failed')),
    ADD COLUMN wake_result_at TIMESTAMPTZ;
