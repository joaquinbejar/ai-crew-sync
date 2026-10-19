-- A short replayable log of task, lock and note events (#211).
--
-- NOTIFY is fire-and-forget: an event published while nobody is listening is
-- gone, and a lock release deletes the very row that could have told a
-- latecomer about it. wait_for_updates subscribes per call, so a client that
-- waits again (or the proxy covering one long wait with several requests)
-- could miss a task, lock or note change landing between two calls. Messages
-- never had the problem: their unread state is in the database.
--
-- Each of those events is now also appended here, and the NOTIFY carries
-- the row id as `event_id`. A wait returns the position it reached, and the
-- next one replays what came after it. Rows hold the same ids-only payload
-- the NOTIFY always carried, scoped by team, and are kept for a day.

CREATE TABLE bus_event_log (
    id      BIGSERIAL   PRIMARY KEY,
    team_id UUID        NOT NULL,
    at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload JSONB       NOT NULL
);

CREATE INDEX bus_event_log_team_idx ON bus_event_log (team_id, id);
CREATE INDEX bus_event_log_at_idx ON bus_event_log (at);

COMMENT ON TABLE bus_event_log IS
    'Replayable copy of task, lock and note events for wait_for_updates (#211). '
    'Ids-only payloads, team-scoped, kept for about a day.';

-- Append, then notify with the row id, so a listener and a replay agree on
-- the position of every event. Roughly one call in a hundred also trims what
-- is older than a day, which keeps the table small without a scheduler.
CREATE FUNCTION bus_emit(p JSONB) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    event_id BIGINT;
BEGIN
    INSERT INTO bus_event_log (team_id, payload)
        VALUES ((p ->> 'team_id')::uuid, p)
        RETURNING id INTO event_id;
    PERFORM pg_notify('bus_events', (p || jsonb_build_object('event_id', event_id))::text);
    IF random() < 0.01 THEN
        DELETE FROM bus_event_log WHERE at < now() - interval '1 day';
    END IF;
END $$;

CREATE OR REPLACE FUNCTION notify_task() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM bus_emit(jsonb_build_object(
        'kind', 'task',
        'team_id', NEW.team_id,
        'key', NEW.key,
        'status', NEW.status,
        'claimed_by', NEW.claimed_by
    ));
    RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION notify_lock() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        PERFORM bus_emit(jsonb_build_object(
            'kind', 'lock',
            'event', 'released',
            'team_id', OLD.team_id,
            'name', OLD.name
        ));
        RETURN NULL;
    END IF;
    PERFORM bus_emit(jsonb_build_object(
        'kind', 'lock',
        'event', 'acquired',
        'team_id', NEW.team_id,
        'name', NEW.name,
        'holder_agent_id', NEW.holder_agent_id
    ));
    RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION notify_note() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM bus_emit(jsonb_build_object(
        'kind', 'note',
        'team_id', NEW.team_id,
        'scope', NEW.scope,
        'key', NEW.key,
        'updated_by', NEW.updated_by
    ));
    RETURN NULL;
END $$;
