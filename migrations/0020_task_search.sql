-- CrewSync task search indexes (additive, safe to rerun).
--
-- Search remains constrained by the caller's team_id in the task query. These
-- indexes accelerate candidate matching only; authorization and all existing
-- task filters must still be applied by the query itself.

-- pg_trgm supports indexed case-insensitive substring and regex searches. The
-- separator prevents a match from being formed by the end of title and the
-- beginning of description. COALESCE keeps NULL descriptions searchable.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS tasks_search_trgm_idx
    ON tasks
    USING GIN ((coalesce(title, '') || ' ' || coalesce(description, '')) gin_trgm_ops);

-- The default search language is deliberately fixed to `simple`: task keys,
-- identifiers, and mixed-language text must not depend on database locale or
-- session configuration. Title terms receive higher weight than description
-- terms. The language-specific indexes keep the explicitly supported V1
-- English and Russian modes indexable without interpolating a regconfig value.
CREATE INDEX IF NOT EXISTS tasks_search_fts_simple_idx
    ON tasks
    USING GIN ((
        setweight(to_tsvector('simple', coalesce(title, '')), 'A') ||
        setweight(to_tsvector('simple', coalesce(description, '')), 'B')
    ));

CREATE INDEX IF NOT EXISTS tasks_search_fts_english_idx
    ON tasks
    USING GIN ((
        setweight(to_tsvector('english', coalesce(title, '')), 'A') ||
        setweight(to_tsvector('english', coalesce(description, '')), 'B')
    ));

CREATE INDEX IF NOT EXISTS tasks_search_fts_russian_idx
    ON tasks
    USING GIN ((
        setweight(to_tsvector('russian', coalesce(title, '')), 'A') ||
        setweight(to_tsvector('russian', coalesce(description, '')), 'B')
    ));

-- Tenant-aware EXPLAIN guidance for disposable benchmark fixtures:
--   EXPLAIN (ANALYZE, BUFFERS)
--   SELECT id, team_id, key, title, description
--   FROM tasks
--   WHERE team_id = $1
--     AND (coalesce(title, '') || ' ' || coalesce(description, '')) ILIKE '%' || $2 || '%'
--   ORDER BY updated_at DESC, key ASC
--   LIMIT $3;
--
-- Always include the authenticated team_id predicate when measuring plans. For
-- contains/regex, look for a Bitmap Index Scan using tasks_search_trgm_idx;
-- for keywords, use the matching fixed-language weighted expression and look
-- for its GIN index. Run ANALYZE on the disposable fixture first. Do not run
-- EXPLAIN ANALYZE or any benchmark query against production from this migration.
