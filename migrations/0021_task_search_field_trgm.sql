-- CrewSync task search field-aligned trigram indexes (additive, safe to rerun).
--
-- The production contains/regex predicates search title and description as
-- separate expressions. Keep these indexes expression-aligned with those
-- predicates; the concatenated index in 0020 serves a different query shape.
-- Tenant authorization and all other task filters remain query responsibilities.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS tasks_title_search_trgm_idx
    ON tasks
    USING GIN (title gin_trgm_ops);

CREATE INDEX IF NOT EXISTS tasks_description_search_trgm_idx
    ON tasks
    USING GIN ((coalesce(description, '')) gin_trgm_ops);

-- pg_trgm can accelerate LIKE/ILIKE and regex predicates when the pattern
-- exposes extractable trigrams. Pathological or otherwise nonindexable regexes
-- may still require a bounded scan; callers must retain statement timeouts.
