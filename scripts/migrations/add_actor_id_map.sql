-- Migration: Create actor_id_map for canonical ID resolution
-- String IDs (gov_us_trump, corp_NVDA, icij_*) are canonical.
-- UUID IDs (from GDELT/search) are aliases that map to canonical IDs.
--
-- Run: PGPASSWORD=gridmaster2026 psql -U grid -d griddb -h localhost -f scripts/migrations/add_actor_id_map.sql

BEGIN;

-- 1. Create the mapping table
CREATE TABLE IF NOT EXISTS actor_id_map (
    alias_id     TEXT NOT NULL,
    canonical_id TEXT NOT NULL,
    match_method TEXT DEFAULT 'name',  -- 'name', 'manual', 'sparql'
    confidence   REAL DEFAULT 1.0,
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (alias_id)
);

CREATE INDEX IF NOT EXISTS idx_actor_id_map_canonical ON actor_id_map(canonical_id);

-- 2. Self-map: every string-ID actor that appears in connections maps to itself
INSERT INTO actor_id_map (alias_id, canonical_id, match_method, confidence)
SELECT DISTINCT id, id, 'self', 1.0
FROM actors
WHERE id !~ '^[0-9a-f]{8}-[0-9a-f]{4}-'
ON CONFLICT (alias_id) DO NOTHING;

-- 3. Map UUID actors to string-ID actors by exact name match
INSERT INTO actor_id_map (alias_id, canonical_id, match_method, confidence)
SELECT uuid_actor.id AS alias_id, string_actor.id AS canonical_id, 'name_exact', 1.0
FROM actors uuid_actor
JOIN actors string_actor ON LOWER(TRIM(uuid_actor.name)) = LOWER(TRIM(string_actor.name))
WHERE uuid_actor.id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-'
  AND string_actor.id !~ '^[0-9a-f]{8}-[0-9a-f]{4}-'
ON CONFLICT (alias_id) DO NOTHING;

-- 4. Self-map remaining UUID actors (no string-ID match found — they ARE canonical)
INSERT INTO actor_id_map (alias_id, canonical_id, match_method, confidence)
SELECT id, id, 'self_uuid', 0.8
FROM actors
WHERE id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-'
  AND id NOT IN (SELECT alias_id FROM actor_id_map)
ON CONFLICT (alias_id) DO NOTHING;

-- 5. Stats
SELECT match_method, COUNT(*), ROUND(AVG(confidence)::numeric, 2) AS avg_conf
FROM actor_id_map
GROUP BY match_method
ORDER BY COUNT(*) DESC;

COMMIT;
