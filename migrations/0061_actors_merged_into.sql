-- Migration: 0061_actors_merged_into.sql
-- Author: handoff-07 actor filer folding
-- Applies via: sudo -u postgres psql griddb -f migrations/0061_actors_merged_into.sql
--
-- Purpose: mark an actor row as an alias of another actor instead of deleting
--          it. The 2026-09-10 curated Louvain run had three communities (255 /
--          245 / 197 nodes) built entirely out of SEC filer display names
--          ("CENOVUS ENERGY INC.  (CVE, CVE-PB)  (CIK 0001071297)") wired only
--          to each other, while the CVE Corp / BLK Corp ticker actors for the
--          same companies sat in the market clusters.
--
--          scripts/fold_actor_aliases.py re-points actor_connections onto the
--          canonical ticker/insider actor and then sets merged_into on the
--          filer row. The row itself is NEVER deleted: decision_journal,
--          wealth_flows, actor_analytics and actor_news all reference
--          actors(id), so a delete would cascade or orphan real history.
--
--          scripts/graph_analytics.py::_CURATED_EDGE_SQL excludes actors with
--          merged_into IS NOT NULL, so folded nodes leave the curated graph
--          while the full-scope actor_analytics run is unaffected.
--
-- Populated by: scripts/fold_actor_aliases.py
-- Consumed by:  scripts/graph_analytics.py (curated scope)
--
-- Idempotent: ADD COLUMN IF NOT EXISTS / CREATE INDEX IF NOT EXISTS, and the
-- GRANT is a no-op on re-run. intelligence/actor_identity.py mirrors this DDL
-- via ensure_merged_into_column() so a tree that has not applied the migration
-- degrades to a self-created column rather than a failed job (the convention
-- documented in 0060_sponsor_ticker_map.sql).

-- ====== SCHEMA CHANGES ======

ALTER TABLE actors ADD COLUMN IF NOT EXISTS merged_into TEXT;

COMMENT ON COLUMN actors.merged_into IS
    'Canonical actors.id this row is an alias of (SEC filer name folded into '
    'its ticker or insider actor). NULL for every actor that stands on its own. '
    'Rows are marked, never deleted — other tables reference actors(id).';

-- Partial: only the folded minority is indexed, and the curated graph query
-- filters on merged_into IS NULL.
CREATE INDEX IF NOT EXISTS idx_actors_merged_into
    ON actors (merged_into)
    WHERE merged_into IS NOT NULL;

-- ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======
-- Migrations run as `postgres`; the API and ingestors connect as `grid`.
-- Without these grants the `grid` user gets `permission denied for table`.
-- (No new table and no SERIAL column, so no sequence grant is needed; the
-- re-issued table grant covers the added column.)
GRANT ALL ON actors TO grid;
