-- Migration: 0032_supply_chain_edge_validation
-- Purpose: Persistent validation state on supply_chain_edges so the weekly
--          validator (intelligence/supply_chain_edge_validator.py) can flag
--          edges whose upstream/downstream price series stop co-moving.
--
-- Columns:
--   relationship_weak       — TRUE once the edge has been below the correlation
--                              floor for a sustained (180d+) stretch. Hand-review
--                              gate before ripping the edge out of the graph.
--   last_validation_at      — wall-clock timestamp of the most recent pass.
--   validation_correlation  — rolling 180d Pearson correlation on daily log
--                              returns, most recent value.
--   weak_since              — first observation date the correlation dipped
--                              below the floor. Cleared when the correlation
--                              recovers above the floor.
--
-- Idempotent: IF NOT EXISTS guards on every column and index.

ALTER TABLE supply_chain_edges
    ADD COLUMN IF NOT EXISTS relationship_weak      BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS last_validation_at     TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS validation_correlation NUMERIC,
    ADD COLUMN IF NOT EXISTS weak_since             DATE;

CREATE INDEX IF NOT EXISTS idx_supply_edges_weak
    ON supply_chain_edges(relationship_weak)
    WHERE relationship_weak = TRUE;

-- ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======
-- Re-grant in case the role lost privileges; no-op if already granted.
GRANT ALL ON supply_chain_edges TO grid;
