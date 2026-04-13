-- Migration: 0041_actor_trust_or_cog.sql
-- Purpose: INTEL-2 — add the trust-vs-cog axis to lever_pullers so every actor
-- can be classified as a leading signal source ("trust") or a downstream cog
-- in the machine ("cog"). The score is in [-1, +1]:
--   +1.0 → pure trust  (signals reliably precede market moves)
--    0.0 → noise / unknown / mixed
--   -1.0 → pure cog    (actor moves AFTER the underlying force already shifted)
--
-- Computation lives in intelligence/actor_trust_cog.py and joins
--   • lever_pullers     — precision (correct/total) + avg_lead_time_days
--   • actor_analytics   — pagerank, betweenness (graph centrality)
--   • actor_credibility — say-do alignment, claims_hit/total
--
-- Idempotent: ALTER ... ADD COLUMN IF NOT EXISTS guards re-runs.
-- Target DB: griddb (NOT grid).

ALTER TABLE lever_pullers
    ADD COLUMN IF NOT EXISTS trust_or_cog_score NUMERIC,
    ADD COLUMN IF NOT EXISTS classification     TEXT,
    ADD COLUMN IF NOT EXISTS classification_at  TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_lever_pullers_trust_or_cog
    ON lever_pullers (trust_or_cog_score DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS idx_lever_pullers_classification
    ON lever_pullers (classification);

-- ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======
GRANT ALL ON lever_pullers TO grid;
GRANT USAGE, SELECT ON SEQUENCE lever_pullers_id_seq TO grid;
