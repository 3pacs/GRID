-- Migration: 0039_synth_b_model_heads
-- Author: SYNTH-B wave (task #99)
-- Applies via: ssh grid@100.75.185.36 "psql -d griddb -f /path/to/this/file.sql"
--              (Note: oracle_models lives in the ``griddb`` database — the
--              ``grid`` DB is for the V5 analytics layer only.)
--
-- Purpose: seed the two offensive-alpha model heads introduced by the
-- SYNTH-B wave (SYNTH-24..27) into ``oracle_models`` so the oracle engine
-- picks them up on the next cycle without waiting for the in-Python
-- seeding path in ``OracleEngine._load_models``. The model head rows are
-- also where the Bayesian weight evolver (ModelRegistry) persists
-- per-model weights — seeding them here means the first PredictionScored
-- contract can already update them.
--
-- Idempotent: the ``ON CONFLICT (name) DO NOTHING`` clause makes this
-- safe to re-run after ``_load_models`` has already seeded defaults.

-- ====== MODEL HEADS ======

INSERT INTO oracle_models (
    name, version, description, signal_families, weight
) VALUES (
    'holder_overlap',
    '1.0',
    'Institutional holder deal overlap. Smart-money pre-positioning on '
    'both legs of an M&A before the announcement is a high-trust '
    'insider-flow confirmation (SYNTH-24/25).',
    '["insider", "flows"]'::jsonb,
    1.0
) ON CONFLICT (name) DO NOTHING;

INSERT INTO oracle_models (
    name, version, description, signal_families, weight
) VALUES (
    'fundamental',
    '1.0',
    'Fundamental-vs-price divergence. Long candidates are '
    'fundamentals-strong / price-lagging; short candidates are '
    'fundamentals-weak / price-ripping. Sector-relative (SYNTH-26/27).',
    '["macro", "equity"]'::jsonb,
    1.0
) ON CONFLICT (name) DO NOTHING;

-- ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======
-- This migration only INSERTs into an already-existing table, but we
-- still re-issue the grants so the lint guard stays happy and the grid
-- role keeps write access even if the previous grant chain was
-- interrupted. Re-grants are idempotent.

GRANT ALL ON oracle_models TO grid;
