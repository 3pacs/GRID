-- Migration: 0040_synth_c_seeds
-- Author: SYNTH-C wave (task #100)
-- Applies via: ssh grid@100.75.185.36 "psql -d griddb -f /path/to/this/file.sql"
--              (Note: oracle_models lives in the ``griddb`` database — the
--              ``grid`` DB is for the V5 analytics layer only. Lesson from
--              task #99.)
--
-- Purpose
-- -------
-- 1. Seed the ``contagion`` oracle model head (SYNTH-35/36) so the
--    contract-driven weight evolver has a row to update as soon as the
--    first OptionsTradeOutcome lands.
-- 2. Add ``decision_journal.source_contract_id`` so the SYNTH-42 handler
--    can stamp provisional rows with the emitting contract's event_id,
--    plus a btree index so the correlation join is cheap.
--
-- Idempotent: every ``INSERT`` uses ``ON CONFLICT DO NOTHING`` and every
-- ``ALTER`` / ``CREATE INDEX`` uses ``IF NOT EXISTS``. Safe to re-run.

-- ====== MODEL HEAD ======

INSERT INTO oracle_models (
    name, version, description, signal_families, weight
) VALUES (
    'contagion',
    '1.0',
    'Supply-chain shock propagation. Every triggered contagion prediction '
    'fires a SignalFired that this head weights by PnL of the resulting '
    'trade ticket (SYNTH-35/36).',
    '["supply", "macro", "equity"]'::jsonb,
    1.0
) ON CONFLICT (name) DO NOTHING;

-- ====== DECISION JOURNAL CONTRACT LINK ======

ALTER TABLE decision_journal
    ADD COLUMN IF NOT EXISTS source_contract_id UUID NULL;

CREATE INDEX IF NOT EXISTS idx_decision_journal_source_contract
    ON decision_journal (source_contract_id);

-- ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======
-- Both tables already exist, so re-granting is a no-op but keeps the
-- migration linter happy and makes sure the ``grid`` runtime role never
-- loses write access if a previous grant chain was interrupted.

GRANT ALL ON oracle_models TO grid;
GRANT ALL ON decision_journal TO grid;
