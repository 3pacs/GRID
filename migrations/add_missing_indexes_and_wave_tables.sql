-- GRID Migration: Missing indexes + Wave 1-3 tables
-- Run: psql -U grid_user -d grid -f migrations/add_missing_indexes_and_wave_tables.sql

-- ============================================================
-- Missing indexes from ATTENTION.md #16
-- ============================================================

-- decision_journal: heavily queried by model_version_id
CREATE INDEX IF NOT EXISTS idx_decision_journal_model_version_id
    ON decision_journal (model_version_id);

-- decision_journal: outcome statistics queries
CREATE INDEX IF NOT EXISTS idx_decision_journal_outcome_recorded_at
    ON decision_journal (outcome_recorded_at)
    WHERE outcome_recorded_at IS NOT NULL;

-- resolved_series: conflict reporting queries
CREATE INDEX IF NOT EXISTS idx_resolved_series_conflict
    ON resolved_series (feature_id, obs_date)
    WHERE conflict_flag = TRUE;

-- ============================================================
-- Wave 1: Circuit breaker state table
-- ============================================================

CREATE TABLE IF NOT EXISTS paper_strategy_breaker_state (
    strategy_id TEXT PRIMARY KEY,
    state TEXT DEFAULT 'CLOSED',
    consecutive_failures INT DEFAULT 0,
    last_failure_at TIMESTAMPTZ,
    opened_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Wave 1: Agent runs additions (conviction + persona + debate)
-- ============================================================

ALTER TABLE agent_runs ADD COLUMN IF NOT EXISTS conviction_score FLOAT;
ALTER TABLE agent_runs ADD COLUMN IF NOT EXISTS persona VARCHAR(50) DEFAULT 'balanced';
ALTER TABLE agent_runs ADD COLUMN IF NOT EXISTS debate_rounds INT;

-- Index for persona performance queries
CREATE INDEX IF NOT EXISTS idx_agent_runs_persona
    ON agent_runs (persona, final_decision);

-- ============================================================
-- Verify
-- ============================================================

SELECT 'Migration complete' AS status;
