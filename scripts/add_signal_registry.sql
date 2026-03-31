-- Migration: add_signal_registry
-- Created: 2026-03-31
-- Purpose: Add signal_registry table for cross-module signal persistence,
--          and extend oracle_models with model factory columns.
--
-- Run with:
--   psql postgresql://grid:gridmaster2026@localhost:5432/griddb -f scripts/add_signal_registry.sql

-- ============================================================
-- 1. signal_registry
--    Central ledger for every signal emitted by any source module.
--    (signal_id, valid_from) is the natural PK for PIT deduplication.
-- ============================================================

CREATE TABLE IF NOT EXISTS signal_registry (
    id              BIGSERIAL PRIMARY KEY,
    signal_id       TEXT NOT NULL,
    source_module   TEXT NOT NULL,
    signal_type     TEXT NOT NULL,
    ticker          TEXT,
    direction       TEXT NOT NULL,
    value           DOUBLE PRECISION NOT NULL,
    z_score         DOUBLE PRECISION,
    confidence      DOUBLE PRECISION NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_from      TIMESTAMPTZ NOT NULL,
    valid_until     TIMESTAMPTZ,
    freshness_hours DOUBLE PRECISION,
    metadata        JSONB,
    provenance      TEXT,
    UNIQUE (signal_id, valid_from)
);

-- Filter by ticker with recency sort — most common query pattern
CREATE INDEX IF NOT EXISTS idx_signal_reg_ticker
    ON signal_registry (ticker, created_at DESC);

-- Filter by emitting module + signal type (used by oracle model factory)
CREATE INDEX IF NOT EXISTS idx_signal_reg_source
    ON signal_registry (source_module, signal_type);

-- Point-in-time validity window lookups
CREATE INDEX IF NOT EXISTS idx_signal_reg_pit
    ON signal_registry (valid_from, valid_until);

-- ============================================================
-- 2. oracle_models — model factory extensions
--    New columns support dynamic model composition and lineage.
-- ============================================================

ALTER TABLE oracle_models ADD COLUMN IF NOT EXISTS signal_sources JSONB;
ALTER TABLE oracle_models ADD COLUMN IF NOT EXISTS signal_filters JSONB;
ALTER TABLE oracle_models ADD COLUMN IF NOT EXISTS weight_config  JSONB;
ALTER TABLE oracle_models ADD COLUMN IF NOT EXISTS created_by     TEXT    DEFAULT 'human';
ALTER TABLE oracle_models ADD COLUMN IF NOT EXISTS parent_model   TEXT;
ALTER TABLE oracle_models ADD COLUMN IF NOT EXISTS active         BOOLEAN DEFAULT TRUE;
