-- Migration: 0060_sponsor_ticker_map.sql
-- Author: trial-gems agent (task #28)
-- Applies via: sudo -u postgres psql griddb -f migrations/0060_sponsor_ticker_map.sql
--
-- Purpose: persistent, curated cache of clinical-trial sponsor -> ticker
--          resolutions (grid/signals/sponsor_resolver.py). Every resolution is
--          written here — including negatives (ticker NULL, source = the
--          rejecting layer, notes = reason) — so the layered lookup (SEC
--          company_tickers.json -> GRID name maps -> local LLM) is never
--          recomputed for the same sponsor. Rows may be hand-corrected:
--          set ticker + source='curated', confidence=1.0.
--
-- Key: sponsor_norm — the normalised sponsor name produced by
--      sponsor_resolver.normalize_sponsor_name (lower-cased, punctuation and
--      legal suffixes stripped, whitespace collapsed).
--
-- Populated by: grid.signals.sponsor_resolver.resolve_sponsor
-- Consumed by:  grid.ingestors.trial_ingestor, grid.signals.trial_signal
--
-- Idempotent: every CREATE uses IF NOT EXISTS; GRANTs are no-ops on re-run.
-- The module also runs the same DDL via ensure_sponsor_map_table() so a
-- missing migration degrades to a self-created table rather than a failed job.

-- ====== SCHEMA CHANGES ======

CREATE TABLE IF NOT EXISTS sponsor_ticker_map (
    sponsor_norm  TEXT PRIMARY KEY,
    ticker        TEXT,                        -- NULL = resolved to "not investable / unknown"
    source        TEXT NOT NULL,               -- non_industry | sec_exact | sec_normalized | sec_fuzzy |
                                               -- sector_map | company_profiles | llm_local | curated | unresolved
    confidence    NUMERIC(4,3) NOT NULL DEFAULT 0,
    resolved_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes         TEXT                         -- reason for NULL ticker, or the raw sponsor name
);

CREATE INDEX IF NOT EXISTS idx_sponsor_ticker_map_ticker
    ON sponsor_ticker_map (ticker)
    WHERE ticker IS NOT NULL;

-- ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======
-- Migrations run as `postgres`; the API and ingestors connect as `grid`.
-- Without these grants the `grid` user gets `permission denied for table`.
-- (No SERIAL column, so no sequence grant is needed.)
GRANT ALL ON sponsor_ticker_map TO grid;
