-- Migration: 0037_options_v2_schema
-- Purpose: GEX V2 — Options Dealer Flow normalized schema (crypto-first, additive).
--
-- Three new tables introduce a canonical cross-venue options pipeline:
--   option_contracts_normalized — canonical contract identity (venue, symbol, ...)
--   option_snapshots_raw        — per-tick market state (TimescaleDB hypertable)
--   option_exposures            — aggregated per-snapshot dealer-flow output
--
-- Additive ONLY: the existing `options_snapshots` table (equity/SPY pipeline) is
-- untouched so the DealerGammaEngine keeps flowing.
--
-- Dependencies: TimescaleDB extension (already installed on grid-svr). The
-- hypertable call is wrapped in a DO block so this migration degrades
-- gracefully on environments that lack the extension (dev, CI, local psql).
--
-- Applies via: sudo -u postgres psql griddb -f migrations/0037_options_v2_schema.sql

BEGIN;

-- ============================================================================
-- 1. option_contracts_normalized — canonical cross-venue contract identity
-- ============================================================================
CREATE TABLE IF NOT EXISTS option_contracts_normalized (
    id                   BIGSERIAL PRIMARY KEY,
    venue                TEXT NOT NULL,                 -- 'deribit', 'okx', 'bybit', 'polygon_equity'
    symbol               TEXT NOT NULL,                 -- venue-native option symbol
    underlying           TEXT NOT NULL,                 -- 'BTC', 'ETH', 'SPY', 'QQQ'
    expiry_ts_utc        BIGINT NOT NULL,               -- UTC milliseconds
    strike               DOUBLE PRECISION NOT NULL CHECK (strike > 0),
    option_type          TEXT NOT NULL CHECK (option_type IN ('call', 'put')),
    contract_size        DOUBLE PRECISION NOT NULL CHECK (contract_size > 0),
    settlement_currency  TEXT NOT NULL,                 -- 'BTC', 'ETH', 'USDC', 'USD'
    quote_currency       TEXT NOT NULL,
    first_seen_utc       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_utc        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_expired           BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (venue, symbol)
);
CREATE INDEX IF NOT EXISTS idx_ocn_underlying_expiry
    ON option_contracts_normalized (underlying, expiry_ts_utc DESC);
CREATE INDEX IF NOT EXISTS idx_ocn_venue_symbol
    ON option_contracts_normalized (venue, symbol);

-- ============================================================================
-- 2. option_snapshots_raw — per-tick normalized market state (hypertable)
-- ============================================================================
CREATE TABLE IF NOT EXISTS option_snapshots_raw (
    id                   BIGSERIAL,
    contract_id          BIGINT NOT NULL REFERENCES option_contracts_normalized(id),
    venue                TEXT NOT NULL,
    underlying           TEXT NOT NULL,
    strike               DOUBLE PRECISION NOT NULL,
    option_type          TEXT NOT NULL,
    mark_price           DOUBLE PRECISION,
    bid                  DOUBLE PRECISION,
    ask                  DOUBLE PRECISION,
    mid                  DOUBLE PRECISION,
    oi_contracts         DOUBLE PRECISION,
    oi_underlying_units  DOUBLE PRECISION,
    volume_24h           DOUBLE PRECISION,
    underlying_price     DOUBLE PRECISION NOT NULL,
    iv_decimal           DOUBLE PRECISION,
    delta                DOUBLE PRECISION,
    gamma                DOUBLE PRECISION,
    vanna                DOUBLE PRECISION,
    charm                DOUBLE PRECISION,
    vomma                DOUBLE PRECISION,
    color                DOUBLE PRECISION,
    zomma                DOUBLE PRECISION,
    speed                DOUBLE PRECISION,
    greek_source_gamma   TEXT CHECK (greek_source_gamma IN ('exchange','recomputed','mixed')),
    greek_source_delta   TEXT,
    source_ts_utc        BIGINT NOT NULL,
    ingest_ts_utc        BIGINT NOT NULL,
    quote_age_ms         INTEGER,
    spread_bps           DOUBLE PRECISION,
    data_quality_flags   JSONB DEFAULT '{}'::jsonb,
    row_confidence       DOUBLE PRECISION,
    PRIMARY KEY (id, source_ts_utc)
);

-- TimescaleDB hypertable, 1-day chunks. Wrapped in a DO block so environments
-- without the timescaledb extension (CI, local dev) still complete the
-- migration — option_snapshots_raw just stays a plain table there.
DO $$
BEGIN
    PERFORM create_hypertable(
        'option_snapshots_raw',
        'source_ts_utc',
        chunk_time_interval => 86400000,  -- 1 day in ms
        if_not_exists => TRUE
    );
EXCEPTION
    WHEN undefined_function THEN
        RAISE NOTICE 'TimescaleDB not installed — option_snapshots_raw left as plain table';
    WHEN undefined_file THEN
        RAISE NOTICE 'TimescaleDB extension missing — option_snapshots_raw left as plain table';
    WHEN OTHERS THEN
        RAISE NOTICE 'create_hypertable skipped (%): option_snapshots_raw left as plain table', SQLERRM;
END
$$;

CREATE INDEX IF NOT EXISTS idx_osr_underlying_ts
    ON option_snapshots_raw (underlying, source_ts_utc DESC);
CREATE INDEX IF NOT EXISTS idx_osr_contract_ts
    ON option_snapshots_raw (contract_id, source_ts_utc DESC);

-- ============================================================================
-- 3. option_exposures — aggregated per-snapshot dealer-flow output
-- ============================================================================
CREATE TABLE IF NOT EXISTS option_exposures (
    snapshot_id          TEXT NOT NULL,                 -- e.g. 'btc_2026-04-11T19:30:00Z'
    venue                TEXT NOT NULL,                 -- or 'multi' for aggregation
    underlying           TEXT NOT NULL,
    spot                 DOUBLE PRECISION NOT NULL,
    max_dte_days         INTEGER NOT NULL,
    net_gex              DOUBLE PRECISION,
    net_cex              DOUBLE PRECISION,
    net_vex              DOUBLE PRECISION,
    net_voex             DOUBLE PRECISION,
    net_colex            DOUBLE PRECISION,
    net_zex              DOUBLE PRECISION,
    net_speedex          DOUBLE PRECISION,
    gamma_flip           DOUBLE PRECISION,
    call_wall            DOUBLE PRECISION,
    put_wall             DOUBLE PRECISION,
    call_charm_wall      DOUBLE PRECISION,
    put_charm_wall       DOUBLE PRECISION,
    confidence_score     DOUBLE PRECISION CHECK (confidence_score BETWEEN 0 AND 1),
    regime_tags          TEXT[],                        -- ['positive_gamma','charm_supportive']
    row_count            INTEGER,
    rejected_rows        INTEGER,
    recomputed_gamma_pct DOUBLE PRECISION,
    stale_quote_pct      DOUBLE PRECISION,
    venue_agreement_score DOUBLE PRECISION,
    computed_at_utc      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    code_version         TEXT,                          -- git sha of the computing code
    PRIMARY KEY (venue, underlying, snapshot_id)
);
CREATE INDEX IF NOT EXISTS idx_oe_underlying_time
    ON option_exposures (underlying, computed_at_utc DESC);
CREATE INDEX IF NOT EXISTS idx_oe_regime_tags
    ON option_exposures USING GIN (regime_tags);

-- ============================================================================
-- GRANT FOOTER (REQUIRED — enforced by scripts/lint_migrations.py)
-- ============================================================================
-- Per-table grants (linter requires exact "GRANT ALL ON <table> TO grid;" form)
GRANT ALL ON option_contracts_normalized TO grid;
GRANT ALL ON option_snapshots_raw TO grid;
GRANT ALL ON option_exposures TO grid;

-- Per-sequence grants for BIGSERIAL primary keys
GRANT USAGE, SELECT ON SEQUENCE option_contracts_normalized_id_seq TO grid;
GRANT USAGE, SELECT ON SEQUENCE option_snapshots_raw_id_seq TO grid;

-- Blanket fallback so any future sequences in public schema are also granted
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO grid;

COMMIT;
