-- ============================================================
-- GRID — Database Schema
-- Execute with: psql -U grid_user -d grid -f schema.sql
-- Or via: python db.py
-- ============================================================

-- Enable TimescaleDB extension if available
-- -- CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ============================================================
-- TABLE: source_catalog
-- Registry of all external data sources with quality metadata.
-- ============================================================
CREATE TABLE IF NOT EXISTS source_catalog (
    id                SERIAL PRIMARY KEY,
    name              TEXT NOT NULL UNIQUE,
    base_url          TEXT NOT NULL,
    cost_tier         TEXT NOT NULL CHECK (cost_tier IN ('FREE', 'LOW', 'PAID')),
    latency_class     TEXT NOT NULL CHECK (latency_class IN ('REALTIME', 'EOD', 'WEEKLY', 'MONTHLY')),
    pit_available     BOOLEAN NOT NULL DEFAULT FALSE,
    revision_behavior TEXT NOT NULL CHECK (revision_behavior IN ('NEVER', 'RARE', 'FREQUENT')),
    trust_score       TEXT NOT NULL CHECK (trust_score IN ('HIGH', 'MED', 'LOW')),
    priority_rank     INTEGER NOT NULL,
    active            BOOLEAN NOT NULL DEFAULT TRUE,
    last_pull_at      TIMESTAMPTZ,
    uptime_score      DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_source_catalog_priority_rank ON source_catalog (priority_rank);
CREATE INDEX IF NOT EXISTS idx_source_catalog_active ON source_catalog (active);
CREATE INDEX IF NOT EXISTS idx_source_catalog_trust_score ON source_catalog (trust_score);

-- ============================================================
-- TABLE: raw_series
-- Raw data as pulled from each source, before any resolution.
-- ============================================================
CREATE TABLE IF NOT EXISTS raw_series (
    id                BIGSERIAL PRIMARY KEY,
    series_id         TEXT NOT NULL,
    source_id         INTEGER NOT NULL REFERENCES source_catalog(id),
    obs_date          DATE NOT NULL,
    pull_timestamp    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    value             DOUBLE PRECISION NOT NULL,
    raw_payload       JSONB,
    pull_status       TEXT NOT NULL CHECK (pull_status IN ('SUCCESS', 'PARTIAL', 'FAILED'))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_series_composite
    ON raw_series (series_id, source_id, obs_date, pull_timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_series_series_obs
    ON raw_series (series_id, obs_date DESC);
CREATE INDEX IF NOT EXISTS idx_raw_series_source_pull
    ON raw_series (source_id, pull_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_raw_series_pull_status
    ON raw_series (pull_status);
CREATE INDEX IF NOT EXISTS idx_raw_series_obs_date
    ON raw_series (obs_date DESC);

-- ============================================================
-- TABLE: feature_registry
-- Canonical list of all features with transformation metadata.
-- ============================================================
CREATE TABLE IF NOT EXISTS feature_registry (
    id                    SERIAL PRIMARY KEY,
    name                  TEXT NOT NULL UNIQUE,
    family                TEXT NOT NULL CHECK (family IN (
                              'rates', 'credit', 'breadth', 'vol', 'fx',
                              'commodity', 'sentiment', 'macro', 'earnings',
                              'crypto')),
    description           TEXT NOT NULL,
    transformation        TEXT NOT NULL,
    transformation_version INTEGER NOT NULL DEFAULT 1,
    lag_days              INTEGER NOT NULL DEFAULT 0,
    normalization         TEXT NOT NULL CHECK (normalization IN (
                              'ZSCORE', 'MINMAX', 'RAW', 'RANK')),
    missing_data_policy   TEXT NOT NULL CHECK (missing_data_policy IN (
                              'FORWARD_FILL', 'INTERPOLATE', 'NAN')),
    eligible_from_date    DATE NOT NULL,
    model_eligible        BOOLEAN NOT NULL DEFAULT FALSE,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deprecated_at         TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_feature_registry_family ON feature_registry (family);
CREATE INDEX IF NOT EXISTS idx_feature_registry_model_eligible ON feature_registry (model_eligible);
CREATE INDEX IF NOT EXISTS idx_feature_registry_name ON feature_registry (name);

-- ============================================================
-- TABLE: resolved_series
-- Point-in-time resolved data with conflict tracking.
-- ============================================================
CREATE TABLE IF NOT EXISTS resolved_series (
    id                    BIGSERIAL PRIMARY KEY,
    feature_id            INTEGER NOT NULL REFERENCES feature_registry(id),
    obs_date              DATE NOT NULL,
    release_date          DATE NOT NULL,
    vintage_date          DATE NOT NULL,
    value                 DOUBLE PRECISION NOT NULL,
    source_priority_used  INTEGER NOT NULL REFERENCES source_catalog(id),
    conflict_flag         BOOLEAN NOT NULL DEFAULT FALSE,
    conflict_detail       JSONB,
    resolution_version    INTEGER NOT NULL DEFAULT 1
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_resolved_series_composite
    ON resolved_series (feature_id, obs_date, vintage_date);
CREATE INDEX IF NOT EXISTS idx_resolved_series_feature_obs
    ON resolved_series (feature_id, obs_date DESC);
CREATE INDEX IF NOT EXISTS idx_resolved_series_release_date
    ON resolved_series (release_date);
CREATE INDEX IF NOT EXISTS idx_resolved_series_vintage_date
    ON resolved_series (vintage_date);
CREATE INDEX IF NOT EXISTS idx_resolved_series_conflict
    ON resolved_series (conflict_flag) WHERE conflict_flag = TRUE;

-- ============================================================
-- TABLE: hypothesis_registry
-- Tracks hypotheses through their lifecycle.
-- ============================================================
CREATE TABLE IF NOT EXISTS hypothesis_registry (
    id                          SERIAL PRIMARY KEY,
    statement                   TEXT NOT NULL,
    layer                       TEXT NOT NULL CHECK (layer IN (
                                    'REGIME', 'TACTICAL', 'EXECUTION')),
    feature_ids                 INTEGER[] NOT NULL,
    lag_structure               JSONB NOT NULL,
    baseline_id                 INTEGER,
    proposed_metric             TEXT NOT NULL,
    proposed_threshold          DOUBLE PRECISION NOT NULL,
    state                       TEXT NOT NULL DEFAULT 'CANDIDATE'
                                CHECK (state IN (
                                    'CANDIDATE', 'TESTING', 'PASSED', 'FAILED', 'KILLED')),
    kill_reason                 TEXT,
    predecessor_hypothesis_id   INTEGER REFERENCES hypothesis_registry(id),
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_hypothesis_registry_layer ON hypothesis_registry (layer);
CREATE INDEX IF NOT EXISTS idx_hypothesis_registry_state ON hypothesis_registry (state);
CREATE INDEX IF NOT EXISTS idx_hypothesis_registry_created ON hypothesis_registry (created_at DESC);

-- ============================================================
-- TABLE: model_registry
-- Tracks model versions through their state machine lifecycle.
-- ============================================================
CREATE TABLE IF NOT EXISTS model_registry (
    id                    SERIAL PRIMARY KEY,
    name                  TEXT NOT NULL,
    layer                 TEXT NOT NULL CHECK (layer IN (
                              'REGIME', 'TACTICAL', 'EXECUTION')),
    version               TEXT NOT NULL,
    state                 TEXT NOT NULL DEFAULT 'CANDIDATE'
                          CHECK (state IN (
                              'CANDIDATE', 'SHADOW', 'STAGING',
                              'PRODUCTION', 'FLAGGED', 'RETIRED')),
    hypothesis_id         INTEGER NOT NULL REFERENCES hypothesis_registry(id),
    validation_run_id     INTEGER,
    feature_set           INTEGER[] NOT NULL,
    parameter_snapshot    JSONB NOT NULL,
    baseline_id           INTEGER,
    promoted_at           TIMESTAMPTZ,
    promoted_by           TEXT,
    retired_at            TIMESTAMPTZ,
    retire_reason         TEXT,
    predecessor_id        INTEGER REFERENCES model_registry(id),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_model_registry_name_version
    ON model_registry (name, version);
CREATE INDEX IF NOT EXISTS idx_model_registry_layer_state
    ON model_registry (layer, state);
CREATE INDEX IF NOT EXISTS idx_model_registry_state
    ON model_registry (state);
CREATE INDEX IF NOT EXISTS idx_model_registry_hypothesis
    ON model_registry (hypothesis_id);

-- Enforce: only one PRODUCTION model per layer at a time
CREATE UNIQUE INDEX IF NOT EXISTS one_production_per_layer
    ON model_registry (layer)
    WHERE state = 'PRODUCTION';

-- ============================================================
-- TABLE: validation_results
-- Stores backtest / walk-forward validation run outputs.
-- ============================================================
CREATE TABLE IF NOT EXISTS validation_results (
    id                    SERIAL PRIMARY KEY,
    run_timestamp         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    hypothesis_id         INTEGER NOT NULL REFERENCES hypothesis_registry(id),
    model_version_id      INTEGER REFERENCES model_registry(id),
    vintage_policy        TEXT NOT NULL CHECK (vintage_policy IN (
                              'FIRST_RELEASE', 'LATEST_AS_OF')),
    era_results           JSONB NOT NULL,
    full_period_metrics   JSONB NOT NULL,
    baseline_comparison   JSONB NOT NULL,
    simplicity_comparison JSONB NOT NULL,
    walk_forward_splits   INTEGER NOT NULL,
    cost_assumption_bps   DOUBLE PRECISION NOT NULL,
    overall_verdict       TEXT NOT NULL CHECK (overall_verdict IN (
                              'PASS', 'FAIL', 'CONDITIONAL')),
    gate_detail           JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_validation_results_hypothesis
    ON validation_results (hypothesis_id);
CREATE INDEX IF NOT EXISTS idx_validation_results_verdict
    ON validation_results (overall_verdict);
CREATE INDEX IF NOT EXISTS idx_validation_results_timestamp
    ON validation_results (run_timestamp DESC);

-- ============================================================
-- TABLE: decision_journal
-- Append-only log of every decision the system makes.
-- ============================================================
CREATE TABLE IF NOT EXISTS decision_journal (
    id                      BIGSERIAL PRIMARY KEY,
    decision_timestamp      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    model_version_id        INTEGER NOT NULL REFERENCES model_registry(id),
    inferred_state          TEXT NOT NULL,
    state_confidence        DOUBLE PRECISION NOT NULL CHECK (
                                state_confidence BETWEEN 0 AND 1),
    transition_probability  DOUBLE PRECISION NOT NULL CHECK (
                                transition_probability BETWEEN 0 AND 1),
    contradiction_flags     JSONB NOT NULL DEFAULT '{}',
    grid_recommendation     TEXT NOT NULL,
    baseline_recommendation TEXT NOT NULL,
    action_taken            TEXT NOT NULL,
    counterfactual          TEXT NOT NULL,
    operator_confidence     TEXT NOT NULL CHECK (operator_confidence IN (
                                'LOW', 'MEDIUM', 'HIGH')),
    outcome_value           DOUBLE PRECISION,
    outcome_recorded_at     TIMESTAMPTZ,
    verdict                 TEXT CHECK (verdict IN (
                                'HELPED', 'HARMED', 'NEUTRAL', 'INSUFFICIENT_DATA')),
    annotation              TEXT
);

CREATE INDEX IF NOT EXISTS idx_decision_journal_timestamp
    ON decision_journal (decision_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_decision_journal_model
    ON decision_journal (model_version_id);
CREATE INDEX IF NOT EXISTS idx_decision_journal_verdict
    ON decision_journal (verdict);
CREATE INDEX IF NOT EXISTS idx_decision_journal_state
    ON decision_journal (inferred_state);
CREATE INDEX IF NOT EXISTS idx_decision_journal_confidence
    ON decision_journal (operator_confidence);

-- Composite index for filtered + sorted queries (journal pagination)
CREATE INDEX IF NOT EXISTS idx_decision_journal_verdict_ts
    ON decision_journal (verdict, decision_timestamp DESC);

-- Partial index for outcome statistics queries
CREATE INDEX IF NOT EXISTS idx_decision_journal_outcome_recorded
    ON decision_journal (outcome_recorded_at)
    WHERE outcome_recorded_at IS NOT NULL;

-- Validation results lookup by model (used in models router)
CREATE INDEX IF NOT EXISTS idx_validation_results_model_ts
    ON validation_results (model_version_id, run_timestamp DESC);

-- Partial index for conflict reporting (used in resolver conflict report)
CREATE INDEX IF NOT EXISTS idx_resolved_series_conflict
    ON resolved_series (feature_id, obs_date)
    WHERE conflict_flag = TRUE;

-- ============================================================
-- TRIGGER: enforce_journal_immutability
-- Only allow updates to annotation, outcome_value,
-- outcome_recorded_at, and verdict columns.
-- ============================================================
CREATE OR REPLACE FUNCTION enforce_journal_immutability()
RETURNS TRIGGER AS $$
BEGIN
    -- Check each immutable column; raise if any changed
    IF OLD.decision_timestamp IS DISTINCT FROM NEW.decision_timestamp THEN
        RAISE EXCEPTION 'decision_journal is append-only: cannot modify decision_timestamp';
    END IF;
    IF OLD.model_version_id IS DISTINCT FROM NEW.model_version_id THEN
        RAISE EXCEPTION 'decision_journal is append-only: cannot modify model_version_id';
    END IF;
    IF OLD.inferred_state IS DISTINCT FROM NEW.inferred_state THEN
        RAISE EXCEPTION 'decision_journal is append-only: cannot modify inferred_state';
    END IF;
    IF OLD.state_confidence IS DISTINCT FROM NEW.state_confidence THEN
        RAISE EXCEPTION 'decision_journal is append-only: cannot modify state_confidence';
    END IF;
    IF OLD.transition_probability IS DISTINCT FROM NEW.transition_probability THEN
        RAISE EXCEPTION 'decision_journal is append-only: cannot modify transition_probability';
    END IF;
    IF OLD.contradiction_flags IS DISTINCT FROM NEW.contradiction_flags THEN
        RAISE EXCEPTION 'decision_journal is append-only: cannot modify contradiction_flags';
    END IF;
    IF OLD.grid_recommendation IS DISTINCT FROM NEW.grid_recommendation THEN
        RAISE EXCEPTION 'decision_journal is append-only: cannot modify grid_recommendation';
    END IF;
    IF OLD.baseline_recommendation IS DISTINCT FROM NEW.baseline_recommendation THEN
        RAISE EXCEPTION 'decision_journal is append-only: cannot modify baseline_recommendation';
    END IF;
    IF OLD.action_taken IS DISTINCT FROM NEW.action_taken THEN
        RAISE EXCEPTION 'decision_journal is append-only: cannot modify action_taken';
    END IF;
    IF OLD.counterfactual IS DISTINCT FROM NEW.counterfactual THEN
        RAISE EXCEPTION 'decision_journal is append-only: cannot modify counterfactual';
    END IF;
    IF OLD.operator_confidence IS DISTINCT FROM NEW.operator_confidence THEN
        RAISE EXCEPTION 'decision_journal is append-only: cannot modify operator_confidence';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_journal_immutability ON decision_journal;
CREATE TRIGGER trg_journal_immutability
    BEFORE UPDATE ON decision_journal
    FOR EACH ROW
    EXECUTE FUNCTION enforce_journal_immutability();

-- ============================================================
-- SEED DATA: source_catalog
-- ============================================================
INSERT INTO source_catalog (name, base_url, cost_tier, latency_class, pit_available, revision_behavior, trust_score, priority_rank, active)
VALUES
    ('FRED',           'https://api.stlouisfed.org/fred',           'FREE', 'EOD',      TRUE,  'RARE',   'HIGH', 1, TRUE),
    ('yfinance',       'https://query1.finance.yahoo.com',          'FREE', 'EOD',      FALSE, 'NEVER',  'HIGH', 2, TRUE),
    ('BLS',            'https://api.bls.gov/publicAPI/v2',          'FREE', 'MONTHLY',  TRUE,  'RARE',   'HIGH', 3, TRUE),
    ('Census',         'https://api.census.gov/data',               'FREE', 'MONTHLY',  TRUE,  'RARE',   'HIGH', 4, TRUE),
    ('CBOE',           'https://www.cboe.com/tradable_products/vix','FREE', 'EOD',      FALSE, 'NEVER',  'HIGH', 5, TRUE),
    ('AlphaVantage',   'https://www.alphavantage.co/query',         'FREE', 'EOD',      FALSE, 'NEVER',  'MED',  6, TRUE),
    ('UnusualWhales',  'https://api.unusualwhales.com',             'FREE', 'EOD',      FALSE, 'NEVER',  'MED',  7, FALSE),
    ('Reddit',         'https://oauth.reddit.com',                  'FREE', 'REALTIME', FALSE, 'NEVER',  'LOW',  8, FALSE),
    ('DexScreener',    'https://api.dexscreener.com',               'FREE', 'EOD',      FALSE, 'NEVER',  'MED',  20, TRUE),
    ('PumpFun',        'https://frontend-api-v3.pump.fun',          'FREE', 'REALTIME', FALSE, 'NEVER',  'LOW',  21, TRUE)
ON CONFLICT (name) DO NOTHING;

-- ============================================================
-- SEED DATA: feature_registry
-- ============================================================
INSERT INTO feature_registry (name, family, description, transformation, transformation_version, lag_days, normalization, missing_data_policy, eligible_from_date, model_eligible)
VALUES
    ('yld_curve_2s10s',       'rates',     'US Treasury 2s10s yield spread',                   'T10Y2Y - T2Y (direct from FRED series T10Y2Y)',              1, 0,  'ZSCORE', 'FORWARD_FILL', '1990-01-01', TRUE),
    ('yld_curve_3m10y',       'rates',     'US Treasury 3m10y yield spread',                   'T10Y3M (direct from FRED)',                                  1, 0,  'ZSCORE', 'FORWARD_FILL', '1990-01-01', TRUE),
    ('fed_funds_rate',        'rates',     'Effective federal funds rate',                     'DFF (direct from FRED)',                                     1, 0,  'RAW',    'FORWARD_FILL', '1990-01-01', TRUE),
    ('fed_funds_3m_chg',      'rates',     '3-month change in fed funds rate',                 'DFF rolling 63-day difference',                              1, 63, 'ZSCORE', 'FORWARD_FILL', '1990-01-01', TRUE),
    ('hy_spread_proxy',       'credit',    'HY credit spread via HYG/LQD ratio',               'log(HYG_yield / LQD_yield) normalized',                      1, 0,  'ZSCORE', 'FORWARD_FILL', '2003-01-01', TRUE),
    ('ig_spread_proxy',       'credit',    'IG credit spread via LQD OAS proxy',                'LQD vs IEF yield differential',                              1, 0,  'ZSCORE', 'FORWARD_FILL', '2003-01-01', TRUE),
    ('hy_spread_3m_chg',      'credit',    '3-month change in HY spread proxy',                'hy_spread_proxy rolling 63-day difference',                  1, 63, 'ZSCORE', 'FORWARD_FILL', '2003-01-01', TRUE),
    ('sp500_pct_above_200ma', 'breadth',   'Pct of S&P 500 stocks above 200-day MA',          'Derived from individual constituent data or proxy ETF',      1, 0,  'RAW',    'FORWARD_FILL', '1999-01-01', FALSE),
    ('sp500_adline',          'breadth',   'S&P 500 advance-decline line cumulative',           'Cumulative sum of (advances - declines)',                    1, 0,  'ZSCORE', 'FORWARD_FILL', '1990-01-01', TRUE),
    ('sp500_adline_slope',    'breadth',   '20-day slope of advance-decline line',              'Linear regression slope of adline over 20 days',            1, 20, 'ZSCORE', 'FORWARD_FILL', '1990-01-01', TRUE),
    ('vix_spot',              'vol',       'CBOE VIX spot level',                              'VIXCLS from FRED',                                          1, 0,  'RAW',    'FORWARD_FILL', '1990-01-01', TRUE),
    ('vix_3m_ratio',          'vol',       'VIX spot / VIX3M ratio (term structure slope)',     'VIX / VIX3M: > 1 = backwardation / stress',                 1, 0,  'ZSCORE', 'FORWARD_FILL', '2007-01-01', TRUE),
    ('vix_1m_chg',            'vol',       '1-month change in VIX',                            'VIX rolling 21-day difference',                              1, 21, 'ZSCORE', 'FORWARD_FILL', '1990-01-01', TRUE),
    ('dxy_index',             'fx',        'US Dollar Index level',                            'DX-Y.NYB via yfinance',                                     1, 0,  'ZSCORE', 'FORWARD_FILL', '1990-01-01', TRUE),
    ('dxy_3m_chg',            'fx',        '3-month change in DXY',                            'DXY rolling 63-day pct change',                              1, 63, 'ZSCORE', 'FORWARD_FILL', '1990-01-01', TRUE),
    ('copper_gold_ratio',     'commodity', 'Copper / Gold price ratio',                        'HG=F / GC=F daily close via yfinance',                      1, 0,  'ZSCORE', 'FORWARD_FILL', '1990-01-01', TRUE),
    ('copper_gold_slope',     'commodity', '3-month slope of copper/gold ratio',                'Linear regression slope over 63 days',                       1, 63, 'ZSCORE', 'FORWARD_FILL', '1990-01-01', TRUE),
    ('sp500_mom_12_1',        'breadth',   'S&P 500 12-month minus 1-month momentum',          '(Close[t-21] / Close[t-252]) - 1',                          1, 21, 'ZSCORE', 'FORWARD_FILL', '1990-01-01', TRUE),
    ('sp500_mom_3m',          'breadth',   'S&P 500 3-month price momentum',                   '(Close[t] / Close[t-63]) - 1',                              1, 0,  'ZSCORE', 'FORWARD_FILL', '1990-01-01', TRUE),
    ('ism_pmi_mfg',           'macro',     'ISM Manufacturing PMI',                            'MANEMP proxy or scraped ISM data',                          1, 0,  'RAW',    'FORWARD_FILL', '1990-01-01', TRUE),
    ('ism_pmi_new_orders',    'macro',     'ISM Manufacturing New Orders sub-index',            'Leading component of PMI',                                  1, 0,  'RAW',    'FORWARD_FILL', '1990-01-01', TRUE),
    ('conf_board_lei',        'macro',     'Conference Board Leading Economic Index',            'USSLIND from FRED',                                         1, 0,  'ZSCORE', 'FORWARD_FILL', '1990-01-01', TRUE),
    ('conf_board_lei_slope',  'macro',     '3-month slope of LEI',                              '63-day linear regression slope',                             1, 63, 'ZSCORE', 'FORWARD_FILL', '1990-01-01', TRUE),
    ('cpi_yoy',               'macro',     'CPI year-over-year change',                        'CPIAUCSL 12-month pct change from FRED',                    1, 0,  'ZSCORE', 'FORWARD_FILL', '1990-01-01', TRUE),
    ('real_ffr',              'rates',     'Real federal funds rate',                           'DFF minus CPI_YOY',                                         1, 0,  'ZSCORE', 'FORWARD_FILL', '1990-01-01', TRUE),
    -- Crypto-native signals (DexScreener + Pump.fun)
    ('dex_sol_volume_24h',    'crypto',    'Total 24h USD volume across top Solana DEX pairs',  'Aggregate sum from DexScreener API',                         1, 0,  'ZSCORE', 'FORWARD_FILL', '2024-01-01', TRUE),
    ('dex_sol_liquidity',     'crypto',    'Total USD liquidity across top Solana DEX pairs',   'Aggregate sum of USD liquidity from DexScreener',            1, 0,  'ZSCORE', 'FORWARD_FILL', '2024-01-01', TRUE),
    ('dex_sol_buy_sell_ratio','crypto',    'Solana DEX 24h buy/sell transaction ratio',          'total_buys / total_sells from DexScreener',                  1, 0,  'RAW',    'FORWARD_FILL', '2024-01-01', TRUE),
    ('dex_sol_momentum_24h',  'crypto',    'Avg 24h price change across top Solana DEX pairs',  'Mean of h24 priceChange from DexScreener',                   1, 0,  'ZSCORE', 'FORWARD_FILL', '2024-01-01', TRUE),
    ('dex_sol_txn_count_24h', 'crypto',    'Total 24h transaction count on Solana DEXs',        'Sum of buys+sells from DexScreener',                         1, 0,  'ZSCORE', 'FORWARD_FILL', '2024-01-01', TRUE),
    ('dex_sol_boosted_tokens','crypto',    'Count of actively boosted tokens on DexScreener',   'Length of /token-boosts/top/v1 response',                    1, 0,  'RAW',    'FORWARD_FILL', '2024-01-01', TRUE),
    ('pump_new_tokens_count', 'crypto',    'New token launches on Pump.fun (memecoin mania)',    'Count from /coins/latest',                                   1, 0,  'ZSCORE', 'FORWARD_FILL', '2024-01-01', TRUE),
    ('pump_koth_mcap',        'crypto',    'Pump.fun king-of-the-hill market cap (USD)',         'usd_market_cap from /coins/king-of-the-hill',                1, 0,  'ZSCORE', 'FORWARD_FILL', '2024-01-01', TRUE),
    ('pump_graduated_count',  'crypto',    'Pump.fun bonding curve completion count',            'Count from /coins?complete=true',                            1, 0,  'ZSCORE', 'FORWARD_FILL', '2024-01-01', TRUE),
    ('pump_graduated_avg_mcap','crypto',   'Avg market cap of graduated Pump.fun tokens',       'Mean usd_market_cap of graduated tokens',                    1, 0,  'ZSCORE', 'FORWARD_FILL', '2024-01-01', TRUE),
    ('pump_latest_avg_mcap',  'crypto',    'Avg market cap of newest Pump.fun launches',        'Mean usd_market_cap of latest 50 tokens',                    1, 0,  'ZSCORE', 'FORWARD_FILL', '2024-01-01', TRUE)
ON CONFLICT (name) DO NOTHING;

-- ============================================================
-- TABLE: agent_runs
-- Tracks TradingAgents multi-agent deliberation runs.
-- ============================================================
CREATE TABLE IF NOT EXISTS agent_runs (
    id                    BIGSERIAL PRIMARY KEY,
    run_timestamp         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ticker                TEXT NOT NULL,
    as_of_date            DATE NOT NULL,
    grid_regime_state     TEXT,
    grid_confidence       DOUBLE PRECISION,
    analyst_reports       JSONB NOT NULL DEFAULT '{}',
    bull_bear_debate      JSONB NOT NULL DEFAULT '{}',
    risk_assessment       JSONB NOT NULL DEFAULT '{}',
    final_decision        TEXT NOT NULL,
    decision_reasoning    TEXT NOT NULL,
    decision_journal_id   BIGINT REFERENCES decision_journal(id),
    llm_provider          TEXT NOT NULL,
    llm_model             TEXT NOT NULL,
    duration_seconds      DOUBLE PRECISION,
    error                 TEXT
);

CREATE INDEX IF NOT EXISTS idx_agent_runs_timestamp
    ON agent_runs (run_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_agent_runs_ticker
    ON agent_runs (ticker);
CREATE INDEX IF NOT EXISTS idx_agent_runs_journal
    ON agent_runs (decision_journal_id);
