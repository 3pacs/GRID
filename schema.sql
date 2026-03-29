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
                              'rates', 'credit', 'equity', 'vol', 'fx',
                              'commodity', 'sentiment', 'macro', 'crypto',
                              'alternative', 'flows', 'systemic', 'trade',
                              'breadth', 'earnings')),
    subfamily             TEXT,
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
CREATE INDEX IF NOT EXISTS idx_decision_journal_outcome_recorded
    ON decision_journal (outcome_recorded_at);

-- Partial index for conflict reporting
CREATE INDEX IF NOT EXISTS idx_resolved_series_conflict_detail
    ON resolved_series (feature_id, obs_date) WHERE conflict_flag = TRUE;

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
-- TRIGGER: prevent_journal_delete
-- Decision journal is append-only: no deletions allowed.
-- ============================================================
CREATE OR REPLACE FUNCTION prevent_journal_delete()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'decision_journal is append-only: DELETE is not permitted. '
                    'Row id=% cannot be deleted.', OLD.id;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_journal_no_delete ON decision_journal;
CREATE TRIGGER trg_journal_no_delete
    BEFORE DELETE ON decision_journal
    FOR EACH ROW
    EXECUTE FUNCTION prevent_journal_delete();

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
    ('pump_latest_avg_mcap',  'crypto',    'Avg market cap of newest Pump.fun launches',        'Mean usd_market_cap of latest 50 tokens',                    1, 0,  'ZSCORE', 'FORWARD_FILL', '2024-01-01', TRUE),
    -- Google Trends sentiment (needs validation before model use)
    ('gt_recession_interest', 'sentiment', 'Google Trends search interest for "recession"',     'pytrends weekly data interpolated to daily (0-100)',          1, 0, 'RAW',    'FORWARD_FILL', '2004-01-01', FALSE),
    ('gt_unemployment_interest','sentiment','Google Trends search interest for "unemployment"', 'pytrends weekly interpolated to daily',                       1, 0, 'RAW',    'FORWARD_FILL', '2004-01-01', FALSE),
    ('gt_inflation_interest', 'sentiment', 'Google Trends search interest for "inflation"',     'pytrends weekly interpolated to daily',                       1, 0, 'RAW',    'FORWARD_FILL', '2004-01-01', FALSE),
    ('gt_stock_market_crash', 'sentiment', 'Google Trends search interest for "stock market crash"','pytrends weekly interpolated to daily',                    1, 0, 'RAW',    'FORWARD_FILL', '2004-01-01', FALSE),
    ('gt_housing_bubble',     'sentiment', 'Google Trends search interest for "housing bubble"','pytrends weekly interpolated to daily',                       1, 0, 'RAW',    'FORWARD_FILL', '2004-01-01', FALSE),
    ('gt_fed_rate_cut',       'sentiment', 'Google Trends search interest for "rate cut"',      'pytrends weekly interpolated to daily',                       1, 0, 'RAW',    'FORWARD_FILL', '2004-01-01', FALSE),
    ('gt_economic_composite', 'sentiment', 'Average of all Google Trends economic queries',     'Mean of gt_* keywords (fear/anxiety index)',                  1, 0, 'ZSCORE', 'FORWARD_FILL', '2004-01-01', FALSE),
    -- CBOE volatility indices
    ('skew_index',            'vol',       'CBOE SKEW index (tail risk pricing)',                'CBOE CSV download, >130 = elevated tail risk',               1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', TRUE),
    ('vvix',                  'vol',       'VIX of VIX (vol-of-vol)',                           'CBOE CSV download',                                          1, 0, 'ZSCORE', 'FORWARD_FILL', '2007-01-01', TRUE),
    ('put_call_ratio',        'vol',       'CBOE total exchange PUT/CALL volume ratio',         'CBOE CSV download',                                          1, 0, 'RAW',    'FORWARD_FILL', '2006-01-01', TRUE),
    ('correlation_index',     'vol',       'CBOE implied correlation index (ICJ)',               'CBOE CSV download',                                          1, 0, 'ZSCORE', 'FORWARD_FILL', '2007-01-01', TRUE),
    -- Federal Reserve communications (needs validation before model use)
    ('fomc_hawkish_score',    'sentiment', 'NLP-derived hawkishness score from Fed speeches',   'Keyword scoring: (hawkish-dovish)/total, range -1 to 1',     1, 0, 'RAW',    'FORWARD_FILL', '2015-01-01', FALSE),
    ('fomc_days_since_meeting','sentiment','Trading days since last FOMC decision',             'Computed from FOMC calendar',                                 1, 0, 'RAW',    'FORWARD_FILL', '2015-01-01', FALSE),
    ('fomc_days_to_meeting',  'sentiment', 'Trading days until next FOMC decision',             'Computed from FOMC calendar',                                 1, 0, 'RAW',    'FORWARD_FILL', '2015-01-01', FALSE),
    ('fed_speech_frequency',  'sentiment', 'Count of Fed speeches in last 7 days',              'Rolling 7-day count from Fed JSON feed',                     1, 0, 'RAW',    'FORWARD_FILL', '2015-01-01', FALSE),
    ('fed_tone_7d_avg',       'sentiment', 'Rolling 7-day average hawkishness score',           '7-day rolling mean of fomc_hawkish_score',                   1, 0, 'RAW',    'FORWARD_FILL', '2015-01-01', FALSE),
    -- Repo and money market stress indicators
    ('sofr_rate',             'rates',     'Secured Overnight Financing Rate',                  'SOFR from FRED',                                              1, 0, 'RAW',    'FORWARD_FILL', '2018-04-01', TRUE),
    ('sofr_spread_to_ffr',    'rates',     'SOFR minus effective federal funds rate',            'SOFR - DFF, positive = funding stress',                       1, 0, 'ZSCORE', 'FORWARD_FILL', '2018-04-01', TRUE),
    ('reverse_repo_usage',    'rates',     'Fed ON RRP facility usage (billions)',               'RRPONTSYD from FRED',                                         1, 0, 'ZSCORE', 'FORWARD_FILL', '2013-09-01', TRUE),
    ('rrp_as_pct_of_peak',    'rates',     'ON RRP usage as percentage of historical peak',     'RRPONTSYD / max(RRPONTSYD)',                                   1, 0, 'RAW',    'FORWARD_FILL', '2013-09-01', TRUE),
    ('treasury_bill_spread',  'rates',     '3M Treasury bill minus SOFR spread',                'DTB3 - SOFR',                                                 1, 0, 'ZSCORE', 'FORWARD_FILL', '2018-04-01', TRUE),
    -- Full yield curve (high signal — model eligible)
    ('yc_1y',                 'rates',     '1-year Treasury constant maturity yield',           'DGS1 from FRED',                                              1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', TRUE),
    ('yc_5y',                 'rates',     '5-year Treasury constant maturity yield',           'DGS5 from FRED',                                              1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', TRUE),
    ('yc_30y',                'rates',     '30-year Treasury constant maturity yield',          'DGS30 from FRED',                                             1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', TRUE),
    ('yc_5s30s_spread',       'rates',     '5s30s yield curve slope (long end steepness)',      'DGS30 - DGS5',                                                1, 0, 'ZSCORE', 'FORWARD_FILL', '1990-01-01', TRUE),
    ('yc_butterfly_2_5_10',   'rates',     'Yield curve butterfly: 2*5Y - (2Y + 10Y)',          'Curvature measure from DGS2, DGS5, DGS10',                   1, 0, 'ZSCORE', 'FORWARD_FILL', '1990-01-01', TRUE),
    ('yc_real_10y',           'rates',     '10-year TIPS yield (real interest rate)',            'DFII10 from FRED',                                            1, 0, 'RAW',    'FORWARD_FILL', '2003-01-01', TRUE),
    ('yc_breakeven_10y',      'rates',     '10Y breakeven inflation rate (market expectations)','T10YIE from FRED',                                            1, 0, 'RAW',    'FORWARD_FILL', '2003-01-01', TRUE),
    ('yc_term_premium',       'rates',     'ACM 10-year term premium estimate',                 'THREEFYTP10 from FRED',                                       1, 0, 'ZSCORE', 'FORWARD_FILL', '1961-06-01', TRUE),
    -- Celestial / esoteric features (alternative family, model_eligible=FALSE until orthogonality audit)
    -- Lunar ephemeris
    ('lunar_phase',              'alternative', 'Lunar phase fraction (0=new, 0.5=full)',                 'Synodic month fraction from J2000 ref',               1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', FALSE),
    ('lunar_illumination',       'alternative', 'Moon illumination percentage (0-100)',                    'Cosine transform of lunar phase',                     1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', FALSE),
    ('days_to_new_moon',         'alternative', 'Calendar days until next new moon',                      'Phase distance to 0.0 * synodic month',               1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', FALSE),
    ('days_to_full_moon',        'alternative', 'Calendar days until next full moon',                     'Phase distance to 0.5 * synodic month',               1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', FALSE),
    ('lunar_eclipse_proximity',  'alternative', 'Days to nearest lunar eclipse',                          'Min abs distance to known eclipse dates',             1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', FALSE),
    ('solar_eclipse_proximity',  'alternative', 'Days to nearest solar eclipse',                          'Min abs distance to known eclipse dates',             1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', FALSE),
    -- Planetary ephemeris
    ('mercury_retrograde',       'alternative', 'Mercury retrograde indicator (1=retro, 0=direct)',       'Precomputed table + synodic fallback',                1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', FALSE),
    ('jupiter_saturn_angle',     'alternative', 'Jupiter-Saturn angular separation (0-180 deg)',          'Geocentric longitude difference',                     1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', FALSE),
    ('mars_volatility_index',    'alternative', 'Mars hard-aspect composite (Gann theory, 0-1)',          'Closeness to conjunction/square/opposition',          1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', FALSE),
    ('planetary_stress_index',   'alternative', 'Count of active hard aspects among planets',             'Pairwise conjunction/square/opposition within 8 deg', 1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', FALSE),
    ('venus_cycle_phase',        'alternative', 'Venus synodic cycle position (0-1)',                     'Days from ref inferior conjunction / 583.9',          1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', FALSE),
    -- Vedic (Jyotish) astrology
    ('nakshatra_index',          'alternative', 'Lunar mansion index (0-26, 27 nakshatras)',              'Moon sidereal longitude / 13.333 deg',                1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', FALSE),
    ('nakshatra_quality',        'alternative', 'Nakshatra quality (0=fixed, 1=movable, 2=dual)',         'Traditional quality mapping per nakshatra',           1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', FALSE),
    ('tithi',                    'alternative', 'Vedic lunar day (1-30, waxing/waning)',                  'Moon-Sun angular distance / 12 deg',                  1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', FALSE),
    ('rahu_ketu_axis',           'alternative', 'Rahu (North Node) sidereal longitude (deg)',             'J2000 ref + retrograde regression rate',              1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', FALSE),
    ('dasha_cycle_phase',        'alternative', 'Vimshottari dasha major period indicator (0-1)',         'Moon nakshatra mapped to 120-year dasha cycle',       1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', FALSE),
    -- Chinese calendar / Feng Shui
    ('chinese_zodiac_year',      'alternative', 'Chinese zodiac animal index (0-11, Rat to Pig)',         '(year - 4) mod 12',                                  1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', FALSE),
    ('chinese_element',          'alternative', 'Chinese element index (0-4, Wood to Water)',             '((year - 4) mod 10) / 2',                             1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', FALSE),
    ('chinese_yin_yang',         'alternative', 'Yin/Yang indicator (0=yang, 1=yin)',                     '(year - 4) mod 2',                                   1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', FALSE),
    ('feng_shui_flying_star',    'alternative', 'Feng Shui Flying Star period number (1-9)',              '20-year period from 180-year grand cycle',            1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', FALSE),
    ('chinese_lunar_month',      'alternative', 'Chinese lunar month (1-12)',                             'Synodic month count from CNY reference',              1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', FALSE),
    ('iching_hexagram_of_day',   'alternative', 'Daily I Ching hexagram (1-64)',                          'Date numerology trigram derivation',                  1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', FALSE),
    -- Solar activity (scientifically backed — geomagnetic/market correlation)
    ('sunspot_number',           'alternative', 'Daily sunspot count from NOAA SWPC',                    'Observed solar cycle indices',                        1, 0, 'ZSCORE', 'FORWARD_FILL', '1990-01-01', FALSE),
    ('solar_flux_10_7cm',        'alternative', 'F10.7 solar radio flux (SFU)',                           'NOAA SWPC observed solar indices',                    1, 0, 'ZSCORE', 'FORWARD_FILL', '1990-01-01', FALSE),
    ('geomagnetic_kp_index',     'alternative', 'Planetary Kp geomagnetic index (0-9)',                   'NOAA SWPC planetary K-index',                         1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', FALSE),
    ('geomagnetic_ap_index',     'alternative', 'Planetary Ap geomagnetic index',                         'Derived from Kp via standard conversion',             1, 0, 'ZSCORE', 'FORWARD_FILL', '1990-01-01', FALSE),
    ('solar_wind_speed',         'alternative', 'Solar wind speed (km/s)',                                'NOAA SWPC solar wind plasma data',                    1, 0, 'ZSCORE', 'FORWARD_FILL', '1990-01-01', FALSE),
    ('solar_storm_probability',  'alternative', 'Geomagnetic storm probability (0-100)',                  'Derived from Kp threshold mapping',                   1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', FALSE),
    ('solar_cycle_phase',        'alternative', 'Position in ~11-year solar cycle (0-1)',                 'Days from Cycle 25 start / cycle length',             1, 0, 'RAW',    'FORWARD_FILL', '1990-01-01', FALSE),

    -- ── Unmapped FRED macro series ──────────────────────────────────────────
    ('nonfarm_payrolls',         'macro',     'Total nonfarm payrolls (thousands)',                    'PAYEMS from FRED',                                        1, 0,  'ZSCORE', 'FORWARD_FILL', '1939-01-01', TRUE),
    ('unemployment_rate',        'macro',     'US civilian unemployment rate (%)',                     'UNRATE from FRED',                                        1, 0,  'RAW',    'FORWARD_FILL', '1948-01-01', TRUE),
    ('housing_starts',           'macro',     'New privately owned housing units started (thousands)', 'HOUST from FRED',                                         1, 0,  'ZSCORE', 'FORWARD_FILL', '1959-01-01', TRUE),
    ('real_disp_income',         'macro',     'Real disposable personal income (billions 2017$)',      'DSPIC96 from FRED',                                       1, 0,  'ZSCORE', 'FORWARD_FILL', '1959-01-01', TRUE),
    ('m2_money_supply',          'macro',     'M2 money stock (billions)',                             'M2SL from FRED',                                          1, 0,  'ZSCORE', 'FORWARD_FILL', '1959-01-01', TRUE),
    ('fed_balance_sheet',        'macro',     'Federal Reserve total assets (millions)',                'WALCL from FRED',                                         1, 0,  'ZSCORE', 'FORWARD_FILL', '2002-12-01', TRUE),
    ('hy_oas_spread',            'credit',    'ICE BofA US High Yield OAS (bps)',                     'BAMLH0A0HYM2 from FRED',                                  1, 0,  'ZSCORE', 'FORWARD_FILL', '1996-12-31', TRUE),
    ('ig_oas_spread',            'credit',    'ICE BofA US Corporate OAS (bps)',                       'BAMLC0A0CM from FRED',                                    1, 0,  'ZSCORE', 'FORWARD_FILL', '1996-12-31', TRUE),
    ('ted_spread',               'rates',     'TED spread (3M LIBOR minus 3M T-bill)',                'TEDRATE from FRED',                                       1, 0,  'ZSCORE', 'FORWARD_FILL', '1986-01-02', TRUE),
    ('breakeven_5y',             'rates',     '5-year breakeven inflation rate',                       'T5YIE from FRED',                                         1, 0,  'RAW',    'FORWARD_FILL', '2003-01-02', TRUE),
    ('umich_sentiment',          'sentiment', 'University of Michigan consumer sentiment index',       'UMCSENT from FRED',                                       1, 0,  'ZSCORE', 'FORWARD_FILL', '1952-11-01', TRUE),
    ('initial_claims',           'macro',     'Initial jobless claims weekly (thousands)',              'ICSA from FRED',                                          1, 0,  'ZSCORE', 'FORWARD_FILL', '1967-01-07', TRUE),
    ('retail_sales_nsa',         'macro',     'Retail sales not seasonally adjusted (millions)',        'RETAILSMNSA from FRED',                                   1, 0,  'ZSCORE', 'FORWARD_FILL', '1992-01-01', TRUE),
    ('industrial_production',    'macro',     'Industrial production index (2017=100)',                'INDPRO from FRED',                                        1, 0,  'ZSCORE', 'FORWARD_FILL', '1919-01-01', TRUE),
    ('retail_sales_sa',          'macro',     'Advance retail sales SA (millions)',                    'RSAFS from FRED',                                         1, 0,  'ZSCORE', 'FORWARD_FILL', '1992-01-01', TRUE),
    ('trade_balance',            'macro',     'US trade balance goods and services (millions)',         'BOPGTB from FRED',                                        1, 0,  'ZSCORE', 'FORWARD_FILL', '1992-01-01', TRUE),
    ('treasury_general_acct',    'macro',     'US Treasury general account balance (billions)',         'WTREGEN from FRED',                                       1, 0,  'ZSCORE', 'FORWARD_FILL', '2002-01-01', TRUE),
    ('building_permits',         'macro',     'New private housing units authorized (thousands)',       'PERMIT from FRED',                                        1, 0,  'ZSCORE', 'FORWARD_FILL', '1960-01-01', TRUE),
    ('continued_claims',         'macro',     'Continued claims insured unemployment (thousands)',      'CCSA from FRED',                                          1, 0,  'ZSCORE', 'FORWARD_FILL', '1967-01-07', TRUE),
    ('pce_deflator',             'macro',     'Personal consumption expenditures price index',          'PCEPI from FRED',                                         1, 0,  'ZSCORE', 'FORWARD_FILL', '1959-01-01', TRUE),
    ('core_pce',                 'macro',     'Core PCE price index (ex food & energy)',                'PCEPILFE from FRED',                                      1, 0,  'ZSCORE', 'FORWARD_FILL', '1959-02-01', TRUE),
    ('capacity_utilization',     'macro',     'Total industry capacity utilization rate (%)',           'TCU from FRED',                                           1, 0,  'RAW',    'FORWARD_FILL', '1967-01-01', TRUE),
    ('manufacturing_employment', 'macro',     'Manufacturing employment level (thousands)',             'MANEMP from FRED',                                        1, 0,  'ZSCORE', 'FORWARD_FILL', '1939-01-01', TRUE),

    -- ── FRED FX series ──────────────────────────────────────────────────────
    ('eurusd_fred',              'fx',        'EUR/USD exchange rate from FRED',                       'DEXUSEU from FRED',                                       1, 0,  'RAW',    'FORWARD_FILL', '1999-01-04', TRUE),
    ('usdjpy_fred',              'fx',        'USD/JPY exchange rate from FRED',                       'DEXJPUS from FRED',                                       1, 0,  'RAW',    'FORWARD_FILL', '1971-01-04', TRUE),
    ('usdcad_fred',              'fx',        'USD/CAD exchange rate from FRED',                       'DEXCAUS from FRED',                                       1, 0,  'RAW',    'FORWARD_FILL', '1971-01-04', TRUE),
    ('usdchf_fred',              'fx',        'USD/CHF exchange rate from FRED',                       'DEXSZUS from FRED',                                       1, 0,  'RAW',    'FORWARD_FILL', '1971-01-04', TRUE),
    ('gbpusd_fred',              'fx',        'GBP/USD exchange rate from FRED',                       'DEXUSUK from FRED',                                       1, 0,  'RAW',    'FORWARD_FILL', '1971-01-04', TRUE),

    -- ── YFinance FX pairs ───────────────────────────────────────────────────
    ('eurusd',                   'fx',        'EUR/USD spot rate (yfinance)',                           'YF:EURUSD=X:close',                                       1, 0,  'RAW',    'FORWARD_FILL', '2003-12-01', TRUE),
    ('gbpusd',                   'fx',        'GBP/USD spot rate (yfinance)',                           'YF:GBPUSD=X:close',                                       1, 0,  'RAW',    'FORWARD_FILL', '2003-12-01', TRUE),
    ('usdjpy',                   'fx',        'USD/JPY spot rate (yfinance)',                           'YF:USDJPY=X:close',                                       1, 0,  'RAW',    'FORWARD_FILL', '2003-12-01', TRUE),
    ('audusd',                   'fx',        'AUD/USD spot rate (yfinance)',                           'YF:AUDUSD=X:close',                                       1, 0,  'RAW',    'FORWARD_FILL', '2003-12-01', TRUE),
    ('usdchf',                   'fx',        'USD/CHF spot rate (yfinance)',                           'YF:USDCHF=X:close',                                       1, 0,  'RAW',    'FORWARD_FILL', '2003-12-01', TRUE),
    ('usdcad',                   'fx',        'USD/CAD spot rate (yfinance)',                           'YF:USDCAD=X:close',                                       1, 0,  'RAW',    'FORWARD_FILL', '2003-12-01', TRUE),
    ('nzdusd',                   'fx',        'NZD/USD spot rate (yfinance)',                           'YF:NZDUSD=X:close',                                       1, 0,  'RAW',    'FORWARD_FILL', '2003-12-01', TRUE),

    -- ── Systemic stress features (derived from existing data) ───────────────
    ('systemic_stress_composite','systemic',  'Weighted avg of HY spread, VIX, TED spread z-scores',  'DERIVED: 0.4*z(HY) + 0.35*z(VIX) + 0.25*z(TED)',         1, 0,  'ZSCORE', 'FORWARD_FILL', '2005-01-01', TRUE),
    ('systemic_credit_stress',   'systemic',  'HY OAS spread / 2-year rolling median',                'DERIVED: BAMLH0A0HYM2 / rolling_median(504)',             1, 0,  'ZSCORE', 'FORWARD_FILL', '2005-01-01', TRUE),
    ('systemic_funding_stress',  'systemic',  'SOFR-FFR spread + inverted RRP trend',                 'DERIVED: z(SOFR-FFR) + z(-dRRP)',                         1, 0,  'ZSCORE', 'FORWARD_FILL', '2005-01-01', TRUE)

ON CONFLICT (name) DO NOTHING;

-- ============================================================
-- TABLE: feature_importance_log
-- Tracks feature importance scores across model versions and time.
-- ============================================================
CREATE TABLE IF NOT EXISTS feature_importance_log (
    id BIGSERIAL PRIMARY KEY,
    model_version_id INTEGER REFERENCES model_registry(id),
    feature_id INTEGER NOT NULL REFERENCES feature_registry(id),
    importance_score DOUBLE PRECISION NOT NULL,
    importance_method TEXT NOT NULL DEFAULT 'permutation',
    as_of_date DATE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_feature_importance_feature ON feature_importance_log (feature_id, as_of_date DESC);
CREATE INDEX IF NOT EXISTS idx_feature_importance_model ON feature_importance_log (model_version_id);

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

-- ============================================================
-- TABLE: options_snapshots
-- Raw options chain snapshots per ticker/expiry/strike.
-- ============================================================
CREATE TABLE IF NOT EXISTS options_snapshots (
    id              BIGSERIAL PRIMARY KEY,
    ticker          TEXT NOT NULL,
    snap_date       DATE NOT NULL,
    expiry          DATE NOT NULL,
    opt_type        TEXT NOT NULL CHECK (opt_type IN ('call', 'put')),
    strike          DOUBLE PRECISION NOT NULL,
    last_price      DOUBLE PRECISION,
    bid             DOUBLE PRECISION,
    ask             DOUBLE PRECISION,
    volume          INTEGER,
    open_interest   INTEGER,
    implied_vol     DOUBLE PRECISION,
    in_the_money    BOOLEAN,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (ticker, snap_date, expiry, opt_type, strike)
);

CREATE INDEX IF NOT EXISTS idx_opts_snap_ticker_date
    ON options_snapshots (ticker, snap_date);

-- ============================================================
-- TABLE: options_daily_signals
-- Computed daily options signals per ticker (PCR, IV skew, etc.)
-- ============================================================
CREATE TABLE IF NOT EXISTS options_daily_signals (
    id                      BIGSERIAL PRIMARY KEY,
    ticker                  TEXT NOT NULL,
    signal_date             DATE NOT NULL,
    put_call_ratio          DOUBLE PRECISION,
    max_pain                DOUBLE PRECISION,
    iv_skew                 DOUBLE PRECISION,
    total_oi                BIGINT,
    total_volume            BIGINT,
    near_expiry             DATE,
    spot_price              DOUBLE PRECISION,
    iv_atm                  DOUBLE PRECISION,
    iv_25d_put              DOUBLE PRECISION,
    iv_25d_call             DOUBLE PRECISION,
    term_structure_slope    DOUBLE PRECISION,
    oi_concentration        DOUBLE PRECISION,
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (ticker, signal_date)
);

CREATE INDEX IF NOT EXISTS idx_opts_signals_ticker_date
    ON options_daily_signals (ticker, signal_date DESC);

-- ============================================================
-- TABLE: options_mispricing_scans
-- Persisted results from the 100x options scanner.
-- ============================================================
CREATE TABLE IF NOT EXISTS options_mispricing_scans (
    id              BIGSERIAL PRIMARY KEY,
    ticker          TEXT NOT NULL,
    scan_date       DATE NOT NULL,
    score           DOUBLE PRECISION NOT NULL,
    payoff_multiple DOUBLE PRECISION NOT NULL,
    direction       TEXT NOT NULL,
    thesis          TEXT NOT NULL,
    signals         JSONB,
    strikes         DOUBLE PRECISION[],
    expiry          DATE,
    spot_price      DOUBLE PRECISION,
    iv_atm          DOUBLE PRECISION,
    confidence      TEXT NOT NULL,
    is_100x         BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (ticker, scan_date, direction)
);

CREATE INDEX IF NOT EXISTS idx_mispricing_score
    ON options_mispricing_scans (score DESC);
CREATE INDEX IF NOT EXISTS idx_mispricing_100x
    ON options_mispricing_scans (is_100x) WHERE is_100x = TRUE;

-- ============================================================
-- TABLE: model_artifacts
-- Trained model serialization metadata and provenance.
-- ============================================================
CREATE TABLE IF NOT EXISTS model_artifacts (
    id                  SERIAL PRIMARY KEY,
    model_id            INTEGER NOT NULL REFERENCES model_registry(id),
    artifact_path       TEXT NOT NULL,
    artifact_hash       TEXT NOT NULL,
    model_type          TEXT NOT NULL CHECK (model_type IN (
                            'xgboost', 'random_forest', 'rule_based', 'ensemble')),
    feature_names       TEXT[] NOT NULL,
    hyperparameters     JSONB NOT NULL DEFAULT '{}',
    training_metrics    JSONB NOT NULL DEFAULT '{}',
    trained_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    training_start_date DATE,
    training_end_date   DATE
);

CREATE INDEX IF NOT EXISTS idx_model_artifacts_model
    ON model_artifacts (model_id);
CREATE INDEX IF NOT EXISTS idx_model_artifacts_type
    ON model_artifacts (model_type);

-- ============================================================
-- TABLE: shadow_scores
-- Tracks SHADOW model predictions alongside PRODUCTION for comparison.
-- ============================================================
CREATE TABLE IF NOT EXISTS shadow_scores (
    id                      BIGSERIAL PRIMARY KEY,
    scored_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    as_of_date              DATE NOT NULL,
    production_model_id     INTEGER NOT NULL REFERENCES model_registry(id),
    shadow_model_id         INTEGER NOT NULL REFERENCES model_registry(id),
    production_state        TEXT NOT NULL,
    production_confidence   DOUBLE PRECISION NOT NULL,
    shadow_state            TEXT NOT NULL,
    shadow_confidence       DOUBLE PRECISION NOT NULL,
    agreement               BOOLEAN NOT NULL,
    feature_vector          JSONB NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_shadow_scores_date
    ON shadow_scores (as_of_date DESC);
CREATE INDEX IF NOT EXISTS idx_shadow_scores_models
    ON shadow_scores (production_model_id, shadow_model_id);

-- Add model_type column to model_registry if not present
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'model_registry' AND column_name = 'model_type'
    ) THEN
        ALTER TABLE model_registry ADD COLUMN model_type TEXT DEFAULT 'rule_based';
    END IF;
END $$;

-- ============================================================
-- TABLE: options_recommendations
-- Logged trade recommendations from the options recommender.
-- ============================================================
CREATE TABLE IF NOT EXISTS options_recommendations (
    id              SERIAL PRIMARY KEY,
    ticker          TEXT NOT NULL,
    direction       TEXT NOT NULL,
    strike          NUMERIC NOT NULL,
    expiry          DATE NOT NULL,
    entry_price     NUMERIC,
    target_price    NUMERIC,
    stop_loss       NUMERIC,
    expected_return NUMERIC,
    kelly_fraction  NUMERIC,
    confidence      NUMERIC,
    thesis          TEXT,
    dealer_context  TEXT,
    sanity_status   JSONB,
    generated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    outcome         TEXT,           -- WIN/LOSS/EXPIRED/OPEN
    actual_return   NUMERIC,
    closed_at       TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_options_rec_ticker
    ON options_recommendations (ticker);
CREATE INDEX IF NOT EXISTS idx_options_rec_generated
    ON options_recommendations (generated_at DESC);
CREATE INDEX IF NOT EXISTS idx_options_rec_expiry_outcome
    ON options_recommendations (expiry) WHERE outcome IS NULL;

-- ============================================================
-- TABLE: scanner_weights
-- Tracks evolving signal weights for the options scanner.
-- Each row is a point-in-time weight snapshot; latest per signal is active.
-- ============================================================
CREATE TABLE IF NOT EXISTS scanner_weights (
    id          SERIAL PRIMARY KEY,
    signal_name TEXT NOT NULL,
    weight      NUMERIC NOT NULL DEFAULT 1.0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reason      TEXT
);

CREATE INDEX IF NOT EXISTS idx_scanner_weights_signal
    ON scanner_weights (signal_name, updated_at DESC);

-- ============================================================
-- TABLE: signal_sources
-- Trust scoring for intelligence edge signals (congressional
-- trades, insider filings, dark pool activity, social, etc.).
-- Tracks signal accuracy over time to build per-source trust.
-- ============================================================
CREATE TABLE IF NOT EXISTS signal_sources (
    id                  SERIAL PRIMARY KEY,
    source_type         TEXT NOT NULL,       -- 'congressional', 'insider', 'darkpool', 'social', etc.
    source_id           TEXT NOT NULL,       -- member name, insider name, account handle
    ticker              TEXT,
    signal_date         DATE NOT NULL,
    signal_type         TEXT NOT NULL,       -- 'BUY', 'SELL', 'CLUSTER_BUY', 'UNUSUAL_VOLUME'
    signal_value        JSONB,              -- details
    outcome             TEXT,               -- filled later: 'CORRECT', 'WRONG', 'PENDING'
    outcome_return      NUMERIC,            -- filled later
    scored_at           TIMESTAMPTZ,
    trust_score         NUMERIC DEFAULT 0.5,
    hit_count           INT DEFAULT 0,
    miss_count          INT DEFAULT 0,
    avg_lead_time_hours NUMERIC,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(source_type, source_id, ticker, signal_date, signal_type)
);

CREATE INDEX IF NOT EXISTS idx_signal_sources_ticker
    ON signal_sources (ticker);
CREATE INDEX IF NOT EXISTS idx_signal_sources_trust
    ON signal_sources (trust_score DESC);
CREATE INDEX IF NOT EXISTS idx_signal_sources_type
    ON signal_sources (source_type, signal_date DESC);

-- ============================================================
-- TABLE: lever_pullers
-- Tracks people and institutions whose actions predict market
-- moves. Populated by intelligence/lever_pullers.py.
-- ============================================================
CREATE TABLE IF NOT EXISTS lever_pullers (
    id                  SERIAL PRIMARY KEY,
    source_type         TEXT NOT NULL,
    source_id           TEXT NOT NULL UNIQUE,
    name                TEXT NOT NULL,
    category            TEXT NOT NULL,
    position            TEXT,
    influence_rank      NUMERIC DEFAULT 0.5,
    motivation_model    TEXT DEFAULT 'unknown',
    trust_score         NUMERIC DEFAULT 0.5,
    avg_lead_time_days  NUMERIC,
    total_signals       INT DEFAULT 0,
    correct_signals     INT DEFAULT 0,
    metadata            JSONB,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_lever_pullers_category
    ON lever_pullers (category);
CREATE INDEX IF NOT EXISTS idx_lever_pullers_influence
    ON lever_pullers (influence_rank DESC);

-- ============================================================
-- TABLE: cross_reference_checks
-- Historical tracking of cross-reference divergence checks
-- comparing official government statistics against physical
-- reality indicators (night lights, shipping, electricity, etc.).
-- ============================================================
CREATE TABLE IF NOT EXISTS cross_reference_checks (
    id                BIGSERIAL PRIMARY KEY,
    name              TEXT NOT NULL,
    category          TEXT NOT NULL,
    official_source   TEXT,
    official_value    DOUBLE PRECISION,
    physical_source   TEXT,
    physical_value    DOUBLE PRECISION,
    divergence_zscore DOUBLE PRECISION,
    assessment        TEXT,
    implication       TEXT,
    confidence        DOUBLE PRECISION,
    checked_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_crossref_category
    ON cross_reference_checks (category, checked_at DESC);
CREATE INDEX IF NOT EXISTS idx_crossref_assessment
    ON cross_reference_checks (assessment, checked_at DESC);

-- ============================================================
-- TABLE: actors
-- Named individuals and entities in the global financial power
-- structure — central bankers, politicians, fund managers,
-- insiders, activists, sovereign wealth funds.
-- ============================================================
CREATE TABLE IF NOT EXISTS actors (
    id                  TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    tier                TEXT NOT NULL,
    category            TEXT NOT NULL,
    title               TEXT,
    net_worth_estimate  NUMERIC,
    aum                 NUMERIC,
    influence_score     NUMERIC DEFAULT 0.5,
    trust_score         NUMERIC DEFAULT 0.5,
    motivation_model    TEXT DEFAULT 'unknown',
    connections         JSONB DEFAULT '[]',
    known_positions     JSONB DEFAULT '[]',
    board_seats         JSONB DEFAULT '[]',
    political_affiliations JSONB DEFAULT '[]',
    data_sources        JSONB DEFAULT '[]',
    credibility         TEXT DEFAULT 'inferred',
    metadata            JSONB DEFAULT '{}',
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_actors_tier
    ON actors (tier);
CREATE INDEX IF NOT EXISTS idx_actors_influence
    ON actors (influence_score DESC);

-- ============================================================
-- TABLE: wealth_flows
-- Tracked capital movements between actors, sectors, and
-- companies with confidence levels and evidence provenance.
-- ============================================================
CREATE TABLE IF NOT EXISTS wealth_flows (
    id              SERIAL PRIMARY KEY,
    from_actor      TEXT REFERENCES actors(id),
    to_entity       TEXT NOT NULL,
    amount_estimate NUMERIC,
    confidence      TEXT DEFAULT 'inferred',
    evidence        JSONB DEFAULT '[]',
    flow_date       DATE,
    implication     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wealth_flows_date
    ON wealth_flows (flow_date DESC);
CREATE INDEX IF NOT EXISTS idx_wealth_flows_actor
    ON wealth_flows (from_actor);

-- ============================================================
-- TABLE: dollar_flows
-- Normalized USD amounts for all signal sources. Converts
-- congressional trades, insider filings, dark pool activity,
-- 13F, ETF flows, whale options, and prediction markets into
-- a single comparable dollar figure.
-- ============================================================
CREATE TABLE IF NOT EXISTS dollar_flows (
    id              SERIAL PRIMARY KEY,
    source_type     TEXT NOT NULL,
    actor_name      TEXT,
    ticker          TEXT,
    amount_usd      NUMERIC NOT NULL,
    direction       TEXT NOT NULL,          -- 'inflow' or 'outflow'
    confidence      TEXT DEFAULT 'estimated',
    evidence        JSONB,
    flow_date       DATE NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dollar_flows_ticker
    ON dollar_flows (ticker, flow_date DESC);
CREATE INDEX IF NOT EXISTS idx_dollar_flows_source
    ON dollar_flows (source_type, flow_date DESC);
CREATE INDEX IF NOT EXISTS idx_dollar_flows_date
    ON dollar_flows (flow_date DESC);
CREATE INDEX IF NOT EXISTS idx_dollar_flows_amount
    ON dollar_flows (amount_usd DESC);

-- ============================================================
-- TABLE: source_accuracy
-- Pairwise source comparison results for redundant features.
-- ============================================================
CREATE TABLE IF NOT EXISTS source_accuracy (
    id                    SERIAL PRIMARY KEY,
    feature_name          TEXT NOT NULL,
    source_a              TEXT NOT NULL,
    source_b              TEXT NOT NULL,
    correlation           NUMERIC,
    mean_deviation        NUMERIC,
    max_deviation         NUMERIC,
    timeliness_winner     TEXT,
    completeness_winner   TEXT,
    accuracy_winner       TEXT,
    overall_winner        TEXT,
    checked_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_source_accuracy_feature
    ON source_accuracy (feature_name, checked_at DESC);

-- ============================================================
-- TABLE: source_discrepancies
-- Active discrepancy log with third-source resolution.
-- ============================================================
CREATE TABLE IF NOT EXISTS source_discrepancies (
    id                    SERIAL PRIMARY KEY,
    feature_name          TEXT NOT NULL,
    source_a              TEXT NOT NULL,
    value_a               NUMERIC,
    source_b              TEXT NOT NULL,
    value_b               NUMERIC,
    deviation             NUMERIC,
    third_source          TEXT,
    third_value           NUMERIC,
    resolution            TEXT,
    detected_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_discrepancies_date
    ON source_discrepancies (detected_at DESC);

-- ============================================================
-- TABLE: thesis_snapshots
-- Versioned archive of unified thesis states over time.
-- ============================================================
CREATE TABLE IF NOT EXISTS thesis_snapshots (
    id                    SERIAL PRIMARY KEY,
    timestamp             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    overall_direction     TEXT NOT NULL,
    conviction            NUMERIC,
    key_drivers           JSONB,
    risk_factors          JSONB,
    model_states          JSONB,
    narrative             TEXT,
    outcome               TEXT,
    actual_market_move    NUMERIC,
    scored_at             TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_thesis_snapshots_ts
    ON thesis_snapshots (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_thesis_snapshots_unscored
    ON thesis_snapshots (timestamp) WHERE outcome IS NULL;

-- ============================================================
-- TABLE: thesis_postmortems
-- Post-mortem analysis for wrong or partially-correct theses.
-- ============================================================
CREATE TABLE IF NOT EXISTS thesis_postmortems (
    id                    SERIAL PRIMARY KEY,
    snapshot_id           INT REFERENCES thesis_snapshots(id),
    thesis_direction      TEXT,
    actual_direction      TEXT,
    models_right          JSONB,
    models_wrong          JSONB,
    what_we_missed        TEXT,
    root_cause            TEXT,
    lesson                TEXT,
    generated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_thesis_pm_snapshot
    ON thesis_postmortems (snapshot_id);

-- ============================================================
-- SCHEMA: astrogrid
-- Separate write boundary for AstroGrid-derived state.
-- AstroGrid may read from shared GRID inputs, but it writes only here.
-- ============================================================
CREATE SCHEMA IF NOT EXISTS astrogrid;

CREATE TABLE IF NOT EXISTS astrogrid.grid_input_allowlist (
    id            BIGSERIAL PRIMARY KEY,
    input_kind    TEXT NOT NULL CHECK (input_kind IN ('feature', 'table', 'view', 'briefing', 'series')),
    object_name   TEXT NOT NULL,
    purpose       TEXT NOT NULL,
    notes         TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deprecated_at TIMESTAMPTZ,
    UNIQUE (input_kind, object_name)
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_allowlist_kind
    ON astrogrid.grid_input_allowlist (input_kind);
CREATE INDEX IF NOT EXISTS idx_astrogrid_allowlist_active
    ON astrogrid.grid_input_allowlist (input_kind, object_name) WHERE deprecated_at IS NULL;

CREATE TABLE IF NOT EXISTS astrogrid.celestial_object_registry (
    id              BIGSERIAL PRIMARY KEY,
    object_key      TEXT NOT NULL UNIQUE,
    display_name    TEXT NOT NULL,
    object_class    TEXT NOT NULL CHECK (object_class IN (
                        'luminary', 'planet', 'node', 'asteroid',
                        'fixed_star', 'satellite', 'surface', 'region',
                        'mission', 'field')),
    source          TEXT NOT NULL,
    precision_label TEXT NOT NULL CHECK (precision_label IN (
                        'authoritative', 'derived', 'approximate', 'mixed')),
    visual_priority INTEGER NOT NULL DEFAULT 0,
    track_mode      TEXT NOT NULL CHECK (track_mode IN ('reliable', 'experimental', 'derived')),
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deprecated_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_object_registry_class
    ON astrogrid.celestial_object_registry (object_class);
CREATE INDEX IF NOT EXISTS idx_astrogrid_object_registry_enabled
    ON astrogrid.celestial_object_registry (enabled);

CREATE TABLE IF NOT EXISTS astrogrid.lens_set (
    id               BIGSERIAL PRIMARY KEY,
    lens_set_key     TEXT NOT NULL,
    version          INTEGER NOT NULL CHECK (version > 0),
    name             TEXT NOT NULL,
    mode             TEXT NOT NULL CHECK (mode IN ('solo', 'chorus', 'intersection', 'shadow')),
    allowed_lenses   TEXT[] NOT NULL DEFAULT '{}'::text[],
    forbidden_lenses TEXT[] NOT NULL DEFAULT '{}'::text[],
    weighting        JSONB NOT NULL DEFAULT '{}'::jsonb,
    doctrine         TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (lens_set_key, version)
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_lens_set_key
    ON astrogrid.lens_set (lens_set_key, version DESC);

CREATE TABLE IF NOT EXISTS astrogrid.sky_snapshot (
    id                   BIGSERIAL PRIMARY KEY,
    snapshot_date        DATE NOT NULL,
    snapshot_ts          TIMESTAMPTZ NOT NULL,
    location_key         TEXT NOT NULL DEFAULT 'geocentric',
    source_mode          TEXT NOT NULL CHECK (source_mode IN ('grid', 'local', 'archive', 'hybrid')),
    precision_label      TEXT NOT NULL CHECK (precision_label IN (
                             'authoritative', 'derived', 'approximate', 'mixed')),
    source_trace         JSONB NOT NULL DEFAULT '{}'::jsonb,
    bodies_payload       JSONB NOT NULL DEFAULT '[]'::jsonb,
    aspects_payload      JSONB NOT NULL DEFAULT '[]'::jsonb,
    cycles_payload       JSONB NOT NULL DEFAULT '{}'::jsonb,
    events_payload       JSONB NOT NULL DEFAULT '[]'::jsonb,
    signals_payload      JSONB NOT NULL DEFAULT '{}'::jsonb,
    grid_overlay_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (snapshot_ts, location_key, source_mode)
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_snapshot_date
    ON astrogrid.sky_snapshot (snapshot_date DESC);
CREATE INDEX IF NOT EXISTS idx_astrogrid_snapshot_source
    ON astrogrid.sky_snapshot (source_mode, snapshot_date DESC);

CREATE TABLE IF NOT EXISTS astrogrid.engine_run (
    id                    BIGSERIAL PRIMARY KEY,
    sky_snapshot_id       BIGINT NOT NULL REFERENCES astrogrid.sky_snapshot(id),
    lens_set_id           BIGINT REFERENCES astrogrid.lens_set(id),
    engine_key            TEXT NOT NULL,
    engine_family         TEXT NOT NULL,
    provider_mode         TEXT NOT NULL CHECK (provider_mode IN ('deterministic', 'llm', 'hybrid')),
    model_name            TEXT,
    direction_label       TEXT,
    confidence            DOUBLE PRECISION CHECK (confidence BETWEEN 0 AND 1),
    horizon_label         TEXT,
    reading               TEXT NOT NULL,
    omen                  TEXT,
    prediction            TEXT,
    claim_payload         JSONB NOT NULL DEFAULT '[]'::jsonb,
    rationale_payload     JSONB NOT NULL DEFAULT '[]'::jsonb,
    contradiction_payload JSONB NOT NULL DEFAULT '[]'::jsonb,
    feature_trace         JSONB NOT NULL DEFAULT '{}'::jsonb,
    citation_payload      JSONB NOT NULL DEFAULT '[]'::jsonb,
    raw_output            JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_engine_run_snapshot
    ON astrogrid.engine_run (sky_snapshot_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_astrogrid_engine_run_key
    ON astrogrid.engine_run (engine_key, created_at DESC);

CREATE TABLE IF NOT EXISTS astrogrid.seer_run (
    id                     BIGSERIAL PRIMARY KEY,
    sky_snapshot_id        BIGINT NOT NULL REFERENCES astrogrid.sky_snapshot(id),
    lens_set_id            BIGINT REFERENCES astrogrid.lens_set(id),
    merge_mode             TEXT NOT NULL CHECK (merge_mode IN ('solo', 'chorus', 'intersection', 'shadow')),
    supporting_lenses      TEXT[] NOT NULL DEFAULT '{}'::text[],
    source_engine_runs     JSONB NOT NULL DEFAULT '[]'::jsonb,
    convergence_map        JSONB NOT NULL DEFAULT '{}'::jsonb,
    contradiction_map      JSONB NOT NULL DEFAULT '{}'::jsonb,
    world_overlay_payload  JSONB NOT NULL DEFAULT '{}'::jsonb,
    reading                TEXT NOT NULL,
    prediction             TEXT NOT NULL,
    confidence             DOUBLE PRECISION CHECK (confidence BETWEEN 0 AND 1),
    confidence_band        TEXT CHECK (confidence_band IN ('low', 'medium', 'high', 'extreme')),
    key_factors            JSONB NOT NULL DEFAULT '[]'::jsonb,
    conflict_payload       JSONB NOT NULL DEFAULT '[]'::jsonb,
    action_bias            TEXT,
    window_label           TEXT,
    raw_output             JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_seer_run_snapshot
    ON astrogrid.seer_run (sky_snapshot_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_astrogrid_seer_run_conf_band
    ON astrogrid.seer_run (confidence_band, created_at DESC);

CREATE TABLE IF NOT EXISTS astrogrid.persona_run (
    id               BIGSERIAL PRIMARY KEY,
    seer_run_id      BIGINT REFERENCES astrogrid.seer_run(id),
    lens_set_id      BIGINT REFERENCES astrogrid.lens_set(id),
    persona_key      TEXT NOT NULL,
    provider_mode    TEXT NOT NULL CHECK (provider_mode IN ('deterministic', 'llm', 'hybrid')),
    model_name       TEXT,
    question         TEXT NOT NULL,
    declared_lens    TEXT,
    allowed_lenses   TEXT[] NOT NULL DEFAULT '{}'::text[],
    excluded_lenses  TEXT[] NOT NULL DEFAULT '{}'::text[],
    answer_text      TEXT NOT NULL,
    answer_payload   JSONB NOT NULL DEFAULT '{}'::jsonb,
    citation_payload JSONB NOT NULL DEFAULT '[]'::jsonb,
    raw_output       JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_persona_run_seer
    ON astrogrid.persona_run (seer_run_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_astrogrid_persona_run_key
    ON astrogrid.persona_run (persona_key, created_at DESC);

CREATE TABLE IF NOT EXISTS astrogrid.hypothesis_log (
    id                 BIGSERIAL PRIMARY KEY,
    sky_snapshot_id    BIGINT NOT NULL REFERENCES astrogrid.sky_snapshot(id),
    seer_run_id        BIGINT REFERENCES astrogrid.seer_run(id),
    hypothesis_key     TEXT NOT NULL,
    title              TEXT NOT NULL,
    statement          TEXT NOT NULL,
    action_label       TEXT NOT NULL,
    window_start       TIMESTAMPTZ,
    window_end         TIMESTAMPTZ,
    confidence         DOUBLE PRECISION CHECK (confidence BETWEEN 0 AND 1),
    supporting_lenses  TEXT[] NOT NULL DEFAULT '{}'::text[],
    supporting_runs    JSONB NOT NULL DEFAULT '[]'::jsonb,
    feature_payload    JSONB NOT NULL DEFAULT '{}'::jsonb,
    event_payload      JSONB NOT NULL DEFAULT '[]'::jsonb,
    outcome_state      TEXT NOT NULL DEFAULT 'open' CHECK (outcome_state IN (
                           'open', 'confirmed', 'mixed', 'failed', 'expired')),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_hypothesis_snapshot
    ON astrogrid.hypothesis_log (sky_snapshot_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_astrogrid_hypothesis_state
    ON astrogrid.hypothesis_log (outcome_state, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_astrogrid_hypothesis_key
    ON astrogrid.hypothesis_log (hypothesis_key, created_at DESC);

CREATE TABLE IF NOT EXISTS astrogrid.world_state (
    id                   BIGSERIAL PRIMARY KEY,
    sky_snapshot_id      BIGINT NOT NULL REFERENCES astrogrid.sky_snapshot(id),
    seer_run_id          BIGINT REFERENCES astrogrid.seer_run(id),
    atlas_payload        JSONB NOT NULL DEFAULT '{}'::jsonb,
    observatory_payload  JSONB NOT NULL DEFAULT '{}'::jsonb,
    coordinate_payload   JSONB NOT NULL DEFAULT '{}'::jsonb,
    capital_flow_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_world_state_snapshot
    ON astrogrid.world_state (sky_snapshot_id, created_at DESC);

CREATE TABLE IF NOT EXISTS astrogrid.outcome_log (
    id               BIGSERIAL PRIMARY KEY,
    target_kind      TEXT NOT NULL CHECK (target_kind IN (
                         'engine_run', 'seer_run', 'persona_run', 'hypothesis')),
    target_id        BIGINT NOT NULL,
    horizon_label    TEXT,
    observed_at      TIMESTAMPTZ NOT NULL,
    outcome_state    TEXT NOT NULL CHECK (outcome_state IN (
                         'open', 'confirmed', 'mixed', 'failed', 'expired')),
    observed_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    score            DOUBLE PRECISION,
    notes            TEXT,
    provenance       JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_outcome_target
    ON astrogrid.outcome_log (target_kind, target_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_astrogrid_outcome_state
    ON astrogrid.outcome_log (outcome_state, observed_at DESC);

CREATE TABLE IF NOT EXISTS astrogrid.session_log (
    id              BIGSERIAL PRIMARY KEY,
    session_key     TEXT NOT NULL,
    event_type      TEXT NOT NULL CHECK (event_type IN (
                        'open', 'view', 'config', 'sync', 'question', 'close')),
    page_key        TEXT,
    lens_set_id     BIGINT REFERENCES astrogrid.lens_set(id),
    sky_snapshot_id BIGINT REFERENCES astrogrid.sky_snapshot(id),
    seer_run_id     BIGINT REFERENCES astrogrid.seer_run(id),
    persona_run_id  BIGINT REFERENCES astrogrid.persona_run(id),
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_session_key
    ON astrogrid.session_log (session_key, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_astrogrid_session_event
    ON astrogrid.session_log (event_type, created_at DESC);

CREATE OR REPLACE FUNCTION astrogrid.prevent_log_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION '% is append-only: % is not permitted on id=%',
        TG_TABLE_NAME, TG_OP, COALESCE(OLD.id, NEW.id);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_astrogrid_lens_set_no_mutation ON astrogrid.lens_set;
CREATE TRIGGER trg_astrogrid_lens_set_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.lens_set
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();

DROP TRIGGER IF EXISTS trg_astrogrid_sky_snapshot_no_mutation ON astrogrid.sky_snapshot;
CREATE TRIGGER trg_astrogrid_sky_snapshot_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.sky_snapshot
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();

DROP TRIGGER IF EXISTS trg_astrogrid_engine_run_no_mutation ON astrogrid.engine_run;
CREATE TRIGGER trg_astrogrid_engine_run_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.engine_run
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();

DROP TRIGGER IF EXISTS trg_astrogrid_seer_run_no_mutation ON astrogrid.seer_run;
CREATE TRIGGER trg_astrogrid_seer_run_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.seer_run
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();

DROP TRIGGER IF EXISTS trg_astrogrid_persona_run_no_mutation ON astrogrid.persona_run;
CREATE TRIGGER trg_astrogrid_persona_run_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.persona_run
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();

DROP TRIGGER IF EXISTS trg_astrogrid_hypothesis_log_no_mutation ON astrogrid.hypothesis_log;
CREATE TRIGGER trg_astrogrid_hypothesis_log_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.hypothesis_log
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();

DROP TRIGGER IF EXISTS trg_astrogrid_world_state_no_mutation ON astrogrid.world_state;
CREATE TRIGGER trg_astrogrid_world_state_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.world_state
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();

DROP TRIGGER IF EXISTS trg_astrogrid_outcome_log_no_mutation ON astrogrid.outcome_log;
CREATE TRIGGER trg_astrogrid_outcome_log_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.outcome_log
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();

DROP TRIGGER IF EXISTS trg_astrogrid_session_log_no_mutation ON astrogrid.session_log;
CREATE TRIGGER trg_astrogrid_session_log_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.session_log
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();

INSERT INTO astrogrid.grid_input_allowlist (input_kind, object_name, purpose, notes)
VALUES
    ('feature', 'geomagnetic_kp_index', 'Solar weather overlay for AstroGrid timing state.', 'Read-only input from public.resolved_series.'),
    ('feature', 'geomagnetic_ap_index', 'Secondary geomagnetic overlay.', 'Read-only input from public.resolved_series.'),
    ('feature', 'sunspot_number', 'Solar-cycle context for AstroGrid signal state.', 'Read-only input from public.resolved_series.'),
    ('feature', 'solar_flux_10_7cm', 'Solar radio flux overlay.', 'Read-only input from public.resolved_series.'),
    ('feature', 'solar_wind_speed', 'Solar wind overlay.', 'Read-only input from public.resolved_series.'),
    ('feature', 'solar_storm_probability', 'Storm-risk overlay.', 'Read-only input from public.resolved_series.'),
    ('feature', 'solar_cycle_phase', 'Solar-cycle phase overlay.', 'Read-only input from public.resolved_series.'),
    ('feature', 'nakshatra_index', 'Cross-check for historical nakshatra studies.', 'Read-only input from public.resolved_series.'),
    ('feature', 'spy_full', 'Market overlay for retrospective scoring and correlation only.', 'AstroGrid may read this; it must not write derived outputs back into GRID market tables.'),
    ('table', 'regime_history', 'Read-only regime overlay when available.', 'Optional upstream table outside astrogrid schema.'),
    ('table', 'briefings', 'Legacy celestial briefing cache if present.', 'Optional upstream table outside astrogrid schema.'),
    ('table', 'celestial_briefings', 'Preferred celestial briefing cache if present.', 'Optional upstream table outside astrogrid schema.')
ON CONFLICT (input_kind, object_name) DO NOTHING;

INSERT INTO astrogrid.celestial_object_registry (
    object_key, display_name, object_class, source, precision_label, visual_priority, track_mode, metadata
)
VALUES
    ('sun', 'Sun', 'luminary', 'analysis.ephemeris', 'derived', 100, 'reliable', '{"glyph":"Su"}'::jsonb),
    ('moon', 'Moon', 'luminary', 'analysis.ephemeris', 'derived', 95, 'reliable', '{"glyph":"Mo"}'::jsonb),
    ('mercury', 'Mercury', 'planet', 'analysis.ephemeris', 'derived', 90, 'reliable', '{"glyph":"Me"}'::jsonb),
    ('venus', 'Venus', 'planet', 'analysis.ephemeris', 'derived', 88, 'reliable', '{"glyph":"Ve"}'::jsonb),
    ('mars', 'Mars', 'planet', 'analysis.ephemeris', 'derived', 86, 'reliable', '{"glyph":"Ma"}'::jsonb),
    ('jupiter', 'Jupiter', 'planet', 'analysis.ephemeris', 'derived', 84, 'reliable', '{"glyph":"Ju"}'::jsonb),
    ('saturn', 'Saturn', 'planet', 'analysis.ephemeris', 'derived', 82, 'reliable', '{"glyph":"Sa"}'::jsonb),
    ('uranus', 'Uranus', 'planet', 'analysis.ephemeris', 'derived', 76, 'reliable', '{"glyph":"Ur"}'::jsonb),
    ('neptune', 'Neptune', 'planet', 'analysis.ephemeris', 'derived', 74, 'reliable', '{"glyph":"Ne"}'::jsonb),
    ('pluto', 'Pluto', 'planet', 'analysis.ephemeris', 'derived', 72, 'reliable', '{"glyph":"Pl"}'::jsonb),
    ('rahu', 'Rahu', 'node', 'analysis.ephemeris', 'derived', 68, 'reliable', '{"glyph":"Ra"}'::jsonb),
    ('ketu', 'Ketu', 'node', 'analysis.ephemeris', 'derived', 66, 'reliable', '{"glyph":"Ke"}'::jsonb)
ON CONFLICT (object_key) DO NOTHING;
