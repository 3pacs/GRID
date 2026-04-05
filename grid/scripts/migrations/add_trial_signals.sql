-- =============================================================================
-- GRID Trial Signals Schema
-- File: grid/scripts/migrations/add_trial_signals.sql
--
-- Run on grid-svr:
--   psql -U grid -d griddb -f add_trial_signals.sql
-- =============================================================================

-- ── Core signal table ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS trial_signals (
    id                      SERIAL PRIMARY KEY,
    run_id                  TEXT,                        -- AutoAgent run identifier

    -- Trial identity
    nct_id                  TEXT NOT NULL,               -- ClinicalTrials.gov ID
    ticker                  TEXT NOT NULL,               -- Equity ticker
    company_name            TEXT NOT NULL,
    sponsor_name            TEXT,

    -- Trial metadata
    trial_phase             TEXT NOT NULL,               -- PHASE2, PHASE3
    primary_indication      TEXT,                        -- oncology, CNS, etc.
    primary_endpoint        TEXT,                        -- OS, PFS, CR, etc.
    endpoint_type           TEXT,                        -- binary, composite, surrogate
    fda_designation         TEXT,                        -- Fast Track, Breakthrough, etc.
    trial_start_date        DATE,
    primary_completion_date DATE,
    days_to_completion      INTEGER,

    -- Enrollment
    enrollment_target       INTEGER,
    enrollment_actual       INTEGER,
    enrollment_pct          NUMERIC(5,2),               -- 0-100

    -- Company snapshot at signal time
    market_cap_mm           NUMERIC(12,2),              -- USD millions
    cash_runway_months      NUMERIC(6,1),
    pipeline_depth          INTEGER,                    -- # drugs in pipeline
    short_interest_pct      NUMERIC(6,2),

    -- Signal scoring
    trial_strength_score    NUMERIC(5,4) NOT NULL,      -- 0.0–1.0
    endpoint_clarity        NUMERIC(5,4),
    phase_weight            NUMERIC(5,4),
    disease_priority        NUMERIC(5,4),
    cash_runway_score       NUMERIC(5,4),
    penalty_factors         JSONB,                      -- applied penalties

    -- GRID integration
    signal_type             TEXT NOT NULL               -- BUY, WATCHLIST, AVOID
                            CHECK (signal_type IN ('BUY', 'WATCHLIST', 'AVOID')),
    regime_at_signal        TEXT NOT NULL               -- GROWTH, NEUTRAL, FRAGILE, CRISIS
                            CHECK (regime_at_signal IN ('GROWTH','NEUTRAL','FRAGILE','CRISIS','UNKNOWN')),
    confidence              NUMERIC(5,4),               -- 0.0–1.0
    suggested_position_pct  NUMERIC(5,2),               -- % of portfolio (null = unscored)

    -- Agent reasoning
    rationale               TEXT,
    red_flags               TEXT[],
    catalysts               TEXT[],

    -- Evaluation (filled by test.py post-hoc)
    fwd_return_30d          NUMERIC(8,4),               -- actual 30d return
    eval_score              NUMERIC(5,4),               -- normalized score contribution
    evaluated_at            TIMESTAMPTZ,

    -- Housekeeping
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for GRID query patterns
CREATE INDEX IF NOT EXISTS idx_ts_ticker
    ON trial_signals(ticker);

CREATE INDEX IF NOT EXISTS idx_ts_regime
    ON trial_signals(regime_at_signal);

CREATE INDEX IF NOT EXISTS idx_ts_signal_type
    ON trial_signals(signal_type);

CREATE INDEX IF NOT EXISTS idx_ts_completion
    ON trial_signals(primary_completion_date);

CREATE INDEX IF NOT EXISTS idx_ts_strength
    ON trial_signals(trial_strength_score DESC);

CREATE INDEX IF NOT EXISTS idx_ts_created
    ON trial_signals(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ts_run
    ON trial_signals(run_id);


-- ── Trial feature cache (avoid re-fetching ct.gov on every run) ───────────────

CREATE TABLE IF NOT EXISTS trial_cache (
    nct_id                  TEXT PRIMARY KEY,
    raw_json                JSONB NOT NULL,
    parsed_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at              TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '24 hours'
);

CREATE INDEX IF NOT EXISTS idx_tc_expires
    ON trial_cache(expires_at);


-- ── Company catalyst calendar ─────────────────────────────────────────────────
-- Tracks upcoming readout dates for active watchlist items

CREATE TABLE IF NOT EXISTS catalyst_calendar (
    id                      SERIAL PRIMARY KEY,
    ticker                  TEXT NOT NULL,
    nct_id                  TEXT,
    event_type              TEXT NOT NULL,              -- READOUT, FDA_DECISION, ENROLLMENT_COMPLETE
    expected_date           DATE,
    confidence_window_days  INTEGER DEFAULT 30,         -- ± days uncertainty
    source                  TEXT,
    notes                   TEXT,
    is_active               BOOLEAN DEFAULT TRUE,
    created_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cc_ticker
    ON catalyst_calendar(ticker, expected_date);

CREATE INDEX IF NOT EXISTS idx_cc_date
    ON catalyst_calendar(expected_date)
    WHERE is_active = TRUE;


-- ── Auto-update trigger ───────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trial_signals_updated_at ON trial_signals;
CREATE TRIGGER trial_signals_updated_at
    BEFORE UPDATE ON trial_signals
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();


-- ── Useful views ──────────────────────────────────────────────────────────────

-- Active BUY signals in favorable regimes
CREATE OR REPLACE VIEW trial_gems AS
SELECT
    ts.ticker,
    ts.company_name,
    ts.primary_indication,
    ts.trial_phase,
    ts.primary_completion_date,
    ts.days_to_completion,
    ts.trial_strength_score,
    ts.fda_designation,
    ts.market_cap_mm,
    ts.confidence,
    ts.regime_at_signal,
    ts.rationale,
    ts.created_at
FROM trial_signals ts
WHERE ts.signal_type = 'BUY'
  AND ts.regime_at_signal IN ('GROWTH', 'NEUTRAL')
  AND ts.trial_strength_score >= 0.65
  AND ts.primary_completion_date >= CURRENT_DATE
ORDER BY ts.trial_strength_score DESC, ts.days_to_completion ASC;


-- Performance tracker (post-evaluation)
CREATE OR REPLACE VIEW trial_signal_performance AS
SELECT
    DATE_TRUNC('week', created_at)  AS week,
    regime_at_signal                AS regime,
    signal_type,
    primary_indication              AS indication,
    COUNT(*)                        AS n_signals,
    AVG(trial_strength_score)       AS avg_strength,
    AVG(fwd_return_30d)             AS avg_30d_return,
    AVG(eval_score)                 AS avg_eval_score,
    STDDEV(fwd_return_30d)          AS stddev_return,
    SUM(CASE WHEN fwd_return_30d > 0 THEN 1 ELSE 0 END)::FLOAT
        / NULLIF(COUNT(*), 0)       AS hit_rate
FROM trial_signals
WHERE evaluated_at IS NOT NULL
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC, avg_eval_score DESC;


-- Upcoming catalysts joined to current signal status
CREATE OR REPLACE VIEW upcoming_catalysts AS
SELECT
    cc.ticker,
    cc.event_type,
    cc.expected_date,
    cc.expected_date - CURRENT_DATE AS days_out,
    cc.confidence_window_days,
    ts.trial_strength_score,
    ts.signal_type,
    ts.regime_at_signal,
    ts.market_cap_mm,
    ts.primary_indication
FROM catalyst_calendar cc
LEFT JOIN trial_signals ts
    ON cc.ticker = ts.ticker
    AND ts.created_at = (
        SELECT MAX(created_at) FROM trial_signals
        WHERE ticker = cc.ticker
    )
WHERE cc.is_active = TRUE
  AND cc.expected_date >= CURRENT_DATE
ORDER BY cc.expected_date ASC;

-- ── Done ──────────────────────────────────────────────────────────────────────
-- To verify:
--   \d trial_signals
--   SELECT * FROM trial_gems LIMIT 5;
--   SELECT * FROM upcoming_catalysts LIMIT 10;
