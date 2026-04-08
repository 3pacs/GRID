-- ============================================================
-- GRID — Valuation & Derivatives Support Tables
-- Migration: add_valuation_tables.sql
-- Execute: psql -U grid_user -d grid -f grid/scripts/migrations/add_valuation_tables.sql
-- ============================================================

-- ============================================================
-- TABLE: company_valuations
-- Point-in-time intrinsic value estimates from balance sheet
-- and earnings data. Multiple valuation methods per snapshot.
-- ============================================================
CREATE TABLE IF NOT EXISTS company_valuations (
    id                  BIGSERIAL PRIMARY KEY,
    ticker              TEXT NOT NULL,
    valuation_date      DATE NOT NULL,
    market_price        DOUBLE PRECISION,
    shares_outstanding  DOUBLE PRECISION,
    market_cap          DOUBLE PRECISION,

    -- Balance sheet intrinsic values
    book_value_ps       DOUBLE PRECISION,   -- Total equity / shares
    tangible_book_ps    DOUBLE PRECISION,   -- (Equity - intangibles - goodwill) / shares
    ncav_ps             DOUBLE PRECISION,   -- (Current assets - total liabilities) / shares (Graham)
    net_cash_ps         DOUBLE PRECISION,   -- (Cash - total debt) / shares
    liquidation_ps      DOUBLE PRECISION,   -- Conservative liquidation: 0.8*receivables + 0.5*inventory + cash - liabilities

    -- Earnings-based intrinsic values
    epv_ps              DOUBLE PRECISION,   -- Earnings power value: normalized_earnings / cost_of_capital
    owner_earnings_ps   DOUBLE PRECISION,   -- Buffett: net_income + depreciation - capex (maintenance)
    dcf_ps              DOUBLE PRECISION,   -- Simple 10yr DCF of free cash flow
    ev_ebitda           DOUBLE PRECISION,   -- Enterprise value / EBITDA

    -- Relative valuation
    pe_ratio            DOUBLE PRECISION,
    pb_ratio            DOUBLE PRECISION,
    ps_ratio            DOUBLE PRECISION,
    peg_ratio           DOUBLE PRECISION,

    -- Composite
    intrinsic_low       DOUBLE PRECISION,   -- Min of valuation methods (conservative)
    intrinsic_mid       DOUBLE PRECISION,   -- Median of valuation methods
    intrinsic_high      DOUBLE PRECISION,   -- Max of valuation methods (optimistic)
    margin_of_safety    DOUBLE PRECISION,   -- (intrinsic_mid - market_price) / intrinsic_mid

    -- Metadata
    data_freshness      TEXT CHECK (data_freshness IN ('CURRENT', 'STALE', 'ESTIMATED')),
    input_payload       JSONB,              -- Raw inputs used for audit trail
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_company_valuations_ticker_date
    ON company_valuations (ticker, valuation_date);
CREATE INDEX IF NOT EXISTS idx_company_valuations_ticker
    ON company_valuations (ticker);
CREATE INDEX IF NOT EXISTS idx_company_valuations_date
    ON company_valuations (valuation_date DESC);
CREATE INDEX IF NOT EXISTS idx_company_valuations_margin
    ON company_valuations (margin_of_safety DESC);


-- ============================================================
-- TABLE: company_milestones
-- Tracks public goals, guidance, plans, and rumors with
-- probability weights and achievement scoring.
-- ============================================================
CREATE TABLE IF NOT EXISTS company_milestones (
    id                  BIGSERIAL PRIMARY KEY,
    ticker              TEXT NOT NULL,
    milestone_type      TEXT NOT NULL CHECK (milestone_type IN (
        'EARNINGS_GUIDANCE',    -- Management forward earnings guidance
        'REVENUE_GUIDANCE',     -- Management revenue guidance
        'PRODUCT_LAUNCH',       -- Product/service launch target
        'EXPANSION',            -- Market expansion, new geography
        'M_AND_A',              -- Merger, acquisition, divestiture
        'REGULATORY',           -- FDA approval, regulatory milestone
        'COST_TARGET',          -- Cost reduction / margin target
        'BUYBACK',              -- Share repurchase program
        'DIVIDEND',             -- Dividend change target
        'DEBT_TARGET',          -- Debt reduction target
        'STRATEGIC',            -- Strategic pivot / restructuring
        'RUMOR'                 -- Unconfirmed market rumor
    )),

    -- Timeline
    announced_date      DATE NOT NULL,       -- When this became known
    target_date         DATE,                -- When company expects to achieve it
    actual_date         DATE,                -- When it actually happened (NULL = pending)

    -- Quantification
    description         TEXT NOT NULL,
    target_value        DOUBLE PRECISION,    -- Quantified target (revenue $, EPS, units, etc.)
    target_unit         TEXT,                -- Unit label ('$M', 'EPS', 'units', '%', etc.)
    actual_value        DOUBLE PRECISION,    -- Actual achieved value (NULL = pending)
    achievement_pct     DOUBLE PRECISION,    -- actual / target as percentage

    -- Probability & confidence
    probability         DOUBLE PRECISION NOT NULL DEFAULT 0.5 CHECK (probability >= 0 AND probability <= 1),
    confidence_source   TEXT CHECK (confidence_source IN (
        'MANAGEMENT',    -- Company stated directly
        'ANALYST',       -- Analyst consensus
        'INSIDER',       -- Insider filing pattern
        'MARKET',        -- Options/market pricing implies
        'RUMOR',         -- Unverified source
        'CALCULATED'     -- Our model estimate
    )),

    -- Impact quantification
    value_impact_ps     DOUBLE PRECISION,    -- Estimated per-share impact on intrinsic value
    value_impact_pct    DOUBLE PRECISION,    -- Estimated % impact on stock price

    -- Status tracking
    status              TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN (
        'PENDING',       -- Not yet due
        'ON_TRACK',      -- Progressing as expected
        'AHEAD',         -- Exceeding expectations
        'BEHIND',        -- Missing targets
        'ACHIEVED',      -- Successfully completed
        'MISSED',        -- Failed to achieve by deadline
        'CANCELLED',     -- Company abandoned this goal
        'SUPERSEDED'     -- Replaced by a new target
    )),

    -- Metadata
    source_url          TEXT,
    notes               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_milestones_ticker
    ON company_milestones (ticker);
CREATE INDEX IF NOT EXISTS idx_milestones_ticker_status
    ON company_milestones (ticker, status);
CREATE INDEX IF NOT EXISTS idx_milestones_target_date
    ON company_milestones (target_date);
CREATE INDEX IF NOT EXISTS idx_milestones_type
    ON company_milestones (milestone_type);


-- ============================================================
-- TABLE: derivatives_support
-- Point-in-time snapshot of how derivatives positioning
-- supports or pressures the current stock price.
-- ============================================================
CREATE TABLE IF NOT EXISTS derivatives_support (
    id                  BIGSERIAL PRIMARY KEY,
    ticker              TEXT NOT NULL,
    snap_date           DATE NOT NULL,

    -- Price context
    spot_price          DOUBLE PRECISION,
    intrinsic_mid       DOUBLE PRECISION,    -- From company_valuations
    premium_to_intrinsic DOUBLE PRECISION,   -- (spot - intrinsic) / intrinsic

    -- Short positioning
    short_float_pct     DOUBLE PRECISION,    -- Short interest as % of float
    short_interest      DOUBLE PRECISION,    -- Total shares short
    days_to_cover       DOUBLE PRECISION,    -- Short interest / avg daily volume
    short_change_pct    DOUBLE PRECISION,    -- % change in short interest (period over period)
    borrow_rate         DOUBLE PRECISION,    -- Cost to borrow (annualized %)

    -- Dealer gamma positioning (from DealerGammaEngine)
    gex_aggregate       DOUBLE PRECISION,    -- Net gamma exposure
    gex_regime          TEXT,                -- LONG_GAMMA / SHORT_GAMMA / NEUTRAL
    gamma_flip          DOUBLE PRECISION,    -- Price where GEX crosses zero
    gamma_wall          DOUBLE PRECISION,    -- Strongest gamma strike (resistance)
    put_wall            DOUBLE PRECISION,    -- Strongest put gamma (support)
    call_wall           DOUBLE PRECISION,    -- Strongest call gamma (ceiling)
    vanna_exposure      DOUBLE PRECISION,    -- Delta sensitivity to IV changes
    charm_exposure      DOUBLE PRECISION,    -- Delta sensitivity to time

    -- Options flow
    put_call_ratio      DOUBLE PRECISION,
    iv_skew             DOUBLE PRECISION,    -- OTM put IV vs ATM (fear gauge)
    iv_percentile       DOUBLE PRECISION,    -- IV rank (0-100)
    max_pain            DOUBLE PRECISION,    -- Options max pain strike
    max_pain_dist_pct   DOUBLE PRECISION,    -- (spot - max_pain) / spot

    -- Composite scores (0-100)
    short_pressure_score    DOUBLE PRECISION, -- Higher = more bearish pressure from shorts
    gamma_support_score     DOUBLE PRECISION, -- Higher = more dealer flow support
    options_sentiment_score DOUBLE PRECISION, -- Higher = more bullish options flow
    derivatives_support_score DOUBLE PRECISION, -- Composite: are derivatives supporting price?

    -- Interpretation
    support_regime      TEXT CHECK (support_regime IN (
        'STRONG_SUPPORT',    -- Derivatives strongly support current price
        'MILD_SUPPORT',      -- Some derivative support
        'NEUTRAL',           -- Derivatives neither supporting nor pressuring
        'MILD_PRESSURE',     -- Some derivative headwind
        'STRONG_PRESSURE'    -- Derivatives actively pressuring price down
    )),
    narrative           TEXT,                -- Human-readable explanation

    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_derivatives_support_ticker_date
    ON derivatives_support (ticker, snap_date);
CREATE INDEX IF NOT EXISTS idx_derivatives_support_ticker
    ON derivatives_support (ticker);
CREATE INDEX IF NOT EXISTS idx_derivatives_support_date
    ON derivatives_support (snap_date DESC);
CREATE INDEX IF NOT EXISTS idx_derivatives_support_regime
    ON derivatives_support (support_regime);


-- ============================================================
-- TABLE: valuation_analysis_log
-- Stores Claude Max responses with date-stamped predictions
-- for accuracy tracking over time.
-- ============================================================
CREATE TABLE IF NOT EXISTS valuation_analysis_log (
    id                  BIGSERIAL PRIMARY KEY,
    analysis_id         TEXT NOT NULL UNIQUE,
    ticker              TEXT NOT NULL,
    analysis_date       DATE NOT NULL,
    response_text       TEXT NOT NULL,
    predictions         JSONB NOT NULL DEFAULT '[]'::jsonb,
    accuracy_score      DOUBLE PRECISION,    -- Computed after prediction window closes
    accuracy_detail     JSONB,               -- Per-prediction accuracy breakdown
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    scored_at           TIMESTAMPTZ           -- When accuracy was last computed
);

CREATE INDEX IF NOT EXISTS idx_val_analysis_log_ticker
    ON valuation_analysis_log (ticker);
CREATE INDEX IF NOT EXISTS idx_val_analysis_log_date
    ON valuation_analysis_log (analysis_date DESC);
CREATE INDEX IF NOT EXISTS idx_val_analysis_log_unscored
    ON valuation_analysis_log (ticker, analysis_date)
    WHERE accuracy_score IS NULL;


-- ============================================================
-- VIEW: valuation_timeline
-- Unified timeline joining valuations, milestones, and
-- derivatives support for a given ticker.
-- ============================================================
CREATE OR REPLACE VIEW valuation_timeline AS
SELECT
    cv.ticker,
    cv.valuation_date AS date,
    'VALUATION' AS event_type,
    cv.market_price,
    cv.intrinsic_mid,
    cv.margin_of_safety,
    cv.book_value_ps,
    cv.tangible_book_ps,
    cv.epv_ps,
    cv.dcf_ps,
    ds.derivatives_support_score,
    ds.support_regime,
    ds.short_float_pct,
    ds.gex_regime,
    ds.gamma_wall,
    ds.put_wall
FROM company_valuations cv
LEFT JOIN derivatives_support ds
    ON ds.ticker = cv.ticker AND ds.snap_date = cv.valuation_date;


-- ============================================================
-- VIEW: milestone_scorecard
-- Aggregated goal achievement by ticker.
-- ============================================================
CREATE OR REPLACE VIEW milestone_scorecard AS
SELECT
    ticker,
    COUNT(*) AS total_milestones,
    COUNT(*) FILTER (WHERE status = 'ACHIEVED') AS achieved,
    COUNT(*) FILTER (WHERE status = 'AHEAD') AS ahead_of_schedule,
    COUNT(*) FILTER (WHERE status IN ('MISSED', 'BEHIND')) AS missed_or_behind,
    COUNT(*) FILTER (WHERE status = 'PENDING') AS pending,
    ROUND(
        AVG(CASE WHEN achievement_pct IS NOT NULL THEN achievement_pct END)::numeric, 1
    ) AS avg_achievement_pct,
    ROUND(
        (COUNT(*) FILTER (WHERE status IN ('ACHIEVED', 'AHEAD'))::numeric /
         NULLIF(COUNT(*) FILTER (WHERE status NOT IN ('PENDING', 'CANCELLED'))::numeric, 0)) * 100, 1
    ) AS execution_score
FROM company_milestones
GROUP BY ticker;
