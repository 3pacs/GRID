-- Migration: 0031_regulatory_events
-- Purpose: Staging table for regulatory enforcement actions pulled from
--          FDA, FTC, SEC, DOJ, USDA FSIS, CFPB, EPA and other public
--          sources. Each event is (a) stored in full in regulatory_events
--          and (b) projected as one-or-more `relationship='regulatory_threat'`
--          edges in supply_chain_edges (regulator_slug -> ticker).
--
-- Populated by: ingestion/altdata/regulatory_events.py
-- Runner:       scripts/run_regulatory_events.py
-- Scheduler:    hermes_operator.py entry "regulatory_events" (weekly)
--
-- Idempotent: DDL uses IF NOT EXISTS. The (url) unique constraint lets the
-- loader upsert via ON CONFLICT (url) DO NOTHING.

CREATE TABLE IF NOT EXISTS regulatory_events (
    id                SERIAL PRIMARY KEY,
    regulator         TEXT NOT NULL,          -- fda | ftc | sec | doj | usda_fsis | cfpb | epa
    action_type       TEXT NOT NULL,          -- warning_letter | recall | enforcement | settlement | indictment | press_release
    event_date        DATE NOT NULL,
    title             TEXT,
    summary           TEXT,
    url               TEXT UNIQUE,
    severity          TEXT,                   -- low | medium | high | critical
    affected_tickers  TEXT[],
    raw_content       TEXT,
    as_of             TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_reg_events_date      ON regulatory_events(event_date DESC);
CREATE INDEX IF NOT EXISTS idx_reg_events_regulator ON regulatory_events(regulator);
CREATE INDEX IF NOT EXISTS idx_reg_events_severity  ON regulatory_events(severity);

-- ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======
-- Migrations run as `postgres`; the API and ingestors connect as `grid`.
GRANT ALL ON regulatory_events TO grid;
GRANT USAGE, SELECT ON SEQUENCE regulatory_events_id_seq TO grid;
