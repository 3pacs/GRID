-- Migration: 0028_sector_health_snapshots
-- Purpose: Daily snapshot store for the sector health composite score.
--          `intelligence/sector_health.py` writes one row per sector per
--          day; the API endpoint reads the latest row plus the row ~30
--          days ago to produce trend_30d labels.
--
-- Idempotent: DDL uses IF NOT EXISTS and a UNIQUE (sector_name, snapshot_date)
-- allows the scheduler to safely re-run on the same day without duplicates.

CREATE TABLE IF NOT EXISTS sector_health_snapshots (
    id              SERIAL PRIMARY KEY,
    sector_name     TEXT NOT NULL,
    score           NUMERIC,
    components      JSONB,
    snapshot_date   DATE NOT NULL,
    as_of           TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (sector_name, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_sector_health_date
    ON sector_health_snapshots(snapshot_date DESC);
CREATE INDEX IF NOT EXISTS idx_sector_health_sector_date
    ON sector_health_snapshots(sector_name, snapshot_date DESC);

-- ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======
-- Migrations run as `postgres`; the API and ingestors connect as `grid`.
GRANT ALL ON sector_health_snapshots TO grid;
GRANT USAGE, SELECT ON SEQUENCE sector_health_snapshots_id_seq TO grid;
