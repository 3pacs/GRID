-- ============================================================
-- freshness_audit_universe.sql
-- Refreshes data_freshness_universe and data_freshness_audit from
-- signal_registry / ticker_metrics_daily. Wired into
-- grid-data-freshness-check.timer (daily, 05:00 UTC) on grid-svr,
-- deployed from /data/grid_v4/astrogrid_dedup/scripts/.
--
-- Usage:
--   sudo -u postgres psql -d griddb -v ON_ERROR_STOP=1 -f freshness_audit_universe.sql
-- ============================================================

-- Self-contained even if invoked without -v ON_ERROR_STOP=1: a failed
-- statement must stop the script here, not fall through to a
-- non-transactional GRANT or run the summary SELECT against a state
-- COMMIT never actually reached.
\set ON_ERROR_STOP on

-- Bounded waits: DELETE/INSERT below only ever need a RowExclusiveLock
-- (compatible with a concurrent pg_dump's ACCESS SHARE), so under normal
-- operation neither of these should ever actually wait long enough to
-- matter. They exist so a genuine conflict (e.g. two runs of this job
-- overlapping, or unrelated DDL against these tables) fails fast and
-- visibly instead of hanging the way the old DROP TABLE did.
SET lock_timeout = '2min';
SET statement_timeout = '10min';

-- 2026-09-17: this used to be DROP TABLE + CREATE TABLE on every run,
-- with each statement auto-committing individually (no explicit
-- transaction — this file was never version-controlled or reviewed
-- before this commit). Three real problems with that:
--   1. DROP TABLE needs an ACCESS EXCLUSIVE lock, which must wait out
--      any concurrent reader — including a multi-hour pg_dump backup
--      transaction, which holds ACCESS SHARE on every table it touches
--      for its entire duration. This job was observed blocked for
--      2h52m behind exactly that (2026-09-17), with every other reader
--      of this table queued behind it in turn, until the stuck session
--      was cancelled with pg_cancel_backend() as a targeted, narrowly
--      scoped response that left the in-progress backup untouched.
--   2. CREATE TABLE recreates the object from scratch, owned by whoever
--      runs this script — any GRANT applied to the previous table
--      (e.g. to the `grid` app role, which does not own this table) is
--      silently gone the next time this script completes. A manual
--      GRANT SELECT/INSERT/UPDATE on data_freshness_audit to grid,
--      applied directly against the live table to unblock
--      scripts/td_backfill_universe.py, would not have survived the
--      next successful run of this job.
--   3. Autocommit meant DROP (or a DELETE in a DROP-free version)
--      became visible to other readers the instant it ran, before the
--      replacement INSERT/CREATE had completed — a concurrent reader
--      could see a committed, empty table, and a mid-script failure
--      left it that way with no way back to the previous dataset.
--
-- CREATE TABLE IF NOT EXISTS + DELETE FROM + INSERT keeps the same
-- table object (and its grants) across every run. Wrapping the whole
-- refresh in one explicit transaction means: a reader always sees
-- either the complete previous dataset or the complete new one, never
-- a committed empty table in between; and if the INSERT fails for any
-- reason, ON_ERROR_STOP exits before COMMIT, so the DELETE rolls back
-- too (Postgres rolls back any open transaction when the connection
-- that started it closes) and the previous dataset is preserved
-- untouched.
BEGIN;

CREATE TABLE IF NOT EXISTS data_freshness_audit (
  ticker         TEXT NOT NULL,
  source_table   TEXT NOT NULL,
  last_obs       DATE,
  age_days       INTEGER,
  bucket         TEXT NOT NULL,
  audited_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (ticker, source_table, audited_at)
);
CREATE INDEX IF NOT EXISTS idx_dfa_bucket    ON data_freshness_audit (bucket);
CREATE INDEX IF NOT EXISTS idx_dfa_audited   ON data_freshness_audit (audited_at DESC);
GRANT SELECT, INSERT, UPDATE, DELETE ON data_freshness_audit TO grid;

CREATE TABLE IF NOT EXISTS data_freshness_universe (ticker TEXT PRIMARY KEY);
GRANT SELECT, INSERT, UPDATE, DELETE ON data_freshness_universe TO grid;

DELETE FROM data_freshness_universe;
INSERT INTO data_freshness_universe
SELECT DISTINCT ticker
FROM signal_registry
WHERE ticker IS NOT NULL
  AND ticker ~ '^[A-Z]{1,6}$'
  AND source_module IN ('lever_pullers','alpha_research:vol_price_divergence','news_intel','sector_network','earnings_intel');

DELETE FROM data_freshness_audit;

-- Use ticker_metrics_daily only — the consumer-facing price table. raw_series is
-- the upstream firehose; if tmd is stale the pipeline is broken regardless.
INSERT INTO data_freshness_audit(ticker, source_table, last_obs, age_days, bucket)
SELECT u.ticker,
       'ticker_metrics_daily',
       last_obs,
       CASE WHEN last_obs IS NULL THEN NULL ELSE CURRENT_DATE - last_obs END,
       CASE
         WHEN last_obs IS NULL THEN 'DEAD'
         WHEN CURRENT_DATE - last_obs >= 30 THEN 'STALE_30+'
         WHEN CURRENT_DATE - last_obs >= 7  THEN 'STALE_7_30'
         ELSE 'FRESH'
       END AS bucket
FROM data_freshness_universe u
LEFT JOIN LATERAL (
  SELECT MAX(obs_date) AS last_obs
  FROM ticker_metrics_daily t
  WHERE t.ticker = u.ticker AND t.close_price IS NOT NULL
) lat ON TRUE;

COMMIT;

SELECT bucket, COUNT(*) AS n FROM data_freshness_audit GROUP BY bucket ORDER BY n DESC;
