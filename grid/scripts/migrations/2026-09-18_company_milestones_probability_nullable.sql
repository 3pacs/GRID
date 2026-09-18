-- ============================================================
-- company_milestones.probability: NULL means UNSCORED
-- ============================================================
-- Companion to add_valuation_tables.sql, which created the table outside
-- alembic with:
--
--     probability DOUBLE PRECISION NOT NULL DEFAULT 0.5
--
-- That default wrote a coin-flip prior into every row nobody scored, so a
-- stated 0.5 and an unscored milestone were indistinguishable
-- (docs/reference/CONFIDENCE_POLICY.md, cluster C4). This drops the
-- constraint and the default. A stated probability must name its basis in
-- confidence_source; the Python writer enforces that.
--
-- NOT APPLIED. The production-drift incident hold (2026-09-18) forbids
-- migrations. Apply by hand after the hold lifts, or fold into the release
-- alembic chain (main currently has four alembic heads, so this is not
-- added as a revision here). Until it is applied:
--   * MilestoneTracker.add() with probability=None fails on NOT NULL — the
--     API returns status "error" rather than inventing 0.5;
--   * scripts/populate_milestones.py checks is_nullable once per run and
--     skips (counts) unscored rows instead of writing them.
--
-- Existing rows: a stored 0.5 cannot be told apart from the old default,
-- so no data is rewritten. Rows whose confidence_source is NULL and whose
-- probability is exactly 0.5 are the likeliest defaults; a later cleanup
-- can NULL them out with the operator's sign-off.

BEGIN;

ALTER TABLE company_milestones
    ALTER COLUMN probability DROP NOT NULL,
    ALTER COLUMN probability DROP DEFAULT;

-- The original CHECK (probability >= 0 AND probability <= 1) already passes
-- NULL (three-valued logic), so it stays as is.

COMMIT;
