"""Additive, append-only corrected outcomes for signal_sources and decision_journal.

Revision ID: outcome_rescore_tables_20261106
Revises: ag_legacy_rescore_20261105

Neither source table is touched. ``signal_sources.outcome`` (the legacy
everything-not-CORRECT-is-WRONG rule) and ``decision_journal.verdict`` (2,361
rows frozen INSUFFICIENT_DATA by the stale ``sp500`` read, 2026-03..10) stay
exactly as written. Corrected results are new rows keyed by a version:

* ``signal_source_outcomes`` -- one row per ``(signal_source_id,
  evaluation_version)``, written by ``evaluation.signal_outcomes``
  (``replay_signal_sources --apply`` and the gated ``score_forward``).
* ``decision_journal_verdicts`` -- one row per ``(decision_id,
  scorer_version)``, written by ``scripts/backfill_journal_verdicts.py
  --rescore-legacy --apply``.

Both tables reject UPDATE, DELETE (row triggers) and TRUNCATE (statement
trigger): a correction is a new version, never an edit. GRANT footer per
``migrations/_TEMPLATE.sql``, narrowed to SELECT, INSERT for ``grid``.
"""

from alembic import op

revision = "outcome_rescore_tables_20261106"
down_revision = "ag_legacy_rescore_20261105"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '60s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS signal_source_outcomes (
            id                   BIGSERIAL PRIMARY KEY,
            signal_source_id     INTEGER NOT NULL REFERENCES signal_sources(id),
            evaluation_version   TEXT NOT NULL,
            source_type          TEXT NOT NULL,
            ticker               TEXT NOT NULL,
            signal_date          DATE NOT NULL,
            direction            TEXT NOT NULL
                                 CHECK (direction IN ('BUY', 'SELL', 'NEUTRAL', 'UNKNOWN')),
            horizon_days         INTEGER NOT NULL CHECK (horizon_days > 0),
            outcome              TEXT NOT NULL
                                 CHECK (outcome IN ('CORRECT', 'WRONG', 'NO_MOVE', 'INELIGIBLE')),
            eligibility_reason   TEXT,
            dead_band_pct        DOUBLE PRECISION NOT NULL,
            cost_bps             DOUBLE PRECISION NOT NULL,
            entry_price          DOUBLE PRECISION,
            entry_price_date     DATE,
            exit_price           DOUBLE PRECISION,
            exit_price_date      DATE,
            raw_return           DOUBLE PRECISION
                                 CHECK (raw_return IS NULL OR raw_return < 'Infinity'::float8
                                        AND raw_return > '-Infinity'::float8),
            cost_adjusted_return DOUBLE PRECISION,
            price_basis          TEXT,
            known_at_source      TEXT,
            legacy_outcome       TEXT,
            evaluated_at         TIMESTAMPTZ NOT NULL,
            created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (signal_source_id, evaluation_version),
            CHECK ((outcome = 'INELIGIBLE') = (eligibility_reason IS NOT NULL))
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_signal_source_outcomes_version_outcome
        ON signal_source_outcomes (evaluation_version, outcome)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_signal_source_outcomes_source
        ON signal_source_outcomes (source_type, evaluation_version)
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS decision_journal_verdicts (
            id                     BIGSERIAL PRIMARY KEY,
            decision_id            BIGINT NOT NULL REFERENCES decision_journal(id),
            scorer_version         TEXT NOT NULL,
            verdict                TEXT NOT NULL
                                   CHECK (verdict IN ('HELPED', 'HARMED', 'NEUTRAL', 'INSUFFICIENT_DATA')),
            outcome_value          DOUBLE PRECISION NOT NULL
                                   CHECK (outcome_value < 'Infinity'::float8
                                          AND outcome_value > '-Infinity'::float8),
            forward_return         DOUBLE PRECISION,
            benchmark_series       TEXT,
            entry_obs_date         DATE,
            entry_value            DOUBLE PRECISION,
            entry_pull_timestamp   TIMESTAMPTZ,
            entry_known_at_decision BOOLEAN,
            exit_obs_date          DATE,
            exit_value             DOUBLE PRECISION,
            exit_pull_timestamp    TIMESTAMPTZ,
            legacy_verdict         TEXT,
            annotation             TEXT NOT NULL,
            evaluated_at           TIMESTAMPTZ NOT NULL,
            created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (decision_id, scorer_version)
        )
    """)
    op.execute("""
        CREATE OR REPLACE FUNCTION grid_reject_outcome_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION '% is append-only: % refused (add a new version instead)',
                TG_TABLE_NAME, TG_OP USING ERRCODE = 'restrict_violation';
        END
        $$
    """)
    op.execute("DROP TRIGGER IF EXISTS trg_signal_source_outcomes_append_only ON signal_source_outcomes")
    op.execute("""
        CREATE TRIGGER trg_signal_source_outcomes_append_only
        BEFORE UPDATE OR DELETE ON signal_source_outcomes
        FOR EACH ROW EXECUTE FUNCTION grid_reject_outcome_mutation()
    """)
    op.execute("DROP TRIGGER IF EXISTS trg_signal_source_outcomes_no_truncate ON signal_source_outcomes")
    op.execute("""
        CREATE TRIGGER trg_signal_source_outcomes_no_truncate
        BEFORE TRUNCATE ON signal_source_outcomes
        FOR EACH STATEMENT EXECUTE FUNCTION grid_reject_outcome_mutation()
    """)
    op.execute("DROP TRIGGER IF EXISTS trg_decision_journal_verdicts_append_only ON decision_journal_verdicts")
    op.execute("""
        CREATE TRIGGER trg_decision_journal_verdicts_append_only
        BEFORE UPDATE OR DELETE ON decision_journal_verdicts
        FOR EACH ROW EXECUTE FUNCTION grid_reject_outcome_mutation()
    """)
    op.execute("DROP TRIGGER IF EXISTS trg_decision_journal_verdicts_no_truncate ON decision_journal_verdicts")
    op.execute("""
        CREATE TRIGGER trg_decision_journal_verdicts_no_truncate
        BEFORE TRUNCATE ON decision_journal_verdicts
        FOR EACH STATEMENT EXECUTE FUNCTION grid_reject_outcome_mutation()
    """)

    # GRANT footer (migrations/_TEMPLATE.sql): alembic runs as the owner role;
    # the `grid` application role needs the tables and their sequences.
    # Append-only is also a privilege (options_digest_calls_20261104 pattern):
    # grid gets SELECT and INSERT only -- no UPDATE / DELETE / TRUNCATE.
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
                EXECUTE 'GRANT SELECT, INSERT ON signal_source_outcomes TO grid';
                EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE signal_source_outcomes_id_seq TO grid';
                EXECUTE 'GRANT SELECT, INSERT ON decision_journal_verdicts TO grid';
                EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE decision_journal_verdicts_id_seq TO grid';
            END IF;
        END
        $$
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DROP TABLE IF EXISTS decision_journal_verdicts")
    op.execute("DROP TABLE IF EXISTS signal_source_outcomes")
    op.execute("DROP FUNCTION IF EXISTS grid_reject_outcome_mutation()")
