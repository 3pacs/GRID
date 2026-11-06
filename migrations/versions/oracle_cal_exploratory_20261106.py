"""Append-only, exploratory oracle recalibration + conformal results (S10).

Revision ID: oracle_cal_exploratory_20261106
Revises: outcome_rescore_tables_20261106

``oracle.calibration.run_exploratory_recalibration`` (daily, intelligence
scheduler) writes one row per (as_of, method_version, scope, group_key):
expiry-clustered reliability curves globally and per horizon / model family /
score source, plus one ``scope = 'run'`` row holding the held-out
before/after calibration report, split and adaptive conformal coverage, and
the conformal state a reader needs to build an interval for a new call.

It is a separate table on purpose. ``oracle_calibration_history`` is the live
drift baseline (``detect_calibration_drift`` compares ``oracle_models``
against it); exploratory numbers built partly from hindsight rescores must
never land there. Every row is pinned ``score_class = 'exploratory'`` and
``official = FALSE`` by CHECK constraint, and the table rejects UPDATE,
DELETE and TRUNCATE through ``grid_reject_outcome_mutation()`` (created by
``outcome_rescore_tables_20261106``). ``grid`` gets SELECT and INSERT only.
"""

from alembic import op

revision = "oracle_cal_exploratory_20261106"
down_revision = "outcome_rescore_tables_20261106"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '60s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS oracle_calibration_exploratory (
            id              BIGSERIAL PRIMARY KEY,
            as_of           DATE NOT NULL,
            method_version  TEXT NOT NULL,
            scope           TEXT NOT NULL
                            CHECK (scope IN ('run', 'global', 'horizon', 'family', 'source')),
            group_key       TEXT NOT NULL,
            target          TEXT NOT NULL,
            n_rows          INTEGER,
            n_clusters      INTEGER,
            n_eff           DOUBLE PRECISION,
            base_rate       DOUBLE PRECISION,
            brier           DOUBLE PRECISION,
            brier_lo        DOUBLE PRECISION,
            brier_hi        DOUBLE PRECISION,
            ece             DOUBLE PRECISION,
            ece_lo          DOUBLE PRECISION,
            ece_hi          DOUBLE PRECISION,
            payload         JSONB NOT NULL,
            score_class     TEXT NOT NULL DEFAULT 'exploratory'
                            CHECK (score_class = 'exploratory'),
            official        BOOLEAN NOT NULL DEFAULT FALSE CHECK (official = FALSE),
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (as_of, method_version, scope, group_key)
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_oracle_calibration_exploratory_run
        ON oracle_calibration_exploratory (method_version, scope, as_of DESC)
    """)
    op.execute(
        "DROP TRIGGER IF EXISTS trg_oracle_calibration_exploratory_append_only "
        "ON oracle_calibration_exploratory"
    )
    op.execute("""
        CREATE TRIGGER trg_oracle_calibration_exploratory_append_only
        BEFORE UPDATE OR DELETE ON oracle_calibration_exploratory
        FOR EACH ROW EXECUTE FUNCTION grid_reject_outcome_mutation()
    """)
    op.execute(
        "DROP TRIGGER IF EXISTS trg_oracle_calibration_exploratory_no_truncate "
        "ON oracle_calibration_exploratory"
    )
    op.execute("""
        CREATE TRIGGER trg_oracle_calibration_exploratory_no_truncate
        BEFORE TRUNCATE ON oracle_calibration_exploratory
        FOR EACH STATEMENT EXECUTE FUNCTION grid_reject_outcome_mutation()
    """)
    # GRANT footer (migrations/_TEMPLATE.sql), narrowed to SELECT, INSERT:
    # append-only is also a privilege.
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
                EXECUTE 'GRANT SELECT, INSERT ON oracle_calibration_exploratory TO grid';
                EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE oracle_calibration_exploratory_id_seq TO grid';
            END IF;
        END
        $$
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DROP TABLE IF EXISTS oracle_calibration_exploratory")
