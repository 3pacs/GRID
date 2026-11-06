"""Add append-only tables for legacy crypto rescores and bounded scorer skips.

Revision ID: ag_legacy_rescore_20261105
Revises: finra_short_volume_agg_20261105
Create Date: 2026-11-05 23:30:00.000000

Until PR #968, ``scripts/generate_crypto_predictions.py`` stored
``target_symbols`` as a list of characters (``['[', '"', 'B', 'T', 'C', '"', ']']``).
The scorer before 766d3058 fell back to SPY for those rows, so 31,329
``prediction_score`` rows grade crypto calls against SPY
(``raw_payload->'symbols' = ["SPY"]``). Those rows are immutable and stay as
they are; readers exclude them (``store/astrogrid.py``), and the legacy
predictions are scored again against the coin.

``prediction_score`` allows one row per prediction (``UNIQUE
(prediction_run_id)``), so a second score cannot go there without changing
that constraint. This revision changes no existing table. It adds:

* ``astrogrid.prediction_score_legacy_rescore``: one coin-priced score per
  legacy prediction (``UNIQUE (prediction_run_id)``), the same score columns
  as ``prediction_score`` plus ``score_version``, the decoded
  ``target_symbols`` and ``superseded_score_id`` (the excluded SPY score, if
  any). Append-only trigger like the other AstroGrid ledgers.
* ``astrogrid.prediction_score_skip``: one row per (prediction, attempt day)
  when the scorer cannot score a matured non-SPY prediction, so it stops
  re-reading the same rows every batch. Append-only.

Idempotent: ``CREATE ... IF NOT EXISTS`` and triggers created only when
missing, so a re-run is a no-op. Downgrade refuses: the tables hold
append-only score records.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "ag_legacy_rescore_20261105"
down_revision: str | Sequence[str] | None = "finra_short_volume_agg_20261105"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

CREATE_RESCORE = """
CREATE TABLE IF NOT EXISTS astrogrid.prediction_score_legacy_rescore (
    id                          BIGSERIAL PRIMARY KEY,
    prediction_run_id           BIGINT NOT NULL UNIQUE REFERENCES astrogrid.prediction_run(id),
    score_version               TEXT NOT NULL,
    superseded_score_id         BIGINT REFERENCES astrogrid.prediction_score(id),
    target_symbols              JSONB NOT NULL,
    scored_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    benchmark_symbol            TEXT,
    realized_return             DOUBLE PRECISION,
    benchmark_return            DOUBLE PRECISION,
    alpha_vs_benchmark          DOUBLE PRECISION,
    verdict                     TEXT CHECK (verdict IN ('hit', 'miss', 'partial', 'invalidated', 'expired')),
    invalidation_status         TEXT CHECK (invalidation_status IN ('respected', 'violated', 'not_triggered', 'unknown')),
    max_favorable_excursion     DOUBLE PRECISION,
    max_adverse_excursion       DOUBLE PRECISION,
    regime_context              JSONB NOT NULL DEFAULT '{}'::jsonb,
    attribution_grid            JSONB NOT NULL DEFAULT '[]'::jsonb,
    attribution_mystical        JSONB NOT NULL DEFAULT '[]'::jsonb,
    attribution_noise           JSONB NOT NULL DEFAULT '[]'::jsonb,
    raw_payload                 JSONB NOT NULL DEFAULT '{}'::jsonb
)
"""

CREATE_SKIP = """
CREATE TABLE IF NOT EXISTS astrogrid.prediction_score_skip (
    id                          BIGSERIAL PRIMARY KEY,
    prediction_run_id           BIGINT NOT NULL REFERENCES astrogrid.prediction_run(id),
    attempt_date                DATE NOT NULL,
    reason                      TEXT NOT NULL,
    permanent                   BOOLEAN NOT NULL DEFAULT FALSE,
    detail                      JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (prediction_run_id, attempt_date)
)
"""

INDEXES = (
    """
    CREATE INDEX IF NOT EXISTS idx_astrogrid_prediction_score_legacy_rescore_scored
        ON astrogrid.prediction_score_legacy_rescore (scored_at DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_astrogrid_prediction_score_skip_run
        ON astrogrid.prediction_score_skip (prediction_run_id, attempt_date DESC)
    """,
)

APPEND_ONLY_TRIGGERS = """
DO $$
BEGIN
    -- astrogrid.prevent_log_mutation() comes from b6bba10f0fdb; a scratch
    -- schema without it (unit-test fixtures) just gets no trigger.
    IF NOT EXISTS (
        SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'astrogrid' AND p.proname = 'prevent_log_mutation'
    ) THEN
        RETURN;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'trg_astrogrid_prediction_score_legacy_rescore_no_mutation'
          AND tgrelid = 'astrogrid.prediction_score_legacy_rescore'::regclass
    ) THEN
        CREATE TRIGGER trg_astrogrid_prediction_score_legacy_rescore_no_mutation
            BEFORE UPDATE OR DELETE ON astrogrid.prediction_score_legacy_rescore
            FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'trg_astrogrid_prediction_score_skip_no_mutation'
          AND tgrelid = 'astrogrid.prediction_score_skip'::regclass
    ) THEN
        CREATE TRIGGER trg_astrogrid_prediction_score_skip_no_mutation
            BEFORE UPDATE OR DELETE ON astrogrid.prediction_score_skip
            FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();
    END IF;
END
$$
"""

# GRANT footer (migrations/_TEMPLATE.sql). Alembic runs as the owner role;
# the API, the scheduler and the AstroGrid learning loop connect as `grid`.
# Re-granting is a no-op, and the guard keeps a developer database without a
# `grid` role migrating cleanly.
GRANTS = """
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
        EXECUTE 'GRANT SELECT, INSERT ON astrogrid.prediction_score_legacy_rescore TO grid';
        EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE astrogrid.prediction_score_legacy_rescore_id_seq TO grid';
        EXECUTE 'GRANT SELECT, INSERT ON astrogrid.prediction_score_skip TO grid';
        EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE astrogrid.prediction_score_skip_id_seq TO grid';
    END IF;
END
$$
"""


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")
    op.execute(CREATE_RESCORE)
    op.execute(CREATE_SKIP)
    for statement in INDEXES:
        op.execute(statement)
    op.execute(APPEND_ONLY_TRIGGERS)
    op.execute(GRANTS)


def downgrade() -> None:
    raise RuntimeError(
        "Downgrade through ag_legacy_rescore_20261105 is refused: the tables "
        "hold append-only score records. Use a separately reviewed forward migration."
    )
