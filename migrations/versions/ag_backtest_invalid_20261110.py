"""Add astrogrid.backtest_run_invalidation: append-only invalid marks for backtest runs.

Revision ID: ag_backtest_invalid_20261110
Revises: pub_review_ingest_20261108
Create Date: 2026-11-10 02:30:00.000000

``astrogrid.backtest_run`` is append-only (``prevent_log_mutation``), so a run
whose result is known to be wrong cannot be edited to say so. The 242
``rotation_variant_*`` runs (2026-05-08..05-19, stored as ``grid_only``) read
``build_price_panel`` while the ``_full`` price panel mixed raw and adjusted
vintages (docs/reference/PRICE_PANEL_BASIS.md, repaired 2026-11-09/10 under
run tags ``full-basis-20261109`` and ``full-basis-20261110-nine``); their
Sharpe ~4.6 and +9,600 % are that artefact.

This revision changes no existing table. It adds
``astrogrid.backtest_run_invalidation``: one row per invalid run
(``UNIQUE (backtest_run_id)``), with a non-empty reason and run tag,
append-only like the other AstroGrid ledgers. ``store/astrogrid.py``
``list_backtest_results`` skips results of runs listed here; the runs and
their results stay as history.

It also records the 242 rotation runs, selected by run_key prefix
(``INSERT ... ON CONFLICT DO NOTHING``; a database without them inserts
nothing). Idempotent; downgrade refuses (the table holds append-only marks).
"""

from collections.abc import Sequence

from alembic import op

revision: str = "ag_backtest_invalid_20261110"
down_revision: str | Sequence[str] | None = "pub_review_ingest_20261108"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS astrogrid.backtest_run_invalidation (
    id                  BIGSERIAL PRIMARY KEY,
    backtest_run_id     BIGINT NOT NULL UNIQUE REFERENCES astrogrid.backtest_run(id),
    reason              TEXT NOT NULL CHECK (btrim(reason) <> ''),
    run_tag             TEXT NOT NULL CHECK (btrim(run_tag) <> ''),
    invalidated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    detail              JSONB NOT NULL DEFAULT '{}'::jsonb
)
"""

APPEND_ONLY_TRIGGER = """
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
        WHERE tgname = 'trg_astrogrid_backtest_run_invalidation_no_mutation'
          AND tgrelid = 'astrogrid.backtest_run_invalidation'::regclass
    ) THEN
        CREATE TRIGGER trg_astrogrid_backtest_run_invalidation_no_mutation
            BEFORE UPDATE OR DELETE ON astrogrid.backtest_run_invalidation
            FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();
    END IF;
END
$$
"""

MARK_ROTATION_RUNS = r"""
INSERT INTO astrogrid.backtest_run_invalidation (backtest_run_id, reason, run_tag, detail)
SELECT r.id,
       'built on the mixed-basis _full price panel (raw/adjusted vintage flips); '
       'Sharpe ~4.6 and +9,600 % are the artefact',
       'mixed-basis-panel-20261110',
       '{"ref": "docs/reference/PRICE_PANEL_BASIS.md", "repair_run_tags": ["full-basis-20261109", "full-basis-20261110-nine"]}'::jsonb
FROM astrogrid.backtest_run r
WHERE r.run_key LIKE 'rotation\_variant\_%'
ON CONFLICT (backtest_run_id) DO NOTHING
"""

# GRANT footer (migrations/_TEMPLATE.sql). Alembic runs as the owner role;
# the API connects as `grid`. Guarded for a developer database without it.
GRANTS = """
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
        EXECUTE 'GRANT SELECT, INSERT ON astrogrid.backtest_run_invalidation TO grid';
        EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE astrogrid.backtest_run_invalidation_id_seq TO grid';
    END IF;
END
$$
"""


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")
    op.execute(CREATE_TABLE)
    op.execute(APPEND_ONLY_TRIGGER)
    op.execute(MARK_ROTATION_RUNS)
    op.execute(GRANTS)


def downgrade() -> None:
    raise RuntimeError(
        "Downgrade through ag_backtest_invalid_20261110 is refused: the table "
        "holds append-only invalidation marks. Use a separately reviewed forward migration."
    )
