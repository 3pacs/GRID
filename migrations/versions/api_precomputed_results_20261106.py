"""Precomputed API results for the slow read endpoints (W3-2).

Revision ID: api_precomputed_results_20261106
Revises: oracle_cal_exploratory_20261106

One row per (endpoint key, parameter-set hash). ``intelligence/api_precompute.py``
(ticked every 5 min from the intelligence scheduler) upserts the latest
result; the endpoint reads it by primary key (``api/precompute_serving.py``)
and never computes inline. See ``store/precomputed.py`` for the column
contract: ``payload`` is NULL until the first successful run, ``as_of`` is the
data date (not the wall clock), and a failed / unavailable attempt only
updates ``last_attempt_at`` / ``last_status`` / ``last_error`` so a good
payload is never replaced by a failure.

Latest-only by design (an upsert cache, not history): the rows are derived
from PIT reads that can be re-run; ``analytical_snapshots`` keeps history
where a view needs it.

Idempotent (``IF NOT EXISTS``); GRANT footer for the ``grid`` role. Applied by
``python3 -m alembic upgrade head`` in deploy.yml.
"""

from alembic import op

revision = "api_precomputed_results_20261106"
down_revision = "oracle_cal_exploratory_20261106"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '60s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS api_precomputed_results (
            key              TEXT NOT NULL,
            params_hash      TEXT NOT NULL,
            params           JSONB NOT NULL DEFAULT '{}'::jsonb,
            payload          JSONB,
            computed_at      TIMESTAMPTZ,
            as_of            TIMESTAMPTZ,
            version          TEXT,
            inputs           JSONB,
            duration_s       DOUBLE PRECISION,
            last_attempt_at  TIMESTAMPTZ,
            last_status      TEXT CHECK (last_status IN ('ok', 'error', 'unavailable')),
            last_error       TEXT,
            PRIMARY KEY (key, params_hash),
            CHECK ((payload IS NULL) = (computed_at IS NULL))
        )
    """)
    # ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======
    op.execute("GRANT ALL ON api_precomputed_results TO grid;")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS api_precomputed_results;")
