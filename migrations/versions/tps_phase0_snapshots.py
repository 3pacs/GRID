"""TPS Phase 0 — Trump-Proximity Score snapshots table.

Revision ID: tps_phase0_snapshots
Revises: phase4_actor_analytics
Create Date: 2026-05-16 00:00:00.000000

Creates the ``tps_snapshots`` table — one row per (ticker, as_of_date)
holding the frozen TPS score and its evidence list. Snapshots are
written by the daily 06:00 ET refresh in ``ingestion/scheduler.py`` and
read by ``api/routers/tps.py`` and the precision@10 backtest harness.

Schema design notes:
  * ``score`` is nullable — NULL means "insufficient upstream coverage"
    (every layer returned None). The TPS module never silently writes
    a default 0/1.0 — see ``intelligence/trump_proximity.py``.
  * ``evidence`` is JSONB so the per-layer breakdown is queryable for
    the precision@10 audit (which layer correlated with forward return?).
  * ``coverage`` records which of the five layers had data so we can
    surface a "low coverage" badge in the PWA.
"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "tps_phase0_snapshots"
down_revision: Union[str, Sequence[str], None] = "phase4_actor_analytics"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS tps_snapshots (
            id              BIGSERIAL PRIMARY KEY,
            ticker          TEXT NOT NULL,
            as_of_date      DATE NOT NULL,
            score           NUMERIC,                 -- 0-100, NULL if no coverage
            coverage        JSONB NOT NULL DEFAULT '{}'::jsonb,
            evidence        JSONB NOT NULL DEFAULT '[]'::jsonb,
            layer_scores    JSONB NOT NULL DEFAULT '{}'::jsonb,
            generated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (ticker, as_of_date)
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_tps_snapshots_as_of_score
            ON tps_snapshots (as_of_date DESC, score DESC NULLS LAST);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_tps_snapshots_ticker_date
            ON tps_snapshots (ticker, as_of_date DESC);
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_tps_snapshots_ticker_date;")
    op.execute("DROP INDEX IF EXISTS ix_tps_snapshots_as_of_score;")
    op.execute("DROP TABLE IF EXISTS tps_snapshots;")
