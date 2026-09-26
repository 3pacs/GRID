"""capital_flows_ttm_state — durable per-actor TTM dirty tracking.

Revision ID: capital_flow_ttm_state_20260920
Revises: god_view_market_tables_20260918
Create Date: 2026-09-20 00:00:00.000000

fable-daily-intel-sql-tasks (2026-09-20, second follow-up): the controller
established that a scalar high-water-mark over ``capital_flows.as_of`` is
NOT commit-order safe — in PostgreSQL ``as_of = NOW()`` records
TRANSACTION START time, so a writer that starts before a rollup's snapshot
but commits after it can carry an ``as_of`` the rollup's persisted
watermark already passed, and ``as_of > watermark`` then skips that row
forever. Equal ``as_of`` timestamps have the same hole under strict ``>``.
Neither hole depends on how "recent" the row's own ``as_of`` looks, so no
scalar cursor over any single column can close them — see
``intelligence/company_financial_rollups.py``'s module docstring for the
full writeup and ``tests/test_capital_flow_rollups_pg.py`` for the two
concurrent-connection PG tests that prove the old design wrong.

Replacement: ``compute_ttm`` now decides which actors are "dirty" (need a
TTM recompute) by comparing a content fingerprint of each actor's current
``period_type='quarter'`` rows against the fingerprint stored here from
the last successful run — commit-order independent, because it compares
committed content, not a timestamp. This table is that durable per-actor
state: exactly one row per actor that has ever had a quarterly row,
updated in the SAME transaction as the TTM write it corresponds to.

``quarter_fingerprint`` is NULL for an actor whose quarterly rows have all
been deleted (not removed from this table) — so a future re-insertion is
still detected as a change from NULL, and a repeat run with no quarterly
rows at all stays stable (NULL vs NULL is "not distinct").
"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "capital_flow_ttm_state_20260920"
down_revision: Union[str, Sequence[str], None] = "god_view_market_tables_20260918"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS capital_flows_ttm_state (
            actor_id            TEXT PRIMARY KEY,
            quarter_fingerprint TEXT,
            computed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )

    # The raw-SQL migration series requires a GRANT footer on every new table
    # (migrations/_TEMPLATE.sql): alembic runs as the owner role, so a freshly
    # created table is unusable by the `grid` application role until it is
    # granted. Guarded on the role existing so a developer database without a
    # `grid` role still migrates.
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
                EXECUTE 'GRANT ALL ON capital_flows_ttm_state TO grid';
            END IF;
        END
        $$
        """
    )


def downgrade() -> None:
    # New table introduced by this migration — safe to drop outright, the
    # per-actor tracking state has no other reader/writer and losing it only
    # forces the next compute_ttm run to treat every actor as dirty once.
    op.execute("DROP TABLE IF EXISTS capital_flows_ttm_state")
