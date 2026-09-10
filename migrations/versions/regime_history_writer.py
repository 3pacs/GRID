"""regime_history — declare the table and guarantee the obs_date upsert key.

Revision ID: regime_history_writer
Revises: idle_fleet_goal_queue_day1
Create Date: 2026-09-10 00:00:00.000000

``regime_history`` has been read by the chat regime context, the oracle
prediction context, AstroGrid, the HMM transition model and the trial signal
since 2026-03, but it was never declared in ``schema.sql`` and nothing in the
repository ever wrote to it — the rows on griddb came from a one-off load and
stopped at 2026-03-29.

``scripts/auto_regime.py`` now writes it on every scheduled run and from its
``--backfill`` entry point, upserting on ``obs_date``. This migration makes the
table exist on a fresh database and guarantees the unique key that upsert needs
on databases where the table was created by hand without one.

It deliberately does not touch existing rows.
"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "regime_history_writer"
down_revision: Union[str, Sequence[str], None] = "idle_fleet_goal_queue_day1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Fresh databases: create the table as documented in the canonical regime
    # contract (.coordination.md, 2026-03-29). A no-op where it already exists.
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS regime_history (
            obs_date    DATE PRIMARY KEY,
            regime      TEXT NOT NULL,
            confidence  DOUBLE PRECISION,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )

    # Existing databases: the writer upserts with ON CONFLICT (obs_date), which
    # needs a unique index on that column. Add one only when the table has
    # neither a primary key nor a unique index covering obs_date, and only when
    # the existing rows actually are one-per-date — a duplicate obs_date means
    # the table was loaded with semantics this writer does not assume, and the
    # migration should surface that rather than quietly dropping rows.
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_index i
                JOIN pg_class c ON c.oid = i.indrelid
                JOIN pg_attribute a
                     ON a.attrelid = c.oid AND a.attnum = ANY (i.indkey)
                WHERE c.relname = 'regime_history'
                  AND i.indisunique
                  AND i.indnatts = 1
                  AND a.attname = 'obs_date'
            ) THEN
                IF EXISTS (
                    SELECT 1 FROM regime_history
                    GROUP BY obs_date HAVING count(*) > 1
                ) THEN
                    RAISE EXCEPTION
                        'regime_history has duplicate obs_date rows; '
                        'deduplicate before adding the unique key';
                END IF;
                CREATE UNIQUE INDEX IF NOT EXISTS uq_regime_history_obs_date
                    ON regime_history (obs_date);
            END IF;
        END
        $$
        """
    )


def downgrade() -> None:
    # The table predates this migration on every deployed database, so the
    # downgrade only removes the index this migration may have added. Dropping
    # the table would destroy history no other writer can rebuild.
    op.execute("DROP INDEX IF EXISTS uq_regime_history_obs_date")
