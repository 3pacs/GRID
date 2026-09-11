"""regime_history.data_as_of — carry the date the label's inputs actually came from.

Revision ID: regime_history_data_as_of
Revises: regime_history_writer
Create Date: 2026-09-11 00:00:00.000000

``regime_history.obs_date`` is the date the row was computed *for*, which for a
scheduled run is always today. That is not the same thing as the date the data
behind the label came from, and while the resolver has been down the two have
been five months apart: the 2026-09-10 backfill wrote 160 rows carrying the
same reading (S 0.5425, dS -0.1056, NEUTRAL 0.4287) because every ``as_of``
from 2026-04-15 onwards saw the same point-in-time frame, which ends 2026-04-03.
Every one of those rows looked fresh — ``/api/v1/regime/current`` reported
``staleness_days: 0``.

``data_as_of`` is the newest real observation in the frame the label was
computed from, measured before any forward-fill, so a reader can tell a fresh
read from a stale one wearing today's date. Nullable: the rows already on
griddb (the 2026-03 one-off load and the 2026-09-10 backfill) cannot have it
reconstructed, and a guess would be the same lie in a new column.
"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "regime_history_data_as_of"
down_revision: Union[str, Sequence[str], None] = "regime_history_writer"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Nullable and IF NOT EXISTS: additive on every deployed database, and a
    # no-op on one that already has the column.
    op.execute(
        "ALTER TABLE regime_history ADD COLUMN IF NOT EXISTS data_as_of DATE"
    )

    # Same guard schema.sql declares: a row can never claim its inputs came
    # from after the day it describes. Added only when absent — Postgres has no
    # ADD CONSTRAINT IF NOT EXISTS — and it validates against the existing rows
    # because every one of them has data_as_of NULL.
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'ck_regime_history_data_as_of'
            ) THEN
                ALTER TABLE regime_history
                    ADD CONSTRAINT ck_regime_history_data_as_of
                    CHECK (data_as_of IS NULL OR data_as_of <= obs_date);
            END IF;
        END
        $$
        """
    )

    # GRANT footer (migrations/_TEMPLATE.sql). Alembic runs as the owner role;
    # the API and the scheduler connect as `grid`. Re-granting on an existing
    # table is a no-op, and the guard keeps a developer database without a
    # `grid` role migrating cleanly.
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
                EXECUTE 'GRANT ALL ON regime_history TO grid';
            END IF;
        END
        $$
        """
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE regime_history "
        "DROP CONSTRAINT IF EXISTS ck_regime_history_data_as_of"
    )
    op.execute("ALTER TABLE regime_history DROP COLUMN IF EXISTS data_as_of")
