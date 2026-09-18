"""godview_pit_finra — PIT/provenance columns for the FINRA short-volume God View pillar.

Revision ID: godview_pit_finra_0918
Revises: godview_pit_cmdty_0918
Create Date: 2026-09-18 00:00:00.000000

Adds the same PIT/provenance column set as the other pillars to the
tracked ``finra_short_volume_daily`` table (``god_view_market_tables_20260918``):
``release_date``, ``available_at``, ``provenance``, ``availability_basis``
(CHECK-constrained, see ``godview/availability_basis.py``), ``generation_id``,
``coverage_fraction``, ``source_ref``, plus ``market`` (the FINRA
reporting-facility code(s) as reported in the source file -- e.g. ``"Q"``
or the real CNMS file's observed comma-joined ``"B,Q,N"`` -- informational
only; the tracked table's own ``UNIQUE (trade_date, ticker)`` is unchanged,
so this column does not participate in uniqueness).

``short_ratio`` (already on the tracked table) is short_volume / total_volume
for that trade date -- see godview/finra_short_volume_pillar.py's module
docstring for the explicit statement that this is short-sale VOLUME, never
short INTEREST, and never a squeeze score.

All nullable and additive; no existing row touched; downgrade drops only
these columns.

NOTE for whoever merges the next packet: chains directly off
``godview_pit_cmdty_0918`` (this lane's own head), not ``research_leases_0918``
as directed -- that revision is not present in this worktree's
``migrations/versions/`` yet. Re-parent when it lands.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "godview_pit_finra_0918"
down_revision: Union[str, Sequence[str], None] = "godview_pit_cmdty_0918"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_CHECK_NAME = "ck_finra_short_volume_availability_basis"

_NEW_COLUMNS: list[tuple[str, str]] = [
    ("market", "TEXT"),
    ("release_date", "DATE"),
    ("available_at", "TIMESTAMPTZ"),
    ("provenance", "TEXT"),
    ("availability_basis", "TEXT"),
    ("generation_id", "TEXT"),
    ("coverage_fraction", "DOUBLE PRECISION"),
    ("source_ref", "TEXT"),
]


def upgrade() -> None:
    for column_name, column_type in _NEW_COLUMNS:
        op.execute(
            f"ALTER TABLE finra_short_volume_daily "
            f"ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
        )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_finra_short_volume_generation "
        "ON finra_short_volume_daily (generation_id)"
    )
    op.execute(
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = '{_CHECK_NAME}'
            ) THEN
                ALTER TABLE finra_short_volume_daily
                    ADD CONSTRAINT {_CHECK_NAME}
                    CHECK (availability_basis IN (
                        'observed_acquisition', 'inferred_schedule', 'unknown'
                    ));
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute(f"ALTER TABLE finra_short_volume_daily DROP CONSTRAINT IF EXISTS {_CHECK_NAME}")
    op.execute("DROP INDEX IF EXISTS idx_finra_short_volume_generation")
    for column_name, _ in reversed(_NEW_COLUMNS):
        op.execute(f"ALTER TABLE finra_short_volume_daily DROP COLUMN IF EXISTS {column_name}")
