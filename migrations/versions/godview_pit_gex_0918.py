"""godview_pit_gex — PIT/provenance columns for the dealer GEX God View pillar.

Revision ID: godview_pit_gex_0918
Revises: godview_pit_buyback_0918
Create Date: 2026-09-18 00:00:00.000000

Adds the standard PIT/provenance columns to the tracked ``dealer_gex_daily``
table (``god_view_market_tables_20260918``): ``release_date``,
``available_at``, ``provenance``, ``availability_basis`` (CHECK,
``godview/availability_basis.py``), ``generation_id``, ``coverage_fraction``,
``source_ref``, plus ``contracts_used``/``contracts_present`` (the raw
counts behind ``coverage_fraction`` -- how many ``options_snapshots`` rows
had a usable IV and were actually used in the gamma calculation, vs. how
many rows existed for that ticker/date at all).

This engine (``godview/dealer_gex_pillar.py``) is a from-scratch
implementation over ``options_snapshots`` -- the untracked, incident-evidence
``derivatives/dealer_gex_engine.py`` in the sibling ``GRID`` checkout was
never read (per this lane's read-only-cross-branch rules, that file is
explicitly out of bounds).

All nullable and additive; no existing row touched; downgrade drops only
these columns.

NOTE for whoever merges the next packet: chains directly off
``godview_pit_buyback_0918`` (this lane's own head), not
``research_leases_0918`` -- same reason as the sibling revisions. This is
the last revision in this lane's chain as of 2026-09-18. Re-parent when
research_leases_0918 lands.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "godview_pit_gex_0918"
down_revision: Union[str, Sequence[str], None] = "godview_pit_buyback_0918"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_CHECK_NAME = "ck_dealer_gex_availability_basis"

_NEW_COLUMNS: list[tuple[str, str]] = [
    ("release_date", "DATE"),
    ("available_at", "TIMESTAMPTZ"),
    ("provenance", "TEXT"),
    ("availability_basis", "TEXT"),
    ("generation_id", "TEXT"),
    ("coverage_fraction", "DOUBLE PRECISION"),
    ("contracts_used", "INTEGER"),
    ("contracts_present", "INTEGER"),
    ("source_ref", "TEXT"),
]


def upgrade() -> None:
    for column_name, column_type in _NEW_COLUMNS:
        op.execute(
            f"ALTER TABLE dealer_gex_daily "
            f"ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
        )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_dealer_gex_generation "
        "ON dealer_gex_daily (generation_id)"
    )
    op.execute(
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '{_CHECK_NAME}') THEN
                ALTER TABLE dealer_gex_daily
                    ADD CONSTRAINT {_CHECK_NAME}
                    CHECK (availability_basis IN (
                        'observed_acquisition', 'inferred_schedule', 'unknown'
                    ));
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute(f"ALTER TABLE dealer_gex_daily DROP CONSTRAINT IF EXISTS {_CHECK_NAME}")
    op.execute("DROP INDEX IF EXISTS idx_dealer_gex_generation")
    for column_name, _ in reversed(_NEW_COLUMNS):
        op.execute(f"ALTER TABLE dealer_gex_daily DROP COLUMN IF EXISTS {column_name}")
