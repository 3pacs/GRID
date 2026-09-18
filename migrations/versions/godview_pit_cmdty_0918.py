"""godview_pit_cmdty — PIT/provenance columns for the commodity warehouse God View pillar (W6 Slice B).

Revision ID: godview_pit_cmdty_0918
Revises: godview_pit_fed_0918
Create Date: 2026-09-18 00:00:00.000000

Adds the same PIT/provenance column set as
``godview_pit_cftc_0918``/``godview_pit_fed_0918`` to the tracked
``commodity_warehouse_inventories`` table (``god_view_market_tables_20260918``):
``release_date``, ``available_at``, ``provenance``, ``availability_basis``
(CHECK-constrained to the shared three values, see
``godview/availability_basis.py``), ``generation_id``, ``coverage_fraction``,
``source_ref``. Index on ``generation_id``.

Note this pillar (see ``godview/commodity_warehouse_pillar.py``'s module
docstring): no official LME warehouse-stocks publication schedule was
found to cite, so ``release_date`` is always NULL and
``availability_basis`` is always ``'unknown'`` for every row this
materializer writes -- these columns are still added for schema
consistency with the other two pillars and so a FUTURE pillar
sharing this table (e.g. a real Cushing series, if one is ever wired up)
can use a real schedule-based rule without another migration.

All nullable and additive; no existing row touched; downgrade drops only
these columns.

NOTE for whoever merges the next packet: chains directly off
``godview_pit_fed_0918`` (this lane's own head), not ``research_leases_0918``
-- same reason as that revision's own NOTE. Re-parent when it lands.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "godview_pit_cmdty_0918"
down_revision: Union[str, Sequence[str], None] = "godview_pit_fed_0918"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_CHECK_NAME = "ck_commodity_warehouse_availability_basis"

_NEW_COLUMNS: list[tuple[str, str]] = [
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
            f"ALTER TABLE commodity_warehouse_inventories "
            f"ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
        )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_commodity_warehouse_generation "
        "ON commodity_warehouse_inventories (generation_id)"
    )
    op.execute(
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = '{_CHECK_NAME}'
            ) THEN
                ALTER TABLE commodity_warehouse_inventories
                    ADD CONSTRAINT {_CHECK_NAME}
                    CHECK (availability_basis IN (
                        'observed_acquisition', 'inferred_schedule', 'unknown'
                    ));
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute(f"ALTER TABLE commodity_warehouse_inventories DROP CONSTRAINT IF EXISTS {_CHECK_NAME}")
    op.execute("DROP INDEX IF EXISTS idx_commodity_warehouse_generation")
    for column_name, _ in reversed(_NEW_COLUMNS):
        op.execute(f"ALTER TABLE commodity_warehouse_inventories DROP COLUMN IF EXISTS {column_name}")
