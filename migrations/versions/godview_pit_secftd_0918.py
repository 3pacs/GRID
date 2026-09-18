"""godview_pit_secftd — PIT/provenance columns for the SEC FTD God View pillar.

Revision ID: godview_pit_secftd_0918
Revises: godview_pit_finra_0918
Create Date: 2026-09-18 00:00:00.000000

Adds the same PIT/provenance column set as the other pillars to the
tracked ``sec_regsho_ftd_cns`` table (``god_view_market_tables_20260918``):
``release_date``, ``available_at``, ``provenance``, ``availability_basis``
(CHECK-constrained, see ``godview/availability_basis.py``), ``generation_id``,
``coverage_fraction``, ``source_ref``.

Deliberately NOT touched, and never written by
``godview/sec_ftd_pillar.py``: ``mandatory_buyin_date``, ``days_remaining``,
``squeeze_risk_score`` (already on the tracked table, from
``god_view_market_tables_20260918``) -- there is no T+35 forced-buy-in
timeline and no squeeze score here; see that module's docstring for why
(the SEC's own page: "Fails-to-deliver ... are not evidence of abusive
short selling or 'naked' short selling"). Those three columns stay
permanently NULL, never a fabricated date/count/score.

``closing_price`` and ``total_failed_usd`` ARE populated: the SEC FTD
file's own PRICE field (``measured``) and a same-settlement-date
``failed_shares * closing_price`` (``derived``) -- never a sum across
settlement dates.

All nullable and additive; no existing row touched; downgrade drops only
these columns.

NOTE for whoever merges the next packet: chains directly off
``godview_pit_finra_0918`` (this lane's own head), not ``research_leases_0918``
-- same reason as the sibling revisions' own NOTEs. Re-parent when it lands.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "godview_pit_secftd_0918"
down_revision: Union[str, Sequence[str], None] = "godview_pit_finra_0918"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_CHECK_NAME = "ck_sec_regsho_ftd_availability_basis"

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
            f"ALTER TABLE sec_regsho_ftd_cns "
            f"ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
        )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_sec_regsho_ftd_generation "
        "ON sec_regsho_ftd_cns (generation_id)"
    )
    op.execute(
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = '{_CHECK_NAME}'
            ) THEN
                ALTER TABLE sec_regsho_ftd_cns
                    ADD CONSTRAINT {_CHECK_NAME}
                    CHECK (availability_basis IN (
                        'observed_acquisition', 'inferred_schedule', 'unknown'
                    ));
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute(f"ALTER TABLE sec_regsho_ftd_cns DROP CONSTRAINT IF EXISTS {_CHECK_NAME}")
    op.execute("DROP INDEX IF EXISTS idx_sec_regsho_ftd_generation")
    for column_name, _ in reversed(_NEW_COLUMNS):
        op.execute(f"ALTER TABLE sec_regsho_ftd_cns DROP COLUMN IF EXISTS {column_name}")
