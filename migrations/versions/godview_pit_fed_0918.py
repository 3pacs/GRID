"""godview_pit_fed — PIT/provenance columns for the Fed net liquidity God View pillar (W6 Slice B).

Revision ID: godview_pit_fed_0918
Revises: godview_avail_basis_0918
Create Date: 2026-09-18 00:00:00.000000

Adds the columns the Fed net liquidity pillar needs on top of the tracked
``fed_net_liquidity_daily`` table created by
``god_view_market_tables_20260918`` (which already has the raw
WALCL/WTREGEN/RRPONTSYD component columns and ``net_liquidity_usd_m`` --
see docs/reference/GODVIEW_PILLAR_CONTRACT.md section 11 for the unit
confirmation: WALCL and WTREGEN are "Millions of U.S. Dollars" per FRED,
RRPONTSYD is "Billions of US Dollars" -- ``net_liquidity_usd_m`` implies
millions, so ``godview/fed_liquidity_pillar.py`` scales RRPONTSYD by 1000
before combining, which the pre-existing
``ingestion/altdata/fed_liquidity.py::COMPUTED:fed_net_liquidity`` does
NOT do -- flagged separately as its own bug, not fixed here).

Same shape as ``godview_pit_cftc_0918`` / ``godview_avail_basis_0918``,
PLUS three per-component pull timestamps this pillar needs that the CFTC
pillar didn't: "per-component availability basis and age" (operator
direction) requires knowing WHEN EACH of WALCL/WTREGEN/RRPONTSYD was
actually pulled, not just the earliest of the three (which is all a
single ``available_at`` column could tell you).

* ``release_date``            DATE            -- obs_date + 1 day (Thursday),
                                                   set only when obs_date is a
                                                   Wednesday (WALCL/WTREGEN's
                                                   own cadence); withheld
                                                   otherwise (quarantined from
                                                   strict-PIT reads).
* ``available_at``             TIMESTAMPTZ     -- earliest of the three
                                                   components' pull_timestamp
                                                   (row-level "first
                                                   acquisition", same
                                                   convention as the CFTC
                                                   pillar).
* ``walcl_pulled_at``           TIMESTAMPTZ     -- WALCL's own pull_timestamp.
* ``wtregen_pulled_at``         TIMESTAMPTZ     -- WTREGEN's own pull_timestamp.
* ``rrp_pulled_at``             TIMESTAMPTZ     -- RRPONTSYD's own pull_timestamp.
* ``provenance``                TEXT            -- coarse row tag ("measured").
* ``availability_basis``        TEXT            -- row-level, CHECK-constrained
                                                    to the same three values as
                                                    the CFTC pillar (see
                                                    godview/availability_basis.py).
* ``generation_id``             TEXT            -- uuid4 of the materializer run.
* ``coverage_fraction``         DOUBLE PRECISION -- derived-window coverage for
                                                    rrp_as_pct_of_peak.
* ``source_ref``                TEXT            -- audit string (H.4.1 release
                                                    rule, or the withholding
                                                    reason).

All nullable and additive (``ADD COLUMN IF NOT EXISTS``); no existing row
touched. Downgrade drops only these columns.

NOTE for whoever merges the next packet: chains directly off
``godview_avail_basis_0918`` (this lane's own head), not
``research_leases_0918`` as originally directed -- same reason as
``godview_avail_basis_0918``'s own NOTE: that revision is not present in
this worktree's ``migrations/versions/`` yet. Re-parent when it lands.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "godview_pit_fed_0918"
down_revision: Union[str, Sequence[str], None] = "godview_avail_basis_0918"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_CHECK_NAME = "ck_fed_net_liquidity_availability_basis"

_NEW_COLUMNS: list[tuple[str, str]] = [
    ("release_date", "DATE"),
    ("available_at", "TIMESTAMPTZ"),
    ("walcl_pulled_at", "TIMESTAMPTZ"),
    ("wtregen_pulled_at", "TIMESTAMPTZ"),
    ("rrp_pulled_at", "TIMESTAMPTZ"),
    ("provenance", "TEXT"),
    ("availability_basis", "TEXT"),
    ("generation_id", "TEXT"),
    ("coverage_fraction", "DOUBLE PRECISION"),
    ("source_ref", "TEXT"),
]


def upgrade() -> None:
    for column_name, column_type in _NEW_COLUMNS:
        op.execute(
            f"ALTER TABLE fed_net_liquidity_daily "
            f"ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
        )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_fed_net_liquidity_generation "
        "ON fed_net_liquidity_daily (generation_id)"
    )
    op.execute(
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = '{_CHECK_NAME}'
            ) THEN
                ALTER TABLE fed_net_liquidity_daily
                    ADD CONSTRAINT {_CHECK_NAME}
                    CHECK (availability_basis IN (
                        'observed_acquisition', 'inferred_schedule', 'unknown'
                    ));
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute(f"ALTER TABLE fed_net_liquidity_daily DROP CONSTRAINT IF EXISTS {_CHECK_NAME}")
    op.execute("DROP INDEX IF EXISTS idx_fed_net_liquidity_generation")
    for column_name, _ in reversed(_NEW_COLUMNS):
        op.execute(f"ALTER TABLE fed_net_liquidity_daily DROP COLUMN IF EXISTS {column_name}")
