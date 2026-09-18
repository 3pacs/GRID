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

**2026-09-18 fix (real-Postgres run 4, composition 0f0451aa):**
``god_view_market_tables_20260918`` declares ``mandatory_buyin_date DATE
NOT NULL`` -- checked, ``days_remaining``/``squeeze_risk_score`` were
already nullable there (``INTEGER``/``DOUBLE PRECISION`` with no NOT NULL),
so only ``mandatory_buyin_date`` needed relaxing. This pillar deliberately
writes NULL into all three (no T+35 timeline, no squeeze score -- see the
module docstring), so ``upgrade()`` now also
``ALTER COLUMN mandatory_buyin_date DROP NOT NULL`` -- the real defect
this fixes: ``materialize_sec_ftd_pillar`` returned FAILED with
``psycopg2.errors.NotNullViolation`` on every real-Postgres run.

All nullable and additive; no existing row touched; downgrade drops only
the added columns and DELIBERATELY DOES NOT restore
``mandatory_buyin_date``'s NOT NULL constraint -- see ``downgrade()``'s own
comment for why (this pillar's own rows hold NULL there by design; blindly
re-adding the constraint would either fail outright on a table with live
rows or require deleting this pillar's own data, and the safer choice is
to leave the column nullable and log why rather than risk either).

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
    # 2026-09-18 fix: godview/sec_ftd_pillar.py deliberately writes NULL into
    # mandatory_buyin_date (no T+35 forced-buy-in timeline is computed here --
    # see that module's docstring) but god_view_market_tables_20260918
    # declared it NOT NULL, so every real-Postgres materializer run failed
    # with psycopg2.errors.NotNullViolation. days_remaining and
    # squeeze_risk_score were checked too and were already nullable in that
    # same CREATE TABLE -- only this one column needed relaxing.
    op.execute(
        "ALTER TABLE sec_regsho_ftd_cns ALTER COLUMN mandatory_buyin_date DROP NOT NULL"
    )


def downgrade() -> None:
    op.execute(f"ALTER TABLE sec_regsho_ftd_cns DROP CONSTRAINT IF EXISTS {_CHECK_NAME}")
    op.execute("DROP INDEX IF EXISTS idx_sec_regsho_ftd_generation")
    for column_name, _ in reversed(_NEW_COLUMNS):
        op.execute(f"ALTER TABLE sec_regsho_ftd_cns DROP COLUMN IF EXISTS {column_name}")
    # Deliberately does NOT restore `mandatory_buyin_date`'s NOT NULL
    # constraint. By the time downgrade() runs, this pillar may have
    # written rows with mandatory_buyin_date = NULL by design (never a
    # fabricated buy-in date) -- blindly re-adding NOT NULL would raise on
    # any such row, and deleting this pillar's own data just to satisfy a
    # downgrade is a worse trade than leaving the column nullable. Chose
    # "skip with a logged note" over "delete rows whose generation_id is
    # set": this table's rows are otherwise real, PIT-relevant observations
    # and deleting them is not a side effect a downgrade should have.
    op.execute(
        """
        DO $$
        BEGIN
            RAISE NOTICE 'godview_pit_secftd_0918 downgrade: leaving '
                'sec_regsho_ftd_cns.mandatory_buyin_date NULLABLE -- this '
                'pillar writes NULL there by design and rows already '
                'materialized would violate a restored NOT NULL constraint; '
                'not deleting data to force it back on';
        END $$;
        """
    )
