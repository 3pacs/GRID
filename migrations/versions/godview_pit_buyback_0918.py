"""godview_pit_buyback — issuer-level modeled quiet-window table for the buyback God View pillar.

Revision ID: godview_pit_buyback_0918
Revises: godview_pit_secftd_0918
Create Date: 2026-09-18 00:00:00.000000

Creates ``issuer_buyback_blackout_windows`` -- a NEW table, not an
extension of the tracked ``corporate_buyback_blackouts`` table
(``god_view_market_tables_20260918``). That table's grain is
market-WIDE, one row per calendar date (``sp500_cap_blackout_pct``,
``active_corporate_bid_m``) -- exactly the aggregate dollar/percent
figures the operator explicitly forbade ("NO dollar amounts, NO '% of
market in blackout' literals -- those need issuer repurchase
disclosures, which do not exist in the DB"). This pillar is per-ISSUER
and MODELED, a different grain entirely; forcing it into the market-wide
table's shape (or widening that table's unique constraint) would blur a
real measured-vs-assumed distinction, not preserve it. The tracked
``corporate_buyback_blackouts`` table is therefore left completely
untouched -- this pillar never writes to it.

``issuer_buyback_blackout_windows`` columns:

* ``ticker``               TEXT NOT NULL -- issuer, from ``earnings_calendar``.
* ``calendar_date``        DATE NOT NULL -- one row per (ticker, date) covered
                            by a modeled window.
* ``window_status``        TEXT NOT NULL CHECK IN ('quiet_window') -- only
                            dates INSIDE a modeled window get a row; there is
                            no 'open' row (absence already means "not in a
                            modeled window for this issuer" -- see contract
                            doc section 15 for why a sparse table, not a
                            dense calendar, is the honest representation).
* ``earnings_date_used``   DATE NOT NULL -- the ``earnings_calendar`` row this
                            window was derived from (traceability).
* ``window_start`` / ``window_end`` DATE NOT NULL -- the modeled window's
                            own boundaries.
* ``provenance``           TEXT NOT NULL -- always ``'modeled'``.
* ``availability_basis``   TEXT (CHECK, shared 3 values) -- always ``'unknown'``
                            here: this is a model, not a published data feed,
                            so "observed vs. inferred against a schedule" does
                            not apply.
* ``generation_id``, ``coverage_fraction``, ``source_ref`` -- standard.
* ``created_at``           TIMESTAMPTZ DEFAULT NOW().

``earnings_calendar`` (the source this pillar reads from) is itself a
lazily-created, untracked table (``ingestion/altdata/earnings_calendar.py::
_ensure_earnings_table``, not in ``schema.sql`` or any Alembic migration --
this codebase's established "app-level lazy table creation" pattern, per
30+ other modules). This migration does NOT create or touch
``earnings_calendar`` -- ``godview/buyback_pillar.py`` probes for it with
``to_regclass`` and degrades to unavailable if it is absent, same as every
other pillar's missing-table handling.

NOTE for whoever merges the next packet: chains directly off
``godview_pit_secftd_0918`` (this lane's own head), not
``research_leases_0918`` -- same reason as the sibling revisions. Re-parent
when it lands.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "godview_pit_buyback_0918"
down_revision: Union[str, Sequence[str], None] = "godview_pit_secftd_0918"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_CHECK_STATUS_NAME = "ck_issuer_buyback_window_status"
_CHECK_BASIS_NAME = "ck_issuer_buyback_availability_basis"


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS issuer_buyback_blackout_windows (
            id                  BIGSERIAL PRIMARY KEY,
            ticker              TEXT NOT NULL,
            calendar_date       DATE NOT NULL,
            window_status       TEXT NOT NULL,
            earnings_date_used  DATE NOT NULL,
            window_start        DATE NOT NULL,
            window_end          DATE NOT NULL,
            provenance          TEXT NOT NULL,
            availability_basis  TEXT,
            generation_id       TEXT,
            coverage_fraction   DOUBLE PRECISION,
            source_ref          TEXT,
            created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (ticker, calendar_date)
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_issuer_buyback_ticker_date "
        "ON issuer_buyback_blackout_windows (ticker, calendar_date DESC)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_issuer_buyback_generation "
        "ON issuer_buyback_blackout_windows (generation_id)"
    )
    op.execute(
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '{_CHECK_STATUS_NAME}') THEN
                ALTER TABLE issuer_buyback_blackout_windows
                    ADD CONSTRAINT {_CHECK_STATUS_NAME}
                    CHECK (window_status IN ('quiet_window'));
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '{_CHECK_BASIS_NAME}') THEN
                ALTER TABLE issuer_buyback_blackout_windows
                    ADD CONSTRAINT {_CHECK_BASIS_NAME}
                    CHECK (availability_basis IN (
                        'observed_acquisition', 'inferred_schedule', 'unknown'
                    ) OR availability_basis IS NULL);
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS issuer_buyback_blackout_windows")
