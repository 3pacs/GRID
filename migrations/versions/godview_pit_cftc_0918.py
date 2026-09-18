"""godview_pit_cftc — PIT/provenance columns for the CFTC God View pillar (W6).

Revision ID: godview_pit_cftc_0918
Revises: promotion_ledger_0918
Create Date: 2026-09-18 00:00:00.000000

Adds the columns the CFTC positioning God View pillar needs on top of the
tracked ``cftc_positioning_daily`` table created by
``god_view_market_tables_20260918``:

* ``release_date``        DATE            -- set only when report_date is a
                                              Tuesday, per the documented CFTC
                                              release-schedule rule (see
                                              docs/reference/GODVIEW_PILLAR_CONTRACT.md
                                              section 8). NULL means
                                              quarantined from strict-PIT
                                              reads, never deleted.
* ``available_at``        TIMESTAMPTZ     -- first acquisition (earliest
                                              raw_series.pull_timestamp across
                                              the row's raw metrics).
* ``provenance``          TEXT            -- coarse row-level tag, "measured"
                                              for this pillar. Per-field
                                              measured/derived distinction
                                              lives in the API layer via
                                              store/availability_fields.FieldRecord,
                                              not in this column.
* ``generation_id``       TEXT            -- uuid4 of the materializer run
                                              that inserted the row.
* ``coverage_fraction``   DOUBLE PRECISION -- fraction of the ideal 156-week
                                              derived-window history available
                                              when z_score_3y/percentile_3y
                                              were computed for this row.
* ``source_ref``          TEXT            -- audit string documenting exactly
                                              which release-date rule fired
                                              (or why it was withheld) for
                                              this row.

All six columns are nullable and additive (``ADD COLUMN IF NOT EXISTS``); no
existing row is touched (no UPDATE, no DELETE, no backfill). An index on
``generation_id`` supports "all rows one materializer run touched" audit
queries.

Also creates ``godview_generations``, a small append-style bookkeeping table
(one row per materializer attempt, upserted by generation_id) that records
whether a given generation completed, failed, or is mid-flight, and when it
was published. See the contract doc section 7 for why publication is a
single transaction rather than a separate final UPDATE, and why this table
is a per-run audit record rather than a data partition.

NOTE for whoever merges this after the data-integrity packet-2 stack: this
revision chains directly off ``promotion_ledger_0918`` (today's sole head,
per tests/test_alembic_single_head.py). Re-parent if another lane also
branches off that same head before this merges — do NOT let two heads land.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "godview_pit_cftc_0918"
down_revision: Union[str, Sequence[str], None] = "promotion_ledger_0918"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- PIT/provenance columns on the tracked CFTC positioning table -----
    op.execute(
        "ALTER TABLE cftc_positioning_daily "
        "ADD COLUMN IF NOT EXISTS release_date DATE"
    )
    op.execute(
        "ALTER TABLE cftc_positioning_daily "
        "ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ"
    )
    op.execute(
        "ALTER TABLE cftc_positioning_daily "
        "ADD COLUMN IF NOT EXISTS provenance TEXT"
    )
    op.execute(
        "ALTER TABLE cftc_positioning_daily "
        "ADD COLUMN IF NOT EXISTS generation_id TEXT"
    )
    op.execute(
        "ALTER TABLE cftc_positioning_daily "
        "ADD COLUMN IF NOT EXISTS coverage_fraction DOUBLE PRECISION"
    )
    op.execute(
        "ALTER TABLE cftc_positioning_daily "
        "ADD COLUMN IF NOT EXISTS source_ref TEXT"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_cftc_positioning_generation "
        "ON cftc_positioning_daily (generation_id)"
    )

    # --- generation bookkeeping (shared shape for every future pillar) ----
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS godview_generations (
            id                  BIGSERIAL PRIMARY KEY,
            pillar              TEXT NOT NULL,
            generation_id       TEXT NOT NULL,
            status              TEXT NOT NULL CHECK (status IN (
                                    'building', 'complete', 'failed')),
            row_count           INTEGER,
            coverage_fraction   DOUBLE PRECISION,
            failure_reason      TEXT,
            started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            published_at        TIMESTAMPTZ,
            created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (generation_id)
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_godview_generations_pillar "
        "ON godview_generations (pillar, published_at DESC)"
    )


def downgrade() -> None:
    # Drops only what this revision added -- no data in cftc_positioning_daily
    # itself (report_date, contract_code, positioning columns, ...) is touched.
    op.execute("DROP TABLE IF EXISTS godview_generations")
    op.execute(
        "DROP INDEX IF EXISTS idx_cftc_positioning_generation"
    )
    op.execute(
        "ALTER TABLE cftc_positioning_daily DROP COLUMN IF EXISTS source_ref"
    )
    op.execute(
        "ALTER TABLE cftc_positioning_daily DROP COLUMN IF EXISTS coverage_fraction"
    )
    op.execute(
        "ALTER TABLE cftc_positioning_daily DROP COLUMN IF EXISTS generation_id"
    )
    op.execute(
        "ALTER TABLE cftc_positioning_daily DROP COLUMN IF EXISTS provenance"
    )
    op.execute(
        "ALTER TABLE cftc_positioning_daily DROP COLUMN IF EXISTS available_at"
    )
    op.execute(
        "ALTER TABLE cftc_positioning_daily DROP COLUMN IF EXISTS release_date"
    )
