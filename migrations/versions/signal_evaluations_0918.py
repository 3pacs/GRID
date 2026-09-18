"""signal_evaluations — versioned, honest per-signal outcome records (W3b).

Revision ID: signal_evaluations_0918
Revises: god_view_market_tables_20260918
Create Date: 2026-09-18 00:00:00.000000

Adds ``signal_evaluations``, an append-only table written by
``evaluation.signal_outcomes.persist_outcomes()``. This is a brand new
table — this revision does not ALTER, UPDATE, or DELETE anything in
``signal_sources`` or any other existing table.

NOTE FOR THE MERGE OWNER: this branch was cut from
``god_view_market_tables_20260918``, which was itself an incident
reconciliation commit (see 5e79b007) and may not be the true head by the
time this lands — the data-integrity "packet-2" workstream may add its own
head off the same parent. Re-parent onto the data-integrity packet-2 head
before merge; do not merge with two heads.
"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "signal_evaluations_0918"
down_revision: Union[str, Sequence[str], None] = "god_view_market_tables_20260918"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS signal_evaluations (
            id                      BIGSERIAL PRIMARY KEY,
            signal_source_id        INTEGER NOT NULL REFERENCES signal_sources(id),
            evaluation_version      TEXT NOT NULL,
            horizon_days            INTEGER NOT NULL,
            source_type             TEXT NOT NULL,
            instrument              TEXT NOT NULL,
            signal_date             DATE NOT NULL,
            direction               TEXT NOT NULL,
            entry_price             DOUBLE PRECISION,
            entry_price_date        DATE,
            entry_price_basis       TEXT,
            exit_price              DOUBLE PRECISION,
            exit_price_date         DATE,
            exit_price_basis        TEXT,
            raw_return              DOUBLE PRECISION,
            cost_bps                DOUBLE PRECISION NOT NULL DEFAULT 0,
            cost_adjusted_return    DOUBLE PRECISION,
            dead_band_pct           DOUBLE PRECISION NOT NULL,
            outcome                 TEXT NOT NULL CHECK (outcome IN (
                                        'CORRECT', 'WRONG', 'NO_MOVE',
                                        'UNRESOLVED', 'INELIGIBLE')),
            eligibility_reason      TEXT,
            origin_tag              TEXT NOT NULL DEFAULT 'unknown',
            evaluated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )

    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_signal_evaluations_identity
            ON signal_evaluations (signal_source_id, evaluation_version, horizon_days)
        """
    )

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_signal_evaluations_outcome
            ON signal_evaluations (outcome)
        """
    )

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_signal_evaluations_instrument
            ON signal_evaluations (instrument, signal_date DESC)
        """
    )


def downgrade() -> None:
    # Drops only this table. Nothing else this revision touched (nothing).
    op.execute("DROP TABLE IF EXISTS signal_evaluations")
