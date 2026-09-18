"""godview_avail_basis — observed vs. inferred vs. unknown availability basis (W6 slice A).

Revision ID: godview_avail_basis_0918
Revises: godview_pit_cftc_0918
Create Date: 2026-09-18 00:00:00.000000

Operator direction (2026-09-18): "a published CFTC release schedule alone
does not establish historical availability for revised or backfilled
records." ``godview_pit_cftc_0918`` already lets a row's ``release_date``
say WHEN a report is scheduled to have published; this revision adds
``availability_basis`` to say WHETHER that date is backed by an observed
acquisition near that time (``observed_acquisition``), only inferred from
the schedule with no such observation -- a revision or backfill
(``inferred_schedule``), or neither (``unknown``).

See ``godview/cftc_pillar.py::classify_availability_basis`` for the pure
classification rule and docs/reference/GODVIEW_PILLAR_CONTRACT.md section
10 for the full contract, including why strict-PIT reads exclude
``inferred_schedule``/``unknown`` rows by default (``include_inferred=True``
admits them, labelled).

Nullable and additive (``ADD COLUMN IF NOT EXISTS``) with a CHECK
constraint restricting it to the three documented values or NULL; no
existing row is touched (no UPDATE, no DELETE, no backfill of historical
rows' basis -- a value already NULL stays NULL until a materializer run
re-derives it for a NEW row, since existing rows are immutable).

NOTE for whoever merges the next packet: this revision chains directly off
``godview_pit_cftc_0918`` (this lane's own head at the time this was
written) rather than ``research_leases_0918`` as originally directed,
because ``research_leases_0918`` is not present in this worktree's git
history (it lives on ``fable/integration-20260918`` /
``fable/write-fencing-20260918``, neither merged into
``fable/godview-20260918`` yet) -- chaining onto a revision id absent from
this tree's ``migrations/versions/`` would break `alembic`'s own DAG
resolution for this branch, not just the eventual merge. Re-parent this
revision (down_revision -> "research_leases_0918") once that lane's
migration is merged in, per the same pattern
``promotion_ledger_0918``/``signal_evaluations_0918`` already used. Do NOT
let two heads land.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "godview_avail_basis_0918"
down_revision: Union[str, Sequence[str], None] = "godview_pit_cftc_0918"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_CHECK_NAME = "ck_cftc_positioning_availability_basis"


def upgrade() -> None:
    op.execute(
        "ALTER TABLE cftc_positioning_daily "
        "ADD COLUMN IF NOT EXISTS availability_basis TEXT"
    )
    op.execute(
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = '{_CHECK_NAME}'
            ) THEN
                ALTER TABLE cftc_positioning_daily
                    ADD CONSTRAINT {_CHECK_NAME}
                    CHECK (availability_basis IN (
                        'observed_acquisition', 'inferred_schedule', 'unknown'
                    ));
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute(
        f"ALTER TABLE cftc_positioning_daily DROP CONSTRAINT IF EXISTS {_CHECK_NAME}"
    )
    op.execute(
        "ALTER TABLE cftc_positioning_daily DROP COLUMN IF EXISTS availability_basis"
    )
