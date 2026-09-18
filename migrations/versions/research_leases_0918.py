"""research_leases — cross-process write leases for the research loop (W4f).

Revision ID: research_leases_0918
Revises: promotion_ledger_0918
Create Date: 2026-09-18 00:00:00.000000

Creates the ``research_leases`` table backing ``governance/leases.py``:
one row per lease name (e.g. "autoresearch"), holding the current
owner_id/generation/expires_at that ``governance.leases.guarded_write``
and ``guarded_write_dbapi`` check (under ``SELECT ... FOR UPDATE``) before
allowing a real write (hypothesis_registry, model_registry,
validation_results) to commit. See that module's docstring for the full
ordering argument.

DDL lives in ``governance/leases.RESEARCH_LEASES_DDL`` so there is exactly
one definition of the table shape; this migration just executes it
(governance/promotion_ledger.py's pattern).

NOTE for whoever merges the next packet: this revision currently chains
directly off ``promotion_ledger_0918`` (today's sole head, per
tests/test_alembic_single_head.py). If another lane also branches off
that same head before this merges, re-parent one of the two revisions (or
add a merge revision, per migrations/versions/merge_heads_20260910.py's
pattern) — do NOT let two heads land. Never merge with two heads.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from governance.leases import RESEARCH_LEASES_DDL

# revision identifiers, used by Alembic.
revision: str = "research_leases_0918"
down_revision: Union[str, Sequence[str], None] = "godview_pit_gex_0918"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    for statement in RESEARCH_LEASES_DDL:
        op.execute(statement)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS research_leases")
