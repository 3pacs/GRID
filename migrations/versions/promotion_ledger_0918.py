"""promotion_ledger — append-only promotion records for GRID W7 safeguards.

Revision ID: promotion_ledger_0918
Revises: god_view_market_tables_20260918
Create Date: 2026-09-18 00:00:00.000000

Creates the ``promotion_ledger`` table backing
``governance/promotion_ledger.py``: append-only records of
recommendations and approvals for weight_override / model /
signal_policy promotions. No UPDATE or DELETE is ever issued against
this table by application code — every state change is a fresh INSERT
(see the module docstring for why).

DDL lives in ``governance/promotion_ledger.PROMOTION_LEDGER_DDL`` so
there is exactly one definition of the table shape; this migration
just executes it.

NOTE for whoever merges the next packet: this revision currently
chains directly off ``god_view_market_tables_20260918`` (today's sole
head, per tests/test_alembic_single_head.py). If another lane also
branches off that same head before this merges, re-parent one of the
two revisions (or add a merge revision, per
migrations/versions/merge_heads_20260910.py's pattern) — do NOT let
two heads land. Never merge with two heads.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from governance.promotion_ledger import PROMOTION_LEDGER_DDL

# revision identifiers, used by Alembic.
revision: str = "promotion_ledger_0918"
down_revision: Union[str, Sequence[str], None] = "signal_evaluations_0918"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    for statement in PROMOTION_LEDGER_DDL:
        op.execute(statement)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS promotion_ledger")
