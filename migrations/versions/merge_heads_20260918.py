"""Merge the two data-integrity remediation heads.

Revision ID: merge_heads_20260918
Revises: actors_provenance_20260917, options_rec_scanner_score_0917
Create Date: 2026-09-18

PR #539 (options_rec_scanner_score_0917) and PR #542
(actors_provenance_20260917) both revise snapshot_actor_col_20260914. Each is
a single head on its own branch; together they are two heads and
``alembic upgrade head`` refuses to run. This empty merge revision joins them.
It must land on whichever of the two merges second (after a rebase onto main
that already contains the first), never before both parents exist on main.
"""

from collections.abc import Sequence

revision: str = "merge_heads_20260918"
down_revision: str | Sequence[str] | None = (
    "actors_provenance_20260917",
    "options_rec_scanner_score_0917",
)
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
