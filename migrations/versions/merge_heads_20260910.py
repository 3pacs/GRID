"""merge divergent alembic heads

Revision ID: merge_heads_20260910
Revises: idle_fleet_goal_queue_day1, phase4_fts_002, phase4_investigation_evidence
Create Date: 2026-09-10 00:00:00.000000

Three branches diverged from a1b2c3d4e5f6 (canvas tables) and were never
reconciled, leaving `alembic upgrade head` failing with "Multiple head
revisions are present":

  idle_fleet_goal_queue_day1 <- tps_phase0_snapshots <- phase4_actor_analytics <- a1b2c3d4e5f6
  phase4_fts_002 <- phase4_fts_001 <- a1b2c3d4e5f6
  phase4_investigation_evidence <- a1b2c3d4e5f6

`.github/workflows/deploy.yml` runs `alembic upgrade head || echo "no
migrations"`, so this error has been silently swallowed on every deploy
since the branches diverged and no migration in any of the three chains
has actually been applied to griddb.

This is a pure merge point: no schema change, just reconciling history so
there is a single head again. See PR body / tests/test_alembic_single_head.py
for the ordering hazard with PR #440 (`regime_history_writer`, parented on
`idle_fleet_goal_queue_day1`), which must re-parent onto this revision (or
vice versa) depending on merge order.
"""

from typing import Sequence, Union

# revision identifiers, used by Alembic.
revision: str = "merge_heads_20260910"
down_revision: Union[str, Sequence[str], None] = (
    "idle_fleet_goal_queue_day1",
    "phase4_fts_002",
    "phase4_investigation_evidence",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
