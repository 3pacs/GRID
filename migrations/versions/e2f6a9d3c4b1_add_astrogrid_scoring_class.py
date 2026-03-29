"""add astrogrid scoring class

Revision ID: e2f6a9d3c4b1
Revises: c91b4a2e7d33
Create Date: 2026-03-29 00:20:00.000000
"""

from typing import Sequence, Union

from alembic import op


revision: str = "e2f6a9d3c4b1"
down_revision: Union[str, Sequence[str], None] = "c91b4a2e7d33"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE astrogrid.prediction_run
        ADD COLUMN IF NOT EXISTS scoring_class TEXT
        NOT NULL DEFAULT 'liquid_market'
        CHECK (scoring_class IN ('liquid_market', 'illiquid_real_asset', 'macro_narrative', 'unscored_experimental'));

        CREATE INDEX IF NOT EXISTS idx_astrogrid_prediction_run_scoring_class
            ON astrogrid.prediction_run (scoring_class, created_at DESC);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP INDEX IF EXISTS idx_astrogrid_prediction_run_scoring_class;
        ALTER TABLE astrogrid.prediction_run
        DROP COLUMN IF EXISTS scoring_class;
        """
    )
