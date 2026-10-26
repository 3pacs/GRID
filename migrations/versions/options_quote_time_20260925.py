"""Require a dated provider quote on new options captures.

Old batches remain NULL and reader-unavailable; no source timestamp is inferred
from capture time or a legacy daily signal.

Revision ID: options_quote_time_20260925
Revises: options_capture_batch_20260924
"""

from alembic import op

revision = "options_quote_time_20260925"
down_revision = "options_capture_batch_20260924"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")
    op.execute("""
        ALTER TABLE options_snapshots
            ADD COLUMN IF NOT EXISTS provider_regular_market_at TIMESTAMPTZ
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")
    op.execute("""
        ALTER TABLE options_snapshots
            DROP COLUMN IF EXISTS provider_regular_market_at
    """)
