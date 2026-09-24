"""Record completed options pulls without backfilling legacy snapshots.

Revision ID: options_capture_batch_20260924
Revises: spy_close_receipt_20260922
"""

from alembic import op

revision = "options_capture_batch_20260924"
down_revision = "spy_close_receipt_20260922"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")
    op.execute("""
        ALTER TABLE options_snapshots
            ADD COLUMN IF NOT EXISTS capture_batch_id TEXT,
            ADD COLUMN IF NOT EXISTS capture_completed_at TIMESTAMPTZ
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")
    op.execute("""
        ALTER TABLE options_snapshots
            DROP COLUMN IF EXISTS capture_completed_at,
            DROP COLUMN IF EXISTS capture_batch_id
    """)
