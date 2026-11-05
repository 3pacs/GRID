"""Record completed options pulls without backfilling legacy snapshots.

Capture order uses PostgreSQL's built-in txid_current(), so the runtime role
does not need privileges on a migration-owned sequence. NULL legacy ordinals
remain reader-unavailable until a complete new capture replaces the day.

Revision ID: options_capture_batch_20260924
Revises: spy_close_receipt_20260922
"""

from alembic import op
import sqlalchemy as sa

revision = "options_capture_batch_20260924"
down_revision = "spy_close_receipt_20260922"
branch_labels = None
depends_on = None


def _options_snapshots_is_table() -> bool:
    """True only while ``options_snapshots`` is still the pre-append-only table.

    ``options_append_only_20260930`` turns it into a view over
    ``options_snapshots_all``, and schema.sql builds that final shape directly
    on a fresh install. A fresh database (schema.sql -> stamp 7e4dfecce247 ->
    upgrade head) therefore reaches this revision with a view, which cannot
    take ADD COLUMN; the columns already exist on ``options_snapshots_all``
    there (issue #523). Production ran this revision long before the view.
    """
    return bool(
        op.get_bind().execute(
            sa.text(
                "SELECT EXISTS (SELECT 1 FROM pg_class "
                "WHERE oid = to_regclass('options_snapshots') AND relkind = 'r')"
            )
        ).scalar()
    )


def upgrade() -> None:
    if not _options_snapshots_is_table():
        return
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")
    op.execute("""
        ALTER TABLE options_snapshots
            ADD COLUMN IF NOT EXISTS capture_batch_id TEXT,
            ADD COLUMN IF NOT EXISTS capture_ordinal BIGINT,
            ADD COLUMN IF NOT EXISTS capture_started_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS capture_completed_at TIMESTAMPTZ
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")
    op.execute("""
        ALTER TABLE options_snapshots
            DROP COLUMN IF EXISTS capture_completed_at,
            DROP COLUMN IF EXISTS capture_started_at,
            DROP COLUMN IF EXISTS capture_ordinal,
            DROP COLUMN IF EXISTS capture_batch_id
    """)
