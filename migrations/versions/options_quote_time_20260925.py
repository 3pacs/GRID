"""Require a dated provider quote on new options captures.

Old batches remain NULL and reader-unavailable; no source timestamp is inferred
from capture time or a legacy daily signal.

Revision ID: options_quote_time_20260925
Revises: options_capture_batch_20260924
"""

from alembic import op
import sqlalchemy as sa

revision = "options_quote_time_20260925"
down_revision = "options_capture_batch_20260924"
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
            ADD COLUMN IF NOT EXISTS provider_regular_market_at TIMESTAMPTZ
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")
    op.execute("""
        ALTER TABLE options_snapshots
            DROP COLUMN IF EXISTS provider_regular_market_at
    """)
