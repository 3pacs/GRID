"""Keep every options capture batch for a ticker and session day.

This migration must run only after a database backup and a reviewed dry run.
Old scheduler and pinned GEM writers still delete whole ticker/day chains; they
must be stopped/replaced before relying on the new retention guarantee.

Revision ID: options_append_only_batches_20260930
Revises: security_master_20260927
"""

from alembic import op

revision = "options_append_only_batches_20260930"
down_revision = "security_master_20260927"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")
    op.execute("""
        ALTER TABLE options_snapshots
            DROP CONSTRAINT options_snapshots_ticker_snap_date_expiry_opt_type_strike_key,
            ADD CONSTRAINT options_snapshots_batch_strike_key
                UNIQUE (ticker, snap_date, capture_batch_id, expiry, opt_type, strike)
    """)
    # Legacy NULL-batch rows retain their old uniqueness while complete,
    # non-NULL capture IDs may now coexist on the same day and strike.
    op.execute("""
        CREATE UNIQUE INDEX options_snapshots_legacy_strike_key
        ON options_snapshots (ticker, snap_date, expiry, opt_type, strike)
        WHERE capture_batch_id IS NULL
    """)
    op.execute("""
        CREATE INDEX options_snapshots_latest_batch_idx
        ON options_snapshots (ticker, snap_date, capture_ordinal DESC, capture_batch_id)
    """)


def downgrade() -> None:
    # Dropping a captured batch to restore ticker/day uniqueness would destroy
    # evidence. An operator must decide how to retain the data before rollback.
    raise RuntimeError("append-only options captures cannot be downgraded automatically")
