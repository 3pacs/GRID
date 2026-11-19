"""Preserve raw option clock semantics without inventing quote freshness.

No historical backfill. Existing rows remain NULL; consumer clock qualification
must treat NULL and explicit unknown alike. No change to underlying clock field.
"""
from alembic import op

revision = "options_contract_clocks_20261114"
down_revision = "finra_dix_feature_20261113"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")
    op.execute("ALTER TABLE options_capture_batches ADD COLUMN IF NOT EXISTS clock_metadata JSONB")
    op.execute("ALTER TABLE options_snapshots_all ADD COLUMN IF NOT EXISTS clock_metadata JSONB")
    op.execute("COMMENT ON COLUMN options_snapshots_all.clock_metadata IS 'Provider raw clocks, not proof of executable bid/ask freshness; NULL means unavailable'")


def downgrade():
    raise RuntimeError("Refusing to erase option clock provenance")
