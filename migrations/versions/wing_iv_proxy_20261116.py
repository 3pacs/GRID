"""Add versioned fixed-moneyness IV proxies without rewriting legacy history.

Legacy iv_25d_* values have unknown/mixed methods. No backfill or relabeling.
"""
from alembic import op

revision = "wing_iv_proxy_20261116"
down_revision = "options_contract_clocks_20261114"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")
    op.execute("ALTER TABLE options_daily_signals ADD COLUMN IF NOT EXISTS iv_m90_put_v1 DOUBLE PRECISION")
    op.execute("ALTER TABLE options_daily_signals ADD COLUMN IF NOT EXISTS iv_m110_call_v1 DOUBLE PRECISION")
    op.execute("ALTER TABLE options_daily_signals ADD COLUMN IF NOT EXISTS wing_iv_metadata JSONB")
    op.execute("ALTER TABLE resolved_series ADD COLUMN IF NOT EXISTS options_iv_metadata JSONB")
    op.execute("COMMENT ON COLUMN options_daily_signals.wing_iv_metadata IS 'Fixed-moneyness proxy method, expiry, provider and capture lineage; NULL is unknown legacy, never 25-delta'")


def downgrade():
    raise RuntimeError("Refusing to erase wing IV lineage")
