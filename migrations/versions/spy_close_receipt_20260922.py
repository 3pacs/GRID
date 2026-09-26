"""Add source-linked SPY close receipts; no historical data rewrite.

Revision ID: spy_close_receipt_20260922
Revises: actors_provenance_columns_0922
"""

from alembic import op

revision = "spy_close_receipt_20260922"
down_revision = "actors_provenance_columns_0922"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS astrogrid.price_close_receipt (
            id BIGSERIAL PRIMARY KEY,
            contract_version TEXT NOT NULL CHECK (contract_version = 'spy_close_v1'),
            raw_series_id BIGINT NOT NULL UNIQUE REFERENCES raw_series(id),
            resolved_series_id BIGINT NOT NULL UNIQUE REFERENCES resolved_series(id),
            feature_id INTEGER NOT NULL REFERENCES feature_registry(id),
            obs_date DATE NOT NULL,
            price_basis TEXT NOT NULL CHECK (price_basis = 'YF:SPY:close'),
            available_at TIMESTAMPTZ NOT NULL,
            value DOUBLE PRECISION NOT NULL CHECK (value > 0 AND value < 'Infinity'::float8),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (contract_version, feature_id, obs_date)
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_price_close_receipt_available
        ON astrogrid.price_close_receipt (feature_id, obs_date, available_at)
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DROP TABLE IF EXISTS astrogrid.price_close_receipt")
