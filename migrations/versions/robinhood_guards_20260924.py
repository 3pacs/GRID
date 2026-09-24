"""Persisted risk state and order log for Robinhood trading guards

Revision ID: robinhood_guards_20260924
Revises: spy_close_receipt_20260922

Adds the two tables trading/robinhood_risk_store.py needs so the Robinhood
connector's drawdown high-water mark, start-of-day equity and order-rate
counter survive process restarts and the fresh-instance-per-call pattern in
trading.robinhood.get_robinhood_trader() (previously an in-memory attribute
that reset on every call -- the drawdown halt could never trip). Also backs
idempotent order submission and cost-accounting audit via
trading_order_log. See PR fix/robinhood-live-guards-20260924 for the full
audit and design notes.

Both tables are venue-scoped (not Robinhood-specific by name) so another
connector's guards can reuse the same store later.
"""

from alembic import op

revision = "robinhood_guards_20260924"
down_revision = "spy_close_receipt_20260922"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")

    op.execute("""
        CREATE TABLE IF NOT EXISTS trading_risk_state (
            venue             TEXT PRIMARY KEY,
            peak_equity       DOUBLE PRECISION NOT NULL,
            day_start_equity  DOUBLE PRECISION NOT NULL,
            day_start_date    DATE NOT NULL,
            orders_today      INTEGER NOT NULL DEFAULT 0,
            updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS trading_order_log (
            id                BIGSERIAL PRIMARY KEY,
            venue             TEXT NOT NULL,
            client_order_id   TEXT NOT NULL,
            wallet_id         TEXT,
            ticker            TEXT NOT NULL,
            side              TEXT NOT NULL,
            direction         TEXT NOT NULL,
            size_usd          DOUBLE PRECISION NOT NULL,
            quantity          TEXT,
            bid               DOUBLE PRECISION,
            ask               DOUBLE PRECISION,
            mid               DOUBLE PRECISION,
            executable_price  DOUBLE PRECISION,
            spread_bps        DOUBLE PRECISION,
            spread_cost_usd   DOUBLE PRECISION,
            order_type        TEXT NOT NULL,
            status            TEXT NOT NULL,
            fill_price        DOUBLE PRECISION,
            guard_results     JSONB,
            error             TEXT,
            raw_response      JSONB,
            simulated         BOOLEAN NOT NULL DEFAULT FALSE,
            created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_trading_order_log_lookup
        ON trading_order_log (venue, client_order_id, status)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_trading_order_log_recent
        ON trading_order_log (venue, created_at DESC)
    """)

    # GRANT footer (migrations/_TEMPLATE.sql). Alembic runs as the owner role;
    # the API and the scheduler connect as `grid`. Re-granting on an existing
    # table is a no-op, and the guard keeps a developer database without a
    # `grid` role migrating cleanly.
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
                EXECUTE 'GRANT ALL ON trading_risk_state TO grid';
                EXECUTE 'GRANT ALL ON trading_order_log TO grid';
                EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE trading_order_log_id_seq TO grid';
            END IF;
        END
        $$
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DROP TABLE IF EXISTS trading_order_log")
    op.execute("DROP TABLE IF EXISTS trading_risk_state")
