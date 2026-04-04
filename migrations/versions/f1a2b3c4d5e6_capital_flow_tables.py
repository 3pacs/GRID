"""capital flow tables

Revision ID: f1a2b3c4d5e6
Revises: e2f6a9d3c4b1
Create Date: 2026-04-02 12:00:00.000000

Creates 13 tables for the capital flow intelligence layer:
  - 5 missing tables that fix existing broken queries (insider_trades,
    congressional_trades, dark_pool_weekly, etf_flows,
    capital_flow_snapshots)
  - 8 new tables for expanded flow intelligence (junction_point_readings,
    flow_layer_snapshots, actor_positions, cross_border_flows,
    corporate_actions, margin_debt_monthly, money_market_flows,
    bond_issuance)

All CREATE TABLE statements use IF NOT EXISTS for idempotency.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f1a2b3c4d5e6'
down_revision: Union[str, Sequence[str], None] = 'e2f6a9d3c4b1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# ---------------------------------------------------------------------------
# All 13 tables managed by this migration, used by both upgrade and downgrade.
# ---------------------------------------------------------------------------
_TABLES = [
    "insider_trades",
    "congressional_trades",
    "dark_pool_weekly",
    "etf_flows",
    "capital_flow_snapshots",
    "junction_point_readings",
    "flow_layer_snapshots",
    "actor_positions",
    "cross_border_flows",
    "corporate_actions",
    "margin_debt_monthly",
    "money_market_flows",
    "bond_issuance",
]


def upgrade() -> None:
    # ------------------------------------------------------------------
    # 1. insider_trades
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE IF NOT EXISTS insider_trades (
            id              BIGSERIAL PRIMARY KEY,
            ticker          TEXT NOT NULL,
            trade_date      DATE NOT NULL,
            insider_name    TEXT NOT NULL,
            insider_title   TEXT,
            trade_type      TEXT NOT NULL,
            shares          NUMERIC,
            value           NUMERIC,
            price_per_share NUMERIC,
            filing_date     DATE,
            is_cluster_buy  BOOLEAN DEFAULT FALSE,
            signal_source_id INTEGER,
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (ticker, trade_date, insider_name, trade_type)
        );
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_insider_trades_ticker_date
            ON insider_trades (ticker, trade_date DESC);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_insider_trades_value
            ON insider_trades (value DESC NULLS LAST);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_insider_trades_cluster_buy
            ON insider_trades (is_cluster_buy)
            WHERE is_cluster_buy = TRUE;
    """)

    # ------------------------------------------------------------------
    # 2. congressional_trades
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE IF NOT EXISTS congressional_trades (
            id                  BIGSERIAL PRIMARY KEY,
            ticker              TEXT NOT NULL,
            disclosure_date     DATE NOT NULL,
            transaction_date    DATE,
            representative      TEXT NOT NULL,
            chamber             TEXT,
            party               TEXT,
            state               TEXT,
            transaction_type    TEXT NOT NULL,
            amount              TEXT,
            amount_midpoint     NUMERIC,
            committee           TEXT,
            signal_source_id    INTEGER,
            created_at          TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (ticker, disclosure_date, representative, transaction_type)
        );
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_congressional_trades_ticker_date
            ON congressional_trades (ticker, disclosure_date DESC);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_congressional_trades_rep_date
            ON congressional_trades (representative, disclosure_date DESC);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_congressional_trades_party
            ON congressional_trades (party);
    """)

    # ------------------------------------------------------------------
    # 3. dark_pool_weekly
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE IF NOT EXISTS dark_pool_weekly (
            id              BIGSERIAL PRIMARY KEY,
            ticker          TEXT NOT NULL,
            report_date     DATE NOT NULL,
            short_volume    NUMERIC,
            total_volume    NUMERIC,
            short_pct       NUMERIC,
            trade_count     INTEGER,
            avg_trade_size  NUMERIC,
            spike_ratio     NUMERIC,
            signal_source_id INTEGER,
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (ticker, report_date)
        );
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_dark_pool_weekly_ticker_date
            ON dark_pool_weekly (ticker, report_date DESC);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_dark_pool_weekly_date
            ON dark_pool_weekly (report_date DESC);
    """)

    # ------------------------------------------------------------------
    # 4. etf_flows
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE IF NOT EXISTS etf_flows (
            id                      BIGSERIAL PRIMARY KEY,
            ticker                  TEXT NOT NULL,
            flow_date               DATE NOT NULL,
            flow_value              NUMERIC NOT NULL,
            shares_outstanding      NUMERIC,
            shares_outstanding_chg  NUMERIC,
            aum                     NUMERIC,
            source                  TEXT DEFAULT 'proxy',
            confidence              TEXT DEFAULT 'estimated',
            created_at              TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (ticker, flow_date, source)
        );
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_etf_flows_ticker_date
            ON etf_flows (ticker, flow_date DESC);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_etf_flows_date
            ON etf_flows (flow_date DESC);
    """)

    # ------------------------------------------------------------------
    # 5. capital_flow_snapshots
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE IF NOT EXISTS capital_flow_snapshots (
            id                  BIGSERIAL PRIMARY KEY,
            snapshot_date       DATE NOT NULL UNIQUE,
            generated_at        TIMESTAMPTZ DEFAULT NOW(),
            sectors             JSONB,
            relative_strength   JSONB,
            monetary            JSONB,
            options_positioning JSONB,
            narrative           TEXT,
            metadata            JSONB
        );
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_capital_flow_snapshots_date
            ON capital_flow_snapshots (snapshot_date DESC);
    """)

    # ------------------------------------------------------------------
    # 6. junction_point_readings
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE IF NOT EXISTS junction_point_readings (
            id              BIGSERIAL PRIMARY KEY,
            junction_id     TEXT NOT NULL,
            obs_date        DATE NOT NULL,
            value           NUMERIC NOT NULL,
            unit            TEXT DEFAULT 'USD',
            change_1d       NUMERIC,
            change_1w       NUMERIC,
            change_1m       NUMERIC,
            z_score         NUMERIC,
            source          TEXT NOT NULL,
            confidence      TEXT DEFAULT 'confirmed',
            metadata        JSONB,
            updated_at      TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (junction_id, obs_date)
        );
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_junction_point_readings_jid_date
            ON junction_point_readings (junction_id, obs_date DESC);
    """)

    # ------------------------------------------------------------------
    # 7. flow_layer_snapshots
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE IF NOT EXISTS flow_layer_snapshots (
            id              BIGSERIAL PRIMARY KEY,
            layer_name      TEXT NOT NULL,
            snapshot_date   DATE NOT NULL,
            total_value_usd NUMERIC,
            net_flow_1w     NUMERIC,
            net_flow_1m     NUMERIC,
            component_data  JSONB,
            stress_score    NUMERIC,
            regime          TEXT,
            metadata        JSONB,
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (layer_name, snapshot_date)
        );
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_flow_layer_snapshots_layer_date
            ON flow_layer_snapshots (layer_name, snapshot_date DESC);
    """)

    # ------------------------------------------------------------------
    # 8. actor_positions
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE IF NOT EXISTS actor_positions (
            id                  BIGSERIAL PRIMARY KEY,
            actor_id            TEXT NOT NULL,
            ticker              TEXT NOT NULL,
            position_date       DATE NOT NULL,
            shares              NUMERIC,
            value_usd           NUMERIC,
            pct_of_portfolio    NUMERIC,
            change_shares       NUMERIC,
            change_pct          NUMERIC,
            action              TEXT,
            filing_type         TEXT,
            filing_accession    TEXT,
            source              TEXT NOT NULL,
            created_at          TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (actor_id, ticker, position_date, filing_type)
        );
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_actor_positions_ticker_date
            ON actor_positions (ticker, position_date DESC);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_actor_positions_actor_date
            ON actor_positions (actor_id, position_date DESC);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_actor_positions_action
            ON actor_positions (action)
            WHERE action IN ('NEW', 'CLOSED');
    """)

    # ------------------------------------------------------------------
    # 9. cross_border_flows
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE IF NOT EXISTS cross_border_flows (
            id              BIGSERIAL PRIMARY KEY,
            source          TEXT NOT NULL,
            country_from    TEXT,
            country_to      TEXT,
            instrument_type TEXT NOT NULL,
            obs_date        DATE NOT NULL,
            value_usd       NUMERIC NOT NULL,
            change_mom      NUMERIC,
            change_yoy      NUMERIC,
            metadata        JSONB,
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (source, country_from, country_to, instrument_type, obs_date)
        );
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_cross_border_flows_date
            ON cross_border_flows (obs_date DESC);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_cross_border_flows_from_date
            ON cross_border_flows (country_from, obs_date DESC);
    """)

    # ------------------------------------------------------------------
    # 10. corporate_actions
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE IF NOT EXISTS corporate_actions (
            id                  BIGSERIAL PRIMARY KEY,
            ticker              TEXT NOT NULL,
            action_type         TEXT NOT NULL,
            announcement_date   DATE NOT NULL,
            effective_date      DATE,
            value_usd           NUMERIC,
            shares_affected     NUMERIC,
            description         TEXT,
            source              TEXT NOT NULL,
            metadata            JSONB,
            created_at          TIMESTAMPTZ DEFAULT NOW()
        );
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_corporate_actions_ticker_date
            ON corporate_actions (ticker, announcement_date DESC);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_corporate_actions_type_date
            ON corporate_actions (action_type, announcement_date DESC);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_corporate_actions_date
            ON corporate_actions (announcement_date DESC);
    """)

    # ------------------------------------------------------------------
    # 11. margin_debt_monthly
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE IF NOT EXISTS margin_debt_monthly (
            id                  BIGSERIAL PRIMARY KEY,
            obs_date            DATE NOT NULL UNIQUE,
            margin_debt         NUMERIC NOT NULL,
            free_credit_cash    NUMERIC,
            free_credit_margin  NUMERIC,
            net_margin          NUMERIC,
            change_mom          NUMERIC,
            change_yoy          NUMERIC,
            source              TEXT DEFAULT 'FINRA',
            created_at          TIMESTAMPTZ DEFAULT NOW()
        );
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_margin_debt_monthly_date
            ON margin_debt_monthly (obs_date DESC);
    """)

    # ------------------------------------------------------------------
    # 12. money_market_flows
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE IF NOT EXISTS money_market_flows (
            id              BIGSERIAL PRIMARY KEY,
            obs_date        DATE NOT NULL,
            fund_category   TEXT NOT NULL,
            total_assets    NUMERIC NOT NULL,
            net_flow        NUMERIC,
            change_pct      NUMERIC,
            source          TEXT DEFAULT 'ICI',
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (obs_date, fund_category)
        );
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_money_market_flows_date
            ON money_market_flows (obs_date DESC);
    """)

    # ------------------------------------------------------------------
    # 13. bond_issuance
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE IF NOT EXISTS bond_issuance (
            id                  BIGSERIAL PRIMARY KEY,
            obs_date            DATE NOT NULL,
            issuer_type         TEXT NOT NULL,
            issuance_usd        NUMERIC NOT NULL,
            matured_usd         NUMERIC,
            net_issuance        NUMERIC,
            avg_coupon          NUMERIC,
            avg_maturity_years  NUMERIC,
            source              TEXT DEFAULT 'SIFMA',
            metadata            JSONB,
            created_at          TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (obs_date, issuer_type)
        );
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_bond_issuance_date
            ON bond_issuance (obs_date DESC);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_bond_issuance_type_date
            ON bond_issuance (issuer_type, obs_date DESC);
    """)


def downgrade() -> None:
    for table in reversed(_TABLES):
        op.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
