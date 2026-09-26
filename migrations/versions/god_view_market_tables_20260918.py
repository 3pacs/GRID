"""create_god_view_market_tables — dedicated institutional market tables and master mart.

Revision ID: god_view_market_tables_20260918
Revises: regime_history_data_as_of
Create Date: 2026-09-18 00:00:00.000000

Creates dedicated, normalized tables for:
1. cftc_positioning_daily (Speculator crowding, commercial net, rolling 3Y z-scores)
2. finra_short_volume_daily (Consolidated NMS daily short volume & ratios)
3. sec_regsho_ftd_cns (SEC CNS Fails-to-Deliver with T+35 Rule 204 mandatory buyin tracking)
4. commodity_warehouse_inventories (LME canceled warrants, Cushing crude floor deltas)
5. fed_net_liquidity_daily (WALCL - WTREGEN - RRPONTSYD, deltas, +10D forward impulse)
6. corporate_buyback_blackouts (S&P 500 10b5-1 blackout calendar, corporate VWAP bid volume)
7. dealer_gex_daily (Dealer Call/Put GEX, Net GEX, and Gamma Flip strikes)
8. market_god_view_daily (Unified institutional analytics materialized view)

All operations are idempotent (CREATE TABLE IF NOT EXISTS, ON CONFLICT DO NOTHING).
"""

from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "god_view_market_tables_20260918"
down_revision: Union[str, Sequence[str], None] = "snapshot_actor_col_20260914"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. CFTC Commitments of Traders Positioning
    op.execute("""
    CREATE TABLE IF NOT EXISTS cftc_positioning_daily (
        id                      BIGSERIAL PRIMARY KEY,
        report_date             DATE NOT NULL,
        contract_code           TEXT NOT NULL,
        contract_name           TEXT NOT NULL,
        asset_class             TEXT NOT NULL,
        total_open_interest     BIGINT NOT NULL,
        commercial_long         BIGINT NOT NULL,
        commercial_short        BIGINT NOT NULL,
        commercial_net          BIGINT NOT NULL,
        noncommercial_long      BIGINT NOT NULL,
        noncommercial_short     BIGINT NOT NULL,
        noncommercial_net       BIGINT NOT NULL,
        spec_net_pct_oi         DOUBLE PRECISION NOT NULL,
        z_score_1y              DOUBLE PRECISION,
        z_score_3y              DOUBLE PRECISION,
        percentile_3y           DOUBLE PRECISION,
        crowding_regime         TEXT NOT NULL,
        created_at              TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (report_date, contract_code)
    );
    CREATE INDEX IF NOT EXISTS idx_cftc_contract_date ON cftc_positioning_daily (contract_code, report_date DESC);
    CREATE INDEX IF NOT EXISTS idx_cftc_regime ON cftc_positioning_daily (crowding_regime);
    """)

    # 2. FINRA Short Sale Volume
    op.execute("""
    CREATE TABLE IF NOT EXISTS finra_short_volume_daily (
        id                      BIGSERIAL PRIMARY KEY,
        trade_date              DATE NOT NULL,
        ticker                  TEXT NOT NULL,
        short_volume            BIGINT NOT NULL,
        short_exempt_volume     BIGINT NOT NULL,
        total_volume            BIGINT NOT NULL,
        short_ratio             DOUBLE PRECISION NOT NULL,
        short_ratio_20d_ma      DOUBLE PRECISION,
        is_spike                BOOLEAN DEFAULT FALSE,
        created_at              TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (trade_date, ticker)
    );
    CREATE INDEX IF NOT EXISTS idx_finra_short_ticker ON finra_short_volume_daily (ticker, trade_date DESC);
    CREATE INDEX IF NOT EXISTS idx_finra_short_ratio ON finra_short_volume_daily (short_ratio DESC);
    """)

    # 3. SEC Reg SHO CNS Failures-to-Deliver (Rule 204 T+35 Calendar)
    op.execute("""
    CREATE TABLE IF NOT EXISTS sec_regsho_ftd_cns (
        id                      BIGSERIAL PRIMARY KEY,
        settlement_date         DATE NOT NULL,
        ticker                  TEXT NOT NULL,
        cusip                   TEXT,
        failed_shares           BIGINT NOT NULL,
        closing_price           DOUBLE PRECISION,
        total_failed_usd        DOUBLE PRECISION,
        mandatory_buyin_date    DATE NOT NULL,
        days_remaining          INTEGER,
        squeeze_risk_score      DOUBLE PRECISION,
        created_at              TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (settlement_date, ticker)
    );
    CREATE INDEX IF NOT EXISTS idx_sec_ftd_ticker ON sec_regsho_ftd_cns (ticker, settlement_date DESC);
    CREATE INDEX IF NOT EXISTS idx_sec_ftd_buyin_date ON sec_regsho_ftd_cns (mandatory_buyin_date);
    """)

    # 4. Physical Commodity Warehouse Inventories
    op.execute("""
    CREATE TABLE IF NOT EXISTS commodity_warehouse_inventories (
        id                      BIGSERIAL PRIMARY KEY,
        report_date             DATE NOT NULL,
        exchange                TEXT NOT NULL,
        commodity               TEXT NOT NULL,
        location_hub            TEXT,
        total_inventory         DOUBLE PRECISION NOT NULL,
        unit                    TEXT NOT NULL,
        on_warrant              DOUBLE PRECISION,
        canceled_warrants       DOUBLE PRECISION,
        canceled_ratio          DOUBLE PRECISION,
        operational_floor       DOUBLE PRECISION,
        floor_buffer_pct        DOUBLE PRECISION,
        net_change_daily        DOUBLE PRECISION,
        physical_tightness_flag BOOLEAN DEFAULT FALSE,
        created_at              TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (report_date, exchange, commodity, location_hub)
    );
    CREATE INDEX IF NOT EXISTS idx_warehouse_comm_date ON commodity_warehouse_inventories (commodity, report_date DESC);
    """)

    # 5. Federal Reserve & Central Bank Net Liquidity
    op.execute("""
    CREATE TABLE IF NOT EXISTS fed_net_liquidity_daily (
        id                      BIGSERIAL PRIMARY KEY,
        obs_date                DATE NOT NULL UNIQUE,
        fed_assets_walcl        DOUBLE PRECISION NOT NULL,
        treasury_tga_wtregen    DOUBLE PRECISION NOT NULL,
        reverse_repo_rrp        DOUBLE PRECISION NOT NULL,
        net_liquidity_usd_m     DOUBLE PRECISION NOT NULL,
        rrp_as_pct_of_peak      DOUBLE PRECISION,
        delta_5d_m              DOUBLE PRECISION,
        delta_30d_m             DOUBLE PRECISION,
        liquidity_regime        TEXT NOT NULL,
        forward_impulse_score   DOUBLE PRECISION,
        created_at              TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_net_liq_date ON fed_net_liquidity_daily (obs_date DESC);
    """)

    # 6. Corporate Buyback Blackouts
    op.execute("""
    CREATE TABLE IF NOT EXISTS corporate_buyback_blackouts (
        id                      BIGSERIAL PRIMARY KEY,
        calendar_date           DATE NOT NULL UNIQUE,
        sp500_cap_blackout_pct  DOUBLE PRECISION NOT NULL,
        active_corporate_bid_m  DOUBLE PRECISION NOT NULL,
        window_status           TEXT NOT NULL,
        next_major_window_change DATE,
        created_at              TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_buyback_date ON corporate_buyback_blackouts (calendar_date DESC);
    """)

    # 7. Options Dealer Gamma Exposure (GEX)
    op.execute("""
    CREATE TABLE IF NOT EXISTS dealer_gex_daily (
        id                      BIGSERIAL PRIMARY KEY,
        obs_date                DATE NOT NULL,
        ticker                  TEXT NOT NULL,
        spot_price              DOUBLE PRECISION NOT NULL,
        net_gex_usd_m           DOUBLE PRECISION NOT NULL,
        call_gex_usd_m          DOUBLE PRECISION NOT NULL,
        put_gex_usd_m           DOUBLE PRECISION NOT NULL,
        gamma_flip_strike       DOUBLE PRECISION NOT NULL,
        spot_to_flip_pct        DOUBLE PRECISION NOT NULL,
        gex_regime              TEXT NOT NULL,
        max_pain_strike         DOUBLE PRECISION NOT NULL,
        put_call_oi_ratio       DOUBLE PRECISION NOT NULL,
        atm_iv                  DOUBLE PRECISION NOT NULL,
        created_at              TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (obs_date, ticker)
    );
    CREATE INDEX IF NOT EXISTS idx_gex_ticker_date ON dealer_gex_daily (ticker, obs_date DESC);
    """)

    # 8. Unified Master Mart Materialized View
    op.execute("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS market_god_view_daily AS
    SELECT DISTINCT ON (m.briefing_date)
        m.briefing_date AS as_of_date,
        (m.snapshot_data->'equities'->'^GSPC'->>'value')::DOUBLE PRECISION AS sp500_close,
        (m.snapshot_data->'equities'->'^IXIC'->>'value')::DOUBLE PRECISION AS nasdaq_close,
        (m.snapshot_data->'volatility'->'^VIX'->>'value')::DOUBLE PRECISION AS vix_spot,
        (m.snapshot_data->'volatility'->'^VIX3M'->>'value')::DOUBLE PRECISION AS vix_3m,
        liq.net_liquidity_usd_m,
        liq.delta_30d_m AS net_liq_30d_delta,
        liq.rrp_as_pct_of_peak,
        cot_es.z_score_3y AS es_spec_zscore,
        cot_zn.z_score_3y AS zn_spec_zscore,
        cot_gc.z_score_3y AS gold_spec_zscore,
        cot_cl.z_score_3y AS oil_spec_zscore,
        oil_hub.total_inventory AS cushing_crude_m_bbl,
        oil_hub.floor_buffer_pct AS cushing_floor_buffer,
        cu_wh.canceled_ratio AS copper_canceled_warrant_pct,
        bb.sp500_cap_blackout_pct,
        bb.active_corporate_bid_m,
        ins.buy_count AS insider_buys,
        ins.sell_count AS insider_sells,
        gex_spy.net_gex_usd_m AS spy_net_gex,
        gex_spy.gamma_flip_strike AS spy_gamma_flip
    FROM market_briefings m
    LEFT JOIN LATERAL (
        SELECT net_liquidity_usd_m, delta_30d_m, rrp_as_pct_of_peak FROM fed_net_liquidity_daily
        WHERE obs_date <= m.briefing_date ORDER BY obs_date DESC LIMIT 1
    ) liq ON TRUE
    LEFT JOIN LATERAL (
        SELECT z_score_3y FROM cftc_positioning_daily
        WHERE contract_code = 'ES' AND report_date <= m.briefing_date ORDER BY report_date DESC LIMIT 1
    ) cot_es ON TRUE
    LEFT JOIN LATERAL (
        SELECT z_score_3y FROM cftc_positioning_daily
        WHERE contract_code = 'ZN' AND report_date <= m.briefing_date ORDER BY report_date DESC LIMIT 1
    ) cot_zn ON TRUE
    LEFT JOIN LATERAL (
        SELECT z_score_3y FROM cftc_positioning_daily
        WHERE contract_code = 'GC' AND report_date <= m.briefing_date ORDER BY report_date DESC LIMIT 1
    ) cot_gc ON TRUE
    LEFT JOIN LATERAL (
        SELECT z_score_3y FROM cftc_positioning_daily
        WHERE contract_code = 'CL' AND report_date <= m.briefing_date ORDER BY report_date DESC LIMIT 1
    ) cot_cl ON TRUE
    LEFT JOIN LATERAL (
        SELECT total_inventory, floor_buffer_pct FROM commodity_warehouse_inventories
        WHERE location_hub = 'CUSHING_OK' AND report_date <= m.briefing_date ORDER BY report_date DESC LIMIT 1
    ) oil_hub ON TRUE
    LEFT JOIN LATERAL (
        SELECT canceled_ratio FROM commodity_warehouse_inventories
        WHERE commodity = 'COPPER' AND report_date <= m.briefing_date ORDER BY report_date DESC LIMIT 1
    ) cu_wh ON TRUE
    LEFT JOIN LATERAL (
        SELECT sp500_cap_blackout_pct, active_corporate_bid_m FROM corporate_buyback_blackouts
        WHERE calendar_date <= m.briefing_date ORDER BY calendar_date DESC LIMIT 1
    ) bb ON TRUE
    LEFT JOIN LATERAL (
        SELECT
            COUNT(*) FILTER (WHERE trade_type ILIKE '%%BUY%%') AS buy_count,
            COUNT(*) FILTER (WHERE trade_type ILIKE '%%SELL%%') AS sell_count
        FROM insider_trades
        WHERE trade_date <= m.briefing_date AND trade_date >= m.briefing_date - INTERVAL '7 days'
    ) ins ON TRUE
    LEFT JOIN LATERAL (
        SELECT net_gex_usd_m, gamma_flip_strike FROM dealer_gex_daily
        WHERE ticker = 'SPY' AND obs_date <= m.briefing_date ORDER BY obs_date DESC LIMIT 1
    ) gex_spy ON TRUE
    WHERE m.briefing_type = 'hourly'
    ORDER BY m.briefing_date DESC, m.id DESC;

    CREATE INDEX IF NOT EXISTS idx_god_view_as_of ON market_god_view_daily (as_of_date DESC);
    """)


def downgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS market_god_view_daily;")
    op.execute("DROP TABLE IF EXISTS dealer_gex_daily;")
    op.execute("DROP TABLE IF EXISTS corporate_buyback_blackouts;")
    op.execute("DROP TABLE IF EXISTS fed_net_liquidity_daily;")
    op.execute("DROP TABLE IF EXISTS commodity_warehouse_inventories;")
    op.execute("DROP TABLE IF EXISTS sec_regsho_ftd_cns;")
    op.execute("DROP TABLE IF EXISTS finra_short_volume_daily;")
    op.execute("DROP TABLE IF EXISTS cftc_positioning_daily;")
