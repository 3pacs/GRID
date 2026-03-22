#!/usr/bin/env python3
"""GRID — Daily options chain puller for watchlist tickers.

Pulls options chains via yfinance for 19 tickers, computes daily signals
(put/call ratio, max pain, IV skew, total OI), and pushes to resolved_series.

Tables created:
  - options_snapshots: Raw options chain data per ticker per day
  - options_daily_signals: Computed signals per ticker per day

Run: python3 pull_options.py
"""

import os
import sys
import json
from datetime import datetime, date

import psycopg2
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings
from loguru import logger as log

try:
    import yfinance as yf
except ImportError:
    log.error("yfinance not installed: pip install yfinance")
    sys.exit(1)

TICKERS = [
    "SPY", "QQQ", "IWM",  # Indices
    "EOG", "DVN",          # Energy
    "CMCSA", "CI", "PYPL", # Value plays
    "RTX", "GD",           # Defense
    "INTC",                # Watchlist
    "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META",  # Mega-cap
    "BTC-USD", "ETH-USD",  # Crypto (options via CME proxy not available via yfinance)
]

# Filter to tickers that actually have options
EQUITY_TICKERS = [t for t in TICKERS if not t.endswith("-USD")]


def connect():
    return psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
    )


def create_tables(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS options_snapshots (
            id           BIGSERIAL PRIMARY KEY,
            ticker       TEXT NOT NULL,
            snap_date    DATE NOT NULL,
            expiry       DATE NOT NULL,
            opt_type     TEXT NOT NULL CHECK (opt_type IN ('call', 'put')),
            strike       DOUBLE PRECISION NOT NULL,
            last_price   DOUBLE PRECISION,
            bid          DOUBLE PRECISION,
            ask          DOUBLE PRECISION,
            volume       INTEGER,
            open_interest INTEGER,
            implied_vol  DOUBLE PRECISION,
            in_the_money BOOLEAN,
            created_at   TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (ticker, snap_date, expiry, opt_type, strike)
        );
        CREATE INDEX IF NOT EXISTS idx_opts_snap_ticker_date ON options_snapshots (ticker, snap_date);

        CREATE TABLE IF NOT EXISTS options_daily_signals (
            id              BIGSERIAL PRIMARY KEY,
            ticker          TEXT NOT NULL,
            signal_date     DATE NOT NULL,
            put_call_ratio  DOUBLE PRECISION,
            max_pain        DOUBLE PRECISION,
            iv_skew         DOUBLE PRECISION,
            total_oi        BIGINT,
            total_volume    BIGINT,
            near_expiry     DATE,
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (ticker, signal_date)
        );
        CREATE INDEX IF NOT EXISTS idx_opts_sig_ticker ON options_daily_signals (ticker, signal_date);
    """)


def compute_max_pain(calls_df, puts_df, spot_price):
    """Compute max pain strike — the strike where total option losses are minimized for writers."""
    if calls_df.empty or puts_df.empty:
        return None

    all_strikes = sorted(set(calls_df["strike"].tolist() + puts_df["strike"].tolist()))
    if not all_strikes:
        return None

    min_pain = float("inf")
    max_pain_strike = spot_price

    for strike in all_strikes:
        # At this strike, compute total intrinsic value of all options
        call_pain = sum(
            max(0, strike - s) * oi
            for s, oi in zip(calls_df["strike"], calls_df["openInterest"].fillna(0))
        )
        put_pain = sum(
            max(0, s - strike) * oi
            for s, oi in zip(puts_df["strike"], puts_df["openInterest"].fillna(0))
        )
        total_pain = call_pain + put_pain
        if total_pain < min_pain:
            min_pain = total_pain
            max_pain_strike = strike

    return max_pain_strike


def compute_iv_skew(puts_df, spot_price):
    """IV skew: OTM put IV vs ATM put IV."""
    if puts_df.empty or "impliedVolatility" not in puts_df.columns:
        return None

    atm_puts = puts_df[
        (puts_df["strike"] >= spot_price * 0.97) & (puts_df["strike"] <= spot_price * 1.03)
    ]
    otm_puts = puts_df[
        (puts_df["strike"] >= spot_price * 0.85) & (puts_df["strike"] <= spot_price * 0.92)
    ]

    if atm_puts.empty or otm_puts.empty:
        return None

    atm_iv = atm_puts["impliedVolatility"].mean()
    otm_iv = otm_puts["impliedVolatility"].mean()

    if atm_iv and atm_iv > 0:
        return otm_iv / atm_iv
    return None


def pull_ticker(ticker, cur, src_id, today_str):
    """Pull options chain for a single ticker and compute signals."""
    try:
        stock = yf.Ticker(ticker)
        spot_price = stock.info.get("regularMarketPrice") or stock.info.get("previousClose")
        if not spot_price:
            log.warning("{t}: no spot price available", t=ticker)
            return 0

        expirations = stock.options
        if not expirations:
            log.warning("{t}: no options expirations", t=ticker)
            return 0

        total_call_oi = 0
        total_put_oi = 0
        total_call_vol = 0
        total_put_vol = 0
        near_expiry = expirations[0]
        snap_count = 0

        # Pull nearest 3 expirations
        for exp_date in expirations[:3]:
            chain = stock.option_chain(exp_date)

            for opt_type, df in [("call", chain.calls), ("put", chain.puts)]:
                if df.empty:
                    continue
                for _, row in df.iterrows():
                    cur.execute(
                        "INSERT INTO options_snapshots (ticker,snap_date,expiry,opt_type,strike,"
                        "last_price,bid,ask,volume,open_interest,implied_vol,in_the_money) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                        (
                            ticker, today_str, exp_date, opt_type,
                            row.get("strike"),
                            row.get("lastPrice"),
                            row.get("bid"),
                            row.get("ask"),
                            int(row["volume"]) if row.get("volume") and not np.isnan(row["volume"]) else 0,
                            int(row["openInterest"]) if row.get("openInterest") and not np.isnan(row["openInterest"]) else 0,
                            row.get("impliedVolatility"),
                            row.get("inTheMoney"),
                        ),
                    )
                    snap_count += 1

                oi = df["openInterest"].fillna(0).sum()
                vol = df["volume"].fillna(0).sum()
                if opt_type == "call":
                    total_call_oi += oi
                    total_call_vol += vol
                else:
                    total_put_oi += oi
                    total_put_vol += vol

        # Compute signals from nearest expiration
        chain = stock.option_chain(near_expiry)
        put_call_ratio = total_put_oi / total_call_oi if total_call_oi > 0 else None
        max_pain = compute_max_pain(chain.calls, chain.puts, spot_price)
        iv_skew = compute_iv_skew(chain.puts, spot_price)
        total_oi = total_call_oi + total_put_oi
        total_volume = total_call_vol + total_put_vol

        cur.execute(
            "INSERT INTO options_daily_signals (ticker,signal_date,put_call_ratio,max_pain,"
            "iv_skew,total_oi,total_volume,near_expiry) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (ticker,signal_date) DO UPDATE SET "
            "put_call_ratio=EXCLUDED.put_call_ratio, max_pain=EXCLUDED.max_pain, "
            "iv_skew=EXCLUDED.iv_skew, total_oi=EXCLUDED.total_oi, "
            "total_volume=EXCLUDED.total_volume, near_expiry=EXCLUDED.near_expiry",
            (ticker, today_str, put_call_ratio, max_pain, iv_skew, total_oi, total_volume, near_expiry),
        )

        # Push signals to resolved_series
        signals = {
            f"{ticker.lower().replace('-', '_')}_pcr": ("sentiment", f"{ticker} Put/Call Ratio", put_call_ratio),
            f"{ticker.lower().replace('-', '_')}_max_pain": ("sentiment", f"{ticker} Max Pain Strike", max_pain),
            f"{ticker.lower().replace('-', '_')}_iv_skew": ("vol", f"{ticker} IV Skew (OTM/ATM)", iv_skew),
            f"{ticker.lower().replace('-', '_')}_total_oi": ("sentiment", f"{ticker} Total Open Interest", total_oi),
            f"{ticker.lower().replace('-', '_')}_opt_vol": ("sentiment", f"{ticker} Total Options Volume", total_volume),
        }

        for feat_name, (family, desc, val) in signals.items():
            if val is None:
                continue
            cur.execute(
                "INSERT INTO feature_registry (name,family,description,transformation,"
                "transformation_version,lag_days,normalization,missing_data_policy,"
                "eligible_from_date,model_eligible) "
                "VALUES (%s,%s,%s,'RAW',1,0,'ZSCORE','FORWARD_FILL','2024-04-01',TRUE) "
                "ON CONFLICT (name) DO NOTHING RETURNING id",
                (feat_name, family, desc),
            )
            row = cur.fetchone()
            if row:
                fid = row[0]
            else:
                cur.execute("SELECT id FROM feature_registry WHERE name=%s", (feat_name,))
                fid = cur.fetchone()[0]

            cur.execute(
                "INSERT INTO resolved_series (feature_id,obs_date,release_date,vintage_date,"
                "value,source_priority_used) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (fid, today_str, today_str, today_str, float(val), src_id),
            )

        log.info("{t}: {n} snapshots, PCR={pcr}, MaxPain={mp}, OI={oi}",
                 t=ticker, n=snap_count,
                 pcr=f"{put_call_ratio:.2f}" if put_call_ratio else "N/A",
                 mp=f"${max_pain:,.0f}" if max_pain else "N/A",
                 oi=f"{total_oi:,}")
        return snap_count

    except Exception as e:
        log.error("{t}: {e}", t=ticker, e=e)
        return 0


def main():
    conn = connect()
    conn.autocommit = True
    cur = conn.cursor()

    create_tables(cur)

    # Ensure source
    cur.execute(
        "INSERT INTO source_catalog (name,base_url,cost_tier,latency_class,pit_available,"
        "revision_behavior,trust_score,priority_rank) "
        "VALUES ('YFINANCE_OPTIONS','https://finance.yahoo.com','FREE','EOD',FALSE,"
        "'NEVER','MED',7) ON CONFLICT (name) DO NOTHING"
    )
    cur.execute("SELECT id FROM source_catalog WHERE name='YFINANCE_OPTIONS'")
    src_id = cur.fetchone()[0]

    today_str = date.today().isoformat()
    total_snaps = 0

    for ticker in EQUITY_TICKERS:
        count = pull_ticker(ticker, cur, src_id, today_str)
        total_snaps += count

    log.info("Options pull complete: {n} total snapshots for {t} tickers",
             n=total_snaps, t=len(EQUITY_TICKERS))

    cur.execute("SELECT count(*) FROM options_snapshots")
    log.info("options_snapshots: {n} rows", n=cur.fetchone()[0])
    cur.execute("SELECT count(*) FROM options_daily_signals")
    log.info("options_daily_signals: {n} rows", n=cur.fetchone()[0])

    conn.close()


if __name__ == "__main__":
    main()
