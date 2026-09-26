#!/usr/bin/env python3
"""Compatibility cron entry point for the provenance-aware options puller."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from loguru import logger as log

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


def create_tables(cur):
    cur.execute("""
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


def main() -> int:
    """Run the single batch-aware writer for the legacy weekday cron."""
    from db import get_engine
    from ingestion.options import OptionsPuller

    results = OptionsPuller(db_engine=get_engine()).pull_all(
        tickers=EQUITY_TICKERS, include_catalyst_universe=False,
    )
    failures = [row for row in results if row["status"] != "SUCCESS"]
    log.info("Options cron pull: {ok}/{total} tickers", ok=len(results) - len(failures), total=len(results))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
