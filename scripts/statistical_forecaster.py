"""Statistical fallback forecaster for timeseries_forecasts table.

Generates forecasts using exponential weighted moving average (EWMA) returns
with log-normal confidence intervals.  Works without TimesFM — pure numpy.

Usage:
    python scripts/statistical_forecaster.py          # default 60 tickers
    python scripts/statistical_forecaster.py --limit 100
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from typing import Any

import numpy as np
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


def _get_tickers(engine: Engine, limit: int = 60) -> list[str]:
    """Get tickers with sufficient adj_close data in raw_series."""
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT split_part(series_id, ':', 2) AS ticker,
                       COUNT(DISTINCT obs_date)       AS n_days
                FROM   raw_series
                WHERE  series_id LIKE :pat
                  AND  series_id NOT LIKE :excl
                  AND  pull_status = 'SUCCESS'
                GROUP  BY split_part(series_id, ':', 2)
                HAVING COUNT(DISTINCT obs_date) >= 60
                ORDER  BY COUNT(DISTINCT obs_date) DESC
                LIMIT  :lim
            """),
            {"pat": "YF:%:adj_close", "excl": "YF:^%", "lim": limit},
        ).fetchall()
    tickers = [r[0] for r in rows]
    log.info("Found {n} tickers with >=60 trading days", n=len(tickers))
    return tickers


def _fetch_prices(engine: Engine, ticker: str, lookback: int = 252) -> np.ndarray | None:
    """Fetch the most recent `lookback` adj_close values for a ticker."""
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT DISTINCT ON (obs_date) obs_date, value
                FROM   raw_series
                WHERE  series_id = :sid
                  AND  pull_status = 'SUCCESS'
                  AND  value IS NOT NULL
                ORDER  BY obs_date DESC, pull_timestamp DESC
                LIMIT  :lim
            """),
            {"sid": f"YF:{ticker}:adj_close", "lim": lookback},
        ).fetchall()

    if len(rows) < 60:
        return None

    # rows are newest-first; reverse to chronological order
    values = np.array([float(r[1]) for r in reversed(rows)])
    return values


def generate_statistical_forecasts(
    engine: Engine,
    tickers: list[str] | None = None,
    horizons: tuple[int, ...] = (5, 10, 21),
    limit: int = 60,
) -> dict[str, Any]:
    """Generate statistical forecasts using EWMA returns + log-normal bands.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine connected to griddb.
    tickers : list[str] | None
        Explicit ticker list.  If None, auto-discover from raw_series.
    horizons : tuple[int, ...]
        Forecast horizons in trading days (5≈1wk, 10≈2wk, 21≈1mo).
    limit : int
        Max tickers to discover when ``tickers`` is None.

    Returns
    -------
    dict  with keys ``tickers``, ``forecasts_inserted``.
    """
    if tickers is None:
        tickers = _get_tickers(engine, limit=limit)

    if not tickers:
        log.warning("No tickers found — nothing to forecast")
        return {"tickers": 0, "forecasts_inserted": 0}

    inserted = 0
    skipped = 0
    today = date.today()

    for ticker in tickers:
        values = _fetch_prices(engine, ticker)
        if values is None:
            skipped += 1
            continue

        # Log returns for the trailing 60-day window
        log_returns = np.diff(np.log(values[-61:]))  # 60 returns from 61 prices
        if len(log_returns) < 30:
            skipped += 1
            continue

        last_price = float(values[-1])

        # EWMA mean with half-life ~20 days (alpha ~0.034)
        alpha = 2.0 / (20 + 1)
        weights = np.array([(1 - alpha) ** i for i in range(len(log_returns) - 1, -1, -1)])
        weights /= weights.sum()
        mu_daily = float(np.dot(weights, log_returns))

        # Annualized vol from trailing 60d
        sigma_daily = float(log_returns[-60:].std())

        for h in horizons:
            mu = mu_daily * h
            sigma = sigma_daily * np.sqrt(h)

            pred = last_price * np.exp(mu)
            lower = last_price * np.exp(mu - 1.96 * sigma)
            upper = last_price * np.exp(mu + 1.96 * sigma)
            fstd = last_price * sigma  # dollar std

            with engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO timeseries_forecasts
                            (ticker, forecast_date, horizon, predictions,
                             lower_bound, upper_bound, forecast_std, model_version)
                        VALUES
                            (:t, :fd, :h, :p, :lb, :ub, :fs, :mv)
                        ON CONFLICT (ticker, forecast_date, horizon) DO UPDATE SET
                            predictions   = EXCLUDED.predictions,
                            lower_bound   = EXCLUDED.lower_bound,
                            upper_bound   = EXCLUDED.upper_bound,
                            forecast_std  = EXCLUDED.forecast_std,
                            model_version = EXCLUDED.model_version,
                            created_at    = NOW()
                    """),
                    {
                        "t": ticker,
                        "fd": today,
                        "h": h,
                        "p": str(round(pred, 2)),
                        "lb": str(round(lower, 2)),
                        "ub": str(round(upper, 2)),
                        "fs": str(round(fstd, 2)),
                        "mv": "statistical_ewma_v1",
                    },
                )
                inserted += 1

        if inserted % 30 == 0 and inserted > 0:
            log.info("Progress: {n} forecasts inserted so far …", n=inserted)

    log.info(
        "Forecast cycle complete — {n} tickers, {ins} forecasts, {sk} skipped",
        n=len(tickers),
        ins=inserted,
        sk=skipped,
    )
    return {"tickers": len(tickers), "forecasts_inserted": inserted, "skipped": skipped}


# ── CLI entry point ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    parser = argparse.ArgumentParser(description="Statistical forecaster for GRID")
    parser.add_argument("--limit", type=int, default=60, help="Max tickers to forecast")
    args = parser.parse_args()

    from db import get_engine

    engine = get_engine()
    result = generate_statistical_forecasts(engine, limit=args.limit)
    print(result)
