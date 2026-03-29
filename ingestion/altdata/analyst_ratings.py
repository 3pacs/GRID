"""
GRID analyst ratings ingestion module.

Pulls analyst recommendation counts (buy/hold/sell) from yfinance for
each ticker in the watchlist. yfinance exposes this via
``yf.Ticker(t).recommendations`` which returns a DataFrame with
strongBuy, buy, hold, sell, strongSell columns.

Series stored with pattern: ANALYST:{ticker_prefix}_analyst_{metric}
where metric is buy, hold, or sell. Buy combines strongBuy + buy;
sell combines strongSell + sell.

Features follow the naming convention already in feature_registry:
  ci_analyst_buy, cmcsa_analyst_hold, eog_analyst_sell, etc.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timezone
from typing import Any

import yfinance as yf
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ---- Watchlist: ticker -> lowercase prefix used in feature_registry ----
# Matches the watchlist in scripts/load_ticker_deep.py.
WATCHLIST: dict[str, str] = {
    "BTC-USD": "btc",
    "ETH-USD": "eth",
    "SOL-USD": "sol",
    "EOG": "eog",
    "DVN": "dvn",
    "CMCSA": "cmcsa",
    "RTX": "rtx",
    "GD": "gd",
    "CI": "ci",
    "PYPL": "pypl",
    "INTC": "intc",
    "SPY": "spy",
    "QQQ": "qqq",
    "IWM": "iwm",
    "XLE": "xle",
    "XLF": "xlf",
    "ITA": "ita",
    "TLT": "tlt",
    "GLD": "gld",
    "URA": "ura",
}

# Metrics we extract from each recommendation row
_METRICS: list[str] = ["buy", "hold", "sell"]

# Rate limiting between yfinance calls
_RATE_LIMIT_DELAY: float = 0.6

# Series ID prefix for entity_map lookups
_SERIES_PREFIX: str = "ANALYST"


class AnalystRatingsPuller(BasePuller):
    """Pull analyst recommendation counts from yfinance.

    For each watchlist ticker, fetches the latest analyst
    recommendation summary (strongBuy, buy, hold, sell, strongSell)
    and stores aggregated buy (strongBuy + buy), hold, and sell
    (strongSell + sell) counts as separate raw_series rows.

    The puller auto-discovers which tickers have matching
    ``{prefix}_analyst_{metric}`` entries in feature_registry so it
    only pulls data for features the system actually needs.

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for Analyst_Ratings.
    """

    SOURCE_NAME: str = "Analyst_Ratings"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://finance.yahoo.com",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "FREQUENT",
        "trust_score": "MED",
        "priority_rank": 42,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the analyst ratings puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        self._needed_features: dict[str, int] = {}
        self._discover_needed_features()
        log.info(
            "AnalystRatingsPuller initialised -- source_id={sid}, "
            "{n} features tracked across {t} tickers",
            sid=self.source_id,
            n=len(self._needed_features),
            t=len(self._get_needed_tickers()),
        )

    def _discover_needed_features(self) -> None:
        """Scan feature_registry for analyst rating features that exist.

        Populates self._needed_features with {feature_name: feature_id}
        for all features matching the ``{prefix}_analyst_{metric}`` pattern.
        """
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT id, name FROM feature_registry "
                    "WHERE name LIKE :pat AND family = 'sentiment'"
                ),
                {"pat": "%_analyst_%"},
            ).fetchall()
        self._needed_features = {r[1]: r[0] for r in rows}
        log.debug(
            "Discovered {n} analyst rating features in registry",
            n=len(self._needed_features),
        )

    def _get_needed_tickers(self) -> dict[str, str]:
        """Return subset of WATCHLIST that has features in the registry.

        Returns:
            dict of ticker -> prefix for tickers that have at least one
            analyst rating feature registered.
        """
        needed: dict[str, str] = {}
        for ticker, prefix in WATCHLIST.items():
            for metric in _METRICS:
                feat_name = f"{prefix}_analyst_{metric}"
                if feat_name in self._needed_features:
                    needed[ticker] = prefix
                    break
        return needed

    def _series_id(self, prefix: str, metric: str) -> str:
        """Build the raw series_id for entity_map resolution.

        Parameters:
            prefix: Lowercase ticker prefix (e.g. 'ci').
            metric: One of 'buy', 'hold', 'sell'.

        Returns:
            Series ID string (e.g. 'ANALYST:ci_analyst_buy').
        """
        return f"{_SERIES_PREFIX}:{prefix}_analyst_{metric}"

    def _feature_name(self, prefix: str, metric: str) -> str:
        """Build the feature_registry name.

        Parameters:
            prefix: Lowercase ticker prefix.
            metric: One of 'buy', 'hold', 'sell'.

        Returns:
            Feature name (e.g. 'ci_analyst_buy').
        """
        return f"{prefix}_analyst_{metric}"

    @retry_on_failure(
        max_attempts=2,
        backoff=3.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError),
    )
    def _fetch_recommendations(self, ticker: str) -> dict[str, float] | None:
        """Fetch the latest analyst recommendations for a ticker.

        Parameters:
            ticker: Yahoo Finance ticker symbol (e.g. 'CI', 'SPY').

        Returns:
            Dict with keys 'buy', 'hold', 'sell' (aggregated counts),
            or None if no data available.
        """
        try:
            t = yf.Ticker(ticker)
            recs = t.recommendations
        except Exception as exc:
            log.warning(
                "yfinance recommendations fetch failed for {t}: {e}",
                t=ticker,
                e=str(exc),
            )
            return None

        if recs is None or recs.empty:
            log.debug("No recommendations for {t}", t=ticker)
            return None

        # Take the most recent row
        recent = recs.tail(1).iloc[0]
        buy = float(recent.get("strongBuy", 0) or 0) + float(recent.get("buy", 0) or 0)
        sell = float(recent.get("strongSell", 0) or 0) + float(recent.get("sell", 0) or 0)
        hold = float(recent.get("hold", 0) or 0)

        return {"buy": buy, "hold": hold, "sell": sell}

    def pull_ticker(self, ticker: str, prefix: str) -> dict[str, Any]:
        """Pull analyst ratings for a single ticker and store in raw_series.

        Parameters:
            ticker: Yahoo Finance ticker symbol.
            prefix: Lowercase prefix for feature names.

        Returns:
            Result dict with status and rows_inserted.
        """
        ratings = self._fetch_recommendations(ticker)
        if ratings is None:
            return {"status": "SKIPPED", "ticker": ticker, "rows_inserted": 0}

        today = date.today()
        inserted = 0

        with self.engine.begin() as conn:
            for metric in _METRICS:
                feat_name = self._feature_name(prefix, metric)
                if feat_name not in self._needed_features:
                    continue

                sid = self._series_id(prefix, metric)
                value = ratings[metric]

                # Dedup: skip if we already have today's data
                if self._row_exists(sid, today, conn):
                    log.debug(
                        "Analyst {sid} already has data for {d}, skipping",
                        sid=sid,
                        d=today,
                    )
                    continue

                self._insert_raw(
                    conn=conn,
                    series_id=sid,
                    obs_date=today,
                    value=value,
                    raw_payload={
                        "ticker": ticker,
                        "prefix": prefix,
                        "metric": metric,
                        "buy": ratings["buy"],
                        "hold": ratings["hold"],
                        "sell": ratings["sell"],
                        "pull_date": today.isoformat(),
                    },
                )
                inserted += 1

        if inserted > 0:
            log.info(
                "Analyst {t} ({p}): {n} metrics stored (buy={b}, hold={h}, sell={s})",
                t=ticker,
                p=prefix,
                n=inserted,
                b=ratings["buy"],
                h=ratings["hold"],
                s=ratings["sell"],
            )

        return {
            "status": "SUCCESS",
            "ticker": ticker,
            "rows_inserted": inserted,
            "ratings": ratings,
        }

    def pull_all(self, tickers: dict[str, str] | None = None) -> dict[str, Any]:
        """Pull analyst ratings for all watchlist tickers (batch method).

        Parameters:
            tickers: Override watchlist dict {ticker: prefix}.
                     Default: auto-discovered tickers with registry features.

        Returns:
            Summary dict with status, total rows, and per-ticker results.
        """
        if tickers is None:
            tickers = self._get_needed_tickers()

        if not tickers:
            log.warning("AnalystRatings: no tickers with registered features found")
            return {"status": "SUCCESS", "rows_inserted": 0, "results": []}

        log.info(
            "AnalystRatings: pulling {n} tickers: {t}",
            n=len(tickers),
            t=", ".join(tickers.keys()),
        )

        results: list[dict[str, Any]] = []
        total_inserted = 0

        for ticker, prefix in tickers.items():
            try:
                result = self.pull_ticker(ticker, prefix)
                results.append(result)
                total_inserted += result["rows_inserted"]
            except Exception as exc:
                log.error(
                    "AnalystRatings: {t} failed: {e}",
                    t=ticker,
                    e=str(exc),
                )
                results.append({
                    "status": "FAILED",
                    "ticker": ticker,
                    "rows_inserted": 0,
                    "error": str(exc),
                })

            time.sleep(_RATE_LIMIT_DELAY)

        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "AnalystRatings pull_all complete -- {ok}/{total} tickers, "
            "{rows} raw_series rows inserted",
            ok=succeeded,
            total=len(results),
            rows=total_inserted,
        )

        return {
            "status": "SUCCESS",
            "rows_inserted": total_inserted,
            "tickers_pulled": succeeded,
            "tickers_total": len(results),
            "results": results,
        }
