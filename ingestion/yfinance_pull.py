"""
GRID yfinance data ingestion module.

Downloads OHLCV market data from Yahoo Finance via the ``yfinance`` library
and stores each field as a separate entry in ``raw_series``.
"""

from __future__ import annotations

import logging
import re
from datetime import date
from typing import Any

import pandas as pd
import yfinance as yf
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# yfinance logs missing/delisted symbols at ERROR level internally. The puller
# already downgrades those outcomes to PARTIAL/SKIPPED, so keep the third-party
# logger from polluting production error scans.
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

# Default tickers to pull
YF_TICKER_LIST: list[str] = [
    # US Equity Indices (ETF proxies — Tiingo compatible)
    "SPY", "DIA", "QQQ", "IWM",
    # Also keep originals for yfinance fallback
    "^GSPC", "^DJI", "^IXIC", "^RUT", "^VIX",
    # Sector ETFs
    "XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLU", "XLRE", "XLB", "XLC",
    # Thematic/Subsector ETFs (sector_map proxies)
    "SMH", "KRE", "ICLN", "LIT", "XBI", "ITA",
    # Sector-map companies
    "TSM",
    # Bond ETFs
    "TLT", "IEF", "SHY", "LQD", "HYG", "JNK", "EMB", "MUB",
    # Commodity ETFs
    "GLD", "SLV", "USO", "DBA", "PDBC",
    # Currency
    "UUP", "FXE", "FXY", "EEM", "DX-Y.NYB",
    # VIX Term Structure
    "^VIX9D", "^VIX3M", "^VIX6M",
    # Futures
    "HG=F", "GC=F", "SI=F", "CL=F",
    # FX Pairs
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "USDCHF=X", "USDCAD=X", "NZDUSD=X",
    # Crypto (queried by layer_crypto.py)
    "BTC-USD", "ETH-USD",
    # International (sovereign/cross-border flow proxies)
    "FXI", "EWJ", "EWZ", "EFA",
    # Real yields / TIPS
    "TIP",
    # Copper / industrial metals (bellwether)
    "COPX",
    # High-yield / distress credit
    "SJNK", "BKLN", "ANGL",
]

# OHLCV fields to store individually
_FIELDS: list[str] = ["Open", "High", "Low", "Close", "Volume", "Adj Close"]
_FIELD_MAP: dict[str, str] = {
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Volume": "volume",
    "Adj Close": "adj_close",
}

_INVALID_TICKER_VALUES = {"", "N/A", "NA", "NONE", "NULL", "-"}
_CLASS_SHARE_RE = re.compile(r"^[A-Z]{1,5}\.[A-Z]$")


def _normalize_yahoo_ticker(ticker: str) -> str | None:
    """Return a yfinance-compatible ticker or None for junk symbols."""
    clean = str(ticker or "").strip().strip("\"'")
    if clean.upper() in _INVALID_TICKER_VALUES:
        return None
    if clean.startswith("(") and clean.endswith(")"):
        clean = clean[1:-1].strip()
    if not clean or clean.upper() in _INVALID_TICKER_VALUES:
        return None
    if ":" in clean or "/" in clean:
        return None
    if _CLASS_SHARE_RE.match(clean):
        clean = clean.replace(".", "-")
    return clean


class YFinancePuller(BasePuller):
    """Pulls OHLCV data from Yahoo Finance into ``raw_series``.

    Attributes:
        engine: SQLAlchemy engine for database writes.
        source_id: The ``source_catalog.id`` for the yfinance source.
    """

    SOURCE_NAME: str = "yfinance"

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the yfinance puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        log.info("YFinancePuller initialised — source_id={sid}", sid=self.source_id)

    def pull_ticker(
        self,
        ticker: str,
        start_date: str | date,
        end_date: str | date | None = None,
        interval: str = "1d",
    ) -> dict[str, Any]:
        """Download OHLCV data for a single ticker and insert into raw_series.

        Each OHLCV field is stored as a separate series with the naming
        convention ``YF:{ticker}:{field}`` (e.g. ``YF:^GSPC:close``).

        Parameters:
            ticker: Yahoo Finance ticker symbol.
            start_date: Earliest date to download.
            end_date: Latest date (default: today).
            interval: Data frequency ('1d', '1wk', '1mo').

        Returns:
            dict: Result with keys ``ticker``, ``rows_inserted``, ``status``, ``errors``.
        """
        yf_ticker = _normalize_yahoo_ticker(ticker)
        result: dict[str, Any] = {
            "ticker": ticker,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }
        if yf_ticker is None:
            result["status"] = "SKIPPED"
            result["errors"].append("Invalid Yahoo ticker")
            log.warning("yfinance {t}: invalid ticker; skipping", t=ticker)
            return result

        log.info("Pulling yfinance ticker {t} from {sd}", t=yf_ticker, sd=start_date)

        try:
            df: pd.DataFrame = yf.download(
                yf_ticker,
                start=str(start_date),
                end=str(end_date) if end_date else None,
                interval=interval,
                progress=False,
                auto_adjust=False,
            )

            # yfinance >=0.2.31 returns MultiIndex columns (field, ticker)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            # Drop any rows where the index is not a valid datetime
            # (yfinance MultiIndex flattening can leak column names as rows)
            valid_idx = pd.to_datetime(df.index, errors="coerce").notna()
            if not valid_idx.all():
                log.warning(
                    "yfinance {t}: dropped {n} non-datetime index entries",
                    t=ticker,
                    n=int((~valid_idx).sum()),
                )
                df = df[valid_idx]

            if df is None or df.empty:
                log.warning("yfinance returned no data for {t}", t=yf_ticker)
                result["status"] = "PARTIAL"
                result["errors"].append("No data returned")
                return result

            inserted = 0

            with self.engine.begin() as conn:
                for col_name, field_key in _FIELD_MAP.items():
                    if col_name not in df.columns:
                        continue

                    series_id = f"YF:{yf_ticker}:{field_key}"
                    col_data = df[col_name].dropna()
                    existing_dates = self._get_existing_dates(series_id, conn)

                    for dt_idx, value in col_data.items():
                        float_val = float(value)

                        # Sanity guard: skip obviously corrupt prices
                        if field_key == "adj_close" and (
                            float_val > 100_000 or float_val < 0.001
                        ):
                            log.warning(
                                "Suspicious adj_close for {t}: {v} on {d} — skipping",
                                t=ticker,
                                v=float_val,
                                d=dt_idx,
                            )
                            continue

                        obs_date_val = dt_idx.date() if hasattr(dt_idx, "date") else dt_idx
                        if obs_date_val in existing_dates:
                            continue

                        conn.execute(
                            text(
                                "INSERT INTO raw_series "
                                "(series_id, source_id, obs_date, value, pull_status) "
                                "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                            ),
                            {
                                "sid": series_id,
                                "src": self.source_id,
                                "od": obs_date_val,
                                "val": float_val,
                            },
                        )
                        existing_dates.add(obs_date_val)
                        inserted += 1

            result["rows_inserted"] = inserted
            log.info("yfinance {t}: inserted {n} rows", t=yf_ticker, n=inserted)

        except Exception as exc:
            log.warning(
                "yfinance {t}: could not pull cleanly: {err}",
                t=yf_ticker,
                err=str(exc),
            )
            result["status"] = "SKIPPED"
            result["errors"].append(str(exc))

        return result

    def pull_all(
        self,
        ticker_list: list[str] | None = None,
        start_date: str | date = "1990-01-01",
    ) -> list[dict[str, Any]]:
        """Pull multiple tickers sequentially.

        Never stops on a single-ticker failure — logs and continues.

        Parameters:
            ticker_list: List of Yahoo Finance ticker symbols.
                         Defaults to YF_TICKER_LIST.
            start_date: Earliest observation date.

        Returns:
            list[dict]: One result dict per ticker.
        """
        if ticker_list is None:
            ticker_list = YF_TICKER_LIST

        log.info(
            "Starting yfinance bulk pull — {n} tickers from {sd}",
            n=len(ticker_list),
            sd=start_date,
        )
        results: list[dict[str, Any]] = []
        for ticker in ticker_list:
            res = self.pull_ticker(ticker, start_date)
            results.append(res)

        log.info(
            "yfinance bulk pull complete — {ok}/{total} succeeded",
            ok=sum(1 for r in results if r["status"] == "SUCCESS"),
            total=len(results),
        )
        return results


if __name__ == "__main__":
    from db import get_engine

    puller = YFinancePuller(db_engine=get_engine())
    results = puller.pull_all(start_date="2020-01-01")
    for r in results:
        print(f"  {r['ticker']}: {r['status']} ({r['rows_inserted']} rows)")
