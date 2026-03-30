#!/usr/bin/env python3
"""
OpenBB data ingestion pipeline for GRID.

Pulls crypto prices (every 5 min), macro data (daily), and equity data (daily)
via OpenBB Platform SDK, storing results in analytical_snapshots.

OpenBB is imported lazily inside class methods because it may only be
installed on the production server.

Run once:  python ingestion/openbb_pipeline.py
"""

from __future__ import annotations

import json
import sys
import time
from datetime import date, datetime
from typing import Any

sys.path.insert(0, "/home/grid/grid_v4/grid_repo/grid")

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_json(obj: Any) -> str:
    """Serialize to JSON, handling dates and numpy types."""
    def _default(o: Any) -> Any:
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        try:
            import numpy as np
            if isinstance(o, (np.integer,)):
                return int(o)
            if isinstance(o, (np.floating,)):
                v = float(o)
                if np.isnan(v) or np.isinf(v):
                    return None
                return v
            if isinstance(o, np.ndarray):
                return o.tolist()
        except ImportError:
            pass
        try:
            import pandas as pd
            if isinstance(o, pd.Timestamp):
                return o.isoformat()
            if isinstance(o, pd.DataFrame):
                return o.to_dict("records")
            if isinstance(o, pd.Series):
                return o.to_dict()
        except ImportError:
            pass
        return str(o)
    return json.dumps(obj, default=_default)


def _store_snapshot(
    engine: Engine,
    category: str,
    subcategory: str,
    payload: dict[str, Any],
    as_of_date: date | None = None,
    metrics: dict[str, Any] | None = None,
) -> int | None:
    """Insert one row into analytical_snapshots. Returns row id or None."""
    if as_of_date is None:
        as_of_date = date.today()
    try:
        with engine.begin() as conn:
            row = conn.execute(
                text(
                    "INSERT INTO analytical_snapshots "
                    "(snapshot_date, category, subcategory, as_of_date, payload, metrics) "
                    "VALUES (:sd, :cat, :sub, :aod, "
                    "CAST(:payload_json AS jsonb), CAST(:metrics_json AS jsonb)) "
                    "RETURNING id"
                ),
                {
                    "sd": date.today(),
                    "cat": category,
                    "sub": subcategory,
                    "aod": as_of_date,
                    "payload_json": _safe_json(payload),
                    "metrics_json": _safe_json(metrics) if metrics else None,
                },
            ).fetchone()
        snap_id = row[0] if row else None
        log.info(
            "OpenBB snapshot saved — id={id} cat={cat} sub={sub}",
            id=snap_id, cat=category, sub=subcategory,
        )
        return snap_id
    except Exception as exc:
        log.error(
            "Failed to save OpenBB snapshot ({cat}/{sub}): {e}",
            cat=category, sub=subcategory, e=str(exc),
        )
        return None


# ---------------------------------------------------------------------------
# Crypto tickers and equity watchlist
# ---------------------------------------------------------------------------

CRYPTO_TICKERS = ["BTC-USD", "ETH-USD", "SOL-USD", "DOGE-USD", "XRP-USD", "AVAX-USD", "LINK-USD"]

EQUITY_WATCHLIST = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "META", "NVDA", "TSLA",
    "JPM", "GS", "BAC", "BRK-B", "V", "MA",
    "SPY", "QQQ", "IWM", "XLF", "XLE",
]

CLI_COUNTRIES = ["united_states", "china", "g7"]


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

class OpenBBPipeline:
    """Ingest market data via OpenBB Platform into GRID analytical_snapshots.

    Categories stored:
      - crypto_price     (5-min cadence)
      - macro_cli        (daily — OECD Composite Leading Indicators)
      - macro_yield_curve(daily — Fed yield curve)
      - macro_gdp        (daily — OECD real GDP)
      - equity_filings   (daily — SEC insider filings)
      - equity_options   (daily — options chains)
      - equity_financials(daily — company fundamentals)
    """

    def __init__(self, engine: Engine) -> None:
        from openbb import obb  # lazy import — only on server
        self.obb = obb
        self.engine = engine
        self.results: dict[str, str] = {}  # task -> "ok" | error string

    # ------------------------------------------------------------------
    # Crypto (every 5 min)
    # ------------------------------------------------------------------

    def run_crypto(self) -> None:
        """Fetch historical crypto prices and store each ticker as a snapshot."""
        log.info("OpenBB crypto pipeline starting — {n} tickers", n=len(CRYPTO_TICKERS))
        for ticker in CRYPTO_TICKERS:
            task = f"crypto_price/{ticker}"
            try:
                result = self.obb.crypto.price.historical(ticker, provider="yfinance")
                df = result.to_df()
                if df.empty:
                    log.warning("No data returned for {t}", t=ticker)
                    self.results[task] = "empty"
                    continue

                # Store the most recent rows (last 24h of 5-min bars, or daily)
                payload = {
                    "ticker": ticker,
                    "rows": len(df),
                    "latest_date": str(df.index[-1]) if hasattr(df.index, '__len__') else str(df.iloc[-1].name),
                    "data": df.tail(300).reset_index().to_dict("records"),
                }
                metrics = {
                    "last_close": float(df["close"].iloc[-1]) if "close" in df.columns else None,
                    "last_volume": float(df["volume"].iloc[-1]) if "volume" in df.columns else None,
                }
                _store_snapshot(
                    self.engine,
                    category="crypto_price",
                    subcategory=ticker.replace("-USD", ""),
                    payload=payload,
                    metrics=metrics,
                )
                self.results[task] = "ok"
            except Exception as exc:
                log.error("OpenBB crypto failed for {t}: {e}", t=ticker, e=str(exc))
                self.results[task] = str(exc)

    # ------------------------------------------------------------------
    # Macro (daily)
    # ------------------------------------------------------------------

    def run_macro(self) -> None:
        """Fetch macro indicators: CLI, yield curve, GDP."""
        self._fetch_cli()
        self._fetch_yield_curve()
        self._fetch_gdp()

    def _fetch_cli(self) -> None:
        """OECD Composite Leading Indicators for US, China, G7."""
        for country in CLI_COUNTRIES:
            task = f"macro_cli/{country}"
            try:
                result = self.obb.economy.composite_leading_indicator(
                    provider="oecd", country=country,
                )
                df = result.to_df()
                if df.empty:
                    self.results[task] = "empty"
                    continue
                payload = {
                    "country": country,
                    "rows": len(df),
                    "data": df.tail(60).reset_index().to_dict("records"),
                }
                metrics = {
                    "latest_value": float(df.iloc[-1].values[0]) if len(df.columns) else None,
                }
                _store_snapshot(
                    self.engine,
                    category="macro_cli",
                    subcategory=country,
                    payload=payload,
                    metrics=metrics,
                )
                self.results[task] = "ok"
            except Exception as exc:
                log.error("OpenBB CLI failed for {c}: {e}", c=country, e=str(exc))
                self.results[task] = str(exc)

    def _fetch_yield_curve(self) -> None:
        """Federal Reserve yield curve."""
        task = "macro_yield_curve/us"
        try:
            result = self.obb.fixedincome.government.yield_curve(provider="federal_reserve")
            df = result.to_df()
            if df.empty:
                self.results[task] = "empty"
                return
            payload = {
                "rows": len(df),
                "data": df.reset_index().to_dict("records"),
            }
            # Extract 2Y-10Y spread if columns available
            metrics: dict[str, Any] = {}
            if "rate" in df.columns and "maturity" in df.columns:
                tens = df.loc[df["maturity"] == "10y", "rate"]
                twos = df.loc[df["maturity"] == "2y", "rate"]
                if len(tens) and len(twos):
                    metrics["spread_2s10s"] = float(tens.iloc[0] - twos.iloc[0])
            _store_snapshot(
                self.engine,
                category="macro_yield_curve",
                subcategory="us",
                payload=payload,
                metrics=metrics or None,
            )
            self.results[task] = "ok"
        except Exception as exc:
            log.error("OpenBB yield curve failed: {e}", e=str(exc))
            self.results[task] = str(exc)

    def _fetch_gdp(self) -> None:
        """OECD real GDP."""
        task = "macro_gdp/oecd"
        try:
            result = self.obb.economy.gdp.real(provider="oecd")
            df = result.to_df()
            if df.empty:
                self.results[task] = "empty"
                return
            payload = {
                "rows": len(df),
                "data": df.tail(40).reset_index().to_dict("records"),
            }
            _store_snapshot(
                self.engine,
                category="macro_gdp",
                subcategory="oecd",
                payload=payload,
            )
            self.results[task] = "ok"
        except Exception as exc:
            log.error("OpenBB GDP failed: {e}", e=str(exc))
            self.results[task] = str(exc)

    # ------------------------------------------------------------------
    # Equity (daily)
    # ------------------------------------------------------------------

    def run_equity(self) -> None:
        """Fetch equity data: SEC filings, options chains, financials."""
        self._fetch_filings()
        self._fetch_options_chains()
        self._fetch_financials()

    def _fetch_filings(self) -> None:
        """SEC insider filings for watchlist tickers."""
        for ticker in EQUITY_WATCHLIST:
            task = f"equity_filings/{ticker}"
            try:
                result = self.obb.equity.fundamental.filings(
                    ticker, provider="sec",
                )
                df = result.to_df()
                if df.empty:
                    self.results[task] = "empty"
                    continue
                payload = {
                    "ticker": ticker,
                    "rows": len(df),
                    "data": df.head(50).reset_index().to_dict("records"),
                }
                _store_snapshot(
                    self.engine,
                    category="equity_filings",
                    subcategory=ticker,
                    payload=payload,
                )
                self.results[task] = "ok"
                # Be polite to SEC EDGAR
                time.sleep(0.5)
            except Exception as exc:
                log.error("OpenBB filings failed for {t}: {e}", t=ticker, e=str(exc))
                self.results[task] = str(exc)

    def _fetch_options_chains(self) -> None:
        """Options chains for key ETFs."""
        options_tickers = ["SPY", "QQQ", "IWM"]
        for ticker in options_tickers:
            task = f"equity_options/{ticker}"
            try:
                result = self.obb.derivatives.options.chains(ticker, provider="cboe")
                df = result.to_df()
                if df.empty:
                    self.results[task] = "empty"
                    continue
                payload = {
                    "ticker": ticker,
                    "rows": len(df),
                    "data": df.to_dict("records"),
                }
                # Summary metrics
                metrics: dict[str, Any] = {}
                if "open_interest" in df.columns:
                    metrics["total_oi"] = int(df["open_interest"].sum())
                if "volume" in df.columns:
                    metrics["total_volume"] = int(df["volume"].sum())
                if "option_type" in df.columns:
                    calls = df[df["option_type"] == "call"]
                    puts = df[df["option_type"] == "put"]
                    if len(puts) and puts["volume"].sum() > 0:
                        metrics["put_call_ratio"] = float(
                            puts["volume"].sum() / calls["volume"].sum()
                        ) if calls["volume"].sum() > 0 else None
                _store_snapshot(
                    self.engine,
                    category="equity_options",
                    subcategory=ticker,
                    payload=payload,
                    metrics=metrics or None,
                )
                self.results[task] = "ok"
            except Exception as exc:
                log.error("OpenBB options failed for {t}: {e}", t=ticker, e=str(exc))
                self.results[task] = str(exc)

    def _fetch_financials(self) -> None:
        """Company financials (income statement) for watchlist."""
        # Subset to avoid hammering the API
        fin_tickers = ["AAPL", "MSFT", "AMZN", "GOOGL", "META", "NVDA", "TSLA", "JPM", "GS"]
        for ticker in fin_tickers:
            task = f"equity_financials/{ticker}"
            try:
                result = self.obb.equity.fundamental.income(
                    ticker, provider="sec", period="annual", limit=4,
                )
                df = result.to_df()
                if df.empty:
                    self.results[task] = "empty"
                    continue
                payload = {
                    "ticker": ticker,
                    "rows": len(df),
                    "data": df.reset_index().to_dict("records"),
                }
                metrics: dict[str, Any] = {}
                if "revenue" in df.columns:
                    metrics["latest_revenue"] = float(df["revenue"].iloc[0])
                if "net_income" in df.columns:
                    metrics["latest_net_income"] = float(df["net_income"].iloc[0])
                _store_snapshot(
                    self.engine,
                    category="equity_financials",
                    subcategory=ticker,
                    payload=payload,
                    metrics=metrics or None,
                )
                self.results[task] = "ok"
                time.sleep(0.5)
            except Exception as exc:
                log.error("OpenBB financials failed for {t}: {e}", t=ticker, e=str(exc))
                self.results[task] = str(exc)

    # ------------------------------------------------------------------
    # Run all
    # ------------------------------------------------------------------

    def run_all(self) -> dict[str, str]:
        """Execute every pipeline stage. Returns results dict."""
        t0 = time.time()
        log.info("=== OpenBB full pipeline starting ===")

        for stage_name, stage_fn in [
            ("crypto", self.run_crypto),
            ("macro", self.run_macro),
            ("equity", self.run_equity),
        ]:
            try:
                stage_fn()
            except Exception as exc:
                log.error("OpenBB stage '{s}' crashed: {e}", s=stage_name, e=str(exc))
                self.results[f"STAGE_{stage_name}"] = str(exc)

        elapsed = time.time() - t0
        ok = sum(1 for v in self.results.values() if v == "ok")
        fail = sum(1 for v in self.results.values() if v != "ok" and v != "empty")
        empty = sum(1 for v in self.results.values() if v == "empty")
        log.info(
            "=== OpenBB pipeline done in {t:.1f}s — ok={ok} empty={e} failed={f} ===",
            t=elapsed, ok=ok, e=empty, f=fail,
        )
        # Log failures explicitly
        for task, result in self.results.items():
            if result not in ("ok", "empty"):
                log.warning("  FAILED: {task} — {err}", task=task, err=result[:200])

        return self.results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import os, sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from db import get_engine

    engine = get_engine()
    pipeline = OpenBBPipeline(engine)
    results = pipeline.run_all()

    # Print summary
    print("\n--- OpenBB Pipeline Results ---")
    for task, status in sorted(results.items()):
        marker = "OK" if status == "ok" else ("EMPTY" if status == "empty" else "FAIL")
        print(f"  [{marker}] {task}")
    total = len(results)
    ok = sum(1 for v in results.values() if v == "ok")
    print(f"\n  Total: {total}  |  OK: {ok}  |  Failed: {total - ok}")
