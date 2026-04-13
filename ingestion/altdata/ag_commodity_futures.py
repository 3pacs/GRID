"""
GRID agricultural + industrial commodity futures ingestion.

Pulls daily OHLCV close prices from yfinance for 14+ commodity futures
contracts (cocoa, coffee, sugar, wheat, corn, soybeans, OJ, cotton, cattle,
hogs, lumber, aluminum, copper, nat gas, plus metals/energy backfills).

Writes to ``raw_series`` with ``series_id = YF:{TICKER}:close`` to match
the existing convention consumed by ``intelligence/cross_lens.py``.  This
unblocks cost pass-through attributions for food companies (cocoa -> HSY,
wheat -> GIS, sugar -> KO), industrial chains (copper -> FCX, aluminum
-> AA), and energy (nat gas -> utilities, livestock -> protein packers).

Rationale for a dedicated module (instead of reusing ``yfinance_pull``):
the general YF puller is equity-centric and does not cover futures root
tickers.  Keeping commodity futures in a narrow module makes cadence,
quality checks, and rate limits trivially tunable.

Usage::

    from db import get_engine
    puller = AgCommodityFuturesPuller(db_engine=get_engine())
    puller.pull_all(backfill_days=1825)  # 5 years
"""

from __future__ import annotations

import time
from datetime import date, timedelta
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# ── Ticker universe ───────────────────────────────────────────────────────

# Agricultural softs + grains + livestock + lumber
AG_FUTURES: list[str] = [
    "CC=F",   # cocoa
    "KC=F",   # coffee
    "SB=F",   # sugar
    "ZW=F",   # wheat (CBOT)
    "ZC=F",   # corn
    "ZS=F",   # soybeans
    "OJ=F",   # orange juice
    "CT=F",   # cotton
    "LE=F",   # live cattle
    "HE=F",   # lean hogs
    "LBR=F",  # lumber
]

# Industrial metals + energy gaps
INDUSTRIAL_FUTURES: list[str] = [
    "ALI=F",  # aluminum
    "HG=F",   # copper
    "NG=F",   # natural gas
]

# Backfill/verify tickers (these may already be live from yfinance_pull)
METALS_ENERGY_BACKFILL: list[str] = [
    "GC=F",   # gold
    "SI=F",   # silver
    "PL=F",   # platinum
    "PA=F",   # palladium
    "BZ=F",   # Brent crude
]

ALL_FUTURES: list[str] = AG_FUTURES + INDUSTRIAL_FUTURES + METALS_ENERGY_BACKFILL

# yfinance allows ~10 tickers per batch reliably.
_YF_BATCH_SIZE: int = 10

# Rate limit between batches — be polite to Yahoo.
_YF_BATCH_DELAY_SEC: float = 0.5

# Default backfill window (5 years of daily bars ~= 1250 trading days).
DEFAULT_BACKFILL_DAYS: int = 5 * 365

# Incremental pull window when the series already has data.
INCREMENTAL_DAYS: int = 10


class AgCommodityFuturesPuller(BasePuller):
    """Pulls daily close prices for agricultural + industrial commodity futures.

    For each ticker in ``ALL_FUTURES``, fetches daily OHLCV via yfinance
    and writes one row per trading day into ``raw_series`` keyed by
    ``YF:{TICKER}:close``.  Uses ``_get_existing_dates`` for cheap
    deduplication so backfills can be re-run idempotently.

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved ``source_catalog.id`` for this puller.
    """

    SOURCE_NAME: str = "YFINANCE_COMMODITY_FUTURES"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://finance.yahoo.com",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 18,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the commodity futures puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        log.info(
            "AgCommodityFuturesPuller initialised — source_id={sid}, "
            "tickers={n}",
            sid=self.source_id,
            n=len(ALL_FUTURES),
        )

    # ── Core fetch ────────────────────────────────────────────────────────

    def _fetch_batch(
        self,
        tickers: list[str],
        start: date,
        end: date,
    ) -> dict[str, pd.DataFrame]:
        """Download daily OHLCV for a batch of tickers via yfinance.

        Parameters:
            tickers: List of yfinance ticker strings (e.g. ``['CC=F', 'KC=F']``).
            start: Inclusive start date.
            end: Exclusive end date (yfinance convention).

        Returns:
            Mapping of ticker -> single-ticker OHLCV DataFrame.  Missing
            tickers are omitted from the result.
        """
        try:
            import yfinance as yf
        except ImportError:
            log.error("yfinance not installed — commodity futures unavailable")
            return {}

        try:
            data = yf.download(
                tickers=" ".join(tickers),
                start=str(start),
                end=str(end + timedelta(days=1)),
                progress=False,
                group_by="ticker",
                auto_adjust=False,
                threads=False,
            )
        except Exception as exc:
            log.error(
                "yf.download failed for batch {t}: {e}",
                t=tickers,
                e=str(exc),
            )
            return {}

        if data is None or data.empty:
            log.warning("yf.download returned empty for batch {t}", t=tickers)
            return {}

        out: dict[str, pd.DataFrame] = {}

        # Single ticker path: yfinance returns flat columns, no ticker level.
        if len(tickers) == 1:
            tkr = tickers[0]
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            if "Close" in data.columns:
                out[tkr] = data
            return out

        # Multi-ticker path: MultiIndex columns, either (ticker, field)
        # or (field, ticker) depending on yfinance version.
        if isinstance(data.columns, pd.MultiIndex):
            level0 = set(data.columns.get_level_values(0))
            for tkr in tickers:
                if tkr in level0:
                    sub = data[tkr].copy()
                elif tkr in set(data.columns.get_level_values(1)):
                    sub = data.xs(tkr, axis=1, level=1).copy()
                else:
                    log.warning("Ticker {t} missing from batch response", t=tkr)
                    continue
                if "Close" in sub.columns:
                    out[tkr] = sub
        else:
            # Flat columns only happen when yfinance consolidates a single
            # usable ticker — best effort.
            if len(tickers) == 1 and "Close" in data.columns:
                out[tickers[0]] = data

        return out

    def _store_ticker(
        self,
        ticker: str,
        df: pd.DataFrame,
    ) -> tuple[int, int]:
        """Insert close-price rows for a single ticker, skipping duplicates.

        Parameters:
            ticker: Yahoo Finance ticker string (e.g. ``'CC=F'``).
            df: OHLCV DataFrame from ``_fetch_batch``.

        Returns:
            (rows_inserted, rows_skipped) tuple.
        """
        series_id = f"YF:{ticker}:close"
        if df is None or df.empty or "Close" not in df.columns:
            return 0, 0

        inserted = 0
        skipped = 0

        with self.engine.begin() as conn:
            existing = self._get_existing_dates(series_id, conn)
            for idx, row in df.iterrows():
                close_val = row.get("Close")
                if pd.isna(close_val):
                    continue
                obs_date = idx.date() if hasattr(idx, "date") else idx
                if obs_date in existing:
                    skipped += 1
                    continue
                payload: dict[str, Any] = {
                    "ticker": ticker,
                    "open": float(row["Open"])
                    if "Open" in df.columns and not pd.isna(row["Open"])
                    else None,
                    "high": float(row["High"])
                    if "High" in df.columns and not pd.isna(row["High"])
                    else None,
                    "low": float(row["Low"])
                    if "Low" in df.columns and not pd.isna(row["Low"])
                    else None,
                    "volume": int(row["Volume"])
                    if "Volume" in df.columns and not pd.isna(row["Volume"])
                    else None,
                }
                self._insert_raw(
                    conn=conn,
                    series_id=series_id,
                    obs_date=obs_date,
                    value=float(close_val),
                    raw_payload=payload,
                )
                inserted += 1

        return inserted, skipped

    # ── Public API ────────────────────────────────────────────────────────

    def pull_all(
        self,
        backfill_days: int | None = None,
    ) -> list[dict[str, Any]]:
        """Pull daily close prices for all commodity futures.

        For each ticker, determines whether this is a first-time pull
        (no data in ``raw_series``) or an incremental update, then
        batches yfinance downloads in groups of ``_YF_BATCH_SIZE``.

        Parameters:
            backfill_days: Explicit number of days to backfill.  If None,
                backfills 5 years on first run and falls back to 10 days
                for subsequent runs.

        Returns:
            List of per-ticker result dicts with keys ``feature``,
            ``status``, ``rows_inserted``, ``rows_skipped``, and optionally
            ``error``.
        """
        end_date = date.today()
        results: list[dict[str, Any]] = []

        # Determine per-ticker start dates.  We run the whole universe
        # with a single start date (the earliest needed) so we can batch
        # yfinance calls.  Individual tickers that already have history
        # still skip duplicates via _get_existing_dates.
        explicit_days = (
            backfill_days
            if backfill_days is not None
            else DEFAULT_BACKFILL_DAYS
        )

        # If every ticker already has recent data, shrink the window.
        all_fresh = True
        for ticker in ALL_FUTURES:
            latest = self._get_latest_date(f"YF:{ticker}:close")
            if latest is None or latest < end_date - timedelta(days=INCREMENTAL_DAYS + 5):
                all_fresh = False
                break
        if backfill_days is None and all_fresh:
            explicit_days = INCREMENTAL_DAYS

        start_date = end_date - timedelta(days=explicit_days)
        log.info(
            "AgCommodityFutures pull — {n} tickers, {s} -> {e} ({d} days)",
            n=len(ALL_FUTURES),
            s=start_date,
            e=end_date,
            d=explicit_days,
        )

        # Process in batches.
        for i in range(0, len(ALL_FUTURES), _YF_BATCH_SIZE):
            batch = ALL_FUTURES[i : i + _YF_BATCH_SIZE]
            log.info(
                "Fetching batch {i}/{t}: {b}",
                i=i // _YF_BATCH_SIZE + 1,
                t=(len(ALL_FUTURES) + _YF_BATCH_SIZE - 1) // _YF_BATCH_SIZE,
                b=batch,
            )
            frames = self._fetch_batch(batch, start_date, end_date)

            for ticker in batch:
                df = frames.get(ticker)
                if df is None or df.empty:
                    log.warning("No data returned for {t}", t=ticker)
                    results.append({
                        "feature": f"YF:{ticker}:close",
                        "status": "NO_DATA",
                        "rows_inserted": 0,
                        "rows_skipped": 0,
                    })
                    continue
                try:
                    inserted, skipped = self._store_ticker(ticker, df)
                    results.append({
                        "feature": f"YF:{ticker}:close",
                        "status": "SUCCESS",
                        "rows_inserted": inserted,
                        "rows_skipped": skipped,
                    })
                    log.info(
                        "{t}: {i} inserted, {s} skipped",
                        t=ticker,
                        i=inserted,
                        s=skipped,
                    )
                except Exception as exc:
                    log.exception("Store failed for {t}", t=ticker)
                    results.append({
                        "feature": f"YF:{ticker}:close",
                        "status": "FAILED",
                        "rows_inserted": 0,
                        "rows_skipped": 0,
                        "error": str(exc),
                    })

            time.sleep(_YF_BATCH_DELAY_SEC)

        total_inserted = sum(r.get("rows_inserted", 0) for r in results)
        ok = sum(1 for r in results if r.get("status") == "SUCCESS")
        log.info(
            "AgCommodityFutures complete — {ok}/{total} ok, {ins} rows inserted",
            ok=ok,
            total=len(results),
            ins=total_inserted,
        )
        return results


if __name__ == "__main__":
    from db import get_engine

    puller = AgCommodityFuturesPuller(db_engine=get_engine())
    results = puller.pull_all()
    for r in results:
        print(
            f"  {r.get('feature', '?')}: {r['status']} "
            f"({r.get('rows_inserted', 0)} inserted, "
            f"{r.get('rows_skipped', 0)} skipped)"
        )
