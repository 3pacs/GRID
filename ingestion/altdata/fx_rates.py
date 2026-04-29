"""
GRID FX rates ingestion module.

Pulls daily FX spot rates from Yahoo Finance via ``yfinance`` and stores
each currency as a canonical ``FX:{CCY}:close`` series where the value
represents 1 unit of CCY in USD.

This is the currency reference layer used by the FX normalization
utility (``utils/fx.py``) to convert local-currency financials (IFRS
XBRL filings from foreign issuers) into USD at point-in-time correct
rates.

Pairs pulled (15 majors + DXY):
    EUR, GBP, JPY, CHF, CAD, AUD, CNY, HKD,
    BRL, MXN, INR, KRW, TWD, SEK, NOK, DXY

Series IDs stored:
    FX:EUR:close, FX:GBP:close, ... FX:NOK:close, FX:DXY:close

For USD-base pairs (EURUSD=X, GBPUSD=X, AUDUSD=X) the value is stored
verbatim.  For quote-USD pairs (JPY=X etc. which yfinance quotes as
USD→CCY) the value is inverted to CCY→USD so every series has the
same interpretation.

Usage:
    from ingestion.altdata.fx_rates import FXRatesPuller
    puller = FXRatesPuller(db_engine=get_engine())
    puller.backfill_days(1825)   # 5 years
    puller.pull()                # daily incremental
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller


# ── Pair configuration ───────────────────────────────────────────────

# Each entry: ccy_code -> (yfinance_symbol, invert)
#   invert=False → yfinance returns CCY→USD directly (e.g. EURUSD=X)
#   invert=True  → yfinance returns USD→CCY, we flip to CCY→USD
#
# DXY is special: stored as a raw index level, not a conversion rate.
_FX_PAIRS: dict[str, tuple[str, bool]] = {
    # USD-base pairs — already CCY→USD
    "EUR": ("EURUSD=X", False),
    "GBP": ("GBPUSD=X", False),
    "AUD": ("AUDUSD=X", False),
    # Quote-USD pairs — yfinance returns USD→CCY, must invert
    "JPY": ("JPY=X", True),
    "CHF": ("CHF=X", True),
    "CAD": ("CAD=X", True),
    "CNY": ("CNY=X", True),
    "HKD": ("HKD=X", True),
    "BRL": ("BRL=X", True),
    "MXN": ("MXN=X", True),
    "INR": ("INR=X", True),
    "KRW": ("KRW=X", True),
    "TWD": ("TWD=X", True),
    "SEK": ("SEK=X", True),
    "NOK": ("NOK=X", True),
}

# DXY dollar index — stored verbatim as FX:DXY:close
_DXY_SYMBOL: str = "DX-Y.NYB"


class FXRatesPuller(BasePuller):
    """Pulls daily FX spot rates from yfinance into ``raw_series``.

    Stores each currency as ``FX:{CCY}:close`` with the canonical
    interpretation: ``value = 1 unit of CCY in USD``.

    Attributes:
        engine: SQLAlchemy engine for database writes.
        source_id: The ``source_catalog.id`` for the yfinance source.
    """

    SOURCE_NAME: str = "yfinance"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://query1.finance.yahoo.com",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 25,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the FX rates puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        log.info("FXRatesPuller initialised — source_id={sid}", sid=self.source_id)

    # ── Core fetch ────────────────────────────────────────────────────

    def _fetch_history(
        self,
        symbol: str,
        start: date,
        end: date,
    ) -> pd.DataFrame:
        """Download daily history for a single yfinance symbol.

        Returns an empty DataFrame on any failure rather than raising so
        that one bad pair does not block the whole backfill.
        """
        try:
            import yfinance as yf  # local import: yfinance is optional at import time
        except ImportError:
            log.error("yfinance not installed; cannot pull FX rates")
            return pd.DataFrame()

        try:
            df = yf.download(
                symbol,
                start=start.isoformat(),
                end=(end + timedelta(days=1)).isoformat(),
                interval="1d",
                progress=False,
                auto_adjust=False,
                threads=False,
            )
        except Exception as exc:
            log.warning("FX fetch {s} failed: {e}", s=symbol, e=str(exc))
            return pd.DataFrame()

        if df is None or df.empty:
            return pd.DataFrame()

        # Flatten yfinance MultiIndex columns ("Close", symbol) → "Close"
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        return df

    def _insert_series(
        self,
        series_id: str,
        df: pd.DataFrame,
        invert: bool,
    ) -> int:
        """Insert Close values from ``df`` as ``series_id`` rows.

        Skips (series_id, obs_date) pairs that already exist to avoid
        duplicate inserts during re-runs.
        """
        if df.empty or "Close" not in df.columns:
            return 0

        inserted = 0
        with self.engine.begin() as conn:
            existing = self._get_existing_dates(series_id, conn)

            for ts, row in df.iterrows():
                close = row.get("Close")
                if close is None or pd.isna(close):
                    continue
                try:
                    val = float(close)
                except (TypeError, ValueError):
                    continue
                if val == 0.0:
                    continue  # cannot invert zero; also clearly bad data

                if invert:
                    val = 1.0 / val

                obs_d = ts.date() if hasattr(ts, "date") else ts
                if obs_d in existing:
                    continue

                self._insert_raw(
                    conn,
                    series_id=series_id,
                    obs_date=obs_d,
                    value=val,
                    raw_payload={"source": "yfinance", "invert": invert},
                )
                inserted += 1

        return inserted

    # ── Public entrypoints ────────────────────────────────────────────

    def pull(self, days_back: int = 7) -> dict[str, Any]:
        """Daily incremental pull — fetches the last ``days_back`` days.

        Intended to be called by the scheduler nightly.
        """
        return self.backfill_days(days_back)

    def backfill_days(self, days: int = 1825) -> dict[str, Any]:
        """Backfill daily FX rates for ``days`` calendar days.

        Parameters:
            days: Number of calendar days to look back. Default 1825 = 5 years.

        Returns:
            Status dict with per-currency row counts and any missing pairs.
        """
        end = date.today()
        start = end - timedelta(days=max(days, 1))

        log.info(
            "FX backfill — {s} to {e} ({d} days, {n} currencies + DXY)",
            s=start, e=end, d=days, n=len(_FX_PAIRS),
        )

        total = 0
        per_ccy: dict[str, int] = {}
        missing: list[str] = []

        for ccy, (symbol, invert) in _FX_PAIRS.items():
            series_id = f"FX:{ccy}:close"
            df = self._fetch_history(symbol, start, end)
            if df.empty:
                missing.append(ccy)
                per_ccy[ccy] = 0
                log.warning("FX {c} ({s}) returned no data", c=ccy, s=symbol)
                continue
            n = self._insert_series(series_id, df, invert)
            per_ccy[ccy] = n
            total += n
            log.info("FX {c}: inserted {n} rows as {sid}", c=ccy, n=n, sid=series_id)

        # DXY dollar index — stored verbatim, never inverted
        dxy_df = self._fetch_history(_DXY_SYMBOL, start, end)
        if dxy_df.empty:
            missing.append("DXY")
            per_ccy["DXY"] = 0
        else:
            n = self._insert_series("FX:DXY:close", dxy_df, invert=False)
            per_ccy["DXY"] = n
            total += n
            log.info("FX DXY: inserted {n} rows", n=n)

        # USD is the base currency — insert a trivial identity series so
        # downstream code can query FX:USD:close uniformly.
        self._ensure_usd_identity(start, end)
        per_ccy["USD"] = 0  # identity; not counted

        status = {
            "status": "SUCCESS" if not missing else "PARTIAL",
            "rows_inserted": total,
            "per_currency": per_ccy,
            "missing_pairs": missing,
        }
        log.info("FX backfill done — {t} rows, missing={m}", t=total, m=missing)
        return status

    def _ensure_usd_identity(self, start: date, end: date) -> None:
        """Ensure ``FX:USD:close`` exists with value=1.0 for each business day.

        The utility code treats USD as a pass-through (rate=1.0) without
        hitting the DB, but storing an identity series keeps the matrix
        join-friendly for ``get_fx_matrix``.
        """
        try:
            with self.engine.begin() as conn:
                existing = self._get_existing_dates("FX:USD:close", conn)
                d = start
                while d <= end:
                    # weekdays only — FX market is closed weekends
                    if d.weekday() < 5 and d not in existing:
                        self._insert_raw(
                            conn,
                            series_id="FX:USD:close",
                            obs_date=d,
                            value=1.0,
                            raw_payload={"identity": True},
                        )
                    d += timedelta(days=1)
        except Exception as exc:
            log.debug("FX:USD identity seed skipped: {e}", e=str(exc))


if __name__ == "__main__":
    # Manual run: `python -m ingestion.altdata.fx_rates`
    from api.dependencies import get_db_engine

    puller = FXRatesPuller(db_engine=get_db_engine())
    result = puller.backfill_days(1825)
    print(result)
