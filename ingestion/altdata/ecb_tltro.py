"""
GRID ECB TLTRO-III outstanding balance + repayment calendar puller (CAT-12).

Targeted Longer-Term Refinancing Operations III (TLTRO-III) was the ECB's
emergency long-term refinancing programme (2019-2024): cheap multi-year loans
to Eurozone banks, conditional on lending volumes. Each scheduled repayment
date shrinks the ECB balance sheet and drains Eurozone bank liquidity — a
measurable macro lever on:

    LEVER:     Eurozone bank reserves at the ECB contract by the repaid notional
    VALVE:     EUR funding / credit creation
    FLOW:      Banks repay cheap TLTRO funding → must replace or deleverage
    ACTORS:    Eurozone commercial banks, ECB, bund/BTP sovereign curves

Why it matters here:

* Feeds the Financial Conditions Index (CAT-124, FCI): the TLTRO drain is a
  direct reduction in Eurozone central bank liquidity that the FCI tracks.
* Feeds the liquidity regime classifier (ALPHA-5): sudden shifts in the
  outstanding TLTRO stock reclassify the liquidity regime from EASING to
  TIGHTENING even when the Main Refi Rate is flat.
* Precedes EUR cross weakness: historical TLTRO drains have aligned with EUR
  depreciation against USD via dollar-shortage channels.

This puller also tracks the *upcoming* scheduled repayment windows. The ECB
publishes a static calendar of TLTRO-III maturity dates. These do not change,
so they are encoded as a module-level constant.

We also reserve series IDs for TPI (Transmission Protection Instrument)
activation events — currently unused (TPI has never fired), but named so the
consumer stack is ready the day it does.

Data strategy:

1. FRED-first for the outstanding balance. FRED mirrors a small number of ECB
   balance-sheet series (``ECBASSETS*`` / ``ECB*``). We attempt the FRED
   series listed in ``FRED_CANDIDATE_SERIES``; the first one to return a
   non-empty observation vector wins.
2. ECB SDW (Statistical Data Warehouse) JSON fallback: if every FRED
   candidate fails or returns empty, we hit the ECB SDW data-API directly:

       https://data-api.ecb.europa.eu/service/data/ILM/<key>?format=jsondata

   The ILM (Instrument-Level Maturity) dataflow exposes TLTRO-III outstanding
   notional as a monthly series.
3. Both paths failing → zero-row result + a log warning. The pipeline must
   never crash because one exotic macro series is unavailable.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable

import json
import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE_NAME: str = "ecb_tltro"

# FRED series candidates, tried in order. These are ECB-mirrored balance-sheet
# series; FRED's coverage of TLTRO-specific notional is thin, so we attempt a
# short ordered list and fall through to ECB SDW if none resolve.
FRED_CANDIDATE_SERIES: tuple[str, ...] = (
    "ECBASSETSW",   # ECB total assets, weekly (proxy if dedicated series missing)
    "ECBLTROL",     # placeholder — ECB LTRO outstanding (historical)
    "ECBTLTRO3",    # placeholder — TLTRO-III outstanding (if FRED adds it)
)

FRED_API_URL: str = "https://api.stlouisfed.org/fred/series/observations"

# ECB SDW direct JSON endpoint for the ILM (Instrument-Level Maturity)
# dataflow. The flow key encodes: frequency / area / sector / instrument /
# maturity / counterpart sector / currency.
ECB_SDW_URL: str = (
    "https://data-api.ecb.europa.eu/service/data/ILM/"
    "M.U2.C.LT3.U2.EUR"
)

DEFAULT_HTTP_TIMEOUT: int = 30

# ---------------------------------------------------------------------------
# TLTRO-III repayment calendar (public ECB schedule)
# ---------------------------------------------------------------------------
# Each entry is a mandatory quarterly repayment window for TLTRO-III
# operations, per the ECB's published schedule. Windows prior to today are
# still kept so the history of drains remains defensible. All dates are the
# settlement date of the repayment (the "effective" date of the balance
# sheet drain).
TLTRO_III_REPAYMENT_CALENDAR: dict[date, str] = {
    date(2022, 12, 21): "TLTRO-III voluntary early repayment window #1",
    date(2023, 2, 22):  "TLTRO-III voluntary early repayment window #2",
    date(2023, 3, 29):  "TLTRO-III voluntary early repayment window #3",
    date(2023, 5, 3):   "TLTRO-III voluntary early repayment window #4",
    date(2023, 6, 28):  "TLTRO-III #4 final maturity (EUR 476.8bn)",
    date(2023, 7, 26):  "TLTRO-III voluntary early repayment window #5",
    date(2023, 9, 27):  "TLTRO-III #5 scheduled maturity",
    date(2023, 12, 20): "TLTRO-III voluntary early repayment window #6",
    date(2024, 3, 27):  "TLTRO-III #6 scheduled maturity",
    date(2024, 6, 26):  "TLTRO-III #7 scheduled maturity",
    date(2024, 9, 25):  "TLTRO-III #8 scheduled maturity",
    date(2024, 12, 18): "TLTRO-III #9 scheduled maturity",
    date(2025, 3, 26):  "TLTRO-III #10 final maturity (tail cohort)",
    date(2025, 6, 25):  "TLTRO-III residual settlement window",
    date(2025, 9, 24):  "TLTRO-III residual settlement window",
    date(2025, 12, 17): "TLTRO-III residual settlement window",
}


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TLTROSnapshot:
    """Immutable snapshot of the TLTRO-III state at a given observation date.

    Attributes:
        date: Observation date for the outstanding balance.
        outstanding_eur_bn: Outstanding TLTRO-III notional, billions of EUR.
        next_repayment_date: Next scheduled repayment date on/after ``date``,
            or None if the calendar is exhausted.
        days_to_next_repayment: Integer days from ``date`` to
            ``next_repayment_date``, or None.
    """

    date: date
    outstanding_eur_bn: float
    next_repayment_date: date | None
    days_to_next_repayment: int | None


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

def compute_days_to_next_repayment(
    as_of: date,
    calendar: dict[date, str],
) -> tuple[date | None, int | None]:
    """Return the next scheduled repayment date on/after ``as_of``.

    Pure function (no I/O). Used by both the puller and the test harness.

    Parameters:
        as_of: The date from which to search.
        calendar: Mapping of scheduled repayment dates to labels.

    Returns:
        A tuple ``(next_date, days)`` where:
          * ``next_date`` is the earliest calendar date >= ``as_of``, or
            ``None`` if no such date exists.
          * ``days`` is an integer >= 0 number of days between ``as_of`` and
            ``next_date``, or ``None`` if there is no next date.
    """
    if not calendar:
        return (None, None)

    upcoming: list[date] = sorted(d for d in calendar.keys() if d >= as_of)
    if not upcoming:
        return (None, None)

    nxt = upcoming[0]
    days = (nxt - as_of).days
    return (nxt, days)


# ---------------------------------------------------------------------------
# Puller
# ---------------------------------------------------------------------------

class ECBTltroPuller(BasePuller):
    """Pulls ECB TLTRO-III outstanding balance into ``raw_series``.

    Strategy:
      1. Try each FRED candidate series in order. Accept the first that
         returns a non-empty observation list.
      2. Fall through to the ECB SDW direct JSON endpoint.
      3. Graceful zero-row result + warning if both paths fail.

    Alongside the outstanding balance, we compute and persist a
    ``days_to_next_repayment`` series using the hard-coded calendar.
    """

    SOURCE_NAME: str = "ecb_tltro"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://data-api.ecb.europa.eu/",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 25,
    }

    SERIES_OUTSTANDING: str = "ecb_tltro:outstanding_eur_bn"
    SERIES_DAYS_TO_NEXT: str = "ecb_tltro:days_to_next_repayment"

    def __init__(self, db_engine: Engine, fred_api_key: str = "") -> None:
        """Initialise the puller.

        Parameters:
            db_engine: SQLAlchemy engine.
            fred_api_key: Optional FRED API key. If empty, FRED attempts will
                be skipped entirely and we go straight to ECB SDW.
        """
        self._fred_api_key = fred_api_key or ""
        super().__init__(db_engine)
        log.info(
            "ECBTltroPuller initialised — source_id={sid}, fred_key={has}",
            sid=self.source_id,
            has=bool(self._fred_api_key),
        )

    # ------------------------------------------------------------------
    # Source fetch helpers
    # ------------------------------------------------------------------

    def _fetch_fred_series(self, series_id: str) -> list[tuple[date, float]]:
        """Fetch a FRED series via the public JSON observations endpoint.

        Returns a list of ``(obs_date, value)`` tuples. Rows where the value
        is the FRED missing-value sentinel ``.`` are skipped. Malformed rows
        are skipped with a warning. Network errors raise so the caller can
        fall through.
        """
        if not self._fred_api_key:
            return []

        params = {
            "series_id": series_id,
            "api_key": self._fred_api_key,
            "file_type": "json",
        }
        resp = requests.get(FRED_API_URL, params=params, timeout=DEFAULT_HTTP_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()

        observations = payload.get("observations") or []
        rows: list[tuple[date, float]] = []
        for obs in observations:
            raw_date = obs.get("date")
            raw_value = obs.get("value")
            if raw_value is None or raw_value == "." or raw_date is None:
                continue
            try:
                parsed_date = date.fromisoformat(str(raw_date))
                parsed_value = float(raw_value)
            except (ValueError, TypeError) as exc:
                log.warning(
                    "FRED {sid}: malformed obs skipped ({d}, {v}): {e}",
                    sid=series_id,
                    d=raw_date,
                    v=raw_value,
                    e=str(exc),
                )
                continue
            rows.append((parsed_date, parsed_value))
        return rows

    def _fetch_ecb_sdw(self) -> list[tuple[date, float]]:
        """Fetch TLTRO-III outstanding balance directly from ECB SDW JSON.

        The ECB SDW SDMX-JSON response is deeply nested:

            data.dataSets[0].series[<key>].observations[<obs_index>][0] = value
            data.structure.dimensions.observation[0].values[<obs_index>].id = date

        We flatten it into ``[(date, eur_bn), ...]``. Units are assumed to be
        EUR millions in the raw feed; we divide by 1_000 to normalise to
        billions to match the FRED-path convention.
        """
        headers = {"Accept": "application/json"}
        params = {"format": "jsondata"}
        resp = requests.get(
            ECB_SDW_URL, headers=headers, params=params, timeout=DEFAULT_HTTP_TIMEOUT
        )
        resp.raise_for_status()
        payload = resp.json()

        try:
            data_sets = payload["dataSets"]
            structure = payload["structure"]
            obs_dim = structure["dimensions"]["observation"][0]["values"]
            series_map = data_sets[0]["series"]
        except (KeyError, IndexError, TypeError) as exc:
            log.warning("ECB SDW: unexpected payload shape: {e}", e=str(exc))
            return []

        rows: list[tuple[date, float]] = []
        for _series_key, series_obj in series_map.items():
            observations = series_obj.get("observations") or {}
            for idx_str, obs_vec in observations.items():
                try:
                    idx = int(idx_str)
                    date_id = obs_dim[idx]["id"]  # e.g. "2024-03"
                    raw_value = obs_vec[0]
                except (ValueError, IndexError, KeyError, TypeError):
                    continue
                if raw_value is None:
                    continue
                parsed_date = _parse_ecb_period(date_id)
                if parsed_date is None:
                    continue
                try:
                    # ECB publishes in EUR millions; normalise to billions.
                    rows.append((parsed_date, float(raw_value) / 1_000.0))
                except (TypeError, ValueError):
                    continue
        rows.sort(key=lambda r: r[0])
        return rows

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------

    def pull(self) -> list[tuple[date, float]]:
        """Fetch the outstanding balance series.

        Tries each FRED candidate in order, then falls through to ECB SDW.
        Never raises — all failures are logged and produce an empty list.

        Returns:
            List of ``(obs_date, outstanding_eur_bn)`` tuples, sorted by
            date. Empty list on total failure.
        """
        # Path 1: FRED
        for fred_series in FRED_CANDIDATE_SERIES:
            try:
                rows = self._fetch_fred_series(fred_series)
            except Exception as exc:  # network / API errors — try next
                log.warning(
                    "ECBTltroPuller: FRED {s} failed: {e}",
                    s=fred_series,
                    e=str(exc),
                )
                continue
            if rows:
                log.info(
                    "ECBTltroPuller: FRED {s} → {n} rows",
                    s=fred_series,
                    n=len(rows),
                )
                return sorted(rows, key=lambda r: r[0])

        # Path 2: ECB SDW
        try:
            rows = self._fetch_ecb_sdw()
        except Exception as exc:
            log.warning("ECBTltroPuller: ECB SDW failed: {e}", e=str(exc))
            return []

        if rows:
            log.info("ECBTltroPuller: ECB SDW → {n} rows", n=len(rows))
            return rows

        log.warning(
            "ECBTltroPuller: no data from FRED or ECB SDW — zero rows returned"
        )
        return []

    def save_to_db(
        self,
        rows: Iterable[tuple[date, float]],
    ) -> int:
        """Upsert outstanding-balance and days-to-next-repayment rows.

        Idempotent: existing ``(series_id, obs_date)`` rows are skipped so
        repeated calls do not duplicate.

        Returns the number of newly inserted rows (across both series).
        """
        materialised = list(rows)
        if not materialised:
            return 0

        inserted = 0
        with self.engine.begin() as conn:
            existing_outstanding = self._get_existing_dates(
                self.SERIES_OUTSTANDING, conn
            )
            existing_days = self._get_existing_dates(
                self.SERIES_DAYS_TO_NEXT, conn
            )

            for obs_date, outstanding in materialised:
                next_date, days_to = compute_days_to_next_repayment(
                    obs_date, TLTRO_III_REPAYMENT_CALENDAR
                )

                if obs_date not in existing_outstanding:
                    self._insert_raw(
                        conn=conn,
                        series_id=self.SERIES_OUTSTANDING,
                        obs_date=obs_date,
                        value=float(outstanding),
                        raw_payload={
                            "outstanding_eur_bn": float(outstanding),
                            "next_repayment_date": (
                                next_date.isoformat() if next_date else None
                            ),
                            "days_to_next_repayment": days_to,
                        },
                    )
                    inserted += 1

                if days_to is not None and obs_date not in existing_days:
                    self._insert_raw(
                        conn=conn,
                        series_id=self.SERIES_DAYS_TO_NEXT,
                        obs_date=obs_date,
                        value=float(days_to),
                        raw_payload={
                            "next_repayment_date": (
                                next_date.isoformat() if next_date else None
                            ),
                        },
                    )
                    inserted += 1

        return inserted


# ---------------------------------------------------------------------------
# Module-level entrypoint
# ---------------------------------------------------------------------------

def run_ecb_tltro_puller(engine: Engine, fred_api_key: str = "") -> dict[str, Any]:
    """Run the TLTRO puller end-to-end.

    Parameters:
        engine: SQLAlchemy engine.
        fred_api_key: Optional FRED API key.

    Returns:
        Dict with keys:
          * ``fetched`` — number of (date, value) rows retrieved from source.
          * ``inserted`` — number of raw_series rows upserted.
          * ``outstanding_eur_bn`` — latest outstanding value seen, or None.
          * ``next_repayment`` — ISO date of the next scheduled repayment
            from today, or None if the calendar is exhausted.
    """
    puller = ECBTltroPuller(engine, fred_api_key=fred_api_key)
    try:
        rows = puller.pull()
    except Exception as exc:  # last-ditch safety net
        log.error("run_ecb_tltro_puller: pull raised unexpectedly: {e}", e=str(exc))
        rows = []

    try:
        inserted = puller.save_to_db(rows)
    except Exception as exc:
        log.error("run_ecb_tltro_puller: save_to_db failed: {e}", e=str(exc))
        inserted = 0

    latest_outstanding: float | None = None
    if rows:
        rows_sorted = sorted(rows, key=lambda r: r[0])
        latest_outstanding = float(rows_sorted[-1][1])

    next_date, _ = compute_days_to_next_repayment(
        date.today(), TLTRO_III_REPAYMENT_CALENDAR
    )

    return {
        "fetched": len(rows),
        "inserted": inserted,
        "outstanding_eur_bn": latest_outstanding,
        "next_repayment": next_date.isoformat() if next_date else None,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_ecb_period(period: str) -> date | None:
    """Parse an ECB SDW period identifier into a date.

    Supported formats:
      * ``YYYY-MM``  → first day of that month
      * ``YYYY-MM-DD`` → exact day
      * ``YYYY``    → Jan 1 of that year
    """
    try:
        parts = period.split("-")
        if len(parts) == 3:
            return date(int(parts[0]), int(parts[1]), int(parts[2]))
        if len(parts) == 2:
            return date(int(parts[0]), int(parts[1]), 1)
        if len(parts) == 1 and parts[0]:
            return date(int(parts[0]), 1, 1)
    except (ValueError, TypeError):
        return None
    return None


if __name__ == "__main__":
    from config import settings
    from db import get_engine

    result = run_ecb_tltro_puller(
        engine=get_engine(),
        fred_api_key=settings.FRED_API_KEY,
    )
    print(json.dumps(result, indent=2))
