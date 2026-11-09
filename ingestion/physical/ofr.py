"""
GRID OFR (Office of Financial Research) ingestion module.

Pulls the OFR Financial Stress Index (FSI) with its five category
sub-indices, and the overnight repo rate / volume from the OFR
Short-Term Funding Monitor (STFM) API.

2026-11-09 rebuild (probed from precision5520, fetch only):

* Financial Stability Monitor (FSM): retired upstream.
  ``www.financialresearch.gov/financial-stability-monitor/`` and every
  ``.../download/`` URL the puller tried answer HTTP 404, and the OFR data
  API lists no FSM dataset (``/v1/series/dataset?dataset=fsm`` -> 400
  "Available datasets are: ['all', 'nypd', 'mmf', 'repo', 'fnyr', 'tyld']").
  ``ofr_fsm_*`` had never stored a row. The FSI file already carries the
  OFR's own category decomposition (credit, equity valuation, safe assets,
  funding, volatility), so those replace the FSM category scores.
* STFM: the ``/short-term-funding-monitor/download/`` pages are 404 and
  ``data.financialresearch.gov/v1/short-term-funding-monitor/download/`` is
  403 "Missing Authentication Token" (not a route). The documented,
  keyless STFM API serves each series by mnemonic:
  ``/v1/series/timeseries?mnemonic=REPO-DVP_AR_OO-P`` -> ``[[date, value]]``.
  The preliminary (``-P``) DVP overnight series are current to the previous
  business day; the final (``-F``) ones lag by months (last 2026-06-30).
  ``ofr_repo_volume`` / ``ofr_repo_rate_1d`` had never stored a row.
* FSI: unchanged URL, but the 1-hour ``_row_exists`` window re-inserted the
  full 2000-to-date history on every daily run (1,766,133 ofr_fsi rows for
  6,795 dates on 2026-11-09). Rows are now written only when new or revised.
"""

from __future__ import annotations

import math
from datetime import date, timedelta
from io import StringIO
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# OFR datasets this puller covers -> primary feature
OFR_DATASETS: dict[str, str] = {
    "short-term-funding-monitor": "ofr_repo_rate_1d",
    "financial-stress-index": "ofr_fsi",
}

_OFR_BASE_URL = "https://www.financialresearch.gov"
_FSI_URL = f"{_OFR_BASE_URL}/financial-stress-index/data/fsi.csv"
_STFM_API_URL = "https://data.financialresearch.gov/v1/series/timeseries"
_REQUEST_TIMEOUT: int = 30

# FSI CSV column -> feature. The headline index keeps its historical id.
FSI_COLUMNS: dict[str, str] = {
    "OFR FSI": "ofr_fsi",
    "Credit": "ofr_fsi_credit",
    "Equity valuation": "ofr_fsi_equity_valuation",
    "Safe assets": "ofr_fsi_safe_assets",
    "Funding": "ofr_fsi_funding",
    "Volatility": "ofr_fsi_volatility",
}

# STFM API mnemonic -> feature (DVP service, overnight/open, preliminary).
STFM_SERIES: dict[str, str] = {
    "REPO-DVP_AR_OO-P": "ofr_repo_rate_1d",   # average rate, percent
    "REPO-DVP_TV_OO-P": "ofr_repo_volume",    # transaction volume, USD
}

# Incremental windows: once a feature has history, only observations this
# close to its latest stored date are compared, so a daily run does not
# re-read 26 years of FSI vintages. Late revisions inside the window are
# still written (new or revised values only).
_FSI_LOOKBACK_DAYS: int = 90
_STFM_LOOKBACK_DAYS: int = 30


def _finite(value: Any) -> float | None:
    """Return ``value`` as a finite float, or None."""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) or math.isinf(f) else f


class OFRPuller(BasePuller):
    """Pulls financial stability data from the OFR."""

    SOURCE_NAME = "OFR"
    SOURCE_CONFIG = {"base_url": "https://www.financialresearch.gov/financial-stress-index/data", "cost_tier": "FREE", "latency_class": "EOD", "pit_available": True, "revision_behavior": "RARE", "trust_score": "HIGH", "priority_rank": 15}

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("OFRPuller initialised — source_id={sid}", sid=self.source_id)

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    def _fetch_csv(self, url: str) -> pd.DataFrame:
        """Fetch a CSV; raises ``requests.RequestException`` on failure."""
        resp = requests.get(url, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return pd.read_csv(StringIO(resp.text))

    def _fetch_stfm(self, mnemonic: str, start: date | None) -> list[tuple[date, float]]:
        """Fetch one STFM series as sorted ``(date, value)`` pairs (nulls dropped)."""
        params: dict[str, str] = {"mnemonic": mnemonic}
        if start is not None:
            params["start_date"] = start.isoformat()
        resp = requests.get(_STFM_API_URL, params=params, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, list):
            raise TypeError(f"OFR STFM {mnemonic}: expected a list, got {type(payload).__name__}")
        out: list[tuple[date, float]] = []
        for item in payload:
            if not isinstance(item, (list, tuple)) or len(item) < 2:
                continue
            value = _finite(item[1])
            if value is None:
                continue
            try:
                obs_dt = date.fromisoformat(str(item[0])[:10])
            except ValueError:
                continue
            out.append((obs_dt, value))
        out.sort()
        return out

    # ------------------------------------------------------------------
    # Storage
    # ------------------------------------------------------------------

    def _store(self, conn: Any, feature: str, observations: list[tuple[date, float]]) -> int:
        """Insert new or revised observations of one feature; returns rows written."""
        if not observations:
            return 0
        stored = self._latest_stored_values(
            feature, conn, start_date=min(d for d, _ in observations),
        )
        inserted = 0
        for obs_dt, value in observations:
            if stored.get(obs_dt) == value:
                continue
            conn.execute(
                text(
                    "INSERT INTO raw_series "
                    "(series_id, source_id, obs_date, value, pull_status) "
                    "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                ),
                {"sid": feature, "src": self.source_id, "od": obs_dt, "val": value},
            )
            stored[obs_dt] = value
            inserted += 1
        return inserted

    # ------------------------------------------------------------------
    # Pulls
    # ------------------------------------------------------------------

    def pull_fsi(self) -> dict[str, Any]:
        """Pull the OFR Financial Stress Index and its category sub-indices."""
        log.info("Pulling OFR Financial Stress Index")
        result: dict[str, Any] = {
            "series_id": "ofr_fsi",
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }
        try:
            df = self._fetch_csv(_FSI_URL)
            if "Date" not in df.columns or "OFR FSI" not in df.columns:
                result["status"] = "FAILED"
                result["errors"].append(f"FSI CSV columns changed: {list(df.columns)[:12]}")
                log.warning("OFR FSI: unexpected columns {c}", c=list(df.columns)[:12])
                return result
            dates = pd.to_datetime(df["Date"], format="%Y-%m-%d", errors="coerce")
            if dates.isna().any():
                log.warning("OFR FSI: {n} unparseable Date cells skipped", n=int(dates.isna().sum()))
            missing = [c for c in FSI_COLUMNS if c not in df.columns]
            if missing:
                result["status"] = "PARTIAL"
                result["errors"].append(f"FSI CSV missing columns: {missing}")
            inserted = 0
            with self.engine.begin() as conn:
                for col, feature in FSI_COLUMNS.items():
                    if col not in df.columns:
                        continue
                    latest = self._get_latest_date(feature)
                    floor = latest - timedelta(days=_FSI_LOOKBACK_DAYS) if latest else date.min
                    # _finite per raw cell: Series.map(_finite) would turn the
                    # None for a blank cell back into NaN.
                    obs = [
                        (d.date(), v)
                        for d, v in ((d, _finite(raw)) for d, raw in zip(dates, df[col]))
                        if not pd.isna(d) and v is not None and d.date() >= floor
                    ]
                    inserted += self._store(conn, feature, obs)
            result["rows_inserted"] = inserted
            log.info("OFR FSI: inserted {n} rows", n=inserted)
        except (requests.RequestException, ValueError) as exc:
            log.warning("OFR FSI pull failed: {err}", err=str(exc)[:300])
            result["status"] = "FAILED"
            result["errors"].append(str(exc))
        return result

    def pull_stfm(self) -> dict[str, Any]:
        """Pull overnight DVP repo rate and volume from the OFR STFM API."""
        log.info("Pulling OFR Short-Term Funding Monitor")
        result: dict[str, Any] = {
            "series_id": "ofr_stfm",
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }
        inserted = 0
        failed: list[str] = []
        for mnemonic, feature in STFM_SERIES.items():
            try:
                latest = self._get_latest_date(feature)
                start = latest - timedelta(days=_STFM_LOOKBACK_DAYS) if latest else None
                obs = self._fetch_stfm(mnemonic, start)
                with self.engine.begin() as conn:
                    inserted += self._store(conn, feature, obs)
            except (requests.RequestException, ValueError, TypeError) as exc:
                log.warning("OFR STFM {m} failed: {err}", m=mnemonic, err=str(exc)[:300])
                failed.append(mnemonic)
                result["errors"].append(f"{mnemonic}: {exc}")
        result["rows_inserted"] = inserted
        if failed:
            result["status"] = "FAILED" if len(failed) == len(STFM_SERIES) else "PARTIAL"
        log.info("OFR STFM: inserted {n} rows", n=inserted)
        return result

    def pull_all(self) -> dict[str, Any]:
        """Pull all OFR data sources."""
        log.info("Starting OFR bulk pull")
        results = [self.pull_fsi(), self.pull_stfm()]

        total_rows = sum(r["rows_inserted"] for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "OFR bulk pull complete — {ok}/{total} succeeded, {rows} rows",
            ok=succeeded, total=len(results), rows=total_rows,
        )
        return {
            "source": "OFR",
            "total_rows": total_rows,
            "succeeded": succeeded,
            "total": len(results),
        }
