"""
GRID Banco Central do Brasil (BCB) ingestion module.

Pulls Brazilian monetary and economic data from the BCB SGS (Time Series
Management System) API. Covers SELIC rate, IPCA inflation, credit, FX, and trade.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from ingestion.base import BasePuller, log_pull_failure
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

# BCB SGS series: series_code -> feature name
BCB_SERIES: dict[int, str] = {
    11: "brazil_selic_rate",
    13522: "brazil_ipca_yoy",
    20539: "brazil_credit_growth",
    # Owner decision 2026-11-04: written under new raw ids so the old,
    # mislabeled rows (code 1 stored as "brazil_discount_rate", code 3545 as
    # "brazil_usd_brl") never mix with correctly labeled data. Neither old
    # name was ever registered, so no resolved history is affected.
    1: "brazil_usd_brl_fx",      # USD/BRL commercial rate (sell), daily
    3545: "brazil_fx_reserves",  # international reserves, annual
    7454: "brazil_trade_balance",
    13761: "brazil_primary_balance_gdp",
}

_BCB_BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados"
_RATE_LIMIT_DELAY: float = 1.0

# SGS caps every query on a DAILY series at a 10-year window: a request from
# 1994 answers HTTP 406 ("... período de consulta ... até 10 anos ..."),
# which is why codes 1 and 11 failed on every bulk pull (verified from
# grid-svr 2026-11-04). Daily series are fetched in <=10-year windows.
_DAILY_SERIES: frozenset[int] = frozenset({1, 11})
_REVISION_LOOKBACK_DAYS: int = 400
_MAX_DAILY_WINDOW_YEARS: int = 10


def _is_retryable(exc: BaseException) -> bool:
    """Retry transport failures and 5xx/429 only; a 4xx query error is final."""
    if isinstance(exc, requests.HTTPError):
        status = getattr(exc.response, "status_code", None)
        return status is None or status >= 500 or status == 429
    return isinstance(exc, (requests.ConnectionError, requests.Timeout))


def _date_windows(start: date, end: date, years: int = _MAX_DAILY_WINDOW_YEARS) -> list[tuple[date, date]]:
    """Split [start, end] into consecutive windows spanning < ``years`` years."""
    windows: list[tuple[date, date]] = []
    cur = start
    while cur <= end:
        try:
            nxt = cur.replace(year=cur.year + years)
        except ValueError:  # Feb 29
            nxt = cur.replace(year=cur.year + years, day=28)
        win_end = min(end, nxt - timedelta(days=1))
        windows.append((cur, win_end))
        cur = win_end + timedelta(days=1)
    return windows


class BCBPuller(BasePuller):
    """Pulls Brazilian economic data from the BCB SGS API."""

    SOURCE_NAME = "BCB_BR"
    SOURCE_CONFIG = {"base_url": "https://api.bcb.gov.br", "cost_tier": "FREE", "latency_class": "EOD", "pit_available": True, "revision_behavior": "RARE", "trust_score": "HIGH", "priority_rank": 24}

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("BCBPuller initialised — source_id={sid}", sid=self.source_id)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=10),
        retry=retry_if_exception(_is_retryable),
        reraise=True,
    )
    def _fetch_series_data(self, series_code: int, start_date: str, end_date: str | None) -> list[dict]:
        """Fetch JSON array from BCB SGS API."""
        url = _BCB_BASE_URL.format(code=series_code)
        params: dict[str, str] = {
            "formato": "json",
            "dataInicial": self._format_date_br(start_date),
        }
        if end_date:
            params["dataFinal"] = self._format_date_br(end_date)
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _fetch_windowed(
        self,
        series_code: int,
        start_date: str | date,
        end_date: str | date | None,
    ) -> list[dict] | Any:
        """Fetch a series, splitting daily series into <=10-year windows."""
        if series_code not in _DAILY_SERIES:
            return self._fetch_series_data(series_code, str(start_date), str(end_date) if end_date else None)
        start = date.fromisoformat(str(start_date)[:10])
        end = date.fromisoformat(str(end_date)[:10]) if end_date else date.today()
        records: list[dict] = []
        for win_start, win_end in _date_windows(start, end):
            chunk = self._fetch_series_data(series_code, win_start.isoformat(), win_end.isoformat())
            if not isinstance(chunk, list):
                return chunk  # error envelope: let the caller report it
            records.extend(chunk)
            time.sleep(_RATE_LIMIT_DELAY)
        return records

    @staticmethod
    def _format_date_br(date_str: str) -> str:
        """Convert YYYY-MM-DD to DD/MM/YYYY format used by BCB API."""
        parts = str(date_str).split("-")
        if len(parts) == 3:
            return f"{parts[2]}/{parts[1]}/{parts[0]}"
        return date_str

    def pull_series(
        self,
        series_code: int,
        start_date: str | date = "1994-01-01",
        end_date: str | date | None = None,
    ) -> dict[str, Any]:
        """Fetch a single BCB series and insert into raw_series."""
        feature_name = BCB_SERIES.get(series_code, f"bcb_{series_code}")
        log.info("Pulling BCB series {code} ({fn})", code=series_code, fn=feature_name)

        result: dict[str, Any] = {
            "series_id": feature_name,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            data = self._fetch_windowed(series_code, start_date, end_date)

            if not data:
                result["status"] = "PARTIAL"
                result["errors"].append("No data returned")
                return result

            # BCB occasionally returns an error envelope (string or dict)
            # rather than the documented list-of-dict response. Iterating
            # over a string yielded single chars whose ``.get`` raised
            # AttributeError and produced 60+ ERROR rows in errors.jsonl.
            if not isinstance(data, list):
                log.warning(
                    "BCB code {code}: unexpected payload type {t} — skipping",
                    code=series_code, t=type(data).__name__,
                )
                result["status"] = "SKIPPED"
                result["errors"].append(
                    f"Non-list payload: {type(data).__name__}"
                )
                return result

            inserted = 0
            with self.engine.begin() as conn:
                for record in data:
                    if not isinstance(record, dict):
                        # Defensive: skip stray non-dict entries (error
                        # strings, nested arrays). Don't kill the whole pull.
                        continue
                    try:
                        # BCB returns {"data": "DD/MM/YYYY", "valor": "1.23"}
                        date_str = record.get("data", "")
                        valor = record.get("valor", "")

                        if not date_str or not valor:
                            continue

                        # Parse DD/MM/YYYY
                        obs_dt = datetime.strptime(date_str, "%d/%m/%Y").date()
                        value = float(valor.replace(",", "."))

                        if self._row_exists(feature_name, obs_dt, conn):
                            continue

                        conn.execute(
                            text(
                                "INSERT INTO raw_series "
                                "(series_id, source_id, obs_date, value, pull_status) "
                                "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                            ),
                            {"sid": feature_name, "src": self.source_id, "od": obs_dt, "val": value},
                        )
                        inserted += 1
                    except (ValueError, TypeError) as row_exc:
                        log.debug("Skipping BCB row: {err}", err=str(row_exc))
                        continue

            result["rows_inserted"] = inserted
            log.info("BCB {fn}: inserted {n} rows", fn=feature_name, n=inserted)

        except Exception as exc:
            log_pull_failure("BCB", f"code {series_code}", exc)
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        time.sleep(_RATE_LIMIT_DELAY)
        return result

    def _incremental_start(self, series_code: int, start_date: str | date) -> str:
        """Start from the latest stored obs minus a revision lookback.

        Re-pulling every series from 1994 on each 48h run re-inserted the
        full history each time (raw_series dedup only spans one hour).
        """
        requested = date.fromisoformat(str(start_date)[:10])
        try:
            latest = self._get_latest_date(BCB_SERIES.get(series_code, f"bcb_{series_code}"))
        except Exception as exc:  # noqa: BLE001 — a lookup failure falls back to the full pull
            log.debug("BCB latest-date lookup failed for {c}: {e}", c=series_code, e=exc)
            latest = None
        if latest is None:
            return requested.isoformat()
        return max(requested, latest - timedelta(days=_REVISION_LOOKBACK_DAYS)).isoformat()

    def pull_all(self, start_date: str | date = "1994-01-01") -> dict[str, Any]:
        """Pull all BCB series."""
        log.info("Starting BCB bulk pull from {sd}", sd=start_date)
        results = [
            self.pull_series(code, self._incremental_start(code, start_date))
            for code in BCB_SERIES
        ]

        total_rows = sum(r["rows_inserted"] for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "BCB bulk pull complete — {ok}/{total} succeeded, {rows} rows",
            ok=succeeded, total=len(results), rows=total_rows,
        )
        return {
            "source": "BCB_BR",
            "total_rows": total_rows,
            "succeeded": succeeded,
            "total": len(results),
        }
