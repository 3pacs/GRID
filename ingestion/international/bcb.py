"""
GRID Banco Central do Brasil (BCB) ingestion module.

Pulls Brazilian monetary and economic data from the BCB SGS (Time Series
Management System) API. Covers SELIC rate, IPCA inflation, credit, FX, and trade.
"""

from __future__ import annotations

import json
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from ingestion.base import BasePuller, log_pull_failure
from tenacity import retry, stop_after_attempt, wait_exponential

# BCB SGS series: series_code -> feature name
BCB_SERIES: dict[int, str] = {
    11: "brazil_selic_rate",
    13522: "brazil_ipca_yoy",
    20539: "brazil_credit_growth",
    1: "brazil_discount_rate",
    3545: "brazil_usd_brl",
    7454: "brazil_trade_balance",
    13761: "brazil_primary_balance_gdp",
}

_BCB_BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados"
_RATE_LIMIT_DELAY: float = 1.0


class BCBPuller(BasePuller):
    """Pulls Brazilian economic data from the BCB SGS API."""

    SOURCE_NAME = "BCB_BR"
    SOURCE_CONFIG = {"base_url": "https://api.bcb.gov.br", "cost_tier": "FREE", "latency_class": "EOD", "pit_available": True, "revision_behavior": "RARE", "trust_score": "HIGH", "priority_rank": 24}

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("BCBPuller initialised — source_id={sid}", sid=self.source_id)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
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
            data = self._fetch_series_data(series_code, str(start_date), str(end_date) if end_date else None)

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

    def pull_all(self, start_date: str | date = "1994-01-01") -> dict[str, Any]:
        """Pull all BCB series."""
        log.info("Starting BCB bulk pull from {sd}", sd=start_date)
        results = [self.pull_series(code, start_date) for code in BCB_SERIES]

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
