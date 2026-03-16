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


class BCBPuller:
    """Pulls Brazilian economic data from the BCB SGS API."""

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.source_id = self._resolve_source_id()
        log.info("BCBPuller initialised — source_id={sid}", sid=self.source_id)

    def _resolve_source_id(self) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": "BCB_BR"},
            ).fetchone()
        if row is None:
            with self.engine.begin() as conn:
                result = conn.execute(
                    text(
                        "INSERT INTO source_catalog "
                        "(name, base_url, license_type, update_frequency, "
                        "has_vintage_data, revision_policy, data_quality, priority, model_eligible) "
                        "VALUES (:name, :url, 'FREE', 'DAILY', FALSE, 'NEVER', 'HIGH', 19, TRUE) "
                        "RETURNING id"
                    ),
                    {"name": "BCB_BR", "url": "https://api.bcb.gov.br/dados/serie"},
                )
                return result.fetchone()[0]
        return row[0]

    def _row_exists(self, series_id: str, obs_date: date, conn: Any) -> bool:
        one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
        result = conn.execute(
            text(
                "SELECT 1 FROM raw_series "
                "WHERE series_id = :sid AND source_id = :src "
                "AND obs_date = :od AND pull_timestamp >= :ts LIMIT 1"
            ),
            {"sid": series_id, "src": self.source_id, "od": obs_date, "ts": one_hour_ago},
        ).fetchone()
        return result is not None

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

            inserted = 0
            with self.engine.begin() as conn:
                for record in data:
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
            log.error("BCB pull failed for code {code}: {err}", code=series_code, err=str(exc))
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
