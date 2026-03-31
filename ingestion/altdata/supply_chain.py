"""
GRID Supply Chain Leading Indicators ingestion module.

Pulls shipping and freight rate data that leads economic turns
by 3-6 months:

1. Freightos Baltic Index (FBX) — global container shipping rates
   via the Freightos Terminal public API
2. Drewry World Container Index (WCI) — composite container freight
   benchmark, scraped from the public data page
3. ISM Manufacturing PMI supplier deliveries — from FRED (MANEMP
   already ingested, we add delivery-related series)

Series stored:
- shipping_rate_global (FBX composite)
- container_index (Drewry WCI)
- freight_baltic_dry (already in baltic_dry.py — we add complementary)
- supply_chain.fbx.{route} — per-route FBX indices
- supply_chain.ism_deliveries — ISM supplier delivery times
- supply_chain.ism_backlog — ISM backlog of orders

Source: Freightos, Drewry (public), FRED
Schedule: Weekly (shipping data updates weekly)
"""

from __future__ import annotations

import json
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ── Configuration ────────────────────────────────────────────────────

_REQUEST_TIMEOUT: int = 30
_RATE_LIMIT_DELAY: float = 1.0

# Freightos Baltic Index (FBX) — public API endpoint
_FREIGHTOS_URL: str = "https://terminal.freightos.com/api/v1/indexes"
_FREIGHTOS_HISTORICAL_URL: str = (
    "https://terminal.freightos.com/api/v1/indexes/history"
)

# Drewry World Container Index — public page
_DREWRY_URL: str = "https://www.drewry.co.uk/supply-chain-advisors/supply-chain-expertise/world-container-index-assessed-by-drewry"

# FRED series for ISM manufacturing supply chain indicators
ISM_FRED_SERIES: dict[str, dict[str, str]] = {
    "supply_chain.ism_deliveries": {
        "fred_id": "NAPMSDI",
        "description": "ISM Manufacturing: Supplier Deliveries Index",
    },
    "supply_chain.ism_backlog": {
        "fred_id": "NAPMNO",
        "description": "ISM Manufacturing: New Orders Index",
    },
    "supply_chain.ism_inventories": {
        "fred_id": "NAPMII",
        "description": "ISM Manufacturing: Inventories Index",
    },
    "supply_chain.ism_prices": {
        "fred_id": "NAPMPRI",
        "description": "ISM Manufacturing: Prices Index (input cost pressure)",
    },
}

# FBX route codes and descriptions
FBX_ROUTES: dict[str, str] = {
    "FBX": "Freightos Baltic Index — Global Composite",
    "FBX01": "China/East Asia to North America West Coast",
    "FBX02": "China/East Asia to North Europe",
    "FBX03": "China/East Asia to Mediterranean",
    "FBX11": "North America West Coast to China/East Asia",
    "FBX12": "North Europe to China/East Asia",
}


class SupplyChainPuller(BasePuller):
    """Pulls supply chain leading indicators into raw_series.

    Combines multiple data sources that collectively lead
    economic turns by 3-6 months:
    - Freightos Baltic Index (container shipping rates)
    - Drewry World Container Index
    - ISM Manufacturing delivery/backlog indicators from FRED

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for Supply_Chain.
        fred_api_key: FRED API key for ISM series.
    """

    SOURCE_NAME: str = "Supply_Chain"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://terminal.freightos.com/",
        "cost_tier": "FREE",
        "latency_class": "WEEKLY",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 28,
    }

    def __init__(
        self,
        db_engine: Engine,
        fred_api_key: str = "",
    ) -> None:
        """Initialise the supply chain puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
            fred_api_key: FRED API key for ISM series (optional —
                ISM pull is skipped if empty).
        """
        self.fred_api_key = fred_api_key
        super().__init__(db_engine)
        fred_status = "set" if fred_api_key else "missing"
        log.info(
            "SupplyChainPuller initialised — source_id={sid}, fred_key={fk}",
            sid=self.source_id,
            fk=fred_status,
        )

    # ------------------------------------------------------------------ #
    # Freightos Baltic Index (FBX)
    # ------------------------------------------------------------------ #

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(
            ConnectionError,
            TimeoutError,
            OSError,
            requests.RequestException,
        ),
    )
    def _fetch_freightos_current(self) -> list[dict[str, Any]]:
        """Fetch current FBX index values from Freightos Terminal.

        Returns:
            List of index data dicts with route, value, date.
        """
        headers = {
            "User-Agent": "GRID-DataPuller/1.0",
            "Accept": "application/json",
        }

        try:
            resp = requests.get(
                _FREIGHTOS_URL,
                headers=headers,
                timeout=_REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()

            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return data.get("indexes", data.get("data", []))
            return []

        except requests.RequestException as exc:
            log.warning("Freightos API request failed: {e}", e=str(exc))
            raise
        except (json.JSONDecodeError, ValueError) as exc:
            log.warning("Freightos response parse failed: {e}", e=str(exc))
            return []

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(
            ConnectionError,
            TimeoutError,
            OSError,
            requests.RequestException,
        ),
    )
    def _fetch_freightos_historical(
        self,
        route: str = "FBX",
        start_date: str = "2020-01-01",
    ) -> list[dict[str, Any]]:
        """Fetch historical FBX data for a route.

        Parameters:
            route: FBX route code (e.g. 'FBX', 'FBX01').
            start_date: Start date for historical data.

        Returns:
            List of historical data points.
        """
        headers = {
            "User-Agent": "GRID-DataPuller/1.0",
            "Accept": "application/json",
        }

        try:
            resp = requests.get(
                _FREIGHTOS_HISTORICAL_URL,
                headers=headers,
                params={
                    "index": route,
                    "from": start_date,
                },
                timeout=_REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()

            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return data.get("history", data.get("data", []))
            return []

        except requests.RequestException as exc:
            log.warning(
                "Freightos historical fetch failed for {r}: {e}",
                r=route,
                e=str(exc),
            )
            raise
        except (json.JSONDecodeError, ValueError) as exc:
            log.warning(
                "Freightos historical parse failed for {r}: {e}",
                r=route,
                e=str(exc),
            )
            return []

    def _pull_freightos(
        self,
        start_date: date,
    ) -> dict[str, Any]:
        """Pull Freightos Baltic Index data.

        Attempts historical data first, falls back to current snapshot.

        Parameters:
            start_date: Earliest observation date.

        Returns:
            Dict with status and rows_inserted.
        """
        total_inserted = 0
        errors: list[str] = []

        # Try each route
        for route_code, route_desc in FBX_ROUTES.items():
            series_id = (
                "shipping_rate_global"
                if route_code == "FBX"
                else f"supply_chain.fbx.{route_code.lower()}"
            )

            try:
                # Try historical first
                historical = self._fetch_freightos_historical(
                    route=route_code,
                    start_date=start_date.isoformat(),
                )

                if historical:
                    inserted = self._store_freightos_data(
                        series_id, historical, start_date, route_code, route_desc,
                    )
                    total_inserted += inserted
                    log.info(
                        "Freightos {r}: {n} rows inserted",
                        r=route_code,
                        n=inserted,
                    )
                else:
                    log.debug(
                        "Freightos: no historical data for {r}",
                        r=route_code,
                    )

            except Exception as exc:
                err_msg = f"Freightos {route_code}: {str(exc)}"
                log.warning(err_msg)
                errors.append(err_msg)

            time.sleep(_RATE_LIMIT_DELAY)

        # Fallback: try current snapshot for the global composite
        if total_inserted == 0:
            try:
                current = self._fetch_freightos_current()
                if current:
                    inserted = self._store_freightos_snapshot(current, start_date)
                    total_inserted += inserted
            except Exception as exc:
                errors.append(f"Freightos current: {str(exc)}")

        return {
            "source": "freightos",
            "status": "SUCCESS" if total_inserted > 0 else "PARTIAL",
            "rows_inserted": total_inserted,
            "errors": errors,
        }

    def _store_freightos_data(
        self,
        series_id: str,
        data: list[dict[str, Any]],
        start_date: date,
        route_code: str,
        route_desc: str,
    ) -> int:
        """Store Freightos historical data points.

        Parameters:
            series_id: Target series ID.
            data: List of historical data points.
            start_date: Earliest date to store.
            route_code: FBX route code.
            route_desc: Route description.

        Returns:
            Number of rows inserted.
        """
        inserted = 0

        with self.engine.begin() as conn:
            existing = self._get_existing_dates(series_id, conn)

            for point in data:
                # Parse date from various formats
                date_val = point.get("date") or point.get("timestamp") or point.get("t")
                value = point.get("value") or point.get("price") or point.get("v")

                if date_val is None or value is None:
                    continue

                try:
                    if isinstance(date_val, str):
                        obs_date = date.fromisoformat(date_val[:10])
                    elif isinstance(date_val, (int, float)):
                        obs_date = datetime.fromtimestamp(
                            date_val, tz=timezone.utc
                        ).date()
                    else:
                        continue
                except (ValueError, OSError):
                    continue

                if obs_date < start_date or obs_date in existing:
                    continue

                try:
                    val_float = float(value)
                except (ValueError, TypeError):
                    continue

                self._insert_raw(
                    conn=conn,
                    series_id=series_id,
                    obs_date=obs_date,
                    value=val_float,
                    raw_payload={
                        "route": route_code,
                        "description": route_desc,
                        "source": "Freightos_FBX",
                        "unit": "USD/FEU",
                    },
                )
                inserted += 1

        return inserted

    def _store_freightos_snapshot(
        self,
        current_data: list[dict[str, Any]],
        start_date: date,
    ) -> int:
        """Store a current FBX snapshot.

        Parameters:
            current_data: List of current index values.
            start_date: Earliest date to store.

        Returns:
            Number of rows inserted.
        """
        inserted = 0
        today = date.today()

        if today < start_date:
            return 0

        with self.engine.begin() as conn:
            for item in current_data:
                index_code = item.get("code") or item.get("index", "")
                value = item.get("value") or item.get("price")

                if not index_code or value is None:
                    continue

                series_id = (
                    "shipping_rate_global"
                    if index_code.upper() == "FBX"
                    else f"supply_chain.fbx.{index_code.lower()}"
                )

                if self._row_exists(series_id, today, conn):
                    continue

                try:
                    val_float = float(value)
                except (ValueError, TypeError):
                    continue

                self._insert_raw(
                    conn=conn,
                    series_id=series_id,
                    obs_date=today,
                    value=val_float,
                    raw_payload={
                        "route": index_code,
                        "source": "Freightos_FBX_snapshot",
                        "unit": "USD/FEU",
                    },
                )
                inserted += 1

        return inserted

    # ------------------------------------------------------------------ #
    # Drewry World Container Index
    # ------------------------------------------------------------------ #

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(
            ConnectionError,
            TimeoutError,
            OSError,
            requests.RequestException,
        ),
    )
    def _fetch_drewry_wci(self) -> list[dict[str, Any]]:
        """Fetch Drewry World Container Index from the public page.

        Scrapes the Drewry WCI page for the latest index values.
        Falls back gracefully if page structure changes.

        Returns:
            List of data points with date and value.
        """
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml",
        }

        resp = requests.get(
            _DREWRY_URL,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        html = resp.text

        data_points: list[dict[str, Any]] = []

        try:
            # Look for structured data in the page (JSON-LD or embedded data)
            # Drewry embeds chart data in script tags
            script_pattern = re.compile(
                r'(?:data|series|values)\s*[=:]\s*\[([^\]]+)\]',
                re.DOTALL,
            )
            matches = script_pattern.findall(html)

            for match in matches:
                # Try to parse as comma-separated values
                try:
                    values = [float(v.strip().strip('"\'')) for v in match.split(",") if v.strip()]
                    if len(values) > 5 and all(100 < v < 20000 for v in values[:5]):
                        # Likely container index values (typical range $500-$15000)
                        today = date.today()
                        for i, val in enumerate(values[-4:]):  # Last 4 weeks
                            obs_date = today - timedelta(weeks=len(values[-4:]) - 1 - i)
                            data_points.append({
                                "date": obs_date.isoformat(),
                                "value": val,
                            })
                        break
                except (ValueError, TypeError):
                    continue

            # Fallback: look for a prominent number in a known pattern
            if not data_points:
                wci_pattern = re.compile(
                    r'(?:WCI|World Container Index|composite)[^0-9]*'
                    r'\$?\s*([\d,]+(?:\.\d+)?)',
                    re.IGNORECASE,
                )
                wci_match = wci_pattern.search(html)
                if wci_match:
                    try:
                        val = float(wci_match.group(1).replace(",", ""))
                        data_points.append({
                            "date": date.today().isoformat(),
                            "value": val,
                        })
                    except (ValueError, TypeError):
                        pass

        except Exception as exc:
            log.warning(
                "Drewry WCI parse failed: {e}",
                e=str(exc),
            )

        log.info(
            "Drewry WCI: parsed {n} data points",
            n=len(data_points),
        )
        return data_points

    def _pull_drewry(
        self,
        start_date: date,
    ) -> dict[str, Any]:
        """Pull Drewry World Container Index data.

        Parameters:
            start_date: Earliest observation date.

        Returns:
            Dict with status and rows_inserted.
        """
        try:
            data = self._fetch_drewry_wci()
        except Exception as exc:
            log.warning("Drewry WCI fetch failed: {e}", e=str(exc))
            return {
                "source": "drewry",
                "status": "FAILED",
                "rows_inserted": 0,
                "error": str(exc),
            }

        if not data:
            return {
                "source": "drewry",
                "status": "PARTIAL",
                "rows_inserted": 0,
                "errors": ["No data parsed from Drewry page"],
            }

        inserted = 0
        series_id = "container_index"

        with self.engine.begin() as conn:
            existing = self._get_existing_dates(series_id, conn)

            for point in data:
                try:
                    obs_date = date.fromisoformat(point["date"][:10])
                except (ValueError, KeyError):
                    continue

                if obs_date < start_date or obs_date in existing:
                    continue

                try:
                    val = float(point["value"])
                except (ValueError, TypeError):
                    continue

                self._insert_raw(
                    conn=conn,
                    series_id=series_id,
                    obs_date=obs_date,
                    value=val,
                    raw_payload={
                        "source": "Drewry_WCI",
                        "description": "Drewry World Container Index",
                        "unit": "USD/FEU",
                    },
                )
                inserted += 1

        log.info("Drewry WCI: {n} rows inserted", n=inserted)
        return {
            "source": "drewry",
            "status": "SUCCESS",
            "rows_inserted": inserted,
        }

    # ------------------------------------------------------------------ #
    # ISM Manufacturing from FRED
    # ------------------------------------------------------------------ #

    def _pull_ism_fred(
        self,
        start_date: date,
    ) -> dict[str, Any]:
        """Pull ISM supply chain indicators from FRED.

        Requires fred_api_key to be set. Skips gracefully if missing.

        Parameters:
            start_date: Earliest observation date.

        Returns:
            Dict with status and rows_inserted.
        """
        if not self.fred_api_key:
            log.info(
                "SupplyChain: FRED API key not set — skipping ISM pull"
            )
            return {
                "source": "fred_ism",
                "status": "SKIPPED",
                "rows_inserted": 0,
                "errors": ["No FRED API key"],
            }

        try:
            from fedfred import FredAPI
        except ImportError:
            log.warning(
                "SupplyChain: fedfred not installed — skipping ISM pull"
            )
            return {
                "source": "fred_ism",
                "status": "SKIPPED",
                "rows_inserted": 0,
                "errors": ["fedfred not installed"],
            }

        fred = FredAPI(self.fred_api_key)
        total_inserted = 0
        errors: list[str] = []

        for series_id, meta in ISM_FRED_SERIES.items():
            fred_id = meta["fred_id"]

            try:
                data: pd.DataFrame = fred.get_series_observations(
                    fred_id,
                    observation_start=str(start_date),
                )

                if data is None or data.empty:
                    log.debug("FRED {fid}: no data returned", fid=fred_id)
                    continue

                # Normalise columns
                if "observation_date" in data.columns:
                    data = data.rename(columns={"observation_date": "date"})
                elif data.index.name == "date" or hasattr(data.index, "date"):
                    data = data.reset_index()

                data = data[data["value"].apply(
                    lambda v: v != "." and pd.notna(v)
                )].copy()
                data["value"] = pd.to_numeric(data["value"], errors="coerce")
                coerced_count = data["value"].isna().sum()
                if coerced_count > 0:
                    log.warning(
                        "Coerced {n} non-numeric values to NaN for {sid}",
                        n=int(coerced_count),
                        sid=series_id,
                    )
                data = data.dropna(subset=["value"])

                inserted = 0
                with self.engine.begin() as conn:
                    existing = self._get_existing_dates(series_id, conn)

                    for _, row in data.iterrows():
                        obs_date_val = (
                            row["date"].date()
                            if hasattr(row["date"], "date") and callable(row["date"].date)
                            else pd.Timestamp(row["date"]).date()
                        )
                        if obs_date_val < start_date or obs_date_val in existing:
                            continue

                        self._insert_raw(
                            conn=conn,
                            series_id=series_id,
                            obs_date=obs_date_val,
                            value=float(row["value"]),
                            raw_payload={
                                "fred_series": fred_id,
                                "description": meta["description"],
                                "source": "FRED_ISM",
                            },
                        )
                        inserted += 1

                total_inserted += inserted
                log.info(
                    "ISM {sid} ({fid}): {n} rows inserted",
                    sid=series_id,
                    fid=fred_id,
                    n=inserted,
                )

            except Exception as exc:
                err_msg = f"ISM {fred_id}: {str(exc)}"
                log.warning(err_msg)
                errors.append(err_msg)

            time.sleep(0.25)  # FRED rate limit

        return {
            "source": "fred_ism",
            "status": "SUCCESS" if total_inserted > 0 else "PARTIAL",
            "rows_inserted": total_inserted,
            "errors": errors,
        }

    # ------------------------------------------------------------------ #
    # Main pull methods
    # ------------------------------------------------------------------ #

    def pull_all(
        self,
        start_date: str | date = "2020-01-01",
    ) -> list[dict[str, Any]]:
        """Pull all supply chain leading indicators.

        Never stops on a single-source failure -- logs and continues.

        Parameters:
            start_date: Earliest observation date.

        Returns:
            List of per-source result dicts.
        """
        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)

        log.info(
            "Starting supply chain pull — Freightos + Drewry + ISM from {sd}",
            sd=start_date,
        )

        results: list[dict[str, Any]] = []

        # 1. Freightos Baltic Index
        try:
            fbx_result = self._pull_freightos(start_date)
            results.append(fbx_result)
        except Exception as exc:
            log.error("SupplyChain: Freightos pull failed: {e}", e=str(exc))
            results.append({
                "source": "freightos",
                "status": "FAILED",
                "error": str(exc),
            })

        # 2. Drewry World Container Index
        try:
            drewry_result = self._pull_drewry(start_date)
            results.append(drewry_result)
        except Exception as exc:
            log.error("SupplyChain: Drewry pull failed: {e}", e=str(exc))
            results.append({
                "source": "drewry",
                "status": "FAILED",
                "error": str(exc),
            })

        # 3. ISM Manufacturing from FRED
        try:
            ism_result = self._pull_ism_fred(start_date)
            results.append(ism_result)
        except Exception as exc:
            log.error("SupplyChain: ISM FRED pull failed: {e}", e=str(exc))
            results.append({
                "source": "fred_ism",
                "status": "FAILED",
                "error": str(exc),
            })

        total_inserted = sum(r.get("rows_inserted", 0) for r in results)
        succeeded = sum(1 for r in results if r.get("status") == "SUCCESS")
        log.info(
            "Supply chain pull complete — {ok}/{total} sources succeeded, "
            "{n} total rows inserted",
            ok=succeeded,
            total=len(results),
            n=total_inserted,
        )

        return results


if __name__ == "__main__":
    from config import settings
    from db import get_engine

    fred_key = getattr(settings, "FRED_API_KEY", "")
    puller = SupplyChainPuller(
        db_engine=get_engine(),
        fred_api_key=fred_key,
    )
    results = puller.pull_all(start_date="2020-01-01")
    for r in results:
        print(
            f"  {r.get('source', '?')}: {r.get('status')} — "
            f"{r.get('rows_inserted', 0)} rows"
        )
