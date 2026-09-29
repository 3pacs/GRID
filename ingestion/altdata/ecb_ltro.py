"""Eurosystem longer-term refinancing operations outstanding (ECB Data Portal).

Why this module exists
----------------------
``ingestion/altdata/ecb_tltro.py`` (source ``ecb_tltro``) has run every
Monday at 09:00 UTC in grid-intelligence and has never written a row:

* The scheduler calls it without a FRED key, so its FRED path is skipped
  (and its FRED candidates were wrong anyway: ``ECBASSETSW`` is *total*
  Eurosystem assets, ``ECBLTROL``/``ECBTLTRO3`` are placeholders).
* Its ECB fallback key ``ILM/M.U2.C.LT3.U2.EUR`` does not exist -- the ECB
  Data Portal answers HTTP 404 "No Series was returned" (checked
  2026-09-29).
* Even the correct ILM series is weekly (periods like ``2026-W38``), which
  its period parser could not read.

TLTRO-III itself fully matured in December 2024, so a TLTRO-only series
would now be permanently zero. The live, official ECB series that carries
the same balance-sheet lever is the weekly Eurosystem **"Longer-term
refinancing operations"** asset line (TLTRO-III was the bulk of it while it
existed; today it is the regular 3-month LTROs):

    https://data-api.ecb.europa.eu/service/data/ILM/W.U2.C.A050200.U2.EUR

This module writes it under its own ``source_catalog`` identity
(``ecb_ilm_ltro``) and series ``ecb_ilm:ltro_outstanding_eur_bn`` so the
``ecb_tltro`` name is never re-used for a broader meaning.

Timestamps: ``obs_date`` is the ECB's own period end for the weekly
observation (``structure.dimensions.observation[].end``).
``raw_series.pull_timestamp`` keeps its column default (fetch time), so a
first-run history load never claims earlier availability.

Failure contract: HTTP error, unexpected payload, or an empty series ->
``status="FAILED"``, nothing written. Free, no API key.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

SOURCE_NAME: str = "ecb_ilm_ltro"
SERIES_LTRO_OUTSTANDING: str = "ecb_ilm:ltro_outstanding_eur_bn"

ECB_LTRO_URL: str = (
    "https://data-api.ecb.europa.eu/service/data/ILM/W.U2.C.A050200.U2.EUR"
)
USER_AGENT: str = "GRID/4.0 (research; stepdadfinance@gmail.com)"
REQUEST_TIMEOUT_S: int = 30

# First run: load from the start of TLTRO-III (Sept 2019) so the drain is
# visible. Later runs: only the recent weeks.
INITIAL_START_PERIOD: str = "2019-01-01"
INCREMENTAL_LAST_N: int = 8


def parse_ecb_sdmx_json(payload: dict[str, Any]) -> list[tuple[date, float, str]]:
    """Flatten an ECB SDMX-JSON single-series payload.

    Returns ``[(obs_date, value_in_eur_bn, period_id), ...]`` sorted by date.
    ``obs_date`` is the period's ``end`` date as published by the ECB; the
    value is scaled by the series ``UNIT_MULT`` attribute (6 = millions) to
    EUR billions. Raises ``ValueError`` on an unexpected shape or unit.
    """
    try:
        structure = payload["structure"]
        obs_values = structure["dimensions"]["observation"][0]["values"]
        series_map = payload["dataSets"][0]["series"]
        series_attrs = structure["attributes"]["series"]
    except (KeyError, IndexError, TypeError) as exc:
        raise ValueError(f"unexpected ECB payload shape: {exc}") from exc

    if len(series_map) != 1:
        raise ValueError(f"expected exactly one series, got {len(series_map)}")
    series_key, series_obj = next(iter(series_map.items()))

    # Resolve UNIT and UNIT_MULT from the series attribute index vector.
    attr_idx = series_obj.get("attributes") or []
    attrs: dict[str, Any] = {}
    for pos, meta in enumerate(series_attrs):
        if pos < len(attr_idx) and attr_idx[pos] is not None:
            try:
                attrs[meta["id"]] = meta["values"][attr_idx[pos]].get("id")
            except (IndexError, KeyError, AttributeError):
                continue
    if attrs.get("UNIT") not in (None, "EUR"):
        raise ValueError(f"unexpected unit {attrs.get('UNIT')!r}")
    unit_mult = int(attrs.get("UNIT_MULT") or 6)
    to_bn = 10 ** unit_mult / 1e9

    rows: list[tuple[date, float, str]] = []
    for idx_str, vec in (series_obj.get("observations") or {}).items():
        try:
            meta = obs_values[int(idx_str)]
            raw_value = vec[0]
        except (ValueError, IndexError, TypeError):
            continue
        if raw_value is None:
            continue
        end = meta.get("end") or ""
        try:
            obs_date = date.fromisoformat(str(end)[:10])
        except ValueError:
            continue
        rows.append((obs_date, float(raw_value) * to_bn, str(meta.get("id"))))
    rows.sort(key=lambda r: r[0])
    return rows


class ECBLtroPuller(BasePuller):
    """Weekly Eurosystem LTRO outstanding from the ECB Data Portal (ILM)."""

    SOURCE_NAME: str = SOURCE_NAME
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": ECB_LTRO_URL,
        "cost_tier": "FREE",
        "latency_class": "WEEKLY",
        "pit_available": True,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 25,
    }

    def __init__(self, db_engine: Engine, session: requests.Session | None = None) -> None:
        super().__init__(db_engine)
        self._session = session or requests.Session()
        self._session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})

    def fetch(self, incremental: bool) -> list[tuple[date, float, str]]:
        params: dict[str, Any] = {"format": "jsondata"}
        if incremental:
            params["lastNObservations"] = INCREMENTAL_LAST_N
        else:
            params["startPeriod"] = INITIAL_START_PERIOD
        resp = self._session.get(ECB_LTRO_URL, params=params, timeout=REQUEST_TIMEOUT_S)
        resp.raise_for_status()
        return parse_ecb_sdmx_json(resp.json())

    def pull(self) -> dict[str, Any]:
        """Fetch and store new weekly observations. Never raises."""
        try:
            latest = self._get_latest_date(SERIES_LTRO_OUTSTANDING)
        except Exception as exc:  # noqa: BLE001
            log.warning("ecb_ilm_ltro: latest-date lookup failed: {e}", e=str(exc))
            latest = None

        try:
            rows = self.fetch(incremental=latest is not None)
        except Exception as exc:  # noqa: BLE001
            log.warning("ecb_ilm_ltro: fetch failed: {e}", e=str(exc))
            return {"status": "FAILED", "rows_inserted": 0, "error": str(exc)[:300]}

        if not rows:
            return {"status": "FAILED", "rows_inserted": 0, "error": "ECB returned no observations"}

        fetched_at = datetime.now(timezone.utc).isoformat()
        inserted = 0
        try:
            with self.engine.begin() as conn:
                existing = self._get_existing_dates(
                    SERIES_LTRO_OUTSTANDING, conn,
                    start_date=rows[0][0] - timedelta(days=1),
                )
                for obs_date, value, period in rows:
                    if obs_date in existing:
                        continue
                    self._insert_raw(
                        conn,
                        SERIES_LTRO_OUTSTANDING,
                        obs_date,
                        value,
                        raw_payload={
                            "period": period,
                            "ecb_key": "ILM.W.U2.C.A050200.U2.EUR",
                            "title": "Longer-term refinancing operations - Eurosystem",
                            "unit": "EUR bn",
                            "fetched_at": fetched_at,
                        },
                    )
                    existing.add(obs_date)
                    inserted += 1
        except Exception as exc:  # noqa: BLE001
            log.warning("ecb_ilm_ltro: save failed: {e}", e=str(exc))
            return {"status": "FAILED", "rows_inserted": 0, "error": str(exc)[:300]}

        latest_date, latest_value, _ = rows[-1]
        log.info(
            "ecb_ilm_ltro: {n} obs fetched, {i} new, latest {d} = {v:.1f} EUR bn",
            n=len(rows), i=inserted, d=latest_date, v=latest_value,
        )
        return {
            "status": "SUCCESS",
            "rows_inserted": inserted,
            "fetched": len(rows),
            "latest_obs_date": latest_date.isoformat(),
            "ltro_outstanding_eur_bn": latest_value,
        }


def run_ecb_ltro_puller(engine: Engine) -> dict[str, Any]:
    """Scheduler entry point."""
    try:
        return ECBLtroPuller(engine).pull()
    except Exception as exc:  # noqa: BLE001
        log.error("ecb_ilm_ltro: puller crashed: {e}", e=str(exc))
        return {"status": "FAILED", "rows_inserted": 0, "error": str(exc)[:300]}


__all__ = [
    "SOURCE_NAME",
    "SERIES_LTRO_OUTSTANDING",
    "ECB_LTRO_URL",
    "parse_ecb_sdmx_json",
    "ECBLtroPuller",
    "run_ecb_ltro_puller",
]
