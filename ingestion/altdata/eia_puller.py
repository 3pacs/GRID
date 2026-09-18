"""
GRID EIA energy data ingestion module.

Pulls Brent/WTI crude oil daily spot prices from the EIA API v2.

Data source: https://api.eia.gov/v2/petroleum/pri/spt/data/
API key required (free): set EIA_API_KEY env var.

Series stored:
- eia.brent_spot: Brent crude oil spot price (USD/bbl)
- eia.wti_spot: WTI crude oil spot price (USD/bbl)

Weekly Cushing crude stocks -- request contract only (W5c, 2026-09-18)
------------------------------------------------------------------------
``build_weekly_stocks_request`` / ``fetch_weekly_stocks`` /
``parse_weekly_stocks_response`` below add the v2 request *contract* for
the weekly petroleum stocks route, documented at
https://www.eia.gov/opendata/browser/petroleum/stoc/wstk (route + facet
shape) and https://www.eia.gov/opendata/documentation.php (general query
parameter shape: ``data[0]=value``, ``facets[series][]=...``,
``frequency``, ``api_key``, ``sort[...]``, ``length``). Per this
workstream's constraints, calling EIA needs a real key, so
``fetch_weekly_stocks`` is NOT called anywhere in this pass -- only the
request builder and the response parser are implemented and tested (with
a constructed response, not a live one). See
``docs/handoffs/2026-09-18/fable-w5b-source-contracts.md`` for the exact
bounded ``curl`` the operator can run once to confirm this contract
against the live API.

This does NOT duplicate the EIA hardening work tracked separately on
another branch (#553) -- ``pull()``/``_fetch_series()`` above (Brent/WTI
spot prices) are untouched; only the new weekly-stocks functions are
added.
"""

from __future__ import annotations

import os
import re
import time
from datetime import date, timedelta
from typing import Any
from urllib.parse import urlencode

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from config import settings
from ingestion.base import BasePuller, retry_on_failure

_EIA_BASE = "https://api.eia.gov/v2/petroleum/pri/spt/data/"
_REQUEST_TIMEOUT: int = 30
_SERIES_MAP: dict[str, str] = {"RBRTE": "brent_spot", "RWTC": "wti_spot"}
_SERIES_PREFIX = "eia"

# ---- Weekly Cushing, OK crude oil stocks (request contract only) ----
# Route documented at https://www.eia.gov/opendata/browser/petroleum/stoc/wstk.
_WEEKLY_STOCKS_ROUTE = "petroleum/stoc/wstk"
# "Stocks of Crude Oil, Cushing, OK (excl. SPR)" -- the series id shown on
# that browser page for the Cushing storage hub.
DEFAULT_WEEKLY_STOCKS_SERIES = "W_EPC0_SAX_YCUOK_MBBL"


def _redact_api_key(text: str) -> str:
    """Mask an ``api_key=...`` query value for safe logging.

    Minimal redaction helper -- no such helper exists elsewhere in
    ``ingestion/`` on this branch (checked before adding this) to reuse.
    Never logs the real key; used for the log line in
    :func:`fetch_weekly_stocks` and by tests asserting the built URL's
    displayed form never contains a real key value.
    """
    return re.sub(r"(api_key=)[^&]*", r"\1***", text)


class EIAPuller(BasePuller):
    """Pulls daily petroleum spot prices from the EIA API v2."""

    SOURCE_NAME: str = "EIA"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": _EIA_BASE,
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 20,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        self._api_key = os.environ.get("EIA_API_KEY", "")
        if not self._api_key:
            log.warning("EIA_API_KEY not set -- EIA pulls will fail")

    @retry_on_failure(
        max_attempts=3, backoff=2.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _fetch_series(self, facet: str, start: str, end: str) -> list[dict[str, Any]]:
        """Fetch daily spot price data for a single EIA series facet."""
        params = {
            "api_key": self._api_key, "frequency": "daily",
            "data[0]": "value", "facets[series][]": facet,
            "start": start, "end": end, "length": 5000,
        }
        resp = requests.get(_EIA_BASE, params=params, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json().get("response", {}).get("data", [])

    def pull(self, days_back: int = 90) -> dict[str, Any]:
        """Pull recent EIA spot prices for Brent and WTI."""
        if not self._api_key:
            return {"status": "FAILED", "rows_inserted": 0, "error": "EIA_API_KEY not set"}

        end_str = date.today().isoformat()
        start_str = (date.today() - timedelta(days=days_back)).isoformat()
        total = 0

        for facet, suffix in _SERIES_MAP.items():
            sid = f"{_SERIES_PREFIX}.{suffix}"
            try:
                records = self._fetch_series(facet, start_str, end_str)
            except Exception as exc:
                log.error("EIA fetch failed for {f}: {e}", f=facet, e=str(exc))
                continue

            with self.engine.begin() as conn:
                existing = self._get_existing_dates(sid, conn)
                for rec in records:
                    val, period = rec.get("value"), rec.get("period", "")
                    if val is None or not period:
                        continue
                    try:
                        obs = date.fromisoformat(period)
                        fv = float(val)
                    except (ValueError, TypeError):
                        continue
                    if obs in existing:
                        continue
                    self._insert_raw(conn=conn, series_id=sid, obs_date=obs,
                                     value=fv, raw_payload={"facet": facet})
                    total += 1
            time.sleep(1.0)

        log.info("EIA: {n} rows inserted", n=total)
        return {"status": "SUCCESS", "rows_inserted": total}


# ---------------------------------------------------------------------------
# Weekly Cushing crude oil stocks -- request contract only (not called live)
# ---------------------------------------------------------------------------


def build_weekly_stocks_request(
    series_id: str = DEFAULT_WEEKLY_STOCKS_SERIES,
    *,
    length: int = 5,
    api_key: str | None = None,
) -> dict[str, Any]:
    """Build (never send) the EIA v2 weekly petroleum-stocks request.

    Documented shape:
        <EIA_BASE_URL>petroleum/stoc/wstk/data/
            ?api_key=...&frequency=weekly&data[0]=value
            &facets[series][]=<series_id>
            &sort[0][column]=period&sort[0][direction]=desc&length=<length>

    Parameters:
        series_id: EIA series id (default
            ``DEFAULT_WEEKLY_STOCKS_SERIES`` -- Cushing, OK crude stocks).
        length: Number of most-recent periods to request.
        api_key: Overrides ``settings.EIA_API_KEY`` (mainly for tests --
            never pass a real key in a log line or test assertion).

    Returns:
        dict with ``url`` (base endpoint, no query string) and ``params``
        (the query parameters, including the real ``api_key`` value --
        callers/tests must run this through :func:`_redact_api_key` (via
        ``as_display_url``, or directly) before logging or asserting on
        it).
    """
    base = settings.EIA_BASE_URL.rstrip("/") + "/" + _WEEKLY_STOCKS_ROUTE + "/data/"
    params = {
        "api_key": api_key if api_key is not None else settings.EIA_API_KEY,
        "frequency": "weekly",
        "data[0]": "value",
        "facets[series][]": series_id,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": length,
    }
    return {"url": base, "params": params}


def as_display_url(request: dict[str, Any]) -> str:
    """Render a :func:`build_weekly_stocks_request` result as a full URL
    with the api_key masked -- safe to log or print."""
    full = f"{request['url']}?{urlencode(request['params'])}"
    return _redact_api_key(full)


def parse_weekly_stocks_response(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Parse the documented EIA v2 response envelope.

    Per EIA's API v2 documentation, a successful response has the shape
    ``{"response": {"data": [{"period": ..., "value": ..., "units": ...,
    ...}, ...], ...}, ...}``. Pure function -- no network.

    Parameters:
        payload: Decoded JSON response body.

    Returns:
        List of dicts ``{period: date, value: float, units: str | None,
        series: str | None}``, skipping any record missing ``period`` or
        ``value`` or with an unparseable value.
    """
    rows: list[dict[str, Any]] = []
    for rec in payload.get("response", {}).get("data", []):
        period, value = rec.get("period"), rec.get("value")
        if period is None or value is None:
            continue
        try:
            obs = date.fromisoformat(period)
            fv = float(value)
        except (ValueError, TypeError):
            log.warning("EIA weekly stocks: unparseable period/value: {r}", r=rec)
            continue
        rows.append(
            {
                "period": obs,
                "value": fv,
                "units": rec.get("units"),
                "series": rec.get("series"),
            }
        )
    return rows


def fetch_weekly_stocks(
    series_id: str = DEFAULT_WEEKLY_STOCKS_SERIES, *, length: int = 5
) -> list[dict[str, Any]]:
    """Fetch and parse weekly petroleum stocks for one series.

    NOT called anywhere in this pass (see module docstring) -- calling
    EIA needs a real ``EIA_API_KEY``, which this workstream's constraints
    do not permit spending. Provided so the contract is runnable once a
    key is available; fails closed (no request) if the key is unset.

    Parameters:
        series_id: EIA series id (default: Cushing, OK crude stocks).
        length: Number of most-recent periods to request.

    Returns:
        Parsed rows -- see :func:`parse_weekly_stocks_response`.

    Raises:
        RuntimeError: if ``settings.EIA_API_KEY`` is unset.
        requests.RequestException: on HTTP failure.
    """
    if not settings.EIA_API_KEY:
        raise RuntimeError(
            "EIA_API_KEY is not set -- refusing to call the EIA API. Set "
            "EIA_API_KEY in .env (see .env.example) first."
        )
    request = build_weekly_stocks_request(series_id, length=length)
    log.info("EIA weekly stocks request: {u}", u=as_display_url(request))  # key masked
    resp = requests.get(
        request["url"], params=request["params"], timeout=_REQUEST_TIMEOUT
    )
    resp.raise_for_status()
    return parse_weekly_stocks_response(resp.json())
