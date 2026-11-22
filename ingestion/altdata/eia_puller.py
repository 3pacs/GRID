"""
GRID EIA energy data ingestion module.

Pulls Brent/WTI crude oil daily spot prices from the EIA API v2.

Data source: https://api.eia.gov/v2/petroleum/pri/spt/data/
API key required (free): set EIA_API_KEY env var.

Series stored (aligned with ``normalization/entity_map.py``):
- ``EIA:eia_brent_spot``: Brent crude oil spot price (RBRTE, USD/bbl)
- ``EIA:eia_crude_price``: WTI crude oil spot price (RWTC, USD/bbl)
- ``EIA:eia_cushing_crude_stocks``: Weekly Cushing, OK crude oil stocks excl. SPR
  (``W_EPC0_SAX_YCUOK_MBBL``, thousand barrels)

Weekly Cushing crude stocks
---------------------------
``build_weekly_stocks_request`` / ``fetch_weekly_stocks`` /
``parse_weekly_stocks_response`` implement the v2 request contract for
the weekly petroleum stocks route, documented at
https://www.eia.gov/opendata/browser/petroleum/stoc/wstk (route + facet
shape) and https://www.eia.gov/opendata/documentation.php (general query
parameter shape: ``data[0]=value``, ``facets[series][]=...``,
``frequency``, ``api_key``, ``sort[...]``, ``length``), and are wired
into ``EIAPuller.pull()``.
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
_SERIES_PREFIX = "EIA"
# EIA API v2 daily spot facet -> entity_map suffix (RWTC aligns to existing
# EIA:eia_crude_price mapping in normalization/entity_map.py).
_SERIES_MAP: dict[str, str] = {
    "RBRTE": "eia_brent_spot",
    "RWTC": "eia_crude_price",
}

# ---- Weekly Cushing, OK crude oil stocks ----
# Route documented at https://www.eia.gov/opendata/browser/petroleum/stoc/wstk.
_WEEKLY_STOCKS_ROUTE = "petroleum/stoc/wstk"
# "Stocks of Crude Oil, Cushing, OK (excl. SPR)" -- the series id shown on
# that browser page for the Cushing storage hub.
DEFAULT_WEEKLY_STOCKS_SERIES = "W_EPC0_SAX_YCUOK_MBBL"
WEEKLY_STOCKS_SERIES_ID: str = f"{_SERIES_PREFIX}:eia_cushing_crude_stocks"

# Single source of truth for every raw_series.series_id this puller emits.
EMITTED_SERIES_IDS: tuple[str, ...] = (
    *(f"{_SERIES_PREFIX}:{suffix}" for suffix in _SERIES_MAP.values()),
    WEEKLY_STOCKS_SERIES_ID,
)

# EIA requests carry ``api_key=<value>`` in the query string, so any
# exception raised by ``requests`` (e.g. ``HTTPError.__str__``, which embeds
# ``response.url``) can echo the live key straight into the log. Redact it
# before anything derived from a request exception is logged. (Reviewed
# helper from #553 -- kept as the single definition per the integration
# fix; see tests/test_source_eia.py for its redaction test. The
# weekly-stocks code below (as_display_url/fetch_weekly_stocks) calls this
# same helper rather than defining its own.)
_API_KEY_QS_PATTERN = re.compile(r"(api_key=)[^&\s]+", re.IGNORECASE)


def _redact_api_key(text: str) -> str:
    """Strip an ``api_key=...`` query value out of an error message."""
    return _API_KEY_QS_PATTERN.sub(r"\1***", text)


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
        """Pull recent EIA spot prices for Brent and WTI.

        Returns:
            dict with ``status``:

            - ``"FAILED"`` when EVERY series facet's fetch failed (nothing
              was inserted this call) -- e.g. an expired/invalid API key,
              EIA's API down, or a network partition.
            - ``"PARTIAL"`` when SOME (not all) facets failed -- rows from
              the facet(s) that DID succeed are already committed, but
              this call was not a clean run. SmartScheduler's own
              vocabulary only distinguishes SUCCESS/SKIPPED/FAILED (see
              ``SmartScheduler._run_puller``'s "Mitigation 3" docstring),
              so this is treated the same as FAILED at the scheduler layer
              -- no ``last_pull_at`` advance, cooldown applies -- so the
              still-missing facet gets retried on the next cheap tick
              instead of waiting a full ``freq_h``.
            - ``"SUCCESS"`` when every facet's fetch succeeded (this can
              still mean ``rows_inserted == 0`` if every row was already
              stored -- that is a normal idempotent no-op, not a failure).

            Before this fix, a total fetch failure across BOTH facets
            still returned ``{"status": "SUCCESS", "rows_inserted": 0}``
            -- indistinguishable from "ran cleanly, nothing new to
            insert" -- which fed straight into
            ``SmartScheduler._run_puller``'s old bug of trusting any
            non-SKIPPED dict as SUCCESS (see that method's docstring).
        """
        if not self._api_key:
            return {"status": "FAILED", "rows_inserted": 0, "error": "EIA_API_KEY not set"}

        end_str = date.today().isoformat()
        start_str = (date.today() - timedelta(days=days_back)).isoformat()
        total = 0
        failed_facets: list[str] = []

        for facet, suffix in _SERIES_MAP.items():
            sid = f"{_SERIES_PREFIX}:{suffix}"
            try:
                records = self._fetch_series(facet, start_str, end_str)
            except Exception as exc:
                # Upstream/network fault (bad JSON, HTTP error, timeout) --
                # not a code bug, so this is a WARNING per the project's
                # log-level convention (see ingestion/base.py::log_pull_failure).
                # The exception text from `requests` can embed the request
                # URL -- including our api_key query param -- so redact it.
                log.warning(
                    "EIA fetch failed for {f}: {e}",
                    f=facet,
                    e=_redact_api_key(str(exc)),
                )
                failed_facets.append(facet)
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
                    # A single response can (rarely) repeat a period -- e.g. a
                    # duplicated row from an upstream retry-within-response.
                    # Record it as seen immediately so a repeat later in this
                    # same `records` list is skipped too, not just repeats
                    # across separate pull() calls (which `existing` already
                    # covered before this fix).
                    existing.add(obs)
            time.sleep(1.0)

        # Weekly Cushing, OK crude oil stocks (petroleum/stoc/wstk)
        weekly_length = max(5, (days_back // 7) + 2)
        try:
            weekly_rows = fetch_weekly_stocks(
                DEFAULT_WEEKLY_STOCKS_SERIES,
                length=weekly_length,
                api_key=self._api_key,
            )
        except Exception as exc:
            log.warning(
                "EIA fetch failed for {f}: {e}",
                f=DEFAULT_WEEKLY_STOCKS_SERIES,
                e=_redact_api_key(str(exc)),
            )
            failed_facets.append(DEFAULT_WEEKLY_STOCKS_SERIES)
        else:
            with self.engine.begin() as conn:
                existing = self._get_existing_dates(WEEKLY_STOCKS_SERIES_ID, conn)
                for row in weekly_rows:
                    obs = row["period"]
                    if obs in existing:
                        continue
                    self._insert_raw(
                        conn=conn,
                        series_id=WEEKLY_STOCKS_SERIES_ID,
                        obs_date=obs,
                        value=row["value"],
                        raw_payload={
                            "facet": DEFAULT_WEEKLY_STOCKS_SERIES,
                            "series": row.get("series"),
                            "units": row.get("units"),
                        },
                    )
                    total += 1
                    existing.add(obs)

        log.info("EIA: {n} rows inserted", n=total)

        if len(failed_facets) == len(EMITTED_SERIES_IDS):
            return {
                "status": "FAILED",
                "rows_inserted": total,
                "error": f"every facet fetch failed: {failed_facets}",
            }
        if failed_facets:
            return {
                "status": "PARTIAL",
                "rows_inserted": total,
                "error": f"facet fetch failed: {failed_facets}",
            }
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
    series_id: str = DEFAULT_WEEKLY_STOCKS_SERIES,
    *,
    length: int = 5,
    api_key: str | None = None,
) -> list[dict[str, Any]]:
    """Fetch and parse weekly petroleum stocks for one series.

    Fails closed (no request) if the API key is unset.

    Parameters:
        series_id: EIA series id (default: Cushing, OK crude stocks).
        length: Number of most-recent periods to request.
        api_key: Optional API key override; defaults to ``settings.EIA_API_KEY``.

    Returns:
        Parsed rows -- see :func:`parse_weekly_stocks_response`.

    Raises:
        RuntimeError: if the effective EIA API key is unset.
        requests.RequestException: on HTTP failure.
    """
    effective_key = settings.EIA_API_KEY if api_key is None else api_key
    if not effective_key:
        raise RuntimeError(
            "EIA_API_KEY is not set -- refusing to call the EIA API. Set "
            "EIA_API_KEY in .env (see .env.example) first."
        )
    request = build_weekly_stocks_request(
        series_id, length=length, api_key=effective_key
    )
    log.info("EIA weekly stocks request: {u}", u=as_display_url(request))  # key masked
    resp = requests.get(
        request["url"], params=request["params"], timeout=_REQUEST_TIMEOUT
    )
    resp.raise_for_status()
    return parse_weekly_stocks_response(resp.json())
