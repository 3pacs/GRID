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

import math
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
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

# Documented observation-date lower bound: pre-dates all EIA API v2 petroleum
# daily spot (WTI 1986+, Brent 1987+) and weekly stocks (1982+, Cushing 2004+)
# series while rejecting Unix-epoch (1970-01-01) and sentinel dates.
EIA_MIN_OBS_DATE: date = date(1980, 1, 1)

# Accepted normalized unit strings (stripped, upper-cased) per EIA feed.
_SPOT_EXPECTED_UNITS: frozenset[str] = frozenset(
    {"$/BBL", "USD/BBL", "DOLLARS PER BARREL"}
)
_WEEKLY_STOCKS_EXPECTED_UNITS: frozenset[str] = frozenset(
    {"MBBL", "THOUSAND BARRELS"}
)
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _utc_today() -> date:
    """Return the current UTC calendar date (pull clock)."""
    return datetime.now(timezone.utc).date()


def _extract_eia_data(payload: Any, *, label: str) -> tuple[list[Any], bool]:
    """Extract ``response.data`` list from a decoded EIA v2 JSON payload.

    Returns ``(data_list, malformed)`` where ``malformed=True`` indicates a
    non-dict envelope or non-list ``data`` field (fail closed). Missing
    ``response`` or missing ``data`` on an otherwise valid dict is treated as
    an empty response ``([], False)``.
    """
    if not isinstance(payload, dict):
        log.warning(
            "{lbl}: response payload is not a JSON object ({t})",
            lbl=label,
            t=type(payload).__name__,
        )
        return [], True
    response = payload.get("response")
    if response is None:
        return [], False
    if not isinstance(response, dict):
        log.warning(
            "{lbl}: response envelope is not a JSON object ({t})",
            lbl=label,
            t=type(response).__name__,
        )
        return [], True
    data = response.get("data")
    if data is None:
        return [], False
    if not isinstance(data, list):
        log.warning(
            "{lbl}: response.data is not a list ({t})",
            lbl=label,
            t=type(data).__name__,
        )
        return [], True
    return data, False


def _validate_eia_records(
    raw_records: Any,
    *,
    expected_series: str,
    expected_units: frozenset[str],
    as_of: date | None = None,
    min_obs_date: date = EIA_MIN_OBS_DATE,
    allow_negative: bool = True,
    label: str = "EIA",
) -> tuple[list[dict[str, Any]], int]:
    """Validate EIA API v2 records before any write.

    Every record must:
    - match ``expected_series`` on ``series``;
    - carry an expected unit in ``expected_units`` on ``units`` (or ``value-units``);
    - parse to a strict ``YYYY-MM-DD`` calendar date within ``[min_obs_date, as_of]``
      where ``as_of`` defaults to the pull's UTC clock;
    - carry a finite numeric ``value`` (rejecting ``None``, booleans, blank
      strings, non-numeric text, ``NaN``, ``inf``/``-inf``, and negative stock
      levels when ``allow_negative`` is False).

    Returns ``(valid_rows, rejected_count)``; every rejected record is logged at
    WARNING with its rejection reason.
    """
    if not isinstance(raw_records, list):
        log.warning(
            "{lbl}: records payload is not a list ({t})",
            lbl=label,
            t=type(raw_records).__name__,
        )
        return [], 1

    pull_date = as_of if as_of is not None else _utc_today()
    valid: list[dict[str, Any]] = []
    rejected = 0
    missing_units = 0

    for rec in raw_records:
        if not isinstance(rec, dict):
            log.warning("{lbl}: rejecting non-dict record: {r}", lbl=label, r=rec)
            rejected += 1
            continue

        # 1. Series / facet identity
        raw_series = rec.get("series")
        series_norm = raw_series.strip() if isinstance(raw_series, str) else ""
        if series_norm != expected_series:
            log.warning(
                "{lbl}: rejecting series mismatch (expected={exp!r}, got={got!r})",
                lbl=label,
                exp=expected_series,
                got=raw_series,
            )
            rejected += 1
            continue

        # 2. Unit verification. No recorded live response pins EIA's unit
        # field, so a record with NO unit field is accepted only because its
        # series id matched exactly above (counted, logged once below). A unit
        # that is present but unexpected is always rejected.
        raw_units = (
            rec.get("units")
            if rec.get("units") is not None
            else rec.get("value-units")
        )
        units_absent = raw_units is None or (
            isinstance(raw_units, str) and not raw_units.strip()
        )
        units_norm = raw_units.strip().upper() if isinstance(raw_units, str) else ""
        if units_absent:
            missing_units += 1
        elif units_norm not in expected_units:
            log.warning(
                "{lbl}: rejecting unexpected units for {s} (expected={exp}, got={got!r})",
                lbl=label,
                s=expected_series,
                exp=sorted(expected_units),
                got=raw_units,
            )
            rejected += 1
            continue

        # 3. Observation date parsing and bounds
        raw_period = rec.get("period")
        if isinstance(raw_period, date) and not isinstance(raw_period, datetime):
            obs = raw_period
        elif isinstance(raw_period, str) and _ISO_DATE_RE.match(raw_period.strip()):
            try:
                obs = date.fromisoformat(raw_period.strip())
            except ValueError:
                log.warning(
                    "{lbl}: rejecting invalid calendar period {p!r} for {s}",
                    lbl=label,
                    p=raw_period,
                    s=expected_series,
                )
                rejected += 1
                continue
        else:
            log.warning(
                "{lbl}: rejecting missing/unparseable period {p!r} for {s}",
                lbl=label,
                p=raw_period,
                s=expected_series,
            )
            rejected += 1
            continue

        if obs > pull_date:
            log.warning(
                "{lbl}: rejecting future observation date {d} > pull_date {p} for {s}",
                lbl=label,
                d=obs.isoformat(),
                p=pull_date.isoformat(),
                s=expected_series,
            )
            rejected += 1
            continue

        if obs < min_obs_date:
            log.warning(
                "{lbl}: rejecting observation date {d} < min bound {m} for {s}",
                lbl=label,
                d=obs.isoformat(),
                m=min_obs_date.isoformat(),
                s=expected_series,
            )
            rejected += 1
            continue

        # 4. Finite numeric value
        raw_val = rec.get("value")
        if raw_val is None or isinstance(raw_val, bool):
            log.warning(
                "{lbl}: rejecting missing/boolean value {v!r} on {d} for {s}",
                lbl=label,
                v=raw_val,
                d=obs.isoformat(),
                s=expected_series,
            )
            rejected += 1
            continue
        if isinstance(raw_val, str) and not raw_val.strip():
            log.warning(
                "{lbl}: rejecting blank value on {d} for {s}",
                lbl=label,
                d=obs.isoformat(),
                s=expected_series,
            )
            rejected += 1
            continue
        try:
            fv = float(raw_val)
        except (ValueError, TypeError):
            log.warning(
                "{lbl}: rejecting non-numeric value {v!r} on {d} for {s}",
                lbl=label,
                v=raw_val,
                d=obs.isoformat(),
                s=expected_series,
            )
            rejected += 1
            continue
        if not math.isfinite(fv):
            log.warning(
                "{lbl}: rejecting non-finite value {v!r} on {d} for {s}",
                lbl=label,
                v=raw_val,
                d=obs.isoformat(),
                s=expected_series,
            )
            rejected += 1
            continue
        if not allow_negative and fv < 0.0:
            log.warning(
                "{lbl}: rejecting negative stock value {v!r} on {d} for {s}",
                lbl=label,
                v=fv,
                d=obs.isoformat(),
                s=expected_series,
            )
            rejected += 1
            continue

        valid.append(
            {
                "period": obs,
                "value": fv,
                "units": None if units_absent else raw_units.strip(),
                "series": series_norm,
            }
        )

    if missing_units:
        log.warning(
            "{lbl}: {n} record(s) for {s} carried no unit field; accepted on exact "
            "series match (expected units {exp})",
            lbl=label,
            n=missing_units,
            s=expected_series,
            exp=sorted(expected_units),
        )
    return valid, rejected


def parse_daily_spot_response(
    payload: dict[str, Any],
    *,
    expected_series: str,
    expected_units: frozenset[str] = _SPOT_EXPECTED_UNITS,
    as_of: date | None = None,
    min_obs_date: date = EIA_MIN_OBS_DATE,
) -> list[dict[str, Any]]:
    """Parse and validate an EIA v2 daily spot response envelope."""
    label = f"EIA daily spot ({expected_series})"
    data, malformed = _extract_eia_data(payload, label=label)
    if malformed:
        return []
    valid, _ = _validate_eia_records(
        data,
        expected_series=expected_series,
        expected_units=expected_units,
        as_of=as_of,
        min_obs_date=min_obs_date,
        allow_negative=True,
        label=label,
    )
    return valid

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
        """Fetch and validate daily spot price data for a single EIA series facet."""
        params = {
            "api_key": self._api_key, "frequency": "daily",
            "data[0]": "value", "facets[series][]": facet,
            "start": start, "end": end, "length": 5000,
        }
        resp = requests.get(_EIA_BASE, params=params, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        label = f"EIA daily spot ({facet})"
        raw_data, malformed = _extract_eia_data(resp.json(), label=label)
        if malformed:
            raise ValueError(f"{label}: malformed response envelope")
        valid_rows, rejected = _validate_eia_records(
            raw_data,
            expected_series=facet,
            expected_units=_SPOT_EXPECTED_UNITS,
            as_of=_utc_today(),
            allow_negative=True,
            label=label,
        )
        if rejected > 0 and not valid_rows:
            raise ValueError(
                f"{label}: rejected all {rejected} record(s) from response"
            )
        return valid_rows

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

        pull_date = _utc_today()
        end_str = pull_date.isoformat()
        start_str = (pull_date - timedelta(days=days_back)).isoformat()
        total = 0
        failed_facets: list[str] = []

        for facet, suffix in _SERIES_MAP.items():
            sid = f"{_SERIES_PREFIX}:{suffix}"
            try:
                records = self._fetch_series(facet, start_str, end_str)
            except Exception as exc:
                # Upstream/network fault (bad JSON, HTTP error, timeout) or
                # wholly invalid payload -- WARNING per the project's log-level
                # convention (see ingestion/base.py::log_pull_failure).
                # The exception text from `requests` can embed the request
                # URL -- including our api_key query param -- so redact it.
                log.warning(
                    "EIA fetch failed for {f}: {e}",
                    f=facet,
                    e=_redact_api_key(str(exc)),
                )
                failed_facets.append(facet)
                continue

            valid_records, rejected = _validate_eia_records(
                records,
                expected_series=facet,
                expected_units=_SPOT_EXPECTED_UNITS,
                as_of=pull_date,
                allow_negative=True,
                label=f"EIA daily spot ({facet})",
            )
            if rejected > 0 and not valid_records:
                log.warning(
                    "EIA fetch failed for {f}: rejected all {n} record(s)",
                    f=facet,
                    n=rejected,
                )
                failed_facets.append(facet)
                time.sleep(1.0)
                continue

            if valid_records:
                with self.engine.begin() as conn:
                    existing = self._get_existing_dates(sid, conn)
                    for row in valid_records:
                        obs = row["period"]
                        if obs in existing:
                            continue
                        self._insert_raw(
                            conn=conn,
                            series_id=sid,
                            obs_date=obs,
                            value=row["value"],
                            raw_payload={
                                "facet": facet,
                                "series": row["series"],
                                "units": row["units"],
                            },
                        )
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
                as_of=pull_date,
            )
        except Exception as exc:
            log.warning(
                "EIA fetch failed for {f}: {e}",
                f=DEFAULT_WEEKLY_STOCKS_SERIES,
                e=_redact_api_key(str(exc)),
            )
            failed_facets.append(DEFAULT_WEEKLY_STOCKS_SERIES)
        else:
            valid_weekly, rejected_weekly = _validate_eia_records(
                weekly_rows,
                expected_series=DEFAULT_WEEKLY_STOCKS_SERIES,
                expected_units=_WEEKLY_STOCKS_EXPECTED_UNITS,
                as_of=pull_date,
                allow_negative=False,
                label=f"EIA weekly stocks ({DEFAULT_WEEKLY_STOCKS_SERIES})",
            )
            if rejected_weekly > 0 and not valid_weekly:
                log.warning(
                    "EIA fetch failed for {f}: rejected all {n} record(s)",
                    f=DEFAULT_WEEKLY_STOCKS_SERIES,
                    n=rejected_weekly,
                )
                failed_facets.append(DEFAULT_WEEKLY_STOCKS_SERIES)
            elif valid_weekly:
                with self.engine.begin() as conn:
                    existing = self._get_existing_dates(WEEKLY_STOCKS_SERIES_ID, conn)
                    for row in valid_weekly:
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
                                "series": row["series"],
                                "units": row["units"],
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


def parse_weekly_stocks_response(
    payload: dict[str, Any],
    *,
    expected_series: str = DEFAULT_WEEKLY_STOCKS_SERIES,
    expected_units: frozenset[str] = _WEEKLY_STOCKS_EXPECTED_UNITS,
    as_of: date | None = None,
    min_obs_date: date = EIA_MIN_OBS_DATE,
) -> list[dict[str, Any]]:
    """Parse and validate the documented EIA v2 weekly stocks response envelope.

    Per EIA's API v2 documentation, a successful response has the shape
    ``{"response": {"data": [{"period": ..., "value": ..., "units": ...,
    "series": ...}, ...], ...}, ...}``. Pure function -- no network.

    Every record is validated before return: ``series`` must match
    ``expected_series``, ``units`` must match ``expected_units`` (thousand
    barrels / ``MBBL`` by default), ``value`` must be a finite non-negative
    float, and ``period`` must lie within ``[min_obs_date, as_of]`` (UTC
    pull clock). Rejected records are skipped and logged at WARNING.

    Parameters:
        payload: Decoded JSON response body.
        expected_series: Expected EIA series code (default Cushing stocks).
        expected_units: Accepted normalized unit strings (upper-cased).
        as_of: Pull clock UTC date upper bound (default: today UTC).
        min_obs_date: Earliest accepted observation date (default: 1980-01-01).

    Returns:
        List of validated dicts ``{period: date, value: float, units: str,
        series: str}``.
    """
    label = f"EIA weekly stocks ({expected_series})"
    raw_data, malformed = _extract_eia_data(payload, label=label)
    if malformed:
        return []
    valid_rows, _ = _validate_eia_records(
        raw_data,
        expected_series=expected_series,
        expected_units=expected_units,
        as_of=as_of,
        min_obs_date=min_obs_date,
        allow_negative=False,
        label=label,
    )
    return valid_rows


def fetch_weekly_stocks(
    series_id: str = DEFAULT_WEEKLY_STOCKS_SERIES,
    *,
    length: int = 5,
    api_key: str | None = None,
    as_of: date | None = None,
) -> list[dict[str, Any]]:
    """Fetch, validate, and parse weekly petroleum stocks for one series.

    Fails closed (no request) if the API key is unset, and raises
    ``ValueError`` when a non-empty response fails validation on every
    record (e.g. wrong series, wrong units, future-dated, or non-finite).

    Parameters:
        series_id: EIA series id (default: Cushing, OK crude stocks).
        length: Number of most-recent periods to request.
        api_key: Optional API key override; defaults to ``settings.EIA_API_KEY``.
        as_of: Optional UTC pull-date upper bound; defaults to today UTC.

    Returns:
        Parsed rows -- see :func:`parse_weekly_stocks_response`.

    Raises:
        RuntimeError: if the effective EIA API key is unset.
        ValueError: if the response envelope is malformed or all returned
            records fail validation.
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
    label = f"EIA weekly stocks ({series_id})"
    raw_data, malformed = _extract_eia_data(resp.json(), label=label)
    if malformed:
        raise ValueError(f"{label}: malformed response envelope")
    valid_rows, rejected = _validate_eia_records(
        raw_data,
        expected_series=series_id,
        expected_units=_WEEKLY_STOCKS_EXPECTED_UNITS,
        as_of=as_of,
        allow_negative=False,
        label=label,
    )
    if rejected > 0 and not valid_rows:
        raise ValueError(
            f"{label}: rejected all {rejected} record(s) from response"
        )
    return valid_rows
