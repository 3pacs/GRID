"""
SDMX-JSON data-message helpers shared by the BIS and IMF pullers.

Both institutions moved to SDMX REST services that return SDMX-JSON data
messages, but they disagree on the envelope:

* BIS (``stats.bis.org/api/v2``) answers SDMX-JSON 1.0:
  ``{"data": {"dataSets": [...], "structure": {...}}}`` with time-period
  codes under ``values[i]["id"]`` (``"2026-Q1"``).
* IMF (``api.imf.org/external/sdmx/3.0``) answers SDMX-JSON 2.0:
  ``{"data": {"dataSets": [...], "structures": [{...}]}}`` with time-period
  codes under ``values[i]["value"]`` (``"2026-Q1"``, ``"2026-M08"``).

Older SDMX-JSON payloads put ``dataSets``/``structure`` at the top level
with no ``data`` wrapper; :func:`parse_sdmx_json` accepts all three.
Observation values arrive as strings (``"-4.9524"``) and are converted to
float; null / non-numeric observations are dropped, never coerced to 0.

Endpoints and keys were verified by probes from grid-svr on 2026-11-04.
"""

from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any

import requests

BIS_DATA_URL: str = "https://stats.bis.org/api/v2/data/dataflow/BIS"
IMF_DATA_URL: str = "https://api.imf.org/external/sdmx/3.0/data/dataflow/IMF.STA"

# BIS answers 406 without an explicit SDMX-JSON Accept header.
BIS_ACCEPT: str = "application/vnd.sdmx.data+json;version=1.0.0"
IMF_ACCEPT: str = "application/json"

DEFAULT_TIMEOUT: tuple[float, float] = (10.0, 60.0)


def parse_period(period: str) -> date | None:
    """Convert an SDMX time-period code to the first day of the period.

    Accepts ``YYYY``, ``YYYY-Qn``, ``YYYYQn``, ``YYYY-Mmm``, ``YYYY-MM``,
    ``YYYY-MM-DD`` and ``YYYY-Sn`` (half-year). Returns None for anything else.
    """
    s = (period or "").strip()
    try:
        if len(s) == 4 and s.isdigit():
            return date(int(s), 1, 1)
        if "-Q" in s or (len(s) == 6 and s[4] == "Q"):
            year, q = s.replace("-Q", "Q").split("Q")
            qn = int(q)
            if 1 <= qn <= 4:
                return date(int(year), (qn - 1) * 3 + 1, 1)
            return None
        if "-M" in s:
            year, m = s.split("-M")
            return date(int(year), int(m), 1)
        if "-S" in s:
            year, h = s.split("-S")
            return date(int(year), 1 if int(h) == 1 else 7, 1)
        if len(s) == 7 and s[4] == "-":
            return datetime.strptime(s, "%Y-%m").date()
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None
    return None


def lookback_start_year(start_date: object, lookback_years: int = 2) -> str | None:
    """SDMX ``startPeriod`` year for an incremental pull.

    Official statistics publish with a lag of one to three quarters, so an
    incremental start date of "last week" must not filter out the latest
    published period. Backs the requested year off by ``lookback_years``.
    """
    if not start_date:
        return None
    try:
        year = int(str(start_date)[:4])
    except ValueError:
        return None
    return str(year - lookback_years)


def _structure(payload: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Return ``(structure, dataSets)`` for any supported SDMX-JSON envelope."""
    body = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    datasets = body.get("dataSets") or []
    structure = body.get("structure")
    if not isinstance(structure, dict):
        structures = body.get("structures") or []
        structure = structures[0] if structures and isinstance(structures[0], dict) else {}
    return structure, datasets


def parse_sdmx_json(payload: dict[str, Any]) -> dict[str, list[tuple[date, float]]]:
    """Parse an SDMX-JSON data message into ``{series_key: [(period, value)]}``.

    ``series_key`` is the dotted dimension-code key (``"Q.US.P.A.C"``) so a
    caller can tell series apart in a multi-series response. Observations
    are sorted by period. Raises ``ValueError`` when the payload is not an
    SDMX-JSON data message at all (an HTML error page decoded as JSON, an
    error envelope), so callers can report a truthful failure instead of a
    silent "no data".
    """
    if not isinstance(payload, dict):
        raise ValueError(f"SDMX payload is {type(payload).__name__}, not an object")
    structure, datasets = _structure(payload)
    dims = structure.get("dimensions") if isinstance(structure, dict) else None
    if not isinstance(dims, dict):
        raise ValueError("SDMX payload has no structure.dimensions")

    series_dims = dims.get("series") or []
    obs_dims = dims.get("observation") or []
    time_codes: list[str] = []
    for dim in obs_dims:
        if dim.get("id") in ("TIME_PERIOD", "TIME") or dim.get("role") == "time":
            time_codes = [str(v.get("id") or v.get("value") or "") for v in dim.get("values") or []]
            break

    out: dict[str, list[tuple[date, float]]] = {}
    for ds in datasets:
        for raw_key, series in (ds.get("series") or {}).items():
            try:
                idx = [int(p) for p in str(raw_key).split(":")]
                key = ".".join(
                    str(series_dims[pos]["values"][i]["id"]) for pos, i in enumerate(idx)
                )
            except (ValueError, IndexError, KeyError, TypeError):
                key = str(raw_key)
            points: list[tuple[date, float]] = []
            for t_idx, obs in (series.get("observations") or {}).items():
                try:
                    period = time_codes[int(t_idx)]
                except (ValueError, IndexError):
                    continue
                raw = obs[0] if isinstance(obs, list) and obs else None
                if raw is None:
                    continue
                try:
                    value = float(raw)
                except (TypeError, ValueError):
                    continue
                if math.isnan(value) or math.isinf(value):
                    continue
                obs_dt = parse_period(period)
                if obs_dt is not None:
                    points.append((obs_dt, value))
            points.sort()
            out[key] = points
    return out


def fetch_sdmx_json(
    url: str,
    params: dict[str, str] | None = None,
    accept: str = IMF_ACCEPT,
    timeout: tuple[float, float] = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """GET an SDMX-JSON data message; raises ``requests.HTTPError`` on non-2xx."""
    resp = requests.get(
        url,
        params=params or {},
        headers={"Accept": accept, "User-Agent": "GRID/1.0 (+https://grid.stepdad.finance)"},
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json()


def bis_series_url(flow: str, key: str, version: str = "1.0") -> str:
    """Return the BIS SDMX v2 data URL for one dataflow key."""
    return f"{BIS_DATA_URL}/{flow}/{version}/{key}"


def imf_series_url(flow: str, key: str) -> str:
    """Return the IMF SDMX 3.0 data URL for one dataflow key (latest version)."""
    return f"{IMF_DATA_URL}/{flow}/+/{key}"


def fetch_bis_series(flow: str, key: str, start_period: str | None = None) -> list[tuple[date, float]]:
    """Fetch one BIS series; returns its observations (empty when BIS has none)."""
    params = {"startPeriod": start_period} if start_period else {}
    payload = fetch_sdmx_json(bis_series_url(flow, key), params, accept=BIS_ACCEPT)
    series = parse_sdmx_json(payload)
    return series.get(key) or (next(iter(series.values())) if len(series) == 1 else [])


def fetch_imf_series(flow: str, key: str, start_period: str | None = None) -> list[tuple[date, float]]:
    """Fetch one IMF series; returns its observations (empty when IMF has none)."""
    params = {"c[TIME_PERIOD]": f"ge:{start_period}"} if start_period else {}
    payload = fetch_sdmx_json(imf_series_url(flow, key), params, accept=IMF_ACCEPT)
    series = parse_sdmx_json(payload)
    return series.get(key) or (next(iter(series.values())) if len(series) == 1 else [])
