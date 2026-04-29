"""
GRID JODI (Joint Organisations Data Initiative) Oil World Database puller.

Why JODI matters
================
JODI Oil is the ONLY public, monthly, country-level oil supply / inventory
dataset that covers the producers EIA and IEA do **not** track in detail:
Saudi Arabia, UAE, Kuwait, Iraq, Russia, Iran, Venezuela, Nigeria, Algeria,
Angola, Indonesia, Malaysia, Vietnam — i.e. the bulk of OPEC+ and most of the
non-OECD supply side.  It is jointly produced by IEA, OPEC, UN Statistics,
Eurostat and APEC, published every month, and free of charge.

For GRID's oil sector intelligence this is a Tier-A novel data source that
is *not* on the original CAT catalog.  Specific edges it unlocks:

* **Saudi / UAE closing-stock drops** — sudden falls in Aramco / ADNOC closing
  inventories are a *physical tightness* signal that historically leads
  Brent by 4-8 weeks.  Nothing else public reports them at this cadence.
* **Russian production / export deltas** — directly relevant for sanctions
  enforcement monitoring; the only monthly print-of-record outside RosStat
  (which is no longer reliably published).
* **OPEC+ compliance** — JODI vs OPEC quota = real-world cheating signal.
* **Refined product flows** — gasoline / jet / diesel imports/exports per
  country let us see the demand side of emerging markets in near real time.

Data strategy
=============
* **Primary**: bulk CSV download from
  ``https://www.jodidata.org/oil/database/data-downloads.aspx``.  Free, no
  API key required.  Format documented in the JODI Oil Data Manual.
* **Backup**: SDMX-JSON endpoint ``https://www.jodidata.org/api/sdmx/data/JODI,OIL``
  used if the CSV path fails (the bulk download URL occasionally changes
  between releases, and the SDMX endpoint is more stable).
* **Graceful degrade**: if both fail we log a warning and return zero rows.
  Never crash the scheduler.

Series identifiers stored in raw_series:
    ``jodi:<COUNTRY>:<PRODUCT>:<FLOW>``  e.g.  ``jodi:SAU:CRUDEOIL:CLOSSTLV``
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure


# ---------------------------------------------------------------------------
# Constants — the tracking matrix
# ---------------------------------------------------------------------------

#: ISO-3 country codes for the producers / consumers worth pulling.
#: 15 entries — the OPEC core plus the largest non-OECD producers + a few
#: heavyweight consumers (USA, CHN, BRA) for cross-checks.
TRACKED_COUNTRIES: tuple[str, ...] = (
    "SAU",  # Saudi Arabia
    "ARE",  # United Arab Emirates
    "KWT",  # Kuwait
    "IRQ",  # Iraq
    "RUS",  # Russia
    "IRN",  # Iran
    "VEN",  # Venezuela
    "NGA",  # Nigeria
    "DZA",  # Algeria
    "AGO",  # Angola
    "USA",  # United States (cross-check vs EIA)
    "CHN",  # China
    "IDN",  # Indonesia
    "MYS",  # Malaysia
    "BRA",  # Brazil
)

#: JODI oil "energy product" codes we track.  CRUDEOIL is the headline series;
#: the refined products give us demand-side colour for each country.
TRACKED_PRODUCTS: tuple[str, ...] = (
    "CRUDEOIL",   # Crude oil
    "GASOLINE",   # Motor gasoline
    "JETKERO",    # Jet kerosene / kerosene-type jet fuel
    "GASDIESEL",  # Gas / diesel oil
    "RESFUEL",    # Residual fuel oil
    "LPG",        # Liquefied petroleum gases
    "NAPHTHA",    # Naphtha
)

#: JODI "flow breakdown" codes we keep.  CLOSSTLV (closing stock level) is
#: the inventory series — the most market-moving one.
TRACKED_FLOWS: tuple[str, ...] = (
    "PRODUCTION",  # Indigenous production
    "IMPORTS",     # Total imports
    "EXPORTS",     # Total exports
    "CLOSSTLV",    # Closing stock level
)

#: Bulk CSV download URL.  JODI publishes this from the public downloads
#: page; the file name is regenerated each release but the canonical "world"
#: bundle is served from this path.  If the URL drifts we fall back to SDMX.
JODI_CSV_URL: str = (
    "https://www.jodidata.org/_resources/files/downloads/oil-data/world_Primary_CSV.csv"
)

#: SDMX-JSON endpoint — used as the resilient fallback.
JODI_SDMX_URL: str = "https://www.jodidata.org/api/sdmx/data/JODI,OIL"

_REQUEST_TIMEOUT: int = 60
_USER_AGENT: str = "GRID/1.0 (+https://grid.stepdad.finance) jodi-oil-puller"

#: JODI sentinel values that mean "no observation" — these must be filtered out
#: before insertion (they are not zero, they mean "missing").
_MISSING_SENTINELS: frozenset[str] = frozenset({"", "..", "...", "x", "X", "n/a", "N/A", "NA"})


# ---------------------------------------------------------------------------
# Domain dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class JODIObservation:
    """A single (country, product, flow, month) observation from JODI Oil.

    Attributes:
        month_end: First-of-month date for the observation period.
        country: ISO-3 country code (REF_AREA).
        product: JODI energy product code (ENERGY_PRODUCT).
        flow: JODI flow breakdown code (FLOW_BREAKDOWN).
        value: Observation value, already coerced to float.
        unit: Unit of measure — typically ``KBD`` (kilobarrels/day) for flows
            or ``KBBL`` (thousand barrels) for stock levels.
        assessment: JODI data-quality / assessment code:
            ``"1"`` definite, ``"2"`` preliminary, ``"3"`` projected/estimate,
            ``"4"`` other.  Empty string when not provided.
    """

    month_end: date
    country: str
    product: str
    flow: str
    value: float
    unit: str
    assessment: str


# ---------------------------------------------------------------------------
# Pure helpers — testable in isolation
# ---------------------------------------------------------------------------


def _is_tracked(country: str, product: str, flow: str) -> bool:
    """Return True if (country, product, flow) is in the tracking matrix.

    Used to skip irrelevant rows during CSV / SDMX parsing.  Uppercased and
    whitespace-stripped before comparison so that minor formatting drift in
    JODI's monthly releases does not cause silent drops.
    """
    if not country or not product or not flow:
        return False
    c = country.strip().upper()
    p = product.strip().upper()
    f = flow.strip().upper()
    return (
        c in TRACKED_COUNTRIES
        and p in TRACKED_PRODUCTS
        and f in TRACKED_FLOWS
    )


def _parse_month_period(period: str) -> date | None:
    """Parse a JODI ``TIME_PERIOD`` (``YYYY-MM`` or ``YYYYMM``) to a first-of-month date.

    Returns None on any parse failure.
    """
    if not period:
        return None
    s = period.strip()
    try:
        if len(s) == 7 and s[4] == "-":
            year, month = int(s[:4]), int(s[5:])
        elif len(s) == 6 and s.isdigit():
            year, month = int(s[:4]), int(s[4:])
        elif len(s) == 10 and s[4] == "-" and s[7] == "-":
            # full ISO date — accept the year/month part
            year, month = int(s[:4]), int(s[5:7])
        else:
            return None
        if not (1900 <= year <= 2100 and 1 <= month <= 12):
            return None
        return date(year, month, 1)
    except (ValueError, TypeError):
        return None


def _coerce_value(raw: Any) -> float | None:
    """Coerce a JODI OBS_VALUE cell to a float, treating sentinels as missing."""
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        try:
            f = float(raw)
            if f != f:  # NaN
                return None
            return f
        except (TypeError, ValueError):
            return None
    s = str(raw).strip()
    if s in _MISSING_SENTINELS:
        return None
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


# JODI column-name fallbacks (JODI has tweaked headers between releases —
# ``REF_AREA`` was once ``COUNTRY``, ``OBS_VALUE`` was once ``VALUE``).
_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "country":    ("REF_AREA", "COUNTRY", "REF AREA", "Country"),
    "product":    ("ENERGY_PRODUCT", "PRODUCT", "ENERGY PRODUCT", "Product"),
    "flow":       ("FLOW_BREAKDOWN", "FLOW", "FLOW BREAKDOWN", "Flow"),
    "unit":       ("UNIT_MEASURE", "UNIT", "UNIT MEASURE", "Unit"),
    "period":     ("TIME_PERIOD", "TIME", "PERIOD", "TIME PERIOD", "Time"),
    "value":      ("OBS_VALUE", "VALUE", "OBS VALUE", "Value"),
    "assessment": ("ASSESSMENT_CODE", "ASSESSMENT", "ASSESSMENT CODE"),
}


def _resolve_columns(fieldnames: Iterable[str] | None) -> dict[str, str] | None:
    """Map our canonical field names to whichever variant the CSV exposes.

    Returns None if any required column is missing.
    """
    if not fieldnames:
        return None
    available = {name: name for name in fieldnames if name}
    # case-insensitive lookup
    lc_index = {name.lower(): name for name in available}
    resolved: dict[str, str] = {}
    for canonical, aliases in _COLUMN_ALIASES.items():
        hit: str | None = None
        for alias in aliases:
            if alias in available:
                hit = alias
                break
            if alias.lower() in lc_index:
                hit = lc_index[alias.lower()]
                break
        if hit is None and canonical != "assessment":
            # assessment is optional; everything else required
            return None
        if hit is not None:
            resolved[canonical] = hit
    return resolved


def _parse_jodi_csv(csv_text: str) -> list[JODIObservation]:
    """Parse a JODI Oil bulk CSV payload into a list of observations.

    * Tolerates header drift between monthly releases via ``_resolve_columns``.
    * Skips rows that fail tracking-matrix filter, missing values, bad dates.
    * Never raises on malformed input — returns an empty list at worst.
    """
    if not csv_text or not csv_text.strip():
        return []

    try:
        reader = csv.DictReader(io.StringIO(csv_text))
        cols = _resolve_columns(reader.fieldnames)
    except (csv.Error, ValueError) as exc:
        log.warning("JODI CSV header parse failed: {e}", e=str(exc))
        return []

    if cols is None:
        log.warning(
            "JODI CSV missing required columns; got: {f}",
            f=list(reader.fieldnames or []),
        )
        return []

    out: list[JODIObservation] = []
    for row in reader:
        try:
            country = (row.get(cols["country"]) or "").strip().upper()
            product = (row.get(cols["product"]) or "").strip().upper()
            flow = (row.get(cols["flow"]) or "").strip().upper()
            if not _is_tracked(country, product, flow):
                continue

            month = _parse_month_period(row.get(cols["period"]) or "")
            if month is None:
                continue

            value = _coerce_value(row.get(cols["value"]))
            if value is None:
                continue

            unit = (row.get(cols["unit"]) or "").strip().upper()
            assessment = (
                (row.get(cols["assessment"]) or "").strip()
                if "assessment" in cols
                else ""
            )

            out.append(
                JODIObservation(
                    month_end=month,
                    country=country,
                    product=product,
                    flow=flow,
                    value=value,
                    unit=unit,
                    assessment=assessment,
                )
            )
        except (KeyError, TypeError, ValueError) as exc:
            log.debug("JODI CSV row skipped: {e}", e=str(exc))
            continue

    return out


def _parse_jodi_sdmx(payload: dict[str, Any]) -> list[JODIObservation]:
    """Parse a JODI SDMX-JSON ``data`` payload into observations.

    Supports the standard SDMX-JSON 1.0 layout used by the JODI endpoint:
    ``dataSets[0].series[<key>].observations[<obs_idx>] = [value, ...]``
    where the series key is colon-separated dimension positions and the
    dimension order comes from ``structure.dimensions.series``.
    Time is on ``structure.dimensions.observation[0]``.

    Returns an empty list on any structural mismatch.
    """
    if not isinstance(payload, dict):
        return []

    try:
        structure = payload.get("structure") or {}
        dims = structure.get("dimensions") or {}
        series_dims = dims.get("series") or []
        obs_dims = dims.get("observation") or []
        if not series_dims or not obs_dims:
            return []

        # Map dim id -> (position, list of code values)
        dim_lookup: dict[str, tuple[int, list[str]]] = {}
        for i, d in enumerate(series_dims):
            did = (d.get("id") or "").upper()
            values = [str(v.get("id", "")) for v in (d.get("values") or [])]
            dim_lookup[did] = (i, values)

        time_dim = obs_dims[0]
        time_codes = [str(v.get("id", "")) for v in (time_dim.get("values") or [])]

        datasets = payload.get("dataSets") or []
        if not datasets:
            return []
        series_blob = (datasets[0] or {}).get("series") or {}

        country_pos_vals = dim_lookup.get("REF_AREA")
        product_pos_vals = dim_lookup.get("ENERGY_PRODUCT") or dim_lookup.get("PRODUCT")
        flow_pos_vals = dim_lookup.get("FLOW_BREAKDOWN") or dim_lookup.get("FLOW")
        unit_pos_vals = dim_lookup.get("UNIT_MEASURE") or dim_lookup.get("UNIT")
        if not (country_pos_vals and product_pos_vals and flow_pos_vals):
            return []

        out: list[JODIObservation] = []
        for series_key, series_obj in series_blob.items():
            try:
                key_parts = [int(p) for p in series_key.split(":") if p != ""]
            except ValueError:
                continue

            def _lookup(pos_vals: tuple[int, list[str]]) -> str:
                pos, vals = pos_vals
                if pos >= len(key_parts):
                    return ""
                idx = key_parts[pos]
                if 0 <= idx < len(vals):
                    return vals[idx]
                return ""

            country = _lookup(country_pos_vals).upper()
            product = _lookup(product_pos_vals).upper()
            flow = _lookup(flow_pos_vals).upper()
            unit = _lookup(unit_pos_vals).upper() if unit_pos_vals else ""
            if not _is_tracked(country, product, flow):
                continue

            observations = (series_obj or {}).get("observations") or {}
            for time_idx_str, obs_arr in observations.items():
                try:
                    time_idx = int(time_idx_str)
                except (TypeError, ValueError):
                    continue
                if not (0 <= time_idx < len(time_codes)):
                    continue
                month = _parse_month_period(time_codes[time_idx])
                if month is None:
                    continue
                if not isinstance(obs_arr, (list, tuple)) or not obs_arr:
                    continue
                value = _coerce_value(obs_arr[0])
                if value is None:
                    continue
                assessment = ""
                if len(obs_arr) > 1 and obs_arr[1] is not None:
                    assessment = str(obs_arr[1]).strip()
                out.append(
                    JODIObservation(
                        month_end=month,
                        country=country,
                        product=product,
                        flow=flow,
                        value=value,
                        unit=unit,
                        assessment=assessment,
                    )
                )
        return out
    except (KeyError, TypeError, AttributeError) as exc:
        log.warning("JODI SDMX parse failed: {e}", e=str(exc))
        return []


# ---------------------------------------------------------------------------
# Puller class
# ---------------------------------------------------------------------------


class JODIOilPuller(BasePuller):
    """Monthly puller for the JODI Oil World Database.

    Tries the bulk CSV first, falls back to SDMX-JSON.  Always graceful: if
    both endpoints fail the puller logs a warning and returns zero rows so
    that the scheduler is never blocked.
    """

    SOURCE_NAME: str = "jodi_oil"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://www.jodidata.org/oil/",
        "cost_tier": "FREE",
        "latency_class": "MONTHLY",
        "pit_available": False,
        "revision_behavior": "REVISED",
        "trust_score": "HIGH",
        "priority_rank": 25,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        self._last_source_used: str = "none"

    # ------------------------------------------------------------------
    # Network fetchers (small, isolated, retried)
    # ------------------------------------------------------------------

    @retry_on_failure(
        max_attempts=3,
        backoff=2.0,
        retryable_exceptions=(
            ConnectionError, TimeoutError, OSError, requests.RequestException,
        ),
    )
    def _fetch_csv(self) -> str:
        """GET the bulk CSV.  Raises on HTTP error."""
        resp = requests.get(
            JODI_CSV_URL,
            timeout=_REQUEST_TIMEOUT,
            headers={"User-Agent": _USER_AGENT, "Accept": "text/csv,*/*"},
        )
        resp.raise_for_status()
        return resp.text

    @retry_on_failure(
        max_attempts=3,
        backoff=2.0,
        retryable_exceptions=(
            ConnectionError, TimeoutError, OSError, requests.RequestException,
        ),
    )
    def _fetch_sdmx(self) -> dict[str, Any]:
        """GET the SDMX-JSON endpoint.  Raises on HTTP error."""
        resp = requests.get(
            JODI_SDMX_URL,
            timeout=_REQUEST_TIMEOUT,
            headers={
                "User-Agent": _USER_AGENT,
                "Accept": "application/vnd.sdmx.data+json,application/json",
            },
        )
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Pull / save
    # ------------------------------------------------------------------

    def pull(self) -> list[JODIObservation]:
        """Fetch observations.  CSV first, SDMX fallback, never raises.

        Returns an empty list if both upstream sources fail.  The chosen
        source is recorded on ``self._last_source_used``.
        """
        # ---- CSV ----
        try:
            csv_text = self._fetch_csv()
            obs = _parse_jodi_csv(csv_text)
            if obs:
                self._last_source_used = "csv"
                log.info("JODI CSV: {n} observations parsed", n=len(obs))
                return obs
            log.warning("JODI CSV returned 0 tracked observations -- trying SDMX")
        except Exception as exc:
            log.warning("JODI CSV fetch failed: {e}", e=str(exc))

        # ---- SDMX fallback ----
        try:
            payload = self._fetch_sdmx()
            obs = _parse_jodi_sdmx(payload)
            if obs:
                self._last_source_used = "sdmx"
                log.info("JODI SDMX: {n} observations parsed", n=len(obs))
                return obs
            log.warning("JODI SDMX returned 0 tracked observations")
        except Exception as exc:
            log.warning("JODI SDMX fetch failed: {e}", e=str(exc))

        self._last_source_used = "none"
        log.warning("JODI: both CSV and SDMX failed; zero rows fetched")
        return []

    @staticmethod
    def series_id(obs: JODIObservation) -> str:
        """Build the canonical series_id for an observation.

        Format: ``jodi:<COUNTRY>:<PRODUCT>:<FLOW>``.  Stable across releases.
        """
        return f"jodi:{obs.country}:{obs.product}:{obs.flow}"

    def save_to_db(self, observations: list[JODIObservation]) -> int:
        """Upsert observations into ``raw_series``.

        Idempotent — re-running with the same (series_id, month) does not
        create duplicates, because we batch-fetch existing dates per series
        and skip anything we already have at SUCCESS status.

        Returns the number of *new* rows inserted.
        """
        if not observations:
            return 0

        # Group by series_id for efficient existing-date batching
        by_series: dict[str, list[JODIObservation]] = {}
        for obs in observations:
            by_series.setdefault(self.series_id(obs), []).append(obs)

        inserted = 0
        with self.engine.begin() as conn:
            for sid, rows in by_series.items():
                existing = self._get_existing_dates(sid, conn)
                for obs in rows:
                    if obs.month_end in existing:
                        continue
                    self._insert_raw(
                        conn=conn,
                        series_id=sid,
                        obs_date=obs.month_end,
                        value=obs.value,
                        raw_payload={
                            "country": obs.country,
                            "product": obs.product,
                            "flow": obs.flow,
                            "unit": obs.unit,
                            "assessment": obs.assessment,
                            "source": self._last_source_used,
                        },
                    )
                    inserted += 1
        return inserted


# ---------------------------------------------------------------------------
# Scheduler entry point
# ---------------------------------------------------------------------------


def run_jodi_oil_puller(engine: Engine) -> dict[str, Any]:
    """Top-level entry point for the JODI Oil puller.

    Returns a dict with the headline counters used by the Hermes scheduler:

    .. code-block:: python

        {
            "fetched": 12345,
            "inserted": 678,
            "source": "csv" | "sdmx" | "none",
            "countries_seen": ["SAU", "RUS", ...],
            "observations_by_flow": {"PRODUCTION": 4321, ...},
        }
    """
    puller = JODIOilPuller(engine)
    observations = puller.pull()

    countries_seen = sorted({o.country for o in observations})
    observations_by_flow: dict[str, int] = {}
    for o in observations:
        observations_by_flow[o.flow] = observations_by_flow.get(o.flow, 0) + 1

    inserted = 0
    if observations:
        try:
            inserted = puller.save_to_db(observations)
        except Exception as exc:
            log.error("JODI save_to_db failed: {e}", e=str(exc))
            inserted = 0

    return {
        "fetched": len(observations),
        "inserted": inserted,
        "source": puller._last_source_used,
        "countries_seen": countries_seen,
        "observations_by_flow": observations_by_flow,
    }
