"""
GRID container freight ingestion module — CAT-82.

Tracks the two canonical weekly composite container-freight benchmarks:

1. Drewry World Container Index (WCI) — composite USD / 40ft container rate
   plus 8 route-pair sub-indices. The best public read on Western-route
   container pricing power.
2. Shanghai Containerized Freight Index (SCFI) — published Fridays by the
   Shanghai Shipping Exchange; the cleanest Asian-origin read.

Why this matters for GRID:
- Container freight rates spiked ~10x during the 2020-2022 COVID supply
  crisis and have been the earliest-warning signal of every subsequent
  global goods-trade shock (Red Sea / Houthis, Panama drought, Taiwan).
- WCI > SCFI usually means Western-route pressure (demand pull into US/EU).
- SCFI leading WCI usually means Asian supply shift (factory-gate repricing).

Downstream consumers:
- ``intelligence/global_growth_impulse`` classifier — one of the eight
  leading indicators for the global goods cycle.
- ``intelligence/supply_chain_bom_propagator`` — bill-of-materials freight
  cost pass-through for every hardware / CPG issuer with Asian origin.

Data strategy (multi-source fallback walk):
1. **FRED** — try ``IR14270`` and any other container-freight proxy series
   first. Cheapest, most reliable, PIT-safe.
2. **akshare** — ``drewry_wci_index()`` + any SCFI-style function. Falls
   back to HTML scrape if akshare is absent or the function is missing.
3. **HTML scrape** — Drewry WCI page and the SSE SCFI page, parsed with
   BeautifulSoup. Pure helper functions ``_parse_drewry_wci_html`` and
   ``_parse_scfi_html`` are factored out so they can be unit-tested in
   isolation without any network call.

All sources fall through gracefully — if every source fails the puller
returns zero rows and a single warning, never crashing the pipeline.

Series namespaces written to ``raw_series``:
- ``freight:wci_composite_usd``             — Drewry WCI composite (USD/FEU)
- ``freight:scfi_composite``                — SCFI composite index level
- ``freight:wci_route:<route_slug>``        — per-route Drewry sub-indices
  (written only when route-level data is successfully parsed)
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, NamedTuple

import requests
from bs4 import BeautifulSoup
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# ── Public constants ───────────────────────────────────────────────────

DREWRY_WCI_URL: str = (
    "https://www.drewry.co.uk/supply-chain-advisors/"
    "supply-chain-expertise/world-container-index-assessed-by-drewry"
)

SCFI_URL: str = "https://en.sse.net.cn/indices/scfinew.jsp"

# Candidate FRED series to try first. IR14270 is the Drewry proxy that
# sometimes appears in FRED international data releases; the other two
# are harmless probes — any 404 / "not found" is swallowed and we move on.
FRED_CANDIDATE_SERIES: list[str] = [
    "IR14270",  # container freight proxy (Drewry-equivalent)
    "WPU301",   # water transport PPI (containers sub-component)
]


class _Route(NamedTuple):
    """A Drewry WCI composite route pair."""

    label: str
    slug: str


# The 8 Drewry route pairs that make up the WCI composite. Slugs use
# lowercase + underscore so they are safe to append to a series_id.
DREWRY_ROUTES: list[_Route] = [
    _Route("Shanghai-Rotterdam", "shanghai_rotterdam"),
    _Route("Shanghai-LA", "shanghai_la"),
    _Route("Shanghai-Genoa", "shanghai_genoa"),
    _Route("Shanghai-NY", "shanghai_ny"),
    _Route("Rotterdam-Shanghai", "rotterdam_shanghai"),
    _Route("Rotterdam-NY", "rotterdam_ny"),
    _Route("NY-Rotterdam", "ny_rotterdam"),
    _Route("LA-Shanghai", "la_shanghai"),
]

# Series IDs (raw_series.series_id)
SERIES_WCI_COMPOSITE: str = "freight:wci_composite_usd"
SERIES_SCFI_COMPOSITE: str = "freight:scfi_composite"
SERIES_WCI_ROUTE_PREFIX: str = "freight:wci_route:"

_REQUEST_TIMEOUT: int = 30
_USER_AGENT: str = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# Plausible index ranges — bounds for parse sanity checks.
_WCI_MIN: float = 200.0
_WCI_MAX: float = 25_000.0
_SCFI_MIN: float = 200.0
_SCFI_MAX: float = 10_000.0

# Lookup table: label -> slug, case-insensitive, for route parsing.
_ROUTE_LABEL_TO_SLUG: dict[str, str] = {r.label.lower(): r.slug for r in DREWRY_ROUTES}


# ── Dataclass ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ContainerFreightSnapshot:
    """A single weekly freight snapshot.

    ``week_end`` is the observation date (typically a Thursday for WCI,
    a Friday for SCFI). Either composite may be ``None`` when the source
    only provided one index for that week; callers must handle nulls.

    ``wci_routes`` is an immutable-by-convention mapping from the
    canonical route slug (``shanghai_rotterdam``, etc.) to the USD rate
    for that week. Empty dict when no route-level breakdown is
    available.
    """

    week_end: date
    wci_composite_usd: float | None
    scfi_composite: float | None
    wci_routes: dict[str, float] = field(default_factory=dict)


# ── HTML helpers (pure, testable) ──────────────────────────────────────


_NUMBER_RE = re.compile(r"-?\$?[\d,]+(?:\.\d+)?")


def _parse_number(cell: str) -> float | None:
    """Extract a single numeric value from a table cell, tolerating
    ``$`` prefixes, thousand separators, and stray whitespace.

    Returns ``None`` when the cell contains no recoverable number.
    """
    if cell is None:
        return None
    text_val = cell.strip().replace("\xa0", " ")
    if not text_val or text_val in {"-", "—", "N/A", "n/a"}:
        return None
    match = _NUMBER_RE.search(text_val)
    if match is None:
        return None
    raw = match.group(0).replace("$", "").replace(",", "")
    try:
        return float(raw)
    except ValueError:
        return None


def _parse_date_cell(cell: str) -> date | None:
    """Parse a Drewry/SCFI date cell. Accepts several common formats."""
    if not cell:
        return None
    text_val = cell.strip()
    fmts = (
        "%Y-%m-%d",
        "%d %b %y",
        "%d %b %Y",
        "%d-%b-%y",
        "%d-%b-%Y",
        "%b %d, %Y",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y/%m/%d",
    )
    for fmt in fmts:
        try:
            return datetime.strptime(text_val, fmt).date()
        except ValueError:
            continue
    return None


def _parse_drewry_wci_html(html: str) -> list[ContainerFreightSnapshot]:
    """Parse the Drewry WCI public page into snapshots.

    The Drewry page contains a single HTML ``<table>`` whose first column
    is the week-ending date, the next column is the composite index in
    USD/40ft, and subsequent columns are the 8 route pairs. This parser
    locates the first table that has a header row containing "composite"
    (case-insensitive) and returns one snapshot per data row.

    This is a pure function — no network calls. Returns ``[]`` on any
    parse failure rather than raising.
    """
    if not html:
        return []

    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception as exc:  # pragma: no cover — defensive
        log.warning("Drewry WCI: BeautifulSoup init failed: {e}", e=str(exc))
        return []

    target_table = None
    target_headers: list[str] = []
    for table in soup.find_all("table"):
        header_cells = table.find_all("th")
        if not header_cells:
            first_row = table.find("tr")
            if first_row is None:
                continue
            header_cells = first_row.find_all(["th", "td"])
        header_text = [c.get_text(strip=True) for c in header_cells]
        joined = " ".join(header_text).lower()
        if "composite" in joined or "wci" in joined:
            target_table = table
            target_headers = header_text
            break

    if target_table is None:
        return []

    # Map header index -> canonical route slug (or composite)
    col_meta: list[tuple[str, str]] = []  # (kind, key) where kind in {"date","composite","route","skip"}
    # Canonical synonyms — Drewry publishes some headers as "Los Angeles"
    # while others shorten to "LA"; same for "New York" / "NY".
    synonyms = {
        "los angeles": "la",
        "new york": "ny",
        "shanghai": "shanghai",
        "rotterdam": "rotterdam",
        "genoa": "genoa",
    }

    def _tokenise(header_text: str) -> list[str]:
        text_norm = header_text.strip().lower()
        text_norm = text_norm.replace("–", "-").replace("—", "-")
        text_norm = text_norm.replace(" to ", "-")
        for long, short in synonyms.items():
            text_norm = text_norm.replace(long, short)
        # split on dash/comma/whitespace, keep alnum tokens
        pieces = re.split(r"[\s\-,/]+", text_norm)
        return [p for p in pieces if p]

    for idx, header in enumerate(target_headers):
        low = header.strip().lower()
        if idx == 0 or "date" in low or "week" in low:
            col_meta.append(("date", ""))
            continue
        if "composite" in low or low == "wci":
            col_meta.append(("composite", ""))
            continue

        tokens = _tokenise(header)
        slug_match: str | None = None
        # Strict ordered match: the header tokens must begin with the
        # exact token sequence of the route label. This preserves
        # directionality (Shanghai->Rotterdam is distinct from the
        # reverse).
        for r in DREWRY_ROUTES:
            want = _tokenise(r.label)
            if len(want) >= 2 and tokens[: len(want)] == want:
                slug_match = r.slug
                break
        if slug_match is not None:
            col_meta.append(("route", slug_match))
        else:
            col_meta.append(("skip", ""))

    # Iterate body rows
    snapshots: list[ContainerFreightSnapshot] = []
    body_rows = target_table.find_all("tr")
    for row in body_rows:
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        values = [c.get_text(strip=True) for c in cells]
        # Skip header row (already captured above)
        if values == target_headers:
            continue

        wk_end: date | None = None
        composite: float | None = None
        routes: dict[str, float] = {}

        for idx, cell in enumerate(values):
            if idx >= len(col_meta):
                break
            kind, key = col_meta[idx]
            if kind == "date":
                wk_end = _parse_date_cell(cell)
            elif kind == "composite":
                composite = _parse_number(cell)
            elif kind == "route":
                parsed = _parse_number(cell)
                if parsed is not None:
                    routes[key] = parsed

        if wk_end is None:
            continue
        if composite is not None and not (_WCI_MIN <= composite <= _WCI_MAX):
            # Implausible — skip this row rather than poison the series.
            composite = None

        snapshots.append(
            ContainerFreightSnapshot(
                week_end=wk_end,
                wci_composite_usd=composite,
                scfi_composite=None,
                wci_routes=routes,
            )
        )

    return snapshots


def _parse_scfi_html(html: str) -> list[ContainerFreightSnapshot]:
    """Parse the Shanghai Shipping Exchange SCFI page into snapshots.

    The SSE page publishes a simple HTML table with columns for the
    reporting date and the composite SCFI index. We accept any table
    whose header row contains "SCFI" or "comprehensive" and grab the
    date + composite columns only. Pure function; returns ``[]`` on
    any parse failure.
    """
    if not html:
        return []

    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception as exc:  # pragma: no cover — defensive
        log.warning("SCFI: BeautifulSoup init failed: {e}", e=str(exc))
        return []

    target_table = None
    for table in soup.find_all("table"):
        header_text = " ".join(
            c.get_text(strip=True)
            for c in table.find_all(["th", "td"], limit=12)
        ).lower()
        if "scfi" in header_text or "comprehensive" in header_text:
            target_table = table
            break

    if target_table is None:
        return []

    snapshots: list[ContainerFreightSnapshot] = []
    for row in target_table.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        values = [c.get_text(strip=True) for c in cells]

        wk_end: date | None = None
        composite: float | None = None
        for cell in values:
            if wk_end is None:
                d = _parse_date_cell(cell)
                if d is not None:
                    wk_end = d
                    continue
            if composite is None:
                parsed = _parse_number(cell)
                if parsed is not None and _SCFI_MIN <= parsed <= _SCFI_MAX:
                    composite = parsed
                    break

        if wk_end is None or composite is None:
            continue

        snapshots.append(
            ContainerFreightSnapshot(
                week_end=wk_end,
                wci_composite_usd=None,
                scfi_composite=composite,
                wci_routes={},
            )
        )

    return snapshots


# ── Source fallback helpers ────────────────────────────────────────────


def _http_get(url: str) -> str | None:
    """Minimal HTTP GET wrapper. Returns page text or None on failure.

    Kept as a top-level function so tests can patch it cleanly.
    """
    headers = {
        "User-Agent": _USER_AGENT,
        "Accept": "text/html,application/xhtml+xml",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as exc:
        log.warning("HTTP GET failed for {u}: {e}", u=url, e=str(exc))
        return None


def _try_fred_sources() -> list[ContainerFreightSnapshot]:
    """Try the FRED candidate series. Returns ``[]`` on any failure.

    FRED is preferred because it is PIT-safe and already integrated into
    the GRID stack via ``fedfred``.
    """
    try:
        from fedfred import FredAPI  # type: ignore
    except ImportError:
        return []

    try:
        from config import settings  # type: ignore
        api_key = getattr(settings, "FRED_API_KEY", "") or ""
    except Exception:
        api_key = ""

    if not api_key:
        return []

    snapshots: list[ContainerFreightSnapshot] = []
    try:
        fred = FredAPI(api_key)
    except Exception as exc:
        log.warning("FRED init failed: {e}", e=str(exc))
        return []

    for fred_id in FRED_CANDIDATE_SERIES:
        try:
            data = fred.get_series_observations(fred_id)
        except Exception as exc:
            log.debug("FRED {f} miss: {e}", f=fred_id, e=str(exc))
            continue
        if data is None or getattr(data, "empty", True):
            continue

        rows = data.to_dict(orient="records") if hasattr(data, "to_dict") else []
        for rec in rows:
            raw_date = rec.get("date") or rec.get("observation_date")
            raw_val = rec.get("value")
            if raw_date is None or raw_val is None or raw_val == ".":
                continue
            try:
                wk_end = (
                    raw_date.date()
                    if hasattr(raw_date, "date") and callable(raw_date.date)
                    else date.fromisoformat(str(raw_date)[:10])
                )
                val = float(raw_val)
            except (ValueError, TypeError):
                continue
            snapshots.append(
                ContainerFreightSnapshot(
                    week_end=wk_end,
                    wci_composite_usd=val,
                    scfi_composite=None,
                    wci_routes={},
                )
            )
        if snapshots:
            return snapshots

    return snapshots


def _try_akshare_sources() -> list[ContainerFreightSnapshot]:
    """Try akshare — prefer native Drewry/SCFI helpers. Returns ``[]``
    if akshare is not installed or no compatible function exists."""
    try:
        import akshare as ak  # type: ignore
    except ImportError:
        return []

    snapshots_by_date: dict[date, dict[str, Any]] = {}

    # Drewry WCI
    drewry_fn = getattr(ak, "drewry_wci_index", None)
    if drewry_fn is not None:
        try:
            df = drewry_fn()
            _merge_akshare_frame(df, snapshots_by_date, kind="wci")
        except Exception as exc:
            log.debug("akshare drewry_wci_index failed: {e}", e=str(exc))

    # SCFI — the exact function name is inconsistent across akshare
    # versions. Accept any function whose name contains "scfi" or
    # "freight_scfi".
    for candidate in ("freight_scfi", "macro_china_freight_index", "freight_scfi_index"):
        scfi_fn = getattr(ak, candidate, None)
        if scfi_fn is None:
            continue
        try:
            df = scfi_fn()
            _merge_akshare_frame(df, snapshots_by_date, kind="scfi")
            break
        except Exception as exc:
            log.debug("akshare {c} failed: {e}", c=candidate, e=str(exc))

    return [
        ContainerFreightSnapshot(
            week_end=d,
            wci_composite_usd=row.get("wci"),
            scfi_composite=row.get("scfi"),
            wci_routes=row.get("routes", {}),
        )
        for d, row in sorted(snapshots_by_date.items())
    ]


def _merge_akshare_frame(
    df: Any,
    bucket: dict[date, dict[str, Any]],
    kind: str,
) -> None:
    """Merge a pandas-style frame returned by akshare into a by-date
    bucket. Tolerant of wildly different column names.
    """
    if df is None:
        return
    try:
        records = df.to_dict(orient="records")
    except Exception:
        return

    for rec in records:
        wk_end: date | None = None
        val: float | None = None
        for key, value in rec.items():
            low = str(key).lower()
            if wk_end is None and ("date" in low or "日期" in low or "week" in low):
                try:
                    wk_end = (
                        value.date()
                        if hasattr(value, "date") and callable(value.date)
                        else date.fromisoformat(str(value)[:10])
                    )
                except (ValueError, TypeError):
                    continue
            elif val is None and any(tok in low for tok in ("value", "index", "price", "composite", "指数")):
                try:
                    val = float(value)
                except (TypeError, ValueError):
                    continue
        if wk_end is None or val is None:
            continue
        entry = bucket.setdefault(wk_end, {"routes": {}})
        if kind == "wci":
            entry["wci"] = val
        elif kind == "scfi":
            entry["scfi"] = val


def _try_html_sources() -> list[ContainerFreightSnapshot]:
    """Scrape Drewry + SCFI public pages. Merges by week_end. Empty list
    on total failure.
    """
    merged: dict[date, ContainerFreightSnapshot] = {}

    drewry_html = _http_get(DREWRY_WCI_URL)
    if drewry_html:
        for snap in _parse_drewry_wci_html(drewry_html):
            merged[snap.week_end] = snap

    scfi_html = _http_get(SCFI_URL)
    if scfi_html:
        for snap in _parse_scfi_html(scfi_html):
            existing = merged.get(snap.week_end)
            if existing is None:
                merged[snap.week_end] = snap
            else:
                merged[snap.week_end] = ContainerFreightSnapshot(
                    week_end=existing.week_end,
                    wci_composite_usd=existing.wci_composite_usd,
                    scfi_composite=snap.scfi_composite,
                    wci_routes=existing.wci_routes,
                )

    return [merged[d] for d in sorted(merged.keys())]


# ── Puller class ───────────────────────────────────────────────────────


class ContainerFreightPuller(BasePuller):
    """Multi-source container-freight puller (CAT-82).

    Fall-through order: FRED → akshare → HTML scrape. The first source
    that returns a non-empty snapshot list wins; the other sources act
    as redundancy. All failures are logged and swallowed so the pipeline
    never crashes on a dead upstream.
    """

    SOURCE_NAME: str = "container_freight"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": DREWRY_WCI_URL,
        "cost_tier": "FREE",
        "latency_class": "WEEKLY",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 32,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        self.last_source: str = "none"
        log.info(
            "ContainerFreightPuller initialised — source_id={sid}",
            sid=self.source_id,
        )

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def pull(self) -> list[ContainerFreightSnapshot]:
        """Run the fallback walk and return the first non-empty result.

        Sets ``self.last_source`` to the name of the winning source, or
        ``"none"`` when every source failed.
        """
        fred_snaps = _try_fred_sources()
        if fred_snaps:
            self.last_source = "fred"
            log.info(
                "container_freight: FRED returned {n} snapshots",
                n=len(fred_snaps),
            )
            return fred_snaps

        ak_snaps = _try_akshare_sources()
        if ak_snaps:
            self.last_source = "akshare"
            log.info(
                "container_freight: akshare returned {n} snapshots",
                n=len(ak_snaps),
            )
            return ak_snaps

        html_snaps = _try_html_sources()
        if html_snaps:
            self.last_source = "html"
            log.info(
                "container_freight: HTML scrape returned {n} snapshots",
                n=len(html_snaps),
            )
            return html_snaps

        self.last_source = "none"
        log.warning(
            "container_freight: every source failed — 0 rows pulled"
        )
        return []

    def save_to_db(
        self,
        snapshots: list[ContainerFreightSnapshot],
    ) -> int:
        """Upsert snapshots into raw_series. Returns rows inserted.

        Idempotent — any ``(series_id, obs_date)`` already present in
        ``raw_series`` with a SUCCESS status is skipped.
        """
        if not snapshots:
            return 0

        inserted = 0
        with self.engine.begin() as conn:
            # Batch existing-date caches per series, so each run is one
            # lookup per series regardless of how many rows we write.
            existing_by_series: dict[str, set[date]] = {}

            def _existing(series_id: str) -> set[date]:
                cache = existing_by_series.get(series_id)
                if cache is None:
                    cache = self._get_existing_dates(series_id, conn)
                    existing_by_series[series_id] = cache
                return cache

            for snap in snapshots:
                payload_base = {
                    "source": self.last_source,
                    "task": "CAT-82",
                    "description": "Container freight composite",
                }

                if snap.wci_composite_usd is not None:
                    if snap.week_end not in _existing(SERIES_WCI_COMPOSITE):
                        self._insert_raw(
                            conn=conn,
                            series_id=SERIES_WCI_COMPOSITE,
                            obs_date=snap.week_end,
                            value=float(snap.wci_composite_usd),
                            raw_payload={
                                **payload_base,
                                "unit": "USD/FEU",
                                "series": "Drewry_WCI_composite",
                            },
                        )
                        existing_by_series[SERIES_WCI_COMPOSITE].add(snap.week_end)
                        inserted += 1

                if snap.scfi_composite is not None:
                    if snap.week_end not in _existing(SERIES_SCFI_COMPOSITE):
                        self._insert_raw(
                            conn=conn,
                            series_id=SERIES_SCFI_COMPOSITE,
                            obs_date=snap.week_end,
                            value=float(snap.scfi_composite),
                            raw_payload={
                                **payload_base,
                                "unit": "index",
                                "series": "SCFI_composite",
                            },
                        )
                        existing_by_series[SERIES_SCFI_COMPOSITE].add(snap.week_end)
                        inserted += 1

                for slug, rate in snap.wci_routes.items():
                    if rate is None:
                        continue
                    series_id = f"{SERIES_WCI_ROUTE_PREFIX}{slug}"
                    if snap.week_end in _existing(series_id):
                        continue
                    self._insert_raw(
                        conn=conn,
                        series_id=series_id,
                        obs_date=snap.week_end,
                        value=float(rate),
                        raw_payload={
                            **payload_base,
                            "unit": "USD/FEU",
                            "series": "Drewry_WCI_route",
                            "route_slug": slug,
                        },
                    )
                    existing_by_series[series_id].add(snap.week_end)
                    inserted += 1

        return inserted


# ── Module-level entry point ───────────────────────────────────────────


def run_container_freight_puller(engine: Engine) -> dict[str, Any]:
    """Entry point for the scheduler.

    Returns a small result dict with:
      - ``fetched``: number of snapshots pulled from the winning source
      - ``inserted``: rows actually written to ``raw_series``
      - ``source``:  ``"fred" | "akshare" | "html" | "none"``

    Never raises — any exception is caught, logged, and mapped to a
    zero-row result with ``source="none"``.
    """
    try:
        puller = ContainerFreightPuller(db_engine=engine)
        snapshots = puller.pull()
        inserted = puller.save_to_db(snapshots)
        return {
            "fetched": len(snapshots),
            "inserted": inserted,
            "source": puller.last_source,
        }
    except Exception as exc:  # pragma: no cover — defensive
        log.error("container_freight puller crashed: {e}", e=str(exc))
        return {"fetched": 0, "inserted": 0, "source": "none"}


if __name__ == "__main__":  # pragma: no cover
    from db import get_engine

    result = run_container_freight_puller(get_engine())
    print(json.dumps(result, indent=2))
