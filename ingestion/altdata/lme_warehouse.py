"""
LME warehouse stocks + cancelled warrant ratio puller (CAT-51, P0, Tier A).

Tracks daily London Metal Exchange warehouse stock levels for the six LME
base metals — copper, aluminum, zinc, nickel, lead, tin — plus the
"cancelled warrant" ratio. When a warehouse customer cancels a warrant,
the metal is being scheduled for physical delivery OUT of the warehouse.

The cancelled-warrant ratio (cancelled / total) is the sharpest read on
imminent physical-market tightness: spikes lead copper, aluminum, zinc
and nickel price moves by ~5-15 days because they force restockers,
merchants and consumer-industry buyers to chase physical metal.

Feeds:
  - intelligence/sector_networks/commodities_agriculture leaf (physical
    tightness signal alongside LME price series from FRED)
  - intelligence/supply_chain_chokepoint classifier — warehouse draws
    are upstream evidence for a chokepoint building

Data strategy (tried in order, graceful fallback):
  1. LME JSON/API endpoint (probe). At the time of writing the public
     warehouse-stocks page at https://www.lme.com/en/Market-Data/
     Reports-and-data/Warehouse-stocks-report loads its data via an
     asynchronous JSON payload; the exact URL is subject to change and
     has not been confirmed by a network capture. We document the
     endpoint as a *probe* and fall back to HTML scraping whenever the
     JSON path returns non-JSON, an error, or an unrecognised shape.
  2. Public HTML warehouse-stocks-report page scraped with BeautifulSoup.
     This is the production path until the JSON endpoint is confirmed.
  3. Zero rows + warning (never crash the scheduler).

Neither FRED nor akshare is a reliable backup for LME warehouse stocks
specifically — FRED only carries LME price series (``PCOPPUSDM`` etc.)
and akshare's ``futures_inventory_em`` / ``futures_inventory_99`` cover
Chinese exchanges (SHFE/DCE/CZCE), not LME. CAT-51 is therefore a
single-source pipeline with an HTML-first production path.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterable

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover - bs4 is a project dependency
    BeautifulSoup = None  # type: ignore[assignment]

from ingestion.base import BasePuller

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LME_METALS: tuple[str, ...] = (
    "copper",
    "aluminum",
    "zinc",
    "nickel",
    "lead",
    "tin",
)

# NOTE: the LME JSON endpoint is a *probe* — the real URL used by the live
# warehouse-stocks-report page has not been confirmed by a network capture.
# If this probe returns non-JSON, the puller automatically falls back to
# HTML scraping (the production path).
LME_API_URL: str = "https://www.lme.com/api/warehousestocks"

LME_REPORT_URL: str = (
    "https://www.lme.com/en/Market-Data/Reports-and-data/Warehouse-stocks-report"
)

_REQUEST_TIMEOUT: int = 30
_USER_AGENT: str = "GRID/4.0 (research; stepdadfinance@gmail.com)"

# Metal header synonym map — all keys are lower-cased, no whitespace.
# The HTML parser calls ``_canonical_metal`` which strips punctuation and
# looks the result up here. Unknown headers are skipped (logged).
_METAL_SYNONYMS: dict[str, str] = {
    # Copper
    "copper": "copper",
    "cu": "copper",
    "ca": "copper",  # LME ring code
    # Aluminum / aluminium (both spellings)
    "aluminum": "aluminum",
    "aluminium": "aluminum",
    "al": "aluminum",
    "ah": "aluminum",  # LME ring code (primary aluminium)
    # Zinc
    "zinc": "zinc",
    "zn": "zinc",
    "zs": "zinc",
    # Nickel
    "nickel": "nickel",
    "ni": "nickel",
    # Lead
    "lead": "lead",
    "pb": "lead",
    # Tin
    "tin": "tin",
    "sn": "tin",
}

# Column header keywords used to identify which column holds total stocks
# vs cancelled warrants in the HTML table. Keys are lower-cased substrings.
_TOTAL_COLUMN_KEYWORDS: tuple[str, ...] = (
    "total",
    "stocks",
    "on warrant",
    "on-warrant",
    "live + cancelled",
)
_CANCELLED_COLUMN_KEYWORDS: tuple[str, ...] = (
    "cancelled",
    "canceled",
    "cancelled warrant",
)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LMEStockSnapshot:
    """A single metal's warehouse state on a single day.

    Attributes:
        date: Report observation date.
        metal: Canonical metal name (one of ``LME_METALS``).
        total_stocks_mt: Total LME warehouse stocks in metric tonnes.
        cancelled_warrants_mt: Cancelled-warrant stocks in metric tonnes.
        live_stocks_mt: Live (on-warrant) stocks = total − cancelled.
        cancelled_ratio: cancelled / total, clamped to [0, 1].
    """

    date: date
    metal: str
    total_stocks_mt: float
    cancelled_warrants_mt: float
    live_stocks_mt: float
    cancelled_ratio: float


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def compute_cancelled_ratio(total: float, cancelled: float) -> float:
    """Return the cancelled / total ratio clamped to ``[0, 1]``.

    Parameters:
        total: Total LME warehouse stocks (metric tonnes).
        cancelled: Cancelled-warrant stocks (metric tonnes).

    Returns:
        ``0.0`` when ``total <= 0``, else ``cancelled / total`` clamped
        to the closed interval ``[0.0, 1.0]``.
    """
    try:
        total_f = float(total)
        cancelled_f = float(cancelled)
    except (TypeError, ValueError):
        return 0.0

    if total_f <= 0:
        return 0.0

    ratio = cancelled_f / total_f
    if ratio < 0.0:
        return 0.0
    if ratio > 1.0:
        return 1.0
    return ratio


def _canonical_metal(header: str) -> str | None:
    """Resolve a free-form header string to a canonical metal name.

    Strips punctuation, whitespace and parenthesised annotations, then
    looks the result up in the synonym map. Also tries the first token
    (so ``"Copper (Cu)"`` → ``"copper"``).

    Returns:
        Canonical metal name, or ``None`` if no match.
    """
    if not header:
        return None
    cleaned = header.strip().lower()
    # Quick direct hit
    if cleaned in _METAL_SYNONYMS:
        return _METAL_SYNONYMS[cleaned]

    # Strip parenthesised section: "Copper (Cu)" → "copper"
    no_parens = re.sub(r"\([^)]*\)", "", cleaned).strip()
    if no_parens in _METAL_SYNONYMS:
        return _METAL_SYNONYMS[no_parens]

    # Try each token
    tokens = re.split(r"[^a-z]+", cleaned)
    for tok in tokens:
        if tok and tok in _METAL_SYNONYMS:
            return _METAL_SYNONYMS[tok]
    return None


def _parse_numeric_cell(cell: str) -> float | None:
    """Parse a numeric cell from an LME HTML table.

    Handles:
      - commas as thousands separators: ``"1,234,567"``
      - em-dash / en-dash / hyphen as "no data": ``"—"``, ``"–"``, ``"-"``
      - ``"N/A"``, ``""``, ``"null"``
      - leading / trailing whitespace

    Returns:
        float value, or ``None`` when the cell is missing / unparsable.
    """
    if cell is None:
        return None
    txt = str(cell).strip()
    if not txt:
        return None
    # Missing markers
    if txt in {"—", "–", "-", "N/A", "n/a", "NA", "null", "None"}:
        return None
    # Strip commas + any stray whitespace
    cleaned = txt.replace(",", "").replace(" ", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_date_from_header(text_block: str, fallback: date) -> date:
    """Best-effort date extraction from an LME report header.

    Tries ISO (``2026-04-13``), LME day-month-year (``13 April 2026``),
    and falls back to ``fallback`` (usually ``date.today()``).
    """
    if not text_block:
        return fallback
    # ISO
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", text_block)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass
    # "13 April 2026" / "13-Apr-2026"
    m = re.search(
        r"(\d{1,2})[\s\-/]+"
        r"(January|February|March|April|May|June|July|August|"
        r"September|October|November|December|"
        r"Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
        r"[\s\-/]+(\d{4})",
        text_block,
        flags=re.IGNORECASE,
    )
    if m:
        for fmt in ("%d %B %Y", "%d %b %Y"):
            try:
                return datetime.strptime(
                    f"{m.group(1)} {m.group(2)} {m.group(3)}", fmt
                ).date()
            except ValueError:
                continue
    return fallback


def _build_snapshot(
    obs_date: date,
    metal: str,
    total: float,
    cancelled: float,
) -> LMEStockSnapshot:
    """Construct an ``LMEStockSnapshot`` with derived fields."""
    total_safe = max(float(total), 0.0)
    cancelled_safe = max(float(cancelled), 0.0)
    # Cancelled cannot exceed total in reality — clamp defensively.
    if cancelled_safe > total_safe:
        cancelled_safe = total_safe
    live = total_safe - cancelled_safe
    ratio = compute_cancelled_ratio(total_safe, cancelled_safe)
    return LMEStockSnapshot(
        date=obs_date,
        metal=metal,
        total_stocks_mt=total_safe,
        cancelled_warrants_mt=cancelled_safe,
        live_stocks_mt=live,
        cancelled_ratio=ratio,
    )


def _parse_lme_html(html: str) -> list[LMEStockSnapshot]:
    """Parse the LME warehouse-stocks HTML page into snapshots.

    The parser walks every ``<table>`` in the document, identifies the
    metal column (left-most cell per data row) and the total / cancelled
    columns (from header labels), then emits one snapshot per recognised
    metal. Header labels like ``"Copper"``, ``"Cu"``, ``"CA"``,
    ``"Copper (Cu)"`` are all mapped via the synonym table.

    This function is intentionally pure (no network, no DB) so it can
    be unit-tested against canned HTML.

    Returns:
        List of snapshots (may be empty — empty input is not an error).
    """
    if not html or BeautifulSoup is None:
        return []

    soup = BeautifulSoup(html, "html.parser")

    # Date: best-effort from anywhere in the document.
    obs_date = _parse_date_from_header(soup.get_text(" ", strip=True), date.today())

    snapshots: list[LMEStockSnapshot] = []
    seen_metals: set[str] = set()

    for table in soup.find_all("table"):
        header_cells = _extract_header_cells(table)
        total_idx = _find_column_index(header_cells, _TOTAL_COLUMN_KEYWORDS)
        cancelled_idx = _find_column_index(header_cells, _CANCELLED_COLUMN_KEYWORDS)

        rows = table.find_all("tr")
        for tr in rows:
            cells = tr.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            metal_label = cells[0].get_text(" ", strip=True)
            metal = _canonical_metal(metal_label)
            if metal is None or metal in seen_metals:
                continue

            total_val = _value_from_row(cells, total_idx, fallback_index=1)
            cancelled_val = _value_from_row(cells, cancelled_idx, fallback_index=2)

            if total_val is None and cancelled_val is None:
                continue

            snapshots.append(
                _build_snapshot(
                    obs_date=obs_date,
                    metal=metal,
                    total=total_val or 0.0,
                    cancelled=cancelled_val or 0.0,
                )
            )
            seen_metals.add(metal)

    return snapshots


def _extract_header_cells(table: Any) -> list[str]:
    """Return lower-cased header labels for a table, empty list if none."""
    thead = table.find("thead")
    if thead is not None:
        header_row = thead.find("tr")
    else:
        header_row = table.find("tr")
    if header_row is None:
        return []
    return [c.get_text(" ", strip=True).lower() for c in header_row.find_all(["th", "td"])]


def _find_column_index(
    headers: list[str], keywords: tuple[str, ...]
) -> int | None:
    """Return the index of the first header cell whose text contains any keyword."""
    for i, h in enumerate(headers):
        for kw in keywords:
            if kw in h:
                return i
    return None


def _value_from_row(
    cells: list[Any],
    col_index: int | None,
    fallback_index: int,
) -> float | None:
    """Extract a numeric value from a row, preferring ``col_index`` over fallback."""
    for idx in (col_index, fallback_index):
        if idx is None:
            continue
        if 0 <= idx < len(cells):
            val = _parse_numeric_cell(cells[idx].get_text(" ", strip=True))
            if val is not None:
                return val
    return None


def _parse_lme_json(payload: Any) -> list[LMEStockSnapshot]:
    """Parse a canned LME JSON payload into snapshots.

    Assumed shape (documented — probe-only, not confirmed by live capture):

        {
          "report_date": "2026-04-13",
          "metals": [
            {
              "metal": "copper" | "Cu" | "Copper",
              "total_stocks": 123456,
              "cancelled_warrants": 45678
            },
            ...
          ]
        }

    Also tolerates a flat list of metal dicts, and alternate field
    names: ``name`` / ``code`` for the metal; ``total`` / ``live`` /
    ``on_warrant`` / ``cancelled`` / ``cancelled_mt`` for the numerics.
    """
    if not payload:
        return []

    if isinstance(payload, dict):
        report_date_raw = payload.get("report_date") or payload.get("date")
        entries: Iterable[Any] = payload.get("metals") or payload.get("data") or []
    elif isinstance(payload, list):
        report_date_raw = None
        entries = payload
    else:
        return []

    obs_date = _coerce_date(report_date_raw)

    snapshots: list[LMEStockSnapshot] = []
    seen: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        label = (
            entry.get("metal")
            or entry.get("name")
            or entry.get("code")
            or ""
        )
        metal = _canonical_metal(str(label))
        if metal is None or metal in seen:
            continue

        total = _coerce_float(
            entry.get("total_stocks"),
            entry.get("total"),
            entry.get("on_warrant"),
            entry.get("stocks"),
        )
        cancelled = _coerce_float(
            entry.get("cancelled_warrants"),
            entry.get("cancelled"),
            entry.get("cancelled_mt"),
        )
        if total is None and cancelled is None:
            continue

        snapshots.append(
            _build_snapshot(
                obs_date=obs_date,
                metal=metal,
                total=total or 0.0,
                cancelled=cancelled or 0.0,
            )
        )
        seen.add(metal)
    return snapshots


def _coerce_date(value: Any) -> date:
    """Coerce an arbitrary value into a ``date`` (falls back to today)."""
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value).date()
        except ValueError:
            return _parse_date_from_header(value, date.today())
    return date.today()


def _coerce_float(*candidates: Any) -> float | None:
    """Return the first candidate that can be coerced into a float."""
    for c in candidates:
        if c is None:
            continue
        try:
            return float(c)
        except (TypeError, ValueError):
            continue
    return None


# ---------------------------------------------------------------------------
# Puller
# ---------------------------------------------------------------------------


class LMEWarehousePuller(BasePuller):
    """Pull daily LME warehouse stocks + cancelled warrant ratio.

    Writes four series per metal into ``raw_series``:
      - ``lme:stocks_total_mt:<metal>``
      - ``lme:stocks_cancelled_mt:<metal>``
      - ``lme:stocks_live_mt:<metal>``
      - ``lme:cancelled_ratio:<metal>``
    """

    SOURCE_NAME: str = "lme_warehouse"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://www.lme.com/",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 20,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        self._last_source: str = "none"

    # -- fetch -------------------------------------------------------------

    def _fetch_json(self) -> list[LMEStockSnapshot]:
        """Try the JSON probe. Returns [] on any failure (never raises)."""
        try:
            resp = requests.get(
                LME_API_URL,
                timeout=_REQUEST_TIMEOUT,
                headers={"User-Agent": _USER_AGENT, "Accept": "application/json"},
            )
            resp.raise_for_status()
            payload = resp.json()
        except ValueError:
            log.warning("LME JSON probe returned non-JSON payload")
            return []
        except requests.RequestException as exc:
            log.warning("LME JSON probe failed: {e}", e=str(exc))
            return []
        except Exception as exc:  # defensive
            log.warning("LME JSON probe unexpected error: {e}", e=str(exc))
            return []

        return _parse_lme_json(payload)

    def _fetch_html(self) -> list[LMEStockSnapshot]:
        """Fetch + scrape the public warehouse-stocks-report page."""
        try:
            resp = requests.get(
                LME_REPORT_URL,
                timeout=_REQUEST_TIMEOUT,
                headers={"User-Agent": _USER_AGENT},
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            log.warning("LME HTML fetch failed: {e}", e=str(exc))
            return []
        except Exception as exc:  # defensive
            log.warning("LME HTML fetch unexpected error: {e}", e=str(exc))
            return []

        return _parse_lme_html(resp.text)

    def pull(self) -> list[LMEStockSnapshot]:
        """JSON-first, HTML-fallback walk.

        Returns:
            Snapshots (possibly empty). The string used for the chosen
            path is stashed on ``self._last_source`` for reporting.
        """
        snapshots = self._fetch_json()
        if snapshots:
            self._last_source = "json"
            log.info("LME: {n} metals parsed from JSON", n=len(snapshots))
            return snapshots

        snapshots = self._fetch_html()
        if snapshots:
            self._last_source = "html"
            log.info("LME: {n} metals parsed from HTML", n=len(snapshots))
            return snapshots

        self._last_source = "none"
        log.warning("LME: all fetch paths failed — returning zero snapshots")
        return []

    # -- save --------------------------------------------------------------

    def save_to_db(self, snapshots: list[LMEStockSnapshot]) -> int:
        """Upsert snapshots into raw_series. Returns rows inserted."""
        if not snapshots:
            return 0

        inserted = 0
        with self.engine.begin() as conn:
            for snap in snapshots:
                series_values: tuple[tuple[str, float], ...] = (
                    (f"lme:stocks_total_mt:{snap.metal}", snap.total_stocks_mt),
                    (f"lme:stocks_cancelled_mt:{snap.metal}", snap.cancelled_warrants_mt),
                    (f"lme:stocks_live_mt:{snap.metal}", snap.live_stocks_mt),
                    (f"lme:cancelled_ratio:{snap.metal}", snap.cancelled_ratio),
                )
                for series_id, value in series_values:
                    if self._row_exists(series_id, snap.date, conn, dedup_hours=24 * 7):
                        continue
                    payload = {
                        "metal": snap.metal,
                        "total_mt": snap.total_stocks_mt,
                        "cancelled_mt": snap.cancelled_warrants_mt,
                        "live_mt": snap.live_stocks_mt,
                        "ratio": snap.cancelled_ratio,
                        "source_path": self._last_source,
                    }
                    conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, "
                            "raw_payload, pull_status) "
                            "VALUES (:sid, :src, :od, :val, :payload, 'SUCCESS')"
                        ),
                        {
                            "sid": series_id,
                            "src": self.source_id,
                            "od": snap.date,
                            "val": float(value),
                            "payload": json.dumps(payload),
                        },
                    )
                    inserted += 1
        return inserted


# ---------------------------------------------------------------------------
# Top-level entry point
# ---------------------------------------------------------------------------


def run_lme_warehouse_puller(engine: Engine) -> dict[str, Any]:
    """Run a full LME warehouse pull cycle.

    Returns:
        dict with keys: ``fetched``, ``inserted``, ``source``,
        ``metals``.
    """
    puller = LMEWarehousePuller(db_engine=engine)
    snapshots = puller.pull()
    inserted = puller.save_to_db(snapshots)

    metals: dict[str, dict[str, float]] = {}
    for snap in snapshots:
        metals[snap.metal] = {
            "total_mt": snap.total_stocks_mt,
            "cancelled_mt": snap.cancelled_warrants_mt,
            "live_mt": snap.live_stocks_mt,
            "cancelled_ratio": snap.cancelled_ratio,
        }

    return {
        "fetched": len(snapshots),
        "inserted": inserted,
        "source": puller._last_source,
        "metals": metals,
    }
