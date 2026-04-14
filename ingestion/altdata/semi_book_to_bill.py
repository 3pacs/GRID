"""
GRID SEMI North American Semiconductor Equipment Book-to-Bill puller (CAT-89).

Pulls the monthly SEMI North American Semiconductor Equipment Billings
Report. The report exposes three numbers per month:

    - bookings   (USD, millions) — new orders placed with NA equipment makers
    - billings   (USD, millions) — shipments actually invoiced in the month
    - ratio      = bookings / billings  (a.k.a. "book-to-bill")

The ratio is the canonical leading indicator for the semi-equipment cycle:

    * ratio > 1.05 sustained   → capex up-cycle (bullish SOX, SMH, NVDA,
                                  AMAT, LRCX, KLAC, ASML, TSM capex)
    * ratio ~ 1.00             → steady-state replacement demand
    * ratio < 0.95 sustained   → capex down-cycle rolling over

Because SEMI publishes the data roughly 3 weeks after month-end, this
module is scheduled monthly (not daily). The release page is:

    https://www.semi.org/en/products-services/market-data/equipment/north-american-billings-report

Data-fetch strategy (primary → fallback):
    1. FRED — the SEMI series are mirrored on FRED under a pair of IDs
       (one for bookings, one for billings). If ``FRED_API_KEY`` is set
       we fetch via the FRED REST API and compute the ratio locally.
    2. HTML scrape of the SEMI press-release page with BeautifulSoup as
       a graceful-degradation fallback. Used when FRED is unreachable,
       has no key, or returns an empty series.

Both paths store into ``raw_series`` under three series IDs:

    * ``semi:bookings``  — USD, millions
    * ``semi:billings``  — USD, millions
    * ``semi:ratio``     — dimensionless book-to-bill ratio

Task: CAT-89 (P0 — Tier A, monthly cadence).
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable

import requests
from bs4 import BeautifulSoup
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# ─────────────────────────────────────────────────────────────────────
# Module-level constants
# ─────────────────────────────────────────────────────────────────────

#: Monthly cadence — SEMI publishes ~3 weeks after month-end.
CADENCE: str = "monthly"

#: Public release URL. Kept as documentation and used by the HTML fallback.
SEMI_RELEASE_URL: str = (
    "https://www.semi.org/en/products-services/market-data/equipment/"
    "north-american-billings-report"
)

#: FRED REST observations endpoint (shared by bookings + billings fetches).
FRED_OBSERVATIONS_URL: str = (
    "https://api.stlouisfed.org/fred/series/observations"
)

#: FRED series IDs used as the primary data path.
#:
#: Note: SEMI syndicates two series that semantically correspond to
#: bookings and billings for North American semiconductor equipment.
#: The IDs below are the best-known FRED mirrors at time of writing;
#: if FRED returns an empty payload the puller transparently falls back
#: to scraping the SEMI press-release page.
FRED_SERIES_IDS: dict[str, str] = {
    "bookings": "SEMIBBILL",   # SEMI NA semi equipment bookings ($M)
    "billings": "SEMIBSHIP",   # SEMI NA semi equipment billings ($M)
}

#: HTTP timeout for FRED / SEMI fetches (seconds).
_REQUEST_TIMEOUT: int = 30

#: User-Agent used for HTML scraping fallback.
_USER_AGENT: str = "GRID-DataPuller/1.0 (CAT-89 semi_book_to_bill)"

#: raw_series labels written by this puller (kept in one place so tests
#: and the scheduler can import them without re-hardcoding).
SERIES_LABELS: tuple[str, ...] = ("bookings", "billings", "ratio")


# ─────────────────────────────────────────────────────────────────────
# Frozen dataclass for a single month's observation
# ─────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SemiBookToBill:
    """A single monthly SEMI NA equipment observation.

    Attributes:
        month_end: Calendar month-end date the observation refers to.
        bookings_usd_m: New orders placed, in USD millions.
            ``None`` if SEMI/FRED did not publish the number.
        billings_usd_m: Shipments invoiced, in USD millions.
            ``None`` if SEMI/FRED did not publish the number.
        ratio: ``bookings / billings``. ``None`` if either side is
            missing or if billings is zero.
    """

    month_end: date
    bookings_usd_m: float | None
    billings_usd_m: float | None
    ratio: float | None

    @staticmethod
    def compute_ratio(
        bookings: float | None,
        billings: float | None,
    ) -> float | None:
        """Safely compute ``bookings / billings``.

        Parameters:
            bookings: Monthly new-order value.
            billings: Monthly shipment value.

        Returns:
            The ratio, or ``None`` if either input is missing or if
            billings is zero (avoids division-by-zero).
        """
        if bookings is None or billings is None:
            return None
        try:
            b = float(billings)
        except (TypeError, ValueError):
            return None
        if b == 0.0:
            return None
        try:
            return float(bookings) / b
        except (TypeError, ValueError):
            return None

    @classmethod
    def from_inputs(
        cls,
        month_end: date,
        bookings: float | None,
        billings: float | None,
    ) -> "SemiBookToBill":
        """Construct an observation, computing the ratio automatically."""
        return cls(
            month_end=month_end,
            bookings_usd_m=bookings,
            billings_usd_m=billings,
            ratio=cls.compute_ratio(bookings, billings),
        )


# ─────────────────────────────────────────────────────────────────────
# Puller
# ─────────────────────────────────────────────────────────────────────


class SemiBookToBillPuller(BasePuller):
    """Pulls the SEMI NA equipment book-to-bill ratio into ``raw_series``.

    FRED is preferred (simple REST, versioned, retry-friendly). The HTML
    scraper is retained as a graceful-degradation fallback so the puller
    is never a hard dependency on a single vendor.

    Attributes:
        source_name: Canonical name written to ``source_catalog``.
        fred_api_key: Optional FRED API key. If empty, FRED path is
            skipped and HTML is used directly.
    """

    SOURCE_NAME: str = "semi_book_to_bill"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": SEMI_RELEASE_URL,
        "cost_tier": "FREE",
        "latency_class": "MONTHLY",
        "pit_available": True,
        "revision_behavior": "RARELY",
        "trust_score": "HIGH",
        "priority_rank": 35,
    }

    #: Public attribute so scheduler / callers can introspect cadence.
    source_name: str = "semi_book_to_bill"

    def __init__(
        self,
        db_engine: Engine,
        fred_api_key: str | None = None,
    ) -> None:
        """Initialise the puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
            fred_api_key: Optional FRED API key. Falls back to the
                ``FRED_API_KEY`` environment variable if not provided.
        """
        self.fred_api_key: str = (
            fred_api_key if fred_api_key is not None else os.environ.get("FRED_API_KEY", "")
        )
        super().__init__(db_engine)
        log.info(
            "SemiBookToBillPuller initialised — fred_key={fk}",
            fk="set" if self.fred_api_key else "missing",
        )

    # ------------------------------------------------------------------
    # Public entrypoints
    # ------------------------------------------------------------------

    def pull(self) -> tuple[list[SemiBookToBill], str]:
        """Fetch observations from FRED, falling back to HTML on failure.

        Returns:
            ``(observations, source)`` where source is one of
            ``"fred"``, ``"html"``, or ``"none"``.
        """
        # Try FRED first
        if self.fred_api_key:
            try:
                fred_rows = self._fetch_from_fred()
                if fred_rows:
                    log.info(
                        "SEMI: FRED returned {n} observations",
                        n=len(fred_rows),
                    )
                    return fred_rows, "fred"
                log.warning(
                    "SEMI: FRED returned 0 rows — falling back to HTML"
                )
            except Exception as exc:  # noqa: BLE001 — intentional catch-all
                log.warning(
                    "SEMI: FRED fetch failed ({e}) — falling back to HTML",
                    e=str(exc),
                )
        else:
            log.info("SEMI: no FRED_API_KEY — going straight to HTML scrape")

        # HTML fallback
        try:
            html_rows = self._scrape_semi_html()
            if html_rows:
                log.info(
                    "SEMI: HTML scrape returned {n} observations",
                    n=len(html_rows),
                )
                return html_rows, "html"
            log.warning("SEMI: HTML scrape returned 0 rows")
        except Exception as exc:  # noqa: BLE001
            log.warning("SEMI: HTML scrape failed: {e}", e=str(exc))

        return [], "none"

    def save_to_db(self, observations: Iterable[SemiBookToBill]) -> int:
        """Upsert observations into ``raw_series``.

        Each observation produces up to three rows (one per label in
        :data:`SERIES_LABELS`). Missing components (``None`` values) are
        skipped so a month with only bookings still persists bookings.

        Parameters:
            observations: Iterable of :class:`SemiBookToBill`.

        Returns:
            Number of rows inserted (skips rows that already exist).
        """
        inserted = 0
        obs_list = list(observations)
        if not obs_list:
            return 0

        with self.engine.begin() as conn:
            # Pre-fetch existing dates per label for dedup
            existing: dict[str, set[date]] = {
                label: self._get_existing_dates(f"semi:{label}", conn)
                for label in SERIES_LABELS
            }

            for ob in obs_list:
                payload = {
                    "source": "SEMI_NA_Equipment",
                    "release_url": SEMI_RELEASE_URL,
                    "bookings_usd_m": ob.bookings_usd_m,
                    "billings_usd_m": ob.billings_usd_m,
                    "ratio": ob.ratio,
                }

                label_values: dict[str, float | None] = {
                    "bookings": ob.bookings_usd_m,
                    "billings": ob.billings_usd_m,
                    "ratio": ob.ratio,
                }

                for label, value in label_values.items():
                    if value is None:
                        continue
                    if ob.month_end in existing[label]:
                        continue

                    series_id = f"semi:{label}"
                    conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, "
                            "raw_payload, pull_status) "
                            "VALUES (:sid, :src, :od, :val, :payload, "
                            "'SUCCESS')"
                        ),
                        {
                            "sid": series_id,
                            "src": self.source_id,
                            "od": ob.month_end,
                            "val": float(value),
                            "payload": json.dumps(payload),
                        },
                    )
                    existing[label].add(ob.month_end)
                    inserted += 1

        return inserted

    # ------------------------------------------------------------------
    # FRED path
    # ------------------------------------------------------------------

    def _fetch_fred_series(self, series_id: str) -> list[tuple[date, float]]:
        """Fetch one FRED series as ``(obs_date, value)`` pairs.

        Parameters:
            series_id: FRED series ID (e.g. ``"SEMIBBILL"``).

        Returns:
            List of ``(date, value)`` observations. Non-numeric and ``"."``
            placeholders are dropped.
        """
        params: dict[str, str] = {
            "series_id": series_id,
            "api_key": self.fred_api_key,
            "file_type": "json",
        }
        resp = requests.get(
            FRED_OBSERVATIONS_URL,
            params=params,
            timeout=_REQUEST_TIMEOUT,
            headers={"User-Agent": _USER_AGENT},
        )
        resp.raise_for_status()
        body = resp.json()

        out: list[tuple[date, float]] = []
        for obs in body.get("observations", []):
            date_str = obs.get("date")
            value_str = obs.get("value")
            if not date_str or value_str in (None, "", "."):
                continue
            try:
                obs_date = date.fromisoformat(date_str)
                out.append((obs_date, float(value_str)))
            except (ValueError, TypeError):
                log.warning(
                    "SEMI FRED {s}: skipping bad row {d}={v}",
                    s=series_id,
                    d=date_str,
                    v=value_str,
                )
        return out

    def _fetch_from_fred(self) -> list[SemiBookToBill]:
        """Fetch bookings + billings from FRED and zip them by month."""
        bookings_raw = self._fetch_fred_series(FRED_SERIES_IDS["bookings"])
        billings_raw = self._fetch_fred_series(FRED_SERIES_IDS["billings"])

        bookings_map: dict[date, float] = dict(bookings_raw)
        billings_map: dict[date, float] = dict(billings_raw)

        # Union of dates — a month present in either series is kept.
        all_dates: set[date] = set(bookings_map) | set(billings_map)
        observations: list[SemiBookToBill] = [
            SemiBookToBill.from_inputs(
                month_end=d,
                bookings=bookings_map.get(d),
                billings=billings_map.get(d),
            )
            for d in sorted(all_dates)
        ]
        return observations

    # ------------------------------------------------------------------
    # HTML fallback path
    # ------------------------------------------------------------------

    def _fetch_semi_html(self) -> str:
        """HTTP GET the SEMI press-release page; return the raw HTML."""
        resp = requests.get(
            SEMI_RELEASE_URL,
            timeout=_REQUEST_TIMEOUT,
            headers={
                "User-Agent": _USER_AGENT,
                "Accept": "text/html,application/xhtml+xml",
            },
        )
        resp.raise_for_status()
        return resp.text

    def _scrape_semi_html(
        self,
        html: str | None = None,
    ) -> list[SemiBookToBill]:
        """Parse the SEMI press-release page into observations.

        The page ships a simple HTML table whose header row contains the
        words "bookings" and "billings" (case-insensitive). Each body row
        is a single month: ``[month_label, bookings, billings, ratio]``.

        Parameters:
            html: Optional raw HTML. If omitted, the page is fetched live.

        Returns:
            List of :class:`SemiBookToBill` observations sorted by date.
        """
        if html is None:
            html = self._fetch_semi_html()

        soup = BeautifulSoup(html, "html.parser")

        target_table = _find_semi_table(soup)
        if target_table is None:
            log.warning("SEMI HTML: no bookings/billings table found")
            return []

        observations: list[SemiBookToBill] = []
        for row in target_table.find_all("tr"):
            cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
            if len(cells) < 3:
                continue
            # Skip rows that are clearly headers (first cell isn't a month).
            month_end = _parse_month_label(cells[0])
            if month_end is None:
                continue

            bookings = _parse_number(cells[1]) if len(cells) > 1 else None
            billings = _parse_number(cells[2]) if len(cells) > 2 else None
            observations.append(
                SemiBookToBill.from_inputs(
                    month_end=month_end,
                    bookings=bookings,
                    billings=billings,
                )
            )

        observations.sort(key=lambda o: o.month_end)
        return observations


# ─────────────────────────────────────────────────────────────────────
# Parsing helpers (module-level so they're unit-testable in isolation)
# ─────────────────────────────────────────────────────────────────────


_MONTH_NAMES: dict[str, int] = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


def _month_end(year: int, month: int) -> date:
    """Return the last calendar day of ``year``/``month``."""
    if month == 12:
        return date(year, 12, 31)
    next_first = date(year, month + 1, 1)
    return date.fromordinal(next_first.toordinal() - 1)


def _parse_month_label(label: str) -> date | None:
    """Parse a label like ``"Jan 2026"`` or ``"January 2026"``.

    Returns the last day of that month, or ``None`` if unparseable.
    """
    if not label:
        return None
    cleaned = re.sub(r"[^A-Za-z0-9 ]+", " ", label).strip().lower()
    parts = cleaned.split()
    if len(parts) < 2:
        return None
    month_token, year_token = parts[0], parts[-1]
    month = _MONTH_NAMES.get(month_token)
    if month is None:
        return None
    try:
        year = int(year_token)
    except ValueError:
        return None
    if year < 1990 or year > 2100:
        return None
    return _month_end(year, month)


def _parse_number(raw: str) -> float | None:
    """Parse a numeric cell like ``"$2,817.4"``, ``"1.08"``, ``"—"``.

    Returns ``None`` for blanks, dashes, and unparseable strings.
    """
    if raw is None:
        return None
    txt = raw.strip()
    if not txt or txt in {"—", "-", "–", "N/A", "n/a"}:
        return None
    # Strip currency / thousands separators.
    txt = re.sub(r"[\$,]", "", txt)
    try:
        return float(txt)
    except ValueError:
        return None


def _find_semi_table(soup: BeautifulSoup) -> Any:
    """Find the first HTML ``<table>`` whose header mentions bookings.

    Parameters:
        soup: A BeautifulSoup document.

    Returns:
        The matching ``<table>`` element, or ``None`` if not found.
    """
    for table in soup.find_all("table"):
        header_text = " ".join(
            c.get_text(" ", strip=True).lower() for c in table.find_all(["th", "td"])
        )
        if "booking" in header_text and "billing" in header_text:
            return table
    return None


# ─────────────────────────────────────────────────────────────────────
# Entrypoint (scheduler-facing)
# ─────────────────────────────────────────────────────────────────────


def run_semi_book_to_bill_puller(
    engine: Engine,
    fred_api_key: str | None = None,
) -> dict[str, Any]:
    """Module-level runner: fetch + save.

    Parameters:
        engine: SQLAlchemy engine.
        fred_api_key: Optional override. If ``None``, falls back to env.

    Returns:
        ``{"fetched": N, "inserted": M, "source": "fred"|"html"|"none"}``.
        Never raises: if both paths fail, a zero-row result is returned
        and a warning is logged.
    """
    try:
        puller = SemiBookToBillPuller(engine, fred_api_key=fred_api_key)
    except Exception as exc:  # noqa: BLE001
        log.error("SEMI puller init failed: {e}", e=str(exc))
        return {"fetched": 0, "inserted": 0, "source": "none"}

    try:
        observations, source = puller.pull()
    except Exception as exc:  # noqa: BLE001
        log.warning("SEMI pull() raised unexpectedly: {e}", e=str(exc))
        return {"fetched": 0, "inserted": 0, "source": "none"}

    fetched = len(observations)
    if not observations:
        return {"fetched": 0, "inserted": 0, "source": source}

    try:
        inserted = puller.save_to_db(observations)
    except Exception as exc:  # noqa: BLE001
        log.error("SEMI save_to_db failed: {e}", e=str(exc))
        return {"fetched": fetched, "inserted": 0, "source": source}

    return {"fetched": fetched, "inserted": inserted, "source": source}


if __name__ == "__main__":  # pragma: no cover
    from db import get_engine

    result = run_semi_book_to_bill_puller(get_engine())
    print(f"SEMI book-to-bill: {result}")
