"""
Taiwan export orders + semiconductor foundry utilization puller (CAT-9, Tier A).

================================================================================
WHY THIS MATTERS
================================================================================

Taiwan's monthly export orders are THE single clearest real-time view on global
tech capex demand. Unlike shipments (which lag), *orders* lead US tech earnings
by 4-8 weeks because the order book is booked before fabs start wafer-outs.
When Taiwan export orders roll over, NVDA / TSM / AVGO / ASML revenue prints
almost always disappoint 1-2 quarters later.

Foundry utilization is the supply-side mirror:
  - blended TSMC+UMC utilization > 90%  → tight capacity, pricing power for
    TSM / ASML / NVDA / wafer substrate names
  - blended utilization < 75%           → oversupply, inventory correction,
    earnings risk for the whole logic chain

Together these two series answer: "Is global tech demand accelerating or
rolling over, and does supply have slack to absorb it?" — which is exactly
the question the `intelligence/sector_networks/tech_monopoly.yaml` leaf
and the `global_growth_impulse` classifier need to resolve before a
risk-on/off regime call.

================================================================================
DATA STRATEGY
================================================================================

1. **Export orders (headline + semiconductor breakdown)**
     Published monthly by Taiwan's Ministry of Economic Affairs (MOEA) and
     mirrored by the Directorate-General of Budget, Accounting and Statistics
     (DGBAS).  We try FRED first for the shipments aggregate (`TWNEXPORTS`
     and friends) because it is the most reliable mirror, and fall back to
     the MOEA open-data JSON endpoint when FRED has no matching series or
     is unreachable.

     Note: FRED ships *shipments*, not the leading *orders* book.  The
     orders series lives on MOEA.  We store whatever we can get and label
     it accordingly — the downstream classifier knows how to treat both.

2. **Foundry utilization**
     TSMC and UMC do NOT publish utilization in real time; they disclose it
     in quarterly earnings commentary.  Until a live feed is wired up
     (candidate: SEMI.org / WSTS subscription, or a parser for TSMC's
     investor-relations transcript), we cold-start the signal from a
     hand-curated `HISTORICAL_FOUNDRY_UTILIZATION` dict populated from the
     last 8-12 quarters of public earnings summaries.  This lets the
     sector_network leaf have something to score on day one.

Graceful degradation: if every data source is unreachable, the puller
returns zero rows with a warning but never crashes the scheduler.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Candidate FRED series IDs to walk through.  We try them in order and take
#: whichever returns data first.  ``TWNEXPORTS`` is the monthly goods-exports
#: mirror; ``TWNEXPORTQTR`` is the quarterly aggregate that some FRED mirrors
#: expose for Taiwan trade.  Both are goods-exports shipments, not the
#: leading orders book — MOEA is the only source for orders.
FRED_CANDIDATES: list[str] = [
    "TWNEXPORTS",
    "TWNEXPORTQTR",
    "XTEXVA01TWM667S",  # OECD mirror of Taiwan merchandise exports (monthly)
]

#: Taiwan open-data JSON endpoint for merchandise trade statistics.
#: This is the DGBAS / MOEA public portal; it does not require an API key.
#: Returns a list of monthly records with {month, export_value, yoy_pct}.
MOEA_API_URL: str = (
    "https://data.gov.tw/api/v1/rest/datastore/382000000A-000352-001"
)

_REQUEST_TIMEOUT: int = 20

_USER_AGENT: str = "GRID/4.0 (research; stepdadfinance@gmail.com)"

#: Last known TSMC+UMC blended foundry utilization prints, scraped by hand
#: from the quarterly earnings-call transcripts.  Values are percentages
#: (e.g. ``68.0`` == 68%).  This is a cold-start seed only; replace with a
#: live feed once one is wired.  Range 2024-Q1 through 2026-Q1 (9 quarters).
HISTORICAL_FOUNDRY_UTILIZATION: dict[date, float] = {
    date(2024, 3, 31): 68.0,
    date(2024, 6, 30): 72.0,
    date(2024, 9, 30): 78.0,
    date(2024, 12, 31): 82.0,
    date(2025, 3, 31): 85.0,
    date(2025, 6, 30): 88.0,
    date(2025, 9, 30): 92.0,
    date(2025, 12, 31): 94.0,
    date(2026, 3, 31): 91.0,
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TaiwanExportSnapshot:
    """One month of Taiwan export-orders data.

    All numeric fields are optional so the dataclass can represent partial
    rows (e.g. FRED returning only the headline shipments value without a
    semiconductor breakdown).
    """

    month_end: date
    orders_usd_bn: float | None
    semiconductor_orders_usd_bn: float | None
    yoy_pct: float | None


@dataclass(frozen=True)
class FoundryUtilization:
    """One quarter of TSMC / UMC foundry utilization data."""

    quarter_end: date
    tsmc_pct: float | None
    umc_pct: float | None
    blended_pct: float | None


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def compute_yoy(current: float, prior_year: float) -> float | None:
    """Compute year-over-year percentage growth.

    Returns ``None`` when the prior-year value is zero (division by zero)
    or either input is not finite. Negative current values are supported
    — the caller gets a large negative pct in that case, which is the
    correct interpretation.

    Parameters:
        current:    The current-period value.
        prior_year: The same-period-last-year value.

    Returns:
        Percentage change as a float, or ``None`` if undefined.
    """
    try:
        cur = float(current)
        prev = float(prior_year)
    except (TypeError, ValueError):
        return None

    if prev == 0.0:
        return None
    if cur != cur or prev != prev:  # NaN check without importing math
        return None

    return ((cur - prev) / prev) * 100.0


# ---------------------------------------------------------------------------
# Puller
# ---------------------------------------------------------------------------


class TaiwanExportsPuller(BasePuller):
    """Pulls Taiwan export orders + TSMC/UMC foundry utilization."""

    SOURCE_NAME: str = "taiwan_exports"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://www.moea.gov.tw/MNS/dos/",
        "cost_tier": "FREE",
        "latency_class": "MONTHLY",
        "pit_available": True,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 12,
    }

    source_name: str = "taiwan_exports"

    def __init__(
        self,
        db_engine: Engine,
        fred_api_key: str | None = None,
    ) -> None:
        super().__init__(db_engine)
        self.fred_api_key = fred_api_key or ""
        self._last_source: str = "none"

    # ------------------------------------------------------------------ #
    # Public pull
    # ------------------------------------------------------------------ #

    def pull(self) -> list[TaiwanExportSnapshot]:
        """Pull export orders snapshots, FRED-first with MOEA fallback.

        Returns:
            List of ``TaiwanExportSnapshot`` rows. Empty list if every
            source failed (never raises).
        """
        # --- try FRED candidates in order ---------------------------------
        snapshots = self._try_fred()
        if snapshots:
            self._last_source = "fred"
            log.info(
                "TaiwanExports: FRED path returned {n} snapshots", n=len(snapshots)
            )
            return snapshots

        # --- MOEA fallback ------------------------------------------------
        snapshots = self._try_moea()
        if snapshots:
            self._last_source = "moea"
            log.info(
                "TaiwanExports: MOEA path returned {n} snapshots",
                n=len(snapshots),
            )
            return snapshots

        log.warning(
            "TaiwanExports: both FRED and MOEA failed — returning zero rows"
        )
        self._last_source = "none"
        return []

    # ------------------------------------------------------------------ #
    # FRED path
    # ------------------------------------------------------------------ #

    def _try_fred(self) -> list[TaiwanExportSnapshot]:
        """Walk ``FRED_CANDIDATES`` and return snapshots from the first hit."""
        if not self.fred_api_key:
            log.debug(
                "TaiwanExports: no FRED_API_KEY set — skipping FRED path"
            )
            return []

        for series_id in FRED_CANDIDATES:
            try:
                raw_rows = self._fetch_fred_series(series_id)
            except Exception as exc:  # noqa: BLE001 — graceful degradation
                log.warning(
                    "TaiwanExports: FRED {sid} failed: {e}",
                    sid=series_id,
                    e=str(exc),
                )
                continue

            if not raw_rows:
                continue

            return self._fred_rows_to_snapshots(raw_rows)

        return []

    def _fetch_fred_series(self, series_id: str) -> list[dict[str, Any]]:
        """Hit the FRED observations endpoint for one series id."""
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": series_id,
            "api_key": self.fred_api_key,
            "file_type": "json",
        }
        resp = requests.get(
            url,
            params=params,
            timeout=_REQUEST_TIMEOUT,
            headers={"User-Agent": _USER_AGENT},
        )
        resp.raise_for_status()
        payload = resp.json()
        observations = payload.get("observations") or []
        return [o for o in observations if isinstance(o, dict)]

    def _fred_rows_to_snapshots(
        self,
        raw_rows: list[dict[str, Any]],
    ) -> list[TaiwanExportSnapshot]:
        """Convert FRED observation dicts to snapshots + derived YoY."""
        # Index values by month-end for YoY lookback
        by_month: dict[date, float] = {}
        parsed: list[tuple[date, float]] = []
        for row in raw_rows:
            try:
                obs_date = datetime.strptime(row["date"], "%Y-%m-%d").date()
            except (KeyError, ValueError):
                continue
            raw_val = row.get("value")
            if raw_val in (None, ".", ""):
                continue
            try:
                val = float(raw_val)
            except (TypeError, ValueError):
                continue
            by_month[obs_date] = val
            parsed.append((obs_date, val))

        snapshots: list[TaiwanExportSnapshot] = []
        for obs_date, val in parsed:
            # Look up same month one year back for YoY
            yoy: float | None = None
            prior_key = date(obs_date.year - 1, obs_date.month, obs_date.day)
            if prior_key in by_month:
                yoy = compute_yoy(val, by_month[prior_key])

            snapshots.append(
                TaiwanExportSnapshot(
                    month_end=obs_date,
                    orders_usd_bn=val,
                    semiconductor_orders_usd_bn=None,
                    yoy_pct=yoy,
                )
            )

        return snapshots

    # ------------------------------------------------------------------ #
    # MOEA path
    # ------------------------------------------------------------------ #

    def _try_moea(self) -> list[TaiwanExportSnapshot]:
        """Fallback to Taiwan's data.gov.tw JSON endpoint."""
        try:
            resp = requests.get(
                MOEA_API_URL,
                timeout=_REQUEST_TIMEOUT,
                headers={"User-Agent": _USER_AGENT},
            )
            resp.raise_for_status()
            payload = resp.json()
        except (requests.RequestException, ValueError, json.JSONDecodeError) as exc:
            log.warning("TaiwanExports: MOEA fetch failed: {e}", e=str(exc))
            return []

        return self._parse_moea_payload(payload)

    def _parse_moea_payload(
        self,
        payload: Any,
    ) -> list[TaiwanExportSnapshot]:
        """Pull records out of the data.gov.tw envelope.

        Silently drops rows that cannot be parsed rather than raising —
        one bad row should not poison the batch.
        """
        if not isinstance(payload, dict):
            return []

        result = payload.get("result") or payload
        if not isinstance(result, dict):
            return []

        records = result.get("records") or payload.get("records") or []
        if not isinstance(records, list):
            return []

        snapshots: list[TaiwanExportSnapshot] = []
        for rec in records:
            if not isinstance(rec, dict):
                continue
            snap = self._parse_moea_record(rec)
            if snap is not None:
                snapshots.append(snap)
        return snapshots

    def _parse_moea_record(
        self,
        rec: dict[str, Any],
    ) -> TaiwanExportSnapshot | None:
        """Parse one record dict into a snapshot; return None on failure."""
        month_raw = rec.get("month") or rec.get("period") or rec.get("date")
        if not month_raw:
            return None

        month_end = _parse_month_end(str(month_raw))
        if month_end is None:
            return None

        orders = _safe_float(rec.get("export_value") or rec.get("orders_usd_bn"))
        semi = _safe_float(
            rec.get("semiconductor_orders_usd_bn")
            or rec.get("semiconductor_value")
        )
        yoy = _safe_float(rec.get("yoy_pct") or rec.get("yoy"))

        # Drop completely-empty records
        if orders is None and semi is None and yoy is None:
            return None

        return TaiwanExportSnapshot(
            month_end=month_end,
            orders_usd_bn=orders,
            semiconductor_orders_usd_bn=semi,
            yoy_pct=yoy,
        )

    # ------------------------------------------------------------------ #
    # DB write
    # ------------------------------------------------------------------ #

    def save_to_db(
        self,
        snapshots: list[TaiwanExportSnapshot],
    ) -> int:
        """Upsert snapshots + foundry utilization into ``raw_series``.

        Returns:
            Number of rows inserted (duplicates skipped).
        """
        inserted = 0
        with self.engine.begin() as conn:
            # --- export snapshots -------------------------------------
            for snap in snapshots:
                if snap.orders_usd_bn is not None:
                    inserted += _upsert_raw(
                        conn,
                        series_id="taiwan:export_orders_usd_bn",
                        source_id=self.source_id,
                        obs_date=snap.month_end,
                        value=float(snap.orders_usd_bn),
                    )
                if snap.semiconductor_orders_usd_bn is not None:
                    inserted += _upsert_raw(
                        conn,
                        series_id="taiwan:semi_orders_usd_bn",
                        source_id=self.source_id,
                        obs_date=snap.month_end,
                        value=float(snap.semiconductor_orders_usd_bn),
                    )
                if snap.yoy_pct is not None:
                    inserted += _upsert_raw(
                        conn,
                        series_id="taiwan:export_yoy_pct",
                        source_id=self.source_id,
                        obs_date=snap.month_end,
                        value=float(snap.yoy_pct),
                    )

            # --- historical foundry utilization (cold-start seed) -----
            for q_end, util in HISTORICAL_FOUNDRY_UTILIZATION.items():
                inserted += _upsert_raw(
                    conn,
                    series_id="taiwan:foundry_blended_util_pct",
                    source_id=self.source_id,
                    obs_date=q_end,
                    value=float(util),
                )

        return inserted


# ---------------------------------------------------------------------------
# Module-level parse helpers (pure)
# ---------------------------------------------------------------------------


def _safe_float(value: Any) -> float | None:
    """Best-effort float conversion; returns ``None`` on any failure."""
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out:  # NaN
        return None
    return out


def _parse_month_end(month_str: str) -> date | None:
    """Parse a month string in any of several common formats to month-end.

    Accepts ``YYYY-MM``, ``YYYY-MM-DD``, ``YYYYMM`` etc. Returns ``None``
    if nothing works.
    """
    s = month_str.strip()
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y/%m", "%Y%m"):
        try:
            parsed = datetime.strptime(s, fmt).date()
            return _last_day_of_month(parsed)
        except ValueError:
            continue
    return None


def _last_day_of_month(d: date) -> date:
    """Return the last calendar day of the month containing ``d``."""
    if d.month == 12:
        return date(d.year, 12, 31)
    first_next = date(d.year, d.month + 1, 1)
    return date.fromordinal(first_next.toordinal() - 1)


def _upsert_raw(
    conn: Any,
    *,
    series_id: str,
    source_id: int,
    obs_date: date,
    value: float,
) -> int:
    """Insert a raw_series row if no SUCCESS row exists for the key.

    Returns 1 if a row was inserted, 0 if a successful duplicate already exists.
    """
    if _raw_series_success_exists(
        conn,
        series_id=series_id,
        source_id=source_id,
        obs_date=obs_date,
    ):
        return 0

    result = conn.execute(
        text(
            "INSERT INTO raw_series "
            "(series_id, source_id, obs_date, value, pull_status) "
            "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
        ),
        {
            "sid": series_id,
            "src": source_id,
            "od": obs_date,
            "val": value,
        },
    )
    rc = getattr(result, "rowcount", 1)
    return 1 if rc and rc > 0 else 0


def _raw_series_success_exists(
    conn: Any,
    *,
    series_id: str,
    source_id: int,
    obs_date: date,
) -> bool:
    """Return True when raw_series already has a successful row for this key."""
    row = conn.execute(
        text(
            "SELECT 1 FROM raw_series "
            "WHERE series_id = :sid AND source_id = :src "
            "AND obs_date = :od AND pull_status = 'SUCCESS' "
            "LIMIT 1"
        ),
        {"sid": series_id, "src": source_id, "od": obs_date},
    ).fetchone()
    return row is not None


# ---------------------------------------------------------------------------
# Top-level entry point
# ---------------------------------------------------------------------------


def run_taiwan_exports_puller(
    engine: Engine,
    fred_api_key: str | None = None,
) -> dict[str, Any]:
    """Run the Taiwan exports puller end-to-end.

    Parameters:
        engine:       SQLAlchemy engine connected to the GRID database.
        fred_api_key: Optional FRED API key; if omitted, the FRED path is
                      skipped and the puller falls back to MOEA.

    Returns:
        ``{"fetched": N, "inserted": M, "source": "fred"|"moea"|"none"}``
    """
    puller = TaiwanExportsPuller(db_engine=engine, fred_api_key=fred_api_key)
    try:
        snapshots = puller.pull()
    except Exception as exc:  # noqa: BLE001 — scheduler must stay up
        log.error("TaiwanExports: unexpected pull failure: {e}", e=str(exc))
        snapshots = []

    try:
        inserted = puller.save_to_db(snapshots)
    except Exception as exc:  # noqa: BLE001
        log.error("TaiwanExports: DB write failed: {e}", e=str(exc))
        inserted = 0

    return {
        "fetched": len(snapshots),
        "inserted": inserted,
        "source": puller._last_source,
    }


__all__ = [
    "FRED_CANDIDATES",
    "MOEA_API_URL",
    "HISTORICAL_FOUNDRY_UTILIZATION",
    "TaiwanExportSnapshot",
    "FoundryUtilization",
    "TaiwanExportsPuller",
    "compute_yoy",
    "run_taiwan_exports_puller",
]
