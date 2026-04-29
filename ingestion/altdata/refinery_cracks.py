"""CAT-54 — Refinery utilization + 3-2-1 crack spread puller.

Pulls weekly US refinery utilization + crack-spread components from FRED
and materializes the composite 3-2-1 crack spread. Refinery margin is
the upstream lever on gasoline/diesel pump prices — and those feed
directly into CPI energy and CPI services (transportation). When the
3-2-1 crack collapses, refiners cut runs, product inventories tighten,
and pump prices lag-diverge from crude, which shows up in headline CPI
2-6 weeks later.

Series pulled from FRED (all weekly, Wed close):

  WCRFPUS2        US refinery percent utilization (operable cap)
  WCESTUS1        US refiner gasoline stocks (kb)
  WDISTUS1        US refiner distillate stocks (kb)
  DCOILWTICO      WTI spot ($/bbl)
  DGASUSGULF      US Gulf Coast regular gasoline spot ($/gal)
  DDFUELUSGULF    US Gulf Coast ULSD diesel spot ($/gal)

Why this matters (Tier A catalog #54): refinery utilization + the 3-2-1
crack are the cleanest weekly read on energy-inflation pass-through.
The credit_cycle_phase classifier consumes implied inflation nowcast
via ``refinery_cracks:crack_321`` and ``refinery_cracks:util_pct``,
and the options_recommender uses crack-spread regime to size XLE
exposure. Same FRED pattern as CAT-27 (h8_bank_balance) and CAT-49
(wage_tracker) pullers. Graceful degradation on missing FRED_API_KEY
matches the rest of the altdata family.

3-2-1 crack formula (industry standard): for every 3 barrels of crude
input, refiners produce ~2 barrels of gasoline + 1 barrel of distillate.
We convert Gulf Coast product spot prices from $/gal to $/bbl (x42)
before differencing against WTI:

    crack_321 = (2 * gasoline_bbl + diesel_bbl) / 3  -  wti

Stored under raw_series ``refinery_cracks:<label>``; the composite
lives under ``refinery_cracks:crack_321``.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text

from ingestion.base import BasePuller


# ── FRED series we pull ───────────────────────────────────────────────────

REFINERY_SERIES: dict[str, str] = {
    "WCRFPUS2":     "util_pct",
    "WCESTUS1":     "gasoline_stocks",
    "WDISTUS1":     "distillate_stocks",
    "DCOILWTICO":   "wti_spot",
    "DGASUSGULF":   "gasoline_spot_gulf",
    "DDFUELUSGULF": "diesel_spot_gulf",
}

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

# Weekly cadence; 3-year window catches revisions and seasonal patterns.
_LOOKBACK_YEARS = 3

# 1 barrel = 42 US gallons (standard petroleum conversion)
GAL_PER_BBL = 42.0


@dataclass
class RefineryRow:
    """One (series, date, value) observation from FRED."""

    series_id: str
    obs_date: date
    value: float


@dataclass
class Crack321:
    """Materialized 3-2-1 crack spread observation.

    All prices in $/bbl. ``gasoline`` and ``diesel`` are the per-barrel
    equivalents (x42 conversion from the Gulf Coast $/gal spot).
    """

    obs_date: date
    wti: float
    gasoline: float
    diesel: float
    crack_321: float

    @classmethod
    def from_spots(
        cls,
        *,
        obs_date: date,
        wti: float,
        gasoline_per_gal: float,
        diesel_per_gal: float,
    ) -> Crack321:
        """Build a Crack321 from WTI ($/bbl) and Gulf Coast product spot
        prices ($/gal). Converts products to $/bbl and applies the
        industry-standard 3-2-1 formula: (2G + D)/3 − WTI.
        """
        gasoline_bbl = gasoline_per_gal * GAL_PER_BBL
        diesel_bbl = diesel_per_gal * GAL_PER_BBL
        crack = (2.0 * gasoline_bbl + diesel_bbl) / 3.0 - wti
        return cls(
            obs_date=obs_date,
            wti=wti,
            gasoline=gasoline_bbl,
            diesel=diesel_bbl,
            crack_321=crack,
        )


class RefineryCracksPuller(BasePuller):
    """Weekly refinery utilization + 3-2-1 crack spread puller from FRED."""

    source_name = "refinery_cracks"  # task-facing name
    SOURCE_NAME = "refinery_cracks"  # BasePuller auto-create hook
    SOURCE_CONFIG = {
        "base_url": FRED_BASE,
        "cost_tier": "FREE",
        "latency_class": "WEEKLY",
        "pit_available": True,
        "revision_behavior": "SMALL",
        "trust_score": "HIGH",
        "priority_rank": 22,
    }

    def __init__(self, api_key: str, db_engine) -> None:
        super().__init__(db_engine)
        self.api_key = api_key
        # Populated by pull() and consumed by save_to_db().
        self._raw_rows: list[RefineryRow] = []
        self._crack_rows: list[RefineryRow] = []

    # ── Fetch ────────────────────────────────────────────────────────────

    def _fetch_series(
        self,
        fred_code: str,
        *,
        timeout: float = 10.0,
    ) -> list[RefineryRow]:
        """Fetch one FRED series into RefineryRow objects.

        Returns an empty list on any error or when FRED_API_KEY is unset.
        Never raises — graceful degradation is the contract.
        """
        if not self.api_key:
            log.warning("refinery_cracks: FRED_API_KEY not set, skipping {c}", c=fred_code)
            return []

        params = {
            "series_id": fred_code,
            "api_key": self.api_key,
            "file_type": "json",
            "observation_start": (
                date.today().replace(year=date.today().year - _LOOKBACK_YEARS)
            ).isoformat(),
        }
        try:
            resp = requests.get(FRED_BASE, params=params, timeout=timeout)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "refinery_cracks fetch failed {c}: {e}",
                c=fred_code, e=str(exc),
            )
            return []

        label = REFINERY_SERIES.get(fred_code, fred_code)
        rows: list[RefineryRow] = []
        for obs in payload.get("observations", []):
            val_str = obs.get("value", ".")
            if val_str in (".", "", None):
                continue
            try:
                rows.append(
                    RefineryRow(
                        series_id=f"refinery_cracks:{label}",
                        obs_date=date.fromisoformat(obs["date"]),
                        value=float(val_str),
                    )
                )
            except (ValueError, KeyError):
                continue
        return rows

    def pull(self) -> dict[str, Any]:
        """Fetch every configured FRED series and materialize crack_321.

        Populates ``self._raw_rows`` with raw component observations and
        ``self._crack_rows`` with the composite 3-2-1 crack per-date.
        Returns per-series row counts (fetched, not yet inserted).
        """
        by_label: dict[str, list[RefineryRow]] = {}
        total_fetched = 0
        for fred_code, label in REFINERY_SERIES.items():
            rows = self._fetch_series(fred_code)
            by_label[label] = rows
            total_fetched += len(rows)

        # Flatten raw rows in one pass.
        raw_rows: list[RefineryRow] = []
        for rows in by_label.values():
            raw_rows.extend(rows)
        self._raw_rows = raw_rows

        # Materialize 3-2-1 crack only on dates where all three
        # ingredients are present (WTI, Gulf gasoline, Gulf diesel).
        wti_map: dict[date, float] = {
            r.obs_date: r.value for r in by_label.get("wti_spot", [])
        }
        gas_map: dict[date, float] = {
            r.obs_date: r.value for r in by_label.get("gasoline_spot_gulf", [])
        }
        diesel_map: dict[date, float] = {
            r.obs_date: r.value for r in by_label.get("diesel_spot_gulf", [])
        }
        common_dates = sorted(
            set(wti_map) & set(gas_map) & set(diesel_map)
        )

        crack_rows: list[RefineryRow] = []
        for d in common_dates:
            crack = Crack321.from_spots(
                obs_date=d,
                wti=wti_map[d],
                gasoline_per_gal=gas_map[d],
                diesel_per_gal=diesel_map[d],
            )
            crack_rows.append(
                RefineryRow(
                    series_id="refinery_cracks:crack_321",
                    obs_date=d,
                    value=crack.crack_321,
                )
            )
        self._crack_rows = crack_rows

        log.info(
            "refinery_cracks: fetched {f} raw rows across {s} series, "
            "materialized {c} crack_321 observations",
            f=total_fetched, s=len(REFINERY_SERIES), c=len(crack_rows),
        )
        return {
            "fetched_raw": total_fetched,
            "crack_321_rows": len(crack_rows),
            "per_series": {
                label: len(rows) for label, rows in by_label.items()
            },
        }

    # ── Upsert ───────────────────────────────────────────────────────────

    def _upsert_rows(self, rows: list[RefineryRow]) -> int:
        """Insert rows into raw_series, skipping any (series_id, obs_date)
        already present. Matches the wage_tracker upsert pattern exactly.
        """
        if not rows:
            return 0
        inserted = 0
        by_series: dict[str, list[RefineryRow]] = {}
        for r in rows:
            by_series.setdefault(r.series_id, []).append(r)

        with self.engine.begin() as conn:
            for series_id, batch in by_series.items():
                existing = self._get_existing_dates(series_id, conn)
                for r in batch:
                    if r.obs_date in existing:
                        continue
                    try:
                        conn.execute(
                            text(
                                "INSERT INTO raw_series "
                                "(series_id, source_id, obs_date, value, "
                                " pull_status, pull_timestamp) "
                                "VALUES (:sid, :src, :od, :val, 'SUCCESS', :ts)"
                            ),
                            {
                                "sid": r.series_id,
                                "src": self.source_id,
                                "od": r.obs_date,
                                "val": r.value,
                                "ts": datetime.now(timezone.utc),
                            },
                        )
                        inserted += 1
                    except Exception as exc:  # noqa: BLE001
                        log.debug(
                            "refinery_cracks insert failed {s} {d}: {e}",
                            s=r.series_id, d=r.obs_date, e=str(exc),
                        )
        return inserted

    def save_to_db(self) -> int:
        """Persist the most-recent pull() output to raw_series.

        Returns the total number of new rows inserted (raw + crack_321).
        Safe to call multiple times — deduped by (series_id, obs_date).
        """
        raw_inserted = self._upsert_rows(self._raw_rows)
        crack_inserted = self._upsert_rows(self._crack_rows)
        total = raw_inserted + crack_inserted
        log.info(
            "refinery_cracks: inserted {r} raw + {c} crack_321 = {t} new rows",
            r=raw_inserted, c=crack_inserted, t=total,
        )
        return total


def run_refinery_cracks_puller(engine: Any) -> dict[str, Any]:
    """Entrypoint mirroring ``run_wage_tracker_puller``.

    Instantiates the puller, fetches all series, upserts raw + composite
    rows, and returns a summary dict with the same shape as the other
    FRED pullers (``fetched``, ``inserted``, ``series``).

    Graceful-degrades on missing FRED_API_KEY: logs a warning and
    returns a zero-row result without raising.
    """
    from config import settings

    api_key = getattr(settings, "FRED_API_KEY", "") or ""
    if not api_key:
        log.warning(
            "refinery_cracks: FRED_API_KEY not set; returning empty result"
        )
        return {
            "fetched": 0,
            "inserted": 0,
            "series": {label: 0 for label in REFINERY_SERIES.values()},
        }

    puller = RefineryCracksPuller(api_key=api_key, db_engine=engine)
    pull_summary = puller.pull()
    inserted = puller.save_to_db()

    series_counts: dict[str, int] = dict(pull_summary.get("per_series", {}))
    series_counts["crack_321"] = pull_summary.get("crack_321_rows", 0)

    return {
        "fetched": pull_summary.get("fetched_raw", 0)
        + pull_summary.get("crack_321_rows", 0),
        "inserted": inserted,
        "series": series_counts,
    }
