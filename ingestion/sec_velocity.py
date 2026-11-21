"""
GRID SEC 8-K filing velocity module.

Counts 8-K filings by SIC sector code per week from 1995 to present.
Stores results as feature series SEC_VELOCITY:{sector_code} in raw_series.

SIC sector codes (first two digits of SIC code) map to broad industries:
  01-09: Agriculture/Mining
  10-14: Mining
  15-17: Construction
  20-39: Manufacturing
  40-49: Transportation/Utilities
  50-51: Wholesale Trade
  52-59: Retail Trade
  60-67: Finance/Insurance/Real Estate
  70-89: Services
  90-99: Public Administration
"""

from __future__ import annotations

import json
import time
from collections import Counter
from datetime import date, timedelta
from typing import Any, Callable

import pandas as pd
from edgar import Company, get_filings, set_identity
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from ingestion.base import BasePuller

# SIC sector code ranges and their labels
SIC_SECTORS: dict[str, str] = {
    "01": "Agriculture",
    "10": "Mining",
    "15": "Construction",
    "20": "Manufacturing_Food",
    "25": "Manufacturing_Furniture",
    "28": "Manufacturing_Chemicals",
    "30": "Manufacturing_Plastics",
    "33": "Manufacturing_Metals",
    "35": "Manufacturing_Machinery",
    "36": "Manufacturing_Electronics",
    "37": "Manufacturing_Transport",
    "38": "Manufacturing_Instruments",
    "40": "Transportation",
    "45": "Transportation_Air",
    "48": "Communications",
    "49": "Utilities",
    "50": "Wholesale_Durable",
    "51": "Wholesale_Nondurable",
    "52": "Retail_Building",
    "53": "Retail_General",
    "54": "Retail_Food",
    "56": "Retail_Apparel",
    "58": "Retail_Eating",
    "59": "Retail_Misc",
    "60": "Finance_Depository",
    "61": "Finance_Nondepository",
    "62": "Finance_Securities",
    "63": "Insurance",
    "65": "Real_Estate",
    "67": "Finance_Holding",
    "70": "Services_Hotels",
    "73": "Services_Business",
    "78": "Services_Entertainment",
    "80": "Services_Health",
    "82": "Services_Education",
    "87": "Services_Engineering",
    "99": "Nonclassifiable",
}

# Broader sector groupings (2-digit prefix -> sector label)
BROAD_SECTORS: dict[str, str] = {
    "AGRI": ("01", "09"),
    "MINING": ("10", "14"),
    "CONSTRUCTION": ("15", "17"),
    "MANUFACTURING": ("20", "39"),
    "TRANSPORT_UTIL": ("40", "49"),
    "WHOLESALE": ("50", "51"),
    "RETAIL": ("52", "59"),
    "FIRE": ("60", "67"),   # Finance, Insurance, Real Estate
    "SERVICES": ("70", "89"),
    "PUBLIC_ADMIN": ("90", "99"),
}

_RATE_LIMIT_DELAY: float = 0.12


def _sic_to_broad_sector(sic_code: str | int) -> str:
    """Map a SIC code to a broad sector label.

    Parameters:
        sic_code: 4-digit SIC code (or first 2 digits).

    Returns:
        str: Broad sector name, or 'UNKNOWN' if unmapped.
    """
    prefix = str(sic_code).zfill(4)[:2]
    code_int = int(prefix)

    for sector, (low, high) in BROAD_SECTORS.items():
        if int(low) <= code_int <= int(high):
            return sector
    return "UNKNOWN"


# ── SIC resolution ──────────────────────────────────────────────────────────
#
# edgartools' Filing (5.62.1 and 6.3.0) has no ``sic`` attribute, so every 8-K
# used to land in SEC_VELOCITY:UNKNOWN. SIC now comes from:
#   1. griddb's own ``edgar_submissions`` (scripts/parse_edgar.py, from SEC's
#      XBRL financial-statement bulk files, about 2009-2022 filers). The SIC
#      on the CIK's latest submission filed on or before ``as_of``, so a
#      historical year is labelled with the SIC known at the time. One query
#      per load, never per filing. A missing table or failed read gives {}.
#   2. On a miss, weekly run only: ``edgar.Company(cik).sic`` (one GET of
#      SEC's submissions JSON per CIK). This is the CIK's *current* SIC, so
#      the historical backfill never uses it: a current SIC applied to a past
#      year is lookahead. Cached for the run and capped at
#      SIC_FALLBACK_MAX_LOOKUPS. After
#      SIC_FALLBACK_MAX_CONSECUTIVE_FAILURES errors in a row the fallback
#      stops for the run, so an SEC outage costs a few requests, not 200
#      retry loops.
# Anything unresolved stays UNKNOWN. A lookup failure never aborts the run.
SIC_FALLBACK_MAX_LOOKUPS: int = 200
SIC_FALLBACK_MAX_CONSECUTIVE_FAILURES: int = 3

_LOCAL_SIC_SQL = text(
    "SELECT DISTINCT ON (cik) cik, sic FROM edgar_submissions "
    "WHERE sic IS NOT NULL AND filed <= :as_of "
    "ORDER BY cik, filed DESC"
)


def _normalize_sic(raw: Any) -> str | None:
    """Return a SIC code as a digit string, or ``None`` when absent or invalid."""
    if raw is None:
        return None
    text_value = str(raw).strip()
    if text_value.endswith(".0"):
        text_value = text_value[:-2]
    if not text_value.isdigit() or int(text_value) == 0:
        return None
    return text_value


class SICResolver:
    """CIK -> SIC for one velocity run: the local table first, SEC on a miss."""

    def __init__(
        self,
        engine: Engine,
        company_factory: Callable[[int], Any] | None = None,
        max_lookups: int | None = None,
        max_consecutive_failures: int | None = None,
        allow_fallback: bool = True,
    ) -> None:
        self.engine = engine
        self.allow_fallback = allow_fallback
        self._company_factory = company_factory or (lambda cik: Company(cik))
        self.max_lookups = (
            SIC_FALLBACK_MAX_LOOKUPS if max_lookups is None else max_lookups
        )
        self.max_consecutive_failures = (
            SIC_FALLBACK_MAX_CONSECUTIVE_FAILURES
            if max_consecutive_failures is None
            else max_consecutive_failures
        )
        self.local: dict[int, str] = {}
        self._fallback: dict[int, str | None] = {}
        self.fallback_lookups = 0
        self._consecutive_failures = 0
        self._cap_logged = False
        self._breaker_logged = False

    def load_local(self, as_of: date) -> None:
        """Read edgar_submissions once into ``self.local`` (SIC as of ``as_of``)."""
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(_LOCAL_SIC_SQL, {"as_of": as_of}).fetchall()
        except Exception as exc:  # table absent, timeout, ...: fall through to SEC
            log.warning(
                "SEC velocity: edgar_submissions SIC read failed, using fallback only: {e}",
                e=exc,
            )
            self.local = {}
            return
        local: dict[int, str] = {}
        for cik, sic in rows:
            norm = _normalize_sic(sic)
            if norm is not None and cik is not None:
                local[int(cik)] = norm
        self.local = local
        log.info(
            "SEC velocity: {n} CIK SIC codes from edgar_submissions (as of {d})",
            n=len(local), d=as_of,
        )

    def _fallback_sic(self, cik: int) -> str | None:
        if not self.allow_fallback:
            return None
        if cik in self._fallback:
            return self._fallback[cik]
        if self._consecutive_failures >= self.max_consecutive_failures:
            if not self._breaker_logged:
                log.warning(
                    "SEC velocity: SIC fallback stopped after {n} consecutive errors; "
                    "remaining misses stay UNKNOWN",
                    n=self._consecutive_failures,
                )
                self._breaker_logged = True
            return None
        if self.fallback_lookups >= self.max_lookups:
            if not self._cap_logged:
                log.warning(
                    "SEC velocity: SIC fallback cap of {n} lookups reached; "
                    "remaining misses stay UNKNOWN",
                    n=self.max_lookups,
                )
                self._cap_logged = True
            return None
        self.fallback_lookups += 1
        try:
            sic = _normalize_sic(self._company_factory(cik).sic)
        except Exception as exc:
            self._consecutive_failures += 1
            log.warning("SEC velocity: SIC lookup failed for CIK {c}: {e}", c=cik, e=exc)
            # Not cached: a transient error may succeed later in the run.
            return None
        self._consecutive_failures = 0
        self._fallback[cik] = sic
        return sic

    def resolve(self, filing: Any) -> tuple[str | None, str]:
        """Return ``(sic, source)``, with source ``local``, ``sec`` or ``unknown``."""
        try:
            cik = int(getattr(filing, "cik"))
        except (AttributeError, TypeError, ValueError):
            return None, "unknown"
        sic = self.local.get(cik)
        if sic is not None:
            return sic, "local"
        sic = self._fallback_sic(cik)
        if sic is not None:
            return sic, "sec"
        return None, "unknown"


def _sector_for(resolver: SICResolver, filing: Any) -> tuple[str, str]:
    sic, source = resolver.resolve(filing)
    return (_sic_to_broad_sector(sic) if sic is not None else "UNKNOWN"), source


class SECVelocityPuller(BasePuller):
    """Counts 8-K filings by SIC sector code per week.

    Provides a filing velocity metric that can signal changes in
    corporate event frequency across sectors — useful for detecting
    regime shifts (e.g., spikes in financial sector 8-Ks during stress).

    Attributes:
        engine: SQLAlchemy engine for database writes.
        source_id: The ``source_catalog.id`` for SEC_EDGAR.
    """

    SOURCE_NAME = "SEC_EDGAR"
    SOURCE_CONFIG = {"base_url": "https://www.sec.gov", "cost_tier": "FREE", "latency_class": "EOD", "pit_available": True, "revision_behavior": "RARE", "trust_score": "HIGH", "priority_rank": 12}

    def __init__(
        self,
        db_engine: Engine,
        identity: str | None = None,
    ) -> None:
        """Initialise the SEC velocity puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
            identity: User-agent identity for SEC EDGAR compliance. Resolved by
                ingestion.edgar.resolve_sec_identity (SEC rejects
                "grid@localhost" with 403).
        """
        from ingestion.edgar import resolve_sec_identity

        set_identity(resolve_sec_identity(identity))
        super().__init__(db_engine)
        log.info("SECVelocityPuller initialised — source_id={sid}", sid=self.source_id)

    def _ensure_feature_registered(self, sector: str) -> None:
        """Register a SEC_VELOCITY feature if it doesn't exist.

        Parameters:
            sector: Broad sector label (e.g., 'FIRE', 'MANUFACTURING').
        """
        from normalization.entity_map import EntityMap

        # Use the canonical ingestion registration contract. Its metadata
        # defaults and name normalization are shared with other auto-writers.
        # Existing registry rows are never updated, including eligibility.
        mapper = getattr(self, "_feature_mapper", None)
        if mapper is None:
            mapper = self._feature_mapper = EntityMap(db_engine=self.engine)
        mapper.ensure_canonical_feature(
            f"SEC_VELOCITY:{sector}",
            family="alternative",
            model_eligible=True,  # preserve this producer's existing intent
        )

    def pull_weekly_velocity(
        self,
        weeks_back: int = 4,
    ) -> dict[str, Any]:
        """Pull recent 8-K filings and compute weekly sector velocity.

        Iterates over recent 8-K filings, groups by SIC sector and
        ISO week, and stores counts as SEC_VELOCITY:{sector} series.

        Parameters:
            weeks_back: Number of weeks to look back from today.

        Returns:
            dict: Result summary with rows_inserted and errors.
        """
        log.info("Computing SEC 8-K velocity (last {w} weeks)", w=weeks_back)
        total_inserted = 0
        errors: list[str] = []
        cutoff = date.today() - timedelta(weeks=weeks_back)

        # Collect filings with their SIC codes
        sector_week_counts: dict[tuple[str, date], int] = {}
        sic_sources: dict[tuple[str, date], Counter] = {}
        resolver = SICResolver(self.engine)
        resolver.load_local(as_of=date.today())

        try:
            filings = get_filings(form="8-K")
            if filings is None:
                return {
                    "type": "SEC_VELOCITY",
                    "rows_inserted": 0,
                    "errors": ["No 8-K filings returned"],
                    "status": "PARTIAL",
                }

            for filing in filings:
                try:
                    filing_date = (
                        filing.filing_date
                        if isinstance(filing.filing_date, date)
                        else pd.Timestamp(filing.filing_date).date()
                    )

                    if filing_date < cutoff:
                        break

                    sector, source = _sector_for(resolver, filing)

                    # Compute ISO week start (Monday)
                    week_start = filing_date - timedelta(days=filing_date.weekday())

                    key = (sector, week_start)
                    sector_week_counts[key] = sector_week_counts.get(key, 0) + 1
                    sic_sources.setdefault(key, Counter())[source] += 1

                except Exception as exc:
                    log.debug("Could not process 8-K filing: {e}", e=str(exc))
                    continue

        except Exception as exc:
            msg = f"8-K velocity pull failed: {exc}"
            log.error(msg)
            errors.append(msg)

        # Store results
        with self.engine.begin() as conn:
            for (sector, week_start), count in sector_week_counts.items():
                series_id = f"SEC_VELOCITY:{sector}"
                self._ensure_feature_registered(sector)

                # QUARANTINED (migration #671: wrong-instrument batch caught
                # after the fact) does not count as existing — allow a fresh
                # insert to replace it rather than silently skipping.
                existing = conn.execute(
                    text(
                        "SELECT 1 FROM raw_series "
                        "WHERE series_id = :sid AND source_id = :src "
                        "AND obs_date = :od AND pull_status != 'QUARANTINED' LIMIT 1"
                    ),
                    {
                        "sid": series_id,
                        "src": self.source_id,
                        "od": week_start,
                    },
                ).fetchone()

                if existing is None:
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
                            "od": week_start,
                            "val": float(count),
                            "payload": json.dumps({
                                "sector": sector,
                                "week_start": str(week_start),
                                "filing_count": count,
                                "sic_sources": dict(sic_sources.get((sector, week_start), {})),
                            }),
                        },
                    )
                    total_inserted += 1

        log.info(
            "SEC velocity complete — {n} rows, {s} sectors",
            n=total_inserted,
            s=len(set(s for s, _ in sector_week_counts.keys())),
        )
        return {
            "type": "SEC_VELOCITY",
            "rows_inserted": total_inserted,
            "sectors_found": len(set(s for s, _ in sector_week_counts.keys())),
            "errors": errors,
            "status": "SUCCESS" if not errors else "PARTIAL",
        }

    def pull_historical_velocity(
        self,
        start_year: int = 1995,
        end_year: int | None = None,
    ) -> dict[str, Any]:
        """Pull historical 8-K velocity year by year.

        Iterates year-by-year from start_year to present, pulling 8-K
        filing data and computing weekly sector velocity for each year.

        Parameters:
            start_year: First year to process (default: 1995).
            end_year: Last year to process (default: current year).

        Returns:
            dict: Aggregate result summary.
        """
        if end_year is None:
            end_year = date.today().year

        log.info(
            "Historical SEC velocity pull — {sy} to {ey}",
            sy=start_year, ey=end_year,
        )

        total_inserted = 0
        all_errors: list[str] = []
        # Point-in-time: the local table only, re-read per year as of that
        # year's end. No SEC fallback: Company(cik).sic is today's SIC, and
        # applying it to a past year is lookahead. Misses stay UNKNOWN.
        resolver = SICResolver(self.engine, allow_fallback=False)

        for year in range(start_year, end_year + 1):
            log.info("Processing year {y}", y=year)

            try:
                year_start = date(year, 1, 1)
                year_end = date(year, 12, 31) if year < date.today().year else date.today()

                # Module-level get_filings takes ``filing_date``
                # ("YYYY-MM-DD:YYYY-MM-DD"), not ``date`` (that keyword exists
                # only on Company.get_filings), in edgartools 5.x and 6.x
                # alike. ``date=`` raised TypeError for every year, so this
                # backfill wrote nothing.
                filings = get_filings(
                    form="8-K",
                    filing_date=f"{year_start}:{year_end}",
                )
                if filings is None:
                    continue

                resolver.load_local(as_of=year_end)
                sector_week_counts: dict[tuple[str, date], int] = {}
                sic_sources: dict[tuple[str, date], Counter] = {}

                for filing in filings:
                    try:
                        filing_date = (
                            filing.filing_date
                            if isinstance(filing.filing_date, date)
                            else pd.Timestamp(filing.filing_date).date()
                        )
                        sector, source = _sector_for(resolver, filing)
                        week_start = filing_date - timedelta(days=filing_date.weekday())
                        key = (sector, week_start)
                        sector_week_counts[key] = sector_week_counts.get(key, 0) + 1
                        sic_sources.setdefault(key, Counter())[source] += 1
                    except Exception as exc:
                        log.warning("SEC velocity filing parse failed: {e}", e=exc)
                        continue

                # Store year's results
                with self.engine.begin() as conn:
                    for (sector, week_start), count in sector_week_counts.items():
                        series_id = f"SEC_VELOCITY:{sector}"
                        self._ensure_feature_registered(sector)

                        # QUARANTINED does not count as existing — see the
                        # comment on the equivalent check above.
                        existing = conn.execute(
                            text(
                                "SELECT 1 FROM raw_series "
                                "WHERE series_id = :sid AND source_id = :src "
                                "AND obs_date = :od AND pull_status != 'QUARANTINED' LIMIT 1"
                            ),
                            {"sid": series_id, "src": self.source_id, "od": week_start},
                        ).fetchone()

                        if existing is None:
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
                                    "od": week_start,
                                    "val": float(count),
                                    "payload": json.dumps({
                                        "sector": sector,
                                        "week_start": str(week_start),
                                        "year": year,
                                        "sic_sources": dict(
                                            sic_sources.get((sector, week_start), {})
                                        ),
                                    }),
                                },
                            )
                            total_inserted += 1

                log.info(
                    "Year {y}: {n} sector-week entries",
                    y=year, n=len(sector_week_counts),
                )

            except Exception as exc:
                msg = f"Historical velocity failed for year {year}: {exc}"
                log.warning(msg)
                all_errors.append(msg)

            time.sleep(1)  # Polite delay between years

        log.info(
            "Historical SEC velocity complete — {n} total rows",
            n=total_inserted,
        )
        return {
            "type": "SEC_VELOCITY_HISTORICAL",
            "rows_inserted": total_inserted,
            "years_processed": end_year - start_year + 1,
            "errors": all_errors,
            "status": "SUCCESS" if not all_errors else "PARTIAL",
        }


if __name__ == "__main__":
    from db import get_engine

    puller = SECVelocityPuller(db_engine=get_engine())

    # Recent velocity
    result = puller.pull_weekly_velocity(weeks_back=2)
    print(f"Weekly velocity: {result}")
