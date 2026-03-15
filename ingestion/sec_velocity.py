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
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
from edgar import get_filings, set_identity
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

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


class SECVelocityPuller:
    """Counts 8-K filings by SIC sector code per week.

    Provides a filing velocity metric that can signal changes in
    corporate event frequency across sectors — useful for detecting
    regime shifts (e.g., spikes in financial sector 8-Ks during stress).

    Attributes:
        engine: SQLAlchemy engine for database writes.
        source_id: The ``source_catalog.id`` for SEC_EDGAR.
    """

    def __init__(
        self,
        db_engine: Engine,
        identity: str = "GRID Trading System grid@localhost",
    ) -> None:
        """Initialise the SEC velocity puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
            identity: User-agent identity for SEC EDGAR compliance.
        """
        set_identity(identity)
        self.engine = db_engine
        self.source_id = self._resolve_source_id()
        log.info("SECVelocityPuller initialised — source_id={sid}", sid=self.source_id)

    def _resolve_source_id(self) -> int:
        """Look up or create the SEC_EDGAR source."""
        with self.engine.begin() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": "SEC_EDGAR"},
            ).fetchone()
            if row is not None:
                return row[0]

            result = conn.execute(
                text(
                    "INSERT INTO source_catalog (name, base_url, pull_frequency, "
                    "trust_score, priority_rank, active) "
                    "VALUES (:name, :url, :freq, :trust, :prio, TRUE) "
                    "RETURNING id"
                ),
                {
                    "name": "SEC_EDGAR",
                    "url": "https://www.sec.gov/cgi-bin/browse-edgar",
                    "freq": "weekly",
                    "trust": "OFFICIAL",
                    "prio": 2,
                },
            )
            return result.fetchone()[0]

    def _ensure_feature_registered(self, sector: str) -> None:
        """Register a SEC_VELOCITY feature if it doesn't exist.

        Parameters:
            sector: Broad sector label (e.g., 'FIRE', 'MANUFACTURING').
        """
        feature_name = f"SEC_VELOCITY:{sector}"
        with self.engine.begin() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM feature_registry WHERE name = :name"),
                {"name": feature_name},
            ).fetchone()
            if exists is None:
                conn.execute(
                    text(
                        "INSERT INTO feature_registry "
                        "(name, family, source_series_id, normalization, "
                        "lag_days, model_eligible) "
                        "VALUES (:name, :family, :ssid, :norm, :lag, :elig)"
                    ),
                    {
                        "name": feature_name,
                        "family": "sec_velocity",
                        "ssid": feature_name,
                        "norm": "ZSCORE",
                        "lag": 0,
                        "elig": True,
                    },
                )
                log.info("Registered feature: {f}", f=feature_name)

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

                    # Get SIC code from the filing's company
                    sic_code = getattr(filing, "sic", None)
                    if sic_code is None:
                        # Try to get from company info
                        sic_code = getattr(filing, "sic_code", "0000")
                    if sic_code is None:
                        sic_code = "0000"

                    sector = _sic_to_broad_sector(sic_code)

                    # Compute ISO week start (Monday)
                    week_start = filing_date - timedelta(days=filing_date.weekday())

                    key = (sector, week_start)
                    sector_week_counts[key] = sector_week_counts.get(key, 0) + 1

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

                existing = conn.execute(
                    text(
                        "SELECT 1 FROM raw_series "
                        "WHERE series_id = :sid AND source_id = :src "
                        "AND obs_date = :od LIMIT 1"
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

        for year in range(start_year, end_year + 1):
            log.info("Processing year {y}", y=year)

            try:
                year_start = date(year, 1, 1)
                year_end = date(year, 12, 31) if year < date.today().year else date.today()

                filings = get_filings(
                    form="8-K",
                    date=f"{year_start}:{year_end}",
                )
                if filings is None:
                    continue

                sector_week_counts: dict[tuple[str, date], int] = {}

                for filing in filings:
                    try:
                        filing_date = (
                            filing.filing_date
                            if isinstance(filing.filing_date, date)
                            else pd.Timestamp(filing.filing_date).date()
                        )
                        sic_code = getattr(filing, "sic", getattr(filing, "sic_code", "0000"))
                        if sic_code is None:
                            sic_code = "0000"

                        sector = _sic_to_broad_sector(sic_code)
                        week_start = filing_date - timedelta(days=filing_date.weekday())
                        key = (sector, week_start)
                        sector_week_counts[key] = sector_week_counts.get(key, 0) + 1
                    except Exception:
                        continue

                # Store year's results
                with self.engine.begin() as conn:
                    for (sector, week_start), count in sector_week_counts.items():
                        series_id = f"SEC_VELOCITY:{sector}"
                        self._ensure_feature_registered(sector)

                        existing = conn.execute(
                            text(
                                "SELECT 1 FROM raw_series "
                                "WHERE series_id = :sid AND source_id = :src "
                                "AND obs_date = :od LIMIT 1"
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
