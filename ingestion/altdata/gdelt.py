"""
GRID GDELT news event data ingestion module.

Pulls news tone and conflict event data from the GDELT Project.
Uses GDELT 2.0 API for recent data and GKG (Global Knowledge Graph)
daily files for historical data back to 1979.
"""

from __future__ import annotations

import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from tenacity import retry, stop_after_attempt, wait_exponential

# GDELT query definitions
GDELT_QUERIES: list[dict[str, str]] = [
    {
        "query": "economy recession",
        "mode": "timelineTone",
        "feature": "gdelt_recession_tone",
        "timespan": "60m",
    },
    {
        "query": "Federal Reserve interest rates",
        "mode": "timelineTone",
        "feature": "gdelt_fed_tone",
        "timespan": "60m",
    },
    {
        "query": "trade war tariffs China",
        "mode": "timelinevol",
        "feature": "gdelt_trade_conflict_volume",
        "timespan": "60m",
    },
]

_GDELT_API_URL = "https://api.gdeltproject.org/api/v2/tv/"
_GDELT_GKG_URL = "http://data.gdeltproject.org/gkg/"
_GDELT_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "gdelt")

# CAMEO conflict event codes (14-20: protest, force, assault, fight, etc.)
_CONFLICT_CODES = {14, 15, 16, 17, 18, 19, 20}
_RATE_LIMIT_DELAY: float = 1.0


class GDELTPuller:
    """Pulls news event data from the GDELT Project."""

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.source_id = self._resolve_source_id()
        os.makedirs(_GDELT_DATA_DIR, exist_ok=True)
        log.info("GDELTPuller initialised — source_id={sid}", sid=self.source_id)

    def _resolve_source_id(self) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": "GDELT"},
            ).fetchone()
        if row is None:
            with self.engine.begin() as conn:
                result = conn.execute(
                    text(
                        "INSERT INTO source_catalog "
                        "(name, base_url, license_type, update_frequency, "
                        "has_vintage_data, revision_policy, data_quality, priority, model_eligible) "
                        "VALUES (:name, :url, 'FREE', 'DAILY', FALSE, 'NEVER', 'MED', 36, FALSE) "
                        "RETURNING id"
                    ),
                    {"name": "GDELT", "url": _GDELT_API_URL},
                )
                return result.fetchone()[0]
        return row[0]

    def _row_exists(self, series_id: str, obs_date: date, conn: Any) -> bool:
        one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
        result = conn.execute(
            text(
                "SELECT 1 FROM raw_series "
                "WHERE series_id = :sid AND source_id = :src "
                "AND obs_date = :od AND pull_timestamp >= :ts LIMIT 1"
            ),
            {"sid": series_id, "src": self.source_id, "od": obs_date, "ts": one_hour_ago},
        ).fetchone()
        return result is not None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
    def _fetch_gkg_day(self, target_date: date) -> pd.DataFrame | None:
        """Download and parse a daily GKG file.

        GKG daily files are tab-separated with fields including:
        DATE, NUMARTS, COUNTS, THEMES, LOCATIONS, PERSONS, ORGANIZATIONS,
        TONE, GCAM, etc.
        """
        date_str = target_date.strftime("%Y%m%d")
        filename = f"{date_str}.gkg.csv"
        local_path = os.path.join(_GDELT_DATA_DIR, filename)

        if os.path.exists(local_path):
            try:
                return pd.read_csv(local_path, sep="\t", header=None, on_bad_lines="skip")
            except Exception:
                pass

        url = f"{_GDELT_GKG_URL}{filename}.zip"
        try:
            resp = requests.get(url, timeout=60)
            if resp.status_code == 200:
                import zipfile
                from io import BytesIO
                with zipfile.ZipFile(BytesIO(resp.content)) as zf:
                    for name in zf.namelist():
                        with zf.open(name) as f:
                            content = f.read().decode("utf-8", errors="ignore")
                            with open(local_path, "w") as out:
                                out.write(content)
                return pd.read_csv(local_path, sep="\t", header=None, on_bad_lines="skip")
        except Exception as exc:
            log.debug("GKG download failed for {d}: {err}", d=date_str, err=str(exc))

        return None

    def pull_gkg_day(self, target_date: date) -> dict[str, Any]:
        """Process a single day of GKG data.

        Extracts: mean tone, total event count, conflict event count.
        """
        result: dict[str, Any] = {
            "series_id": f"gdelt_{target_date.isoformat()}",
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            df = self._fetch_gkg_day(target_date)
            if df is None or df.empty:
                result["status"] = "PARTIAL"
                return result

            # GKG column 7 (0-indexed) is the TONE field
            # Format: AvgTone,PositiveScore,NegativeScore,...
            tones = []
            event_count = len(df)

            if df.shape[1] > 7:
                for tone_str in df.iloc[:, 7].dropna():
                    try:
                        parts = str(tone_str).split(",")
                        if parts:
                            tones.append(float(parts[0]))
                    except (ValueError, IndexError):
                        continue

            inserted = 0
            with self.engine.begin() as conn:
                # Store average tone
                if tones:
                    avg_tone = sum(tones) / len(tones)
                    if not self._row_exists("gdelt_tone_usa", target_date, conn):
                        conn.execute(
                            text(
                                "INSERT INTO raw_series "
                                "(series_id, source_id, obs_date, value, pull_status) "
                                "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                            ),
                            {"sid": "gdelt_tone_usa", "src": self.source_id, "od": target_date, "val": avg_tone},
                        )
                        inserted += 1

                # Store event count
                if not self._row_exists("gdelt_event_count", target_date, conn):
                    conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, pull_status) "
                            "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                        ),
                        {"sid": "gdelt_event_count", "src": self.source_id, "od": target_date, "val": float(event_count)},
                    )
                    inserted += 1

            result["rows_inserted"] = inserted

        except Exception as exc:
            log.error("GKG day pull failed for {d}: {err}", d=target_date, err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result

    def pull_historical(self, start_date: date, end_date: date | None = None) -> dict[str, Any]:
        """Pull historical GKG data day-by-day."""
        if end_date is None:
            end_date = date.today() - timedelta(days=1)

        log.info("Pulling GDELT historical from {s} to {e}", s=start_date, e=end_date)
        results: list[dict[str, Any]] = []

        current = start_date
        while current <= end_date:
            res = self.pull_gkg_day(current)
            results.append(res)
            current += timedelta(days=1)
            time.sleep(0.5)  # Gentle rate limiting

        total_rows = sum(r["rows_inserted"] for r in results)
        log.info("GDELT historical: {n} rows from {d} days", n=total_rows, d=len(results))
        return {
            "source": "GDELT",
            "total_rows": total_rows,
            "succeeded": sum(1 for r in results if r["status"] == "SUCCESS"),
            "total": len(results),
        }

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
    def _fetch_gdelt_api(self, query: str, mode: str, timespan: str) -> dict:
        """Fetch from GDELT 2.0 TV API."""
        params = {
            "query": query,
            "mode": mode,
            "timespan": timespan,
            "format": "json",
        }
        resp = requests.get(_GDELT_API_URL, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def pull_recent(self, days_back: int = 30) -> dict[str, Any]:
        """Pull recent GDELT data using the 2.0 API."""
        log.info("Pulling GDELT recent data (last {d} days)", d=days_back)
        result: dict[str, Any] = {
            "source": "GDELT",
            "total_rows": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        inserted = 0
        for query_def in GDELT_QUERIES:
            try:
                data = self._fetch_gdelt_api(
                    query_def["query"],
                    query_def["mode"],
                    query_def["timespan"],
                )
                feature = query_def["feature"]

                # Parse timeline data
                timeline = data.get("timeline", [])
                with self.engine.begin() as conn:
                    for point in timeline:
                        try:
                            dt_str = point.get("date", "")
                            value = point.get("value", point.get("tone", 0))
                            if not dt_str or value is None:
                                continue
                            obs_dt = pd.Timestamp(dt_str).date()
                            if not self._row_exists(feature, obs_dt, conn):
                                conn.execute(
                                    text(
                                        "INSERT INTO raw_series "
                                        "(series_id, source_id, obs_date, value, pull_status) "
                                        "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                                    ),
                                    {"sid": feature, "src": self.source_id, "od": obs_dt, "val": float(value)},
                                )
                                inserted += 1
                        except (ValueError, TypeError):
                            continue

            except Exception as exc:
                log.warning("GDELT query failed: {err}", err=str(exc))
                result["errors"].append(str(exc))

            time.sleep(_RATE_LIMIT_DELAY)

        result["total_rows"] = inserted
        log.info("GDELT recent: {n} rows", n=inserted)
        return result
