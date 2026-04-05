"""
pmxt Archive puller — free hourly Parquet snapshots of prediction market data.

Downloads historical orderbook and trade data from Polymarket + Kalshi
in bulk Parquet format. Perfect for backtesting.

Source: https://archive.pmxt.dev/
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

_ARCHIVE_BASE = "https://archive.pmxt.dev"
_DOWNLOAD_DIR = Path("/data/grid/pmxt_archive")
_REQUEST_TIMEOUT = 60


class PmxtArchivePuller(BasePuller):
    """Downloads bulk prediction market data from pmxt archive."""

    SOURCE_NAME: str = "PMXT_ARCHIVE"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://archive.pmxt.dev",
        "cost_tier": "FREE",
        "latency_class": "DAILY",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "MED",
        "priority_rank": 16,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        _DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    @retry_on_failure(max_attempts=2)
    def pull_all(self, days_back: int = 7, **kwargs) -> list[dict[str, Any]]:
        """Download recent Parquet snapshots from pmxt archive."""
        result: dict[str, Any] = {"rows_inserted": 0, "files_downloaded": 0, "status": "SUCCESS"}

        try:
            # Try to fetch the index/listing page
            resp = requests.get(
                f"{_ARCHIVE_BASE}/api/files",
                timeout=_REQUEST_TIMEOUT,
                headers={"Accept": "application/json"},
            )

            if resp.status_code != 200:
                # Fallback: try direct date-based paths
                log.info("pmxt archive API not available, trying date-based paths")
                for days_ago in range(days_back):
                    dt = date.today() - timedelta(days=days_ago)
                    date_str = dt.strftime("%Y-%m-%d")

                    for exchange in ["polymarket", "kalshi"]:
                        url = f"{_ARCHIVE_BASE}/{exchange}/{date_str}.parquet"
                        try:
                            r = requests.get(url, timeout=_REQUEST_TIMEOUT)
                            if r.status_code == 200:
                                fpath = _DOWNLOAD_DIR / f"{exchange}_{date_str}.parquet"
                                fpath.write_bytes(r.content)
                                result["files_downloaded"] += 1
                                log.info("Downloaded {f}", f=fpath.name)
                        except Exception as exc:
                            log.warning("PMXT archive download failed for {e}/{d}: {err}", e=exchange, d=date_str, err=exc)
                            continue

                if result["files_downloaded"] == 0:
                    result["status"] = "PARTIAL"
                return [result]

            files = resp.json()
            if not files:
                result["status"] = "PARTIAL"
                return [result]

            # Download recent files
            for file_info in files[:20]:
                url = file_info.get("url") or f"{_ARCHIVE_BASE}/{file_info.get('path', '')}"
                fname = file_info.get("name", "unknown.parquet")

                try:
                    r = requests.get(url, timeout=_REQUEST_TIMEOUT)
                    if r.status_code == 200:
                        fpath = _DOWNLOAD_DIR / fname
                        fpath.write_bytes(r.content)
                        result["files_downloaded"] += 1
                except Exception as exc:
                    log.warning("PMXT file download failed for {f}: {e}", f=fname, e=exc)
                    continue

            # Load any downloaded Parquet files into raw_series
            parquet_files = list(_DOWNLOAD_DIR.glob("*.parquet"))
            for pf in parquet_files:
                try:
                    df = pd.read_parquet(pf)
                    if "question" in df.columns and "price" in df.columns:
                        inserted = self._load_parquet(df)
                        result["rows_inserted"] += inserted
                except Exception as e:
                    log.debug("Failed to load {f}: {e}", f=pf.name, e=str(e))

            log.info(
                "pmxt archive: {f} files, {r} rows",
                f=result["files_downloaded"], r=result["rows_inserted"],
            )

        except Exception as exc:
            log.error("pmxt archive pull failed: {e}", e=str(exc))
            result["status"] = "FAILED"
            result["error"] = str(exc)

        return [result]

    def _load_parquet(self, df: pd.DataFrame) -> int:
        """Load a prediction market Parquet DataFrame into raw_series."""
        inserted = 0
        today = date.today()

        with self.engine.begin() as conn:
            for _, row in df.head(500).iterrows():
                question = str(row.get("question", row.get("title", "")))[:100]
                price = float(row.get("price", row.get("yes_price", 0.5)))

                if not question:
                    continue

                import hashlib
                q_hash = hashlib.md5(question.encode()).hexdigest()[:12]
                series_id = f"PMXT:{q_hash}"

                obs_date = row.get("date", today)
                if hasattr(obs_date, "date"):
                    obs_date = obs_date.date()

                conn.execute(
                    text(
                        "INSERT INTO raw_series "
                        "(series_id, source_id, obs_date, value, pull_status) "
                        "VALUES (:sid, :src, :od, :val, 'SUCCESS') "
                        "ON CONFLICT (series_id, source_id, obs_date, pull_timestamp) DO NOTHING"
                    ),
                    {"sid": series_id, "src": self.source_id, "od": obs_date, "val": price},
                )
                inserted += 1

        return inserted
