"""
Google Trends puller — search interest anomaly detection.

Uses pytrends (unofficial Google Trends API) to track search interest
for financially relevant entities and detect breakout signals.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# Reuse Wikipedia watchlist for consistency
from ingestion.altdata.wikipedia_puller import WATCHLIST


class GoogleTrendsPuller(BasePuller):
    """Track Google Trends interest and detect breakout signals."""

    SOURCE_NAME = "google_trends"
    SOURCE_CONFIG = {
        "base_url": "https://trends.google.com",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "FREQUENT",
        "trust_score": "MED",
        "priority_rank": 46,
    }

    def pull(self, lookback_days: int = 180) -> dict[str, Any]:
        """Pull Google Trends data for watchlist entities.

        Args:
            lookback_days: Days of history to fetch.

        Returns:
            Summary with entity counts and breakouts found.
        """
        try:
            from pytrends.request import TrendReq
        except ImportError:
            log.warning("pytrends not installed — pip install pytrends")
            return {"error": "pytrends not installed", "entities_pulled": 0}

        pytrends = TrendReq(hl="en-US", tz=360, timeout=(10, 25))
        end_date = date.today()
        start_date = end_date - timedelta(days=lookback_days)
        timeframe = f"{start_date.isoformat()} {end_date.isoformat()}"

        breakouts: list[dict[str, Any]] = []
        entities_pulled = 0

        # Process in batches of 5 (Google Trends limit)
        entity_names = list(WATCHLIST.keys())
        for i in range(0, len(entity_names), 5):
            batch = entity_names[i:i + 5]

            try:
                pytrends.build_payload(batch, timeframe=timeframe)
                df = pytrends.interest_over_time()

                if df.empty:
                    continue

                for entity in batch:
                    if entity not in df.columns:
                        continue

                    series = df[entity]
                    entities_pulled += 1

                    # Store raw trends
                    with self.engine.begin() as conn:
                        existing = self._get_existing_dates(f"trends:{entity}", conn)
                        for idx, value in series.items():
                            obs_date = idx.date() if hasattr(idx, "date") else idx
                            if obs_date not in existing:
                                self._insert_raw(
                                    conn,
                                    series_id=f"trends:{entity}",
                                    obs_date=obs_date,
                                    value=float(value),
                                    raw_payload={"source": "google_trends"},
                                )

                    # Breakout detection: current > 2x 90-day average
                    if len(series) > 12:
                        recent_avg = float(series[-4:].mean())   # last 4 weeks
                        baseline = float(series[:-4].mean())

                        if baseline > 0 and recent_avg > baseline * 2:
                            breakouts.append({
                                "entity": entity,
                                "recent_avg": round(recent_avg, 1),
                                "baseline_avg": round(baseline, 1),
                                "breakout_ratio": round(recent_avg / baseline, 2),
                            })

            except Exception as exc:
                log.debug("Google Trends batch failed for {b}: {e}", b=batch, e=str(exc))

        # Store breakouts
        if breakouts:
            self._store_breakouts(breakouts)

        log.info("Google Trends: {e} entities, {b} breakouts",
                 e=entities_pulled, b=len(breakouts))
        return {"entities_pulled": entities_pulled, "breakouts": breakouts}

    def _store_breakouts(self, breakouts: list[dict[str, Any]]) -> None:
        with self.engine.begin() as conn:
            for b in breakouts:
                conn.execute(
                    text(
                        "INSERT INTO attention_anomaly "
                        "(entity_name, anomaly_date, trends_breakout, combined_score, source) "
                        "VALUES (:name, :dt, :ratio, :score, 'trends')"
                    ),
                    {
                        "name": b["entity"],
                        "dt": date.today(),
                        "ratio": b["breakout_ratio"],
                        "score": min(b["breakout_ratio"] * 25, 100),
                    },
                )
