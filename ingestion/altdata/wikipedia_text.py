"""
Wikipedia Pageviews puller — attention anomaly detection.

Uses the Wikimedia REST API to track pageviews for financially
relevant entities and detect anomalous spikes (Z-score > 3).

API docs: https://wikimedia.org/api/rest_v1/
No authentication required.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# Top entities to track — map display name → Wikipedia article title
WATCHLIST: dict[str, str] = {
    # Tech
    "Apple": "Apple_Inc.", "Google": "Alphabet_Inc.", "Microsoft": "Microsoft",
    "Amazon": "Amazon_(company)", "Meta": "Meta_Platforms", "Nvidia": "Nvidia",
    "Tesla": "Tesla,_Inc.", "Netflix": "Netflix", "AMD": "Advanced_Micro_Devices",
    "Intel": "Intel", "Salesforce": "Salesforce", "Oracle": "Oracle_Corporation",
    # Finance
    "JPMorgan": "JPMorgan_Chase", "Goldman Sachs": "Goldman_Sachs",
    "BlackRock": "BlackRock", "Citadel": "Citadel_LLC",
    "Berkshire Hathaway": "Berkshire_Hathaway", "Morgan Stanley": "Morgan_Stanley",
    # Energy
    "ExxonMobil": "ExxonMobil", "Chevron": "Chevron_Corporation",
    "Saudi Aramco": "Saudi_Aramco", "Shell": "Shell_plc",
    # Pharma
    "Pfizer": "Pfizer", "Moderna": "Moderna", "Johnson & Johnson": "Johnson_%26_Johnson",
    "Eli Lilly": "Eli_Lilly_and_Company", "Novo Nordisk": "Novo_Nordisk",
    # People
    "Elon Musk": "Elon_Musk", "Warren Buffett": "Warren_Buffett",
    "Jamie Dimon": "Jamie_Dimon", "Jerome Powell": "Jerome_Powell",
    "Larry Fink": "Larry_Fink", "Janet Yellen": "Janet_Yellen",
    "Tim Cook": "Tim_Cook", "Satya Nadella": "Satya_Nadella",
    "Jensen Huang": "Jensen_Huang", "Sam Altman": "Sam_Altman",
    # Geopolitical
    "Federal Reserve": "Federal_Reserve", "SEC": "U.S._Securities_and_Exchange_Commission",
    "NATO": "NATO", "OPEC": "OPEC", "IMF": "International_Monetary_Fund",
    "World Bank": "World_Bank", "Bitcoin": "Bitcoin", "Ethereum": "Ethereum",
    # Sectors
    "S&P 500": "S%26P_500", "Nasdaq": "Nasdaq", "Dow Jones": "Dow_Jones_Industrial_Average",
}

WIKI_API = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"


class WikipediaPuller(BasePuller):
    """Track Wikipedia pageviews and detect attention anomalies."""

    SOURCE_NAME = "wikipedia_pageviews"
    SOURCE_CONFIG = {
        "base_url": "https://wikimedia.org",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "MED",
        "priority_rank": 45,
    }

    @retry_on_failure(max_attempts=3)
    def _fetch_pageviews(
        self,
        article: str,
        start_date: date,
        end_date: date,
    ) -> list[dict[str, Any]]:
        """Fetch daily pageviews for a Wikipedia article.

        Args:
            article: Wikipedia article title (URL-encoded).
            start_date: Start date.
            end_date: End date.

        Returns:
            List of {date, views} dicts.
        """
        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")

        url = (
            f"{WIKI_API}/en.wikipedia/all-access/all-agents/"
            f"{article}/daily/{start_str}00/{end_str}00"
        )

        resp = requests.get(url, timeout=30, headers={"User-Agent": "GRID/1.0"})
        resp.raise_for_status()
        data = resp.json()

        results: list[dict[str, Any]] = []
        for item in data.get("items", []):
            ts = item.get("timestamp", "")
            views = item.get("views", 0)
            if ts and len(ts) >= 8:
                d = datetime.strptime(ts[:8], "%Y%m%d").date()
                results.append({"date": d, "views": views})

        return results

    def pull(self, lookback_days: int = 90) -> dict[str, Any]:
        """Pull pageviews for all watchlist entities and detect anomalies.

        Args:
            lookback_days: Days of history to fetch.

        Returns:
            Summary with entity counts and anomalies found.
        """
        import numpy as np

        end_date = date.today() - timedelta(days=1)  # yesterday (today may be incomplete)
        start_date = end_date - timedelta(days=lookback_days)

        anomalies: list[dict[str, Any]] = []
        entities_pulled = 0

        for display_name, article_title in WATCHLIST.items():
            try:
                data = self._fetch_pageviews(article_title, start_date, end_date)
                if len(data) < 30:
                    continue

                entities_pulled += 1

                # Store raw pageviews
                with self.engine.begin() as conn:
                    existing = self._get_existing_dates(f"wiki:{display_name}", conn)
                    for point in data:
                        if point["date"] not in existing:
                            self._insert_raw(
                                conn,
                                series_id=f"wiki:{display_name}",
                                obs_date=point["date"],
                                value=float(point["views"]),
                                raw_payload={"article": article_title, "source": "wikipedia"},
                            )

                # Z-score anomaly detection on last 7 days
                views = np.array([d["views"] for d in data], dtype=float)
                rolling_mean = np.mean(views[:-7]) if len(views) > 7 else np.mean(views)
                rolling_std = np.std(views[:-7]) if len(views) > 7 else np.std(views)

                if rolling_std > 0:
                    for point in data[-7:]:
                        z = (point["views"] - rolling_mean) / rolling_std
                        if abs(z) > 3.0:
                            anomalies.append({
                                "entity": display_name,
                                "date": point["date"],
                                "views": point["views"],
                                "z_score": round(z, 2),
                                "mean_30d": round(rolling_mean, 0),
                            })

            except Exception as exc:
                log.debug("Wikipedia pull failed for {e}: {err}", e=display_name, err=str(exc))

        # Store anomalies
        if anomalies:
            self._store_anomalies(anomalies)

        log.info("Wikipedia pull: {e} entities, {a} anomalies", e=entities_pulled, a=len(anomalies))
        return {"entities_pulled": entities_pulled, "anomalies": anomalies}

    def _store_anomalies(self, anomalies: list[dict[str, Any]]) -> None:
        with self.engine.begin() as conn:
            for a in anomalies:
                # Cast numpy scalars to Python floats — psycopg2 doesn't adapt
                # np.float64 and would otherwise emit repr like `np.float64(x)`
                # into the SQL, which parses as schema-qualified `np.float64`
                # and blows up with InvalidSchemaName.
                z_val = float(a["z_score"])
                score_val = float(min(abs(z_val) * 20, 100))
                conn.execute(
                    text(
                        "INSERT INTO attention_anomaly "
                        "(entity_name, anomaly_date, wikipedia_zscore, combined_score, source) "
                        "VALUES (:name, :dt, :z, :score, 'wikipedia') "
                        "ON CONFLICT DO NOTHING"
                    ),
                    {
                        "name": a["entity"],
                        "dt": a["date"],
                        "z": z_val,
                        "score": score_val,
                    },
                )
