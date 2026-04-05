"""
QuiverQuant Pro Bulk Downloader — pull everything we're paying for.

Congressional trading, insider filings, lobbying, corporate flights,
WSB sentiment. All stored in raw_series with actor auto-discovery.

Run: python scripts/quiver_bulk_download.py
"""

from __future__ import annotations

import os
import sys
import time
from datetime import date, datetime
from typing import Any

import requests
from loguru import logger as log

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_engine
from sqlalchemy import text
from ingestion.base import BasePuller

QQ_BASE = "https://api.quiverquant.com"
DELAY = 0.3


class QQBulkPuller(BasePuller):
    SOURCE_NAME = "quiverquant_bulk"
    SOURCE_CONFIG = {
        "base_url": QQ_BASE,
        "cost_tier": "PAID",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 8,
    }

    def __init__(self, engine, api_key: str):
        super().__init__(engine)
        self.api_key = api_key
        self.headers = {"Authorization": f"Token {api_key}", "Accept": "application/json"}
        self.calls = 0

    def _qq_get(self, endpoint: str) -> list[dict]:
        resp = requests.get(f"{QQ_BASE}/{endpoint}", headers=self.headers, timeout=30)
        self.calls += 1
        time.sleep(DELAY)
        if resp.status_code != 200:
            log.debug("QQ {ep}: {s}", ep=endpoint, s=resp.status_code)
            return []
        data = resp.json()
        return data if isinstance(data, list) else []

    def pull_congressional(self) -> int:
        """Pull all congressional trading data."""
        data = self._qq_get("beta/live/congresstrading")
        stored = 0
        with self.engine.begin() as conn:
            for r in data:
                try:
                    ticker = r.get("Ticker", "")
                    rep = r.get("Representative", "")
                    txn_date = r.get("TransactionDate", "")
                    if not txn_date:
                        continue
                    obs = datetime.strptime(txn_date[:10], "%Y-%m-%d").date()
                    amount = float(r.get("Amount", 0) or 0)

                    try:
                        try:
                        self._insert_raw(conn,
                            series_id=f"qq:congress:{rep}:{ticker}",
                            obs_date=obs, value=amount,
                            raw_payload=r)
                        stored += 1
                    except Exception:
                        pass  # skip dupes

                    # Auto-discover congresspeople as actors
                    try:
                        from intelligence.actor_ingest import ingest_actor
                        ingest_actor(self.engine, rep, "person", source="quiverquant",
                                     metadata={"role": "congress", "party": r.get("Party", "")})
                    except Exception:
                        pass
                except (ValueError, TypeError):
                    pass
        return stored

    def pull_insiders(self) -> int:
        """Pull insider trading filings."""
        data = self._qq_get("beta/live/insiders")
        stored = 0
        with self.engine.begin() as conn:
            for r in data:
                try:
                    ticker = r.get("Ticker", "")
                    name = r.get("Name", "")
                    txn_date = r.get("Date", "")
                    if not txn_date:
                        continue
                    obs = datetime.strptime(txn_date[:10], "%Y-%m-%d").date()
                    shares = float(r.get("Shares", 0) or 0)
                    value = float(r.get("Value", 0) or 0)

                    try:
                        self._insert_raw(conn,
                        series_id=f"qq:insider:{ticker}:{name}",
                        obs_date=obs, value=value,
                        raw_payload=r)
                    stored += 1

                    try:
                        from intelligence.actor_ingest import ingest_actor
                        ingest_actor(self.engine, name, "person", source="quiverquant",
                                     metadata={"role": "insider", "title": r.get("Title", "")})
                    except Exception:
                        pass
                except (ValueError, TypeError):
                    pass
        return stored

    def pull_lobbying(self) -> int:
        """Pull lobbying disclosure data."""
        data = self._qq_get("beta/live/lobbying")
        stored = 0
        with self.engine.begin() as conn:
            for r in data:
                try:
                    client = r.get("Client", "")
                    txn_date = r.get("Date", "")
                    if not txn_date:
                        continue
                    obs = datetime.strptime(txn_date[:10], "%Y-%m-%d").date()
                    amount = float(r.get("Amount", 0) or 0)

                    try:
                        self._insert_raw(conn,
                        series_id=f"qq:lobbying:{client}",
                        obs_date=obs, value=amount,
                        raw_payload=r)
                    stored += 1

                    try:
                        from intelligence.actor_ingest import ingest_actor
                        ingest_actor(self.engine, client, "company", source="quiverquant",
                                     metadata={"lobby_amount": amount, "issue": r.get("Issue", "")})
                    except Exception:
                        pass
                except (ValueError, TypeError):
                    pass
        return stored

    def pull_flights(self) -> int:
        """Pull corporate jet tracking data."""
        data = self._qq_get("beta/live/flights")
        stored = 0
        with self.engine.begin() as conn:
            for r in data:
                try:
                    ticker = r.get("Ticker", "")
                    flight_date = r.get("Date", "")
                    if not flight_date:
                        continue
                    obs = datetime.strptime(flight_date[:10], "%Y-%m-%d").date()
                    try:
                        self._insert_raw(conn,
                        series_id=f"qq:flight:{ticker}",
                        obs_date=obs, value=1.0,
                        raw_payload=r)
                    stored += 1
                except (ValueError, TypeError):
                    pass
        return stored

    def pull_wsb(self) -> int:
        """Pull WallStreetBets mention counts + sentiment."""
        data = self._qq_get("beta/live/wallstreetbets")
        stored = 0
        today = date.today()
        with self.engine.begin() as conn:
            for r in data:
                try:
                    ticker = r.get("Ticker", "")
                    count = float(r.get("Count", 0) or 0)
                    sentiment = float(r.get("Sentiment", 0) or 0)

                    try:
                        self._insert_raw(conn,
                        series_id=f"qq:wsb:{ticker}:mentions",
                        obs_date=today, value=count)
                    try:
                        self._insert_raw(conn,
                        series_id=f"qq:wsb:{ticker}:sentiment",
                        obs_date=today, value=sentiment,
                        raw_payload=r)
                    stored += 2
                except (ValueError, TypeError):
                    pass
        return stored


def main():
    api_key = os.environ.get("QUIVERQUANT_API_KEY", "")
    if not api_key:
        # Try from .env
        for line in open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")):
            if line.startswith("QUIVERQUANT_API_KEY="):
                api_key = line.strip().split("=", 1)[1]
    if not api_key:
        print("QUIVERQUANT_API_KEY not found")
        return

    engine = get_engine()
    puller = QQBulkPuller(engine, api_key)
    total = 0
    start = time.time()

    print("=" * 60)
    print("QUIVERQUANT BULK DOWNLOAD")
    print("=" * 60)

    for name, method in [
        ("Congressional Trading", puller.pull_congressional),
        ("Insider Filings", puller.pull_insiders),
        ("Lobbying", puller.pull_lobbying),
        ("Corporate Flights", puller.pull_flights),
        ("WSB Sentiment", puller.pull_wsb),
    ]:
        n = method()
        total += n
        print(f"  {name}: {n:,} rows")

    elapsed = time.time() - start
    print()
    print(f"DONE: {total:,} rows, {puller.calls} API calls in {elapsed:.0f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
