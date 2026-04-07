"""
LittleSis power-mapping puller -- board seats, donations, lobbying ties.

Searches top financial actors via the free LittleSis API, pulls their
relationships, stores each as a raw_series row with series_id format:
``littlesis.{entity_slug}.{relationship_type}``
"""

from __future__ import annotations

import re
import time
from datetime import date
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

_SEARCH_URL = "https://littlesis.org/api/entities/search"
_RELS_URL = "https://littlesis.org/api/entities/{id}/relationships"
_RATE_LIMIT: float = 1.0
_TIMEOUT: int = 30

_ACTORS: list[str] = [
    "BlackRock", "JPMorgan", "Goldman Sachs", "Citadel",
    "Federal Reserve", "SEC", "Treasury Department",
    "Vanguard", "State Street", "Bridgewater",
]

_CATEGORY_MAP: dict[int, str] = {
    1: "position", 2: "education", 3: "membership", 4: "family",
    5: "donation", 6: "transaction", 7: "lobbying", 8: "social",
    9: "professional", 10: "ownership", 11: "hierarchy", 12: "generic",
}

_HDR = {"User-Agent": "GRID-Intelligence/1.0", "Accept": "application/json"}


def _slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


class LittleSisPuller(BasePuller):
    """Pulls entity relationships from the LittleSis power-mapping DB."""

    SOURCE_NAME: str = "littlesis"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://littlesis.org/api",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": False,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 35,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("LittleSisPuller ready -- source_id={sid}", sid=self.source_id)

    @retry_on_failure(max_attempts=3, retryable_exceptions=(
        ConnectionError, TimeoutError, OSError, requests.RequestException,
    ))
    def _search(self, query: str) -> list[dict[str, Any]]:
        resp = requests.get(
            _SEARCH_URL, params={"q": query, "per_page": 5},
            headers=_HDR, timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json().get("data", [])

    @retry_on_failure(max_attempts=3, retryable_exceptions=(
        ConnectionError, TimeoutError, OSError, requests.RequestException,
    ))
    def _get_rels(self, entity_id: int) -> list[dict[str, Any]]:
        resp = requests.get(
            _RELS_URL.format(id=entity_id), params={"per_page": 100},
            headers=_HDR, timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json().get("data", [])

    def pull(self) -> dict[str, Any]:
        """Search top financial actors and store their relationships.

        Returns:
            dict with status and rows_inserted.
        """
        today = date.today()
        rows_inserted = 0

        for actor in _ACTORS:
            try:
                entities = self._search(actor)
                time.sleep(_RATE_LIMIT)
            except Exception as exc:
                log.warning("LittleSis search {n}: {e}", n=actor, e=exc)
                continue
            if not entities:
                continue

            ent = entities[0]
            eid = ent.get("id")
            attrs = ent.get("attributes", {})
            slug = _slugify(attrs.get("name", actor))

            try:
                rels = self._get_rels(eid)
                time.sleep(_RATE_LIMIT)
            except Exception as exc:
                log.warning("LittleSis rels {n}: {e}", n=actor, e=exc)
                continue

            with self.engine.begin() as conn:
                for rel in rels:
                    ra = rel.get("attributes", {})
                    cat_id = ra.get("category_id")
                    rel_type = _CATEGORY_MAP.get(int(cat_id), "unknown") if cat_id else "unknown"
                    rel_id = rel.get("id", ra.get("id", ""))
                    self._insert_raw(
                        conn=conn,
                        series_id=f"littlesis.{slug}.{rel_type}.{rel_id}",
                        obs_date=today,
                        value=1,
                        raw_payload={
                            "entity1_id": ra.get("entity1_id"),
                            "entity2_id": ra.get("entity2_id"),
                            "category": rel_type,
                            "description1": ra.get("description1", ""),
                            "description2": ra.get("description2", ""),
                            "amount": ra.get("amount"),
                            "is_current": ra.get("is_current"),
                            "source_entity": attrs.get("name", actor),
                        },
                    )
                    rows_inserted += 1

        log.info("LittleSis pull -- {n} rows inserted", n=rows_inserted)
        return {"status": "SUCCESS", "rows_inserted": rows_inserted}
