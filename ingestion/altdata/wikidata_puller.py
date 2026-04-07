"""
Wikidata SPARQL entity relationship puller.

Runs bulk SPARQL queries for board members (P3320) and subsidiaries
(P355), groups by company, stores as raw_series with series_id format:
``wikidata.board.{company_slug}`` / ``wikidata.subsidiary.{parent_slug}``
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

_SPARQL_URL = "https://query.wikidata.org/sparql"
_RATE_LIMIT: float = 0.5  # 2 req/sec
_TIMEOUT: int = 60
_HEADERS: dict[str, str] = {
    "Accept": "application/sparql-results+json",
    "User-Agent": "GRID-Intelligence/1.0",
}

_BOARD_QUERY = (
    'SELECT ?company ?companyLabel ?person ?personLabel WHERE { '
    '?company wdt:P3320 ?person . '
    '?company wdt:P31 wd:Q4830453 . '
    'SERVICE wikibase:label { bd:serviceParam wikibase:language "en" } '
    '} LIMIT 500'
)

_SUB_QUERY = (
    'SELECT ?parent ?parentLabel ?sub ?subLabel WHERE { '
    '?parent wdt:P355 ?sub . '
    'SERVICE wikibase:label { bd:serviceParam wikibase:language "en" } '
    '} LIMIT 500'
)


def _slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def _qid(uri: str) -> str:
    return uri.rsplit("/", 1)[-1] if "/" in uri else uri


class WikidataPuller(BasePuller):
    """Pulls board memberships and subsidiaries from Wikidata SPARQL."""

    SOURCE_NAME: str = "wikidata"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": _SPARQL_URL,
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": False,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 40,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("WikidataPuller ready -- source_id={sid}", sid=self.source_id)

    @retry_on_failure(max_attempts=3, retryable_exceptions=(
        ConnectionError, TimeoutError, OSError, requests.RequestException,
    ))
    def _sparql(self, query: str) -> list[dict[str, Any]]:
        resp = requests.get(
            _SPARQL_URL, params={"query": query},
            headers=_HEADERS, timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json().get("results", {}).get("bindings", [])

    def _store_grouped(
        self, results: list[dict], prefix: str,
        key_label: str, val_label: str, val_id_key: str, payload_key: str,
    ) -> int:
        """Group SPARQL results by key_label, store one row per group."""
        grouped: dict[str, list[dict[str, str]]] = {}
        for r in results:
            k = r.get(key_label, {}).get("value", "")
            v = r.get(val_label, {}).get("value", "")
            vid = _qid(r.get(val_id_key, {}).get("value", ""))
            if k and v:
                grouped.setdefault(k, []).append({"name": v, "qid": vid})
        today = date.today()
        rows = 0
        with self.engine.begin() as conn:
            for name, items in grouped.items():
                self._insert_raw(
                    conn=conn,
                    series_id=f"wikidata.{prefix}.{_slugify(name)}",
                    obs_date=today, value=len(items),
                    raw_payload={payload_key: name, f"{prefix}_list": items},
                )
                rows += 1
        return rows

    def pull(self) -> dict[str, Any]:
        """Run both SPARQL queries and store results.

        Returns:
            dict with status and rows_inserted.
        """
        total = 0
        try:
            results = self._sparql(_BOARD_QUERY)
            n = self._store_grouped(
                results, "board", "companyLabel", "personLabel", "person", "company")
            total += n
            log.info("Wikidata board: {n} companies", n=n)
        except Exception as exc:
            log.error("Wikidata board query failed: {e}", e=exc)
        time.sleep(_RATE_LIMIT)
        try:
            results = self._sparql(_SUB_QUERY)
            n = self._store_grouped(
                results, "subsidiary", "parentLabel", "subLabel", "sub", "parent")
            total += n
            log.info("Wikidata subsidiaries: {n} parents", n=n)
        except Exception as exc:
            log.error("Wikidata subsidiary query failed: {e}", e=exc)

        log.info("Wikidata pull -- {n} rows inserted", n=total)
        return {"status": "SUCCESS", "rows_inserted": total}
