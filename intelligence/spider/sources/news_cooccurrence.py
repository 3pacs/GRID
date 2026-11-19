"""GDELT news co-occurrence adapter — discovers entity co-mentions in news.

Queries the GDELT DOC API for recent articles mentioning an actor,
then extracts other entity names that co-appear frequently using
a simple capitalized-phrase NER heuristic.
Confidence tier: 3 (inferred).
"""

from __future__ import annotations

import re
from collections import Counter
from typing import Any

import requests
from loguru import logger as log

from ingestion.altdata import gdelt_doc
from ingestion.altdata.gdelt_doc import GDELTRateLimited
from intelligence.spider.models import DiscoveredConnection

GDELT_DOC_API = gdelt_doc.DOC_API_URL
# A spider round waits at most this long for a GDELT slot, then moves on.
_GDELT_MAX_WAIT_S: float = 15.0

# Pattern for capitalized multi-word phrases (simple NER heuristic)
_ENTITY_RE = re.compile(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b")

# Common false-positive phrases to ignore
_STOPWORDS = frozenset({
    "The Associated Press", "United States", "New York", "Wall Street",
    "White House", "Capitol Hill", "Main Street", "Silicon Valley",
    "The Wall Street Journal", "The New York Times", "The Washington Post",
    "Los Angeles", "San Francisco", "Hong Kong", "Saudi Arabia",
    "North Korea", "South Korea", "European Union", "United Kingdom",
    "United Nations",
})


class NewsCooccurrenceAdapter:
    """Discover actor connections via GDELT news co-occurrence."""

    name = "news_cooccurrence"

    def discover(
        self, actor_name: str, actor_hint: dict[str, Any]
    ) -> list[DiscoveredConnection]:
        try:
            # Through the host-wide GDELT DOC gate at low priority: the spider
            # shares grid-svr's IP with the scheduled GDELT puller and the
            # breaking-news monitor, and must not send into a 429 cooldown.
            resp = gdelt_doc.doc_get(
                {
                    "query": f'"{actor_name}"',
                    "mode": "artlist",
                    "maxrecords": "50",
                    "format": "json",
                },
                max_wait_s=_GDELT_MAX_WAIT_S,
                attempts=1,
                priority="low",
                caller="spider_news_cooccurrence",
            )
            if resp.status_code != 200:
                log.debug("GDELT query failed for {a}: HTTP {s}", a=actor_name, s=resp.status_code)
                return []

            data = resp.json()
            articles = data.get("articles", [])
            if not articles:
                log.debug("GDELT: no articles for {a}", a=actor_name)
                return []

            # Count co-occurring entity names across article titles
            entity_counts: Counter[str] = Counter()
            entity_evidence: dict[str, list[dict[str, Any]]] = {}
            actor_lower = actor_name.lower()

            for article in articles:
                title = article.get("title", "")
                url = article.get("url", "")

                entities = _ENTITY_RE.findall(title)
                for entity in entities:
                    entity = entity.strip()
                    if (
                        entity.lower() == actor_lower
                        or entity in _STOPWORDS
                        or len(entity) < 5
                    ):
                        continue
                    entity_counts[entity] += 1
                    if entity not in entity_evidence:
                        entity_evidence[entity] = []
                    if len(entity_evidence[entity]) < 3:
                        entity_evidence[entity].append({
                            "source": "gdelt",
                            "url": url,
                            "excerpt": title[:200],
                        })

            # Only keep entities mentioned in 2+ articles
            connections: list[DiscoveredConnection] = []
            for entity, count in entity_counts.most_common(20):
                if count < 2:
                    continue
                connections.append(DiscoveredConnection(
                    target_name=entity,
                    relationship="co_mentioned_with",
                    strength=min(0.4 + (count * 0.05), 0.8),
                    confidence_tier=3,
                    target_hint={"co_mention_count": count},
                    evidence=entity_evidence.get(entity, []),
                ))

            log.debug("GDELT: {n} connections for {a}", n=len(connections), a=actor_name)
            return connections

        except GDELTRateLimited as exc:
            log.debug("GDELT throttled; skipping {a}: {e}", a=actor_name, e=str(exc))
            return []
        except requests.RequestException as exc:
            log.debug("GDELT request failed for {a}: {e}", a=actor_name, e=str(exc))
            return []
        except Exception as exc:
            log.debug("GDELT adapter error for {a}: {e}", a=actor_name, e=str(exc))
            return []
