"""
GRID trending news ingestion via last30days-skill.

Runs the last30days multi-source research engine on financial topics
(Reddit, Hacker News, Polymarket, Bluesky, web search) and feeds
the results into GRID's intelligence network.

Each research run produces:
    - Trending items from Reddit, HN, Polymarket, Bluesky, web
    - Engagement metrics (upvotes, comments, volume, liquidity)
    - Relevance + recency + engagement sub-scores
    - Cross-platform convergence signals

Series stored:
    - TRENDING:{platform}:{topic_slug}:{item_id}
    - TRENDING_AGG:{topic_slug}:{platform}_count

Source: last30days-skill (vendor/last30days-skill)
Schedule: Every 6 hours (aligned with Oracle cycle)
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ── last30days-skill integration ────────────────────────────────────────

_VENDOR_DIR = Path(__file__).resolve().parent.parent.parent / "vendor" / "last30days-skill"
_SCRIPTS_DIR = _VENDOR_DIR / "scripts"

# Financial topics to research on each pull cycle
DEFAULT_TOPICS: list[str] = [
    "stock market moves today",
    "Federal Reserve interest rates",
    "crypto bitcoin ethereum",
    "options unusual activity",
    "earnings surprises",
    "geopolitical risk markets",
    "commodities oil gold",
    "AI stocks semiconductor",
]

# Platform mapping for series IDs
_PLATFORM_NAMES: dict[str, str] = {
    "reddit": "reddit",
    "hackernews": "hackernews",
    "polymarket": "polymarket",
    "bluesky": "bluesky",
    "web": "web",
    "x": "x",
}

# Maximum items to store per platform per topic
_MAX_ITEMS_PER_PLATFORM: int = 25

# Request timeout for the research engine (seconds)
_RESEARCH_TIMEOUT: int = 120

# Rate limit between topic researches (seconds)
_TOPIC_DELAY: float = 5.0


def _slugify(topic: str) -> str:
    """Convert topic string to a safe slug for series IDs."""
    return (
        topic.lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
        .replace("&", "and")
        [:50]
    )


def _hash_item(platform: str, item_id: str, topic: str) -> str:
    """SHA-256 hash for deduplication."""
    content = f"{platform}|{item_id}|{topic}".lower()
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:32]


def _total_engagement(item: dict) -> int:
    """Extract a single engagement number from an item dict."""
    eng = item.get("engagement") or {}
    if isinstance(eng, dict):
        return (
            (eng.get("score") or 0)
            + (eng.get("num_comments") or 0)
            + (eng.get("likes") or 0)
            + (eng.get("reposts") or 0)
            + (eng.get("replies") or 0)
            + (eng.get("views") or 0)
            + int(eng.get("volume") or 0)
        )
    return 0


# Source name mapping: pipeline uses "grounding" for web search
_SOURCE_TO_PLATFORM: dict[str, str] = {
    "grounding": "web",
}


def _source_item_to_dict(item: Any) -> dict:
    """Convert a last30days SourceItem dataclass to the dict format _convert_items expects."""
    result = {
        "id": item.item_id,
        "title": item.title,
        "text": item.body,
        "url": item.url,
        "author": item.author or "",
        "subreddit": getattr(item, "container", "") or "",
        "source_domain": getattr(item, "container", "") or "",
        "date": item.published_at,
        "date_confidence": getattr(item, "date_confidence", "low"),
        "engagement": item.engagement if isinstance(item.engagement, dict) else {},
        "relevance": (
            item.local_relevance
            if getattr(item, "local_relevance", None) is not None
            else item.relevance_hint
        ),
        "score": int(getattr(item, "local_rank_score", 0) or 0),
        "why_relevant": item.why_relevant,
        "snippet": getattr(item, "snippet", ""),
    }
    # Spread platform-specific metadata (outcome_prices, hn_url, etc.)
    if hasattr(item, "metadata") and isinstance(item.metadata, dict):
        result.update(item.metadata)
    return result


# ── Dataclass ───────────────────────────────────────────────────────────


@dataclass
class TrendingItem:
    """A trending item from any platform."""

    platform: str
    item_id: str
    topic: str
    title: str
    url: str
    content: str
    author: str
    published: datetime | None
    engagement_total: int
    relevance: float
    score: int
    raw_engagement: dict
    dedup_hash: str
    metadata: dict = field(default_factory=dict)


# ── Main Puller ─────────────────────────────────────────────────────────


class TrendingNewsPuller(BasePuller):
    """Pulls trending financial news from multiple platforms via last30days-skill.

    Uses the last30days research engine to search Reddit, Hacker News,
    Polymarket, Bluesky, and the web for financial topics. Results are
    stored in the trending_items table and emitted as signals for trust
    scoring.

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id.
        topics: List of financial topics to research.
    """

    SOURCE_NAME: str = "TrendingNews_Last30Days"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://github.com/mvanhorn/last30days-skill",
        "cost_tier": "FREE",
        "latency_class": "INTRADAY",
        "pit_available": False,
        "revision_behavior": "NEVER",
        "trust_score": "MED",
        "priority_rank": 35,
    }

    def __init__(
        self,
        db_engine: Engine,
        topics: list[str] | None = None,
    ) -> None:
        super().__init__(db_engine)
        self.topics = topics or DEFAULT_TOPICS
        self._seen_hashes: set[str] = set()
        self._last30days_available = self._check_last30days()
        self._ensure_trending_table()
        log.info(
            "TrendingNewsPuller initialised — source_id={sid}, last30days={avail}",
            sid=self.source_id,
            avail=self._last30days_available,
        )

    def _check_last30days(self) -> bool:
        """Verify the last30days-skill vendor directory exists."""
        if not _SCRIPTS_DIR.exists():
            log.warning(
                "last30days-skill not found at {p} — trending news disabled",
                p=_SCRIPTS_DIR,
            )
            return False
        if not (_SCRIPTS_DIR / "last30days.py").exists():
            log.warning("last30days.py not found in {p}", p=_SCRIPTS_DIR)
            return False
        return True

    # ── Table Setup ─────────────────────────────────────────────────────

    def _ensure_trending_table(self) -> None:
        """Create trending_items table if it doesn't exist."""
        with self.engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS trending_items (
                    id SERIAL PRIMARY KEY,
                    dedup_hash TEXT UNIQUE NOT NULL,
                    platform TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    item_id TEXT,
                    title TEXT NOT NULL,
                    url TEXT,
                    content TEXT,
                    author TEXT,
                    published_at TIMESTAMPTZ,
                    engagement_total INTEGER DEFAULT 0,
                    relevance DOUBLE PRECISION DEFAULT 0.5,
                    score INTEGER DEFAULT 0,
                    raw_engagement JSONB,
                    metadata JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_trending_platform
                ON trending_items (platform)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_trending_topic
                ON trending_items (topic)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_trending_created
                ON trending_items (created_at DESC)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_trending_engagement
                ON trending_items (engagement_total DESC)
            """))

    # ── Deduplication ───────────────────────────────────────────────────

    def _is_duplicate(self, dedup_hash: str) -> bool:
        """Check if item already stored (memory cache + DB)."""
        if dedup_hash in self._seen_hashes:
            return True
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT 1 FROM trending_items WHERE dedup_hash = :h LIMIT 1"),
                {"h": dedup_hash},
            ).fetchone()
        if row is not None:
            self._seen_hashes.add(dedup_hash)
            return True
        return False

    # ── Research Engine ─────────────────────────────────────────────────

    def _run_research(self, topic: str, days: int = 7) -> dict[str, list] | None:
        """Run last30days research for a single topic.

        Parameters:
            topic: The search topic.
            days: Lookback window in days.

        Returns:
            Dict mapping platform names to lists of item dicts, or None on failure.
        """
        if not self._last30days_available:
            return None

        # Add the scripts dir to path temporarily for imports
        scripts_str = str(_SCRIPTS_DIR)
        if scripts_str not in sys.path:
            sys.path.insert(0, scripts_str)

        try:
            from lib import env, pipeline

            config = env.get_config()
            report = pipeline.run(
                topic=topic,
                config=config,
                depth="quick",
                requested_sources=[
                    "reddit", "hackernews", "polymarket", "bluesky", "grounding",
                ],
                lookback_days=days,
            )

            # Log errors but don't fail
            for source, err in report.errors_by_source.items():
                log.debug(
                    "last30days {s} error for '{t}': {e}",
                    s=source, t=topic, e=err,
                )

            # Convert SourceItems to dicts keyed by platform name
            platform_items: dict[str, list] = {}

            for source, items in report.items_by_source.items():
                platform = _SOURCE_TO_PLATFORM.get(source, source)
                if platform not in _PLATFORM_NAMES:
                    continue
                platform_items[platform] = [
                    _source_item_to_dict(item)
                    for item in items[:_MAX_ITEMS_PER_PLATFORM]
                ]

            return platform_items

        except Exception as exc:
            log.warning(
                "last30days research failed for '{t}': {e}",
                t=topic, e=str(exc),
            )
            return None
        finally:
            # Clean up sys.path
            if scripts_str in sys.path:
                sys.path.remove(scripts_str)

    # ── Item Conversion ─────────────────────────────────────────────────

    def _convert_items(
        self,
        platform: str,
        topic: str,
        raw_items: list[dict],
    ) -> list[TrendingItem]:
        """Convert raw platform items to TrendingItem objects.

        Parameters:
            platform: Platform name (reddit, hackernews, etc.)
            topic: Research topic.
            raw_items: List of item dicts from last30days.

        Returns:
            List of TrendingItem objects.
        """
        items: list[TrendingItem] = []

        for item in raw_items:
            item_id = item.get("id", "")
            dedup_hash = _hash_item(platform, item_id, topic)

            if self._is_duplicate(dedup_hash):
                continue

            # Extract title/content based on platform
            title = item.get("title") or item.get("text") or item.get("question") or ""
            content = item.get("why_relevant") or item.get("snippet") or ""
            author = (
                item.get("author")
                or item.get("author_handle")
                or item.get("subreddit")
                or item.get("source_domain")
                or ""
            )
            url = item.get("url") or item.get("hn_url") or ""

            # Parse date
            published = None
            date_str = item.get("date")
            if date_str:
                try:
                    published = datetime.fromisoformat(
                        date_str.replace("Z", "+00:00")
                    )
                except (ValueError, TypeError):
                    pass

            raw_eng = item.get("engagement") or {}
            if isinstance(raw_eng, dict):
                engagement_total = _total_engagement(item)
            else:
                raw_eng = {}
                engagement_total = 0

            items.append(TrendingItem(
                platform=platform,
                item_id=item_id,
                topic=topic,
                title=title[:500],
                url=url[:1000],
                content=content[:2000],
                author=author[:200],
                published=published,
                engagement_total=engagement_total,
                relevance=float(item.get("relevance", 0.5)),
                score=int(item.get("score", 0)),
                raw_engagement=raw_eng if isinstance(raw_eng, dict) else {},
                dedup_hash=dedup_hash,
                metadata={
                    "subs": item.get("subs"),
                    "date_confidence": item.get("date_confidence"),
                    "cross_refs": item.get("cross_refs"),
                    "outcome_prices": item.get("outcome_prices"),
                    "price_movement": item.get("price_movement"),
                },
            ))

        return items

    # ── Storage ─────────────────────────────────────────────────────────

    def _store_items(self, items: list[TrendingItem]) -> int:
        """Store trending items in the database.

        Parameters:
            items: List of TrendingItem objects to store.

        Returns:
            Number of rows inserted.
        """
        if not items:
            return 0

        inserted = 0
        today = date.today()

        with self.engine.begin() as conn:
            for item in items:
                try:
                    conn.execute(
                        text(
                            "INSERT INTO trending_items "
                            "(dedup_hash, platform, topic, item_id, title, url, "
                            "content, author, published_at, engagement_total, "
                            "relevance, score, raw_engagement, metadata) "
                            "VALUES (:hash, :platform, :topic, :item_id, :title, "
                            ":url, :content, :author, :pub, :eng, :rel, :score, "
                            ":raw_eng, :meta) "
                            "ON CONFLICT (dedup_hash) DO NOTHING"
                        ),
                        {
                            "hash": item.dedup_hash,
                            "platform": item.platform,
                            "topic": item.topic,
                            "item_id": item.item_id,
                            "title": item.title,
                            "url": item.url,
                            "content": item.content,
                            "author": item.author,
                            "pub": item.published,
                            "eng": item.engagement_total,
                            "rel": item.relevance,
                            "score": item.score,
                            "raw_eng": json.dumps(item.raw_engagement),
                            "meta": json.dumps(item.metadata, default=str),
                        },
                    )
                    inserted += 1
                    self._seen_hashes.add(item.dedup_hash)
                except Exception as exc:
                    log.debug(
                        "Failed to store trending item: {e}", e=str(exc),
                    )
                    continue

                # Store in raw_series for PIT queries
                topic_slug = _slugify(item.topic)
                series_id = f"TRENDING:{item.platform}:{topic_slug}:{item.item_id}"
                try:
                    if not self._row_exists(series_id, today, conn):
                        self._insert_raw(
                            conn=conn,
                            series_id=series_id,
                            obs_date=today,
                            value=float(item.engagement_total),
                            raw_payload={
                                "title": item.title[:200],
                                "url": item.url,
                                "platform": item.platform,
                                "relevance": item.relevance,
                                "score": item.score,
                                "author": item.author,
                            },
                        )
                except Exception as exc:
                    log.debug(
                        "Failed to store raw_series for {sid}: {e}",
                        sid=series_id, e=str(exc),
                    )

                # Emit signal for trust scoring
                self._emit_signal(conn, item, today)

        return inserted

    def _emit_signal(
        self,
        conn: Any,
        item: TrendingItem,
        obs_date: date,
    ) -> None:
        """Emit trending item as a signal for trust scoring.

        Parameters:
            conn: Active DB connection.
            item: The trending item.
            obs_date: Observation date.
        """
        # Only emit high-engagement items as signals
        if item.engagement_total < 50 or item.relevance < 0.4:
            return

        try:
            conn.execute(
                text(
                    "INSERT INTO signal_sources "
                    "(source_type, source_name, ticker, direction, confidence, "
                    "signal_date, metadata) "
                    "VALUES ('social', :name, :ticker, :direction, :conf, "
                    ":sig_date, :meta) "
                    "ON CONFLICT DO NOTHING"
                ),
                {
                    "name": f"trending_{item.platform}",
                    "ticker": "MARKET",
                    "direction": "neutral",
                    "conf": min(item.relevance, 1.0),
                    "sig_date": obs_date,
                    "meta": json.dumps({
                        "title": item.title[:200],
                        "url": item.url,
                        "platform": item.platform,
                        "topic": item.topic,
                        "engagement": item.engagement_total,
                        "score": item.score,
                    }),
                },
            )
        except Exception:
            # signal_sources table may not exist yet — not critical
            pass

    # ── Aggregate Storage ───────────────────────────────────────────────

    def _store_aggregates(
        self,
        topic: str,
        platform_counts: dict[str, int],
        target_date: date,
    ) -> None:
        """Store per-topic aggregate counts in raw_series.

        Parameters:
            topic: The research topic.
            platform_counts: Dict of platform -> item count.
            target_date: Observation date.
        """
        topic_slug = _slugify(topic)

        with self.engine.begin() as conn:
            for platform, count in platform_counts.items():
                series_id = f"TRENDING_AGG:{topic_slug}:{platform}_count"
                try:
                    if not self._row_exists(series_id, target_date, conn):
                        self._insert_raw(
                            conn=conn,
                            series_id=series_id,
                            obs_date=target_date,
                            value=float(count),
                            raw_payload={
                                "topic": topic,
                                "platform": platform,
                            },
                        )
                except Exception as exc:
                    log.debug(
                        "Failed to store aggregate {sid}: {e}",
                        sid=series_id, e=str(exc),
                    )

            # Total across platforms
            total = sum(platform_counts.values())
            total_sid = f"TRENDING_AGG:{topic_slug}:total"
            try:
                if not self._row_exists(total_sid, target_date, conn):
                    self._insert_raw(
                        conn=conn,
                        series_id=total_sid,
                        obs_date=target_date,
                        value=float(total),
                        raw_payload={
                            "topic": topic,
                            "platforms": platform_counts,
                        },
                    )
            except Exception as exc:
                log.debug(
                    "Failed to store total aggregate: {e}", e=str(exc),
                )

    # ── Feature Registration ────────────────────────────────────────────

    def _ensure_features(self) -> None:
        """Register trending news features in feature_registry."""
        features = [
            ("trending_volume_daily", "social", "Trending: total items across all platforms per day"),
            ("trending_reddit_count", "social", "Trending: Reddit items per day"),
            ("trending_hackernews_count", "social", "Trending: Hacker News items per day"),
            ("trending_polymarket_count", "social", "Trending: Polymarket items per day"),
            ("trending_avg_engagement", "social", "Trending: average engagement score per day"),
            ("trending_avg_relevance", "social", "Trending: average relevance score per day"),
        ]
        with self.engine.begin() as conn:
            for name, family, desc in features:
                conn.execute(
                    text(
                        "INSERT INTO feature_registry "
                        "(name, family, description, transformation, transformation_version, "
                        "lag_days, normalization, missing_data_policy, eligible_from_date, model_eligible) "
                        "VALUES (:name, :family, :desc, 'RAW', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2024-01-01', TRUE) "
                        "ON CONFLICT (name) DO NOTHING"
                    ),
                    {"name": name, "family": family, "desc": desc},
                )

    # ── Public API ──────────────────────────────────────────────────────

    def pull_topic(self, topic: str, days: int = 7) -> dict[str, int]:
        """Pull trending items for a single topic.

        Parameters:
            topic: The research topic.
            days: Lookback window in days.

        Returns:
            Dict of platform -> items stored count.
        """
        platform_items = self._run_research(topic, days=days)
        if not platform_items:
            return {}

        platform_counts: dict[str, int] = {}

        for platform, raw_items in platform_items.items():
            items = self._convert_items(platform, topic, raw_items)
            stored = self._store_items(items)
            platform_counts[platform] = stored
            log.info(
                "Trending {p} for '{t}': {n} items, {s} stored",
                p=platform, t=topic, n=len(raw_items), s=stored,
            )

        return platform_counts

    def pull_all(self) -> dict[str, Any]:
        """Pull trending news for all configured topics.

        Returns:
            Summary dict with per-topic counts and totals.
        """
        self._ensure_features()
        today = date.today()

        topic_results: dict[str, dict[str, int]] = {}
        total_items = 0
        total_stored = 0

        for topic in self.topics:
            try:
                platform_counts = self.pull_topic(topic)
                topic_results[topic] = platform_counts
                topic_stored = sum(platform_counts.values())
                total_stored += topic_stored
                total_items += topic_stored

                # Store aggregates
                if platform_counts:
                    self._store_aggregates(topic, platform_counts, today)

                log.info(
                    "Trending '{t}': {n} items across {p} platforms",
                    t=topic, n=topic_stored, p=len(platform_counts),
                )
            except Exception as exc:
                log.warning(
                    "Trending pull failed for '{t}': {e}",
                    t=topic, e=str(exc),
                )
                topic_results[topic] = {}

            time.sleep(_TOPIC_DELAY)

        summary = {
            "date": today.isoformat(),
            "total_items": total_items,
            "total_stored": total_stored,
            "topics_researched": len(self.topics),
            "topic_results": {
                topic: {
                    "platforms": counts,
                    "total": sum(counts.values()),
                }
                for topic, counts in topic_results.items()
            },
        }

        log.info(
            "Trending news pull complete — {n} items across {t} topics",
            n=total_stored, t=len(self.topics),
        )
        return summary

    def get_recent(
        self,
        platform: str | None = None,
        topic: str | None = None,
        hours: int = 24,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Retrieve recent trending items from the database.

        Parameters:
            platform: Optional platform filter.
            topic: Optional topic filter.
            hours: Hours to look back.
            limit: Max items to return.

        Returns:
            List of trending item dicts.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

        conditions = ["created_at >= :cutoff"]
        params: dict[str, Any] = {"cutoff": cutoff, "lim": limit}

        if platform:
            conditions.append("platform = :platform")
            params["platform"] = platform
        if topic:
            conditions.append("topic = :topic")
            params["topic"] = topic

        where = " AND ".join(conditions)

        query = text(
            f"SELECT platform, topic, title, url, content, author, "
            f"published_at, engagement_total, relevance, score, "
            f"raw_engagement, metadata "
            f"FROM trending_items "
            f"WHERE {where} "
            f"ORDER BY engagement_total DESC, created_at DESC "
            f"LIMIT :lim"
        )

        with self.engine.connect() as conn:
            rows = conn.execute(query, params).fetchall()

        return [
            {
                "platform": r[0],
                "topic": r[1],
                "title": r[2],
                "url": r[3],
                "content": r[4],
                "author": r[5],
                "published": r[6].isoformat() if r[6] else None,
                "engagement": r[7],
                "relevance": r[8],
                "score": r[9],
                "raw_engagement": r[10],
                "metadata": r[11],
            }
            for r in rows
        ]
