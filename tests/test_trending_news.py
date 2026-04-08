"""Tests for the trending news ingestion puller (last30days-skill integration)."""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

os.environ.setdefault("FRED_API_KEY", "test")
os.environ.setdefault("GRID_MASTER_PASSWORD_HASH", "$2b$12$test")

from ingestion.altdata.trending_news import (
    TrendingItem,
    TrendingNewsPuller,
    _hash_item,
    _slugify,
    _total_engagement,
    DEFAULT_TOPICS,
)


# ── Fixtures ────────────────────────────────────────────────────────────


@pytest.fixture
def mock_engine():
    """Create a mock SQLAlchemy engine."""
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    # Default: source_catalog row exists, but no duplicate items found
    conn.execute.return_value.fetchone.return_value = (1,)
    conn.execute.return_value.fetchall.return_value = []
    # Store a reference to the mock conn for test access
    engine._mock_conn = conn
    return engine


@pytest.fixture
def puller(mock_engine):
    """Create TrendingNewsPuller with mock engine."""
    with patch.object(TrendingNewsPuller, "_check_last30days", return_value=False):
        return TrendingNewsPuller(mock_engine, topics=["test topic"])


@pytest.fixture
def sample_reddit_items():
    """Sample Reddit item dicts as returned by last30days."""
    return [
        {
            "id": "R1",
            "title": "Fed signals rate cut in September meeting minutes",
            "url": "https://reddit.com/r/stocks/abc",
            "subreddit": "stocks",
            "date": "2026-04-05T12:00:00Z",
            "date_confidence": "high",
            "engagement": {"score": 500, "num_comments": 120, "upvote_ratio": 0.92},
            "relevance": 0.85,
            "why_relevant": "Fed policy directly impacts market direction",
            "subs": {"relevance": 90, "recency": 80, "engagement": 85},
            "score": 255,
        },
        {
            "id": "R2",
            "title": "NVDA earnings whisper numbers suggest big beat",
            "url": "https://reddit.com/r/wallstreetbets/def",
            "subreddit": "wallstreetbets",
            "date": "2026-04-04T08:30:00Z",
            "date_confidence": "high",
            "engagement": {"score": 1200, "num_comments": 340},
            "relevance": 0.72,
            "why_relevant": "Earnings expectations for key AI stock",
            "subs": {"relevance": 75, "recency": 70, "engagement": 95},
            "score": 240,
        },
    ]


@pytest.fixture
def sample_hn_items():
    """Sample Hacker News item dicts."""
    return [
        {
            "id": "HN1",
            "title": "The Fed's hidden liquidity problem",
            "url": "https://example.com/fed-liquidity",
            "hn_url": "https://news.ycombinator.com/item?id=12345",
            "author": "fintech_guru",
            "date": "2026-04-06T10:00:00Z",
            "engagement": {"score": 200, "num_comments": 80},
            "relevance": 0.90,
            "score": 180,
        },
    ]


@pytest.fixture
def sample_polymarket_items():
    """Sample Polymarket item dicts."""
    return [
        {
            "id": "PM1",
            "title": "Will the Fed cut rates in June 2026?",
            "question": "Federal Reserve June 2026 rate decision",
            "url": "https://polymarket.com/event/fed-june-2026",
            "outcome_prices": [("Yes", 0.62), ("No", 0.38)],
            "price_movement": "up 5.2% this week",
            "date": "2026-04-06",
            "engagement": {"volume": 2500000, "liquidity": 500000},
            "relevance": 0.95,
            "score": 290,
        },
    ]


# ── Unit Tests: Helper Functions ────────────────────────────────────────


class TestHelperFunctions:

    def test_slugify_basic(self):
        assert _slugify("stock market moves today") == "stock_market_moves_today"

    def test_slugify_special_chars(self):
        assert _slugify("AI/ML & semiconductors") == "ai_ml_and_semiconductors"

    def test_slugify_truncation(self):
        long_topic = "a" * 100
        assert len(_slugify(long_topic)) == 50

    def test_hash_item_deterministic(self):
        h1 = _hash_item("reddit", "R1", "test")
        h2 = _hash_item("reddit", "R1", "test")
        assert h1 == h2

    def test_hash_item_different_inputs(self):
        h1 = _hash_item("reddit", "R1", "test")
        h2 = _hash_item("hackernews", "HN1", "test")
        assert h1 != h2

    def test_hash_item_length(self):
        h = _hash_item("reddit", "R1", "topic")
        assert len(h) == 32

    def test_total_engagement_reddit(self):
        item = {"engagement": {"score": 500, "num_comments": 120}}
        assert _total_engagement(item) == 620

    def test_total_engagement_polymarket(self):
        item = {"engagement": {"volume": 2500000, "liquidity": 500000}}
        assert _total_engagement(item) == 2500000

    def test_total_engagement_empty(self):
        assert _total_engagement({}) == 0
        assert _total_engagement({"engagement": None}) == 0

    def test_total_engagement_mixed(self):
        item = {"engagement": {"likes": 100, "reposts": 20, "views": 5000}}
        assert _total_engagement(item) == 5120


# ── Unit Tests: Puller Initialization ───────────────────────────────────


class TestPullerInit:

    def test_init_with_custom_topics(self, mock_engine):
        with patch.object(TrendingNewsPuller, "_check_last30days", return_value=False):
            puller = TrendingNewsPuller(mock_engine, topics=["crypto", "AI"])
        assert puller.topics == ["crypto", "AI"]

    def test_init_with_default_topics(self, mock_engine):
        with patch.object(TrendingNewsPuller, "_check_last30days", return_value=False):
            puller = TrendingNewsPuller(mock_engine)
        assert puller.topics == DEFAULT_TOPICS
        assert len(puller.topics) > 0

    def test_source_name(self, puller):
        assert puller.SOURCE_NAME == "TrendingNews_Last30Days"

    def test_ensures_table_on_init(self, mock_engine):
        """Verify _ensure_trending_table is called during init."""
        with patch.object(TrendingNewsPuller, "_check_last30days", return_value=False):
            with patch.object(TrendingNewsPuller, "_ensure_trending_table") as mock_table:
                TrendingNewsPuller(mock_engine, topics=["test"])
                mock_table.assert_called_once()


# ── Unit Tests: Item Conversion ─────────────────────────────────────────


class TestItemConversion:

    def test_convert_reddit_items(self, puller, sample_reddit_items):
        with patch.object(puller, "_is_duplicate", return_value=False):
            items = puller._convert_items("reddit", "test topic", sample_reddit_items)
        assert len(items) == 2
        assert items[0].platform == "reddit"
        assert items[0].title == "Fed signals rate cut in September meeting minutes"
        assert items[0].engagement_total == 620  # 500 + 120
        assert items[0].relevance == 0.85

    def test_convert_hn_items(self, puller, sample_hn_items):
        with patch.object(puller, "_is_duplicate", return_value=False):
            items = puller._convert_items("hackernews", "test topic", sample_hn_items)
        assert len(items) == 1
        assert items[0].platform == "hackernews"
        assert items[0].author == "fintech_guru"
        assert items[0].engagement_total == 280  # 200 + 80

    def test_convert_polymarket_items(self, puller, sample_polymarket_items):
        with patch.object(puller, "_is_duplicate", return_value=False):
            items = puller._convert_items("polymarket", "test topic", sample_polymarket_items)
        assert len(items) == 1
        assert items[0].platform == "polymarket"
        assert items[0].relevance == 0.95
        assert items[0].metadata.get("outcome_prices") is not None

    def test_convert_deduplicates(self, puller, sample_reddit_items):
        """Second call with same items returns empty (already seen)."""
        with patch.object(puller, "_is_duplicate", return_value=False):
            items1 = puller._convert_items("reddit", "test topic", sample_reddit_items)
        assert len(items1) == 2
        # Mark as seen in memory cache
        for item in items1:
            puller._seen_hashes.add(item.dedup_hash)
        # Now _is_duplicate will find them in memory cache
        items2 = puller._convert_items("reddit", "test topic", sample_reddit_items)
        assert len(items2) == 0

    def test_convert_handles_missing_fields(self, puller):
        """Items with minimal fields should not crash."""
        with patch.object(puller, "_is_duplicate", return_value=False):
            items = puller._convert_items("web", "test", [
                {"id": "W1", "title": "Minimal item"},
            ])
        assert len(items) == 1
        assert items[0].url == ""
        assert items[0].author == ""
        assert items[0].engagement_total == 0

    def test_convert_parses_dates(self, puller, sample_reddit_items):
        with patch.object(puller, "_is_duplicate", return_value=False):
            items = puller._convert_items("reddit", "test", sample_reddit_items)
        assert items[0].published is not None
        assert items[0].published.year == 2026

    def test_convert_truncates_long_title(self, puller):
        long_title = "x" * 1000
        with patch.object(puller, "_is_duplicate", return_value=False):
            items = puller._convert_items("web", "test", [
                {"id": "W1", "title": long_title},
            ])
        assert len(items[0].title) <= 500


# ── Unit Tests: Storage ─────────────────────────────────────────────────


class TestStorage:

    def test_store_items_inserts(self, puller):
        items = [
            TrendingItem(
                platform="reddit",
                item_id="R1",
                topic="test",
                title="Test article",
                url="https://example.com",
                content="Test content",
                author="testuser",
                published=datetime(2026, 4, 5, tzinfo=timezone.utc),
                engagement_total=500,
                relevance=0.8,
                score=200,
                raw_engagement={"score": 500},
                dedup_hash="abc123",
            ),
        ]
        stored = puller._store_items(items)
        assert stored == 1

    def test_store_items_empty(self, puller):
        assert puller._store_items([]) == 0

    def test_emit_signal_filters_low_engagement(self, puller):
        """Low engagement items should not emit signals."""
        conn = MagicMock()
        item = TrendingItem(
            platform="reddit",
            item_id="R1",
            topic="test",
            title="Low engagement",
            url="",
            content="",
            author="",
            published=None,
            engagement_total=10,  # Below threshold of 50
            relevance=0.2,  # Below threshold of 0.4
            score=5,
            raw_engagement={},
            dedup_hash="xyz",
        )
        puller._emit_signal(conn, item, date.today())
        conn.execute.assert_not_called()

    def test_emit_signal_high_engagement(self, puller):
        """High engagement items should emit signals."""
        conn = MagicMock()
        item = TrendingItem(
            platform="reddit",
            item_id="R1",
            topic="test",
            title="Big news",
            url="https://example.com",
            content="",
            author="",
            published=None,
            engagement_total=1000,
            relevance=0.9,
            score=200,
            raw_engagement={"score": 1000},
            dedup_hash="xyz",
        )
        puller._emit_signal(conn, item, date.today())
        conn.execute.assert_called_once()


# ── Unit Tests: Deduplication ───────────────────────────────────────────


class TestDeduplication:

    def test_is_duplicate_memory_cache(self, puller):
        puller._seen_hashes.add("known_hash")
        assert puller._is_duplicate("known_hash") is True

    def test_is_not_duplicate_when_db_empty(self, puller, mock_engine):
        """When DB returns no row, item is not a duplicate."""
        mock_engine._mock_conn.execute.return_value.fetchone.return_value = None
        assert puller._is_duplicate("new_hash") is False


# ── Unit Tests: Pull All ────────────────────────────────────────────────


class TestPullAll:

    def test_pull_all_when_unavailable(self, puller):
        """When last30days is unavailable, pull_all returns empty results."""
        puller._last30days_available = False
        result = puller.pull_all()
        assert result["total_items"] == 0
        assert result["topics_researched"] == 1

    @patch.object(TrendingNewsPuller, "pull_topic")
    def test_pull_all_aggregates(self, mock_pull, puller):
        """pull_all should aggregate results from all topics."""
        mock_pull.return_value = {"reddit": 5, "hackernews": 3}
        result = puller.pull_all()
        assert result["total_stored"] == 8
        assert "test topic" in result["topic_results"]

    @patch.object(TrendingNewsPuller, "pull_topic")
    def test_pull_all_handles_errors(self, mock_pull, puller):
        """pull_all should handle topic failures gracefully."""
        mock_pull.side_effect = RuntimeError("API down")
        result = puller.pull_all()
        assert result["total_items"] == 0
        assert result["topic_results"]["test topic"]["platforms"] == {}
        assert result["topic_results"]["test topic"]["total"] == 0


# ── Unit Tests: get_recent ──────────────────────────────────────────────


class TestGetRecent:

    def test_get_recent_no_filters(self, puller):
        result = puller.get_recent()
        assert isinstance(result, list)

    def test_get_recent_with_platform_filter(self, puller):
        result = puller.get_recent(platform="reddit")
        assert isinstance(result, list)

    def test_get_recent_with_topic_filter(self, puller):
        result = puller.get_recent(topic="crypto")
        assert isinstance(result, list)


# ── Integration-style Tests ─────────────────────────────────────────────


class TestTrendingItemDataclass:

    def test_dataclass_fields(self):
        item = TrendingItem(
            platform="reddit",
            item_id="R1",
            topic="test",
            title="Title",
            url="https://example.com",
            content="Content",
            author="author",
            published=datetime(2026, 4, 5, tzinfo=timezone.utc),
            engagement_total=100,
            relevance=0.8,
            score=50,
            raw_engagement={"score": 100},
            dedup_hash="abc123",
            metadata={"cross_refs": ["X1"]},
        )
        assert item.platform == "reddit"
        assert item.metadata == {"cross_refs": ["X1"]}

    def test_default_metadata(self):
        item = TrendingItem(
            platform="web",
            item_id="W1",
            topic="test",
            title="Title",
            url="",
            content="",
            author="",
            published=None,
            engagement_total=0,
            relevance=0.5,
            score=0,
            raw_engagement={},
            dedup_hash="def456",
        )
        assert item.metadata == {}
