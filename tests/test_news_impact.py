"""Tests for the news impact attribution engine."""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("FRED_API_KEY", "test")
os.environ.setdefault("GRID_MASTER_PASSWORD_HASH", "$2b$12$test")

from intelligence.news_impact import (
    Catalyst,
    CatalystClassifier,
    DeepDiveEngine,
    Expectation,
    ExpectationTracker,
    MoveAttribution,
    PriceDecomposer,
    ensure_tables,
)


# ── Fixtures ─────────────────────────────────────────────────────────────

@pytest.fixture
def mock_engine():
    """Create a mock SQLAlchemy engine."""
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    # Default: no existing data
    conn.execute.return_value.fetchone.return_value = None
    conn.execute.return_value.fetchall.return_value = []
    return engine


@pytest.fixture
def classifier(mock_engine):
    return CatalystClassifier(mock_engine)


@pytest.fixture
def decomposer(mock_engine, classifier):
    return PriceDecomposer(mock_engine, classifier)


# ── CatalystClassifier Tests ────────────────────────────────────────────

class TestCatalystClassifier:

    def test_classify_earnings_news(self, classifier):
        cat = classifier.classify_news(
            ticker="NVDA",
            title="NVIDIA beats Q4 earnings by 15%, revenue up 40%",
            summary="Strong data center demand drives record quarter",
            sentiment="BULLISH",
            source="Reuters",
            pub_date=date(2026, 2, 20),
            confidence=0.9,
        )
        assert cat.ticker == "NVDA"
        assert cat.catalyst_type == "earnings"
        assert cat.direction == "bullish"
        assert cat.horizon == "short"
        assert cat.estimated_bps > 0
        assert cat.confidence == 0.9

    def test_classify_regulation_news(self, classifier):
        cat = classifier.classify_news(
            ticker="GOOGL",
            title="DOJ antitrust probe into Google ad business expands",
            summary="Department of Justice widens investigation",
            sentiment="BEARISH",
            source="WSJ",
            pub_date=date(2026, 3, 15),
        )
        assert cat.catalyst_type == "regulation"
        assert cat.direction == "bearish"
        assert cat.horizon == "long"

    def test_classify_macro_news(self, classifier):
        cat = classifier.classify_news(
            ticker="SPY",
            title="Fed signals rate cut in June, inflation moderating",
            summary="FOMC minutes show dovish tilt",
            sentiment="BULLISH",
            source="Bloomberg",
            pub_date=date(2026, 3, 20),
        )
        assert cat.catalyst_type == "macro_data"
        assert cat.direction == "bullish"

    def test_classify_unknown_news(self, classifier):
        cat = classifier.classify_news(
            ticker="AAPL",
            title="Apple CEO Tim Cook visits India office",
            summary="CEO met with local staff",
            sentiment="NEUTRAL",
            source="CNBC",
            pub_date=date(2026, 3, 10),
        )
        assert cat.catalyst_type == "unknown"
        assert cat.direction == "neutral"

    def test_classify_strong_language_amplifies(self, classifier):
        normal = classifier.classify_news(
            ticker="TSLA", title="Tesla reports earnings",
            summary="Results in line", sentiment="NEUTRAL",
            source="test", pub_date=date(2026, 3, 1),
        )
        strong = classifier.classify_news(
            ticker="TSLA", title="Tesla earnings surge to historic levels",
            summary="Massive beat on all metrics", sentiment="BULLISH",
            source="test", pub_date=date(2026, 3, 1), confidence=0.9,
        )
        # Strong language should produce higher bps estimate
        assert strong.estimated_bps > normal.estimated_bps

    def test_classify_signal_insider_buy(self, classifier):
        cat = classifier.classify_signal(
            ticker="AAPL",
            signal_type="BUY",
            signal_value={"amount": 500000},
            signal_date=date(2026, 3, 25),
            actor="Tim Cook",
        )
        assert cat.catalyst_type == "insider_trade"
        assert cat.direction == "bullish"
        assert "Tim Cook" in cat.title

    def test_classify_signal_unusual_options(self, classifier):
        cat = classifier.classify_signal(
            ticker="NVDA",
            signal_type="UNUSUAL_OPTIONS",
            signal_value={"volume": 10000},
            signal_date=date(2026, 3, 25),
        )
        assert cat.catalyst_type == "options_flow"

    def test_classify_signal_contract_award(self, classifier):
        cat = classifier.classify_signal(
            ticker="RTX",
            signal_type="CONTRACT_AWARD",
            signal_value={"amount": 1_000_000_000},
            signal_date=date(2026, 3, 20),
            actor="US DoD",
        )
        assert cat.catalyst_type == "gov_contract"
        assert cat.direction == "bullish"

    def test_id_is_deterministic(self, classifier):
        c1 = classifier.classify_news(
            ticker="AAPL", title="Test",
            summary="", sentiment="NEUTRAL",
            source="x", pub_date=date(2026, 1, 1),
        )
        c2 = classifier.classify_news(
            ticker="AAPL", title="Test",
            summary="", sentiment="NEUTRAL",
            source="x", pub_date=date(2026, 1, 1),
        )
        assert c1.id == c2.id


# ── MoveAttribution Tests ───────────────────────────────────────────────

class TestMoveAttribution:

    def test_move_attribution_dataclass(self):
        attr = MoveAttribution(
            ticker="NVDA",
            move_date=date(2026, 3, 20),
            move_pct=0.05,
            move_direction="up",
            total_explained_bps=350,
            unexplained_bps=150,
        )
        assert attr.ticker == "NVDA"
        assert attr.move_pct == 0.05
        assert attr.total_explained_bps + attr.unexplained_bps == 500

    def test_decompose_with_no_data(self, decomposer):
        # No price data or catalysts → should still return valid attribution
        attr = decomposer.decompose_move("AAPL", date(2026, 3, 20), 0.03)
        assert attr.ticker == "AAPL"
        assert attr.move_pct == 0.03


# ── ExpectationTracker Tests ─────────────────────────────────────────────

class TestExpectationTracker:

    def test_compute_net_expectations_empty(self, mock_engine):
        tracker = ExpectationTracker(mock_engine)
        net = tracker.compute_net_expectations("AAPL")
        assert net["total_baked_in_bps"] == 0
        assert net["total_pending_bps"] == 0
        assert net["active_count"] == 0

    def test_expectation_dataclass(self):
        exp = Expectation(
            id="test1",
            ticker="NVDA",
            description="Q4 earnings beat by 15%",
            catalyst_type="earnings",
            horizon="short",
            expected_direction="bullish",
            expected_magnitude_bps=300,
            baked_in_pct=70,
            deadline=date(2026, 2, 20),
            status="active",
        )
        assert exp.baked_in_pct == 70
        assert exp.expected_magnitude_bps == 300
        # 70% baked in means 30% still pending
        pending = exp.expected_magnitude_bps * (100 - exp.baked_in_pct) / 100
        assert pending == 90


# ── Catalyst Type Detection ──────────────────────────────────────────────

class TestCatalystTypeDetection:

    def test_guidance_detection(self, classifier):
        assert classifier._detect_type("company raised full-year guidance") == "guidance"

    def test_ma_detection(self, classifier):
        assert classifier._detect_type("company announces $10b acquisition of rival") == "m_and_a"

    def test_geopolitical_detection(self, classifier):
        assert classifier._detect_type("new china tariff on semiconductor exports") == "geopolitical"

    def test_legal_detection(self, classifier):
        assert classifier._detect_type("patent lawsuit filed against competitor") == "legal"

    def test_analyst_detection(self, classifier):
        assert classifier._detect_type("goldman upgrade to overweight, price target $200") == "analyst_rating"

    def test_product_detection(self, classifier):
        assert classifier._detect_type("apple to unveil new ai chip at wwdc") == "product_launch"


# ── Horizon Detection ────────────────────────────────────────────────────

class TestHorizonDetection:

    def test_short_term_explicit(self, classifier):
        assert classifier._detect_horizon("earnings report today", "earnings") == "short"

    def test_medium_term_quarter(self, classifier):
        assert classifier._detect_horizon("next quarter pipeline looks strong", "guidance") == "medium"

    def test_long_term_structural(self, classifier):
        assert classifier._detect_horizon("long-term structural shift in AI", "product_launch") == "long"

    def test_default_from_type(self, classifier):
        assert classifier._detect_horizon("some news", "regulation") == "long"
        assert classifier._detect_horizon("some news", "earnings") == "short"
        assert classifier._detect_horizon("some news", "insider_trade") == "medium"


# ── Deep Dive Engine Tests ───────────────────────────────────────────────

class TestDeepDiveEngine:

    def test_engine_init(self, mock_engine):
        dive = DeepDiveEngine(mock_engine)
        assert dive.engine == mock_engine
        assert isinstance(dive.classifier, CatalystClassifier)
        assert isinstance(dive.decomposer, PriceDecomposer)
        assert isinstance(dive.tracker, ExpectationTracker)

    def test_ticker_names(self):
        names = DeepDiveEngine.TICKER_NAMES
        assert names["AAPL"] == "Apple"
        assert names["NVDA"] == "NVIDIA"
        assert "TSLA" in names

    @patch("intelligence.news_impact.DeepDiveEngine._generate_narrative")
    @patch("intelligence.news_impact.DeepDiveEngine._store_report")
    def test_generate_deep_dive_minimal(self, mock_store, mock_narrative, mock_engine):
        mock_narrative.return_value = "Test narrative"
        dive = DeepDiveEngine(mock_engine)
        report = dive.generate_deep_dive("AAPL", days=30)
        assert report.ticker == "AAPL"
        assert report.name == "Apple"
        assert report.narrative == "Test narrative"
        mock_store.assert_called_once()
