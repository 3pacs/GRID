"""Tests for intelligence.context_provider — LLM prompt context injection."""

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock
from sqlalchemy import create_engine, text


@pytest.fixture
def engine():
    """Create an in-memory SQLite engine with hypothesis tables."""
    eng = create_engine("sqlite:///:memory:")
    with eng.connect() as conn:
        conn.execute(text("""
            CREATE TABLE discovered_hypotheses (
                id TEXT PRIMARY KEY,
                thesis TEXT NOT NULL,
                pattern_type TEXT,
                confidence REAL,
                status TEXT DEFAULT 'active',
                times_tested INTEGER DEFAULT 0,
                times_correct INTEGER DEFAULT 0,
                role TEXT DEFAULT 'thesis',
                pair_id TEXT,
                created_at TIMESTAMP
            )
        """))
        conn.execute(text("""
            CREATE TABLE hypothesis_postmortems (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hypothesis_id TEXT,
                kill_reason TEXT,
                thesis_text TEXT,
                antithesis_text TEXT,
                confidence_at_death REAL,
                times_tested INTEGER DEFAULT 0,
                times_correct INTEGER DEFAULT 0,
                lifespan_days INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        conn.execute(text("""
            CREATE TABLE company_profiles (
                ticker TEXT PRIMARY KEY,
                suspicion_score REAL DEFAULT 0,
                sector TEXT,
                profile TEXT
            )
        """))
        conn.commit()
    return eng


@pytest.fixture
def seeded_engine(engine):
    """Engine with sample data."""
    now = datetime.now(timezone.utc)
    with engine.connect() as conn:
        # Thesis
        conn.execute(text("""
            INSERT INTO discovered_hypotheses (id, thesis, pattern_type, confidence, status, role, pair_id, times_tested, times_correct, created_at)
            VALUES
            ('hyp_001', 'When congressional trading spikes, insider filings increase within 5 days', 'lead_lag', 0.72, 'active', 'thesis', 'hyp_001', 3, 2, :now),
            ('hyp_002', 'Congressional trading does NOT lead insider filings (noise)', 'lead_lag', 0.28, 'active', 'antithesis', 'hyp_001', 3, 1, :now),
            ('hyp_003', 'CONVERGENCE: 4 sources agree AAPL heading bullish', 'convergence', 0.65, 'active', 'thesis', 'hyp_003', 0, 0, :now),
            ('hyp_004', 'AAPL bearish (convergence misleading)', 'convergence', 0.35, 'active', 'antithesis', 'hyp_003', 0, 0, :now),
            ('hyp_005', 'Dead hypothesis', 'lead_lag', 0.05, 'invalidated', 'thesis', 'hyp_005', 5, 0, :now)
        """), {"now": now})

        # Postmortem
        conn.execute(text("""
            INSERT INTO hypothesis_postmortems (hypothesis_id, kill_reason, thesis_text, antithesis_text, confidence_at_death, times_tested, times_correct, lifespan_days, created_at)
            VALUES ('hyp_005', 'CONFIDENCE_COLLAPSED', 'Volume anomaly leads to price reversal', 'Volume anomaly is noise', 0.05, 5, 0, 14, :now)
        """), {"now": now})

        conn.commit()
    return engine


class TestGetActiveHypotheses:
    def test_returns_empty_for_no_data(self, engine):
        from intelligence.context_provider import get_active_hypotheses
        result = get_active_hypotheses(engine)
        assert result == ""

    def test_returns_formatted_hypotheses(self, seeded_engine):
        from intelligence.context_provider import get_active_hypotheses
        result = get_active_hypotheses(seeded_engine)
        assert "ACTIVE HYPOTHESES" in result
        assert "congressional trading" in result
        assert "conf=0.72" in result
        assert "2/3" in result  # accuracy
        assert "ANTI:" in result  # antithesis shown

    def test_excludes_dead_hypotheses(self, seeded_engine):
        from intelligence.context_provider import get_active_hypotheses
        result = get_active_hypotheses(seeded_engine)
        assert "Dead hypothesis" not in result

    def test_only_shows_thesis_role(self, seeded_engine):
        from intelligence.context_provider import get_active_hypotheses
        result = get_active_hypotheses(seeded_engine)
        lines = [l for l in result.split("\n") if l.startswith("- [")]
        # Should only have 2 thesis entries (hyp_001 and hyp_003), not antitheses
        assert len(lines) == 2

    def test_respects_limit(self, seeded_engine):
        from intelligence.context_provider import get_active_hypotheses
        result = get_active_hypotheses(seeded_engine, limit=1)
        lines = [l for l in result.split("\n") if l.startswith("- [")]
        assert len(lines) == 1

    def test_handles_db_error_gracefully(self):
        from intelligence.context_provider import get_active_hypotheses
        bad_engine = MagicMock()
        bad_engine.connect.side_effect = Exception("DB down")
        result = get_active_hypotheses(bad_engine)
        assert result == ""


class TestGetRecentPostmortems:
    def test_returns_empty_for_no_data(self, engine):
        from intelligence.context_provider import get_recent_postmortems
        result = get_recent_postmortems(engine)
        assert result == ""

    def test_returns_formatted_postmortems(self, seeded_engine):
        from intelligence.context_provider import get_recent_postmortems
        result = get_recent_postmortems(seeded_engine)
        assert "KILL POSTMORTEMS" in result
        assert "CONFIDENCE_COLLAPSED" in result
        assert "Volume anomaly" in result
        assert "avoid repeating" in result

    def test_handles_db_error_gracefully(self):
        from intelligence.context_provider import get_recent_postmortems
        bad_engine = MagicMock()
        bad_engine.connect.side_effect = Exception("DB down")
        result = get_recent_postmortems(bad_engine)
        assert result == ""


class TestBuildFullContext:
    def test_combines_all_sections(self, seeded_engine):
        from intelligence.context_provider import build_full_context
        result = build_full_context(seeded_engine)
        assert "ACTIVE HYPOTHESES" in result
        assert "KILL POSTMORTEMS" in result

    def test_returns_empty_for_no_data(self, engine):
        from intelligence.context_provider import build_full_context
        result = build_full_context(engine)
        assert result == ""

    def test_handles_all_failures_gracefully(self):
        from intelligence.context_provider import build_full_context
        bad_engine = MagicMock()
        bad_engine.connect.side_effect = Exception("DB down")
        result = build_full_context(bad_engine)
        assert result == ""


class TestGetHypothesisContextForTicker:
    def test_finds_ticker_mentions(self, seeded_engine):
        from intelligence.context_provider import get_hypothesis_context_for_ticker
        result = get_hypothesis_context_for_ticker(seeded_engine, "AAPL")
        assert "AAPL" in result
        assert "convergence" in result

    def test_returns_empty_for_no_match(self, seeded_engine):
        from intelligence.context_provider import get_hypothesis_context_for_ticker
        result = get_hypothesis_context_for_ticker(seeded_engine, "ZZZZ")
        assert result == ""
