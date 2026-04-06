"""Tests for hypothesis kill system, antithesis tracking, and postmortems."""

import json
import pytest
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from intelligence.hypothesis_engine import (
    ensure_tables,
    HypothesisGenerator,
    Hypothesis,
    KILL_REASONS,
)


@pytest.fixture
def engine():
    """Create a test engine with hypothesis tables."""
    eng = create_engine("postgresql://grid_user:changeme@localhost:5432/grid")
    try:
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception:
        pytest.skip("PostgreSQL not available")
    ensure_tables(eng)
    yield eng
    eng.dispose()


def test_schema_has_new_columns(engine):
    """New columns exist on discovered_hypotheses."""
    with engine.connect() as conn:
        cols = {
            row[0]
            for row in conn.execute(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'discovered_hypotheses'"
            ))
        }
    assert "role" in cols
    assert "pair_id" in cols
    assert "kill_reason" in cols
    assert "killed_at" in cols


def test_postmortem_table_exists(engine):
    """hypothesis_postmortems table exists with expected columns."""
    with engine.connect() as conn:
        cols = {
            row[0]
            for row in conn.execute(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'hypothesis_postmortems'"
            ))
        }
    expected = {
        "id", "hypothesis_id", "kill_reason", "evidence",
        "thesis_text", "antithesis_text", "confidence_at_death",
        "times_tested", "times_correct", "lifespan_days", "created_at",
    }
    assert expected.issubset(cols)


def test_kill_reasons_constant_is_exported():
    """KILL_REASONS dict is importable and non-empty."""
    assert len(KILL_REASONS) >= 10
    assert "ANTITHESIS_CONFIRMED" in KILL_REASONS
    assert "EXPIRED" in KILL_REASONS
    assert "PATTERN_BROKEN" in KILL_REASONS
    assert "WRONG_DIRECTION" in KILL_REASONS
    assert "NO_FOLLOW_THROUGH" in KILL_REASONS
    assert "ACTOR_RETREATED" in KILL_REASONS
