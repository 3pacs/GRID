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


def test_antithesis_generated_for_lead_lag(engine):
    """Storing a lead_lag thesis auto-creates its antithesis."""
    gen = HypothesisGenerator(engine)
    hyp = Hypothesis(
        id="hyp_test_ll_thesis",
        thesis="When sig:A spikes, sig:B increases within 5 days",
        pattern_type="lead_lag",
        evidence=[{"signal_a": "sig:A", "signal_b": "sig:B", "lag_days": 5}],
        test_criteria={
            "watch_signal": "sig:A",
            "expect_signal": "sig:B",
            "lag_days": 5,
            "expected_direction": "increases",
        },
        invalidation="If sig:A spikes 3+ times and sig:B does NOT increase",
        confidence=0.65,
    )
    # Clean up from prior runs
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM discovered_hypotheses WHERE id = :t OR id = :a OR pair_id = :t"),
                     {"t": "hyp_test_ll_thesis", "a": "hyp_test_ll_thesis_anti"})

    gen._store_hypothesis(hyp)

    with engine.connect() as conn:
        thesis_row = conn.execute(text(
            "SELECT role, pair_id FROM discovered_hypotheses WHERE id = :id"
        ), {"id": "hyp_test_ll_thesis"}).fetchone()
        anti_row = conn.execute(text(
            "SELECT id, role, pair_id, thesis, pattern_type, test_criteria "
            "FROM discovered_hypotheses WHERE pair_id = :pid AND role = 'antithesis'"
        ), {"pid": "hyp_test_ll_thesis"}).fetchone()

    assert thesis_row is not None
    assert thesis_row[0] == "thesis"
    assert thesis_row[1] == "hyp_test_ll_thesis"

    assert anti_row is not None
    assert anti_row[1] == "antithesis"
    assert anti_row[2] == "hyp_test_ll_thesis"
    assert "does NOT" in anti_row[3] or "fails to" in anti_row[3]


def test_antithesis_generated_for_convergence(engine):
    """Storing a convergence thesis auto-creates its antithesis."""
    gen = HypothesisGenerator(engine)
    hyp = Hypothesis(
        id="hyp_test_conv_thesis",
        thesis="CONVERGENCE: 4 sources agree AAPL is heading bullish",
        pattern_type="convergence",
        evidence=[{"ticker": "AAPL", "direction": "bullish", "n_sources": 4}],
        test_criteria={
            "ticker": "AAPL",
            "expected_direction": "bullish",
            "window_days": 14,
            "min_move_pct": 2.0,
        },
        invalidation="If AAPL moves opposite to bullish by >2% within 14 days",
        confidence=0.72,
    )
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM discovered_hypotheses WHERE pair_id = :pid OR id = :id"),
                     {"pid": "hyp_test_conv_thesis", "id": "hyp_test_conv_thesis"})

    gen._store_hypothesis(hyp)

    with engine.connect() as conn:
        anti = conn.execute(text(
            "SELECT thesis, test_criteria FROM discovered_hypotheses "
            "WHERE pair_id = :pid AND role = 'antithesis'"
        ), {"pid": "hyp_test_conv_thesis"}).fetchone()

    assert anti is not None
    anti_criteria = anti[1] if isinstance(anti[1], dict) else json.loads(anti[1])
    assert anti_criteria["expected_direction"] in ("bearish", "down")
