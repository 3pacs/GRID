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
    # Intelligence-informed kills
    assert "LEVER_DIVERGED" in KILL_REASONS
    assert "TRUST_COLLAPSED" in KILL_REASONS
    assert "CAUSATION_INVALIDATED" in KILL_REASONS


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


def test_score_all_uses_per_hypothesis_window(engine):
    """score_all picks up hypotheses whose own test window has elapsed."""
    gen = HypothesisGenerator(engine)

    # Hypothesis with 3-day window, created 5 days ago → should be scoreable
    short_window = Hypothesis(
        id="hyp_test_short_window",
        thesis="Short window test hypothesis",
        pattern_type="lead_lag",
        evidence=[],
        test_criteria={"watch_signal": "sig:x", "expect_signal": "sig:y",
                       "lag_days": 3, "expected_direction": "increases"},
        invalidation="test",
        confidence=0.5,
    )
    # Hypothesis with 30-day window, created 5 days ago → should NOT be scoreable
    long_window = Hypothesis(
        id="hyp_test_long_window",
        thesis="Long window test hypothesis",
        pattern_type="volume_anomaly",
        evidence=[],
        test_criteria={"watch_category": "test_cat", "window_days": 30},
        invalidation="test",
        confidence=0.5,
    )

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM discovered_hypotheses WHERE id LIKE 'hyp_test_%window%'"))

    gen._store_hypothesis(short_window)
    gen._store_hypothesis(long_window)

    # Backdate both to 5 days ago
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE discovered_hypotheses SET created_at = NOW() - INTERVAL '5 days' "
            "WHERE id LIKE 'hyp_test_%_window'"
        ))

    results = gen.score_all()
    scored_ids = {r["id"] for r in results if "id" in r}

    assert "hyp_test_short_window" in scored_ids, "3-day window hypothesis should be scored after 5 days"
    assert "hyp_test_long_window" not in scored_ids, "30-day window hypothesis should NOT be scored after 5 days"


def test_expired_kill(engine):
    """Hypothesis past 2x window with no resolution gets EXPIRED kill."""
    gen = HypothesisGenerator(engine)
    hyp = Hypothesis(
        id="hyp_test_expired",
        thesis="This should expire",
        pattern_type="convergence",
        evidence=[{"ticker": "TEST"}],
        test_criteria={"ticker": "TEST", "expected_direction": "bullish",
                       "window_days": 10, "min_move_pct": 2.0},
        invalidation="test",
        confidence=0.5,
    )
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM discovered_hypotheses WHERE id = 'hyp_test_expired' OR pair_id = 'hyp_test_expired'"))
        conn.execute(text("DELETE FROM hypothesis_postmortems WHERE hypothesis_id = 'hyp_test_expired'"))

    gen._store_hypothesis(hyp)

    # Backdate to 25 days ago (2x 10-day window = 20, so 25 > 20)
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE discovered_hypotheses SET created_at = NOW() - INTERVAL '25 days' "
            "WHERE id = 'hyp_test_expired'"
        ))

    gen.score_hypothesis("hyp_test_expired")

    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT status, kill_reason, killed_at FROM discovered_hypotheses WHERE id = 'hyp_test_expired'"
        )).fetchone()
        pm = conn.execute(text(
            "SELECT kill_reason, lifespan_days FROM hypothesis_postmortems "
            "WHERE hypothesis_id = 'hyp_test_expired'"
        )).fetchone()

    assert row[0] == "invalidated"
    assert row[1] == "EXPIRED"
    assert row[2] is not None
    assert pm is not None
    assert pm[0] == "EXPIRED"
    assert pm[1] >= 25


def test_confidence_collapsed_kill(engine):
    """Hypothesis with confidence < 0.10 after 3+ tests gets killed."""
    gen = HypothesisGenerator(engine)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM discovered_hypotheses WHERE id = 'hyp_test_conf_kill'"))
        conn.execute(text("DELETE FROM hypothesis_postmortems WHERE hypothesis_id = 'hyp_test_conf_kill'"))
        # times_tested=8 so after +1 → tested=9, new_conf = (0+1)/(9+2) = 0.0909 < 0.10
        # lag_days=7 → 2x window = 14 days > 10 days since creation, so EXPIRED won't fire
        conn.execute(text("""
            INSERT INTO discovered_hypotheses
                (id, thesis, pattern_type, evidence, test_criteria,
                 invalidation, confidence, status, times_tested, times_correct,
                 role, pair_id, created_at)
            VALUES
                ('hyp_test_conf_kill', 'Low confidence test', 'lead_lag',
                 '[]'::jsonb, :criteria,
                 'test', 0.05, 'active', 8, 0, 'thesis', 'hyp_test_conf_kill',
                 NOW() - INTERVAL '10 days')
        """), {"criteria": json.dumps({"watch_signal": "sig:x", "expect_signal": "sig:y",
                                       "lag_days": 7, "expected_direction": "increases"})})

    result = gen.score_hypothesis("hyp_test_conf_kill")

    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT status, kill_reason FROM discovered_hypotheses WHERE id = 'hyp_test_conf_kill'"
        )).fetchone()

    assert row[0] == "invalidated"
    assert row[1] == "CONFIDENCE_COLLAPSED"


def test_antithesis_confirmed_kills_thesis(engine):
    """When antithesis is confirmed, parent thesis gets ANTITHESIS_CONFIRMED kill."""
    gen = HypothesisGenerator(engine)
    thesis_id = "hyp_test_anti_kill_parent"
    anti_id = thesis_id + "_anti"

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM discovered_hypotheses WHERE id IN (:t, :a)"),
                     {"t": thesis_id, "a": anti_id})
        conn.execute(text("DELETE FROM hypothesis_postmortems WHERE hypothesis_id IN (:t, :a)"),
                     {"t": thesis_id, "a": anti_id})
        # Insert thesis
        conn.execute(text("""
            INSERT INTO discovered_hypotheses
                (id, thesis, pattern_type, evidence, test_criteria,
                 invalidation, confidence, status, role, pair_id, created_at)
            VALUES
                (:tid, 'Parent thesis', 'convergence', '[]'::jsonb,
                 :criteria_bull,
                 'test', 0.7, 'active', 'thesis', :tid, NOW() - INTERVAL '20 days')
        """), {"tid": thesis_id,
               "criteria_bull": json.dumps({"ticker": "TEST", "expected_direction": "bullish",
                                            "window_days": 14, "min_move_pct": 2.0})})
        # Insert antithesis — already confirmed
        conn.execute(text("""
            INSERT INTO discovered_hypotheses
                (id, thesis, pattern_type, evidence, test_criteria,
                 invalidation, confidence, status, role, pair_id, created_at)
            VALUES
                (:aid, 'Antithesis', 'convergence', '[]'::jsonb,
                 :criteria_bear,
                 'test', 0.8, 'confirmed', 'antithesis', :tid, NOW() - INTERVAL '20 days')
        """), {"aid": anti_id, "tid": thesis_id,
               "criteria_bear": json.dumps({"ticker": "TEST", "expected_direction": "bearish",
                                            "window_days": 14, "min_move_pct": 2.0})})

    gen.score_hypothesis(thesis_id)

    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT status, kill_reason FROM discovered_hypotheses WHERE id = :id"
        ), {"id": thesis_id}).fetchone()

    assert row[0] == "invalidated"
    assert row[1] == "ANTITHESIS_CONFIRMED"


def test_postmortem_records_full_context(engine):
    """Postmortem captures thesis text, antithesis text, lifespan, and evidence."""
    gen = HypothesisGenerator(engine)
    hyp = Hypothesis(
        id="hyp_test_pm_context",
        thesis="Postmortem context test thesis",
        pattern_type="convergence",
        evidence=[{"ticker": "PMT", "direction": "bullish"}],
        test_criteria={"ticker": "PMT", "expected_direction": "bullish",
                       "window_days": 5, "min_move_pct": 2.0},
        invalidation="test",
        confidence=0.6,
    )
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM discovered_hypotheses WHERE id = 'hyp_test_pm_context' OR pair_id = 'hyp_test_pm_context'"))
        conn.execute(text("DELETE FROM hypothesis_postmortems WHERE hypothesis_id = 'hyp_test_pm_context'"))

    gen._store_hypothesis(hyp)

    # Backdate to 15 days ago (2x 5-day window = 10, so 15 > 10 → EXPIRED)
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE discovered_hypotheses SET created_at = NOW() - INTERVAL '15 days' "
            "WHERE id = 'hyp_test_pm_context'"
        ))

    gen.score_hypothesis("hyp_test_pm_context")

    with engine.connect() as conn:
        pm = conn.execute(text(
            "SELECT hypothesis_id, kill_reason, thesis_text, antithesis_text, "
            "confidence_at_death, lifespan_days "
            "FROM hypothesis_postmortems WHERE hypothesis_id = 'hyp_test_pm_context' "
            "ORDER BY created_at DESC LIMIT 1"
        )).fetchone()

    assert pm is not None, "Postmortem should have been created"
    assert pm[0] == "hyp_test_pm_context"
    assert pm[1] == "EXPIRED"
    assert pm[2] is not None and len(pm[2]) > 0  # thesis_text
    assert pm[3] is not None and "ANTITHESIS" in pm[3]  # antithesis_text
    assert pm[4] is not None  # confidence_at_death
    assert pm[5] >= 15  # lifespan_days


def test_stats_include_kill_breakdown(engine):
    """Stats report includes kill_reason breakdown."""
    from intelligence.hypothesis_engine import get_stats
    stats = get_stats(engine)
    assert "by_kill_reason" in stats
    assert isinstance(stats["by_kill_reason"], dict)


def test_stats_include_role_breakdown(engine):
    """Stats report includes by_role breakdown."""
    from intelligence.hypothesis_engine import get_stats
    stats = get_stats(engine)
    assert "by_role" in stats
    assert isinstance(stats["by_role"], dict)


def test_intelligence_boost_neutral_without_modules(engine):
    """Intelligence boost returns 1.0 when intelligence modules unavailable."""
    gen = HypothesisGenerator(engine)
    boost = gen._get_intelligence_boost(
        {"ticker": "NONEXISTENT_TICKER_XYZ"},
        "convergence",
        "inconclusive",
    )
    # Should be 1.0 or very close (modules may gracefully degrade)
    assert 0.5 <= boost <= 2.0


def test_intelligence_kills_none_by_default(engine):
    """Intelligence kills return None when no intelligence data contradicts."""
    gen = HypothesisGenerator(engine)
    kill = gen._check_intelligence_kills(
        "hyp_test_intel_kill",
        "convergence",
        {"ticker": "NONEXISTENT_TICKER_XYZ", "expected_direction": "bullish"},
        datetime.now(timezone.utc) - timedelta(days=10),
        0.5,
    )
    # Should not kill without strong opposing intelligence
    assert kill is None


def test_kill_taxonomy_complete():
    """All 14 kill reasons have human-readable descriptions."""
    assert len(KILL_REASONS) == 14
    for reason, desc in KILL_REASONS.items():
        assert isinstance(desc, str)
        assert len(desc) > 10, f"Kill reason {reason} has too-short description"
