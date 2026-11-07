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
from intelligence import hypothesis_engine


@pytest.fixture(autouse=True)
def _scoring_unheld(monkeypatch):
    # These tests pin kill/scoring behaviour when enabled; the production
    # hold is covered by tests/test_hypothesis_scoring_hold.py.
    monkeypatch.setattr(hypothesis_engine, "ACTIVE_HYPOTHESIS_SCORING_HELD", False)


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


def _ledgered(trials: list[dict]):
    """Record ``trials`` as one S11 hypothesis batch in an in-memory ledger."""
    import analysis.ledger_steered_exploration as lse
    from intelligence import hypothesis_trials

    ledger = hypothesis_trials.dry_run_ledger()
    full = [{
        "identity_sha256": t["hypothesis_id"], "pattern_type": "convergence",
        "test_kind": "ticker_move", "created_at": "2026-09-01T00:00:00+00:00",
        "effect_ok": t["status"] == "tested", "reason": t.get("reason"), **t,
    } for t in trials]
    return lse.record_hypothesis_batch(ledger, scorer_version="test", evaluated_at="x", trials=full)


def _insert_pair(engine, thesis_id: str, days_ago: int = 60) -> str:
    anti_id = thesis_id + "_anti"
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM discovered_hypotheses WHERE id IN (:t, :a)"),
                     {"t": thesis_id, "a": anti_id})
        conn.execute(text("DELETE FROM hypothesis_postmortems WHERE hypothesis_id IN (:t, :a)"),
                     {"t": thesis_id, "a": anti_id})
        for hid, role, thesis, direction in (
            (thesis_id, "thesis", "Parent thesis", "bullish"),
            (anti_id, "antithesis", "ANTITHESIS: misleading", "bearish"),
        ):
            conn.execute(text("""
                INSERT INTO discovered_hypotheses
                    (id, thesis, pattern_type, evidence, test_criteria,
                     invalidation, confidence, status, role, pair_id, created_at)
                VALUES
                    (:id, :thesis, 'convergence', '[]'::jsonb, :criteria,
                     'test', 0.5, 'active', :role, :pair, NOW() - make_interval(days => :days))
            """), {"id": hid, "thesis": thesis, "role": role, "pair": thesis_id, "days": days_ago,
                   "criteria": json.dumps({"ticker": "TEST", "expected_direction": direction,
                                           "window_days": 14, "min_move_pct": 2.0})})
    return anti_id


def test_single_row_scoring_never_writes_a_verdict(engine):
    """score_hypothesis defers to the ledgered batch: the row is untouched."""
    thesis_id = "hyp_test_v2_defer"
    _insert_pair(engine, thesis_id)
    result = HypothesisGenerator(engine).score_hypothesis(thesis_id)
    assert result["outcome"] == "deferred"
    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT status, times_tested, kill_reason FROM discovered_hypotheses WHERE id = :id"
        ), {"id": thesis_id}).fetchone()
    assert tuple(row) == ("active", 0, None)


def test_confirmed_thesis_kills_its_antithesis(engine):
    from intelligence import hypothesis_trials

    hypothesis_trials.ensure_trial_columns(engine)
    thesis_id = "hyp_test_v2_anti_kill"
    anti_id = _insert_pair(engine, thesis_id)
    rec = _ledgered([{"hypothesis_id": thesis_id, "status": "tested", "p_value": 0.0001}])
    counts = hypothesis_trials.apply_verdicts(engine, rec["record"], rec["sha256"])
    assert counts["confirmed"] == 1
    with engine.connect() as conn:
        rows = dict(conn.execute(text(
            "SELECT id, status || ':' || COALESCE(kill_reason, '') FROM discovered_hypotheses "
            "WHERE id IN (:t, :a)"
        ), {"t": thesis_id, "a": anti_id}).fetchall())
        sha = conn.execute(text(
            "SELECT trial_ledger_sha256 FROM discovered_hypotheses WHERE id = :t"
        ), {"t": thesis_id}).scalar()
    assert rows[thesis_id] == "confirmed:"
    assert rows[anti_id] == "invalidated:ANTITHESIS_CONFIRMED"
    assert sha == rec["sha256"]


def test_invalidated_trial_writes_postmortem_with_context(engine):
    from intelligence import hypothesis_trials

    hypothesis_trials.ensure_trial_columns(engine)
    thesis_id = "hyp_test_v2_pm_context"
    _insert_pair(engine, thesis_id, days_ago=30)
    rec = _ledgered([{"hypothesis_id": thesis_id, "status": "tested", "p_value": 0.7}])
    hypothesis_trials.apply_verdicts(engine, rec["record"], rec["sha256"])
    with engine.connect() as conn:
        pm = conn.execute(text(
            "SELECT kill_reason, thesis_text, antithesis_text, lifespan_days "
            "FROM hypothesis_postmortems WHERE hypothesis_id = :id"
        ), {"id": thesis_id}).fetchone()
    assert pm is not None
    assert pm[0] == "NOT_SIGNIFICANT"
    assert pm[1] == "Parent thesis"
    assert "ANTITHESIS" in pm[2]
    assert pm[3] >= 30


def test_apply_verdicts_is_idempotent(engine):
    from intelligence import hypothesis_trials

    hypothesis_trials.ensure_trial_columns(engine)
    thesis_id = "hyp_test_v2_idempotent"
    _insert_pair(engine, thesis_id)
    rec = _ledgered([{"hypothesis_id": thesis_id, "status": "untestable", "p_value": 1.0,
                      "reason": "NO_DATA"}])
    first = hypothesis_trials.apply_verdicts(engine, rec["record"], rec["sha256"])
    second = hypothesis_trials.apply_verdicts(engine, rec["record"], rec["sha256"])
    assert first["expired"] == 1 and second["unchanged"] == 1
    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT status, kill_reason, times_tested FROM discovered_hypotheses WHERE id = :id"
        ), {"id": thesis_id}).fetchone()
    assert tuple(row) == ("expired", "NO_DATA", 1)


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
        "hyp_test_boost",
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
    """All 21 kill reasons (14 v1 + 7 v2 trial reasons) have descriptions."""
    from intelligence import hypothesis_trials as ht

    assert len(KILL_REASONS) == 21
    for reason in (ht.SELF_TELEMETRY, ht.NO_MARKET_OUTCOME, ht.NO_DIRECTION,
                   ht.UNSUPPORTED_INSTRUMENT, ht.NO_DATA, ht.INSUFFICIENT_HISTORY,
                   ht.NOT_SIGNIFICANT):
        assert reason in KILL_REASONS
    for reason, desc in KILL_REASONS.items():
        assert isinstance(desc, str)
        assert len(desc) > 10, f"Kill reason {reason} has too-short description"
