"""Surfacer must not invent numbers it does not have.

Covers audit findings B-H6 (a `score_parts.backtest` literal with no backtest
behind it), B-H7 (a missing confidence becoming 0.30/0.35), B-M15 (regime
defaulting to the string "NEUTRAL" and then selecting regime-conditional Brier
history), and B-M16 (every unweighted evidence item getting weight 0.5).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from api.routers.surfacer import (
    _build_conviction_gate,
    _build_operator_brief,
    _evidence_from_payload,
    _extract_calibration_context,
    _fetch_signal_scorecards,
    _hypothesis_candidate,
    _oracle_candidate,
    _signal_candidate,
)


def _row(**kwargs):
    return SimpleNamespace(_mapping=kwargs)


def _signal_row(**overrides):
    base = {
        "id": 1,
        "signal_type": "insider_cluster",
        "signal_date": "2026-04-18",
        "ticker": "NVDA",
        "actor": None,
        "direction": "up",
        "magnitude": 5_000_000,
        "description": "Cluster of Form 4 buys",
        "data": '[{"label":"form4","detail":"three buys"}]',
        "confidence": 0.71,
        "source_id": 1,
        "created_at": datetime.now(timezone.utc) - timedelta(hours=3),
    }
    base.update(overrides)
    return _row(**base)


def _oracle_row(**overrides):
    base = {
        "id": 5,
        "created_at": datetime.now(timezone.utc) - timedelta(hours=2),
        "ticker": "NVDA",
        "prediction_type": "CALL",
        "direction": "up",
        "expiry": datetime.now(timezone.utc) + timedelta(days=14),
        "confidence": 0.66,
        "expected_move_pct": 5.0,
        "signal_strength": 0.7,
        "coherence": 0.6,
        "model_name": "qwen",
        "signals": '[{"label":"flow","detail":"call sweep"}]',
        "anti_signals": [],
        "flow_context": {},
        "verdict": "pending",
    }
    base.update(overrides)
    return _row(**base)


def _hypothesis_row(**overrides):
    base = {
        "id": 7,
        "thesis": "Semis rally when export panic fades; watch NVDA",
        "pattern_type": "policy_reversal",
        "evidence": '["policy headline"]',
        "test_criteria": "NVDA confirms",
        "invalidation": "NVDA loses relative strength",
        "confidence": 0.6,
        "status": "active",
        "times_tested": 4,
        "times_correct": 3,
        "created_at": datetime.now(timezone.utc) - timedelta(hours=6),
        "last_tested": datetime.now(timezone.utc),
        "role": "candidate",
    }
    base.update(overrides)
    return _row(**base)


# ── B-H6: no fabricated "backtest" ────────────────────────────────────────


def test_no_candidate_type_emits_a_backtest_score_part():
    for candidate in (
        _oracle_candidate(_oracle_row()),
        _signal_candidate(_signal_row()),
        _hypothesis_candidate(_hypothesis_row()),
    ):
        assert "backtest" not in candidate["score_parts"], candidate["id"]


def test_static_prior_is_named_and_labelled_static():
    parts = _oracle_candidate(_oracle_row(verdict="hit"))["score_parts"]

    assert parts["prior_weight"] == 70
    assert parts["prior_weight_basis"] == "static"


def test_unsettled_oracle_row_carries_no_prior_at_all():
    for verdict in (None, "pending", ""):

        parts = _oracle_candidate(_oracle_row(verdict=verdict))["score_parts"]
        assert parts["prior_weight"] is None, verdict
        assert parts["prior_weight_basis"] == "unavailable", verdict


def test_signal_candidates_claim_no_prior_history():
    parts = _signal_candidate(_signal_row())["score_parts"]

    assert parts["prior_weight"] is None
    assert parts["prior_weight_basis"] == "unavailable"


def test_untested_hypothesis_reports_no_accuracy_instead_of_zero_percent():
    tested = _hypothesis_candidate(_hypothesis_row())["score_parts"]
    untested = _hypothesis_candidate(_hypothesis_row(times_tested=0, times_correct=0))["score_parts"]

    assert tested["prior_weight"] == 75.0
    assert tested["prior_weight_basis"] == "hypothesis_test_history"
    assert untested["prior_weight"] is None
    assert untested["prior_weight_basis"] == "unavailable"


# ── B-H7: missing confidence is unknown, not 0.30 / 0.35 ──────────────────


def test_signal_without_confidence_reports_null_not_thirty_percent():
    candidate = _signal_candidate(_signal_row(confidence=None))

    assert candidate["confidence"] is None
    assert candidate["score_parts"]["confidence"] is None
    assert candidate["status"] == "needs_research"


def test_zero_confidence_signal_is_treated_as_unknown_not_as_zero():
    candidate = _signal_candidate(_signal_row(confidence=0))

    assert candidate["confidence"] is None
    assert candidate["score_parts"]["risk_penalty"] >= 13


def test_hypothesis_without_confidence_reports_null_not_thirty_five_percent():
    candidate = _hypothesis_candidate(_hypothesis_row(confidence=None))

    assert candidate["confidence"] is None
    assert candidate["score_parts"]["confidence"] is None


def test_oracle_without_confidence_says_confidence_unknown_in_the_summary():
    candidate = _oracle_candidate(_oracle_row(confidence=None))

    assert candidate["confidence"] is None
    assert "confidence unknown" in candidate["summary"]
    assert "0% confidence" not in candidate["summary"]


def test_conviction_gate_says_confidence_unknown_instead_of_zero_percent():
    conviction = _build_conviction_gate({
        "tickers": ["NVDA"],
        "confidence": None,
        "evidence": [{"source": "signal", "label": "form4", "detail": "three buys"}],
        "contradictions": [],
        "source_modules": ["signal_data"],
        "calibration": {"signal_contributions": {}, "regime": None},
    })
    evidence_gate = next(gate for gate in conviction["gates"] if gate["name"] == "evidence")

    assert "confidence unknown" in evidence_gate["detail"]
    assert "confidence 0%" not in evidence_gate["detail"]


def test_unknown_confidence_never_outranks_a_measured_one():
    """A downstream sort must not treat null as a middling 0.3/0.5."""
    known = _signal_candidate(_signal_row(id=1, confidence=0.8))
    unknown = _signal_candidate(_signal_row(id=2, confidence=None))

    assert known["alpha_score"] > unknown["alpha_score"]


def test_partial_confidence_coverage_keeps_each_candidate_honest():
    """Some rows have confidence, some do not; neither state contaminates the other."""
    candidates = [
        _signal_candidate(_signal_row(id=1, confidence=0.9)),
        _signal_candidate(_signal_row(id=2, confidence=None)),
        _signal_candidate(_signal_row(id=3, confidence=0.4)),
    ]

    assert [item["confidence"] for item in candidates] == [0.9, None, 0.4]


# ── B-M15: unresolved regime is null, and skips regime-conditional history ─


def test_unresolved_regime_is_null_not_the_string_neutral():
    assert _extract_calibration_context(None, "qwen")["regime"] is None
    assert _extract_calibration_context('{"signal_contributions": {}}', None)["regime"] is None
    assert _signal_candidate(_signal_row())["calibration"]["regime"] is None
    assert _hypothesis_candidate(_hypothesis_row())["calibration"]["regime"] is None


def test_resolved_regime_is_still_carried_through():
    calibration = _extract_calibration_context(
        '{"regime": "TIGHTENING", "signal_contributions": {"flow": 0.5}}', "qwen"
    )

    assert calibration["regime"] == "TIGHTENING"


def test_null_regime_skips_regime_conditional_brier_history():
    """calibration.regime is None implies no regime-conditional history is read."""
    class _Result:
        def __init__(self, value=None, row=None):
            self._value = value
            self._row = row

        def scalar(self):
            return self._value

        def fetchone(self):
            return self._row

    class _Conn:
        def __init__(self):
            self.statements: list[str] = []

        def execute(self, statement, params=None):
            sql = str(statement)
            self.statements.append(sql)
            if "to_regclass" in sql:
                return _Result(value=True)
            if "regime_conditional_brier_history" in sql:
                raise AssertionError("regime-conditional history read with an unknown regime")
            return _Result(row=_row(
                horizon_days=7,
                scored_count=25,
                running_brier=0.2,
                running_ece=0.05,
                hit_count=15,
                last_updated=datetime.now(timezone.utc),
            ))

    conn = _Conn()
    cards = _fetch_signal_scorecards(conn, {"options_flow": 1.0}, 7, None)

    assert cards, "flat per-signal history should still be used"
    assert all(card["regime"] is None for card in cards)


# ── B-M16: evidence with no weight serializes null ────────────────────────


def test_unweighted_evidence_items_serialize_null_weight():
    timestamp = datetime.now(timezone.utc)
    from_list = _evidence_from_payload('[{"label":"a","detail":"b"}]', "signal", timestamp)
    from_scalars = _evidence_from_payload('["plain string"]', "signal", timestamp)
    from_dict = _evidence_from_payload('{"key": "value"}', "signal", timestamp)
    from_text = _evidence_from_payload("free text", "signal", timestamp)

    for evidence in (from_list, from_scalars, from_dict, from_text):
        assert evidence
        assert all(item["weight"] is None for item in evidence), evidence


def test_evidence_weight_is_kept_when_the_payload_supplies_one():
    evidence = _evidence_from_payload(
        '[{"label":"a","detail":"b","weight":0.8},{"label":"c","detail":"d","confidence":0.2}]',
        "signal",
        datetime.now(timezone.utc),
    )

    assert [item["weight"] for item in evidence] == [0.8, 0.2]


def test_evidence_aggregation_counts_items_and_never_sums_null_weights():
    """The only evidence aggregate in the router is a count, not a weight sum."""
    conviction = _build_conviction_gate({
        "tickers": ["NVDA"],
        "confidence": 0.5,
        "evidence": [
            {"source": "signal", "label": "a", "detail": "b", "weight": None},
            {"source": "signal", "label": "c", "detail": "d", "weight": 0.8},
        ],
        "contradictions": [],
        "source_modules": ["signal_data"],
        "calibration": {"signal_contributions": {}, "regime": None},
    })
    evidence_gate = next(gate for gate in conviction["gates"] if gate["name"] == "evidence")

    assert "2 evidence items" in evidence_gate["detail"]
    assert isinstance(evidence_gate["score"], float)


# ── Zero candidates with a healthy DB stays an honest read ────────────────


def test_zero_candidates_is_still_a_real_read():
    brief = _build_operator_brief([], {"missing_data_requests": 0}, None)

    assert brief["posture"] == "stand_down"
    assert brief["stance"] == "Stand down"
