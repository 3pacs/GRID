"""Synthetic Oracle score/coherence, serialized lineage, and report contracts."""
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
import json
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from intelligence import actor_signal_bridge as bridge
from oracle import engine as oe
from oracle.hallucination_guard import _check_signal_freshness
from oracle.report import _prediction_section

NOW = datetime(2026, 1, 20, 12, tzinfo=timezone.utc)


def candidate(i=1, **updates):
    row = {"id": i, "actor": "Example Actor", "signal_type": "congressional",
           "source_id": f"synthetic:{i}", "direction": "BULL",
           "signal_date": date(2026, 1, 10), "created_at": NOW - timedelta(days=9),
           "actor_trust": .5, "actor_influence": .9}
    row.update(updates)
    return row


def test_injection_preserves_canonical_sign_and_lineage():
    evidence = bridge.select_actor_evidence([candidate(), candidate(2, actor="Other Actor", direction="BEAR")], as_of=NOW)
    signals = []
    audit = oe._inject_actor_evidence(signals, evidence)
    assert [(s.direction, s.z_score) for s in signals] == [("bullish", 1.5), ("bearish", -1.5)]
    assert all(s.freshness_hours is None for s in signals)
    payload = json.loads(json.dumps({"signals": [asdict(s) for s in signals], "audit": audit}, allow_nan=False))
    assert payload["signals"][0]["provenance"]["row_id"] == 1
    assert payload["signals"][0]["provenance"]["source_id"] == "synthetic:1"
    assert audit["direct_vote_row_ids"] == [1, 2]


def test_repeated_rows_do_not_multiply_score_or_coherence():
    rows = [candidate(i, signal_date=date(2026, 1, 10 + i)) for i in range(5)]
    base = oe.Signal("counter", "equity", 1, -1, "bearish", .5, 1)
    signals = [base]
    oe._inject_actor_evidence(signals, bridge.select_actor_evidence(rows, as_of=NOW))
    assert len(signals) == 2
    assert sum(s.z_score * s.weight for s in signals) == pytest.approx(.175)
    assert oe.OracleEngine.__new__(oe.OracleEngine)._compute_coherence(signals, "CALL") == .5


def test_rejected_evidence_cannot_boost_existing_weights():
    signals = [oe.Signal("context", "congressional", 1, 1, "bullish", 1, 1)]
    oe._inject_actor_evidence(signals, bridge.select_actor_evidence([
        candidate(direction=None, actor_trust=.95)], as_of=NOW))
    assert len(signals) == 1 and signals[0].weight == 1


def test_true_stale_lower_bound_checked_without_fabricated_exact_age():
    signals = []
    oe._inject_actor_evidence(signals, bridge.select_actor_evidence([candidate()], as_of=NOW))
    check = _check_signal_freshness(SimpleNamespace(signals=signals))
    assert not check.passed and check.adjustment == .7
    signals[0].provenance = {}
    check = _check_signal_freshness(SimpleNamespace(signals=signals))
    assert check.severity == "warning" and "1 freshness unknown" in check.message
    assert check.adjustment == 1  # No new tuning of the unknown-age penalty.


def test_report_labels_observation_date_and_availability():
    signals = []
    audit = oe._inject_actor_evidence(signals, bridge.select_actor_evidence([candidate()], as_of=NOW))
    section = _prediction_section({"ticker": "TEST", "signals": [asdict(s) for s in signals],
                                   "flow_context": {"actor_evidence": audit}}, NOW.date())
    html = json.dumps(section)
    assert "exact source age unknown" in html
    assert "2026-01-10 (DATE only)" in html
    assert "calendar date age: 10d" in html
    assert "observation-date age lower bound (UTC calendar): 228.0h" in html
    assert "ingested: 2026-01-11" in html
    assert "source age at generation: 0.0h" not in html
    assert "not independent confirmations" in html


@pytest.mark.parametrize("base_count", [3, 11])
def test_real_generation_uses_decision_clock_and_persists_audit(monkeypatch, base_count):
    oracle = oe.OracleEngine.__new__(oe.OracleEngine)
    oracle.engine = object()
    oracle.models = [oe.OracleModel(name, "test", "test", ["equity"])
                     for name in ("synthetic", "second_synthetic")]
    monkeypatch.setattr(oe, "_USE_SIGNAL_REGISTRY", False)
    monkeypatch.setattr(oracle, "_get_flow_context", lambda ticker: {})
    monkeypatch.setattr(oracle, "_get_spot_price", lambda ticker: 100)
    monkeypatch.setattr(oracle, "_get_credit_cycle_routing", lambda: {})
    monkeypatch.setattr(oracle, "_get_journal_feedback", lambda ticker: {})
    monkeypatch.setattr(oracle, "_get_convergence_for_ticker", lambda ticker: [])
    monkeypatch.setattr(oracle, "_find_anti_signals", lambda signals, direction: [])
    monkeypatch.setitem(sys.modules, "config", SimpleNamespace(settings=SimpleNamespace(SIGNAL_OVERRIDES_ENABLED=False)))
    monkeypatch.setitem(sys.modules, "intelligence.confidence_calibration", SimpleNamespace(calibrate_confidence=lambda value, *args: value))
    # Three neutral context observations ensure the standard model data gate
    # does not itself suppress the synthetic actor-only scoring comparison.
    monkeypatch.setattr(oracle, "_gather_signals", lambda *args: [
        oe.Signal(f"context{i}", "equity", 0, 0, "neutral", 1, 1) for i in range(base_count)])
    calls = []
    def read(engine, ticker, days, *, as_of):
        calls.append(as_of)
        return bridge.select_actor_evidence([
            candidate(i, signal_date=date(2026, 1, 10 + i)) for i in range(5)], as_of=as_of)
    monkeypatch.setattr(bridge, "get_actor_evidence_for_ticker", read)
    predictions = oracle._oracle_one_ticker(1, "TEST", 1, NOW, {}, horizon_days=7)
    assert calls == [NOW]
    assert len(predictions) == 2
    assert predictions[0].flow_context["actor_evidence"] is not predictions[1].flow_context["actor_evidence"]
    pred = predictions[0]
    assert pred.direction == "CALL"
    assert pred.signal_strength == .675
    assert pred.expected_move_pct == .34  # One .5*.9*1.5 vote, not five.
    assert pred.coherence == 1.0
    payload = pred.to_dict()
    json.dumps(payload, allow_nan=False)
    assert len(payload["flow_context"]["actor_evidence"]["observations"]) == 5
    if base_count == 3:
        assert payload["signals"][-1]["provenance"]["row_id"] == 4
    else:
        assert all(not sig["provenance"] for sig in payload["signals"])
        assert payload["flow_context"]["actor_evidence"]["direct_vote_row_ids"] == [4]
        html = json.dumps(_prediction_section(payload, NOW.date()))
        assert "exact event age is unknown" in html


def test_credibility_weight_changes_record_used_lineage_per_model():
    batch = bridge.select_actor_evidence([candidate(actor_trust=.9)], as_of=NOW)
    first = [oe.Signal("base", "congressional", 1, 1, "bullish", 2, 1)]
    second = [oe.Signal("base", "congressional", 1, 1, "bullish", 2, 1)]
    audit_a = oe._inject_actor_evidence(first, batch)
    audit_b = oe._inject_actor_evidence(second, batch)
    assert audit_a["weight_boosts"] == audit_b["weight_boosts"]
    assert audit_a["weight_boosts"][0]["actor_row_ids"] == [1]
    assert audit_a["weight_boosts"][0]["prior_weight"] == 2
    assert audit_a["weight_boosts"][0]["result_weight"] == pytest.approx(2.62)
    assert "weight_boosts" not in batch


def test_zero_trust_cannot_add_a_directional_coherence_vote():
    signals = []
    batch = bridge.select_actor_evidence([candidate(actor_trust=0.0)], as_of=NOW)
    receipt = oe._inject_actor_evidence(signals, batch)
    assert signals == []
    assert receipt["direct_vote_row_ids"] == []
    assert receipt["direct_vote_disposition_counts"]["zero_weight"] == 1
    assert receipt["observations"][0]["actor_trust"] == 0


@pytest.mark.parametrize("count", [1000, 1001])
def test_candidate_window_receipts_stay_small_and_disclose_sampling(count):
    rows = [candidate(i, direction=None) for i in range(count)]
    batch = bridge.select_actor_evidence(rows, as_of=NOW)
    signals = []
    receipt = oe._inject_actor_evidence(signals, batch)
    assert not signals
    assert receipt["observation_count"] == count
    assert not receipt["observations_complete"]
    assert len(receipt["observations"]) == bridge.MAX_ACTOR_AUDIT_SAMPLES
    assert len(json.dumps(receipt).encode()) < oe.MAX_ACTOR_RECEIPT_BYTES
    if count == 1001:
        assert receipt["reason"] == "candidate_limit_exceeded"
        assert receipt["candidate_count_lower_bound"] == 1001


def test_size_failure_is_atomic_for_boosts_and_direct_votes():
    # The sample and all used identities are preserved or no actor effect occurs.
    batch = bridge.select_actor_evidence([candidate(source_id="x" * 20_000, actor_trust=.9)], as_of=NOW)
    signals = [oe.Signal("base", "congressional", 1, 1, "bullish", 2, 1)]
    before = [asdict(s) for s in signals]
    receipt = oe._inject_actor_evidence(signals, batch)
    assert [asdict(s) for s in signals] == before
    assert receipt["reason"] == "actor_receipt_size_limit_exceeded"
    assert not receipt["complete"] and not receipt["used_groups"]
    assert receipt["direct_vote_row_ids"] == receipt["weight_boosts"] == []
    assert len(json.dumps(receipt).encode()) < oe.MAX_ACTOR_RECEIPT_BYTES


def test_used_support_receipt_survives_sampling_and_covers_duplicate_payload_size():
    rows = [candidate(i, actor=f"Actor {i // 3}") for i in range(15)]
    signals = []
    receipt = oe._inject_actor_evidence(signals, bridge.select_actor_evidence(rows, as_of=NOW))
    assert not receipt["observations_complete"]
    refs = [r["row_id"] for group in receipt["used_groups"] for r in group["supporting_rows"]]
    assert set(refs) == set(range(15))
    assert all(s.provenance["used_group_row_id"] in receipt["direct_vote_row_ids"] for s in signals)
    payload = {"actor_evidence": receipt, "actor_signals": [asdict(s) for s in signals]}
    assert len(json.dumps(payload, ensure_ascii=True).encode()) <= oe.MAX_ACTOR_RECEIPT_BYTES


def test_report_discloses_sample_count_and_validates_count_html():
    batch = bridge.select_actor_evidence([candidate(i, direction=None) for i in range(50)], as_of=NOW)
    receipt = oe._inject_actor_evidence([], batch)
    payload = {"ticker": "TEST", "flow_context": {"actor_evidence": receipt}}
    html = json.dumps(_prediction_section(payload, NOW.date()))
    assert "Stored candidate detail: 12/50 (sampled/truncated)" in html
    receipt["observation_count"] = "<script>alert(1)</script>"
    html = json.dumps(_prediction_section(payload, NOW.date()))
    assert "<script>" not in html and "12/unknown" in html


def test_storage_keeps_used_receipt_atomic_on_insert_and_update_after_display_cap(monkeypatch):
    from oracle import prediction_context as context
    for name in ("fetch_liquidity_regime", "fetch_fci_regime", "fetch_vix_level"):
        monkeypatch.setattr(context, name, lambda *args: None)
    engine = MagicMock()
    conn = engine.begin.return_value.__enter__.return_value
    stored = {}
    statements = []
    def execute(statement, params=None):
        sql = str(statement)
        assert "INSERT INTO oracle_predictions" in sql
        statements.append(sql)
        inserted = not stored
        if inserted:
            stored["fc"] = params["fc"]
        # Model the exact existing SQL update: signals replaced, flow untouched.
        stored["sig"] = params["sig"]
        result = MagicMock()
        result.fetchone.return_value = (inserted,)
        return result
    conn.execute.side_effect = execute
    oracle = oe.OracleEngine.__new__(oe.OracleEngine)
    oracle.engine = engine
    for row_id in (1, 2):
        signals = [oe.Signal(f"base{i}", "equity", 0, 0, "neutral", 1, 1) for i in range(11)]
        audit = oe._inject_actor_evidence(signals, bridge.select_actor_evidence([candidate(row_id)], as_of=NOW))
        pred = oe.OraclePrediction(
            id=f"synthetic{row_id}", timestamp=NOW, ticker="TEST",
            prediction_type=oe.PredictionType.DIRECTION, direction="CALL",
            target_price=101, current_price=100, expiry=date(2026, 1, 27),
            confidence=.5, expected_move_pct=1, signals=signals[:10],
            flow_context={"actor_evidence": audit}, model_name="synthetic", model_version="test")
        oracle._store_predictions([pred])
        payload = json.loads(stored["sig"])
        assert len(payload["items"]) == 10
        assert all(s["family"] != "actor_intelligence" for s in payload["items"])
        receipt = payload["actor_evidence"]
        assert receipt["direct_vote_row_ids"] == [row_id]
        assert receipt["used_groups"][0]["supporting_rows"][0]["row_id"] == row_id
        assert "actor_evidence" not in json.loads(stored["fc"])
        html = json.dumps(_prediction_section({"ticker": "TEST", "signals": payload}, NOW.date()))
        assert "1 direct votes" in html and "exact event age is unknown" in html
    assert all("signals    = EXCLUDED.signals" in sql for sql in statements)
