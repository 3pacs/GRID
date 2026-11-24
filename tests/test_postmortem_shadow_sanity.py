"""Shadow sanity verdicts are recorded but never post-mortem evidence (Codex review 6030902445).

Since #1400, CROSS_ASSET is a shadow check (``trading.options_recommender.
SHADOW_CHECKS``): its verdict is persisted in ``options_recommendations.
sanity_status`` with ``"shadow": true`` but never gates. ``intelligence.
postmortem._classify_failure`` used to turn every PASS/FAIL into an attributed
signal and count every FAIL toward ``model_error``, so a shadow FAIL would
have changed a post-mortem's attribution and category although it changed
no recommendation.

The pairs below hold the real (gating) verdicts constant and flip only the
shadow verdict between FAIL, PASS, SKIP and absent: category, root cause and
both signal lists must not move. Rows written before #1400 carry no flag;
their CROSS_ASSET verdict (always PASS on griddb, from the replaced momentum
read) is excluded by name.

Composed with #1355: ``entry_price`` is the option premium and the classifier
fails closed with ``insufficient_data`` when no underlying entry anchor is
known, before any sanity-based category. The fixtures give it the underlying
anchor (a completed close) so the pairs reach the sanity branches, and the
fail-closed result is held to the same rule.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import pytest

import intelligence.postmortem as pm_mod
from intelligence.postmortem import _active_sanity, _classify_failure

SHADOW_FLIPS = [
    {"CROSS_ASSET": {"status": "FAIL", "reason": "Momentum (-9.00%) and regime both contradict CALL", "shadow": True}},
    {"CROSS_ASSET": {"status": "PASS", "reason": "Momentum (+1.00%) and regime aligned with CALL", "shadow": True}},
    {"CROSS_ASSET": {"status": "SKIP", "reason": "No momentum: stale", "shadow": True}},
    {},
]
FLIP_IDS = ["shadow-FAIL", "shadow-PASS", "shadow-SKIP", "absent"]


# Underlying SPY spot at entry (a completed close); entry_price=1.0 is the premium.
UNDERLYING_ENTRY = 700.0


def _classify(sanity: dict, *, underlying_entry_price: float | None = UNDERLYING_ENTRY) -> tuple:
    return _classify_failure(
        ticker="SPY", direction="CALL", entry_price=1.0, strike=700.0, target=2.0,
        expiry="2026-12-18", outcome="LOSS", actual_return=-0.6, price_path=[],
        data_at_decision={}, sanity=sanity, underlying_entry_price=underlying_entry_price,
    )


ONE_REAL_FAIL = {
    "DATA_QUALITY": {"status": "PASS", "reason": "fresh"},
    "DEALER_FLOW": {"status": "FAIL", "reason": "long gamma, low confidence"},
    "LLM_REVIEW": {"status": "SKIP", "reason": "offline"},
}
TWO_REAL_FAILS = {**ONE_REAL_FAIL, "HISTORICAL_ANALOG": {"status": "FAIL", "reason": "analogs lost"}}


def test_shadow_flip_does_not_change_attribution_with_one_real_fail():
    # One real FAIL: wrong_signal. A shadow FAIL counted would make it two,
    # i.e. model_error.
    results = [_classify({**ONE_REAL_FAIL, **flip}) for flip in SHADOW_FLIPS]
    assert all(r == results[0] for r in results), dict(zip(FLIP_IDS, results, strict=True))
    category, _root, wrong, right, _missed = results[0]
    assert category == "wrong_signal"
    assert wrong == ["sanity_DEALER_FLOW_failed"]
    assert right == ["sanity_DATA_QUALITY_passed"]


def test_shadow_flip_does_not_change_model_error_with_two_real_fails():
    results = [_classify({**TWO_REAL_FAILS, **flip}) for flip in SHADOW_FLIPS]
    assert all(r == results[0] for r in results), dict(zip(FLIP_IDS, results, strict=True))
    category, root, wrong, right, _missed = results[0]
    assert category == "model_error"
    assert root.startswith("2 sanity layers failed")
    assert wrong == ["sanity_DEALER_FLOW_failed", "sanity_HISTORICAL_ANALOG_failed"]
    assert right == ["sanity_DATA_QUALITY_passed"]


def test_legacy_row_without_flag_excludes_cross_asset_by_name():
    # Pre-#1400 rows: CROSS_ASSET PASS, no shadow flag (all 778 on griddb).
    legacy = {**ONE_REAL_FAIL, "CROSS_ASSET": {"status": "PASS", "reason": "Momentum (+0.00%) and regime aligned with CALL"}}
    assert _classify(legacy) == _classify(ONE_REAL_FAIL)
    assert "sanity_CROSS_ASSET_passed" not in _classify(legacy)[3]


@pytest.mark.parametrize("real", [ONE_REAL_FAIL, TWO_REAL_FAILS], ids=["one-real-FAIL", "two-real-FAILs"])
def test_shadow_flip_does_not_change_the_fail_closed_result_without_an_anchor(real):
    # #1355: no underlying entry anchor -> insufficient_data. The shadow verdict
    # changes neither that result nor the signal lists it carries.
    results = [_classify({**real, **flip}, underlying_entry_price=None) for flip in SHADOW_FLIPS]
    assert all(r == results[0] for r in results), dict(zip(FLIP_IDS, results, strict=True))
    category, _root, wrong, right, _missed = results[0]
    assert category == "insufficient_data"
    assert not any("CROSS_ASSET" in name for name in wrong + right)


def test_active_sanity_excludes_by_flag_and_by_name_and_never_mutates():
    raw = {
        "DATA_QUALITY": {"status": "PASS", "reason": "ok"},
        "CROSS_ASSET": {"status": "FAIL", "reason": "x", "shadow": True},
        "NEW_LAYER": {"status": "FAIL", "reason": "y", "shadow": True},  # a future shadow layer, by flag
        "DEALER_FLOW": {"status": "FAIL", "reason": "z", "shadow": False},
    }
    before = {k: dict(v) for k, v in raw.items()}
    assert set(_active_sanity(raw)) == {"DATA_QUALITY", "DEALER_FLOW"}
    assert raw == before
    assert _active_sanity(None) == {}


class _Row:
    def __init__(self, row: tuple):
        self._row = row

    def fetchone(self):
        return self._row


class _FakeEngine:
    def __init__(self, row: tuple):
        self._row = row

    @contextmanager
    def connect(self):
        row = self._row

        class _Conn:
            def execute(self, *_a, **_k):
                return _Row(row)

        yield _Conn()


def _generate(monkeypatch: pytest.MonkeyPatch, sanity: dict) -> tuple[Any, dict]:
    """generate_postmortem on one stored LOSS, with the DB and LLM replaced by fakes."""
    import json

    row = (7, "SPY", "CALL", 700.0, "2026-12-18", 1.0, 2.0, 0.5, 0.6, "thesis",
           json.dumps(sanity), "2026-11-20T16:00:00Z", "LOSS", -0.6, None, "", None, None)
    seen: dict = {}
    stored: dict = {}
    monkeypatch.setattr(pm_mod, "_ensure_tables", lambda _e: None)
    monkeypatch.setattr(pm_mod, "_reconstruct_decision_data",
                        lambda _e, _t, _g, s, d: {"sanity_status": s, "dealer_context": d})
    # #1355 reads the path with its source and anchors on a completed close.
    monkeypatch.setattr(pm_mod, "_load_price_path_with_source", lambda *_a, **_k: ([], None))
    monkeypatch.setattr(pm_mod, "_load_underlying_entry_spot",
                        lambda *_a, **_k: (UNDERLYING_ENTRY, "fixture: completed close known at decision"))
    monkeypatch.setattr(pm_mod, "_get_llm_postmortem", lambda **kw: seen.update(kw) or {})
    monkeypatch.setattr(pm_mod, "_store_postmortem", lambda _e, pm, **_k: stored.update(pm=pm))
    monkeypatch.setattr(pm_mod, "_emit_trade_postmortem", lambda **_k: None)
    pm = pm_mod.generate_postmortem(_FakeEngine(row), 7)
    assert stored["pm"] is pm
    return pm, seen


@pytest.mark.parametrize("real", [ONE_REAL_FAIL, TWO_REAL_FAILS], ids=["one-real-FAIL", "two-real-FAILs"])
def test_generate_postmortem_keeps_raw_shadow_but_not_as_evidence(monkeypatch: pytest.MonkeyPatch, real):
    shadow_fail = {**real, **SHADOW_FLIPS[0]}
    shadow_pass = {**real, **SHADOW_FLIPS[1]}
    pm_fail, llm_fail = _generate(monkeypatch, shadow_fail)
    pm_pass, llm_pass = _generate(monkeypatch, shadow_pass)
    # Attribution and classification do not move with the shadow verdict.
    for field in ("failure_category", "root_cause", "which_signals_were_wrong", "which_signals_were_right"):
        assert getattr(pm_fail, field) == getattr(pm_pass, field), field
    assert "sanity_CROSS_ASSET_failed" not in pm_fail.which_signals_were_wrong
    # The raw diagnostic record keeps the shadow verdict exactly as stored.
    assert pm_fail.sanity_results_at_decision == shadow_fail
    assert pm_fail.data_at_decision["sanity_status"] == shadow_fail
    # The narrative LLM sees only active layers, the same for both.
    assert "CROSS_ASSET" not in llm_fail["data_at_decision"]["sanity_status"]
    assert llm_fail["data_at_decision"] == llm_pass["data_at_decision"]
    assert llm_fail["signals_wrong"] == llm_pass["signals_wrong"]
