"""Honesty guards for the stepdad.finance dad path gold / decision stack (B-M18, B-L7).

`gold.score` and `decision_stack.score` were 0-100 numbers built entirely from
hand-picked point values and rendered as conviction gauges in `TickerLookup.jsx`.
They are now `heuristic_score` with a sibling `weights` object that lists every
term and its weight, and a missing workbook footprint is `null`, never 0.
"""

from __future__ import annotations

from api.routers.dad import (
    _DECISION_STACK_WEIGHTS,
    _GOLD_SCORE_WEIGHTS,
    _gold_from_summary,
    _grid_decision_stack,
)

SUMMARY = {
    "mentions": 10,
    "file_count": 3,
    "sheet_count": 4,
    "evidence_score": 8.0,
    "source_types": "cell",
}

GRID = {
    "metrics": {"return_1y_pct": 32.0, "pct_from_52w_high": -4.0},
    "source_freshness": [{"source": "yfinance", "state": "fresh"}],
}

FINVIZ = {
    "status": "ready",
    "field_count": 5,
    "freshness": {"state": "fresh", "label": "fresh"},
    "fields": {
        "forward_pe": {"parsed": 22.0},
        "roe": {"parsed": 19.0},
        "debt_equity": {"parsed": 0.4},
        "profit_margin": {"parsed": 16.0},
    },
}

SIGNALS = {"signal_sources": [{"trust_score": 0.7}], "tradingview_signals": [], "regime": None}


def _decision(summary=SUMMARY, grid=GRID, finviz=FINVIZ, options=None, signals=SIGNALS):
    gold = _gold_from_summary(summary)
    return gold, _grid_decision_stack(summary, gold, grid, finviz, options, signals)


# ── gold ──────────────────────────────────────────────────────────────────────


def test_gold_no_longer_publishes_a_bare_score_key() -> None:
    gold = _gold_from_summary(SUMMARY)
    assert "score" not in gold
    assert isinstance(gold["heuristic_score"], int)
    assert gold["score_basis"] == "workbook_footprint_weighted_count"


def test_gold_weights_list_every_term_with_its_weight() -> None:
    gold = _gold_from_summary(SUMMARY)
    weights = gold["weights"]
    for term, spec in _GOLD_SCORE_WEIGHTS.items():
        assert weights[term]["weight"] == spec["weight"]
        assert weights[term]["cap"] == spec["cap"]
        assert "input" in weights[term]
        assert "points" in weights[term]
    assert weights["_clamp"] == {"min": 0, "max": 100}


def test_gold_weights_reproduce_the_published_score() -> None:
    gold = _gold_from_summary(SUMMARY)
    weights = gold["weights"]
    total = sum(weights[term]["points"] for term in _GOLD_SCORE_WEIGHTS)
    assert round(total) == gold["heuristic_score"]


def test_gold_with_no_workbook_history_is_null_not_zero() -> None:
    gold = _gold_from_summary(None)
    assert gold["heuristic_score"] is None
    assert gold["weights"] is None
    assert gold["score_basis"] == "no_workbook_history"
    assert "score" not in gold


def test_gold_mentions_term_is_capped_and_says_so() -> None:
    gold = _gold_from_summary({**SUMMARY, "mentions": 500})
    assert gold["weights"]["mentions"]["cap"] == 30
    assert gold["weights"]["mentions"]["points"] == 30
    assert gold["weights"]["mentions"]["input"] == 500


# ── decision stack ────────────────────────────────────────────────────────────


def test_decision_stack_no_longer_publishes_a_bare_score_key() -> None:
    _, decision = _decision()
    assert "score" not in decision
    assert isinstance(decision["heuristic_score"], float | int)
    assert decision["score_basis"] == "hand_picked_point_awards"
    assert decision["stance_basis"] == "heuristic_score_thresholds"


def test_decision_stack_weights_list_every_term_and_weight() -> None:
    _, decision = _decision()
    weights = decision["weights"]
    assert weights == _DECISION_STACK_WEIGHTS
    # Every hand-picked constant used by the arithmetic is named and published.
    for key in (
        "workbook_prior_multiplier",
        "grid_return_window_strong_ge_20pct",
        "finviz_stale",
        "options_row_present",
        "signals_trusted_cap",
        "regime_cautious",
        "stale_source_cap",
    ):
        assert key in weights
    assert weights["_clamp"] == {"min": 0, "max": 100}
    assert weights["_stance_thresholds"]["Deep review first"] == 70


def test_decision_stack_keeps_the_real_evidence_gate_cards() -> None:
    _, decision = _decision()
    sources = {card["source"] for card in decision["cards"]}
    assert {"Dad workbooks", "GRID price history", "Finviz fundamentals", "GRID options", "GRID signals"} <= sources


def test_decision_stack_with_no_workbook_prior_does_not_invent_one() -> None:
    gold = _gold_from_summary(None)
    decision = _grid_decision_stack(None, gold, GRID, FINVIZ, None, SIGNALS)
    workbook_card = next(card for card in decision["cards"] if card["source"] == "Dad workbooks")
    assert workbook_card["state"] == "missing"
    assert workbook_card["points"] == 0.0
    assert decision["weights"]["workbook_prior_multiplier"] == 0.35


def test_decision_stack_with_no_grid_or_finviz_data_reports_blockers() -> None:
    _, decision = _decision(
        grid={"metrics": {}, "source_freshness": []},
        finviz={"status": "unavailable", "field_count": 0, "freshness": {"state": "missing", "label": "missing"}, "fields": {}},
        options=None,
        signals={"signal_sources": [], "tradingview_signals": [], "regime": None},
    )
    blob = " ".join(decision["blockers"])
    assert "no resolved price history" in blob
    assert "Finviz fundamentals are not in GRID" in blob
    # Partial data must not silently become a mid-range reading.
    assert decision["heuristic_score"] <= 40


def test_arithmetic_uses_only_published_weights() -> None:
    """A weight change must move the score — the table is the single source of truth."""
    _, baseline = _decision(options={"date": "2026-06-17", "put_call_ratio": 0.8})
    original = _DECISION_STACK_WEIGHTS["options_row_present"]
    try:
        _DECISION_STACK_WEIGHTS["options_row_present"] = 0
        _, changed = _decision(options={"date": "2026-06-17", "put_call_ratio": 0.8})
    finally:
        _DECISION_STACK_WEIGHTS["options_row_present"] = original
    assert changed["heuristic_score"] == baseline["heuristic_score"] - original
