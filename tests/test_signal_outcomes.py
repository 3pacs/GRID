"""Tests for evaluation/signal_outcomes.py (workstream W3b).

Pure Python — no database, no network. Price data is supplied via a small
fake PIT-style accessor built from an in-memory {date: price} calendar per
instrument, so these tests say nothing about store/pit.py itself (that has
its own test_pit.py).
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from pathlib import Path

import pytest

from evaluation.signal_outcomes import (
    EVALUATION_VERSION,
    REASON_MISSING_ENTRY,
    REASON_MISSING_EXIT,
    REASON_PRICE_OUT_OF_BOUNDS,
    REASON_UNKNOWN_DIRECTION,
    REASON_UNSUPPORTED_INSTRUMENT,
    PricePoint,
    SignalRecord,
    UnsupportedInstrumentError,
    dedupe_records,
    evaluate_signal,
    filter_by_origin_tag,
    summarize_outcomes,
)


# ---------------------------------------------------------------------------
# Fake price accessor
# ---------------------------------------------------------------------------


def make_fake_accessor(calendars: dict[str, dict[date, float]], max_lookback_days: int = 3):
    """Build a PIT-style accessor: last bar with bar_date <= as_of, or None.

    ``max_lookback_days`` mimics a real market-data accessor's tolerance for
    small gaps (weekends/holidays) — it will asof-match a bar up to that many
    days stale, but will NOT reach arbitrarily far into the past. That lets
    these fixtures represent "no exit data yet" (nothing within the window)
    distinctly from "the market was simply flat" (a same-day/near-day bar).
    """

    def _accessor(instrument: str, as_of: date):
        cal = calendars.get(instrument)
        if not cal:
            if instrument == "DELISTED":
                raise UnsupportedInstrumentError(instrument)
            return None
        eligible_dates = [
            d for d in cal if d <= as_of and (as_of - d).days <= max_lookback_days
        ]
        if not eligible_dates:
            return None
        bar_date = max(eligible_dates)
        return PricePoint(price=cal[bar_date], bar_date=bar_date, basis="close")

    return _accessor


D0 = date(2026, 1, 5)


def _rec(**kwargs):
    defaults = dict(
        source_type="congressional",
        instrument="ACME",
        signal_date=D0,
        direction="BUY",
        horizon_days=5,
        signal_source_id=1,
        origin_tag="live",
    )
    defaults.update(kwargs)
    return SignalRecord(**defaults)


# ---------------------------------------------------------------------------
# CORRECT / WRONG / NO_MOVE by direction and band
# ---------------------------------------------------------------------------


def test_buy_correct_above_band():
    exit_date = D0 + timedelta(days=5)
    acc = make_fake_accessor({"ACME": {D0: 100.0, exit_date: 103.0}})
    rec = _rec(direction="BUY")
    out = evaluate_signal(rec, acc, dead_band_pct=1.0, today=exit_date)
    assert out.outcome == "CORRECT"
    assert out.raw_return == pytest.approx(3.0)
    assert out.evaluation_version == EVALUATION_VERSION


def test_buy_wrong_below_negative_band():
    exit_date = D0 + timedelta(days=5)
    acc = make_fake_accessor({"ACME": {D0: 100.0, exit_date: 97.0}})
    rec = _rec(direction="BUY")
    out = evaluate_signal(rec, acc, dead_band_pct=1.0, today=exit_date)
    assert out.outcome == "WRONG"


def test_sell_correct_on_decline():
    exit_date = D0 + timedelta(days=5)
    acc = make_fake_accessor({"ACME": {D0: 100.0, exit_date: 96.0}})
    rec = _rec(direction="SELL")
    out = evaluate_signal(rec, acc, dead_band_pct=1.0, today=exit_date)
    assert out.outcome == "CORRECT"


def test_sell_wrong_on_rally():
    exit_date = D0 + timedelta(days=5)
    acc = make_fake_accessor({"ACME": {D0: 100.0, exit_date: 104.0}})
    rec = _rec(direction="SELL")
    out = evaluate_signal(rec, acc, dead_band_pct=1.0, today=exit_date)
    assert out.outcome == "WRONG"


def test_no_move_within_band_is_not_wrong():
    exit_date = D0 + timedelta(days=5)
    acc = make_fake_accessor({"ACME": {D0: 100.0, exit_date: 100.3}})
    rec = _rec(direction="BUY")
    out = evaluate_signal(rec, acc, dead_band_pct=1.0, today=exit_date)
    assert out.outcome == "NO_MOVE"
    assert out.outcome != "WRONG"


def test_no_move_band_is_recorded_on_record():
    exit_date = D0 + timedelta(days=5)
    acc = make_fake_accessor({"ACME": {D0: 100.0, exit_date: 100.3}})
    rec = _rec(direction="BUY")
    out = evaluate_signal(rec, acc, dead_band_pct=2.5, today=exit_date)
    assert out.dead_band_pct == 2.5


# ---------------------------------------------------------------------------
# UNKNOWN direction -> INELIGIBLE, never WRONG, never retried
# ---------------------------------------------------------------------------


def test_unknown_direction_is_ineligible_never_wrong():
    calls = []

    def counting_accessor(instrument, as_of):
        calls.append((instrument, as_of))
        return PricePoint(price=100.0, bar_date=as_of)

    rec = _rec(direction="UNKNOWN")
    out = evaluate_signal(rec, counting_accessor, today=D0)
    assert out.outcome == "INELIGIBLE"
    assert out.eligibility_reason == REASON_UNKNOWN_DIRECTION
    # never guesses at a price for an unknown-direction signal
    assert calls == []


def test_unknown_direction_stays_ineligible_on_repeat_eval():
    def counting_accessor(instrument, as_of):
        return PricePoint(price=999.0, bar_date=as_of)

    rec = _rec(direction="UNKNOWN")
    out1 = evaluate_signal(rec, counting_accessor, today=D0)
    out2 = evaluate_signal(rec, counting_accessor, today=D0 + timedelta(days=365))
    assert out1.outcome == out2.outcome == "INELIGIBLE"
    assert out1.eligibility_reason == out2.eligibility_reason == REASON_UNKNOWN_DIRECTION


# ---------------------------------------------------------------------------
# Missing entry / exit / unsupported instrument / sanity bounds
# ---------------------------------------------------------------------------


def test_missing_entry_price_is_ineligible():
    acc = make_fake_accessor({})  # no data for ACME at all
    rec = _rec()
    out = evaluate_signal(rec, acc, today=D0)
    assert out.outcome == "INELIGIBLE"
    assert out.eligibility_reason == REASON_MISSING_ENTRY


def test_missing_exit_before_horizon_is_unresolved():
    acc = make_fake_accessor({"ACME": {D0: 100.0}})  # nothing after entry yet
    rec = _rec(horizon_days=10)
    out = evaluate_signal(rec, acc, today=D0 + timedelta(days=3))
    assert out.outcome == "UNRESOLVED"
    assert out.eligibility_reason is None
    assert out.entry_price == 100.0


def test_missing_exit_after_horizon_is_ineligible():
    acc = make_fake_accessor({"ACME": {D0: 100.0}})
    rec = _rec(horizon_days=10)
    out = evaluate_signal(rec, acc, today=D0 + timedelta(days=30))
    assert out.outcome == "INELIGIBLE"
    assert out.eligibility_reason == REASON_MISSING_EXIT


def test_unsupported_instrument_is_ineligible():
    acc = make_fake_accessor({})
    rec = _rec(instrument="DELISTED")
    out = evaluate_signal(rec, acc, today=D0)
    assert out.outcome == "INELIGIBLE"
    assert out.eligibility_reason == REASON_UNSUPPORTED_INSTRUMENT


def test_price_out_of_sanity_bounds_is_ineligible():
    exit_date = D0 + timedelta(days=5)
    acc = make_fake_accessor({"ACME": {D0: -5.0, exit_date: 100.0}})
    rec = _rec()
    out = evaluate_signal(rec, acc, today=exit_date, sanity_bounds=(0.0, 1_000_000.0))
    assert out.outcome == "INELIGIBLE"
    assert out.eligibility_reason == REASON_PRICE_OUT_OF_BOUNDS


# ---------------------------------------------------------------------------
# Future rows appearing after the horizon must not change an already
# resolved record — exit_as_of is fixed at signal_date + horizon_days.
# ---------------------------------------------------------------------------


def test_future_rows_after_horizon_do_not_change_the_record():
    exit_date = D0 + timedelta(days=5)
    far_future = D0 + timedelta(days=400)

    acc_without_future = make_fake_accessor({"ACME": {D0: 100.0, exit_date: 103.0}})
    acc_with_future = make_fake_accessor(
        {"ACME": {D0: 100.0, exit_date: 103.0, far_future: 9999.0}}
    )

    rec = _rec()
    out1 = evaluate_signal(rec, acc_without_future, today=far_future)
    out2 = evaluate_signal(rec, acc_with_future, today=far_future)

    assert out1.outcome == out2.outcome == "CORRECT"
    assert out1.exit_price == out2.exit_price == 103.0
    assert out1.raw_return == out2.raw_return


# ---------------------------------------------------------------------------
# Cost adjustment sign
# ---------------------------------------------------------------------------


def test_cost_adjustment_always_drags_return_down():
    exit_date = D0 + timedelta(days=5)
    acc = make_fake_accessor({"ACME": {D0: 100.0, exit_date: 103.0}})
    rec = _rec(direction="BUY")

    out_no_cost = evaluate_signal(rec, acc, today=exit_date, cost_bps=0)
    out_with_cost = evaluate_signal(rec, acc, today=exit_date, cost_bps=50)

    assert out_with_cost.cost_adjusted_return < out_no_cost.cost_adjusted_return
    assert out_with_cost.cost_adjusted_return < out_with_cost.raw_return
    assert out_with_cost.cost_bps == 50


# ---------------------------------------------------------------------------
# Duplicate collapse
# ---------------------------------------------------------------------------


def test_dedupe_records_collapses_same_identity():
    rec = _rec()
    dup = _rec()  # identical source_type/instrument/date/horizon
    other = _rec(instrument="OTHER")

    deduped, n_dropped = dedupe_records([rec, dup, other])
    assert len(deduped) == 2
    assert n_dropped == 1


def test_summarize_outcomes_reports_dedup_count():
    exit_date = D0 + timedelta(days=5)
    acc = make_fake_accessor({"ACME": {D0: 100.0, exit_date: 103.0}})
    rec = _rec()
    out = evaluate_signal(rec, acc, today=exit_date)
    summary = summarize_outcomes([out, out, out])  # 3 identical outcome rows
    assert summary.n_total_input == 3
    assert summary.n_duplicates_dropped == 2
    assert summary.n_total == 1
    assert summary.n_correct == 1


# ---------------------------------------------------------------------------
# Baseline derived from the population, not an assumed 50%
# ---------------------------------------------------------------------------


def test_baseline_is_population_derived_not_fifty_fifty():
    exit_date = D0 + timedelta(days=5)

    outcomes = []
    # 3 correct, 1 wrong at horizon=5 -> population baseline should be 0.75,
    # not 0.5.
    for i, ret in enumerate([2.0, 2.0, 2.0, -2.0]):
        acc = make_fake_accessor({f"T{i}": {D0: 100.0, exit_date: 100.0 + ret}})
        rec = _rec(instrument=f"T{i}", direction="BUY")
        outcomes.append(evaluate_signal(rec, acc, today=exit_date, dead_band_pct=1.0))

    summary = summarize_outcomes(outcomes)
    assert summary.n_correct == 3
    assert summary.n_wrong == 1
    baseline = summary.baseline_by_horizon[5]
    assert baseline["baseline_correct_rate"] == pytest.approx(0.75)
    assert baseline["baseline_correct_rate"] != 0.5


# ---------------------------------------------------------------------------
# Synthetic origin tag is tagged and separable
# ---------------------------------------------------------------------------


def test_synthetic_origin_tag_is_separable_in_cohort_summary():
    exit_date = D0 + timedelta(days=5)

    live_acc = make_fake_accessor({"LIVE1": {D0: 100.0, exit_date: 103.0}})
    synth_acc = make_fake_accessor({"SYN1": {D0: 100.0, exit_date: 97.0}})

    live_out = evaluate_signal(
        _rec(instrument="LIVE1", origin_tag="live"), live_acc, today=exit_date
    )
    synth_out = evaluate_signal(
        _rec(instrument="SYN1", origin_tag="synthetic"), synth_acc, today=exit_date
    )

    summary = summarize_outcomes([live_out, synth_out])
    assert summary.origin_tag_counts["live"] == 1
    assert summary.origin_tag_counts["synthetic"] == 1

    synthetic_only = filter_by_origin_tag([live_out, synth_out], "synthetic")
    assert len(synthetic_only) == 1
    assert synthetic_only[0].instrument == "SYN1"
    assert synthetic_only[0].outcome == "WRONG"


# ---------------------------------------------------------------------------
# Static migration-chain test
# ---------------------------------------------------------------------------


def test_new_revision_down_revision_matches_current_head():
    repo_root = Path(__file__).resolve().parents[1]
    head_file = repo_root / "migrations" / "versions" / "god_view_market_tables_20260918.py"
    head_source = head_file.read_text(encoding="utf-8")
    match = re.search(r'^revision:\s*str\s*=\s*"([^"]+)"', head_source, re.MULTILINE)
    assert match, "could not find revision id in god_view_market_tables_20260918.py"
    head_revision_id = match.group(1)

    new_file = repo_root / "migrations" / "versions" / "signal_evaluations_0918.py"
    new_source = new_file.read_text(encoding="utf-8")
    down_match = re.search(r'^down_revision:.*=\s*"([^"]+)"', new_source, re.MULTILINE)
    assert down_match, "could not find down_revision in signal_evaluations_0918.py"

    assert down_match.group(1) == head_revision_id == "god_view_market_tables_20260918"
    assert len("signal_evaluations_0918") <= 32
