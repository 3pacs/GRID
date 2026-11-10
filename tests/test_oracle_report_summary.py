"""Offline Oracle email contracts: truthful counts, units, scope and summaries."""
from types import SimpleNamespace
import sys
from unittest.mock import Mock

import pytest

from oracle import report


@pytest.fixture
def capture(monkeypatch):
    sender = Mock()
    monkeypatch.setattr(report, "_send_in_thread", sender)
    monkeypatch.setitem(sys.modules, "config", SimpleNamespace(settings=SimpleNamespace(REF_CHECK_ENABLED=False)))
    return sender


def payload(**updates):
    result = {
        "cycle_started_at": "2026-11-07T12:00:00+00:00",
        "cycle_completed_at": "2026-11-07T12:05:00+00:00",
        "new_predictions": 4340, "distinct_positions_written": 14,
        "positions_updated": 4326, "upsert_statements": 4340,
        "scoring": {"total": 4, "hits": 2, "partials": 1, "misses": 1,
                    "unscorable_entry_price": 3, "held_legacy_entry_price": 2,
                    "held_unmarked_null_entry_price": 1},
        "leaderboard": [{"name": "model_one", "weight": 1.2, "total": 10,
                         "hits": 5, "partials": 2, "misses": 3,
                         "hit_rate": .5, "pnl": 132879.0}],
    }
    result.update(updates)
    return result


def test_summary_leads_with_new_rows_not_model_votes(capture):
    report.send_oracle_report(payload())
    subject, html, plain = capture.call_args.args
    assert "14 New prediction rows" in subject
    assert "4,340 model votes" in plain
    assert "4,326 update operations" in plain
    assert html.index("At a glance") < html.index("Model diagnostics")
    assert "4 scored this cycle" in plain
    assert "3 unscorable" in plain and "3 held" in plain


def test_legacy_sum_is_never_a_percentage_return(capture):
    report.send_oracle_report(payload())
    _, html, plain = capture.call_args.args
    assert "P/L" not in html and "132879.0%" not in html
    assert "+132,879.0 pp" in plain
    assert "Signed move sum (pp)" in plain
    assert "not portfolio or option returns" in plain
    assert "unreconciled" in plain
    assert "capital" in plain and "costs" in plain


@pytest.mark.parametrize("counts", [{}, {"distinct_positions_written": None},
    {"distinct_positions_written": -1}, {"distinct_positions_written": True},
    {"distinct_positions_written": 14, "upsert_statements": 5000}])
def test_unknown_or_incomplete_counts_do_not_fall_back_to_votes(capture, counts):
    data = payload()
    data.pop("distinct_positions_written")
    data.update(counts)
    report.send_oracle_report(data)
    subject, _, plain = capture.call_args.args
    assert "New rows unknown" in subject
    assert "4,340 model votes" in plain
    assert "4,340 New" not in subject
    assert "update count unknown" in plain


def test_confirmed_zero_rows_is_not_unknown(capture):
    report.send_oracle_report(payload(distinct_positions_written=0, positions_updated=4340))
    assert "0 New prediction rows" in capture.call_args.args[0]


def test_missing_scoring_is_not_zero_results(capture):
    report.send_oracle_report(payload(scoring={}))
    assert "Scoring unavailable" in capture.call_args.args[2]


def test_explicit_zero_scoring_is_reported(capture):
    report.send_oracle_report(payload(scoring={"total": 0, "hits": 0, "misses": 0, "partials": 0}))
    assert "0 scored this cycle" in capture.call_args.args[2]


def test_cycle_and_render_times_are_distinct(capture):
    report.send_oracle_report(payload())
    plain = capture.call_args.args[2]
    assert "Cycle completed: 2026-11-07 12:05:00 UTC" in plain
    assert "Report generated:" in plain
    assert "not live quotes" in plain


def test_missing_cycle_time_is_not_replaced_with_report_time(capture):
    report.send_oracle_report(payload(cycle_completed_at=None))
    assert "Cycle completed: unknown" in capture.call_args.args[2]


def test_predictions_keep_detail_and_handle_unknown_prices(capture):
    predictions = [{"ticker": "<ABC>", "direction": "CALL", "confidence": .6,
                    "target_price": None, "current_price": None, "expected_move_pct": None,
                    "expiry": "2000-01-01", "timestamp": "1999-12-01T12:00:00+00:00",
                    "model_name": "test", "signals": [{"name": f"signal {i}", "z_score": 1,
                    "direction": "bullish"} for i in range(6)],
                    "anti_signals": [{"name": f"anti {i}", "z_score": -1} for i in range(4)]}]
    report.send_oracle_report(payload(top_predictions=predictions))
    _, html, plain = capture.call_args.args
    assert "&lt;ABC&gt;" in html and "<ABC>" not in html
    assert "Expired" in plain and "not an active call" in plain
    assert "signal 5" in plain and "anti 3" in plain
    assert "$0.00" not in plain and "n/a" in plain
    assert "<div" not in plain and "<table" not in plain


def test_nonfinite_diagnostic_is_unknown(capture):
    data = payload()
    data["leaderboard"][0]["pnl"] = float("inf")
    report.send_oracle_report(data)
    assert "inf" not in capture.call_args.args[2].lower()


def test_naive_prediction_time_does_not_claim_utc(capture):
    assert "timezone unknown" in report._timestamp("2026-11-07T12:00:00")


def test_cycle_exposes_source_times_and_clears_prior_write_counts(monkeypatch):
    from datetime import datetime
    from oracle.engine import OracleEngine
    engine = object.__new__(OracleEngine)
    engine._last_store_counts = {"inserted": 999, "updated": 1000, "upsert_statements": 1999}
    monkeypatch.setattr(engine, "score_expired_predictions", lambda: {"total": 0})
    monkeypatch.setattr(engine, "evolve_weights", lambda: {})
    monkeypatch.setattr(engine, "_load_models", lambda: [])
    monkeypatch.setattr(engine, "generate_predictions", lambda *a, **k: [])
    monkeypatch.setattr(engine, "_get_leaderboard", lambda: [])
    engine.engine = None
    monkeypatch.setitem(sys.modules, "oracle.model_evolver", SimpleNamespace(ModelEvolver=Mock(side_effect=RuntimeError("offline"))))
    monkeypatch.setitem(sys.modules, "oracle.trace_evolver", SimpleNamespace(TraceEvolver=Mock(side_effect=RuntimeError("offline"))))
    result = engine.run_cycle([])
    assert result["distinct_positions_written"] is None
    start = datetime.fromisoformat(result["cycle_started_at"])
    end = datetime.fromisoformat(result["cycle_completed_at"])
    assert start.tzinfo is not None and end >= start


def test_mismatched_score_totals_do_not_claim_a_rate(capture):
    report.send_oracle_report(payload(scoring={"total": 99, "hits": 2, "misses": 1, "partials": 1}))
    assert "Scoring counts disagree" in capture.call_args.args[2]


def test_weight_changes_and_all_supplied_predictions_survive(capture):
    report.send_oracle_report(payload(
        evolution={"changes": {"<model>": {"old": 1, "new": 2, "hit_rate": .7}}},
        top_predictions=[{"ticker": f"TEST{i}", "expiry": "2099-01-01"} for i in range(10)]))
    _, html, plain = capture.call_args.args
    assert "TEST9" in plain and "1 model weight changes" in plain
    assert "&lt;Model&gt;" in html
    assert "Future expiry; current validity not rechecked" in plain
    assert "recorded hit-rate input" in plain


@pytest.mark.parametrize("value", ["bad", float("nan"), float("inf"), True, None])
def test_invalid_numbers_are_never_shown_as_measurements(value):
    assert report._number(value, '.1f') == 'n/a'


def test_prediction_flow_and_today_expiry_are_explicit():
    from datetime import date
    section = report._prediction_section({"expiry": "2026-11-07", "flow_context": {
        "regime": "<risk>", "sector": "test", "posture": "watch"}}, date(2026, 11, 7))
    assert "Expires today; intraday expiry status unknown" in section['body']
    assert "&lt;risk&gt;" in section['body']
    assert "Recorded flow context" in section['body']


def test_unknown_prediction_expiry_is_explicit():
    from datetime import date
    section = report._prediction_section({"expiry": "not-a-date"}, date(2026, 11, 7))
    assert "Expiry unknown; current validity unknown" in section["body"]


def test_reference_guard_uses_mocked_results_before_queueing(capture, monkeypatch):
    monkeypatch.setitem(sys.modules, "config", SimpleNamespace(settings=SimpleNamespace(
        REF_CHECK_ENABLED=True, REF_CHECK_MAX_CONCURRENT=1, REF_CHECK_TIMEOUT_S=1,
        REF_CHECK_WAYBACK_ENABLED=False)))
    monkeypatch.setitem(sys.modules, "verification.ref_extractor", SimpleNamespace(
        extract_refs=lambda _: [SimpleNamespace(url="https://example.invalid")]))
    # Mock loop creation too: Windows asyncio creates a local socket pair.
    import asyncio
    loop = Mock()
    loop.run_until_complete.return_value = []
    monkeypatch.setattr(asyncio, "new_event_loop", lambda: loop)
    def check_urls(urls, **kwargs):
        assert urls == ["https://example.invalid"]
        return "synthetic awaitable"
    monkeypatch.setitem(sys.modules, "verification.url_health", SimpleNamespace(check_urls=check_urls))
    monkeypatch.setitem(sys.modules, "verification.annotator", SimpleNamespace(
        annotate_output=lambda *a: SimpleNamespace(cleaned_text="verified synthetic plain text",
                                                   removed_count=1, replaced_count=0)))
    report.send_oracle_report(payload())
    assert capture.call_args.args[2] == "verified synthetic plain text"
    loop.run_until_complete.assert_called_once_with("synthetic awaitable")
    loop.close.assert_called_once()


def test_reference_guard_failure_does_not_break_report(capture, monkeypatch):
    monkeypatch.setitem(sys.modules, "config", SimpleNamespace(settings=SimpleNamespace(REF_CHECK_ENABLED=True)))
    def unavailable(_):
        raise RuntimeError("synthetic unavailable verifier")
    monkeypatch.setitem(sys.modules, "verification.ref_extractor", SimpleNamespace(extract_refs=unavailable))
    report.send_oracle_report(payload())
    assert "At a glance" in capture.call_args.args[2]


def test_forward_report_separates_accepted_abstained_and_unchanged_records():
    data = payload(new_predictions=2, distinct_positions_written=2,
                   positions_updated=0, upsert_statements=10,
                   new_abstentions=3, unchanged_forecasts=5, new_record_rows=5)
    subject, _, plain = report.build_oracle_report(data)
    assert '2 New prediction rows' in subject
    assert '2 accepted forecasts' in plain
    assert '3 new abstention records' in plain
    assert '5 prior forecasts preserved unchanged' in plain
    assert 'New rows unknown' not in plain
