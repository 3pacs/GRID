"""Caller regressions from independent PR742 review; offline fixtures only."""
import sys
from unittest.mock import MagicMock

import pytest

from tests import test_smart_scheduler_honest_success as honest
from tests import test_scheduler_pull_logging as group_tests
from ingestion import smart_scheduler as ss
from ingestion import scheduler

sched = honest.sched


@pytest.mark.parametrize("value", [None, True, "done", {"status": "SUCCESS"}])
def test_unreported_writes_never_mark_source_fresh(sched, monkeypatch, value):
    class Unknown(honest._ListPuller):
        RETURN = value

    monkeypatch.setattr(honest, "_ReviewUnknown", Unknown, raising=False)
    s = sched(honest._entry("review_unknown", "_ReviewUnknown"))
    result = s.tick()["results"][0]
    assert result["status"] != "SUCCESS"
    assert honest._last_pull(sched.engine, "REVIEW_UNKNOWN") is None
    assert honest._pull_log(sched.engine)[0][2] is None


def test_negative_count_is_not_a_clean_completed_check():
    assert ss._classify_outcome(-1)[0] == "FAILED"


def test_direct_partial_preserves_log_status_rows_and_freshness(monkeypatch):
    engine, summary = group_tests._run_list_result(
        monkeypatch, {"status": "PARTIAL", "rows_inserted": 12, "error": "two feeds failed"}
    )
    assert engine.pull_logs[1]["status"] == "PARTIAL"
    assert engine.pull_logs[1]["rows_inserted"] == 12
    assert engine.touched_source_ids == []
    assert summary["success_count"] == 0


def test_list_partial_with_rows_stays_partial():
    result = ss._classify_outcome([
        {"status": "PARTIAL", "rows_inserted": 12, "error": "incomplete universe"},
    ])
    assert result[:2] == ("PARTIAL", 12)


class OptionsPullResults(list):
    @property
    def summary(self):
        return {"status": "PARTIAL", "rows_inserted": 12, "scope": "subset", "error": "GEM subset"}


def test_options_summary_is_read_before_list_items():
    out = OptionsPullResults([{"ticker": "SPY", "status": "SUCCESS", "rows_inserted": 12}])
    assert ss._classify_outcome(out)[:2] == ("PARTIAL", 12)


@pytest.mark.parametrize("items", [
    [{"status": "UNCHANGED", "rows_inserted": 0}, {"status": "FAILED", "rows_inserted": 0}],
    [{"status": "SUCCESS", "rows_inserted": 12}, {"status": "FAILED", "rows_inserted": 0}],
])
def test_hf_mixed_dataset_failure_is_partial(items):
    assert ss._classify_outcome(items)[0] == "PARTIAL"


def test_hf_all_unchanged_is_checked_no_new_data():
    out = [{"status": "UNCHANGED", "rows_inserted": 0}, {"status": "UNCHANGED", "rows_inserted": 0}]
    assert ss._classify_outcome(out)[:2] == ("NO_NEW_DATA", 0)


def test_sec_summary_is_not_added_to_per_ticker_write_counts():
    out = [{"ticker": "AAPL", "status": "SUCCESS", "rows": 17},
           {"status": "SUMMARY", "rows_written": 17}]
    assert ss._extract_rows(out) == 17


def test_zero_row_repair_does_not_refresh_source(monkeypatch):
    from scripts import hermes_fixers

    honest._install_retry_puller(monkeypatch, "review_zero", {"status": "SUCCESS", "rows_inserted": 0})
    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    result = hermes_fixers._retry_source("review_zero", engine)
    assert not any("last_pull_at" in str(call.args[0]) for call in conn.execute.call_args_list)
    assert result.get("outcome") == "NO_NEW_DATA"


def test_classifier_and_direct_log_share_identical_row_extraction(monkeypatch):
    out = {"inserted": 4}
    assert ss._classify_outcome(out)[:2] == ("SUCCESS", 4)
    engine, _summary = group_tests._run_list_result(monkeypatch, out)
    assert engine.pull_logs[1]["rows_inserted"] == 4


def test_nested_yfinance_results_have_real_row_count():
    out = {"status": "SUCCESS", "counts": {"inserted": 1, "error": 0},
           "results": [{"ticker": "SPY", "status": "SUCCESS", "rows_inserted": 11}]}
    assert ss._classify_outcome(out)[:2] == ("SUCCESS", 11)


def test_error_and_zero_alternate_count_is_failed():
    assert ss._classify_outcome({"total_inserted": 0, "error": "provider 503"})[0] == "FAILED"


def test_completed_yfinance_envelope_with_every_ticker_failed_is_failed():
    out = {"status": "SUCCESS", "stopped_by_budget": False,
           "counts": {"inserted": 0, "error": 1, "no_data": 0, "duplicate_only": 0, "unattempted": 0},
           "results": [{"ticker": "SPY", "status": "PARTIAL", "rows_inserted": 0, "outcome": "error"}]}
    assert ss._classify_outcome(out)[0] == "FAILED"


def test_smart_and_direct_keep_partial_rows_in_log(sched, monkeypatch):
    class Partial(honest._ListPuller):
        RETURN = {"status": "PARTIAL", "rows_inserted": 12, "error": "incomplete"}

    monkeypatch.setattr(honest, "_ReviewPartial", Partial, raising=False)
    s = sched(honest._entry("review_partial", "_ReviewPartial"))
    assert s.tick()["results"][0]["status"] == "PARTIAL"
    assert honest._last_pull(sched.engine, "REVIEW_PARTIAL") is None
    assert honest._pull_log(sched.engine)[0][1:3] == ("PARTIAL", 12)


@pytest.mark.parametrize("items", [
    [{"ticker": "SPY", "status": "SUCCESS", "rows_inserted": 0, "outcome": "duplicate_only"}],
    [{"ticker": "SPY", "status": "PARTIAL", "rows_inserted": 0, "outcome": "error"}],
    [{"ticker": "SPY", "status": "SUCCESS", "rows_inserted": 12, "outcome": "inserted"}],
    [{"ticker": "SPY", "status": "SUCCESS", "rows_inserted": 12, "outcome": "inserted"},
     {"ticker": "QQQ", "status": "PARTIAL", "rows_inserted": 0, "outcome": "error"}],
])
def test_actual_daily_yfinance_block_does_not_bump_on_zero_or_failure(monkeypatch, items):
    # Execute the exact yfinance try block from _run_equity_pulls, with
    # every reachable engine/provider mocked. Other feed blocks are excluded.
    import ast
    import inspect
    from types import ModuleType
    import db

    tree = ast.parse(inspect.getsource(scheduler._run_equity_pulls))
    block = next(n for n in tree.body[0].body if isinstance(n, ast.Try)
                 and any(isinstance(v, ast.ImportFrom) and v.module == "ingestion.yfinance_pull"
                         for v in ast.walk(n)))
    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    monkeypatch.setattr(db, "get_engine", lambda: engine)
    monkeypatch.setattr(scheduler, "_pull_completed_spy_close", lambda *_args: None)
    fake_yf = ModuleType("ingestion.yfinance_pull")

    class YFinancePuller:
        def __init__(self, **_kwargs):
            pass

        def pull_all(self, **_kwargs):
            return items

    fake_yf.YFinancePuller = YFinancePuller
    monkeypatch.setitem(sys.modules, "ingestion.yfinance_pull", fake_yf)
    exec(compile(ast.Module(body=[block], type_ignores=[]), "daily-yfinance-block", "exec"),
         scheduler.__dict__, {"start_date": "2026-09-28"})
    bumped = any("last_pull_at" in str(call.args[0]) for call in conn.execute.call_args_list)
    assert bumped == (ss._classify_outcome(items)[0] == "SUCCESS")
    terminal = [call.kwargs if call.kwargs else call.args[1]
                for call in conn.execute.call_args_list if "UPDATE pull_log SET" in str(call.args[0])]
    assert len(terminal) == 1  # the try block executed through durable accounting
    assert terminal[0]["rows"] == sum(i["rows_inserted"] for i in items)


def test_coingecko_attempted_provider_outage_is_failure(monkeypatch):
    from ingestion import coingecko

    puller = coingecko.CoinGeckoPuller.__new__(coingecko.CoinGeckoPuller)
    monkeypatch.setattr(puller, "_get_fresh_tickers", lambda: set())

    def unavailable(_coin):
        raise RuntimeError("mock provider outage")

    monkeypatch.setattr(puller, "_fetch_price", unavailable)
    out = puller.pull_all(tickers=["BTC"])
    assert ss._classify_outcome(out)[0] == "FAILED"


class AggregateResults(list):
    def __init__(self, status, rows):
        super().__init__([{"ticker": "SPY", "status": "SUCCESS", "rows_inserted": rows}])
        self.summary = {"status": status, "rows_inserted": rows, "scope": "fixture"}


@pytest.mark.parametrize("status,rows,expected", [
    ("SUCCESS", 12, "SUCCESS"), ("PARTIAL", 12, "PARTIAL"),
    ("SUCCESS", 0, "NO_NEW_DATA"), ("SUCCESS", None, "FAILED"),
    ("SKIPPED", 0, "SKIPPED"),
])
def test_aggregate_summary_through_all_three_callers(sched, monkeypatch, status, rows, expected):
    from scripts import hermes_fixers

    out = AggregateResults(status, rows)

    class SummaryPuller(honest._ListPuller):
        RETURN = out

    monkeypatch.setattr(honest, "_SummaryPuller", SummaryPuller, raising=False)
    smart = sched(honest._entry("summary", "_SummaryPuller"))
    smart_result = smart.tick()["results"][0]
    assert smart_result["status"] == expected
    assert (honest._last_pull(sched.engine, "SUMMARY") is not None) == (expected == "SUCCESS")
    if expected != "SKIPPED":
        assert honest._pull_log(sched.engine)[0][2] == rows

    engine, group = group_tests._run_list_result(monkeypatch, out)
    assert group["results"][0]["status"] == expected
    assert engine.pull_logs[1]["rows_inserted"] == rows
    assert bool(engine.touched_source_ids) == (expected == "SUCCESS")

    honest._install_retry_puller(monkeypatch, "summary", out)
    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    repaired = hermes_fixers._retry_source("summary", engine)
    assert repaired["outcome"] == expected
    assert repaired["rows_inserted"] == rows
    assert any("last_pull_at" in str(c.args[0]) for c in conn.execute.call_args_list) == (expected == "SUCCESS")


@pytest.mark.parametrize("key", ss._ROW_COUNT_KEYS)
def test_write_count_keys_have_identical_group_and_smart_counts(monkeypatch, key):
    out = {key: 4}
    assert ss._classify_outcome(out)[:2] == ("SUCCESS", 4)
    engine, group = group_tests._run_list_result(monkeypatch, out)
    assert group["success_count"] == 1
    assert engine.pull_logs[1]["rows_inserted"] == 4


@pytest.mark.parametrize("out,expected,rows", [
    ({"total_rows": 12, "succeeded": 1, "total": 2}, "PARTIAL", 12),
    ({"total_rows": 0, "succeeded": 0, "total": 2}, "FAILED", 0),
    ({"total_rows": 0, "succeeded": 0, "total": 0, "skipped_reason": "package unavailable"}, "SKIPPED", 0),
    ({"status": "SUCCESS", "bills": {"status": "SUCCESS", "stored": 3},
      "votes": {"status": "FAILED", "stored": 0}}, "PARTIAL", 3),
    ({"status": "SUCCESS", "rows": 12, "errors": ["provider failed"]}, "PARTIAL", 12),
    ([{"status": "SUCCESS", "rows": 12}, {"status": "DEFERRED", "rows": 0}], "PARTIAL", 12),
    ([{"status": "SUCCESS", "rows": 12}, {"status": "SUCCESS"}], "PARTIAL", 12),
    ({"status": "SUCCESS", "rows": -1}, "FAILED", None),
    ({"status": "SUCCESS", "rows": True}, "FAILED", None),
    ({"status": "PARTIAL", "rows": 12, "results": []}, "PARTIAL", 12),
])
def test_coverage_and_invalid_count_edges(out, expected, rows):
    assert ss._classify_outcome(out)[:2] == (expected, rows)


def test_repull_zero_check_keeps_cadence_without_reporting_recovery(monkeypatch):
    from scripts import hermes_fixers

    state = MagicMock()
    state.cooldowns.can_retry.return_value = True
    monkeypatch.setattr(hermes_fixers, "_retry_source", lambda *_a, **_kw:
                        {"outcome": "NO_NEW_DATA", "rows_inserted": 0})
    result = hermes_fixers._execute_hermes_repair_command("REPULL:fixture", MagicMock(), {}, state)
    assert result["status"] == "no_new_data"
    state.cooldowns.record_attempt.assert_called_once_with("fixture", success=True)


def test_missing_registry_contracts_are_held_before_provider_import(sched, monkeypatch):
    import importlib

    entry = honest._entry("held", "_MissingProviderClass")
    entry["hold_reason"] = "No persistent write contract"
    smart = sched(entry)
    monkeypatch.setattr(importlib, "import_module", lambda *_a: pytest.fail("held job imported provider"))
    assert smart.tick()["results"][0]["status"] == "SKIPPED"
    assert honest._pull_log(sched.engine) == []


@pytest.mark.parametrize("status,rows", [("SUCCESS", 12), ("PARTIAL", 12)])
def test_options_publication_has_no_second_catalog_write_in_any_wrapper(sched, monkeypatch, status, rows):
    from scripts import hermes_fixers

    out = AggregateResults(status, rows)

    class OwnOptions(honest._ListPuller):
        SOURCE_NAME = "YFINANCE_OPTIONS"
        RETURN = out

    monkeypatch.setattr(honest, "_OwnOptions", OwnOptions, raising=False)
    smart = sched(honest._entry("options", "_OwnOptions", _catalog="YFINANCE_OPTIONS"))
    smart._update_last_pull = MagicMock()
    assert smart.tick()["results"][0]["status"] == status
    smart._update_last_pull.assert_not_called()

    engine = group_tests._FakeEngine({"YFINANCE_OPTIONS": 185})
    monkeypatch.setattr(scheduler, "_get_pullers_for_group", lambda *_a:
                        [("Options", OwnOptions(engine), "pull_all", {})])
    assert scheduler.run_pull_group("daily", engine, config={})["results"][0]["status"] == status
    assert engine.touched_source_ids == []
    assert engine.pull_logs[1]["rows_inserted"] == 12

    honest._install_retry_puller(monkeypatch, "options", out)
    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    assert hermes_fixers._retry_source("options", engine)["outcome"] == status
    assert not any("last_pull_at" in str(c.args[0]) for c in conn.execute.call_args_list)


def test_registry_holds_are_carried_to_repair_lookup(monkeypatch):
    from scripts import hermes_operator, hermes_fixers

    entry = honest._entry("held_contract", "_MissingClass")
    entry["hold_reason"] = "No persistent write contract"
    monkeypatch.setattr(ss, "PULLER_REGISTRY", [entry])
    registry = hermes_operator._build_source_registry()
    assert registry["held_contract"]["skip_runtime"] == entry["hold_reason"]
    monkeypatch.setattr(hermes_operator, "_SOURCE_REGISTRY", registry)
    with pytest.raises(ValueError, match="skipped at runtime"):
        hermes_fixers._resolve_puller("held_contract", MagicMock())
