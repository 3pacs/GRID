"""Hermes retry observes scheduler captures without claiming source recovery."""

from __future__ import annotations

import ast
import inspect
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy.engine import make_url

from ingestion import options, scheduler, smart_scheduler
from scripts import hermes_fixers as hf


@pytest.fixture
def observation(monkeypatch):
    import sqlalchemy

    engine = MagicMock()
    engine.url = make_url("postgresql+psycopg2://fixture@127.0.0.1:55499/synthetic")
    observed = MagicMock()
    observed.connect.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = None
    factory = MagicMock(return_value=observed)
    monkeypatch.setattr(sqlalchemy, "create_engine", factory)
    active = {}
    monkeypatch.setattr(hf, "_REPAIRS_IN_FLIGHT", active)
    return engine, observed, factory, active


@pytest.mark.parametrize("source", ["options", "yfinance_options", "YFINANCE_OPTIONS", "yfinance-options"])
def test_real_retry_skip_preserves_backlog_and_withholds_publication(monkeypatch, observation, source):
    engine, observed, factory, active = observation
    conn = observed.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchone.return_value = ("completed-synthetic-batch", "daily_scheduler")
    resolve = MagicMock(side_effect=AssertionError("provider puller constructed"))
    monkeypatch.setattr(hf, "_resolve_puller", resolve)
    state = SimpleNamespace(repair_backlog={source.lower(): ["QQQ"]},
                            repair_last_check={}, repair_uncovered={})
    result = hf._retry_source(source, engine, state=state)
    assert result["outcome"] == result["status"] == "SKIPPED"
    assert result["rows_inserted"] == 0
    assert result["scheduler_capture_source"] == "daily_scheduler"
    assert hf.retry_not_fresh_reason(result) == "puller reported SKIPPED"
    assert state.repair_backlog == {source.lower(): ["QQQ"]}
    assert not state.repair_last_check and not state.repair_uncovered and not active
    resolve.assert_not_called()
    engine.begin.assert_not_called()
    assert str(conn.execute.call_args_list[0].args[0]) == "SET TRANSACTION READ ONLY"
    args = factory.call_args.kwargs["connect_args"]
    assert args["connect_timeout"] == 5 and "statement_timeout=5000" in args["options"]
    observed.dispose.assert_called_once()


@pytest.mark.parametrize("unavailable", [False, True])
def test_empty_or_unavailable_observation_keeps_retry_and_actual_outcome(monkeypatch, observation, unavailable):
    engine, observed, _factory, active = observation
    if unavailable:
        observed.connect.side_effect = RuntimeError("synthetic observation failure")
    puller = SimpleNamespace(pull=MagicMock(return_value={"status": "PARTIAL", "rows_inserted": 3}))
    resolve = MagicMock(return_value=(puller, "pull", {}))
    monkeypatch.setattr(hf, "_resolve_puller", resolve)
    result = hf._retry_source("yfinance_options", engine)
    assert result["outcome"] == "PARTIAL" and result["rows_inserted"] == 3
    assert hf.retry_not_fresh_reason(result).startswith("puller reported PARTIAL")
    assert "scheduler_capture_batch_id" not in result
    resolve.assert_called_once()
    puller.pull.assert_called_once()
    engine.begin.assert_not_called()
    assert not active
    observed.dispose.assert_called_once()


def test_non_options_retry_never_observes_options(monkeypatch, observation):
    engine, _observed, factory, _active = observation
    puller = SimpleNamespace(pull=lambda: {"status": "PARTIAL", "rows_inserted": 1})
    monkeypatch.setattr(hf, "_resolve_puller", lambda *_a: (puller, "pull", {}))
    assert hf._retry_source("other_source", engine)["rows_inserted"] == 1
    factory.assert_not_called()


def test_existing_live_repair_owner_is_preserved_before_observation(observation):
    engine, _observed, factory, active = observation
    owner = {"started": hf.time.monotonic(), "token": -1, "thread": threading.get_ident()}
    active["yfinance_options"] = owner
    assert hf._retry_source("yfinance_options", engine)["reason"] == "in_flight"
    assert active["yfinance_options"] is owner
    factory.assert_not_called()


def test_smart_scheduler_labels_capture_and_keeps_summary_and_budget(monkeypatch):
    puller = MagicMock()
    summary = {"status": "PARTIAL", "rows_inserted": 9}
    puller.pull_all.return_value.summary = summary
    monkeypatch.setattr(options, "OptionsPuller", lambda **_kwargs: puller)
    def budget():
        return True
    assert smart_scheduler._OptionsSchedulerAdapter(MagicMock()).pull(budget) is summary
    puller.pull_all.assert_called_once_with(should_continue=budget, capture_source="smart_scheduler")


def test_actual_daily_options_block_labels_capture(monkeypatch):
    monkeypatch.setenv("TESTING", "true")
    monkeypatch.setenv("DB_PASSWORD", "synthetic-test-only")
    import db

    puller = MagicMock()
    puller.pull_all.return_value = [{"status": "SUCCESS", "snapshots": 2}]
    monkeypatch.setattr(db, "get_engine", MagicMock(return_value=MagicMock()))
    monkeypatch.setattr(options, "OptionsPuller", lambda **_kwargs: puller)
    # Execute the actual bounded options block; the full daily group would
    # invoke unrelated providers. This is its real code, not a rewritten flow.
    nodes = ast.parse(inspect.getsource(scheduler._run_equity_pulls)).body[0].body
    block = next(node for node in nodes if isinstance(node, ast.Try)
                 and any(isinstance(item, ast.ImportFrom) and item.module == "ingestion.options"
                         for item in node.body))
    exec(compile(ast.Module(body=[block], type_ignores=[]), "actual-daily-options-block", "exec"),
         dict(vars(scheduler)))
    puller.pull_all.assert_called_once_with(capture_source="daily_scheduler")
