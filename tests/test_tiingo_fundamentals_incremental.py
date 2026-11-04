"""TiingoFundamentalsPuller incremental window + cooperative time budget.

Regression for the 2026-11-04 Hermes log audit: every scheduled run
re-requested history from 2020-01-01 and ran ~7,500 zero-row NOT EXISTS
inserts per ticker, so the DOW-30 pull overran SmartScheduler's 120 s timeout
("tiingo_fundamentals TIMEOUT after 120s — daemon thread orphaned").
Network and DB are faked.
"""

from __future__ import annotations

import inspect
from contextlib import contextmanager
from datetime import date

import pytest

from ingestion import tiingo_fundamentals_pull as tf
from ingestion.smart_scheduler import _classify_outcome


class _Res:
    rowcount = 0

    def fetchall(self):
        return []


class FakeEngine:
    def __init__(self):
        self.executed = []

    @contextmanager
    def begin(self):
        yield self

    @contextmanager
    def connect(self):
        yield self

    def execute(self, sql, params=None):
        self.executed.append((str(sql), params))
        return _Res()


class FakeResponse:
    def __init__(self, data):
        self._data = data

    def raise_for_status(self):
        return None

    def json(self):
        return self._data


@pytest.fixture
def env(monkeypatch):
    calls: list[dict] = []

    def fake_get(url, params=None, headers=None, timeout=None):
        calls.append({"url": url, **(params or {})})
        return FakeResponse([{"date": params["endDate"], "marketCap": 1.0}])

    monkeypatch.setattr(tf, "_TIINGO_API_KEY", "SYNTHETIC_TEST_KEY")
    monkeypatch.setattr(tf, "_RATE_LIMIT_DELAY", 0)
    monkeypatch.setattr(tf.requests, "get", fake_get)
    p = tf.TiingoFundamentalsPuller.__new__(tf.TiingoFundamentalsPuller)
    p.engine = FakeEngine()
    p.source_id = 565
    latest = {"TIINGO_FUND:AAPL:market_cap": date(2026, 11, 3)}
    p._get_latest_date = lambda series_id: latest.get(series_id)
    return p, calls


def test_scheduled_run_resumes_from_latest_stored_date(env):
    p, calls = env
    p.pull_all(["AAPL", "NEWCO"])
    assert [(c["url"].rsplit("/", 2)[-2], c["startDate"]) for c in calls] == [
        ("AAPL", "2026-10-27"),   # latest 2026-11-03 minus 7-day lookback
        ("NEWCO", "2020-01-01"),  # nothing stored -> full backfill
    ]


def test_explicit_start_date_is_honoured_for_every_ticker(env):
    p, calls = env
    p.pull_all(["AAPL", "NEWCO"], start_date="2026-10-04")
    assert [c["startDate"] for c in calls] == ["2026-10-04", "2026-10-04"]


def test_budget_stop_is_partial_with_unattempted_list(env):
    p, calls = env
    seen = {"n": 0}

    def should_continue():
        seen["n"] += 1
        return seen["n"] <= 1

    out = p.pull_all(["AAPL", "KO", "VZ"], should_continue=should_continue)
    assert out["status"] == "PARTIAL" and out["stopped_by_budget"] is True
    assert [r["ticker"] for r in out["results"]] == ["AAPL"]
    assert out["tickers_not_attempted"] == ["KO", "VZ"]
    assert len(calls) == 1
    assert _classify_outcome(out)[0] == "PARTIAL"


def test_full_budgeted_run_with_no_new_rows_is_no_new_data(env):
    p, _ = env
    out = p.pull_all(["AAPL", "KO"], should_continue=lambda: True)
    assert out["status"] == "SUCCESS" and out["tickers_not_attempted"] == []
    # FakeEngine reports rowcount 0: the NOT EXISTS guard found every row.
    assert _classify_outcome(out)[:2] == ("NO_NEW_DATA", 0)


def test_legacy_list_shape_without_budget(env):
    p, _ = env
    out = p.pull_all(["AAPL"])
    assert isinstance(out, list) and out[0]["ticker"] == "AAPL"


def test_scheduler_and_repair_can_wire_budget_and_window():
    params = inspect.signature(tf.TiingoFundamentalsPuller.pull_all).parameters
    assert "should_continue" in params and "start_date" in params
    assert params["start_date"].default is None
