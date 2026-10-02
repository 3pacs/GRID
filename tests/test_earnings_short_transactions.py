"""Exercise committed earnings rows, rollback isolation and outage containment."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest
from sqlalchemy import exc as sa_exc

from ingestion.altdata import earnings_puller as ep


def _frames(n: int = 45) -> SimpleNamespace:
    return SimpleNamespace(
        earnings_dates=pd.DataFrame({
            "EPS Estimate": [1.0] * n,
            "Reported EPS": [1.05] * n,
            "Surprise(%)": [5.0] * n,
        }, index=pd.date_range("2025-01-01", periods=n)),
        quarterly_earnings=pd.DataFrame({"Revenue": [100.0], "Earnings": [5.0]},
                                        index=pd.to_datetime(["2025-03-31"])),
        earnings_history=pd.DataFrame({"epsEstimate": [1.0], "epsActual": [1.05], "surprisePercent": [5.0]},
                                      index=pd.to_datetime(["2025-03-31"])),
    )


class _Engine:
    def __init__(self, *, bad_dates=(), begin_error=None, fail_after=None, insert_error=None,
                 commit_error=None, commit_before_error=False):
        self.committed: dict[tuple[str, date], dict] = {}
        self.per_txn: list[int] = []
        self.rolled_back: list[int] = []
        self.active = False
        self.begin_calls = 0
        self.bad_dates = set(bad_dates)
        self.begin_error = begin_error
        self.fail_after = fail_after
        self.insert_error = insert_error
        self.commit_error = commit_error
        self.commit_before_error = commit_before_error

    def begin(self):
        self.begin_calls += 1
        assert not self.active, "nested transaction"
        if self.begin_error:
            raise self.begin_error
        if self.fail_after is not None and len(self.committed) >= self.fail_after:
            raise sa_exc.OperationalError("connect", {}, Exception("database unavailable"))
        return _Txn(self)


class _Txn:
    def __init__(self, engine):
        self.engine = engine
        self.pending: dict[tuple[str, date], dict] = {}
        self.insert_attempts = 0

    def __enter__(self):
        self.engine.active = True
        return self

    def __exit__(self, exc_type, *rest):
        self.engine.active = False
        if exc_type is None and self.engine.commit_error:
            if self.engine.commit_before_error:
                self.engine.committed.update(self.pending)
            self.engine.rolled_back.append(self.insert_attempts)
            raise self.engine.commit_error
        if exc_type is None:
            self.engine.committed.update(self.pending)
            self.engine.per_txn.append(self.insert_attempts)
        else:
            self.engine.rolled_back.append(self.insert_attempts)
        return False

    def begin_nested(self):
        raise AssertionError("earnings writers must not use savepoints")

    def execute(self, stmt, params=None):
        sql = " ".join(str(stmt).split()).upper()
        result = MagicMock()
        result.fetchall.return_value = []
        if sql.startswith("SELECT DISTINCT OBS_DATE FROM RAW_SERIES"):
            result.fetchall.return_value = [
                (day,) for (sid, day), row in {**self.engine.committed, **self.pending}.items()
                if sid == params["sid"] and row["src"] == params["src"] and row["status"] == "SUCCESS"
                and params["start_date"] <= day <= params["end_date"]
            ]
        elif sql.startswith("INSERT INTO RAW_SERIES"):
            self.insert_attempts += 1
            if self.engine.insert_error:
                raise self.engine.insert_error
            if params["sid"].endswith(":eps_actual") and params["od"] in self.engine.bad_dates:
                raise sa_exc.IntegrityError("INSERT", {}, Exception("bad earnings row"))
            key = (params["sid"], params["od"])
            assert key not in self.engine.committed and key not in self.pending, "duplicate was rewritten"
            self.pending[key] = dict(params)
        return result


def _puller(engine, frames=None):
    puller = ep.EarningsPuller.__new__(ep.EarningsPuller)
    puller.engine = engine
    puller.source_id = 9
    puller._fetch_ticker_data = lambda ticker: frames if frames is not None else _frames()
    return puller


def test_all_fields_share_a_50_row_bound_and_dedupe_after_commit():
    engine = _Engine()
    frames = _frames(45)  # 135 EPS points + 2 quarterly points + 1 history point
    # A repeated upstream observation must dedupe within/across batches too.
    frames.earnings_dates = pd.concat([frames.earnings_dates, frames.earnings_dates.iloc[[0]]])
    puller = _puller(engine, frames)
    out = puller.pull_ticker("AAPL")
    assert out["status"] == "SUCCESS" and out["rows_inserted"] == 138
    assert engine.per_txn == [50, 50, 38]
    assert all(row["src"] == 9 and row["status"] == "SUCCESS" for row in engine.committed.values())
    assert all("payload" in row for row in engine.committed.values())
    original = dict(engine.committed)
    frames.earnings_dates["Reported EPS"] = 10.0  # revised provider values stay append-only
    # Keep the surprise unchanged so no new beat_flag series is introduced.
    rerun = puller.pull_ticker("AAPL")
    assert rerun["rows_inserted"] == 0 and engine.committed == original
    assert "No earnings data available" not in rerun["errors"]


def test_each_yfinance_property_is_fetched_once_outside_transactions():
    engine = _Engine()
    frames = _frames()
    calls = []

    class _Stock:
        def __getattr__(self, name):
            assert not engine.active, "network access inside a database transaction"
            calls.append(name)
            return getattr(frames, name)

    out = _puller(engine, _Stock()).pull_ticker("AAPL")
    assert out["status"] == "SUCCESS"
    assert calls == ["earnings_dates", "quarterly_earnings", "earnings_history"]


def test_bad_middle_rows_rollback_batch_and_only_lose_themselves(monkeypatch):
    engine = _Engine(bad_dates={date(2025, 1, 8), date(2025, 1, 9)})
    monkeypatch.setattr(ep.time, "sleep", lambda _: None)
    out = _puller(engine).pull_all(["AAPL", "MSFT"])
    assert [r["ticker"] for r in out] == ["AAPL", "MSFT"]
    for result in out:
        assert result["status"] == "PARTIAL" and result["rows_inserted"] == 136
        assert result["rows_failed"] == 2 and len(result["errors"]) == 2
        assert "aborted" not in result
    assert len(engine.committed) == 272
    assert max(engine.per_txn + engine.rolled_back) <= ep.STORE_BATCH_ROWS
    # The first batch reaches its middle bad row, rolls back, then retries singles.
    assert engine.rolled_back[0] > 1 and engine.per_txn[:20] == [1] * 20
    assert ("earnings:AAPL:eps_actual", date(2025, 2, 14)) in engine.committed


@pytest.mark.parametrize("failure", [
    RuntimeError("cannot acquire database connection"),
    sa_exc.OperationalError("connect", {}, Exception("FATAL: out of shared memory")),
])
def test_database_outage_stops_before_other_tickers(failure):
    engine = _Engine(begin_error=failure)
    out = _puller(engine).pull_all(["AAPL", "MSFT"])
    assert len(out) == 1 and out[0]["status"] == "FAILED" and out[0]["aborted"]
    assert out[0]["rows_inserted"] == 0 and not engine.committed
    assert engine.begin_calls == ep.MAX_CONSECUTIVE_CONNECTION_FAILURES


def test_lost_commit_acknowledgement_stops_and_reports_an_unknown_total():
    error = sa_exc.OperationalError("COMMIT", {}, Exception("connection disappeared"))
    engine = _Engine(commit_error=error, commit_before_error=True)
    out = _puller(engine).pull_all(["AAPL", "MSFT"])
    assert len(out) == 1 and out[0]["status"] == "FAILED" and out[0]["aborted"]
    assert out[0]["commit_outcome_unknown"]
    assert out[0]["rows_inserted"] == 0  # no acknowledged commit; actual total is explicitly unknown
    assert len(engine.committed) == 50  # server committed before losing its acknowledgement
    assert engine.begin_calls == 1  # no retry silently recasts these as duplicates


@pytest.mark.parametrize("where", ["insert_error", "commit_error"])
def test_connection_errors_during_statement_or_commit_do_not_count_rolled_back_rows(where):
    error = sa_exc.DBAPIError("INSERT", {}, Exception("connection gone"), connection_invalidated=True)
    engine = _Engine(**{where: error})
    out = _puller(engine).pull_ticker("AAPL")
    assert out["status"] == "FAILED" and out["aborted"]
    assert out["rows_inserted"] == 0 and not engine.committed
    if where == "commit_error":
        assert engine.begin_calls == 1 and out["commit_outcome_unknown"]
    else:
        assert engine.begin_calls == ep.MAX_CONSECUTIVE_CONNECTION_FAILURES


def test_outage_preserves_prior_committed_batch_count():
    engine = _Engine(fail_after=50)
    out = _puller(engine).pull_all(["AAPL", "MSFT"])
    assert len(out) == 1 and out[0]["status"] == "PARTIAL" and out[0]["aborted"]
    assert out[0]["rows_inserted"] == 50 == len(engine.committed)
    assert engine.per_txn == [50]


def test_data_errors_on_every_row_report_failure_without_stopping_next_ticker(monkeypatch):
    engine = _Engine(insert_error=sa_exc.IntegrityError("INSERT", {}, Exception("bad data")))
    monkeypatch.setattr(ep.time, "sleep", lambda _: None)
    out = _puller(engine, _frames(1)).pull_all(["AAPL", "MSFT"])
    assert len(out) == 2 and all(r["status"] == "FAILED" and r["rows_inserted"] == 0 for r in out)
    assert all(r["rows_failed"] == 6 and "aborted" not in r for r in out)


def test_upstream_phase_failure_is_reported_while_other_phases_commit():
    frames = _frames(1)

    class _Stock:
        @property
        def earnings_dates(self):
            raise RuntimeError("provider unavailable")

        quarterly_earnings = frames.quarterly_earnings
        earnings_history = frames.earnings_history

    engine = _Engine()
    out = _puller(engine, _Stock()).pull_ticker("AAPL")
    assert out["status"] == "PARTIAL" and out["rows_inserted"] == 3
    assert any("earnings_dates" in error for error in out["errors"])


def test_store_batch_rejects_an_oversized_transaction():
    engine = _Engine()
    puller = _puller(engine)
    with pytest.raises(ValueError, match="exceeds 50"):
        puller._store_batch("AAPL", [{}] * 51, {"connection_failures": 0})
    assert engine.begin_calls == 0
