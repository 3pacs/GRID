"""unusual_whales writes in short batched transactions, never a savepoint per row.

2026-10-02 offshore_leaks incident: one long transaction with a SAVEPOINT per
row holds one subtransaction XID lock per row until the top-level commit and
can exhaust PostgreSQL's shared lock table. pull_ticker had the same shape
(one transaction per ticker, two savepoints per signal).

The fake engine keeps committed raw_series series_ids, so ``_row_exists``
dedupe (inside a batch, across batches and across a per-row retry) is real.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy import exc as sa_exc

import ingestion.smart_scheduler as ss
from ingestion.altdata import unusual_whales as uw


def _signal(i: int, ticker: str = "SPY") -> dict:
    return {
        "ticker": ticker, "strike": 400.0 + i, "expiration": "2026-10-16", "direction": "CALL",
        "open_interest": 10, "volume": 5000, "last_price": 1.5, "implied_volatility": 0.2,
        "notional_premium": 750000.0, "signals": ["volume_spike"], "oi_ratio": 1.0,
        "volume_ratio": 9.0, "avg_oi": 10.0, "avg_volume": 100.0,
    }


class _Engine:
    """engine.begin() -> a transaction whose raw_series inserts commit only on success."""

    def __init__(self, fail_on: set[int] | None = None, begin_error: Exception | None = None,
                 fail_after_commits: int | None = None) -> None:
        self.committed: set[str] = set()
        self.per_txn: list[int] = []
        self.begin_calls = 0
        self.fail_on = fail_on or set()
        self.begin_error = begin_error
        self.fail_after_commits = fail_after_commits

    def begin(self):
        self.begin_calls += 1
        if self.begin_error is not None:
            raise self.begin_error
        if self.fail_after_commits is not None and len(self.committed) >= self.fail_after_commits:
            raise sa_exc.OperationalError("connect", {}, Exception("FATAL: out of shared memory"))
        return _Txn(self)


class _Txn:
    def __init__(self, engine: _Engine) -> None:
        self.engine = engine
        self.pending: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *rest):
        if exc_type is None:
            self.engine.committed.update(self.pending)
            self.engine.per_txn.append(len(self.pending))
        return False

    def begin_nested(self):
        raise AssertionError("savepoint per row: each one holds a subtransaction lock")

    def execute(self, stmt, params=None):
        sql = " ".join(str(stmt).split()).upper()
        res = MagicMock()
        res.fetchall.return_value = []
        res.fetchone.return_value = None
        if sql.startswith("SELECT 1 FROM RAW_SERIES"):
            sid = params["sid"]
            res.fetchone.return_value = (1,) if sid in self.engine.committed or sid in self.pending else None
        elif sql.startswith("INSERT INTO RAW_SERIES"):
            if int(float(params["sid"].split(":")[2])) in self.engine.fail_on:
                raise sa_exc.IntegrityError("INSERT", {}, Exception("bad row"))
            self.pending.append(params["sid"])
        return res


def _puller(engine: _Engine, signals: list[dict], monkeypatch) -> uw.UnusualWhalesPuller:
    monkeypatch.setattr(uw.time, "sleep", lambda _s: None)
    puller = uw.UnusualWhalesPuller.__new__(uw.UnusualWhalesPuller)
    puller.engine = engine
    puller.source_id = 9
    puller._get_expirations = lambda ticker: ["2026-10-16"]
    puller._fetch_options_chain = lambda ticker, exp: {"calls": [{}], "puts": []}
    puller._detect_unusual_activity = lambda t, e, o, d: [dict(s, ticker=t) for s in signals]
    return puller


def test_pull_ticker_never_holds_one_transaction_across_many_inserts(monkeypatch) -> None:
    engine = _Engine()
    signals = [_signal(i) for i in range(230)] + [_signal(3), _signal(120)]  # two duplicates
    out = _puller(engine, signals, monkeypatch).pull_ticker("SPY")
    assert out["status"] == "SUCCESS" and out["rows_inserted"] == 230
    assert max(engine.per_txn) <= uw.STORE_BATCH_ROWS
    assert len(engine.committed) == 230
    rerun = _puller(engine, signals, monkeypatch).pull_ticker("SPY")
    assert rerun["rows_inserted"] == 0  # same day: deduped, never rewritten


def test_a_bad_row_only_loses_itself_and_is_reported(monkeypatch) -> None:
    engine = _Engine(fail_on={407})
    signals = [_signal(i) for i in range(60)] + [_signal(5)]  # duplicate of a row in the retried batch
    out = _puller(engine, signals, monkeypatch).pull_ticker("SPY")
    assert out["rows_inserted"] == 59 and len(engine.committed) == 59
    assert out["status"] == "PARTIAL" and out["errors"]
    assert ss._classify_outcome([out])[0] == ss.OUTCOME_PARTIAL
    assert max(engine.per_txn) <= uw.STORE_BATCH_ROWS


def test_adjacent_bad_rows_never_abort_the_scan(monkeypatch) -> None:
    engine = _Engine(fail_on={400, 401})
    out = _puller(engine, [_signal(i) for i in range(60)], monkeypatch).pull_all(["SPY", "QQQ"])
    assert [r["ticker"] for r in out] == ["SPY", "QQQ"]  # QQQ still processed
    for r in out:
        assert r["status"] == "PARTIAL" and r["rows_inserted"] == 58  # only the 2 bad rows lost
        assert "aborted" not in r
    assert len(engine.committed) == 116
    assert ss._classify_outcome(out)[0] == ss.OUTCOME_PARTIAL


def test_database_outage_fails_fast_and_stops_the_scan(monkeypatch) -> None:
    engine = _Engine(begin_error=RuntimeError("db down"))  # cannot even open a transaction
    puller = _puller(engine, [_signal(i) for i in range(120)], monkeypatch)
    out = puller.pull_all(["SPY", "QQQ", "IWM"])
    assert [r["status"] for r in out] == ["FAILED"]  # scan stopped, no other ticker attempted
    assert engine.begin_calls == uw.MAX_CONSECUTIVE_CONNECTION_FAILURES
    assert ss._classify_outcome(out)[0] == ss.OUTCOME_FAILED


def test_outage_after_some_rows_reports_them_and_stops_the_scan(monkeypatch) -> None:
    engine = _Engine(fail_after_commits=50)  # first batch commits, then the database goes away
    out = _puller(engine, [_signal(i) for i in range(120)], monkeypatch).pull_all(["SPY", "QQQ"])
    assert len(out) == 1 and out[0]["ticker"] == "SPY"
    assert out[0]["status"] == "PARTIAL" and out[0]["aborted"] is True
    assert out[0]["rows_inserted"] == 50 == len(engine.committed)


def test_connection_level_error_aborts_after_batch_and_first_retry(monkeypatch) -> None:
    down = sa_exc.OperationalError("connect", {}, Exception("FATAL: out of shared memory"))
    engine = _Engine(begin_error=down)
    puller = _puller(engine, [_signal(i) for i in range(120)], monkeypatch)
    with pytest.raises(uw.WhaleStoreAborted):
        puller.pull_ticker("SPY")
    assert engine.begin_calls == 2
