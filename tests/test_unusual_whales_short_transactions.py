"""unusual_whales writes in short batched transactions, never a savepoint per row.

2026-10-02 offshore_leaks incident: one long transaction with a SAVEPOINT per
row holds one subtransaction XID lock per row until the top-level commit and
can exhaust PostgreSQL's shared lock table. pull_ticker had the same shape
(one transaction per ticker, two savepoints per signal).
"""

from __future__ import annotations

from unittest.mock import MagicMock

from ingestion.altdata import unusual_whales as uw


def _signal(i: int) -> dict:
    return {
        "ticker": "SPY", "strike": 400.0 + i, "expiration": "2026-10-16", "direction": "CALL",
        "open_interest": 10, "volume": 5000, "last_price": 1.5, "implied_volatility": 0.2,
        "notional_premium": 750000.0, "signals": ["volume_spike"], "oi_ratio": 1.0,
        "volume_ratio": 9.0, "avg_oi": 10.0, "avg_volume": 100.0,
    }


class _Conn:
    def __init__(self, fail_on: set[int] | None = None) -> None:
        self.raw_inserts = 0
        self.fail_on = fail_on or set()

    def execute(self, stmt, params=None):
        sql = " ".join(str(stmt).split()).upper()
        if sql.startswith("INSERT INTO RAW_SERIES"):
            strike = int(float(params["sid"].split(":")[2]))
            if strike in self.fail_on:
                raise RuntimeError("bad row")
            self.raw_inserts += 1
        res = MagicMock()
        res.fetchone.return_value = None
        res.fetchall.return_value = []
        return res

    def begin_nested(self):
        raise AssertionError("savepoint per row: each one holds a subtransaction lock")


def _puller(per_txn: list[int], fail_on: set[int] | None = None):
    class Begin:
        def __enter__(self):
            self.conn = _Conn(fail_on)
            return self.conn

        def __exit__(self, exc_type, *rest):
            if exc_type is None:
                per_txn.append(self.conn.raw_inserts)
            return False

    puller = uw.UnusualWhalesPuller.__new__(uw.UnusualWhalesPuller)
    puller.engine = MagicMock()
    puller.engine.begin.side_effect = lambda: Begin()
    puller.source_id = 9
    puller._get_expirations = lambda ticker: ["2026-10-16"]
    puller._fetch_options_chain = lambda ticker, exp: {"calls": [{}], "puts": []}
    return puller


def test_pull_ticker_never_holds_one_transaction_across_many_inserts(monkeypatch) -> None:
    per_txn: list[int] = []
    puller = _puller(per_txn)
    monkeypatch.setattr(uw.time, "sleep", lambda _s: None)
    puller._detect_unusual_activity = lambda t, e, o, d: [_signal(i) for i in range(230)]
    out = puller.pull_ticker("SPY")
    assert out["rows_inserted"] == 230
    assert max(per_txn) <= uw.STORE_BATCH_ROWS
    assert sum(per_txn) == 230


def test_a_bad_row_only_loses_itself(monkeypatch) -> None:
    per_txn: list[int] = []
    puller = _puller(per_txn, fail_on={407})
    monkeypatch.setattr(uw.time, "sleep", lambda _s: None)
    puller._detect_unusual_activity = lambda t, e, o, d: [_signal(i) for i in range(60)]
    out = puller.pull_ticker("SPY")
    assert out["rows_inserted"] == 59  # batch 1 retried row by row; batch 2 intact
    assert max(per_txn) <= uw.STORE_BATCH_ROWS
