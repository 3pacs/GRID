"""congressional_trades rebuild: real disclosure dates, real amounts, fail-closed."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

from ingestion import flow_materializer as fm


def qq(sid, ticker, traded, member, txn, rng, modified, chamber="house"):
    key = "Representative" if chamber == "house" else "Senator"
    return (sid, f"quiverquant:{chamber}", ticker, date.fromisoformat(traded), "qq_house_trading",
            "house_trading", {key: member, "Transaction": txn, "Range": rng,
                              "Amount": "1001.0", "last_modified": modified, "BioGuideID": "X1"})


def native(sid, ticker, traded, member, side, disclosure, basis=None, rng="1001.0"):
    sv = {"chamber": "HOUSE", "amount_range": rng, "disclosure_date": disclosure,
          "disclosure_lag_days": 0}
    if basis:
        sv["disclosure_basis"] = basis
    return (sid, "congressional", ticker, date.fromisoformat(traded), member, side, sv)


def test_disclosure_date_is_never_the_trade_date():
    rows, skips = fm.build_congressional_rows([
        qq(1, "MCD", "2026-09-25", "Josh Gottheimer", "Sale", "$1,001 - $15,000", "2026-10-08"),
    ])
    assert len(rows) == 1
    r = rows[0]
    assert r["transaction_date"] == date(2026, 9, 25)
    assert r["disclosure_date"] == date(2026, 10, 8)
    assert r["amount"] == "$1,001 - $15,000"
    assert r["amount_midpoint"] == 8000.5
    assert r["transaction_type"] == "SELL"
    assert r["chamber"] == "HOUSE"
    assert r["signal_source_id"] == 1


def test_senate_rows_and_ticker_normalization():
    rows, _ = fm.build_congressional_rows([
        qq(2, " jpm ", "2026-09-04", "Sheldon Whitehouse", "Sale (Partial)", "$15,001 - $50,000",
           "2026-10-02", chamber="senate"),
    ])
    assert rows[0]["ticker"] == "JPM"
    assert rows[0]["chamber"] == "SENATE"
    assert rows[0]["representative"] == "Sheldon Whitehouse"


def test_pre_gdfix_native_rows_are_skipped_not_trusted():
    """No disclosure_basis = the old writer, which copied the trade date."""
    rows, skips = fm.build_congressional_rows([
        native(3, "AAPL", "2026-09-22", "Some Member", "BUY", "2026-09-22"),
    ])
    assert rows == []
    assert skips["native_no_disclosure_bound"] == 1


def test_native_statutory_bound_is_not_a_known_disclosure():
    rows, skips = fm.build_congressional_rows([
        native(4, "AAPL", "2026-09-01", "Some Member", "BUY", "2026-10-16", basis="statutory_bound"),
    ])
    assert rows == []
    assert skips["native_statutory_bound"] == 1


def test_native_mirror_of_qq_trade_is_dropped():
    rows, skips = fm.build_congressional_rows([
        native(5, "MCD", "2026-09-25", "Josh Gottheimer", "SELL", "2026-10-08",
               basis="reported", rng="$1,001 - $15,000"),
        qq(6, "MCD", "2026-09-25", "Josh Gottheimer", "Sale", "$1,001 - $15,000", "2026-10-08"),
    ])
    assert [r["signal_source_id"] for r in rows] == [6]
    assert skips["native_mirror_of_qq"] == 1


def test_qq_without_any_disclosure_bound_is_skipped():
    rows, skips = fm.build_congressional_rows([
        qq(7, "MCD", "2026-09-25", "Josh Gottheimer", "Sale", "$1,001 - $15,000", ""),
    ])
    assert rows == []
    assert skips["qq_no_disclosure_bound"] == 1


def test_unique_key_collision_keeps_largest_band():
    rows, skips = fm.build_congressional_rows([
        qq(8, "NVDA", "2026-09-02", "A Member", "Purchase", "$1,001 - $15,000", "2026-10-01"),
        qq(9, "NVDA", "2026-09-03", "A Member", "Purchase", "$50,001 - $100,000", "2026-10-01"),
    ])
    assert len(rows) == 1
    assert rows[0]["signal_source_id"] == 9
    assert skips["unique_key_collision"] == 1


def _engine_with(src_rows, existing):
    conn = MagicMock()
    executed = []

    def execute(stmt, params=None):
        sql = str(stmt)
        executed.append(sql)
        res = MagicMock()
        if "FROM signal_sources" in sql:
            res.fetchall.return_value = src_rows
        elif "COUNT(*)" in sql:
            res.scalar.return_value = existing
        return res

    conn.execute.side_effect = execute
    engine = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn
    return engine, executed


def test_rebuild_replaces_table(monkeypatch):
    monkeypatch.setattr(fm, "_ensure_tables", lambda _e: None)
    engine, executed = _engine_with(
        [qq(1, "MCD", "2026-09-25", "Josh Gottheimer", "Sale", "$1,001 - $15,000", "2026-10-08")], 1)
    assert fm.sync_congressional_trades(engine) == 1
    assert any(s.startswith("DELETE FROM congressional_trades") for s in executed)
    assert any("INSERT INTO congressional_trades" in s for s in executed)


def test_rebuild_refuses_when_nothing_built(monkeypatch):
    monkeypatch.setattr(fm, "_ensure_tables", lambda _e: None)
    engine, executed = _engine_with([], 727)
    assert fm.sync_congressional_trades(engine) == 0
    assert not any("DELETE" in s for s in executed)


def test_rebuild_refuses_sharp_shrink(monkeypatch):
    monkeypatch.setattr(fm, "_ensure_tables", lambda _e: None)
    engine, executed = _engine_with(
        [qq(1, "MCD", "2026-09-25", "Josh Gottheimer", "Sale", "$1,001 - $15,000", "2026-10-08")], 727)
    assert fm.sync_congressional_trades(engine) == 0
    assert not any("DELETE" in s for s in executed)
