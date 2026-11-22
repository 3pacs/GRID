"""Synthetic MOVE proxy: variance interpolation, quote hygiene, Schwab exclusion.

Pure functions are tested directly; the writer runs against a MagicMock
connection. Sockets are refused, so nothing reaches the network or a database.
"""

from __future__ import annotations

import socket
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from ingestion.options_rates_vol import (
    CHAIN_SQL,
    OptionQuoteRow,
    OptionsRatesVolPuller,
    compute_expiry_atm_iv,
    interpolate_constant_maturity_iv,
)
from store.options_provenance import VISIBLE_SNAPSHOTS, visible_sql


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    calls: list[object] = []

    def _refuse(*args, **kwargs):
        calls.append(args)
        raise AssertionError("network access attempted in an offline test")

    monkeypatch.setattr(socket.socket, "connect", _refuse)
    monkeypatch.setattr(socket, "create_connection", _refuse)
    monkeypatch.setattr(socket, "getaddrinfo", _refuse)
    yield
    assert calls == []


SNAP = date(2026, 11, 20)
EXP20 = SNAP + timedelta(days=20)
EXP40 = SNAP + timedelta(days=40)


def _quotes() -> list[OptionQuoteRow]:
    return [
        # 20 DTE ATM at strike 82 -> IV 0.16 (schema spells types call/put)
        OptionQuoteRow(EXP20, "call", 82.0, 1.40, 1.50, 0.16),
        OptionQuoteRow(EXP20, "put", 82.0, 1.38, 1.48, 0.16),
        # 40 DTE ATM at strike 82 -> IV 0.20
        OptionQuoteRow(EXP40, "C", 82.0, 2.20, 2.35, 0.20),
        OptionQuoteRow(EXP40, "P", 82.0, 2.18, 2.33, 0.20),
        # Garbage quote (zero bid, wide spread, absurd IV) must be ignored
        OptionQuoteRow(EXP20, "call", 80.0, 0.0, 5.0, 1.90),
    ]


def test_synthetic_move_variance_interpolation_and_hygiene() -> None:
    quotes = _quotes()
    assert compute_expiry_atm_iv([q for q in quotes if q.expiry == EXP20]) == pytest.approx(0.16)
    iv30 = interpolate_constant_maturity_iv(SNAP, quotes, target_dte=30)
    expected = ((0.5 * (0.16**2 * 20) + 0.5 * (0.20**2 * 40)) / 30.0) ** 0.5
    assert iv30 == pytest.approx(expected, rel=1e-6)


def test_no_clean_pair_and_far_one_sided_expiry_fail_closed() -> None:
    assert compute_expiry_atm_iv([OptionQuoteRow(EXP20, "call", 82.0, 1.40, 1.50, 0.16)]) is None
    far = [OptionQuoteRow(SNAP + timedelta(days=60), t, 82.0, 3.0, 3.1, 0.2) for t in ("call", "put")]
    assert interpolate_constant_maturity_iv(SNAP, far, target_dte=30) is None


def test_chain_reads_through_the_schwab_filtered_fragment() -> None:
    assert ("FROM " + VISIBLE_SNAPSHOTS + " os ") in CHAIN_SQL
    sql = visible_sql(CHAIN_SQL, include_schwab=False).text
    assert "NOT EXISTS (SELECT 1 FROM options_capture_batches schwab_pb" in sql
    assert "FROM options_snapshots schwab_v" in sql  # the latest-batch view, not options_snapshots_all


def _chain_rows() -> list[tuple]:
    done = datetime(2026, 11, 20, 21, 5, tzinfo=timezone.utc)
    return [(q.expiry, q.opt_type, q.strike, q.bid, q.ask, q.iv, "b-1", done) for q in _quotes()]


def test_writer_runs_filtered_sql_and_keeps_batch_provenance() -> None:
    puller = OptionsRatesVolPuller.__new__(OptionsRatesVolPuller)
    conn = MagicMock()
    executed: list[str] = []

    def _execute(statement, params=None):
        executed.append(str(statement))
        result = MagicMock()
        result.fetchall.return_value = _chain_rows() if params and params["tk"] == "TLT" else []
        return result

    conn.execute.side_effect = _execute
    puller.engine = MagicMock()
    puller.engine.begin.return_value.__enter__.return_value = conn
    puller._get_existing_dates = lambda sid, c, start_date=None, end_date=None: set()
    inserted: list[dict] = []
    puller._insert_raw = lambda **kw: inserted.append(kw)

    out = puller.pull_for_date(SNAP)

    assert executed and all("schwab_pb" in s for s in executed)
    assert all("options_snapshots_all" not in s for s in executed)
    assert [r["series_id"] for r in inserted] == ["options_rates_vol:tlt_iv_30d"]
    payload = inserted[0]["raw_payload"]
    assert payload["capture_batch_ids"] == ["b-1"]
    assert payload["capture_completed_at"] == "2026-11-20T21:05:00+00:00"
    assert out["metrics"]["options_rates_vol:ief_iv_30d"] is None


def test_writer_refuses_a_future_day() -> None:
    puller = OptionsRatesVolPuller.__new__(OptionsRatesVolPuller)
    puller.engine = MagicMock()
    out = puller.pull_for_date(date.today() + timedelta(days=1))
    assert out["status"] == "FAILED" and out["rows_inserted"] == 0
    puller.engine.begin.assert_not_called()
