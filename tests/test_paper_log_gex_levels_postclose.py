"""End-to-end tests for `run_postclose`. No network: `fetch_session_ohlc`,
`fetch_intraday_bars`, and (in one test) `expected_bar_count` are
monkeypatched where `postclose.py` imported them. Preopen fixtures are
hand-built JSONL records (not `run_preopen`) so this file has no
dependency on `db.py` / GRID's root `config.py` at all.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

import paper_log.gex_levels.postclose as postclose_mod
from paper_log.gex_levels.config import EASTERN
from paper_log.gex_levels.market_data import Bar, IntradayBars, SessionOHLC
from paper_log.gex_levels.placebo import build_placebos
from paper_log.gex_levels.postclose import run_postclose
from paper_log.gex_levels.storage import PaperLogStore

TRADING_DAY = date(2026, 9, 24)  # Thursday, regular 78-bar day
SUNDAY = date(2026, 9, 27)
FETCH_TIME = datetime(2026, 9, 24, 16, 5, tzinfo=timezone.utc)


def _seed_preopen(
    store: PaperLogStore,
    *,
    session_date: date,
    excluded: bool = False,
    exclusion_reason: str | None = None,
    p0: float = 500.0,
    gamma_flip: float | None = 495.0,
    put_wall: float | None = 490.0,
    call_wall: float | None = 520.0,
    regime: str = "SHORT_GAMMA",
) -> dict[str, Any]:
    # Mirrors preopen.py's own construction: placebos are built from only
    # the present (non-None) tested levels, and the recorded "real" dict
    # carries an explicit `<name>_missing` flag alongside each value
    # (Amendment 1).
    present = {}
    if gamma_flip is not None:
        present["gamma_flip"] = gamma_flip
    if put_wall is not None:
        present["put_wall"] = put_wall
    if call_wall is not None:
        present["call_wall"] = call_wall
    placebo = build_placebos(present, p0)

    real = {
        "gamma_flip": gamma_flip, "gamma_flip_missing": gamma_flip is None,
        "put_wall": put_wall, "put_wall_missing": put_wall is None,
        "call_wall": call_wall, "call_wall_missing": call_wall is None,
    }
    record = {
        "kind": "preopen",
        "run_at": datetime.combine(session_date, datetime.min.time(), tzinfo=timezone.utc),
        "session_date": session_date,
        "code_sha": "deadbeef",
        "excluded": excluded,
        "exclusion_reason": exclusion_reason,
        "chain": None if excluded and exclusion_reason in ("market_closed", "late_preopen", "no_chain") else {
            "snap_date": session_date - timedelta(days=1), "created_at": FETCH_TIME,
        },
        "engine": {
            "available": True, "unavailable_reason": None, "spot": p0, "spot_source": "test",
            "gamma_flip": gamma_flip, "engine_put_wall": put_wall, "engine_call_wall": call_wall,
            "gex_aggregate": 1.0, "gex_normalized": 0.1, "regime": regime,
        },
        "p0": {"price": p0, "as_of_date": session_date - timedelta(days=1), "fetched_at": FETCH_TIME},
        "vix_prev_close": {"price": 15.0, "as_of_date": session_date - timedelta(days=1), "fetched_at": FETCH_TIME},
        "ref_mismatch_pct": 0.0,
        "levels": {"real": real, "placebo": {name: asdict(pb) for name, pb in placebo.items()}},
    }
    return store.append(record)


def _bar(session_date: date, hh: int, mm: int, o: float, h: float, l: float, c: float) -> Bar:  # noqa: E741
    base = datetime(session_date.year, session_date.month, session_date.day, 0, 0, tzinfo=EASTERN)
    t = base + timedelta(hours=hh, minutes=mm)
    return Bar(time=t, open=o, high=h, low=l, close=c)


def _now_fn(d: date, hour: int = 16, minute: int = 30):
    dt_et = datetime(d.year, d.month, d.day, hour, minute, tzinfo=EASTERN)
    dt_utc = dt_et.astimezone(timezone.utc)
    return lambda: dt_utc


# ── market_closed ────────────────────────────────────────────────────


def test_market_closed_never_looks_for_preopen(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ohlc_spy = MagicMock(side_effect=AssertionError("should not fetch OHLC"))
    monkeypatch.setattr(postclose_mod, "fetch_session_ohlc", ohlc_spy)

    record = run_postclose(log_dir=tmp_path, code_sha="deadbeef", now_fn=_now_fn(SUNDAY))
    assert record["excluded"] is True
    assert record["exclusion_reason"] == "market_closed"
    ohlc_spy.assert_not_called()


# ── missing / excluded preopen ──────────────────────────────────────


def test_no_preopen_record_excludes_no_preopen(tmp_path: Path) -> None:
    """Amendment 1: this used to raise (no taxonomy code covered it); now
    it writes an excluded no_preopen record instead, so no session can go
    missing silently."""
    record = run_postclose(log_dir=tmp_path, code_sha="deadbeef", now_fn=_now_fn(TRADING_DAY))
    assert record["excluded"] is True
    assert record["exclusion_reason"] == "no_preopen"
    store = PaperLogStore(tmp_path)
    assert len(store.read_all()) == 1


def test_excluded_preopen_propagates_same_reason(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = PaperLogStore(tmp_path)
    _seed_preopen(store, session_date=TRADING_DAY, excluded=True, exclusion_reason="ref_mismatch")

    ohlc_spy = MagicMock(side_effect=AssertionError("should not fetch OHLC for an excluded session"))
    monkeypatch.setattr(postclose_mod, "fetch_session_ohlc", ohlc_spy)

    record = run_postclose(log_dir=tmp_path, code_sha="deadbeef", now_fn=_now_fn(TRADING_DAY))
    assert record["excluded"] is True
    assert record["exclusion_reason"] == "ref_mismatch"
    ohlc_spy.assert_not_called()


# ── ohlc missing / bars_missing ─────────────────────────────────────


def test_missing_ohlc_is_market_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = PaperLogStore(tmp_path)
    _seed_preopen(store, session_date=TRADING_DAY)
    monkeypatch.setattr(postclose_mod, "fetch_session_ohlc", lambda ticker, d, now_fn: None)

    record = run_postclose(log_dir=tmp_path, code_sha="deadbeef", now_fn=_now_fn(TRADING_DAY))
    assert record["excluded"] is True
    assert record["exclusion_reason"] == "market_closed"


def test_bars_missing_excludes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = PaperLogStore(tmp_path)
    _seed_preopen(store, session_date=TRADING_DAY)
    monkeypatch.setattr(
        postclose_mod, "fetch_session_ohlc",
        lambda ticker, d, now_fn: SessionOHLC(open=500, high=505, low=495, close=502, fetched_at=FETCH_TIME),
    )
    monkeypatch.setattr(postclose_mod, "expected_bar_count", lambda d: 10)
    # Only 5 of the "expected" 10 bars present -> 50% missing, way over 10%.
    bars = tuple(_bar(TRADING_DAY, 9, 30 + 5 * i, 500, 501, 499, 500) for i in range(5))
    monkeypatch.setattr(
        postclose_mod, "fetch_intraday_bars",
        lambda ticker, d, now_fn: IntradayBars(bars=bars, fetched_at=FETCH_TIME),
    )

    record = run_postclose(log_dir=tmp_path, code_sha="deadbeef", now_fn=_now_fn(TRADING_DAY))
    assert record["excluded"] is True
    assert record["exclusion_reason"] == "bars_missing"
    assert record["bars"]["expected"] == 10
    assert record["bars"]["present"] == 5
    assert record["bars"]["missing_pct"] == pytest.approx(0.5)


def test_bars_present_at_exactly_90_percent_is_not_excluded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = PaperLogStore(tmp_path)
    _seed_preopen(store, session_date=TRADING_DAY, regime="NEUTRAL")
    monkeypatch.setattr(
        postclose_mod, "fetch_session_ohlc",
        lambda ticker, d, now_fn: SessionOHLC(open=500, high=505, low=495, close=502, fetched_at=FETCH_TIME),
    )
    monkeypatch.setattr(postclose_mod, "expected_bar_count", lambda d: 10)
    bars = tuple(_bar(TRADING_DAY, 9, 30 + 5 * i, 500, 501, 499, 500) for i in range(9))  # 90% present
    monkeypatch.setattr(
        postclose_mod, "fetch_intraday_bars",
        lambda ticker, d, now_fn: IntradayBars(bars=bars, fetched_at=FETCH_TIME),
    )

    record = run_postclose(log_dir=tmp_path, code_sha="deadbeef", now_fn=_now_fn(TRADING_DAY))
    assert record["exclusion_reason"] != "bars_missing"


# ── full success path: reaches + H3 trades, real and placebo ───────


def test_success_computes_reaches_and_h3_trades_for_real_and_placebo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = PaperLogStore(tmp_path)
    # real: gamma_flip=495, put_wall=490, call_wall=520, P0=500.
    # placebo (mirrored, asymmetric so nothing collides at the 0.10% rule):
    #   gamma_flip_slot=505, put_wall_slot=510, call_wall_slot=480.
    _seed_preopen(
        store, session_date=TRADING_DAY, p0=500.0,
        gamma_flip=495.0, put_wall=490.0, call_wall=520.0, regime="SHORT_GAMMA",
    )

    monkeypatch.setattr(
        postclose_mod, "fetch_session_ohlc",
        lambda ticker, d, now_fn: SessionOHLC(open=500.0, high=523.0, low=499.0, close=518.0, fetched_at=FETCH_TIME),
    )
    monkeypatch.setattr(postclose_mod, "expected_bar_count", lambda d: 6)
    bars = (
        _bar(TRADING_DAY, 9, 30, 500.0, 501.0, 499.0, 500.5),
        _bar(TRADING_DAY, 9, 35, 500.5, 503.0, 500.0, 502.0),
        _bar(TRADING_DAY, 9, 40, 502.0, 505.0, 501.0, 504.0),
        _bar(TRADING_DAY, 9, 45, 504.0, 522.0, 503.0, 521.0),
        _bar(TRADING_DAY, 9, 50, 521.0, 523.0, 519.0, 520.0),
        _bar(TRADING_DAY, 9, 55, 520.0, 521.0, 515.0, 518.0),
    )
    monkeypatch.setattr(
        postclose_mod, "fetch_intraday_bars",
        lambda ticker, d, now_fn: IntradayBars(bars=bars, fetched_at=FETCH_TIME),
    )

    record = run_postclose(log_dir=tmp_path, code_sha="deadbeef", now_fn=_now_fn(TRADING_DAY))

    assert record["excluded"] is False
    assert record["session_ohlc"] == {
        "open": 500.0, "high": 523.0, "low": 499.0, "close": 518.0, "fetched_at": FETCH_TIME.isoformat(),
    }
    import math
    assert record["range_ln"] == pytest.approx(math.log(523.0 / 499.0))

    real_reach = record["reaches"]["real"]
    assert real_reach["gamma_flip"]["status"] == "none"
    assert real_reach["put_wall"]["status"] == "none"
    assert real_reach["call_wall"]["status"] == "reached"
    assert real_reach["call_wall"]["bar_time"] == bars[3].time.isoformat()
    assert real_reach["call_wall"]["held"] is True

    placebo_reach = record["reaches"]["placebo"]
    assert placebo_reach["gamma_flip"]["status"] == "reached"
    assert placebo_reach["gamma_flip"]["bar_time"] == bars[2].time.isoformat()
    assert placebo_reach["gamma_flip"]["held"] is False
    assert placebo_reach["put_wall"]["status"] == "reached"
    assert placebo_reach["put_wall"]["bar_time"] == bars[3].time.isoformat()
    assert placebo_reach["put_wall"]["held"] is False
    assert placebo_reach["call_wall"]["status"] == "none"

    real_trade = record["h3_trade"]["real"]
    assert real_trade["triggered"] is True
    assert real_trade["regime_rule"] == "follow"
    assert real_trade["direction"] == "long"
    assert real_trade["trigger_level_name"] == "call_wall"
    assert real_trade["trigger_time"] == bars[3].time.isoformat()
    assert real_trade["raw_entry_price"] == 521.0
    assert real_trade["raw_exit_price"] == 518.0

    placebo_trade = record["h3_trade"]["placebo"]
    assert placebo_trade["triggered"] is True
    assert placebo_trade["direction"] == "short"
    assert placebo_trade["trigger_level_name"] == "put_wall"
    assert placebo_trade["trigger_time"] == bars[0].time.isoformat()
    assert placebo_trade["raw_entry_price"] == 500.5

    store2 = PaperLogStore(tmp_path)
    assert store2.verify_chain().ok is True


def test_neutral_regime_never_produces_a_trade(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = PaperLogStore(tmp_path)
    _seed_preopen(store, session_date=TRADING_DAY, regime="NEUTRAL")
    monkeypatch.setattr(
        postclose_mod, "fetch_session_ohlc",
        lambda ticker, d, now_fn: SessionOHLC(open=500.0, high=523.0, low=488.0, close=518.0, fetched_at=FETCH_TIME),
    )
    monkeypatch.setattr(postclose_mod, "expected_bar_count", lambda d: 1)
    bars = (_bar(TRADING_DAY, 9, 30, 500.0, 523.0, 488.0, 518.0),)
    monkeypatch.setattr(
        postclose_mod, "fetch_intraday_bars",
        lambda ticker, d, now_fn: IntradayBars(bars=bars, fetched_at=FETCH_TIME),
    )

    record = run_postclose(log_dir=tmp_path, code_sha="deadbeef", now_fn=_now_fn(TRADING_DAY))
    assert record["excluded"] is False
    assert record["h3_trade"]["real"]["triggered"] is False
    assert record["h3_trade"]["placebo"]["triggered"] is False


def test_missing_real_wall_cannot_reach_or_trigger(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Amendment 1: a tested wall can be missing (no strike qualified) —
    postclose must record it as `null` rather than crash, and it must not
    be able to reach or trigger an H3 trade."""
    store = PaperLogStore(tmp_path)
    _seed_preopen(
        store, session_date=TRADING_DAY, p0=500.0,
        gamma_flip=495.0, put_wall=None, call_wall=520.0, regime="SHORT_GAMMA",
    )
    monkeypatch.setattr(
        postclose_mod, "fetch_session_ohlc",
        lambda ticker, d, now_fn: SessionOHLC(open=500.0, high=505.0, low=470.0, close=500.0, fetched_at=FETCH_TIME),
    )
    monkeypatch.setattr(postclose_mod, "expected_bar_count", lambda d: 1)
    # Low of 470 would have reached a put_wall of 490 if one existed.
    bars = (_bar(TRADING_DAY, 9, 30, 500.0, 505.0, 470.0, 500.0),)
    monkeypatch.setattr(
        postclose_mod, "fetch_intraday_bars",
        lambda ticker, d, now_fn: IntradayBars(bars=bars, fetched_at=FETCH_TIME),
    )

    record = run_postclose(log_dir=tmp_path, code_sha="deadbeef", now_fn=_now_fn(TRADING_DAY))
    assert record["excluded"] is False
    assert record["reaches"]["real"]["put_wall"] is None
    assert record["reaches"]["real"]["gamma_flip"] is not None  # unaffected
    assert record["h3_trade"]["real"]["trigger_level_name"] != "put_wall"


def test_dropped_placebo_wall_cannot_trigger_or_reach(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = PaperLogStore(tmp_path)
    # Symmetric walls around P0 -> both placebo put/call walls collide with
    # the real opposite wall and are dropped (see test_paper_log_gex_levels_placebo.py).
    _seed_preopen(
        store, session_date=TRADING_DAY, p0=500.0,
        gamma_flip=500.0, put_wall=490.0, call_wall=510.0, regime="SHORT_GAMMA",
    )
    monkeypatch.setattr(
        postclose_mod, "fetch_session_ohlc",
        lambda ticker, d, now_fn: SessionOHLC(open=500.0, high=515.0, low=485.0, close=500.0, fetched_at=FETCH_TIME),
    )
    monkeypatch.setattr(postclose_mod, "expected_bar_count", lambda d: 1)
    bars = (_bar(TRADING_DAY, 9, 30, 500.0, 515.0, 485.0, 500.0),)
    monkeypatch.setattr(
        postclose_mod, "fetch_intraday_bars",
        lambda ticker, d, now_fn: IntradayBars(bars=bars, fetched_at=FETCH_TIME),
    )

    record = run_postclose(log_dir=tmp_path, code_sha="deadbeef", now_fn=_now_fn(TRADING_DAY))
    assert record["reaches"]["placebo"]["put_wall"] is None
    assert record["reaches"]["placebo"]["call_wall"] is None
    assert record["h3_trade"]["placebo"]["triggered"] is False
