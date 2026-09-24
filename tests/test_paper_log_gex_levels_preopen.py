"""End-to-end tests for `run_preopen` — every exclusion gate, in order,
plus the successful path. No network, no database: `select_chain_snapshot`,
`assert_read_only`, and `fetch_previous_close` are monkeypatched where
`preopen.py` imported them; the adapter is a plain test double."""

from __future__ import annotations

import os

# preopen.py imports db.py, which imports GRID's root config.settings —
# see test_paper_log_gex_levels_db.py for why this guard is needed.
os.environ.setdefault("DB_PASSWORD", "test-password")

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable
from unittest.mock import MagicMock

import pytest

import paper_log.gex_levels.preopen as preopen_mod
from paper_log.gex_levels.chain import ChainSelection
from paper_log.gex_levels.config import EASTERN, TICKER, VIX_TICKER
from paper_log.gex_levels.engine_adapter import LevelsResult
from paper_log.gex_levels.market_data import PricePoint
from paper_log.gex_levels.preopen import run_preopen
from paper_log.gex_levels.storage import PaperLogStore

TRADING_DAY = date(2026, 9, 24)  # Thursday
SUNDAY = date(2026, 9, 27)


def _now_fn(d: date, hour: int, minute: int, second: int = 0) -> Callable[[], datetime]:
    dt_et = datetime(d.year, d.month, d.day, hour, minute, second, tzinfo=EASTERN)
    dt_utc = dt_et.astimezone(timezone.utc)
    return lambda: dt_utc


def _price_point(price: float, as_of: date = date(2026, 9, 23)) -> PricePoint:
    return PricePoint(price=price, as_of_date=as_of, fetched_at=datetime(2026, 9, 24, 12, 0, tzinfo=timezone.utc))


FULL_LEVELS = LevelsResult(
    available=True, unavailable_reason=None,
    spot=500.5, spot_source="test", gamma_flip=495.0, put_wall=490.0, call_wall=510.0,
    gex_aggregate=1.0, gex_normalized=0.1, regime="NEUTRAL", raw={"ok": True},
)

UNAVAILABLE_LEVELS = LevelsResult(
    available=False, unavailable_reason="engine result missing: call_wall",
    spot=500.5, spot_source="test", gamma_flip=495.0, put_wall=490.0, call_wall=None,
    gex_aggregate=None, gex_normalized=None, regime=None, raw={},
)


class _FakeAdapter:
    def __init__(self, result: LevelsResult) -> None:
        self.result = result
        self.calls: list[tuple[str, date]] = []

    def get_levels(self, ticker: str, snap_date: date) -> LevelsResult:
        self.calls.append((ticker, snap_date))
        return self.result


def _fake_fetch_previous_close(spy_price: float | None = 500.0, vix_price: float | None = 15.0):
    def _fn(ticker: str, before: date, *, now_fn=None):
        if ticker == TICKER:
            return None if spy_price is None else _price_point(spy_price)
        if ticker == VIX_TICKER:
            return None if vix_price is None else _price_point(vix_price)
        raise AssertionError(f"unexpected ticker {ticker!r}")
    return _fn


def _patch_success_path(monkeypatch: pytest.MonkeyPatch, *, chain_snap_date: date, spy_price=500.0, vix_price=15.0) -> None:
    monkeypatch.setattr(preopen_mod, "assert_read_only", lambda engine: None)
    monkeypatch.setattr(
        preopen_mod, "select_chain_snapshot",
        lambda engine, ticker, run_at: ChainSelection(
            snap_date=chain_snap_date,
            created_at=datetime.combine(chain_snap_date, datetime.min.time(), tzinfo=timezone.utc),
        ),
    )
    monkeypatch.setattr(preopen_mod, "fetch_previous_close", _fake_fetch_previous_close(spy_price, vix_price))


# ── market_closed ────────────────────────────────────────────────────


def test_market_closed_excludes_before_touching_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    chain_spy = MagicMock(side_effect=AssertionError("select_chain_snapshot should not be called"))
    assert_spy = MagicMock(side_effect=AssertionError("assert_read_only should not be called"))
    monkeypatch.setattr(preopen_mod, "select_chain_snapshot", chain_spy)
    monkeypatch.setattr(preopen_mod, "assert_read_only", assert_spy)

    record = run_preopen(
        log_dir=tmp_path, db_engine=MagicMock(), code_sha="deadbeef",
        now_fn=_now_fn(SUNDAY, 8, 0), adapter=_FakeAdapter(FULL_LEVELS),
    )

    assert record["excluded"] is True
    assert record["exclusion_reason"] == "market_closed"
    chain_spy.assert_not_called()
    assert_spy.assert_not_called()


# ── late_preopen ─────────────────────────────────────────────────────


def test_late_preopen_at_exactly_0930_is_excluded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    chain_spy = MagicMock(side_effect=AssertionError("should not reach the DB"))
    monkeypatch.setattr(preopen_mod, "select_chain_snapshot", chain_spy)

    record = run_preopen(
        log_dir=tmp_path, db_engine=MagicMock(), code_sha="deadbeef",
        now_fn=_now_fn(TRADING_DAY, 9, 30, 0), adapter=_FakeAdapter(FULL_LEVELS),
    )
    assert record["excluded"] is True
    assert record["exclusion_reason"] == "late_preopen"
    chain_spy.assert_not_called()


def test_one_second_before_0930_is_not_late(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 23))
    record = run_preopen(
        log_dir=tmp_path, db_engine=MagicMock(), code_sha="deadbeef",
        now_fn=_now_fn(TRADING_DAY, 9, 29, 59), adapter=_FakeAdapter(FULL_LEVELS),
    )
    assert record["exclusion_reason"] != "late_preopen"


# ── no_chain / stale_chain ───────────────────────────────────────────


def test_no_chain(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(preopen_mod, "assert_read_only", lambda engine: None)
    monkeypatch.setattr(preopen_mod, "select_chain_snapshot", lambda engine, ticker, run_at: None)

    record = run_preopen(
        log_dir=tmp_path, db_engine=MagicMock(), code_sha="deadbeef",
        now_fn=_now_fn(TRADING_DAY, 8, 45), adapter=_FakeAdapter(FULL_LEVELS),
    )
    assert record["excluded"] is True
    assert record["exclusion_reason"] == "no_chain"


def test_stale_chain(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # 2026-09-24 is Thursday; the prior trading day is Wednesday 2026-09-23.
    # A chain dated 2026-09-22 (Tuesday) is more than one trading day old.
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 22))
    record = run_preopen(
        log_dir=tmp_path, db_engine=MagicMock(), code_sha="deadbeef",
        now_fn=_now_fn(TRADING_DAY, 8, 45), adapter=_FakeAdapter(FULL_LEVELS),
    )
    assert record["excluded"] is True
    assert record["exclusion_reason"] == "stale_chain"
    assert record["chain"]["snap_date"] == "2026-09-22"


def test_chain_from_prior_trading_day_is_not_stale(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 23))
    record = run_preopen(
        log_dir=tmp_path, db_engine=MagicMock(), code_sha="deadbeef",
        now_fn=_now_fn(TRADING_DAY, 8, 45), adapter=_FakeAdapter(FULL_LEVELS),
    )
    assert record["exclusion_reason"] != "stale_chain"


# ── engine_unavailable ───────────────────────────────────────────────


def test_engine_unavailable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 23))
    record = run_preopen(
        log_dir=tmp_path, db_engine=MagicMock(), code_sha="deadbeef",
        now_fn=_now_fn(TRADING_DAY, 8, 45), adapter=_FakeAdapter(UNAVAILABLE_LEVELS),
    )
    assert record["excluded"] is True
    assert record["exclusion_reason"] == "engine_unavailable"
    # Partial engine data is still recorded for audit even though excluded.
    assert record["engine"]["gamma_flip"] == 495.0
    assert record["engine"]["call_wall"] is None
    # Never reached P0/VIX — engine_unavailable is checked first.
    assert record["p0"] is None


# ── ref_mismatch ─────────────────────────────────────────────────────


def test_ref_mismatch_when_spot_and_p0_diverge(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # engine spot=500.5 (FULL_LEVELS); P0 forced far away (1% off > 0.25% threshold).
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 23), spy_price=495.0, vix_price=15.0)
    record = run_preopen(
        log_dir=tmp_path, db_engine=MagicMock(), code_sha="deadbeef",
        now_fn=_now_fn(TRADING_DAY, 8, 45), adapter=_FakeAdapter(FULL_LEVELS),
    )
    assert record["excluded"] is True
    assert record["exclusion_reason"] == "ref_mismatch"
    assert record["p0"]["price"] == 495.0
    assert record["ref_mismatch_pct"] == pytest.approx(abs(500.5 - 495.0) / 495.0)


def test_ref_match_within_threshold_is_not_excluded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # 500.5 vs 500.0 -> 0.1% divergence, under the 0.25% threshold.
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 23), spy_price=500.0, vix_price=15.0)
    record = run_preopen(
        log_dir=tmp_path, db_engine=MagicMock(), code_sha="deadbeef",
        now_fn=_now_fn(TRADING_DAY, 8, 45), adapter=_FakeAdapter(FULL_LEVELS),
    )
    assert record["exclusion_reason"] != "ref_mismatch"


# ── P0 / VIX fetch failure -> loud crash, not a taxonomy exclusion ─────


def test_missing_p0_raises_and_writes_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 23), spy_price=None, vix_price=15.0)
    with pytest.raises(RuntimeError, match="P0"):
        run_preopen(
            log_dir=tmp_path, db_engine=MagicMock(), code_sha="deadbeef",
            now_fn=_now_fn(TRADING_DAY, 8, 45), adapter=_FakeAdapter(FULL_LEVELS),
        )
    store = PaperLogStore(tmp_path)
    assert store.read_all() == []


def test_missing_vix_raises_and_writes_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 23), spy_price=500.0, vix_price=None)
    with pytest.raises(RuntimeError, match="VIX"):
        run_preopen(
            log_dir=tmp_path, db_engine=MagicMock(), code_sha="deadbeef",
            now_fn=_now_fn(TRADING_DAY, 8, 45), adapter=_FakeAdapter(FULL_LEVELS),
        )
    store = PaperLogStore(tmp_path)
    assert store.read_all() == []


# ── success path ─────────────────────────────────────────────────────


def test_successful_preopen_builds_real_levels_and_placebos(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 23), spy_price=500.0, vix_price=15.0)
    record = run_preopen(
        log_dir=tmp_path, db_engine=MagicMock(), code_sha="deadbeef",
        now_fn=_now_fn(TRADING_DAY, 8, 45), adapter=_FakeAdapter(FULL_LEVELS),
    )

    assert record["excluded"] is False
    assert record["exclusion_reason"] is None
    assert record["kind"] == "preopen"
    assert record["session_date"] == "2026-09-24"
    assert record["code_sha"] == "deadbeef"
    assert record["chain"]["snap_date"] == "2026-09-23"

    real = record["levels"]["real"]
    assert real == {"gamma_flip": 495.0, "put_wall": 490.0, "call_wall": 510.0}

    placebo = record["levels"]["placebo"]
    # mirror(495, 500) = 505; mirror(490, 500) = 510 -> collides w/ call_wall (510);
    # mirror(510, 500) = 490 -> collides w/ put_wall (490).
    assert placebo["gamma_flip"]["value"] == 505.0
    assert placebo["gamma_flip"]["dropped"] is False
    assert placebo["put_wall"]["dropped"] is True
    assert placebo["call_wall"]["dropped"] is True

    # Written via the real store — hash-chain fields present.
    assert record["prev_sha256"] is None
    assert record["prereg_sha256"] is not None

    store = PaperLogStore(tmp_path)
    assert store.verify_chain().ok is True


def test_successful_preopen_uses_the_selected_snap_date_against_the_adapter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 23))
    adapter = _FakeAdapter(FULL_LEVELS)
    run_preopen(
        log_dir=tmp_path, db_engine=MagicMock(), code_sha="deadbeef",
        now_fn=_now_fn(TRADING_DAY, 8, 45), adapter=adapter, ticker="SPY",
    )
    assert adapter.calls == [("SPY", date(2026, 9, 23))]


def test_two_preopen_runs_chain_correctly(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 23))
    run_preopen(
        log_dir=tmp_path, db_engine=MagicMock(), code_sha="sha1",
        now_fn=_now_fn(TRADING_DAY, 8, 45), adapter=_FakeAdapter(FULL_LEVELS),
    )
    second = run_preopen(
        log_dir=tmp_path, db_engine=MagicMock(), code_sha="sha2",
        now_fn=_now_fn(date(2026, 9, 25), 8, 45), adapter=_FakeAdapter(FULL_LEVELS),
    )
    assert second["prev_sha256"] is not None
    assert "prereg_sha256" not in second
    store = PaperLogStore(tmp_path)
    assert store.verify_chain().ok is True
    assert len(store.read_all()) == 2
