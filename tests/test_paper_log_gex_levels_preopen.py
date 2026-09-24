"""End-to-end tests for `run_preopen` — every exclusion gate, in order,
plus the successful path. No network, no database: `select_chain_snapshot`,
`assert_read_only`, and `fetch_previous_close` are monkeypatched where
`preopen.py` imported them; the adapter and tested-wall computation are
plain test doubles injected via `run_preopen`'s own parameters."""

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
from paper_log.gex_levels.tested_walls import WallSelection

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
    spot=500.5, spot_source="test", gamma_flip=495.0, engine_put_wall=489.0, engine_call_wall=511.0,
    gex_aggregate=1.0, gex_normalized=0.1, regime="NEUTRAL", raw={"ok": True},
)

# Amendment 1: engine_unavailable is narrower now -- only spot/regime missing.
UNAVAILABLE_LEVELS = LevelsResult(
    available=False, unavailable_reason="engine result missing: regime",
    spot=500.5, spot_source="test", gamma_flip=495.0, engine_put_wall=489.0, engine_call_wall=None,
    gex_aggregate=None, gex_normalized=None, regime=None, raw={},
)

DEFAULT_TESTED_WALLS = WallSelection(put_wall=490.0, call_wall=510.0)


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


def _fake_compute_tested_walls(walls: WallSelection = DEFAULT_TESTED_WALLS, *, calls: list | None = None):
    def _fn(db_engine, ticker, snap_date, spot, p0):
        if calls is not None:
            calls.append((ticker, snap_date, spot, p0))
        return walls
    return _fn


def _patch_success_path(
    monkeypatch: pytest.MonkeyPatch, *, chain_snap_date: date, spy_price=500.0, vix_price=15.0,
) -> None:
    monkeypatch.setattr(preopen_mod, "assert_read_only", lambda engine: None)
    monkeypatch.setattr(
        preopen_mod, "select_chain_snapshot",
        lambda engine, ticker, run_at: ChainSelection(
            snap_date=chain_snap_date,
            created_at=datetime.combine(chain_snap_date, datetime.min.time(), tzinfo=timezone.utc),
        ),
    )
    monkeypatch.setattr(preopen_mod, "fetch_previous_close", _fake_fetch_previous_close(spy_price, vix_price))


def _run(monkeypatch=None, **overrides):
    kwargs = dict(
        log_dir=None, db_engine=MagicMock(), code_sha="deadbeef",
        now_fn=_now_fn(TRADING_DAY, 8, 45), adapter=_FakeAdapter(FULL_LEVELS),
        compute_tested_walls=_fake_compute_tested_walls(),
    )
    kwargs.update(overrides)
    return run_preopen(**kwargs)


# ── market_closed ────────────────────────────────────────────────────


def test_market_closed_excludes_before_touching_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    chain_spy = MagicMock(side_effect=AssertionError("select_chain_snapshot should not be called"))
    assert_spy = MagicMock(side_effect=AssertionError("assert_read_only should not be called"))
    monkeypatch.setattr(preopen_mod, "select_chain_snapshot", chain_spy)
    monkeypatch.setattr(preopen_mod, "assert_read_only", assert_spy)

    record = _run(log_dir=tmp_path, now_fn=_now_fn(SUNDAY, 8, 0))

    assert record["excluded"] is True
    assert record["exclusion_reason"] == "market_closed"
    chain_spy.assert_not_called()
    assert_spy.assert_not_called()


# ── late_preopen ─────────────────────────────────────────────────────


def test_late_preopen_at_exactly_0930_is_excluded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    chain_spy = MagicMock(side_effect=AssertionError("should not reach the DB"))
    monkeypatch.setattr(preopen_mod, "select_chain_snapshot", chain_spy)

    record = _run(log_dir=tmp_path, now_fn=_now_fn(TRADING_DAY, 9, 30, 0))
    assert record["excluded"] is True
    assert record["exclusion_reason"] == "late_preopen"
    chain_spy.assert_not_called()


def test_one_second_before_0930_is_not_late(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 23))
    record = _run(log_dir=tmp_path, now_fn=_now_fn(TRADING_DAY, 9, 29, 59))
    assert record["exclusion_reason"] != "late_preopen"


# ── no_chain / stale_chain ───────────────────────────────────────────


def test_no_chain(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(preopen_mod, "assert_read_only", lambda engine: None)
    monkeypatch.setattr(preopen_mod, "select_chain_snapshot", lambda engine, ticker, run_at: None)

    record = _run(log_dir=tmp_path)
    assert record["excluded"] is True
    assert record["exclusion_reason"] == "no_chain"


def test_stale_chain(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # 2026-09-24 is Thursday; the prior trading day is Wednesday 2026-09-23.
    # A chain dated 2026-09-22 (Tuesday) is more than one trading day old.
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 22))
    record = _run(log_dir=tmp_path)
    assert record["excluded"] is True
    assert record["exclusion_reason"] == "stale_chain"
    assert record["chain"]["snap_date"] == "2026-09-22"


def test_chain_from_prior_trading_day_is_not_stale(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 23))
    record = _run(log_dir=tmp_path)
    assert record["exclusion_reason"] != "stale_chain"


# ── engine_unavailable (Amendment 1: narrowed to spot/regime only) ─────


def test_engine_unavailable_when_no_spot_or_regime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 23))
    record = _run(log_dir=tmp_path, adapter=_FakeAdapter(UNAVAILABLE_LEVELS))
    assert record["excluded"] is True
    assert record["exclusion_reason"] == "engine_unavailable"
    # Partial engine data is still recorded for audit even though excluded.
    assert record["engine"]["gamma_flip"] == 495.0
    assert record["engine"]["engine_put_wall"] == 489.0
    # Never reached P0/VIX — engine_unavailable is checked first.
    assert record["p0"] is None


def test_missing_flip_or_wall_is_not_engine_unavailable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Amendment 1: "A missing flip or wall is recorded, not excluded" —
    the engine itself reporting no flip/walls no longer excludes the
    session (a *tested* wall can still be missing separately; see the
    tested-walls tests below)."""
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 23))
    levels = LevelsResult(
        available=True, unavailable_reason=None, spot=500.5, spot_source="test",
        gamma_flip=None, engine_put_wall=None, engine_call_wall=None,
        gex_aggregate=1.0, gex_normalized=0.1, regime="NEUTRAL", raw={},
    )
    record = _run(log_dir=tmp_path, adapter=_FakeAdapter(levels))
    assert record["excluded"] is False
    assert record["engine"]["gamma_flip"] is None
    assert record["levels"]["real"]["gamma_flip"] is None
    assert record["levels"]["real"]["gamma_flip_missing"] is True


# ── ref_mismatch ─────────────────────────────────────────────────────


def test_ref_mismatch_when_spot_and_p0_diverge(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # engine spot=500.5 (FULL_LEVELS); P0 forced far away (1% off > 0.25% threshold).
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 23), spy_price=495.0, vix_price=15.0)
    record = _run(log_dir=tmp_path)
    assert record["excluded"] is True
    assert record["exclusion_reason"] == "ref_mismatch"
    assert record["p0"]["price"] == 495.0
    assert record["ref_mismatch_pct"] == pytest.approx(abs(500.5 - 495.0) / 495.0)


def test_ref_match_within_threshold_is_not_excluded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # 500.5 vs 500.0 -> 0.1% divergence, under the 0.25% threshold.
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 23), spy_price=500.0, vix_price=15.0)
    record = _run(log_dir=tmp_path)
    assert record["exclusion_reason"] != "ref_mismatch"


# ── data_unavailable (Amendment 1) ──────────────────────────────────────


def test_p0_unavailable_after_retry_excludes_data_unavailable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 23), spy_price=None, vix_price=15.0)
    record = _run(log_dir=tmp_path)
    assert record["excluded"] is True
    assert record["exclusion_reason"] == "data_unavailable"
    assert record["p0"] is None
    store = PaperLogStore(tmp_path)
    assert len(store.read_all()) == 1  # a record WAS written, unlike the pre-amendment raise


def test_vix_unavailable_after_retry_excludes_data_unavailable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 23), spy_price=500.0, vix_price=None)
    record = _run(log_dir=tmp_path)
    assert record["excluded"] is True
    assert record["exclusion_reason"] == "data_unavailable"
    assert record["p0"] is not None  # P0 succeeded; VIX is what failed
    assert record["vix_prev_close"] is None


def test_p0_succeeds_on_the_retry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """First call returns None, second (the retry) succeeds — must not be
    treated as data_unavailable."""
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 23))
    calls = {"n": 0}

    def flaky_fetch(ticker, before, *, now_fn=None):
        if ticker == TICKER:
            calls["n"] += 1
            return None if calls["n"] == 1 else _price_point(500.0)
        return _price_point(15.0)  # VIX always succeeds

    monkeypatch.setattr(preopen_mod, "fetch_previous_close", flaky_fetch)
    record = _run(log_dir=tmp_path)

    assert calls["n"] == 2  # exactly one retry, not more
    assert record["exclusion_reason"] != "data_unavailable"
    assert record["p0"]["price"] == 500.0


def test_fetch_exception_counts_as_unreachable_and_still_retries(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """"unreachable" (an exception) must be handled the same as "returned
    nothing" (None) — retried once, then data_unavailable, never an
    uncaught crash."""
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 23))

    def always_raises(ticker, before, *, now_fn=None):
        if ticker == TICKER:
            raise ConnectionError("yfinance unreachable")
        return _price_point(15.0)

    monkeypatch.setattr(preopen_mod, "fetch_previous_close", always_raises)
    record = _run(log_dir=tmp_path)  # must not raise
    assert record["exclusion_reason"] == "data_unavailable"


# ── success path ─────────────────────────────────────────────────────


def test_successful_preopen_builds_real_levels_and_placebos(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 23), spy_price=500.0, vix_price=15.0)
    record = _run(log_dir=tmp_path, compute_tested_walls=_fake_compute_tested_walls(DEFAULT_TESTED_WALLS))

    assert record["excluded"] is False
    assert record["exclusion_reason"] is None
    assert record["kind"] == "preopen"
    assert record["session_date"] == "2026-09-24"
    assert record["code_sha"] == "deadbeef"
    assert record["chain"]["snap_date"] == "2026-09-23"

    # Engine's own (untested) walls, recorded separately.
    assert record["engine"]["engine_put_wall"] == 489.0
    assert record["engine"]["engine_call_wall"] == 511.0

    real = record["levels"]["real"]
    assert real["gamma_flip"] == 495.0
    assert real["gamma_flip_missing"] is False
    assert real["put_wall"] == 490.0  # from the tested-walls double, NOT engine_put_wall (489.0)
    assert real["put_wall_missing"] is False
    assert real["call_wall"] == 510.0  # NOT engine_call_wall (511.0)
    assert real["call_wall_missing"] is False

    placebo = record["levels"]["placebo"]
    # mirror(495, 500) = 505; mirror(490, 500) = 510 -> collides w/ tested call_wall (510);
    # mirror(510, 500) = 490 -> collides w/ tested put_wall (490).
    assert placebo["gamma_flip"]["value"] == 505.0
    assert placebo["gamma_flip"]["dropped"] is False
    assert placebo["put_wall"]["dropped"] is True
    assert placebo["call_wall"]["dropped"] is True

    # Written via the real store — hash-chain fields present.
    assert record["prev_sha256"] is None
    assert record["prereg_sha256"] is not None

    store = PaperLogStore(tmp_path)
    assert store.verify_chain().ok is True


def test_tested_walls_receives_engine_spot_and_p0(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Per Amendment 1's own module docstring: per-strike GEX uses the
    engine's spot; the threshold/tie-break uses P0. Both must reach
    compute_tested_walls with the right values."""
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 23), spy_price=500.0, vix_price=15.0)
    calls: list = []
    _run(log_dir=tmp_path, compute_tested_walls=_fake_compute_tested_walls(DEFAULT_TESTED_WALLS, calls=calls))

    assert len(calls) == 1
    ticker, snap_date, spot, p0 = calls[0]
    assert ticker == "SPY"
    assert snap_date == date(2026, 9, 23)
    assert spot == 500.5  # levels.spot (the engine's own spot)
    assert p0 == 500.0    # P0, not spot


def test_missing_tested_wall_is_recorded_not_excluded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 23), spy_price=500.0, vix_price=15.0)
    one_sided = WallSelection(put_wall=480.0, call_wall=None)
    record = _run(log_dir=tmp_path, compute_tested_walls=_fake_compute_tested_walls(one_sided))

    assert record["excluded"] is False
    real = record["levels"]["real"]
    assert real["put_wall"] == 480.0
    assert real["put_wall_missing"] is False
    assert real["call_wall"] is None
    assert real["call_wall_missing"] is True
    # No placebo for a wall that doesn't exist.
    assert "call_wall" not in record["levels"]["placebo"]


def test_successful_preopen_uses_the_selected_snap_date_against_the_adapter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 23))
    adapter = _FakeAdapter(FULL_LEVELS)
    _run(log_dir=tmp_path, adapter=adapter, ticker="SPY")
    assert adapter.calls == [("SPY", date(2026, 9, 23))]


def test_two_preopen_runs_chain_correctly(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_success_path(monkeypatch, chain_snap_date=date(2026, 9, 23))
    _run(log_dir=tmp_path, code_sha="sha1")
    second = _run(log_dir=tmp_path, code_sha="sha2", now_fn=_now_fn(date(2026, 9, 25), 8, 45))
    assert second["prev_sha256"] is not None
    assert "prereg_sha256" not in second
    store = PaperLogStore(tmp_path)
    assert store.verify_chain().ok is True
    assert len(store.read_all()) == 2
