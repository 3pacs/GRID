"""Tests for ``validation.backtest.run_hold_validation`` (LEVER-PACKAGE §7 T2.3).

No live database: the realized-alpha PIT helpers imported into
``validation.backtest`` are monkeypatched to return synthetic ticker and SPY
price paths (``pd.Series`` on a business-day ``DatetimeIndex``). The engine is
a bare ``MagicMock`` and must never be queried by the function under test
except through the patched helpers.

Covers:
  1. positive-alpha scored entries → ``pass`` with correct hit_rate/mean/t-stat
     and holding_days taken from the SPY calendar
  2. exits after ``as_of`` → ``open`` (marked to last PIT close, excluded from stats)
  3. an entry before the price path starts → ``skipped`` with reason
  4. ``n_scored < 5`` → ``insufficient``
  5. SHORT direction flips the sign
  6. missing ticker feature → ``ValueError``
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

import validation.backtest as vb

AS_OF = date(2026, 9, 10)
PATH_START = "2025-06-02"
TICKER_FID = 4242
SPY_FID = 77


def _bday_index(start: str = PATH_START, end: date = AS_OF) -> pd.DatetimeIndex:
    return pd.bdate_range(start, end)


def _flat_path(level: float, start: str = PATH_START, end: date = AS_OF) -> pd.Series:
    idx = _bday_index(start, end)
    return pd.Series([level] * len(idx), index=idx, dtype="float64")


def _linear_path(
    start_level: float, daily_step: float, start: str = PATH_START, end: date = AS_OF
) -> pd.Series:
    """Price rising (or falling) by ``daily_step`` per business day."""
    idx = _bday_index(start, end)
    return pd.Series(start_level + daily_step * np.arange(len(idx)), index=idx, dtype="float64")


def _patch_helpers(
    monkeypatch: pytest.MonkeyPatch,
    ticker_path: pd.Series,
    spy_path: pd.Series,
    *,
    ticker_fid: int | None = TICKER_FID,
) -> dict[str, list[Any]]:
    """Patch the PIT plumbing on ``validation.backtest``; record the calls."""
    calls: dict[str, list[Any]] = {"load_price_path": [], "resolve_ticker": []}

    def fake_resolve_ticker(engine: Any, ticker: str) -> int | None:
        calls["resolve_ticker"].append(ticker)
        return ticker_fid

    def fake_resolve_spy(engine: Any) -> tuple[int, str]:
        return SPY_FID, "spy_full"

    def fake_load_price_path(
        engine: Any, feature_id: int, start: date, end: date, as_of: date
    ) -> pd.Series:
        calls["load_price_path"].append((feature_id, start, end, as_of))
        src = spy_path if feature_id == SPY_FID else ticker_path
        lo, hi = pd.Timestamp(start), pd.Timestamp(min(end, as_of))
        return src[(src.index >= lo) & (src.index <= hi)]

    def fake_feature_name(engine: Any, feature_id: int) -> str | None:
        return "tst_full"

    monkeypatch.setattr(vb, "_resolve_ticker_feature_id", fake_resolve_ticker)
    monkeypatch.setattr(vb, "resolve_spy_feature", fake_resolve_spy)
    monkeypatch.setattr(vb, "load_price_path", fake_load_price_path)
    monkeypatch.setattr(vb, "_feature_name_for_id", fake_feature_name)
    return calls


@pytest.fixture
def engine() -> MagicMock:
    eng = MagicMock(name="engine")
    return eng


# ─────────────────────────────────────────────────────────────────
# 1. Scored, positive alpha → pass
# ─────────────────────────────────────────────────────────────────


class TestScoredPass:
    ENTRIES = [
        date(2025, 7, 7),
        date(2025, 8, 4),
        date(2025, 9, 8),
        date(2025, 10, 6),
        date(2025, 11, 3),
        date(2025, 12, 1),
    ]
    HOLD = 60

    def test_pass_verdict_and_statistics(self, engine: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        # Ticker rises +1/day from 100; SPY flat at 500 → alpha = net return.
        ticker = _linear_path(100.0, 1.0)
        spy = _flat_path(500.0)
        calls = _patch_helpers(monkeypatch, ticker, spy)

        res = vb.run_hold_validation(engine, "tst", self.ENTRIES, self.HOLD, as_of=AS_OF)

        assert res["ticker"] == "TST"
        assert res["direction"] == "LONG"
        assert res["hold_days"] == self.HOLD
        assert res["cost_bps"] == 5.0
        assert res["as_of"] == AS_OF.isoformat()
        assert res["feature_name"] == "tst_full"
        assert res["spy_feature"] == "spy_full"

        assert res["n_scored"] == 6
        assert res["n_open"] == 0
        assert res["n_skipped"] == 0
        assert all(e["status"] == "scored" for e in res["entries"])

        # Hand-compute every entry to pin the arithmetic.
        expected_alphas: list[float] = []
        for e in res["entries"]:
            entry_d = date.fromisoformat(e["entry_date"])
            exit_d = date.fromisoformat(e["exit_date"])
            assert exit_d == entry_d + timedelta(days=self.HOLD)
            entry_px = vb.price_at(ticker, entry_d)
            exit_px = vb.price_at(ticker, exit_d)
            gross = exit_px / entry_px - 1.0
            net = gross - 0.001  # 2 × 5 bp
            assert e["entry_price"] == pytest.approx(entry_px)
            assert e["exit_price"] == pytest.approx(exit_px)
            assert e["gross_return"] == pytest.approx(gross)
            assert e["net_return"] == pytest.approx(net)
            assert e["spy_return"] == pytest.approx(0.0)
            assert e["alpha"] == pytest.approx(net)
            # holding_days comes from the SPY (business-day) calendar, not
            # the 60 calendar days.
            n_bdays = int(((spy.index > pd.Timestamp(entry_d)) & (spy.index <= pd.Timestamp(exit_d))).sum())
            assert e["holding_days"] == n_bdays
            assert 40 <= e["holding_days"] < self.HOLD
            expected_alphas.append(net)

        arr = np.asarray(expected_alphas)
        assert res["hit_rate"] == 1.0
        assert res["mean_alpha"] == pytest.approx(arr.mean())
        assert res["median_alpha"] == pytest.approx(np.median(arr))
        assert res["min_alpha"] == pytest.approx(arr.min())
        assert res["max_alpha"] == pytest.approx(arr.max())
        assert res["mean_net_return"] == pytest.approx(arr.mean())
        assert res["mean_spy_return"] == pytest.approx(0.0)
        expected_t = arr.mean() / (arr.std(ddof=1) / np.sqrt(len(arr)))
        assert res["alpha_t_stat"] == pytest.approx(expected_t)
        assert res["verdict"] == "pass"
        assert "6 scored" in res["verdict_reason"]

        # Paths loaded exactly once each, window = [min(entry) - 10d, as_of],
        # and as_of is the only vintage cut-off passed to the PIT reads.
        assert len(calls["load_price_path"]) == 2
        for _fid, start, end, as_of in calls["load_price_path"]:
            assert start == min(self.ENTRIES) - timedelta(days=10)
            assert end == AS_OF
            assert as_of == AS_OF
        assert {c[0] for c in calls["load_price_path"]} == {TICKER_FID, SPY_FID}
        assert calls["resolve_ticker"] == ["TST"]

    def test_fail_verdict_when_ticker_lags_spy(self, engine: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        ticker = _flat_path(100.0)
        spy = _linear_path(500.0, 1.0)
        _patch_helpers(monkeypatch, ticker, spy)
        res = vb.run_hold_validation(engine, "TST", self.ENTRIES, self.HOLD, as_of=AS_OF)
        assert res["n_scored"] == 6
        assert res["hit_rate"] == 0.0
        assert res["mean_alpha"] < 0
        assert res["mean_spy_return"] > 0
        assert res["verdict"] == "fail"

    def test_result_is_json_safe(self, engine: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        import json

        _patch_helpers(monkeypatch, _linear_path(100.0, 1.0), _flat_path(500.0))
        res = vb.run_hold_validation(engine, "TST", self.ENTRIES, self.HOLD, as_of=AS_OF)
        json.dumps(res)  # must not raise


# ─────────────────────────────────────────────────────────────────
# 2. Open entries (exit after as_of)
# ─────────────────────────────────────────────────────────────────


class TestOpenEntries:
    def test_open_marked_to_last_close_and_excluded(
        self, engine: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ticker = _linear_path(100.0, 1.0)
        spy = _flat_path(500.0)
        _patch_helpers(monkeypatch, ticker, spy)

        closed = [date(2025, 7, 7), date(2025, 8, 4), date(2025, 9, 8), date(2025, 10, 6), date(2025, 11, 3)]
        still_open = [date(2026, 8, 3), date(2026, 8, 24)]  # + 60d > 2026-09-10
        hold = 60

        res = vb.run_hold_validation(engine, "TST", closed + still_open, hold, as_of=AS_OF)

        assert res["n_scored"] == 5
        assert res["n_open"] == 2
        assert res["n_skipped"] == 0

        open_rows = [e for e in res["entries"] if e["status"] == "open"]
        assert len(open_rows) == 2
        last_close_date = ticker.index[-1].date()
        assert last_close_date <= AS_OF
        for e in open_rows:
            entry_d = date.fromisoformat(e["entry_date"])
            assert date.fromisoformat(e["exit_date"]) == entry_d + timedelta(days=hold)
            assert date.fromisoformat(e["exit_date"]) > AS_OF
            assert e["mark_date"] == last_close_date.isoformat()
            assert e["exit_price"] == pytest.approx(float(ticker.iloc[-1]))
            assert e["alpha"] is not None
            assert e["net_return"] is not None
            assert e["alpha"] == pytest.approx(
                float(ticker.iloc[-1]) / vb.price_at(ticker, entry_d) - 1.0 - 0.001
            )

        # Stats only over the 5 scored rows.
        scored_alphas = [e["alpha"] for e in res["entries"] if e["status"] == "scored"]
        assert len(scored_alphas) == 5
        assert res["mean_alpha"] == pytest.approx(np.mean(scored_alphas))
        assert res["hit_rate"] == 1.0
        assert res["verdict"] == "pass"

    def test_entry_after_as_of_is_skipped(self, engine: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_helpers(monkeypatch, _linear_path(100.0, 1.0), _flat_path(500.0))
        res = vb.run_hold_validation(engine, "TST", [AS_OF + timedelta(days=1)], 30, as_of=AS_OF)
        assert res["n_skipped"] == 1
        assert res["entries"][0]["status"] == "skipped"
        assert "after as_of" in res["entries"][0]["reason"]
        assert res["verdict"] == "insufficient"


# ─────────────────────────────────────────────────────────────────
# 3. Entry before the path starts → skipped, others scored
# ─────────────────────────────────────────────────────────────────


class TestSkipped:
    def test_pre_path_entry_skipped_others_scored(
        self, engine: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ticker = _linear_path(100.0, 1.0)  # starts 2025-06-02
        spy = _flat_path(500.0)
        _patch_helpers(monkeypatch, ticker, spy)

        too_early = date(2025, 1, 6)
        good = [date(2025, 7, 7), date(2025, 8, 4), date(2025, 9, 8), date(2025, 10, 6), date(2025, 11, 3)]
        res = vb.run_hold_validation(engine, "TST", [too_early] + good, 60, as_of=AS_OF)

        assert res["n_skipped"] == 1
        assert res["n_scored"] == 5
        bad = next(e for e in res["entries"] if e["entry_date"] == too_early.isoformat())
        assert bad["status"] == "skipped"
        assert "no observation on or before" in bad["reason"]
        assert bad["alpha"] is None
        assert res["verdict"] == "pass"

    def test_spy_gap_skips_only_that_entry(self, engine: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        # SPY path starts later than the ticker path → the first entry has
        # no benchmark and compute_trade_alpha raises; batch continues.
        ticker = _linear_path(100.0, 1.0)
        spy = _flat_path(500.0, start="2025-08-01")
        _patch_helpers(monkeypatch, ticker, spy)
        entries = [date(2025, 7, 7), date(2025, 9, 8), date(2025, 10, 6)]
        res = vb.run_hold_validation(engine, "TST", entries, 60, as_of=AS_OF)
        assert res["n_skipped"] == 1
        assert res["n_scored"] == 2
        assert res["entries"][0]["status"] == "skipped"

    def test_empty_ticker_path_skips_everything(self, engine: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        empty = pd.Series(dtype="float64", index=pd.DatetimeIndex([]))
        _patch_helpers(monkeypatch, empty, _flat_path(500.0))
        res = vb.run_hold_validation(engine, "TST", [date(2025, 7, 7), date(2025, 8, 4)], 30, as_of=AS_OF)
        assert res["n_skipped"] == 2
        assert res["n_scored"] == 0
        assert res["mean_alpha"] is None
        assert res["alpha_t_stat"] is None
        assert res["verdict"] == "insufficient"


# ─────────────────────────────────────────────────────────────────
# 4. n_scored < 5 → insufficient
# ─────────────────────────────────────────────────────────────────


class TestInsufficient:
    def test_four_scored_is_insufficient(self, engine: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_helpers(monkeypatch, _linear_path(100.0, 1.0), _flat_path(500.0))
        entries = [date(2025, 7, 7), date(2025, 8, 4), date(2025, 9, 8), date(2025, 10, 6)]
        res = vb.run_hold_validation(engine, "TST", entries, 60, as_of=AS_OF)
        assert res["n_scored"] == 4
        assert res["mean_alpha"] > 0
        assert res["hit_rate"] == 1.0
        assert res["verdict"] == "insufficient"
        assert "need at least 5" in res["verdict_reason"]
        assert res["alpha_t_stat"] is not None

    def test_single_entry_has_no_t_stat(self, engine: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_helpers(monkeypatch, _linear_path(100.0, 1.0), _flat_path(500.0))
        res = vb.run_hold_validation(engine, "TST", [date(2025, 7, 7)], 60, as_of=AS_OF)
        assert res["n_scored"] == 1
        assert res["alpha_t_stat"] is None
        assert res["median_alpha"] == pytest.approx(res["mean_alpha"])
        assert res["verdict"] == "insufficient"

    def test_zero_variance_has_no_t_stat(self, engine: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        # Flat ticker, flat SPY → every alpha is exactly -0.001 (cost drag).
        _patch_helpers(monkeypatch, _flat_path(100.0), _flat_path(500.0))
        entries = [date(2025, 7, 7), date(2025, 8, 4), date(2025, 9, 8), date(2025, 10, 6), date(2025, 11, 3)]
        res = vb.run_hold_validation(engine, "TST", entries, 60, as_of=AS_OF)
        assert res["n_scored"] == 5
        assert res["mean_alpha"] == pytest.approx(-0.001)
        assert res["alpha_t_stat"] is None
        assert res["hit_rate"] == 0.0
        assert res["verdict"] == "fail"

    def test_no_entries(self, engine: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        calls = _patch_helpers(monkeypatch, _flat_path(100.0), _flat_path(500.0))
        res = vb.run_hold_validation(engine, "TST", [], 60, as_of=AS_OF)
        assert res["entries"] == []
        assert res["n_scored"] == res["n_open"] == res["n_skipped"] == 0
        assert res["verdict"] == "insufficient"
        assert calls["load_price_path"] == []  # nothing to read


# ─────────────────────────────────────────────────────────────────
# 5. SHORT flips the sign
# ─────────────────────────────────────────────────────────────────


class TestShort:
    ENTRIES = [date(2025, 7, 7), date(2025, 8, 4), date(2025, 9, 8), date(2025, 10, 6), date(2025, 11, 3)]

    def test_short_flips_gross_and_alpha(self, engine: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        ticker = _linear_path(100.0, 1.0)
        spy = _flat_path(500.0)
        _patch_helpers(monkeypatch, ticker, spy)

        long_res = vb.run_hold_validation(engine, "TST", self.ENTRIES, 60, direction="LONG", as_of=AS_OF)
        short_res = vb.run_hold_validation(engine, "TST", self.ENTRIES, 60, direction="SHORT", as_of=AS_OF)

        assert short_res["direction"] == "SHORT"
        for l_row, s_row in zip(long_res["entries"], short_res["entries"]):
            assert s_row["gross_return"] == pytest.approx(-l_row["gross_return"])
            # net = gross - drag in both cases, so alpha_short = -gross - drag
            assert s_row["alpha"] == pytest.approx(-l_row["gross_return"] - 0.001)
        assert short_res["mean_alpha"] < 0
        assert short_res["hit_rate"] == 0.0
        assert short_res["verdict"] == "fail"
        assert long_res["verdict"] == "pass"

    def test_short_wins_on_falling_ticker(self, engine: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        ticker = _linear_path(500.0, -0.5)
        spy = _flat_path(500.0)
        _patch_helpers(monkeypatch, ticker, spy)
        res = vb.run_hold_validation(engine, "TST", self.ENTRIES, 60, direction="bearish", as_of=AS_OF)
        assert res["direction"] == "SHORT"
        assert res["hit_rate"] == 1.0
        assert res["verdict"] == "pass"

    def test_unknown_direction_raises(self, engine: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_helpers(monkeypatch, _flat_path(100.0), _flat_path(500.0))
        with pytest.raises(ValueError, match="direction"):
            vb.run_hold_validation(engine, "TST", self.ENTRIES, 60, direction="sideways", as_of=AS_OF)


# ─────────────────────────────────────────────────────────────────
# 6. Missing ticker feature → ValueError; input validation
# ─────────────────────────────────────────────────────────────────


class TestErrors:
    def test_missing_ticker_feature_raises(self, engine: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        calls = _patch_helpers(monkeypatch, _flat_path(100.0), _flat_path(500.0), ticker_fid=None)
        with pytest.raises(ValueError, match="no price feature for ticker"):
            vb.run_hold_validation(engine, "NOPE", [date(2025, 7, 7)], 30, as_of=AS_OF)
        assert calls["load_price_path"] == []

    def test_bad_hold_days_raises(self, engine: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_helpers(monkeypatch, _flat_path(100.0), _flat_path(500.0))
        with pytest.raises(ValueError, match="hold_days"):
            vb.run_hold_validation(engine, "TST", [date(2025, 7, 7)], 0, as_of=AS_OF)

    def test_spy_unavailable_raises_value_error(self, engine: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_helpers(monkeypatch, _flat_path(100.0), _flat_path(500.0))

        def no_spy(engine: Any) -> tuple[int, str]:
            raise LookupError("no SPY benchmark feature in feature_registry")

        monkeypatch.setattr(vb, "resolve_spy_feature", no_spy)
        with pytest.raises(ValueError, match="SPY benchmark unavailable"):
            vb.run_hold_validation(engine, "TST", [date(2025, 7, 7)], 30, as_of=AS_OF)

    def test_engine_only_touched_via_patched_helpers(
        self, engine: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_helpers(monkeypatch, _linear_path(100.0, 1.0), _flat_path(500.0))
        vb.run_hold_validation(engine, "TST", [date(2025, 7, 7)], 30, as_of=AS_OF)
        # No writes: never opens a transaction.
        engine.begin.assert_not_called()
        engine.connect.assert_not_called()


# ─────────────────────────────────────────────────────────────────
# Helpers: verdict + stats pure functions
# ─────────────────────────────────────────────────────────────────


class TestPureHelpers:
    @pytest.mark.parametrize(
        "n, mean, hit, expected",
        [
            (5, 0.02, 0.6, "pass"),
            (5, 0.02, 0.5, "pass"),
            (5, -0.01, 0.6, "fail"),
            (5, 0.02, 0.2, "fail"),
            (5, 0.02, 0.45, "insufficient"),  # grey zone: positive but 0.4 <= hr < 0.5
            (4, 0.02, 1.0, "insufficient"),
            (0, None, None, "insufficient"),
        ],
    )
    def test_verdict_rules(self, n: int, mean: float | None, hit: float | None, expected: str) -> None:
        verdict, reason = vb._hold_verdict(n, mean, hit)
        assert verdict == expected
        assert reason

    def test_stats_t_stat_formula(self) -> None:
        alphas = [0.01, 0.03, -0.02, 0.04, 0.02]
        s = vb._hold_stats(alphas)
        arr = np.asarray(alphas)
        assert s["hit_rate"] == pytest.approx(0.8)
        assert s["alpha_t_stat"] == pytest.approx(arr.mean() / (arr.std(ddof=1) / np.sqrt(5)))
        assert s["min_alpha"] == pytest.approx(-0.02)
        assert s["max_alpha"] == pytest.approx(0.04)

    def test_feature_name_lookup_uses_bound_param(self) -> None:
        eng = MagicMock()
        conn = MagicMock()
        eng.connect.return_value.__enter__ = MagicMock(return_value=conn)
        eng.connect.return_value.__exit__ = MagicMock(return_value=False)
        conn.execute.return_value.fetchone.return_value = ("aapl_full",)
        assert vb._feature_name_for_id(eng, 12) == "aapl_full"
        stmt, params = conn.execute.call_args[0]
        assert ":fid" in str(stmt)
        assert params == {"fid": 12}

    def test_feature_name_lookup_failure_returns_none(self) -> None:
        eng = MagicMock()
        eng.connect.side_effect = RuntimeError("db down")
        assert vb._feature_name_for_id(eng, 12) is None
