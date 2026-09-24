"""Unit tests for alpha_research/signals/credit_cycle.py and exposure_scaler.py.

Both functions drive oracle/engine.py::_get_credit_cycle_routing family-weight
multipliers (see PUNCH-LIST-2026-05-13.md auditor 2026-06-07 alpha_research/
[P1] line 62). Tests cover the contraction/expansion bucketing in
compute_credit_cycle and the calm/elevated/stressed bucketing in
compute_vix_exposure_scalar with deterministic synthetic series.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
import pytest

from alpha_research.signals.credit_cycle import (
    HY_SPREAD_FEATURE_ID,
    M2_FEATURE_ID,
    TREND_WINDOW_DAYS,
    compute_credit_cycle,
)
from alpha_research.signals import exposure_scaler
from alpha_research.signals.exposure_scaler import compute_vix_exposure_scalar


# ── Fake Engine ───────────────────────────────────────────────────────


class _FakeResult:
    def __init__(self, rows: list[tuple[Any, Any]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple[Any, Any]]:
        return self._rows


class _FakeConn:
    def __init__(self, engine: "_FakeEngine") -> None:
        self._engine = engine

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False

    def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> _FakeResult:
        params = params or {}
        fid = params.get("fid")
        rows = self._engine.feature_rows.get(fid, [])
        self._engine.calls.append({"fid": fid, "params": dict(params)})
        return _FakeResult(rows)


class _FakeEngine:
    """Returns canned (obs_date, value) rows keyed by feature_id."""

    def __init__(self, feature_rows: dict[int, list[tuple[Any, float]]]) -> None:
        self.feature_rows = feature_rows
        self.calls: list[dict[str, Any]] = []

    def connect(self) -> _FakeConn:
        return _FakeConn(self)


def _date_rows(start: date, values: list[float]) -> list[tuple[date, float]]:
    """Generate sequential (date, value) rows starting at ``start``."""
    from datetime import timedelta

    return [(start + timedelta(days=i), v) for i, v in enumerate(values)]


# ── compute_credit_cycle ───────────────────────────────────────────────


class TestComputeCreditCycle:
    def test_returns_expansion_default_on_empty(self) -> None:
        engine = _FakeEngine({})
        result = compute_credit_cycle(engine, as_of_date=date(2026, 6, 15))
        assert result["state"] == "expansion"
        assert result["confidence"] == 0.0
        assert result["hy_spread_trend"] is None
        assert result["m2_trend"] is None
        assert result["error"] == "insufficient data"
        assert result["signal_families"]["prefer"] == ["equity", "flows", "earnings"]

    def test_contraction_widening_hy_and_falling_m2(self) -> None:
        """HY spread rising 50% + M2 falling 5% → contraction state."""
        as_of = date(2026, 6, 15)
        # 200 rising values (HY widening)
        hy_rows = _date_rows(date(2026, 1, 1), [3.0 + 0.01 * i for i in range(200)])
        # 200 falling values (M2 contracting)
        m2_rows = _date_rows(date(2026, 1, 1), [20000.0 - 5.0 * i for i in range(200)])
        engine = _FakeEngine({HY_SPREAD_FEATURE_ID: hy_rows, M2_FEATURE_ID: m2_rows})

        result = compute_credit_cycle(engine, as_of_date=as_of)

        assert result["state"] == "contraction"
        assert result["confidence"] > 0.0
        assert result["hy_spread_trend"] > 0
        assert result["m2_trend"] < 0
        assert result["signal_families"]["prefer"] == ["vol", "alternative"]
        assert "avoid" in result["signal_families"]

    def test_expansion_falling_hy_and_rising_m2(self) -> None:
        """HY spread tightening + M2 growing → expansion state."""
        as_of = date(2026, 6, 15)
        hy_rows = _date_rows(date(2026, 1, 1), [5.0 - 0.01 * i for i in range(200)])
        m2_rows = _date_rows(date(2026, 1, 1), [18000.0 + 10.0 * i for i in range(200)])
        engine = _FakeEngine({HY_SPREAD_FEATURE_ID: hy_rows, M2_FEATURE_ID: m2_rows})

        result = compute_credit_cycle(engine, as_of_date=as_of)

        assert result["state"] == "expansion"
        assert result["hy_spread_trend"] < 0
        assert result["m2_trend"] > 0
        assert result["signal_families"]["prefer"] == ["equity", "flows", "earnings"]

    def test_only_one_signal_available(self) -> None:
        """When only HY has enough data, the single-signal vote still resolves a state."""
        as_of = date(2026, 6, 15)
        hy_rows = _date_rows(date(2026, 1, 1), [3.0 + 0.01 * i for i in range(200)])
        engine = _FakeEngine({HY_SPREAD_FEATURE_ID: hy_rows, M2_FEATURE_ID: []})

        result = compute_credit_cycle(engine, as_of_date=as_of)

        assert result["state"] == "contraction"
        assert result["hy_spread_trend"] is not None
        assert result["m2_trend"] is None

    def test_below_min_data_falls_through_to_default(self) -> None:
        """If both series have < TREND_WINDOW_DAYS // 2 rows, neither signal counts."""
        as_of = date(2026, 6, 15)
        too_few = TREND_WINDOW_DAYS // 2 - 1
        hy_rows = _date_rows(date(2026, 1, 1), [3.0] * too_few)
        m2_rows = _date_rows(date(2026, 1, 1), [20000.0] * too_few)
        engine = _FakeEngine({HY_SPREAD_FEATURE_ID: hy_rows, M2_FEATURE_ID: m2_rows})

        result = compute_credit_cycle(engine, as_of_date=as_of)

        assert result["state"] == "expansion"
        assert result["confidence"] == 0.0
        assert result.get("error") == "insufficient data"

    def test_default_as_of_uses_today(self) -> None:
        """When as_of_date=None, the function should still execute without raising."""
        engine = _FakeEngine({})
        result = compute_credit_cycle(engine, as_of_date=None)
        assert result["state"] == "expansion"

    def test_confidence_is_clamped_to_unit_interval(self) -> None:
        """confidence ∈ [0, 1] even when underlying signals are extreme."""
        as_of = date(2026, 6, 15)
        # Extreme HY spread move (10x)
        hy_rows = _date_rows(date(2026, 1, 1), [1.0 + 0.1 * i for i in range(200)])
        m2_rows = _date_rows(date(2026, 1, 1), [20000.0 - 50.0 * i for i in range(200)])
        engine = _FakeEngine({HY_SPREAD_FEATURE_ID: hy_rows, M2_FEATURE_ID: m2_rows})

        result = compute_credit_cycle(engine, as_of_date=as_of)
        assert 0.0 <= result["confidence"] <= 1.0


# ── compute_vix_exposure_scalar ───────────────────────────────────────
#
# 2026-09-24 rotation-inputs fix: compute_vix_exposure_scalar used to query a
# hardcoded VIX_FEATURE_ID=105 directly (dead since 2026-04-02). It now reads
# through panel_builder.get_vix_series — the same PIT-correct, exact-name
# ("vix_spot") source adaptive_rotation.py uses (VERIFIED FACTS 1 & 2). These
# tests monkeypatch exposure_scaler.get_vix_series with a synthetic pd.Series
# rather than poking a fake DB engine — get_vix_series's own DB-query
# correctness (no vvix mixing, deterministic latest-vintage) is covered in
# tests/test_panel_builder.py.


def _vix_series(values: list[float], end_date: date) -> pd.Series:
    idx = pd.date_range(end=pd.Timestamp(end_date), periods=len(values), freq="D")
    return pd.Series(values, index=idx, name="VIX")


class TestComputeVixExposureScalar:
    def test_insufficient_data_returns_full_exposure(self, monkeypatch) -> None:
        """Fewer than MA_WINDOW rows → scalar=1.0 with unknown regime."""
        monkeypatch.setattr(
            exposure_scaler, "get_vix_series",
            lambda *a, **kw: _vix_series([18.0] * 5, date(2026, 6, 15)),
        )
        result = compute_vix_exposure_scalar(object(), as_of_date=date(2026, 6, 15))
        assert result["scalar"] == 1.0
        assert result["regime_hint"] == "unknown"
        assert result["vix"] is None
        assert "error" in result

    def test_calm_regime_below_moving_average(self, monkeypatch) -> None:
        """Current VIX < MA → ratio < 1.0 → scalar capped at 1.0, regime calm."""
        # 20 days at 20, then current VIX dropped to 14 → MA still ~20, ratio < 1
        values = [20.0] * 20 + [14.0]
        monkeypatch.setattr(
            exposure_scaler, "get_vix_series",
            lambda *a, **kw: _vix_series(values, date(2026, 6, 15)),
        )
        result = compute_vix_exposure_scalar(object(), as_of_date=date(2026, 6, 15))
        assert result["regime_hint"] == "calm"
        assert result["scalar"] == pytest.approx(1.0)
        assert result["ratio"] < 1.0
        assert result["vix"] == pytest.approx(14.0)

    def test_elevated_regime_between_1_and_1_3(self, monkeypatch) -> None:
        """1.0 <= ratio < 1.3 → elevated."""
        # 20 days at 20 then current 23 → ratio ≈ 1.15
        values = [20.0] * 20 + [23.0]
        monkeypatch.setattr(
            exposure_scaler, "get_vix_series",
            lambda *a, **kw: _vix_series(values, date(2026, 6, 15)),
        )
        result = compute_vix_exposure_scalar(object(), as_of_date=date(2026, 6, 15))
        assert result["regime_hint"] == "elevated"
        assert 1.0 <= result["ratio"] < 1.3
        # scalar = clip(1 - (ratio-1), 0, 1)
        assert 0.0 < result["scalar"] < 1.0

    def test_stressed_regime_above_1_3(self, monkeypatch) -> None:
        """ratio >= 1.3 → stressed, scalar drops sharply."""
        # 20 days at 20, current 30 → ratio ≈ 1.5
        values = [20.0] * 20 + [30.0]
        monkeypatch.setattr(
            exposure_scaler, "get_vix_series",
            lambda *a, **kw: _vix_series(values, date(2026, 6, 15)),
        )
        result = compute_vix_exposure_scalar(object(), as_of_date=date(2026, 6, 15))
        assert result["regime_hint"] == "stressed"
        assert result["ratio"] >= 1.3
        assert result["scalar"] < 0.6

    def test_scalar_clamped_to_zero_when_vix_doubles_ma(self, monkeypatch) -> None:
        """When ratio >= 2.0, the linear formula yields a clipped 0.0 scalar."""
        values = [20.0] * 20 + [50.0]
        monkeypatch.setattr(
            exposure_scaler, "get_vix_series",
            lambda *a, **kw: _vix_series(values, date(2026, 6, 15)),
        )
        result = compute_vix_exposure_scalar(object(), as_of_date=date(2026, 6, 15))
        assert result["scalar"] == 0.0
        assert result["regime_hint"] == "stressed"

    def test_zero_vix_ma_returns_safe_default(self, monkeypatch) -> None:
        """vix_ma <= 0 (degenerate data) → scalar 1.0 with unknown regime."""
        values = [0.0] * 20 + [0.0]
        monkeypatch.setattr(
            exposure_scaler, "get_vix_series",
            lambda *a, **kw: _vix_series(values, date(2026, 6, 15)),
        )
        result = compute_vix_exposure_scalar(object(), as_of_date=date(2026, 6, 15))
        assert result["scalar"] == 1.0
        assert result["regime_hint"] == "unknown"
        assert result["vix_ma"] == 0
        assert result["ratio"] is None

    def test_default_as_of_uses_today(self, monkeypatch) -> None:
        monkeypatch.setattr(
            exposure_scaler, "get_vix_series",
            lambda *a, **kw: pd.Series(dtype=float, name="VIX"),
        )
        result = compute_vix_exposure_scalar(object(), as_of_date=None)
        assert result["scalar"] == 1.0

    def test_no_20_0_fabrication_when_series_is_empty(self, monkeypatch) -> None:
        """VERIFIED FACT 2: an empty/insufficient VIX read must never be
        papered over with a fabricated 20.0 — it returns the honest
        'insufficient data' payload with vix=None."""
        monkeypatch.setattr(
            exposure_scaler, "get_vix_series",
            lambda *a, **kw: pd.Series(dtype=float, name="VIX"),
        )
        result = compute_vix_exposure_scalar(object(), as_of_date=date(2026, 6, 15))
        assert result["vix"] is None
        assert result["vix"] != 20.0
        assert "error" in result

    def test_passes_as_of_date_through_pit_correctly(self, monkeypatch) -> None:
        """as_of_date must reach get_vix_series as both the end_date and the
        as_of_date bound (PIT correctness end-to-end)."""
        as_of = date(2026, 6, 15)
        captured = {}

        def fake_get_vix_series(engine, start_date=None, end_date=None, as_of_date=None):
            captured["start_date"] = start_date
            captured["end_date"] = end_date
            captured["as_of_date"] = as_of_date
            return _vix_series([20.0] * 21, as_of)

        monkeypatch.setattr(exposure_scaler, "get_vix_series", fake_get_vix_series)
        compute_vix_exposure_scalar(object(), as_of_date=as_of)

        assert captured["end_date"] == as_of
        assert captured["as_of_date"] == as_of

    def test_custom_ma_window(self, monkeypatch) -> None:
        """Passing a smaller ma_window reduces the rows-needed gate."""
        values = [20.0] * 5 + [15.0]
        monkeypatch.setattr(
            exposure_scaler, "get_vix_series",
            lambda *a, **kw: _vix_series(values, date(2026, 6, 15)),
        )
        result = compute_vix_exposure_scalar(
            object(), as_of_date=date(2026, 6, 15), ma_window=5
        )
        assert result["regime_hint"] == "calm"
        assert result["scalar"] == pytest.approx(1.0)


# ── PIT correctness: credit_cycle queries also bind release_date ──────


class TestCreditCyclePitBinding:
    def test_both_feature_queries_bind_as_of(self) -> None:
        as_of = date(2026, 6, 15)
        engine = _FakeEngine({HY_SPREAD_FEATURE_ID: [], M2_FEATURE_ID: []})
        compute_credit_cycle(engine, as_of_date=as_of)
        # both HY and M2 queries fired with as_of=:end (the function passes end as as_of)
        fids = {c["fid"] for c in engine.calls}
        assert fids == {HY_SPREAD_FEATURE_ID, M2_FEATURE_ID}
        for c in engine.calls:
            assert c["params"]["as_of"] == as_of
