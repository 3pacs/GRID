"""Unit tests for alpha_research/data/split_adjuster.py.

Covers detect_splits, adjust_splits, adjust_panel, detect_panel_splits,
get_post_split_series (legacy alias), and compute_real_drawdown. The
multi-split compounding showcase (TSLA 5:1 in 2020 then 3:1 in 2022) is
the auditor-named target case from PUNCH-LIST-2026-05-13.md auditor
2026-06-07 alpha_research/ [P1] line 64.

Every price panel that flows through panel_builder.build_price_panel
runs through this module, so test coverage locks down the
forward-adjustment contract (current-day prices preserved; history
scaled down) and the multi-split compounding semantics.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from alpha_research.data.split_adjuster import (
    adjust_panel,
    adjust_splits,
    compute_real_drawdown,
    detect_panel_splits,
    detect_splits,
    get_post_split_series,
)


def _series(dates: list[str], values: list[float], name: str = "TEST") -> pd.Series:
    """Build a price series with DatetimeIndex matching the prod shape."""
    idx = pd.DatetimeIndex([pd.Timestamp(d) for d in dates])
    return pd.Series(values, index=idx, name=name, dtype=float)


# ── detect_splits ─────────────────────────────────────────────────────


class TestDetectSplits:
    def test_empty_returns_empty_list(self) -> None:
        assert detect_splits(pd.Series([], dtype=float)) == []

    def test_single_value_returns_empty_list(self) -> None:
        assert detect_splits(_series(["2024-01-02"], [100.0])) == []

    def test_no_split_returns_empty(self) -> None:
        prices = _series(
            ["2024-01-02", "2024-01-03", "2024-01-04"], [100.0, 101.0, 102.0]
        )
        assert detect_splits(prices) == []

    def test_simple_2to1_split(self) -> None:
        prices = _series(
            ["2024-01-02", "2024-01-03", "2024-01-04"], [100.0, 50.0, 51.0]
        )
        splits = detect_splits(prices)
        assert len(splits) == 1
        s = splits[0]
        assert s["ratio"] == 2
        assert s["pre_price"] == 100.0
        assert s["post_price"] == 50.0
        assert s["adjustment_factor"] == pytest.approx(0.5)
        assert s["date"] == pd.Timestamp("2024-01-03")

    def test_5to1_split(self) -> None:
        prices = _series(
            ["2020-08-28", "2020-08-31", "2020-09-01"], [500.0, 100.0, 102.0]
        )
        splits = detect_splits(prices)
        assert len(splits) == 1
        assert splits[0]["ratio"] == 5
        assert splits[0]["adjustment_factor"] == pytest.approx(0.2)

    def test_threshold_filters_normal_drawdowns(self) -> None:
        # 30% drop is not a split — below the default -40% threshold magnitude.
        prices = _series(
            ["2024-01-02", "2024-01-03", "2024-01-04"], [100.0, 70.0, 71.0]
        )
        assert detect_splits(prices) == []

    def test_ratio_below_2_not_a_split(self) -> None:
        # 45% drop is over the threshold but ratio rounds to 2 only at 50%.
        # round(100/55) = 2, but round(100/56) = 2 still. Use 60: round(100/60)=2.
        # round(100/65) = 2. Test ratio < 2 case: round(100/67) = 1 (no split).
        prices = _series(
            ["2024-01-02", "2024-01-03"],
            [100.0, 67.0],
        )
        # 33% drop is above -40% threshold magnitude (chg=-0.33 > -0.40) — filtered out.
        assert detect_splits(prices) == []

    def test_zero_post_price_skipped(self) -> None:
        prices = _series(
            ["2024-01-02", "2024-01-03", "2024-01-04"],
            [100.0, 0.0, 5.0],
        )
        # post=0 must not cause divide-by-zero
        assert detect_splits(prices) == []

    def test_multiple_splits_chronological_order(self) -> None:
        # TSLA-style: 5:1 then later 3:1
        prices = _series(
            [
                "2020-08-28",
                "2020-08-31",
                "2022-08-24",
                "2022-08-25",
            ],
            [500.0, 100.0, 900.0, 300.0],
        )
        splits = detect_splits(prices)
        assert [s["ratio"] for s in splits] == [5, 3]
        assert splits[0]["date"] == pd.Timestamp("2020-08-31")
        assert splits[1]["date"] == pd.Timestamp("2022-08-25")

    def test_custom_threshold(self) -> None:
        # With a less aggressive threshold (-0.25), a 30% drop with ratio>=2
        # would still need round(pre/post)>=2 which needs ~50%+ drop, so a pure
        # threshold-only event without the ratio still gets filtered.
        prices = _series(
            ["2024-01-02", "2024-01-03"], [100.0, 30.0]
        )
        splits = detect_splits(prices, threshold=-0.25)
        assert len(splits) == 1
        assert splits[0]["ratio"] == 3


# ── adjust_splits ─────────────────────────────────────────────────────


class TestAdjustSplits:
    def test_no_split_returns_copy(self) -> None:
        prices = _series(
            ["2024-01-02", "2024-01-03", "2024-01-04"], [100.0, 101.0, 102.0]
        )
        adjusted = adjust_splits(prices)
        pd.testing.assert_series_equal(adjusted, prices.astype(float))
        # Must be a copy — mutating it must not touch the original.
        adjusted.iloc[0] = 999.0
        assert prices.iloc[0] == 100.0

    def test_empty_input_returns_empty_copy(self) -> None:
        empty = pd.Series([], dtype=float)
        assert adjust_splits(empty).empty

    def test_single_value_returns_copy(self) -> None:
        prices = _series(["2024-01-02"], [100.0])
        out = adjust_splits(prices)
        assert out.iloc[0] == 100.0

    def test_simple_2to1_adjustment(self) -> None:
        # Pre-split $100 → adjusted to $50; post-split unchanged.
        prices = _series(
            ["2024-01-02", "2024-01-03", "2024-01-04"], [100.0, 50.0, 51.0]
        )
        adjusted = adjust_splits(prices)
        assert adjusted.iloc[0] == pytest.approx(50.0)  # pre-split $100 → $50
        assert adjusted.iloc[1] == pytest.approx(50.0)  # post-split unchanged
        assert adjusted.iloc[2] == pytest.approx(51.0)  # post-split unchanged

    def test_current_day_price_preserved(self) -> None:
        """Forward-adjustment contract: latest price is real market price."""
        prices = _series(
            ["2020-08-28", "2020-08-31", "2024-01-02"],
            [500.0, 100.0, 250.0],
        )
        adjusted = adjust_splits(prices)
        assert adjusted.iloc[-1] == pytest.approx(250.0)

    def test_tsla_5to1_then_3to1_compounding(self) -> None:
        """TSLA-style: 5:1 in 2020, then 3:1 in 2022.

        Pre-2020 history must be divided by 15 (compounded), 2020-2022
        history divided by 3, post-2022 unchanged. Auditor-named target
        case from PUNCH-LIST-2026-05-13.md alpha_research/ [P1] line 64.
        """
        # Pick pre-split prices that don't accidentally look like a 3rd split
        # (a >40% drop with ratio>=2 would be misdetected).
        prices = _series(
            [
                "2019-06-03",   # pre-both:  $400 → $26.67 (/15)
                "2020-08-28",   # pre-5:1:   $500 → $33.33 (/15)
                "2020-08-31",   # post-5:1:  $100 → $33.33 (/3)
                "2022-08-24",   # pre-3:1:   $900 → $300 (/3)
                "2022-08-25",   # post-3:1:  $300 → $300 unchanged
                "2024-06-01",   # current:   $200 → $200 unchanged
            ],
            [400.0, 500.0, 100.0, 900.0, 300.0, 200.0],
        )
        # Sanity: only the two real splits are detected, in chronological order.
        detected = detect_splits(prices)
        assert [s["ratio"] for s in detected] == [5, 3]

        adjusted = adjust_splits(prices)
        # Pre-both-splits: divided by 15 (5 × 3)
        assert adjusted.iloc[0] == pytest.approx(400.0 / 15.0)
        assert adjusted.iloc[1] == pytest.approx(500.0 / 15.0)
        # Between 5:1 and 3:1: divided by 3 only
        assert adjusted.iloc[2] == pytest.approx(100.0 / 3.0)
        assert adjusted.iloc[3] == pytest.approx(300.0)
        # Post-3:1: unchanged
        assert adjusted.iloc[4] == pytest.approx(300.0)
        assert adjusted.iloc[5] == pytest.approx(200.0)

    def test_compounding_uses_reverse_chronological_walk(self) -> None:
        # Three sequential splits — adjustment factor is the product.
        prices = _series(
            [
                "2020-01-02",
                "2020-06-01",
                "2021-06-01",
                "2022-06-01",
                "2023-06-01",
            ],
            [800.0, 200.0, 50.0, 10.0, 12.0],
        )
        adjusted = adjust_splits(prices)
        # 4:1, then 4:1, then 5:1. Earliest day adjusted by 4*4*5 = 80.
        assert adjusted.iloc[0] == pytest.approx(800.0 / 80.0)
        assert adjusted.iloc[1] == pytest.approx(200.0 / 20.0)
        assert adjusted.iloc[2] == pytest.approx(50.0 / 5.0)
        assert adjusted.iloc[3] == pytest.approx(10.0)
        assert adjusted.iloc[4] == pytest.approx(12.0)

    def test_does_not_mutate_input(self) -> None:
        prices = _series(
            ["2024-01-02", "2024-01-03"], [100.0, 50.0]
        )
        before = prices.copy()
        _ = adjust_splits(prices)
        pd.testing.assert_series_equal(prices, before)


# ── adjust_panel ──────────────────────────────────────────────────────


class TestAdjustPanel:
    def test_empty_panel(self) -> None:
        empty = pd.DataFrame()
        out = adjust_panel(empty)
        assert out.empty

    def test_per_column_independent_adjustment(self) -> None:
        idx = pd.DatetimeIndex(
            [pd.Timestamp(d) for d in ["2024-01-02", "2024-01-03", "2024-01-04"]]
        )
        panel = pd.DataFrame(
            {
                "SPLIT": [100.0, 50.0, 51.0],      # 2:1 split
                "NOSPLIT": [100.0, 101.0, 102.0],  # no split
            },
            index=idx,
        )
        out = adjust_panel(panel)
        # SPLIT col adjusted
        assert out["SPLIT"].iloc[0] == pytest.approx(50.0)
        assert out["SPLIT"].iloc[1] == pytest.approx(50.0)
        # NOSPLIT col untouched
        assert out["NOSPLIT"].iloc[0] == pytest.approx(100.0)
        assert out["NOSPLIT"].iloc[2] == pytest.approx(102.0)

    def test_does_not_mutate_input_panel(self) -> None:
        idx = pd.DatetimeIndex(
            [pd.Timestamp(d) for d in ["2024-01-02", "2024-01-03"]]
        )
        panel = pd.DataFrame({"X": [100.0, 50.0]}, index=idx)
        before = panel.copy()
        _ = adjust_panel(panel)
        pd.testing.assert_frame_equal(panel, before)

    def test_nan_column_skipped(self) -> None:
        idx = pd.DatetimeIndex(
            [pd.Timestamp(d) for d in ["2024-01-02", "2024-01-03", "2024-01-04"]]
        )
        panel = pd.DataFrame(
            {
                "GOOD": [100.0, 50.0, 51.0],
                "MOSTLY_NAN": [np.nan, np.nan, 42.0],  # only 1 valid → skip
            },
            index=idx,
        )
        out = adjust_panel(panel)
        # GOOD adjusted
        assert out["GOOD"].iloc[0] == pytest.approx(50.0)
        # MOSTLY_NAN left as-is (the single valid value is unchanged)
        assert out["MOSTLY_NAN"].iloc[2] == pytest.approx(42.0)


# ── detect_panel_splits ───────────────────────────────────────────────


class TestDetectPanelSplits:
    def test_returns_only_tickers_with_splits(self) -> None:
        idx = pd.DatetimeIndex(
            [pd.Timestamp(d) for d in ["2024-01-02", "2024-01-03", "2024-01-04"]]
        )
        panel = pd.DataFrame(
            {
                "SPLIT": [100.0, 50.0, 51.0],
                "NOSPLIT": [100.0, 101.0, 102.0],
            },
            index=idx,
        )
        result = detect_panel_splits(panel)
        assert set(result.keys()) == {"SPLIT"}
        assert len(result["SPLIT"]) == 1
        assert result["SPLIT"][0]["ratio"] == 2

    def test_empty_when_no_splits(self) -> None:
        idx = pd.DatetimeIndex(
            [pd.Timestamp(d) for d in ["2024-01-02", "2024-01-03"]]
        )
        panel = pd.DataFrame({"X": [100.0, 101.0]}, index=idx)
        assert detect_panel_splits(panel) == {}


# ── get_post_split_series (legacy alias) ──────────────────────────────


class TestGetPostSplitSeries:
    def test_matches_adjust_splits(self) -> None:
        prices = _series(
            ["2024-01-02", "2024-01-03", "2024-01-04"], [100.0, 50.0, 51.0]
        )
        legacy = get_post_split_series(prices)
        modern = adjust_splits(prices)
        pd.testing.assert_series_equal(legacy, modern)


# ── compute_real_drawdown ─────────────────────────────────────────────


class TestComputeRealDrawdown:
    def test_empty_returns_error(self) -> None:
        out = compute_real_drawdown(pd.Series([], dtype=float))
        assert out == {"error": "no data"}

    def test_monotonic_uptrend_no_drawdown(self) -> None:
        prices = _series(
            ["2024-01-02", "2024-01-03", "2024-01-04"], [100.0, 110.0, 120.0]
        )
        out = compute_real_drawdown(prices)
        assert out["current"] == pytest.approx(120.0)
        assert out["ath"] == pytest.approx(120.0)
        assert out["atl"] == pytest.approx(100.0)
        assert out["drawdown_pct"] == pytest.approx(0.0)
        assert out["has_split"] is False
        assert out["last_split_date"] is None
        assert out["last_split_ratio"] is None
        assert out["n_points"] == 3

    def test_drawdown_from_ath(self) -> None:
        prices = _series(
            ["2024-01-02", "2024-01-03", "2024-01-04"], [100.0, 150.0, 120.0]
        )
        out = compute_real_drawdown(prices)
        assert out["ath"] == pytest.approx(150.0)
        assert out["current"] == pytest.approx(120.0)
        assert out["drawdown_pct"] == pytest.approx(-20.0)

    def test_uses_split_adjusted_series(self) -> None:
        """Drawdown is computed on split-adjusted prices, not raw."""
        # Raw: 1000 → 200 looks like a -80% drawdown, but the split makes it flat.
        prices = _series(
            ["2024-01-02", "2024-01-03"], [1000.0, 200.0]
        )
        out = compute_real_drawdown(prices)
        # Both adjusted to 200 — no drawdown.
        assert out["ath"] == pytest.approx(200.0)
        assert out["current"] == pytest.approx(200.0)
        assert out["drawdown_pct"] == pytest.approx(0.0)
        assert out["has_split"] is True
        assert out["last_split_ratio"] == 5

    def test_has_split_metadata(self) -> None:
        prices = _series(
            ["2024-01-02", "2024-01-03", "2024-01-04"], [100.0, 50.0, 52.0]
        )
        out = compute_real_drawdown(prices)
        assert out["has_split"] is True
        assert out["last_split_date"] == pd.Timestamp("2024-01-03")
        assert out["last_split_ratio"] == 2

    def test_short_series_momentum_safe(self) -> None:
        # Series shorter than 5/30/90 days returns 0 for those windows.
        prices = _series(
            ["2024-01-02", "2024-01-03"], [100.0, 105.0]
        )
        out = compute_real_drawdown(prices)
        assert out["mom_5d"] == 0
        assert out["mom_30d"] == 0
        assert out["mom_90d"] == 0
