"""Tests for intelligence/cross_lens.py.

Covers:

    * resolve_price_series_id
    * compute_log_returns
    * lagged_correlation (happy path + perfect correlation at known lag)
    * detect_shock_events (stdev threshold + downstream window)
    * build_lagged_evidence / build_event_evidence narrative templates
    * upsert_attributions idempotency (uses an in-memory stub engine)
    * detect_attributions end-to-end with mocked fetch/pairs
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from intelligence.cross_lens import (
    Attribution,
    MIN_OBSERVATIONS,
    build_actor_narrative,
    build_event_evidence,
    build_lagged_evidence,
    compute_log_returns,
    detect_attributions,
    detect_shock_events,
    lagged_correlation,
    resolve_price_series_id,
    upsert_attributions,
)


# ── Resolution ────────────────────────────────────────────────────────────

class TestResolvePriceSeriesId:
    def test_known_commodity_slug(self):
        assert resolve_price_series_id("cocoa") == "YF:CC=F:close"
        assert resolve_price_series_id("Coffee") == "YF:KC=F:close"
        assert resolve_price_series_id("crude_oil") == "YF:CL=F:close"

    def test_ticker_like(self):
        assert resolve_price_series_id("hsy") == "YF:HSY:close"
        assert resolve_price_series_id("MDLZ") == "YF:MDLZ:close"

    def test_non_ticker_returns_none(self):
        # Country / descriptor slugs shouldn't yield a series id
        assert resolve_price_series_id("west_africa_region") is None

    def test_empty(self):
        assert resolve_price_series_id("") is None
        assert resolve_price_series_id(None) is None  # type: ignore[arg-type]


# ── Log returns ───────────────────────────────────────────────────────────

class TestComputeLogReturns:
    def test_basic(self):
        df = pd.DataFrame(
            {
                "obs_date": pd.date_range("2026-01-01", periods=4, freq="D"),
                "value": [100.0, 110.0, 121.0, 133.1],
            }
        )
        returns = compute_log_returns(df)
        assert len(returns) == 3
        # Constant growth factor -> constant log return
        assert np.allclose(returns.values, np.log(1.1), atol=1e-9)

    def test_empty(self):
        assert compute_log_returns(pd.DataFrame(columns=["obs_date", "value"])).empty

    def test_single_row(self):
        df = pd.DataFrame({"obs_date": [pd.Timestamp("2026-01-01")], "value": [100.0]})
        assert compute_log_returns(df).empty


# ── Lagged correlation ────────────────────────────────────────────────────

class TestLaggedCorrelation:
    def test_perfect_correlation_at_known_lag(self):
        n = 80
        rng = np.random.default_rng(42)
        up_vals = rng.normal(0, 0.02, n)
        up_idx = pd.date_range("2026-01-01", periods=n, freq="D")
        up = pd.Series(up_vals, index=up_idx)

        # Downstream: negative-sign copy of upstream, shifted by +3 days
        down_vals = np.full(n, np.nan)
        down_vals[3:] = -up_vals[:-3]
        down = pd.Series(down_vals, index=up_idx).dropna()

        corr, lag, n_obs = lagged_correlation(up, down, (1, 10))
        assert lag == 3
        assert corr < -0.95
        assert n_obs >= MIN_OBSERVATIONS

    def test_no_relationship(self):
        rng = np.random.default_rng(7)
        up = pd.Series(
            rng.normal(0, 0.02, 100),
            index=pd.date_range("2026-01-01", periods=100, freq="D"),
        )
        down = pd.Series(
            rng.normal(0, 0.02, 100),
            index=pd.date_range("2026-01-01", periods=100, freq="D"),
        )
        corr, _lag, _n = lagged_correlation(up, down, (1, 5))
        assert abs(corr) < 0.4

    def test_empty_inputs(self):
        empty = pd.Series(dtype=float)
        corr, lag, n = lagged_correlation(empty, empty, (1, 5))
        assert (corr, lag, n) == (0.0, 0, 0)

    def test_invalid_lag_window(self):
        s = pd.Series(
            [0.01, -0.01, 0.02],
            index=pd.date_range("2026-01-01", periods=3, freq="D"),
        )
        with pytest.raises(ValueError):
            lagged_correlation(s, s, (5, 1))


# ── Event-study shock detection ───────────────────────────────────────────

class TestDetectShockEvents:
    def test_finds_stdev_shock_and_measures_downstream(self):
        # 30 days of small moves, then a big shock on day 20.
        n = 30
        idx = pd.date_range("2026-01-01", periods=n, freq="D")
        up_vals = np.full(n, 0.001)
        up_vals[20] = 0.08  # 8% shock, far above 1-sigma
        up_returns = pd.Series(up_vals, index=idx)

        # Downstream: price drifts slowly down after day 20
        down_prices = np.linspace(100, 99, n)
        down_prices[20:] = np.linspace(99, 95, n - 20)
        down_df = pd.DataFrame({"obs_date": idx, "value": down_prices})

        events = detect_shock_events(
            up_returns, down_df, window_days=5, stdev_threshold=1.0
        )
        assert events, "expected at least one shock event"
        top = events[0]
        assert top["shock_date"] == idx[20].date()
        assert top["shock_magnitude"] == pytest.approx(0.08, rel=1e-6)
        # Downstream went down, so log-return should be negative
        assert top["downstream_move_pct"] < 0

    def test_no_shocks_in_calm_series(self):
        # Zero variance -> sample_std == 0 -> function guards and returns []
        idx = pd.date_range("2026-01-01", periods=30, freq="D")
        up_returns = pd.Series(np.zeros(30), index=idx)
        down_df = pd.DataFrame({"obs_date": idx, "value": np.full(30, 100.0)})
        assert detect_shock_events(up_returns, down_df) == []

    def test_sub_threshold_noise_has_no_events(self):
        # Alternating tiny moves: none should exceed 1-sigma
        rng = np.random.default_rng(0)
        idx = pd.date_range("2026-01-01", periods=60, freq="D")
        up_returns = pd.Series(rng.normal(0, 0.01, 60), index=idx)
        down_df = pd.DataFrame({"obs_date": idx, "value": np.full(60, 100.0)})
        # With stdev_threshold=5 (way above any sample) we should get zero events
        assert detect_shock_events(
            up_returns, down_df, stdev_threshold=5.0
        ) == []


# ── Narrative templates ──────────────────────────────────────────────────

class TestNarrativeTemplates:
    def test_lagged_evidence_inverse(self):
        msg = build_lagged_evidence(
            "cocoa", "HSY", correlation=-0.72, lag=4, n_obs=120, pct_cogs=0.15
        )
        assert "cocoa" in msg and "HSY" in msg
        assert "inverse" in msg
        assert "-0.72" in msg
        assert "4-day" in msg
        assert "15% of HSY COGS" in msg

    def test_event_evidence(self):
        msg = build_event_evidence(
            "cocoa", "HSY", shock_magnitude=0.08, downstream_move=-0.03,
            window_days=5, pct_cogs=0.20,
        )
        assert "+8.0%" in msg
        assert "-3.0%" in msg
        assert "5 trading days" in msg

    def test_actor_narrative_empty(self):
        assert "No historical" in build_actor_narrative([])

    def test_actor_narrative_picks_strongest(self):
        rows = [
            {"upstream_id": "cocoa", "downstream_id": "HSY",
             "correlation": -0.72, "lag_days": 4},
            {"upstream_id": "sugar", "downstream_id": "HSY",
             "correlation": -0.30, "lag_days": 2},
        ]
        msg = build_actor_narrative(rows)
        assert "cocoa -> HSY" in msg
        assert "-0.72" in msg


# ── Upsert idempotency (mocked engine) ────────────────────────────────────

class TestUpsertIdempotency:
    def test_upsert_issues_one_stmt_per_row(self):
        attrs = [
            Attribution(
                upstream_id="cocoa",
                downstream_id="HSY",
                shock_date=date(2026, 2, 10),
                shock_magnitude=0.08,
                downstream_move_pct=-0.03,
                lag_days=5,
                correlation=None,
                confidence="inferred",
                evidence="x",
                method="event_study",
            ),
            Attribution(
                upstream_id="cocoa",
                downstream_id="MDLZ",
                shock_date=date(2026, 2, 10),
                shock_magnitude=0.08,
                downstream_move_pct=-0.01,
                lag_days=5,
                correlation=None,
                confidence="inferred",
                evidence="y",
                method="event_study",
            ),
        ]
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        mock_engine.begin.return_value.__exit__.return_value = False

        n = upsert_attributions(mock_engine, attrs)
        assert n == 2
        assert mock_conn.execute.call_count == 2

    def test_upsert_empty_is_noop(self):
        mock_engine = MagicMock()
        assert upsert_attributions(mock_engine, []) == 0
        mock_engine.begin.assert_not_called()


# ── End-to-end with mocked deps ───────────────────────────────────────────

class TestDetectAttributionsE2E:
    def test_runs_and_writes_when_correlation_strong(self):
        # Build a synthetic upstream and downstream with strong negative
        # correlation at a 2-day lag.
        n = 100
        idx = pd.date_range("2026-01-01", periods=n, freq="D")
        rng = np.random.default_rng(1)
        up_returns = rng.normal(0, 0.02, n)
        up_prices = 100 * np.exp(np.cumsum(up_returns))
        up_df = pd.DataFrame({"obs_date": idx, "value": up_prices})

        # Downstream log returns = -upstream_returns shifted by 2 days
        down_returns = np.zeros(n)
        down_returns[2:] = -up_returns[:-2]
        down_prices = 50 * np.exp(np.cumsum(down_returns))
        down_df = pd.DataFrame({"obs_date": idx, "value": down_prices})

        def fake_fetch(engine, series_id, lookback_days):
            if series_id.startswith("YF:CC=F"):
                return up_df
            if series_id.startswith("YF:HSY"):
                return down_df
            return pd.DataFrame(columns=["obs_date", "value"])

        with patch(
            "intelligence.cross_lens.list_candidate_pairs",
            return_value=[("cocoa", "HSY", 0.15)],
        ), patch(
            "intelligence.cross_lens.fetch_close_series",
            side_effect=fake_fetch,
        ), patch(
            "intelligence.cross_lens.upsert_attributions",
            return_value=0,
        ) as mock_upsert:
            rows = detect_attributions(
                engine=MagicMock(),
                lookback_days=180,
                min_correlation=0.5,
                lag_window=(1, 5),
            )

        # Should have at least the lagged_correlation row
        methods = {r["method"] for r in rows}
        assert "lagged_correlation" in methods
        # upsert called once with the list
        assert mock_upsert.call_count == 1
