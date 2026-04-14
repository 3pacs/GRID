"""
Tests for intelligence/pattern_library.py — analog matching base-rate engine.

Covers:
- Pure math helpers (cosine similarity, normalizers, percentiles)
- compute_base_rate classification + percentile sanity
- find_nearest_analogs ordering + identity
- confidence_signal_from_base_rates dampening curve
- build_state_vector with mocked engine (all-None and partial coverage)
- build_pattern_match_report happy path + insufficient-analogs path
- Frozen dataclass to_dict roundtrip
"""

from __future__ import annotations

import math
from datetime import date, timedelta
from unittest.mock import MagicMock

import pandas as pd
import pytest

from intelligence.pattern_library import (
    BaseRateDistribution,
    CONFIDENCE_SATURATION_COUNT,
    DEFAULT_K_NEAREST,
    DEFAULT_OUTCOME_THRESHOLD_PCT,
    FORWARD_HORIZONS_DAYS,
    HistoricalAnalog,
    LIQUIDITY_STATE_ORDERING,
    LOOKBACK_DAYS_DEFAULT,
    MIN_ANALOGS_FOR_BASE_RATE,
    MarketStateVector,
    PatternMatchReport,
    STATE_VECTOR_DIM,
    STATE_VECTOR_FEATURES,
    build_pattern_match_report,
    build_state_vector,
    compute_base_rate,
    confidence_signal_from_base_rates,
    cosine_similarity,
    find_nearest_analogs,
    normalize_clamp_div,
    normalize_minmax,
    normalize_ordinal,
    normalize_zscore,
)


# ── Cosine similarity ─────────────────────────────────────────────────────

class TestCosineSimilarity:
    def test_identical(self):
        v = (1.0, 2.0, 3.0, 4.0)
        assert cosine_similarity(v, v) == pytest.approx(1.0, abs=1e-9)

    def test_orthogonal(self):
        a = (1.0, 0.0)
        b = (0.0, 1.0)
        assert cosine_similarity(a, b) == pytest.approx(0.0, abs=1e-9)

    def test_opposite(self):
        a = (1.0, 2.0, 3.0)
        b = (-1.0, -2.0, -3.0)
        assert cosine_similarity(a, b) == pytest.approx(-1.0, abs=1e-9)

    def test_zero_norm(self):
        a = (0.0, 0.0, 0.0)
        b = (1.0, 1.0, 1.0)
        assert cosine_similarity(a, b) == 0.0

    def test_length_mismatch(self):
        assert cosine_similarity((1.0, 2.0), (1.0, 2.0, 3.0)) == 0.0


# ── Normalizers ───────────────────────────────────────────────────────────

class TestNormalizeZscore:
    def test_none_returns_zero(self):
        assert normalize_zscore(None, mean=0.0, std=1.0) == 0.0

    def test_clamping_high(self):
        # value 10 sigma above mean should clamp to +1 (clamp/clamp)
        assert normalize_zscore(10.0, mean=0.0, std=1.0, clamp=3.0) == pytest.approx(1.0)

    def test_clamping_low(self):
        assert normalize_zscore(-10.0, mean=0.0, std=1.0, clamp=3.0) == pytest.approx(-1.0)

    def test_zero_std(self):
        assert normalize_zscore(5.0, mean=0.0, std=0.0) == 0.0

    def test_one_sigma(self):
        # 1 sigma above mean with clamp 3 -> 1/3
        assert normalize_zscore(1.0, mean=0.0, std=1.0, clamp=3.0) == pytest.approx(1.0 / 3.0)


class TestNormalizeMinmax:
    def test_none_returns_half(self):
        assert normalize_minmax(None, lo=0.0, hi=1.0) == 0.5

    def test_below_lo(self):
        assert normalize_minmax(-5.0, lo=0.0, hi=1.0) == 0.0

    def test_above_hi(self):
        assert normalize_minmax(99.0, lo=0.0, hi=1.0) == 1.0

    def test_midpoint(self):
        assert normalize_minmax(0.5, lo=0.0, hi=1.0) == pytest.approx(0.5)


class TestNormalizeOrdinal:
    def test_known_key(self):
        assert normalize_ordinal("EXPANSION", LIQUIDITY_STATE_ORDERING, scale=2) == pytest.approx(0.5)
        assert normalize_ordinal("CRISIS", LIQUIDITY_STATE_ORDERING, scale=2) == pytest.approx(-1.0)

    def test_unknown_key(self):
        assert normalize_ordinal("MYSTERY", LIQUIDITY_STATE_ORDERING, scale=2) == 0.0

    def test_none(self):
        assert normalize_ordinal(None, LIQUIDITY_STATE_ORDERING, scale=2) == 0.0


class TestNormalizeClampDiv:
    def test_none(self):
        assert normalize_clamp_div(None, lo=0.0, hi=5.0, scale=5.0) == 0.0

    def test_clamps_then_divides(self):
        assert normalize_clamp_div(10.0, lo=0.0, hi=5.0, scale=5.0) == pytest.approx(1.0)
        assert normalize_clamp_div(2.5, lo=0.0, hi=5.0, scale=5.0) == pytest.approx(0.5)


# ── compute_base_rate ─────────────────────────────────────────────────────

class TestComputeBaseRate:
    def test_60_pct_wins(self):
        # 60 returns of +2%, 40 returns of -2% -> 60% wins, 40% losses
        returns = [2.0] * 60 + [-2.0] * 40
        dist = compute_base_rate(returns, threshold_pct=0.5, horizon_days=7)
        assert dist.win_pct == pytest.approx(0.6)
        assert dist.loss_pct == pytest.approx(0.4)
        assert dist.flat_pct == pytest.approx(0.0)
        assert dist.n_analogs == 100
        assert dist.sufficient_sample is True

    def test_empty(self):
        dist = compute_base_rate([], horizon_days=7)
        assert dist.n_analogs == 0
        assert dist.sufficient_sample is False
        assert dist.win_pct == 0.0

    def test_percentile_sanity(self):
        # 21 returns from -10 to +10 in steps of 1
        returns = [float(x) for x in range(-10, 11)]
        dist = compute_base_rate(returns, threshold_pct=0.5, horizon_days=7)
        assert dist.p05_return_pct < dist.median_return_pct < dist.p95_return_pct
        assert dist.median_return_pct == pytest.approx(0.0, abs=1e-9)

    def test_insufficient_sample_below_min(self):
        returns = [1.0] * (MIN_ANALOGS_FOR_BASE_RATE - 1)
        dist = compute_base_rate(returns, horizon_days=7)
        assert dist.sufficient_sample is False

    def test_nan_handled_as_flat(self):
        returns = [float("nan"), float("nan"), 2.0, -2.0]
        dist = compute_base_rate(returns, threshold_pct=0.5, horizon_days=7)
        # NaN counted as flat for the tally
        assert dist.flat_pct == pytest.approx(0.5)
        assert dist.n_analogs == 4


# ── find_nearest_analogs ──────────────────────────────────────────────────

class TestFindNearestAnalogs:
    def test_query_matches_one_exactly(self):
        history = [
            (date(2024, 1, 1), (1.0, 0.0, 0.0)),
            (date(2024, 1, 2), (0.0, 1.0, 0.0)),
            (date(2024, 1, 3), (1.0, 0.0, 0.0)),  # identical to query
        ]
        result = find_nearest_analogs((1.0, 0.0, 0.0), history, k=3)
        assert result[0][1] == pytest.approx(1.0)

    def test_returns_k_sorted_desc(self):
        history = [
            (date(2024, 1, i + 1), (float(i), 1.0, 0.0)) for i in range(10)
        ]
        result = find_nearest_analogs((5.0, 1.0, 0.0), history, k=3)
        assert len(result) == 3
        sims = [s for _, s in result]
        assert sims == sorted(sims, reverse=True)

    def test_empty_history(self):
        assert find_nearest_analogs((1.0, 0.0), [], k=5) == []


# ── confidence_signal_from_base_rates ─────────────────────────────────────

class TestConfidenceSignal:
    def _make_dist(self, n: int, win: float) -> BaseRateDistribution:
        return BaseRateDistribution(
            horizon_days=7,
            n_analogs=n,
            win_pct=win,
            loss_pct=1.0 - win,
            flat_pct=0.0,
            median_return_pct=0.0,
            p05_return_pct=-1.0,
            p95_return_pct=1.0,
            mean_return_pct=0.0,
            std_return_pct=1.0,
            sufficient_sample=n >= MIN_ANALOGS_FOR_BASE_RATE,
        )

    def test_strong_sample(self):
        dist = self._make_dist(n=CONFIDENCE_SATURATION_COUNT, win=0.7)
        sig = confidence_signal_from_base_rates({7: dist}, horizon=7)
        assert sig == pytest.approx(0.7, abs=1e-9)

    def test_above_saturation(self):
        dist = self._make_dist(n=CONFIDENCE_SATURATION_COUNT * 2, win=0.7)
        sig = confidence_signal_from_base_rates({7: dist}, horizon=7)
        assert sig == pytest.approx(0.7, abs=1e-9)

    def test_insufficient_sample_returns_zero(self):
        dist = self._make_dist(n=5, win=0.7)
        sig = confidence_signal_from_base_rates({7: dist}, horizon=7)
        assert sig == 0.0

    def test_missing_horizon(self):
        sig = confidence_signal_from_base_rates({30: self._make_dist(50, 0.6)}, horizon=7)
        assert sig == 0.0


# ── build_state_vector with mocked engines ───────────────────────────────

def _make_engine_returning(rows_by_series: dict[str, list[tuple]]) -> MagicMock:
    """Build a MagicMock engine whose connect().execute().fetchall() returns
    rows for the series_id passed in the bound parameters.
    """
    engine = MagicMock()

    def fake_execute(stmt, params):
        sid = params.get("sid") if isinstance(params, dict) else None
        rows = rows_by_series.get(sid, []) if sid else []
        result = MagicMock()
        result.fetchall.return_value = rows
        result.fetchone.return_value = rows[0] if rows else None
        return result

    conn = MagicMock()
    conn.execute.side_effect = fake_execute
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)

    engine.connect.return_value = conn
    return engine


class TestBuildStateVector:
    def test_all_none_zero_coverage(self):
        engine = _make_engine_returning({})
        vec = build_state_vector(engine, as_of=date(2026, 1, 15))
        assert isinstance(vec, MarketStateVector)
        assert len(vec.vector) == STATE_VECTOR_DIM
        assert vec.coverage == 0.0
        # All features absent → zeros (or 0.5 for the lone minmax breadth feature)
        for spec, val in zip(STATE_VECTOR_FEATURES, vec.vector):
            if spec.get("kind") == "minmax":
                assert val == 0.5
            else:
                assert val == 0.0

    def test_partial_coverage(self):
        # Provide rich z-score data for VIXCLS and a minmax for breadth
        dates = [date(2025, 1, 1) + timedelta(days=i) for i in range(40)]
        vix_rows = [(d, 15.0 + (i % 5)) for i, d in enumerate(dates)]
        breadth_rows = [(d, 0.6) for d in dates]
        engine = _make_engine_returning({
            "VIXCLS": vix_rows,
            "GRID_SECTOR_BREADTH_200D": breadth_rows,
        })
        vec = build_state_vector(engine, as_of=date(2026, 1, 15))
        assert vec.coverage > 0.0
        # Two features loaded out of STATE_VECTOR_DIM
        expected_coverage = 2 / STATE_VECTOR_DIM
        assert vec.coverage == pytest.approx(expected_coverage, abs=1e-9)

    def test_engine_none_safe(self):
        vec = build_state_vector(None, as_of=date(2026, 1, 15))  # type: ignore[arg-type]
        assert vec.coverage == 0.0
        assert len(vec.vector) == STATE_VECTOR_DIM


# ── build_pattern_match_report end-to-end ────────────────────────────────

class TestBuildPatternMatchReport:
    def test_happy_path_structure(self, monkeypatch):
        from intelligence import pattern_library as pl

        query_date = date(2026, 1, 15)
        query_state = MarketStateVector(
            as_of=query_date,
            vector=tuple([0.5] * STATE_VECTOR_DIM),
            feature_names=tuple(s["name"] for s in STATE_VECTOR_FEATURES),
            coverage=1.0,
        )

        # 30 historical states all very similar to the query
        history = [
            (query_date - timedelta(days=i + 1), tuple([0.5] * STATE_VECTOR_DIM))
            for i in range(30)
        ]

        # Forward returns: 70% wins of +2%, 30% losses of -2% at every horizon
        forward_lookup: dict[date, dict[int, float]] = {}
        for idx, (d, _) in enumerate(history):
            ret = 2.0 if idx < 21 else -2.0
            forward_lookup[d] = {h: ret for h in FORWARD_HORIZONS_DAYS}

        monkeypatch.setattr(pl, "build_state_vector", lambda eng, as_of: query_state)
        monkeypatch.setattr(
            pl,
            "query_historical_states",
            lambda eng, lookback_days=LOOKBACK_DAYS_DEFAULT, as_of=None: history,
        )
        monkeypatch.setattr(
            pl,
            "read_forward_returns",
            lambda eng, ticker, dates, horizons=FORWARD_HORIZONS_DAYS: {
                d: forward_lookup.get(d, {h: float("nan") for h in horizons})
                for d in dates
            },
        )

        report = build_pattern_match_report(
            engine=MagicMock(),
            ticker="SPY",
            as_of=query_date,
            k=DEFAULT_K_NEAREST,
        )

        assert isinstance(report, PatternMatchReport)
        assert report.ticker == "SPY"
        assert report.query_date == query_date
        assert report.analog_count == 30
        assert len(report.top_k_analogs) == 30
        assert set(report.base_rates.keys()) == set(FORWARD_HORIZONS_DAYS)

        seven_d = report.base_rates[7]
        assert seven_d.n_analogs == 30
        assert seven_d.win_pct == pytest.approx(21 / 30)
        assert seven_d.sufficient_sample is True
        # Confidence should be > 0 since we have >= MIN_ANALOGS
        assert report.confidence_signal > 0.0
        assert report.confidence_signal <= 1.0

    def test_insufficient_analogs_zero_confidence(self, monkeypatch):
        from intelligence import pattern_library as pl

        query_date = date(2026, 1, 15)
        query_state = MarketStateVector(
            as_of=query_date,
            vector=tuple([0.0] * STATE_VECTOR_DIM),
            feature_names=tuple(s["name"] for s in STATE_VECTOR_FEATURES),
            coverage=0.0,
        )

        # Only 5 historical states — well below MIN_ANALOGS_FOR_BASE_RATE
        history = [
            (query_date - timedelta(days=i + 1), tuple([0.1] * STATE_VECTOR_DIM))
            for i in range(5)
        ]

        monkeypatch.setattr(pl, "build_state_vector", lambda eng, as_of: query_state)
        monkeypatch.setattr(
            pl,
            "query_historical_states",
            lambda eng, lookback_days=LOOKBACK_DAYS_DEFAULT, as_of=None: history,
        )
        monkeypatch.setattr(
            pl,
            "read_forward_returns",
            lambda eng, ticker, dates, horizons=FORWARD_HORIZONS_DAYS: {
                d: {h: 1.0 for h in horizons} for d in dates
            },
        )

        report = build_pattern_match_report(
            engine=MagicMock(),
            ticker=None,
            as_of=query_date,
        )
        assert report.analog_count == 5
        assert report.confidence_signal == 0.0


# ── Frozen dataclass to_dict roundtrip ────────────────────────────────────

class TestDataclassRoundtrip:
    def test_all_three_to_dict(self):
        msv = MarketStateVector(
            as_of=date(2026, 1, 15),
            vector=tuple([0.1] * STATE_VECTOR_DIM),
            feature_names=tuple(s["name"] for s in STATE_VECTOR_FEATURES),
            coverage=1.0,
        )
        msv_d = msv.to_dict()
        assert msv_d["as_of"] == "2026-01-15"
        assert msv_d["coverage"] == 1.0
        assert len(msv_d["vector"]) == STATE_VECTOR_DIM

        brd = BaseRateDistribution(
            horizon_days=7,
            n_analogs=50,
            win_pct=0.6,
            loss_pct=0.3,
            flat_pct=0.1,
            median_return_pct=1.5,
            p05_return_pct=-3.0,
            p95_return_pct=6.0,
            mean_return_pct=1.4,
            std_return_pct=2.5,
            sufficient_sample=True,
        )
        brd_d = brd.to_dict()
        assert brd_d["horizon_days"] == 7
        assert brd_d["sufficient_sample"] is True

        analog = HistoricalAnalog(
            date=date(2025, 6, 1),
            similarity=0.95,
            forward_returns={1: 0.5, 7: 2.1},
            forward_outcomes={1: "flat", 7: "win"},
        )
        report = PatternMatchReport(
            ticker="SPY",
            query_date=date(2026, 1, 15),
            query_state=msv,
            analog_count=1,
            top_k_analogs=(analog,),
            base_rates={7: brd},
            confidence_signal=0.55,
            generated_at="2026-01-15T00:00:00+00:00",
        )
        report_d = report.to_dict()
        assert report_d["ticker"] == "SPY"
        assert report_d["query_date"] == "2026-01-15"
        assert report_d["analog_count"] == 1
        assert "7" in report_d["base_rates"]
        assert report_d["top_k_analogs"][0]["forward_outcomes"]["7"] == "win"

        # Frozen dataclass immutability check
        with pytest.raises(Exception):
            msv.coverage = 0.5  # type: ignore[misc]
