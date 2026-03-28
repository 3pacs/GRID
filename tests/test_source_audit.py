"""
Tests for the GRID source audit engine (intelligence/source_audit.py).

Tests redundancy map building, pairwise source comparison, discrepancy
detection, and the full audit pipeline with mocked database results.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch, call

import numpy as np
import pandas as pd
import pytest

from intelligence.source_audit import (
    DEFAULT_DISCREPANCY_THRESHOLD,
    FAMILY_DISCREPANCY_THRESHOLDS,
    MIN_OVERLAP_DAYS,
    WEIGHT_ACCURACY,
    WEIGHT_TIMELINESS,
    WEIGHT_COMPLETENESS,
    _SEED_REDUNDANCY,
    build_redundancy_map,
    compare_sources,
    detect_discrepancies,
    run_full_audit,
    update_source_priorities,
    get_latest_audit_summary,
    _find_yfinance_tiebreaker,
    _fetch_series,
    ensure_tables,
)


# ── Redundancy Map Tests ─────────────────────────────────────────────────


class TestBuildRedundancyMap:
    """Test auto-detection of redundant sources from entity_map."""

    def test_detects_known_redundancy_from_entity_map(self):
        """SEED_MAPPINGS maps both VIXCLS and YF:^VIX:close to VIX features.
        Combined with CBOE:VIX from NEW_MAPPINGS_V2, vix_spot should appear."""
        engine = MagicMock()
        conn_mock = MagicMock()
        conn_mock.execute.return_value.fetchall.return_value = []
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn_mock)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        rmap = build_redundancy_map(engine)

        # vix_spot is mapped from VIXCLS, CBOE:VIX (and seed redundancy adds YF:^VIX:close)
        assert "vix_spot" in rmap
        assert len(rmap["vix_spot"]) >= 2

    def test_seed_redundancy_merged(self):
        """_SEED_REDUNDANCY entries are merged into the map."""
        engine = MagicMock()
        conn_mock = MagicMock()
        conn_mock.execute.return_value.fetchall.return_value = []
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn_mock)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        rmap = build_redundancy_map(engine)

        for feature_name in _SEED_REDUNDANCY:
            assert feature_name in rmap, f"{feature_name} should be in redundancy map"
            assert len(rmap[feature_name]) >= 2

    def test_single_source_excluded(self):
        """Features with only one source should NOT appear in the map."""
        engine = MagicMock()
        conn_mock = MagicMock()
        conn_mock.execute.return_value.fetchall.return_value = []
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn_mock)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        rmap = build_redundancy_map(engine)

        # fed_funds_rate maps from only DFF
        assert "fed_funds_rate" not in rmap

    def test_crypto_redundancy(self):
        """Crypto features should show up with Binance, CoinGecko, yfinance sources."""
        engine = MagicMock()
        conn_mock = MagicMock()
        conn_mock.execute.return_value.fetchall.return_value = []
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn_mock)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        rmap = build_redundancy_map(engine)
        assert "btc_close" in rmap
        assert len(rmap["btc_close"]) >= 2


# ── Compare Sources Tests ────────────────────────────────────────────────


class TestCompareSources:
    """Test pairwise source accuracy comparison."""

    @patch("intelligence.source_audit._fetch_series")
    @patch("intelligence.source_audit.build_redundancy_map")
    def test_insufficient_sources(self, mock_rmap, mock_fetch):
        """Single-source feature should return insufficient_sources."""
        engine = MagicMock()
        mock_rmap.return_value = {"some_feature": ["SOURCE_A"]}

        result = compare_sources(engine, "some_feature")
        assert result["status"] == "insufficient_sources"

    @patch("intelligence.source_audit._fetch_series")
    @patch("intelligence.source_audit.build_redundancy_map")
    def test_perfectly_correlated_sources(self, mock_rmap, mock_fetch):
        """Two identical series should have correlation ~1 and deviation ~0."""
        engine = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock()
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        mock_rmap.return_value = {"test_feat": ["SRC_A", "SRC_B"]}

        dates = pd.date_range("2024-01-01", periods=60, freq="D")
        values = np.sin(np.arange(60)) * 100 + 1000
        series = pd.Series(values, index=dates)

        mock_fetch.side_effect = [series.copy(), series.copy()]

        result = compare_sources(engine, "test_feat")
        assert result["status"] == "ok"
        assert len(result["pairs"]) == 1
        pair = result["pairs"][0]
        assert pair["correlation"] > 0.99
        assert pair["pct_mean_deviation"] < 0.001

    @patch("intelligence.source_audit._fetch_series")
    @patch("intelligence.source_audit.build_redundancy_map")
    def test_insufficient_overlap(self, mock_rmap, mock_fetch):
        """Two non-overlapping series should report insufficient_overlap."""
        engine = MagicMock()
        mock_rmap.return_value = {"test_feat": ["SRC_A", "SRC_B"]}

        dates_a = pd.date_range("2024-01-01", periods=10, freq="D")
        dates_b = pd.date_range("2025-01-01", periods=10, freq="D")
        mock_fetch.side_effect = [
            pd.Series(range(10), index=dates_a, dtype=float),
            pd.Series(range(10), index=dates_b, dtype=float),
        ]

        result = compare_sources(engine, "test_feat")
        assert result["pairs"][0]["status"] == "insufficient_overlap"


# ── Discrepancy Detection Tests ──────────────────────────────────────────


class TestDetectDiscrepancies:
    """Test discrepancy detection and third-source resolution."""

    @patch("intelligence.source_audit._get_feature_family")
    @patch("intelligence.source_audit.build_redundancy_map")
    def test_no_discrepancy_when_values_match(self, mock_rmap, mock_family):
        """Sources agreeing within threshold should produce no discrepancy."""
        engine = MagicMock()
        mock_rmap.return_value = {"test_feat": ["SRC_A", "SRC_B"]}
        mock_family.return_value = "equity"

        conn_mock = MagicMock()
        # Both sources return essentially the same value
        conn_mock.execute.return_value.fetchone.side_effect = [
            (date(2024, 6, 1), 100.0),
            (date(2024, 6, 1), 100.5),  # 0.5% diff, under 1.5% equity threshold
        ]
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn_mock)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        discs = detect_discrepancies(engine, threshold=0.02)
        assert len(discs) == 0

    @patch("intelligence.source_audit._get_feature_family")
    @patch("intelligence.source_audit.build_redundancy_map")
    def test_discrepancy_detected_when_values_diverge(self, mock_rmap, mock_family):
        """Sources disagreeing beyond threshold should produce a discrepancy."""
        engine = MagicMock()
        mock_rmap.return_value = {"test_feat": ["SRC_A", "SRC_B"]}
        mock_family.return_value = "equity"

        conn_mock = MagicMock()
        begin_mock = MagicMock()
        begin_conn = MagicMock()

        # First two calls: fetch latest values for SRC_A and SRC_B
        conn_mock.execute.return_value.fetchone.side_effect = [
            (date(2024, 6, 1), 100.0),
            (date(2024, 6, 1), 120.0),  # 20% divergence
        ]
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn_mock)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        begin_conn.execute = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=begin_conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        discs = detect_discrepancies(engine, threshold=0.02)
        assert len(discs) == 1
        assert discs[0]["feature"] == "test_feat"
        assert discs[0]["deviation"] > 0.15

    def test_third_source_resolution(self):
        """When a third source is available, it should break the tie."""
        # If third_value is closer to value_a, source_a wins
        # This is tested via the logic in detect_discrepancies
        # For unit-level, test the yfinance tiebreaker finder
        result = _find_yfinance_tiebreaker(
            "eurusd",
            ["FRED:DEXUSEU", "ECB:EXR"],
            "FRED:DEXUSEU",
            "ECB:EXR",
        )
        # YF:EURUSD=X:close maps to "eurusd" in SEED_MAPPINGS
        assert result == "YF:EURUSD=X:close"

    def test_yfinance_tiebreaker_none_when_already_used(self):
        """No tiebreaker when all yfinance sources are already in comparison."""
        result = _find_yfinance_tiebreaker(
            "eurusd",
            ["YF:EURUSD=X:close", "FRED:DEXUSEU"],
            "YF:EURUSD=X:close",
            "FRED:DEXUSEU",
        )
        assert result is None


# ── Full Audit Tests ─────────────────────────────────────────────────────


class TestRunFullAudit:
    """Test the full audit orchestration."""

    @patch("intelligence.source_audit.detect_discrepancies")
    @patch("intelligence.source_audit.compare_sources")
    @patch("intelligence.source_audit.build_redundancy_map")
    @patch("intelligence.source_audit.ensure_tables")
    def test_audit_report_structure(self, mock_ensure, mock_rmap, mock_compare, mock_disc):
        """Full audit should return a well-structured report."""
        engine = MagicMock()
        mock_rmap.return_value = {
            "feat_a": ["SRC_1", "SRC_2"],
            "feat_b": ["SRC_2", "SRC_3"],
        }
        mock_compare.side_effect = [
            {
                "feature_name": "feat_a",
                "pairs": [{"status": "compared"}],
                "rankings": {"SRC_1": 0.8, "SRC_2": 0.4},
                "best_source": "SRC_1",
                "status": "ok",
            },
            {
                "feature_name": "feat_b",
                "pairs": [],
                "rankings": {"SRC_2": 0.5, "SRC_3": 0.7},
                "best_source": "SRC_3",
                "status": "ok",
            },
        ]
        mock_disc.return_value = []

        report = run_full_audit(engine)

        assert "redundant_features" in report
        assert report["redundant_features"] == 2
        assert "source_rankings" in report
        assert "single_source_features" in report
        assert "recommendations" in report
        assert "comparisons" in report
        assert isinstance(report["active_discrepancies"], int)


# ── Update Priorities Tests ──────────────────────────────────────────────


class TestUpdateSourcePriorities:
    """Test auto-promotion of best sources."""

    def test_empty_rankings(self):
        """No rankings should result in no changes."""
        engine = MagicMock()
        result = update_source_priorities(engine, {"source_rankings": {}})
        assert result["changes"] == 0

    def test_priority_update(self):
        """Sources should be ranked by score with spacing of 10."""
        engine = MagicMock()
        conn_mock = MagicMock()
        conn_mock.execute.return_value.fetchone.return_value = (1, "FRED", 10)
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn_mock)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        audit_results = {
            "source_rankings": {
                "FRED:SP500": 0.9,
                "YF:^GSPC:close": 0.7,
                "BINANCE:BTCUSDT:close": 0.5,
            },
        }

        result = update_source_priorities(engine, audit_results)
        assert result["changes"] >= 1


# ── Constants Tests ──────────────────────────────────────────────────────


class TestConstants:
    """Test that configuration constants are sensible."""

    def test_weights_sum_to_one(self):
        total = WEIGHT_ACCURACY + WEIGHT_TIMELINESS + WEIGHT_COMPLETENESS
        assert abs(total - 1.0) < 1e-9

    def test_family_thresholds_positive(self):
        for family, threshold in FAMILY_DISCREPANCY_THRESHOLDS.items():
            assert threshold > 0, f"{family} threshold must be positive"
            assert threshold < 1.0, f"{family} threshold must be < 100%"

    def test_min_overlap_positive(self):
        assert MIN_OVERLAP_DAYS > 0

    def test_seed_redundancy_has_multiple_sources(self):
        for fname, sources in _SEED_REDUNDANCY.items():
            assert len(sources) >= 2, f"{fname} should have 2+ sources"
