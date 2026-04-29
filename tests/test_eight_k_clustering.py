"""CAT-61 — 8-K clustering tests."""
from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

from intelligence.eight_k_clustering import (
    ITEM_SEVERITY,
    ClusterAlert,
    EightKFiling,
    _CLUSTER_WINDOW_DAYS,
    _MIN_CLUSTER_SIZE,
    classify_severity,
    detect_clusters,
    scan_for_clusters,
    score_filing,
)


_TODAY = date(2026, 4, 13)


class TestScoreFiling:
    def test_no_items_zero(self):
        f = EightKFiling(ticker="A", filed_date=_TODAY, item_codes=[])
        assert score_filing(f) == 0.0

    def test_single_item(self):
        f = EightKFiling(ticker="A", filed_date=_TODAY, item_codes=["2.02"])
        assert score_filing(f) == ITEM_SEVERITY["2.02"]

    def test_multi_item_takes_max(self):
        f = EightKFiling(
            ticker="A", filed_date=_TODAY,
            item_codes=["2.02", "4.02"],
        )
        assert score_filing(f) == ITEM_SEVERITY["4.02"]

    def test_unknown_item_default(self):
        f = EightKFiling(ticker="A", filed_date=_TODAY, item_codes=["99.99"])
        assert score_filing(f) == 0.2


class TestClassifySeverity:
    def test_critical(self):
        assert classify_severity(2.5) == "critical"

    def test_elevated(self):
        assert classify_severity(1.2) == "elevated"

    def test_warn(self):
        assert classify_severity(0.6) == "warn"

    def test_neutral(self):
        assert classify_severity(0.1) == "neutral"


class TestDetectClusters:
    def test_no_filings(self):
        assert detect_clusters([]) == []

    def test_below_min_size(self):
        filings = [
            EightKFiling(ticker="A", filed_date=_TODAY, item_codes=["8.01"]),
            EightKFiling(ticker="A", filed_date=_TODAY - timedelta(days=5), item_codes=["2.02"]),
        ]
        assert detect_clusters(filings) == []

    def test_cluster_detected(self):
        filings = [
            EightKFiling(ticker="ABC", filed_date=_TODAY - timedelta(days=20), item_codes=["2.02"]),
            EightKFiling(ticker="ABC", filed_date=_TODAY - timedelta(days=10), item_codes=["5.02"]),
            EightKFiling(ticker="ABC", filed_date=_TODAY, item_codes=["4.02"]),
        ]
        alerts = detect_clusters(filings)
        assert len(alerts) == 1
        assert alerts[0].ticker == "ABC"
        assert alerts[0].filing_count == 3
        assert "4.02" in alerts[0].unique_items

    def test_high_severity_cluster(self):
        filings = [
            EightKFiling(ticker="XYZ", filed_date=_TODAY - timedelta(days=15), item_codes=["3.01"]),  # delist
            EightKFiling(ticker="XYZ", filed_date=_TODAY - timedelta(days=10), item_codes=["4.02"]),  # restatement
            EightKFiling(ticker="XYZ", filed_date=_TODAY - timedelta(days=5), item_codes=["2.04"]),   # default trigger
        ]
        alerts = detect_clusters(filings)
        assert len(alerts) == 1
        # 3 filings at 1.0 each → composite 3.0 → critical
        assert alerts[0].severity_label == "critical"
        assert alerts[0].top_item in ("3.01", "4.02", "2.04")

    def test_multiple_tickers(self):
        filings = []
        for i, t in enumerate(["A", "B"]):
            for d in range(3):
                filings.append(EightKFiling(
                    ticker=t,
                    filed_date=_TODAY - timedelta(days=d * 5),
                    item_codes=["8.01"],
                ))
        alerts = detect_clusters(filings)
        assert len(alerts) == 2
        assert {a.ticker for a in alerts} == {"A", "B"}

    def test_sorted_by_severity(self):
        # A is dense critical, B is dense neutral
        filings = []
        for d in range(3):
            filings.append(EightKFiling(
                ticker="HIGH",
                filed_date=_TODAY - timedelta(days=d * 5),
                item_codes=["4.02"],
            ))
            filings.append(EightKFiling(
                ticker="LOW",
                filed_date=_TODAY - timedelta(days=d * 5),
                item_codes=["9.01"],
            ))
        alerts = detect_clusters(filings)
        assert alerts[0].ticker == "HIGH"
        assert alerts[1].ticker == "LOW"

    def test_outside_window_excluded(self):
        filings = [
            EightKFiling(ticker="A", filed_date=_TODAY - timedelta(days=100), item_codes=["4.02"]),
            EightKFiling(ticker="A", filed_date=_TODAY - timedelta(days=10), item_codes=["2.02"]),
            EightKFiling(ticker="A", filed_date=_TODAY - timedelta(days=5), item_codes=["2.02"]),
        ]
        # Only 2 within window → no cluster
        alerts = detect_clusters(filings)
        assert alerts == []

    def test_alert_to_dict(self):
        filings = [
            EightKFiling(ticker="ABC", filed_date=_TODAY - timedelta(days=d * 5), item_codes=["8.01"])
            for d in range(3)
        ]
        alerts = detect_clusters(filings)
        d = alerts[0].to_dict()
        for k in ("ticker", "window_start", "window_end", "filing_count",
                  "unique_items", "composite_severity", "severity_label",
                  "top_item", "top_item_severity"):
            assert k in d


class TestScanForClusters:
    def test_db_error_empty(self):
        eng = MagicMock()
        eng.connect.side_effect = RuntimeError("down")
        assert scan_for_clusters(eng) == []

    def test_patched_filings(self):
        filings = [
            EightKFiling(ticker="ABC", filed_date=_TODAY - timedelta(days=d * 5), item_codes=["4.02"])
            for d in range(3)
        ]
        with patch(
            "intelligence.eight_k_clustering._read_recent_filings",
            return_value=filings,
        ):
            eng = MagicMock()
            alerts = scan_for_clusters(eng)
        assert len(alerts) == 1
        assert alerts[0].ticker == "ABC"
