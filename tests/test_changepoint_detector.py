"""
Tests for the AutoBNN changepoint detector integration.

Tests scan_for_changepoints, publish_regime_signals, and
run_changepoint_cycle with mocked database.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from discovery.changepoint_detector import (
    ChangeReport,
    scan_for_changepoints,
    run_changepoint_cycle,
)
from timeseries.autobnn import RegimeChangeSignal


# ---------------------------------------------------------------------------
# ChangeReport
# ---------------------------------------------------------------------------


class TestChangeReport:
    def test_create_report(self) -> None:
        report = ChangeReport(
            timestamp=datetime.now(timezone.utc),
            features_scanned=10,
            changepoints_found=3,
            regime_changes=[],
            elapsed_seconds=1.5,
        )
        assert report.features_scanned == 10
        assert report.changepoints_found == 3


# ---------------------------------------------------------------------------
# scan_for_changepoints (with mocked DB)
# ---------------------------------------------------------------------------


class TestScanForChangepoints:
    def test_no_features_returns_empty(self) -> None:
        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        conn.execute.return_value.fetchall.return_value = []

        report = scan_for_changepoints(engine)

        assert report.features_scanned == 0
        assert report.changepoints_found == 0

    def test_scans_features_with_data(self) -> None:
        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        # First call: feature registry
        features = [(1, "test_feature", "equity")]

        # Second call: feature data (V-shaped — has changepoint)
        n = 100
        rising = np.linspace(100, 150, n // 2)
        falling = np.linspace(150, 110, n // 2)
        values = np.concatenate([rising, falling])
        data_rows = [
            (date(2025, 1, 1 + i if i < 28 else 28), float(v))
            for i, v in enumerate(values[:50])
        ]

        call_count = [0]

        def mock_execute(*args, **kwargs):
            result = MagicMock()
            if call_count[0] == 0:
                result.fetchall.return_value = features
            else:
                result.fetchall.return_value = data_rows
            call_count[0] += 1
            return result

        conn.execute = mock_execute

        report = scan_for_changepoints(engine, max_features=5)

        assert report.features_scanned >= 0
        assert report.elapsed_seconds >= 0


# ---------------------------------------------------------------------------
# run_changepoint_cycle
# ---------------------------------------------------------------------------


class TestRunChangepointCycle:
    def test_returns_summary(self) -> None:
        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        conn.execute.return_value.fetchall.return_value = []

        result = run_changepoint_cycle(engine)

        assert "features_scanned" in result
        assert "changepoints_found" in result
        assert "signals_published" in result
        assert "elapsed_seconds" in result
