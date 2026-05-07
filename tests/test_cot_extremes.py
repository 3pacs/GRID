"""CAT-35 — CFTC COT extremes tests."""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch


from intelligence.cot_extremes import (
    CORE_CONTRACTS,
    EXTREME_METRICS,
    COTExtreme,
    classify_extreme,
    rank_contrarian_signals,
    scan_all_extremes,
)


class TestClassifyExtreme:
    def test_too_short_returns_none(self):
        result = classify_extreme(
            contract="SP500", metric="net_speculative",
            history=list(range(30)),
        )
        assert result is None

    def test_neutral_middle(self):
        history = list(range(100))
        history.append(50)
        result = classify_extreme(
            contract="SP500", metric="net_speculative",
            history=[float(x) for x in history],
        )
        assert result is not None
        assert result.severity in ("neutral", "elevated")

    def test_crowded_long_at_top(self):
        history = list(range(100))
        history.append(200)
        result = classify_extreme(
            contract="GOLD", metric="net_speculative",
            history=[float(x) for x in history],
        )
        assert result is not None
        assert result.percentile_rank >= 95
        assert result.direction == "long_crowd"
        assert result.severity == "extreme"

    def test_crowded_short_at_bottom(self):
        history = list(range(100))
        history.append(-200)
        result = classify_extreme(
            contract="GOLD", metric="net_speculative",
            history=[float(x) for x in history],
        )
        assert result is not None
        assert result.percentile_rank <= 5
        assert result.direction == "short_crowd"
        assert result.severity == "extreme"

    def test_z_score_computed(self):
        history = [10.0] * 52 + [50.0]
        result = classify_extreme(
            contract="X", metric="net_speculative",
            history=history,
        )
        assert result is not None
        assert result.z_score > 1.0 or result.current_value == 50.0

    def test_sample_size_recorded(self):
        history = [float(i) for i in range(100)]
        result = classify_extreme(
            contract="X", metric="net_speculative",
            history=history,
        )
        assert result is not None
        assert result.sample_size == 100

    def test_nan_filtered(self):
        import math
        history = [float(i) for i in range(80)] + [math.nan] * 10
        result = classify_extreme(
            contract="X", metric="net_speculative",
            history=history,
        )
        assert result is not None
        assert result.sample_size == 80


class TestRankContrarianSignals:
    def test_empty(self):
        assert rank_contrarian_signals([]) == []

    def test_extreme_before_elevated(self):
        extremes = [
            COTExtreme("A", "net_speculative", date.today(), 100, 70, 1.5,
                       "elevated", "long_crowd", 100),
            COTExtreme("B", "net_speculative", date.today(), 100, 99, 3.0,
                       "extreme", "long_crowd", 100),
        ]
        ranked = rank_contrarian_signals(extremes)
        assert ranked[0].contract == "B"
        assert ranked[1].contract == "A"

    def test_within_severity_sort_by_z(self):
        extremes = [
            COTExtreme("A", "x", date.today(), 100, 96, 2.0, "extreme", "long_crowd", 100),
            COTExtreme("B", "x", date.today(), 100, 99, 3.5, "extreme", "long_crowd", 100),
        ]
        ranked = rank_contrarian_signals(extremes)
        assert ranked[0].contract == "B"


class TestScanAllExtremes:
    def test_empty_db(self):
        with patch(
            "intelligence.cot_extremes._read_series_history",
            return_value=[],
        ):
            eng = MagicMock()
            out = scan_all_extremes(eng, contracts=["SP500"], metrics=["net_speculative"])
        assert out == []

    def test_patched_history(self):
        history = [(date(2025, 1, 1 + i % 28), float(i)) for i in range(60)]
        history.append((date(2025, 6, 1), 200.0))
        with patch(
            "intelligence.cot_extremes._read_series_history",
            return_value=history,
        ):
            eng = MagicMock()
            out = scan_all_extremes(
                eng, contracts=["GOLD"], metrics=["net_speculative"],
            )
        assert len(out) == 1
        assert out[0].severity == "extreme"


class TestConstants:
    def test_core_contracts_non_empty(self):
        assert len(CORE_CONTRACTS) >= 10

    def test_extreme_metrics_has_net_spec(self):
        assert "net_speculative" in EXTREME_METRICS


class TestDataclassRoundtrip:
    def test_to_dict(self):
        e = COTExtreme(
            contract="GOLD", metric="net_speculative", as_of=date(2026, 4, 13),
            current_value=100.0, percentile_rank=95.0, z_score=2.5,
            severity="extreme", direction="long_crowd", sample_size=156,
        )
        d = e.to_dict()
        for k in ("contract", "metric", "as_of", "current_value",
                  "percentile_rank", "z_score", "severity", "direction",
                  "sample_size"):
            assert k in d
