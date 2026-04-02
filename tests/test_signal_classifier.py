"""
Tests for the Gemma 270M signal classification integration.

Tests classify_signal_text, _parse_classification, and narrate_anomalies.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from ingestion.signal_classifier import (
    ClassificationResult,
    classify_signal_text,
    _parse_classification,
)


# ---------------------------------------------------------------------------
# ClassificationResult
# ---------------------------------------------------------------------------


class TestClassificationResult:
    def test_create_result(self) -> None:
        result = ClassificationResult(
            signal_id=1,
            category="rates",
            urgency="critical",
            reason="Fed raised rates 50bp",
            raw_output="CATEGORY: rates\nURGENCY: critical\nREASON: Fed raised rates 50bp",
        )
        assert result.category == "rates"
        assert result.urgency == "critical"


# ---------------------------------------------------------------------------
# _parse_classification
# ---------------------------------------------------------------------------


class TestParseClassification:
    def test_valid_output(self) -> None:
        raw = "CATEGORY: rates\nURGENCY: critical\nREASON: Fed raised rates"
        result = _parse_classification(raw, signal_id=1)

        assert result is not None
        assert result.category == "rates"
        assert result.urgency == "critical"
        assert result.reason == "Fed raised rates"

    def test_invalid_category_defaults_unknown(self) -> None:
        raw = "CATEGORY: aliens\nURGENCY: high\nREASON: test"
        result = _parse_classification(raw, signal_id=2)

        assert result is not None
        assert result.category == "unknown"
        assert result.urgency == "high"

    def test_invalid_urgency_defaults_medium(self) -> None:
        raw = "CATEGORY: equity\nURGENCY: extreme\nREASON: test"
        result = _parse_classification(raw, signal_id=3)

        assert result is not None
        assert result.urgency == "medium"

    def test_missing_fields(self) -> None:
        raw = "Some unexpected output"
        result = _parse_classification(raw, signal_id=4)

        assert result is not None
        assert result.category == "unknown"
        assert result.urgency == "medium"
        assert result.reason == ""

    def test_case_insensitive_labels(self) -> None:
        raw = "category: equity\nurgency: HIGH\nreason: Earnings beat"
        result = _parse_classification(raw, signal_id=5)

        assert result is not None
        assert result.category == "equity"
        assert result.urgency == "high"

    def test_all_valid_categories(self) -> None:
        valid = [
            "rates", "credit", "equity", "volatility", "flows", "macro",
            "geopolitical", "insider", "options", "crypto", "commodities", "fx",
        ]
        for cat in valid:
            raw = f"CATEGORY: {cat}\nURGENCY: low\nREASON: test"
            result = _parse_classification(raw, signal_id=cat)
            assert result.category == cat


# ---------------------------------------------------------------------------
# classify_signal_text
# ---------------------------------------------------------------------------


class TestClassifySignalText:
    def test_classify_when_pool_available(self) -> None:
        mock_pool = MagicMock()
        mock_pool.classify_signal.return_value = (
            "CATEGORY: macro\nURGENCY: high\nREASON: CPI above expectations"
        )

        with patch("gemma.micro.get_micro_pool", return_value=mock_pool):
            result = classify_signal_text("CPI printed 3.5% vs 3.2% expected")

        assert result is not None
        assert result.category == "macro"
        assert result.urgency == "high"

    def test_classify_when_pool_unavailable(self) -> None:
        with patch(
            "gemma.micro.get_micro_pool",
            side_effect=ImportError("not installed"),
        ):
            result = classify_signal_text("test signal")

        assert result is None

    def test_classify_when_model_returns_none(self) -> None:
        mock_pool = MagicMock()
        mock_pool.classify_signal.return_value = None

        with patch("gemma.micro.get_micro_pool", return_value=mock_pool):
            result = classify_signal_text("test signal")

        assert result is None
