"""Tests for earnings calendar ingestion and intelligence modules."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest


# ── Ingestion: classify_surprise ─────────────────────────────────────────

def test_classify_surprise_beat():
    from ingestion.altdata.earnings_calendar import _classify_surprise
    assert _classify_surprise(1.0, 1.05) == "beat"  # +5%


def test_classify_surprise_miss():
    from ingestion.altdata.earnings_calendar import _classify_surprise
    assert _classify_surprise(1.0, 0.90) == "miss"  # -10%


def test_classify_surprise_inline():
    from ingestion.altdata.earnings_calendar import _classify_surprise
    assert _classify_surprise(1.0, 1.01) == "inline"  # +1%


def test_classify_surprise_pending():
    from ingestion.altdata.earnings_calendar import _classify_surprise
    assert _classify_surprise(1.0, None) == "pending"
    assert _classify_surprise(None, 1.0) == "pending"


def test_classify_surprise_zero_estimate():
    from ingestion.altdata.earnings_calendar import _classify_surprise
    assert _classify_surprise(0, 0.5) == "beat"
    assert _classify_surprise(0, -0.5) == "miss"
    assert _classify_surprise(0, 0) == "inline"


# ── Ingestion: safe_float ────────────────────────────────────────────────

def test_safe_float_normal():
    from ingestion.altdata.earnings_calendar import _safe_float
    assert _safe_float(3.14) == 3.14
    assert _safe_float("2.5") == 2.5
    assert _safe_float(0) == 0.0


def test_safe_float_nan():
    import math
    from ingestion.altdata.earnings_calendar import _safe_float
    assert _safe_float(float("nan")) is None
    assert _safe_float(float("inf")) is None
    assert _safe_float(None) is None
    assert _safe_float("not_a_number") is None


# ── Intelligence: EarningsPrediction dataclass ───────────────────────────

def test_earnings_prediction_to_dict():
    from intelligence.earnings_intel import EarningsPrediction
    pred = EarningsPrediction(
        id="abc123",
        ticker="AAPL",
        earnings_date="2026-01-29",
        predicted_direction="up",
        predicted_move_pct=3.5,
        confidence=0.72,
        iv_rank=65.0,
        historical_surprise_avg=4.2,
        historical_beat_rate=0.875,
        sector_momentum=0.03,
        insider_signal="bullish",
        congressional_signal="none",
        reasoning="Strong beat history; sector tailwind",
    )
    d = pred.to_dict()
    assert d["ticker"] == "AAPL"
    assert d["predicted_direction"] == "up"
    assert d["verdict"] == "pending"
    assert d["confidence"] == 0.72
