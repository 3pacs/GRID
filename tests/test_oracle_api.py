"""Tests for the Oracle API router."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch


@dataclass
class _DummyPrediction:
    ticker: str
    direction: str
    score: int
    confidence: float
    strength: float
    coherence: float
    model_count: int
    level: str
    model_votes: list[dict]
    as_of: datetime | None
    horizon: int = 7


def _run(coro):
    """Run an async coroutine synchronously."""
    return asyncio.new_event_loop().run_until_complete(coro)


def test_predict_live_returns_serialized_prediction_payload():
    """Live oracle predictions should expose a JSON-safe confidence stack payload."""
    engine = MagicMock()
    prediction = _DummyPrediction(
        ticker="AAPL",
        direction="bullish",
        score=73,
        confidence=0.61,
        strength=0.55,
        coherence=0.8,
        model_count=4,
        level="meta",
        model_votes=[{"model_name": "macro-1", "direction": "bullish"}],
        as_of=datetime(2026, 4, 18, 12, 30, tzinfo=timezone.utc),
        horizon=7,
    )

    with patch("api.routers.oracle.get_db_engine", return_value=engine), patch("api.routers.oracle.EnsemblePredictor") as predictor_cls:
        predictor_cls.return_value.predict.return_value = prediction

        from api.routers.oracle import predict_live

        result = _run(predict_live(ticker="aapl", horizon=7, _token="test"))

    predictor_cls.assert_called_once_with(engine)
    predictor_cls.return_value.predict.assert_called_once()
    assert result["ticker"] == "AAPL"
    assert result["direction"] == "bullish"
    assert result["model_count"] == 4
    assert result["horizon"] == 7
    assert result["as_of"] == "2026-04-18T12:30:00+00:00"
