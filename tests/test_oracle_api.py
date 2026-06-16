"""Tests for the Oracle API router.

Stubs ``api.auth`` and ``api.dependencies`` if their heavy transitive deps
(``psycopg2``, ``jose``, ``passlib``) cannot be imported in the lightweight
CI environment. Mirrors the pattern in ``tests/test_intelligence_search.py``.
"""

from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from types import ModuleType
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Prefer the real api.auth — only stub if heavy deps are unavailable.
# Unconditional stubbing pollutes sys.modules for every later test.
# ---------------------------------------------------------------------------

try:
    import api.auth  # noqa: F401
except Exception:
    _auth_stub = ModuleType("api.auth")
    _auth_stub.require_auth = lambda: None  # type: ignore[attr-defined]
    sys.modules["api.auth"] = _auth_stub

try:
    import api.dependencies  # noqa: F401
except Exception:
    _deps_stub = ModuleType("api.dependencies")
    _deps_stub.get_db_engine = lambda: None  # type: ignore[attr-defined]
    sys.modules["api.dependencies"] = _deps_stub


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


# ── Pagination envelope (no DB needed) ────────────────────────────────────

class TestGetPredictionsPaginationEnvelope:
    """Verify ``GET /api/v1/oracle/predictions`` carries ``limit``/``offset``/
    ``has_more`` per ``.claude/rules/security.md`` list-endpoint contract
    (canonical pattern at ``api/routers/journal.py:77``)."""

    @staticmethod
    def _patched_engine(*, total: int, rows: list | None = None):
        """Mock ``engine.connect()`` context manager for the count + results
        queries. First ``execute()`` is the COUNT (fetchone → ``(total,)``);
        second is the SELECT (fetchall → ``rows or []``)."""
        conn = MagicMock()
        count_result = MagicMock()
        count_result.fetchone.return_value = (total,)
        rows_result = MagicMock()
        rows_result.fetchall.return_value = rows or []
        conn.execute.side_effect = [count_result, rows_result]
        engine = MagicMock()
        engine.connect.return_value.__enter__.return_value = conn
        engine.connect.return_value.__exit__.return_value = False
        return engine

    def test_empty_result_envelope_includes_pagination_fields(self):
        from api.routers import oracle as mod

        with patch.object(mod, "get_db_engine",
                          return_value=self._patched_engine(total=0)):
            result = mod.get_predictions(
                ticker=None, model=None, status=None,
                limit=50, offset=20, _token="test",
            )

        assert result["predictions"] == []
        assert result["total"] == 0
        assert result["limit"] == 50
        assert result["offset"] == 20
        assert result["has_more"] is False

    def test_has_more_true_when_more_pages_exist(self):
        from api.routers import oracle as mod

        # total=100, offset=0, limit=10 → (0+10) < 100 ⇒ has_more=True.
        # Build a single result row with the 23 columns the SELECT projects.
        # Use verdict='hit' (non-'pending') so the tracking-pnl branch at
        # oracle.py:124 stays inert and we only need the count + select
        # results queued on the conn.execute side_effect.
        now = datetime(2026, 5, 19, tzinfo=timezone.utc)
        fake_row = (
            "pred-1", now, "AAPL", "options", "CALL", 200.0, 180.0,
            "2026-06-19", 0.7, 5.0, 0.6, 0.8, "model-a", "v1",
            [], [], {}, "hit", 210.0, 16.6, 16.6, now, "scored",
        )
        with patch.object(mod, "get_db_engine",
                          return_value=self._patched_engine(total=100, rows=[fake_row])):
            result = mod.get_predictions(
                ticker=None, model=None, status=None,
                limit=10, offset=0, _token="test",
            )

        assert result["total"] == 100
        assert result["limit"] == 10
        assert result["offset"] == 0
        assert result["has_more"] is True
        assert len(result["predictions"]) == 1
        assert result["predictions"][0]["ticker"] == "AAPL"

    def test_has_more_false_on_last_page(self):
        from api.routers import oracle as mod

        # total=20, offset=10, limit=10 → (10+10) < 20 is False, last page.
        now = datetime(2026, 5, 19, tzinfo=timezone.utc)
        fake_row = (
            "pred-2", now, "NVDA", "options", "PUT", 100.0, 120.0,
            "2026-06-19", 0.55, -3.0, 0.4, 0.7, "model-b", "v1",
            [], [], {}, "miss", 130.0, 8.3, -8.3, now, "scored",
        )
        with patch.object(mod, "get_db_engine",
                          return_value=self._patched_engine(total=20, rows=[fake_row])):
            result = mod.get_predictions(
                ticker=None, model=None, status=None,
                limit=10, offset=10, _token="test",
            )

        assert result["total"] == 20
        assert result["limit"] == 10
        assert result["offset"] == 10
        assert result["has_more"] is False
