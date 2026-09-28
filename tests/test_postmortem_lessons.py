"""Tests for the postmortem-lessons honesty fix (GRID-WAVE3-HELD-WRITERS-
TRIAGE-20260927.md #6): the label format ("LLM summary of N postmortems
dated X-Y") and the empty-window guard that must never regenerate from
zero rows nor overwrite a still-useful prior cache.

Pure unit tests: the DB engine and every LLM call are mocked. Nothing here
opens a real database connection or calls a real LLM.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from api.routers import postmortem_lessons as pml


def _record(generated_at: str, **overrides) -> dict:
    base = {
        "id": 1,
        "trade_id": 1,
        "prediction_id": None,
        "ticker": "AAPL",
        "outcome": "LOSS",
        "failure_category": "wrong_signal",
        "root_cause": "test",
        "signals_wrong": [],
        "signals_right": [],
        "what_we_missed": "",
        "recommended_fix": "",
        "full_analysis": {"trade_id": 1, "ticker": "AAPL", "outcome": "LOSS"},
        "confidence": 0.5,
        "generated_at": generated_at,
    }
    base.update(overrides)
    return base


class TestDateRangeLabel:
    def test_single_day_has_no_range(self):
        label = pml._date_range_label(["2026-09-18T13:03:00+00:00", "2026-09-18T09:00:00+00:00"])
        assert label == "2026-09-18"

    def test_multi_day_range(self):
        label = pml._date_range_label(["2026-09-01T00:00:00+00:00", "2026-09-05T12:00:00+00:00"])
        assert label == "2026-09-01 to 2026-09-05"

    def test_empty_input_returns_none(self):
        assert pml._date_range_label([]) is None
        assert pml._date_range_label([None, None]) is None


class TestGenerateLabel:
    def test_populated_window_produces_label_with_count_and_date_range(self, monkeypatch):
        records = [
            _record("2026-09-01T00:00:00+00:00"),
            _record("2026-09-03T00:00:00+00:00"),
            _record("2026-09-05T00:00:00+00:00"),
        ]
        monkeypatch.setattr(
            "intelligence.postmortem.load_postmortems", lambda engine, days: records,
        )
        llm_mock = MagicMock(return_value="Cover puts before earnings.")
        monkeypatch.setattr("intelligence.postmortem.generate_lessons_learned", llm_mock)

        result = pml._generate(MagicMock(), n=5, days=30)

        assert result["count"] == 3
        assert result["label"] == "LLM summary of 3 postmortems dated 2026-09-01 to 2026-09-05"
        llm_mock.assert_called_once()

    def test_empty_window_never_calls_the_llm(self, monkeypatch):
        monkeypatch.setattr(
            "intelligence.postmortem.load_postmortems", lambda engine, days: [],
        )
        llm_mock = MagicMock()
        monkeypatch.setattr("intelligence.postmortem.generate_lessons_learned", llm_mock)

        result = pml._generate(MagicMock(), n=5, days=30)

        assert result["count"] == 0
        assert result["label"] is None
        llm_mock.assert_not_called()

    def test_unhydrated_records_are_also_treated_as_empty(self, monkeypatch):
        """Records without full_analysis can't build a PostMortem -- this
        must not call the LLM either, and must not raise."""
        monkeypatch.setattr(
            "intelligence.postmortem.load_postmortems",
            lambda engine, days: [_record("2026-09-01T00:00:00+00:00", full_analysis={})],
        )
        llm_mock = MagicMock()
        monkeypatch.setattr("intelligence.postmortem.generate_lessons_learned", llm_mock)

        result = pml._generate(MagicMock(), n=5, days=30)

        assert result["count"] == 0
        assert result["label"] is None
        llm_mock.assert_not_called()


class TestEndpointEmptyWindowGuard:
    """Exercises the FastAPI route function directly (no TestClient/HTTP
    needed — it's a plain async function) with every collaborator mocked."""

    def _run(self, **kwargs):
        return asyncio.run(pml.get_postmortem_lessons(_token="test", **kwargs))

    def test_empty_window_with_no_prior_cache_returns_honest_empty_state(self, monkeypatch):
        monkeypatch.setattr(pml, "get_db_engine", lambda: MagicMock())
        monkeypatch.setattr(pml, "_read_cache", lambda engine: None)
        monkeypatch.setattr(
            pml, "_generate",
            lambda engine, n, days: {"text": "No post-mortems found in the last 30 days.", "count": 0, "label": None},
        )
        write_mock = MagicMock()
        monkeypatch.setattr(pml, "_write_cache", write_mock)

        result = self._run(n=5, days=30, refresh=1)

        assert result["lessons"]["count"] == 0
        assert result["note"] == "no postmortems in the last 30 days"
        assert result["label"] is None
        write_mock.assert_not_called()

    def test_empty_window_with_prior_cache_preserves_it_untouched(self, monkeypatch):
        """The core of Wave 3 #6: an empty regeneration must not erase a
        still-useful prior cached summary."""
        prior_cache = {
            "lessons": {"text": "Cover puts before earnings.", "count": 5,
                        "label": "LLM summary of 5 postmortems dated 2026-09-14 to 2026-09-18"},
            "generated_at": "2026-09-24T13:03:00+00:00",
            "n": 5,
            "days": 30,
            "_ts": None,
        }
        monkeypatch.setattr(pml, "get_db_engine", lambda: MagicMock())
        monkeypatch.setattr(pml, "_read_cache", lambda engine: dict(prior_cache))
        monkeypatch.setattr(
            pml, "_generate",
            lambda engine, n, days: {"text": "No post-mortems found in the last 30 days.", "count": 0, "label": None},
        )
        write_mock = MagicMock()
        monkeypatch.setattr(pml, "_write_cache", write_mock)

        # refresh=1 forces past the fresh-cache short-circuit so the empty
        # regeneration path actually runs and has to make this decision.
        result = self._run(n=5, days=30, refresh=1)

        write_mock.assert_not_called()
        assert result["cached"] is True
        assert result["lessons"] == prior_cache["lessons"]
        assert result["generated_at"] == prior_cache["generated_at"]
        assert result["note"] == "no postmortems in the last 30 days"
        assert result["label"] == "LLM summary of 5 postmortems dated 2026-09-14 to 2026-09-18"

    def test_populated_window_writes_cache_and_surfaces_label(self, monkeypatch):
        monkeypatch.setattr(pml, "get_db_engine", lambda: MagicMock())
        monkeypatch.setattr(pml, "_read_cache", lambda engine: None)
        payload = {"text": "Cover puts before earnings.", "count": 3,
                   "label": "LLM summary of 3 postmortems dated 2026-09-01 to 2026-09-05"}
        monkeypatch.setattr(pml, "_generate", lambda engine, n, days: payload)
        write_mock = MagicMock(return_value="2026-09-28T00:00:00+00:00")
        monkeypatch.setattr(pml, "_write_cache", write_mock)

        result = self._run(n=5, days=30, refresh=1)

        write_mock.assert_called_once()
        assert result["cached"] is False
        assert result["label"] == payload["label"]
        assert result["lessons"] == payload
        assert "note" not in result
