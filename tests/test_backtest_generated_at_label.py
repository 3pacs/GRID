"""Tests for the backtest results/summary honesty labeling (GRID-WAVE3-HELD-
WRITERS-TRIAGE-20260927.md #7): ``backtest_results.json`` carries no
``generated_at`` of its own, so the API endpoints must label the response
with the file's mtime plus an honest "pitch backtest, in-sample" note.

Pure unit tests: no real backtest ever runs and no live yfinance call is
made. The engine's file I/O is exercised against a tmp_path, never the
project's real outputs/backtest directory.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from api.routers import backtest as backtest_router


class _FakeBacktester:
    """Stands in for backtest.engine.PitchBacktester with a tmp output_dir."""

    def __init__(self, output_dir, results=None, summary=None):
        self.output_dir = output_dir
        self._results = results
        self._summary = summary

    def get_latest_results(self):
        return self._results

    def get_summary(self):
        return self._summary


class TestResultsGeneratedAtHelper:
    def test_returns_none_when_file_missing(self, tmp_path):
        bt = _FakeBacktester(tmp_path)
        assert backtest_router._results_generated_at(bt) is None

    def test_returns_file_mtime_as_iso_utc(self, tmp_path):
        import os

        json_path = tmp_path / "backtest_results.json"
        json_path.write_text("{}")
        fixed_mtime = datetime(2026, 3, 24, 12, 0, 0, tzinfo=timezone.utc).timestamp()
        os.utime(json_path, (fixed_mtime, fixed_mtime))

        bt = _FakeBacktester(tmp_path)
        generated_at = backtest_router._results_generated_at(bt)

        assert generated_at == "2026-03-24T12:00:00+00:00"


class TestBacktestNoteConstant:
    def test_note_says_in_sample_pitch_backtest(self):
        assert "in-sample" in backtest_router._BACKTEST_NOTE
        assert "pitch backtest" in backtest_router._BACKTEST_NOTE


class TestEndpointsCarryGeneratedAtAndNote:
    def _run(self, coro):
        return asyncio.run(coro)

    def test_get_results_adds_generated_at_and_note(self, tmp_path, monkeypatch):
        json_path = tmp_path / "backtest_results.json"
        json_path.write_text("{}")
        fake_bt = _FakeBacktester(tmp_path, results={"period": "2015-2026", "final_value": 123})
        monkeypatch.setattr(
            "backtest.engine.PitchBacktester", lambda *a, **kw: fake_bt,
        )

        result = self._run(backtest_router.get_results())

        assert result["period"] == "2015-2026"
        assert result["generated_at"] is not None
        assert result["note"] == backtest_router._BACKTEST_NOTE

    def test_get_summary_adds_generated_at_and_note(self, tmp_path, monkeypatch):
        json_path = tmp_path / "backtest_results.json"
        json_path.write_text("{}")
        fake_bt = _FakeBacktester(tmp_path, summary={"period": "2015-2026"})
        monkeypatch.setattr(
            "backtest.engine.PitchBacktester", lambda *a, **kw: fake_bt,
        )

        result = self._run(backtest_router.get_summary())

        assert result["period"] == "2015-2026"
        assert result["generated_at"] is not None
        assert result["note"] == backtest_router._BACKTEST_NOTE

    def test_get_results_404_when_no_results_has_no_stray_fields(self, tmp_path, monkeypatch):
        from fastapi import HTTPException

        import pytest

        fake_bt = _FakeBacktester(tmp_path, results=None)
        monkeypatch.setattr(
            "backtest.engine.PitchBacktester", lambda *a, **kw: fake_bt,
        )

        with pytest.raises(HTTPException) as excinfo:
            self._run(backtest_router.get_results())
        assert excinfo.value.status_code == 404
