"""Tests for the dormant LLM second-opinion hypothesis reviewer
(``scripts.hermes_llm_hypothesis_review``).

Engine is stubbed and ``HermesAgent`` is patched — no DB, no network.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from scripts.hermes_llm_hypothesis_review import llm_review_active_hypotheses

pytestmark = pytest.mark.unit


def _row(hid, confidence, *, thesis="T", ptype="lead_lag", evidence=None, inv="X"):
    # matches SELECT id, thesis, pattern_type, evidence, invalidation, confidence
    return (hid, thesis, ptype, evidence, inv, confidence)


class _FakeConn:
    def __init__(self, rows, captured):
        self._rows, self._captured = rows, captured

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, stmt, params=None):
        self._captured["params"] = params
        res = MagicMock()
        res.fetchall.return_value = self._rows
        return res


class _FakeEngine:
    def __init__(self, rows):
        self.rows, self.captured = rows, {}

    def connect(self):
        return _FakeConn(self.rows, self.captured)


class _Agent:
    def __init__(self, fail_on=None):
        self.fail_on = fail_on
        self.calls = []

    def score_hypothesis(self, hypothesis, *, context=None):
        self.calls.append((hypothesis, context))
        if hypothesis == self.fail_on:
            raise RuntimeError("boom")
        return {"probability": 0.7, "direction": "up", "source": "codex", "cost_usd": 0.01}


# --------------------------------------------------------------------------- #
def test_disabled_by_default_is_noop():
    engine = _FakeEngine([_row("h1", 0.9)])
    with patch("intelligence.hermes.HermesAgent") as MockAgent:
        out = llm_review_active_hypotheses(engine)  # no override -> settings default False
    assert out == {"enabled": False, "reviewed": 0}
    MockAgent.assert_not_called()  # never even imported/constructed


def test_enabled_reviews_top_n_and_writes_report(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HYPO_LLM_OUTDIR", str(tmp_path))
    engine = _FakeEngine([_row("h1", 0.9), _row("h2", 0.8)])
    agent = _Agent()
    with patch("intelligence.hermes.HermesAgent", return_value=agent):
        out = llm_review_active_hypotheses(engine, enabled=True, limit=5)

    assert out["reviewed"] == 2
    assert out["errors"] == 0
    assert out["by_source"] == {"codex": 2}
    assert out["total_cost_usd"] == pytest.approx(0.02)
    assert engine.captured["params"] == {"limit": 5}          # top-N bound applied
    assert agent.calls[0][0] == "T"                            # thesis passed as the prompt
    assert "pattern_type" in agent.calls[0][1]                 # context assembled
    assert out["report_path"] and Path(out["report_path"]).exists()


def test_one_failure_is_counted_others_proceed(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HYPO_LLM_OUTDIR", str(tmp_path))
    engine = _FakeEngine([_row("h1", 0.9, thesis="BAD"), _row("h2", 0.8)])
    with patch("intelligence.hermes.HermesAgent", return_value=_Agent(fail_on="BAD")):
        out = llm_review_active_hypotheses(engine, enabled=True)
    assert out["errors"] == 1
    assert out["reviewed"] == 1
    assert out["total_cost_usd"] == pytest.approx(0.01)


def test_no_active_hypotheses_is_clean_noop():
    out = llm_review_active_hypotheses(_FakeEngine([]), enabled=True)
    assert out == {"enabled": True, "reviewed": 0}


def test_select_failure_degrades_gracefully():
    class _BoomEngine:
        def connect(self):
            raise RuntimeError("db down")

    out = llm_review_active_hypotheses(_BoomEngine(), enabled=True)
    assert out["reviewed"] == 0 and out["errors"] == 1  # never raises into the caller
