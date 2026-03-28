"""
Tests for the GRID thesis tracker (intelligence/thesis_tracker.py).

Tests data classes, direction normalisation, root cause classification,
JSON parsing, snapshot creation, scoring logic, and post-mortem generation
using mocked database results.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch, call

import pytest

from intelligence.thesis_tracker import (
    ThesisSnapshot,
    ThesisPostMortem,
    ROOT_CAUSES,
    _parse_json,
    _normalise_direction,
    _classify_root_cause,
    snapshot_thesis,
    score_old_theses,
    generate_thesis_postmortem,
    get_thesis_history,
    get_thesis_accuracy,
    load_thesis_postmortems,
)


# ── Data Class Tests ─────────────────────────────────────────────────────


class TestThesisSnapshot:
    def test_defaults(self):
        snap = ThesisSnapshot(
            id=1,
            timestamp="2026-01-01T00:00:00",
            overall_direction="bullish",
            conviction=0.8,
            key_drivers=["rates"],
            risk_factors=["vol"],
            model_states={"flow_momentum": {"direction": "bullish"}},
            narrative="test",
        )
        assert snap.outcome is None
        assert snap.actual_market_move is None
        assert snap.scored_at is None

    def test_to_dict(self):
        snap = ThesisSnapshot(
            id=1,
            timestamp="2026-01-01T00:00:00",
            overall_direction="bearish",
            conviction=0.6,
            key_drivers=[],
            risk_factors=[],
            model_states={},
            narrative="n",
            outcome="correct",
            actual_market_move=-1.2,
            scored_at="2026-01-04T00:00:00",
        )
        d = snap.to_dict()
        assert d["id"] == 1
        assert d["outcome"] == "correct"
        assert d["actual_market_move"] == -1.2


class TestThesisPostMortem:
    def test_to_dict(self):
        pm = ThesisPostMortem(
            snapshot_id=1,
            thesis_direction="bullish",
            actual_direction="bearish",
            models_that_were_right=["regime_contrarian"],
            models_that_were_wrong=["flow_momentum"],
            what_we_missed="vol spike",
            root_cause="external_shock",
            lesson="weight contrarian models higher",
            generated_at="2026-01-05T00:00:00",
        )
        d = pm.to_dict()
        assert d["snapshot_id"] == 1
        assert d["root_cause"] == "external_shock"
        assert "regime_contrarian" in d["models_that_were_right"]


# ── Helper Tests ─────────────────────────────────────────────────────────


class TestParseJson:
    def test_dict_passthrough(self):
        assert _parse_json({"a": 1}) == {"a": 1}

    def test_list_passthrough(self):
        assert _parse_json(["x", "y"]) == ["x", "y"]

    def test_string_json(self):
        assert _parse_json('{"a": 1}') == {"a": 1}

    def test_none_default(self):
        assert _parse_json(None) == {}

    def test_none_custom_default(self):
        assert _parse_json(None, []) == []

    def test_bad_json(self):
        assert _parse_json("not json", []) == []


class TestNormaliseDirection:
    def test_bullish_aliases(self):
        for d in ("bullish", "CALL", "long", "Up", "BUY"):
            assert _normalise_direction(d) == "bullish"

    def test_bearish_aliases(self):
        for d in ("bearish", "PUT", "short", "Down", "SELL"):
            assert _normalise_direction(d) == "bearish"

    def test_neutral(self):
        assert _normalise_direction("neutral") == "neutral"
        assert _normalise_direction("unknown") == "neutral"
        assert _normalise_direction("") == "neutral"


class TestClassifyRootCause:
    def test_model_disagreement_ignored(self):
        rc = _classify_root_cause(
            direction="bullish",
            actual_direction="bearish",
            actual_move=-1.5,
            models_right=["a", "b", "c"],
            models_wrong=["d"],
            model_states={},
        )
        assert rc == "model_disagreement_ignored"

    def test_external_shock(self):
        rc = _classify_root_cause(
            direction="bullish",
            actual_direction="bearish",
            actual_move=-4.0,
            models_right=["a"],
            models_wrong=["b", "c"],
            model_states={},
        )
        assert rc == "external_shock"

    def test_correct_but_early(self):
        rc = _classify_root_cause(
            direction="bullish",
            actual_direction="bullish",
            actual_move=0.3,
            models_right=[],
            models_wrong=[],
            model_states={},
        )
        assert rc == "correct_but_early"

    def test_bad_data(self):
        rc = _classify_root_cause(
            direction="bullish",
            actual_direction="bearish",
            actual_move=-1.0,
            models_right=[],
            models_wrong=["a"],
            model_states={},
        )
        assert rc == "bad_data"

    def test_thesis_outdated(self):
        rc = _classify_root_cause(
            direction="bullish",
            actual_direction="bearish",
            actual_move=-1.0,
            models_right=["a"],
            models_wrong=["b", "c"],
            model_states={},
        )
        assert rc == "thesis_outdated"

    def test_all_root_causes_valid(self):
        """Verify ROOT_CAUSES list contains expected entries."""
        assert "model_disagreement_ignored" in ROOT_CAUSES
        assert "external_shock" in ROOT_CAUSES
        assert "bad_data" in ROOT_CAUSES
        assert "thesis_outdated" in ROOT_CAUSES
        assert "correct_but_early" in ROOT_CAUSES


# ── Snapshot Tests ────────────────────────────────────────────────────────


class TestSnapshotThesis:
    def test_snapshot_returns_id(self, mock_engine):
        mock_conn = mock_engine.begin.return_value.__enter__.return_value
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (42,)
        mock_conn.execute.return_value = mock_result

        thesis_data = {
            "overall_direction": "bullish",
            "conviction": 0.75,
            "key_drivers": ["rates falling"],
            "risk_factors": ["vol spike"],
            "model_states": {"flow_momentum": {"direction": "bullish", "confidence": 0.8}},
            "narrative": "Markets look strong.",
        }

        snap_id = snapshot_thesis(mock_engine, thesis_data)
        assert snap_id == 42

    def test_snapshot_defaults(self, mock_engine):
        mock_conn = mock_engine.begin.return_value.__enter__.return_value
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (1,)
        mock_conn.execute.return_value = mock_result

        snap_id = snapshot_thesis(mock_engine, {})
        assert snap_id == 1


# ── Scoring Tests ─────────────────────────────────────────────────────────


class TestScoreOldTheses:
    def test_scoring_empty(self, mock_engine):
        """No unscored snapshots returns empty results."""
        mock_conn_begin = mock_engine.begin.return_value.__enter__.return_value
        mock_conn_begin.execute.return_value = MagicMock(fetchall=MagicMock(return_value=[]))

        results = score_old_theses(mock_engine, lookback_days=7)
        assert results["correct"] == 0
        assert results["wrong"] == 0
        assert results["partial"] == 0


# ── History Tests ─────────────────────────────────────────────────────────


class TestGetThesisHistory:
    def test_empty_history(self, mock_engine):
        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        mock_conn.execute.return_value = MagicMock(fetchall=MagicMock(return_value=[]))

        snapshots = get_thesis_history(mock_engine, days=30)
        assert snapshots == []


# ── Post-Mortem Load Tests ────────────────────────────────────────────────


class TestLoadThesisPostmortems:
    def test_empty(self, mock_engine):
        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        mock_conn.execute.return_value = MagicMock(fetchall=MagicMock(return_value=[]))

        result = load_thesis_postmortems(mock_engine, days=30)
        assert result == []
