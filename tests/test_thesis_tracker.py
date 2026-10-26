"""
Tests for the GRID thesis tracker (intelligence/thesis_tracker.py).

Tests data classes, direction normalisation, root cause classification,
JSON parsing, snapshot creation, scoring logic, and post-mortem generation
using mocked database results.
"""

from __future__ import annotations

from unittest.mock import MagicMock


from intelligence.thesis_tracker import (
    ThesisSnapshot,
    ThesisPostMortem,
    ROOT_CAUSES,
    THESIS_SCORING_HELD,
    _parse_json,
    _normalise_direction,
    _classify_root_cause,
    snapshot_thesis,
    score_old_theses,
    get_thesis_history,
    load_thesis_postmortems,
    run_thesis_cycle,
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

    def test_held_by_default_and_never_touches_the_engine(self, mock_engine):
        """Item #16 (Wave 3 triage report): score_old_theses has a real
        look-ahead bug (_get_spy_price_near resolves a mid-session
        snapshot's price against that SAME day's close once it exists) and
        is reachable from scripts/grid_cron.sh's "thesis-snapshot" job, not
        just the unscheduled run_thesis_cycle. THESIS_SCORING_HELD must
        default True and the function must short-circuit before any query."""
        assert THESIS_SCORING_HELD is True

        results = score_old_theses(mock_engine)

        assert results["held"] is True
        assert results["correct"] == 0
        assert results["wrong"] == 0
        assert results["partial"] == 0
        mock_engine.begin.assert_not_called()
        mock_engine.connect.assert_not_called()

    def test_unheld_via_monkeypatch_runs_the_real_query(self, monkeypatch, mock_engine):
        """Sanity check that the hold is a real gate, not dead code: with it
        patched off, the pre-existing query path still runs."""
        import intelligence.thesis_tracker as tt
        monkeypatch.setattr(tt, "THESIS_SCORING_HELD", False)
        mock_conn_begin = mock_engine.begin.return_value.__enter__.return_value
        mock_conn_begin.execute.return_value = MagicMock(fetchall=MagicMock(return_value=[]))

        results = tt.score_old_theses(mock_engine, lookback_days=7)

        assert "held" not in results
        mock_engine.begin.assert_called()  # _ensure_tables + the scoring query both use begin()


class TestRunThesisCycleHoldsPostmortems:
    def test_postmortem_generation_is_held_and_never_queried(self, monkeypatch, mock_engine):
        """Item #16: run_thesis_cycle step 3 (postmortem generation) is an
        LLM-narrated write over outcomes the (held) scorer produces -- the
        same standing 'postmortem write that feeds learning' category
        Hermes already holds for postmortem_batch. It must not run, or even
        query for candidate snapshots, while THESIS_SCORING_HELD is True."""
        import intelligence.thesis_tracker as tt

        monkeypatch.setattr(
            tt, "_ensure_tables", lambda engine: None,
        )
        monkeypatch.setattr(
            tt, "get_thesis_accuracy",
            lambda engine: {"overall": {"accuracy_pct": 0, "total_scored": 0}},
        )
        called = {"generate_thesis_postmortem": False}

        def _fail_if_called(*a, **kw):
            called["generate_thesis_postmortem"] = True
            raise AssertionError("generate_thesis_postmortem must not be called while held")

        monkeypatch.setattr(tt, "generate_thesis_postmortem", _fail_if_called)

        # Let the thesis-snapshot step fail naturally (no flow_thesis wiring
        # in this unit test) -- run_thesis_cycle catches that itself.
        report = tt.run_thesis_cycle(mock_engine)

        assert report["postmortems_held"] is True
        assert report["postmortems_generated"] == 0
        assert called["generate_thesis_postmortem"] is False


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
