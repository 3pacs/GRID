"""Wave 3 triage report item #20: hypothesis_registry/validation_results are
noise-generator output (autoresearch paused, Hermes hypothesis scoring/
discovery/review held) with no trial ledger, FDR correction or holdout.

``GET /discovery/hypotheses/results`` and ``GET /discovery/hypotheses`` must
never let a PASSED verdict or a correlation/Sharpe figure read as a
validated finding — both responses now carry an explicit
``research_lane_off`` flag and a ``note`` explaining why, so a caller (the
PWA or otherwise) never has to infer that from silence.

This does not touch ``/discovery/results/orthogonality`` or
``/discovery/results/clustering`` (PR #691's on-demand, staleness-guarded
jobs) — those are a different, still-in-scope feature.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from api.routers import discovery as d


def _engine(rows):
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = rows
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine


def _hypothesis_result_row(**over):
    row = {
        "id": 1, "statement": "X leads Y", "state": "PASSED", "layer": "TACTICAL",
        "feature_ids": [1, 2], "lag_structure": {"lag": 3},
        "created_at": None, "updated_at": None,
        "full_period_metrics": '{"correlation": 0.91, "sharpe": 10.0}',
        "overall_verdict": "PASS", "run_timestamp": None,
    }
    row.update(over)
    return type("Row", (), {"_mapping": row})()


class TestHypothesisResultsResearchLaneOff:
    @patch.object(d, "get_db_engine")
    def test_response_carries_research_lane_off_note(self, mock_db):
        mock_db.return_value = _engine([_hypothesis_result_row()])
        out = d.get_hypothesis_results(
            verdict=None, sector=None, min_correlation=0.0, limit=50, _token="t",
        )
        assert out["research_lane_off"] is True
        assert "noise-generator" in out["note"]
        assert out["count"] == 1

    @patch.object(d, "get_db_engine")
    def test_note_present_even_with_zero_results(self, mock_db):
        mock_db.return_value = _engine([])
        out = d.get_hypothesis_results(
            verdict=None, sector=None, min_correlation=0.0, limit=50, _token="t",
        )
        assert out["research_lane_off"] is True
        assert out["count"] == 0

    @patch.object(d, "get_db_engine")
    def test_passed_verdict_is_still_returned_but_labeled(self, mock_db):
        # The raw data isn't hidden from API consumers (the PWA now shows an
        # honest summary instead of per-row findings) — but it must always
        # travel with the research_lane_off label.
        mock_db.return_value = _engine([_hypothesis_result_row(state="PASSED")])
        out = d.get_hypothesis_results(
            verdict=None, sector=None, min_correlation=0.0, limit=50, _token="t",
        )
        assert out["results"][0]["state"] == "PASSED"
        assert out["research_lane_off"] is True


class TestHypothesesListResearchLaneOff:
    @patch.object(d, "get_db_engine")
    def test_response_carries_research_lane_off_note(self, mock_db):
        row = type("Row", (), {"_mapping": {
            "id": 1, "statement": "X leads Y", "state": "PASSED",
            "created_at": None, "updated_at": None,
        }})()
        mock_db.return_value = _engine([row])
        out = d.get_hypotheses(state=None, limit=100, offset=0, _token="t")
        assert out["research_lane_off"] is True
        assert "noise-generator" in out["note"]
        assert len(out["hypotheses"]) == 1
