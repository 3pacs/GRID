"""Wave 3 triage report item #21: surfacer hit-rate/Brier calibration
(surfacer_ticker_calibration, per_signal_brier_history,
regime_conditional_brier_history) is downstream of oracle_predictions
verdicts, i.e. the broken Hermes signal meter. This module does not repair
the meter — it only makes sure every place that carries a hit-rate/Brier
number also carries an honest label instead of presenting it as a live,
trustworthy track record.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from fastapi import Response

from api.routers import surfacer
from api.routers.surfacer import (
    SURFACER_CALIBRATION_NOTE,
    _fetch_track_record,
    _materialized_track_record,
    _merge_track_records,
    _scorecard_from_row,
)


def _row(**kwargs):
    return SimpleNamespace(_mapping=kwargs)


class TestScorecardFromRowCarriesNote:
    def test_note_present_on_every_scorecard(self):
        card = _scorecard_from_row(
            _row(horizon_days=7, scored_count=25, running_brier=0.2, running_ece=0.05,
                 hit_count=15, last_updated=datetime.now(timezone.utc)),
            "options_flow", 7, None,
        )
        assert card["note"] == SURFACER_CALIBRATION_NOTE


class TestFetchTrackRecordFallbackCarriesNote:
    class _Conn:
        def __init__(self, row):
            self._row = row

        def execute(self, *_a, **_kw):
            return SimpleNamespace(fetchone=lambda: self._row)


    def test_oracle_predictions_fallback_carries_note(self, monkeypatch):
        monkeypatch.setattr(surfacer, "_materialized_track_record", lambda *a, **k: None)
        monkeypatch.setattr(surfacer, "_table_exists", lambda conn, name: True)
        row = _row(samples=20, hits=10, partials=2, misses=8, avg_pnl_pct=1.0, avg_confidence=0.6)
        conn = self._Conn(row)

        record = _fetch_track_record(conn, "NVDA", "up")

        assert record["source"] == "oracle_predictions"
        assert record["note"] == SURFACER_CALIBRATION_NOTE


class TestMaterializedTrackRecordStaleness:
    class _Conn:
        def __init__(self, rows):
            self._rows = rows

        def execute(self, *_a, **_kw):
            return SimpleNamespace(fetchall=lambda: self._rows)

    def test_note_reports_the_newest_last_scored_date(self, monkeypatch):
        monkeypatch.setattr(surfacer, "_table_exists", lambda conn, name: True)
        older = datetime(2026, 4, 18, tzinfo=timezone.utc)
        newer = datetime(2026, 4, 20, tzinfo=timezone.utc)
        rows = [
            _row(ticker="NVDA", direction="up", horizon_days=7, regime=None, model_name=None,
                 prediction_type="CALL", samples=10, hits=6, partials=1, misses=3,
                 hit_rate=0.65, avg_pnl_pct=1.0, avg_confidence=0.6,
                 avg_expected_move_pct=2.0, avg_actual_move_pct=1.5,
                 brier=0.2, ece=0.05, first_seen=older, last_seen=older,
                 last_scored_at=older, volume_rank=1, dollar_volume=1e9),
            _row(ticker="NVDA", direction="up", horizon_days=7, regime=None, model_name=None,
                 prediction_type="CALL", samples=15, hits=9, partials=1, misses=5,
                 hit_rate=0.63, avg_pnl_pct=0.9, avg_confidence=0.55,
                 avg_expected_move_pct=2.1, avg_actual_move_pct=1.4,
                 brier=0.22, ece=0.06, first_seen=older, last_seen=newer,
                 last_scored_at=newer, volume_rank=2, dollar_volume=8e8),
        ]
        conn = self._Conn(rows)

        record = _materialized_track_record(conn, "NVDA", "up", 7, None, None)

        assert record is not None
        assert record["last_scored"] == newer.isoformat()
        assert "surfacer_ticker_calibration last scored" in record["note"]
        assert newer.isoformat() in record["note"]
        assert "broken and unrepaired" in record["note"]

    def test_no_table_means_no_record_and_no_fabricated_note(self, monkeypatch):
        monkeypatch.setattr(surfacer, "_table_exists", lambda conn, name: False)
        record = _materialized_track_record(self._Conn([]), "NVDA", "up", 7, None, None)
        assert record is None


class TestMergeTrackRecordsPropagatesNote:
    def test_ticker_record_note_survives_the_merge(self):
        ticker_record = {
            "samples": 10, "hit_rate": 0.6, "source": "surfacer_ticker_calibration",
            "note": "surfacer_ticker_calibration last scored 2026-04-18T00:00:00+00:00; " + SURFACER_CALIBRATION_NOTE,
        }
        merged = _merge_track_records(ticker_record, [])
        assert merged["note"] == ticker_record["note"]

    def test_signal_only_merge_still_carries_a_note(self):
        signal_cards = [{
            "signal_source": "options_flow", "horizon_days": 7, "samples": 20,
            "hit_rate": 0.55, "running_brier": 0.24, "contribution_weight": 1.0,
            "aggregate_fallback": False, "horizon_fallback": False,
        }]
        merged = _merge_track_records({"samples": 0}, signal_cards)
        assert merged["note"] == SURFACER_CALIBRATION_NOTE


class TestListCandidatesMetaAlwaysCarriesCalibrationNote:
    def test_meta_calibration_note_present_even_with_zero_candidates(self, monkeypatch):
        class _Engine:
            class _Tx:
                def __enter__(self):
                    return object()

                def __exit__(self, exc_type, exc, tb):
                    return False

            def begin(self):
                return self._Tx()

        monkeypatch.setattr(surfacer, "_fetch_oracle_candidates", lambda conn, limit: [])
        monkeypatch.setattr(surfacer, "_fetch_signal_candidates", lambda conn, limit: [])
        monkeypatch.setattr(surfacer, "_fetch_hypothesis_candidates", lambda conn, limit: [])
        monkeypatch.setattr(surfacer, "_attach_conviction", lambda conn, candidates: candidates)
        monkeypatch.setattr(surfacer, "_fetch_thesis_snapshot", lambda conn: None)
        monkeypatch.setattr(surfacer, "_select_candidates", lambda candidates, **kwargs: ([], {}))

        payload = surfacer.list_candidates(Response(), limit=5, queue_missing_data=False, engine=_Engine())

        assert payload["meta"]["calibration_note"] == SURFACER_CALIBRATION_NOTE
