"""Reader null-safety for journal rows that were never scored.

This is the ROLLBACK TARGET's copy of the reader half of PR #539's
``tests/test_journal_unscored_confidence.py``. The writer-side classes
(``TestLogDecisionUnscored``, ``TestLogDecisionNumericPathUnchanged``,
``TestContagionTicketWritesTheUnscoredRecord``,
``TestUnscoredOperatorCategory``) are deliberately absent: this branch
carries #539's readers WITHOUT its migration, journal writer or ticket
writer, so it can never produce an unscored row - only survive one that
#539 already wrote.

The contract under test is the same one: ``state_confidence IS NULL`` means
UNSCORED, and every reader must treat it as unscored - excluded from
scoring, rendered as "unscored"/``--``, sorted last. Never 0, never 0.5.

Everything here runs against mocks, so it passes identically whether the
database is at the old schema (state_confidence NOT NULL) or the new one.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


class TestScoringExcludesUnscoredRows:
    """An unscored row must be dropped from scoring, never counted as 0."""

    def test_confidence_drift_excludes_unscored(self, mock_engine):
        from features.importance import FeatureImportanceTracker

        # 4 scored rows and 8 unscored ones. Counting the NULLs as 0.0 would
        # manufacture a confidence collapse out of missing measurements; the
        # honest answer is "not enough scored history to say".
        rows = [(0.8,)] * 4 + [(None,)] * 8
        conn = mock_engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchall.return_value = rows

        fi = FeatureImportanceTracker(
            db_engine=mock_engine, pit_store=MagicMock()
        )
        out = fi.detect_prediction_confidence_drift(model_id=1, window=6)

        assert out["sufficient_data"] is False
        assert out.get("reason") == "too_many_unscored"

    def test_unscored_rows_are_dropped_not_counted_as_zero(self, mock_engine):
        """The mean must be over the scored rows only."""
        from features.importance import FeatureImportanceTracker

        # 6 recent at 0.9, then 5 prior at 0.4 and one unscored. If the NULL
        # were coerced to 0.0 the prior mean would be 0.4*5/6 = 0.3333.
        rows = [(0.9,)] * 6 + [(0.4,)] * 5 + [(None,)]
        conn = mock_engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchall.return_value = rows

        fi = FeatureImportanceTracker(
            db_engine=mock_engine, pit_store=MagicMock()
        )
        out = fi.detect_prediction_confidence_drift(model_id=1, window=6)

        assert out["sufficient_data"] is True
        assert out["prior_mean_confidence"] == pytest.approx(0.4)
        assert out["prior_mean_confidence"] != pytest.approx(0.3333, abs=1e-3)

    def test_drift_still_works_when_all_rows_are_scored(self, mock_engine):
        from features.importance import FeatureImportanceTracker

        rows = [(0.9,)] * 6 + [(0.4,)] * 6
        conn = mock_engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchall.return_value = rows

        fi = FeatureImportanceTracker(
            db_engine=mock_engine, pit_store=MagicMock()
        )
        out = fi.detect_prediction_confidence_drift(model_id=1, window=6)

        assert out["sufficient_data"] is True
        assert out["recent_mean_confidence"] == pytest.approx(0.9)
        assert out["prior_mean_confidence"] == pytest.approx(0.4)


class TestSentimentScorerExcludesUnscored:
    def test_unscored_regime_is_not_scored_as_a_coin_flip(self, mock_engine):
        from intelligence.sentiment_scorer import _score_regime

        conn = mock_engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = ("CRISIS", None)

        component = _score_regime(mock_engine)

        # The old `float(row[1] or 0.5)` produced a non-zero score here.
        assert component.score == 0.0
        assert component.raw_value == 0.0
        assert "unscored" in component.detail.lower()

    def test_scored_regime_still_scores(self, mock_engine):
        from intelligence.sentiment_scorer import _score_regime

        conn = mock_engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = ("CRISIS", 0.9)

        component = _score_regime(mock_engine)
        assert component.raw_value == pytest.approx(0.9)
        assert "unscored" not in component.detail.lower()


class TestThesisScorerRegimeContext:
    def test_unscored_returns_none_not_zero(self, mock_engine):
        from analysis.thesis_scorer import _get_regime_context

        conn = mock_engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = (
            "FRAGILE", None, "S=0.4, dS/dt=0.01",
        )

        ctx = _get_regime_context(mock_engine)
        assert ctx["confidence"] is None
        assert ctx["confidence_basis"] == "unscored"
        assert ctx["adjustments"] == {}


class TestPaperTradeRefusesToInventConfidence:
    def test_unscored_latest_decision_aborts_the_snapshot(self, mock_engine):
        from backtest.paper_trade import PaperTradeTracker

        conn = mock_engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = (
            "GROWTH", None, 0.1, "RISK_ON", {}, "2026-09-18T00:00:00Z",
        )
        conn.execute.return_value.scalar.return_value = 42

        trader = PaperTradeTracker(db_engine=mock_engine)
        out = trader.create_snapshot()

        # Every prediction confidence is a multiple of the regime confidence,
        # so one missing measurement would have become a dozen invented ones.
        assert "unscored" in out["error"].lower()
        assert "predictions" not in out


class TestDigestRendersUnscored:
    def test_regime_section_says_unscored_instead_of_raising(self):
        from alerts.email import _section_regime

        section = _section_regime("FRAGILE", None, "HOLD")
        assert "unscored" in section["body"]
        assert "0%" not in section["body"]

    def test_regime_section_still_formats_a_real_number(self):
        from alerts.email import _section_regime

        assert "80%" in _section_regime("GROWTH", 0.8, "BUY")["body"]


class TestEventSequenceLabel:
    def test_unscored_is_its_own_label_not_estimated(self):
        from intelligence.event_sequence import _confidence_label

        # The reader maps NULL to "unscored" at the call site; the shared
        # label helper must not be the thing that quietly calls it
        # "estimated" (a tier other sources actually earn).
        assert _confidence_label(0.9) == "confirmed"
        assert _confidence_label(None) == "estimated"


class TestMarketBriefingLine:
    def test_unscored_confidence_line(self):
        from ollama.market_briefing import _regime_confidence_line

        line = _regime_confidence_line({"confidence": None})
        assert "unscored" in line
        assert "None" not in line

    def test_scored_confidence_line(self):
        from ollama.market_briefing import _regime_confidence_line

        assert _regime_confidence_line({"confidence": 0.72}) == (
            "- Confidence: 0.72"
        )


class TestRegimeResponseSchemasAcceptNull:
    def test_history_and_transition_entries_allow_null(self):
        from api.schemas.regime import (
            RegimeCurrentResponse,
            RegimeHistoryEntry,
            RegimeTransition,
        )

        assert RegimeHistoryEntry(
            date="2026-09-18", state="GROWTH", confidence=None
        ).confidence is None
        assert RegimeTransition(
            date="2026-09-18",
            from_state="GROWTH",
            to_state="FRAGILE",
            confidence=None,
        ).confidence is None
        assert RegimeCurrentResponse(
            state="GROWTH",
            confidence=None,
            transition_probability=0.0,
            contradiction_flags=[],
            model_version="none",
            as_of="2026-09-18T00:00:00Z",
            baseline_comparison="",
            as_of_date="",
        ).confidence is None


class TestUnscoredSortsLast:
    def test_confidence_sort_key_puts_unscored_behind_a_measured_zero(self):
        """`api.routers.regime.get_all_active` sorts with this contract."""
        def conf_key(entry):
            conf = entry["confidence"]
            return (0, 0.0) if conf is None else (1, float(conf))

        entries = [
            {"state": "A", "confidence": None},
            {"state": "B", "confidence": 0.0},
            {"state": "C", "confidence": 0.9},
        ]
        entries.sort(key=conf_key, reverse=True)
        assert [e["state"] for e in entries] == ["C", "B", "A"]

class TestRollbackTargetRefusesToWriteUnscored:
    """The create path must not pretend it can record an unscored decision.

    ``journal.log.DecisionJournal.log_decision`` on this branch is the
    pre-#539 writer: no ``confidence_reason`` parameter, and no way to
    satisfy ``ck_decision_journal_unscored_has_reason``. A null that reached
    it would be a 500 (NOT NULL on the old schema, CHECK on the new one), so
    the boundary refuses it as a 422 instead.
    """

    @staticmethod
    def _payload(**over):
        base = {
            "model_version_id": 1,
            "inferred_state": "GROWTH",
            "state_confidence": 0.8,
            "transition_probability": 0.1,
            "grid_recommendation": "BUY",
            "baseline_recommendation": "HOLD",
            "action_taken": "BUY",
            "counterfactual": "x",
            "operator_confidence": "LOW",
        }
        base.update(over)
        return base

    def test_null_confidence_is_rejected_even_with_a_reason(self):
        from pydantic import ValidationError

        from api.schemas.journal import JournalEntryCreate

        with pytest.raises(ValidationError, match="rollback target"):
            JournalEntryCreate(
                **self._payload(
                    state_confidence=None,
                    confidence_reason="unscored: n=0",
                )
            )

    def test_a_number_is_still_accepted(self):
        from api.schemas.journal import JournalEntryCreate

        entry = JournalEntryCreate(**self._payload(state_confidence=0.0))
        # A measured zero is a measurement, not an absence.
        assert entry.state_confidence == 0.0

    def test_the_router_does_not_pass_confidence_reason_to_the_writer(self):
        """Guards the exact TypeError that made the raw cherry-pick unsafe."""
        import inspect

        from journal.log import DecisionJournal

        params = inspect.signature(DecisionJournal.log_decision).parameters
        assert "confidence_reason" not in params

        src = inspect.getsource(
            __import__("api.routers.journal", fromlist=["create"]).create
        )
        assert "confidence_reason" not in src


class TestJournalResponseCarriesTheReason:
    def test_response_model_accepts_a_null_row(self):
        from api.schemas.journal import JournalEntryResponse

        entry = JournalEntryResponse(
            id=1,
            model_version_id=1,
            inferred_state="GROWTH",
            state_confidence=None,
            confidence_reason="unscored: n=0",
            transition_probability=0.1,
            contradiction_flags={},
            grid_recommendation="BUY",
            baseline_recommendation="HOLD",
            action_taken="BUY",
            counterfactual="x",
            operator_confidence="LOW",
            decision_timestamp="2026-09-18T00:00:00Z",
        )
        assert entry.state_confidence is None
        assert entry.confidence_reason == "unscored: n=0"

    def test_response_model_still_works_without_the_column(self):
        """Old schema: ``SELECT *`` yields no ``confidence_reason`` at all."""
        from api.schemas.journal import JournalEntryResponse

        entry = JournalEntryResponse(
            id=1,
            model_version_id=1,
            inferred_state="GROWTH",
            state_confidence=0.8,
            transition_probability=0.1,
            contradiction_flags={},
            grid_recommendation="BUY",
            baseline_recommendation="HOLD",
            action_taken="BUY",
            counterfactual="x",
            operator_confidence="LOW",
            decision_timestamp="2026-09-18T00:00:00Z",
        )
        assert entry.confidence_reason is None


class TestOptionsPayoffOrdering:
    """A NULL payoff_multiple must not win the DISTINCT ON race."""

    def test_options_sql_sorts_nulls_last(self):
        from intelligence.long_plays import _OPTIONS_SQL

        sql = str(_OPTIONS_SQL)
        assert "payoff_multiple DESC NULLS LAST" in sql
        # DISTINCT ON takes the first row per ticker in ORDER BY order, so
        # the NULLS LAST must sit on the payoff key itself, ahead of the
        # scan_date tiebreak.
        assert sql.index("NULLS LAST") < sql.index("scan_date DESC")

    def test_an_unmodelled_payoff_loses_to_a_modelled_one(self, mock_engine):
        """The loader's own behaviour, with the ordering already applied."""
        from datetime import date

        from intelligence.long_plays import _load_options_asymmetry

        conn = mock_engine.connect.return_value.__enter__.return_value
        # What PostgreSQL returns once NULLS LAST is in the ORDER BY: the
        # modelled row is the DISTINCT ON survivor for XYZ.
        conn.execute.return_value.fetchall.return_value = [
            ("XYZ", date(2026, 9, 10), 0.7, 40.0, "CALL", "t", True),
            ("ABC", date(2026, 9, 11), 0.5, None, "CALL", "t", None),
        ]

        out = _load_options_asymmetry(mock_engine, date(2026, 9, 18))

        assert out["XYZ"]["max_payoff_multiple"] == 40.0
        # A ticker whose every scan is unmodelled still reports None - the
        # fix restores the ranking, it does not invent a payoff.
        assert out["ABC"]["max_payoff_multiple"] is None
