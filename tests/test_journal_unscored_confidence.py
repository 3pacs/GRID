"""The journal's third state: UNSCORED (state_confidence NULL + a reason).

Background
----------
``trading/contagion_to_ticket.py`` used to write a fabricated ``0.5``
``state_confidence`` for tickets whose shock type had no backtest history at
all. PR #539 stopped that by SKIPPING the journal row — which swapped a false
number for a missing audit record. Neither is honest.

Revision ``journal_unscored_conf_0918`` makes the honest option expressible:
``state_confidence IS NULL`` plus a mandatory ``confidence_reason``. These
tests pin the three rules that make it safe:

1. ``None`` is only ever accepted *with* a reason — never silently.
2. A real number keeps every bit of the old finite/0–1 validation.
3. Every reader treats an unscored row as unscored: excluded from scoring,
   rendered as "unscored"/``--``, sorted last. Never coerced to 0 or 0.5.
"""

from __future__ import annotations

import math
from unittest.mock import MagicMock

import pytest

from journal.log import DecisionJournal


def _kwargs(**overrides):
    base = {
        "model_version_id": 1,
        "inferred_state": "CONTAGION_SUPPLY_DISRUPTION",
        "state_confidence": 0.8,
        "transition_probability": 0.1,
        "contradiction_flags": {},
        "grid_recommendation": "BUY",
        "baseline_recommendation": "HOLD",
        "action_taken": "BUY",
        "counterfactual": "Would have held",
        "operator_confidence": "HIGH",
    }
    base.update(overrides)
    return base


@pytest.fixture
def journal(mock_engine):
    return DecisionJournal(db_engine=mock_engine)


# ── log_decision: the three states ────────────────────────────────────────


class TestLogDecisionUnscored:
    def test_none_with_a_reason_is_accepted_and_writes_null(
        self, journal, mock_engine
    ):
        conn = mock_engine.begin.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = (77,)

        entry_id = journal.log_decision(
            **_kwargs(
                state_confidence=None,
                confidence_reason="unscored: no contagion backtest history (n=0)",
            )
        )

        assert entry_id == 77
        params = conn.execute.call_args[0][1]
        # The whole point: a NULL goes to the database, not a number.
        assert params["sc"] is None
        assert params["cr"] == (
            "unscored: no contagion backtest history (n=0)"
        )

    def test_none_without_a_reason_is_a_valueerror(self, journal):
        with pytest.raises(ValueError, match="confidence_reason"):
            journal.log_decision(**_kwargs(state_confidence=None))

    @pytest.mark.parametrize("blank", ["", "   ", "\t\n"])
    def test_blank_reason_does_not_count_as_a_reason(self, journal, blank):
        with pytest.raises(ValueError, match="confidence_reason"):
            journal.log_decision(
                **_kwargs(state_confidence=None, confidence_reason=blank)
            )

    def test_nothing_is_invented_for_the_unscored_row(self, journal, mock_engine):
        """Regression guard for the fabricated 0.5 this whole change exists to kill."""
        conn = mock_engine.begin.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = (1,)

        journal.log_decision(
            **_kwargs(state_confidence=None, confidence_reason="unscored: n=0")
        )

        params = conn.execute.call_args[0][1]
        assert params["sc"] is not None or params["sc"] is None
        assert params["sc"] not in (0.0, 0.5, 1.0)
        assert params["sc"] is None


class TestLogDecisionNumericPathUnchanged:
    def test_a_real_number_is_still_written(self, journal, mock_engine):
        conn = mock_engine.begin.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = (5,)

        assert journal.log_decision(**_kwargs(state_confidence=0.62)) == 5
        assert conn.execute.call_args[0][1]["sc"] == 0.62

    def test_zero_is_a_measurement_not_an_absence(self, journal, mock_engine):
        """0.0 must survive: it means "measured, no confidence", not "unscored"."""
        conn = mock_engine.begin.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = (6,)

        journal.log_decision(
            **_kwargs(state_confidence=0.0, confidence_reason=None)
        )
        assert conn.execute.call_args[0][1]["sc"] == 0.0

    @pytest.mark.parametrize(
        "bad", [float("nan"), math.nan, float("inf"), float("-inf")]
    )
    def test_non_finite_still_rejected(self, journal, bad):
        with pytest.raises(ValueError, match="state_confidence"):
            journal.log_decision(**_kwargs(state_confidence=bad))

    @pytest.mark.parametrize("bad", [-0.01, 1.01, 5.0, -3.0])
    def test_out_of_range_still_rejected(self, journal, bad):
        with pytest.raises(ValueError, match="between 0 and 1"):
            journal.log_decision(**_kwargs(state_confidence=bad))

    def test_reason_is_optional_when_a_number_is_present(self, journal, mock_engine):
        conn = mock_engine.begin.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = (9,)

        journal.log_decision(**_kwargs(state_confidence=0.4))
        assert conn.execute.call_args[0][1]["cr"] is None


# ── Reader null-safety ────────────────────────────────────────────────────


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


class TestJournalCreateSchema:
    def test_null_confidence_requires_a_reason(self):
        from pydantic import ValidationError

        from api.schemas.journal import JournalEntryCreate

        payload = {
            "model_version_id": 1,
            "inferred_state": "GROWTH",
            "state_confidence": None,
            "transition_probability": 0.1,
            "grid_recommendation": "BUY",
            "baseline_recommendation": "HOLD",
            "action_taken": "BUY",
            "counterfactual": "x",
            "operator_confidence": "LOW",
        }
        with pytest.raises(ValidationError, match="confidence_reason"):
            JournalEntryCreate(**payload)

        ok = JournalEntryCreate(**{**payload, "confidence_reason": "unscored: n=0"})
        assert ok.state_confidence is None
        assert ok.confidence_reason == "unscored: n=0"

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


# ── The contagion writer: the record exists now, and carries its reason ───


class TestContagionTicketWritesTheUnscoredRecord:
    def test_unscored_ticket_is_journaled_with_a_reason(self, monkeypatch):
        from trading import contagion_to_ticket as ctt

        captured: dict = {}

        class FakeJournal:
            def __init__(self, db_engine=None):
                pass

            def log_decision(self, **kw):
                captured.update(kw)
                return 4242

        monkeypatch.setattr("journal.log.DecisionJournal", FakeJournal)
        monkeypatch.setattr(
            ctt, "_get_default_model_version_id", lambda engine: 1
        )

        ticket = {
            "ticker": "aapl",
            "direction": "short",
            "instrument": "put",
            "strike": 180.0,
            "expiry": "2026-11-20",
            "kelly_size": 0.0,
            "shock_type": "supply_disruption",
            "confidence": None,
            "confidence_basis": ctt.CONFIDENCE_BASIS_NO_HISTORY,
            "confidence_n": 0,
            "thesis": "t",
        }

        journal_id = ctt.write_ticket_to_journal(MagicMock(), ticket)

        # #539 returned None here. The audit record now exists.
        assert journal_id == 4242
        assert captured["state_confidence"] is None
        reason = captured["confidence_reason"]
        assert "unscored" in reason
        assert ctt.CONFIDENCE_BASIS_NO_HISTORY in reason
        assert "n=0" in reason
        # And absolutely no invented number or assessment anywhere near it.
        assert captured["operator_confidence"] == "UNSCORED"

    def test_scored_ticket_still_clamps_and_journals_a_number(self, monkeypatch):
        from trading import contagion_to_ticket as ctt

        captured: dict = {}

        class FakeJournal:
            def __init__(self, db_engine=None):
                pass

            def log_decision(self, **kw):
                captured.update(kw)
                return 7

        monkeypatch.setattr("journal.log.DecisionJournal", FakeJournal)
        monkeypatch.setattr(
            ctt, "_get_default_model_version_id", lambda engine: 1
        )

        ticket = {
            "ticker": "msft",
            "direction": "long",
            "instrument": "call",
            "strike": 400.0,
            "expiry": "2026-11-20",
            "kelly_size": 0.05,
            "shock_type": "supply_disruption",
            "confidence": 1.4,  # out of range on purpose
            "confidence_basis": ctt.CONFIDENCE_BASIS_BACKTEST,
            "confidence_n": 25,
            "thesis": "t",
        }

        assert ctt.write_ticket_to_journal(MagicMock(), ticket) == 7
        assert captured["state_confidence"] == 1.0  # clamp preserved
        assert captured["operator_confidence"] == "HIGH"



class TestUnscoredOperatorCategory:
    def test_log_decision_accepts_unscored_category(self, journal, mock_engine):
        conn = mock_engine.begin.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = (5,)
        journal.log_decision(
            model_version_id=1,
            inferred_state="supply_disruption",
            state_confidence=None,
            confidence_reason="unscored: no backtest history (n=0)",
            transition_probability=0.1,
            contradiction_flags={},
            grid_recommendation="HOLD",
            baseline_recommendation="HOLD",
            action_taken="HOLD",
            counterfactual="",
            operator_confidence="UNSCORED",
        )
        params = conn.execute.call_args[0][1]
        assert params["oc"] == "UNSCORED"
        assert params["sc"] is None

    def test_api_schema_accepts_unscored_category(self):
        from api.schemas.journal import JournalEntryCreate

        entry = JournalEntryCreate(
            model_version_id=1,
            inferred_state="x",
            state_confidence=None,
            confidence_reason="unscored: no history",
            transition_probability=0.0,
            grid_recommendation="HOLD",
            baseline_recommendation="HOLD",
            action_taken="HOLD",
            counterfactual="",
            operator_confidence="UNSCORED",
        )
        assert entry.operator_confidence == "UNSCORED"

    def test_schema_sql_and_migration_agree_on_the_four_categories(self):
        import re
        from pathlib import Path

        root = Path(__file__).resolve().parents[1]
        schema = (root / "schema.sql").read_text(encoding="utf-8")
        mig = (
            root / "migrations" / "versions" / "journal_unscored_confidence_0918.py"
        ).read_text(encoding="utf-8")
        want = "'LOW', 'MEDIUM', 'HIGH', 'UNSCORED'"
        assert want in re.sub(r"\s+", " ", schema)
        assert want in mig
        # The downgrade never UPDATEs or DELETEs the journal; the upgrade
        # never UPDATEs it either.
        body = mig[mig.index("def upgrade"):]
        assert "UPDATE decision_journal" not in body
        assert "DELETE FROM decision_journal" not in body
