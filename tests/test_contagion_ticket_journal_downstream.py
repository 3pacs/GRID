"""Downstream consequences of how a null-confidence ticket is journaled.

History of this file, in three steps:

1. Batch 3b found ``write_ticket_to_journal`` writing a fabricated 0.5
   ``state_confidence`` into the immutable ``decision_journal`` for tickets
   with no backtest history.
2. PR #539 stopped that by SKIPPING the journal row. This file originally
   asked "does skipping starve anything downstream?" and answered no.
3. The operator's call: silence is not honesty either. Revision
   ``journal_unscored_conf_0918`` makes ``state_confidence`` nullable with a
   mandatory ``confidence_reason``, and the ticket now writes an explicit
   UNSCORED audit record instead of vanishing.

So the question this file answers has changed. It is no longer "what does the
missing row cost?" but "the row is back — is it honest, and does its return
break the two things that could plausibly care?"

The two independence findings from step 2 still hold and are still worth
pinning, because they are what makes step 3 safe:

1. The accuracy history that later *gives* tickets a confidence comes from
   ``contagion_backtest_results``, never from ``decision_journal`` — so there
   is no "no journal row -> no history -> no confidence" feedback loop, and
   equally no risk that the new unscored rows feed back into their own inputs.
2. Ticket close-out (``finalize_ticket``) is keyed on the ticket id and never
   touches the journal, so it behaves identically whether or not a row exists.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from trading import contagion_to_ticket as ctt


def _engine_with_accuracy(accuracy):
    engine = MagicMock()
    conn = MagicMock()
    seen: list[str] = []

    def execute(sql, params=None):
        sql_str = str(sql).lower()
        seen.append(sql_str)
        result = MagicMock()
        result.fetchone.return_value = accuracy if "from contagion_backtest_results" in sql_str else None
        result.fetchall.return_value = []
        return result

    conn.execute.side_effect = execute
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine, seen


def test_accuracy_history_comes_from_backtest_results_not_the_journal():
    engine, seen = _engine_with_accuracy((0.62, 14))
    acc, n = ctt._load_contagion_accuracy(engine, "supply_disruption")
    assert (acc, n) == (0.62, 14)
    assert any("from contagion_backtest_results" in s for s in seen)
    assert not any("decision_journal" in s for s in seen)


def test_zero_history_is_reported_as_zero_not_invented():
    engine, _ = _engine_with_accuracy((None, 0))
    assert ctt._load_contagion_accuracy(engine, "supply_disruption") == (-1.0, 0)


def test_finalize_ticket_emits_outcome_without_a_journal_row():
    """An unjournaled ticket (string id, no journal_id) still closes out."""
    emitted = []

    fake_emit = MagicMock(side_effect=lambda evt, **kw: emitted.append(evt))
    fake_corr = MagicMock()
    corr_uuid = "0f9d4b1e-5c2a-4e3b-9a77-2d1c8e6f0a11"
    fake_corr.get_current_correlation_id.return_value = corr_uuid
    fake_corr.new_correlation_id.return_value = corr_uuid

    from contracts import schemas as real_schemas

    with patch.dict(
        "sys.modules",
        {
            "contracts.emit": MagicMock(emit=fake_emit),
            "contracts.correlation": fake_corr,
            "contracts.schemas": real_schemas,
        },
    ):
        ctt.finalize_ticket(
            MagicMock(),
            ticket_id="12:aapl:put",   # no journal row exists for this ticket
            pnl=-120.0,
            outcome="LOSS",
            ticker="AAPL",
            signals_used=["contagion", "iv"],
            duration_s=3600,
        )

    assert len(emitted) == 1, "close-out must not depend on a journal row"
    evt = emitted[0]
    assert getattr(evt, "trade_id", None) is not None
    assert getattr(evt, "ticker", "AAPL") == "AAPL"


def _unscored_ticket(**overrides):
    ticket = {
        "prediction_id": 12,
        "ticker": "aapl",
        "direction": "short",
        "instrument": "put",
        "strike": 180.0,
        "expiry": "2026-11-20",
        "kelly_size": 0.0,
        "shock_type": "supply_disruption",
        "shock_node": "TSMC",
        "thesis": "margin compression",
        "confidence": None,
        "confidence_basis": ctt.CONFIDENCE_BASIS_NO_HISTORY,
        "confidence_n": 0,
    }
    ticket.update(overrides)
    return ticket


def _capture_journal(monkeypatch):
    """Patch DecisionJournal and return the dict its kwargs land in."""
    captured: dict = {}

    class FakeJournal:
        def __init__(self, db_engine=None):
            pass

        def log_decision(self, **kw):
            captured.update(kw)
            return 9001

    monkeypatch.setattr("journal.log.DecisionJournal", FakeJournal)
    monkeypatch.setattr(ctt, "_get_default_model_version_id", lambda engine: 1)
    return captured


def test_null_confidence_ticket_is_recorded_not_skipped(monkeypatch):
    """The skip is gone: the audit record exists, with a NULL and a reason."""
    captured = _capture_journal(monkeypatch)

    journal_id = ctt.write_ticket_to_journal(MagicMock(), _unscored_ticket())

    assert journal_id == 9001, "an unscored ticket must still leave an audit row"
    assert captured["state_confidence"] is None
    assert captured["confidence_reason"]


def test_the_unscored_record_carries_no_number_anywhere(monkeypatch):
    """Not 0.5, not 0.0, not 1.0 — the whole point of the change."""
    captured = _capture_journal(monkeypatch)
    ctt.write_ticket_to_journal(MagicMock(), _unscored_ticket())

    assert captured["state_confidence"] is None
    # kelly_size is genuinely 0.0 for an unscored ticket (no size was taken),
    # so transition_probability being 0.0 is a measurement, not a placeholder.
    assert captured["transition_probability"] == 0.0
    reason = captured["confidence_reason"]
    assert "unscored" in reason
    assert ctt.CONFIDENCE_BASIS_NO_HISTORY in reason
    assert "n=0" in reason
    assert "supply_disruption" in reason


def test_operator_confidence_is_unscored_not_low_for_an_unscored_ticket(monkeypatch):
    """A categorical NOT NULL column still has to be filled; the category is
    UNSCORED, because LOW would be an assessment nobody made."""
    captured = _capture_journal(monkeypatch)
    ctt.write_ticket_to_journal(MagicMock(), _unscored_ticket())
    assert captured["operator_confidence"] == "UNSCORED"


def test_a_scored_ticket_is_unaffected(monkeypatch):
    captured = _capture_journal(monkeypatch)
    ctt.write_ticket_to_journal(
        MagicMock(),
        _unscored_ticket(
            confidence=0.62,
            confidence_basis=ctt.CONFIDENCE_BASIS_BACKTEST,
            confidence_n=14,
            kelly_size=0.05,
        ),
    )
    assert captured["state_confidence"] == 0.62
    assert captured["operator_confidence"] == "MEDIUM"
    assert "scored" in captured["confidence_reason"]
