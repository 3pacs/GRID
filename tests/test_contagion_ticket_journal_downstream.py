"""Downstream consequences of declining to journal a null-confidence ticket.

Batch 3b stopped ``write_ticket_to_journal`` from writing a fabricated 0.5
``state_confidence`` into the immutable ``decision_journal`` for tickets with
no backtest history. Review question: does skipping the journal row starve or
break anything downstream? Two things could plausibly depend on the row:

1. The accuracy history that later *gives* tickets a confidence. It is read
   from ``contagion_backtest_results`` (prediction-level scoring), never from
   ``decision_journal``, so the skip cannot create a "no journal row -> no
   history -> no confidence -> no journal row" deadlock.
2. Ticket close-out (``finalize_ticket``). It emits ``OptionsTradeOutcome``
   through the contracts layer keyed on the ticket id and never touches the
   journal, so an unjournaled ticket closes exactly like a journaled one.

The only thing lost is the audit row itself; that is the deliberate trade
(no fabricated number becomes permanent) and is flagged for the controller.
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
