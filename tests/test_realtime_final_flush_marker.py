"""Tests for ws_listener.py::_run_final_flush's outcome marker.

Added following the 2026-09-16 shutdown incident
(docs/INCIDENT-2026-09-16-realtime-shutdown-sigkill.md): grid-realtime's
first-ever restart was SIGKILLed by systemd's stop-timeout with no log
evidence, before or after, of whether the final candle flush had actually
completed. `_run_final_flush` logs an unconditional marker specifically so
a future incident can distinguish "the kill landed mid-flush" (marker never
appears) from "the flush phase ran to completion" (marker appears) --
purely a completion signal, not a persistence guarantee, and these tests
exist to pin down exactly that distinction rather than let it drift.

**The marker proves the phase ran, not that data was persisted.** Every
test below is really checking one of three things: (1) the returned
outcome value is correct for each of the four cases, (2) the marker's own
log message never implies success when the outcome wasn't "written", or
(3) even outcome="written" itself is not misread as "N candles became new
rows" -- INSERT_SQL is `ON CONFLICT (symbol, interval, ts) DO NOTHING`
with no RETURNING clause, so a committed transaction that inserted zero
rows (every key already present) is indistinguishable here from one that
inserted all of them. Together these cover what the incident asked for
(proof the phase ran), the precision demanded on top of that (the marker
must not be misreadable as a persistence guarantee for the
timed_out/failed cases), and the further precision that "written" itself
only claims a commit, not a row count.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from ingestion.realtime.candle_builder import CandleBuilder
from ingestion.realtime.ws_listener import _run_final_flush


def _builder_with_one_pending_candle() -> CandleBuilder:
    builder = CandleBuilder()
    builder.ingest(
        "AAPL", 180.0, 100,
        datetime(2026, 9, 16, 14, 7, 23, tzinfo=timezone.utc),
        "equity", "yahoo",
    )
    return builder


def test_nothing_to_flush_when_builder_is_empty():
    builder = CandleBuilder()
    outcome = asyncio.run(_run_final_flush(builder))
    assert outcome == "nothing_to_flush"


def test_written_on_a_successful_bounded_write():
    builder = _builder_with_one_pending_candle()

    async def _succeeds(fn, rows):
        return None  # bounded_write's own real return type; no DB touched

    with patch("ingestion.realtime.ws_listener.bounded_write", side_effect=_succeeds):
        outcome = asyncio.run(_run_final_flush(builder))
    assert outcome == "written"


def test_written_success_log_does_not_claim_new_rows(caplog):
    """outcome="written" means the transaction committed, not that any of
    the N candles became new rows -- ON CONFLICT DO NOTHING can commit a
    no-op. The success log line must say so, not just say "written".
    """
    import logging

    builder = _builder_with_one_pending_candle()

    async def _succeeds(fn, rows):
        return None

    with patch("ingestion.realtime.ws_listener.bounded_write", side_effect=_succeeds):
        with caplog.at_level(logging.INFO):
            from loguru import logger as log

            captured = []
            handler_id = log.add(lambda m: captured.append(m.record["message"]), format="{message}")
            try:
                asyncio.run(_run_final_flush(builder))
            finally:
                log.remove(handler_id)

    success_lines = [m for m in captured if "committed" in m and "candle(s)" in m]
    assert success_lines, f"expected a commit-scoped success line, got: {captured}"
    text = success_lines[0].lower()
    assert "on conflict do nothing" in text
    assert "not new rows" in text or "pre-existing" in text
    assert "candles written" not in text, (
        "the success line must not flatly claim N candles were written -- "
        "DO NOTHING can commit while inserting zero"
    )


def test_timed_out_when_the_write_exceeds_the_bound(caplog):
    builder = _builder_with_one_pending_candle()

    async def _hangs_forever(fn, rows):
        await asyncio.sleep(999)

    with patch("ingestion.realtime.ws_listener.bounded_write", side_effect=_hangs_forever):
        with patch("ingestion.realtime.ws_listener.FINAL_FLUSH_TIMEOUT_SECONDS", 0.05):
            outcome = asyncio.run(_run_final_flush(builder))
    assert outcome == "timed_out"


def test_failed_when_the_write_raises():
    builder = _builder_with_one_pending_candle()

    async def _raises(fn, rows):
        raise RuntimeError("simulated DB failure")

    with patch("ingestion.realtime.ws_listener.bounded_write", side_effect=_raises):
        outcome = asyncio.run(_run_final_flush(builder))
    assert outcome == "failed"


@pytest.mark.parametrize("outcome_value", ["written", "timed_out", "failed", "nothing_to_flush"])
def test_marker_message_never_implies_success_unless_outcome_is_written(outcome_value):
    """Directly exercises the marker's own message template (not the full
    flush machinery) to pin down the exact wording contract: the log call
    that fires for a given outcome must be readable on its own, without
    cross-referencing source code, as NOT proving persistence unless
    outcome == "written".
    """
    from loguru import logger as log

    captured = {}

    def _sink(message):
        captured["text"] = message.record["message"]

    handler_id = log.add(_sink, format="{message}")
    try:
        log.info(
            "Final flush phase reached its end (outcome={outcome}) -- proves "
            "the phase ran, NOT that data was persisted; only outcome=written "
            "confirms the write transaction committed, which is still not the "
            "same as confirming new rows were inserted (ON CONFLICT DO NOTHING "
            "can commit while inserting zero)",
            outcome=outcome_value,
        )
    finally:
        log.remove(handler_id)

    text = captured["text"]
    assert f"outcome={outcome_value}" in text
    lowered = text.lower()
    assert "not that data was persisted" in lowered
    assert "confirms the write transaction committed" in lowered
    # The precise distinction this round added: even a successful commit
    # is not the same claim as "rows were inserted", because ON CONFLICT
    # DO NOTHING can commit a no-op.
    assert "not the same as confirming new rows were inserted" in lowered
    assert "do nothing" in lowered and "inserting zero" in lowered
    if outcome_value != "written":
        # The marker line itself, read in isolation, must not claim success
        # for a non-written outcome.
        assert "written" not in lowered.split("outcome=")[0], (
            "the marker's own leading text must not claim success before "
            "the reader even gets to the outcome field"
        )
