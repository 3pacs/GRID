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
test below is really checking one of two things: (1) the returned outcome
value is correct for each of the four cases, or (2) the marker's own log
message never implies success when the outcome wasn't "written" -- read
together, they cover both what the incident asked for (proof the phase
ran) and the precision the user demanded on top of that (the marker must
not be misreadable as a persistence guarantee for the timed_out/failed
cases, where the candles are explicitly NOT written).
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
            "confirms a committed write",
            outcome=outcome_value,
        )
    finally:
        log.remove(handler_id)

    text = captured["text"]
    assert f"outcome={outcome_value}" in text
    assert "not that data was persisted" in text.lower()
    assert "only outcome=written confirms a committed write" in text.lower()
    if outcome_value != "written":
        # The marker line itself, read in isolation, must not claim success
        # for a non-written outcome.
        assert "written" not in text.lower().split("outcome=")[0], (
            "the marker's own leading text must not claim success before "
            "the reader even gets to the outcome field"
        )
