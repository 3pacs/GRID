"""Regression tests for viz_intelligence.compute_source_weights log hygiene.

PUNCH-LIST-2026-05-13.md analysis/ [P2] line 151 called for the two silent
`except Exception: pass` blocks in `analysis/viz_intelligence.compute_source_weights`
to surface via `log.warning(...)` per CLAUDE.md log-level guidance, so a
broken freshness lookup shows up in `errors.jsonl` instead of silently
falling back to the static weight schedule.

These tests pin the two warning paths + the clean-run silence.
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest
from loguru import logger

from analysis.viz_intelligence import compute_source_weights


@pytest.fixture
def loguru_warnings():
    """Capture loguru WARNING-level messages during a test.

    Returns the list that will be populated by the sink. `caplog` cannot be
    used because loguru does not route through the stdlib logging system.
    """
    messages: list[str] = []
    sink_id = logger.add(
        lambda msg: messages.append(msg.record["message"]),
        level="WARNING",
    )
    try:
        yield messages
    finally:
        logger.remove(sink_id)


def test_get_engine_failure_logs_warning(loguru_warnings):
    """When db.get_engine() raises and no engine is provided, log a warning."""
    families = ["fed_liquidity"]

    fake_db = MagicMock()
    fake_db.get_engine.side_effect = RuntimeError("db unavailable")

    with patch.dict(sys.modules, {"db": fake_db}):
        weights = compute_source_weights(families, engine=None)

    # Weights still come back (static fallback), and the warning was emitted.
    assert isinstance(weights, dict)
    assert set(weights.keys()) == {"fed_liquidity"}
    assert any(
        "get_engine failed" in msg for msg in loguru_warnings
    ), f"expected get_engine failure warning, got: {loguru_warnings}"


def test_freshness_query_failure_logs_warning(loguru_warnings):
    """When the source_catalog SELECT raises, log a warning and continue."""
    families = ["fed_liquidity"]

    engine = MagicMock()
    engine.connect.side_effect = RuntimeError("timeout")

    weights = compute_source_weights(families, engine=engine)

    assert isinstance(weights, dict)
    assert set(weights.keys()) == {"fed_liquidity"}
    assert any(
        "source_catalog freshness query failed" in msg for msg in loguru_warnings
    ), f"expected freshness query failure warning, got: {loguru_warnings}"


def test_clean_run_emits_no_warning(loguru_warnings):
    """When the engine returns cleanly, no warning about this file should fire."""
    families = ["fed_liquidity"]

    engine = MagicMock()
    conn_ctx = MagicMock()
    conn_ctx.__enter__ = MagicMock(return_value=conn_ctx)
    conn_ctx.__exit__ = MagicMock(return_value=False)
    result = MagicMock()
    result.fetchall.return_value = []
    conn_ctx.execute.return_value = result
    engine.connect.return_value = conn_ctx

    weights = compute_source_weights(families, engine=engine)

    assert isinstance(weights, dict)
    assert set(weights.keys()) == {"fed_liquidity"}
    viz_warnings = [m for m in loguru_warnings if "viz_intelligence.compute_source_weights" in m]
    assert viz_warnings == [], f"unexpected warnings: {viz_warnings}"
