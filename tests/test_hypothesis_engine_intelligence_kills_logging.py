"""Regression tests for hypothesis_engine._check_intelligence_kills logging.

ATTENTION.md / DEV-NOTES H9: previously the three subchecks in
``HypothesisGenerator._check_intelligence_kills`` swallowed every exception
with a bare ``except Exception: pass``. Genuine degradation paths (lever
puller / forensics / trust scorer module failures) silently disappeared
from operator logs.

The fix preserves graceful degradation but routes the swallowed exception
through ``log.debug`` so a developer can see *why* a kill check skipped
when investigating an unkilled-but-should-have-been-killed hypothesis.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from intelligence.hypothesis_engine import HypothesisGenerator


@pytest.fixture
def loguru_caplog(caplog):
    """Bridge loguru output into pytest's stdlib ``caplog`` fixture.

    Mirrors tests/test_redfin_puller.py::test_logs_when_inventory_silently_coerced
    (canonical recipe from 2026-05-13 routine handoff).
    """
    from loguru import logger

    class _Bridge(logging.Handler):
        def emit(self, record):  # noqa: D401
            logging.getLogger(record.name).handle(record)

    handler_id = logger.add(_Bridge(), level="DEBUG", format="{message}")
    caplog.set_level(logging.DEBUG)
    yield caplog
    logger.remove(handler_id)


def _make_generator() -> HypothesisGenerator:
    """Build a HypothesisGenerator backed by a mock engine.

    Both subcomponents (TemporalPatternDetector, AnomalyHunter) only stash
    the engine in __init__, so a MagicMock is safe.
    """
    return HypothesisGenerator(MagicMock())


def test_lever_diverged_subcheck_logs_on_import_failure(
    monkeypatch, loguru_caplog
):
    """LEVER_DIVERGED check must log when its import target raises."""
    import intelligence.lever_pullers as lp

    def _boom(*args, **kwargs):
        raise RuntimeError("lever puller boom")

    monkeypatch.setattr(
        lp, "get_lever_context_for_ticker", _boom, raising=False
    )

    gen = _make_generator()
    result = gen._check_intelligence_kills(
        h_id="h1",
        ptype="convergence",
        criteria={"ticker": "TSLA", "expected_direction": "bullish"},
        created_at=None,  # unused on this path
        confidence=0.5,
    )
    # Graceful degradation: subcheck still swallows -> returns None
    assert result is None

    messages = [r.getMessage() for r in loguru_caplog.records]
    assert any(
        "LEVER_DIVERGED check failed" in m and "TSLA" in m and "boom" in m
        for m in messages
    ), messages


def test_forensic_contradiction_subcheck_logs_on_import_failure(
    monkeypatch, loguru_caplog
):
    """FORENSIC_CONTRADICTION check must log when forensics import raises."""
    import intelligence.forensics as fx

    def _boom(*args, **kwargs):
        raise RuntimeError("forensics boom")

    # Neutralize the lever check so its except branch doesn't pollute logs.
    import intelligence.lever_pullers as lp
    monkeypatch.setattr(
        lp, "get_lever_context_for_ticker",
        lambda *a, **k: {"active_pullers": []}, raising=False,
    )
    monkeypatch.setattr(
        fx, "find_significant_moves", _boom, raising=False
    )

    gen = _make_generator()
    result = gen._check_intelligence_kills(
        h_id="h2",
        ptype="convergence",
        criteria={"ticker": "NVDA", "expected_direction": "bearish"},
        created_at=None,
        confidence=0.5,
    )
    assert result is None

    messages = [r.getMessage() for r in loguru_caplog.records]
    assert any(
        "FORENSIC_CONTRADICTION check failed" in m and "NVDA" in m
        and "boom" in m
        for m in messages
    ), messages


def test_trust_collapsed_subcheck_logs_on_import_failure(
    monkeypatch, loguru_caplog
):
    """TRUST_COLLAPSED check must log when trust_scorer import raises."""
    import intelligence.trust_scorer as ts

    def _boom(*args, **kwargs):
        raise RuntimeError("trust boom")

    monkeypatch.setattr(
        ts, "get_trusted_sources", _boom, raising=False
    )

    gen = _make_generator()
    result = gen._check_intelligence_kills(
        h_id="h3",
        ptype="convergence",
        criteria={"watch_actor": "SRC42"},
        created_at=None,
        confidence=0.5,
    )
    assert result is None

    messages = [r.getMessage() for r in loguru_caplog.records]
    assert any(
        "TRUST_COLLAPSED check failed" in m and "SRC42" in m and "boom" in m
        for m in messages
    ), messages
