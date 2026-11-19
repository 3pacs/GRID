"""Repair-loop warnings that repeat every cycle are throttled per source.

On 2026-11-07 grid-hermes logged 131 "Hermes says CHECK_KEY TIINGO_FUNDAMENTALS"
lines in 24 h: the repair pass runs every few minutes and logged each
key check, escalation and failed recovery every time.
"""
from __future__ import annotations

import inspect

import pytest
from loguru import logger as log

from scripts import hermes_fixers as hf


@pytest.fixture
def warnings_seen(monkeypatch):
    monkeypatch.setattr(hf, "_THROTTLED_WARNINGS", {})
    seen: list[str] = []
    sink = log.add(lambda m: seen.append(m.record["message"]), level="WARNING")
    yield seen
    log.remove(sink)


def test_same_key_warns_once_per_interval(warnings_seen):
    for _ in range(5):
        hf._warn_throttled("check_key:X", "Hermes says CHECK_KEY {s}", s="X")
    assert warnings_seen == ["Hermes says CHECK_KEY X"]


def test_a_changed_failed_set_warns_again(warnings_seen):
    hf._warn_throttled("failed_sources:a,b", "Failed sources: {s}", s="a,b")
    hf._warn_throttled("failed_sources:a,b", "Failed sources: {s}", s="a,b")
    hf._warn_throttled("failed_sources:a,b,c", "Failed sources: {s}", s="a,b,c")
    assert warnings_seen == ["Failed sources: a,b", "Failed sources: a,b,c"]


@pytest.mark.parametrize("message", [
    "Hermes says CHECK_KEY {s}",
    "Hermes says ESCALATE {s}",
    "Repair for {s} did not recover",
])
def test_repeating_repair_messages_go_through_the_throttle(message):
    src = inspect.getsource(hf)
    for line in src.splitlines():
        if message in line:
            assert "log.warning(" not in line, line
    assert message in src
