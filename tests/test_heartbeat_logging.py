"""Regression tests for alpha_research/heartbeat.py exception logging.

Covers the PUNCH-LIST-2026-05-13.md item:
"Log instead of silently swallowing in heartbeat exception handlers".

Verifies that ``_check_puller_health`` and ``_check_pit_freshness`` no longer
``except Exception: pass``; they must log a warning so failed checks surface
in errors.jsonl instead of masquerading as "all clear".
"""

from __future__ import annotations

from typing import Any

import pytest

from alpha_research import heartbeat


class _RaisingEngine:
    """Engine whose ``connect()`` blows up on entry — simulates DB outage / schema drift."""

    def __init__(self, message: str = "boom") -> None:
        self._message = message

    def connect(self) -> Any:  # pragma: no cover - never returns
        raise RuntimeError(self._message)


@pytest.fixture
def captured_warnings(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, tuple[Any, ...]]]:
    """Capture loguru ``log.warning`` calls without touching the global sink."""
    captured: list[tuple[str, tuple[Any, ...]]] = []

    def _capture(message: str, *args: Any, **_kwargs: Any) -> None:
        captured.append((message, args))

    monkeypatch.setattr(heartbeat.log, "warning", _capture)
    return captured


def test_check_puller_health_logs_warning_on_failure(
    captured_warnings: list[tuple[str, tuple[Any, ...]]],
) -> None:
    engine = _RaisingEngine("table missing")

    alerts = heartbeat._check_puller_health(engine)  # type: ignore[arg-type]

    assert alerts == []  # still degrades gracefully
    assert len(captured_warnings) == 1
    msg, args = captured_warnings[0]
    assert "puller_health" in msg
    assert any("table missing" in str(a) for a in args)


def test_check_pit_freshness_logs_warning_on_failure(
    captured_warnings: list[tuple[str, tuple[Any, ...]]],
) -> None:
    engine = _RaisingEngine("connection refused")

    alerts = heartbeat._check_pit_freshness(engine)  # type: ignore[arg-type]

    assert alerts == []
    assert len(captured_warnings) == 1
    msg, args = captured_warnings[0]
    assert "pit_freshness" in msg
    assert any("connection refused" in str(a) for a in args)
