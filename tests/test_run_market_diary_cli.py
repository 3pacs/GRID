"""CLI target-date selection for scripts/run_market_diary.py.

``--previous-session`` (used by the 05:30Z timer) must pick the last US trading
day strictly before today, and an explicit ``--date`` must still win.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "run_market_diary.py"


@pytest.fixture(scope="module")
def runner():
    spec = importlib.util.spec_from_file_location("run_market_diary_under_test", _SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.parametrize(
    "today, expected",
    [
        (date(2026, 10, 20), date(2026, 10, 19)),  # Tue -> Mon
        (date(2026, 10, 24), date(2026, 10, 23)),  # Sat -> Fri
        (date(2026, 10, 25), date(2026, 10, 23)),  # Sun -> Fri
        (date(2026, 10, 19), date(2026, 10, 16)),  # Mon -> Fri
        (date(2026, 4, 6), date(2026, 4, 2)),  # Mon after Good Friday -> Thu
    ],
)
def test_previous_session_date(runner, today, expected):
    assert runner.previous_session_date(today) == expected


def _run(runner, monkeypatch, argv, today):
    """Run main() with a frozen 'today'; return the target_date passed on."""
    monkeypatch.setattr(runner, "previous_session_date", lambda t=None, _f=runner.previous_session_date: _f(today))
    fake = MagicMock(return_value={"date": "x", "thesis_accuracy": {}, "market_moves": {}})
    monkeypatch.setattr("intelligence.market_diary.write_diary_entry", fake)
    assert runner.main(argv, engine=object()) == 0
    return fake.call_args.kwargs["target_date"]


def test_main_previous_session_tuesday(runner, monkeypatch):
    assert _run(runner, monkeypatch, ["--previous-session"], date(2026, 10, 20)) == date(2026, 10, 19)


def test_main_previous_session_saturday(runner, monkeypatch):
    assert _run(runner, monkeypatch, ["--previous-session"], date(2026, 10, 24)) == date(2026, 10, 23)


def test_main_previous_session_monday_after_holiday_friday(runner, monkeypatch):
    assert _run(runner, monkeypatch, ["--previous-session"], date(2026, 4, 6)) == date(2026, 4, 2)


def test_explicit_date_wins_over_previous_session(runner, monkeypatch):
    got = _run(runner, monkeypatch, ["--previous-session", "--date", "2026-09-01"], date(2026, 10, 20))
    assert got == date(2026, 9, 1)


def test_default_is_today_and_unchanged(runner, monkeypatch):
    fake = MagicMock(return_value={"date": "x", "thesis_accuracy": {}, "market_moves": {}})
    monkeypatch.setattr("intelligence.market_diary.write_diary_entry", fake)
    assert runner.main([], engine=object()) == 0
    assert fake.call_args.kwargs["target_date"] == date.today()


def test_templates_wire_previous_session():
    root = Path(__file__).resolve().parent.parent / "deploy" / "systemd"
    svc = (root / "grid-market-diary.service.template").read_text()
    tmr = (root / "grid-market-diary.timer.template").read_text()
    assert "run_market_diary.py --previous-session" in svc
    assert "OnCalendar=Tue..Sat *-*-* 05:30:00 UTC" in tmr
    assert "RandomizedDelaySec=" in tmr and "Persistent=true" in tmr
