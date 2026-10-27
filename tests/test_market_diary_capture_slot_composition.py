"""05:15Z scheduler slot x 05:30Z diary runner agree on which session and when (PR #858 job B).

Composition test: it needs the tree that contains
``ingestion.scheduler.run_yfinance_morning_slot`` (PR #901 / main a22a1efc).
On a tree without it the whole module SKIPS (inert, labelled in the result
file); on the composition it runs for real. Invented clocks only: the pull is
patched out, so nothing here fetches, connects or writes.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

scheduler = pytest.importorskip("ingestion.scheduler")
if not hasattr(scheduler, "run_yfinance_morning_slot"):
    pytest.skip("tree has no 05:15Z yfinance morning slot (needs #901/main)", allow_module_level=True)

from ingestion.market_calendar import is_market_open
from price_close_contract import daily_capture_boundary

REPO = Path(__file__).resolve().parent.parent
UTC = timezone.utc


@pytest.fixture(scope="module")
def runner():
    spec = importlib.util.spec_from_file_location("run_market_diary_composition_test", REPO / "scripts" / "run_market_diary.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def captures(monkeypatch):
    calls: list[dict] = []
    monkeypatch.setattr(
        scheduler, "_run_yfinance_daily",
        lambda start, **kw: calls.append({"start": start, **kw}),
    )
    return calls


def _at(d: date, hh: int, mm: int) -> datetime:
    return datetime(d.year, d.month, d.day, hh, mm, tzinfo=UTC)


def test_slot_constant_sits_between_the_ny_midnight_boundary_and_the_diary_timer():
    assert scheduler.YFINANCE_MORNING_SLOT_UTC == "05:15"
    timer = (REPO / "deploy" / "systemd" / "grid-market-diary.timer.template").read_text()
    assert "05:30:00 UTC" in timer
    # 05:00Z (EST) is the latest NY-midnight boundary; the capture must start after it.
    assert _at(date(2026, 1, 6), 5, 15) >= daily_capture_boundary("^GSPC", date(2026, 1, 5))


def test_positive_slot_captures_exactly_the_session_the_diary_then_writes(runner, captures):
    d = date(2026, 1, 1)
    both = neither = 0
    while d <= date(2026, 12, 31):
        captures.clear()
        scheduler.run_yfinance_morning_slot(now_utc=_at(d, 5, 15))
        diary_runs = d.weekday() in (1, 2, 3, 4, 5) and runner.previous_session_skip_reason(d) is None
        if captures:
            assert captures[0]["target_session"] == d - timedelta(days=1)
            assert captures[0]["purpose"] == "completed_session_capture"
            if d.weekday() in (1, 2, 3, 4, 5):  # timer days
                assert diary_runs and runner.previous_session_date(d) == captures[0]["target_session"], d
                both += 1
        elif d.weekday() in (1, 2, 3, 4, 5):
            assert not diary_runs, d  # slot captured nothing -> the diary run is skipped
            neither += 1
        d += timedelta(days=1)
    assert both > 240 and neither >= 8  # 2026 weekday holidays (Mon-Fri prior day)


def test_refusal_slot_skips_when_previous_day_was_not_a_session(captures):
    for d in (date(2026, 9, 8), date(2026, 11, 27), date(2026, 12, 26), date(2026, 9, 6), date(2026, 9, 7)):
        assert not is_market_open(d - timedelta(days=1))
        scheduler.run_yfinance_morning_slot(now_utc=_at(d, 5, 15))
    assert captures == []


@pytest.mark.parametrize("released_at, runs", [((5, 15), True), ((6, 44), True), ((6, 46), False), ((7, 0), False)])
def test_unknown_late_slot_is_skipped_after_90_minutes(captures, released_at, runs):
    scheduler.run_yfinance_morning_slot(now_utc=_at(date(2026, 9, 5), *released_at))
    assert bool(captures) is runs


class _OneRowEngine:
    """Serves one invented SUCCESS capture row through the window SQL, honouring as_of_ts."""

    def __init__(self, obs: date, pulled: datetime):
        from price_close_contract import raw_capture_payload

        self.row = (obs, 7050.5, pulled, "yfinance", raw_capture_payload("^GSPC", "close", obs, pulled, "1d"))

    def connect(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, stmt, params=None):
        ok = params["as_of_ts"] is None or self.row[2] <= params["as_of_ts"]

        class _R:
            def __init__(self, rows):
                self._rows = rows

            def fetchall(self):
                return self._rows

        return _R([self.row] if ok else [])


def test_unknown_late_but_in_limit_slot_lands_after_the_diary_so_the_run_reports_not_ready(runner):
    """Slot released 06:40Z (inside the 90-minute limit) writes its row after the
    05:30Z diary: that run sees no qualified close, a later run does."""
    d = date(2026, 9, 4)
    engine = _OneRowEngine(d, _at(date(2026, 9, 5), 6, 40))
    assert runner.qualified_close_ready(engine, d, now_utc=_at(date(2026, 9, 5), 5, 30)) is False
    assert runner.qualified_close_ready(engine, d, now_utc=_at(date(2026, 9, 5), 6, 45)) is True


def test_duplicate_adoption_slot_registered_once_under_one_constant():
    src = (REPO / "ingestion" / "scheduler.py").read_text()
    assert src.count("schedule.every().day.at(YFINANCE_MORNING_SLOT_UTC") == 1
