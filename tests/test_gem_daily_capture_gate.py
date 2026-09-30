"""Offline gates for the held GEM daily timer; no provider or database."""

import json
import subprocess
from datetime import date, datetime, timezone

from scripts import gem_daily_capture as daily

DAY = date(2026, 10, 1)


def _event(hour: int, minute: int, message: str, day: date = DAY) -> str:
    stamp = datetime(day.year, day.month, day.day, hour, minute, tzinfo=timezone.utc)
    return json.dumps({"__REALTIME_TIMESTAMP": str(int(stamp.timestamp() * 1_000_000)),
                       "MESSAGE": message})


def test_one_start_then_complete_passes() -> None:
    assert daily._scheduler_gate([
        _event(13, 30, "Starting daily pulls market_open=True"),
        _event(13, 55, "Options daily pull complete — 1/8 tickers, 10 snapshots"),
    ], DAY)


def test_duplicate_failed_skipped_and_out_of_order_refuse() -> None:
    start = _event(13, 30, "Starting daily pulls market_open=True")
    done = _event(13, 55, "Options daily pull complete — 1/8 tickers, 10 snapshots")
    assert not daily._scheduler_gate([start, start, done], DAY)
    assert not daily._scheduler_gate([start, done, done], DAY)
    assert not daily._scheduler_gate([done, start], DAY)
    assert not daily._scheduler_gate([start, done, _event(13, 56, "Options daily pull failed")], DAY)
    assert not daily._scheduler_gate([start, _event(13, 55, "Options pull skipped")], DAY)
    assert not daily._scheduler_gate([_event(13, 30, "Starting daily pulls market_open=False"), done], DAY)
    assert not daily._scheduler_gate(["broken-json"], DAY)


def test_winter_session_requires_shifted_open_and_positive_rows() -> None:
    winter = date(2026, 11, 2)
    start = _event(14, 30, "Starting daily pulls market_open=True", winter)
    assert daily._scheduler_gate([
        start, _event(14, 55, "Options daily pull complete — 2/8 tickers, 30 snapshots", winter)
    ], winter)
    assert not daily._scheduler_gate([
        _event(13, 30, "Starting daily pulls market_open=True", winter),
        _event(13, 55, "Options daily pull complete — 2/8 tickers, 30 snapshots", winter)
    ], winter)
    assert not daily._scheduler_gate([
        start, _event(14, 55, "Options daily pull complete — 0/8 tickers, 0 snapshots", winter)
    ], winter)
    assert daily._session_deadline(winter).hour == 15
    assert daily._session_deadline(DAY).hour == 14


def test_same_day_attempt_is_single_use(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(daily, "_ATTEMPTS", tmp_path)
    assert daily._claim_day(DAY)
    assert not daily._claim_day(DAY)
    assert len(list(tmp_path.iterdir())) == 1


def test_unactivated_code_never_claims_or_calls_provider(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(daily, "_ACTIVATED", tmp_path / "absent")
    monkeypatch.setattr(daily, "_claim_day", lambda _day: (_ for _ in ()).throw(
        AssertionError("attempt claimed before activation")))
    assert daily.main() == 1


def test_pinned_checkout_refuses_dirty_executable_or_untracked_code(tmp_path) -> None:
    def git(*args):
        subprocess.run(["git", "-C", str(tmp_path), *args], check=True,
                       capture_output=True, text=True)
    git("init")
    git("-c", "user.name=Test", "-c", "user.email=test@example.invalid",
        "commit", "--allow-empty", "-m", "init")
    executable = tmp_path / "scripts" / "gem_daily_capture.py"
    executable.parent.mkdir()
    executable.write_text("reviewed\n", encoding="utf-8")
    git("add", "scripts/gem_daily_capture.py")
    git("-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-m", "pin")
    assert daily._checkout_clean(tmp_path)
    executable.write_text("modified\n", encoding="utf-8")
    assert not daily._checkout_clean(tmp_path)
    git("checkout", "--", "scripts/gem_daily_capture.py")
    (tmp_path / "ingestion").mkdir()
    (tmp_path / "ingestion" / "options.py").write_text("new code\n", encoding="utf-8")
    assert not daily._checkout_clean(tmp_path)
