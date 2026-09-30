"""Offline gates for the held GEM daily timer; no provider or database."""

import json
from datetime import date, datetime, timezone

from scripts import gem_daily_capture as daily

DAY = date(2026, 10, 1)


def _event(hour: int, minute: int, message: str) -> str:
    stamp = datetime(2026, 10, 1, hour, minute, tzinfo=timezone.utc)
    return json.dumps({"__REALTIME_TIMESTAMP": str(int(stamp.timestamp() * 1_000_000)),
                       "MESSAGE": message})


def test_one_start_then_complete_passes() -> None:
    assert daily._scheduler_gate([
        _event(13, 30, "Starting daily pulls market_open=True"),
        _event(13, 55, "Options daily pull complete"),
    ], DAY)


def test_duplicate_failed_skipped_and_out_of_order_refuse() -> None:
    start = _event(13, 30, "Starting daily pulls market_open=True")
    done = _event(13, 55, "Options daily pull complete")
    assert not daily._scheduler_gate([start, start, done], DAY)
    assert not daily._scheduler_gate([start, done, done], DAY)
    assert not daily._scheduler_gate([done, start], DAY)
    assert not daily._scheduler_gate([start, done, _event(13, 56, "Options daily pull failed")], DAY)
    assert not daily._scheduler_gate([start, _event(13, 55, "Options pull skipped")], DAY)
    assert not daily._scheduler_gate([_event(13, 30, "Starting daily pulls market_open=False"), done], DAY)
    assert not daily._scheduler_gate(["broken-json"], DAY)


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
