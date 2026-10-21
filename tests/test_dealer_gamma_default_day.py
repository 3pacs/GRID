"""``compute_gex_profile(snap_date=None)`` resolves to the latest completed capture, never the bare UTC day.

The SPY chain is captured once per session (~22:20Z), so the UTC calendar day
has no chain for most of the day. The default must land on the latest
completed capture on or before the last NYSE trading day, carry its age, and
fail closed with a reason when nothing is registered. No database: a fake
engine answers the capture-batch lookup and the chain read from one table.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Self

import pytest
from sqlalchemy.exc import SQLAlchemyError

from physics import dealer_gamma
from physics.dealer_gamma import DEFAULT_RESOLUTION, DealerGammaEngine

BATCH = "11111111-1111-4111-8111-111111111111"


def _midnight(day: date) -> datetime:
    return datetime.combine(day, datetime.min.time(), timezone.utc)


def _completed_at(day: date) -> datetime:
    return _midnight(day) + timedelta(hours=22, minutes=20, seconds=46)


def _chain_rows(day: date) -> list[tuple]:
    captured = _midnight(day) + timedelta(hours=22, minutes=20)
    quote = _midnight(day) + timedelta(hours=20)  # 16:00 New York on ``day``
    return [
        (760.0, "call", 100, 0.20, day + timedelta(days=7), timedelta(days=7), captured,
         BATCH, 1, captured, _completed_at(day), quote),
        (750.0, "put", 120, 0.25, day + timedelta(days=7), timedelta(days=7), captured,
         BATCH, 1, captured, _completed_at(day), quote),
    ]


class _Result:
    def __init__(self, rows: list[tuple]) -> None:
        self.rows = rows

    def fetchall(self) -> list[tuple]:
        return self.rows

    def fetchone(self) -> tuple | None:
        return self.rows[0] if self.rows else None


class _DB:
    """Fake engine: the batch lookup and the chain read both answer from ``days``."""

    def __init__(self, days: dict[date, list[tuple]]) -> None:
        self.days = days
        self.queries: list[tuple[str, dict]] = []

    def connect(self) -> Self:
        return self

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: Any, params: dict | None = None) -> _Result:
        sql, params = str(sql), dict(params or {})
        self.queries.append((sql, params))
        if "options_capture_batches" in sql:
            eligible = [day for day, rows in self.days.items()
                        if day <= params["on_or_before"] and rows[0][10] <= params["now"]]
            return _Result([(max(eligible),)] if eligible else [(None,)])
        return _Result(self.days.get(params["snap_date"], []))


class _BrokenDB(_DB):
    def execute(self, sql: Any, params: dict | None = None) -> _Result:
        raise SQLAlchemyError("relation does not exist")


def _engine(monkeypatch: pytest.MonkeyPatch, db: _DB, now: datetime) -> DealerGammaEngine:
    monkeypatch.setattr(dealer_gamma, "_utc_now", lambda: now)
    engine = DealerGammaEngine(db)

    def receipt(_ticker: str, completed: datetime | None) -> dict | None:
        if completed is None:
            return None
        chain_day = completed.astimezone(timezone.utc).date()
        return {
            "price": 766.48, "receipt_id": 7,
            "obs_date": chain_day - timedelta(days=1),
            "available_at": _midnight(chain_day) + timedelta(minutes=30),
            "receipt_created_at": _midnight(chain_day) + timedelta(hours=1),
            "release_date": chain_day, "vintage_date": chain_day,
        }

    monkeypatch.setattr(engine, "_get_spot_receipt", receipt)
    return engine


def _chain_days_queried(db: _DB) -> list[date]:
    return [params["snap_date"] for sql, params in db.queries if "options_capture_batches" not in sql]


def _lookups(db: _DB) -> list[dict]:
    return [params for sql, params in db.queries if "options_capture_batches" in sql]


def test_weekday_early_utc_resolves_to_previous_session_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    # 02:55Z on Wednesday 2026-10-21: the UTC day is a trading day but its
    # chain lands at ~22:20Z, so the latest completed capture is Tuesday's.
    now = datetime(2026, 10, 21, 2, 55, tzinfo=timezone.utc)
    db = _DB({date(2026, 10, 19): _chain_rows(date(2026, 10, 19)),
              date(2026, 10, 20): _chain_rows(date(2026, 10, 20))})
    profile = _engine(monkeypatch, db, now).compute_gex_profile("SPY")

    assert "error" not in profile
    assert profile["snap_date"] == "2026-10-20"
    assert profile["chain_snap_date"] == "2026-10-20"
    assert profile["default_resolution"] == DEFAULT_RESOLUTION == "last_completed_capture"
    assert profile["chain_age_seconds"] == int((now - _completed_at(date(2026, 10, 20))).total_seconds())
    assert profile["chain_age_seconds"] == 4 * 3600 + 34 * 60 + 14
    assert profile["regime"] in {"LONG_GAMMA", "SHORT_GAMMA", "NEUTRAL"}
    assert _lookups(db) == [{"ticker": "SPY", "on_or_before": date(2026, 10, 21), "now": now}]
    assert _chain_days_queried(db) == [date(2026, 10, 20)]


def test_saturday_resolves_to_friday_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 10, 24, 15, 0, tzinfo=timezone.utc)  # Saturday
    db = _DB({date(2026, 10, 22): _chain_rows(date(2026, 10, 22)),
              date(2026, 10, 23): _chain_rows(date(2026, 10, 23))})
    profile = _engine(monkeypatch, db, now).compute_gex_profile("SPY")

    assert profile["chain_snap_date"] == "2026-10-23"
    assert profile["default_resolution"] == DEFAULT_RESOLUTION
    assert _lookups(db)[0]["on_or_before"] == date(2026, 10, 23)
    assert profile["chain_age_seconds"] == int((now - _completed_at(date(2026, 10, 23))).total_seconds())


def test_holiday_bound_uses_the_nyse_calendar_not_weekdays(monkeypatch: pytest.MonkeyPatch) -> None:
    # Friday 2026-12-25 is a market holiday: the bound is Thursday 12-24.
    now = datetime(2026, 12, 26, 12, 0, tzinfo=timezone.utc)
    db = _DB({date(2026, 12, 24): _chain_rows(date(2026, 12, 24))})
    profile = _engine(monkeypatch, db, now).compute_gex_profile("SPY")

    assert profile["chain_snap_date"] == "2026-12-24"
    assert _lookups(db)[0]["on_or_before"] == date(2026, 12, 24)


def test_resolver_is_point_in_time_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    # A batch registered with a completion after ``now`` (clock skew, replayed
    # fixture) is never the default; the query itself carries both bounds.
    now = datetime(2026, 10, 21, 2, 55, tzinfo=timezone.utc)
    db = _DB({date(2026, 10, 20): _chain_rows(date(2026, 10, 20)),
              date(2026, 10, 21): _chain_rows(date(2026, 10, 21))})
    profile = _engine(monkeypatch, db, now).compute_gex_profile("SPY")

    assert profile["chain_snap_date"] == "2026-10-20"
    lookup_sql = next(sql for sql, _ in db.queries if "options_capture_batches" in sql)
    assert "snap_date <= :on_or_before" in lookup_sql
    assert "capture_completed_at <= :now" in lookup_sql
    assert "MAX(snap_date)" in lookup_sql


def test_explicit_snap_date_is_used_as_given(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 10, 21, 2, 55, tzinfo=timezone.utc)
    db = _DB({date(2026, 10, 19): _chain_rows(date(2026, 10, 19)),
              date(2026, 10, 20): _chain_rows(date(2026, 10, 20))})
    profile = _engine(monkeypatch, db, now).compute_gex_profile("SPY", date(2026, 10, 19))

    assert profile["snap_date"] == profile["chain_snap_date"] == "2026-10-19"
    assert "default_resolution" not in profile
    assert profile["chain_age_seconds"] == int((now - _completed_at(date(2026, 10, 19))).total_seconds())
    assert _lookups(db) == []
    assert _chain_days_queried(db) == [date(2026, 10, 19)]


def test_no_completed_capture_is_unavailable_with_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 10, 21, 2, 55, tzinfo=timezone.utc)
    db = _DB({})
    result = _engine(monkeypatch, db, now).compute_gex_profile("SPY")

    assert result["available"] is False
    assert result["status"] == "unavailable"
    assert result["source"] == "options_capture_batches"
    assert "SPY" in result["reason"] and "on or before 2026-10-21" in result["reason"]
    assert result["error"] == result["reason"]
    assert result["ticker"] == "SPY"
    assert result["default_resolution"] == DEFAULT_RESOLUTION
    assert result["chain_age_seconds"] is None
    for key in ("snap_date", "chain_snap_date", "spot", "regime", "gamma_flip",
                "put_wall", "call_wall", "gex_aggregate", "profile", "per_strike"):
        assert result[key] is None
    assert _chain_days_queried(db) == []


def test_lookup_failure_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 10, 21, 2, 55, tzinfo=timezone.utc)
    result = _engine(monkeypatch, _BrokenDB({}), now).compute_gex_profile("SPY")

    assert result["available"] is False
    assert result["error"] == result["reason"]


def test_resolved_day_without_loadable_chain_keeps_the_marker(monkeypatch: pytest.MonkeyPatch) -> None:
    # The batch is registered but its rows fail the chain validation (created
    # on another day): report the resolved day, never fall back further.
    now = datetime(2026, 10, 21, 2, 55, tzinfo=timezone.utc)
    rows = [(*row[:6], row[6] + timedelta(days=1), *row[7:]) for row in _chain_rows(date(2026, 10, 20))]
    db = _DB({date(2026, 10, 20): rows})
    result = _engine(monkeypatch, db, now).compute_gex_profile("SPY")

    assert result["error"] == "No options data for SPY on 2026-10-20"
    assert result["default_resolution"] == DEFAULT_RESOLUTION
    assert _chain_days_queried(db) == [date(2026, 10, 20)]


def test_latest_completed_capture_day_coerces_and_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 10, 21, 2, 55, tzinfo=timezone.utc)

    class _Timestamped(_DB):
        def execute(self, sql: Any, params: dict | None = None) -> _Result:
            return _Result([(datetime(2026, 10, 20, tzinfo=timezone.utc),)])

    class _Future(_DB):
        def execute(self, sql: Any, params: dict | None = None) -> _Result:
            return _Result([(date(2026, 10, 22),)])

    assert DealerGammaEngine(_Timestamped({})).latest_completed_capture_day(
        "SPY", date(2026, 10, 21), now=now) == date(2026, 10, 20)
    assert DealerGammaEngine(_Future({})).latest_completed_capture_day(
        "SPY", date(2026, 10, 21), now=now) is None
