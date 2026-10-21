"""Opt-in latest-completed GEX read: one cutoff, one exact batch, base defaults untouched.

``compute_latest_completed_gex_profile`` is the only route to a prior-session
chain without an explicit day (read-only ``/derivatives/gex`` and ``/walls``).
It takes one cutoff, selects one exact registered batch completed by it, loads
that batch through the registered-replay path and validates it against the same
cutoff. ``compute_gex_profile(snap_date=None)`` keeps its UTC-day default for
every internal caller. No database: a fake engine whose registry and latest view
can disagree, and a clock that advances between calls (the PR #890 head-1 R1
witness: batch B completed 22:21:02 after a 22:21:00 cutoff was admitted with
``chain_age_seconds=-2``).
"""

from __future__ import annotations

import inspect
from datetime import date, datetime, timedelta, timezone
from typing import Any, Self

import pytest
from sqlalchemy.exc import SQLAlchemyError

from physics import dealer_gamma
from physics.dealer_gamma import DEFAULT_RESOLUTION, DealerGammaEngine

BATCH_A = "11111111-1111-4111-8111-111111111111"
BATCH_B = "22222222-2222-4222-8222-222222222222"
DAY = date(2026, 10, 20)  # Tuesday
UTC = timezone.utc


def _at(day: date, h: int, m: int, s: int = 0) -> datetime:
    return datetime.combine(day, datetime.min.time(), UTC) + timedelta(hours=h, minutes=m, seconds=s)


class _Batch:
    def __init__(self, batch_id: str, day: date, ordinal: int, started: datetime,
                 completed: datetime, *, created: datetime | None = None,
                 registered_rows: int | None = None) -> None:
        self.batch_id, self.day, self.ordinal = batch_id, day, ordinal
        self.started, self.completed = started, completed
        self.created = created or started
        quote = _at(day, 20, 0)  # 16:00 New York
        self.rows = [
            (760.0, "call", 100, 0.20, day + timedelta(days=7), timedelta(days=7), self.created,
             batch_id, ordinal, started, completed, quote),
            (750.0, "put", 120, 0.25, day + timedelta(days=7), timedelta(days=7), self.created,
             batch_id, ordinal, started, completed, quote),
        ]
        self.registered_rows = len(self.rows) if registered_rows is None else registered_rows


def _a(day: date = DAY, **kw) -> _Batch:
    return _Batch(BATCH_A, day, 1, _at(day, 22, 20), _at(day, 22, 20, 30), **kw)


def _b(day: date = DAY, **kw) -> _Batch:
    return _Batch(BATCH_B, day, 2, _at(day, 22, 20, 40), _at(day, 22, 21, 2), **kw)


class _Result:
    def __init__(self, rows: list[tuple]) -> None:
        self.rows = rows

    def fetchall(self) -> list[tuple]:
        return self.rows

    def fetchone(self) -> tuple | None:
        return self.rows[0] if self.rows else None


class _DB:
    """Registry select, exact-batch replay load and latest-view load, each answered separately.

    ``registry`` and ``view`` are independent so a test can make the latest view
    show a batch the registry query would not admit at the cutoff.
    """

    def __init__(self, registry: list[_Batch], view: dict[date, _Batch] | None = None) -> None:
        self.registry = registry
        self.view = view if view is not None else {}
        self.calls: list[tuple[str, dict]] = []

    def connect(self) -> Self:
        return self

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: Any, params: dict | None = None) -> _Result:
        sql, params = str(sql), dict(params or {})
        if "LIMIT 1" in sql and "options_capture_batches" in sql:
            self.calls.append(("registry", params))
            eligible = [b for b in self.registry
                        if b.day <= params["on_or_before"] and b.completed <= params["cutoff"]]
            eligible.sort(key=lambda b: (b.day, b.ordinal), reverse=True)
            return _Result([(eligible[0].day, eligible[0].batch_id)] if eligible else [])
        if "batch" in params:
            self.calls.append(("batch", params))
            hit = [b for b in self.registry
                   if b.batch_id == params["batch"] and b.day == params["snap_date"]]
            return _Result([(*row, hit[0].registered_rows) for row in hit[0].rows] if hit else [])
        self.calls.append(("view", params))
        latest = self.view.get(params["snap_date"])
        return _Result([(*row, None) for row in latest.rows] if latest else [])


class _BrokenDB(_DB):
    def execute(self, sql: Any, params: dict | None = None) -> _Result:
        raise SQLAlchemyError("relation does not exist")


class _Clock:
    """First call returns ``start``; every later call ``later`` (the loader's own clock)."""

    def __init__(self, start: datetime, later: datetime | None = None) -> None:
        self.start, self.later, self.n = start, later or start, 0

    def __call__(self) -> datetime:
        self.n += 1
        return self.start if self.n == 1 else self.later


def _engine(monkeypatch: pytest.MonkeyPatch, db: _DB, clock) -> DealerGammaEngine:
    monkeypatch.setattr(dealer_gamma, "_utc_now", clock)
    engine = DealerGammaEngine(db)

    def receipt(ticker: str, completed: datetime | None) -> dict | None:
        if ticker != "SPY" or completed is None:
            return None
        day = completed.astimezone(UTC).date()
        return {"price": 766.48, "receipt_id": 7, "obs_date": day - timedelta(days=1),
                "available_at": _at(day, 0, 30), "receipt_created_at": _at(day, 1, 0),
                "release_date": day, "vintage_date": day}

    monkeypatch.setattr(engine, "_get_spot_receipt", receipt)
    return engine


def _kinds(db: _DB) -> list[str]:
    return [kind for kind, _ in db.calls]


# ── R1: one cutoff bound through selection and load ─────────────────────────

def test_batch_completed_after_cutoff_is_never_admitted(monkeypatch: pytest.MonkeyPatch) -> None:
    # Head-1 witness: cutoff 22:21:00; A completed 22:20:30 makes the day
    # eligible; the latest view already shows B (completed 22:21:02) and the
    # loader's own clock reads 22:21:03. Head-1 returned B with age -2.
    cutoff, later = _at(DAY, 22, 21, 0), _at(DAY, 22, 21, 3)
    a, b = _a(), _b()
    db = _DB(registry=[a, b], view={DAY: b})
    profile = _engine(monkeypatch, db, _Clock(cutoff, later)).compute_latest_completed_gex_profile("SPY")

    assert "error" not in profile, profile
    assert profile["chain_batch_id"] == BATCH_A
    assert profile["chain_capture_completed_at"] == a.completed.isoformat()
    assert profile["chain_age_seconds"] == 30
    assert profile["chain_read_cutoff"] == cutoff.isoformat()
    assert profile["default_resolution"] == DEFAULT_RESOLUTION
    assert _kinds(db) == ["registry", "batch"]  # the mutable latest view is never read
    assert db.calls[0][1]["cutoff"] == cutoff and db.calls[1][1]["batch"] == BATCH_A


def test_batch_completed_before_cutoff_is_selected_over_earlier_ordinal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cutoff = _at(DAY, 22, 21, 5)
    db = _DB(registry=[_a(), _b()])
    profile = _engine(monkeypatch, db, _Clock(cutoff)).compute_latest_completed_gex_profile("SPY")

    assert profile["chain_batch_id"] == BATCH_B
    assert profile["chain_age_seconds"] == 3


@pytest.mark.parametrize("late", ["completed", "created"])
def test_loaded_clock_after_cutoff_fails_closed(monkeypatch: pytest.MonkeyPatch, late: str) -> None:
    # The registry admits A, but its stored rows carry a completion or creation
    # clock after the cutoff (registry/row disagreement): never published.
    cutoff = _at(DAY, 22, 21, 0)
    a = _a()
    if late == "completed":
        a.rows = [(*r[:10], _at(DAY, 22, 21, 2), r[11]) for r in a.rows]
    else:
        a.rows = [(*r[:6], _at(DAY, 22, 21, 1), *r[7:]) for r in a.rows]
    db = _DB(registry=[a])
    result = _engine(monkeypatch, db, _Clock(cutoff, _at(DAY, 22, 30))).compute_latest_completed_gex_profile("SPY")

    assert result["error"] == f"No options data for SPY on {DAY}"
    assert result["default_resolution"] == DEFAULT_RESOLUTION
    assert "chain_age_seconds" not in result or result["chain_age_seconds"] is None


def test_registered_row_count_mismatch_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    db = _DB(registry=[_a(registered_rows=3)])
    result = _engine(monkeypatch, db, _Clock(_at(DAY, 23, 0))).compute_latest_completed_gex_profile("SPY")
    assert result["error"] == f"No options data for SPY on {DAY}"


# ── default-day resolution ──────────────────────────────────────────────────

def test_weekday_early_utc_reads_previous_session_with_age(monkeypatch: pytest.MonkeyPatch) -> None:
    cutoff = datetime(2026, 10, 21, 2, 55, tzinfo=UTC)  # Wednesday, before its capture
    db = _DB(registry=[_a(date(2026, 10, 19)), _a(DAY)])
    profile = _engine(monkeypatch, db, _Clock(cutoff)).compute_latest_completed_gex_profile("SPY")

    assert profile["snap_date"] == profile["chain_snap_date"] == "2026-10-20"
    assert profile["chain_age_seconds"] == 4 * 3600 + 34 * 60 + 30
    assert db.calls[0][1] == {"ticker": "SPY", "on_or_before": date(2026, 10, 21), "cutoff": cutoff}


def test_saturday_reads_friday(monkeypatch: pytest.MonkeyPatch) -> None:
    cutoff = datetime(2026, 10, 24, 15, 0, tzinfo=UTC)
    db = _DB(registry=[_a(date(2026, 10, 22)), _a(date(2026, 10, 23))])
    profile = _engine(monkeypatch, db, _Clock(cutoff)).compute_latest_completed_gex_profile("SPY")

    assert profile["chain_snap_date"] == "2026-10-23"
    assert db.calls[0][1]["on_or_before"] == date(2026, 10, 23)


def test_holiday_bound_uses_the_nyse_calendar(monkeypatch: pytest.MonkeyPatch) -> None:
    cutoff = datetime(2026, 12, 26, 12, 0, tzinfo=UTC)  # 12-25 is a market holiday
    db = _DB(registry=[_a(date(2026, 12, 24))])
    profile = _engine(monkeypatch, db, _Clock(cutoff)).compute_latest_completed_gex_profile("SPY")

    assert profile["chain_snap_date"] == "2026-12-24"
    assert db.calls[0][1]["on_or_before"] == date(2026, 12, 24)


def test_no_completed_capture_is_unavailable_with_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    cutoff = datetime(2026, 10, 21, 2, 55, tzinfo=UTC)
    db = _DB(registry=[_b(date(2026, 10, 21))])  # completes after the cutoff
    result = _engine(monkeypatch, db, _Clock(cutoff)).compute_latest_completed_gex_profile("SPY")

    assert result["available"] is False and result["status"] == "unavailable"
    assert result["source"] == "options_capture_batches"
    assert "on or before 2026-10-21" in result["reason"] and cutoff.isoformat() in result["reason"]
    assert result["error"] == result["reason"]
    assert result["chain_age_seconds"] is None and result["chain_batch_id"] is None
    for key in ("spot", "regime", "gamma_flip", "put_wall", "call_wall", "gex_aggregate", "per_strike"):
        assert result[key] is None
    assert _kinds(db) == ["registry"]


def test_registry_failure_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    result = _engine(monkeypatch, _BrokenDB([]), _Clock(_at(DAY, 23, 0))).compute_latest_completed_gex_profile("SPY")
    assert result["available"] is False and result["error"] == result["reason"]


def test_selector_query_carries_both_bounds_and_exact_order() -> None:
    src = inspect.getsource(DealerGammaEngine.latest_completed_capture_batch)
    for clause in ("snap_date <= :on_or_before", "capture_completed_at <= :cutoff",
                   "ORDER BY snap_date DESC, capture_ordinal DESC", "LIMIT 1"):
        assert clause in src


@pytest.mark.parametrize("row", [[(date(2026, 10, 22), BATCH_A)], [(DAY, None)], [(DAY, "")]])
def test_selector_rejects_future_or_unnamed_rows(row) -> None:
    class _Fixed(_DB):
        def execute(self, sql: Any, params: dict | None = None) -> _Result:
            return _Result(row)

    assert DealerGammaEngine(_Fixed([])).latest_completed_capture_batch(
        "SPY", date(2026, 10, 21), _at(DAY, 23, 0)) is None


# ── R3: base defaults unchanged ─────────────────────────────────────────────

def test_shared_default_still_reads_the_utc_day_view(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 10, 21, 2, 55, tzinfo=UTC)
    monkeypatch.setattr(dealer_gamma, "datetime", _FrozenDatetime.at(now))
    db = _DB(registry=[_a(DAY)], view={DAY: _a(DAY)})
    result = _engine(monkeypatch, db, _Clock(now)).compute_gex_profile("SPY")

    assert result == {"error": "No options data for SPY on 2026-10-21", "ticker": "SPY"}
    assert _kinds(db) == ["view"] and db.calls[0][1]["snap_date"] == date(2026, 10, 21)


def test_explicit_day_is_unchanged_and_unlabelled(monkeypatch: pytest.MonkeyPatch) -> None:
    db = _DB(registry=[_a(DAY), _b(DAY)], view={DAY: _b(DAY)})
    profile = _engine(monkeypatch, db, _Clock(_at(DAY, 23, 0))).compute_gex_profile("SPY", DAY)

    assert profile["chain_batch_id"] == BATCH_B  # latest view, as before
    for key in ("chain_age_seconds", "default_resolution", "chain_read_cutoff"):
        assert key not in profile
    assert _kinds(db) == ["view"]


# ── R2: market summary labels the loaded chain day ──────────────────────────

def _summary_row(ticker: str, day: str) -> dict:
    return {"ticker": ticker, "snap_date": day, "chain_snap_date": day, "spot": 766.48,
            "gex_aggregate": -1e9, "vanna_exposure": 2, "charm_exposure": 3,
            "regime": "SHORT_GAMMA", "gamma_flip": 769, "put_wall": 769, "call_wall": 775}


def test_summary_labels_the_chain_day_not_the_wall_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    # Review witness: a supported SPY-only 10-20 profile summarised on 10-21.
    engine = DealerGammaEngine(_DB([]))
    monkeypatch.setattr(engine, "compute_all_tickers", lambda _d: [_summary_row("SPY", "2026-10-20")])
    summary = engine.get_market_gex_summary()

    assert summary["snap_date"] == "2026-10-20"
    assert summary["chain_snap_dates"] == ["2026-10-20"]
    assert summary["tickers"][0]["chain_snap_date"] == "2026-10-20"


def test_summary_of_mixed_chain_days_has_no_single_label(monkeypatch: pytest.MonkeyPatch) -> None:
    # Interface-level only: the receipt guard does not qualify non-SPY tickers today.
    engine = DealerGammaEngine(_DB([]))
    monkeypatch.setattr(engine, "compute_all_tickers", lambda _d: [
        _summary_row("SPY", "2026-10-20"), _summary_row("QQQ", "2026-10-19")])
    summary = engine.get_market_gex_summary()

    assert summary["snap_date"] is None
    assert summary["chain_snap_dates"] == ["2026-10-19", "2026-10-20"]


class _FrozenDatetime(datetime):
    _now: datetime

    @classmethod
    def at(cls, now: datetime) -> type:
        return type("_Frozen", (cls,), {"_now": now})

    @classmethod
    def now(cls, tz=None):  # type: ignore[override]
        return cls._now if tz is None else cls._now.astimezone(tz)
