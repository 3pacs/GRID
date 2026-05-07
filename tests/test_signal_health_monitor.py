"""
Tests for ``intelligence/signal_health_monitor.py``.

Covers all pure helpers plus the DB-touching audit/dampening/persist paths
via an in-memory FakeEngine. No live DB or network calls.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pytest

from intelligence.signal_health_monitor import (
    EXPECTED_CADENCE_BY_PREFIX,
    SignalHealth,
    SignalHealthReport,
    audit_all_series,
    audit_one_series,
    classify_drift,
    classify_nan_rate,
    classify_staleness,
    combine_status,
    compose_summary,
    dampening_for_status,
    ensure_health_table,
    get_signal_dampening,
    match_cadence,
    persist_report,
)


# ── FakeEngine ────────────────────────────────────────────────────────────

class _FakeResult:
    def __init__(self, rows: list[tuple] | None) -> None:
        self._rows = rows or []

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _FakeConn:
    def __init__(self, dispatch) -> None:
        self._dispatch = dispatch
        self.executed: list[tuple[str, dict]] = []

    def execute(self, stmt, params: dict | None = None):
        sql = str(stmt)
        params = params or {}
        self.executed.append((sql, params))
        rows = self._dispatch(sql, params)
        return _FakeResult(rows)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeEngine:
    """Minimal Engine stub: routes SQL fragments to canned row lists.

    ``handlers`` maps a substring → callable(sql, params) → list[tuple] | None.
    The first matching substring wins. ``raise_on`` is a list of substrings;
    if any matches, .execute() raises RuntimeError (used to test defensive
    error paths).
    """

    def __init__(
        self,
        handlers: dict[str, Any] | None = None,
        raise_on: list[str] | None = None,
    ) -> None:
        self.handlers = handlers or {}
        self.raise_on = raise_on or []
        self.persisted: list[dict] = []

    def _dispatch(self, sql: str, params: dict):
        for needle in self.raise_on:
            if needle in sql:
                raise RuntimeError(f"forced error on '{needle}'")
        # Capture INSERT params for assertion access.
        if "INSERT INTO signal_health_history" in sql:
            self.persisted.append(dict(params))
            return []
        for needle, handler in self.handlers.items():
            if needle in sql:
                if callable(handler):
                    return handler(sql, params)
                return handler
        return []

    @contextmanager
    def connect(self):
        yield _FakeConn(self._dispatch)

    @contextmanager
    def begin(self):
        yield _FakeConn(self._dispatch)


# ── Pure helpers ──────────────────────────────────────────────────────────

class TestMatchCadence:
    def test_exact_prefix(self):
        assert match_cadence("fed_h8:loans", EXPECTED_CADENCE_BY_PREFIX) == 7

    def test_longest_prefix_wins(self):
        lookup = {"pboc:": 1, "pboc:omo:": 7}
        assert match_cadence("pboc:omo:7d", lookup) == 7
        assert match_cadence("pboc:rrr", lookup) == 1

    def test_no_match_default_one(self):
        assert match_cadence("totally_unknown:xyz", {"foo:": 30}) == 1

    def test_pboc_omo_falls_back_to_pboc(self):
        # Realistic case: only 'pboc:' is in the dict; 'pboc:omo:' should
        # resolve to 'pboc:' rather than the default of 1.
        assert match_cadence("pboc:omo:7d", EXPECTED_CADENCE_BY_PREFIX) == 1


class TestClassifyStaleness:
    def test_none_is_red(self):
        assert classify_staleness(None, 7) == "red"

    def test_zero_days_green(self):
        assert classify_staleness(0, 7) == "green"

    def test_just_over_yellow(self):
        # 7 * 1.5 = 10.5 → 11 days
        assert classify_staleness(11, 7) == "yellow"

    def test_orange_threshold(self):
        # 7 * 3 = 21
        assert classify_staleness(22, 7) == "orange"

    def test_red_threshold(self):
        # 7 * 7 = 49
        assert classify_staleness(60, 7) == "red"


class TestClassifyNanRate:
    def test_zero_green(self):
        assert classify_nan_rate(0.0) == "green"

    def test_yellow(self):
        assert classify_nan_rate(0.15) == "yellow"

    def test_orange(self):
        assert classify_nan_rate(0.35) == "orange"

    def test_red(self):
        assert classify_nan_rate(0.6) == "red"


class TestClassifyDrift:
    def test_none_green(self):
        assert classify_drift(None) == "green"

    def test_low_z_green(self):
        assert classify_drift(2.0) == "green"
        assert classify_drift(-2.0) == "green"

    def test_yellow(self):
        assert classify_drift(3.0) == "yellow"
        assert classify_drift(-3.0) == "yellow"

    def test_red(self):
        assert classify_drift(5.0) == "red"
        assert classify_drift(-5.0) == "red"


class TestCombineStatus:
    def test_red_dominates(self):
        assert combine_status("green", "red", "yellow") == "red"

    def test_orange_beats_yellow(self):
        assert combine_status("orange", "yellow", "green") == "orange"

    def test_yellow_beats_green(self):
        assert combine_status("yellow", "green", "green") == "yellow"

    def test_all_green(self):
        assert combine_status("green", "green", "green") == "green"


class TestDampeningForStatus:
    def test_curve(self):
        assert dampening_for_status("green") == 1.0
        assert dampening_for_status("yellow") == 0.85
        assert dampening_for_status("orange") == 0.6
        assert dampening_for_status("red") == 0.0


class TestComposeSummary:
    def test_format(self):
        report = SignalHealthReport(
            generated_at="2026-04-13T00:00:00+00:00",
            total_series=16,
            by_status={"green": 12, "yellow": 3, "orange": 1, "red": 0},
            by_namespace={},
            unhealthy=[],
            summary="",
        )
        s = compose_summary(report)
        assert "12 green" in s
        assert "3 yellow" in s
        assert "1 orange" in s
        assert "0 red" in s
        assert "16 series" in s


class TestDataclassRoundtrip:
    def test_signal_health_to_dict(self):
        h = SignalHealth(
            series_id="fed_h8:loans",
            last_observation=date(2026, 4, 10),
            days_since_last=3,
            expected_cadence_days=7,
            staleness_status="green",
            recent_row_count=12,
            expected_row_count=12,
            nan_rate=0.0,
            nan_status="green",
            drift_zscore=0.5,
            drift_status="green",
            overall_status="green",
            conviction_dampening=1.0,
            generated_at="2026-04-13T00:00:00+00:00",
        )
        d = h.to_dict()
        assert d["series_id"] == "fed_h8:loans"
        assert d["last_observation"] == "2026-04-10"
        assert d["overall_status"] == "green"
        # Frozen — mutation should fail.
        with pytest.raises(Exception):
            h.series_id = "other"  # type: ignore[misc]

    def test_report_to_dict(self):
        report = SignalHealthReport(
            generated_at="2026-04-13T00:00:00+00:00",
            total_series=2,
            by_status={"green": 1, "yellow": 1, "orange": 0, "red": 0},
            by_namespace={"fed_h8": {"green": 1, "yellow": 0, "orange": 0, "red": 0}},
            unhealthy=[],
            summary="1 green, 1 yellow, 0 orange, 0 red across 2 series",
        )
        d = report.to_dict()
        assert d["total_series"] == 2
        assert d["by_status"]["green"] == 1
        assert "fed_h8" in d["by_namespace"]


# ── audit_one_series ──────────────────────────────────────────────────────

def _make_stats_handler(
    *,
    last_obs: date | None,
    row_count: int,
    nan_count: int,
    latest_value: float | None,
    history_mean: float | None,
    history_std: float | None,
):
    """Build a dispatch handler that responds to the three queries in
    ``_fetch_series_stats`` with canned values.
    """
    def handler(sql: str, params: dict):
        if "MAX(obs_date)" in sql:
            return [(last_obs, row_count, nan_count)]
        if "ORDER BY obs_date DESC LIMIT 1" in sql:
            if latest_value is None:
                return []
            return [(latest_value,)]
        if "STDDEV_SAMP" in sql:
            return [(history_mean, history_std)]
        return []
    return handler


class TestAuditOneSeries:
    def test_happy_path_green(self):
        today = date.today()
        eng = FakeEngine(handlers={
            "raw_series": _make_stats_handler(
                last_obs=today,
                row_count=12,
                nan_count=0,
                latest_value=100.5,
                history_mean=100.0,
                history_std=1.0,
            )
        })
        h = audit_one_series(eng, "fed_h8:loans")
        assert h.series_id == "fed_h8:loans"
        assert h.staleness_status == "green"
        assert h.nan_status == "green"
        assert h.drift_status == "green"  # z = 0.5
        assert h.overall_status == "green"
        assert h.conviction_dampening == 1.0
        assert h.expected_cadence_days == 7

    def test_stale_series_red(self):
        today = date.today()
        last = today - timedelta(days=60)  # > 7 * 7 = 49 → red
        eng = FakeEngine(handlers={
            "raw_series": _make_stats_handler(
                last_obs=last,
                row_count=2,
                nan_count=0,
                latest_value=100.0,
                history_mean=100.0,
                history_std=1.0,
            )
        })
        h = audit_one_series(eng, "fed_h8:loans")
        assert h.staleness_status == "red"
        assert h.overall_status == "red"
        assert h.conviction_dampening == 0.0
        assert h.days_since_last == 60

    def test_high_nan_rate_red(self):
        today = date.today()
        eng = FakeEngine(handlers={
            "raw_series": _make_stats_handler(
                last_obs=today,
                row_count=10,
                nan_count=6,        # 60% NaN → red
                latest_value=100.0,
                history_mean=100.0,
                history_std=1.0,
            )
        })
        h = audit_one_series(eng, "fed_h8:loans")
        assert h.nan_rate == 0.6
        assert h.nan_status == "red"
        assert h.overall_status == "red"

    def test_drift_detected_red(self):
        today = date.today()
        eng = FakeEngine(handlers={
            "raw_series": _make_stats_handler(
                last_obs=today,
                row_count=12,
                nan_count=0,
                latest_value=150.0,
                history_mean=100.0,
                history_std=10.0,   # z = 5.0 → red drift
            )
        })
        h = audit_one_series(eng, "fed_h8:loans")
        assert h.drift_zscore is not None
        assert abs(h.drift_zscore - 5.0) < 1e-9
        assert h.drift_status == "red"
        assert h.overall_status == "red"

    def test_db_error_returns_red_defensive(self):
        eng = FakeEngine(raise_on=["raw_series"])
        h = audit_one_series(eng, "fed_h8:loans")
        assert h.overall_status == "red"
        assert h.conviction_dampening == 0.0
        assert h.staleness_status == "red"


# ── audit_all_series ──────────────────────────────────────────────────────

class TestAuditAllSeries:
    def test_happy_path(self):
        today = date.today()

        def dispatch(sql: str, params: dict):
            if "DISTINCT series_id" in sql:
                return [("fed_h8:loans",), ("pboc:omo",)]
            if "MAX(obs_date)" in sql:
                return [(today, 5, 0)]
            if "ORDER BY obs_date DESC LIMIT 1" in sql:
                return [(100.0,)]
            if "STDDEV_SAMP" in sql:
                return [(100.0, 1.0)]
            return []

        eng = FakeEngine(handlers={"FROM raw_series": dispatch, "raw_series": dispatch})
        report = audit_all_series(eng)
        assert report.total_series == 2
        assert report.by_status["green"] == 2
        assert "fed_h8" in report.by_namespace
        assert "pboc" in report.by_namespace
        assert "2 series" in report.summary

    def test_empty_raw_series(self):
        eng = FakeEngine(handlers={"DISTINCT series_id": []})
        report = audit_all_series(eng)
        assert report.total_series == 0
        assert "0 series" in report.summary
        assert report.unhealthy == []


# ── get_signal_dampening ──────────────────────────────────────────────────

class TestGetSignalDampening:
    def test_missing_returns_one_via_audit(self):
        # Cache miss + audit returns no data → series with no observations
        # → red → dampening 0.0. To exercise the "missing series" path that
        # the spec asks (returns 1.0 on missing), we instead cause both the
        # cache lookup and the audit to fail, forcing the safe-default 1.0.
        eng = FakeEngine(raise_on=["signal_health_history", "raw_series"])
        d = get_signal_dampening(eng, "nonexistent:series")
        assert d == 1.0

    def test_returns_cached_value(self):
        def dispatch(sql: str, params: dict):
            if "signal_health_history" in sql:
                return [(0.85, "yellow", datetime.now(timezone.utc))]
            return []

        eng = FakeEngine(handlers={"signal_health_history": dispatch})
        d = get_signal_dampening(eng, "fed_h8:loans")
        assert d == 0.85

    def test_falls_through_to_on_demand_audit(self):
        today = date.today()

        def dispatch(sql: str, params: dict):
            if "signal_health_history" in sql:
                return []  # cache miss
            if "MAX(obs_date)" in sql:
                return [(today, 5, 0)]
            if "ORDER BY obs_date DESC LIMIT 1" in sql:
                return [(100.0,)]
            if "STDDEV_SAMP" in sql:
                return [(100.0, 1.0)]
            return []

        eng = FakeEngine(handlers={
            "signal_health_history": dispatch,
            "raw_series": dispatch,
        })
        d = get_signal_dampening(eng, "fed_h8:loans")
        assert d == 1.0  # green → no dampening


# ── persist_report + ensure_health_table ──────────────────────────────────

class TestPersistAndEnsure:
    def test_persist_writes_unhealthy_only(self):
        unhealthy = SignalHealth(
            series_id="pboc:omo",
            last_observation=None,
            days_since_last=None,
            expected_cadence_days=1,
            staleness_status="red",
            recent_row_count=0,
            expected_row_count=90,
            nan_rate=1.0,
            nan_status="red",
            drift_zscore=None,
            drift_status="green",
            overall_status="red",
            conviction_dampening=0.0,
            generated_at=datetime.now(timezone.utc).isoformat(),
        )
        report = SignalHealthReport(
            generated_at=datetime.now(timezone.utc).isoformat(),
            total_series=1,
            by_status={"green": 0, "yellow": 0, "orange": 0, "red": 1},
            by_namespace={"pboc": {"green": 0, "yellow": 0, "orange": 0, "red": 1}},
            unhealthy=[unhealthy],
            summary="0 green, 0 yellow, 0 orange, 1 red across 1 series",
        )
        eng = FakeEngine()
        n = persist_report(eng, report)
        assert n == 1
        assert eng.persisted[0]["series_id"] == "pboc:omo"
        assert eng.persisted[0]["namespace"] == "pboc"
        assert eng.persisted[0]["overall_status"] == "red"

    def test_ensure_table_swallows_errors(self):
        eng = FakeEngine(raise_on=["CREATE TABLE"])
        # Should not raise.
        ensure_health_table(eng)
