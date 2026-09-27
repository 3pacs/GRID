from __future__ import annotations

import types
from datetime import datetime, timedelta, timezone

import pytest

from ingestion.smart_scheduler import (
    MissingPullerApiKey,
    SmartScheduler,
    _cftc_cot_is_due,
    _cftc_release_anchor,
)


class _EnvPuller:
    def __init__(self, db_engine):
        self.db_engine = db_engine


class _FirstArgPuller:
    def __init__(self, api_key, db_engine):
        self.api_key = api_key
        self.db_engine = db_engine


class _KeywordPuller:
    def __init__(self, db_engine, api_key=""):
        self.api_key = api_key
        self.db_engine = db_engine


def _scheduler(engine=object()) -> SmartScheduler:
    sched = SmartScheduler.__new__(SmartScheduler)
    sched.engine = engine
    return sched


def test_build_puller_instance_supports_env_api_key_mode() -> None:
    instance = _scheduler("engine")._build_puller_instance(
        {"name": "env_source", "api_key": "API_KEY", "api_key_mode": "env"},
        _EnvPuller,
        {"API_KEY": "secret"},
    )

    assert isinstance(instance, _EnvPuller)
    assert instance.db_engine == "engine"


def test_build_puller_instance_supports_first_arg_api_key_mode() -> None:
    instance = _scheduler("engine")._build_puller_instance(
        {"name": "first_source", "api_key": "API_KEY"},
        _FirstArgPuller,
        {"API_KEY": "secret"},
    )

    assert instance.api_key == "secret"
    assert instance.db_engine == "engine"


def test_build_puller_instance_supports_keyword_api_key_mode() -> None:
    instance = _scheduler("engine")._build_puller_instance(
        {"name": "keyword_source", "api_key": "API_KEY", "api_key_mode": "keyword"},
        _KeywordPuller,
        {"API_KEY": "secret"},
    )

    assert instance.api_key == "secret"
    assert instance.db_engine == "engine"


def test_build_puller_instance_raises_on_missing_required_api_key() -> None:
    with pytest.raises(MissingPullerApiKey):
        _scheduler("engine")._build_puller_instance(
            {"name": "source", "api_key": "API_KEY"},
            _FirstArgPuller,
            {},
        )


def test_gdelt_bounded_recent_skips_heavy_sections(monkeypatch) -> None:
    from ingestion.altdata import gdelt

    puller = gdelt.GDELTPuller.__new__(gdelt.GDELTPuller)
    puller.engine = types.SimpleNamespace(
        begin=lambda: _NullContext(types.SimpleNamespace())
    )
    puller.source_id = "gdelt-source"

    calls = {"themes": 0, "actors": 0, "tensions": 0, "signals": 0}

    def fake_fetch(query, mode, timespan):
        calls["themes"] += 1
        assert timespan == "1d"
        return {"timeline": []}

    monkeypatch.setattr(puller, "_fetch_gdelt_api", fake_fetch)
    monkeypatch.setattr(puller, "_pull_actor_tones", lambda: calls.__setitem__("actors", 1) or 0)
    monkeypatch.setattr(puller, "_pull_tension_scores", lambda: calls.__setitem__("tensions", 1) or 0)
    monkeypatch.setattr(puller, "_emit_tension_signals", lambda: calls.__setitem__("signals", 1) or 0)
    monkeypatch.setattr(gdelt, "time", types.SimpleNamespace(sleep=lambda _seconds: None))

    result = puller.pull_recent(
        days_back=1,
        max_theme_queries=2,
        include_actor_tones=False,
        include_tensions=False,
        include_signals=False,
    )

    assert result["status"] == "SUCCESS"
    assert calls == {"themes": 2, "actors": 0, "tensions": 0, "signals": 0}


def test_gdelt_rate_limit_is_clean_skip(monkeypatch) -> None:
    from ingestion.altdata import gdelt

    response = types.SimpleNamespace(status_code=429, raise_for_status=lambda: None)
    monkeypatch.setattr(gdelt.requests, "get", lambda *args, **kwargs: response)

    puller = gdelt.GDELTPuller.__new__(gdelt.GDELTPuller)
    result = puller._fetch_gdelt_api("economy recession", "timelineTone", "1d")

    assert result == {"timeline": [], "status": "SKIPPED", "http_status": 429}


class _NullContext:
    def __init__(self, value):
        self.value = value

    def __enter__(self):
        return self.value

    def __exit__(self, exc_type, exc, tb):
        return False


class _HangingPuller:
    """Puller whose method blocks past the scheduler timeout."""

    def __init__(self, db_engine):
        self.db_engine = db_engine

    def pull(self, **_kwargs):
        import time

        time.sleep(2.0)
        return "should-not-return"


def test_run_puller_timeout_increments_orphan_counter(monkeypatch) -> None:
    """A puller that exceeds its timeout must increment the orphan counter
    and surface the cumulative total via get_status().
    """
    import importlib

    from ingestion import smart_scheduler as ss

    sched = SmartScheduler.__new__(SmartScheduler)
    sched.engine = object()
    sched._state = {}
    sched._thread_semaphore = ss.threading.Semaphore(2)
    sched._active_threads = set()
    sched._threads_lock = ss.threading.Lock()
    sched._orphan_thread_count = 0

    monkeypatch.setattr(
        importlib,
        "import_module",
        lambda _modpath: types.SimpleNamespace(_HangingPuller=_HangingPuller),
    )

    puller_entry = {
        "name": "hanging_puller",
        "mod": "fake.module",
        "cls": "_HangingPuller",
        "method": "pull",
        "timeout_s": 0.05,
    }

    result = sched._run_puller(puller_entry)

    assert result["status"] == "TIMEOUT"
    assert sched._orphan_thread_count == 1

    # A second timeout further increments the counter
    result2 = sched._run_puller(puller_entry)
    assert result2["status"] == "TIMEOUT"
    assert sched._orphan_thread_count == 2


def test_get_status_reports_orphan_thread_count() -> None:
    """get_status() must surface the cumulative orphan-thread count and
    the active-thread snapshot for operator observability.
    """
    from ingestion import smart_scheduler as ss

    sched = SmartScheduler.__new__(SmartScheduler)
    sched.engine = object()
    sched._state = {}
    sched._thread_semaphore = ss.threading.Semaphore(1)
    sched._active_threads = {"foo"}
    sched._threads_lock = ss.threading.Lock()
    sched._orphan_thread_count = 7

    # _get_due_pullers reads PULLER_REGISTRY; stub it out to avoid DB calls
    sched._get_due_pullers = lambda: []  # type: ignore[method-assign]

    status = sched.get_status()

    assert status["orphan_thread_count_total"] == 7
    assert status["active_threads"] == ["foo"]
    assert status["max_concurrent_threads"] == SmartScheduler.MAX_CONCURRENT_THREADS


# ── GRID task A1: cftc_cot Friday-release + Saturday-retry gate ─────────

_FRIDAY_BEFORE_CUTOFF = datetime(2026, 9, 25, 19, 44, tzinfo=timezone.utc)
_FRIDAY_AT_CUTOFF = datetime(2026, 9, 25, 19, 45, tzinfo=timezone.utc)
_FRIDAY_AFTER_CUTOFF = datetime(2026, 9, 25, 21, 0, tzinfo=timezone.utc)
_SATURDAY = datetime(2026, 9, 26, 12, 0, tzinfo=timezone.utc)
_SUNDAY = datetime(2026, 9, 27, 12, 0, tzinfo=timezone.utc)
_MONDAY = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)


def test_cftc_release_anchor_is_the_most_recent_friday_1945_utc() -> None:
    assert _cftc_release_anchor(_FRIDAY_AFTER_CUTOFF) == _FRIDAY_AT_CUTOFF
    assert _cftc_release_anchor(_SATURDAY) == _FRIDAY_AT_CUTOFF
    assert _cftc_release_anchor(_SUNDAY) == _FRIDAY_AT_CUTOFF
    # Monday's "most recent" anchor is the Friday 3 days earlier, same as
    # Sat/Sun — the anchor only jumps back a further 7 days once it would
    # otherwise land in the future (exercised next: Friday itself, before
    # today's own cutoff, must resolve to last week's anchor, not a
    # not-yet-arrived one later today).
    assert _cftc_release_anchor(_MONDAY) == _FRIDAY_AT_CUTOFF
    assert _cftc_release_anchor(_FRIDAY_BEFORE_CUTOFF) == _FRIDAY_AT_CUTOFF - timedelta(days=7)


def test_cftc_cot_not_due_on_friday_before_1945_utc() -> None:
    assert _cftc_cot_is_due(None, _FRIDAY_BEFORE_CUTOFF) is False
    assert _cftc_cot_is_due(_FRIDAY_AT_CUTOFF - timedelta(days=14), _FRIDAY_BEFORE_CUTOFF) is False


def test_cftc_cot_due_on_friday_at_or_after_1945_utc() -> None:
    assert _cftc_cot_is_due(None, _FRIDAY_AT_CUTOFF) is True
    assert _cftc_cot_is_due(_FRIDAY_AT_CUTOFF - timedelta(days=7), _FRIDAY_AFTER_CUTOFF) is True


def test_cftc_cot_saturday_retries_when_friday_was_missed() -> None:
    # Last success predates this week's Friday anchor → Friday's pull never
    # happened (or failed before recording a success) → Saturday retries.
    stale = _FRIDAY_AT_CUTOFF - timedelta(days=7)
    assert _cftc_cot_is_due(stale, _SATURDAY) is True
    assert _cftc_cot_is_due(None, _SATURDAY) is True


def test_cftc_cot_saturday_does_not_rerun_after_friday_succeeded() -> None:
    # Friday's run already landed at/after this week's anchor → no retry.
    assert _cftc_cot_is_due(_FRIDAY_AFTER_CUTOFF, _SATURDAY) is False
    assert _cftc_cot_is_due(_FRIDAY_AT_CUTOFF, _SATURDAY) is False


def test_cftc_cot_never_due_outside_friday_saturday_even_if_very_stale() -> None:
    # Fail-closed: no amount of staleness makes it due on Sun/Mon/etc — it
    # waits for the next release window instead of firing off-schedule.
    ancient = _FRIDAY_AT_CUTOFF - timedelta(days=365)
    for probe in (_SUNDAY, _MONDAY):
        assert _cftc_cot_is_due(ancient, probe) is False
        assert _cftc_cot_is_due(None, probe) is False


def test_cftc_cot_is_due_accepts_naive_last_success_as_utc() -> None:
    naive_stale = (_FRIDAY_AT_CUTOFF - timedelta(days=7)).replace(tzinfo=None)
    assert _cftc_cot_is_due(naive_stale, _SATURDAY) is True


def test_is_due_dispatches_cftc_cot_through_the_release_gate() -> None:
    """SmartScheduler._is_due must route cftc_cot through the Friday/
    Saturday gate instead of the generic freq_h cadence, for both a
    never-run puller and one with a recent (but off-window) last_success.
    """
    from ingestion import smart_scheduler as ss

    sched = SmartScheduler.__new__(SmartScheduler)
    sched._state = {}
    puller = {"name": "cftc_cot", "freq_h": 168}

    real_datetime = ss.datetime

    class _FrozenDatetime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            return _FRIDAY_BEFORE_CUTOFF if tz is None else _FRIDAY_BEFORE_CUTOFF

    ss.datetime = _FrozenDatetime
    try:
        # Never run + before this week's cutoff → not due (would be True
        # under the old "state is None → definitely due" shortcut).
        assert sched._is_due(puller) is False

        # A recent success (well within 168h) that predates this week's
        # anchor must still not be treated as "satisfied" once Friday's
        # window opens — but before the window opens it stays not-due.
        sched._state = {
            "cftc_cot": {
                "last_success": _FRIDAY_BEFORE_CUTOFF - timedelta(hours=1),
                "cooldown_until": None,
            }
        }
        assert sched._is_due(puller) is False
    finally:
        ss.datetime = real_datetime
