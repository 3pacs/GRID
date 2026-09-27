from __future__ import annotations

import types
from datetime import date, datetime, timedelta, timezone

import pytest

from ingestion.altdata.cftc_markets import compute_release
from ingestion.smart_scheduler import (
    MissingPullerApiKey,
    SmartScheduler,
    _cftc_cot_is_due,
    _cftc_current_report_date,
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


# ── GRID task A1: cftc_cot holiday/DST-aware release gate ───────────────
#
# Coordinator REQUEST CHANGES on PR #681 @ dd53a3dd (BLOCKER: fixed 19:45
# UTC anchor is wrong in EST — fires 45min before the report exists and the
# Saturday retry never fires because the premature run counts as a
# success). These fixtures use compute_release() itself (also exercised
# directly in tests/test_cftc_cot_market_code.py) as the source of truth
# for each report_date's actual release_at, then add the 15-minute margin
# the gate applies, so a bug in the fixture math can't hide a bug in the
# gate — both use the one holiday/DST-aware rule.

_MARGIN = timedelta(minutes=15)


def _release_plus_margin(report_date) -> datetime:
    release = compute_release(report_date)
    assert release.release_at is not None
    return release.release_at + _MARGIN


# Ordinary EDT week (no holiday): Tue 2026-09-22 -> Fri 2026-09-25 19:30 UTC.
_EDT_REPORT_DATE = date(2026, 9, 22)
_EDT_RELEASE = _release_plus_margin(_EDT_REPORT_DATE)  # Fri 19:45 UTC
_EDT_FRIDAY_BEFORE = _EDT_RELEASE - timedelta(minutes=1)
_EDT_FRIDAY_AFTER = _EDT_RELEASE + timedelta(hours=1, minutes=15)  # Fri 21:00 UTC
_EDT_SATURDAY = _EDT_RELEASE + timedelta(hours=16, minutes=15)  # Sat noon UTC
_EDT_SUNDAY_PAST_WINDOW = _EDT_RELEASE + timedelta(days=1, hours=16, minutes=15)  # Sun noon UTC
_EDT_MONDAY = _EDT_RELEASE + timedelta(days=3)

# EST week (Dec/Jan): Tue 2026-12-01 -> Fri 2026-12-04 20:30 UTC. This is
# the exact regression case: the OLD fixed-19:45-UTC anchor would have
# fired here 61 minutes before this release_at+margin (20:45 UTC).
_EST_REPORT_DATE = date(2026, 12, 1)
_EST_RELEASE = _release_plus_margin(_EST_REPORT_DATE)  # Fri 20:45 UTC
_EST_OLD_WRONG_ANCHOR = datetime(2026, 12, 4, 19, 45, tzinfo=timezone.utc)
_EST_FRIDAY_AFTER = _EST_RELEASE + timedelta(hours=1)
_EST_SATURDAY = _EST_RELEASE + timedelta(hours=16)

# DST spring-forward 2026 (clocks go EST->EDT on Sun 2026-03-08 02:00 ET):
# the Friday immediately before is still EST, the one immediately after is
# already EDT.
_SPRING_BEFORE_REPORT_DATE = date(2026, 3, 3)   # Fri 2026-03-06, EST
_SPRING_AFTER_REPORT_DATE = date(2026, 3, 10)   # Fri 2026-03-13, EDT
_SPRING_BEFORE_RELEASE = _release_plus_margin(_SPRING_BEFORE_REPORT_DATE)  # 20:45 UTC
_SPRING_AFTER_RELEASE = _release_plus_margin(_SPRING_AFTER_REPORT_DATE)    # 19:45 UTC

# DST fall-back 2026 (clocks go EDT->EST on Sun 2026-11-01 02:00 ET).
_FALL_BEFORE_REPORT_DATE = date(2026, 10, 27)  # Fri 2026-10-30, EDT
_FALL_AFTER_REPORT_DATE = date(2026, 11, 3)    # Fri 2026-11-06, EST
_FALL_BEFORE_RELEASE = _release_plus_margin(_FALL_BEFORE_REPORT_DATE)  # 19:45 UTC
_FALL_AFTER_RELEASE = _release_plus_margin(_FALL_AFTER_REPORT_DATE)    # 20:45 UTC

# Holiday-shifted week: Tue 2026-06-30's Friday (2026-07-03) is the
# Independence Day observed holiday (Jul 4 falls on a Saturday in 2026),
# so compute_release shifts the release to the next federal business day,
# Mon 2026-07-06 -- matching cftc_markets.py's own worked 2026 example.
_HOLIDAY_REPORT_DATE = date(2026, 6, 30)
_HOLIDAY_RELEASE = _release_plus_margin(_HOLIDAY_REPORT_DATE)  # Mon 2026-07-06 19:45 UTC
assert compute_release(_HOLIDAY_REPORT_DATE).holiday_shifted is True


def test_current_report_date_maps_the_week_to_its_tuesday() -> None:
    # Tue..Sun of a week all resolve to that week's own Tuesday; Monday
    # resolves to the PRIOR week's Tuesday (this week's report isn't out).
    tuesday = date(2026, 9, 22)
    for offset in range(6):  # Tue..Sun
        assert _cftc_current_report_date(tuesday + timedelta(days=offset)) == tuesday
    assert _cftc_current_report_date(date(2026, 9, 28)) == tuesday  # next Monday


def test_current_report_date_holds_through_a_holiday_shifted_week() -> None:
    # Every ET calendar day from the report Tuesday through the shifted
    # Monday release still maps to that same report_date -- the mapping is
    # purely calendar-based; compute_release (not this function) is what
    # knows the release itself moved.
    for offset in range(7):  # Tue 06-30 .. Mon 07-06 inclusive (7 days)
        assert (
            _cftc_current_report_date(_HOLIDAY_REPORT_DATE + timedelta(days=offset))
            == _HOLIDAY_REPORT_DATE
        )


def test_cftc_cot_not_due_before_release_plus_margin_edt() -> None:
    assert _cftc_cot_is_due(None, _EDT_FRIDAY_BEFORE) is False
    assert _cftc_cot_is_due(_EDT_RELEASE - timedelta(days=14), _EDT_FRIDAY_BEFORE) is False


def test_cftc_cot_due_at_or_after_release_plus_margin_edt() -> None:
    assert _cftc_cot_is_due(None, _EDT_RELEASE) is True
    assert _cftc_cot_is_due(_EDT_RELEASE - timedelta(days=7), _EDT_FRIDAY_AFTER) is True


def test_cftc_cot_saturday_retries_when_friday_was_missed_edt() -> None:
    stale = _EDT_RELEASE - timedelta(days=7)
    assert _cftc_cot_is_due(stale, _EDT_SATURDAY) is True
    assert _cftc_cot_is_due(None, _EDT_SATURDAY) is True


def test_cftc_cot_no_rerun_after_friday_succeeded_edt() -> None:
    assert _cftc_cot_is_due(_EDT_FRIDAY_AFTER, _EDT_SATURDAY) is False
    assert _cftc_cot_is_due(_EDT_RELEASE, _EDT_SATURDAY) is False


def test_cftc_cot_never_due_outside_window_even_if_very_stale_edt() -> None:
    ancient = _EDT_RELEASE - timedelta(days=365)
    for probe in (_EDT_SUNDAY_PAST_WINDOW, _EDT_MONDAY):
        assert _cftc_cot_is_due(ancient, probe) is False
        assert _cftc_cot_is_due(None, probe) is False


def test_cftc_cot_is_due_accepts_naive_last_success_as_utc() -> None:
    naive_stale = (_EDT_RELEASE - timedelta(days=7)).replace(tzinfo=None)
    assert _cftc_cot_is_due(naive_stale, _EDT_SATURDAY) is True


def test_cftc_cot_est_release_is_2030_utc_not_1930() -> None:
    """DST regression pin: in EST, 15:30 ET is 20:30 UTC (not 19:30), so
    release+margin is 20:45 UTC, not 19:45.
    """
    assert _EST_RELEASE == datetime(2026, 12, 4, 20, 45, tzinfo=timezone.utc)


def test_cftc_cot_not_due_at_the_old_fixed_1945_utc_anchor_in_est() -> None:
    """The exact bug the coordinator flagged: at the OLD hardcoded 19:45
    UTC anchor, an EST Friday's real release+margin (20:45 UTC) is still
    61 minutes away -- must not be due, or it would re-store the prior
    week's report and (per the old code) suppress the Saturday retry too.
    """
    assert _cftc_cot_is_due(None, _EST_OLD_WRONG_ANCHOR) is False
    assert _cftc_cot_is_due(_EST_RELEASE - timedelta(days=7), _EST_OLD_WRONG_ANCHOR) is False


def test_cftc_cot_due_at_or_after_release_plus_margin_est() -> None:
    assert _cftc_cot_is_due(None, _EST_RELEASE) is True
    assert _cftc_cot_is_due(_EST_RELEASE - timedelta(days=7), _EST_FRIDAY_AFTER) is True


def test_cftc_cot_saturday_retries_when_friday_was_missed_est() -> None:
    assert _cftc_cot_is_due(_EST_RELEASE - timedelta(days=7), _EST_SATURDAY) is True
    assert _cftc_cot_is_due(_EST_RELEASE, _EST_SATURDAY) is False  # already succeeded


@pytest.mark.parametrize(
    "before_release,after_release",
    [
        (_SPRING_BEFORE_RELEASE, _SPRING_AFTER_RELEASE),
        (_FALL_BEFORE_RELEASE, _FALL_AFTER_RELEASE),
    ],
)
def test_cftc_cot_dst_transition_weeks_both_sides_correct(before_release, after_release) -> None:
    """The Friday immediately before a DST transition and the Friday
    immediately after must each use their OWN correct UTC offset (not the
    offset in effect on whichever day `now` happens to be evaluated).
    """
    for release_at in (before_release, after_release):
        assert _cftc_cot_is_due(None, release_at - timedelta(minutes=1)) is False
        assert _cftc_cot_is_due(None, release_at) is True


def test_cftc_cot_holiday_shifted_week_is_due_on_the_shifted_day_not_friday() -> None:
    # The (observed-holiday) Friday itself must NOT be due -- the report
    # doesn't exist until the shifted Monday.
    holiday_friday = datetime.combine(
        date(2026, 7, 3), datetime.min.time(), tzinfo=timezone.utc
    ) + timedelta(hours=20)
    assert _cftc_cot_is_due(None, holiday_friday) is False

    assert _cftc_cot_is_due(None, _HOLIDAY_RELEASE - timedelta(minutes=1)) is False
    assert _cftc_cot_is_due(None, _HOLIDAY_RELEASE) is True
    # Retry still available the next ET day (Tuesday 07-07) before the new
    # week's own Tuesday report_date takes over mid-day.
    early_tuesday = _HOLIDAY_RELEASE + timedelta(hours=2)
    assert _cftc_cot_is_due(_HOLIDAY_RELEASE - timedelta(days=7), early_tuesday) is True


def test_is_due_dispatches_cftc_cot_through_the_release_gate() -> None:
    """SmartScheduler._is_due must route cftc_cot through the holiday/DST
    -aware gate instead of the generic freq_h cadence, for both a
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
            return _EDT_FRIDAY_BEFORE if tz is None else _EDT_FRIDAY_BEFORE

    ss.datetime = _FrozenDatetime
    try:
        # Never run + before this week's release+margin → not due (would
        # be True under the old "state is None → definitely due" shortcut).
        assert sched._is_due(puller) is False

        # A recent success (well within 168h) that predates this week's
        # release must still not be treated as "satisfied" before the
        # window opens.
        sched._state = {
            "cftc_cot": {
                "last_success": _EDT_FRIDAY_BEFORE - timedelta(hours=1),
                "cooldown_until": None,
            }
        }
        assert sched._is_due(puller) is False
    finally:
        ss.datetime = real_datetime
