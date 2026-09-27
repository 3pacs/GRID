"""Tests for the Wave 1 puller activation (2026-09-27) and the 2026-09-27
independent review's fixes to it.

Covers the pullers registered in ``ingestion.smart_scheduler.PULLER_REGISTRY``:

- eia                 (PR #553, previously unregistered as "EIA_Energy")
- finra_short_volume  (PR #564, previously unregistered as "FINRA_Short_Volume")
- sec_ftd             (PR #564, previously registered as "SEC_FTD")

LME_Warehouse is deliberately NOT registered (both LME URLs 403 from
grid-svr; see ``_LMEWarehouseSchedulerAdapter``'s docstring) -- it is not
covered here.

These were NOT wired via the two .patch files shipped in
docs/handoffs/2026-09-18/ -- those target ``ingestion/scheduler.py``,
which is not the scheduler Hermes actually runs. See the "Wave 1
activation helpers" comment in ingestion/smart_scheduler.py.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest

from ingestion.smart_scheduler import (
    MissingPullerApiKey,
    PULLER_REGISTRY,
    SmartScheduler,
    _finra_short_volume_trade_date,
    _SECFTDSchedulerAdapter,
    _sec_ftd_latest_published_half,
    _sec_ftd_published_halves_since,
)


def _registry_entry(name: str) -> dict:
    matches = [p for p in PULLER_REGISTRY if p["name"] == name]
    assert len(matches) == 1, f"expected exactly one {name!r} entry, found {len(matches)}"
    return matches[0]


def _scheduler(engine=object()) -> SmartScheduler:
    sched = SmartScheduler.__new__(SmartScheduler)
    sched.engine = engine
    return sched


# ── Registration + cadence ──────────────────────────────────────────────


def test_wave1_pullers_registered_exactly_once() -> None:
    names = [p["name"] for p in PULLER_REGISTRY]
    for expected in ("eia", "finra_short_volume", "sec_ftd"):
        assert names.count(expected) == 1


def test_lme_warehouse_is_not_registered() -> None:
    """Both LME URLs returned HTTP 403 from grid-svr (2026-09-27 review) --
    registering it would just burn a scheduler slot forever returning zero
    rows. The adapter class itself is kept (see its docstring) but must
    not appear in PULLER_REGISTRY."""
    names = [p["name"] for p in PULLER_REGISTRY]
    assert "LME_Warehouse" not in names
    assert "lme_warehouse" not in names
    assert not any("lme" in n.lower() for n in names)


def test_registry_names_match_lowercased_source_catalog_names() -> None:
    """`_load_state_from_db` keys restart state by `name.lower()` read from
    `source_catalog`, and `_update_last_pull` writes back the same way
    (`WHERE LOWER(name) = :n`). A registry entry whose own name doesn't
    already lower() to the catalog name it will resolve to means restart
    state never loads for it -- every process restart looks like "never
    run" and a puller with a long freq_h re-runs immediately. Confirmed
    against grid-svr (read-only, name-only): the real catalog row is
    "EIA"; "FINRA_SHORT_VOLUME"/"SEC_FTD" don't exist yet and get
    auto-created with exactly the puller classes' own SOURCE_NAME.
    """
    from ingestion.altdata.eia_puller import EIAPuller
    from ingestion.altdata.finra_short_volume import FINRAShortVolumePuller
    from ingestion.altdata.sec_ftd import SECFTDPuller

    eia_entry = _registry_entry("eia")
    assert eia_entry["name"] == EIAPuller.SOURCE_NAME.lower()

    finra_entry = _registry_entry("finra_short_volume")
    assert finra_entry["name"] == FINRAShortVolumePuller.SOURCE_NAME.lower()

    sec_entry = _registry_entry("sec_ftd")
    assert sec_entry["name"] == SECFTDPuller.SOURCE_NAME.lower()


def test_eia_cadence_and_api_key_wiring() -> None:
    entry = _registry_entry("eia")
    assert entry["mod"] == "ingestion.altdata.eia_puller"
    assert entry["cls"] == "EIAPuller"
    assert entry["method"] == "pull"
    assert entry["freq_h"] == 24  # daily petroleum spot prices
    assert entry["api_key"] == "EIA_API_KEY"
    assert entry["api_key_mode"] == "env"


def test_finra_short_volume_cadence_and_catchup_wiring() -> None:
    entry = _registry_entry("finra_short_volume")
    assert entry["mod"] == "ingestion.altdata.finra_short_volume"
    assert entry["cls"] == "FINRAShortVolumePuller"
    # Registered against pull_recent (catch-up), NOT the single-date pull
    # -- see FINRAShortVolumePuller.pull_recent's docstring for why a
    # scheduler that only ever tries "today" silently loses missed dates.
    assert entry["method"] == "pull_recent"
    assert entry["freq_h"] == 24  # daily, published ~18:00 ET
    assert entry["kwargs"]["anchor_date"] is _finra_short_volume_trade_date
    assert callable(entry["kwargs"]["anchor_date"])
    assert entry["kwargs"]["weekdays_back"] == 5


def test_sec_ftd_cadence_and_adapter_wiring() -> None:
    entry = _registry_entry("sec_ftd")
    assert entry["mod"] == "ingestion.smart_scheduler"
    assert entry["cls"] == "_SECFTDSchedulerAdapter"
    assert entry["method"] == "pull"
    # Was 336h (~twice monthly); the 2026-09-27 review moved this to a 24h
    # cadence with a file-level already-ingested check inside pull_recent,
    # so a missed/failed half gets retried the next day instead of
    # waiting two weeks.
    assert entry["freq_h"] == 24


# ── FINRA trade_date rolling (deterministic, injected clock) ────────────


def _freeze_now(monkeypatch, fixed: datetime) -> None:
    from ingestion import smart_scheduler as ss

    class _FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):  # noqa: ARG003 -- tz ignored, fixed is already "ET"
            return fixed

    monkeypatch.setattr(ss, "datetime", _FixedDateTime)


def test_finra_trade_date_monday_before_1800et_rolls_to_friday(monkeypatch) -> None:
    # 2026-09-21 is a Monday. Before the ~18:00 ET publish cutoff, the
    # prior trading day's file is the latest one that can exist -- and
    # that prior trading day is the preceding Friday, not Sunday.
    _freeze_now(monkeypatch, datetime(2026, 9, 21, 17, 59))
    assert _finra_short_volume_trade_date() == date(2026, 9, 18)


def test_finra_trade_date_saturday_rolls_to_friday(monkeypatch) -> None:
    _freeze_now(monkeypatch, datetime(2026, 9, 19, 20, 0))  # Saturday evening
    assert _finra_short_volume_trade_date() == date(2026, 9, 18)


def test_finra_trade_date_sunday_rolls_to_friday(monkeypatch) -> None:
    _freeze_now(monkeypatch, datetime(2026, 9, 20, 20, 0))  # Sunday evening
    assert _finra_short_volume_trade_date() == date(2026, 9, 18)


def test_finra_trade_date_has_no_holiday_calendar_by_design(monkeypatch) -> None:
    """Documented, bounded limitation (see the function's own docstring):
    this rolls back over WEEKENDS only, with no market-holiday calendar --
    a market holiday that falls on a weekday is NOT rolled past here.
    Actual "no file exists for this date" handling (a holiday included)
    happens one layer down, in FINRAShortVolumePuller.pull/pull_recent
    treating a 403/404 as SKIPPED -- see
    tests/test_source_finra_short_volume.py::test_pull_404_returns_skipped_not_failed.
    """
    # 2026-11-26 is a Thursday (US Thanksgiving in 2026) -- a real market
    # holiday that falls on a weekday. This function has no way to know
    # that, so it returns the holiday date itself, unchanged.
    _freeze_now(monkeypatch, datetime(2026, 11, 26, 20, 0))
    assert _finra_short_volume_trade_date() == date(2026, 11, 26)


def test_finra_trade_date_is_always_a_weekday() -> None:
    trade_date = _finra_short_volume_trade_date()
    assert trade_date.weekday() < 5


def test_finra_trade_date_is_a_date_not_future() -> None:
    from datetime import timezone

    trade_date = _finra_short_volume_trade_date()
    assert trade_date <= (datetime.now(timezone.utc).date())


# ── SEC FTD half-month rolling ───────────────────────────────────────────


@pytest.mark.parametrize(
    "today,expected",
    [
        # Aug 2nd half publishes ~Sep 15 -> already out by Sep 26.
        (date(2026, 9, 26), {"yyyymm": "202608", "half": "b"}),
        # On Sep 1, only Aug's 1st half (published end-of-Aug) is out.
        (date(2026, 9, 1), {"yyyymm": "202608", "half": "a"}),
        # Exactly on the ~15th publish day for Aug's 2nd half.
        (date(2026, 9, 15), {"yyyymm": "202608", "half": "b"}),
        # Cross-year boundary: Dec 2025's 1st half published end-of-Dec.
        (date(2026, 1, 3), {"yyyymm": "202512", "half": "a"}),
        # Dec 31 is the publish day for Dec's own 1st half.
        (date(2026, 12, 31), {"yyyymm": "202612", "half": "a"}),
    ],
)
def test_sec_ftd_half_month_rolling(today: date, expected: dict) -> None:
    assert _sec_ftd_latest_published_half(today) == expected


def test_sec_ftd_half_month_rolling_handles_january_year_boundary() -> None:
    result = _sec_ftd_latest_published_half(date(2026, 1, 2))
    assert set(result) == {"yyyymm", "half"}
    assert result == {"yyyymm": "202512", "half": "a"}


def test_sec_ftd_published_halves_since_ends_at_the_latest_published_half() -> None:
    """The list-of-halves helper (used by pull_recent's catch-up) and the
    single-latest-half helper must agree on where the window ends."""
    today = date(2026, 9, 26)
    halves = _sec_ftd_published_halves_since(lookback_months=2, today=today)
    assert halves[-1] == _sec_ftd_latest_published_half(today)
    assert len(halves) >= 4  # at least ~2 months x 2 halves
    # Oldest-first, no duplicates, and nothing published in the future.
    assert halves == sorted(halves, key=lambda h: (h["yyyymm"], h["half"]))
    assert len(halves) == len({(h["yyyymm"], h["half"]) for h in halves})


# ── SEC FTD adapter: fail-closed on missing config, catch-up wiring ─────


class _StubSECFTDPuller:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def pull_recent(self, *, periods: list[dict]) -> dict:
        self.calls.append({"periods": periods})
        return {"status": "SUCCESS", "rows_inserted": 3, "periods": []}


def _sec_ftd_adapter_with_stub(stub: _StubSECFTDPuller) -> _SECFTDSchedulerAdapter:
    adapter = _SECFTDSchedulerAdapter.__new__(_SECFTDSchedulerAdapter)
    adapter._puller = stub
    return adapter


def test_sec_ftd_adapter_skips_cleanly_when_user_agent_unset(monkeypatch) -> None:
    from config import settings

    monkeypatch.setattr(settings, "SEC_USER_AGENT", "", raising=False)
    stub = _StubSECFTDPuller()
    adapter = _sec_ftd_adapter_with_stub(stub)

    result = adapter.pull()

    assert result["status"] == "SKIPPED"
    assert "SEC_USER_AGENT" in result["skipped_reason"]
    assert stub.calls == []  # never attempted the network call


def test_sec_ftd_adapter_feeds_pull_recent_with_published_halves(monkeypatch) -> None:
    from config import settings

    monkeypatch.setattr(settings, "SEC_USER_AGENT", "GRID test contact@example.com", raising=False)
    stub = _StubSECFTDPuller()
    adapter = _sec_ftd_adapter_with_stub(stub)

    result = adapter.pull()

    assert result == {"status": "SUCCESS", "rows_inserted": 3, "periods": []}
    assert len(stub.calls) == 1
    periods = stub.calls[0]["periods"]
    assert periods  # non-empty
    assert periods[-1] == _sec_ftd_latest_published_half()


# ── LME adapter: must fetch AND save (the bug the patch existed to fix) ──


class _StubLMEPuller:
    def __init__(self) -> None:
        self.saved_with: list = None
        self._last_source = "json"

    def pull(self) -> list:
        return ["snap1", "snap2"]

    def save_to_db(self, snapshots: list) -> int:
        self.saved_with = snapshots
        return len(snapshots)


def test_lme_adapter_fetches_and_saves() -> None:
    from ingestion.smart_scheduler import _LMEWarehouseSchedulerAdapter

    stub = _StubLMEPuller()
    adapter = _LMEWarehouseSchedulerAdapter.__new__(_LMEWarehouseSchedulerAdapter)
    adapter._puller = stub

    result = adapter.pull()

    assert stub.saved_with == ["snap1", "snap2"]  # save_to_db was actually called
    assert result == {
        "status": "SUCCESS",
        "fetched": 2,
        "inserted": 2,
        "source": "json",
    }


# ── EIA fail-closed on missing API key (registry-level contract) ────────


def test_eia_missing_api_key_skips_via_build_puller_instance() -> None:
    entry = _registry_entry("eia")

    class _FakeEIAPullerCls:
        def __init__(self, db_engine):
            self.db_engine = db_engine

    with pytest.raises(MissingPullerApiKey):
        _scheduler("engine")._build_puller_instance(entry, _FakeEIAPullerCls, {})


def test_eia_present_api_key_builds_instance_via_build_puller_instance() -> None:
    entry = _registry_entry("eia")

    class _FakeEIAPullerCls:
        def __init__(self, db_engine):
            self.db_engine = db_engine

    instance = _scheduler("engine")._build_puller_instance(
        entry, _FakeEIAPullerCls, {"EIA_API_KEY": "secret"}
    )
    assert isinstance(instance, _FakeEIAPullerCls)
    assert instance.db_engine == "engine"


# ── _run_puller: a puller's own FAILED/PARTIAL self-report is honoured ──


class _SelfReportingPuller:
    """Stands in for any real puller (EIAPuller, FINRAShortVolumePuller,
    ...) that returns a status-carrying dict instead of raising."""

    def __init__(self, db_engine, result: dict) -> None:
        self.db_engine = db_engine
        self._result = result

    def pull(self, **_kwargs) -> dict:
        return self._result


def _run_self_reporting(monkeypatch, result: dict) -> tuple[dict, SmartScheduler]:
    import importlib
    import types

    from ingestion import smart_scheduler as ss

    sched = SmartScheduler.__new__(SmartScheduler)
    sched.engine = object()
    sched._state = {}
    sched._thread_semaphore = ss.threading.Semaphore(2)
    sched._active_threads = set()
    sched._threads_lock = ss.threading.Lock()
    sched._orphan_thread_count = 0
    sched._update_last_pull = lambda name: sched._state.setdefault(  # type: ignore[method-assign]
        "last_pull_calls", []
    ).append(name)

    def _fake_cls(db_engine):
        return _SelfReportingPuller(db_engine, result)

    monkeypatch.setattr(
        importlib,
        "import_module",
        lambda _modpath: types.SimpleNamespace(_SelfReportingPuller=_fake_cls),
    )

    entry = {
        "name": "self_reporting_source",
        "mod": "fake.module",
        "cls": "_SelfReportingPuller",
        "method": "pull",
        "timeout_s": 5,
    }
    out = sched._run_puller(entry)
    return out, sched


def test_run_puller_honours_self_reported_failed(monkeypatch) -> None:
    """Before the 2026-09-27 fix, ANYTHING except an explicit SKIPPED was
    recorded as SUCCESS regardless of the puller's own returned status --
    a puller reporting {"status": "FAILED", ...} still advanced
    last_pull_at and reset its cooldown as if it had succeeded."""
    result, sched = _run_self_reporting(
        monkeypatch, {"status": "FAILED", "rows_inserted": 0, "error": "boom"}
    )

    assert result["status"] == "FAILED"
    assert result["error"] == "boom"
    assert sched._state.get("last_pull_calls", []) == []  # never advanced


def test_run_puller_honours_self_reported_partial(monkeypatch) -> None:
    result, sched = _run_self_reporting(
        monkeypatch,
        {"status": "PARTIAL", "rows_inserted": 1, "error": "one facet failed"},
    )

    assert result["status"] == "PARTIAL"
    assert sched._state.get("last_pull_calls", []) == []  # never advanced


def test_run_puller_treats_rows_inserted_zero_with_error_as_failed(monkeypatch) -> None:
    """Defensive fallback for a puller that doesn't set "status" at all
    but does report rows_inserted=0 alongside an "error" key."""
    result, sched = _run_self_reporting(
        monkeypatch, {"rows_inserted": 0, "error": "no status field at all"}
    )

    assert result["status"] == "FAILED"
    assert sched._state.get("last_pull_calls", []) == []


def test_run_puller_still_honours_genuine_success(monkeypatch) -> None:
    result, sched = _run_self_reporting(
        monkeypatch, {"status": "SUCCESS", "rows_inserted": 5}
    )

    assert result["status"] == "SUCCESS"
    assert sched._state.get("last_pull_calls", []) == ["self_reporting_source"]
