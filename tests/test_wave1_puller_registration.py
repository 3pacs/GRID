"""Tests for the Wave 1 puller activation (2026-09-27).

Covers the four merged-but-previously-unscheduled pullers registered in
``ingestion.smart_scheduler.PULLER_REGISTRY``:

- EIA_Energy       (PR #553, previously unregistered)
- LME_Warehouse    (PR #553, previously unregistered)
- FINRA_Short_Volume (PR #564, previously unregistered)
- SEC_FTD          (PR #564, previously unregistered)

These were NOT wired via the two .patch files shipped in
docs/handoffs/2026-09-18/ -- those target ``ingestion/scheduler.py``,
which is not the scheduler Hermes actually runs. See the "Wave 1
activation helpers" comment in ingestion/smart_scheduler.py.
"""

from __future__ import annotations

from datetime import date

import pytest

from ingestion.smart_scheduler import (
    MissingPullerApiKey,
    PULLER_REGISTRY,
    SmartScheduler,
    _finra_short_volume_trade_date,
    _LMEWarehouseSchedulerAdapter,
    _SECFTDSchedulerAdapter,
    _sec_ftd_latest_published_half,
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


def test_all_four_wave1_pullers_registered_exactly_once() -> None:
    names = [p["name"] for p in PULLER_REGISTRY]
    for expected in (
        "EIA_Energy",
        "LME_Warehouse",
        "FINRA_Short_Volume",
        "SEC_FTD",
    ):
        assert names.count(expected) == 1


def test_eia_energy_cadence_and_api_key_wiring() -> None:
    entry = _registry_entry("EIA_Energy")
    assert entry["mod"] == "ingestion.altdata.eia_puller"
    assert entry["cls"] == "EIAPuller"
    assert entry["method"] == "pull"
    assert entry["freq_h"] == 24  # daily petroleum spot prices
    assert entry["api_key"] == "EIA_API_KEY"
    assert entry["api_key_mode"] == "env"


def test_lme_warehouse_cadence_and_adapter_wiring() -> None:
    entry = _registry_entry("LME_Warehouse")
    assert entry["mod"] == "ingestion.smart_scheduler"
    assert entry["cls"] == "_LMEWarehouseSchedulerAdapter"
    assert entry["method"] == "pull"
    assert entry["freq_h"] == 24  # daily warehouse stocks (CAT-51)
    assert "api_key" not in entry  # public endpoint, no key needed


def test_finra_short_volume_cadence_and_trade_date_kwarg() -> None:
    entry = _registry_entry("FINRA_Short_Volume")
    assert entry["mod"] == "ingestion.altdata.finra_short_volume"
    assert entry["cls"] == "FINRAShortVolumePuller"
    assert entry["method"] == "pull"
    assert entry["freq_h"] == 24  # daily, published ~18:00 ET
    assert entry["kwargs"]["trade_date"] is _finra_short_volume_trade_date
    assert callable(entry["kwargs"]["trade_date"])


def test_sec_ftd_cadence_and_adapter_wiring() -> None:
    entry = _registry_entry("SEC_FTD")
    assert entry["mod"] == "ingestion.smart_scheduler"
    assert entry["cls"] == "_SECFTDSchedulerAdapter"
    assert entry["method"] == "pull"
    assert entry["freq_h"] == 336  # ~twice monthly (14 days)


# ── FINRA trade_date rolling ─────────────────────────────────────────────


def test_finra_trade_date_is_always_a_weekday() -> None:
    trade_date = _finra_short_volume_trade_date()
    assert trade_date.weekday() < 5


def test_finra_trade_date_is_a_date_not_future() -> None:
    from datetime import datetime, timezone

    trade_date = _finra_short_volume_trade_date()
    # Never more than 1 calendar day ahead of "now" in any timezone we'd
    # plausibly be running in -- guards against an inverted rolling bug.
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
    # January 2nd is early enough in the month that even last month's
    # (December's) 2nd half has not published yet (~Jan 15) -- exercises
    # the "walk back crosses a year boundary" arithmetic without going
    # anywhere near the y/m=0 edge that divmod() can't represent as a date.
    result = _sec_ftd_latest_published_half(date(2026, 1, 2))
    assert set(result) == {"yyyymm", "half"}
    assert result == {"yyyymm": "202512", "half": "a"}


# ── SEC FTD adapter: fail-closed on missing config ─────────────────────


class _StubSECFTDPuller:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def pull(self, *, yyyymm: str, half: str) -> dict:
        self.calls.append({"yyyymm": yyyymm, "half": half})
        return {"status": "SUCCESS", "rows_inserted": 3}


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


def test_sec_ftd_adapter_rolls_period_when_configured(monkeypatch) -> None:
    from config import settings

    monkeypatch.setattr(settings, "SEC_USER_AGENT", "GRID test contact@example.com", raising=False)
    stub = _StubSECFTDPuller()
    adapter = _sec_ftd_adapter_with_stub(stub)

    result = adapter.pull()

    assert result == {"status": "SUCCESS", "rows_inserted": 3}
    assert len(stub.calls) == 1
    assert stub.calls[0] == _sec_ftd_latest_published_half()


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
    entry = _registry_entry("EIA_Energy")

    class _FakeEIAPullerCls:
        def __init__(self, db_engine):
            self.db_engine = db_engine

    with pytest.raises(MissingPullerApiKey):
        _scheduler("engine")._build_puller_instance(entry, _FakeEIAPullerCls, {})


def test_eia_present_api_key_builds_instance_via_build_puller_instance() -> None:
    entry = _registry_entry("EIA_Energy")

    class _FakeEIAPullerCls:
        def __init__(self, db_engine):
            self.db_engine = db_engine

    instance = _scheduler("engine")._build_puller_instance(
        entry, _FakeEIAPullerCls, {"EIA_API_KEY": "secret"}
    )
    assert isinstance(instance, _FakeEIAPullerCls)
    assert instance.db_engine == "engine"
