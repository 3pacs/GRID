"""The rule calendar in ``ingestion.market_calendar`` against the NYSE record (#1084, R1).

From 2007-11-01 to 2027-12-31 the reference is the pinned exchange_calendars
XNYS session manifests already in ``docs/reference/calendar-manifests``: every
weekday must be open in ``market_calendar`` exactly when the manifest lists a
session. Before 2007-11 the reference is the NYSE closure history written out
below (MLK Day from 1998; the one-off closures of 1985-2007).

Before this change the rule calendar called MLK Day closed in 1985-1997 and
Juneteenth closed in 2008-2021, and it missed every one-off closure, so a
repair or audit asking it whether a stored close could exist got the wrong
answer on those days (gold_full and cl_close still serve a 0.655 / 4.175 print
on 2001-09-11, a day the calendar called a session).
"""
from __future__ import annotations

import hashlib
import itertools
import json
from datetime import date, timedelta
from pathlib import Path

import pytest

from ingestion import market_calendar as mc

MANIFEST_DIR = Path(__file__).resolve().parents[1] / "docs" / "reference" / "calendar-manifests"

#: (file, sha256 of its bytes, first day used, last day used). Pinned so a
#: changed manifest cannot silently change what this test proves.
MANIFESTS = (
    ("vs1-xnys-2007-11-01-2026-06-30.json", "9b8f0079f89eb6b5f744a6466373fb9af1af74abaafb01b68dc89e39984372ac", date(2007, 11, 1), date(2026, 6, 30)),
    ("factory-xnys-2025-01-01-2026-11-10.json", "8ba2e6ee00506cb60a4128c550d977090518b25f9d571e828122826bb5b7e141", date(2026, 7, 1), date(2026, 11, 10)),
    ("xnys-rth-20261001-20271231.json", "efa8897e2fd94afd4f9f9b29951f142b97aece83812aca7403026df1ec4eb8ad", date(2026, 11, 11), date(2027, 12, 31)),
)


def _sessions(name: str) -> tuple[set[date], str, date, date]:
    raw = (MANIFEST_DIR / name).read_bytes()
    doc = json.loads(raw)
    assert doc["exchange"] == "XNYS" and doc["session_type"] == "RTH"
    days = {date.fromisoformat(s["key"].split(":")[1]) for s in doc["sessions"]}
    return days, hashlib.sha256(raw).hexdigest(), date.fromisoformat(doc["start"]), date.fromisoformat(doc["end"])


def _weekdays(lo: date, hi: date):
    d = lo
    while d <= hi:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


@pytest.mark.parametrize("name,sha,lo,hi", MANIFESTS)
def test_rule_calendar_matches_pinned_xnys_manifest(name: str, sha: str, lo: date, hi: date) -> None:
    sessions, digest, start, end = _sessions(name)
    assert digest == sha
    assert start <= lo and hi <= end
    wrong = [d for d in _weekdays(lo, hi) if mc.is_market_open(d) != (d in sessions)]
    assert wrong == [], f"{name}: {len(wrong)} weekdays disagree, first {wrong[:5]}"


def test_manifests_cover_2007_11_to_2027_without_a_gap() -> None:
    covered = sorted((lo, hi) for _n, _s, lo, hi in MANIFESTS)
    for (_lo1, hi1), (lo2, _hi2) in itertools.pairwise(covered):
        assert lo2 == hi1 + timedelta(days=1)


# NYSE history before the manifests start (2007-11-01).
CLOSED_BEFORE_2007_11 = (
    date(1985, 9, 27),   # Hurricane Gloria
    date(1994, 4, 27),   # Nixon day of mourning
    date(2001, 9, 11), date(2001, 9, 12), date(2001, 9, 13), date(2001, 9, 14),
    date(2004, 6, 11),   # Reagan day of mourning
    date(2007, 1, 2),    # Ford day of mourning
    date(1998, 1, 19),   # first MLK Day holiday
    date(2001, 1, 15), date(2007, 1, 15),
)
OPEN_BEFORE_2007_11 = (
    # Third Monday of January before MLK Day became an NYSE holiday in 1998.
    date(1985, 1, 21), date(1990, 1, 15), date(1996, 1, 15), date(1997, 1, 20),
    # Ordinary sessions next to the one-off closures.
    date(1985, 9, 26), date(1985, 9, 30), date(2001, 9, 10), date(2001, 9, 17),
    date(2004, 6, 10), date(2004, 6, 14), date(2007, 1, 3),
    # June 19 / its weekday observance before Juneteenth became a holiday.
    date(2000, 6, 19), date(2006, 6, 19),
)


@pytest.mark.parametrize("d", CLOSED_BEFORE_2007_11)
def test_known_pre_manifest_closures_are_closed(d: date) -> None:
    assert not mc.is_market_open(d)


@pytest.mark.parametrize("d", OPEN_BEFORE_2007_11)
def test_known_pre_manifest_sessions_are_open(d: date) -> None:
    assert mc.is_market_open(d)


def test_every_special_closure_is_a_weekday_and_closed() -> None:
    for d in mc.SPECIAL_CLOSURES:
        assert d.weekday() < 5
        assert mc.is_market_holiday(d) and not mc.is_market_open(d)


def test_juneteenth_first_observed_2022_and_kept_after() -> None:
    assert mc.is_market_open(date(2021, 6, 18))  # Saturday 2021-06-19 observance was not a holiday
    assert not mc.is_market_open(date(2022, 6, 20))
    assert not mc.is_market_open(date(2026, 6, 19))


def test_trading_day_navigation_steps_over_special_closures() -> None:
    assert mc.next_trading_day(date(2001, 9, 11)) == date(2001, 9, 17)
    assert mc.last_trading_day(date(2001, 9, 16)) == date(2001, 9, 10)
    assert date(2012, 10, 29) not in mc.trading_days_between(date(2012, 10, 26), date(2012, 11, 1))


def test_repair_script_trading_day_unchanged_from_2010() -> None:
    """The applied _full repairs ran from 2010-01-01; their calendar answer must not move."""
    import scripts.repair_full_panel_vintages as rep

    old_special = {date(2012, 10, 29), date(2012, 10, 30), date(2018, 12, 5), date(2025, 1, 9)}

    def juneteenth_observed(year: int) -> date:
        d = date(year, 6, 19)
        return d - timedelta(days=1) if d.weekday() == 5 else d + timedelta(days=1) if d.weekday() == 6 else d

    def old_market_open(d: date) -> bool:
        # market_calendar before 2026-11-24: no one-off closures, Juneteenth every year.
        holidays = (mc.market_holidays(d.year) - set(mc.SPECIAL_CLOSURES)) | {juneteenth_observed(d.year)}
        return d.weekday() < 5 and d not in holidays

    def old_rule(d: date) -> bool:
        # The rule the repair script carried locally before 2026-11-24.
        if d in old_special:
            return False
        if old_market_open(d):
            return True
        return d.year < 2022 and d.weekday() < 5 and d == juneteenth_observed(d.year)

    changed = [d for d in _weekdays(date(2010, 1, 1), date(2027, 12, 31)) if rep.is_trading_day(d) != old_rule(d)]
    assert changed == []
