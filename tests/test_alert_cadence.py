"""Tests for alerts/cadence.py (GRID-STALE-SOURCES-AUDIT-20260929.md).

Pure-function coverage — no database needed. Exercises:
  * override-map precedence over catalog metadata over the DAILY default
  * business-day-aware grace for DAILY (weekend + a holiday case)
  * never-pulled sources are always stale
  * the SEC_FTD-style SEMIMONTHLY tier
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from alerts import cadence


def _dt(y, m, d, h=0):
    return datetime(y, m, d, h, tzinfo=timezone.utc)


# ── cadence_for_source precedence ────────────────────────────────────────


class TestCadenceForSource:
    def test_override_map_hit_is_case_and_whitespace_insensitive(self):
        assert cadence.cadence_for_source("BLS") == cadence.WEEKLY
        assert cadence.cadence_for_source(" bls ") == cadence.WEEKLY
        assert cadence.cadence_for_source("Bls") == cadence.WEEKLY

    def test_override_map_wins_over_catalog_metadata(self):
        # BLS's catalog update_frequency famously says DAILY (the audit's
        # "110 of 111" finding) — the override must win regardless.
        got = cadence.cadence_for_source("BLS", update_frequency="DAILY")
        assert got == cadence.WEEKLY

    def test_sec_ftd_gets_the_semimonthly_tier(self):
        assert cadence.cadence_for_source("sec_ftd") == cadence.SEMIMONTHLY

    def test_quarterly_override(self):
        assert cadence.cadence_for_source("buyback_execution") == cadence.QUARTERLY

    def test_unmapped_source_falls_back_to_update_frequency(self):
        got = cadence.cadence_for_source("some_new_source", update_frequency="monthly")
        assert got == cadence.MONTHLY

    def test_unmapped_source_falls_back_to_latency_class_when_no_update_frequency(self):
        got = cadence.cadence_for_source("some_new_source", latency_class="WEEKLY")
        assert got == cadence.WEEKLY

    def test_update_frequency_takes_priority_over_latency_class(self):
        got = cadence.cadence_for_source(
            "some_new_source", update_frequency="QUARTERLY", latency_class="WEEKLY",
        )
        assert got == cadence.QUARTERLY

    def test_unrecognized_metadata_values_fall_through_to_default(self):
        got = cadence.cadence_for_source("some_new_source", update_frequency="FORTNIGHTLY")
        assert got == cadence.DEFAULT_CADENCE

    def test_fully_unknown_source_gets_conservative_daily_default(self):
        assert cadence.cadence_for_source("totally_unclassified_source") == cadence.DAILY
        assert cadence.DEFAULT_CADENCE == cadence.DAILY


# ── is_excluded ──────────────────────────────────────────────────────────


class TestIsExcluded:
    def test_exclusion_set_starts_empty(self):
        # No existing exclusion-hook mechanism was found in the codebase,
        # so this fix leaves retired-row deactivation to the owner (audit
        # §5 item 4) rather than inventing one and silently populating it.
        assert cadence.RETIRED_SOURCE_EXCLUSIONS == frozenset()

    def test_nothing_is_excluded_by_default(self):
        assert cadence.is_excluded("GDELT_BULK") is False
        assert cadence.is_excluded("any_source") is False


# ── is_stale / age_hours ─────────────────────────────────────────────────


class TestIsStale:
    def test_never_pulled_is_always_stale(self):
        assert cadence.is_stale(None, _dt(2026, 9, 28), cadence.DAILY) is True
        assert cadence.age_hours(None, _dt(2026, 9, 28)) is None

    def test_daily_within_grace_is_fresh(self):
        # A Tuesday->Wednesday gap (no weekend involved): 20h < 30h grace.
        last = _dt(2026, 9, 22, 8)  # Tuesday 08:00
        now = _dt(2026, 9, 23, 4)   # Wednesday 04:00 (+20h)
        assert cadence.is_stale(last, now, cadence.DAILY) is False

    def test_daily_past_grace_on_a_weekday_is_stale(self):
        last = _dt(2026, 9, 22, 8)   # Tuesday 08:00
        now = _dt(2026, 9, 23, 20)   # Wednesday 20:00 (+36h, no weekend)
        assert cadence.is_stale(last, now, cadence.DAILY) is True

    def test_weekly_within_cadence_is_fresh(self):
        last = _dt(2026, 9, 20)
        now = _dt(2026, 9, 25)  # +5 days, well under the 8-day grace
        assert cadence.is_stale(last, now, cadence.WEEKLY) is False

    def test_weekly_past_cadence_is_stale(self):
        last = _dt(2026, 9, 1)
        now = _dt(2026, 9, 15)  # +14 days > 8-day grace
        assert cadence.is_stale(last, now, cadence.WEEKLY) is True

    def test_monthly_within_cadence_is_fresh(self):
        last = _dt(2026, 8, 1)
        now = _dt(2026, 9, 10)  # ~40 days, within the ~43-day grace
        assert cadence.is_stale(last, now, cadence.MONTHLY) is False


class TestDailyBusinessDayAwareGrace:
    """The audit's business-day-aware requirement: weekends (and a small
    set of US market holidays) don't count against a DAILY source."""

    def test_friday_pull_is_not_stale_monday_morning(self):
        # Friday 2026-09-18 08:00 -> Monday 2026-09-21 10:00 is 74h of wall
        # clock, well past the plain 30h grace, but only ~26h of it is
        # business time (Fri 08:00->24:00 = 16h, then Mon 00:00->10:00 =
        # 10h; Sat/Sun don't count).
        last = _dt(2026, 9, 18, 8)   # Friday
        now = _dt(2026, 9, 21, 10)   # Monday
        assert cadence.is_stale(last, now, cadence.DAILY) is False

    def test_friday_pull_is_stale_if_still_missing_tuesday(self):
        last = _dt(2026, 9, 18, 8)   # Friday
        now = _dt(2026, 9, 22, 20)   # Tuesday (one full extra business day late)
        assert cadence.is_stale(last, now, cadence.DAILY) is True

    def test_weekday_gap_is_unaffected_by_weekend_logic(self):
        # A Wednesday->Thursday gap should behave identically to the plain
        # (non-business-day-aware) case since no weekend falls inside it.
        last = _dt(2026, 9, 23, 8)   # Wednesday
        now = _dt(2026, 9, 25, 20)   # Friday (+60h, no weekend crossed)
        assert cadence.is_stale(last, now, cadence.DAILY) is True

    def test_holiday_adjacent_to_a_weekend_extends_the_grace(self):
        # Memorial Day 2026 falls on Monday 2026-05-25, right after a
        # weekend -- a classic 3-day-weekend gap. A source pulled Friday
        # 2026-05-22 08:00 and not checked again until Tuesday 2026-05-26
        # 10:00 has three non-business days in between (Sat, Sun, the
        # Monday holiday) plus the plain 30h grace -- must not be flagged.
        # (Without the holiday in the calendar, grace would only cover the
        # weekend -- 30h + 48h = 78h -- and this same 98h gap WOULD be
        # flagged; the holiday is what keeps it fresh.)
        last = _dt(2026, 5, 22, 8)
        now = _dt(2026, 5, 26, 10)
        assert cadence.is_stale(last, now, cadence.DAILY) is False

    def test_holiday_extension_does_not_cover_a_second_missed_business_day(self):
        last = _dt(2026, 5, 22, 8)
        now = _dt(2026, 5, 27, 20)  # Wednesday -- one more business day late
        assert cadence.is_stale(last, now, cadence.DAILY) is True

    @pytest.mark.parametrize(
        "year,expected",
        [
            (2026, {
                "2026-01-01", "2026-01-19", "2026-02-16", "2026-05-25",
                "2026-06-19", "2026-07-04", "2026-09-07", "2026-11-26",
                "2026-12-25",
            }),
        ],
    )
    def test_computed_holiday_set_matches_known_dates(self, year, expected):
        holidays = cadence._us_market_holidays(year)
        assert {d.isoformat() for d in holidays} == expected
