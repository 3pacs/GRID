"""Cadence-aware source staleness.

Background: ``GRID-STALE-SOURCES-AUDIT-20260929.md`` found the "N sources
stale" email (``scripts/hermes_health.py::check_db_health`` ->
``alerts/health_alerter.py``) applied one 26h freshness window to every
active ``source_catalog`` row regardless of how often that source actually
publishes. Of the 63 sources it flagged on the audit date, 46 were false
positives:

  * 14 whose writers legitimately never bump ``source_catalog.last_pull_at``
    even though fresh rows are landing in ``raw_series`` (mostly
    ``intelligence.scheduler`` writers and a few registry/catalog name
    mismatches);
  * 23 weekly/monthly/quarterly sources that were well within their real
    publication cadence;
  * 9 retired/orphan rows still marked ``active = TRUE`` (not handled here --
    deactivating them is a production data change that needs owner sign-off;
    see the audit, section 5, item 4).

``source_catalog`` carries an ``update_frequency`` column on the live DB,
but the audit found it reads ``'DAILY'`` for 110 of 111 active rows -- it
is not usable cadence metadata on its own. Rather than add a migration to
fix that column (out of scope for a "no new emails" fix, and the audit's
own recommendation keeps the schema change separate, gated on owner
approval), this module supplies a small in-code cadence map, built from the
audit's per-source investigation, that overrides catalog metadata for the
~40 sources it actually classified. Anything not in that map falls back to
``source_catalog.update_frequency`` / ``latency_class`` when either has a
recognized value, and finally to a conservative DAILY default -- i.e. any
source this module doesn't know about keeps exactly the grace window the
alert already used, so an unclassified source can only become MORE
correctly flagged over time (as it's added to the map), never silently
exempted.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta


@dataclass(frozen=True)
class Cadence:
    """One freshness tier: how overdue a source has to be before it's stale.

    ``business_day_aware`` only matters for DAILY: weekend (and a small set
    of US market holiday) calendar days between the last success and now
    are not counted against the source, so a Friday pull isn't flagged
    stale on Saturday morning.
    """

    kind: str
    grace_hours: float
    business_day_aware: bool = False


# Grace periods per the fix spec: daily ~26-30h business-day-aware, weekly
# ~8d, monthly ~40-45d, quarterly ~100d. SEMIMONTHLY is an extra tier for
# the one source (SEC_FTD) that publishes on a fixed twice-a-month window --
# WEEKLY is too tight for it and MONTHLY is too loose.
DAILY = Cadence("DAILY", grace_hours=30.0, business_day_aware=True)
WEEKLY = Cadence("WEEKLY", grace_hours=8 * 24.0)
MONTHLY = Cadence("MONTHLY", grace_hours=43 * 24.0)
QUARTERLY = Cadence("QUARTERLY", grace_hours=100 * 24.0)
SEMIMONTHLY = Cadence("SEMIMONTHLY", grace_hours=18 * 24.0)

DEFAULT_CADENCE = DAILY
"""Conservative default for any source not in ``SOURCE_CADENCE_OVERRIDES``
and with no usable catalog metadata: the same grace window the alert used
before this fix, so an unclassified source is never made *more* lenient by
accident."""

# source_catalog.update_frequency / latency_class raw value -> Cadence.
# Used only as a fallback for sources not in SOURCE_CADENCE_OVERRIDES below.
_METADATA_CADENCE: dict[str, Cadence] = {
    "DAILY": DAILY,
    "REALTIME": DAILY,
    "EOD": DAILY,
    "WEEKLY": WEEKLY,
    "MONTHLY": MONTHLY,
    "QUARTERLY": QUARTERLY,
    "BIANNUAL": QUARTERLY,  # coarsest supported tier; still far tighter than "never flag"
}

# Explicit per-source cadence, from GRID-STALE-SOURCES-AUDIT-20260929.md
# section 2's per-source table. Keys are source_catalog.name, lowercased.
# This is ground truth from the audit's individual investigation of each
# source's real publication schedule -- it takes priority over any catalog
# column, which is exactly why it exists (the catalog is wrong or absent
# for these rows). Sources in section 2D/2E (genuinely stale/broken writers,
# or narrow release windows) are included too, with their real cadence --
# the point of this map is an honest cadence, not a blanket exemption, so
# those legitimately-overdue sources still trip the alert.
SOURCE_CADENCE_OVERRIDES: dict[str, Cadence] = {
    # 2A -- fresh; writer just never bumps last_pull_at
    "treasury_auction": DAILY,
    "ais_ground_truth": DAILY,
    "sge_premium": DAILY,
    "credit_index_proxies": DAILY,
    "iron_ore_ports": WEEKLY,
    "container_freight": WEEKLY,
    "fed_h8": WEEKLY,
    "wikipedia_attention": DAILY,
    "yfinance_commodity_futures": DAILY,
    "fedspeeches": DAILY,
    "noaa_swpc": DAILY,
    "usaspending_gov": WEEKLY,
    "cboe": DAILY,
    "tiingo": DAILY,
    # 2B -- within real cadence; the old 26h rule was too tight
    "bls": WEEKLY,
    "aaii_sentiment": WEEKLY,
    "eia": WEEKLY,
    "nowcast": WEEKLY,
    "sec_edgar_fundamentals": WEEKLY,
    "finra_margin": MONTHLY,
    "finra_ats": WEEKLY,
    "ads_index": WEEKLY,
    "opencorporates": WEEKLY,
    "wikidata_persons": WEEKLY,
    "cftc_cot": WEEKLY,
    "bis": WEEKLY,
    "rbi": WEEKLY,
    "usda_nass": WEEKLY,
    "dbnomics": WEEKLY,
    "supply_chain": WEEKLY,
    "bis_export_controls": WEEKLY,
    "redfin": WEEKLY,
    "littlesis": WEEKLY,
    "wikidata": WEEKLY,
    "foia_cables": WEEKLY,
    "buyback_execution": QUARTERLY,
    "atlanta_fed_wage_tracker": MONTHLY,
    # 2D -- genuinely stale or broken writers (real cadence recorded so
    # they still trip the alert, honestly, instead of via the wrong reason)
    "open_meteo": DAILY,
    "tiingo_fundamentals": DAILY,
    "finviz_fundamentals": DAILY,
    # Owner-approved archive: weekly revision checks, not live news.
    "hf_financial_news": WEEKLY,
    "yfinance_options": DAILY,
    "lme_warehouse": DAILY,
    "reddit_options_pulse": DAILY,
    "taiwan_strait_osint": DAILY,
    "pboc_omo": DAILY,
    "taiwan_exports": MONTHLY,
    "jodi_oil": MONTHLY,
    "semi_book_to_bill": MONTHLY,
    "refinery_cracks": WEEKLY,
    "fed_mmf_composition": WEEKLY,
    "freight_cass_ata": MONTHLY,
    # 2E -- starved by scheduler restarts, or a narrow release window
    "finra_short_volume": DAILY,
    "sec_ftd": SEMIMONTHLY,
}

# Retired/orphan source_catalog rows still active = TRUE (audit section
# 2C). Deactivating them is a production data change needing owner sign-off
# (audit section 5, item 4) -- NOT done here. This is the exclusion hook the
# fix spec asks for *if one already exists*; none did, so it starts empty
# and is not populated by this change. An operator who gets owner sign-off
# to suppress these from the alert (without flipping `active`) can list
# their lowercased names here.
RETIRED_SOURCE_EXCLUSIONS: frozenset[str] = frozenset()


def cadence_for_source(
    name: str,
    *,
    update_frequency: str | None = None,
    latency_class: str | None = None,
) -> Cadence:
    """Resolve the cadence to use for one source_catalog row.

    Precedence: the audit-derived override map, then catalog metadata
    (``update_frequency`` first, then ``latency_class``) if it has a
    recognized value, then the conservative DAILY default.
    """
    override = SOURCE_CADENCE_OVERRIDES.get(name.strip().lower())
    if override is not None:
        return override
    if update_frequency:
        meta = _METADATA_CADENCE.get(update_frequency.strip().upper())
        if meta is not None:
            return meta
    if latency_class:
        meta = _METADATA_CADENCE.get(latency_class.strip().upper())
        if meta is not None:
            return meta
    return DEFAULT_CADENCE


def is_excluded(name: str) -> bool:
    return name.strip().lower() in RETIRED_SOURCE_EXCLUSIONS


# --------------------------------------------------------------------------
# Business-day-aware grace for DAILY cadence
# --------------------------------------------------------------------------

def _is_weekend(d: date) -> bool:
    return d.weekday() >= 5  # 5=Saturday, 6=Sunday


def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> date:
    """The n-th (1-indexed) occurrence of ``weekday`` (Mon=0) in year/month."""
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + 7 * (n - 1))


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    nxt = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    last_day = nxt - timedelta(days=1)
    offset = (last_day.weekday() - weekday) % 7
    return last_day - timedelta(days=offset)


def _us_market_holidays(year: int) -> frozenset[date]:
    """A conservative subset of NYSE-observed holidays.

    Not a full market calendar -- deliberately so. Missing a minor holiday
    only means a source keeps the same grace it already had (still counted
    as a business day); it can never make the check MORE lenient than the
    fixed + weekend set already gives it.
    """
    return frozenset({
        date(year, 1, 1),                      # New Year's Day
        _nth_weekday_of_month(year, 1, 0, 3),   # MLK Day (3rd Mon Jan)
        _nth_weekday_of_month(year, 2, 0, 3),   # Presidents' Day (3rd Mon Feb)
        _last_weekday_of_month(year, 5, 0),     # Memorial Day (last Mon May)
        date(year, 6, 19),                      # Juneteenth
        date(year, 7, 4),                       # Independence Day
        _nth_weekday_of_month(year, 9, 0, 1),   # Labor Day (1st Mon Sep)
        _nth_weekday_of_month(year, 11, 3, 4),  # Thanksgiving (4th Thu Nov)
        date(year, 12, 25),                     # Christmas
    })


def _non_business_hours_between(start: datetime, end: datetime) -> float:
    """Hours of weekend/holiday calendar time strictly after ``start``'s
    own day, up to and including ``end``'s day. Bounded, cheap loop -- for
    a DAILY-cadence source the gap here is always a handful of days, never
    unbounded."""
    if end <= start:
        return 0.0
    hours = 0.0
    day = start.date() + timedelta(days=1)
    end_date = end.date()
    holidays_by_year: dict[int, frozenset[date]] = {}
    while day <= end_date:
        holidays = holidays_by_year.setdefault(day.year, _us_market_holidays(day.year))
        if _is_weekend(day) or day in holidays:
            hours += 24.0
        day += timedelta(days=1)
    return hours


def age_hours(last_success: datetime | None, now: datetime) -> float | None:
    """Hours since ``last_success``, or ``None`` if it never happened."""
    if last_success is None:
        return None
    return (now - last_success).total_seconds() / 3600.0


def is_stale(last_success: datetime | None, now: datetime, cadence: Cadence) -> bool:
    """True when ``last_success`` is older than ``cadence`` allows as of ``now``.

    Both datetimes must be timezone-aware and in the same timezone (the
    caller uses UTC throughout). A ``None`` ``last_success`` (never
    successfully pulled) is always stale.
    """
    if last_success is None:
        return True
    hours = age_hours(last_success, now)
    grace = cadence.grace_hours
    if cadence.business_day_aware:
        grace += _non_business_hours_between(last_success, now)
    return hours > grace
