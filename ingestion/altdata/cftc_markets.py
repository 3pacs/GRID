"""CFTC COT market identity and publication-time rules (pure, no I/O).

Why this module exists
----------------------
The legacy puller matched Socrata rows by a substring of
``market_and_exchange_names`` (``"S&P 500"``, ``"GOLD"``, ``"WHEAT"`` ...)
and kept whichever row arrived first per report date. Many markets share
those substrings (E-mini S&P 500, Micro E-mini, S&P 500 dividend-index
futures; COMEX gold, micro gold, Coinbase PAX GOLD perp; SRW/HRW/HRSpring
wheat; Henry Hub vs basis-index natural gas; NYMEX vs ICE WTI), so one
``cftc.<KEY>.*`` series silently switched instrument from week to week.
Names also drift: on 2022-02-08 the CFTC renamed most markets
(``10-YEAR U.S. TREASURY NOTES`` -> ``UST 10Y NOTE``, ``CRUDE OIL, LIGHT
SWEET`` -> ``WTI-PHYSICAL``), which is why the Treasury keys went dead.

The stable identity is the Socrata field ``cftc_contract_market_code``
(verified 2026-09-26 against dataset ``6dca-aqww``: every code below maps to
one market across every rename, and no code has two rows on one report date).

Identity scheme
---------------
``cftc.<cftc_contract_market_code>.<metric>`` — e.g.
``cftc.13874A.net_speculative`` for the CME E-mini S&P 500. The root symbol
(``ES``) and the human-readable label are metadata only (``raw_payload``);
nothing ever matches on a name. The legacy ``cftc.SP500.*`` /
``cftc.GOLD.*`` / ... ids are frozen history and must not be read by
analytical code (``LEGACY_CONTRACT_KEYS``; guard test
``tests/test_cftc_cot_market_code.py``).

Publication time
----------------
``report_date`` is the Tuesday the positions are measured. The CFTC
publishes them the following Friday at 15:30 ET. When a federal holiday
falls Tuesday–Friday of that week, the release moves to the next federal
business day after the Friday (the 2026 CFTC schedule: Jan 5, Jun 22, Jul 6,
Nov 16, Nov 30, Dec 28 — all Mondays; Monday holidays such as Labor Day do
*not* shift the Friday release). ``release_at`` from this rule is a
**floor**, not an observation: ad-hoc closures (e.g. a national day of
mourning) and funding lapses (the 2025 shutdown) delay releases further and
are not in any calendar. The binding known-at for a stored row is its
``raw_series.pull_timestamp``.

Non-Tuesday report dates (holiday-shifted weeks)
-------------------------------------------------
The CFTC moves the position date to Monday (once, to Wednesday) when the
usual Tuesday is a federal holiday. #682's first production run skipped 220
rows across 14 such report dates (13 Mondays, one Wednesday) because the
Tuesday-only rule above returned ``release_at = None`` for every one of
them. Fixed here with two layers, in priority order:

1. ``CONFIRMED_HOLIDAY_RELEASES`` — an explicit table of the six dates where
   the actual publication instant was pinned against a primary source (a
   CFTC special announcement or press release, cross-checked against the
   archived report file's own HTTP ``Last-Modified`` timestamp where that
   timestamp predates the 2018-01-17 site migration that touched every
   pre-2018 archive file). See the table's own comments for citations.
2. A conservative fallback for every other non-Tuesday (Monday/Wednesday)
   report date: 15:30 ET on the next federal business day *after* the
   Friday that would normally close that report's week. This is
   deliberately pessimistic — every confirmed case in this module's history
   published no later than that (one, 2008-12-22, published exactly on it;
   the rest published on or before their normal Friday, which is earlier
   than the fallback) — so it never claims a row was public before it
   truly was, at the cost of being up to one business day late for weeks
   that in fact released on the normal, unshifted Friday. A week disrupted
   by something bigger than a single holiday (a funding lapse) needs its
   own confirmed entry, not this fallback: see 2025-11-10 in the table.

A report date that is not a Tuesday, Monday, or Wednesday has no rule at
all and ``release_at`` is ``None`` with a reason (this should not happen
for real CFTC data; every observed shift has used Monday or, once,
Wednesday).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from zoneinfo import ZoneInfo

#: Socrata dataset: Legacy Commitments of Traders, futures only.
SOCRATA_DATASET: str = "6dca-aqww"

#: Socrata field that carries the stable market identity.
MARKET_CODE_FIELD: str = "cftc_contract_market_code"

_ET = ZoneInfo("America/New_York")
_RELEASE_TIME_ET = time(15, 30)

#: Rule identifier stored alongside every computed ``release_at``. Bumped to
#: v2 for #682's fix: v1 always returned ``release_at = None`` for a
#: non-Tuesday report date; v2 adds ``CONFIRMED_HOLIDAY_RELEASES`` and the
#: conservative Monday/Wednesday fallback below.
RELEASE_RULE: str = "cftc_schedule_rule_v2"

#: Confirmed publication instants (UTC) for non-Tuesday report dates,
#: pinned against a primary source and never earlier than the true release.
#: Checked 2026-09-27. Every date here is one of the 14 (13 Mondays, one
#: Wednesday) that #682's first production run skipped with
#: ``no_computable_release_time``; the other 8 have no confirmed source and
#: fall through to the conservative fallback in ``compute_release``.
CONFIRMED_HOLIDAY_RELEASES: dict[date, datetime] = {
    # 2008-12-22 (Monday; the following Tuesday, 2008-12-23, is not itself a
    # federal holiday, but Thursday 2008-12-25 (Christmas) falls inside that
    # report week and delayed processing). CFTC's Historical Special
    # Announcements page, 2008 section, undated note titled "Holiday
    # Release Schedule" (posted alongside the December 29, 2008 entries),
    # verbatim: "the next two releases will be on Monday, December 29, and
    # Monday, January 5 at 3:30 p.m. In addition, the COT report released
    # on December 29 will be for the prior Monday's open interest positions
    # instead of the usual Tuesday's."
    # <https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalSpecialAnnouncements/index.htm>
    date(2008, 12, 22): datetime(2008, 12, 29, 20, 30, tzinfo=timezone.utc),  # 15:30 ET exactly, as stated
    # 2018-12-24 and 2018-12-31 (Monday; the following Tuesdays, 2018-12-25
    # and 2019-01-01, are both federal holidays). CFTC's special
    # announcement of 2018-12-21 said these would publish "Friday as usual"
    # (2018-12-28 / 2019-01-04), but the 35-day funding lapse that began
    # the very next day, 2018-12-22, suspended COT publication entirely
    # ("December 22, 2018: During the shutdown of the federal government,
    # the Commitments of Traders report will not be published."). Press
    # release 7864-19 (2019-01-29) gives the actual catch-up schedule: the
    # 2018-12-24 report -- "previously scheduled for release on Friday,
    # December 28, 2018" -- was expected to publish "Friday, February 1,
    # 2019", after which the CFTC published "one report on Tuesday and
    # another on Friday of each week until the reports are current"; the
    # next report in that sequence is 2018-12-31, published the following
    # Tuesday, 2019-02-05. Confirmed against the archived report files'
    # own HTTP ``Last-Modified`` (checked 2026-09-27; these files postdate
    # the 2018-01-17 CFTC site migration, so their timestamps are genuine):
    #   financial_lof122418.htm -> Fri, 01 Feb 2019 20:31:34 GMT (15:31 ET)
    #   financial_lof123118.htm -> Tue, 05 Feb 2019 20:33:29 GMT (15:33 ET)
    # <https://www.cftc.gov/PressRoom/PressReleases/7864-19>
    # <https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalSpecialAnnouncements/index.htm>
    # <https://www.cftc.gov/sites/default/files/files/dea/cotarchives/2018/options/financial_lof122418.htm>
    # <https://www.cftc.gov/sites/default/files/files/dea/cotarchives/2018/options/financial_lof123118.htm>
    date(2018, 12, 24): datetime(2019, 2, 1, 20, 32, tzinfo=timezone.utc),  # rounded up from 20:31:34
    date(2018, 12, 31): datetime(2019, 2, 5, 20, 34, tzinfo=timezone.utc),  # rounded up from 20:33:29
    # 2020-12-21 (Monday; the following Tuesday, 2020-12-22, is not a
    # holiday, but Friday 2020-12-25 -- the normal release day -- is
    # Christmas). Special announcement, 2020-12-28: "Due to the additional
    # federal holiday on December 24th, we published data dated Monday
    # December 21, 2020 on Monday December 28, 2020." Archived file
    # financial_lof122120.htm: Last-Modified Mon, 28 Dec 2020 20:29:04 GMT
    # (15:29 ET; checked 2026-09-27).
    # <https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalSpecialAnnouncements/index.htm>
    # <https://www.cftc.gov/sites/default/files/files/dea/cotarchives/2020/options/financial_lof122120.htm>
    date(2020, 12, 21): datetime(2020, 12, 28, 20, 30, tzinfo=timezone.utc),  # rounded up from 20:29:04
    # 2023-07-03 (Monday; the following Tuesday, 2023-07-04, is the
    # holiday). No special announcement was needed: this was a routine,
    # unshifted Friday release. Archived file financial_lof070323.htm:
    # Last-Modified Fri, 07 Jul 2023 19:28:53 GMT (15:28 ET; checked
    # 2026-09-27).
    # <https://www.cftc.gov/sites/default/files/files/dea/cotarchives/2023/options/financial_lof070323.htm>
    date(2023, 7, 3): datetime(2023, 7, 7, 19, 29, tzinfo=timezone.utc),  # rounded up from 19:28:53
    # 2025-11-10 (Monday; the following Tuesday, 2025-11-11, Veterans Day,
    # is the holiday) fell inside the 2025-10-01..11-12 federal funding
    # lapse and was published on the post-shutdown catch-up schedule, not
    # on any weekly rule -- exactly the case this module's fallback rule is
    # NOT meant to guess at. Press release 9138-25 (2025-11-18) originally
    # scheduled it for 2025-12-12; press release 9147-25 (2025-12-09)
    # accelerated the catch-up and moved it to 2025-12-10. Archived file
    # financial_lof111025.htm: Last-Modified Wed, 10 Dec 2025 21:14:18 GMT
    # (16:14 ET), confirming the accelerated date is what actually shipped
    # (checked 2026-09-27).
    # <https://www.cftc.gov/PressRoom/PressReleases/9138-25>
    # <https://www.cftc.gov/PressRoom/PressReleases/9147-25>
    # <https://www.cftc.gov/sites/default/files/files/dea/cotarchives/2025/options/financial_lof111025.htm>
    date(2025, 11, 10): datetime(2025, 12, 10, 21, 15, tzinfo=timezone.utc),  # rounded up from 21:14:18
}


@dataclass(frozen=True)
class CFTCMarket:
    """One tracked CFTC market, identified only by its contract market code."""

    code: str       # cftc_contract_market_code, e.g. "13874A"
    root: str       # exchange root symbol, metadata only, e.g. "ES"
    label: str      # human-readable, metadata only


#: Tracked markets, keyed by ``cftc_contract_market_code``. Codes verified
#: against Socrata on 2026-09-26 (latest report 2026-09-22 for each).
#: EURODOLLAR (132741) is intentionally absent: the contract stopped
#: reporting on 2023-06-13.
MARKETS: dict[str, CFTCMarket] = {
    m.code: m
    for m in (
        CFTCMarket("13874A", "ES", "E-mini S&P 500 (CME)"),
        CFTCMarket("209742", "NQ", "E-mini NASDAQ-100 (CME)"),
        CFTCMarket("124603", "YM", "DJIA x $5 (CBOT)"),
        CFTCMarket("1170E1", "VX", "VIX futures (CFE)"),
        CFTCMarket("042601", "ZT", "2-Year UST Note (CBOT)"),
        CFTCMarket("044601", "ZF", "5-Year UST Note (CBOT)"),
        CFTCMarket("043602", "ZN", "10-Year UST Note (CBOT)"),
        CFTCMarket("020601", "ZB", "UST Bond (CBOT)"),
        CFTCMarket("088691", "GC", "Gold (COMEX)"),
        CFTCMarket("084691", "SI", "Silver (COMEX)"),
        CFTCMarket("085692", "HG", "Copper #1 (COMEX)"),
        CFTCMarket("067651", "CL", "WTI crude oil, physical (NYMEX)"),
        CFTCMarket("023651", "NG", "Henry Hub natural gas (NYMEX)"),
        CFTCMarket("002602", "ZC", "Corn (CBOT)"),
        CFTCMarket("005602", "ZS", "Soybeans (CBOT)"),
        CFTCMarket("001602", "ZW", "SRW wheat (CBOT)"),
    )
}

#: Root symbol -> market code, for consumers that think in roots.
CODE_BY_ROOT: dict[str, str] = {m.root: m.code for m in MARKETS.values()}

#: Legacy name-matched keys (``cftc.<KEY>.<metric>``). Frozen history that
#: mixes markets; never read these for analysis.
LEGACY_CONTRACT_KEYS: frozenset[str] = frozenset({
    "SP500", "NASDAQ", "DJIA", "USBOND", "NOTE10Y", "NOTE5Y", "NOTE2Y",
    "EURODOLLAR", "GOLD", "SILVER", "CRUDE_OIL", "NATGAS", "COPPER",
    "CORN", "SOYBEANS", "WHEAT", "VIX",
})

#: Raw Socrata fields per stored metric. All five are required per row.
RAW_FIELD_MAP: dict[str, str] = {
    "commercial_long": "comm_positions_long_all",
    "commercial_short": "comm_positions_short_all",
    "noncommercial_long": "noncomm_positions_long_all",
    "noncommercial_short": "noncomm_positions_short_all",
    "total_open_interest": "open_interest_all",
}

#: Every metric written per (market, report_date); net_speculative derived.
COT_METRICS: tuple[str, ...] = (*RAW_FIELD_MAP.keys(), "net_speculative")


def series_id(code: str, metric: str) -> str:
    """Build ``cftc.<market_code>.<metric>``.

    Raises ValueError for anything that is not a tracked market code, so a
    legacy key (``SP500``) or a root (``ES``) can never produce an id.
    """
    if code not in MARKETS:
        raise ValueError(f"not a tracked CFTC market code: {code!r}")
    if metric not in COT_METRICS:
        raise ValueError(f"not a CFTC COT metric: {metric!r}")
    return f"cftc.{code}.{metric}"


def series_id_for_root(root: str, metric: str) -> str:
    """``series_id`` looked up by root symbol (``ES`` -> ``cftc.13874A.<m>``)."""
    code = CODE_BY_ROOT.get(root)
    if code is None:
        raise ValueError(f"no tracked CFTC market for root {root!r}")
    return series_id(code, metric)


# ── Publication time ─────────────────────────────────────────────────────


@lru_cache(maxsize=64)
def _federal_holidays(year: int) -> frozenset[date]:
    from pandas.tseries.holiday import USFederalHolidayCalendar

    idx = USFederalHolidayCalendar().holidays(
        start=f"{year - 1}-12-01", end=f"{year + 1}-01-31",
    )
    return frozenset(d.date() for d in idx)


def is_federal_holiday(d: date) -> bool:
    """US federal holiday (observed), per pandas' USFederalHolidayCalendar."""
    return d in _federal_holidays(d.year)


def _next_federal_business_day(d: date) -> date:
    nxt = d + timedelta(days=1)
    while nxt.weekday() >= 5 or is_federal_holiday(nxt):
        nxt += timedelta(days=1)
    return nxt


@dataclass(frozen=True)
class ReleaseTime:
    """Scheduled publication of one weekly report (a floor, not observed)."""

    report_date: date
    release_at: datetime | None     # tz-aware UTC; None when the rule can't apply
    holiday_shifted: bool
    #: Always set when ``release_at`` is None (why the rule can't apply).
    #: Also set for a non-Tuesday ``release_at`` to say whether it is a
    #: confirmed value (from ``CONFIRMED_HOLIDAY_RELEASES``) or the
    #: conservative fallback estimate; None for an ordinary Tuesday release.
    reason: str | None = None

    def to_payload(self) -> dict[str, object]:
        return {
            "report_date": self.report_date.isoformat(),
            "release_at": self.release_at.isoformat() if self.release_at else None,
            "release_at_et": (
                self.release_at.astimezone(_ET).isoformat() if self.release_at else None
            ),
            "release_rule": RELEASE_RULE,
            "release_is_floor": True,
            "release_holiday_shifted": self.holiday_shifted,
            "release_reason": self.reason,
        }


#: Reasons carried on a computed (non-None) ``release_at`` for a non-Tuesday
#: report date -- see ``CONFIRMED_HOLIDAY_RELEASES`` and the module docstring.
RELEASE_REASON_CONFIRMED = "confirmed_holiday_shifted_release"
RELEASE_REASON_CONSERVATIVE_ESTIMATE = (
    "conservative_estimate: no confirmed release time on file for this date; using the next "
    "federal business day after the normal Friday release, which is never earlier than the "
    "true (possibly further-delayed) release"
)


def compute_release(report_date: date) -> ReleaseTime:
    """Scheduled release for positions measured on ``report_date``.

    Tuesday report date: Friday of that week at 15:30 ET; when a federal
    holiday falls Tuesday..Friday of that week, the next federal business
    day after that Friday at 15:30 ET.

    Monday or Wednesday report date (the CFTC's known holiday-shift
    pattern): ``CONFIRMED_HOLIDAY_RELEASES[report_date]`` if present,
    else 15:30 ET on the next federal business day after the Friday that
    would normally close that report's week -- a conservative floor, never
    earlier than the true release (see the module docstring).

    Any other report date returns ``release_at = None`` (no rule describes
    it -- this should not happen for real CFTC data).
    """
    if report_date.weekday() == 1:
        friday = report_date + timedelta(days=3)
        week = [report_date + timedelta(days=k) for k in range(4)]  # Tue..Fri
        shifted = any(is_federal_holiday(d) for d in week)
        release_day = _next_federal_business_day(friday) if shifted else friday
        local = datetime.combine(release_day, _RELEASE_TIME_ET, tzinfo=_ET)
        return ReleaseTime(
            report_date=report_date,
            release_at=local.astimezone(timezone.utc),
            holiday_shifted=shifted,
        )

    confirmed = CONFIRMED_HOLIDAY_RELEASES.get(report_date)
    if confirmed is not None:
        return ReleaseTime(
            report_date=report_date,
            release_at=confirmed,
            holiday_shifted=True,
            reason=RELEASE_REASON_CONFIRMED,
        )

    if report_date.weekday() in (0, 2):  # Monday or Wednesday
        normal_friday = report_date + timedelta(days=4 - report_date.weekday())
        release_day = _next_federal_business_day(normal_friday)
        local = datetime.combine(release_day, _RELEASE_TIME_ET, tzinfo=_ET)
        return ReleaseTime(
            report_date=report_date,
            release_at=local.astimezone(timezone.utc),
            holiday_shifted=True,
            reason=RELEASE_REASON_CONSERVATIVE_ESTIMATE,
        )

    return ReleaseTime(
        report_date=report_date,
        release_at=None,
        holiday_shifted=False,
        reason="report_date is not a Tuesday, Monday, or Wednesday; schedule rule does not apply",
    )
