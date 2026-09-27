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
``raw_series.pull_timestamp``. When the report date is not a Tuesday (the
CFTC moves the position date to Monday when Tuesday is a holiday), the rule
does not apply and ``release_at`` is ``None`` with a reason.
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

#: Rule identifier stored alongside every computed ``release_at``.
RELEASE_RULE: str = "cftc_schedule_rule_v1"


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
    reason: str | None = None       # set when release_at is None

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


def compute_release(report_date: date) -> ReleaseTime:
    """Scheduled release for positions measured on ``report_date``.

    Friday after the Tuesday report date at 15:30 ET; when a federal holiday
    falls Tuesday..Friday of that week, the next federal business day after
    that Friday at 15:30 ET. Non-Tuesday report dates return ``release_at =
    None`` (the weekly rule does not describe them).
    """
    if report_date.weekday() != 1:
        return ReleaseTime(
            report_date=report_date,
            release_at=None,
            holiday_shifted=False,
            reason="report_date is not a Tuesday; schedule rule does not apply",
        )
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
