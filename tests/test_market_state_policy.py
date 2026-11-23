"""market-state/3 policy path: the Kalshi KXFED ladder, its same-contract change, and the policy_response block.

Unit tests price hand-made ladders (intelligence/regime/policy_path.py). The record tests seed raw_series on in-memory
SQLite the way griddb holds it: FRED and CBOE history backfilled by one later pull (modeled next-business-day lag),
KXFED rows pulled live each morning at 05:00 UTC (no modeled path).
"""

from __future__ import annotations

import json
import math
import sqlite3
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import Column, Date, DateTime, Float, ForeignKey, Integer, MetaData, String, Table, Text, create_engine, text

from intelligence.regime import market_state as M
from intelligence.regime import policy_path as PP

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

FRED, CBOE, KALSHI = 1, 2, 3
BACKFILL = datetime(2026, 3, 24, 6, 0, 0)
AS_OF = date(2025, 11, 14)
FIRST = date(2024, 9, 3)
JAN = "2026-01-28T19:00:00Z"
UTC = timezone.utc


# ── ladders ───────────────────────────────────────────────────────────────

def _title_date(meeting_iso: str) -> str:
    m = datetime.fromisoformat(meeting_iso.replace("Z", "+00:00")).astimezone(ZoneInfo("America/New_York"))
    return f"{m:%b} {m.day}, {m.year}"


def _strike(k: float, bid: float | None, ask: float | None, *, event: str = "KXFED-26JAN", meeting: str = JAN,
            title: str | None = None, **extra) -> dict:
    return {"ticker": f"{event}-T{k:.2f}", "yes_bid": bid, "yes_ask": ask, "yes_price": 0.42, "volume": 1.0,
            "title": title or f"Will the upper bound of the federal funds rate be above {k:.2f}% following the "
                              f"Fed's {_title_date(meeting)} meeting?", **extra}


def _hand_ladder() -> list[dict]:
    mids = {3.50: 0.995, 3.75: 0.995, 4.00: 0.925, 4.25: 0.025, 4.50: 0.005}
    return [_strike(k, round(m - 0.005, 3), round(m + 0.005, 3)) for k, m in mids.items()]


def _price(rows):
    return PP.price_ladder(rows, event="KALSHI:KXFED-26JAN", meeting_at=datetime(2026, 1, 28, 19, tzinfo=UTC))


def test_expected_upper_bound_from_quote_bands():
    lad, why = _price(_hand_ladder())
    assert why is None and lad.strikes == (3.50, 3.75, 4.00, 4.25, 4.50)
    # 3.50*.005 + 4.00*(.995-.925) + 4.25*(.925-.025) + 4.50*(.025-.005) + 4.75*.005 = 3.50 + .25 * sum(P)
    assert lad.expected_upper == pytest.approx(4.23625)
    assert lad.tail_below == pytest.approx(0.005) and lad.tail_above == pytest.approx(0.005)
    assert lad.monotone_adjusted == 0 and lad.max_spread == pytest.approx(0.01)
    # 3.50 and 3.75 (0.99/1.00: no ask) and 4.50 (0.00/0.01: no bid) are one-sided bands with the same points
    assert lad.one_sided == 3
    lo, hi = lad.expected_bounds
    assert lo == pytest.approx(3.50 + 0.25 * (0.99 + 0.99 + 0.92 + 0.02 + 0.0))
    assert hi == pytest.approx(3.50 + 0.25 * (1.0 + 1.0 + 0.93 + 0.03 + 0.01))
    assert lo < lad.expected_upper < hi


def test_the_live_december_ladder_shape_prices_from_one_sided_bands():
    # KXFED-26DEC as pulled 2026-11-23 05:30Z (public market data): five strikes 0.99/1.00, five 0.00/0.01.
    quotes = {2.75: (0.99, 1.0), 3.00: (0.99, 1.0), 3.25: (0.99, 1.0), 3.50: (0.99, 1.0), 3.75: (0.99, 1.0),
              4.00: (0.93, 0.94), 4.25: (0.03, 0.04), 4.50: (0.0, 0.01), 4.75: (0.0, 0.01), 5.00: (0.0, 0.01),
              5.25: (0.0, 0.01)}
    dec = "2026-12-09T19:00:00Z"
    rows = [_strike(k, b, a, event="KXFED-26DEC", meeting=dec) for k, (b, a) in quotes.items()]
    lad, why = PP.price_ladder(rows, event="KALSHI:KXFED-26DEC", meeting_at=datetime(2026, 12, 9, 19, tzinfo=UTC))
    assert why is None and lad.one_sided == 9
    assert lad.expected_upper == pytest.approx(2.75 + 0.25 * (5 * 0.995 + 0.935 + 0.035 + 4 * 0.005))
    assert lad.expected_bounds == pytest.approx((4.2275, 4.255))
    odds, bounds, withheld = PP.outcome_odds(lad, 4.00)
    assert withheld is None and odds == {"hike": 0.935, "hold": 0.06, "cut": 0.005}
    assert bounds["hike"] == [0.93, 0.94] and bounds["cut"] == [0.0, 0.01]


def test_last_trade_price_is_never_used():
    a = _hand_ladder()
    b = [{**s, "yes_price": 0.99} for s in a]
    assert _price(a)[0] == _price(b)[0]


def test_a_book_no_non_increasing_ladder_fits_is_refused_not_clipped():
    rows = _hand_ladder()
    rows[3] = _strike(4.25, 0.94, 0.95)            # its bid is above the 4.00 strike's 0.93 ask
    lad, why = _price(rows)
    assert lad is None and "no non-increasing ladder fits" in why and "4.25" in why


def test_crossing_mids_are_held_inside_every_quote_band():
    rows = _hand_ladder()
    rows[2] = _strike(4.00, 0.90, 0.95)            # mid 0.925
    rows[3] = _strike(4.25, 0.91, 0.99)            # mid 0.95 crosses it, but 0.91..0.95 fits both
    rows[4] = _strike(4.50, 0.0, 0.01)
    rows[1] = _strike(3.75, 0.99, 1.0)
    rows[0] = _strike(3.50, 0.99, 1.0)
    rows.append(_strike(4.75, None, 0.01))
    lad, why = _price(rows)
    assert why is None and lad.monotone_adjusted == 1
    assert all(lo - 1e-12 <= p <= hi + 1e-12 for p, lo, hi in zip(lad.p_above, lad.lower, lad.upper))
    assert all(a >= b for a, b in zip(lad.p_above, lad.p_above[1:]))
    assert lad.p_above[3] == pytest.approx(0.925) and 0.91 <= lad.p_above[3] <= 0.99


def _set(i, row):
    return lambda r: r.__setitem__(i, row)


@pytest.mark.parametrize("mutate, needle", [
    (_set(2, _strike(4.00, 0.80, 0.95)), "wider"),
    (_set(2, _strike(4.00, None, 0.93)), "wider"),                       # no bid: the band is 0..0.93
    (_set(2, _strike(4.00, 0.93, 0.92)), "crossed"),
    (_set(2, _strike(4.00, 0.0, 0.0)), "either side"),                   # the old writer's missing-quote zeros
    (_set(2, _strike(4.00, None, None)), "either side"),
    (_set(2, _strike(4.00, 0.92, 0.93, yes_bid_size=0, yes_ask_size=0)), "either side"),
    (lambda r: r.pop(2), "contiguous"),
    (lambda r: r.__setitem__(slice(0, 2), [_strike(4.75, None, 0.01)]), "does not cover"),
    (_set(1, _strike(3.75, 0.99, 1.0, title="Will CPI be above 3.75%?")), "upper bound above"),
    (_set(1, {**_strike(3.75, 0.99, 1.0), "ticker": "KXFED-26JAN-T3.80"}), "different strikes"),
    (_set(1, _strike(3.75, 0.99, 1.0, event="KXFED-26MAR")), "not a contract of KXFED-26JAN"),
    (_set(1, _strike(3.75, 0.99, 1.0, event_ticker="KXFED-26MAR")), "names event"),
    (_set(1, _strike(3.75, 0.99, 1.0, meeting="2026-03-18T18:00:00Z")), "names the 2026-03-18 meeting"),
    (_set(1, _strike(3.75, 0.99, 1.0, strike_type="less")), "pays on 'less'"),
    (_set(1, _strike(3.75, 0.99, 1.0, floor_strike=3.5)), "floor_strike"),
    (_set(1, _strike(3.75, 0.99, 1.0, cap_strike=4.0)), "cap strike"),
    (lambda r: r[:] and r.__delitem__(slice(3, None)), "need 4"),
])
def test_an_unusable_ladder_is_refused_with_its_reason(mutate, needle):
    rows = _hand_ladder()
    mutate(rows)
    lad, why = _price(rows)
    assert lad is None and needle in why


def test_an_off_grid_strike_is_refused():
    rows = _hand_ladder() + [_strike(4.60, None, 0.01)]
    assert "off the 0.25 grid" in _price(rows)[1]


def test_hike_hold_cut_odds_against_the_current_upper_bound():
    lad, _ = _price(_hand_ladder())
    assert PP.outcome_odds(lad, 4.00)[0] == {"hike": 0.925, "hold": 0.07, "cut": 0.005}
    assert PP.outcome_odds(lad, 4.25)[0] == {"hike": 0.025, "hold": 0.9, "cut": 0.075}


@pytest.mark.parametrize("target", [3.50, 3.25, 4.75, 3.88])
def test_odds_are_withheld_unless_both_thresholds_are_quoted(target):
    # 3.50: one step below (3.25) is not on the ladder, whose lowest strike carries a 0.03 tail; the old code printed
    # an exact 100% hike / 0% cut there. 4.75 is past the top strike; 3.88 is off the grid.
    rows = [_strike(k, b, a) for k, (b, a) in {3.50: (0.96, 0.98), 3.75: (0.96, 0.97), 4.00: (0.5, 0.52),
                                                4.25: (0.0, 0.01)}.items()]
    lad, why = _price(rows)
    assert why is None
    odds, bounds, withheld = PP.outcome_odds(lad, target)
    assert odds is None and bounds is None and withheld


# ── clocks ────────────────────────────────────────────────────────────────

def _row_snap(event, meeting, d, pulled, level, *, acquired=None, requested=None, clock=None):
    payload = _payload(event, meeting, level)
    if acquired is not None:
        payload["clock"] = {"rule": PP.ACQUIRED_CLOCK_RULE, "acquired_at": acquired.isoformat(),
                            "requested_at": (requested or acquired - timedelta(seconds=5)).isoformat()}
    if clock is not None:
        payload["clock"] = clock
    return PP.snapshot_from_row(f"KALSHI:{event}", d, pulled, json.dumps(payload))


def test_a_transaction_start_row_is_known_only_after_the_legacy_span():
    pulled = datetime(2025, 11, 14, 23, 50, tzinfo=UTC)
    end_of_day = datetime(2025, 11, 15, tzinfo=UTC)
    legacy = _row_snap("KXFED-26JAN", JAN, date(2025, 11, 14), pulled, 4.3)
    assert legacy.clock_basis == "transaction_start" and legacy.acquired_by == pulled + PP.LEGACY_SPAN
    assert not PP.known_by(legacy, end_of_day, inclusive=False)          # quotes may have arrived after midnight
    stamped = _row_snap("KXFED-26JAN", JAN, date(2025, 11, 14), pulled, 4.3, acquired=pulled)
    assert stamped.clock_basis == "acquired" and stamped.available_at == pulled + PP.COMMIT_GRACE
    assert stamped.ladder is not None and PP.known_by(stamped, end_of_day, inclusive=False)
    later = pulled + timedelta(minutes=20)
    late = _row_snap("KXFED-26JAN", JAN, date(2025, 11, 14), later, 4.3, acquired=later)
    assert not PP.known_by(late, end_of_day, inclusive=False)            # response after the cutoff


_T = "2025-11-14T23:50:00Z"


@pytest.mark.parametrize("clock, why", [
    ({"rule": PP.ACQUIRED_CLOCK_RULE, "acquired_at": "not a time", "requested_at": "2025-11-14T23:49:55Z"},
     "acquired_at is missing or unreadable"),
    ({"rule": PP.ACQUIRED_CLOCK_RULE, "requested_at": "2025-11-14T23:49:55Z"}, "acquired_at is missing or unreadable"),
    ({"rule": PP.ACQUIRED_CLOCK_RULE, "acquired_at": _T}, "requested_at is missing or unreadable"),
    ({"rule": PP.ACQUIRED_CLOCK_RULE, "acquired_at": _T, "requested_at": 17}, "requested_at is missing or unreadable"),
    ({"rule": PP.ACQUIRED_CLOCK_RULE, "acquired_at": _T, "requested_at": "2025-11-14T23:51:00Z"},
     "requested_at is after clock.acquired_at"),
    ({"rule": PP.ACQUIRED_CLOCK_RULE, "acquired_at": "2025-11-14T23:40:00Z", "requested_at": "2025-11-14T23:39:55Z"},
     "pull_timestamp is not clock.acquired_at"),         # stored clock and payload clock disagree
    ({"rule": "kalshi-acquired/0", "acquired_at": _T, "requested_at": _T}, "unrecognised clock"),
    (_T, "unrecognised clock"),
])
def test_an_explicit_clock_that_cannot_be_trusted_makes_the_row_unusable(clock, why):
    pulled = datetime(2025, 11, 14, 23, 50, tzinfo=UTC)
    s = _row_snap("KXFED-26JAN", JAN, date(2025, 11, 14), pulled, 4.3, clock=clock)
    assert s.ladder is None and why in s.reason and s.clock_basis == "clock_unreadable"
    assert s.available_at == pulled + PP.LEGACY_SPAN          # never earlier than an older row would be


def test_only_an_absent_clock_key_is_an_older_row_an_explicit_null_is_untrusted():
    # Matched controls (Codex review 6023958626): the same valid ladder, the clock key absent versus null.
    pulled = datetime(2025, 11, 14, 5, tzinfo=UTC)
    payload = _payload("KXFED-26JAN", JAN, 4.3)
    absent = PP.snapshot_from_row("KALSHI:KXFED-26JAN", date(2025, 11, 14), pulled, json.dumps(payload))
    null = PP.snapshot_from_row("KALSHI:KXFED-26JAN", date(2025, 11, 14), pulled,
                                json.dumps({**payload, "clock": None}))
    assert absent.clock_basis == "transaction_start" and absent.ladder is not None and absent.reason is None
    assert null.clock_basis == "clock_unreadable" and null.ladder is None and null.reason == "clock: clock is null"
    assert null.available_at == absent.available_at == pulled + PP.LEGACY_SPAN


def test_the_writer_and_reader_share_one_clock_rule():
    from ingestion.altdata import kalshi_markets as K

    assert K.CLOCK_RULE == PP.ACQUIRED_CLOCK_RULE
    assert timedelta(0) < K.COMMIT_FENCE < PP.COMMIT_GRACE           # the commit keeps part of the grace


# ── meetings, rolls, same-contract change ─────────────────────────────────

def _payload(event: str, meeting: str, level: float) -> dict:
    return {"event_ticker": event, "strike_date": meeting, "series_ticker": "KXFED",
            "all_strikes": _ladder_for(level, event, meeting)}


def _ladder_for(level: float, event: str = "KXFED-26JAN", meeting: str = JAN) -> list[dict]:
    out = []
    for i in range(7):
        k = 3.50 + 0.25 * i
        p = 1.0 / (1.0 + math.exp(-(level - k - 0.125) / 0.04))
        mid = min(0.995, max(0.005, round(p, 3)))
        out.append(_strike(k, round(mid - 0.005, 4), round(mid + 0.005, 4), event=event, meeting=meeting))
    return out


def _snap(event: str, meeting: str, d: date, hour: int, level: float) -> PP.Snapshot:
    return PP.snapshot_from_row(f"KALSHI:{event}", d, datetime(d.year, d.month, d.day, hour, tzinfo=UTC),
                                json.dumps(_payload(event, meeting, level)))


DEC_M, JAN_M = "2025-12-10T19:00:00Z", "2026-01-28T19:00:00Z"
SESS = [date(2025, 12, 1) + timedelta(days=i) for i in range(14) if (date(2025, 12, 1) + timedelta(days=i)).weekday() < 5]


def test_the_next_meeting_rolls_after_the_decision_and_is_never_substituted():
    snaps = []
    for d in SESS:
        snaps.append(_snap("KXFED-26JAN", JAN_M, d, 5, 4.40))
        if d != date(2025, 12, 4):                 # the December row is missing that day
            snaps.append(_snap("KXFED-25DEC", DEC_M, d, 5, 4.20))
    readings, missing = PP.next_meeting_readings(snaps, SESS)
    assert readings[date(2025, 12, 10)].event == "KALSHI:KXFED-25DEC"        # pulled 05:00Z, decision 19:00Z
    assert readings[date(2025, 12, 11)].event == "KALSHI:KXFED-26JAN"
    assert date(2025, 12, 4) not in readings and "KXFED-25DEC" in missing[date(2025, 12, 4)]


def test_a_later_discovery_never_rewrites_an_earlier_day():
    # Codex's case: GRID first reads the nearer December event on Monday. Friday's reading must be what Friday knew.
    fri, mon = date(2025, 12, 5), date(2025, 12, 8)
    days = [d for d in SESS if d <= mon]
    jan_only = [_snap("KXFED-26JAN", JAN_M, d, 5, 4.40) for d in days]
    before, _ = PP.next_meeting_readings([s for s in jan_only if s.obs_date <= fri], days)
    after, _ = PP.next_meeting_readings(jan_only + [_snap("KXFED-25DEC", DEC_M, mon, 5, 4.20)], days)
    assert after[fri] == before[fri] and after[fri].event == "KALSHI:KXFED-26JAN"
    assert after[mon].event == "KALSHI:KXFED-25DEC"
    assert PP.meetings_seen(jan_only, on=fri) == [datetime(2026, 1, 28, 19, tzinfo=UTC)]


def _at(d: date, h: int, m: int, sec: int) -> datetime:
    return datetime(d.year, d.month, d.day, h, m, sec, tzinfo=UTC)


def test_a_row_dated_the_next_day_never_changes_the_day_before():
    # Codex's synthetic (review 6017948338): two pulls overlap across midnight. The nearer event's request starts
    # 23:59:00Z on D, the farther event is read 23:59:20Z on D, and the nearer response lands 00:00:05Z on D + 1.
    d, d1 = date(2025, 12, 4), date(2025, 12, 5)
    far = _row_snap("KXFED-26JAN", JAN_M, d, _at(d, 23, 59, 20), 4.40, acquired=_at(d, 23, 59, 20),
                    requested=_at(d, 23, 59, 10))
    near = _row_snap("KXFED-25DEC", DEC_M, d1, _at(d1, 0, 0, 5), 4.20, acquired=_at(d1, 0, 0, 5),
                     requested=_at(d, 23, 59, 0))
    assert near.acquired_from < far.acquired_by                   # its request began before the farther row's read
    before, _ = PP.next_meeting_readings([far], [d, d1])
    after, _ = PP.next_meeting_readings([far, near], [d, d1])
    assert after[d] == before[d] and after[d].event == "KALSHI:KXFED-26JAN"
    assert after[d1].event == "KALSHI:KXFED-25DEC"
    assert PP.meetings_seen([far, near], on=d) == [datetime(2026, 1, 28, 19, tzinfo=UTC)]
    # A later row for a third, nearer-still event dated D + 1 never reaches D either.
    nov = _row_snap("KXFED-25NOV", "2025-12-06T19:00:00Z", d1, _at(d1, 0, 0, 6), 4.0, acquired=_at(d1, 0, 0, 6),
                    requested=_at(d, 23, 58, 0))
    again, _ = PP.next_meeting_readings([far, near, nov], [d, d1])
    assert again[d] == before[d]


def test_an_older_row_begun_before_midnight_counts_for_its_own_day_once_available():
    # Legacy control: a pull began 23:59:00Z on D in one transaction, so its row is dated D and its quotes may have
    # been read up to 15 min later. At D's end it is not available, and neither is what it teaches about the
    # schedule; once available it is D's own row.
    d, d1 = date(2025, 12, 4), date(2025, 12, 5)
    far = _row_snap("KXFED-26JAN", JAN_M, d, _at(d, 5, 0, 0), 4.40)
    near = _row_snap("KXFED-25DEC", DEC_M, d, _at(d, 23, 59, 0), 4.20)
    end_of_d = datetime(2025, 12, 5, tzinfo=UTC)
    cut = [s for s in (far, near) if PP.known_by(s, end_of_d, inclusive=False)]
    at_end, _ = PP.next_meeting_readings(cut, [d])
    assert cut == [far] and at_end[d].event == "KALSHI:KXFED-26JAN"
    later, _ = PP.next_meeting_readings([far, near], [d, d1])
    assert later[d].event == "KALSHI:KXFED-25DEC" and later[d].acquired_by == _at(d1, 0, 14, 0)


def test_overlapping_pulls_of_one_event_on_one_day_read_the_later_response_known_by_the_cutoff():
    d = date(2025, 12, 4)
    first = _row_snap("KXFED-25DEC", DEC_M, d, _at(d, 5, 0, 8), 4.20, acquired=_at(d, 5, 0, 8),
                      requested=_at(d, 5, 0, 1))
    second = _row_snap("KXFED-25DEC", DEC_M, d, _at(d, 5, 0, 40), 4.25, acquired=_at(d, 5, 0, 40),
                       requested=_at(d, 5, 0, 0))
    both, _ = PP.next_meeting_readings([second, first], [d])
    assert both[d].acquired_by == second.acquired_by
    cutoff = _at(d, 5, 1, 30)                                     # first available 05:01:08, second 05:01:40
    cut = [s for s in (first, second) if PP.known_by(s, cutoff, inclusive=True)]
    early, _ = PP.next_meeting_readings(cut, [d])
    assert early[d].acquired_by == first.acquired_by


def test_an_untrusted_clock_still_teaches_the_schedule_so_it_refuses_rather_than_substitutes():
    d = date(2025, 12, 4)
    far = _row_snap("KXFED-26JAN", JAN_M, d, _at(d, 5, 0, 0), 4.40, acquired=_at(d, 5, 0, 0))
    bad = _row_snap("KXFED-25DEC", DEC_M, d, _at(d, 5, 0, 0), 4.20,
                    clock={"rule": PP.ACQUIRED_CLOCK_RULE, "acquired_at": "garbage"})
    readings, missing = PP.next_meeting_readings([far, bad], [d])
    assert d not in readings and "clock: clock.acquired_at" in missing[d]


def test_two_events_naming_one_meeting_give_no_reading():
    snaps = [_snap("KXFED-26JAN", JAN_M, d, 5, 4.40) for d in SESS]
    snaps.append(_snap("KXFED-26JANB", JAN_M, SESS[0], 5, 4.40))
    readings, missing = PP.next_meeting_readings(snaps, SESS)
    assert SESS[0] not in readings and "2 KXFED events" in missing[SESS[0]]


def test_the_five_session_change_never_spans_two_contracts():
    snaps = [_snap("KXFED-25DEC", DEC_M, d, 5, 4.20) for d in SESS]
    snaps += [_snap("KXFED-26JAN", JAN_M, d, 5, 4.40 + 0.01 * i) for i, d in enumerate(SESS)]
    path = PP.path_series(snaps, SESS)
    d = date(2025, 12, 11)                         # first January day; five sessions earlier is 12-04
    assert path.readings[d].event == "KALSHI:KXFED-26JAN"
    jan = {s.obs_date: s.ladder.expected_upper for s in snaps if s.event.endswith("26JAN")}
    assert path.change[d] == pytest.approx(jan[d] - jan[date(2025, 12, 4)])
    assert path.change_start[d].obs_date == date(2025, 12, 4) and path.change_start[d].event == path.readings[d].event
    assert path.level[d] - path.level[date(2025, 12, 10)] > 0.15   # the level jumps at the roll; the change does not
    assert abs(path.change[d]) < 0.10


def test_a_target_printed_before_the_decision_took_effect_is_not_paired():
    snaps = [_snap("KXFED-25DEC", DEC_M, d, 5, 4.20) for d in SESS]
    snaps += [_snap("KXFED-26JAN", JAN_M, d, 5, 4.30) for d in SESS]
    readings, _ = PP.next_meeting_readings(snaps, SESS)
    meetings = PP.meetings_seen(snaps)
    import pandas as pd
    old_only = pd.Series({date(2025, 12, 1) + timedelta(days=i): 4.00 for i in range(10)})      # ends 12-10
    assert PP.target_pairing(readings[date(2025, 12, 11)], old_only, meetings) is None
    with_new = pd.concat([old_only, pd.Series({date(2025, 12, 11): 4.25})])
    assert PP.target_pairing(readings[date(2025, 12, 11)], with_new, meetings) == (date(2025, 12, 11), 4.25)
    assert PP.target_pairing(readings[date(2025, 12, 9)], old_only, meetings) == (date(2025, 12, 9), 4.00)
    off_grid = pd.Series({date(2025, 12, 9): 3.88})
    assert PP.target_pairing(readings[date(2025, 12, 9)], off_grid, meetings) is None


def test_a_snapshot_is_pre_open_only_between_a_close_and_the_next_open():
    s = _snap("KXFED-26JAN", JAN_M, date(2025, 12, 2), 5, 4.4)             # 05:00Z..05:15Z, EST open 14:30Z
    r = PP._reading(s.obs_date, s)
    assert PP.pre_open(r, date(2025, 12, 1))
    s2 = _snap("KXFED-26JAN", JAN_M, date(2025, 12, 2), 15, 4.4)           # 15:00Z: the session is open
    assert not PP.pre_open(PP._reading(s2.obs_date, s2), date(2025, 12, 1))


def test_the_2y_change_spans_exactly_five_sessions():
    import pandas as pd

    days = _sessions(date(2025, 10, 1), date(2025, 11, 21))
    vals = {d: 4.0 + 0.01 * i for i, d in enumerate(days)}
    holiday = date(2025, 11, 11)                    # Veterans Day: NYSE open, no DGS2 print
    del vals[holiday]
    ch, start = M.session_change_parts(pd.Series(vals), 5)
    t = date(2025, 11, 14)                          # five sessions earlier is 11-07 (the old diff(5) read 11-06)
    assert start[t] == (date(2025, 11, 7), date(2025, 11, 7))
    assert ch[t] == pytest.approx(vals[t] - vals[date(2025, 11, 7)])
    t2 = date(2025, 11, 18)                         # its start session is the holiday: the 11-10 close is carried
    assert start[t2] == (holiday, date(2025, 11, 10))
    gap = pd.Series({d: v for d, v in vals.items() if not date(2025, 10, 20) <= d <= date(2025, 10, 22)})
    ch_gap, _ = M.session_change_parts(gap, 5)
    assert date(2025, 10, 29) not in ch_gap          # its start (10-22) would be three sessions stale


# ── the record ────────────────────────────────────────────────────────────

@pytest.fixture()
def engine():
    eng = create_engine("sqlite://")
    md = MetaData()
    Table("source_catalog", md, Column("id", Integer, primary_key=True), Column("name", String, nullable=False))
    Table("raw_series", md,
          Column("series_id", String, nullable=False), Column("source_id", Integer, ForeignKey("source_catalog.id")),
          Column("obs_date", Date, nullable=False), Column("pull_timestamp", DateTime, nullable=False),
          Column("value", Float, nullable=False), Column("raw_payload", Text), Column("pull_status", String, nullable=False))
    md.create_all(eng)
    with eng.begin() as c:
        c.execute(text("INSERT INTO source_catalog (id, name) VALUES (:i, :n)"),
                  [{"i": FRED, "n": "fred"}, {"i": CBOE, "n": "CBOE"}, {"i": KALSHI, "n": "Kalshi"}])
    return eng


def _ins(engine, rows):
    with engine.begin() as c:
        c.execute(text("INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, value, raw_payload, "
                       "pull_status) VALUES (:s, :src, :d, :ts, :v, :p, 'SUCCESS')"),
                  [{"p": "{}", **r} for r in rows])


def _sessions(a, b):
    _, _, days, _ = M.xnys_sessions()
    return [d for d in days if a <= d <= b]


def _seed(engine, *, scenario: str = "absent", dgs2: bool = True, flat_2y: bool = False):
    days = _sessions(FIRST, AS_OF)
    rows = []
    n = len(days)
    for i, d in enumerate(days):
        tail = i - (n - 6)                          # 0..5 over the last six sessions
        vix = 18 + 2 * math.sin(i / 11.0)
        y2 = 4.0 + 0.10 * math.sin(i / 13.0)
        path = 4.25 + 0.05 * math.sin(i / 9.0)
        if tail >= 0:
            vix = 40.0 if d == days[-2] else vix
            if scenario == "priced":
                y2 -= 0.09 * tail
                path -= 0.07 * tail
            else:                                   # no rescue: the front end firms into the stress
                y2 += 0.03 * tail
                path += 0.02 * tail
        if flat_2y:
            y2 = 4.0
        rows.append({"s": "CBOE:VIX", "src": CBOE, "d": d, "ts": BACKFILL, "v": vix})
        if dgs2:
            rows.append({"s": "DGS2", "src": FRED, "d": d, "ts": BACKFILL, "v": y2})
        payload = json.dumps(_payload("KXFED-26JAN", JAN, path))
        rows.append({"s": "KALSHI:KXFED-26JAN", "src": KALSHI, "d": d, "v": 0.5 + 0.4 * math.sin(i),
                     "ts": datetime(d.year, d.month, d.day, 5, 0), "p": payload})
    d = FIRST
    while d <= AS_OF:
        rows.append({"s": "DFEDTARU", "src": FRED, "d": d, "ts": BACKFILL, "v": 4.00})
        d += timedelta(days=1)
    _ins(engine, rows)


def _record(engine, **kw):
    return M.compute_market_state(engine, AS_OF, calendar={}, **kw).to_dict()


def test_policy_variables_are_standardized_and_stress_reads_without_easing(engine):
    _seed(engine, scenario="absent")
    st = _record(engine)
    v = st["variables"]
    for k in ("ust_2y_change_5d", "fed_path_next_meeting", "fed_path_change_5d", "fed_priced_move_next_meeting"):
        assert v[k]["status"] == "ok", (k, v[k])
    assert v["fed_path_next_meeting"]["obs_date"] == AS_OF.isoformat()          # pulled 05:00Z that morning
    assert v["ust_2y_change_5d"]["obs_date"] == "2025-11-13"                    # next-business-day lag
    pr = st["policy_response"]
    assert pr["status"] == "ok" and pr["label"] == "stress_front_end_not_eased" and pr["decision_use"] == "none"
    assert "does not identify a policy response" in pr["validation"]
    assert pr["controls"]["vix_only_z"] >= M.POLICY_STRESS_Z and pr["controls"]["front_end_easing_z"] <= 0
    w = pr["windows"]                               # every leg ends at the 11-13 close; both changes start at 11-06
    assert w["vix"]["end_close"] == w["ust_2y"]["end_close"] == w["fed_path"]["end_close"] == "2025-11-13"
    assert w["ust_2y"]["start_close"] == w["fed_path"]["start_close"] == "2025-11-06"
    assert w["fed_path"]["end_snapshot"]["pre_open"] and w["fed_path"]["end_snapshot"]["session"] == "2025-11-14"
    nm = pr["next_meeting"]
    assert nm["event"] == "KALSHI:KXFED-26JAN" and nm["target"]["upper"] == 4.00
    assert nm["obs_date"] == AS_OF.isoformat() and nm["snapshot"]["clock_basis"] == "transaction_start"
    assert nm["snapshot"]["availability"] == "proxy" and "not exact point-in-time evidence" in pr["validation"]
    assert nm["priced_move"] == pytest.approx(nm["expected_upper"] - 4.00)
    assert nm["expected_bounds"][0] <= nm["expected_upper"] <= nm["expected_bounds"][1]
    assert set(nm["odds"]) == {"hike", "hold", "cut"} and sum(nm["odds"].values()) == pytest.approx(1.0, abs=1e-3)
    assert set(nm["odds_bounds"]) == {"hike", "hold", "cut"}
    lineage = st["source_lineage"]["kxfed"]
    assert lineage["rows"] > 250 and lineage["sha256"] and lineage["clock_basis"] == {"transaction_start": lineage["rows"]}
    assert lineage["schema"] == "market-state-kxfed-selection/3" and lineage["pit_qualified"] is False
    assert lineage["availability"].startswith("proxy, not a persistence receipt")
    assert any(x.startswith("KXFED row availability") for x in st["source_lineage"]["unqualified_read_paths"])


def test_a_front_end_that_eases_into_the_stress_reads_eased(engine):
    _seed(engine, scenario="priced")
    pr = _record(engine)["policy_response"]
    assert pr["status"] == "ok" and pr["label"] == "stress_front_end_eased"
    assert pr["controls"]["ust_2y_easing_z"] > 0 and pr["controls"]["fed_path_easing_z"] > 0


def test_rows_pulled_after_as_of_and_the_value_column_never_change_the_record(engine):
    _seed(engine)
    before = _record(engine)
    late = datetime(AS_OF.year, AS_OF.month, AS_OF.day) + timedelta(days=1, hours=1)
    wild = json.dumps(_payload("KXFED-26JAN", JAN, 3.60))
    # KXFED has no modeled path: a row pulled an hour after as_of ends is simply not known. FRED history is a
    # modeled-lag backfill (earliest vintage wins), so its late re-pull is dated after the backfill, as in
    # tests/test_market_state.py.
    _ins(engine, [{"s": "KALSHI:KXFED-26JAN", "src": KALSHI, "d": AS_OF, "ts": late, "v": 0.01, "p": wild},
                  {"s": "KALSHI:KXFED-26JAN", "src": KALSHI, "d": AS_OF - timedelta(days=3), "ts": late, "v": 0.99,
                   "p": wild},
                  {"s": "DFEDTARU", "src": FRED, "d": AS_OF - timedelta(days=1), "ts": datetime(2026, 4, 1, 12),
                   "v": 4.50}])
    with engine.begin() as c:
        c.execute(text("UPDATE raw_series SET value = 0.123 WHERE series_id LIKE 'KALSHI:%' AND pull_timestamp < :t"),
                  {"t": late})
    after = _record(engine)
    assert json.dumps(before, sort_keys=True) == json.dumps(after, sort_keys=True)


def test_a_missing_leg_is_unknown_not_a_label(engine):
    _seed(engine, dgs2=False)
    pr = _record(engine)["policy_response"]
    assert pr["status"] == "unknown" and pr["label"] == "unknown" and "ust_2y_change_5d" in pr["reason"]


def test_a_leg_with_no_spread_is_unknown_not_easing(engine):
    _seed(engine, flat_2y=True)                    # every 5-session change is exactly zero: no robust z
    st = _record(engine)
    assert st["variables"]["ust_2y_change_5d"].get("robust_z") is None
    assert st["policy_response"]["status"] == "unknown"


def test_a_stale_kalshi_snapshot_makes_the_block_unknown(engine):
    _seed(engine)
    with engine.begin() as c:
        c.execute(text("DELETE FROM raw_series WHERE series_id LIKE 'KALSHI:%' AND obs_date >= :d"),
                  {"d": AS_OF - timedelta(days=4)})
    st = _record(engine)
    assert st["variables"]["fed_path_change_5d"]["status"] == "stale"
    assert st["policy_response"]["status"] == "unknown" and "fed_path_change_5d" in st["policy_response"]["reason"]


def test_the_provisional_cutoff_drops_a_kalshi_row_pulled_after_it(engine):
    _seed(engine)
    cutoff = datetime(AS_OF.year, AS_OF.month, AS_OF.day, 4, 0, tzinfo=UTC)    # before that morning's 05:00Z pull
    st = _record(engine, known_by=cutoff)
    v = st["variables"]["fed_path_next_meeting"]
    assert v["obs_date"] == "2025-11-13" and v["status"] == "stale"          # the day before's ladder is not reused
    assert st["policy_response"]["status"] == "unknown"
    # 05:00Z + the 15-minute legacy span: known at 05:15Z, not at 05:10Z
    early = _record(engine, known_by=datetime(AS_OF.year, AS_OF.month, AS_OF.day, 5, 10, tzinfo=UTC))
    late = _record(engine, known_by=datetime(AS_OF.year, AS_OF.month, AS_OF.day, 5, 15, tzinfo=UTC))
    assert early["variables"]["fed_path_next_meeting"]["obs_date"] == "2025-11-13"
    assert late["variables"]["fed_path_next_meeting"]["obs_date"] == AS_OF.isoformat()
    assert st["source_lineage"]["kxfed"]["cutoff"] == cutoff.isoformat()
    assert st["source_lineage"]["kxfed"]["cutoff_inclusive"] is True
    assert "kxfed" in M.CUTOFF_READS


def test_policy_inputs_have_lags_and_max_ages():
    assert M.LAGS["DGS2"] is not None and M.LAGS["DFEDTARU"] is not None
    for k in ("ust_2y_change_5d", "fed_path_next_meeting", "fed_path_change_5d", "fed_priced_move_next_meeting"):
        assert k in M.MAX_AGE and k in M.VAR_NAMES
    assert M.RECORD_RULE == "market-state/3"


def test_the_desk_brief_shows_the_block_as_research_only(engine):
    from analysis.desk_brief_sources import _record_lines

    _seed(engine)
    lines, _ = _record_lines(_record(engine))
    line = next(x for x in lines if x.startswith("policy response"))
    assert "research only, decides nothing" in line and "stress_front_end_not_eased" in line and "hike" in line
    assert "through the 2025-11-13 close" in line
    assert "Kalshi availability: proxy, not exact point-in-time evidence" in line      # beside the odds
    unknown, _ = _record_lines({"policy_response": {"rule": M.POLICY_RESPONSE_RULE, "status": "unknown",
                                                    "reason": "vix: stale"}})
    assert unknown[-1].endswith("unknown (vix: stale)")
    assert not any(x.startswith("policy response") for x in _record_lines({"variables": {}})[0])


def _rewrite_kxfed(engine, d: date, **change):
    """Replace the session's KXFED row: a new payload (``payload``) and/or pull clock (``ts``)."""
    with engine.begin() as c:
        row = c.execute(text("SELECT raw_payload, pull_timestamp FROM raw_series WHERE series_id LIKE 'KALSHI:%' "
                             "AND obs_date = :d"), {"d": d}).fetchone()
        c.execute(text("UPDATE raw_series SET raw_payload = :p, pull_timestamp = :ts WHERE series_id LIKE 'KALSHI:%' "
                       "AND obs_date = :d"),
                  {"d": d, "p": change.get("payload", row[0]), "ts": change.get("ts", row[1])})


def test_an_unusable_current_snapshot_is_never_replaced_by_an_older_ladder(engine):
    _seed(engine)
    bad = _payload("KXFED-26JAN", JAN, 4.3)
    bad["all_strikes"][3] = {**bad["all_strikes"][3], "yes_bid": 0.0, "yes_ask": 0.0}     # missing-quote zeros
    _rewrite_kxfed(engine, AS_OF, payload=json.dumps(bad))
    st = _record(engine)
    assert st["variables"]["fed_path_next_meeting"]["status"] == "stale"
    nm = st["policy_response"]["next_meeting"]
    assert nm["status"] == "unavailable" and "either side" in nm["reason"] and nm["newest_usable_session"] == "2025-11-13"
    pr = st["policy_response"]
    assert pr["status"] == "unknown" and "next_meeting" in pr["reason"]


def test_legs_ending_at_different_closes_are_unknown(engine):
    _seed(engine)
    with engine.begin() as c:                     # the 11-13 DGS2 print is missing: the 2y leg ends at 11-12
        c.execute(text("DELETE FROM raw_series WHERE series_id = 'DGS2' AND obs_date = :d"),
                  {"d": AS_OF - timedelta(days=1)})
    st = _record(engine)
    assert st["variables"]["ust_2y_change_5d"]["status"] == "ok"          # still fresh on its own
    pr = st["policy_response"]
    assert pr["status"] == "unknown" and "different closes" in pr["reason"]
    assert pr["windows"]["ust_2y"]["end_close"] == "2025-11-12"


def test_a_snapshot_read_after_the_open_has_no_close(engine):
    _seed(engine)
    _rewrite_kxfed(engine, AS_OF, ts=datetime(AS_OF.year, AS_OF.month, AS_OF.day, 15, 0))   # 15:00Z, open 14:30Z
    st = _record(engine)
    assert st["variables"]["fed_path_next_meeting"]["status"] == "ok"
    pr = st["policy_response"]
    assert pr["status"] == "unknown" and "between a close and the next open" in pr["reason"]
    assert pr["windows"]["fed_path"]["end_close"] is None
