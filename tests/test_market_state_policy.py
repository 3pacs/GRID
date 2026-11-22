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

def _strike(k: float, bid: float | None, ask: float | None, *, event: str = "KXFED-26JAN",
            title: str | None = None) -> dict:
    return {"ticker": f"{event}-T{k:.2f}", "yes_bid": bid, "yes_ask": ask, "yes_price": 0.42, "volume": 1.0,
            "title": title or f"Will the upper bound of the federal funds rate be above {k:.2f}% following the "
                              f"Fed's Jan 28, 2026 meeting?"}


def _hand_ladder() -> list[dict]:
    mids = {3.50: 0.995, 3.75: 0.995, 4.00: 0.925, 4.25: 0.025, 4.50: 0.005}
    return [_strike(k, round(m - 0.005, 3), round(m + 0.005, 3)) for k, m in mids.items()]


def test_expected_upper_bound_from_bid_ask_mids():
    lad, why = PP.price_ladder(_hand_ladder())
    assert why is None and lad.strikes == (3.50, 3.75, 4.00, 4.25, 4.50)
    # 3.50*.005 + 4.00*(.995-.925) + 4.25*(.925-.025) + 4.50*(.025-.005) + 4.75*.005
    assert lad.expected_upper == pytest.approx(4.23625)
    assert lad.tail_below == pytest.approx(0.005) and lad.tail_above == pytest.approx(0.005)
    assert lad.monotone_adjusted == 0 and lad.max_spread == pytest.approx(0.01)


def test_last_trade_price_is_never_used():
    a = _hand_ladder()
    b = [{**s, "yes_price": 0.99} for s in a]
    assert PP.price_ladder(a)[0] == PP.price_ladder(b)[0]


def test_a_crossed_quote_is_clipped_to_keep_the_ladder_non_increasing():
    rows = _hand_ladder()
    rows[3] = _strike(4.25, 0.94, 0.95)            # above the 4.00 strike's 0.925
    lad, why = PP.price_ladder(rows)
    assert why is None and lad.monotone_adjusted == 1 and lad.p_above[3] == pytest.approx(0.925)


@pytest.mark.parametrize("mutate, needle", [
    (lambda r: r.__setitem__(2, _strike(4.00, 0.80, 0.95)), "spread"),
    (lambda r: r.__setitem__(2, _strike(4.00, None, 0.93)), "two-sided"),
    (lambda r: r.__setitem__(2, _strike(4.00, 0.93, 0.92)), "two-sided"),
    (lambda r: r.pop(2), "contiguous"),
    (lambda r: r.__setitem__(0, _strike(3.50, 0.80, 0.81)), "does not cover"),
    (lambda r: r.__setitem__(1, _strike(3.75, 0.99, 1.0, title="Will CPI be above 3.75%?")), "upper bound above"),
    (lambda r: r.__setitem__(1, {**_strike(3.75, 0.99, 1.0), "ticker": "KXFED-26JAN-T3.80"}), "different strikes"),
    (lambda r: r[:] and r.__delitem__(slice(3, None)), "need 4"),
])
def test_an_unusable_ladder_is_refused_with_its_reason(mutate, needle):
    rows = _hand_ladder()
    mutate(rows)
    lad, why = PP.price_ladder(rows)
    assert lad is None and needle in why


def test_hike_hold_cut_odds_against_the_current_upper_bound():
    lad, _ = PP.price_ladder(_hand_ladder())
    assert PP.outcome_odds(lad, 4.00) == {"hike": 0.925, "hold": 0.07, "cut": 0.005}
    assert PP.outcome_odds(lad, 4.25) == {"hike": 0.025, "hold": 0.9, "cut": 0.075}


# ── meetings, rolls, same-contract change ─────────────────────────────────

def _payload(event: str, meeting: str, level: float) -> dict:
    return {"event_ticker": event, "strike_date": meeting, "series_ticker": "KXFED",
            "all_strikes": _ladder_for(level, event)}


def _ladder_for(level: float, event: str = "KXFED-26JAN") -> list[dict]:
    out = []
    for i in range(7):
        k = 3.50 + 0.25 * i
        p = 1.0 / (1.0 + math.exp(-(level - k - 0.125) / 0.04))
        mid = min(0.995, max(0.005, round(p, 3)))
        out.append(_strike(k, round(mid - 0.005, 4), round(mid + 0.005, 4), event=event))
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


def test_the_five_session_change_never_spans_two_contracts():
    snaps = [_snap("KXFED-25DEC", DEC_M, d, 5, 4.20) for d in SESS]
    snaps += [_snap("KXFED-26JAN", JAN_M, d, 5, 4.40 + 0.01 * i) for i, d in enumerate(SESS)]
    level, change, readings = PP.path_series(snaps, SESS)
    d = date(2025, 12, 11)                         # first January day; five sessions earlier is 12-04
    assert readings[d].event == "KALSHI:KXFED-26JAN"
    jan = {s.obs_date: s.ladder.expected_upper for s in snaps if s.event.endswith("26JAN")}
    assert change[d] == pytest.approx(jan[d] - jan[date(2025, 12, 4)])
    assert level[d] - level[date(2025, 12, 10)] > 0.15   # the level jumps at the roll; the change does not see it
    assert abs(change[d]) < 0.10


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


def test_policy_variables_are_standardized_and_the_put_reads_absent(engine):
    _seed(engine, scenario="absent")
    st = _record(engine)
    v = st["variables"]
    for k in ("ust_2y_change_5d", "fed_path_next_meeting", "fed_path_change_5d", "fed_priced_move_next_meeting"):
        assert v[k]["status"] == "ok", (k, v[k])
    assert v["fed_path_next_meeting"]["obs_date"] == AS_OF.isoformat()          # pulled 05:00Z that morning
    assert v["ust_2y_change_5d"]["obs_date"] == "2025-11-13"                    # next-business-day lag
    pr = st["policy_response"]
    assert pr["status"] == "ok" and pr["label"] == "put_absent" and pr["decision_use"] == "none"
    assert pr["controls"]["vix_only_z"] >= M.POLICY_STRESS_Z and pr["controls"]["front_end_easing_z"] <= 0
    nm = pr["next_meeting"]
    assert nm["event"] == "KALSHI:KXFED-26JAN" and nm["target"]["upper"] == 4.00
    assert nm["priced_move"] == pytest.approx(nm["expected_upper"] - 4.00)
    assert set(nm["odds"]) == {"hike", "hold", "cut"} and sum(nm["odds"].values()) == pytest.approx(1.0, abs=1e-3)
    assert st["source_lineage"]["kxfed"]["rows"] > 250 and st["source_lineage"]["kxfed"]["sha256"]


def test_a_front_end_that_eases_into_the_stress_reads_put_priced(engine):
    _seed(engine, scenario="priced")
    pr = _record(engine)["policy_response"]
    assert pr["status"] == "ok" and pr["label"] == "put_priced"
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
    assert st["variables"]["fed_path_next_meeting"]["obs_date"] == "2025-11-13"
    assert st["source_lineage"]["kxfed"]["cutoff"] == cutoff.isoformat()
    assert st["source_lineage"]["kxfed"]["cutoff_inclusive"] is True
    assert "kxfed" in M.CUTOFF_READS


def test_policy_inputs_have_lags_and_max_ages():
    assert M.LAGS["DGS2"] is not None and M.LAGS["DFEDTARU"] is not None
    for k in ("ust_2y_change_5d", "fed_path_next_meeting", "fed_path_change_5d", "fed_priced_move_next_meeting"):
        assert k in M.MAX_AGE and k in M.VAR_NAMES
    assert M.RECORD_RULE == "market-state/3"
