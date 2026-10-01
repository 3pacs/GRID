"""GD5 people-density features (analysis/people_density.py): acceptance tests 1-12 and 14.

Synthetic events only. Nothing here reads a price, a return or a label; the
"planted label" in the leak self-test is a synthetic number built from the
synthetic events themselves. Test 13 (the people_events-backed PostgreSQL
look-ahead canary) is tests/test_people_density_lookahead.py.
"""

from __future__ import annotations

import ast
import dataclasses
import os
import subprocess
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from analysis import people_density as P
from store.people_events import PeopleEvent

REPO = Path(__file__).resolve().parent.parent
UTC = timezone.utc
DAY = pd.Timedelta(days=1)

E1 = P.entity_id_for_cik(1001)
E2 = P.entity_id_for_cik(1002)


def ev(**kw) -> dict:
    row = {
        "channel": "form4", "dedup_key": None, "event_time": None, "known_at": None,
        "known_at_basis": "filing", "actor_id": "0000000001", "actor_id_basis": "owner_cik",
        "actor_type": "insider", "entity_cik": "1001", "entity_ticker": None, "direction": "buy",
        "transaction_code": "P", "size_usd": 50_000.0, "plan_10b5_1": False, "echo_of": None,
    }
    row.update(kw)
    if row["event_time"] is None:
        row["event_time"] = row["known_at"] - 2 * DAY
    if row["dedup_key"] is None:
        row["dedup_key"] = f"{row['channel']}|{row['actor_id']}|{row['known_at'].value}|{row['entity_cik']}"
    return row


def frame(rows) -> pd.DataFrame:
    return P.events_frame(pd.DataFrame(rows))


def ts(s: str) -> pd.Timestamp:
    return pd.Timestamp(s, tz="UTC")


def one(t: pd.Timestamp) -> pd.DatetimeIndex:
    return pd.DatetimeIndex([t])


SPEC90 = P.DECLARED_SPECS["A_insider_buy_w90"]
SPEC30 = P.DECLARED_SPECS["A_insider_buy_w30"]


def a_at(rows, t, spec=SPEC90, entity=E1) -> float:
    return float(P.density_A(frame(rows), spec, [entity], one(t), coverage=None).iloc[0, 0])


# --- 1. known-at boundary ------------------------------------------------------------------


def test_window_is_open_at_t_minus_w_and_closed_at_t():
    t = ts("2024-06-14 20:00")
    w = pd.Timedelta(days=SPEC90.window_days)
    # Traded before t, public after t: never counts.
    assert a_at([ev(event_time=t - 10 * DAY, known_at=t + pd.Timedelta(seconds=1))], t) == 0.0
    assert a_at([ev(event_time=t - 10 * DAY, known_at=t + pd.Timedelta(microseconds=1))], t) == 0.0
    # known_at == t counts with full weight.
    assert a_at([ev(known_at=t)], t) == 1.0
    # known_at == t - W does not count; just inside does.
    assert a_at([ev(known_at=t - w)], t) == 0.0
    inside = a_at([ev(known_at=t - w + pd.Timedelta(seconds=1))], t)
    assert 0.0 < inside < np.exp(-1.99)


def test_event_time_never_enters_a_feature():
    t = ts("2024-06-14 20:00")
    base = [ev(known_at=t - 5 * DAY, event_time=t - 6 * DAY)]
    moved = [ev(known_at=t - 5 * DAY, event_time=t - 400 * DAY, dedup_key=base[0]["dedup_key"])]
    assert a_at(base, t) == a_at(moved, t)


# --- 2. append-future determinism -----------------------------------------------------------

CHANNEL_DIRS = {"form4": ("buy", "sell"), "congress": ("buy", "sell"), "thirteen_f": ("buy", "sell")}
CUT = ts("2021-06-25 20:00")


def random_events(rng, n, lo, hi, n_entities=15, actor_offset=0, determined=True) -> pd.DataFrame:
    span = int((hi - lo).total_seconds())
    known = lo + pd.to_timedelta(rng.integers(0, span, n), unit="s")
    channel = rng.choice(list(CHANNEL_DIRS), n)
    direction = np.array([rng.choice(CHANNEL_DIRS[c]) for c in channel])
    f4_code = np.where(direction == "buy", "P", "S")
    f13_code = np.where(direction == "buy", rng.choice(["NEW", "INC"], n), rng.choice(["DEC", "EXIT"], n))
    code = np.where(channel == "form4", f4_code, np.where(channel == "thirteen_f", f13_code, None))
    plan = rng.random(n) < 0.3
    rows = pd.DataFrame({
        "channel": channel, "dedup_key": [f"r{actor_offset}-{i}" for i in range(n)],
        "event_time": known - pd.to_timedelta(rng.integers(0, 30, n), unit="D"), "known_at": known,
        "known_at_basis": "filing", "actor_id": [f"{a:010d}" for a in rng.integers(0, 40, n) + actor_offset],
        "actor_id_basis": "owner_cik", "actor_type": "x",
        "entity_cik": [str(1000 + e) for e in rng.integers(0, n_entities, n)],
        "direction": direction, "transaction_code": code,
        "plan_10b5_1": [bool(p) if determined else None for p in plan],
    })
    return rows


def membership(n_entities=15, n_sectors=3) -> pd.DataFrame:
    return pd.DataFrame({
        "entity_id": [P.entity_id_for_cik(1000 + e) for e in range(n_entities)],
        "sector": [f"S{e % n_sectors}" for e in range(n_entities)],
        "valid_from": [date(2010, 1, 1)] * n_entities, "valid_to": [None] * n_entities,
    })


def all_features(events, decisions, members, coverage) -> dict[str, bytes]:
    entities = sorted(members["entity_id"])
    out = {}
    for name in ("A_multi_mc1_w90", "A_congress_w30", "A_inst_w90"):
        a = P.density_A(events, P.DECLARED_SPECS[name], entities, decisions, coverage=coverage)
        out[name] = a.to_numpy().tobytes()
        out[name + ":D_self"] = P.d_self(a).to_numpy().tobytes()
        out[name + ":D_peer"] = P.d_peer(a, members).to_numpy().tobytes()
    out["C"] = P.channel_count_C(events, P.DECLARED_SPECS["C_people_w90"], entities, decisions,
                                 coverage=coverage).to_numpy().tobytes()
    for name in ("S_insider_w90", "S_congress_w30"):
        out[name] = P.signed_S(events, P.DECLARED_SPECS[name], entities, decisions, coverage=coverage).to_numpy().tobytes()
    agg = P.sector_weekly_aggregates(events, [P.DECLARED_SPECS["A_multi_mc1_w90"], P.DECLARED_SPECS["S_insider_w30"]],
                                     members, decisions, coverage=coverage)
    out["aggregates"] = agg.to_json(orient="split", date_format="iso", date_unit="ns").encode()
    return out


COVERAGE = [P.CoverageSpan(c, ts("2015-01-01")) for c in P._ACTOR_CHANNELS]


@pytest.mark.parametrize("seed", range(6))
def test_appending_events_known_after_T_changes_nothing_at_or_before_T(seed):
    rng = np.random.default_rng(seed)
    base = random_events(rng, 1500, ts("2018-01-01"), CUT)
    # Future rows: new and old actors, new and old entities' events, and
    # Form 4 sells with no 10b5-1 determination (S must not see them).
    future = pd.concat([
        random_events(rng, 400, CUT + pd.Timedelta(microseconds=1), CUT + 400 * DAY, actor_offset=0),
        random_events(rng, 400, CUT + pd.Timedelta(seconds=1), CUT + 400 * DAY, actor_offset=7, determined=False),
    ], ignore_index=True)
    # Some events traded before T but public after T.
    future.loc[:50, "event_time"] = CUT - 20 * DAY
    decisions = P.weekly_decisions(date(2019, 1, 4), CUT.date())
    assert decisions[-1] == CUT
    members = membership()
    before = all_features(frame(base), decisions, members, COVERAGE)
    after = all_features(frame(pd.concat([base, future], ignore_index=True)), decisions, members, COVERAGE)
    assert before.keys() == after.keys()
    for key in before:
        assert before[key] == after[key], key


def test_features_at_t_do_not_depend_on_later_decisions():
    rng = np.random.default_rng(99)
    events = frame(random_events(rng, 2000, ts("2018-01-01"), ts("2023-01-01")))
    members = membership()
    entities = sorted(members["entity_id"])
    short = P.weekly_decisions(date(2019, 1, 4), CUT.date())
    long = P.weekly_decisions(date(2019, 1, 4), date(2022, 12, 30))
    for name in ("A_multi_mc1_w90", "A_inst_w30"):
        spec = P.DECLARED_SPECS[name]
        a_s = P.density_A(events, spec, entities, short, coverage=COVERAGE)
        a_l = P.density_A(events, spec, entities, long, coverage=COVERAGE)
        assert a_s.to_numpy().tobytes() == a_l.iloc[: len(short)].to_numpy().tobytes()
        assert P.d_self(a_s).to_numpy().tobytes() == P.d_self(a_l).iloc[: len(short)].to_numpy().tobytes()


# --- 3. planted-leak self-test --------------------------------------------------------------

LEAK_SEED = 20261001
LEAK_TRIPS = 0.9
LEAK_CLEAN = 0.1


def planted_leak_world(seed: int = LEAK_SEED):
    """Per (entity, decision): ``label`` insiders trade just before t but file after t.

    ``label`` is the planted future quantity. Independent background buys are
    filed inside the window. A reader keyed on event_time sees the planted
    trades; a reader keyed on known_at must not.
    """
    rng = np.random.default_rng(seed)
    decisions = pd.DatetimeIndex([ts("2016-03-04 21:00"), ts("2016-10-07 20:00"), ts("2017-05-05 20:00")])
    entities = [P.entity_id_for_cik(5000 + i) for i in range(600)]
    rows, labels = [], np.zeros((len(decisions), len(entities)))
    k = 0
    for i, t in enumerate(decisions):
        for j in range(len(entities)):
            n_planted = int(rng.integers(0, 6))
            labels[i, j] = n_planted
            for _ in range(n_planted):
                trade = t - pd.Timedelta(hours=float(rng.uniform(6, 72)))
                rows.append(ev(entity_cik=str(5000 + j), actor_id=f"{k:010d}", event_time=trade,
                               known_at=t + pd.Timedelta(days=float(rng.uniform(1, 30)))))
                k += 1
            for _ in range(int(rng.integers(0, 3))):
                known = t - pd.Timedelta(days=float(rng.uniform(0.1, 80)))
                rows.append(ev(entity_cik=str(5000 + j), actor_id=f"{k:010d}", known_at=known,
                               event_time=known - pd.Timedelta(days=float(rng.uniform(1, 60)))))
                k += 1
    return pd.DataFrame(rows), decisions, entities, labels


def _corr(x: np.ndarray, y: np.ndarray) -> float:
    return float(np.corrcoef(x.ravel(), y.ravel())[0, 1])


def test_planted_leak_trips_an_event_time_reader_and_not_the_real_one():
    rows, decisions, entities, labels = planted_leak_world()
    real = P.density_A(P.events_frame(rows), SPEC30, entities, decisions, coverage=None).to_numpy()
    leaky_rows = rows.assign(known_at=rows["event_time"])  # the deliberate leak: availability = trade time
    leaky = P.density_A(P.events_frame(leaky_rows), SPEC30, entities, decisions, coverage=None).to_numpy()
    assert _corr(leaky, labels) > LEAK_TRIPS
    assert abs(_corr(real, labels)) < LEAK_CLEAN


# --- 4. actor once / 5. echoes --------------------------------------------------------------


def _pe(**kw) -> PeopleEvent:
    base = dict(channel="form4", dedup_key="k", event_time=datetime(2024, 5, 1, tzinfo=UTC),
                known_at=datetime(2024, 5, 3, 2, tzinfo=UTC), known_at_basis="filing", actor_id="0000000007",
                actor_id_basis="owner_cik", actor_type="insider", source="sec", entity_cik="1001",
                direction="buy", transaction_code="P")
    base.update(kw)
    return PeopleEvent(**base)


def test_one_actor_filing_ten_times_counts_once_two_actors_count_two():
    t = ts("2024-06-14 20:00")
    tau = SPEC90.tau_days
    same = [ev(known_at=t - (i + 1) * DAY, dedup_key=f"d{i}") for i in range(10)]
    assert a_at(same, t) == pytest.approx(np.exp(-1 / tau))  # latest filing only
    two = [ev(known_at=t - DAY), ev(known_at=t - DAY, actor_id="0000000002")]
    assert a_at(two, t) == pytest.approx(2 * np.exp(-1 / tau))


def test_co_actor_ids_do_not_add_actors():
    t = pd.Timestamp("2024-06-14 20:00", tz="UTC")
    plain = P.events_frame([_pe()])
    with_co = P.events_frame([_pe(co_actor_ids=("0000000008", "0000000009"))])
    a1 = P.density_A(plain, SPEC90, [E1], one(t), coverage=None)
    a2 = P.density_A(with_co, SPEC90, [E1], one(t), coverage=None)
    assert a1.equals(a2) and a1.iloc[0, 0] > 0


def test_echo_rows_never_count():
    t = pd.Timestamp("2024-06-14 20:00", tz="UTC")
    echo = P.events_frame([_pe(dedup_key="e", echo_of=41)])
    assert echo.empty and echo.attrs["dropped"]["echo"] == 1
    both = P.events_frame([_pe(), _pe(dedup_key="e2", actor_id="0000000009", echo_of=41)])
    assert P.density_A(both, SPEC90, [E1], one(t), coverage=None).iloc[0, 0] == pytest.approx(
        np.exp(-(t - pd.Timestamp("2024-05-03 02:00", tz="UTC")) / pd.Timedelta(days=45)))


# --- 6. VS1 parity --------------------------------------------------------------------------


@pytest.mark.parametrize("seed", [0, 1, 2])
def test_insider_buy_density_equals_vs1_panel_density_exactly(seed):
    from analysis import panel_insider_density as v1

    rng = np.random.default_rng(seed)
    issuers = list(range(2000, 2040))
    n = 4000
    known = v1.filing_known_at(pd.Series(pd.to_datetime("2014-01-01") + pd.to_timedelta(rng.integers(0, 1500, n), unit="D")))
    purchases = pd.DataFrame({"issuer_cik": rng.choice(issuers, n), "actor": rng.integers(1, 80, n), "known_at": known})
    decisions = v1.decision_instants(pd.bdate_range("2014-02-03", "2018-01-31").date)
    events = P.events_frame(pd.DataFrame({
        "channel": "form4", "dedup_key": [f"k{i}" for i in range(n)], "event_time": known - 2 * DAY, "known_at": known,
        "known_at_basis": "filing", "actor_id": [f"{a:010d}" for a in purchases["actor"]], "actor_id_basis": "owner_cik",
        "actor_type": "insider", "entity_cik": purchases["issuer_cik"].astype(str), "transaction_code": "P",
        "direction": "buy", "plan_10b5_1": None,
    }))
    for feature, (window, tau) in v1.FEATURES.items():
        spec = P.DECLARED_SPECS[f"A_insider_buy_w{window}"]
        assert (spec.window_days, spec.tau_days) == (window, tau), feature
        ref = v1.density(purchases, issuers, decisions, window, tau)
        got = P.density_A(events, spec, [P.entity_id_for_cik(i) for i in issuers], decisions, coverage=None)
        assert np.array_equal(ref.to_numpy(), got.to_numpy()), feature
        assert (ref.to_numpy() > 0).mean() > 0.2  # the comparison is not vacuous


def test_insider_buy_spec_counts_code_p_only_and_drops_planned_buys():
    t = ts("2024-06-14 20:00")
    rows = [ev(known_at=t - DAY, actor_id=f"{i:010d}", transaction_code=code, direction=d, plan_10b5_1=plan)
            for i, (code, d, plan) in enumerate([("P", "buy", False), ("A", "award", False), ("M", None, False),
                                                 ("S", "sell", False), ("P", "buy", True), ("P", "buy", None)])]
    assert a_at(rows, t) == pytest.approx(2 * np.exp(-1 / 45))


# --- 7. D_self ------------------------------------------------------------------------------


def _weekly(values, start=date(2020, 1, 3), cols=("x",)) -> pd.DataFrame:
    idx = P.weekly_decisions(start, start + pd.Timedelta(weeks=len(values) - 1))
    return pd.DataFrame(np.asarray(values, float).reshape(len(values), -1), index=idx, columns=list(cols))


def test_d_self_is_nan_below_52_weeks_never_zero():
    out = P.d_self(_weekly(np.arange(51.0)))
    assert out.isna().all().all()
    out = P.d_self(_weekly(np.arange(60.0)))
    assert out.iloc[:51].isna().all().all() and out.iloc[51:].notna().all().all()


def test_d_self_value_mad_zero_and_trailing_only():
    vals = np.r_[np.zeros(60), 3.0]
    out = P.d_self(_weekly(vals))
    assert out.iloc[-1, 0] == 3.0 / (0.0 + P.D_SELF_EPSILON)
    rng = np.random.default_rng(3)
    vals = rng.random(80)
    out = P.d_self(_weekly(vals))
    win = vals[80 - 52:]
    med = np.median(win)
    assert out.iloc[-1, 0] == pytest.approx((vals[-1] - med) / (np.median(np.abs(win - med)) + P.D_SELF_EPSILON))
    later = vals.copy()
    later[70:] += 100.0
    assert np.array_equal(P.d_self(_weekly(later)).iloc[:70].to_numpy(), out.iloc[:70].to_numpy(), equal_nan=True)


def test_d_self_nan_inside_window_gives_nan_and_needs_weekly_index():
    vals = np.r_[np.ones(55), np.nan, np.ones(10)]
    out = P.d_self(_weekly(vals))
    assert out.iloc[55:].isna().all().all() and out.iloc[54].notna().all()
    bad = _weekly(np.ones(60)).drop(index=_weekly(np.ones(60)).index[10])
    with pytest.raises(ValueError, match="weekly"):
        P.d_self(bad)


# --- 8. D_peer ------------------------------------------------------------------------------


def test_d_peer_average_ranks_and_membership_as_of_t():
    idx = P.weekly_decisions(date(2024, 1, 5), date(2024, 1, 12))
    ents = [f"e{i}" for i in range(6)]
    a = pd.DataFrame([[0, 0, 1, 2, 3, 9], [0, 0, 1, 2, 3, 9]], index=idx, columns=ents, dtype=float)
    members = pd.DataFrame({
        "entity_id": ents, "sector": ["S"] * 6,
        "valid_from": [date(2020, 1, 1)] * 5 + [date(2024, 1, 10)], "valid_to": [None] * 6,
    })
    peer = P.d_peer(a, members)
    assert peer.iloc[0].tolist()[:5] == [1.5 / 5, 1.5 / 5, 3 / 5, 4 / 5, 5 / 5]
    assert np.isnan(peer.iloc[0, 5])  # joins the sector after the first decision
    assert peer.iloc[1].tolist() == [1.5 / 6, 1.5 / 6, 3 / 6, 4 / 6, 5 / 6, 6 / 6]


def test_d_peer_needs_min_peers_and_ignores_nan_and_rejects_two_sectors():
    idx = P.weekly_decisions(date(2024, 1, 5), date(2024, 1, 5))
    ents = [f"e{i}" for i in range(6)]
    a = pd.DataFrame([[0, 1, 2, 3, np.nan, 5]], index=idx, columns=ents, dtype=float)
    members = pd.DataFrame({"entity_id": ents, "sector": ["S"] * 6, "valid_from": [date(2020, 1, 1)] * 6,
                            "valid_to": [None] * 6})
    peer = P.d_peer(a, members)
    assert np.isnan(peer.iloc[0, 4]) and peer.iloc[0, 5] == 1.0
    assert P.d_peer(a, members, min_peers=6).isna().all().all()
    clash = pd.concat([members, members.iloc[[0]].assign(sector="T")], ignore_index=True)
    with pytest.raises(ValueError, match="two sectors"):
        P.d_peer(a, clash)


# --- 9. coverage guard ----------------------------------------------------------------------


def test_coverage_guard_needs_w_days_on_every_counted_channel():
    t = ts("2024-06-14 20:00")
    spec = P.DECLARED_SPECS["A_multi_mc1_w30"]
    rows = [ev(known_at=t - DAY), ev(channel="congress", actor_id="B000001", actor_id_basis="bioguide",
                                      transaction_code=None, known_at=t - DAY)]
    events = frame(rows)
    w = pd.Timedelta(days=30)
    ok = [P.CoverageSpan("form4", t - w), P.CoverageSpan("congress", t - w)]
    short = [P.CoverageSpan("form4", t - w), P.CoverageSpan("congress", t - w + DAY)]
    stopped = [P.CoverageSpan("form4", t - w), P.CoverageSpan("congress", t - 400 * DAY, stop=t - DAY)]
    other_entity = [P.CoverageSpan("form4", t - w), P.CoverageSpan("congress", t - w, entity_id=E2)]
    assert P.density_A(events, spec, [E1], one(t), coverage=ok).iloc[0, 0] > 0
    for cov in (short, stopped, other_entity, [P.CoverageSpan("form4", t - w)]):
        assert np.isnan(P.density_A(events, spec, [E1], one(t), coverage=cov).iloc[0, 0])
    log = P.coverage_change_log(stopped + other_entity)
    assert list(log["kind"]).count("start") == 4 and list(log["kind"]).count("stop") == 1
    assert log["at"].is_monotonic_increasing
    assert set(log["entity_id"]) == {"*", E2}


# --- 10. S refusal --------------------------------------------------------------------------


def test_signed_s_refuses_undetermined_form4_sells():
    t = ts("2024-06-14 20:00")
    spec = P.DECLARED_SPECS["S_insider_w90"]
    buy = ev(known_at=t - DAY)
    sell = ev(known_at=t - DAY, actor_id="0000000002", transaction_code="S", direction="sell", plan_10b5_1=None)
    with pytest.raises(P.UndefinedFeature):
        P.signed_S(frame([buy, sell]), spec, [E1], one(t), coverage=None)
    w = np.exp(-1 / 45)
    discretionary = dict(sell, plan_10b5_1=False)
    planned = dict(sell, plan_10b5_1=True)
    assert P.signed_S(frame([buy, discretionary]), spec, [E1], one(t), coverage=None).iloc[0, 0] == pytest.approx(0.0)
    assert P.signed_S(frame([buy, planned]), spec, [E1], one(t), coverage=None).iloc[0, 0] == pytest.approx(w)
    # A sell known only after the last decision does not make S undefined at t.
    late = dict(sell, known_at=t + DAY)
    assert P.signed_S(frame([buy, late]), spec, [E1], one(t), coverage=None).iloc[0, 0] == pytest.approx(w)


def test_signed_s_on_non_form4_channels_needs_no_determination():
    t = ts("2024-06-14 20:00")
    rows = [ev(channel="congress", actor_id="B1", actor_id_basis="bioguide", transaction_code=None, plan_10b5_1=None,
               known_at=t - DAY, direction=d, dedup_key=f"c{i}") for i, d in enumerate(["buy", "sell", "sell"])]
    rows[2]["actor_id"] = "B2"
    s = P.signed_S(frame(rows), P.DECLARED_SPECS["S_congress_w90"], [E1], one(t), coverage=None).iloc[0, 0]
    assert s == pytest.approx(-np.exp(-1 / 45))  # B1 counts once per sign, B2 sells
    with pytest.raises(ValueError, match="signed"):
        P.density_A(frame(rows), P.DECLARED_SPECS["S_congress_w90"], [E1], one(t), coverage=None)


# --- 11. basis filter -----------------------------------------------------------------------


def test_allowed_known_at_bases_and_first_seen_live_start():
    t = ts("2024-06-14 20:00")
    filing_only = P.DensitySpec("filing_only", SPEC90.rules, 90, 45.0, allowed_known_at_bases=("filing",))
    rows = [ev(known_at=t - DAY), ev(known_at=t - DAY, actor_id="0000000002", known_at_basis="first_seen")]
    events = frame(rows)
    assert P.density_A(events, filing_only, [E1], one(t), coverage=None).iloc[0, 0] == pytest.approx(np.exp(-1 / 45))
    assert P.density_A(events, SPEC90, [E1], one(t), coverage=None).iloc[0, 0] == pytest.approx(2 * np.exp(-1 / 45))
    span = P.CoverageSpan("form4", ts("2010-01-01"))
    live_before = P.CoverageSpan("form4", t - 10 * DAY, known_at_basis="first_seen")
    live_after = P.CoverageSpan("form4", t - DAY / 2, known_at_basis="first_seen")
    assert P.density_A(events, SPEC90, [E1], one(t), coverage=[span, live_before]).iloc[0, 0] > 0
    with pytest.raises(P.ImpossibleEvent):
        P.density_A(events, SPEC90, [E1], one(t), coverage=[span, live_after])
    with pytest.raises(P.ImpossibleEvent, match="no live start"):
        P.density_A(events, SPEC90, [E1], one(t), coverage=[span])
    # A spec that excludes first_seen never needs the live start.
    assert P.density_A(events, filing_only, [E1], one(t), coverage=[span]).iloc[0, 0] > 0


def test_events_frame_refuses_naive_known_at_and_unknown_vocabulary():
    with pytest.raises(ValueError, match="tz-aware"):
        P.events_frame(pd.DataFrame([ev(known_at=pd.Timestamp("2024-01-01"), event_time=pd.Timestamp("2023-12-30"))]))
    with pytest.raises(ValueError, match="unknown"):
        frame([ev(known_at=ts("2024-01-01"), known_at_basis="guess")])


def test_ticker_only_rows_need_a_resolver_and_are_counted_when_dropped():
    row = ev(known_at=ts("2024-01-02 15:00"), entity_cik=None, entity_ticker="abc")
    dropped = frame([row])
    assert dropped.empty and dropped.attrs["dropped"]["unresolved_entity"] == 1
    seen = []

    def resolver(ticker, day):
        seen.append((ticker, day))
        return "sm_tkr_ABC"

    got = P.events_frame(pd.DataFrame([row]), ticker_resolver=resolver)
    assert list(got["entity_id"]) == ["sm_tkr_ABC"] and seen == [("ABC", date(2024, 1, 2))]


# --- 12. reproducible artifact --------------------------------------------------------------

_ARTIFACT_SCRIPT = r"""
import sys
sys.path.insert(0, {repo!r})
import tests.test_people_density as T
print(T.build_artifact({out!r}))
"""


def build_artifact(path: str) -> str:
    rng = np.random.default_rng(5)
    events = frame(random_events(rng, 1200, ts("2019-01-01"), ts("2021-06-01")))
    members = membership()
    decisions = P.weekly_decisions(date(2020, 1, 3), date(2021, 5, 28))
    specs = [P.DECLARED_SPECS["A_multi_mc1_w90"], P.DECLARED_SPECS["A_inst_w30"]]
    agg = P.sector_weekly_aggregates(events, specs, members, decisions, coverage=COVERAGE)
    receipt = P.build_receipt(events=events, specs=specs, membership=members, as_of=decisions[-1].to_pydatetime())
    return P.write_frozen_artifact(agg, path, receipt=receipt)


def test_aggregate_artifact_is_byte_reproducible_and_timezone_independent(tmp_path):
    a = build_artifact(str(tmp_path / "a.parquet"))
    b = build_artifact(str(tmp_path / "b.parquet"))
    assert a == b
    receipt_a = (tmp_path / "a.parquet.receipt.json").read_bytes()
    assert receipt_a == (tmp_path / "b.parquet.receipt.json").read_bytes()
    assert b"\r\n" not in receipt_a
    out = tmp_path / "tz.parquet"
    env = {**os.environ, "TZ": "Asia/Kolkata", "PYTHONHASHSEED": "123"}
    proc = subprocess.run([sys.executable, "-c", _ARTIFACT_SCRIPT.format(repo=str(REPO), out=str(out))],
                          env=env, capture_output=True, text=True, cwd=str(REPO), timeout=600)
    assert proc.returncode == 0, proc.stderr[-2000:]
    assert proc.stdout.strip().splitlines()[-1] == a
    agg = pd.read_parquet(tmp_path / "a.parquet")
    assert list(agg.columns) == list(P.AGGREGATE_COLUMNS) and len(agg) > 0


def test_code_hash_ignores_crlf_checkout(tmp_path):
    src = Path(P.__file__).read_bytes().replace(b"\r\n", b"\n")
    (tmp_path / "lf.py").write_bytes(src)
    (tmp_path / "crlf.py").write_bytes(src.replace(b"\n", b"\r\n"))
    assert P.lf_sha256(tmp_path / "lf.py") == P.lf_sha256(tmp_path / "crlf.py") == P.code_sha256()


def test_event_set_hash_is_order_independent_and_specs_hash_stably():
    rows = random_events(np.random.default_rng(1), 200, ts("2020-01-01"), ts("2021-01-01"))
    assert P.event_set_sha256(frame(rows)) == P.event_set_sha256(frame(rows.sample(frac=1.0, random_state=3)))
    assert len({s.sha256 for s in P.DECLARED_SPECS.values()}) == len(P.DECLARED_SPECS)
    assert P.DECLARED_SPECS["A_insider_buy_w90"].sha256 == P.DensitySpec(
        "A_insider_buy_w90", SPEC90.rules, 90, 45.0).sha256


# --- 14. budget -----------------------------------------------------------------------------


def test_budget_200_entities_600_decisions_50k_events():
    rng = np.random.default_rng(14)
    n_ent = 200
    rows = random_events(rng, 50_000, ts("2012-01-01"), ts("2024-01-01"), n_entities=n_ent)
    rows["actor_id"] = [f"{a:010d}" for a in rng.integers(0, 5000, len(rows))]
    members = membership(n_ent, 11)
    entities = sorted(members["entity_id"])
    decisions = P.weekly_decisions(date(2012, 6, 1), date(2024, 1, 1))[:600]
    assert len(decisions) == 600
    start = time.perf_counter()
    events = frame(rows)
    a = P.density_A(events, P.DECLARED_SPECS["A_multi_mc1_w90"], entities, decisions, coverage=COVERAGE)
    P.channel_count_C(events, P.DECLARED_SPECS["C_people_w90"], entities, decisions, coverage=COVERAGE)
    P.signed_S(events, P.DECLARED_SPECS["S_insider_w90"], entities, decisions, coverage=COVERAGE)
    P.d_self(a)
    P.d_peer(a, members)
    elapsed = time.perf_counter() - start
    assert elapsed < 30.0, f"{elapsed:.1f}s"


# --- materializer contract (GRID-PEOPLE-EVENTS-PIPELINE-DESIGN-20261001) ---------------------


def test_contract_reads_10b5_1_from_v2_attrs_and_absent_is_undetermined():
    c = P.PE_CONTRACT
    assert c.plan_flag({"attrs": {"is_10b5_1": True}}) is True
    assert c.plan_flag({"attrs": {"is_10b5_1": False}}) is False
    assert c.plan_flag({"is_10b5_1": "Y"}) is True
    assert c.plan_flag({"attrs": {"is_director": True}}) is None
    assert c.plan_flag({}) is None and c.plan_flag(None) is None
    sell = _pe(dedup_key="s", direction="sell", transaction_code="S", provenance={"attrs": {"is_10b5_1": True}})
    assert P.events_frame([sell])["plan_10b5_1"].tolist() == [True]


def test_text_security_id_wins_bigint_is_ignored_and_fara_never_resolves():
    v2 = _pe(dedup_key="a", security_id="sm_0000000042")  # v2 TEXT FK onto security_master
    v1 = _pe(dedup_key="b", security_id=12345)  # v1 BIGINT: no identity
    fara = _pe(dedup_key="c", channel="fara", entity_cik=None, entity_ticker="XLE") if "fara" in P.CHANNELS else None
    got = P.events_frame([v2, v1] + ([fara] if fara else []), ticker_resolver=lambda t, d: "sm_tkr_" + t)
    assert sorted(got["entity_id"]) == ["sm_0000000042", E1]
    if fara:
        assert got.attrs["dropped"]["non_entity_channel"] == 1
    sector_proxy = pd.DataFrame([ev(known_at=ts("2024-01-02"), entity_cik=None, entity_ticker="XLE")]).assign(channel="fara")
    if "fara" in P.CHANNELS:
        assert P.events_frame(sector_proxy, ticker_resolver=lambda t, d: "sm_tkr_" + t).empty


def test_inst_counts_new_and_increased_13f_positions_only():
    t = ts("2024-06-14 20:00")
    rows = [ev(channel="thirteen_f", actor_id=f"{i:010d}", actor_id_basis="filer_cik", transaction_code=code,
               direction="buy" if code in ("NEW", "INC") else "sell", known_at=t - DAY, plan_10b5_1=None)
            for i, code in enumerate(["NEW", "INC", "DEC", "EXIT"])]
    a = P.density_A(frame(rows), P.DECLARED_SPECS["A_inst_w90"], [E1], one(t), coverage=None).iloc[0, 0]
    assert a == pytest.approx(2 * np.exp(-1 / 45))


# --- integrity: features only, people_events only (R2/R6) -----------------------------------

FORBIDDEN_SOURCES = (
    "signal_data", "insider_trades", "congressional_trades", "wealth_flows", "dollar_flows",
    "actor_connections", "lever_pullers", "influence_loops", "sector_health_snapshots", "sector_density",
    "is_cluster_buy", "raw_series", "resolved_series", "read_window", "signal_sources",
)
FORBIDDEN_IMPORTS = ("store.observations", "store.pit", "analysis.panel_insider_density", "analysis.offline_research_proof")


def test_module_reads_only_people_events_and_no_prices():
    source = Path(P.__file__).read_text(encoding="utf-8")
    for token in FORBIDDEN_SOURCES:
        assert token not in source, token
    tree = ast.parse(source)
    imported = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)}
    imported |= {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    assert not imported & set(FORBIDDEN_IMPORTS)
    assert {m for m in imported if m and m.startswith(("store", "intelligence"))} == {
        "store.people_events", "intelligence.security_master"}


def test_declared_specs_are_frozen_and_complete():
    names = set(P.DECLARED_SPECS)
    for base in ("A_insider_buy", "A_congress", "A_inst", "A_contract", "A_lobby", "A_multi_mc1", "C_people",
                 "S_insider", "S_congress"):
        assert {f"{base}_w30", f"{base}_w90"} <= names
    for spec in P.DECLARED_SPECS.values():
        assert spec.tau_days == spec.window_days / 2
        with pytest.raises(dataclasses.FrozenInstanceError):
            spec.window_days = 1  # type: ignore[misc]
    assert "gov_contract_qq_aggregate" not in P.DECLARED_SPECS["C_people_w90"].channels
