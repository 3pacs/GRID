"""The ETF flow partial-bar repair (scripts/repair_etf_flow_partial_bars.py).

Rows stored before GRID #1496 carry the volume traded so far on the day they
were written. The repair writes, as new vintages, the value the fixed writer
stores from settled bars, on the price basis of the run that wrote each row.
These cases pin what it plans, what it leaves alone, that it writes only what
was planned and only once, that it can stop and resume, and that a read for an
instant before the repair still sees what GRID held then.
"""
from __future__ import annotations

import json
import math
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest
from sqlalchemy import text

import scripts.repair_etf_flow_partial_bars as mod
from ingestion.altdata import institutional_flows as inst
from store.observations import read_window_known_at
from tests import etf_flow_repair_support as m
from tests import thirteenf_holdings_support as s
from tests import test_etf_flow_repair_parity as parity
from tests.test_etf_flow_repair_parity import (  # the market's special dates and helpers
    BEFORE_RANGE, EX_DIVIDEND_AFTER_CLOSE, EX_DIVIDEND_OWN_DAY, FROM, SESSION_DAY, SETTLED_NEXT_MORNING,
    SETTLED_SAME_DAY, _bars, _plan, _planned, _seed_history,
)

# Fixtures shared with the pinned parity file (bound here so pytest uses them in this module too).
_one_etf = parity._one_etf
market = parity.market
world = parity.world


# ── 1. The arithmetic ────────────────────────────────────────────────────


@pytest.mark.parametrize(("d", "basis_session"), [
    (date(2026, 5, 12), date(2026, 5, 12)),      # before both dividends
    (date(2026, 5, 12), date(2026, 6, 1)),       # S after the first one
    (date(2026, 5, 15), date(2026, 5, 14)),      # the ex-dividend day, seen before its dividend
    (date(2026, 6, 25), date(2026, 6, 25)),      # after both
])
def test_todays_bars_priced_as_of_s_are_the_bars_the_writing_run_saw(market, d, basis_session):
    """close_S(k) = AdjClose_now(k) x RawClose(S) / AdjClose_now(S) for k <= S, the raw close after S."""
    bars = _bars(market)
    pos = {x: k for k, x in enumerate(bars.index)}
    seen = market.view(max(d, basis_session), seen_on=basis_session + timedelta(days=1))
    dv = seen.adj * seen.vol
    direct = {"5d": dv.rolling(5).sum(), "20d": dv.rolling(20).sum()}
    direct["accel"] = direct["5d"].diff(5)
    for metric in mod.METRICS:
        got = mod.window_value(bars, metric, pos[d], pos[basis_session])
        assert math.isclose(got, direct[metric][pd.Timestamp(d)], rel_tol=1e-12), metric
    # and the dividend matters: on today's basis 05-12 is not what was stored then
    now = mod.window_value(bars, "5d", pos[date(2026, 5, 12)], len(bars) - 1)
    assert not math.isclose(now, m.old_writer_values(market, date(2026, 5, 12), None)["5d"], rel_tol=1e-3)


@pytest.mark.parametrize(("pulled", "expected"), [
    (m.ny(date(2026, 5, 12), 14, 30), date(2026, 5, 11)),     # mid-session: the dividend state of the day before
    (m.ny(date(2026, 5, 12), 20), date(2026, 5, 11)),         # after the close, likewise
    (m.ny(date(2026, 5, 13), 4), date(2026, 5, 12)),          # before the next open
    (m.ny(date(2026, 5, 16), 10), date(2026, 5, 15)),         # a Saturday
    (datetime(2026, 11, 27, 20, tzinfo=timezone.utc), date(2026, 11, 25)),  # the settled-bars writer; 11-26 Thanksgiving
    (m.ny(date(2026, 4, 6), 10), date(2026, 4, 2)),           # Monday after Good Friday
])
def test_the_price_basis_is_the_last_exchange_session_before_the_new_york_date_of_the_write(pulled, expected):
    assert mod.price_basis_session(pulled) == expected
    assert mod.price_basis_session(pulled, [expected]) == expected
    assert mod.price_basis_session(pulled, [expected - timedelta(days=1)]) is None   # the provider lacks it


# ── 2. What is planned ───────────────────────────────────────────────────


def test_a_missing_basis_session_outside_every_window_refuses_the_etf(world):
    """A row written days after its date has its basis session S after every window it is in."""
    engine, market, _ = world
    d, written = date(2026, 4, 10), date(2026, 4, 24)
    m.seed_row(engine, "SPY", d, m.old_writer_values(market, d, (0.3, 1.0), seen_on=written), m.ny(written, 12))
    basis = mod.price_basis_session(m.ny(written, 12))
    assert basis == date(2026, 4, 23)
    kw = {"from_date": FROM, "to_date": d, "session_day": SESSION_DAY}
    assert mod.build_plan(engine, m.FakeYF(market), **kw)["tickers"]["SPY"]["status"] == "OK"
    plan = mod.build_plan(engine, m.FakeYF(market, drop={basis}), **kw)
    assert plan["tickers"]["SPY"]["status"] == "REFUSED"
    assert plan["tickers"]["SPY"]["missing_sessions"] == [basis.isoformat()]
    assert plan["items"] == []


def _own_bar_delta(market, d, stored_payload, basis_session):
    """settled own bar minus the stored one, both priced as of ``basis_session``."""
    ts = pd.Timestamp(d)
    if basis_session < d:
        close = float(market.raw[ts])
    else:
        seen = market.view(basis_session)
        close = float(seen.adj[ts])
    return close * float(market.vol[ts]) - stored_payload["close"] * stored_payload["volume"]


def test_a_same_day_row_changes_by_its_own_bar_only_even_on_an_ex_dividend_date(world):
    """The other bars of a same-day row were settled; the repair moves the row by its own bar and
    keeps the dividend basis it was written on (the partner review's ex-dividend case)."""
    engine, market, kinds = world
    planned = _planned(_plan(engine, market))
    checked = 0
    for (sid, d), item in planned.items():
        if kinds[d] != "partial_same_day":
            continue
        (old,) = m.vintages(engine, sid, d)
        delta = _own_bar_delta(market, d, old["payload"], date.fromisoformat(item["price_basis_session"]))
        scale = abs(planned[(sid.rsplit(":", 1)[0] + ":5d", d)]["value"])
        assert abs(item["value"] - old["value"] - delta) / scale < 1e-12, (sid, d)
        assert abs(item["stored_bar_residual"]) < 1e-12
        checked += 1
    assert checked > 150
    ex = planned[("ETF_FLOW:SPY:5d", EX_DIVIDEND_OWN_DAY)]
    assert ex["price_basis_session"] == "2026-05-14"
    assert ex["close"] == pytest.approx(float(market.raw[pd.Timestamp(EX_DIVIDEND_OWN_DAY)]))
    next_morning = market.view(EX_DIVIDEND_OWN_DAY)                     # the dividend applied
    dv = next_morning.adj * next_morning.vol
    assert not math.isclose(ex["value"], dv.rolling(5).sum()[pd.Timestamp(EX_DIVIDEND_OWN_DAY)], rel_tol=1e-4)


def test_the_plan_takes_every_short_row_in_range_and_nothing_else(world):
    engine, market, kinds = world
    plan = _plan(engine, market)
    planned_dates = {date.fromisoformat(i["obs_date"]) for i in plan["items"]}
    in_range = {d for d in kinds if FROM <= d <= SESSION_DAY - timedelta(days=15)}
    expected = {d for d in in_range if kinds[d] in ("partial_same_day", "short_next_morning")}
    assert planned_dates == expected
    assert SETTLED_SAME_DAY not in planned_dates and SETTLED_NEXT_MORNING not in planned_dates
    assert BEFORE_RANGE not in planned_dates
    assert plan["summary"]["rows"] == 3 * len(expected) == len(plan["items"])
    assert plan["summary"]["left_alone"] == {"within_tolerance": 9}
    assert plan["tickers"]["SPY"]["status"] == "OK"
    every = plan["items"][0]
    assert every["revises_value"] != every["value"] and every["revises_id"]
    assert plan["digest"] == mod._digest(plan)


def test_recomputing_on_todays_basis_would_also_rewrite_a_correct_row(world, monkeypatch):
    """Why the repair is not the writer's ``revise_from``: dividends since move every value."""
    engine, market, _ = world
    plan = _plan(engine, market)
    assert ("ETF_FLOW:SPY:5d", SETTLED_SAME_DAY) not in _planned(plan)
    m.install_yfinance(monkeypatch, m.FakeYF(market))
    inst.InstitutionalFlowsPuller(engine).pull_etf_only(start_date=FROM, revise_from=FROM)
    assert len(m.vintages(engine, "ETF_FLOW:SPY:5d", SETTLED_SAME_DAY)) == 2      # rewritten, though correct


def test_the_plan_stays_out_of_the_scheduled_writers_window(world):
    engine, market, _ = world
    plan = _plan(engine, market)
    assert plan["to"] == (SESSION_DAY - timedelta(days=15)).isoformat()
    assert plan["writer_floor"] == (SESSION_DAY - timedelta(days=14)).isoformat()
    with pytest.raises(mod.Refused, match="scheduled writer"):
        _plan(engine, market, to_date=SESSION_DAY - timedelta(days=14))


def test_planning_writes_nothing(world):
    engine, market, _ = world
    before = m.row_count(engine)
    _plan(engine, market)
    assert m.row_count(engine) == before


def test_an_etf_the_provider_does_not_serve_is_reported_and_not_planned(world, monkeypatch):
    engine, market, _ = world
    monkeypatch.setattr(inst, "ETF_FLOW_TICKERS", ["SPY", "QQQ"])
    _seed_history(engine, market, tickers=("QQQ",))
    plan = mod.build_plan(engine, m.FakeYF(market, fail={"QQQ"}), from_date=FROM, session_day=SESSION_DAY)
    assert plan["tickers"]["QQQ"]["status"] == "FAILED"
    assert {i["ticker"] for i in plan["items"]} == {"SPY"}


def test_an_etf_with_a_split_in_the_fetched_range_is_refused(world):
    engine, market, _ = world
    market.splits[date(2026, 5, 4)] = 2.0
    plan = _plan(engine, market)
    assert plan["tickers"]["SPY"]["status"] == "REFUSED" and plan["items"] == []


def test_planning_starts_no_etf_after_its_deadline(world, monkeypatch):
    engine, market, _ = world
    monkeypatch.setattr(inst, "ETF_FLOW_TICKERS", ["SPY", "QQQ"])
    clock = {"t": 0.0}
    monkeypatch.setattr(mod, "_monotonic", lambda: clock["t"])
    fake = m.FakeYF(market)
    real = fake.download
    fake.download = lambda ticker, **kw: (clock.update(t=100.0), real(ticker, **kw))[1]
    plan = mod.build_plan(engine, fake, from_date=FROM, session_day=SESSION_DAY, deadline_at=50.0)
    assert [a["ticker"] for a in fake.asked] == ["SPY"]
    assert plan["tickers"]["QQQ"] == {"status": "NOT_ATTEMPTED", "reason": "deadline"}


# ── 3. Apply, verify, undo ───────────────────────────────────────────────


def test_apply_appends_the_planned_rows_and_keeps_every_old_one(world):
    engine, market, _ = world
    plan = _plan(engine, market)
    before = m.row_count(engine)
    result = mod.run_writes(engine, plan, session_day=SESSION_DAY)
    assert result["status"] == "DONE" and result["totals"] == {"written": len(plan["items"])}
    assert m.row_count(engine) == before + len(plan["items"])
    item = plan["items"][0]
    old, new = m.vintages(engine, item["series_id"], date.fromisoformat(item["obs_date"]))
    assert old["value"] == item["revises_value"] and new["value"] == item["value"]
    assert new["payload"]["revises_value"] == item["revises_value"]
    assert new["payload"]["volume"] == item["volume"]
    assert new["payload"]["repair"]["run_id"] == plan["run_id"]
    assert new["payload"]["repair"]["revises_id"] == old["id"]
    assert new["payload"]["repair"]["plan_digest"] == plan["digest"]
    mark = new["payload"]["repair"]
    basis_day = pd.Timestamp(mark["price_basis_session"])
    assert mark["basis_raw_close"] == pytest.approx(float(market.raw[basis_day]))
    assert mark["basis_adj_close_now"] == pytest.approx(float(market.view(market.sessions[-1]).adj[basis_day]))
    assert mod.verify_plan(engine, plan) == {
        "run_id": plan["run_id"], "digest": plan["digest"], "planned": len(plan["items"]),
        "states": {"applied": len(plan["items"]), "undone": 0, "applied_then_superseded": 0,
                   "superseded_before_apply": 0, "not_applied": 0, "repair_quarantined": 0},
        "plan_gaps": {"tickers_not_ok": {}, "rows_left_unrepaired": {}}, "plan_gaps_accepted": False,
        "repair_in_force": len(plan["items"]),
        "accepted": {"undone": False, "superseded_over_tolerance": False},
        "superseded_vs_plan": {"count": 0, "max_rel_diff_of_5d": None, "over_tolerance": 0},
        "integrity_ok": True, "complete": True, "ok": True, "problems": []}


def test_verify_is_not_ok_before_apply_or_after_an_interrupted_apply(world, monkeypatch, tmp_path):
    engine, market, _ = world
    plan = _plan(engine, market)
    n = len(plan["items"])
    before = mod.verify_plan(engine, plan)
    assert before["integrity_ok"] and not before["complete"] and not before["ok"]
    assert before["states"]["not_applied"] == n
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(plan))
    assert mod.main(["verify", "--plan", str(path), "--expect-digest", plan["digest"]], engine=engine) == 4
    clock = {"t": 0.0}
    monkeypatch.setattr(inst, "_monotonic", lambda: clock["t"])
    monkeypatch.setattr(mod, "_monotonic", lambda: clock["t"])
    real = mod._write_series

    def _slow(*a, **kw):
        out = real(*a, **kw)
        clock["t"] += 10.0
        return out

    monkeypatch.setattr(mod, "_write_series", _slow)
    assert mod.run_writes(engine, plan, deadline_at=15.0, session_day=SESSION_DAY)["status"] == "STOPPED_AT_DEADLINE"
    partial = mod.verify_plan(engine, plan)
    assert partial["integrity_ok"] and not partial["complete"]
    assert 0 < partial["states"]["applied"] < n and partial["states"]["not_applied"] == n - partial["states"]["applied"]
    assert mod.main(["verify", "--plan", str(path), "--expect-digest", plan["digest"]], engine=engine) == 4
    mod.run_writes(engine, plan, session_day=SESSION_DAY)
    assert mod.verify_plan(engine, plan)["ok"]
    assert mod.main(["verify", "--plan", str(path), "--expect-digest", plan["digest"]], engine=engine) == 0


def test_verify_counts_a_legitimate_supersession_as_complete(world):
    engine, market, _ = world
    plan = _plan(engine, market)
    first, second = sorted({i["obs_date"] for i in plan["items"]})[:2]
    later = {"5d": 1.0, "20d": 2.0, "accel": 3.0, "close": 1.0, "volume": 1}
    m.seed_row(engine, "SPY", date.fromisoformat(first), later, datetime(2026, 7, 20, 16, tzinfo=timezone.utc))
    mod.run_writes(engine, plan, session_day=SESSION_DAY)                # 'first' is left to that revision
    repaired = m.vintages(engine, "ETF_FLOW:SPY:5d", date.fromisoformat(second))[-1]["pulled"]
    repaired = datetime.fromisoformat(repaired) if isinstance(repaired, str) else repaired
    m.seed_row(engine, "SPY", date.fromisoformat(second), later,      # a revision after the repair row
               repaired.replace(tzinfo=repaired.tzinfo or timezone.utc) + timedelta(minutes=1))
    result = mod.verify_plan(engine, plan)
    assert result["integrity_ok"] and not result["complete"]           # 6 superseders far from the plan
    assert mod.verify_plan(engine, plan, accept_superseded=True)["ok"]
    result = mod.verify_plan(engine, plan, accept_superseded=True)
    assert result["accepted"]["superseded_over_tolerance"] is True
    assert result["states"]["superseded_before_apply"] == 3
    assert result["states"]["applied_then_superseded"] == 3
    assert result["states"]["applied"] == len(plan["items"]) - 6
    assert result["superseded_vs_plan"]["count"] == 6                    # reported, with how far off
    assert result["superseded_vs_plan"]["over_tolerance"] == 6 and result["superseded_vs_plan"]["max_rel_diff_of_5d"] > 0.9


def test_verify_does_not_count_a_quarantined_repair_row_as_done(world, tmp_path):
    engine, market, _ = world
    plan = _plan(engine, market)
    mod.run_writes(engine, plan, session_day=SESSION_DAY)
    item = plan["items"][0]
    repair = m.vintages(engine, item["series_id"], date.fromisoformat(item["obs_date"]))[-1]
    with engine.begin() as conn:
        conn.execute(text("UPDATE raw_series SET pull_status = 'QUARANTINED' WHERE id = :i"), {"i": repair["id"]})
    result = mod.verify_plan(engine, plan)
    assert result["integrity_ok"] and not result["complete"] and not result["ok"]
    assert result["states"]["repair_quarantined"] == 1 and result["states"]["applied_then_superseded"] == 0
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(plan))
    assert mod.main(["verify", "--plan", str(path), "--expect-digest", plan["digest"]], engine=engine) == 4
    again = mod.run_writes(engine, plan, session_day=SESSION_DAY)      # a quarantine is a retraction
    assert again["status"] == "DONE_WITH_REFUSED_ROWS"
    assert again["totals"] == {"refused_repair_was_quarantined": 1, "already_applied": len(plan["items"]) - 1}
    assert mod.main(["apply", "--plan", str(path), "--expect-digest", plan["digest"]], engine=engine) == 1
    forced = mod.run_writes(engine, plan, session_day=SESSION_DAY, reapply_quarantined=True)
    assert forced["totals"] == {"written": 1, "already_applied": len(plan["items"]) - 1}
    assert mod.verify_plan(engine, plan)["ok"]


def test_verify_is_not_complete_while_the_plan_left_an_etf_or_rows_unrepaired(world, monkeypatch, tmp_path):
    engine, market, _ = world
    monkeypatch.setattr(inst, "ETF_FLOW_TICKERS", ["SPY", "QQQ"])
    _seed_history(engine, market, tickers=("QQQ",))
    path = tmp_path / "plan.json"
    assert mod.main(["plan", "--plan-out", str(path)], yf=m.FakeYF(market, fail={"QQQ"}), engine=engine) == 1
    plan = json.loads(path.read_text())
    assert mod.main(["apply", "--plan", str(path), "--expect-digest", plan["digest"],
                     "--allow-incomplete-plan"], engine=engine) == 0
    result = mod.verify_plan(engine, plan)
    assert result["integrity_ok"] and result["states"]["not_applied"] == 0 and not result["complete"]
    assert result["plan_gaps"] == {"tickers_not_ok": {"QQQ": "FAILED"}, "rows_left_unrepaired": {}}
    verify = ["verify", "--plan", str(path), "--expect-digest", plan["digest"]]
    assert mod.main(verify, engine=engine) == 4
    assert mod.main([*verify, "--accept-plan-gaps"], engine=engine) == 0
    assert mod.verify_plan(engine, plan, accept_plan_gaps=True)["plan_gaps_accepted"] is True
    assert mod.main(["verify", "--plan", str(path), "--expect-digest", "0" * 64], engine=engine) == 2
    unpriced = json.loads(json.dumps(plan))
    unpriced["tickers"] = {"SPY": plan["tickers"]["SPY"]}
    unpriced["summary"]["left_alone"] = {"within_tolerance": 9, "not_computable": 3}
    gaps = mod.plan_gaps(unpriced)
    assert gaps == {"tickers_not_ok": {}, "rows_left_unrepaired": {"not_computable": 3}}


def test_verify_flags_a_repair_row_whose_marker_is_not_this_plans(world):
    engine, market, _ = world
    plan = _plan(engine, market)
    mod.run_writes(engine, plan, session_day=SESSION_DAY)
    item = plan["items"][0]
    repair = m.vintages(engine, item["series_id"], date.fromisoformat(item["obs_date"]))[-1]
    with engine.begin() as conn:
        conn.execute(text("UPDATE raw_series SET raw_payload = REPLACE(raw_payload, :a, :b) WHERE id = :i"),
                     {"a": plan["digest"], "b": "0" * 64, "i": repair["id"]})
    result = mod.verify_plan(engine, plan)
    assert not result["integrity_ok"] and not result["ok"]
    assert any("marker" in p for p in result["problems"])


def test_apply_twice_writes_each_row_once(world):
    engine, market, _ = world
    plan = _plan(engine, market)
    mod.run_writes(engine, plan, session_day=SESSION_DAY)
    again = mod.run_writes(engine, plan, session_day=SESSION_DAY)
    assert again["totals"] == {"already_applied": len(plan["items"])}
    assert mod.verify_plan(engine, plan)["ok"]


def test_a_date_revised_after_planning_is_left_to_that_revision(world):
    engine, market, _ = world
    plan = _plan(engine, market)
    item = plan["items"][0]
    d = date.fromisoformat(item["obs_date"])
    m.seed_row(engine, "SPY", d, {"5d": 1.0, "20d": 2.0, "accel": 3.0, "close": 1.0, "volume": 1},
               datetime(2026, 7, 20, 16, tzinfo=timezone.utc), {"revises_value": item["revises_value"]})
    result = mod.run_writes(engine, plan, session_day=SESSION_DAY)
    assert result["totals"]["superseded"] == 3
    assert result["totals"]["written"] == len(plan["items"]) - 3
    newest = m.vintages(engine, item["series_id"], d)[-1]
    assert newest["value"] == {"5d": 1.0, "20d": 2.0, "accel": 3.0}[item["metric"]]      # left as revised


def test_a_stopped_apply_resumes_where_it_stopped(world, monkeypatch, tmp_path):
    engine, market, _ = world
    monkeypatch.setattr(inst, "ETF_FLOW_TICKERS", ["SPY", "QQQ"])
    _seed_history(engine, market, tickers=("QQQ",))
    plan = _plan(engine, market)
    assert len({i["series_id"] for i in plan["items"]}) == 6
    clock = {"t": 0.0}
    monkeypatch.setattr(inst, "_monotonic", lambda: clock["t"])
    monkeypatch.setattr(mod, "_monotonic", lambda: clock["t"])
    real = mod._write_series

    def _slow(*a, **kw):
        out = real(*a, **kw)
        clock["t"] += 10.0
        return out

    monkeypatch.setattr(mod, "_write_series", _slow)
    progress = tmp_path / "p.jsonl"
    first = mod.run_writes(engine, plan, deadline_at=25.0, progress_path=progress, session_day=SESSION_DAY)
    assert first["status"] == "STOPPED_AT_DEADLINE"
    done = first["totals"]["written"]
    assert 0 < done < len(plan["items"])
    lines = [json.loads(x) for x in progress.read_text().splitlines()]
    assert [x.get("series") for x in lines[:3]] == sorted({i["series_id"] for i in plan["items"]})[:3]
    assert lines[-1]["event"] == "deadline"
    second = mod.run_writes(engine, plan, deadline_at=None, session_day=SESSION_DAY)
    assert second["status"] == "DONE"
    assert second["totals"]["written"] == len(plan["items"]) - done
    assert second["totals"]["already_applied"] == done
    assert mod.verify_plan(engine, plan)["ok"]


def test_an_edited_plan_or_another_digest_is_refused(world, tmp_path):
    engine, market, _ = world
    plan = _plan(engine, market)
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(plan))
    assert mod.load_plan(path, plan["digest"])["run_id"] == plan["run_id"]
    with pytest.raises(mod.Refused, match="expected"):
        mod.load_plan(path, "0" * 64)
    for field, edit in (("items", lambda p: p["items"][0].update(value=p["items"][0]["value"] * 2)),
                        ("run_id", lambda p: p.update(run_id="etfpb-other")),
                        ("source_id", lambda p: p.update(source_id=1)),
                        ("tickers", lambda p: p["tickers"]["SPY"].update(status="OK", planned=0))):
        edited = json.loads(json.dumps(plan))
        edit(edited)
        path.write_text(json.dumps(edited))
        with pytest.raises(mod.Refused, match="edited"):
            mod.load_plan(path)


def test_apply_refuses_a_plan_in_which_an_etf_was_not_planned(world, monkeypatch, tmp_path):
    engine, market, _ = world
    monkeypatch.setattr(inst, "ETF_FLOW_TICKERS", ["SPY", "QQQ"])
    _seed_history(engine, market, tickers=("QQQ",))
    path = tmp_path / "plan.json"
    fake = m.FakeYF(market, fail={"QQQ"})
    assert mod.main(["plan", "--plan-out", str(path)], yf=fake, engine=engine) == 1
    digest = json.loads(path.read_text())["digest"]
    before = m.row_count(engine)
    assert mod.main(["apply", "--plan", str(path), "--expect-digest", digest], engine=engine) == 2
    assert m.row_count(engine) == before
    assert mod.main(["apply", "--plan", str(path), "--expect-digest", digest,
                     "--allow-incomplete-plan"], engine=engine) == 0
    with engine.connect() as conn:
        tickers = {r[0] for r in conn.execute(text(
            "SELECT DISTINCT series_id FROM raw_series WHERE raw_payload LIKE '%\"repair\"%'"))}
    assert tickers and all(":SPY:" in t for t in tickers)


def test_a_plan_for_another_database_is_refused(world):
    engine, market, _ = world
    plan = _plan(engine, market)
    plan["source_id"] = 99
    with pytest.raises(mod.Refused, match="source_id"):
        mod.run_writes(engine, plan, session_day=SESSION_DAY)


def test_a_date_stored_twice_is_repaired_once_from_its_newest_vintage(world):
    """04-06 / 04-08 on production: two rows, milliseconds apart, the same partial value."""
    engine, market, _ = world
    d = date(2026, 4, 6)
    (first,) = m.vintages(engine, "ETF_FLOW:SPY:5d", d)
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, value, raw_payload, pull_status) "
            "SELECT series_id, source_id, obs_date, pull_timestamp, value, raw_payload, pull_status "
            "FROM raw_series WHERE obs_date = :d"), {"d": d})
    plan = _plan(engine, market)
    item = _planned(plan)[("ETF_FLOW:SPY:5d", d)]
    assert item["revises_id"] > first["id"]                   # the newer of the two
    mod.run_writes(engine, plan, session_day=SESSION_DAY)
    rows = m.vintages(engine, "ETF_FLOW:SPY:5d", d)
    assert [("repair" in r["payload"]) for r in rows] == [False, False, True]
    assert mod.verify_plan(engine, plan)["ok"]


def test_a_read_for_an_instant_before_the_repair_still_sees_what_was_held_then(world):
    engine, market, _ = world
    plan = _plan(engine, market)
    mod.run_writes(engine, plan, session_day=SESSION_DAY)
    item = next(i for i in plan["items"] if i["metric"] == "5d")
    d = date.fromisoformat(item["obs_date"])
    repaired = max(v["pulled"] for v in m.vintages(engine, "ETF_FLOW:SPY:5d", d))
    repaired = datetime.fromisoformat(repaired) if isinstance(repaired, str) else repaired
    with engine.connect() as conn:
        then = {o.obs_date: o.value for o in read_window_known_at(
            conn, "ETF_FLOW:SPY:5d", as_of=date(2026, 7, 10), lag=None, start=d)}
        now = {o.obs_date: o.value for o in read_window_known_at(   # the day after the repair row's own date
            conn, "ETF_FLOW:SPY:5d", as_of=repaired.date() + timedelta(days=1), lag=None, start=d)}
    assert then[d] == item["revises_value"]
    assert now[d] == item["value"]


def test_undo_appends_the_replaced_values_and_keeps_the_repair(world):
    engine, market, _ = world
    plan = _plan(engine, market)
    mod.run_writes(engine, plan, session_day=SESSION_DAY)
    result = mod.run_writes(engine, plan, mode="undo", session_day=SESSION_DAY, expect_digest=plan["digest"])
    assert result["status"] == "DONE" and result["totals"] == {"written": len(plan["items"])}
    item = plan["items"][0]
    old, repair, undo = m.vintages(engine, item["series_id"], date.fromisoformat(item["obs_date"]))
    assert undo["value"] == old["value"] and repair["value"] == item["value"]
    assert undo["payload"]["repair_undo"]["undoes_id"] == repair["id"]
    assert undo["payload"]["repair_undo"]["restores_id"] == old["id"]
    assert undo["payload"]["repair_undo"]["plan_digest"] == plan["digest"]
    assert {k: v for k, v in undo["payload"].items() if k not in ("repair_undo", "revises_value")} == old["payload"]
    assert "repair" not in undo["payload"]
    verified = mod.verify_plan(engine, plan)
    assert verified["integrity_ok"] and not verified["ok"]                # "ok" never means undone
    assert verified["states"]["undone"] == len(plan["items"]) and verified["repair_in_force"] == 0
    assert mod.verify_plan(engine, plan, accept_undone=True)["ok"]
    again = mod.run_writes(engine, plan, mode="undo", session_day=SESSION_DAY, expect_digest=plan["digest"])
    assert again["totals"] == {"not_newest": len(plan["items"])}
    reapplied = mod.run_writes(engine, plan, session_day=SESSION_DAY)
    assert reapplied["totals"] == {"superseded": len(plan["items"])}      # an undo is not re-applied


def test_undoing_a_repair_of_an_earlier_repair_keeps_that_earlier_runs_lineage():
    # Bars to the session before SESSION_DAY, so the first run's rows (dated 07-20 here) have a basis.
    market = m.Market(end=date(2026, 7, 17), periods=150,
                      dividends={date(2026, 5, 15): 1.5, EX_DIVIDEND_AFTER_CLOSE: 1.2})
    engine = s.make_sqlite_engine()
    _seed_history(engine, market)
    first = _plan(engine, market)
    mod.run_writes(engine, first, session_day=SESSION_DAY)
    with engine.begin() as conn:                                         # the first run, dated in this world
        conn.execute(text("UPDATE raw_series SET pull_timestamp = :t WHERE raw_payload LIKE :p"),
                     {"t": "2026-07-20 15:30:00.000000", "p": f'%{first["run_id"]}%'})
    d = date.fromisoformat(first["items"][0]["obs_date"])
    market.vol[pd.Timestamp(d)] *= 1.05                                   # the provider revises that bar
    second = _plan(engine, market)
    item = next(i for i in second["items"] if i["obs_date"] == d.isoformat() and i["metric"] == "5d")
    earlier = m.vintages(engine, item["series_id"], d)[-1]
    assert item["revises_id"] == earlier["id"] and earlier["payload"]["repair"]["run_id"] == first["run_id"]
    mod.run_writes(engine, second, session_day=SESSION_DAY)
    mod.run_writes(engine, second, mode="undo", session_day=SESSION_DAY, expect_digest=second["digest"])
    restored = m.vintages(engine, item["series_id"], d)[-1]
    assert restored["value"] == earlier["value"]
    assert restored["payload"]["restored_repair"] == earlier["payload"]["repair"]
    assert "repair" not in restored["payload"]
    assert mod.verify_plan(engine, second, accept_undone=True)["ok"]
    assert mod.verify_plan(engine, first)["integrity_ok"]                # no second row of the first run


def test_accept_undone_passes_only_a_finished_undo(world, monkeypatch):
    engine, market, _ = world
    plan = _plan(engine, market)
    mod.run_writes(engine, plan, session_day=SESSION_DAY)
    clock = {"t": 0.0}
    monkeypatch.setattr(inst, "_monotonic", lambda: clock["t"])
    monkeypatch.setattr(mod, "_monotonic", lambda: clock["t"])
    real = mod._write_series

    def _slow(*a, **kw):
        out = real(*a, **kw)
        clock["t"] += 10.0
        return out

    monkeypatch.setattr(mod, "_write_series", _slow)
    half = mod.run_writes(engine, plan, mode="undo", deadline_at=15.0, session_day=SESSION_DAY,
                          expect_digest=plan["digest"])
    assert half["status"] == "STOPPED_AT_DEADLINE"
    partly = mod.verify_plan(engine, plan, accept_undone=True)
    assert partly["states"]["undone"] > 0 and partly["repair_in_force"] > 0 and not partly["ok"]
    mod.run_writes(engine, plan, mode="undo", session_day=SESSION_DAY, expect_digest=plan["digest"])
    assert mod.verify_plan(engine, plan, accept_undone=True)["ok"]


def test_undo_writes_only_for_the_exact_approved_digest(world, tmp_path):
    engine, market, _ = world
    plan = _plan(engine, market)
    mod.run_writes(engine, plan, session_day=SESSION_DAY)
    before = m.row_count(engine)
    for digest in (None, "0" * 64):
        with pytest.raises(mod.Refused, match="digest"):
            mod.run_writes(engine, plan, mode="undo", session_day=SESSION_DAY, expect_digest=digest)
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(plan))
    assert mod.main(["undo", "--plan", str(path), "--expect-digest", "0" * 64, "--apply"], engine=engine) == 2
    with pytest.raises(SystemExit):                                       # --expect-digest is required
        mod.main(["undo", "--plan", str(path), "--apply"], engine=engine)
    assert m.row_count(engine) == before


def test_undo_refuses_a_date_whose_original_row_is_missing_or_changed(world, tmp_path):
    engine, market, _ = world
    plan = _plan(engine, market)
    mod.run_writes(engine, plan, session_day=SESSION_DAY)
    gone, changed = (next(i for i in plan["items"] if i["metric"] == mt) for mt in ("5d", "20d"))
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM raw_series WHERE id = :i"), {"i": gone["revises_id"]})
        conn.execute(text("UPDATE raw_series SET value = value * 2 WHERE id = :i"), {"i": changed["revises_id"]})
    assert not mod.verify_plan(engine, plan)["integrity_ok"]
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(plan))
    assert mod.main(["undo", "--plan", str(path), "--expect-digest", plan["digest"]], engine=engine) == 1  # dry
    before = m.row_count(engine)
    result = mod.run_writes(engine, plan, mode="undo", session_day=SESSION_DAY, expect_digest=plan["digest"])
    assert result["status"] == "DONE_WITH_REFUSED_ROWS"
    assert result["totals"]["refused_original_missing"] == 1
    assert result["totals"]["refused_original_value_changed"] == 1
    assert result["totals"]["written"] == len(plan["items"]) - 2
    assert m.row_count(engine) == before + len(plan["items"]) - 2
    for item in (gone, changed):                                         # nothing written for those dates
        newest = m.vintages(engine, item["series_id"], date.fromisoformat(item["obs_date"]))[-1]
        assert newest["payload"]["repair"]["run_id"] == plan["run_id"]


# ── 4. The command ───────────────────────────────────────────────────────


def test_the_command_plans_then_applies_then_verifies(world, tmp_path, capsys):
    engine, market, _ = world
    path = tmp_path / "plan.json"
    fake = m.FakeYF(market)
    assert mod.main(["plan", "--plan-out", str(path), "--from", FROM.isoformat()], yf=fake, engine=engine) == 0
    out = capsys.readouterr().out
    plan = json.loads(path.read_text())
    assert f"rows to write {plan['summary']['rows']}" in out and "nothing written" in out
    assert mod.main(["plan", "--plan-out", str(path)], yf=fake, engine=engine) == 2        # never overwritten
    assert mod.main(["apply", "--plan", str(path), "--expect-digest", "f" * 64], engine=engine) == 2
    before = m.row_count(engine)
    assert mod.main(["undo", "--plan", str(path), "--expect-digest", plan["digest"]], engine=engine) == 0  # dry
    assert m.row_count(engine) == before
    assert mod.main(["apply", "--plan", str(path), "--expect-digest", plan["digest"]], engine=engine) == 0
    assert m.row_count(engine) == before + plan["summary"]["rows"]
    assert mod.main(["verify", "--plan", str(path), "--expect-digest", plan["digest"]], engine=engine) == 0
    assert '"ok": true' in capsys.readouterr().out
    assert (tmp_path / "plan.apply.progress.jsonl").exists()
