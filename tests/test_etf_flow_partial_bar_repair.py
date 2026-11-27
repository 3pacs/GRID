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

import scripts.repair_etf_flow_partial_bars as mod
from ingestion.altdata import institutional_flows as inst
from store.observations import read_window_known_at
from tests import etf_flow_repair_support as m
from tests import thirteenf_holdings_support as s

SESSION_DAY = date(2026, 7, 20)               # New York's date when the repair runs
FROM = date(2026, 4, 1)
SETTLED_SAME_DAY = date(2026, 5, 12)          # written after the close, volume final; dividend ex 05-15 after it
SETTLED_NEXT_MORNING = date(2026, 6, 3)       # written before the next open, volume final
SHORT_NEXT_MORNING = date(2026, 5, 14)        # written before the next open, volume 3 % short
BEFORE_RANGE = date(2026, 3, 25)              # partial, but before --from
PARTIAL = (0.3, None)                         # 30 % of the volume, price set per day


@pytest.fixture(autouse=True)
def _one_etf(monkeypatch):
    monkeypatch.setattr(inst, "ETF_FLOW_TICKERS", ["SPY"])
    monkeypatch.setattr(inst, "_YF_RATE_DELAY", 0)
    monkeypatch.setattr(inst, "_new_york_today", lambda: SESSION_DAY)
    monkeypatch.setattr(mod, "PROVIDER_DELAY_S", 0)
    monkeypatch.setattr(mod, "_sleep", lambda _s: None)


@pytest.fixture()
def market():
    return m.Market(dividends={date(2026, 5, 15): 1.5, date(2026, 6, 19): 1.2})


def _seed_history(engine, market: m.Market, tickers=("SPY",)) -> dict[date, str]:
    """The stored state before the repair; returns how each date was written."""
    kinds: dict[date, str] = {}
    for ticker in tickers:
        for d in market.sessions:
            if d < date(2026, 3, 20):
                continue
            if d == SETTLED_SAME_DAY:
                m.seed_row(engine, ticker, d, m.old_writer_values(market, d, None), m.ny(d, 20))
                kinds[d] = "settled_same_day"
            elif d == SETTLED_NEXT_MORNING:
                m.seed_row(engine, ticker, d, m.old_writer_values(market, d, None),
                           m.ny(market.next_session(d), 4))
                kinds[d] = "settled_next_morning"
            elif d == SHORT_NEXT_MORNING:
                price = float(market.raw[pd.Timestamp(d)])
                m.seed_row(engine, ticker, d, m.old_writer_values(market, d, (0.97, price)),
                           m.ny(market.next_session(d), 4))
                kinds[d] = "short_next_morning"
            else:
                price = float(market.raw[pd.Timestamp(d)]) * 1.01
                m.seed_row(engine, ticker, d, m.old_writer_values(market, d, (0.3, price)), m.ny(d, 14, 30))
                kinds[d] = "partial_same_day"
    return kinds


@pytest.fixture()
def world(market):
    engine = s.make_sqlite_engine()
    kinds = _seed_history(engine, market)
    return engine, market, kinds


def _plan(engine, market, **kw):
    return mod.build_plan(engine, m.FakeYF(market), from_date=FROM, session_day=SESSION_DAY,
                          now=datetime(2026, 7, 20, 15, tzinfo=timezone.utc), **kw)


def _planned(plan) -> dict[tuple[str, date], dict]:
    return {(i["series_id"], date.fromisoformat(i["obs_date"])): i for i in plan["items"]}


# ── 1. The arithmetic ────────────────────────────────────────────────────


def test_todays_values_times_the_basis_factor_are_the_values_on_that_sessions_basis(market):
    """settled(D) on L's basis = recompute_now(D) x RawClose(L) / AdjClose_now(L), every metric."""
    now = mod.settled_frame(mod.fetch_settled_bars(m.FakeYF(market), "SPY", date(2026, 1, 1), SESSION_DAY))
    for d, basis_session in [(date(2026, 5, 12), date(2026, 5, 12)),     # before both dividends
                             (date(2026, 5, 12), date(2026, 6, 1)),      # L after the first one
                             (date(2026, 6, 25), date(2026, 6, 25))]:    # after both
        asof = market.view(basis_session)
        dv = asof.adj * asof.vol
        direct = {"5d": dv.rolling(5).sum(), "20d": dv.rolling(20).sum()}
        direct["accel"] = direct["5d"].diff(5)
        factor = now.at[basis_session, "basis"]
        for metric in mod.METRICS:
            assert math.isclose(now.at[d, metric] * factor, direct[metric][pd.Timestamp(d)], rel_tol=1e-12)
    # and the dividend matters: on today's basis 05-12 is not what was stored then
    assert not math.isclose(now.at[date(2026, 5, 12), "5d"],
                            m.old_writer_values(market, date(2026, 5, 12), None)["5d"], rel_tol=1e-3)


@pytest.mark.parametrize(("pulled", "expected"), [
    (m.ny(date(2026, 5, 12), 14, 30), date(2026, 5, 12)),     # mid-session: that bar
    (m.ny(date(2026, 5, 12), 20), date(2026, 5, 12)),         # after the close
    (m.ny(date(2026, 5, 13), 4), date(2026, 5, 12)),          # before the next open
    (m.ny(date(2026, 5, 16), 10), date(2026, 5, 15)),         # a Saturday
    (datetime(2026, 11, 27, 20, tzinfo=timezone.utc), date(2026, 11, 26)),  # settled-bars writer, mid-session
])
def test_the_price_basis_is_the_newest_session_the_writing_run_saw(pulled, expected):
    sessions = [d.date() for d in pd.bdate_range("2026-05-01", "2026-11-30")]
    assert mod.price_basis_session(pulled, sessions) == expected


# ── 2. What is planned ───────────────────────────────────────────────────


def test_a_repaired_value_is_what_the_fixed_writer_stores_from_the_settled_bars(world, monkeypatch):
    """Parity with the real writer (#1496) run the morning after each date."""
    engine, market, kinds = world
    planned = _planned(_plan(engine, market))
    checked = 0
    for d in (date(2026, 4, 1), date(2026, 5, 15), date(2026, 6, 22), SHORT_NEXT_MORNING):
        fresh = s.make_sqlite_engine()
        m.install_yfinance(monkeypatch, m.FakeYF(market, asof=d))
        monkeypatch.setattr(inst, "_new_york_today", lambda d=d: market.next_session(d))
        inst.InstitutionalFlowsPuller(fresh).pull_etf_only(start_date=d - timedelta(days=10))
        for metric in mod.METRICS:
            sid = f"ETF_FLOW:SPY:{metric}"
            (writer_row,) = m.vintages(fresh, sid, d)
            assert math.isclose(planned[(sid, d)]["value"], writer_row["value"], rel_tol=1e-9), (d, metric)
            assert planned[(sid, d)]["volume"] == writer_row["payload"]["volume"]
            checked += 1
    assert checked == 12


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
    assert plan["summary"]["left_alone"] == {"within_tolerance": 6}
    assert plan["tickers"]["SPY"]["status"] == "OK"
    every = plan["items"][0]
    assert every["revises_value"] != every["value"] and every["revises_id"]
    assert plan["digest"] == mod._digest(plan["items"])


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
    assert mod.verify_plan(engine, plan) == {"run_id": plan["run_id"], "planned": len(plan["items"]),
                                             "applied": len(plan["items"]), "problems": [], "ok": True}


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
    plan["items"][0]["value"] *= 2
    path.write_text(json.dumps(plan))
    with pytest.raises(mod.Refused, match="edited"):
        mod.load_plan(path)


def test_a_read_for_an_instant_before_the_repair_still_sees_what_was_held_then(world):
    engine, market, _ = world
    plan = _plan(engine, market)
    mod.run_writes(engine, plan, session_day=SESSION_DAY)
    item = next(i for i in plan["items"] if i["metric"] == "5d")
    d = date.fromisoformat(item["obs_date"])
    with engine.connect() as conn:
        then = {o.obs_date: o.value for o in read_window_known_at(
            conn, "ETF_FLOW:SPY:5d", as_of=date(2026, 7, 10), lag=None, start=d)}
        now = {o.obs_date: o.value for o in read_window_known_at(
            conn, "ETF_FLOW:SPY:5d", as_of=date.today() + timedelta(days=1), lag=None, start=d)}
    assert then[d] == item["revises_value"]
    assert now[d] == item["value"]


def test_undo_appends_the_replaced_values_and_keeps_the_repair(world):
    engine, market, _ = world
    plan = _plan(engine, market)
    mod.run_writes(engine, plan, session_day=SESSION_DAY)
    result = mod.run_writes(engine, plan, mode="undo", session_day=SESSION_DAY)
    assert result["totals"] == {"written": len(plan["items"])}
    item = plan["items"][0]
    old, repair, undo = m.vintages(engine, item["series_id"], date.fromisoformat(item["obs_date"]))
    assert undo["value"] == old["value"] and repair["value"] == item["value"]
    assert undo["payload"]["repair_undo"]["undoes_id"] == repair["id"]
    assert "repair" not in undo["payload"]
    again = mod.run_writes(engine, plan, mode="undo", session_day=SESSION_DAY)
    assert again["totals"] == {"not_newest": len(plan["items"])}
    reapplied = mod.run_writes(engine, plan, session_day=SESSION_DAY)
    assert reapplied["totals"] == {"superseded": len(plan["items"])}      # an undo is not re-applied


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
    assert mod.main(["undo", "--plan", str(path)], engine=engine) == 0                    # dry: nothing yet
    assert m.row_count(engine) == before
    assert mod.main(["apply", "--plan", str(path), "--expect-digest", plan["digest"]], engine=engine) == 0
    assert m.row_count(engine) == before + plan["summary"]["rows"]
    assert mod.main(["verify", "--plan", str(path)], engine=engine) == 0
    assert '"ok": true' in capsys.readouterr().out
    assert (tmp_path / "plan.apply.progress.jsonl").exists()
