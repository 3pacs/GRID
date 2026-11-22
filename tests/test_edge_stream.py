"""Edge stream: insert-only registry, sanity gate, budget, fresh data, one look, no peeking."""

from __future__ import annotations

import json
import os
import stat
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pytest

from alpha_research.edge_stream import families as F
from alpha_research.edge_stream import hermes as H
from alpha_research.edge_stream import ledger as L
from alpha_research.edge_stream import policy as P
from alpha_research.edge_stream import runner as R
from alpha_research.edge_stream import template_queue as TQ
from alpha_research.edge_stream import sanity as S
from alpha_research.edge_stream import stats as ST
from alpha_research.edge_stream import data as D
from alpha_research.edge_stream.data import StaticSource, manifest_sha256

TODAY = date(2026, 11, 13)  # a Friday
NOW = datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc)


def synthetic_bars(tickers, start, end, effect, seed=7, price=200.0):
    """Sparse RTH bars at the minutes IM1 reads; ``effect`` = last-half-hour drift with the first half hour."""
    rng = np.random.default_rng(seed)
    out = {}
    for t in tickers:
        days, c = {}, price
        d = start
        while d < end:
            if d.weekday() < 5:
                r1 = rng.normal(0, 0.004)
                p1000 = c * (1 + r1)
                p1529 = p1000 * (1 + rng.normal(0, 0.004))
                p1559 = p1529 * (1 + effect * np.sign(r1) + rng.normal(0, 0.001))
                days[d.isoformat()] = [("09:30", c, c, c, c, 1.0), ("09:59", p1000, p1000, p1000, p1000, 1.0),
                                       ("15:29", p1529, p1529, p1529, p1529, 1.0),
                                       ("15:59", p1559, p1559, p1559, p1559, 1.0)]
                c = p1559
            d += timedelta(days=1)
        out[t] = days
    return out


def metals_source(effect, today=TODAY):
    return StaticSource(synthetic_bars(("GLD", "SLV"), date(2024, 11, 1), today, effect), today=today)


def metals_proposal(today=TODAY, **over):
    prop = F.build_proposal(dict(TQ.QUEUE[0]), today)
    prop.update(over)
    return prop


def register(led, prop, today=TODAY):
    report = S.check(prop, L.load_state(led), today)
    assert S.passed(report), report
    return L.append_registration(led, {**prop, "sanity": report, "recorded_at": f"{today.isoformat()}T12:00:00+00:00"})


# --- ledger -------------------------------------------------------------------------


def test_file_ledger_round_trip_and_tamper_refusal(tmp_path):
    led_path, anc_path = tmp_path / "l.jsonl", tmp_path / "a.jsonl"
    led = L.create(led_path, anc_path)
    sha = register(led, metals_proposal())
    again = L.open_ledger(led_path, anc_path)
    assert sha in L.load_state(again).registrations
    with pytest.raises(ValueError):
        L.create(led_path, anc_path)  # a second genesis would reset the count
    data = led_path.read_bytes()
    led_path.write_bytes(data.replace(b"GLD", b"GLX", 1))
    with pytest.raises(ValueError):
        L.open_ledger(led_path, anc_path)
    led_path.write_bytes(data.rsplit(b"\n", 2)[0] + b"\n")  # drop the last record
    with pytest.raises(ValueError):
        L.open_ledger(led_path, anc_path)


def test_policy_record_pins_the_policy():
    led = L.memory()
    rec = led.records[1]
    assert rec["kind"] == "edge_policy" and rec["policy_sha256"] == P.policy_sha256()
    assert rec["policy"]["budget"]["alpha_per_look"] == pytest.approx(0.025)
    assert all(r["promotion_allowed"] is False for r in led.records)


def test_one_look_then_spent_and_abandon_closes():
    led = L.memory()
    sha = register(led, metals_proposal())
    look = {"verdict": "FAIL", "trials": [{"trial_id": "pooled", "verdict": "FAIL"}], "data": {"window_end": "x"},
            "report_sha256": "0" * 64}
    L.append_look(led, sha, look)
    with pytest.raises(L.StreamRefused):
        L.append_look(led, sha, look)
    with pytest.raises(L.StreamRefused):
        L.append_abandoned(led, sha, "late")
    sha2 = register(led, F.build_proposal(dict(TQ.QUEUE[1]), TODAY))
    L.append_abandoned(led, sha2, "fetch failed")
    with pytest.raises(L.StreamRefused):
        L.append_look(led, sha2, look)
    assert L.load_state(led).stage1_count(2026) == 2  # abandoned registrations count


def test_history_window_cannot_be_reused_even_bypassing_the_gate():
    led = L.memory()
    register(led, metals_proposal())
    later = metals_proposal(today=TODAY + timedelta(days=3))
    later["proposal_id"] += ":again"
    with pytest.raises(L.StreamRefused, match="reuses consumed data"):
        L.append_registration(led, {**later, "sanity": {"G": "ok"}, "recorded_at": "2026-11-16T12:00:00+00:00"})
    assert "reuses consumed data" in S.check(later, L.load_state(led), TODAY + timedelta(days=3))["G7_fresh"]


def test_budget_exhaustion(monkeypatch):
    monkeypatch.setattr(P, "MAX_REGISTRATIONS_PER_YEAR", 1)
    led = L.memory()
    register(led, metals_proposal())
    nxt = F.build_proposal(dict(TQ.QUEUE[1]), TODAY)
    assert "budget" in S.check(nxt, L.load_state(led), TODAY)["G8_budget"]
    with pytest.raises(L.StreamRefused, match="budget"):
        L.append_registration(led, {**nxt, "sanity": {"G": "ok"}, "recorded_at": "2026-11-13T12:00:00+00:00"})


def test_prior_consumption_blocks_spent_spy_history():
    prop = F.build_proposal({"family": "IM1", "group": "us_equity_index", "instruments": ("SPY",),
                             "lane": "history"}, TODAY)
    assert S.check(prop, L.load_state(L.memory()), TODAY)["G7_fresh"].startswith("reuses consumed data")


# --- gate ---------------------------------------------------------------------------


@pytest.mark.parametrize("mutate,check", [
    (lambda p: p.update(mechanism=""), "G1_mechanism"),
    (lambda p: p.update(direction_rule={"rule": "15:30", "inputs": ["time_of_day"]}), "G2_direction"),
    (lambda p: p.update(sides="long"), "G3_both_sides"),
    (lambda p: p["costs"]["GLD"].update(bp=0.5), "G4_costs"),
    (lambda p: p.update(account="robinhood_agentic"), "G5_exit"),
    (lambda p: p.update(exit_kind="settlement", account="robinhood_agentic"), "G5_exit"),
    (lambda p: p.update(window={"start": "2026-11-02", "end": "2026-11-13"}), "G6_power"),
    (lambda p: p.update(instruments=["GLD", "SPY"]), "G7_fresh"),
    (lambda p: p.update(alpha_trial=1e-9), "G8_budget"),
    (lambda p: p.update(instruments=["ZZZZ"]), "G10_liquid"),
])
def test_gate_rejections(mutate, check):
    prop = metals_proposal()
    mutate(prop)
    report = S.check(prop, L.load_state(L.memory()), TODAY)
    assert report[check] != "ok"
    assert not S.passed(report)


def test_direction_probe_changes_with_state_not_clock():
    fam = F.FAMILIES["IM1"]
    a, b = fam.direction_probe()
    assert a["time_of_day"] == b["time_of_day"] and fam.direction(a) != fam.direction(b)


# --- harness and stats --------------------------------------------------------------


def test_lookahead_guard():
    bars = [("15:28", 1, 1, 1, 1, 1), ("15:29", 1, 1, 1, 1, 1), ("15:30", 9, 9, 9, 9, 1)]
    seen = F.decision_bars(bars, "15:30")
    assert [b[0] for b in seen] == ["15:28", "15:29"]
    F.assert_no_lookahead(seen, "15:30")
    with pytest.raises(F.LookaheadError):
        F.assert_no_lookahead(bars, "15:30")


def _trades(effect, n_sessions=400, seed=1, both=True, start=date(2025, 1, 2), effect_until=None):
    """One trade a day from ``start``; the planted effect stops after ``effect_until`` (a day), if given."""
    rng = np.random.default_rng(seed)
    out = []
    for i in range(n_sessions):
        day = (start + timedelta(days=i)).isoformat()
        side = (1 if rng.random() < 0.5 else -1) if both else 1
        planted = effect if effect_until is None or day <= effect_until else -effect / 4
        move = side * planted + rng.normal(0, 0.001)
        out.append({"session": day, "side": side, "net": side * move - 0.0003,
                    "always_long_net": move - 0.0003})
    return out


def test_pass_fail_underpowered():
    assert ST.judge(_trades(0.001), P.ALPHA_PER_LOOK)["verdict"] == "PASS"
    assert ST.judge(_trades(0.0), P.ALPHA_PER_LOOK)["verdict"] == "FAIL"
    assert ST.judge(_trades(0.001, n_sessions=60), P.ALPHA_PER_LOOK)["verdict"] == "UNDERPOWERED"
    one_sided = ST.judge(_trades(0.001, both=False), P.ALPHA_PER_LOOK)
    assert one_sided["verdict"] == "UNDERPOWERED" and "sides" in one_sided["reasons"][0]


def test_discovery_is_refitted_without_its_largest_year():
    # the effect holds in both years: the largest year goes and the rest still pays
    both_years = ST.judge(_trades(0.001, n_sessions=700), P.ALPHA_PER_LOOK, discovery=True)
    refit = both_years["largest_year_out"]
    assert both_years["verdict"] == "PASS" and refit["status"] == "survives"
    assert refit["years"] == ["2025", "2026"] and refit["n_left_out"] + refit["n_without"] == 700
    # the effect lives in 2025 only: it clears the test on the whole window, and is an episode
    trades = _trades(0.003, n_sessions=700, effect_until="2025-12-31")
    assert ST.judge(trades, P.ALPHA_PER_LOOK)["verdict"] == "PASS"  # a confirmation look does not refit
    one_year = ST.judge(trades, P.ALPHA_PER_LOOK, discovery=True)
    assert one_year["verdict"] == "EPISODE" and one_year["largest_year_out"]["year_left_out"] == "2025"
    assert one_year["largest_year_out"]["mean_net_without"] < 0 and "2025 left out" in one_year["reasons"][0]
    # a window inside one calendar year has nothing to refit on: recorded, and the verdict stands
    single = ST.judge(_trades(0.001, n_sessions=300), P.ALPHA_PER_LOOK, discovery=True)
    assert single["verdict"] == "PASS" and single["largest_year_out"]["status"] == "not_possible"
    assert single["largest_year_out"]["n_without"] == 0
    # a trial that fails is not refitted at all
    assert "largest_year_out" not in ST.judge(_trades(0.0), P.ALPHA_PER_LOOK, discovery=True)


def test_noise_is_measured_per_day_or_week_and_a_finer_unit_is_refused():
    rows = _trades(0.001, n_sessions=120)
    weekly = [{**t, "session": F.iso_week(t["session"])} for t in rows]
    assert ST.judge(weekly, P.ALPHA_PER_LOOK)["sessions"] < 120  # days of one week are resampled together
    with pytest.raises(ValueError, match="a day or an ISO week"):
        ST.judge([{**t, "session": t["session"] + "T15:30"} for t in rows], P.ALPHA_PER_LOOK)
    with pytest.raises(ValueError, match="a day or an ISO week"):
        ST.judge([{**t, "session": f"s{i}"} for i, t in enumerate(rows)], P.ALPHA_PER_LOOK)


def test_bootstrap_is_seeded_and_resolvable():
    vals = np.random.default_rng(3).normal(0, 1, 300)
    sess = [f"d{i}" for i in range(300)]
    assert ST.session_bootstrap_lower(sess, vals, 0.01) == ST.session_bootstrap_lower(sess, vals, 0.01)
    with pytest.raises(ValueError):
        ST.bootstrap_b(1e-7)


# --- cycles -------------------------------------------------------------------------


def test_cycle_registers_then_looks_then_confirms(tmp_path):
    led = L.memory()
    src = metals_source(0.002)
    s1 = R.run_cycle(led, src, tmp_path, now=NOW)
    assert [r["proposal_id"] for r in s1["registered"]] == ["IM1-v1:metals:history:2024-12-13"]
    assert s1["looks"] == []  # the look waits for the next cycle: sealed first, read after
    s2 = R.run_cycle(led, src, tmp_path, now=NOW + timedelta(days=3))
    assert s2["looks"][0]["verdict"] == "PASS"
    state = L.load_state(led)
    reg_sha = next(iter(state.looks))
    report = tmp_path / "looks" / f"{reg_sha[:16]}.json"
    assert not os.stat(report).st_mode & stat.S_IWUSR
    body = json.loads(report.read_bytes())
    assert body["trial"]["n_long"] >= 50 and body["trial"]["n_short"] >= 50
    confirm = [r for r in state.registrations.values() if r["stage"] == 2]
    assert len(confirm) == 1 and confirm[0]["lane"] == "forward" and confirm[0]["confirms"] == reg_sha
    assert confirm[0]["alpha_trial"] == pytest.approx(0.05)
    nxt = [r for r in state.registrations.values() if r["stage"] == 1 and r["group"] == "energy"]
    assert len(nxt) == 1  # one new stage-1 registration on the next weekday
    status = json.loads((tmp_path / "status.json").read_bytes())
    assert status["verdicts"]["PASS"] == 1 and status["candidates"] == []  # unconfirmed: not a candidate
    assert "Edge stream digest" in (tmp_path / "DIGEST.md").read_text()


def test_null_effect_fails(tmp_path):
    led = L.memory()
    src = metals_source(0.0)
    R.run_cycle(led, src, tmp_path, now=NOW)
    s2 = R.run_cycle(led, src, tmp_path, now=NOW + timedelta(days=3))
    assert s2["looks"][0]["verdict"] == "FAIL"
    assert not [r for r in L.load_state(led).registrations.values() if r["stage"] == 2]


def test_crash_replay_same_bytes_then_different_bytes(tmp_path):
    led = L.memory()
    src = metals_source(0.002)
    R.run_cycle(led, src, tmp_path, now=NOW, register=True)
    reg_sha, reg = L.load_state(led).open_registrations()[0]
    _, body = R.compute_look(reg_sha, reg, src, TODAY + timedelta(days=3))
    R._write_report(tmp_path, reg_sha, body)  # crash after the report, before the record
    R.run_cycle(led, src, tmp_path, now=NOW + timedelta(days=3), register=False)
    assert L.load_state(led).looks[reg_sha]["report_sha256"] == L.sha256_bytes(body)

    led2 = L.memory()
    d2 = tmp_path / "second"
    R.run_cycle(led2, src, d2, now=NOW)
    reg_sha2, _ = L.load_state(led2).open_registrations()[0]
    (d2 / "looks").mkdir()
    (d2 / "looks" / f"{reg_sha2[:16]}.json").write_bytes(b"{}")
    s = R.run_cycle(led2, src, d2, now=NOW + timedelta(days=3), register=False)
    assert s["abandoned"][0]["reason"] == "nondeterministic"
    assert reg_sha2 in L.load_state(led2).abandoned


def test_forward_never_peeks_before_its_look(tmp_path, monkeypatch):
    led = L.memory()
    item = dict(next(q for q in TQ.QUEUE if q["lane"] == "forward"))
    prop = F.build_proposal(item, TODAY)
    prop["window"]["n_target_sessions"] = 30
    sha = register(led, prop)
    later = TODAY + timedelta(days=20)
    src = StaticSource(synthetic_bars(("SPY", "QQQ", "IWM"), TODAY, later, 0.002), today=later)

    def boom(*a, **k):
        raise AssertionError("an outcome was computed before the look")

    monkeypatch.setattr(F.IntradayMomentum, "session_trade", boom)
    s = R.run_cycle(led, src, tmp_path, now=datetime(2026, 12, 3, 12, tzinfo=timezone.utc), register=False)
    assert s["forward"][0]["ready"] is False and sha not in L.load_state(led).looks


def test_dry_run_writes_nothing(tmp_path):
    led = L.memory()
    before = len(led.records)
    s = R.run_cycle(led, metals_source(0.002), tmp_path, now=NOW, dry_run=True)
    assert s["registered"][0]["would_register"].startswith("IM1-v1:metals")
    assert s["registered"][0]["preflight_sessions_with_bars"]["GLD"] > 400
    assert len(led.records) == before and not (tmp_path / "status.json").exists()


def test_code_change_abandons_instead_of_running_other_code(tmp_path, monkeypatch):
    led = L.memory()
    src = metals_source(0.002)
    R.run_cycle(led, src, tmp_path, now=NOW)
    monkeypatch.setattr(R, "code_sha256", lambda fam: "f" * 64)
    s = R.run_cycle(led, src, tmp_path, now=NOW + timedelta(days=3), register=False)
    assert "code differs" in s["abandoned"][0]["reason"]


def test_manifest_hash_is_order_free():
    a = {"X": {"2026-01-02": [("09:59", 1.0, 1.0, 1.0, 1.0, 1.0)], "2026-01-01": []}}
    b = {"X": {"2026-01-01": [], "2026-01-02": [("09:59", 1.0, 1.0, 1.0, 1.0, 1.0)]}}
    assert manifest_sha256(a) == manifest_sha256(b)


# --- hermes -------------------------------------------------------------------------


def test_hermes_spawn_gating(tmp_path):
    calls = []

    class Proc:
        pid = 42

    def popen(cmd, **kw):
        calls.append((cmd, kw))
        return Proc()

    env = {"EDGE_STREAM_LEDGER_PATH": "l", "EDGE_STREAM_ANCHOR_PATH": "a", "EDGE_STREAM_DATA_DIR": str(tmp_path)}
    assert H.maybe_spawn_cycle(NOW, env={}, popen=popen) is None
    assert H.maybe_spawn_cycle(NOW.replace(hour=9), env=env, popen=popen) is None
    out = H.maybe_spawn_cycle(NOW, env=env, popen=popen)
    assert out["spawned"] and calls[0][1]["start_new_session"] is True
    assert calls[0][0][-2:] == ["alpha_research.edge_stream", "cycle"]
    assert calls[0][1]["env"]["OMP_NUM_THREADS"] == "2"
    assert H.maybe_spawn_cycle(NOW.replace(hour=20), env=env, popen=popen) is None  # once a day
    assert len(calls) == 1


def test_hermes_operator_calls_the_hook_first_in_intel_step():
    src = open(os.path.join(os.path.dirname(__file__), "..", "scripts", "hermes_operator.py")).read()
    body = src.split("def run_intelligence_tasks(", 1)[1]
    assert body.index("maybe_spawn_cycle") < body.index("earnings_events_to_calendar")


# --- review fixes (2026-11-13) ------------------------------------------------------


def _polygon(tmp_path, now, results_by_day):
    src = D.PolygonMinuteSource(tmp_path / "bars.sqlite3", api_key="x", gap_s=0, now=now)

    def fake_get(path):
        out = []
        for day, closes in results_by_day.items():
            for hm, c in closes:
                ts = datetime.fromisoformat(f"{day}T{hm}:00").replace(tzinfo=D.ET)
                out.append({"t": int(ts.timestamp() * 1000), "o": c, "h": c, "l": c, "c": c, "v": 1})
        return {"results": out}

    src._get = fake_get
    return src


def test_unpublished_recent_day_is_not_cached_empty(tmp_path):
    now = datetime(2026, 11, 13, 12, tzinfo=timezone.utc)
    src = _polygon(tmp_path, now, {"2026-11-10": [("09:59", 1.0)]})  # 11-11 and 11-12 unpublished
    got = src.sessions("GLD", date(2026, 11, 9), date(2026, 11, 13))
    assert list(got) == ["2026-11-10"]
    cached = src._cached("GLD", date(2026, 11, 9), date(2026, 11, 13))
    assert "2026-11-12" not in cached and "2026-11-11" not in cached  # retried next cycle
    assert cached["2026-11-09"] == []  # settled (4 days old) empty weekday
    with pytest.raises(D.SourceError, match="neither served nor settled"):
        src.sessions("GLD", date(2026, 11, 9), date(2026, 11, 13), require_complete=True)


def test_request_outside_retention_is_refused(tmp_path):
    now = datetime(2026, 11, 13, 12, tzinfo=timezone.utc)
    src = _polygon(tmp_path, now, {})
    with pytest.raises(D.SourceError, match="retention"):
        src.sessions("GLD", date(2024, 11, 1), date(2024, 11, 20))


def test_split_sized_gap_skips_the_session():
    fam = F.FAMILIES["IM1"]
    prev = [("15:59", 100.0, 100.0, 100.0, 100.0, 1.0)]
    day = [("09:59", 50.2, 0, 0, 50.2, 1.0), ("15:29", 50.0, 0, 0, 50.0, 1.0), ("15:59", 50.1, 0, 0, 50.1, 1.0)]
    assert fam.session_trade(prev, day, "NVDA", "2026-06-10") is None
    ok = [("09:59", 100.4, 0, 0, 100.4, 1.0), ("15:29", 100.0, 0, 0, 100.0, 1.0), ("15:59", 100.3, 0, 0, 100.3, 1.0)]
    assert fam.session_trade(prev, ok, "NVDA", "2026-06-10")["side"] == 1


def test_code_hash_ignores_docstrings_not_code(tmp_path):
    src = "def f(x):\n    \"\"\"doc\"\"\"\n    return x + 1\n"
    assert F._ast_digest(src) == F._ast_digest(src.replace("doc", "other words"))
    assert F._ast_digest(src) != F._ast_digest(src.replace("x + 1", "x + 2"))
    assert len(F.code_sha256(F.FAMILIES["IM1"])) == 64


def test_same_family_forward_twice_on_a_group_is_refused():
    led = L.memory()
    forward = next(q for q in TQ.QUEUE if q["lane"] == "forward")
    register(led, F.build_proposal(dict(forward), TODAY))
    again = F.build_proposal(dict(forward), TODAY + timedelta(days=3))
    assert "reuses consumed data" in S.check(again, L.load_state(led), TODAY + timedelta(days=3))["G7_fresh"]


def test_malformed_inbox_proposal_is_rejected_not_fatal(tmp_path):
    (tmp_path / "inbox").mkdir()
    (tmp_path / "inbox" / "bad.json").write_text(json.dumps(
        {"family": "IM1", "group": "metals", "instruments": ["GLD", "GLD"], "lane": "bogus"}))
    (tmp_path / "inbox" / "junk.json").write_text("{not json")
    led = L.memory()
    for q in TQ.QUEUE:  # mark the template queue done so the inbox is reached
        prop = F.build_proposal(dict(q), TODAY)
        led_state = L.load_state(led)
        if S.passed(S.check(prop, led_state, TODAY)):
            L.append_registration(led, {**prop, "sanity": S.check(prop, led_state, TODAY),
                                        "recorded_at": "2026-11-13T12:00:00+00:00"})
    s = R.run_cycle(led, StaticSource({}, today=TODAY + timedelta(days=3)), tmp_path,
                    now=NOW + timedelta(days=3))
    assert any("invalid" in r for r in s["rejected"])
    assert any(r.get("failed", {}).get("G0_wellformed", "").startswith("malformed") for r in s["rejected"])
    assert (tmp_path / "status.json").exists()


def test_request_cap_is_not_counted_against_a_registration(tmp_path):
    led = L.memory()
    sha = register(led, metals_proposal())
    reg = L.load_state(led).registrations[sha]
    for i in range(8):
        out = R._attempt_failed(led, tmp_path, sha, reg, D.RequestCapReached("cap"), NOW + timedelta(days=i), False)
        assert out.startswith("deferred")
    assert sha not in L.load_state(led).abandoned
    for i in range(P.FETCH_ATTEMPTS_BEFORE_ABANDON):
        R._attempt_failed(led, tmp_path, sha, reg, D.SourceError("down"), NOW + timedelta(days=i), False)
    assert sha in L.load_state(led).abandoned


def test_stale_readonly_tmp_does_not_jam_the_report(tmp_path):
    (tmp_path / "looks").mkdir()
    tmp = tmp_path / "looks" / ("a" * 16 + ".tmp")
    tmp.write_bytes(b"old")
    os.chmod(tmp, 0o444)
    path, same = R._write_report(tmp_path, "a" * 64, b"{}")
    assert same and path.read_bytes() == b"{}"


def test_clustered_t_floor_matches_the_textbook_bound_and_gates_the_verdict():
    from scipy.stats import t as student_t

    vals = np.random.default_rng(5).normal(0.0002, 0.001, 200)
    sess = [f"s{i:03d}" for i in range(200)]  # one trade per session: plain one-sample t
    want = vals.mean() - student_t.ppf(1 - 0.01, 199) * vals.std(ddof=1) / np.sqrt(200)
    assert ST.session_t_lower(sess, vals, 0.01) == pytest.approx(want, rel=1e-9)
    out = ST.judge(_trades(0.001), P.ALPHA_PER_LOOK)
    assert out["verdict"] == "PASS" and out["t_lower"] > 0


# --- G11 cost feasibility -------------------------------------------------------------


@pytest.fixture
def g11_source():
    """Set the G11 bar source for one test and always clear it (it is module state)."""
    def use(src):
        S.set_feasibility_source(src)
        return src
    yield use
    S.set_feasibility_source(None)


class _Recording(StaticSource):
    def __init__(self, bars, today):
        super().__init__(bars, today=today)
        self.calls = []

    def sessions(self, ticker, start, end, require_complete=False):
        self.calls.append((ticker, start, end))
        return super().sessions(ticker, start, end, require_complete)


def _two_regime_bars(pre_effect, window_effect, window_start):
    """Pre-window sessions drift by ``pre_effect``, window sessions by ``window_effect``."""
    pre = synthetic_bars(("GLD", "SLV"), date(2024, 11, 1), window_start, pre_effect, seed=3)
    win = synthetic_bars(("GLD", "SLV"), window_start, TODAY, window_effect, seed=4)
    return {t: {**pre[t], **win[t]} for t in pre}


def test_g11_refuses_when_cost_swamps_the_move(g11_source):
    prop = metals_proposal()
    g11_source(metals_source(0.0))  # |move| ~8 bp vs a 3 bp round trip: break-even ~0.69
    r = S.check(prop, L.load_state(L.memory()), TODAY)
    assert r["G11_cost_feasible"].startswith("pooled break-even hit rate")
    assert "GLD" in r["G11_cost_feasible"] and not S.passed(r)


def test_g11_passes_when_the_move_dwarfs_the_cost(g11_source):
    g11_source(metals_source(0.004))
    r = S.check(metals_proposal(), L.load_state(L.memory()), TODAY)
    assert r["G11_cost_feasible"] == "ok" and S.passed(r)


def test_g11_judges_the_pooled_trial_not_each_instrument(g11_source):
    bars = synthetic_bars(("GLD",), date(2024, 11, 1), TODAY, 0.0)               # p* ~0.75 alone
    bars.update(synthetic_bars(("SLV",), date(2024, 11, 1), TODAY, 0.01, seed=5))  # p* ~0.51 alone
    g11_source(StaticSource(bars, today=TODAY))
    prop = metals_proposal()
    m = S.breakeven_hits(prop, S._feasibility_source, TODAY)
    assert m["GLD"]["breakeven_hit"] > S.MAX_BREAKEVEN_HIT >= m["pooled"]["breakeven_hit"]
    assert S.check(prop, L.load_state(L.memory()), TODAY)["G11_cost_feasible"] == "ok"


def test_g11_reads_only_sessions_before_the_window(g11_source):
    prop = metals_proposal()
    start = date.fromisoformat(prop["window"]["start"])
    # cheap-to-trade window, expensive pre-window: only the pre-window may decide
    src = g11_source(_Recording(_two_regime_bars(0.0, 0.01, start), TODAY))
    r = S.check(prop, L.load_state(L.memory()), TODAY)
    assert r["G11_cost_feasible"] != "ok"
    assert src.calls and all(end <= start for _, _, end in src.calls)
    m = S.breakeven_hits(prop, src, TODAY)
    assert all(v["to"] < start.isoformat() for v in m.values())
    assert m["pooled"]["n"] == m["GLD"]["n"] + m["SLV"]["n"] >= S.PREWINDOW_MIN_TRADES


def test_g11_stays_inside_polygon_retention(g11_source):
    prop = metals_proposal()
    src = g11_source(_Recording(metals_source(0.004).bars, TODAY))
    S.check(prop, L.load_state(L.memory()), TODAY)
    assert min(s for _, s, _ in src.calls) >= TODAY - timedelta(days=D.RETENTION_DAYS - 1)


def test_g11_not_run_without_a_source_or_for_a_confirmation(g11_source):
    assert "G11_cost_feasible" not in S.check(metals_proposal(), L.load_state(L.memory()), TODAY)
    g11_source(metals_source(0.0))
    assert "G11_cost_feasible" not in S.check(metals_proposal(stage=2), L.load_state(L.memory()), TODAY)


def test_g11_skipped_when_an_earlier_check_already_failed(g11_source):
    src = g11_source(_Recording({}, TODAY))
    r = S.check(metals_proposal(sides="long"), L.load_state(L.memory()), TODAY)
    assert "G11_cost_feasible" not in r and not src.calls


def test_g11_too_few_sessions_and_source_errors_refuse(g11_source):
    g11_source(StaticSource({}, today=TODAY))
    assert S.check(metals_proposal(), L.load_state(L.memory()), TODAY)["G11_cost_feasible"] == \
        "fewer than 10 pre-window trades (0)"

    class Down:
        def sessions(self, *a, **k):
            raise D.SourceError("polygon 503")
    g11_source(Down())
    assert S.check(metals_proposal(), L.load_state(L.memory()), TODAY)["G11_cost_feasible"] == "bar source: polygon 503"


def test_g11_refusal_lets_the_next_queue_item_register(tmp_path, g11_source):
    bars = synthetic_bars(("GLD", "SLV"), date(2024, 11, 1), TODAY, 0.0)
    bars.update(synthetic_bars(("USO", "UNG"), date(2024, 11, 1), TODAY, 0.004, seed=9))
    src = g11_source(StaticSource(bars, today=TODAY))
    s = R.run_cycle(L.memory(), src, tmp_path, now=NOW)
    assert s["rejected"][0]["proposal_id"].startswith("IM1-v1:metals")
    assert "G11_cost_feasible" in s["rejected"][0]["failed"]
    assert s["registered"][0]["proposal_id"].startswith("IM1-v1:energy")


def test_g11_does_not_change_the_code_hash():
    assert "sanity" not in F.EVALUATION_MODULES  # installing the gate abandons no open registration


def test_cli_cycle_sets_and_clears_the_g11_source(tmp_path, monkeypatch):
    from alpha_research.edge_stream import __main__ as CLI

    seen = {}

    class FakeSource:
        def __init__(self, *a, **k):
            pass

    def fake_cycle(ledger, source, data, **kw):
        seen["source"] = S._feasibility_source
        seen["passed"] = source
        return {}

    monkeypatch.setattr(CLI, "PolygonMinuteSource", FakeSource)
    monkeypatch.setattr(CLI, "_extras", lambda source, data: {})  # the event families' sources (EC1)
    monkeypatch.setattr(CLI, "run_cycle", fake_cycle)
    for k in ("EDGE_STREAM_LEDGER_PATH", "EDGE_STREAM_ANCHOR_PATH"):
        monkeypatch.delenv(k, raising=False)
    assert CLI.main(["cycle", "--dry-run", "--data-dir", str(tmp_path)]) == 0
    assert isinstance(seen["source"], FakeSource) and seen["source"] is seen["passed"]
    assert S._feasibility_source is None


# --- per-family hash, unhashed queue, unread abandonments (policy 4) ----------------


def _sources():
    here = Path(F.__file__).resolve().parent
    return {m: (here / f"{m}.py").read_text() for m in F.EVALUATION_MODULES}


def _edit(src: str, old: str, new: str) -> str:
    assert src.count(old) == 1, old
    return src.replace(old, new)


def test_a_family_hash_ignores_the_other_families_and_the_registry(monkeypatch):
    im1, ec1 = F.FAMILIES["IM1"], F.FAMILIES["EC1"]
    base = _sources()
    assert F.code_sha256(im1, base) == F.code_sha256(im1)
    fam = base["families"]
    added = fam.replace("FAMILIES: dict[str, object] = {",
                        "@dataclass(frozen=True)\nclass NewFamily:\n    id: str = 'NF1'\n    version: int = 1\n\n\n"
                        "FAMILIES: dict[str, object] = {'NF1': NewFamily(), ")
    ec1_changed = _edit(fam, "hold_sessions: int = 15", "hold_sessions: int = 16")
    im1_changed = _edit(fam, 'id: str = "IM1"', 'id: str = "IM1"\n    extra: int = 1')
    shared_changed = _edit(fam, "return [b for b in bars if _plus1(b[0]) <= decision_hm]",
                           "return [b for b in bars if _plus1(b[0]) < decision_hm]")
    h = lambda fm, s: F.code_sha256(fm, {**base, "families": s})  # noqa: E731
    # adding a family, or changing another one, leaves IM1's open registrations alone
    assert h(im1, fam) == h(im1, ec1_changed)
    new_family = type("NewFamily", (), {"id": "NF1", "version": 1})()
    with monkeypatch.context() as m:  # the deployed code: the new class is in the registry too
        m.setattr(F, "FAMILIES", {**F.FAMILIES, "NF1": new_family})
        assert h(im1, added) == h(im1, fam)
    assert h(ec1, im1_changed) == h(ec1, fam)
    # its own class, or code every family shares, still abandons
    assert h(im1, im1_changed) != h(im1, fam)
    assert h(ec1, ec1_changed) != h(ec1, fam)
    assert h(im1, shared_changed) != h(im1, fam) and h(ec1, shared_changed) != h(ec1, fam)
    # the other evaluation modules are shared by every family
    stats_changed = {**base, "stats": base["stats"] + "\nX_NEW = 1\n"}
    assert F.code_sha256(im1, stats_changed) != F.code_sha256(im1, base)


def test_the_queue_is_not_hashed():
    assert "template_queue" not in F.EVALUATION_MODULES
    assert not hasattr(F, "QUEUE") and R.QUEUE is TQ.QUEUE


def _v3_payload():
    pol = json.loads(P.canonical_bytes(P.policy_payload()))
    pol.pop("unread_abandonment")
    pol["owner_decisions"] = pol["owner_decisions"][:1]
    pol["policy_version"] = "edge-stream-policy/3"
    return pol


def _v3_ledger(monkeypatch):
    old = _v3_payload()
    monkeypatch.setattr(P, "policy_payload", lambda: old)
    return L.memory("2026-11-13T06:27:00+00:00")


def _forward_item():
    return dict(next(q for q in TQ.QUEUE if q["lane"] == "forward"))


class _NoReads(StaticSource):
    def sessions(self, *a, **k):
        raise AssertionError("a window was read")


def test_code_change_abandons_a_forward_registration_at_once(tmp_path, monkeypatch):
    """A forward registration is checked before its sessions are counted, not only when it is due."""
    led = L.memory()
    sha = register(led, F.build_proposal(_forward_item(), TODAY))
    monkeypatch.setattr(R, "code_sha256", lambda fam: "f" * 64)
    s = R.run_cycle(led, _NoReads({}, today=TODAY), tmp_path, now=NOW + timedelta(days=3), register=False)
    assert s["abandoned"][0]["registration"] == sha[:16]
    state = L.load_state(led)
    rec = state.abandoned[sha]
    assert rec["cause"] == L.CODE_CHANGED and rec["read_evidence"]["ledger_reads"] == []
    # abandoned before its first presence count: nothing of it was read, so it is unread like history
    assert state.unread_abandoned(sha) and state.reregistrable(sha)
    assert not any(c["source"] == sha for c in state.consumption())


def test_forward_progress_then_code_change_keeps_the_window_and_the_group(tmp_path, monkeypatch):
    """Codex 6010600452 / 6011669661: two cycles of forward progress, then a code change. A presence
    count is a read: the window stays consumed, the alpha spent, and the group closed (no re-entry)."""
    led = L.memory()
    sha = register(led, F.build_proposal(_forward_item(), TODAY))
    alpha = L.load_state(led).registrations[sha]["alpha_look"]
    for k in (3, 4):
        day = TODAY + timedelta(days=k)
        src = StaticSource(synthetic_bars(("SPY", "QQQ", "IWM"), TODAY, day, 0.002), today=day)
        s = R.run_cycle(led, src, tmp_path, now=NOW + timedelta(days=k), register=False)
        assert s["forward"][0]["ready"] is False
    with monkeypatch.context() as m:
        m.setattr(R, "code_sha256", lambda fam: "f" * 64)
        R.run_cycle(led, _NoReads({}, today=TODAY), tmp_path, now=NOW + timedelta(days=5), register=False)
    state = L.load_state(led)
    ev = state.abandoned[sha]["read_evidence"]
    assert ev["ledger_reads"] == ["progress"] and state.reads[sha] == {"progress"} and ev["report_staged"] is False
    assert sum(1 for r in led.records if r.get("kind") == L.READ_KIND) == 1  # first read of a kind only
    assert not state.unread_abandoned(sha) and not state.reregistrable(sha)
    assert any(c["source"] == sha for c in state.consumption())
    assert state.abandoned[sha]["alpha_spent"] == alpha and state.stage1_count(2026) == 1
    nxt = NOW + timedelta(days=6)
    monkeypatch.setattr(R, "QUEUE", (_forward_item(),))
    s = R.run_cycle(led, StaticSource({}, today=nxt.date()), tmp_path, now=nxt)
    assert s["registered"] == [] and s["rejected"] == []  # the group is done: not even proposed
    assert len(L.load_state(led).registrations) == 1


@pytest.mark.parametrize("trace", ["staged_report", "aborted_attempt", "look_read_only"])
def test_any_trace_of_a_look_keeps_the_window_and_the_group(tmp_path, monkeypatch, trace):
    """Codex 6010600452: a staged report (crash after the report, before append_look), a failed
    attempt, or a look read with nothing after it (crash mid-look) each keep the window consumed and
    the group closed after a code change."""
    led = L.memory()
    sha = register(led, metals_proposal())
    if trace == "staged_report":
        (tmp_path / "looks").mkdir()
        (tmp_path / "looks" / f"{sha[:16]}.json").write_bytes(b"{}")
    elif trace == "aborted_attempt":
        L.append_read(led, sha, "look", NOW.isoformat())
        R._append_jsonl(tmp_path / "attempts.jsonl", {"registration_sha256": sha, "at": NOW.isoformat(),
                                                     "error": "SourceError: x"})
    else:
        L.append_read(led, sha, "look", NOW.isoformat())
    with monkeypatch.context() as m:
        m.setattr(R, "code_sha256", lambda fam: "f" * 64)
        R.run_cycle(led, _NoReads({}, today=TODAY), tmp_path, now=NOW + timedelta(days=3), register=False)
    state = L.load_state(led)
    assert state.abandoned[sha]["cause"] == L.CODE_CHANGED
    assert not state.unread_abandoned(sha) and not state.reregistrable(sha)
    assert any(c["source"] == sha for c in state.consumption())
    later = NOW + timedelta(days=4)
    s = R.run_cycle(led, metals_source(0.002, later.date()), tmp_path, now=later)
    assert not any(r["proposal_id"].startswith("IM1-v1:metals") for r in s["registered"])


def test_a_look_read_is_marked_before_the_data_is_read(tmp_path):
    led = L.memory()
    sha = register(led, metals_proposal())
    seen = []

    class Watch(StaticSource):
        def sessions(self, *a, **k):
            seen.append(set(L.load_state(led).reads.get(sha, ())))
            return super().sessions(*a, **k)

    src = Watch(synthetic_bars(("GLD", "SLV"), date(2024, 11, 1), TODAY, 0.002), today=TODAY)
    R.run_cycle(led, src, tmp_path, now=NOW + timedelta(days=3), register=False)
    assert seen and all(k == {"look"} for k in seen)


def test_a_dry_run_reads_no_open_window_and_writes_nothing(tmp_path):
    led = L.memory()
    register(led, metals_proposal())
    register(led, F.build_proposal(_forward_item(), TODAY))
    n = len(led.records)
    s = R.run_cycle(led, _NoReads({}, today=TODAY), tmp_path, now=NOW + timedelta(days=3), dry_run=True,
                    register=False)
    assert all(x.get("dry_run") == "window not read" for x in s["looks"] + s["forward"])
    assert len(led.records) == n and not L.load_state(led).reads


def test_unread_abandonment_frees_its_window_under_policy_4(tmp_path, monkeypatch):
    led = L.memory()  # sealed under policy 4: every cycle of its life marks reads
    src = metals_source(0.002)
    R.run_cycle(led, src, tmp_path, now=NOW)
    reg_sha, = L.load_state(led).registrations
    with monkeypatch.context() as m:
        m.setattr(R, "code_sha256", lambda fam: "f" * 64)
        R.run_cycle(led, src, tmp_path, now=NOW + timedelta(days=3), register=False)
    state = L.load_state(led)
    assert reg_sha in state.marked and state.unread_abandoned(reg_sha)
    assert not any(c["source"] == reg_sha for c in state.consumption())
    later = NOW + timedelta(days=4)
    s = R.run_cycle(led, metals_source(0.002, today=later.date()), tmp_path, now=later)
    assert s["registered"][0]["proposal_id"].startswith("IM1-v1:metals:history:")
    state = L.load_state(led)
    assert state.stage1_count(2026) == 2 and state.abandoned[reg_sha]["alpha_spent"] > 0


def test_a_registration_sealed_before_policy_4_needs_an_attestation(tmp_path, monkeypatch):
    """Its earlier cycles ran code that marked no reads, so clean-looking evidence proves nothing."""
    with monkeypatch.context() as m:
        led = _v3_ledger(m)
        src = metals_source(0.002)
        R.run_cycle(led, src, tmp_path, now=NOW)
        reg_sha, = L.load_state(led).registrations
        m.setattr(R, "code_sha256", lambda fam: "f" * 64)
        R.run_cycle(led, src, tmp_path, now=NOW + timedelta(days=3), register=False)
        state = L.load_state(led)
        assert not state.reopens_unread() and any(c["source"] == reg_sha for c in state.consumption())
    later = NOW + timedelta(days=4)  # the cycle amends to policy 4; the window stays consumed
    s = R.run_cycle(led, metals_source(0.002, today=later.date()), tmp_path, now=later)
    state = L.load_state(led)
    assert s["policy_amended"] and state.reopens_unread() and reg_sha not in state.marked
    assert not state.unread_abandoned(reg_sha) and not state.reregistrable(reg_sha)
    assert any(c["source"] == reg_sha for c in state.consumption())
    assert not any(r["proposal_id"].startswith("IM1-v1:metals") for r in s["registered"])


def test_other_abandonments_keep_their_window(tmp_path, monkeypatch):
    led = L.memory()
    sha = register(led, metals_proposal())
    L.append_abandoned(led, sha, "look failed on 5 day(s); last: SourceError: x")
    state = L.load_state(led)
    assert state.reopens_unread() and not state.unread_abandoned(sha)
    assert any(c["source"] == sha for c in state.consumption())
    s = R.run_cycle(led, metals_source(0.002, TODAY + timedelta(days=3)), tmp_path, now=NOW + timedelta(days=3))
    assert not any(r["proposal_id"].startswith("IM1-v1:metals") for r in s["registered"])


def test_no_registration_sealed_before_policy_4_is_freed_or_reopened():
    """Codex 6010864065 / 6011669661: without an attestation nothing sealed before policy 4 is freed;
    the live SPY/QQQ/IWM forward registration is named by nothing and stays closed."""
    assert [a["lane"] for a in P.UNREAD_ABANDONMENT["attested"]] == ["history"]
    live_ss = "cfa2537678d7af7153e767810fda3d87a9316c45687d9cc2747e54a1fd0b224b"
    live_fwd = "69a8482023b8306323ac5d18d5e70e5af2451fa2846912f24a1990656660218d"
    pol = json.loads(P.canonical_bytes(P.policy_payload()))
    pol["unread_abandonment"]["attested"] = []
    state = L.StreamState(policy=pol, policy_sha256="x")
    state.registrations[live_ss] = {"lane": "history", "stage": 1}
    state.abandoned[live_ss] = {"reason": "family IM1 code differs from the registered sha"}
    clean = {"ledger_reads": [], "failed_attempts": 0, "report_staged": False}
    state.registrations[live_fwd] = {"lane": "forward", "stage": 1}
    state.abandoned[live_fwd] = {"reason": "x", "cause": L.CODE_CHANGED, "read_evidence": clean}
    for sha in (live_ss, live_fwd):
        assert not state.unread_abandoned(sha) and not state.reregistrable(sha)


LIVE_SS = "cfa2537678d7af7153e767810fda3d87a9316c45687d9cc2747e54a1fd0b224b"


def _live_ss_state(policy=None) -> L.StreamState:
    pol = policy or P.policy_payload()
    state = L.StreamState(policy=pol, policy_sha256="x")
    state.registrations[LIVE_SS] = {"lane": "history", "stage": 1, "group": "us_single_stocks",
                                    "horizon": "intraday", "window": {"start": "2024-12-19", "end": "2026-11-19"},
                                    "proposal_id": "IM1-v1:us_single_stocks:history:2024-12-19"}
    state.abandoned[LIVE_SS] = {"reason": "family IM1 code differs from the registered sha"}
    return state


def test_policy_5_frees_single_stocks_except_the_sessions_read():
    """The receipt's four read sessions stay consumed; the rest of the window is freed."""
    state = _live_ss_state()
    assert state.unread_abandoned(LIVE_SS) and state.reregistrable(LIVE_SS)
    kept = [c for c in state.consumption() if c["source"] == LIVE_SS]
    assert [(c["start"], c["end"]) for c in kept] == [("2024-12-19", "2024-12-25")]
    hits = L.conflicts(state, "us_single_stocks", "intraday", "2024-12-24", "2026-11-24", "history")
    assert hits and L.conflicts(state, "us_single_stocks", "intraday", "2024-12-25", "2026-11-25", "history") == []


def test_policy_4_frees_nothing_sealed_before_it():
    pol = json.loads(P.canonical_bytes(P.policy_payload()))
    pol["owner_decisions"] = pol["owner_decisions"][:2]
    pol["unread_abandonment"]["attested"] = [a for a in pol["unread_abandonment"]["attested"] if a["lane"] == "forward"]
    state = _live_ss_state(pol)
    assert not state.unread_abandoned(LIVE_SS) and not state.reregistrable(LIVE_SS)
    assert any(c["source"] == LIVE_SS and c["end"] == "2026-11-19" for c in state.consumption())


@pytest.mark.parametrize("contrary", ["look", "edge_read", "dirty_evidence", "no_decision", "bad_receipt_sha"])
def test_contrary_evidence_beats_the_attestation(contrary):
    pol = json.loads(P.canonical_bytes(P.policy_payload()))
    state = _live_ss_state(pol)
    if contrary == "look":
        state.looks[LIVE_SS] = {"verdict": "FAIL"}
    elif contrary == "edge_read":
        state.reads[LIVE_SS] = {"progress"}
    elif contrary == "dirty_evidence":
        state.abandoned[LIVE_SS] = {"reason": "x", "cause": L.CODE_CHANGED,
                                    "read_evidence": {"ledger_reads": [], "failed_attempts": 1, "report_staged": False}}
    elif contrary == "no_decision":
        pol["owner_decisions"] = pol["owner_decisions"][:2]
    else:
        next(a for a in pol["unread_abandonment"]["attested"] if a["lane"] == "history")["receipt_sha256"] = "x"
    assert not state.unread_abandoned(LIVE_SS) and not state.reregistrable(LIVE_SS)
    assert any(c["source"] == LIVE_SS and c["end"] == "2026-11-19" for c in state.consumption())


def test_a_lost_sidecar_cannot_free_a_read_window(tmp_path, monkeypatch):
    """The read record is in the hash-chained ledger: deleting or restoring the data dir changes
    nothing, and a cut-out record makes the ledger refuse to load."""
    led = L.memory()
    sha = register(led, metals_proposal())
    L.append_read(led, sha, "look", NOW.isoformat())
    assert L.append_read(led, sha, "look", NOW.isoformat()) is None  # one record per kind
    with monkeypatch.context() as m:
        m.setattr(R, "code_sha256", lambda fam: "f" * 64)
        R.run_cycle(led, _NoReads({}, today=TODAY), tmp_path, now=NOW + timedelta(days=3), register=False)
    for f in tmp_path.iterdir():
        if f.is_file():
            f.unlink()
    state = L.load_state(led)
    assert state.reads[sha] == {"look"} and not state.unread_abandoned(sha) and not state.reregistrable(sha)
    with pytest.raises(L.StreamRefused, match="already closed"):  # no read is recorded after a close
        L.append_read(led, sha, "progress", NOW.isoformat())
    other = L.memory()
    osha = register(other, metals_proposal())
    L.append_read(other, osha, "look", NOW.isoformat())
    L._append(other, {"kind": L.READ_KIND, "registration_sha256": osha, "read": "look", "recorded_at": "x"})
    with pytest.raises(L.StreamRefused, match="recorded twice"):  # a forged duplicate is refused on load
        L.load_state(other)


def test_abandonment_evidence_is_complete_or_refused():
    led = L.memory()
    sha = register(led, metals_proposal())
    with pytest.raises(L.StreamRefused, match="unknown abandonment cause"):
        L.append_abandoned(led, sha, "x", cause="bored", read_evidence={})
    with pytest.raises(L.StreamRefused, match="read evidence goes with"):
        L.append_abandoned(led, sha, "x", cause=L.CODE_CHANGED)
    with pytest.raises(L.StreamRefused, match="incomplete"):
        L.append_abandoned(led, sha, "x", cause=L.CODE_CHANGED, read_evidence={"look_reads": 0})
