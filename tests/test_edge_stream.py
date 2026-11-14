"""Edge stream: insert-only registry, sanity gate, budget, fresh data, one look, no peeking."""

from __future__ import annotations

import json
import os
import stat
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pytest

from alpha_research.edge_stream import families as F
from alpha_research.edge_stream import hermes as H
from alpha_research.edge_stream import ledger as L
from alpha_research.edge_stream import policy as P
from alpha_research.edge_stream import runner as R
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
    prop = F.build_proposal(dict(F.QUEUE[0]), today)
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
    assert rec["policy"]["budget"]["alpha_per_look"] == pytest.approx(0.00125)
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
    sha2 = register(led, F.build_proposal(dict(F.QUEUE[1]), TODAY))
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
    nxt = F.build_proposal(dict(F.QUEUE[1]), TODAY)
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


def _trades(effect, n_sessions=400, seed=1, both=True):
    rng = np.random.default_rng(seed)
    out = []
    for i in range(n_sessions):
        side = (1 if rng.random() < 0.5 else -1) if both else 1
        move = side * effect + rng.normal(0, 0.001)
        out.append({"session": f"s{i:04d}", "side": side, "net": side * move - 0.0003,
                    "always_long_net": move - 0.0003})
    return out


def test_pass_fail_underpowered():
    assert ST.judge(_trades(0.001), P.ALPHA_PER_LOOK)["verdict"] == "PASS"
    assert ST.judge(_trades(0.0), P.ALPHA_PER_LOOK)["verdict"] == "FAIL"
    assert ST.judge(_trades(0.001, n_sessions=60), P.ALPHA_PER_LOOK)["verdict"] == "UNDERPOWERED"
    one_sided = ST.judge(_trades(0.001, both=False), P.ALPHA_PER_LOOK)
    assert one_sided["verdict"] == "UNDERPOWERED" and "sides" in one_sided["reasons"][0]


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
    item = dict(F.QUEUE[-1])
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
    register(led, F.build_proposal(dict(F.QUEUE[-1]), TODAY))
    again = F.build_proposal(dict(F.QUEUE[-1]), TODAY + timedelta(days=3))
    assert "reuses consumed data" in S.check(again, L.load_state(led), TODAY + timedelta(days=3))["G7_fresh"]


def test_malformed_inbox_proposal_is_rejected_not_fatal(tmp_path):
    (tmp_path / "inbox").mkdir()
    (tmp_path / "inbox" / "bad.json").write_text(json.dumps(
        {"family": "IM1", "group": "metals", "instruments": ["GLD", "GLD"], "lane": "bogus"}))
    (tmp_path / "inbox" / "junk.json").write_text("{not json")
    led = L.memory()
    for q in F.QUEUE:  # mark the template queue done so the inbox is reached
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
