"""Edge stream, multi-day event families: policy amendment, filings and daily bars, the EC1 rule, its look."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pytest

from alpha_research.edge_stream import data as D
from alpha_research.edge_stream import families as F
from alpha_research.edge_stream import ledger as L
from alpha_research.edge_stream import policy as P
from alpha_research.edge_stream import runner as R
from alpha_research.edge_stream import sanity as S
from alpha_research.edge_stream import universe as U
from alpha_research.edge_stream.data import StaticDailySource, StaticEventSource, StaticSource

TODAY = date(2026, 11, 24)  # a Tuesday
NOW = datetime(2026, 11, 24, 12, 0, tzinfo=timezone.utc)
EC1 = F.FAMILIES["EC1"]
TICKERS = U.SP500_20251231[:60]
REPO = Path(__file__).resolve().parents[1]


def weekdays(start: date, end: date) -> list[str]:
    out, d = [], start
    while d < end:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def synthetic(effect: float, tickers=TICKERS, seed: int = 11, start=date(2025, 12, 15), end=TODAY):
    """Daily bars and quarterly filings with a planted drift of ``effect`` (over the hold) in the
    direction of the reaction day's market-adjusted return. Most events fall on a day the market
    moved 1.5 % the other way, so both sides trade."""
    rng = np.random.default_rng(seed)
    days = weekdays(start, end)
    market = np.where(rng.random(len(days)) < 0.5, 0.015, -0.015) * (rng.random(len(days)) < 0.9) \
        + rng.normal(0, 0.001, len(days))
    spy, px = [], 600.0
    for d, r in zip(days, market):
        spy.append((d, px, px * (1 + r)))
        px *= 1 + r
    bars, events = {"SPY": spy}, {}
    for k, t in enumerate(tickers):
        first = 12 + (k * 7) % 55
        ev_idx = [i for i in range(first, len(days) - 1, 63)]
        drift = np.zeros(len(days))
        shock = np.zeros(len(days))
        evs = []
        for i in ev_idx:  # filing accepted after the close of day i - 1 -> reaction day i
            sign = -np.sign(market[i]) if rng.random() < 0.85 else np.sign(market[i])
            shock[i] = sign * 0.04
            drift[i + 1:i + 1 + EC1.hold_sessions] += sign * effect / EC1.hold_sessions
            evs.append({"accession": f"{t}-{i}", "filing_date": days[i - 1],
                        "accepted_et": f"{days[i - 1]}T16:05:00"})
        rows, px = [], 100.0 + k
        for i, d in enumerate(days):
            r = market[i] + shock[i] + drift[i] + rng.normal(0, 0.002)
            rows.append((d, px, px * (1 + r)))
            px *= 1 + r
        bars[t], events[t] = rows, evs
    return bars, events


def extras_for(effect: float, today=TODAY, **kw) -> dict:
    bars, events = synthetic(effect, **kw)
    return {"daily": StaticDailySource(bars, today=today), "events": StaticEventSource(events)}


def ec1_item(tickers=TICKERS, lane="history") -> dict:
    return {"family": "EC1", "group": "sp500_events", "instruments": tuple(tickers), "lane": lane}


V2_RULE = ("per calendar year (UTC): q_year over at most max_registrations stage-1 registrations; "
           "each look alpha_per_look, Bonferroni over its trials; abandoned registrations count; "
           "confirmations (forward, after a stage-1 PASS) at confirm_alpha/k outside the yearly count")


def v3_payload() -> dict:
    """Policy 3 (live from 2026-11-20, amendment 2811e06b): policy 4 without its owner decision
    on unread abandonments."""
    pol = json.loads(P.canonical_bytes(P.policy_payload()))
    pol.pop("unread_abandonment")
    pol["owner_decisions"] = pol["owner_decisions"][:1]
    pol["policy_version"] = "edge-stream-policy/3"
    return pol


def v2_payload() -> dict:
    """Policy 2: the single 0.00125 stage-1 bar, before the two-stage evidence bar."""
    pol = v3_payload()
    pol.pop("owner_decisions")
    pol.pop("evidence_bar")
    pol["budget"].update(alpha_per_look=0.00125, rule=V2_RULE)
    for k in ("largest_year_out", "year_out_n_min", "noise_unit"):
        pol["pass_test"].pop(k)
    pol["promotion"] = "never: a confirmed PASS is a candidate report; trading needs Anik's explicit go"
    pol["policy_version"] = "edge-stream-policy/2"
    return pol


def v1_payload() -> dict:
    """Policy 1: policy 2 without the sp500_events group, its tier and the multiday section."""
    pol = v2_payload()
    names = set(pol["groups"].pop("sp500_events"))
    pol["liquidity"] = {t: v for t, v in pol["liquidity"].items() if t not in names}
    pol["cost_floors"].pop("large_cap")
    pol.pop("multiday")
    pol["prior_consumption"] = [c for c in pol["prior_consumption"] if c["group"] != "sp500_events"]
    pol["policy_version"] = "edge-stream-policy/1"
    return pol


def v1_ledger(monkeypatch):
    old = v1_payload()
    with monkeypatch.context() as m:
        m.setattr(P, "policy_payload", lambda: old)
        return L.memory("2026-11-13T06:27:00+00:00")


# --- universe and policy ------------------------------------------------------------


def test_universe_is_the_membership_file_on_its_date_minus_the_stated_names():
    cfg = json.loads((REPO / "config" / "sp500_membership.json").read_text())
    members = set(cfg["base_members"])
    for ch in sorted(cfg["changes"], key=lambda c: c["date"]):
        if ch["date"] <= U.SP500_AS_OF:
            members |= set(ch["add"])
            members -= set(ch["remove"])
    want = sorted(t for t in members if t.isalpha() and t not in U.SP500_PRIOR_USE and t not in U.SP500_SYMBOL_CLASH)
    assert list(U.SP500_20251231) == want
    assert not set(U.SP500_20251231) & {t for g, ts in P.GROUPS.items() if g != "sp500_events" for t in ts}
    assert all(P.LIQUIDITY[t] == "large_cap" and P.group_of(t) == "sp500_events" for t in U.SP500_20251231)


def test_v2_only_tightens_v1_and_loosening_is_named():
    old, new = v1_payload(), v2_payload()
    assert P.tightening_violations(old, new) == []
    assert "policy_version unchanged" in P.tightening_violations(new, new)
    # the live ledger's genesis policy and the policy 2 of this branch's parent, byte for byte
    assert L.sha256_bytes(P.canonical_bytes(old)).startswith("c1005e96")

    def changed(**edit):
        pol = json.loads(P.canonical_bytes(new))
        pol["policy_version"] = "edge-stream-policy/9"
        for path, value in edit.items():
            node = pol
            *head, last = path.split("__")
            for k in head:
                node = node[k]
            node[last] = value
        return P.tightening_violations(new, pol)

    assert changed() == []
    assert changed(budget__alpha_per_look=0.01) == ["budget.alpha_per_look raised or missing"]
    assert changed(pass_test__n_min=50) == ["pass_test.n_min lowered or missing"]
    assert changed(pass_test__baseline="none") == ["pass_test.baseline changed"]
    assert changed(cost_floors__large_cap={"usd": 0.02, "bp": 1.0}) == ["cost floor large_cap lowered or removed"]
    assert changed(groups__metals=["GLD"]) == ["group metals lost an instrument"]
    assert changed(liquidity__GLD="thin") == ["liquidity of GLD changed"]
    assert changed(prior_consumption=new["prior_consumption"][1:]) == ["prior consumption entry removed or edited"]
    assert changed(multiday__short_carry_annual=0.0) == ["multiday changed"]
    assert changed(accounts__robinhood_agentic__shares_short=True) == ["accounts changed"]


def test_g11_cost_gate_does_not_run_on_an_event_family(monkeypatch):
    """G11 prices a session family's session_trade on minute bars; EC1 has none to price."""
    class Untouchable:
        def sessions(self, *a, **k):
            raise AssertionError("G11 read bars for an event family")

    monkeypatch.setattr(S, "_feasibility_source", Untouchable())
    led = L.memory()
    report = S.check(F.build_proposal(ec1_item(), TODAY), L.load_state(led), TODAY)
    assert "G11_cost_feasible" not in report and S.passed(report), report


# --- the amendment record -------------------------------------------------------------


def test_amendment_is_recorded_once_and_registrations_follow_it(monkeypatch):
    led = v1_ledger(monkeypatch)
    assert L.load_state(led).policy["policy_version"] == "edge-stream-policy/1"
    prop = F.build_proposal(ec1_item(), TODAY)
    report = S.check(prop, L.load_state(led), TODAY)
    with pytest.raises(L.StreamRefused, match="amend_policy first"):
        L.append_registration(led, {**prop, "sanity": report, "recorded_at": NOW.isoformat()})
    sha = L.amend_policy(led, "code policy edge-stream-policy/2", NOW.isoformat())
    assert sha and L.amend_policy(led, "again") is None
    state = L.load_state(led)
    assert state.policy_sha256 == P.policy_sha256() and len(state.amendments) == 1
    assert any(c["group"] == "sp500_events" for c in state.consumption())
    report = S.check(prop, state, TODAY)
    assert S.passed(report), report
    reg_sha = L.append_registration(led, {**prop, "sanity": report, "recorded_at": NOW.isoformat()})
    assert L.load_state(led).registrations[reg_sha]["policy_sha256"] == P.policy_sha256()


def test_a_loosening_code_policy_is_refused_and_nothing_registers(monkeypatch, tmp_path):
    led = L.memory()
    loose = json.loads(P.canonical_bytes(P.policy_payload()))
    loose["policy_version"] = "edge-stream-policy/4"
    loose["budget"]["alpha_per_look"] = 0.05
    monkeypatch.setattr(P, "policy_payload", lambda: loose)
    with pytest.raises(L.StreamRefused, match="loosens the policy"):
        L.amend_policy(led, "looser")
    s = R.run_cycle(led, StaticSource({}, today=TODAY), tmp_path, now=NOW, extras=extras_for(0.0))
    assert s["errors"] and "loosens" in s["errors"][0]["policy"] and s["registered"] == []
    assert len(led.records) == 2


def test_a_forged_amendment_is_refused_on_load(monkeypatch):
    led = v1_ledger(monkeypatch)
    forged = json.loads(P.canonical_bytes(P.policy_payload()))
    forged["budget"]["max_registrations_per_year"] = 400
    L._append(led, {"kind": L.AMENDMENT_KIND, "policy": forged, "policy_sha256": L.sha256_bytes(P.canonical_bytes(forged)),
                    "previous_policy_sha256": L.sha256_bytes(P.canonical_bytes(v1_payload())), "reason": "x",
                    "recorded_at": NOW.isoformat()})
    with pytest.raises(L.StreamRefused, match="loosens the policy"):
        L.load_state(led)


def test_cycle_amends_the_live_v1_ledger_before_it_registers(monkeypatch, tmp_path):
    led = v1_ledger(monkeypatch)
    monkeypatch.setattr(R, "QUEUE", (ec1_item(),))
    s = R.run_cycle(led, StaticSource({}, today=TODAY), tmp_path, now=NOW, extras=extras_for(0.0))
    assert s["policy_amended"]["policy_sha256"] == P.policy_sha256()
    assert s["registered"][0]["proposal_id"] == "EC1-v1:sp500_events:history:2026-01-02"
    kinds = [r.get("kind") for r in led.records[1:]]
    assert kinds == [L.POLICY_KIND, L.AMENDMENT_KIND, L.REGISTRATION_KIND]


# --- policy 3: the two-stage evidence bar (owner decision GRID#1213) ------------------


def test_policy_3_changes_only_what_the_owner_decision_waives():
    new = v3_payload()
    # the live ledger's policy 3 (amendment 2811e06b, 2026-11-20), byte for byte
    assert L.sha256_bytes(P.canonical_bytes(new)).startswith("1986091237b7")
    assert new["policy_version"] == "edge-stream-policy/3" and new["evidence_bar"]["version"] == P.EVIDENCE_BAR
    b = new["budget"]
    assert b["alpha_per_look"] == pytest.approx(0.025)
    assert b["max_registrations_per_year"] * b["alpha_per_look"] * b["confirm_alpha"] == pytest.approx(b["q_year"])
    for old in (v1_payload(), v2_payload()):  # the live ledger is on 1; this branch's parent is 2
        assert sorted(P.tightening_violations(old, new)) == [
            "budget.alpha_per_look raised or missing", "budget.rule changed", "promotion changed"]
        assert P.amendment_violations(old, new) == []
    decision, = new["owner_decisions"]
    assert decision["id"] == "GRID#1213" and sorted(decision["waives"]) == [
        "budget.alpha_per_look", "budget.rule", "promotion"]


def test_an_owner_decision_cannot_be_stretched():
    old = v2_payload()

    def refused(mutate) -> list[str]:
        pol = v3_payload()
        mutate(pol)
        return P.amendment_violations(old, pol)

    # the same change without the decision is a loosening
    assert "budget.alpha_per_look raised or missing" in refused(lambda p: p.pop("owner_decisions"))
    # a decision waives only what it names, and can only name the few waivable fields
    assert refused(lambda p: p["cost_floors"].update(liquid={"usd": 0.0, "bp": 0.0})) == [
        "cost floor liquid lowered or removed"]
    assert refused(lambda p: p["budget"].update(max_registrations_per_year=80)) == [
        "budget.max_registrations_per_year raised or missing",
        "budget: max_registrations x alpha_per_look x confirm_alpha exceeds q_year"]
    assert refused(lambda p: p["owner_decisions"][0]["waives"].append("cost_floors")) == [
        "owner decision GRID#1213 waives what no decision can change: cost_floors",
        "budget.alpha_per_look raised or missing", "budget.rule changed", "promotion changed"]
    assert "owner decision GRID#1213 is incomplete" in refused(lambda p: p["owner_decisions"][0].pop("url"))
    assert "owner decision GRID#1213 is incomplete" in refused(
        lambda p: p["owner_decisions"][0].update(text_sha256="not-a-hash"))
    # the yearly budget holds whatever a decision waives
    assert refused(lambda p: p["budget"].update(alpha_per_look=0.05)) == [
        "budget: max_registrations x alpha_per_look x confirm_alpha exceeds q_year"]
    # once recorded, a decision is never edited or dropped, and a later version needs its own
    cur = P.policy_payload()
    later = json.loads(P.canonical_bytes(cur))
    later["policy_version"] = "edge-stream-policy/9"
    assert P.amendment_violations(cur, later) == []
    later["owner_decisions"] = []
    assert P.amendment_violations(cur, later) == ["an owner decision was removed or edited"]
    later = json.loads(P.canonical_bytes(cur))
    later["policy_version"] = "edge-stream-policy/9"
    later["budget"]["alpha_per_look"] = 0.03  # the recorded decision does not cover a second loosening
    assert "budget.alpha_per_look raised or missing" in P.amendment_violations(cur, later)


def test_registrations_sealed_under_policy_1_replay_on_their_own_terms(monkeypatch):
    old = v1_payload()
    with monkeypatch.context() as m:  # a stage-1 registration and its look, sealed at 0.00125
        m.setattr(P, "policy_payload", lambda: old)
        m.setattr(P, "policy_sha256", lambda: L.sha256_bytes(P.canonical_bytes(old)))
        m.setattr(P, "ALPHA_PER_LOOK", 0.00125)
        m.setattr(P, "EVIDENCE_BAR", None)
        led = L.memory("2026-11-13T06:27:00+00:00")
        prop = F.build_proposal({"family": "IM1", "group": "metals", "instruments": ("GLD", "SLV"),
                                 "lane": "history"}, date(2026, 11, 13))
        prop.pop("evidence_bar")
        report = S.check(prop, L.load_state(led), date(2026, 11, 13))
        assert S.passed(report), report
        old_sha = L.append_registration(led, {**prop, "sanity": report, "recorded_at": "2026-11-13T11:05:00+00:00"})
        L.append_look(led, old_sha, {"verdict": "FAIL", "trials": [{"trial_id": "pooled", "verdict": "FAIL"}],
                                     "data": {"window_end": "2026-11-13"}, "report_sha256": "0" * 64})
    assert L.amend_policy(led, f"code policy {P.POLICY_VERSION}", NOW.isoformat())
    state = L.load_state(led)  # the 0.00125 registration is still valid under the policy it was sealed with
    assert state.registrations[old_sha]["alpha_look"] == pytest.approx(0.00125)
    assert state.policy["policy_version"] == P.POLICY_VERSION and state.closed_families() == {}
    new = F.build_proposal({"family": "IM1", "group": "energy", "instruments": ("USO", "UNG"), "lane": "history"},
                           TODAY)
    report = S.check(new, state, TODAY)
    assert S.passed(report), report
    sha = L.append_registration(led, {**new, "sanity": report, "recorded_at": NOW.isoformat()})
    reg = L.load_state(led).registrations[sha]
    assert reg["alpha_look"] == pytest.approx(0.025) and reg["evidence_bar"] == P.EVIDENCE_BAR
    # a registration at the retired level, or without the bar in force, is refused from here on
    again = F.build_proposal({"family": "IM1", "group": "fx", "instruments": ("FXE", "FXY", "UUP"),
                              "lane": "history"}, TODAY + timedelta(days=1))
    stamp = {"sanity": {"G": "ok"}, "recorded_at": (NOW + timedelta(days=1)).isoformat()}
    with pytest.raises(L.StreamRefused, match="wrong alpha for stage 1"):
        L.append_registration(led, {**again, **stamp, "alpha_look": 0.00125, "alpha_trial": 0.00125})
    with pytest.raises(L.StreamRefused, match="evidence bar in force"):
        L.append_registration(led, {**again, **stamp, "evidence_bar": None})


def test_a_missed_confirmation_closes_the_family_in_that_group():
    led = L.memory()
    item = {"family": "IM1", "group": "metals", "instruments": ("GLD", "SLV"), "lane": "history"}
    day = date(2026, 11, 13)

    def seal(prop, today):
        report = S.check(prop, L.load_state(led), today)
        assert S.passed(report), report
        return L.append_registration(led, {**prop, "sanity": report,
                                           "recorded_at": f"{today.isoformat()}T12:00:00+00:00"})

    def look(sha, verdict, end):
        L.append_look(led, sha, {"verdict": verdict, "trials": [{"trial_id": "pooled", "verdict": verdict}],
                                 "data": {"window_end": end}, "report_sha256": "0" * 64})

    parent = seal(F.build_proposal(item, day), day)
    look(parent, "PASS", day.isoformat())
    forward = {**item, "lane": "forward"}
    confirm = seal(F.build_proposal(forward, day, proposer="confirmation", confirms=parent, stage=2), day)
    assert L.load_state(led).closed_families() == {}  # open: nothing is closed yet
    look(confirm, "FAIL", "2027-04-09")
    state = L.load_state(led)
    closed = state.closed_families()
    assert list(closed) == [("IM1", "metals")] and "confirms-" in closed[("IM1", "metals")]
    later = date(2027, 4, 12)  # fresh forward nights, same family, same group: a rescue cut
    retry = F.build_proposal(forward, later)
    assert "family closed in this group" in S.check(retry, state, later)["G8_budget"]
    with pytest.raises(L.StreamRefused, match="IM1 is closed in metals"):
        L.append_registration(led, {**retry, "sanity": {"G": "ok"}, "recorded_at": "2027-04-12T12:00:00+00:00"})
    other = F.build_proposal({"family": "IM1", "group": "energy", "instruments": ("USO", "UNG"), "lane": "history"},
                             later)
    assert S.check(other, state, later)["G8_budget"] == "ok"  # the same family elsewhere is untouched


def test_an_abandoned_confirmation_closes_nothing_and_cannot_be_repeated():
    led = L.memory()
    item = {"family": "IM1", "group": "metals", "instruments": ("GLD", "SLV"), "lane": "history"}
    day = date(2026, 11, 13)
    stamp = {"sanity": {"G": "ok"}, "recorded_at": f"{day.isoformat()}T12:00:00+00:00"}
    parent = L.append_registration(led, {**F.build_proposal(item, day), **stamp})
    L.append_look(led, parent, {"verdict": "PASS", "trials": [{"trial_id": "pooled", "verdict": "PASS"}],
                                "data": {"window_end": day.isoformat()}, "report_sha256": "0" * 64})
    forward = {**item, "lane": "forward"}
    confirm = L.append_registration(led, {**F.build_proposal(forward, day, proposer="confirmation",
                                                             confirms=parent, stage=2), **stamp})
    L.append_abandoned(led, confirm, "code changed", "2026-12-01T12:00:00+00:00")
    assert L.load_state(led).closed_families() == {}  # no look was run: not a miss
    second = F.build_proposal(forward, date(2026, 12, 2), proposer="confirmation", confirms=parent, stage=2)
    with pytest.raises(L.StreamRefused, match="confirmed twice"):
        L.append_registration(led, {**second, "sanity": {"G": "ok"}, "recorded_at": "2026-12-02T12:00:00+00:00"})


def test_an_episode_is_not_confirmed(tmp_path, monkeypatch):
    """A discovery carried by one year is recorded EPISODE and registers no confirmation."""
    from alpha_research.edge_stream import stats as ST

    real = ST.largest_year_out
    monkeypatch.setattr(ST, "largest_year_out", lambda trades: {**real(trades), "status": "disappears"})
    led, s2 = run_two_cycles(0.04, tmp_path, monkeypatch)
    assert s2["looks"][0]["verdict"] == "EPISODE"
    state = L.load_state(led)
    look, = state.looks.values()
    assert look["trials"][0]["largest_year_out"]["status"] == "disappears"
    assert not [r for r in state.registrations.values() if r["stage"] == 2]
    status = json.loads((tmp_path / "status.json").read_bytes())
    assert status["verdicts"]["EPISODE"] == 1 and status["verdicts"]["PASS"] == 0 and status["candidates"] == []
    assert "EPISODE 1" in (tmp_path / "DIGEST.md").read_text()


# --- filings and daily bars -----------------------------------------------------------


def test_acceptance_time_is_utc_converted_to_eastern():
    assert D.parse_acceptance("2026-01-29T21:05:12.000Z") == datetime(2026, 1, 29, 16, 5, 12)     # winter
    assert D.parse_acceptance("2026-07-29T20:05:12.000Z") == datetime(2026, 7, 29, 16, 5, 12)     # summer
    assert D.parse_acceptance("2026-01-30T01:30:00.000Z") == datetime(2026, 1, 29, 20, 30, 0)     # the evening before
    assert D.parse_acceptance("2026-01-29T16:05:12") == datetime(2026, 1, 29, 16, 5, 12)          # no zone: Eastern
    assert D.parse_acceptance("2026-01-29T21:05:12+00:00") == datetime(2026, 1, 29, 16, 5, 12)
    assert D.parse_acceptance("2026-07-29T20:05:12+00:00") == datetime(2026, 7, 29, 16, 5, 12)
    assert D.parse_acceptance("") is None and D.parse_acceptance("soon") is None


PAGE = {
    "form": ["8-K", "8-K", "8-K/A", "10-Q", "8-K", "8-K"],
    "filingDate": ["2026-04-23", "2026-03-02", "2026-02-10", "2026-01-30", "2026-01-29", "2025-10-23"],
    "acceptanceDateTime": ["2026-04-23T20:10:00.000Z", "2026-03-02T13:00:00.000Z", "2026-02-10T22:00:00.000Z",
                           "2026-01-30T11:30:00.000Z", "2026-01-29T21:05:12.000Z", "2025-10-23T20:07:00.000Z"],
    "items": ["2.02,9.01", "5.02", "2.02", "", "9.01, 2.02", "2.02,9.01"],
    "accessionNumber": ["a4", "a3", "a2", "a1", "a0", "z9"],
}


def test_only_original_8k_item_202_filings_in_range_are_events():
    rows, oldest = D.earnings_filings(PAGE, date(2026, 1, 1), date(2026, 5, 1))
    assert [r["accession"] for r in rows] == ["a4", "a0"] and oldest == "2025-10-23"
    assert rows[1] == {"accession": "a0", "filing_date": "2026-01-29", "accepted_et": "2026-01-29T16:05:12",
                       "accepted_raw": "2026-01-29T21:05:12.000Z"}
    assert rows[0]["accepted_et"] == "2026-04-23T16:10:00"                                        # 20:10Z in summer


class FakeEdgar(D.EdgarEventSource):
    def __init__(self, tmp, pages, **kw):
        super().__init__(tmp / "c.sqlite3", user_agent=kw.pop("ua", "GRID tests"), gap_s=0, **kw)
        self.pages, self.urls = pages, []

    def _get(self, url):
        self.urls.append(url)
        if url == D.SEC_TICKER_MAP_URL:
            return {"0": {"ticker": "ABT", "cik_str": 1800}, "1": {"ticker": "JPM", "cik_str": 19617}}
        return self.pages[url.rsplit("/", 1)[-1]]


def test_edgar_source_reads_pages_caches_and_leaves_out_what_it_cannot_cover(tmp_path):
    older = {"form": ["8-K"], "filingDate": ["2025-07-24"], "acceptanceDateTime": ["2025-07-24T20:06:00.000Z"],
             "items": ["2.02"], "accessionNumber": ["old"]}
    recent_short = {k: v[:2] for k, v in PAGE.items()}          # oldest 2026-03-02: does not reach the start
    src = FakeEdgar(tmp_path, {
        "CIK0000001800.json": {"filings": {"recent": PAGE, "files": []}},
        "CIK0000019617.json": {"filings": {"recent": recent_short, "files": [
            {"name": "p1.json", "filingFrom": "2025-01-01", "filingTo": "2026-02-28"},
            {"name": "p0.json", "filingFrom": "2020-01-01", "filingTo": "2024-12-31"}]}},
        "p1.json": {k: v[2:] + older[k] for k, v in PAGE.items()},
    })
    got = src.events("ABT", date(2025, 11, 18), date(2026, 5, 1))
    assert [e["accession"] for e in got] == ["a0", "a4"]
    n = len(src.urls)
    assert src.events("ABT", date(2026, 1, 1), date(2026, 5, 1)) == got and len(src.urls) == n     # from the cache
    jpm = src.events("JPM", date(2025, 11, 18), date(2026, 5, 1))
    assert [e["accession"] for e in jpm] == ["a0", "a4"] and not any(u.endswith("p0.json") for u in src.urls)
    assert src.events("ZZZZ", date(2026, 1, 1), date(2026, 5, 1)) is None                             # no CIK


def test_edgar_source_refuses_a_filer_with_too_many_pages_and_needs_a_user_agent(tmp_path, monkeypatch):
    monkeypatch.setattr(D, "EDGAR_MAX_EXTRA_PAGES", 1)
    files = [{"name": f"p{i}.json", "filingFrom": "2025-01-01", "filingTo": "2026-12-31"} for i in range(2)]
    src = FakeEdgar(tmp_path, {"CIK0000019617.json": {"filings": {"recent": PAGE, "files": files}}})
    assert src.events("JPM", date(2026, 1, 1), date(2026, 5, 1)) is None
    bare = D.EdgarEventSource(tmp_path / "d.sqlite3", user_agent="")
    assert bare.ready() == "SEC_USER_AGENT not configured"
    with pytest.raises(D.SourceError):
        bare.events("ABT", date(2026, 1, 1), date(2026, 5, 1))


def test_edgar_requests_retry_a_timeout_and_a_429_then_give_up(tmp_path, monkeypatch):
    import requests

    class Resp:
        def __init__(self, code, body=None):
            self.status_code, self._body = code, body

        def json(self):
            return self._body

    script = []

    def fake_get(url, headers, timeout):
        assert headers["User-Agent"] == "GRID tests"
        step = script.pop(0)
        if isinstance(step, Exception):
            raise step
        return step

    monkeypatch.setattr(requests, "get", fake_get)
    src = D.EdgarEventSource(tmp_path / "e.sqlite3", user_agent="GRID tests", gap_s=0, backoff_s=0)
    script[:] = [requests.exceptions.ReadTimeout(), Resp(429), Resp(200, {"ok": 1})]
    assert src._get("https://data.sec.gov/submissions/CIK1.json") == {"ok": 1} and src.requests == 3
    script[:] = [Resp(503)] * D.EDGAR_ATTEMPTS
    with pytest.raises(D.SourceError, match="gave up on CIK1.json after 4 attempts"):
        src._get("https://data.sec.gov/submissions/CIK1.json")
    script[:] = [Resp(404)]
    with pytest.raises(D.SourceError, match="EDGAR 404"):
        src._get("https://data.sec.gov/submissions/CIK1.json")


def test_polygon_daily_source_parses_caches_and_refuses_gaps(tmp_path):
    minute = D.PolygonMinuteSource(tmp_path / "b.sqlite3", api_key="k", gap_s=0,
                                   now=datetime(2026, 3, 2, 12, tzinfo=timezone.utc))
    calls = []

    def ms(day, hour=5):
        return int(datetime.fromisoformat(f"{day}T{hour:02d}:00:00+00:00").timestamp() * 1000)

    def fake_get(path):
        calls.append(path)
        days = ["2026-02-23", "2026-02-24", "2026-02-26", "2026-02-27"] if "/GAP/" in path else \
            ["2026-02-23", "2026-02-24", "2026-02-25", "2026-02-26", "2026-02-27"]
        return {"results": [{"t": ms(d), "o": 10.0 + i, "c": 10.5 + i} for i, d in enumerate(days)]}

    minute._get = fake_get
    src = D.PolygonDailySource(minute)
    bars = src.daily("ABT", date(2026, 2, 23), date(2026, 2, 28), require_complete=True)
    assert bars[0] == ("2026-02-23", 10.0, 10.5) and len(bars) == 5 and "range/1/day/2026-02-23/2026-02-27" in calls[0]
    assert src.daily("ABT", date(2026, 2, 24), date(2026, 2, 27)) == bars[1:4] and len(calls) == 1
    assert len(src.daily("GAP", date(2026, 2, 23), date(2026, 2, 28))) == 4                 # a stock may miss a day
    with pytest.raises(D.SourceError, match="without a daily bar"):
        src.daily("GAP", date(2026, 2, 23), date(2026, 2, 28), require_complete=True)       # the hedge may not
    assert src.daily("ABT", date(2026, 2, 23), date(2026, 3, 9))[-1][0] == "2026-02-27"     # never today or later


# --- the rule ---------------------------------------------------------------------------


def test_reaction_day_is_the_first_close_after_acceptance():
    s = ["2026-01-29", "2026-01-30", "2026-02-02"]
    assert F.reaction_day("2026-01-29T07:00:00", s) == "2026-01-29"      # before the open
    assert F.reaction_day("2026-01-29T12:30:00", s) == "2026-01-29"      # during the session
    assert F.reaction_day("2026-01-29T16:00:00", s) == "2026-01-30"      # at the close: next session
    assert F.reaction_day("2026-01-30T18:00:00", s) == "2026-02-02"      # Friday evening -> Monday
    assert F.reaction_day("2026-01-31T10:00:00", s) == "2026-02-02"      # a Saturday
    assert F.reaction_day("2026-02-02T17:00:00", s) is None


def test_direction_needs_both_thresholds_and_trades_both_sides():
    def st(resid, market):
        return {"reaction_day_market_adjusted_return": resid, "reaction_day_market_return": market}

    assert EC1.direction(st(0.02, -0.0075)) == 1 and EC1.direction(st(-0.02, 0.0075)) == -1
    assert EC1.direction(st(0.0199, -0.02)) == 0 and EC1.direction(st(0.05, -0.0074)) == 0
    assert EC1.direction(st(0.05, 0.02)) == 0 and EC1.direction(st(-0.05, -0.02)) == 0   # market agreed: not hidden
    a, b = EC1.direction_probe()
    assert a["time_of_day"] == b["time_of_day"] and EC1.direction(a) == -EC1.direction(b) != 0


def trade_inputs(resid=0.04, market=-0.012, drift=0.03):
    sessions = weekdays(date(2026, 1, 26), date(2026, 3, 2))
    d0 = sessions.index("2026-01-30")
    spy = {d: (600.0, 600.0) for d in sessions}
    spy[sessions[d0]] = (600.0, 600.0 * (1 + market))
    for d in sessions[d0 + 1:]:
        spy[d] = (spy[sessions[d0]][1], spy[sessions[d0]][1])
    c0 = 100.0 * (1 + market + resid)
    stock = {d: (100.0, 100.0) for d in sessions[:d0]}
    stock[sessions[d0]] = (100.0, c0)
    for d in sessions[d0 + 1:]:
        stock[d] = (c0, c0)
    dx = sessions[d0 + EC1.hold_sessions]
    stock[dx] = (c0, c0 * (1 + drift))
    return sessions, stock, spy, {"accepted_et": "2026-01-29T16:05:00", "accession": "acc", "filing_date": "2026-01-29"}


def test_event_trade_numbers_costs_and_hedge():
    sessions, stock, spy, event = trade_inputs()
    t, why = EC1.event_trade("ABT", event, sessions, stock, spy)
    assert why == "" and t["side"] == 1 and (t["reaction_day"], t["entry_day"]) == ("2026-01-30", "2026-02-02")
    assert t["exit_day"] == sessions[sessions.index("2026-01-30") + 15] and t["session"] == "2026-W06"
    assert t["residual"] == pytest.approx(0.04) and t["r_market"] == pytest.approx(-0.012)
    held = (date.fromisoformat(t["exit_day"]) - date.fromisoformat(t["entry_day"])).days
    cost = (P.cost_per_side("ABT", t["entry"]) + P.cost_per_side("ABT", t["exit"])
            + 2 * P.cost_per_side("SPY", t["hedge_entry"]) + 0.02 * held / 365)
    assert t["cost_rt"] == pytest.approx(cost) and t["gross"] == pytest.approx(0.03)
    assert t["net"] == pytest.approx(0.03 - cost) and t["always_long_net"] == pytest.approx(0.03 - cost)
    s2, stock2, spy2, _ = trade_inputs(-0.04, 0.012, -0.03)
    short, _ = EC1.event_trade("ABT", event, s2, stock2, spy2)
    assert short["side"] == -1 and short["gross"] == pytest.approx(0.03)
    assert short["always_long_net"] == pytest.approx(-0.03 - short["cost_rt"])


def test_event_trade_skips_and_the_decision_refuses_later_bars():
    sessions, stock, spy, event = trade_inputs()
    assert EC1.event_trade("ABT", event, sessions, stock, spy)[0] is not None
    assert EC1.event_trade("ABT", event, sessions[:12], stock, spy)[1] == "hold_incomplete"
    assert EC1.event_trade("ABT", {**event, "accepted_et": "2026-01-25T10:00:00"}, sessions, stock, spy)[1] \
        == "no_prior_session"
    assert EC1.event_trade("ABT", event, sessions, {d: b for d, b in stock.items() if d != "2026-02-10"}, spy)[1] \
        == "missing_bars"
    split = dict(stock)
    split["2026-02-10"] = (stock["2026-02-10"][0], stock["2026-02-10"][1] / 2)
    assert EC1.event_trade("ABT", event, sessions, split, spy)[1] == "basis_jump"
    s3, stock3, spy3, _ = trade_inputs(resid=0.01)
    assert EC1.event_trade("ABT", event, s3, stock3, spy3)[1] == "no_signal"
    with pytest.raises(F.LookaheadError):
        EC1.decide(stock, spy, "2026-01-29", "2026-01-30")            # handed bars after the reaction day
    seen = {d: b for d, b in stock.items() if d <= "2026-01-30"}
    seen_spy = {d: b for d, b in spy.items() if d <= "2026-01-30"}
    assert EC1.decide(seen, seen_spy, "2026-01-29", "2026-01-30")[0] == 1
    later = dict(stock)
    later[sessions[-1]] = (1.0, 1.0)                                  # what happens after cannot change the decision
    assert EC1.event_trade("ABT", event, sessions, later, spy)[0]["side"] == 1


def test_one_event_per_company_per_forty_days():
    evs = [{"accepted_et": f"2026-0{m}-{d:02d}T16:05:00", "accession": f"{m}{d}"} for m, d in ((1, 29), (2, 20), (3, 12), (4, 23))]
    assert [e["accession"] for e in EC1.company_events(evs)] == ["129", "312", "423"]


def test_collect_ignores_events_before_the_window_and_names_what_it_leaves_out():
    bars, events = synthetic(0.0, tickers=TICKERS[:6])
    events[TICKERS[0]] = None
    got = EC1.collect(list(TICKERS[:6]), date(2026, 1, 2), TODAY, StaticDailySource(bars, today=TODAY),
                      StaticEventSource(events))
    assert got["left_out"] == {TICKERS[0]: "filings_unreadable"}
    assert got["trades"] and all(t["reaction_day"] >= "2026-01-02" and t["exit_day"] < TODAY.isoformat()
                                 for t in got["trades"])
    assert "hold_incomplete" in got["skipped"] and TICKERS[0] not in got["used"]["bars"]


# --- the gate ---------------------------------------------------------------------------


def test_ec1_history_proposal_passes_the_gate_and_the_reserved_years_are_refused():
    led = L.memory()
    prop = F.build_proposal(ec1_item(U.SP500_20251231), TODAY)
    assert prop["window"] == {"start": "2026-01-02", "end": TODAY.isoformat()} and prop["horizon"] == "multiday"
    assert prop["hedge"]["instrument"] == "SPY" and prop["short_carry_annual"] == 0.02 and prop["kind"] == "event"
    report = S.check(prop, L.load_state(led), TODAY)
    assert S.passed(report), report
    early = {**prop, "window": {"start": "2025-06-02", "end": TODAY.isoformat()}}
    assert "reserved" in S.check(early, L.load_state(led), TODAY)["G7_fresh"]
    cheap = {**prop, "hedge": {**prop["hedge"], "bp": 0.5}}
    assert "hedge" in S.check(cheap, L.load_state(led), TODAY)["G4_costs"]
    free_carry = {**prop, "short_carry_annual": 0.0}
    assert "hedge" in S.check(free_carry, L.load_state(led), TODAY)["G4_costs"]
    forward = F.build_proposal(ec1_item(U.SP500_20251231, "forward"), TODAY)
    assert forward["window"]["n_target_sessions"] == 250 and S.passed(S.check(forward, L.load_state(led), TODAY))


# --- the look ---------------------------------------------------------------------------


def run_two_cycles(effect, tmp_path, monkeypatch):
    monkeypatch.setattr(R, "QUEUE", (ec1_item(),))
    led = L.memory()
    ex = extras_for(effect, today=TODAY + timedelta(days=1))
    none = StaticSource({}, today=TODAY + timedelta(days=1))
    s1 = R.run_cycle(led, none, tmp_path, now=NOW, extras=ex)
    assert [r["proposal_id"] for r in s1["registered"]] == ["EC1-v1:sp500_events:history:2026-01-02"]
    assert s1["looks"] == []                                           # sealed first; the look is a later cycle
    s2 = R.run_cycle(led, none, tmp_path, now=NOW + timedelta(days=1), extras=ex)
    return led, s2


def test_planted_drift_passes_and_registers_a_year_long_forward_confirmation(tmp_path, monkeypatch):
    led, s2 = run_two_cycles(0.04, tmp_path, monkeypatch)
    assert s2["looks"][0]["verdict"] == "PASS"
    state = L.load_state(led)
    (reg_sha, look), = state.looks.items()
    report = json.loads(Path(look["report_path"]).read_bytes())
    trial = report["trial"]
    assert trial["n"] >= 100 and trial["n_long"] >= 50 and trial["n_short"] >= 50 and trial["mean_net"] > 0.02
    assert trial["sessions"] < trial["n"] and report["data"]["resampling_unit"].startswith("ISO week")
    assert {"reaction_day", "entry_day", "exit_day", "residual", "hedge_entry", "accession"} <= set(report["trades"][0])
    assert all(t["entry_day"] > t["reaction_day"] for t in report["trades"]) and report["promotion_allowed"] is False
    confirm = [r for r in state.registrations.values() if r.get("confirms") == reg_sha]
    assert len(confirm) == 1 and confirm[0]["lane"] == "forward" and confirm[0]["window"]["n_target_sessions"] == 250
    assert confirm[0]["alpha_look"] == P.CONFIRM_ALPHA and confirm[0]["evidence_bar"] == P.EVIDENCE_BAR
    # every EC1 trade is in 2026: the largest-year refit is recorded as not possible and the verdict stands
    assert look["trials"][0]["largest_year_out"]["status"] == "not_possible"
    with pytest.raises(L.StreamRefused):
        L.append_look(led, reg_sha, look)                              # one look per registration


def test_no_drift_fails(tmp_path, monkeypatch):
    led, s2 = run_two_cycles(0.0, tmp_path, monkeypatch)
    assert s2["looks"][0]["verdict"] == "FAIL"
    assert not any(r.get("confirms") for r in L.load_state(led).registrations.values())


def test_nothing_is_sealed_while_filings_cannot_be_read(tmp_path, monkeypatch):
    monkeypatch.setattr(R, "QUEUE", (ec1_item(),))
    led = L.memory()
    bars, events = synthetic(0.0, tickers=TICKERS[:5])
    ex = {"daily": StaticDailySource(bars, today=TODAY), "events": StaticEventSource(events, "SEC_USER_AGENT not configured")}
    for extras in (ex, None):
        s = R.run_cycle(led, StaticSource({}, today=TODAY), tmp_path, now=NOW, extras=extras)
        assert s["registered"] == [] and s["rejected"] == [] and s["deferred"][0]["family"] == "EC1"
    assert len(led.records) == 2 and not (tmp_path / "rejections.jsonl").exists()


def test_dry_run_counts_filings_and_hours_without_forming_a_trade(tmp_path, monkeypatch):
    monkeypatch.setattr(R, "QUEUE", (ec1_item(TICKERS[:10]),))

    def boom(*a, **k):
        raise AssertionError("a trade was formed in a dry run")

    monkeypatch.setattr(F.EarningsContamination, "event_trade", boom)
    led = L.memory()
    s = R.run_cycle(led, StaticSource({}, today=TODAY), tmp_path, now=NOW, dry_run=True,
                    extras=extras_for(0.0, tickers=TICKERS[:10]))
    pre = s["registered"][0]["preflight_sessions_with_bars"]
    assert pre["instruments"] == 10 and pre["left_out"] == 0 and pre["left_out_tickers"] == []
    assert pre["events_in_window"] >= 25
    assert set(pre["accepted_hour_et"]) == {"16"} and pre["sessions"] > 200
    assert len(led.records) == 2 and not (tmp_path / "status.json").exists()


def test_forward_ec1_never_forms_a_trade_before_its_look(tmp_path, monkeypatch):
    led = L.memory()
    prop = F.build_proposal(ec1_item(TICKERS[:10], "forward"), TODAY)
    report = S.check(prop, L.load_state(led), TODAY)
    sha = L.append_registration(led, {**prop, "sanity": report, "recorded_at": NOW.isoformat()})
    later = TODAY + timedelta(days=40)
    bars, events = synthetic(0.0, tickers=TICKERS[:10], end=later)

    def boom(*a, **k):
        raise AssertionError("an outcome was computed before the look")

    monkeypatch.setattr(F.EarningsContamination, "collect", boom)
    s = R.run_cycle(led, StaticSource({}, today=later), tmp_path, now=NOW + timedelta(days=40), register=False,
                    extras={"daily": StaticDailySource(bars, today=later), "events": StaticEventSource(events)})
    assert s["forward"][0]["ready"] is False and 20 < s["forward"][0]["sessions"] < 250
    assert sha not in L.load_state(led).looks


def test_family_preview_is_dry_run_only_and_skips_the_earlier_queue(tmp_path):
    with pytest.raises(ValueError):
        R.run_cycle(L.memory(), StaticSource({}, today=TODAY), tmp_path, now=NOW, only_family="EC1")
    s = R.run_cycle(L.memory(), StaticSource({}, today=TODAY), tmp_path, now=NOW, dry_run=True, only_family="EC1",
                    extras={"daily": StaticDailySource({"SPY": []}, today=TODAY), "events": StaticEventSource({})})
    assert s["registered"][0]["would_register"].startswith("EC1-v1:sp500_events:history")
    assert s["registered"][0]["preflight_sessions_with_bars"]["instruments"] == len(U.SP500_20251231)


def test_code_hash_covers_the_universe_through_the_policy(monkeypatch):
    before = F.code_sha256(EC1)
    smaller = json.loads(P.canonical_bytes(P.policy_payload()))
    smaller["groups"]["sp500_events"] = smaller["groups"]["sp500_events"][:-1]
    monkeypatch.setattr(P, "policy_payload", lambda: smaller)
    assert F.code_sha256(EC1) != before


# --- policy 4: unread abandonments (owner decision, 2026-11-21) -------------------------


def test_policy_4_adds_one_owner_decision_and_loosens_nothing_else():
    old, new = v3_payload(), P.policy_payload()
    assert new["policy_version"] == "edge-stream-policy/4"
    assert P.tightening_violations(old, new) == []  # a new section and a new decision only
    assert P.amendment_violations(old, new) == []   # fails until the decision's url and sha are filled
    first, decision = new["owner_decisions"]
    assert first == old["owner_decisions"][0] and decision["waives"] == []
    assert decision["id"] == "GRID#1336" and decision["url"].startswith("https://github.com/3pacs/GRID/issues/1336#issuecomment-")
    assert new["unread_abandonment"]["decision"] == decision["id"]
    assert new["unread_abandonment"]["cause"] == L.CODE_CHANGED


# --- a forward window that has not started yet (2026-11-26: polygon 400 on day one) -----


def _polygon_daily(tmp_path, now):
    """The real daily source over a fake Polygon that refuses a range whose end is before its start,
    as Polygon did on 2026-11-26 (``/range/1/day/2026-11-26/2026-11-25``)."""
    minute = D.PolygonMinuteSource(tmp_path / "b.sqlite3", api_key="k", gap_s=0, now=now)
    calls = []

    def fake_get(path):
        calls.append(path)
        lo, hi = path.split("?")[0].rsplit("/", 2)[-2:]
        if hi < lo:
            raise D.SourceError(f"polygon 400 for {path}")
        return {"results": []}

    minute._get = fake_get
    return minute, {"daily": D.PolygonDailySource(minute), "events": StaticEventSource({})}, calls


def _ec1_forward(day: date):
    led = L.memory()
    prop = F.build_proposal(ec1_item(lane="forward"), day)
    sha = L.append_registration(led, {**prop, "sanity": {"G": "ok"}, "recorded_at": f"{day.isoformat()}T12:00:00+00:00"})
    return led, sha, prop


def test_a_forward_window_that_has_not_started_is_not_read(tmp_path):
    led, sha, prop = _ec1_forward(date(2026, 11, 25))
    start = date.fromisoformat(prop["window"]["start"])
    assert start == date(2026, 11, 26)
    now = datetime(2026, 11, 26, 11, 3, tzinfo=timezone.utc)  # the first cycle after it was sealed
    minute, extras, calls = _polygon_daily(tmp_path, now)
    s = R.run_cycle(led, minute, tmp_path, now=now, extras=extras, register=False)
    state = L.load_state(led)
    assert calls == [] and not (tmp_path / "attempts.jsonl").exists()
    assert s["forward"] == [{"registration": sha[:16], "sessions": 0, "ready": False, "not_started": start.isoformat()}]
    assert s["looks"] == [] and s["abandoned"] == [] and not state.closed(sha)
    assert state.reads.get(sha, set()) == set()  # nothing read, nothing recorded as read


def test_the_first_cycle_after_the_window_opens_reads_it(tmp_path):
    led, sha, prop = _ec1_forward(date(2026, 11, 25))
    now = datetime(2026, 11, 27, 11, 3, tzinfo=timezone.utc)
    minute, extras, calls = _polygon_daily(tmp_path, now)
    s = R.run_cycle(led, minute, tmp_path, now=now, extras=extras, register=False)
    assert calls and "/range/1/day/2026-11-26/2026-11-26?" in calls[0]
    assert s["forward"][0]["sessions"] == 0 and "not_started" not in s["forward"][0]
    assert L.load_state(led).reads[sha] == {"progress"} and not (tmp_path / "attempts.jsonl").exists()
