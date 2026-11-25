"""intelligence.regime.spells: spells and transitions of the market-state record (#1309).

Synthetic rows on the real pinned XNYS sessions; the writer and the read-only CLI on in-memory SQLite.
"""

from __future__ import annotations

import importlib.util
import inspect as pyinspect
import json
import random
import sqlite3
from bisect import bisect_left
from datetime import date, datetime
from itertools import pairwise
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect, text

from intelligence.regime import market_state as M
from intelligence.regime import spells as S

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

ROOT = Path(__file__).resolve().parents[1]


def _load(name: str, rel: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


W = _load("state_writer_spells", "scripts/run_market_state.py")
CLI = _load("state_spells_cli", "scripts/market_state_spells.py")
SESSIONS = M.xnys_sessions()[2]


def days(start: date, n: int) -> list[date]:
    i = bisect_left(SESSIONS, start)
    return list(SESSIONS[i:i + n])


def entry(label=None, status="ok", value=None, z=None, obs=None):
    return {"status": status, "label": label if status == "ok" else None, "value": value, "robust_z": z,
            "obs_date": obs}


def row(day: date, origin=S.DAILY_WRITE, **variables) -> S.SpellRow:
    for v in variables.values():
        if v.get("obs_date") is None:
            v["obs_date"] = day.isoformat()
    return S.SpellRow(as_of=day, rule="market-state/3", origin=origin, variables=variables)


def vix_rows(ds, labels, statuses=None):
    statuses = statuses or ["ok"] * len(labels)
    return [row(d, vix=entry(lab, st, z=1.5)) for d, lab, st in zip(ds, labels, statuses)]


def folded(rows):
    return S.fold(rows, SESSIONS)


# ── Spells follow eligible readings only ───────────────────────────────────

@pytest.mark.parametrize("status", ["stale", "unavailable", "no_baseline"])
def test_an_ineligible_reading_neither_starts_nor_ends_a_spell(status):
    ds = days(date(2026, 11, 2), 4)
    f = folded(vix_rows(ds, ["high", "high", "low", "high"], ["ok", "ok", status, "ok"]))
    assert f.transitions == []
    sp = f.spells[(S.LABEL, "vix")].summary()
    assert sp["state"] == "high" and sp["since"] == ds[0].isoformat() and sp["last_eligible"] == ds[3].isoformat()
    assert sp["spell_sessions"] == 3 and sp["ineligible_sessions"] == 1 and sp["trailing_ineligible_sessions"] == 0


def test_a_missing_row_neither_starts_nor_ends_a_spell():
    ds = days(date(2026, 11, 2), 4)
    rows = vix_rows([ds[0], ds[1], ds[3]], ["high", "high", "high"])
    f = folded(rows)
    assert f.transitions == [] and f.missing_sessions == [ds[2]]
    sp = f.spells[(S.LABEL, "vix")].summary()
    assert sp["spell_sessions"] == 3 and sp["ineligible_sessions"] == 1


def test_a_change_across_an_ineligible_session_is_one_transition_with_its_gap():
    ds = days(date(2026, 11, 2), 4)
    f = folded(vix_rows(ds, ["high", "high", "low", "normal"], ["ok", "ok", "stale", "ok"]))
    assert len(f.transitions) == 1
    t = f.transitions[0]
    assert (t["from"], t["to"], t["as_of"], t["gap_sessions"]) == ("high", "normal", ds[3].isoformat(), 1)
    assert t["ended_spell"]["spell_sessions"] == 2 and t["ended_spell"]["ineligible_sessions"] == 0
    assert t["ended_spell"]["last_eligible"] == ds[1].isoformat()
    assert t["status"] == "ok" and t["robust_z"] == 1.5 and t["origin"] == S.DAILY_WRITE


def test_trailing_ineligible_sessions_are_reported_not_counted():
    ds = days(date(2026, 11, 2), 4)
    f = folded(vix_rows(ds, ["high", "high", None, None], ["ok", "ok", "stale", "unavailable"]))
    sp = f.spells[(S.LABEL, "vix")].summary()
    assert sp["spell_sessions"] == 2 and sp["ineligible_sessions"] == 0
    assert sp["trailing_ineligible_sessions"] == 2 and sp["as_of_reading"] == "unavailable"


def test_ineligible_readings_before_the_first_eligible_one_start_nothing():
    ds = days(date(2026, 11, 2), 3)
    f = folded(vix_rows(ds, [None, None, "normal"], ["stale", "unavailable", "ok"]))
    sp = f.spells[(S.LABEL, "vix")].summary()
    assert sp["since"] == ds[2].isoformat() and sp["spell_sessions"] == 1 and sp["left_censored"] is True
    assert sp["ineligible_sessions"] == 0 and f.transitions == []


def test_a_variable_never_read_eligibly_has_no_spell():
    ds = days(date(2026, 11, 2), 2)
    sp = folded(vix_rows(ds, [None, None], ["unavailable", "stale"])).spells[(S.LABEL, "vix")].summary()
    assert sp == {"state": None, "as_of_reading": "stale", "reason": "no eligible reading yet"}


def test_spells_agree_with_the_stored_labels():
    """Every eligible session's spell state is its stored label; every change of eligible label is one transition."""
    rng = random.Random(1309)
    ds = days(date(2025, 3, 3), 160)
    labels, statuses = [], []
    current = "normal"
    for _ in ds:
        if rng.random() < 0.15:
            current = rng.choice(["normal", "above", "below", "high", "low"])
        st = rng.choice(["ok"] * 8 + ["stale", "unavailable"])
        labels.append(current if st == "ok" else rng.choice(["normal", "high"]))  # junk where ineligible
        statuses.append(st)
    rows = vix_rows(ds, labels, statuses)
    eligible = [(d, lab) for d, lab, st in zip(ds, labels, statuses) if st == "ok"]
    changes = [(b[0], a[1], b[1]) for a, b in pairwise(eligible) if a[1] != b[1]]
    f = folded(rows)
    assert [(date.fromisoformat(t["as_of"]), t["from"], t["to"]) for t in f.transitions] == changes
    for k, d in enumerate(ds):
        block = S.spells_block(rows[: k + 1], d)
        vix = block["variables"]["vix"]
        if statuses[k] == "ok":
            assert vix["state"] == labels[k] and vix["last_eligible"] == d.isoformat()
            run = 0
            for e_day, e_lab in reversed([e for e in eligible if e[0] <= d]):
                if e_lab != labels[k]:
                    break
                run += 1
            assert vix["spell_sessions"] == run
        assert [t["as_of"] for t in block["transitions"]] == [d.isoformat() for c in changes if c[0] == d]


def test_a_row_on_a_non_session_day_is_ignored():
    """2025-01-09 (national day of mourning) was backfilled by the scheduler helpers; it never advances a spell."""
    rows = vix_rows([date(2025, 1, 8), date(2025, 1, 9), date(2025, 1, 10)], ["high", "low", "high"])
    f = folded(rows)
    assert f.ignored_rows == [date(2025, 1, 9)] and f.transitions == [] and f.missing_sessions == []
    assert f.spells[(S.LABEL, "vix")].summary()["spell_sessions"] == 2
    block = S.spells_block(rows, date(2025, 1, 10))
    assert block["ignored_non_session_rows"] == ["2025-01-09"]


def test_two_rows_for_one_session_are_refused():
    d = days(date(2026, 11, 2), 1)[0]
    with pytest.raises(ValueError, match="two market-state rows"):
        folded(vix_rows([d, d], ["high", "low"]))


def test_left_censoring_and_the_previous_spell():
    ds = days(date(2026, 11, 2), 5)
    f = folded(vix_rows(ds, ["normal", "normal", "normal", "high", "high"]))
    sp = f.spells[(S.LABEL, "vix")].summary()
    assert sp["left_censored"] is False and sp["spell_sessions"] == 2
    assert sp["previous"] == {"state": "normal", "since": ds[0].isoformat(), "last_eligible": ds[2].isoformat(),
                              "spell_sessions": 3, "observations": 3, "ineligible_sessions": 0,
                              "left_censored": True}


def test_a_weekly_reading_counts_sessions_and_distinct_observations():
    ds = days(date(2026, 11, 2), 6)
    rows = [row(d, initial_claims=entry("above", obs="2026-10-31" if k < 5 else "2026-11-07"))
            for k, d in enumerate(ds)]
    sp = folded(rows).spells[(S.LABEL, "initial_claims")].summary()
    assert sp["spell_sessions"] == 6 and sp["observations"] == 2


def test_a_variable_added_later_starts_at_its_first_reading():
    ds = days(date(2026, 11, 2), 3)
    rows = [row(ds[0], vix=entry("normal")), row(ds[1], vix=entry("normal"), ust_2y_change_5d=entry("low")),
            row(ds[2], vix=entry("normal"), ust_2y_change_5d=entry("low"))]
    sp = folded(rows).spells[(S.LABEL, "ust_2y_change_5d")].summary()
    assert sp["since"] == ds[1].isoformat() and sp["spell_sessions"] == 2 and sp["ineligible_sessions"] == 0


# ── Named spells ───────────────────────────────────────────────────────────

def test_value_spells_count_no_baseline_readings_but_never_stale_ones():
    ds = days(date(2026, 11, 2), 4)
    flip = [entry(status="no_baseline", value=-1.0), entry(status="no_baseline", value=-0.5),
            entry(status="stale", value=1.0), entry(status="no_baseline", value=0.5)]
    f = folded([row(d, spot_vs_gamma_flip=e) for d, e in zip(ds, flip)])
    [t] = [t for t in f.transitions if t["name"] == "spot_below_gamma_flip"]
    assert (t["kind"], t["from"], t["to"], t["as_of"], t["gap_sessions"]) == (S.NAMED, "on", "off", ds[3].isoformat(), 1)
    assert t["variable"] == "spot_vs_gamma_flip" and t["value"] == 0.5
    assert not [t for t in f.transitions if t["kind"] == S.LABEL]   # no label spell from no_baseline readings


def test_vix_above_vix3m_is_strictly_above_one():
    ds = days(date(2026, 11, 2), 3)
    f = folded([row(d, vix_term=entry("above", value=v)) for d, v in zip(ds, [1.02, 1.0, 0.97])])
    sp = f.spells[(S.NAMED, "vix_above_vix3m")].summary()
    assert sp["state"] == "off" and sp["since"] == ds[1].isoformat() and sp["spell_sessions"] == 2
    assert sp["previous"]["state"] == "on"


def test_hy_spread_above_normal_follows_its_label_set():
    ds = days(date(2026, 11, 2), 4)
    f = folded([row(d, hy_spread=entry(lab)) for d, lab in zip(ds, ["normal", "above", "high", "normal"])])
    named = [(t["from"], t["to"], t["as_of"]) for t in f.transitions if t["name"] == "hy_spread_above_normal"]
    assert named == [("off", "on", ds[1].isoformat()), ("on", "off", ds[3].isoformat())]
    labels = [(t["from"], t["to"]) for t in f.transitions if t["name"] == "hy_spread"]
    assert labels == [("normal", "above"), ("above", "high"), ("high", "normal")]


def test_a_non_numeric_value_is_not_eligible():
    spec = next(s for s in S.NAMED_SPELLS if s.name == "vix_above_vix3m")
    for bad in (None, float("nan"), "1.2", True):
        assert spec.reading(entry("above", value=bad))[0] is None


# ── Origin ─────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("prov,origin", [
    ({"mode": "daily", "dry_run": False}, S.DAILY_WRITE),
    ({"mode": "daily"}, S.RECONSTRUCTION),                       # no dry_run field: not proof of a real write
    ({"mode": "daily", "dry_run": None}, S.RECONSTRUCTION), ({"mode": "daily", "dry_run": 0}, S.RECONSTRUCTION),
    ({"mode": "daily", "dry_run": "false"}, S.RECONSTRUCTION), ({"mode": "daily", "dry_run": True}, S.RECONSTRUCTION),
    ({"mode": "backfill", "dry_run": False}, S.RECONSTRUCTION),
    ({"mode": "explicit_session", "dry_run": False}, S.RECONSTRUCTION),
    ({"mode": "replacement", "dry_run": False}, S.RECONSTRUCTION), (None, S.RECONSTRUCTION),
    ("daily", S.RECONSTRUCTION)])
def test_only_an_explicit_non_dry_daily_write_is_daily_write(prov, origin):
    assert S.row_origin({"writer_provenance": prov}) == origin


def test_daily_write_claims_the_writer_mode_not_the_timer():
    # The writer stamps every record execution_origin=unverified; the label must not say more than that.
    assert S.DAILY_WRITE == "daily_write" and "nightly" not in S.DAILY_WRITE
    assert '"execution_origin": "unverified"' in pyinspect.getsource(W.main)


def test_transitions_name_the_origin_of_their_row_and_spells_count_by_origin():
    ds = days(date(2026, 11, 2), 4)
    rows = [row(ds[0], S.RECONSTRUCTION, vix=entry("normal")), row(ds[1], S.RECONSTRUCTION, vix=entry("high")),
            row(ds[2], S.DAILY_WRITE, vix=entry("high")), row(ds[3], S.DAILY_WRITE, vix=entry("low"))]
    f = folded(rows)
    assert [(t["to"], t["origin"]) for t in f.transitions] == [("high", S.RECONSTRUCTION), ("low", S.DAILY_WRITE)]
    assert f.spells[(S.LABEL, "vix")].previous["state"] == "high"
    block = S.spells_block(rows[:3], ds[2])
    assert block["variables"]["vix"]["basis"] == {S.DAILY_WRITE: 1, S.RECONSTRUCTION: 1}


# ── The block ──────────────────────────────────────────────────────────────

def test_a_block_names_its_source_and_only_the_writer_claims_writer():
    ds = days(date(2026, 11, 2), 2)
    rows = vix_rows(ds, ["high", "high"])
    recomputed = S.spells_block(rows, ds[1])
    assert recomputed["source"] == S.RECOMPUTED and "not what was known" in recomputed["source_detail"]
    assert S.spells_block(rows, ds[1], source=S.WRITER)["source"] == S.WRITER
    assert S.record_spells(rows[:1], ds[1], "market-state/3", {"variables": {"vix": entry("high")}})["source"] \
        == S.WRITER
    with pytest.raises(ValueError):
        S.spells_block(rows, ds[1], source="nightly")


def test_lineage_counts_earlier_rows_written_after_the_sessions_own_record():
    ds = days(date(2026, 11, 2), 3)
    at = [datetime(2026, 11, 13, 9), datetime(2026, 11, 20, 23), datetime(2026, 11, 13, 9, 5)]
    rows = [S.SpellRow(d, "market-state/3", S.RECONSTRUCTION, {"vix": entry("high", obs=d.isoformat())}, c)
            for d, c in zip(ds, at)]
    lin = S.recompute_lineage(rows, ds[2])
    assert lin == {"rows_folded": 3, "own_record_computed_at": "2026-11-13T09:05:00",
                   "own_record_origin": S.RECONSTRUCTION, "earlier_rows_written_after_own_record": 1,
                   "latest_computed_at": "2026-11-20T23:00:00"}
    assert S.recompute_lineage(rows[:1], ds[0])["earlier_rows_written_after_own_record"] == 0
    assert S.recompute_lineage(rows[:1], ds[1])["earlier_rows_written_after_own_record"] is None   # no own record


def test_a_block_needs_a_session_and_its_own_record():
    ds = days(date(2026, 11, 2), 2)
    rows = vix_rows(ds, ["high", "high"])
    assert S.spells_block(rows, date(2026, 11, 7))["status"] == "unavailable"       # a Saturday
    no_own = S.spells_block(rows[:1], ds[1])
    assert no_own["status"] == "unavailable" and "no record" in no_own["reason"]
    assert S.spells_block(rows, ds[1])["status"] == "ok"


def test_the_block_ignores_rows_after_its_session():
    ds = days(date(2026, 11, 2), 3)
    rows = vix_rows(ds, ["high", "high", "low"])
    block = S.spells_block(rows, ds[1])
    assert block["variables"]["vix"]["state"] == "high" and block["transitions"] == []


def test_a_provisional_or_plain_record_has_no_spells_key():
    assert "spells" not in M.MarketState(date(2026, 11, 13)).to_dict()
    assert "spell" not in pyinspect.getsource(W.run_provisional)


# ── Writer ─────────────────────────────────────────────────────────────────

TARGET = date(2026, 11, 13)


@pytest.fixture()
def engine():
    return create_engine("sqlite://")


def _state(target, vix_label, mode=None):
    st = M.MarketState(target)
    st.variables = {"vix": entry(vix_label, z=2.4, obs=(target).isoformat()),
                    "vix_term": entry("above", value=1.03, obs=target.isoformat())}
    st.gamma_state = {"status": "unavailable", "reason": "synthetic"}
    if mode:
        st.writer_provenance = {"mode": mode}
    return st


@pytest.fixture()
def bounded(monkeypatch):
    labels = {date(2026, 11, 10): "normal", date(2026, 11, 11): "normal", date(2026, 11, 12): "normal",
              TARGET: "high"}
    monkeypatch.setattr(W, "utc_today", lambda: date(2026, 11, 14))
    monkeypatch.setattr(M, "load_calendar", dict)
    monkeypatch.setattr(M, "compute_market_state", lambda e, t, **kw: _state(t, labels[t]))
    return labels


def _stored(engine, day):
    with engine.connect() as c:
        raw = c.execute(text("SELECT record FROM market_state_daily WHERE as_of_date = :d"), {"d": day}).scalar_one()
    return json.loads(raw)


def _backfill(engine, labels):
    for d, lab in labels.items():
        if d < TARGET:
            assert M.store_market_state(engine, _state(d, lab))      # no provenance: reconstruction


def test_a_daily_write_stores_spells_and_the_sessions_transitions(engine, bounded, capsys):
    _backfill(engine, bounded)
    assert W.main(["--no-refresh", "--json"], engine) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["outcomes"][0]["spells_status"] == "ok"
    sp = _stored(engine, TARGET)["spells"]
    assert sp["rule"] == S.SPELL_RULE and sp["status"] == "ok" and sp["as_of"] == "2026-11-13"
    assert sp["source"] == S.WRITER
    vix = sp["variables"]["vix"]
    assert (vix["state"], vix["since"], vix["spell_sessions"], vix["left_censored"]) == ("high", "2026-11-13", 1, False)
    assert vix["previous"]["spell_sessions"] == 3 and vix["previous"]["left_censored"] is True
    [t] = sp["transitions"]
    assert (t["name"], t["from"], t["to"], t["origin"], t["robust_z"]) == ("vix", "normal", "high", S.DAILY_WRITE, 2.4)
    assert sp["named"]["vix_above_vix3m"]["state"] == "on" and sp["named"]["vix_above_vix3m"]["spell_sessions"] == 4
    assert sp["first_session"] == "2026-11-10" and sp["missing_sessions"] == 0


def test_a_dry_run_range_follows_its_own_earlier_targets_and_writes_nothing(engine, bounded, capsys):
    assert W.main(["--no-refresh", "--json", "--dry-run", "--from", "2026-11-10", "--to", "2026-11-13"], engine) == 0
    records = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert [r["as_of"] for r in records] == ["2026-11-10", "2026-11-11", "2026-11-12", "2026-11-13"]
    last = records[-1]["spells"]
    assert last["first_session"] == "2026-11-10" and [t["to"] for t in last["transitions"]] == ["high"]
    assert last["transitions"][0]["origin"] == S.RECONSTRUCTION      # a dry run is never a daily write
    assert not inspect(engine).has_table("market_state_daily")


def test_a_range_backfill_reads_the_store_once_and_chains_its_own_records(engine, bounded, monkeypatch):
    calls = []
    real = S.load_rows
    monkeypatch.setattr(S, "load_rows", lambda e, **kw: calls.append(kw) or real(e, **kw))
    assert W.main(["--no-refresh", "--json", "--from", "2026-11-10", "--to", "2026-11-13"], engine) == 0
    assert calls == [{"through": TARGET}]
    sp = _stored(engine, TARGET)["spells"]
    assert sp["variables"]["vix"]["previous"]["spell_sessions"] == 3
    assert sp["transitions"][0]["origin"] == S.RECONSTRUCTION        # a range is reconstruction


def test_a_spells_failure_never_sinks_the_record(engine, bounded, monkeypatch, capsys):
    def boom(*a, **kw):
        raise RuntimeError("db gone")
    monkeypatch.setattr(S, "load_rows", boom)
    assert W.main(["--no-refresh", "--json"], engine) == 0
    rec = _stored(engine, TARGET)
    assert rec["variables"]["vix"]["label"] == "high"
    assert rec["spells"] == {"rule": S.SPELL_RULE, "as_of": "2026-11-13", "source": S.WRITER,
                             "status": "unavailable", "reason": "error reading stored labels: RuntimeError"}


# ── A dry run follows the live run's admission ─────────────────────────────

@pytest.mark.parametrize("existing,replace,hold,admission", [
    (False, False, False, "would_write"), (False, True, False, "would_replace"),
    (False, False, True, "would_hold"), (True, False, False, "existing_row_preserved"),
    (True, False, True, "existing_row_preserved")])        # the live run skips an existing row before its gate
def test_dry_run_admission_follows_the_live_order(existing, replace, hold, admission):
    assert W.dry_run_admission(existing=existing, replace=replace, hold=hold) == admission
    assert (admission in W.ADVANCES_SPELLS) == (admission in {"would_write", "would_replace"})


def _mixed_store(engine):
    # 11-10 stored normal (eligible, kept); 11-11 absent (would write normal); 11-12 stored LOW, which a live run
    # without --replace keeps although it would compute normal; 11-13 absent (would write high).
    assert M.store_market_state(engine, _state(date(2026, 11, 10), "normal"))
    assert M.store_market_state(engine, _state(date(2026, 11, 12), "low"))


def _dry(engine, capsys, *extra):
    assert W.main(["--no-refresh", "--json", "--dry-run", "--from", "2026-11-10", "--to", "2026-11-13", *extra],
                  engine) == 0
    return {r["as_of"]: r for r in (json.loads(x) for x in capsys.readouterr().out.splitlines() if x.strip())}


def test_a_dry_run_range_matches_the_live_range_over_existing_rows(engine, bounded, capsys):
    _mixed_store(engine)
    dry = _dry(engine, capsys)
    assert {d: r["writer_provenance"]["dry_run_admission"] for d, r in dry.items()} == {
        "2026-11-10": "existing_row_preserved", "2026-11-11": "would_write",
        "2026-11-12": "existing_row_preserved", "2026-11-13": "would_write"}
    [t] = dry["2026-11-13"]["spells"]["transitions"]
    assert (t["from"], t["to"], t["ended_spell"]["spell_sessions"]) == ("low", "high", 1)   # the stored 11-12 label
    assert W.main(["--no-refresh", "--json", "--from", "2026-11-10", "--to", "2026-11-13"], engine) == 0
    capsys.readouterr()
    assert _stored(engine, date(2026, 11, 12))["variables"]["vix"]["label"] == "low"       # preserved
    assert dry["2026-11-13"]["spells"] == _stored(engine, TARGET)["spells"]
    assert dry["2026-11-11"]["spells"] == _stored(engine, date(2026, 11, 11))["spells"]


def test_a_dry_replace_range_matches_the_live_replace(engine, bounded, capsys):
    _mixed_store(engine)
    dry = _dry(engine, capsys, "--replace", "--reason", "test: recompute")
    assert {r["writer_provenance"]["dry_run_admission"] for r in dry.values()} == {"would_replace"}
    [t] = dry["2026-11-13"]["spells"]["transitions"]
    assert (t["from"], t["ended_spell"]["spell_sessions"]) == ("normal", 3)                # 11-12 recomputed
    assert W.main(["--no-refresh", "--json", "--replace", "--reason", "test: recompute", "--from", "2026-11-10",
                   "--to", "2026-11-13"],
                  engine) == 0
    assert dry["2026-11-13"]["spells"] == _stored(engine, TARGET)["spells"]
    assert _stored(engine, TARGET)["writer_provenance"]["replacement_reason"] == "test: recompute"


def test_a_held_target_is_labelled_in_the_dry_run_and_never_stored_live(engine, bounded, capsys):
    _backfill(engine, bounded)
    args = ["--no-refresh", "--json", "--as-of", "2026-11-13", "--require-target-dealer"]
    assert W.main([*args, "--dry-run"], engine) == 0
    rec = json.loads(capsys.readouterr().out)
    assert rec["writer_provenance"]["dry_run_admission"] == "would_hold"
    assert W.main(args, engine) == 3                     # synthetic gamma is unavailable: the live run holds
    assert json.loads(capsys.readouterr().out)["outcomes"][0]["outcome"] == "held_missing_target_dealer"
    with engine.connect() as c:
        assert c.execute(text("SELECT COUNT(*) FROM market_state_daily WHERE as_of_date = :d"),
                         {"d": TARGET}).scalar_one() == 0


def test_a_range_cannot_carry_the_dealer_gate(engine, bounded):
    # So no would-hold target can sit between two targets of one run; the dry run mirrors the hold regardless.
    assert W.main(["--no-refresh", "--dry-run", "--require-target-dealer", "--from", "2026-11-10",
                   "--to", "2026-11-13"], engine) == 2


# ── Read-only CLI ──────────────────────────────────────────────────────────

def _replace_later(engine, day, label):
    assert M.store_market_state(engine, _state(day, label), replace=M.MarketStateReplacement("test", "test-run"))
    with engine.begin() as c:       # SQLite's CURRENT_TIMESTAMP has one-second resolution: make "later" certain
        c.execute(text("UPDATE market_state_daily SET computed_at = '2099-01-01 00:00:00' WHERE as_of_date = :d"),
                  {"d": day})


def test_cli_check_matches_stored_blocks_and_flags_a_later_replacement(engine, bounded, capsys):
    _backfill(engine, bounded)
    assert W.main(["--no-refresh", "--json"], engine) == 0
    capsys.readouterr()
    assert CLI.main(["--check"], engine) == 0
    out = json.loads(capsys.readouterr().out)
    assert out.pop("read_at") and out == {"rows": 4, "with_spells": 1, "without_spells": 3, "mismatched": []}
    _replace_later(engine, date(2026, 11, 12), "high")
    assert CLI.main(["--check"], engine) == 1
    assert json.loads(capsys.readouterr().out)["mismatched"] == [
        {"as_of": "2026-11-13", "differs": ["variables", "transitions"], "earlier_rows_written_after_block": 1}]


def test_cli_as_of_shows_the_stored_block_beside_a_labelled_recomputation(engine, bounded, capsys):
    _backfill(engine, bounded)
    assert W.main(["--no-refresh", "--json"], engine) == 0
    capsys.readouterr()
    _replace_later(engine, date(2026, 11, 12), "high")
    assert CLI.main(["--as-of", "2026-11-13"], engine) == 0
    out = json.loads(capsys.readouterr().out)
    stored, recomputed = out["stored"], out["recomputed"]
    assert stored["source"] == S.WRITER and recomputed["source"] == S.RECOMPUTED
    assert [t["from"] for t in stored["transitions"]] == ["normal"]       # what the writer saw that evening
    assert recomputed["transitions"] == []                               # today's store: 11-12 now says high
    assert recomputed["lineage"]["earlier_rows_written_after_own_record"] == 1
    assert recomputed["lineage"]["own_record_origin"] == S.DAILY_WRITE
    assert CLI.main(["--as-of", "2026-11-12"], engine) == 0
    assert json.loads(capsys.readouterr().out)["stored"]["status"] == "absent"


def test_cli_stored_transitions_are_only_what_the_writer_saved(engine, bounded, capsys):
    _backfill(engine, bounded)
    assert W.main(["--no-refresh", "--json"], engine) == 0
    capsys.readouterr()
    assert CLI.main(["--transitions", "--stored"], engine) == 0
    captured = capsys.readouterr()
    lines = [json.loads(x) for x in captured.out.splitlines()]
    assert [(t["as_of"], t["name"], t["source"], t["origin"]) for t in lines] == [
        ("2026-11-13", "vix", "stored", S.DAILY_WRITE)]
    assert "1 of 4 records" in captured.err
    assert CLI.main(["--transitions", "--stored", "--to", "2026-11-12"], engine) == 0
    assert capsys.readouterr().out == ""


def test_cli_transitions_label_backfilled_rows_reconstruction(engine, capsys):
    labels = dict(zip(days(date(2026, 11, 2), 4), ["normal", "high", "high", "low"]))
    for d, lab in labels.items():
        assert M.store_market_state(engine, _state(d, lab))
    assert CLI.main(["--transitions", "--name", "vix"], engine) == 0
    lines = [json.loads(x) for x in capsys.readouterr().out.splitlines()]
    assert [(t["from"], t["to"], t["origin"]) for t in lines] == [("normal", "high", S.RECONSTRUCTION),
                                                                ("high", "low", S.RECONSTRUCTION)]
    assert {t["source"] for t in lines} == {S.RECOMPUTED}
    assert CLI.main(["--transitions", "--from", "2026-11-05", "--name", "vix"], engine) == 0
    assert [json.loads(x)["to"] for x in capsys.readouterr().out.splitlines()] == ["low"]


def test_cli_as_of_prints_one_block_and_refuses_stray_flags(engine, capsys):
    ds = days(date(2026, 11, 2), 2)
    for d in ds:
        assert M.store_market_state(engine, _state(d, "normal"))
    assert CLI.main(["--as-of", ds[1].isoformat()], engine) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["recomputed"]["variables"]["vix"]["spell_sessions"] == 2
    assert out["stored"] == {"status": "absent",
                             "reason": "the record was stored without a spells block (before #1309)"}
    assert CLI.main(["--as-of", ds[1].isoformat(), "--name", "vix"], engine) == 2
    assert CLI.main(["--as-of", ds[1].isoformat(), "--stored"], engine) == 2


def test_cli_never_writes(engine, capsys):
    assert CLI.main(["--check"], engine) == 0
    assert not inspect(engine).has_table("market_state_daily")
