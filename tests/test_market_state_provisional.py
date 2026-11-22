"""The provisional market-state record at the close (#1312): the writer's close gate, provenance, label, storage.

Display only. Synthetic in-memory SQLite and a patched ``compute_market_state``; no provider is called. Session
times come from the pinned XNYS parts (``analysis/factory_session_calendar.PINS``): 2026-11-20 closes 21:00Z,
2026-07-01 closes 20:00Z (summer), 2026-11-27 closes early at 18:00Z, and 2026-11-26 (Thanksgiving) and 2026-07-03
(Independence Day observed) are not sessions.
"""

from __future__ import annotations

import importlib.util
import json
from datetime import date, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import IntegrityError

from intelligence.regime import market_state as M

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("state_writer_provisional", ROOT / "scripts/run_market_state.py")
W = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(W)

DAY = date(2026, 11, 20)


def z(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


@pytest.fixture()
def engine():
    return create_engine("sqlite://")


def state(target: date) -> M.MarketState:
    st = M.MarketState(target)
    st.gamma_state = {"status": "stale", "obs_date": "2026-11-19", "reason": "no capture for the session yet"}
    st.variables = {"vix": {"status": "ok", "obs_date": "2026-11-19", "age_days": 1, "label": "high",
                            "robust_z": 2.4},
                    "dealer_gamma": {"status": "stale", "obs_date": "2026-11-19"}}
    st.events = {"status": "ok", "within_14d": []}
    return st


@pytest.fixture(autouse=True)
def bounded(monkeypatch):
    calls: list[date] = []

    def compute(engine, target, **kw):
        calls.append(target)
        return state(target)

    monkeypatch.setattr(W, "utc_now", lambda: z("2026-11-20T21:15:00Z"))
    monkeypatch.setattr(W, "utc_today", lambda: date(2026, 11, 20))
    monkeypatch.setattr(M, "load_calendar", dict)
    monkeypatch.setattr(M, "compute_market_state", compute)
    return calls


def run(engine, capsys, *extra: str) -> tuple[int, dict | None]:
    code = W.main(["--provisional", "--no-refresh", "--json", *extra], engine)
    out = capsys.readouterr().out.strip()
    return code, (json.loads(out) if out else None)


def stored(engine) -> list[tuple]:
    with engine.connect() as c:
        return c.execute(text("SELECT as_of_date, rule, record, computed_at, provisional "
                              "FROM market_state_provisional ORDER BY as_of_date")).fetchall()


def tables(engine) -> set[str]:
    return set(inspect(engine).get_table_names())


# ── the close gate (pinned XNYS calendar) ────────────────────────────────────

@pytest.mark.parametrize("now, target", [
    ("2026-11-20T21:00:00Z", "2026-11-20"),      # at the close
    ("2026-11-20T21:15:00Z", "2026-11-20"),      # the timer's minute
    ("2026-11-20T23:59:59Z", "2026-11-20"),      # last second of the UTC day
    ("2026-07-01T20:00:00Z", "2026-07-01"),      # summer: the close is 20:00Z
    ("2026-11-27T18:00:00Z", "2026-11-27"),      # early close, 13:00 ET
    ("2026-11-27T21:15:00Z", "2026-11-27"),
])
def test_after_the_close_writes_todays_session(engine, monkeypatch, capsys, bounded, now, target):
    monkeypatch.setattr(W, "utc_now", lambda: z(now))
    code, summary = run(engine, capsys)
    assert code == 0 and summary["target_session"] == target and summary["written"] == 1
    assert bounded == [date.fromisoformat(target)]
    rows = stored(engine)
    assert [str(r[0]) for r in rows] == [target]
    assert "market_state_daily" not in tables(engine)


@pytest.mark.parametrize("now, why", [
    ("2026-11-20T20:59:59Z", "closes at 2026-11-20T21:00:00+00:00"),   # one second before the close
    ("2026-11-20T14:30:00Z", "closes at"),                            # at the open
    ("2026-07-01T19:59:59Z", "closes at 2026-07-01T20:00:00+00:00"),  # summer, before the close
    ("2026-11-27T17:59:59Z", "closes at 2026-11-27T18:00:00+00:00"),  # early-close day, before it
    ("2026-11-26T21:15:00Z", "not an XNYS session"),                  # Thanksgiving
    ("2026-07-03T21:15:00Z", "not an XNYS session"),                  # Independence Day observed
    ("2026-11-21T21:15:00Z", "not an XNYS session"),                  # Saturday
    ("2026-11-21T00:30:00Z", "not an XNYS session"),                  # Friday evening in New York, Saturday UTC
    ("2026-11-24T00:30:00Z", "closes at 2026-11-24T21:00:00+00:00"),  # after midnight UTC: Monday's session is not targeted
    ("2028-01-03T21:15:00Z", "outside"),                              # past the pinned parts
])
def test_before_the_close_or_not_a_session_refuses(engine, monkeypatch, capsys, bounded, now, why):
    monkeypatch.setattr(W, "utc_now", lambda: z(now))
    code = W.main(["--provisional", "--no-refresh", "--json"], engine)
    captured = capsys.readouterr()
    assert code == 2 and why in captured.err and captured.out == ""
    assert bounded == [] and tables(engine) == set()


def test_provisional_target_needs_an_aware_clock():
    with pytest.raises(ValueError, match="timezone-aware"):
        W.provisional_target(datetime(2026, 11, 20, 21, 15))  # noqa: DTZ001 - the naive clock under test
    day, session, cal = W.provisional_target(z("2026-11-20T21:15:00Z"))
    assert day == DAY and session.closes == z("2026-11-20T21:00:00Z") and cal.version.startswith("pinned XNYS: ")


@pytest.mark.parametrize("extra", [
    ["--from", "2026-11-19", "--to", "2026-11-20"],
    ["--as-of", "2026-11-19"],
    ["--require-target-dealer"],
])
def test_never_runs_with_a_range_a_date_or_the_dealer_gate(engine, capsys, bounded, extra):
    code = W.main(["--provisional", "--no-refresh", *extra], engine)
    assert code == 2 and "targets today's session only" in capsys.readouterr().err
    assert bounded == [] and tables(engine) == set()


# ── the record: label, provenance, storage ───────────────────────────────────

def test_record_is_labelled_provisional_with_writer_provenance(engine, capsys):
    code, summary = run(engine, capsys)
    assert code == 0
    assert summary["mode"] == "provisional" and summary["decision_use"] == "display_only"
    assert summary["table"] == "market_state_provisional" and summary["session_closes"] == "2026-11-20T21:00:00+00:00"
    (_day, rule, raw, computed_at, provisional), = stored(engine)
    rec = json.loads(raw)
    assert rule == M.RECORD_RULE == rec["rule"] and rec["as_of"] == "2026-11-20" and provisional in (1, True)
    assert computed_at is not None
    assert rec["provisional"] is True and rec["decision_use"] == "display_only"
    assert rec["label"] == M.PROVISIONAL_LABEL.format(day="2026-11-20")
    assert "computed at the close of session 2026-11-20 from the data available then" in rec["label"]
    assert "decision record is written to market_state_daily the next evening (#1191)" in rec["label"]
    p = rec["writer_provenance"]
    assert p["mode"] == "provisional" and p["decision_use"] == "display_only"
    assert p["table"] == "market_state_provisional" and p["target_session"] == "2026-11-20"
    assert p["started_at"] == "2026-11-20T21:15:00+00:00" and p["session_closes"] == "2026-11-20T21:00:00+00:00"
    assert p["computed_after_close"] is True and p["calendar"].startswith("pinned XNYS: ")
    assert p["run_id"] == summary["run_id"] and p["execution_origin"] == "unverified"
    assert p["target_dealer_required"] is False and p["dry_run"] is False and p["replace"] is False
    assert p["readiness"]["forward_qualified"] is False and p["readiness"]["dealer_target_ready"] is False
    assert len(p["code_sha256"]["scripts/run_market_state.py"]) == 64
    # The rest of the record is compute_market_state's own output, unchanged.
    assert rec["variables"]["vix"]["label"] == "high" and rec["gamma_state"]["status"] == "stale"


def test_insert_only_unless_replace(engine, monkeypatch, capsys, bounded):
    assert run(engine, capsys)[0] == 0
    first = stored(engine)
    monkeypatch.setattr(W, "utc_now", lambda: z("2026-11-20T22:00:00Z"))
    code, summary = run(engine, capsys)
    assert code == 0 and summary["written"] == 0 and summary["already_present"] == 1
    outcome = summary["outcomes"][0]
    assert outcome["outcome"] == "already_present" and outcome["written_this_run"] is False
    assert outcome["stored_rule"] == M.RECORD_RULE and outcome["stored_computed_at"]
    assert bounded == [DAY]                         # the existing row was not recomputed
    assert stored(engine) == first
    code, summary = run(engine, capsys, "--replace")
    assert code == 0 and summary["outcomes"][0]["outcome"] == "replaced" and bounded == [DAY, DAY]
    rec = json.loads(stored(engine)[0][2])
    assert rec["writer_provenance"]["replace"] is True
    assert rec["writer_provenance"]["started_at"] == "2026-11-20T22:00:00+00:00"


def test_store_is_insert_only_and_refuses_a_decision_record(engine):
    st = state(DAY)
    with pytest.raises(ValueError, match="mode 'provisional'"):
        M.store_provisional_market_state(engine, st)        # no provenance: not the provisional writer's
    st.writer_provenance = {"mode": "daily"}
    with pytest.raises(ValueError, match="mode 'provisional'"):
        M.store_provisional_market_state(engine, st)
    assert tables(engine) == set()
    st.writer_provenance = {"mode": "provisional"}
    assert M.store_provisional_market_state(engine, st) is True
    assert M.store_provisional_market_state(engine, st) is False
    assert M.store_provisional_market_state(engine, st, replace=True) is True
    assert len(stored(engine)) == 1


def test_table_refuses_a_row_that_is_not_provisional(engine):
    M.ensure_provisional_table(engine)
    with pytest.raises(IntegrityError), engine.begin() as c:
        c.execute(text("INSERT INTO market_state_provisional (as_of_date, rule, record, provisional) "
                       "VALUES (:d, :r, :rec, :p)"), {"d": DAY, "r": M.RECORD_RULE, "rec": "{}", "p": False})
    with engine.begin() as c:                                   # the default is provisional
        c.execute(text("INSERT INTO market_state_provisional (as_of_date, rule, record) VALUES (:d, :r, :rec)"),
                  {"d": DAY, "r": M.RECORD_RULE, "rec": "{}"})
    assert stored(engine)[0][4] in (1, True)


def test_dry_run_prints_the_labelled_record_and_writes_nothing(engine, capsys):
    code, rec = run(engine, capsys, "--dry-run")
    assert code == 0 and rec["provisional"] is True and rec["writer_provenance"]["dry_run"] is True
    assert rec["label"].startswith("Provisional, display only")
    assert tables(engine) == set()


def test_refreshes_vintages_first_like_the_decision_writer(engine, monkeypatch, capsys):
    import ingestion.fred_vintages as F

    seen = []
    monkeypatch.setattr(F, "refresh", lambda eng, s: seen.append(s) or {"series": s, "status": "SUCCESS",
                                                                         "rows_sent": 0})
    assert W.main(["--provisional", "--json"], engine) == 0
    summary = json.loads(capsys.readouterr().out)
    assert seen == list(F.SERIES) and [r["series"] for r in summary["vintage_refresh"]] == list(F.SERIES)
