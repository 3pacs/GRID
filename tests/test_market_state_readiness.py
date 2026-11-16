"""Writer outcome/PIT regression: synthetic in-memory SQLite, no provider calls."""
import importlib.util
import json
from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect, text

from intelligence.regime import market_state as M

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("state_writer", ROOT / "scripts/run_market_state.py")
W = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(W)
TARGET = date(2026, 11, 13)


@pytest.fixture()
def engine():
    return create_engine("sqlite://")


@pytest.fixture(autouse=True)
def bounded(monkeypatch):
    monkeypatch.setattr(W, "utc_today", lambda: date(2026, 11, 14))
    monkeypatch.setattr(M, "load_calendar", lambda: {})
    monkeypatch.setattr(M, "compute_market_state", lambda e, t, **kw: state(t))


def state(target=TARGET, dealer_date=TARGET):
    st = M.MarketState(target)
    st.gamma_state = ({"status": "ok", "obs_date": dealer_date.isoformat()}
                      if dealer_date else {"status": "unavailable", "reason": "no registered capture"})
    st.variables = {"vix": {"status": "ok", "obs_date": "2026-11-12", "age_days": 1},
                    "dealer_gamma": {"status": "no_baseline", "obs_date": str(target)}}
    st.events = {"status": "stale_calendar"}
    return st


def args(*extra):
    return ["--no-refresh", "--json", *extra]


def output(capsys):
    return json.loads(capsys.readouterr().out)


def stored(engine):
    with engine.connect() as c:
        row = c.execute(text("SELECT record FROM market_state_daily")).scalar_one()
    return json.loads(row)


def test_new_target_row_reports_only_actual_write_and_saves_provenance(engine, capsys):
    assert W.main(args("--require-target-dealer"), engine) == 0
    summary = output(capsys)
    row = summary["outcomes"][0]
    assert row["outcome"] == "written" and row["written_this_run"]
    assert row["target_session"] == "2026-11-13" and row["dealer_target_ready"]
    assert row["analysis_gaps"] == ["dealer_gamma"]
    assert row["variable_status"]["vix"]["obs_date"] == "2026-11-12"
    assert row["forward_qualified"] is False
    rec = stored(engine)
    provenance = rec["writer_provenance"]
    assert provenance["run_id"] == summary["run_id"]
    assert provenance["cutoff_exclusive"] == "2026-11-14T00:00:00+00:00"
    assert provenance["execution_origin"] == "unverified"
    assert len(provenance["code_sha256"]["scripts/run_market_state.py"]) == 64
    assert rec["variables"]["vix"]["obs_date"] == "2026-11-12"


@pytest.mark.parametrize("dealer_date", [None, date(2026, 11, 12)])
def test_missing_or_previous_dealer_holds_without_even_creating_table(engine, monkeypatch, capsys, dealer_date):
    monkeypatch.setattr(M, "compute_market_state", lambda e, t, **kw: state(t, dealer_date))
    assert W.main(args("--require-target-dealer"), engine) == 3
    summary = output(capsys)
    assert summary["held"] == 1 and summary["written"] == 0
    assert summary["outcomes"][0]["outcome"] == "held_missing_target_dealer"
    assert not inspect(engine).has_table("market_state_daily")


def test_existing_backfill_is_preserved_no_compute_or_refresh(engine, monkeypatch, capsys):
    M.store_market_state(engine, state())
    before = stored(engine)
    monkeypatch.setattr(M, "compute_market_state", lambda *a, **kw: pytest.fail("existing row recomputed"))
    # No --no-refresh: if existing-row fast path regresses, importing/calling refresh is a failure.
    import ingestion.fred_vintages as F
    monkeypatch.setattr(F, "refresh", lambda *a: pytest.fail("existing row refreshed provider"))
    assert W.main(["--json", "--require-target-dealer"], engine) == 0
    summary = output(capsys)
    outcome = summary["outcomes"][0]
    assert outcome["outcome"] == "already_present" and not outcome["written_this_run"]
    assert outcome["forward_qualified"] is False and outcome["stored_computed_at"]
    assert summary["written"] == 0 and summary["already_present"] == 1
    assert "dealer_target_ready" not in outcome
    assert stored(engine) == before and before["writer_provenance"] is None


def test_concurrent_insert_winner_keeps_its_own_record(engine, monkeypatch, capsys):
    original = M.store_market_state
    def race(eng, computed, **kwargs):
        winner = state()
        winner.spy_basis = "race-winner"
        original(eng, winner)
        return original(eng, computed, **kwargs)
    monkeypatch.setattr(M, "store_market_state", race)
    assert W.main(args(), engine) == 0
    outcome = output(capsys)["outcomes"][0]
    assert outcome["outcome"] == "already_present_race"
    assert not outcome["written_this_run"] and "dealer_target_ready" not in outcome
    assert stored(engine)["spy_basis"] == "race-winner"
    assert stored(engine)["writer_provenance"] is None


def test_default_mode_still_records_unknowns_not_false_readiness(engine, monkeypatch, capsys):
    monkeypatch.setattr(M, "compute_market_state", lambda e, t, **kw: state(t, None))
    assert W.main(args(), engine) == 0
    row = output(capsys)["outcomes"][0]
    assert row["outcome"] == "written" and not row["dealer_target_ready"]
    assert stored(engine)["gamma_state"]["status"] == "unavailable"


def test_dry_run_does_not_write_or_refresh_and_shows_required_gate(engine, monkeypatch, capsys):
    monkeypatch.setattr(M, "compute_market_state", lambda e, t, **kw: state(t, None))
    assert W.main(["--json", "--dry-run", "--require-target-dealer"], engine) == 0
    rec = output(capsys)
    assert rec["writer_provenance"]["dry_run"]
    assert rec["writer_provenance"]["target_dealer_required"]
    assert not rec["writer_provenance"]["readiness"]["dealer_target_ready"]
    assert not inspect(engine).has_table("market_state_daily")


@pytest.mark.parametrize("extra", [
    ["--as-of", "2026-11-14"], ["--as-of", "2026-11-15"],
    ["--as-of", "2026-11-08"], ["--from", "2026-11-13", "--to", "2026-11-12"],
    ["--require-target-dealer", "--replace"],
    ["--require-target-dealer", "--from", "2026-11-12", "--to", "2026-11-13"],
])
def test_invalid_or_mutating_strict_targets_refused_before_db(engine, extra):
    assert W.main(args(*extra), engine) == 2
    assert not inspect(engine).has_table("market_state_daily")


@pytest.mark.parametrize("today, target", [
    (date(2026, 11, 14), "2026-11-13"),  # Friday -> Saturday UTC
    (date(2026, 11, 2), "2026-10-30"),  # fall DST weekend
    (date(2026, 3, 9), "2026-03-06"),   # spring DST weekend
    (date(2026, 11, 28), "2026-11-27"), # early-close session
])
def test_default_target_is_frozen_prior_session(engine, monkeypatch, capsys, today, target):
    monkeypatch.setattr(W, "utc_today", lambda: today)
    assert W.main(args(), engine) == 0
    row = output(capsys)["outcomes"][0]
    assert row["target_session"] == target


def test_explicit_recovery_date_kept_not_relabelled_daily(engine, capsys):
    assert W.main(args("--as-of", "2026-11-12"), engine) == 0
    output(capsys)
    p = stored(engine)["writer_provenance"]
    assert p["mode"] == "explicit_session" and p["target_session"] == "2026-11-12"
    assert not p["readiness"]["forward_qualified"]
