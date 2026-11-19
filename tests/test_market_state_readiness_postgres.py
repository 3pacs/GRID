"""Opt-in real PostgreSQL writer tests; disposable Unix-socket database only."""
import importlib.util
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path
import threading
import uuid

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url

from intelligence.regime import market_state as M

SPEC = importlib.util.spec_from_file_location(
    "market_state_pg_writer", Path(__file__).resolve().parents[1] / "scripts/run_market_state.py")
W = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(W)
DAY = date(2026, 11, 13)


@pytest.fixture()
def engine():
    raw = os.environ.get("GRID_STATE_TEST_DB_URL")
    if not raw:
        pytest.skip("requires disposable GRID_STATE_TEST_DB_URL")
    url = make_url(raw)
    host = url.query.get("host", "")
    assert url.database == "grid_state_test" and not url.host
    assert host.startswith("/tmp/grid-state-pg.") and host.endswith("/socket")
    assert url.query.get("port") == "55432"
    admin = create_engine(url)
    schema = "state_test_" + uuid.uuid4().hex
    with admin.begin() as conn:
        assert conn.execute(text("SHOW listen_addresses")).scalar_one() == ""
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    eng = create_engine(url, connect_args={"options": f"-c search_path={schema} -c timezone=UTC"})
    try:
        yield eng
    finally:
        eng.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()


@pytest.fixture(autouse=True)
def no_provider(monkeypatch):
    monkeypatch.setattr(W, "utc_today", lambda: date(2026, 11, 14))
    monkeypatch.setattr(M, "load_calendar", lambda: {})
    monkeypatch.setattr(M, "compute_market_state", lambda e, t, **kw: state(t))


def state(day=DAY, dealer=DAY):
    s = M.MarketState(day)
    s.gamma_state = ({"status": "ok", "obs_date": dealer.isoformat()} if dealer else
                     {"status": "unavailable", "reason": "synthetic missing input"})
    s.variables = {"vix": {"status": "ok", "obs_date": "2026-11-12", "age_days": 1}}
    return s


def record(engine):
    with engine.connect() as c:
        return c.execute(text("SELECT record, computed_at FROM market_state_daily")).mappings().one()


def test_actual_jsonb_persists_run_source_metadata(engine, capsys):
    assert W.main(["--json", "--no-refresh", "--require-target-dealer"], engine) == 0
    out = json.loads(capsys.readouterr().out)
    row = record(engine)
    assert isinstance(row["record"], dict)
    provenance = row["record"]["writer_provenance"]
    assert provenance["run_id"] == out["run_id"]
    assert provenance["target_session"] == "2026-11-13"
    assert provenance["cutoff_exclusive"] == "2026-11-14T00:00:00+00:00"
    assert len(provenance["code_sha256"]["store/observations.py"]) == 64
    assert provenance["readiness"]["forward_qualified"] is False
    assert out["outcomes"][0]["written_this_run"]


def test_backfill_timestamp_and_jsonb_never_relabelled(engine, monkeypatch, capsys):
    M.store_market_state(engine, state())
    with engine.begin() as c:
        c.execute(text("UPDATE market_state_daily SET computed_at = '2026-11-13 12:12:14'"))
    before = record(engine)
    monkeypatch.setattr(M, "compute_market_state", lambda *a, **kw: pytest.fail("duplicate recomputed"))
    assert W.main(["--json", "--no-refresh", "--require-target-dealer"], engine) == 0
    out = json.loads(capsys.readouterr().out)
    assert record(engine) == before
    assert out["written"] == 0 and out["already_present"] == 1
    assert out["outcomes"][0]["stored_computed_at"] == "2026-11-13 12:12:14"
    assert out["outcomes"][0]["forward_qualified"] is False


@pytest.mark.parametrize("dealer", [None, date(2026, 11, 12)])
def test_strict_missing_or_wrong_session_never_inserts(engine, monkeypatch, capsys, dealer):
    monkeypatch.setattr(M, "compute_market_state", lambda e, t, **kw: state(t, dealer))
    assert W.main(["--json", "--no-refresh", "--require-target-dealer"], engine) == 3
    out = json.loads(capsys.readouterr().out)
    assert out["held"] == 1 and out["written"] == 0
    assert not inspect(engine).has_table("market_state_daily")


def test_actual_two_connection_conflict_has_one_writer(engine, monkeypatch, capsys):
    M.ensure_table(engine)
    barrier = threading.Barrier(2)
    def both_saw_absent(*a):
        barrier.wait(timeout=10)
        return None
    monkeypatch.setattr(W, "existing_row", both_saw_absent)
    with ThreadPoolExecutor(max_workers=2) as pool:
        jobs = [pool.submit(W.main, ["--json", "--no-refresh", "--require-target-dealer"], engine)
                for _ in range(2)]
        assert [j.result(timeout=20) for j in jobs] == [0, 0]
    outs = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert len(outs) == 2
    assert sorted(o["outcomes"][0]["outcome"] for o in outs) == ["already_present_race", "written"]
    winner = next(o for o in outs if o["written"] == 1)
    assert record(engine)["record"]["writer_provenance"]["run_id"] == winner["run_id"]
    with engine.connect() as c:
        assert c.execute(text("SELECT count(*) FROM market_state_daily")).scalar_one() == 1


def test_dry_run_creates_no_table(engine, capsys):
    assert W.main(["--json", "--dry-run", "--require-target-dealer"], engine) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["writer_provenance"]["dry_run"]
    assert not inspect(engine).has_table("market_state_daily")


def test_compute_failure_leaves_no_half_written_row(engine, monkeypatch):
    def fail(*a, **kw):
        raise RuntimeError("synthetic computation failure")
    monkeypatch.setattr(M, "compute_market_state", fail)
    with pytest.raises(RuntimeError, match="synthetic"):
        W.main(["--json", "--no-refresh", "--require-target-dealer"], engine)
    assert not inspect(engine).has_table("market_state_daily")


def test_range_preserves_existing_and_writes_only_missing(engine, capsys):
    prior = state(date(2026, 11, 12), date(2026, 11, 12))
    M.store_market_state(engine, prior)
    before = record(engine)
    assert W.main(["--json", "--no-refresh", "--from", "2026-11-12", "--to", "2026-11-13"], engine) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["written"] == 1 and out["already_present"] == 1
    with engine.connect() as c:
        rows = c.execute(text("SELECT record, computed_at FROM market_state_daily ORDER BY as_of_date")).mappings().all()
    assert rows[0] == before
    assert rows[1]["record"]["writer_provenance"]["mode"] == "backfill"
    assert not rows[1]["record"]["writer_provenance"]["readiness"]["forward_qualified"]
