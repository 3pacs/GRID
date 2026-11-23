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


# ── provisional record at the close (#1312, display only) ────────────────────
#
# Production never creates market_state_provisional: these tests create it with the reviewed migration's own DDL.

CLOSE_1113 = "2026-11-13T21:00:00+00:00"


def _migration():
    spec = importlib.util.spec_from_file_location(
        "market_state_prov_migration", Path(__file__).resolve().parents[1] / "migrations/versions/market_state_prov_20261122.py")
    mig = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mig)
    return mig


def _migrate(engine):
    mig = _migration()
    with engine.begin() as c:
        c.execute(text(mig.TABLE))
        c.execute(text(mig.GRANTS))


def _with_cutoff(s, known_by):
    s.source_lineage = {"availability_cutoff": {"at": known_by.isoformat(), "reads": dict(M.CUTOFF_READS)}}
    return s


@pytest.fixture()
def provisional_compute(monkeypatch):
    """compute_market_state as the real one reports a cutoff: only when it is given one (the provisional writer)."""
    monkeypatch.setattr(M, "compute_market_state", lambda e, t, **kw: _with_cutoff(state(t), kw["known_by"])
                        if kw.get("known_by") is not None else state(t))


def test_provisional_store_refuses_while_the_migration_is_not_applied(engine, monkeypatch, provisional_compute):
    from datetime import datetime, timezone

    monkeypatch.setattr(W, "utc_now", lambda: datetime(2026, 11, 13, 21, 15, tzinfo=timezone.utc))
    with pytest.raises(RuntimeError, match="apply alembic migration market_state_prov_20261122"):
        W.main(["--provisional", "--json"], engine)
    with pytest.raises(RuntimeError, match="production never creates it"):
        M.ensure_provisional_table(engine)
    assert not inspect(engine).has_table("market_state_provisional")


def test_provisional_table_is_jsonb_timestamptz_and_provisional_only(engine, monkeypatch, capsys, provisional_compute):
    from datetime import datetime, timezone

    from sqlalchemy.exc import IntegrityError

    _migrate(engine)
    monkeypatch.setattr(W, "utc_now", lambda: datetime(2026, 11, 13, 21, 15, tzinfo=timezone.utc))  # after 21:00Z
    assert W.main(["--provisional", "--json"], engine) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["written"] == 1 and out["target_session"] == "2026-11-13"
    with engine.connect() as c:
        row = c.execute(text("SELECT record, computed_at, provisional, pg_typeof(record)::text AS rt, "
                             "pg_typeof(computed_at)::text AS ct FROM market_state_provisional")).mappings().one()
    assert row["rt"] == "jsonb" and row["ct"] == "timestamp with time zone" and row["provisional"] is True
    assert row["computed_at"].utcoffset() is not None
    assert row["record"]["provisional"] is True and row["record"]["decision_use"] == "display_only"
    assert row["record"]["writer_provenance"]["mode"] == "provisional"
    assert row["record"]["availability_cutoff"] == CLOSE_1113
    assert not inspect(engine).has_table("market_state_daily")
    with pytest.raises(IntegrityError), engine.begin() as c:
        c.execute(text("INSERT INTO market_state_provisional (as_of_date, rule, record, provisional) "
                       "VALUES ('2026-11-12', 'market-state/2', CAST('{}' AS JSONB), FALSE)"))
    assert W.main(["--provisional", "--json"], engine) == 0       # insert-only
    assert json.loads(capsys.readouterr().out)["already_present"] == 1
    with engine.begin() as c:                                    # the GRANT footer ran (the role exists here)
        assert c.execute(text("SELECT has_table_privilege('grid', 'market_state_provisional', 'INSERT')")).scalar()


def test_desk_brief_reads_the_provisional_record_in_its_read_only_transaction(engine, monkeypatch, capsys,
                                                                               provisional_compute):
    from datetime import datetime, timedelta, timezone

    from analysis import desk_brief_sources as S
    from analysis.desk_brief import SessionTimes

    _migrate(engine)
    assert W.main(["--json", "--no-refresh"], engine) == 0                       # decision record for 2026-11-13
    monkeypatch.setattr(W, "utc_now", lambda: datetime(2026, 11, 16, 21, 15, tzinfo=timezone.utc))
    assert W.main(["--provisional", "--json"], engine) == 0                      # provisional for 2026-11-16
    capsys.readouterr()
    session = SessionTimes(date(2026, 11, 16), datetime(2026, 11, 16, 14, 30, tzinfo=timezone.utc),
                           datetime(2026, 11, 16, 21, 0, tzinfo=timezone.utc))
    as_of = datetime.now(timezone.utc) + timedelta(seconds=1)   # both rows carry CURRENT_TIMESTAMP
    sec = S.market_state(engine, "post_close", session, as_of)
    assert sec.status == "ok" and sec.facts["record_date"] == "2026-11-13"
    assert sec.facts["provisional"]["record_date"] == "2026-11-16"
    assert sec.facts["provisional"]["availability_cutoff"] == "2026-11-16T21:00:00Z"
    assert any(line.startswith("PROVISIONAL (display only): record for session 2026-11-16") for line in sec.lines)
    assert "market_state_provisional" in sec.source and not sec.problems


def test_cutoff_reads_on_postgres_column_types(engine):
    """fred_vintages.fetched_at is a naive TIMESTAMP (UTC session); dealer_gex_daily.created_at is TIMESTAMPTZ."""
    from datetime import datetime, timezone

    from store import fred_vintages as FV

    close = datetime(2026, 11, 20, 21, 0, tzinfo=timezone.utc)
    FV.ensure_table(engine)
    with engine.begin() as c:
        c.execute(text("CREATE TABLE dealer_gex_daily (obs_date DATE, ticker TEXT, spot_price DOUBLE PRECISION, "
                       "net_gex_usd_m DOUBLE PRECISION, spot_to_flip_pct DOUBLE PRECISION, "
                       "gamma_flip_strike DOUBLE PRECISION, gex_regime TEXT, created_at TIMESTAMPTZ DEFAULT NOW())"))
        c.execute(text("INSERT INTO dealer_gex_daily VALUES ('2026-11-19', 'SPY', 600, -1, -1, 606, 'S', "
                       "'2026-11-19 22:00:00+00'), ('2026-11-20', 'SPY', 600, -1, -1, 606, 'L', "
                       "'2026-11-20 17:30:00-05')")) # 22:30Z, written after the close
        c.execute(text("INSERT INTO fred_vintages (series_id, obs_date, realtime_start, realtime_end, value, fetched_at) "
                       "VALUES ('USEPUINDXD', '2026-11-18', '2026-11-19', '2026-11-19', 1, '2026-11-19 23:15:00'), "
                       "('USEPUINDXD', '2026-11-19', '2026-11-20', '9999-12-31', 2, '2026-11-20 23:15:00')"))
    reads = M._Reads(engine, date(2026, 11, 20), 30, known_by=close)
    assert [str(d) for d in reads.gex_rows().index] == ["2026-11-19"]
    assert len(M._Reads(engine, date(2026, 11, 20), 30).gex_rows()) == 2
    window = ("USEPUINDXD", date(2026, 11, 1), date(2026, 11, 20), date(2026, 11, 20))
    cut = FV.load(engine, *window, fetched_by=close)
    assert list(cut["value"]) == [1.0] and FV.values_as_of(cut, [date(2026, 11, 18)], date(2026, 11, 20)) == {
        date(2026, 11, 18): 1.0}
    assert len(FV.load(engine, *window)) == 2
    # The refresh after the close extends the first version's end in place (real upsert on Postgres): the read cut
    # at the close is unchanged.
    FV.upsert(engine, "USEPUINDXD", [{"obs_date": date(2026, 11, 18), "realtime_start": date(2026, 11, 19),
                                      "realtime_end": FV.OPEN_END, "value": 1.0}])
    with engine.connect() as c:
        end, fetched = c.execute(text("SELECT realtime_end, fetched_at FROM fred_vintages "
                                      "WHERE obs_date = '2026-11-18'")).one()
    assert end == FV.OPEN_END and fetched == datetime(2026, 11, 19, 23, 15)  # noqa: DTZ001 - fetched_at is naive
    assert FV.load(engine, *window, fetched_by=close).equals(cut)
