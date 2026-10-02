"""Real PG14 short-transaction proof; providers are static, DB must be private."""
from __future__ import annotations

import os
import re
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pandas as pd
import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine.url import make_url

from ingestion import fred


@pytest.fixture
def pg(monkeypatch):
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        pytest.skip("GRID_TEST_DB_URL required for private PostgreSQL proof")
    parsed = make_url(url)
    if (parsed.host != "127.0.0.1" or parsed.username != "fred805_writer"
            or parsed.database != "fred805_safety_test" or not parsed.port):
        pytest.fail("FRED proof requires its own loopback fred805_safety_test database/role")
    admin = create_engine(url)
    schema = "fred805_" + uuid4().hex[:12]
    with admin.begin() as conn:
        assert not conn.execute(text("SELECT rolsuper FROM pg_roles WHERE rolname=current_user")).scalar_one()
        assert conn.execute(text("SELECT host(inet_server_addr())")).scalar_one() == "127.0.0.1"
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    options = {"options": f"-csearch_path={schema} -ctimezone=UTC"}
    writer = create_engine(url, connect_args=options)
    observer = create_engine(url, connect_args=options)
    ddl = (Path(__file__).resolve().parents[1] / "schema.sql").read_text()
    with writer.begin() as conn:
        for table in ("source_catalog", "raw_series"):
            conn.execute(text(re.search(rf"CREATE TABLE IF NOT EXISTS {table} \(.*?\n\);", ddl, re.S).group()))
        conn.execute(text(re.search(r"CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_series_composite\s+ON [^;]+;", ddl).group()))
        conn.execute(text(
            "INSERT INTO source_catalog (name, base_url, cost_tier, latency_class, pit_available, "
            "revision_behavior, trust_score, priority_rank) "
            "VALUES ('FRED','fixture-only','FREE','EOD',FALSE,'FREQUENT','HIGH',1)"
        ))
    monkeypatch.setattr(fred.time, "sleep", lambda *_args: None)
    try:
        yield writer, observer
    finally:
        writer.dispose()
        observer.dispose()
        with admin.begin() as conn:
            conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
        admin.dispose()


class WitnessEngine:
    """Runs actual writer SQL and observes rows only after actual COMMIT/ROLLBACK."""
    def __init__(self, writer, observer):
        self.writer, self.observer = writer, observer
        self.commits, self.rollbacks, self.batch_sizes = [], [], []
        self.active = False
        self.current_dml = 0
        self.fail_ack_at = None
        self.commit_attempts = 0
        self.cancel_once = False

        @event.listens_for(writer, "before_cursor_execute")
        def count_writes(_conn, _cursor, statement, parameters, _context, executemany):
            assert "SAVEPOINT" not in statement.upper()
            if statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")):
                assert self.active
                self.current_dml += len(parameters) if executemany else 1
                assert self.current_dml <= fred.STORE_BATCH_ROWS

        self.count_writes = count_writes

    def visible(self):
        with self.observer.connect() as conn:
            return conn.execute(text("SELECT count(*) FROM raw_series WHERE pull_status='SUCCESS'")).scalar_one()

    def connect(self):
        return WitnessConnection(self, self.writer.connect())

    def remove_listener(self):
        event.remove(self.writer, "before_cursor_execute", self.count_writes)


class WitnessConnection:
    def __init__(self, engine, conn):
        self.engine, self.conn = engine, conn

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()

    def begin(self):
        assert not self.engine.active
        self.engine.active, self.engine.current_dml = True, 0
        self.tx = self.conn.begin()
        return self

    def execute(self, statement, params):
        if "INSERT INTO raw_series" in str(statement) and self.engine.cancel_once:
            self.engine.cancel_once = False
            self.conn.exec_driver_sql("SET LOCAL statement_timeout='1ms'")
            self.conn.exec_driver_sql("SELECT pg_sleep(0.03)")  # real SQLSTATE 57014
        return self.conn.execute(statement, params)

    def commit(self):
        self.tx.commit()
        self.engine.active = False
        self.engine.batch_sizes.append(self.engine.current_dml)
        self.engine.commits.append(self.engine.visible())
        self.engine.commit_attempts += 1
        if self.engine.commit_attempts == self.engine.fail_ack_at:
            raise RuntimeError("fixture lost acknowledgement after actual server COMMIT")

    def rollback(self):
        self.tx.rollback()
        self.engine.active = False
        self.engine.batch_sizes.append(self.engine.current_dml)
        self.engine.rollbacks.append(self.engine.visible())

    def close(self):
        self.conn.close()


def puller_with_frame(monkeypatch, pg, count=121):
    writer, observer = pg
    # Constructor/source lookup uses production BasePuller and static FredAPI.
    monkeypatch.setattr(fred, "FredAPI", lambda _key: SimpleNamespace())
    puller = fred.FREDPuller("nonsecret-fixture", writer)
    witness = WitnessEngine(writer, observer)
    puller.engine = witness
    frame = pd.DataFrame({"value": list(range(count))}, index=pd.date_range("2026-01-01", periods=count))
    requests = []

    def fetch(sid, **kwargs):
        assert not witness.active
        requests.append((sid, kwargs))
        return frame.copy()

    puller.fred.get_series_observations = fetch
    return puller, witness, frame, requests


def test_pg_cold_boundary_visibility_provenance_and_revised_rerun(pg, monkeypatch):
    puller, witness, frame, requests = puller_with_frame(monkeypatch, pg)
    before = datetime.now(timezone.utc)
    out = puller.pull_series("DFF")
    after = datetime.now(timezone.utc)
    assert (out["status"], out["rows_inserted"]) == ("SUCCESS", 121)
    assert witness.commits == [50, 100, 121]
    assert witness.batch_sizes == [50, 50, 21]
    assert requests == [("DFF", {"observation_start": "1990-01-01"})]
    with pg[1].connect() as conn:
        rows = conn.execute(text(
            "SELECT series_id, source_id, obs_date, value, pull_status, pull_timestamp, raw_payload FROM raw_series ORDER BY obs_date"
        )).fetchall()
    assert all(r.series_id == "DFF" and r.source_id == puller.source_id and r.pull_status == "SUCCESS" for r in rows)
    assert all(before <= r.pull_timestamp <= after and r.raw_payload is None for r in rows)
    assert rows[0].obs_date == date(2026, 1, 1)
    original = [(r.obs_date, r.value, r.pull_timestamp) for r in rows]
    frame["value"] += 99
    again = puller.pull_all(["DFF"])
    assert again[0]["status"] == "SUCCESS" and again[0]["rows_inserted"] == 0
    assert requests[-1][1]["observation_start"] == "2026-04-24"  # latest May 1 minus 7 days
    with pg[1].connect() as conn:
        retained = conn.execute(text("SELECT obs_date,value,pull_timestamp FROM raw_series ORDER BY obs_date")).fetchall()
    assert [tuple(r) for r in retained] == original
    witness.remove_listener()


def test_pg_middle_constraint_error_rollback_fallback_and_failure_record(pg, monkeypatch):
    with pg[0].begin() as conn:
        conn.execute(text("ALTER TABLE raw_series ADD CONSTRAINT fred_fixture_bad_point CHECK(value <> 75)"))
    puller, witness, frame, _requests = puller_with_frame(monkeypatch, pg)
    out = puller.pull_series("DFF")
    assert (out["status"], out["rows_inserted"], out["rows_failed"]) == ("PARTIAL", 120, 1)
    assert witness.rollbacks == [50, 75]  # failed batch, then failed point
    assert witness.commits[0] == 50 and witness.commits[-2:] == [120, 120]
    assert max(witness.batch_sizes) <= 50 and witness.batch_sizes[-1] == 1
    with pg[1].connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM raw_series WHERE pull_status='FAILED'")).scalar_one() == 1
        assert conn.execute(text("SELECT count(*) FROM raw_series WHERE obs_date=:d AND pull_status='SUCCESS'"),
                            {"d": frame.index[75].date()}).scalar_one() == 0
    # Repaired provider point appends its first successful observation. Existing
    # successful values are still untouched even though all others are revised.
    frame.loc[frame.index[75], "value"] = 76
    again = puller.pull_series("DFF")
    assert (again["status"], again["rows_inserted"]) == ("SUCCESS", 1)
    assert witness.visible() == 121
    witness.remove_listener()


def test_pg_lost_commit_ack_has_known_lower_bound_and_no_replay(pg, monkeypatch):
    puller, witness, _frame, requests = puller_with_frame(monkeypatch, pg)
    witness.fail_ack_at = 2
    results = puller.pull_all(["DFF", "UNRATE"])
    assert (results[0]["status"], results[0]["rows_inserted"]) == ("PARTIAL", 50)
    assert results[0]["commit_outcome_unknown"] and results[0]["rows_inserted_total"] is None
    assert results[1]["aborted"] and results[1]["status"] == "SKIPPED"
    assert witness.commits == [50, 100] and witness.batch_sizes == [50, 50]
    assert len(requests) == 1 and witness.visible() == 100
    witness.remove_listener()


def test_pg_answered_query_cancellation_falls_back_after_clean_rollback(pg, monkeypatch):
    puller, witness, _frame, _requests = puller_with_frame(monkeypatch, pg, 3)
    witness.cancel_once = True
    out = puller.pull_series("DFF")
    assert (out["status"], out["rows_inserted"]) == ("SUCCESS", 3)
    assert witness.rollbacks == [0] and witness.commits == [1, 2, 3]
    assert max(witness.batch_sizes) == 1
    witness.remove_listener()
