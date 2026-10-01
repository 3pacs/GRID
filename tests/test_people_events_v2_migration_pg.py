"""Real-PostgreSQL proof for migrations/versions/people_events_v2_20261001.py and the writer.

Throwaway schema per test (dropped afterwards), built from the chain
people_events_20260927 -> security_master_20260927 -> people_events_v2_20261001.
Proves the append-only contract the design doc relies on:

* security_id is TEXT with a real FK onto security_master(entity_id);
* the upgrade refuses a table that already holds BIGINT security_id values;
* DELETE/TRUNCATE are refused, descriptive content is immutable, known_at
  only moves earlier, every UPDATE lands in people_event_revisions (which is
  itself append-only), an idempotent re-upsert logs nothing;
* one *current* row per (channel, dedup_key) -- a superseded version stays;
* people_events_runs refuses SUCCESS with nothing written;
* the writer applies a plan, a second identical run writes nothing
  (NO_NEW_ROWS), and a supersession leaves exactly one visible version;
* downgrade works on a clean table and refuses to collapse versions.
"""

from __future__ import annotations

import importlib
import json
from datetime import date, datetime, timezone
from uuid import uuid4

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError

CHAIN = (
    "migrations.versions.people_events_20260927",
    "migrations.versions.security_master_20260927",
    "migrations.versions.people_events_v2_20261001",
)
UTC = timezone.utc


def _run(engine: Engine, module: str, fn: str = "upgrade") -> None:
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    migration = importlib.import_module(module)
    with engine.connect() as conn:
        trans = conn.begin()
        real_op = migration.op
        migration.op = Operations(MigrationContext.configure(conn))
        try:
            getattr(migration, fn)()
        finally:
            migration.op = real_op
        trans.commit()


@pytest.fixture()
def schema_engine(pg_engine: Engine):
    schema = f"people_events_v2_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url, connect_args={"options": f"-csearch_path={schema}"})
    try:
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


@pytest.fixture()
def v2(schema_engine: Engine) -> Engine:
    for module in CHAIN:
        _run(schema_engine, module)
    with schema_engine.begin() as conn:
        conn.execute(text("INSERT INTO security_master (entity_id, cik, name, source) "
                          "VALUES ('sm_0000320193', 320193, 'Apple Inc.', 'test')"))
    return schema_engine


_ROW = """
    INSERT INTO people_events (channel, dedup_key, event_time, known_at, known_at_basis, actor_id,
        actor_id_basis, actor_type, source, source_refs, security_id)
    VALUES ('form4', :key, '2026-04-01', :known, 'filing', 'X', 'owner_cik', 'insider', 'sec_form345',
        CAST(:refs AS jsonb), :sid)
    RETURNING id
"""


def _insert(engine: Engine, key: str = "k1", known: str = "2026-04-04T02:00:00Z", sid: str | None = None,
            refs: str = '[{"source": "sec_form345", "source_record_id": "a:1"}]') -> int:
    with engine.begin() as conn:
        return conn.execute(text(_ROW), {"key": key, "known": known, "refs": refs, "sid": sid}).scalar()


def _raises(engine: Engine, sql: str, params: dict | None = None) -> None:
    with pytest.raises(DBAPIError):
        with engine.begin() as conn:
            conn.execute(text(sql), params or {})


def test_security_id_is_text_with_fk(v2):
    with v2.connect() as conn:
        dtype = conn.execute(text(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = 'people_events' AND column_name = 'security_id'"
        )).scalar()
    assert dtype == "text"
    _insert(v2, sid="sm_0000320193")
    with pytest.raises(DBAPIError):
        _insert(v2, key="k2", sid="sm_9999999999")


def test_upgrade_refuses_existing_bigint_security_ids(schema_engine):
    _run(schema_engine, CHAIN[0])
    _run(schema_engine, CHAIN[1])
    with schema_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO people_events (channel, dedup_key, event_time, known_at, known_at_basis, actor_id, "
            "actor_id_basis, actor_type, source, security_id) VALUES ('form4', 'k', NOW(), NOW(), 'filing', "
            "'X', 'owner_cik', 'insider', 's', 42)"))
    with pytest.raises(RuntimeError, match="security_id"):
        _run(schema_engine, CHAIN[2])


def test_append_only_guards(v2):
    eid = _insert(v2)
    _raises(v2, "DELETE FROM people_events WHERE id = :i", {"i": eid})
    _raises(v2, "TRUNCATE people_events CASCADE")
    _raises(v2, "UPDATE people_events SET actor_id = 'Y' WHERE id = :i", {"i": eid})
    _raises(v2, "UPDATE people_events SET known_at = known_at + interval '1 day' WHERE id = :i", {"i": eid})
    _raises(v2, "UPDATE people_events SET source_refs = '[]'::jsonb WHERE id = :i", {"i": eid})
    with v2.begin() as conn:
        conn.execute(text("UPDATE people_events SET known_at = known_at - interval '1 hour', "
                          "known_at_basis = 'first_seen' WHERE id = :i"), {"i": eid})
        conn.execute(text("UPDATE people_events SET known_at = known_at WHERE id = :i"), {"i": eid})  # no-op
        revs = conn.execute(text("SELECT op FROM people_event_revisions WHERE event_id = :i"), {"i": eid}).fetchall()
    assert [r[0] for r in revs] == ["tighten_known_at"]
    _raises(v2, "DELETE FROM people_event_revisions")
    _raises(v2, "UPDATE people_event_revisions SET op = 'other'")


def test_one_current_version_per_key(v2):
    eid = _insert(v2)
    with pytest.raises(DBAPIError):
        _insert(v2)
    with v2.begin() as conn:
        conn.execute(text("UPDATE people_events SET superseded_at = NOW() WHERE id = :i"), {"i": eid})
    _insert(v2)  # a new current version of the same key
    _raises(v2, "UPDATE people_events SET superseded_at = NULL WHERE id = :i", {"i": eid})


def test_runs_refuse_success_without_rows(v2):
    _raises(v2, "INSERT INTO people_events_runs (run_id, mode, materializer_version, status, counts) "
                "VALUES ('r', 'incremental', 'v', 'SUCCESS', '{\"written\": 0}'::jsonb)")
    with v2.begin() as conn:
        conn.execute(text("INSERT INTO people_events_runs (run_id, mode, materializer_version, status, counts) "
                          "VALUES ('r', 'incremental', 'v', 'NO_NEW_ROWS', '{\"written\": 0}'::jsonb)"))


def test_fara_channel_accepted(v2):
    with v2.begin() as conn:
        conn.execute(text(
            "INSERT INTO people_events (channel, dedup_key, event_time, known_at, known_at_basis, actor_id, "
            "actor_id_basis, actor_type, source) VALUES ('fara', 'f', NOW(), NOW(), 'first_seen', 'R', "
            "'normalized_name', 'foreign_agent', 'fara')"))


def test_downgrade_clean_and_refuses_versions(v2):
    eid = _insert(v2)
    with v2.begin() as conn:
        conn.execute(text("UPDATE people_events SET superseded_at = NOW() WHERE id = :i"), {"i": eid})
    with pytest.raises(RuntimeError, match="superseded"):
        _run(v2, CHAIN[2], "downgrade")


def test_downgrade_on_clean_table(v2):
    _insert(v2)
    _run(v2, CHAIN[2], "downgrade")
    with v2.connect() as conn:
        dtype = conn.execute(text(
            "SELECT data_type FROM information_schema.columns WHERE table_schema = current_schema() "
            "AND table_name = 'people_events' AND column_name = 'security_id'")).scalar()
    assert dtype == "bigint"


# --- writer --------------------------------------------------------------------------------


def _sec(**over):
    row = dict(accession_number="acc-1", document_type="4", amended=False, filing_date="2026-04-03",
               issuer_cik="0000320193", issuer_ticker="AAPL", owner_cik="0001214156", owner_name="COOK TIMOTHY D",
               is_director=False, is_officer=True, is_ten_pct_owner=False, nonderiv_trans_sk="1",
               transaction_date="2026-04-01", transaction_date_raw="01-APR-2026", transaction_code="P",
               shares=1000.0, price_per_share=200.0, acquired_disposed_code="A")
    row.update(over)
    return row


def _plan(engine: Engine, rows: list[dict], observed: datetime):
    from intelligence.people_events_pipeline import adapters as A
    from intelligence.people_events_pipeline import merge as M
    from intelligence.people_events_pipeline import plan as P
    from intelligence.people_events_pipeline import security as S

    cands, _ = A.form4_from_form345(pd.DataFrame(rows))
    ids = pd.DataFrame([{"entity_id": "sm_0000320193", "id_scheme": "cik", "id_value": "320193",
                         "valid_from": "2026-09-27", "valid_to": None, "is_primary": True, "conflict_flag": False}])
    events = S.resolve_securities(M.merge_candidates(cands).events, ids)
    with engine.connect() as conn:
        stored = pd.read_sql(text("SELECT channel, dedup_key, known_at, known_at_basis, source_refs, content_hash, "
                                  "superseded_at, retracted_at FROM people_events"), conn)
    return events, P.build_write_plan(events, stored, pd.Timestamp(observed))


def test_writer_idempotent_and_supersedes(v2):
    from intelligence.people_events_pipeline.writer import apply_write_plan
    from store.people_events import read_events

    t0 = datetime(2026, 10, 1, 12, tzinfo=UTC)
    ev, plan = _plan(v2, [_sec()], t0)
    out = apply_write_plan(v2, ev, plan, run_id="r1", mode="backfill", observed_at=t0)
    assert out["status"] == "SUCCESS" and out["counts"]["insert"] == 1

    ev, plan = _plan(v2, [_sec()], t0)
    out = apply_write_plan(v2, ev, plan, run_id="r2", mode="incremental", observed_at=t0)
    assert out["status"] == "NO_NEW_ROWS" and out["counts"]["unchanged"] == 1

    t1 = datetime(2026, 10, 6, 12, tzinfo=UTC)
    ev, plan = _plan(v2, [_sec(price_per_share=250.0)], t1)
    out = apply_write_plan(v2, ev, plan, run_id="r3", mode="incremental", observed_at=t1)
    assert out["counts"]["supersede"] == 1
    with v2.connect() as conn:
        rows = conn.execute(text("SELECT id, security_id, superseded_by, superseded_at, size_usd, confidence "
                                 "FROM people_events ORDER BY id")).fetchall()
        statuses = [r[0] for r in conn.execute(text("SELECT status FROM people_events_runs ORDER BY started_at"))]
    assert len(rows) == 2 and rows[0][2] == rows[1][0] and rows[1][1] == "sm_0000320193"
    # The original (filing basis, owner CIK, resolved issuer) is high confidence;
    # the correction is only first_seen by this run, so it is low.
    assert rows[0][5] == "high" and rows[1][5] == "low"
    assert statuses == ["SUCCESS", "NO_NEW_ROWS", "SUCCESS"]
    before = read_events(v2, as_of=datetime(2026, 10, 2, tzinfo=UTC))
    after = read_events(v2, as_of=datetime(2026, 10, 7, tzinfo=UTC))
    assert len(before) == 1 and before[0].size_usd == 200_000.0
    assert len(after) == 1 and after[0].size_usd == 250_000.0
    assert json.loads(json.dumps(after[0].provenance))["act_known_at"].startswith("2026-04-04")
    assert read_events(v2, as_of=datetime(2026, 4, 4, 1, 59, tzinfo=UTC)) == []
    assert date(2026, 4, 1) == before[0].event_time.date()
