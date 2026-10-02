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
from datetime import datetime, timezone
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
        conn.execute(text("UPDATE people_events SET superseded_at = '2026-05-01T00:00:00Z' WHERE id = :i"),
                     {"i": eid})
    _insert(v2, known="2026-05-01T00:00:00Z")  # a new current version, visible from the supersession on
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


def test_upgrade_refuses_a_non_empty_table(schema_engine):
    _run(schema_engine, CHAIN[0])
    _run(schema_engine, CHAIN[1])
    with schema_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO people_events (channel, dedup_key, event_time, known_at, known_at_basis, actor_id, "
            "actor_id_basis, actor_type, source) VALUES ('form4', 'k', NOW(), NOW(), 'filing', "
            "'X', 'normalized_name', 'insider', 's')"))
    with pytest.raises(RuntimeError, match="pre-v2 rows"):
        _run(schema_engine, CHAIN[2])


def test_version_floor_is_enforced_by_the_database(v2):
    eid = _insert(v2, known="2026-04-04T02:00:00Z")
    with v2.begin() as conn:
        conn.execute(text("UPDATE people_events SET retracted_at = '2026-06-01T00:00:00Z' WHERE id = :i"), {"i": eid})
    # A re-appearing act may not be visible before the retraction ...
    with pytest.raises(DBAPIError):
        _insert(v2, known="2026-04-04T02:00:00Z")
    # ... and is fine from the retraction on; it then cannot be tightened below it.
    new_id = _insert(v2, known="2026-06-01T00:00:00Z")
    _raises(v2, "UPDATE people_events SET known_at = '2026-05-01T00:00:00Z' WHERE id = :i", {"i": new_id})


def test_identity_enrichment_only_from_name_to_stable_id(v2):
    with v2.begin() as conn:
        eid = conn.execute(text(
            "INSERT INTO people_events (channel, dedup_key, event_time, known_at, known_at_basis, actor_id, "
            "actor_id_basis, actor_type, source) VALUES ('form4', 'n', NOW(), NOW(), 'filing', 'COOK_TIMOTHY', "
            "'normalized_name', 'insider', 'quiverquant') RETURNING id")).scalar()
        conn.execute(text("UPDATE people_events SET actor_id = '0001214156', actor_id_basis = 'owner_cik', "
                          "entity_cik = '0000320193' WHERE id = :i"), {"i": eid})
        op = conn.execute(text("SELECT op FROM people_event_revisions WHERE event_id = :i"), {"i": eid}).scalar()
    assert op == "enrich_identity"
    _raises(v2, "UPDATE people_events SET actor_id = '0000000001' WHERE id = :i", {"i": eid})
    _raises(v2, "UPDATE people_events SET entity_cik = '0000000002' WHERE id = :i", {"i": eid})


def test_downgrade_refuses_when_revision_history_exists(v2):
    eid = _insert(v2)
    with v2.begin() as conn:
        conn.execute(text("UPDATE people_events SET known_at = known_at - interval '1 hour', "
                          "known_at_basis = 'first_seen' WHERE id = :i"), {"i": eid})
    with pytest.raises(RuntimeError, match="history"):
        _run(v2, CHAIN[2], "downgrade")


# --- writer --------------------------------------------------------------------------------


def _sec(**over):
    row = {"accession_number": "acc-1", "document_type": "4", "amended": False, "filing_date": "2026-04-03",
           "issuer_cik": "0000320193", "issuer_ticker": "AAPL", "owner_cik": "0001214156",
           "owner_name": "COOK TIMOTHY D", "is_director": False, "is_officer": True, "is_ten_pct_owner": False,
           "nonderiv_trans_sk": "1", "transaction_date": "2026-04-01", "transaction_date_raw": "01-APR-2026",
           "transaction_code": "P", "shares": 1000.0, "price_per_share": 200.0, "acquired_disposed_code": "A"}
    row.update(over)
    return row


def _holdings(q1_shares):
    base = {"cik": "1001", "holder_name": "Big Fund LLC", "ticker": "AAPL", "cusip": "037833100",
            "source": "sec_13f_live", "created_at": "2026-05-01"}
    return [{**base, "id": 1, "shares_held": 100, "value_usd": 20_000.0, "report_date": "2025-12-31",
             "filed_date": "2026-02-10"},
            {**base, "id": 2, "shares_held": q1_shares, "value_usd": q1_shares * 200.0, "report_date": "2026-03-31",
             "filed_date": "2026-05-12"}]


_IDS = pd.DataFrame([{"entity_id": "sm_0000320193", "id_scheme": "cik", "id_value": "320193",
                      "valid_from": "2026-09-27", "valid_to": None, "is_primary": True, "conflict_flag": False}])


def _plan(engine: Engine, observed: datetime, form345=None, holdings=None, signal_sources=None):
    from intelligence.people_events_pipeline import adapters as A
    from intelligence.people_events_pipeline import merge as M
    from intelligence.people_events_pipeline import plan as P
    from intelligence.people_events_pipeline import security as S

    parts = []
    if form345:
        parts.append(A.form4_from_form345(pd.DataFrame(form345))[0])
    if holdings:
        parts.append(A.thirteen_f_changes(pd.DataFrame(holdings))[0])
    if signal_sources:
        parts.append(A.from_signal_sources(pd.DataFrame(signal_sources), observed)[0])
    events = S.resolve_securities(M.merge_candidates(pd.concat(parts, ignore_index=True)).events, _IDS)
    with engine.connect() as conn:
        stored = pd.read_sql(text("SELECT channel, dedup_key, known_at, known_at_basis, source_refs, content_hash, "
                                  "actor_id, actor_id_basis, entity_cik, superseded_at, retracted_at "
                                  "FROM people_events"), conn)
    return events, P.build_write_plan(events, stored, pd.Timestamp(observed))


def test_writer_idempotent_supersedes_and_keeps_one_visible_version(v2):
    from intelligence.people_events_pipeline.writer import apply_write_plan
    from store.people_events import read_events

    t0 = datetime(2026, 9, 1, 12, tzinfo=UTC)
    ev, plan = _plan(v2, t0, form345=[_sec()], holdings=_holdings(150))
    out = apply_write_plan(v2, ev, plan, run_id="r1", mode="backfill", observed_at=t0)
    assert out["status"] == "SUCCESS" and out["counts"]["insert"] == 2

    ev, plan = _plan(v2, t0, form345=[_sec()], holdings=_holdings(150))
    out = apply_write_plan(v2, ev, plan, run_id="r2", mode="incremental", observed_at=t0)
    assert out["status"] == "NO_NEW_ROWS" and out["counts"]["unchanged"] == 2

    t1 = datetime(2026, 9, 6, 12, tzinfo=UTC)
    ev, plan = _plan(v2, t1, form345=[_sec()], holdings=_holdings(175))  # 13F amendment
    out = apply_write_plan(v2, ev, plan, run_id="r3", mode="incremental", observed_at=t1)
    assert out["counts"]["supersede"] == 1
    # A fourth, identical run changes nothing and never pulls the correction back.
    t2 = datetime(2026, 9, 7, 12, tzinfo=UTC)
    ev, plan = _plan(v2, t2, form345=[_sec()], holdings=_holdings(175))
    out = apply_write_plan(v2, ev, plan, run_id="r4", mode="incremental", observed_at=t2)
    assert out["status"] == "NO_NEW_ROWS"

    with v2.connect() as conn:
        rows = conn.execute(text("SELECT id, superseded_by, known_at, confidence FROM people_events "
                                 "WHERE channel = 'thirteen_f' ORDER BY id")).fetchall()
        f4 = conn.execute(text("SELECT security_id, confidence FROM people_events WHERE channel = 'form4'")).one()
        statuses = [r[0] for r in conn.execute(text("SELECT status FROM people_events_runs ORDER BY started_at"))]
    assert len(rows) == 2 and rows[0][1] == rows[1][0] and rows[1][2] == t1 and rows[1][3] == "low"
    assert f4 == ("sm_0000320193", "high")
    assert statuses == ["SUCCESS", "NO_NEW_ROWS", "SUCCESS", "NO_NEW_ROWS"]
    for as_of in (datetime(2026, 5, 14, tzinfo=UTC), datetime(2026, 9, 2, tzinfo=UTC),
                  datetime(2026, 9, 6, 12, tzinfo=UTC), datetime(2026, 9, 30, tzinfo=UTC)):
        assert len(read_events(v2, as_of=as_of, channel="thirteen_f")) == 1
    assert read_events(v2, as_of=datetime(2026, 9, 30, tzinfo=UTC), channel="thirteen_f")[0].provenance[
        "act_known_at"].startswith("2026-05-13")


def test_writer_enriches_a_live_feed_row_when_sec_arrives(v2):
    from intelligence.people_events_pipeline.writer import apply_write_plan

    qq = {"id": 5, "source_type": "quiverquant:insider", "source_id": "qq_insider_trading", "ticker": "AAPL",
          "signal_date": "2026-04-01", "signal_type": "insider_buy", "created_at": "2026-04-04T00:00:00Z",
          "signal_value": {"Name": "Timothy D. Cook", "TransactionCode": "P", "Shares": 1000,
                           "PricePerShare": 200, "fileDate": "2026-04-03"}}
    t0 = datetime(2026, 4, 5, 12, tzinfo=UTC)
    ev, plan = _plan(v2, t0, signal_sources=[qq])
    apply_write_plan(v2, ev, plan, run_id="q1", mode="incremental", observed_at=t0)
    t1 = datetime(2026, 7, 15, 12, tzinfo=UTC)
    ev, plan = _plan(v2, t1, form345=[_sec()], signal_sources=[qq])
    out = apply_write_plan(v2, ev, plan, run_id="s1", mode="backfill", observed_at=t1)
    assert out["counts"]["enrich_identity"] == 1 and out["counts"]["supersede"] == 0
    with v2.connect() as conn:
        row = conn.execute(text("SELECT actor_id, actor_id_basis, entity_cik, n_sources FROM people_events")).one()
    assert row == ("0001214156", "owner_cik", "0000320193", 2)


def test_read_event_versions_serves_every_decision_time_like_read_events(v2):
    from intelligence.people_events_pipeline import plan as P
    from intelligence.people_events_pipeline.writer import apply_write_plan
    from store.people_events import read_event_versions, read_events

    t0 = datetime(2026, 9, 1, 12, tzinfo=UTC)
    ev, plan = _plan(v2, t0, holdings=_holdings(150))
    apply_write_plan(v2, ev, plan, run_id="h1", mode="backfill", observed_at=t0)
    t1 = datetime(2026, 9, 6, 12, tzinfo=UTC)
    ev, plan = _plan(v2, t1, holdings=_holdings(175))
    apply_write_plan(v2, ev, plan, run_id="h2", mode="incremental", observed_at=t1)

    known_by = datetime(2026, 9, 30, tzinfo=UTC)
    versions = pd.DataFrame(read_event_versions(v2, known_by, channel="thirteen_f"))
    assert len(versions) == 2  # the superseded version is returned, not hidden
    for t in pd.date_range("2026-05-01", "2026-09-29", freq="1D", tz="UTC"):
        got = P.visible_at(versions, t)
        want = read_events(v2, as_of=t.to_pydatetime(), channel="thirteen_f")
        assert len(got) == len(want) <= 1
        if want:
            assert got.iloc[0]["size_usd"] == want[0].size_usd
    # Known by a date before the supersession: the later end is masked, not leaked.
    early = pd.DataFrame(read_event_versions(v2, datetime(2026, 9, 3, tzinfo=UTC), channel="thirteen_f"))
    assert len(early) == 1 and pd.isna(early.iloc[0]["superseded_at"])


def test_writer_stores_null_not_nan_for_unmapped_codes_and_missing_tickers(v2):
    from intelligence.people_events_pipeline.writer import apply_write_plan

    t0 = datetime(2026, 9, 1, 12, tzinfo=UTC)
    rows = [_sec(transaction_code="M", acquired_disposed_code="A"),
            _sec(accession_number="acc-2", nonderiv_trans_sk="2", issuer_ticker="NONE", transaction_code="S",
                 shares=10.0)]
    ev, plan = _plan(v2, t0, form345=rows)
    out = apply_write_plan(v2, ev, plan, run_id="nan1", mode="backfill", observed_at=t0)
    assert out["status"] == "SUCCESS" and out["counts"]["insert"] == 2
    with v2.connect() as conn:
        got = conn.execute(text("SELECT transaction_code, direction, entity_ticker FROM people_events "
                                "ORDER BY transaction_code")).fetchall()
    assert [tuple(r) for r in got] == [("M", None, "AAPL"), ("S", "sell", None)]


def test_read_event_versions_omits_current_only_counts_and_rejects_naive_time(v2):
    from store.people_events import read_event_versions

    _insert(v2)
    rows = read_event_versions(v2, datetime(2026, 9, 30, tzinfo=UTC))
    assert rows and not ({"n_sources", "n_source_rows", "source_refs", "confidence", "source", "provenance"}
                         & set(rows[0]))
    assert "attrs" in rows[0]
    with pytest.raises(ValueError):
        read_event_versions(v2, datetime(2026, 9, 30))


def test_gd3_progress_digest_matches_real_postgres_audit_and_all_table_growth(v2, tmp_path):
    from intelligence.people_events_pipeline.writer import apply_write_plan
    from scripts import people_events_backfill as B
    from scripts import people_events_backfill_safety as G

    prefix = "gd3-20261002T180000Z"
    observed = datetime(2026, 9, 1, 12, tzinfo=UTC)
    events, plan = _plan(v2, observed, form345=[_sec()])
    before = B._scalar(v2, B._SIZE_SQL)
    guards = []
    result = apply_write_plan(v2, events, plan, run_id=prefix + "-b00000", mode="backfill",
        observed_at=observed, inputs={"form345_sha256": "0" * 64, "batch": 0},
        before_transaction=lambda: guards.append(1))
    assert result["counts"]["insert"] == 1 and len(guards) == 3
    after = B._scalar(v2, B._SIZE_SQL)
    with v2.connect() as conn:
        relation_sizes = [conn.execute(text("SELECT pg_total_relation_size(:name)"),
                                      {"name": name}).scalar()
                          for name in ("people_events", "people_event_revisions", "people_events_runs")]
        assert conn.execute(text("SELECT count(*) FROM people_event_revisions")).scalar() == 0
    assert after == sum(relation_sizes) and after > before
    path = tmp_path / G.PROGRESS_NAME
    G.append_progress(path, {"run_id": prefix + "-b00000", "status": "SUCCESS", "batch_rows": 1,
                             "rows_written": 1})
    batches, rows, digest = G.read_progress(path, prefix)
    audit = B._run_audit(v2, prefix, "0" * 64)
    G.validate_resume(batches=batches, rows=rows, progress_digest=digest, database=audit,
                      stored_rows=1, plan_counts={"unchanged": 1}, total_rows=1)
    assert B._run_audit(v2, prefix, "wrong-source-hash")["invalid"] == 1
