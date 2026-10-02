"""Real-PostgreSQL proof for ``scripts/apply_security_master_seed.py``.

Runs the loader against a throwaway schema (random name, dropped afterwards) that holds the GD1 tables
(``intelligence.security_master.ensure_tables``, the same DDL as the migration). Proves, on a real database:

* the batched ``jsonb_to_recordset`` INSERT ... ON CONFLICT DO NOTHING really inserts, with the right types
  (dates, JSONB, NULL ``conflict_detail`` / ``valid_to`` / ``sic``) and a correct row count;
* a pre-existing Technology-seed row is left byte-for-byte alone (no UPDATE), including its ``updated_at``;
* a second apply of the same artifact inserts nothing;
* a CIK held by another entity is skipped by the plan, and by the database itself if the plan is bypassed;
* the read-only existence read cannot write.

Uses the shared ``pg_engine`` fixture (``GRID_TEST_DB_URL``); skips when no PostgreSQL is reachable.
"""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError

from intelligence.security_master import ensure_tables
from scripts import apply_security_master_seed as loader
from scripts import build_security_master_all_issuers as builder

NOON = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"sm_seed_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url, connect_args={"options": f"-csearch_path={schema}"})
    ensure_tables(engine)
    try:
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _seed(tmp_path):
    sub = pd.DataFrame(
        [("a1", "2006-02-02", "320193", "AAPL"), ("a2", "2020-02-02", "320193", "AAPL"),
         ("b1", "2010-01-04", "1", "OLD"), ("b2", "2012-01-04", "1", "NEW"),
         ("c1", "2008-01-02", "2", "REUSE"), ("c2", "2020-01-02", "2", "REUSE"), ("d1", "2015-06-01", "3", "REUSE")],
        columns=["accession_number", "filing_date", "issuer_cik", "issuer_ticker"])
    built = builder.build_rows(sub, {320193: {"name": "Apple Inc.", "tickers": ["AAPL"]}}, tickers_as_of="2026-09-27",
                               sic_map={320193: {"sic": 3571, "name": "Apple", "fetched_at": "x"}})
    builder.write_artifact(built, tmp_path / "art", inputs=[], params={}, code_sha="test")
    return loader.load_seed(tmp_path / "art", require_receipt=True)


_COUNT_SQL = {
    "security_master": text("SELECT count(*) FROM security_master"),
    "security_identifiers": text("SELECT count(*) FROM security_identifiers"),
}


def _count(engine, table):
    with engine.connect() as conn:
        return conn.execute(_COUNT_SQL[table]).scalar()


def _existing(engine):
    existing, counts = loader.read_existing(engine, now=NOON)
    return existing, counts


def test_apply_inserts_once_keeps_existing_rows_and_is_idempotent(scratch, tmp_path):
    seed = _seed(tmp_path)
    # The Technology seed's own AAPL rows, dated at the seed day.
    with scratch.begin() as conn:
        conn.execute(text(
            "INSERT INTO security_master (entity_id, cik, name, source, updated_at) "
            "VALUES ('sm_0000320193', 320193, 'Apple (tech seed)', 'sector_map+sec_company_tickers', '2026-09-28T00:00:00Z')"))
        conn.execute(text(
            "INSERT INTO security_identifiers (entity_id, id_scheme, id_value, valid_from, source) VALUES "
            "('sm_0000320193', 'cik', '320193', '2026-09-28', 'sec_company_tickers'), "
            "('sm_0000320193', 'ticker', 'AAPL', '2026-09-28', 'sector_map')"))
    existing, before = _existing(scratch)
    assert before == {"security_master": 1, "security_identifiers": 2}
    plan = loader.plan_inserts(seed, existing)
    assert len(plan.sm_inserts) == 3  # AAPL's entity is skipped

    inserted = loader.apply_plan(scratch, plan, batch_size=2, now=lambda: NOON)
    assert inserted == {"security_master": 3, "security_identifiers": len(plan.si_inserts)}
    assert _count(scratch, "security_master") == 4
    assert _count(scratch, "security_identifiers") == 2 + len(plan.si_inserts)

    with scratch.connect() as conn:
        apple = conn.execute(text("SELECT name, source, updated_at, sic FROM security_master WHERE entity_id = 'sm_0000320193'")).one()
        assert apple.name == "Apple (tech seed)" and apple.source == "sector_map+sec_company_tickers"
        assert apple.updated_at == datetime(2026, 9, 28, tzinfo=timezone.utc) and apple.sic is None  # untouched, not even the new SIC
        old = conn.execute(text(
            "SELECT valid_from, valid_to, is_primary, conflict_flag, conflict_detail, source FROM security_identifiers "
            "WHERE entity_id = 'sm_0000000001' AND id_value = 'OLD'")).one()
        assert (str(old.valid_from), str(old.valid_to), old.is_primary, old.conflict_flag) == ("2010-01-04", "2012-01-03", True, False)
        assert old.conflict_detail is None and old.source == "all_issuers_v1:sec_form345"
        new = conn.execute(text("SELECT valid_to FROM security_identifiers WHERE entity_id = 'sm_0000000001' AND id_value = 'NEW'")).one()
        assert new.valid_to is None
        flagged = conn.execute(text(
            "SELECT entity_id, is_primary, conflict_detail->>'kind' AS kind FROM security_identifiers "
            "WHERE id_value = 'REUSE' ORDER BY entity_id")).all()
        assert [(r.entity_id, r.is_primary, r.kind) for r in flagged] == [
            ("sm_0000000002", False, "overlapping_ticker_claim"), ("sm_0000000003", False, "overlapping_ticker_claim")]
        # The AAPL ticker row dated at the first filing was added to the existing entity.
        assert conn.execute(text(
            "SELECT count(*) FROM security_identifiers WHERE entity_id = 'sm_0000320193' AND id_scheme = 'ticker'")).scalar() == 2
        assert conn.execute(text("SELECT count(*) FROM security_master WHERE source <> 'all_issuers_v1'")).scalar() == 1

    # Second run: the plan is empty, and even a forced re-send of the very same rows inserts nothing.
    existing, _ = _existing(scratch)
    again = loader.plan_inserts(seed, existing)
    assert again.sm_inserts == [] and again.si_inserts == []
    forced = loader.Plan(sm_inserts=list(plan.sm_inserts), si_inserts=list(plan.si_inserts))
    assert loader.apply_plan(scratch, forced, now=lambda: NOON) == {"security_master": 0, "security_identifiers": 0}


def test_a_cik_held_by_another_entity_is_refused_by_the_database_too(scratch):
    with scratch.begin() as conn:
        conn.execute(text("INSERT INTO security_master (entity_id, cik, name, source) VALUES ('sm_tkr_NINE', 9, 'Nine', 'sector_map')"))
    row = {"entity_id": "sm_0000000009", "cik": 9, "name": "N", "security_type": "equity", "is_active": True, "delisted_at": None,
           "delisted_reason": None, "delisted_basis": None, "sic": None, "source": "all_issuers_v1", "provenance": {}}
    plan = loader.Plan(sm_inserts=[row])
    assert loader.apply_plan(scratch, plan, now=lambda: NOON) == {"security_master": 0, "security_identifiers": 0}
    assert _count(scratch, "security_master") == 1


def test_the_existence_read_is_read_only(scratch):
    from sqlalchemy import text as sql

    with scratch.connect() as conn:
        with conn.begin():
            conn.execute(sql("SET TRANSACTION READ ONLY"))
            with pytest.raises(DBAPIError):
                conn.execute(sql("INSERT INTO security_master (entity_id, name, source) VALUES ('x', 'x', 'x')"))
    existing, counts = _existing(scratch)
    assert counts == {"security_master": 0, "security_identifiers": 0} and existing.entities == set()
