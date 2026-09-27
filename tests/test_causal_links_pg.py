"""Real-PostgreSQL contract for slice N2's causal-link writer and readers.

Proves, in a disposable schema:

* ``migrations/versions/causal_links_provenance_20260927.py`` is additive on the
  legacy runtime-created ``causal_links`` table (legacy rows survive), can be
  re-applied, and downgrades cleanly;
* ``intelligence.causal_links.run_causal_links`` persists only edges whose
  event was public before the trade day, merges one Form 4 act seen through two
  channels, records known_at / run id / code sha, and is idempotent across runs
  (same edge count, ``first_run_id`` kept, ``run_id``/``code_sha`` updated);
* the writer refuses to run without the migration, and ``scripts/run_causal_links.py``
  exits 2 in that case;
* the GET routes behind the Timeline / Causal Map / Why views read the persisted
  rows with an as-of label and issue SELECT statements only.

CI runs this file in its own step with GRID_TEST_DB_URL and fails if it skips.
"""

from __future__ import annotations

import importlib
import json
import os
import re
import uuid
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta, timezone
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, event, text

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("DB_PASSWORD", "testpass")

_DB_URL = os.environ.get("GRID_TEST_DB_URL") or os.environ.get("DB_URL")
pytestmark = [
    pytest.mark.skipif(not _DB_URL, reason="requires disposable PostgreSQL via GRID_TEST_DB_URL or DB_URL"),
    pytest.mark.xdist_group("postgres"),
]

_MIGRATION = "migrations.versions.causal_links_provenance_20260927"
_WRITE = re.compile(r"\b(INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|TRUNCATE|MERGE|GRANT|REVOKE)\b", re.I)
UTC = timezone.utc
TODAY = datetime.now(UTC).date()


def _d(offset: int) -> date:
    return TODAY + timedelta(days=offset)


def _ts(offset: int, hour: int = 12) -> datetime:
    return datetime.combine(_d(offset), time(hour, 0), tzinfo=UTC)


@contextmanager
def _schema():
    admin = create_engine(_DB_URL)
    name = "n2_causal_" + uuid.uuid4().hex[:12]
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{name}"'))
    engine = create_engine(_DB_URL, connect_args={"options": f"-csearch_path={name}"})
    try:
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{name}" CASCADE'))
        admin.dispose()


def _migrate(engine, fn: str) -> None:
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    migration = importlib.import_module(_MIGRATION)
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            real = migration.op
            migration.op = Operations(MigrationContext.configure(conn))
            try:
                getattr(migration, fn)()
            finally:
                migration.op = real
            trans.commit()
        except Exception:
            trans.rollback()
            raise


def _seed(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE signal_sources (
                id SERIAL PRIMARY KEY, source_type TEXT NOT NULL, source_id TEXT NOT NULL,
                ticker TEXT, signal_date DATE NOT NULL, signal_type TEXT NOT NULL,
                signal_value JSONB, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            CREATE TABLE earnings_calendar (
                id SERIAL PRIMARY KEY, ticker TEXT NOT NULL, earnings_date DATE NOT NULL,
                fiscal_quarter TEXT, eps_estimate DOUBLE PRECISION, eps_actual DOUBLE PRECISION,
                eps_surprise_pct DOUBLE PRECISION, reported BOOLEAN DEFAULT FALSE
            )
        """))
        ins = text(
            "INSERT INTO signal_sources (source_type, source_id, ticker, signal_date, signal_type, "
            "signal_value, created_at) VALUES (:st, :sid, :t, :d, :typ, CAST(:v AS JSONB), :c)"
        )
        conn.execute(ins, [
            # One Form 4 sale, reported by EDGAR and QuiverQuant.
            {"st": "insider", "sid": "Cutt Timothy J.", "t": "GPOR", "d": _d(-10), "typ": "SELL",
             "v": "{}", "c": _ts(-8)},
            {"st": "quiverquant:insider", "sid": "qq_insider_trading", "t": "GPOR", "d": _d(-10),
             "typ": "insider_sell", "c": _ts(-9),
             "v": json.dumps({"Name": "Timothy Cutt", "fileDate": f"{_d(-10).isoformat()}T18:00:00.000"})},
            # Cluster aggregate: never an action.
            {"st": "insider", "sid": "Someone Else", "t": "GPOR", "d": _d(-10), "typ": "CLUSTER_BUY",
             "v": "{}", "c": _ts(-8)},
            # Congressional buy 60 days ago (public by the 45-day statutory bound).
            {"st": "congressional", "sid": "Rep A", "t": "GPOR", "d": _d(-60), "typ": "BUY",
             "v": json.dumps({"disclosure_date": _d(-60).isoformat()}), "c": _ts(-59)},
            # Contract award first seen 20 days ago; Start Date in the future.
            {"st": "gov_contract", "sid": "DoD", "t": "GPOR", "d": _d(100), "typ": "CONTRACT_AWARD",
             "v": json.dumps({"award_id": "W1", "amount": 5000000}), "c": _ts(-20)},
        ])
        conn.execute(text(
            "INSERT INTO earnings_calendar (ticker, earnings_date, fiscal_quarter, eps_estimate, "
            "eps_actual, eps_surprise_pct, reported) VALUES (:t, :d, 'Q', 1.0, :a, 5.0, :r)"
        ), [
            {"t": "GPOR", "d": _d(-15), "a": 1.1, "r": True},    # before the Form 4 sale
            {"t": "GPOR", "d": _d(-5), "a": 1.2, "r": True},     # after the sale: never linked
            {"t": "GPOR", "d": _d(-12), "a": None, "r": False},  # unreported estimate
            {"t": "GPOR", "d": _d(-70), "a": 0.9, "r": True},    # before the congressional buy
        ])


def _links(engine) -> list[dict]:
    with engine.connect() as conn:
        return [dict(r) for r in conn.execute(text(
            "SELECT * FROM causal_links WHERE edge_key IS NOT NULL ORDER BY action_date, event_key"
        )).mappings()]


def test_migration_is_additive_reapplyable_and_downgrades():
    with _schema() as engine:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE causal_links (
                    id SERIAL PRIMARY KEY, signal_id INT, actor TEXT, ticker TEXT,
                    action_date DATE, cause_type TEXT, probable_cause TEXT, evidence JSONB,
                    probability NUMERIC, created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            conn.execute(text(
                "INSERT INTO causal_links (actor, ticker, cause_type) VALUES ('legacy', 'AAA', 'macro')"
            ))
        _migrate(engine, "upgrade")
        _migrate(engine, "upgrade")  # IF NOT EXISTS throughout
        from intelligence.causal_links import schema_ready

        with engine.connect() as conn:
            assert schema_ready(conn)
            assert conn.execute(text("SELECT count(*) FROM causal_links")).scalar() == 1
            idx = conn.execute(text(
                "SELECT indexdef FROM pg_indexes WHERE indexname = 'uq_causal_links_edge_key' "
                "AND schemaname = current_schema()"
            )).scalar()
            assert idx and "UNIQUE" in idx
        _migrate(engine, "downgrade")
        with engine.connect() as conn:
            assert not schema_ready(conn)
            assert conn.execute(text("SELECT to_regclass('causal_link_runs')")).scalar() is None
            assert conn.execute(text("SELECT count(*) FROM causal_links")).scalar() == 1


def test_writer_refuses_without_migration_and_script_exits_2():
    from intelligence.causal_links import CausalLinksSchemaMissing, run_causal_links
    from scripts.run_causal_links import main

    with _schema() as engine:
        _seed(engine)
        with pytest.raises(CausalLinksSchemaMissing):
            run_causal_links(engine, days=90, code_sha="x")
        assert main(["--days", "90"], engine=engine) == 2
        with engine.connect() as conn:
            assert conn.execute(text("SELECT to_regclass('causal_links')")).scalar() is None


def test_run_persists_only_pre_trade_events_idempotently_with_provenance():
    from intelligence.causal_links import day_start, run_causal_links

    with _schema() as engine:
        _seed(engine)
        _migrate(engine, "upgrade")

        first = run_causal_links(engine, days=90, code_sha="sha-one", batch_size=1)
        rows = _links(engine)
        assert first.status == "succeeded"
        assert first.edges_written == len(rows) == 3
        keys = {(r["actor"], r["event_key"]) for r in rows}
        assert keys == {
            ("Rep A", f"earnings:GPOR:{_d(-70).isoformat()}"),
            ("Timothy Cutt", f"earnings:GPOR:{_d(-15).isoformat()}"),
            ("Timothy Cutt", "contract:GPOR:W1"),
        }
        for r in rows:
            assert r["event_known_at"] <= day_start(r["action_date"])  # no look-ahead
            assert r["known_at"] == max(r["event_known_at"], r["action_known_at"])
            assert r["run_id"] == first.run_id == r["first_run_id"]
            assert r["code_sha"] == "sha-one"
            assert r["score_method"] == "recency_linear_v1"
        form4 = [r for r in rows if r["actor"] == "Timothy Cutt"]
        assert {r["action_known_at_basis"] for r in form4} == {"filing"}
        assert all(r["action_channel"] == "form4" and r["action"] == "SELL" for r in form4)
        contract = next(r for r in rows if r["event_kind"] == "contract")
        assert contract["event_date"] == _d(-20)  # first seen, not the future Start Date
        congress = next(r for r in rows if r["actor"] == "Rep A")
        assert congress["action_known_at_basis"] == "statutory_bound"

        second = run_causal_links(engine, days=90, code_sha="sha-two")
        rows2 = _links(engine)
        assert len(rows2) == 3
        assert {r["first_run_id"] for r in rows2} == {first.run_id}
        assert {r["run_id"] for r in rows2} == {second.run_id}
        assert {r["code_sha"] for r in rows2} == {"sha-two"}
        with engine.connect() as conn:
            runs = conn.execute(text(
                "SELECT status, edges_written, code_sha FROM causal_link_runs ORDER BY started_at"
            )).fetchall()
        assert [(r[0], r[1]) for r in runs] == [("succeeded", 3), ("succeeded", 3)]


def test_get_routes_read_persisted_links_select_only_with_as_of():
    from api.routers import intelligence_causation as c
    from api.routers import intelligence_forensics as f
    from intelligence.causal_links import run_causal_links
    from scripts.run_causal_links import main

    with _schema() as engine:
        _seed(engine)
        _migrate(engine, "upgrade")
        assert main(["--days", "90", "--code-sha", "sha-cli", "--json"], engine=engine) == 0
        run_causal_links(engine, days=90, code_sha="sha-final")

        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        statements: list[str] = []

        def record(_conn, _cursor, statement, _params, _context, _many):
            statements.append(statement.strip())

        event.listen(engine, "before_cursor_execute", record)
        try:
            with patch.object(c, "get_db_engine", return_value=engine), \
                 patch.object(f, "get_db_engine", return_value=engine):
                timeline = c.get_causal_links(ticker="gpor", days=90, _token="t")
                why = f.get_causation(ticker="GPOR", days=90, _token="t")
                batch = f.get_causation(ticker=None, days=90, _token="t")
        finally:
            event.remove(engine, "before_cursor_execute", record)

        writes = [s for s in statements if _WRITE.search(re.sub(r"'[^']*'", "''", s))]
        assert writes == [], writes
        assert all(s.upper().startswith(("SELECT", "WITH")) for s in statements), statements

        assert timeline["generated"] is True and timeline["as_of"]
        assert timeline["last_run"]["code_sha"] == "sha-final"
        assert len(timeline["links"]) == 3
        for link in timeline["links"]:
            assert link["cause_date"] <= link["effect_date"]
            assert link["score_is_probability"] is False
            assert link["code_sha"] == "sha-final"
        assert why["narrative"] is None and why["total_causes"] == 3
        assert why["as_of"] == timeline["as_of"]
        assert batch["total_causes"] == 3


def test_get_routes_are_honest_before_any_run():
    from api.routers import intelligence_causation as c

    with _schema() as engine:
        _seed(engine)
        _migrate(engine, "upgrade")
        with patch.object(c, "get_db_engine", return_value=engine):
            out = c.get_causal_links(ticker="GPOR", days=90, _token="t")
        assert out["generated"] is False and out["links"] == []
        assert "not scheduled" in out["reason"]
