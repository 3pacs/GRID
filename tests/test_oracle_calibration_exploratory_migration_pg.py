"""Real-PostgreSQL proof for migrations/versions/oracle_cal_exploratory_20261106.py.

Runs in a throwaway schema (random name, dropped afterwards) on the shared
``pg_engine`` fixture (``GRID_TEST_DB_URL``); skips when no PostgreSQL is
reachable. Applies the parent revision (which owns
``grid_reject_outcome_mutation()``) and then this one, and proves:

* the daily job writes JSONB payloads that round-trip into a usable
  conformal state, and a second run on the same day inserts nothing;
* UPDATE, DELETE and TRUNCATE are refused, and so is a row claiming
  ``official = TRUE`` or a non-exploratory ``score_class``;
* the job reads ``oracle_predictions`` / ``oracle_prediction_rescores``
  without changing a single row of either.
"""

from __future__ import annotations

import importlib
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import numpy as np
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError

from oracle.calibration import (
    exploratory_conformal_interval,
    load_latest_exploratory_state,
    run_exploratory_recalibration,
)

_PARENT = "migrations.versions.outcome_rescore_tables_20261106"
_MIGRATION = "migrations.versions.oracle_cal_exploratory_20261106"
AS_OF = datetime(2026, 11, 6, 2, 35, tzinfo=timezone.utc)

_ORACLE_DDL = """
CREATE TABLE oracle_predictions (
    id TEXT PRIMARY KEY, created_at TIMESTAMPTZ, ticker TEXT, direction TEXT,
    expiry DATE, confidence DOUBLE PRECISION, model_name TEXT, verdict TEXT,
    scored_at TIMESTAMPTZ, dedup_keep BOOLEAN, horizon_days INTEGER
)
"""


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"oracle_cal_expl_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url, connect_args={"options": f"-csearch_path={schema}"})
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE signal_sources (id INTEGER PRIMARY KEY)"))
        conn.execute(text("CREATE TABLE decision_journal (id BIGINT PRIMARY KEY)"))
        conn.execute(text(_ORACLE_DDL))
        import scripts.score_oracle_trades as sot

        conn.execute(text(sot._RESCORE_DDL))
    try:
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _apply(engine: Engine, module: str) -> None:
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    migration = importlib.import_module(module)
    with engine.connect() as conn:
        trans = conn.begin()
        real_op = migration.op
        migration.op = Operations(MigrationContext.configure(conn))
        try:
            migration.upgrade()
        finally:
            migration.op = real_op
        trans.commit()


def _seed(engine: Engine) -> None:
    rng = np.random.default_rng(2)
    with engine.begin() as conn:
        for d in range(40):
            expiry = date(2026, 5, 1) + timedelta(days=4 * d)
            created = datetime.combine(expiry - timedelta(days=7), datetime.min.time(), timezone.utc)
            for k in range(25):
                pid = f"r{d}-{k}"
                t = float(rng.uniform(0.3, 0.8))
                verdict = "miss" if rng.uniform() > t else "hit"
                official = k < 8
                conn.execute(text(
                    "INSERT INTO oracle_predictions VALUES (:id, :c, 'AAA', 'PUT', :e, :conf, :m, "
                    ":v, :s, TRUE, NULL)"
                ), {"id": pid, "c": created, "e": expiry, "conf": t, "m": "options_flow_mut_x",
                    "v": verdict if official else "no_data", "s": created + timedelta(days=8)})
                if not official:
                    conn.execute(text(
                        "INSERT INTO oracle_prediction_rescores (prediction_id, scoring_version, "
                        "score_class, official, prior_verdict, verdict, entry_price, actual_price, "
                        "actual_move_pct, pnl_pct, price_session_date, expiry_rule, rescored_at) "
                        "VALUES (:id, 'oracle_rescore_hindsight_v1', 'exploratory_hindsight', FALSE, "
                        "'no_data', :v, 10, 10, 0, 0, :e, 'rule', :rs)"
                    ), {"id": pid, "v": verdict, "e": expiry, "rs": datetime(2026, 11, 6, 1, 49, tzinfo=timezone.utc)})


def _fingerprint(engine: Engine) -> tuple:
    with engine.connect() as conn:
        return tuple(
            conn.execute(text(f"SELECT md5(string_agg(t::text, '|' ORDER BY t::text)) FROM {tbl} t")).scalar()
            for tbl in ("oracle_predictions", "oracle_prediction_rescores")
        )


def test_job_end_to_end_and_append_only(scratch):
    engine = scratch
    _apply(engine, _PARENT)
    _apply(engine, _MIGRATION)
    _seed(engine)
    before = _fingerprint(engine)

    summary = run_exploratory_recalibration(engine, as_of=AS_OF)
    assert summary["status"] == "ok"
    assert summary["inserted"] == summary["groups"] + 1
    assert run_exploratory_recalibration(engine, as_of=AS_OF)["inserted"] == 0
    assert _fingerprint(engine) == before

    state = load_latest_exploratory_state(engine)
    assert state is not None and state["official"] is False
    iv = exploratory_conformal_interval(state, 0.6, "options_flow_mut_y")
    assert 0.0 <= iv.lower <= iv.point <= iv.upper <= 1.0

    with engine.connect() as conn:
        run_payload = conn.execute(text(
            "SELECT payload FROM oracle_calibration_exploratory WHERE scope = 'run'")).scalar()
        assert run_payload["holdout"]["aci"]["nominal_coverage"] == 0.9
        assert run_payload["writes"] == ["oracle_calibration_exploratory"]

    for stmt in ("UPDATE oracle_calibration_exploratory SET n_rows = 0",
                 "DELETE FROM oracle_calibration_exploratory",
                 "TRUNCATE oracle_calibration_exploratory"):
        with pytest.raises(DBAPIError, match="append-only"):
            with engine.begin() as conn:
                conn.execute(text(stmt))

    for official, score_class in ((True, "exploratory"), (False, "official")):
        with pytest.raises(DBAPIError, match="check"):
            with engine.begin() as conn:
                conn.execute(text(
                    "INSERT INTO oracle_calibration_exploratory (as_of, method_version, scope, "
                    "group_key, target, payload, score_class, official) VALUES "
                    "('2026-11-07', 'v', 'run', 'all', 't', '{}'::jsonb, :sc, :o)"
                ), {"sc": score_class, "o": official})
