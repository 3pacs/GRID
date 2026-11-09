"""PostgreSQL contract for ``calendar_dated_leftover_sql`` and its readers.

The SQL mirror must flag exactly the rows ``is_calendar_dated_leftover`` flags
(on grid-svr, 2026-11-09: 148 rows both ways), and the trust inputs and the
money-flow corporate layer must leave those rows out. Disposable schema on
GRID_TEST_DB_URL; the CI step "QuiverQuant gov_contracts leftovers PostgreSQL
contract" sets REQUIRE_QQ_GOV_LEFTOVERS_PG so a missing database fails instead
of skipping.
"""

from __future__ import annotations

import json
import os
from datetime import date
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

from analysis.money_flow_engine import layer_corporate
from ingestion.altdata.quiverquant_identity import (
    calendar_dated_leftover_sql,
    is_calendar_dated_leftover,
)
from intelligence import trust_scorer
from tests.test_qq_gov_calendar_leftovers import CASES, QQ

pytestmark = pytest.mark.xdist_group("postgres")

_REQUIRE_ENV = "REQUIRE_QQ_GOV_LEFTOVERS_PG"


@pytest.fixture(scope="module")
def pg():
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        if os.environ.get(_REQUIRE_ENV) == "1":
            pytest.fail(f"GRID_TEST_DB_URL is required ({_REQUIRE_ENV}=1)")
        pytest.skip("GRID_TEST_DB_URL is required for disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("the gov_contracts leftovers contract requires a local disposable test database")
    schema = "qq_gov_left_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE signal_sources (id SERIAL PRIMARY KEY, source_type TEXT, source_id TEXT, "
            "ticker TEXT, signal_date DATE, signal_type TEXT, signal_value JSONB, outcome TEXT, "
            "outcome_return DOUBLE PRECISION)"
        ))
        for i, (day, payload, _) in enumerate(CASES):
            for source_type in (QQ, "gov_contract"):
                conn.execute(text(
                    "INSERT INTO signal_sources (source_type, source_id, ticker, signal_date, signal_type, "
                    "signal_value, outcome, outcome_return) VALUES (:st, 'qq_gov_contracts', :t, :d, "
                    "'gov_contracts', CAST(:v AS jsonb), 'CORRECT', 1.0)"
                ), {"st": source_type, "t": f"T{i}", "d": day, "v": json.dumps(payload)})
    yield engine
    engine.dispose()
    with admin.begin() as conn:
        conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
    admin.dispose()


def test_sql_flags_exactly_what_python_flags(pg):
    with pg.connect() as conn:
        rows = conn.execute(text(
            f"SELECT ticker, signal_date, signal_value, {calendar_dated_leftover_sql()} "
            f"FROM signal_sources WHERE source_type = :st ORDER BY id"), {"st": QQ}).fetchall()
    assert len(rows) == len(CASES)
    for (ticker, day, value, flagged), (_, _, expected) in zip(rows, CASES):
        assert flagged is expected, ticker
        assert is_calendar_dated_leftover(day, value) is expected, ticker


def test_trust_inputs_leave_out_leftovers_of_quiverquant_only(pg):
    with pg.connect() as conn:
        rows = conn.execute(text(trust_scorer._LEGACY_SCORED_ROWS_SQL)).fetchall()
    kept = {(r.source_type, r.ticker) for r in rows}
    for i, (_, _, leftover) in enumerate(CASES):
        assert ((QQ, f"T{i}") in kept) is not leftover
        assert ("gov_contract", f"T{i}") in kept  # the predicate is scoped to QuiverQuant


def test_corporate_layer_does_not_sum_leftovers(pg):
    node = layer_corporate._build_qq_gov_contracts(pg, date(2026, 11, 8))
    # 90 days back from 2026-11-08 (no upper bound): 2026-09-30 x3, 2026-12-31 x2, 2027-03-31.
    in_window = [(d, p, x) for d, p, x in CASES if d >= date(2026, 8, 10)]
    kept = [p for d, p, x in in_window if not x]
    assert node.metadata["contract_count"] == len(kept)
    assert node.metadata["total_value_90d"] == pytest.approx(sum(p.get("Amount", 0.0) for p in kept))
