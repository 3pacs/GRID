"""PostgreSQL proof that the postmortem batch selectors skip rows the writers refuse (#959).

`IS NOT NULL` alone admits NaN and +/-Infinity. The writers refuse those returns (they write nothing),
so without a finite predicate enough newer non-finite rows fill every batch's LIMIT and an older valid
miss never advances (Codex holds on #1486 and #1488). A measured 0 is a value and must stay selected.
"""
from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, text

import intelligence.postmortem as pm_mod


@pytest.fixture
def selector_pg():
    db_url = os.environ.get("GRID_TEST_DB_URL")
    if not db_url:
        if os.environ.get("REQUIRE_POSTMORTEM_PG") == "1":
            pytest.fail("GRID_TEST_DB_URL is required for the postmortem selector PostgreSQL proof")
        pytest.skip("GRID_TEST_DB_URL is not configured")
    admin = create_engine(db_url, pool_pre_ping=True)
    schema = f"pmsel_{uuid.uuid4().hex[:12]}"
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(
        admin.url, pool_size=1, max_overflow=0,
        connect_args={"options": f"-csearch_path={schema}"},
    )
    try:
        with engine.begin() as conn:
            # Only the columns the selectors read; types follow the real tables
            # (oracle/engine.py: pnl_pct DOUBLE PRECISION; schema.sql: actual_return NUMERIC).
            conn.execute(text(
                "CREATE TABLE oracle_predictions (id text PRIMARY KEY, verdict text, "
                "scored_at timestamptz, pnl_pct double precision, flow_context jsonb)"
            ))
            conn.execute(text(
                "CREATE TABLE options_recommendations (id integer PRIMARY KEY, outcome text, "
                "closed_at timestamptz, actual_return numeric)"
            ))
            conn.execute(text("CREATE TABLE trade_postmortems (trade_id integer, prediction_id text)"))
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()


def _run_batch(monkeypatch, engine, limit):
    pred_calls: list[str] = []
    trade_calls: list[int] = []
    monkeypatch.setattr(pm_mod, "_ensure_tables", lambda _e: None)
    # The writers' own refusal is what leaves a row selectable; record the selection, write nothing.
    monkeypatch.setattr(pm_mod, "generate_prediction_postmortem", lambda _e, pid: pred_calls.append(pid))
    monkeypatch.setattr(pm_mod, "generate_postmortem", lambda _e, tid: trade_calls.append(tid))
    pm_mod.batch_postmortem(engine, days=30, limit=limit)
    return pred_calls, trade_calls


def _insert_prediction(engine, pid, hours_ago, pnl_sql):
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO oracle_predictions (id, verdict, scored_at, pnl_pct, flow_context) "
            "VALUES (:id, 'miss', now() - make_interval(hours => :h), " + pnl_sql + ", '{}'::jsonb)"
        ), {"id": pid, "h": hours_ago})


def test_newer_non_finite_predictions_do_not_fill_the_limit(selector_pg, monkeypatch):
    # Four newer misses the writer refuses (NaN, +Inf, -Inf, NULL), then a measured 0 and finite misses.
    _insert_prediction(selector_pg, "p-nan", 1, "'NaN'::float8")
    _insert_prediction(selector_pg, "p-inf", 2, "'Infinity'::float8")
    _insert_prediction(selector_pg, "p-ninf", 3, "'-Infinity'::float8")
    _insert_prediction(selector_pg, "p-null", 4, "NULL")
    _insert_prediction(selector_pg, "p-zero", 5, "0.0")
    _insert_prediction(selector_pg, "p-finite", 6, "-5.25")
    _insert_prediction(selector_pg, "p-older", 7, "-1.0")

    pred_calls, _ = _run_batch(monkeypatch, selector_pg, limit=2)
    assert pred_calls == ["p-zero", "p-finite"]          # the measured zero is kept, the older miss advances

    # Run again with the writers still writing nothing: the same finite rows, never the refused ones.
    pred_calls_again, _ = _run_batch(monkeypatch, selector_pg, limit=2)
    assert pred_calls_again == ["p-zero", "p-finite"]

    unlimited, _ = _run_batch(monkeypatch, selector_pg, limit=None)
    assert unlimited == ["p-zero", "p-finite", "p-older"]


def _insert_trade(engine, tid, hours_ago, ret_sql, outcome="LOSS"):
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO options_recommendations (id, outcome, closed_at, actual_return) "
            "VALUES (:id, :o, now() - make_interval(hours => :h), " + ret_sql + ")"
        ), {"id": tid, "o": outcome, "h": hours_ago})


def test_newer_non_finite_trades_do_not_fill_the_limit(selector_pg, monkeypatch):
    # NUMERIC holds NaN (Infinity only from PostgreSQL 14, so it is not used here) and values far beyond float8.
    _insert_trade(selector_pg, 1, 1, "'NaN'::numeric")
    _insert_trade(selector_pg, 2, 2, "NULL")
    _insert_trade(selector_pg, 3, 3, "'NaN'::numeric", outcome="EXPIRED")
    _insert_trade(selector_pg, 4, 4, "NULL", outcome="EXPIRED")
    _insert_trade(selector_pg, 5, 5, "0", outcome="EXPIRED")        # a measured 0 stays selected
    _insert_trade(selector_pg, 6, 6, "-0.62")
    _insert_trade(selector_pg, 7, 7, "-1.0")
    _insert_trade(selector_pg, 8, 1, "-0.3", outcome="WIN")         # not a failure
    # Storable in unconstrained NUMERIC but outside float8: must be skipped, not abort the selection.
    _insert_trade(selector_pg, 9, 1, "1e400::numeric")
    _insert_trade(selector_pg, 10, 1, "-1e400::numeric", outcome="EXPIRED")

    _, trade_calls = _run_batch(monkeypatch, selector_pg, limit=2)
    assert trade_calls == [5, 6]

    _, trade_calls_again = _run_batch(monkeypatch, selector_pg, limit=2)
    assert trade_calls_again == [5, 6]

    _, unlimited = _run_batch(monkeypatch, selector_pg, limit=None)
    assert unlimited == [5, 6, 7]
