"""Real-PostgreSQL proof for the S16 SQL (edge report loaders, corrected trust).

A throwaway schema (random name, dropped afterwards) holds minimal stand-ins
for signal_sources, signal_source_outcomes, oracle_predictions,
oracle_prediction_rescores and the AstroGrid prediction tables. Uses the shared
``pg_engine`` fixture (``GRID_TEST_DB_URL``); skips without PostgreSQL.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError

UTC = timezone.utc

DDL = [
    """CREATE TABLE signal_sources (
        id SERIAL PRIMARY KEY, source_type TEXT NOT NULL, source_id TEXT NOT NULL,
        ticker TEXT, signal_date DATE NOT NULL, signal_type TEXT, signal_value JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), outcome TEXT DEFAULT 'PENDING',
        outcome_return DOUBLE PRECISION, trust_score DOUBLE PRECISION DEFAULT 0.5,
        hit_count INTEGER DEFAULT 0, miss_count INTEGER DEFAULT 0,
        avg_lead_time_hours DOUBLE PRECISION)""",
    """CREATE TABLE signal_source_outcomes (
        id BIGSERIAL PRIMARY KEY, signal_source_id INTEGER NOT NULL REFERENCES signal_sources(id),
        evaluation_version TEXT NOT NULL, source_type TEXT NOT NULL, ticker TEXT NOT NULL,
        signal_date DATE NOT NULL, direction TEXT NOT NULL, horizon_days INTEGER NOT NULL,
        outcome TEXT NOT NULL, eligibility_reason TEXT, dead_band_pct DOUBLE PRECISION NOT NULL DEFAULT 1,
        cost_bps DOUBLE PRECISION NOT NULL DEFAULT 0, entry_price_date DATE, exit_price_date DATE,
        raw_return DOUBLE PRECISION, evaluated_at TIMESTAMPTZ NOT NULL,
        UNIQUE (signal_source_id, evaluation_version))""",
    """CREATE TABLE oracle_predictions (
        id TEXT PRIMARY KEY, created_at TIMESTAMPTZ, ticker TEXT NOT NULL, direction TEXT NOT NULL,
        expiry DATE NOT NULL, confidence DOUBLE PRECISION, model_name TEXT NOT NULL,
        verdict TEXT DEFAULT 'pending', scored_at TIMESTAMPTZ, dedup_keep BOOLEAN NOT NULL DEFAULT TRUE)""",
    """CREATE TABLE oracle_prediction_rescores (
        prediction_id TEXT NOT NULL, scoring_version TEXT NOT NULL, verdict TEXT NOT NULL,
        PRIMARY KEY (prediction_id, scoring_version))""",
    """CREATE TABLE prediction_run (
        id BIGSERIAL PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL, horizon_label TEXT NOT NULL,
        model_version TEXT NOT NULL)""",
    """CREATE TABLE prediction_score (
        id BIGSERIAL PRIMARY KEY, prediction_run_id BIGINT NOT NULL UNIQUE REFERENCES prediction_run(id),
        scored_at TIMESTAMPTZ NOT NULL, verdict TEXT, raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb)""",
    """CREATE TABLE prediction_score_legacy_rescore (
        id BIGSERIAL PRIMARY KEY, prediction_run_id BIGINT NOT NULL UNIQUE REFERENCES prediction_run(id),
        scored_at TIMESTAMPTZ NOT NULL, verdict TEXT, raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb)""",
]

V = "sig-eval-4-sessions"


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"s16_edge_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url, connect_args={"options": f"-csearch_path={schema}"})
    with engine.begin() as conn:
        for ddl in DDL:
            conn.execute(text(ddl))
    try:
        yield engine, schema
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _signal(conn, *, st, sid, sig_date, created, outcome, entry, exit_, evaluated, direction="BUY",
            version=V, legacy="WRONG", ret=None):
    new_id = conn.execute(text(
        "INSERT INTO signal_sources (source_type, source_id, ticker, signal_date, signal_type, created_at, outcome) "
        "VALUES (:st, :sid, 'NVDA', :d, 'BUY', :c, :lo) RETURNING id"),
        {"st": st, "sid": sid, "d": sig_date, "c": created, "lo": legacy}).scalar()
    conn.execute(text(
        "INSERT INTO signal_source_outcomes (signal_source_id, evaluation_version, source_type, ticker, "
        "signal_date, direction, horizon_days, outcome, eligibility_reason, entry_price_date, exit_price_date, "
        "raw_return, evaluated_at) VALUES (:id, :v, :st, 'NVDA', :d, :dir, 5, :o, :r, :e, :x, :ret, :ev)"),
        {"id": new_id, "v": version, "st": st, "d": sig_date, "dir": direction, "o": outcome,
         "r": "unknown_direction" if outcome == "INELIGIBLE" else None, "e": entry, "x": exit_,
         "ret": ret, "ev": evaluated})
    return new_id


def _seed(engine: Engine) -> None:
    d = date(2026, 10, 1)
    before = datetime(2026, 10, 1, 14, 0, tzinfo=UTC)  # 10:00 ET, before the entry close
    replay = datetime(2026, 11, 6, 3, 0, tzinfo=UTC)
    with engine.begin() as conn:
        _signal(conn, st="insider", sid="a", sig_date=d, created=before, outcome="CORRECT",
                entry=d, exit_=date(2026, 10, 8), evaluated=datetime(2026, 10, 9, 22, 0, tzinfo=UTC), ret=2.0)
        _signal(conn, st="insider", sid="a", sig_date=d, created=before + timedelta(minutes=1), outcome="WRONG",
                entry=d, exit_=date(2026, 10, 8), evaluated=replay, ret=-2.0)
        _signal(conn, st="insider", sid="b", sig_date=d, created=before, outcome="NO_MOVE",
                entry=d, exit_=date(2026, 10, 8), evaluated=replay, direction="SELL", ret=0.2)
        _signal(conn, st="insider", sid="b", sig_date=d, created=before + timedelta(minutes=2),
                outcome="INELIGIBLE", entry=None, exit_=None, evaluated=replay)
        # Recorded after the entry close (21:00 UTC = 17:00 ET): exploratory.
        _signal(conn, st="insider", sid="c", sig_date=d, created=datetime(2026, 10, 1, 21, 0, tzinfo=UTC),
                outcome="CORRECT", entry=d, exit_=date(2026, 10, 8), evaluated=replay, ret=3.0)
        # Another version is ignored.
        _signal(conn, st="insider", sid="d", sig_date=d, created=before, outcome="CORRECT",
                entry=d, exit_=date(2026, 10, 8), evaluated=replay, version="sig-eval-3-dryrun")
        # QuiverQuant act rows collapse into one feed.
        _signal(conn, st="quiverquant", sid="qq_offexchange:SPY", sig_date=d, created=before,
                outcome="INELIGIBLE", entry=None, exit_=None, evaluated=replay)

        oracle = [
            # id, created, direction, expiry, confidence, model, verdict, scored, keep
            ("o1", datetime(2026, 10, 1, 13, tzinfo=UTC), "CALL", date(2026, 10, 10), 0.8, "m1", "hit",
             datetime(2026, 10, 11, tzinfo=UTC), True),
            ("o2", datetime(2026, 10, 1, 13, tzinfo=UTC), "PUT", date(2026, 10, 10), 0.6, "m1", "partial",
             datetime(2026, 10, 11, tzinfo=UTC), True),
            ("o3", datetime(2026, 10, 2, 13, tzinfo=UTC), "CALL", date(2026, 10, 10), None, "m1", "miss",
             datetime(2026, 10, 11, tzinfo=UTC), True),
            ("o4", datetime(2026, 10, 12, 13, tzinfo=UTC), "CALL", date(2026, 10, 10), 0.9, "m1", "hit",
             datetime(2026, 10, 13, tzinfo=UTC), True),   # created after expiry: exploratory
            ("o5", datetime(2026, 10, 1, 13, tzinfo=UTC), "CALL", date(2026, 10, 10), 0.9, "m1", "hit",
             datetime(2026, 10, 11, tzinfo=UTC), False),  # dedup duplicate: excluded
            ("o6", datetime(2026, 10, 1, 13, tzinfo=UTC), "NEUTRAL", date(2026, 10, 10), 0.9, "m1", "hit",
             datetime(2026, 10, 11, tzinfo=UTC), True),   # non-directional: excluded
            ("o7", datetime(2026, 9, 1, 13, tzinfo=UTC), "PUT", date(2026, 9, 10), None, "m2", "no_data",
             datetime(2026, 9, 20, tzinfo=UTC), True),    # rescored in hindsight
        ]
        for row in oracle:
            conn.execute(text(
                "INSERT INTO oracle_predictions (id, created_at, ticker, direction, expiry, confidence, "
                "model_name, verdict, scored_at, dedup_keep) VALUES (:i, :c, 'NVDA', :d, :e, :p, :m, :v, :s, :k)"),
                dict(zip("icdepmvsk", row)))
        conn.execute(text("INSERT INTO oracle_prediction_rescores VALUES ('o7', 'oracle_rescore_hindsight_v1', 'hit')"))

        legacy_created = datetime(2026, 10, 1, tzinfo=UTC)
        runs = [
            # created, horizon, model, score table, verdict, payload
            (legacy_created, "swing", "grid-crypto-v1", "base", "partial",
             '{"symbols": ["BTC"], "direction": "bullish", "signed_return": 0.03}'),
            (legacy_created, "swing", "grid-crypto-v1", "base", "miss",
             '{"symbols": ["SPY"], "direction": "bullish", "signed_return": 0.05}'),  # SPY-graded legacy
            (legacy_created, "swing", "grid-crypto-v1", "base", "miss",
             '{"symbols": ["ETH"], "direction": "neutral", "signed_return": 0.001}'),  # neutral
            (legacy_created, "swing", "grid-crypto-v1", "rescore", "miss",
             '{"symbols": ["BTC"], "direction": "bearish", "signed_return": -0.02}'),
            (datetime(2026, 11, 7, tzinfo=UTC), "macro", "astro-v2", "base", "hit",
             '{"symbols": ["GLD"], "direction": "bearish", "signed_return": 0.09}'),
        ]
        for created, horizon, model, table, verdict, payload in runs:
            run_id = conn.execute(text(
                "INSERT INTO prediction_run (created_at, horizon_label, model_version) VALUES (:c, :h, :m) "
                "RETURNING id"), {"c": created, "h": horizon, "m": model}).scalar()
            target = "prediction_score" if table == "base" else "prediction_score_legacy_rescore"
            conn.execute(text(
                f"INSERT INTO {target} (prediction_run_id, scored_at, verdict, raw_payload) "
                "VALUES (:r, :s, :v, CAST(:p AS jsonb))"),
                {"r": run_id, "s": created + timedelta(days=8), "v": verdict, "p": payload})


def test_load_all_forward_and_exploratory_columns(scratch):
    from evaluation import edge_report as er

    engine, schema = scratch
    _seed(engine)
    with engine.connect() as conn:
        conn.execute(text("SET TRANSACTION READ ONLY"))
        loaded = er.load_all(conn, astrogrid_schema=schema)
        conn.rollback()

    assert loaded["context"]["signal_outcome_mix"] == {"CORRECT": 2, "WRONG": 1, "NO_MOVE": 1, "INELIGIBLE": 2}
    insider = loaded["signal_source_type"].forward["insider"]
    (cell,) = insider.cells.values()
    assert (cell.hits, cell.n, cell.no_move, cell.long_calls, cell.live) == (1, 2, 1, 2, 1)
    assert insider.horizon_days == 14.0  # EVALUATION_WINDOWS["insider"]
    late = loaded["signal_source_type"].exploratory["insider [not forward]"]
    assert sum(c.hits for c in late.cells.values()) == 1
    assert set(loaded["signal_feed"].forward) == {"insider / a", "insider / b"}

    m1 = loaded["oracle_model"].forward["m1"]
    agg = er.DateCell()
    for c in m1.cells.values():
        agg.add(c)
    assert (agg.hits, agg.n, agg.target_hits, agg.long_calls, agg.prob_n, agg.prob_y) == (2, 3, 1, 2, 2, 2)
    assert agg.prob_sq == pytest.approx(0.2 ** 2 + 0.4 ** 2)
    assert set(loaded["oracle_model"].exploratory) == {"m1 [not forward]", "m2 [rescore:hindsight]"}

    ag = loaded["astrogrid_model"]
    crypto = ag.forward["grid-crypto-v1"]
    assert sum(c.n for c in crypto.cells.values()) == 1  # SPY-graded and neutral rows excluded
    assert sum(c.hits for c in crypto.cells.values()) == 1
    assert "astro-v2" in ag.forward and ag.forward["astro-v2"].horizon_days == 30.0
    assert set(ag.exploratory) == {"grid-crypto-v1 [legacy_rescore]"}
    assert loaded["context"]["astrogrid_verdict_mix"]["grid-crypto-v1"] == {"partial": 1, "miss": 1}

    report = er.build_report(loaded, n_boot=99)
    assert report["families"]["signal_source_type"]["forward"][0].verdict == "INSUFFICIENT"


def test_corrected_trust_and_readers_run_on_postgres(scratch, monkeypatch):
    import config
    from intelligence import trust_scorer as ts

    engine, _schema = scratch
    _seed(engine)
    monkeypatch.setattr(config, "settings", SimpleNamespace(
        GRID_TRUST_USE_CORRECTED_OUTCOMES=True, GRID_TRUST_PRUNE_NO_EDGE=False, GRID_TRUST_PRUNE_LIST_PATH=""))
    result = ts.compute_trust_scores(engine, today=date(2026, 11, 6))
    by_key = {(s["source_type"], s["source_id"]): s for s in result["sources"]}
    assert (by_key[("insider", "a")]["hit_count"], by_key[("insider", "a")]["miss_count"]) == (1, 1)
    assert by_key[("insider", "b")]["no_move_count"] == 1 and by_key[("insider", "b")]["ineligible_count"] == 1
    assert by_key[("insider", "b")]["hit_count"] == 0
    assert ("quiverquant", "qq_offexchange") in by_key
    assert ("insider", "d") not in by_key  # other evaluation version

    legacy = ts.compute_trust_scores(engine, use_corrected=False, today=date(2026, 11, 6))
    assert {s["source_id"] for s in legacy["sources"]} >= {"a", "b"}

    monkeypatch.setattr(ts, "_ensure_tables", lambda engine: None)
    assert ts.get_trusted_sources(engine, min_signals=1, min_trust=0.0) == []  # trust aggregates not written
    ts.detect_convergence(engine)  # executes the corrected NOT EXISTS query


def test_cli_engine_refuses_writes(scratch):
    import importlib

    engine, schema = scratch
    cli = importlib.import_module("scripts.edge_report")
    ro = cli.read_only_engine(str(engine.url.render_as_string(hide_password=False)), 30)
    try:
        with ro.connect() as conn, pytest.raises(DBAPIError):
            conn.execute(text(f'CREATE TABLE "{schema}".should_fail (x int)'))
    finally:
        ro.dispose()
