"""Exclude the SPY-graded legacy grid-crypto-v1 scores and rescore them (2026-11-05).

Until PR #968 ``generate_crypto_predictions.py`` stored ``target_symbols`` as a
character list; the scorer before 766d3058 graded those rows against SPY and
recorded ``raw_payload->'symbols' = ["SPY"]``. Readers now use the effective
score set (``AstroGridStore._effective_scores_sql``) and the legacy rows are
rescored against their coin into ``prediction_score_legacy_rescore``.

Unit tests run everywhere. The PostgreSQL tests need ``GRID_TEST_DB_URL``
pointing at a local, disposable database whose name contains ``test``; like
``test_spy_price_receipt_pg.py`` they reset the ``astrogrid`` schema, so run
them serially (``-p no:xdist`` or one worker).
"""

from __future__ import annotations

import importlib
import json
import os
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import DBAPIError

from store.astrogrid import (
    EFFECTIVE_SCORE_SOURCE,
    LEGACY_CRYPTO_PAYLOAD_CUTOFF,
    LEGACY_CRYPTO_RESCORE_VERSION,
    AstroGridStore,
    _weight_proposal_eligibility,
    is_excluded_legacy_score,
)

LEGACY_CREATED = datetime(2026, 9, 1, 16, 29, tzinfo=timezone.utc)
LEGACY_BTC = json.dumps(list(json.dumps(["BTC"])))  # '["[", "\\"", "B", ...]'
LEGACY_ETH = json.dumps(list(json.dumps(["ETH"])))
SPY_FALLBACK_PAYLOAD = {"symbols": ["SPY"], "prediction_id": "x"}


# --------------------------------------------------------------------------
# Unit tests (no database)
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "model_version, created_at, payload, expected",
    [
        ("grid-crypto-v1", LEGACY_CREATED, SPY_FALLBACK_PAYLOAD, True),
        ("grid-crypto-v1", LEGACY_CREATED, json.dumps(SPY_FALLBACK_PAYLOAD), True),
        ("grid-crypto-v1", LEGACY_CREATED, {"symbols": ["BTC"]}, False),
        ("grid-crypto-v1", LEGACY_CRYPTO_PAYLOAD_CUTOFF, SPY_FALLBACK_PAYLOAD, False),
        ("astrogrid-seed-v1", LEGACY_CREATED, SPY_FALLBACK_PAYLOAD, False),
        ("grid-crypto-v1", LEGACY_CREATED, {}, False),
        ("grid-crypto-v1", None, SPY_FALLBACK_PAYLOAD, False),
    ],
)
def test_is_excluded_legacy_score(model_version, created_at, payload, expected) -> None:
    assert is_excluded_legacy_score(model_version, created_at, payload) is expected


def test_weight_proposal_eligibility_requires_effective_score_marker() -> None:
    assert _weight_proposal_eligibility({"score_source": EFFECTIVE_SCORE_SOURCE}) == {
        "eligible_for_approval": True, "ineligible_reason": None,
    }
    for window in ({"limit": 12, "latest_by_variant": []}, {}, None, "junk"):
        assert _weight_proposal_eligibility(window)["eligible_for_approval"] is False


def test_approve_refuses_proposal_built_from_legacy_scores() -> None:
    engine = MagicMock()
    store = AstroGridStore(engine)
    proposal = {
        "weight_proposal_id": "proposal-old", "status": "pending_review",
        **_weight_proposal_eligibility({"limit": 12}),
    }
    store.list_weight_proposals = MagicMock(return_value=[proposal])
    store.ensure_active_weight_version = MagicMock()

    result = store.approve_weight_proposal("proposal-old", decided_by="anik")

    assert result["approval_refused"] is True
    assert "built_from_legacy_spy_graded_scores" in result["error"]
    store.ensure_active_weight_version.assert_not_called()
    engine.begin.assert_not_called()


def test_list_backtest_results_filters_char_list_symbol() -> None:
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchall.return_value = []
    AstroGridStore(engine).list_backtest_results()
    statement, params = conn.execute.call_args.args
    assert "br.target_symbol IS DISTINCT FROM :legacy_char_symbol" in str(statement)
    assert params["legacy_char_symbol"] == "["


def test_backtest_summary_reads_only_marked_runs() -> None:
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchall.return_value = []
    AstroGridStore(engine).get_backtest_summary()
    statement, params = conn.execute.call_args.args
    assert "params_payload->>'score_source' = :score_source" in str(statement)
    assert params["score_source"] == EFFECTIVE_SCORE_SOURCE


def test_runner_defaults_to_dry_run() -> None:
    runner = importlib.import_module("scripts.rescore_legacy_crypto_predictions")
    args = runner.parse_args([])
    assert args.apply is False and args.batch_size == 500 and args.limit is None
    with pytest.raises(SystemExit):
        runner.parse_args(["--batch-size", "0"])


# --------------------------------------------------------------------------
# PostgreSQL tests (real schema from the AstroGrid migration chain)
# --------------------------------------------------------------------------

_CHAIN = (
    "b6bba10f0fdb_add_astrogrid_schema_boundary",
    "c91b4a2e7d33_add_astrogrid_learning_loop_scaffold",
    "e2f6a9d3c4b1_add_astrogrid_scoring_class",
    "spy_close_receipt_20260922",
    "ag_legacy_rescore_20261105",
)


def _run_migration(conn, name: str) -> None:
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    module = importlib.import_module(f"migrations.versions.{name}")
    original = getattr(module, "op", None)
    module.op = Operations(MigrationContext.configure(conn))
    try:
        module.upgrade()
    finally:
        module.op = original


@pytest.fixture
def pg_engine() -> Engine:
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        pytest.skip("GRID_TEST_DB_URL is required for disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("legacy rescore tests require a local disposable test database")
    scratch = "legacy_rescore_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {scratch}"))
        conn.execute(text("DROP SCHEMA IF EXISTS astrogrid CASCADE"))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={scratch},public"})
    try:
        with engine.begin() as conn:
            # Public tables the scorer's SQL references (minimal shapes, as in
            # test_spy_price_receipt_pg.py), then the AstroGrid chain.
            conn.execute(text("CREATE TABLE source_catalog (id INTEGER PRIMARY KEY, name TEXT NOT NULL, priority_rank INTEGER NOT NULL)"))
            conn.execute(text("CREATE TABLE feature_registry (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)"))
            conn.execute(text("""
                CREATE TABLE raw_series (
                    id BIGSERIAL PRIMARY KEY, series_id TEXT NOT NULL,
                    source_id INTEGER NOT NULL REFERENCES source_catalog(id),
                    obs_date DATE NOT NULL, pull_timestamp TIMESTAMPTZ NOT NULL,
                    value DOUBLE PRECISION NOT NULL, raw_payload JSONB, pull_status TEXT NOT NULL)
            """))
            conn.execute(text("""
                CREATE TABLE resolved_series (
                    id BIGSERIAL PRIMARY KEY, feature_id INTEGER NOT NULL REFERENCES feature_registry(id),
                    obs_date DATE NOT NULL, release_date DATE NOT NULL, vintage_date DATE NOT NULL,
                    value DOUBLE PRECISION NOT NULL,
                    source_priority_used INTEGER REFERENCES source_catalog(id))
            """))
            conn.execute(text("CREATE TABLE regime_history (obs_date DATE, regime TEXT, confidence DOUBLE PRECISION)"))
            for name in _CHAIN:
                _run_migration(conn, name)
            conn.execute(text(
                "INSERT INTO feature_registry VALUES (1, 'btc_full'), (2, 'eth_full'), (3, 'sol_full'), (4, 'spy_full')"
            ))
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text("DROP SCHEMA IF EXISTS astrogrid CASCADE"))
            conn.execute(text(f"DROP SCHEMA IF EXISTS {scratch} CASCADE"))
        admin.dispose()


def _run(engine: Engine, *, model: str, created_at: datetime, symbols_json: str,
         as_of: datetime | None = None, horizon: str = "swing",
         call: str = "BTC bullish breakout", jsonb_string_payloads: bool = True) -> int:
    overlay = json.dumps(json.dumps({"prices": {"BTC": 100.0}})) if jsonb_string_payloads else "{}"
    with engine.begin() as conn:
        return conn.execute(text("""
            INSERT INTO astrogrid.prediction_run (
                prediction_id, created_at, as_of_ts, horizon_label, target_universe, target_symbols,
                question, call, timing, setup, invalidation, market_overlay_snapshot,
                mystical_feature_payload, grid_feature_payload, weight_version, model_version,
                live_or_local, status, scoring_class)
            VALUES (:pid, :created, :as_of, :horizon, 'hybrid', CAST(:symbols AS jsonb),
                    'q', :call, 't', 'setup', 'inv', CAST(:overlay AS jsonb),
                    CAST(:overlay AS jsonb), CAST(:overlay AS jsonb), 'astrogrid-v1', :model,
                    'live', 'pending', 'liquid_market')
            RETURNING id
        """), {
            "pid": uuid4().hex, "created": created_at, "as_of": as_of or created_at,
            "horizon": horizon, "symbols": symbols_json, "call": call, "overlay": overlay,
            "model": model,
        }).scalar_one()


def _score(engine: Engine, run_id: int, *, symbols: list[str], verdict: str = "hit") -> int:
    with engine.begin() as conn:
        return conn.execute(text("""
            INSERT INTO astrogrid.prediction_score (
                prediction_run_id, benchmark_symbol, realized_return, verdict, raw_payload)
            VALUES (:run, 'SPY', 0.02, :verdict, CAST(:payload AS jsonb))
            RETURNING id
        """), {"run": run_id, "verdict": verdict,
               "payload": json.dumps({"symbols": symbols})}).scalar_one()


def _price(engine: Engine, feature_id: int, obs_date: date, value: float) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value)
            VALUES (:f, :d, :r, :r, :v)
        """), {"f": feature_id, "d": obs_date, "r": obs_date + timedelta(days=1), "v": value})


def _effective_run_ids(engine: Engine) -> set[int]:
    store = AstroGridStore(engine)
    from store.astrogrid import _legacy_score_params

    with engine.connect() as conn:
        rows = conn.execute(
            text(f"SELECT prediction_run_id FROM {store._effective_scores_sql()} es"),
            _legacy_score_params(),
        ).fetchall()
    return {int(r[0]) for r in rows}


def test_effective_scores_drop_only_legacy_spy_scores(pg_engine) -> None:
    legacy_spy = _run(pg_engine, model="grid-crypto-v1", created_at=LEGACY_CREATED, symbols_json=LEGACY_BTC)
    legacy_other = _run(pg_engine, model="grid-crypto-v1", created_at=LEGACY_CREATED, symbols_json=LEGACY_ETH)
    seed_spy = _run(pg_engine, model="astrogrid-seed-v1", created_at=LEGACY_CREATED, symbols_json='["SPY"]')
    after_cutoff = _run(pg_engine, model="grid-crypto-v1", symbols_json='["BTC"]',
                        created_at=LEGACY_CRYPTO_PAYLOAD_CUTOFF + timedelta(hours=1))
    _score(pg_engine, legacy_spy, symbols=["SPY"])
    _score(pg_engine, legacy_other, symbols=["ETH"])
    _score(pg_engine, seed_spy, symbols=["SPY"])
    _score(pg_engine, after_cutoff, symbols=["SPY"])

    assert _effective_run_ids(pg_engine) == {legacy_other, seed_spy, after_cutoff}


def _legacy_fixture(engine: Engine, today: date) -> dict[str, int]:
    start = today - timedelta(days=20)
    ids = {
        # graded against SPY before; BTC priced at start and horizon end
        "spy_graded": _run(engine, model="grid-crypto-v1", created_at=LEGACY_CREATED,
                           as_of=datetime.combine(start, datetime.min.time(), timezone.utc),
                           symbols_json=LEGACY_BTC),
        # never scored; ETH has no price at its horizon end (only an older one)
        "unpriced": _run(engine, model="grid-crypto-v1", created_at=LEGACY_CREATED,
                         as_of=datetime.combine(start, datetime.min.time(), timezone.utc),
                         symbols_json=LEGACY_ETH),
        # horizon not reached yet
        "immature": _run(engine, model="grid-crypto-v1", created_at=LEGACY_CREATED,
                         as_of=datetime.combine(today - timedelta(days=2), datetime.min.time(), timezone.utc),
                         symbols_json=LEGACY_BTC),
    }
    ids["spy_score_id"] = _score(engine, ids["spy_graded"], symbols=["SPY"], verdict="miss")
    _price(engine, 1, start, 100.0)
    _price(engine, 1, start + timedelta(days=7), 110.0)  # horizon end: +10%
    _price(engine, 1, start + timedelta(days=12), 50.0)  # later crash must not be read
    _price(engine, 2, start, 2000.0)
    return ids


def _rescores(engine: Engine) -> list[dict]:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT prediction_run_id, score_version, superseded_score_id, target_symbols,
                   verdict, realized_return, raw_payload
            FROM astrogrid.prediction_score_legacy_rescore ORDER BY id
        """)).mappings().fetchall()
    return [dict(r) for r in rows]


def _count(engine: Engine, table: str) -> int:
    with engine.connect() as conn:
        return int(conn.execute(text(f"SELECT COUNT(*) FROM astrogrid.{table}")).scalar_one())


def test_backfill_dry_run_writes_nothing_and_apply_is_idempotent(pg_engine) -> None:
    today = datetime.now(timezone.utc).date()
    ids = _legacy_fixture(pg_engine, today)
    store = AstroGridStore(pg_engine)

    dry = store.rescore_legacy_crypto_predictions(apply=False, as_of_date=today, batch_size=2)
    assert (dry["candidates"], dry["scored"], dry["not_mature"]) == (3, 1, 1)
    assert dry["with_excluded_spy_score"] == 1
    assert dry["unscored_by_reason"] == {"stale_canonical_price": 1}
    assert dry["verdicts_by_symbol"] == {"BTC": {"hit": 1}}
    assert dry["inserted"] == 0
    assert _count(pg_engine, "prediction_score_legacy_rescore") == 0
    assert _count(pg_engine, "prediction_score_skip") == 0

    applied = store.rescore_legacy_crypto_predictions(apply=True, as_of_date=today, batch_size=2)
    assert (applied["inserted"], applied["scored"]) == (1, 1)
    [rescore] = _rescores(pg_engine)
    assert rescore["prediction_run_id"] == ids["spy_graded"]
    assert rescore["score_version"] == LEGACY_CRYPTO_RESCORE_VERSION
    assert rescore["superseded_score_id"] == ids["spy_score_id"]
    assert rescore["target_symbols"] == ["BTC"]
    # PIT: measured at the horizon end (+10%), not at the later crash
    assert rescore["realized_return"] == pytest.approx(0.10)
    assert rescore["verdict"] == "hit"
    assert rescore["raw_payload"]["rescored_from_legacy"] is True
    assert rescore["raw_payload"]["symbols"] == ["BTC"]
    assert _count(pg_engine, "prediction_score_skip") == 1  # unpriced ETH, stays pending
    # the original SPY score is untouched
    assert _count(pg_engine, "prediction_score") == 1

    again = store.rescore_legacy_crypto_predictions(apply=True, as_of_date=today)
    assert (again["candidates"], again["inserted"]) == (2, 0)
    assert _count(pg_engine, "prediction_score_legacy_rescore") == 1
    assert _count(pg_engine, "prediction_score_skip") == 1


def test_readers_count_rescore_not_legacy_spy_score(pg_engine) -> None:
    today = datetime.now(timezone.utc).date()
    ids = _legacy_fixture(pg_engine, today)
    seed = _run(pg_engine, model="astrogrid-seed-v1", created_at=LEGACY_CREATED, symbols_json='["SPY"]')
    _score(pg_engine, seed, symbols=["SPY"], verdict="hit")
    store = AstroGridStore(pg_engine)

    board = store.build_prediction_scoreboard()
    assert board["overall"]["scored"] == 1  # the seed score only
    assert board["overall"]["excluded_legacy_spy_scores"] == 1
    assert [row["symbol"] for row in board["by_symbol"]] == ["SPY"]
    statuses = {p["target_symbols"][0]: p["status"] for p in store.list_predictions(limit=10)
                if p["model_version"] == "grid-crypto-v1"}
    assert statuses == {"BTC": "pending", "ETH": "pending"}

    store.rescore_legacy_crypto_predictions(apply=True, as_of_date=today)

    board = store.build_prediction_scoreboard()
    assert board["overall"]["scored"] == 2
    assert {row["symbol"]: row["hits"] for row in board["by_symbol"]} == {"SPY": 1, "BTC": 1}
    detail = next(p for p in store.list_predictions(limit=10) if p["status"] == "hit"
                  and p["model_version"] == "grid-crypto-v1")
    assert detail["target_symbols"] == ["BTC"]

    backtest = store.run_backtests(strategy_variants=["grid_only"], limit=50)
    with pg_engine.connect() as conn:
        symbols = {r[0] for r in conn.execute(text("SELECT target_symbol FROM astrogrid.backtest_result"))}
        params = conn.execute(text("SELECT params_payload FROM astrogrid.backtest_run")).scalar_one()
    assert symbols == {"SPY", "BTC"}
    assert params["score_source"] == EFFECTIVE_SCORE_SOURCE
    assert backtest["runs"][0]["summary"]["total_predictions"] == 2
    latest = store.get_backtest_summary()["latest_by_variant"]["grid_only"]["summary"]
    assert latest["total_predictions"] == 2

    review = store.generate_review_run()
    assert review["based_on_prediction_count"] == 2
    [proposal] = store.list_weight_proposals()
    assert proposal["eligible_for_approval"] is True
    assert proposal["based_on_backtest_window"]["score_source"] == EFFECTIVE_SCORE_SOURCE
    assert ids["spy_score_id"]


def test_hourly_scorer_rescores_legacy_rows_and_bounds_skips(pg_engine) -> None:
    today = datetime.now(timezone.utc).date()
    ids = _legacy_fixture(pg_engine, today)
    store = AstroGridStore(pg_engine)

    first = store.score_predictions(as_of_date=today, limit=50)
    assert first["rescored_legacy"] == 1 and first["scored"] == 1
    assert _count(pg_engine, "prediction_score") == 1  # nothing new in the base table
    assert [r["prediction_run_id"] for r in _rescores(pg_engine)] == [ids["spy_graded"]]

    # Same evaluation day: the unpriced row is not re-read.
    second = store.score_predictions(as_of_date=today, limit=50)
    assert second["candidates"] == first["candidates"] - 2  # rescored + skipped today
    # It is retried once per day, at most SCORE_SKIP_MAX_ATTEMPTS days.
    with patch("store.astrogrid._utc_today", return_value=today + timedelta(days=5)):
        attempts = [
            store.score_predictions(as_of_date=today + timedelta(days=offset), limit=50)["skipped_no_price"]
            for offset in (1, 2, 3, 4)
        ]
    assert attempts == [1, 1, 0, 0]
    assert _count(pg_engine, "prediction_score_skip") == 3


def test_clean_symbols_in_legacy_window_score_normally(pg_engine) -> None:
    """The fixed generator wrote 16 clean rows at 23:38Z, before the cutoff."""
    today = datetime.now(timezone.utc).date()
    ids = _legacy_fixture(pg_engine, today)
    start = today - timedelta(days=20)
    clean = _run(pg_engine, model="grid-crypto-v1", created_at=LEGACY_CREATED, symbols_json='["BTC"]',
                 as_of=datetime.combine(start, datetime.min.time(), timezone.utc),
                 jsonb_string_payloads=False)
    store = AstroGridStore(pg_engine)

    backfill = store.rescore_legacy_crypto_predictions(apply=True, as_of_date=today)
    assert backfill["candidates"] == 3  # the char-list rows only
    assert clean not in {r["prediction_run_id"] for r in _rescores(pg_engine)}

    # The base-table scorer measures at the run date (unchanged rule), so it
    # needs that day's close.
    yesterday = today - timedelta(days=1)
    _price(pg_engine, 1, yesterday, 120.0)
    summary = store.score_predictions(as_of_date=yesterday, limit=50)
    assert summary["scored"] == 1 and summary["rescored_legacy"] == 0
    with pg_engine.connect() as conn:
        base = {r[0] for r in conn.execute(text("SELECT prediction_run_id FROM astrogrid.prediction_score"))}
    assert base == {ids["spy_graded"], clean}  # clean row scored in the base table


def test_permanent_skip_is_not_retried(pg_engine) -> None:
    today = datetime.now(timezone.utc).date()
    start = datetime.combine(today - timedelta(days=20), datetime.min.time(), timezone.utc)
    _run(pg_engine, model="astrogrid-seed-v1", created_at=start, as_of=start, symbols_json='["NOPE"]')
    store = AstroGridStore(pg_engine)
    assert store.score_predictions(as_of_date=today)["unscored"][0]["reason"] == "unsupported_target"
    assert store.score_predictions(as_of_date=today + timedelta(days=1))["candidates"] == 0


def test_rescore_and_skip_tables_are_append_only(pg_engine) -> None:
    today = datetime.now(timezone.utc).date()
    _legacy_fixture(pg_engine, today)
    AstroGridStore(pg_engine).rescore_legacy_crypto_predictions(apply=True, as_of_date=today)
    for table in ("prediction_score_legacy_rescore", "prediction_score_skip"):
        with pytest.raises(DBAPIError, match="append-only"), pg_engine.begin() as conn:
            conn.execute(text(f"UPDATE astrogrid.{table} SET id = id"))
        with pytest.raises(DBAPIError, match="append-only"), pg_engine.begin() as conn:
            conn.execute(text(f"DELETE FROM astrogrid.{table}"))


def test_approval_refuses_legacy_proposal_and_accepts_marked_one(pg_engine) -> None:
    with pg_engine.begin() as conn:
        for pid, window in (("proposal-legacy", {"limit": 12}),
                            ("proposal-new", {"limit": 12, "score_source": EFFECTIVE_SCORE_SOURCE})):
            conn.execute(text("""
                INSERT INTO astrogrid.weight_proposal (weight_proposal_id, based_on_backtest_window, status)
                VALUES (:pid, CAST(:window AS jsonb), 'pending_review')
            """), {"pid": pid, "window": json.dumps(window)})
    store = AstroGridStore(pg_engine)

    refused = store.approve_weight_proposal("proposal-legacy", decided_by="test")
    approved = store.approve_weight_proposal("proposal-new", decided_by="test")

    assert refused["approval_refused"] is True
    assert approved["status"] == "approved"
    with pg_engine.connect() as conn:
        decided = {r[0] for r in conn.execute(text(
            "SELECT weight_proposal_id FROM astrogrid.weight_proposal_decision"))}
    assert decided == {"proposal-new"}


def _schema_snapshot(conn) -> tuple:
    columns = conn.execute(text("""
        SELECT table_name, column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = 'astrogrid'
          AND table_name IN ('prediction_score_legacy_rescore', 'prediction_score_skip')
        ORDER BY 1, 2
    """)).fetchall()
    constraints = conn.execute(text("""
        SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint
        WHERE conrelid IN ('astrogrid.prediction_score_legacy_rescore'::regclass,
                           'astrogrid.prediction_score_skip'::regclass)
        ORDER BY 1
    """)).fetchall()
    triggers = conn.execute(text("""
        SELECT tgname FROM pg_trigger
        WHERE tgrelid IN ('astrogrid.prediction_score_legacy_rescore'::regclass,
                          'astrogrid.prediction_score_skip'::regclass)
          AND NOT tgisinternal ORDER BY 1
    """)).fetchall()
    indexes = conn.execute(text("""
        SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'astrogrid'
          AND tablename IN ('prediction_score_legacy_rescore', 'prediction_score_skip') ORDER BY 1
    """)).fetchall()
    return tuple(map(tuple, columns)), tuple(map(tuple, constraints)), tuple(map(tuple, triggers)), tuple(map(tuple, indexes))


def test_migration_rerun_is_a_noop(pg_engine) -> None:
    today = datetime.now(timezone.utc).date()
    _legacy_fixture(pg_engine, today)
    AstroGridStore(pg_engine).rescore_legacy_crypto_predictions(apply=True, as_of_date=today)
    with pg_engine.begin() as conn:
        before = _schema_snapshot(conn)
        assert len(before[2]) == 2  # both append-only triggers
        _run_migration(conn, "ag_legacy_rescore_20261105")
        after = _schema_snapshot(conn)
    assert after == before
    assert _count(pg_engine, "prediction_score_legacy_rescore") == 1
    assert _count(pg_engine, "prediction_score_skip") == 1


def test_runner_dry_run_uses_read_only_sessions(pg_engine) -> None:
    today = datetime.now(timezone.utc).date()
    _legacy_fixture(pg_engine, today)
    runner = importlib.import_module("scripts.rescore_legacy_crypto_predictions")
    read_only = runner.read_only_engine(pg_engine)
    with pytest.raises(DBAPIError, match="read-only"), read_only.begin() as conn:
        conn.execute(text("INSERT INTO astrogrid.prediction_score_skip "
                          "(prediction_run_id, attempt_date, reason) VALUES (1, CURRENT_DATE, 'x')"))
    read_only.dispose()

    assert runner.main(["--as-of", today.isoformat()], engine=pg_engine) == 0
    assert _count(pg_engine, "prediction_score_legacy_rescore") == 0
    assert runner.main(["--apply", "--as-of", today.isoformat()], engine=pg_engine) == 0
    assert _count(pg_engine, "prediction_score_legacy_rescore") == 1
