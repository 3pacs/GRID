"""Real-PostgreSQL proof that the F2 GET routes issue SELECT statements only.

Each test gets a disposable schema, hooks ``before_cursor_execute`` (as the
#641 read-contract tests do), calls the route, and asserts that no INSERT,
UPDATE, DELETE or DDL reached the database — and that the tables those GETs
used to create or fill are still absent/empty afterwards.

Runs in CI's main pytest step (``DB_URL``); skipped without PostgreSQL.
"""

from __future__ import annotations

import asyncio
import os
import re
import uuid
from contextlib import contextmanager
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, text

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("DB_PASSWORD", "testpass")

from api.auth import require_auth  # noqa: E402

_DB_URL = os.environ.get("GRID_TEST_DB_URL") or os.environ.get("DB_URL")
pytestmark = [
    pytest.mark.skipif(not _DB_URL, reason="requires disposable PostgreSQL via GRID_TEST_DB_URL or DB_URL"),
    pytest.mark.xdist_group("postgres"),
]

_WRITE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|TRUNCATE|MERGE|GRANT|REVOKE)\b",
    re.IGNORECASE,
)


@contextmanager
def _schema():
    admin = create_engine(_DB_URL)
    name = "f2_readonly_" + uuid.uuid4().hex[:12]
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


@contextmanager
def _recording(engine):
    with engine.connect() as conn:  # warm the dialect before recording
        conn.execute(text("SELECT 1"))
    statements: list[str] = []

    def record(_conn, _cursor, statement, _params, _context, _many):
        statements.append(statement.strip())

    event.listen(engine, "before_cursor_execute", record)
    try:
        yield statements
    finally:
        event.remove(engine, "before_cursor_execute", record)


def _assert_select_only(statements: list[str]) -> None:
    assert statements, "expected the route to read the database"
    writes = [s for s in statements if _WRITE.search(re.sub(r"'[^']*'", "''", s))]
    assert writes == [], writes
    assert all(s.upper().startswith(("SELECT", "WITH")) for s in statements), statements


def _regclass(engine, table: str):
    with engine.connect() as conn:
        return conn.execute(text("SELECT to_regclass(:t)"), {"t": table}).scalar()


def _seed_signal_sources(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE signal_sources (
                id BIGSERIAL PRIMARY KEY, source_type TEXT NOT NULL,
                source_id TEXT NOT NULL, ticker TEXT NOT NULL,
                signal_type TEXT NOT NULL, signal_date DATE NOT NULL,
                signal_value JSONB, outcome TEXT DEFAULT 'PENDING',
                trust_score DOUBLE PRECISION DEFAULT 0.5, metadata JSONB
            )
        """))
        conn.execute(text("""
            INSERT INTO signal_sources (source_type, source_id, ticker, signal_type, signal_date)
            VALUES ('congressional', 'Rep A', 'AAA', 'BUY', CURRENT_DATE - 3),
                   ('insider', 'Officer B', 'AAA', 'BUY', CURRENT_DATE - 2),
                   ('darkpool', 'Pool C', 'AAA', 'BUY', CURRENT_DATE - 1)
        """))


def test_thesis_get_is_select_only():
    from analysis import thesis_scorer
    from api.routers import intelligence_thesis

    with _schema() as engine:
        _seed_signal_sources(engine)
        intelligence_thesis._thesis_cache.clear()
        thesis_scorer._thesis_result_cache["data"] = None
        with _recording(engine) as statements, \
             patch.object(intelligence_thesis, "get_db_engine", return_value=engine):
            body = asyncio.run(intelligence_thesis.get_unified_thesis(_token="t"))
        intelligence_thesis._thesis_cache.clear()
        thesis_scorer._thesis_result_cache["data"] = None
        assert "overall_direction" in body
        _assert_select_only(statements)
        assert _regclass(engine, "thesis_snapshots") is None


def test_causation_and_causal_chain_gets_are_select_only():
    from api.routers import intelligence_forensics as f

    with _schema() as engine:
        _seed_signal_sources(engine)
        with _recording(engine) as statements, \
             patch.object(f, "get_db_engine", return_value=engine), \
             patch("intelligence.causation_scoring._try_llm_narrative", return_value=None), \
             patch("intelligence.causation_graph._try_chain_llm_narrative", return_value=None):
            batch = f.get_causation(ticker=None, days=30, _token="t")
            single = f.get_causation(ticker="AAA", days=30, _token="t")
            chains = asyncio.run(f.get_causal_chains(ticker="AAA", hops=3, days=30, _token="t"))
            longest = asyncio.run(f.get_causal_chains(ticker=None, hops=3, days=30, _token="t"))
            active = asyncio.run(f.get_active_causal_chains(_token="t"))
        for payload in (batch, single, chains, longest, active):
            assert isinstance(payload, dict)
        _assert_select_only(statements)
        # Previously these GETs ran ensure_table (CREATE) and stored results.
        assert _regclass(engine, "causal_links") is None
        assert _regclass(engine, "causal_chains") is None


def test_forensics_get_does_not_create_or_store_reports():
    from api.routers import intelligence_forensics as f

    with _schema() as engine:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE forensic_reports (
                    id BIGSERIAL PRIMARY KEY, ticker TEXT NOT NULL, move_date DATE NOT NULL,
                    move_pct DOUBLE PRECISION, preceding_events JSONB, warning_signals INT,
                    key_actors JSONB, narrative TEXT, pattern_match JSONB,
                    confidence DOUBLE PRECISION, created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            conn.execute(text("""
                INSERT INTO forensic_reports (ticker, move_date, move_pct, narrative, confidence)
                VALUES ('AAA', CURRENT_DATE - 5, 3.1, 'stored', 0.4)
            """))
            # Empty PIT tables so _get_price_moves falls through to the
            # options spot-price series below (a fallback prod also uses).
            conn.execute(text("CREATE TABLE feature_registry (id INT PRIMARY KEY, name TEXT)"))
            conn.execute(text(
                "CREATE TABLE resolved_series (feature_id INT, obs_date DATE, value DOUBLE PRECISION)"
            ))
            conn.execute(text("""
                CREATE TABLE options_daily_signals (
                    ticker TEXT, signal_date DATE, spot_price DOUBLE PRECISION
                )
            """))
            conn.execute(text("""
                INSERT INTO options_daily_signals (ticker, signal_date, spot_price)
                SELECT 'AAA', CURRENT_DATE - g, CASE WHEN g % 2 = 0 THEN 100 ELSE 105 END
                FROM generate_series(1, 20) AS g
            """))
        with _recording(engine) as statements, \
             patch.object(f, "get_db_engine", return_value=engine), \
             patch("intelligence.forensics._get_llm_narrative", return_value=None), \
             patch("intelligence.forensics._get_llm_summary", return_value=None):
            body = asyncio.run(f.get_forensic_reports(ticker="AAA", days=30, _token="t"))
        assert body["count"] == 1
        _assert_select_only(statements)
        with engine.connect() as conn:
            assert conn.execute(text("SELECT COUNT(*) FROM forensic_reports")).scalar() == 1


def test_watchlist_gets_are_select_only_and_missing_table_is_503():
    from api.routers import watchlist, watchlist_helpers

    app = FastAPI()
    app.include_router(watchlist.router)
    app.dependency_overrides[require_auth] = lambda: "test"
    client = TestClient(app)

    with _schema() as engine:
        with _recording(engine) as statements, \
             patch("api.routers.watchlist_core.get_db_engine", return_value=engine), \
             patch.object(watchlist_helpers, "get_db_engine", return_value=engine), \
             patch.object(watchlist_helpers, "_table_ready", False):
            for path in ("/api/v1/watchlist/", "/api/v1/watchlist/enriched",
                         "/api/v1/watchlist/preload"):
                response = client.get(path)
                assert response.status_code == 503, (path, response.text)
        assert not any(_WRITE.search(s) for s in statements), statements
        assert _regclass(engine, "watchlist") is None

        with patch.object(watchlist_helpers, "get_db_engine", return_value=engine):
            watchlist_helpers._ensure_watchlist_table()
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO watchlist (ticker, display_name, asset_type) VALUES ('AAA', 'Alpha', 'stock')"
            ))
        watchlist_helpers._analysis_cache.clear()
        with _recording(engine) as statements, \
             patch("api.routers.watchlist_core.get_db_engine", return_value=engine), \
             patch.object(watchlist_helpers, "get_db_engine", return_value=engine), \
             patch("api.routers.watchlist_core._fetch_live_price", return_value=None), \
             patch.object(watchlist_helpers, "_table_ready", False):
            listed = client.get("/api/v1/watchlist/")
            enriched = client.get("/api/v1/watchlist/enriched")
            preload = client.get("/api/v1/watchlist/preload")
        watchlist_helpers._analysis_cache.clear()
        assert listed.status_code == 200 and listed.json()["total"] == 1
        assert enriched.status_code == 200
        assert preload.status_code == 200
        _assert_select_only(statements)


def _seed_observations_tables(engine) -> None:
    """Empty ``source_catalog``/``raw_series`` — enough for
    ``store.observations.read_window`` (and the regime state-vector
    dimension reads built on it) to run and return no rows, rather than
    erroring on a missing table."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE source_catalog (
                id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL
            )
        """))
        conn.execute(text("""
            CREATE TABLE raw_series (
                series_id TEXT NOT NULL,
                source_id BIGINT NOT NULL REFERENCES source_catalog(id),
                obs_date DATE NOT NULL,
                pull_timestamp TIMESTAMPTZ NOT NULL,
                value DOUBLE PRECISION,
                pull_status TEXT NOT NULL
            )
        """))


def test_regime_gets_are_select_only():
    """``/regime`` and ``/regime/analogs`` used to write-on-GET via
    ``cache_state_vector`` (Wave 3 W3.2, PR #713). Both now call
    ``get_or_compute_state_vector(..., persist=False)``; this is the
    real-Postgres proof that neither route issues a write, and that
    ``regime_state_vectors`` — the table the old code created and filled
    on every GET — is never created."""
    from api.routers import intelligence_regime as ir

    with _schema() as engine:
        _seed_observations_tables(engine)
        with _recording(engine) as statements, \
             patch.object(ir, "get_db_engine", return_value=engine):
            regime_body = asyncio.run(ir.get_regime(_token="t"))
            analogs_body = asyncio.run(
                ir.get_regime_analogs(n=20, min_quality=0.4, include_timesfm=False, _token="t")
            )

        # Empty raw_series -> every dimension is missing -> below the
        # completeness floor -> both routes report "available: false"
        # rather than computing/serving a partial vector.
        assert regime_body["available"] is False
        assert "reason" in regime_body
        assert analogs_body["available"] is False
        assert "reason" in analogs_body

        _assert_select_only(statements)
        assert _regclass(engine, "regime_state_vectors") is None
