"""Disposable PostgreSQL proof for the gold GET cache and refresh paths."""

import json
import os
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine.url import make_url

import api.routers.dad as dad


@pytest.mark.xdist_group("postgres")
def test_gold_get_absent_schema_legacy_hit_and_live_refresh_are_read_only():
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        pytest.skip("GRID_TEST_DB_URL is required for disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("gold read-only proof requires a local disposable test database")

    schema = "dad_gold_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    statements = []

    def record_sql(_conn, _cursor, statement, _params, _context, _many):
        statements.append(statement.strip().upper())

    event.listen(engine, "before_cursor_execute", record_sql)
    workbook = {
        "status": "ready", "summary": {"mentions": 2, "file_count": 1, "sheet_count": 1},
        "workbook": {"files": [], "sheets": [], "evidence": []},
        "source_lanes": [], "dad_stats": [], "fit_signals": [],
        "source": {"attached": True, "db_path": "synthetic"},
    }
    dad._GOLD_MEMORY_CACHE.clear()
    try:
        with patch.object(dad, "get_db_engine", return_value=engine), \
             patch.object(dad, "_research_db_path", return_value=Path("/missing/research.duckdb")), \
             patch.object(dad, "_load_workbook_context", return_value=workbook), \
             patch.object(dad, "_fetch_finviz_snapshot", return_value={"Price": "123.45"}):
            # No summary table exists. A cold GET still calculates useful workbook gold.
            cold = dad.get_dad_ticker_gold("AAPL", refresh_finviz=False, _token="test")
            assert cold["status"] == "ready"
            assert cold["gold"]["score"] > 0
            assert cold["cache"]["hit"] is False
            warm = dad.get_dad_ticker_gold("AAPL", refresh_finviz=False, _token="test")
            assert warm["cache"]["hit"] is True
            assert warm["gold"] == cold["gold"]
            assert not any("CREATE TABLE" in sql for sql in statements)
            assert not any(sql.startswith(("INSERT", "UPDATE", "DELETE", "ALTER", "CREATE")) for sql in statements)

            # Legacy persisted cache rows remain readable without schema bootstrap.
            dad._GOLD_MEMORY_CACHE.clear()
            with engine.begin() as conn:
                conn.execute(text("""CREATE TABLE dad_ticker_summary_cache (
                    ticker TEXT PRIMARY KEY, payload_version TEXT NOT NULL,
                    generated_at TIMESTAMPTZ NOT NULL, research_db_path TEXT,
                    research_db_mtime DOUBLE PRECISION, payload JSONB NOT NULL,
                    timings JSONB NOT NULL DEFAULT '{}'::jsonb)"""))
                conn.execute(text("""INSERT INTO dad_ticker_summary_cache
                    (ticker, payload_version, generated_at, research_db_path, payload)
                    VALUES ('MSFT', :version, NOW(), '/missing/research.duckdb',
                    CAST(:payload AS JSONB))"""),
                    {"version": dad.DAD_CACHE_VERSION, "payload": json.dumps({
                        "ticker": "MSFT", "status": "ready", "gold": {"score": 77}, "performance": {},
                    })})
            statements.clear()
            hit = dad.get_dad_ticker_gold("MSFT", refresh_finviz=False, _token="test")
            assert hit["cache"]["hit"] is True
            assert hit["gold"]["score"] == 77
            assert not any(sql.startswith(("INSERT", "UPDATE", "DELETE", "ALTER", "CREATE")) for sql in statements)

            # Explicit refresh uses provider data immediately, without persisting raw rows.
            statements.clear()
            refreshed = dad.get_dad_ticker_gold("AAPL", refresh_finviz=True, _token="test")
            assert refreshed["finviz"]["status"] == "ready"
            assert refreshed["finviz"]["stats"][0]["parsed"] == 123.45
            assert refreshed["finviz"]["source"] == "live-readonly"
            assert refreshed["finviz"]["rows_inserted"] == 0
            assert not any(sql.startswith(("INSERT", "UPDATE", "DELETE", "ALTER", "CREATE")) for sql in statements)
    finally:
        dad._GOLD_MEMORY_CACHE.clear()
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
        admin.dispose()
