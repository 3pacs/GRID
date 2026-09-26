"""Real-PostgreSQL proof for the persisted, read-only briefing sentiment GET.

Runs only against the explicit disposable CI database URL.  The test creates
and removes its own schema, writes through the real sentiment prediction
writer, and directs the route's unqualified table read to that schema.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, event, text

_TEST_DB_URL = os.environ.get("GRID_BRIEFING_ROUTE_TEST_DB_URL")

pytestmark = pytest.mark.skipif(
    not _TEST_DB_URL,
    reason="requires explicit GRID_BRIEFING_ROUTE_TEST_DB_URL",
)


def test_persisted_sentiment_route_uses_real_writer_payload_and_select_only(monkeypatch):
    from api.routers import briefing
    from intelligence.sentiment_scorer import (
        SentimentComponent,
        SentimentResult,
        _ensure_tables,
        log_prediction,
    )

    schema = f"briefing_sentiment_{uuid.uuid4().hex}"
    admin_engine = create_engine(_TEST_DB_URL, pool_pre_ping=True)
    route_engine = create_engine(
        _TEST_DB_URL,
        connect_args={"options": f"-csearch_path={schema}"},
        pool_pre_ping=True,
    )
    statements: list[str] = []

    try:
        with admin_engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA "{schema}"'))

        # Setup uses the same DDL the scheduled scorer owns; the GET is
        # observed only after setup, so any DDL/DML there fails this test.
        _ensure_tables(route_engine)
        result = SentimentResult(
            score=0.42,
            label="BULLISH",
            components=[
                SentimentComponent(
                    name="momentum",
                    raw_value=1.0,
                    score=0.6,
                    weight=0.08,
                    detail="writer-compatible fixture",
                )
            ],
            context="writer fixture",
            timestamp=datetime.now(timezone.utc).isoformat(),
            weights_version=7,
        )
        assert log_prediction(route_engine, result) is not None

        def _capture_sql(_conn, _cursor, statement, _params, _context, _many):
            statements.append(statement)

        event.listen(route_engine, "before_cursor_execute", _capture_sql)
        monkeypatch.setattr(briefing, "get_engine", lambda: route_engine)

        payload = asyncio.run(briefing.get_current_sentiment())

        assert payload["available"] is True
        assert payload["score"] == pytest.approx(0.42)
        assert payload["label"] == "BULLISH"
        assert payload["components"] == [
            {"name": "momentum", "score": 0.6, "weight": 0.08}
        ]
        assert payload["weights"] == {"momentum": 0.08}
        assert payload["weights_version"] == 7
        assert payload["prediction_date"]
        assert payload["created_at"]
        assert payload["served_at"]
        assert payload["source"] == "latest_precomputed_prediction"
        assert len(statements) == 1
        assert statements[0].lstrip().upper().startswith("SELECT")

        with route_engine.begin() as conn:
            conn.execute(text("DELETE FROM sentiment_predictions"))
        statements.clear()

        absent = asyncio.run(briefing.get_current_sentiment())

        assert absent == {
            "error": "No persisted sentiment prediction found",
            "available": False,
            "source": "latest_precomputed_prediction",
        }
        assert len(statements) == 1
        assert statements[0].lstrip().upper().startswith("SELECT")
    finally:
        event.remove(route_engine, "before_cursor_execute", _capture_sql) if "_capture_sql" in locals() else None
        route_engine.dispose()
        with admin_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        admin_engine.dispose()
