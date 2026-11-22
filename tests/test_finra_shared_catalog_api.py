"""/system/pipeline-health, /system/freshness and the architecture puller stats
judge FINRA_SHORT_VOLUME by the short-volume puller's own series.

finra_dix writes under the same source_catalog row and bumps its
last_pull_at, so on 2026-11-18..11-20 (short-volume obs missing, DIX landing
daily) every one of these read the row as fresh. See
ingestion/source_freshness.py::SERIES_ANCHORS and
tests/test_source_freshness_anchors.py for the store-level reads.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only")
os.environ.setdefault("GRID_JWT_EXPIRE_HOURS", "1")
os.environ.setdefault(
    "GRID_MASTER_PASSWORD_HASH",
    "$2b$12$abcdefghijklmnopqrstuuFb1mY3p5oXq0rN8sxqf6vV2QcVx1zSi",
)

from fastapi.testclient import TestClient  # noqa: E402

from api.auth import create_token  # noqa: E402
from api.main import app  # noqa: E402
from api.routers import system  # noqa: E402

client = TestClient(app)

FINRA = "FINRA_SHORT_VOLUME"
FINRA_ID = 4679
MARKET_RATIO = "finra:short_volume_agg:market_ratio"
SPY = "finra:short_volume:SPY"


def _auth_header() -> dict[str, str]:
    return {"Authorization": f"Bearer {create_token(expires_hours=1)}"}


def _ago(hours: float) -> datetime:
    return datetime.now(timezone.utc) - timedelta(hours=hours)


# DIX landed this morning; the short-volume series last landed 3 days ago.
DIX_LANDED = _ago(4)
SHORT_VOLUME_LANDED = _ago(72)


def _engine(*, source_rows=(), run_rows=(), stale_rows=(), puller_rows=(), anchors=None):
    """Fake engine dispatching on query text, like tests/test_pipeline_health_contract.py.

    ``anchors`` maps series_id -> (obs_date, landed_at) for the
    store.observations read_latest query (one row, or none).
    """
    anchors = anchors or {}
    calls: list[tuple[str, dict]] = []
    conn = MagicMock()

    def execute(clause, params=None):
        sql = str(clause)
        calls.append((sql, dict(params or {})))
        result = MagicMock()
        result.fetchall.return_value = []
        result.fetchone.return_value = None
        if "r.series_id = :sid" in sql and "LIMIT 1" in sql:
            hit = anchors.get((params or {}).get("sid"))
            result.fetchone.return_value = (hit[0], 0.48, hit[1], FINRA) if hit else None
        elif "series_count" in sql:
            result.fetchall.return_value = list(puller_rows)
        elif "FROM source_catalog sc" in sql and "ORDER BY sc.name" in sql:
            result.fetchall.return_value = list(source_rows)
        elif "FROM pull_log" in sql:
            result.fetchall.return_value = list(run_rows)
        elif "sc.last_pull_at < NOW()" in sql:
            result.fetchall.return_value = list(stale_rows)
        elif "current_setting" in sql:
            result.scalar_one.return_value = "0"
        elif "MAX(audited_at)" in sql:
            result.scalar_one_or_none.return_value = None
        return result

    conn.execute.side_effect = execute
    engine = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine, calls


STALE_ANCHORS = {
    MARKET_RATIO: (date.today() - timedelta(days=4), SHORT_VOLUME_LANDED),
    SPY: (date.today() - timedelta(days=4), SHORT_VOLUME_LANDED),
}
FRESH_ANCHORS = {
    MARKET_RATIO: (date.today() - timedelta(days=1), _ago(3)),
    SPY: (date.today() - timedelta(days=1), _ago(3)),
}
# (source_id, puller_name, last_run_at, last_rows_at, runs_7d, status, rows_inserted, error)
DIX_ONLY_RUNS = [
    (FINRA_ID, "smart:finra_dix", DIX_LANDED, DIX_LANDED, 7, "SUCCESS", 1, None),
    (FINRA_ID, "smart:finra_short_volume", _ago(2), None, 7, "SUCCESS", 0,
     "NO_NEW_DATA: run completed, 0 rows written"),
]
# (name, last raw write, 48h rows, catalog id, active): the newest row under
# the source is DIX's.
FINRA_SOURCE_ROW = [(FINRA, DIX_LANDED, 1, FINRA_ID, True)]


def _pipeline(engine) -> dict:
    with patch("api.routers.system.get_db_engine", return_value=engine):
        resp = client.get("/api/v1/system/pipeline-health", headers=_auth_header())
    assert resp.status_code == 200
    return {s["name"]: s for s in resp.json()["sources"]}[FINRA]


def test_pipeline_health_dix_only_day_is_not_healthy():
    engine, calls = _engine(source_rows=FINRA_SOURCE_ROW, run_rows=DIX_ONLY_RUNS, anchors=STALE_ANCHORS)

    finra = _pipeline(engine)

    assert finra["status"] == "quiet"  # the owner ran and wrote nothing; not "healthy"
    assert finra["last_pull"] == SHORT_VOLUME_LANDED.isoformat()
    assert finra["last_success_with_rows_at"] == SHORT_VOLUME_LANDED.isoformat()
    assert finra["last_status"] == "SUCCESS"
    assert finra["last_error"].startswith("NO_NEW_DATA")
    anchor_reads = [p for sql, p in calls if "r.series_id = :sid" in sql]
    assert {p["sid"] for p in anchor_reads} == {MARKET_RATIO, SPY}
    assert all(p["source"] == FINRA for p in anchor_reads)


def test_pipeline_health_dix_failure_is_not_the_short_volume_failure():
    runs = [
        (FINRA_ID, "smart:finra_dix", _ago(1), None, 7, "FAILED", 0, "TIMEOUT: 600s"),
        (FINRA_ID, "smart:finra_short_volume", _ago(2), _ago(2), 7, "SUCCESS", 12_000, None),
    ]
    engine, _ = _engine(source_rows=FINRA_SOURCE_ROW, run_rows=runs, anchors=FRESH_ANCHORS)

    finra = _pipeline(engine)

    assert finra["status"] == "healthy"
    assert finra["last_status"] == "SUCCESS"
    assert finra["error"] is None


def test_pipeline_health_missing_anchor_is_not_healthy():
    engine, _ = _engine(
        source_rows=FINRA_SOURCE_ROW, run_rows=DIX_ONLY_RUNS,
        anchors={SPY: FRESH_ANCHORS[SPY]},  # per-symbol rows landed, aggregate never did
    )

    finra = _pipeline(engine)

    assert finra["status"] != "healthy"
    assert finra["last_pull"] is None


def _freshness(engine) -> list[dict]:
    with patch("api.routers.system.get_db_engine", return_value=engine):
        resp = client.get("/api/v1/system/freshness", headers=_auth_header())
    assert resp.status_code == 200
    return resp.json()["stale_sources"]


def test_freshness_dix_only_day_lists_short_volume_stale():
    # The stale query always returns the anchored row; its last_pull_at is DIX's bump.
    engine, calls = _engine(
        stale_rows=[(FINRA, DIX_LANDED), ("fred:UNRATE", _ago(500))],
        anchors=STALE_ANCHORS,
    )

    stale = {s["source"]: s for s in _freshness(engine)}

    assert stale[FINRA]["last_pull"] == SHORT_VOLUME_LANDED.isoformat()
    assert stale[FINRA]["stale"] is True
    assert "fred:UNRATE" in stale
    sql, params = next((s, p) for s, p in calls if "sc.last_pull_at < NOW()" in s)
    assert "sc.name = ANY(:anchored)" in sql
    assert params["anchored"] == [FINRA]
    assert params["limit"] == 21


def test_freshness_fresh_short_volume_series_is_not_listed():
    engine, _ = _engine(stale_rows=[(FINRA, DIX_LANDED)], anchors=FRESH_ANCHORS)

    assert _freshness(engine) == []


def test_architecture_puller_stats_judge_finra_by_its_series():
    # (name, 48h row count, newest row or last_pull_at, series count)
    engine, _ = _engine(puller_rows=[(FINRA, 1, DIX_LANDED, 3)], anchors=STALE_ANCHORS)

    stats = {p["id"]: p for p in system._get_puller_stats(engine)}

    # 72 h since the short-volume series landed: past the 48 h daily window.
    # The DIX row (4 h old) used to make this "healthy".
    assert stats["finra_short_volume"]["status"] == "stale"
    assert stats["finra_short_volume"]["last_run"] == "3d ago"
