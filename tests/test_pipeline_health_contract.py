"""Contract-adoption tests for /api/v1/system/freshness and /pipeline-health.

Before this change, an exception anywhere inside `pipeline_health()`'s try
block fell through to a plain 200 with `sources`/`coverage`/`recent_errors`
all at their empty defaults — indistinguishable from "this pipeline
genuinely has zero sources" (see PipelineHealth.jsx's summary tiles, which
render `summary.total_sources || 0` etc. either way). These tests pin the
new behaviour: each source gets a `field_record`
(`store/availability_fields.py::FieldRecord.to_dict()`), and a total query
failure sets a top-level `availability: "unavailable"` + `stale_reason`
instead of silently returning empty lists.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
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

client = TestClient(app)


def _auth_header() -> dict[str, str]:
    return {"Authorization": f"Bearer {create_token(expires_hours=1)}"}


def _make_engine(source_rows=None, cov_rows=None, err_rows=None, resolver_rows=None, family_rows=None, stale_rows=None):
    """A fake engine whose ``conn.execute`` dispatches on the query text.

    Each ``*_rows`` argument is the ``fetchall``/``fetchone`` payload for the
    matching query in ``pipeline_health()``/``freshness()``; omitted ones
    default to an empty result, exactly like the real "nothing found" case.
    """
    conn = MagicMock()

    def execute(clause, params=None):
        sql = str(clause)
        result = MagicMock()
        if "FROM source_catalog sc" in sql and "ORDER BY sc.name" in sql:
            result.fetchall.return_value = source_rows or []
        elif "rs.has_data" in sql:
            result.fetchall.return_value = cov_rows or []
        elif "rs.latest_date >= CURRENT_DATE" in sql:
            result.fetchall.return_value = family_rows or []
        elif "sc.last_pull_at < NOW()" in sql:
            result.fetchall.return_value = stale_rows or []
        elif "FROM server_log" in sql:
            result.fetchall.return_value = err_rows or []
        elif "WITH recent_raw AS" in sql:
            result.fetchone.return_value = (resolver_rows or {}).get("pending", (0,))
        elif "SELECT MAX(vintage_date)" in sql:
            result.fetchone.return_value = (resolver_rows or {}).get("last_run", (None,))
        elif "vintage_date >= CURRENT_DATE - INTERVAL" in sql:
            result.fetchone.return_value = (resolver_rows or {}).get("last_resolved", (0,))
        else:
            result.fetchall.return_value = []
            result.fetchone.return_value = None
        return result

    conn.execute.side_effect = execute
    engine = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine


class TestPipelineHealthFieldRecords:
    def test_never_configured_source_is_unavailable(self):
        engine = _make_engine(source_rows=[("acme_widgets", None, 0, 0)])
        with patch("api.routers.system.get_db_engine", return_value=engine):
            resp = client.get("/api/v1/system/pipeline-health", headers=_auth_header())
        assert resp.status_code == 200
        data = resp.json()
        src = data["sources"][0]
        assert src["status"] == "broken"
        fr = src["field_record"]
        assert fr["availability"] == "unavailable"
        assert fr["provenance"] is None
        assert fr["stale_reason"] == "never_configured"
        assert fr["value"] is None

    def test_stale_source_is_available_but_flagged_stale(self):
        # Fed_Liquidity is a daily source (48h threshold, "stale" up to 2x =
        # 96h): 3 days = 72h old lands in the stale band, not broken.
        old_pull = datetime.now(timezone.utc) - timedelta(days=3)
        engine = _make_engine(source_rows=[("Fed_Liquidity", old_pull, 3, 2)])
        with patch("api.routers.system.get_db_engine", return_value=engine):
            resp = client.get("/api/v1/system/pipeline-health", headers=_auth_header())
        data = resp.json()
        src = data["sources"][0]
        assert src["status"] == "stale"
        fr = src["field_record"]
        assert fr["availability"] == "available"
        assert fr["provenance"] == "measured"
        assert fr["stale_reason"] == "stale"
        assert fr["ingested_at"] is not None

    def test_measured_zero_rows_source_is_not_empty_source(self):
        # A healthy, recently-pulled source whose 48h row count happens to
        # be 0 (e.g. it pulled just outside the window) must NOT be
        # relabelled empty_source -- this query cannot tell "the last pull
        # returned nothing" from "no new rows landed in the counting
        # window", so it must not guess.
        recent_pull = datetime.now(timezone.utc) - timedelta(hours=1)
        engine = _make_engine(source_rows=[("yfinance", recent_pull, 0, 5)])
        with patch("api.routers.system.get_db_engine", return_value=engine):
            resp = client.get("/api/v1/system/pipeline-health", headers=_auth_header())
        data = resp.json()
        src = data["sources"][0]
        assert src["status"] == "healthy"
        assert src["rows_last_pull"] == 0
        fr = src["field_record"]
        assert fr["availability"] == "available"
        assert fr["stale_reason"] is None  # not "empty_source", not invented

    def test_representative_mixed_sources(self):
        recent = datetime.now(timezone.utc) - timedelta(hours=2)
        old = datetime.now(timezone.utc) - timedelta(days=10)
        engine = _make_engine(source_rows=[
            ("yfinance", recent, 120, 40),
            ("Fed_Liquidity", old, 0, 5),
            ("acme_widgets", None, 0, 0),
        ])
        with patch("api.routers.system.get_db_engine", return_value=engine):
            resp = client.get("/api/v1/system/pipeline-health", headers=_auth_header())
        data = resp.json()
        assert data["availability"] == "available"
        assert data["stale_reason"] is None
        assert len(data["sources"]) == 3
        by_name = {s["name"]: s for s in data["sources"]}
        assert by_name["yfinance"]["field_record"]["availability"] == "available"
        assert by_name["Fed_Liquidity"]["field_record"]["stale_reason"] == "stale"
        assert by_name["acme_widgets"]["field_record"]["availability"] == "unavailable"


class TestPipelineHealthFailurePath:
    def test_query_exception_returns_explicit_unavailable_not_empty_zero(self):
        engine = MagicMock()
        engine.connect.side_effect = RuntimeError("connection refused: could not connect to server")
        with patch("api.routers.system.get_db_engine", return_value=engine):
            resp = client.get("/api/v1/system/pipeline-health", headers=_auth_header())
        assert resp.status_code == 200
        data = resp.json()
        # Old behaviour: sources == [] and summary all-zero look identical
        # to "no sources exist". New behaviour: the top level says outright
        # that the computation failed, and why.
        assert data["sources"] == []
        assert data["summary"]["total_sources"] == 0
        assert data["availability"] == "unavailable"
        assert data["stale_reason"] == "fetch_failed"

    def test_schema_mismatch_exception_classified_as_query_mismatch(self):
        engine = MagicMock()
        engine.connect.side_effect = RuntimeError('column sc."bogus_column" does not exist')
        with patch("api.routers.system.get_db_engine", return_value=engine):
            resp = client.get("/api/v1/system/pipeline-health", headers=_auth_header())
        data = resp.json()
        assert data["availability"] == "unavailable"
        assert data["stale_reason"] == "consumer_query_mismatch"


class TestFreshnessFieldRecords:
    def test_never_pulled_stale_source_is_unavailable(self):
        engine = _make_engine(stale_rows=[("acme_widgets", None)])
        with patch("api.routers.system.get_db_engine", return_value=engine):
            resp = client.get("/api/v1/system/freshness", headers=_auth_header())
        data = resp.json()
        src = data["stale_sources"][0]
        fr = src["field_record"]
        assert fr["availability"] == "unavailable"
        assert fr["stale_reason"] == "never_configured"

    def test_old_pull_stale_source_is_available_with_stale_reason(self):
        old_pull = datetime.now(timezone.utc) - timedelta(days=3)
        engine = _make_engine(stale_rows=[("Fed_Liquidity", old_pull)])
        with patch("api.routers.system.get_db_engine", return_value=engine):
            resp = client.get("/api/v1/system/freshness", headers=_auth_header())
        data = resp.json()
        src = data["stale_sources"][0]
        fr = src["field_record"]
        assert fr["availability"] == "available"
        assert fr["provenance"] == "measured"
        assert fr["stale_reason"] == "stale"
        assert data["availability"] == "available"

    def test_query_exception_marks_top_level_unavailable(self):
        engine = MagicMock()
        engine.connect.side_effect = RuntimeError("statement timeout")
        with patch("api.routers.system.get_db_engine", return_value=engine):
            resp = client.get("/api/v1/system/freshness", headers=_auth_header())
        data = resp.json()
        assert data["availability"] == "unavailable"
        assert data["stale_reason"] == "fetch_failed"
        # Existing behaviour preserved: worst-case family status.
        assert data["overall_status"] == "RED"
