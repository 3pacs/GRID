"""Contract tests for the `daily_audit` field on /pipeline-health and
/freshness (api/routers/system.py::_read_daily_freshness_audit).

F3: both endpoints previously computed everything from live lateral scans
over `raw_series`/`resolved_series`, which can 524 under load on the
production-sized tables. `data_freshness_audit` is refreshed once daily by
`grid-data-freshness-check.timer` and is small/indexed, so reading it here
is a fixed-cost addition regardless of table growth elsewhere. These tests
use a fake `data_freshness_audit` (a MagicMock engine dispatching on SQL
text, matching the style already used in test_pipeline_health_contract.py)
to pin three states: available (fresh), unavailable/"stale" (audit exists
but its run is more than one full cycle overdue), and unavailable/
"never_configured" (no audit row at all) -- plus that a query failure is
classified and never silently rendered as "everything is DEAD".
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


def _make_audit_engine(*, audited_at=None, bucket_rows=None, source_rows=None, raise_on="none"):
    """A fake engine whose only configured behaviour is the daily-audit
    reader's three queries. Every *other* query used by pipeline_health()/
    freshness() (source status, coverage, resolver, stale_sources, ...)
    gets the same "nothing found" default already exercised in
    test_pipeline_health_contract.py, so these tests isolate `daily_audit`
    without needing to fake the rest of either endpoint's SQL.
    """
    conn = MagicMock()

    def execute(clause, params=None):
        sql = str(clause)
        result = MagicMock()
        if "current_setting" in sql:
            result.scalar_one.return_value = "0"
            return result
        if "set_config" in sql:
            return result
        if "MAX(audited_at)" in sql:
            if raise_on == "max_audited_at":
                raise RuntimeError("relation \"data_freshness_audit\" does not exist")
            result.scalar_one_or_none.return_value = audited_at
            return result
        if "GROUP BY bucket" in sql:
            result.fetchall.return_value = bucket_rows or []
            return result
        if "DISTINCT source_table" in sql:
            result.fetchall.return_value = source_rows or []
            return result
        result.fetchall.return_value = []
        result.fetchone.return_value = None
        result.scalar_one_or_none.return_value = None
        return result

    conn.execute.side_effect = execute
    engine = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine


class TestDailyAuditOnPipelineHealth:
    def test_available_when_audit_is_fresh(self):
        audited_at = datetime.now(timezone.utc) - timedelta(hours=2)
        engine = _make_audit_engine(
            audited_at=audited_at,
            bucket_rows=[("DEAD", 5), ("FRESH", 12), ("STALE_30+", 3), ("STALE_7_30", 1)],
            source_rows=[("ticker_metrics_daily",)],
        )
        with patch("api.routers.system.get_db_engine", return_value=engine):
            resp = client.get("/api/v1/system/pipeline-health", headers=_auth_header())
        assert resp.status_code == 200
        audit = resp.json()["daily_audit"]
        assert audit["availability"] == "available"
        assert audit["stale_reason"] is None
        assert audit["audited_at"] == audited_at.isoformat()
        assert audit["total_tickers"] == 21
        assert audit["source_tables"] == ["ticker_metrics_daily"]
        assert {b["bucket"]: b["ticker_count"] for b in audit["buckets"]} == {
            "DEAD": 5, "FRESH": 12, "STALE_30+": 3, "STALE_7_30": 1,
        }
        assert audit["field_record"]["availability"] == "available"
        assert audit["field_record"]["provenance"] == "measured"

    def test_unavailable_when_audit_is_missing(self):
        engine = _make_audit_engine(audited_at=None)
        with patch("api.routers.system.get_db_engine", return_value=engine):
            resp = client.get("/api/v1/system/pipeline-health", headers=_auth_header())
        audit = resp.json()["daily_audit"]
        assert audit["availability"] == "unavailable"
        assert audit["stale_reason"] == "never_configured"
        assert audit["audited_at"] is None
        assert audit["buckets"] == []
        assert audit["total_tickers"] == 0

    def test_unavailable_when_audit_is_stale(self):
        # The timer runs daily at 05:00 UTC; 50h is more than one full
        # missed cycle, not just "running a little late".
        stale_audited_at = datetime.now(timezone.utc) - timedelta(hours=50)
        engine = _make_audit_engine(
            audited_at=stale_audited_at,
            bucket_rows=[("DEAD", 700), ("FRESH", 18)],
            source_rows=[("ticker_metrics_daily",)],
        )
        with patch("api.routers.system.get_db_engine", return_value=engine):
            resp = client.get("/api/v1/system/pipeline-health", headers=_auth_header())
        audit = resp.json()["daily_audit"]
        assert audit["availability"] == "unavailable"
        assert audit["stale_reason"] == "stale"
        # Last-known numbers stay visible for context but are flagged, not
        # hidden and not presented as current.
        assert audit["total_tickers"] == 718
        assert audit["audited_at"] == stale_audited_at.isoformat()
        assert audit["field_record"]["availability"] == "unavailable"
        assert audit["field_record"]["value"] is None

    def test_query_failure_is_classified_not_swallowed_as_missing(self):
        engine = _make_audit_engine(raise_on="max_audited_at")
        with patch("api.routers.system.get_db_engine", return_value=engine):
            resp = client.get("/api/v1/system/pipeline-health", headers=_auth_header())
        assert resp.status_code == 200
        audit = resp.json()["daily_audit"]
        assert audit["availability"] == "unavailable"
        assert audit["stale_reason"] == "consumer_query_mismatch"

    def test_daily_audit_failure_does_not_affect_the_rest_of_the_response(self):
        """An audit-table problem is its own, independent signal -- it must
        not flip the whole pipeline-health response to unavailable when the
        source/coverage/resolver queries themselves succeeded."""
        engine = _make_audit_engine(raise_on="max_audited_at")
        with patch("api.routers.system.get_db_engine", return_value=engine):
            resp = client.get("/api/v1/system/pipeline-health", headers=_auth_header())
        data = resp.json()
        assert data["availability"] == "available"
        assert data["stale_reason"] is None
        assert data["daily_audit"]["availability"] == "unavailable"


class TestDailyAuditOnFreshness:
    def test_available_when_audit_is_fresh(self):
        audited_at = datetime.now(timezone.utc) - timedelta(hours=1)
        engine = _make_audit_engine(
            audited_at=audited_at,
            bucket_rows=[("FRESH", 100)],
            source_rows=[("ticker_metrics_daily",)],
        )
        with patch("api.routers.system.get_db_engine", return_value=engine):
            resp = client.get("/api/v1/system/freshness", headers=_auth_header())
        audit = resp.json()["daily_audit"]
        assert audit["availability"] == "available"
        assert audit["total_tickers"] == 100

    def test_unavailable_when_audit_is_missing(self):
        engine = _make_audit_engine(audited_at=None)
        with patch("api.routers.system.get_db_engine", return_value=engine):
            resp = client.get("/api/v1/system/freshness", headers=_auth_header())
        audit = resp.json()["daily_audit"]
        assert audit["availability"] == "unavailable"
        assert audit["stale_reason"] == "never_configured"
        # Independent of the (also empty here) family-based freshness
        # signal, which keeps its own pre-existing overall_status contract.
        assert resp.json()["overall_status"] == "RED"
