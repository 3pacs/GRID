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


def _make_engine(source_rows=None, cov_rows=None, err_rows=None, resolver_rows=None, family_rows=None, stale_rows=None,
                 run_rows=None, run_error=None):
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
        elif "FROM pull_log" in sql:
            if run_error is not None:
                raise run_error
            result.fetchall.return_value = run_rows or []
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
        elif "current_setting" in sql:
            result.scalar_one.return_value = "0"
        elif "MAX(audited_at)" in sql:
            # No data_freshness_audit fixture wired into this fake engine —
            # every test here exercises the pre-existing sources/families
            # contract, not the daily-audit reader (see
            # tests/test_daily_freshness_audit.py for that). "Never
            # configured" is the correct, honest default rather than a
            # truthy MagicMock that would blow up age-in-hours arithmetic.
            result.scalar_one_or_none.return_value = None
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
        engine = _make_engine(source_rows=[("acme_widgets", None, 0, 9, False)])
        with patch("api.routers.system.get_db_engine", return_value=engine):
            resp = client.get("/api/v1/system/pipeline-health", headers=_auth_header())
        assert resp.status_code == 200
        data = resp.json()
        src = data["sources"][0]
        # Never ran, never wrote: inactive, not "broken" (2026-11-07).
        assert src["status"] == "inactive"
        assert data["summary"]["broken"] == 0
        assert data["summary"]["inactive"] == 1
        fr = src["field_record"]
        assert fr["availability"] == "unavailable"
        assert fr["provenance"] is None
        assert fr["stale_reason"] == "never_configured"
        assert fr["value"] is None

    def test_stale_source_is_available_but_flagged_stale(self):
        # Fed_Liquidity is a daily source (48h threshold, "stale" up to 2x =
        # 96h): 3 days = 72h old lands in the stale band, not broken.
        old_pull = datetime.now(timezone.utc) - timedelta(days=3)
        engine = _make_engine(source_rows=[("Fed_Liquidity", old_pull, 3, 2, True)])
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
        engine = _make_engine(source_rows=[("yfinance", recent_pull, 0, 5, True)])
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
            ("yfinance", recent, 120, 40, True),
            ("Fed_Liquidity", old, 0, 5, True),
            ("acme_widgets", None, 0, 9, False),
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


def _ago(hours: float) -> datetime:
    return datetime.now(timezone.utc) - timedelta(hours=hours)


class TestPipelineHealthRunStates:
    """pull_log drives the per-source status (2026-11-07).

    Source rows are (name, last raw write, 48h rows, catalog id, active);
    run rows mirror CATALOG_RUN_SUMMARY_SQL: (source_id, puller_name,
    last_run_at, last_rows_at, runs_7d, status, rows_inserted, error).
    Names default to the 168 h window (none is in _SOURCE_SCHEDULE).
    """

    SOURCES = [
        ("acme_options", None, 0, 1, True),
        ("acme_monthly", _ago(300), 0, 2, True),
        ("acme_nokey", None, 0, 3, True),
        ("acme_failing", _ago(200), 0, 4, True),
        ("acme_stopped", _ago(900), 0, 5, False),
        ("acme_dupe", None, 0, 6, True),
    ]
    RUNS = [
        (1, "YFinance_Options", _ago(2), _ago(2), 4, "SUCCESS", 75994, None),
        (2, "acme_monthly", _ago(1), None, 7, "SUCCESS", 0, "NO_NEW_DATA: run completed, 0 rows written"),
        (3, "acme_nokey", _ago(3), None, 2, "SUCCESS", 0, "SKIPPED: ACME_API_KEY not configured"),
        (4, "acme_failing", _ago(1), None, 3, "FAILED", 0,
         "TIMEOUT: GET https://api.acme.test/v1?api_key=TOPSECRET999 timed out " + "z" * 600),
    ]

    def _get(self, **kwargs):
        engine = _make_engine(source_rows=self.SOURCES, **kwargs)
        with patch("api.routers.system.get_db_engine", return_value=engine):
            resp = client.get("/api/v1/system/pipeline-health", headers=_auth_header())
        assert resp.status_code == 200
        return resp

    def test_each_status_and_summary(self):
        resp = self._get(run_rows=self.RUNS)
        data = resp.json()
        by_name = {s["name"]: s for s in data["sources"]}
        assert {n: s["status"] for n, s in by_name.items()} == {
            "acme_options": "healthy",
            "acme_monthly": "quiet",
            "acme_nokey": "skipped_config",
            "acme_failing": "failing",
            "acme_stopped": "stopped",
            "acme_dupe": "inactive",
        }
        summary = data["summary"]
        assert summary["healthy"] == 1
        assert summary["quiet"] == 1
        assert summary["skipped_config"] == 1
        assert summary["failing"] == 1
        assert summary["stopped"] == 1
        assert summary["inactive"] == 1
        assert summary["stale"] == 0
        assert summary["broken"] == 2  # failing + stopped
        assert summary["active_sources"] == 5
        assert data["status_basis"] == "pull_log+raw_series"
        assert data["availability"] == "available"

        failing = by_name["acme_failing"]
        assert failing["last_status"] == "FAILED"
        assert failing["last_error_class"] == "TIMEOUT"
        assert failing["error"] == failing["last_error"]
        assert len(failing["last_error"]) <= 300
        assert "TOPSECRET999" not in resp.text
        assert failing["freshness"] == "red"
        assert failing["field_record"]["stale_reason"] == "fetch_failed"
        assert failing["field_record"]["availability"] == "available"  # had rows 200h ago

        options = by_name["acme_options"]
        assert options["last_rows_inserted"] == 75994
        assert options["runs_7d"] == 4
        assert options["last_success_with_rows_at"] is not None
        assert options["error"] is None

        nokey = by_name["acme_nokey"]
        assert nokey["last_error"] == "SKIPPED: ACME_API_KEY not configured"
        assert nokey["field_record"]["availability"] == "unavailable"
        assert nokey["field_record"]["stale_reason"] == "never_configured"

        assert by_name["acme_monthly"]["field_record"]["stale_reason"] == "not_published_yet"
        assert by_name["acme_stopped"]["catalog_active"] is False
        assert by_name["acme_dupe"]["runs_7d"] == 0
        assert by_name["acme_dupe"]["last_run_at"] is None

    def test_pull_log_failure_falls_back_visibly(self):
        resp = self._get(run_error=RuntimeError('relation "pull_log" does not exist'))
        data = resp.json()
        assert data["availability"] == "unavailable"
        assert data["stale_reason"] == "consumer_query_mismatch"
        assert data["status_basis"] == "raw_series_only"
        statuses = {s["name"]: s["status"] for s in data["sources"]}
        # Without runs, only raw_series is left to go on.
        assert statuses["acme_options"] == "inactive"
        assert statuses["acme_stopped"] == "stopped"


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
