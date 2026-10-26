"""GRID W4c — GET /api/v1/snapshots/research/latest.

Covers the three response shapes the endpoint must produce, using fakes
only (no real Postgres): a run/event exists (with and without a
hypothesis_registry outcome to merge in), no research_run rows exist yet,
and the underlying query fails (missing table / unreachable database) —
which must never surface as an HTTP 500.

Uses the minimal-app + dependency_overrides pattern from
tests/test_snapshot_categories.py, but monkeypatches
scripts.research_status's functions directly rather than building a
SQL-parsing fake engine — the router's job being tested here is "shape the
response correctly given what research_status returns", not
research_status's own SQL (that's tests/test_research_status.py's job).

Run with:
    DB_PASSWORD=testpass PYTHONUTF8=1 python -m pytest tests/test_snapshots_research_latest.py -q
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from api.auth import require_auth  # noqa: E402
from api.routers import snapshots as snapshots_router  # noqa: E402
from api.routers.snapshots import router as snapshots_api_router  # noqa: E402
import scripts.research_status as research_status  # noqa: E402


def _build_client(monkeypatch) -> TestClient:
    monkeypatch.setattr(snapshots_router, "get_db_engine", lambda: object())
    app = FastAPI()
    app.include_router(snapshots_api_router)
    app.dependency_overrides[require_auth] = lambda: {"sub": "test-user"}
    return TestClient(app)


def test_returns_the_flattened_record_when_a_run_exists(monkeypatch):
    record = {
        "id": 42,
        "created_at": "2026-09-18T10:00:00+00:00",
        "run_id": "abc-123",
        "status": "failed",
        "phase": "feature_list",
        "error": "column does not exist",
        "error_category": "db_load_failure",
        "iteration": None,
        "iterations": 0,
        "skip_reasons": [],
        "failure_reasons": [],
        "duration_s": 0.42,
        "generation": None,
        "code_sha": "deadbeef",
        "inputs": None,
    }
    monkeypatch.setattr(research_status, "latest_research_run_result", lambda engine: dict(record))
    monkeypatch.setattr(research_status, "latest_hypothesis_outcome", lambda engine: None)

    client = _build_client(monkeypatch)
    resp = client.get("/api/v1/snapshots/research/latest")

    assert resp.status_code == 200
    body = resp.json()
    assert body["run_id"] == "abc-123"
    assert body["status"] == "failed"
    assert body["phase"] == "feature_list"
    assert "latest_hypothesis" not in body


def test_merges_in_the_latest_hypothesis_outcome_when_present(monkeypatch):
    record = {
        "id": 43, "created_at": "2026-09-18T11:00:00+00:00", "run_id": "abc-124",
        "status": "ok", "phase": "complete", "error": None, "error_category": None,
        "iteration": 1, "iterations": 1, "skip_reasons": [], "failure_reasons": [],
        "duration_s": 12.3, "generation": None, "code_sha": "deadbeef",
        "inputs": {"feature_ids_count": 3, "market_snapshot_keys": ["vix"], "evaluation_version": None},
    }
    hypothesis = {
        "id": 7, "statement": "When VIX spikes...", "layer": "REGIME",
        "state": "PASSED", "kill_reason": None, "updated_at": "2026-09-18T11:00:01+00:00",
    }
    monkeypatch.setattr(research_status, "latest_research_run_result", lambda engine: dict(record))
    monkeypatch.setattr(research_status, "latest_hypothesis_outcome", lambda engine: dict(hypothesis))

    client = _build_client(monkeypatch)
    body = client.get("/api/v1/snapshots/research/latest").json()

    assert body["status"] == "ok"
    assert body["latest_hypothesis"]["state"] == "PASSED"
    assert body["latest_hypothesis"]["statement"] == "When VIX spikes..."
    assert body["inputs"]["evaluation_version"] is None


def test_returns_no_runs_when_the_table_is_reachable_but_empty(monkeypatch):
    monkeypatch.setattr(research_status, "latest_research_run_result", lambda engine: {"status": "no_runs"})
    hypothesis_calls = []
    monkeypatch.setattr(
        research_status, "latest_hypothesis_outcome",
        lambda engine: hypothesis_calls.append(1),
    )

    client = _build_client(monkeypatch)
    resp = client.get("/api/v1/snapshots/research/latest")

    assert resp.status_code == 200
    assert resp.json() == {"status": "no_runs"}
    # Don't bother querying hypothesis_registry when there is no run to
    # report on in the first place.
    assert hypothesis_calls == []


def test_never_500s_when_the_underlying_table_is_missing(monkeypatch):
    monkeypatch.setattr(
        research_status, "latest_research_run_result",
        lambda engine: {"status": "unavailable", "reason": 'relation "analytical_snapshots" does not exist'},
    )
    hypothesis_calls = []
    monkeypatch.setattr(
        research_status, "latest_hypothesis_outcome",
        lambda engine: hypothesis_calls.append(1),
    )

    client = _build_client(monkeypatch)
    resp = client.get("/api/v1/snapshots/research/latest")

    assert resp.status_code == 200  # never a 500
    body = resp.json()
    assert body["status"] == "unavailable"
    assert "analytical_snapshots" in body["reason"]
    assert hypothesis_calls == []


def test_requires_auth(monkeypatch):
    """The endpoint must be behind require_auth like every other snapshots
    route — no dependency_overrides here, so a request with no credentials
    must not reach the handler."""
    monkeypatch.setattr(snapshots_router, "get_db_engine", lambda: object())
    app = FastAPI()
    app.include_router(snapshots_api_router)
    client = TestClient(app)

    resp = client.get("/api/v1/snapshots/research/latest")

    assert resp.status_code in (401, 403)
