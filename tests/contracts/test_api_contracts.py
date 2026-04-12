from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.auth import require_auth
from api.routers.contracts import router
from contracts import observability as obs


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(router)
    # FastAPI dependency_overrides is the correct way to bypass auth —
    # patching api.auth after import does not rebind the already-imported
    # symbol inside api.routers.contracts.
    app.dependency_overrides[require_auth] = lambda: {"role": "admin"}
    return TestClient(app)


def setup_function(_):
    obs.reset()


def test_metrics_endpoint_returns_prometheus_text():
    obs.emitted("PullLifecycle")
    client = _client()

    r = client.get("/api/v1/contracts/metrics")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/plain")
    assert "contracts_emitted_total" in r.text


def test_lineage_endpoint_returns_empty_for_unknown_correlation(monkeypatch):
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchall.return_value = []
    engine = MagicMock()
    engine.begin.return_value.__enter__.return_value = mock_conn
    engine.begin.return_value.__exit__.return_value = False

    from api.routers import contracts as api_mod
    monkeypatch.setattr(api_mod, "get_db_engine", lambda: engine)

    client = _client()
    r = client.get(f"/api/v1/contracts/lineage/{uuid4()}")
    assert r.status_code == 200
    assert r.json() == {"events": []}


def test_dead_letter_replay_endpoint_returns_success(monkeypatch):
    from api.routers import contracts as api_mod

    engine = MagicMock()
    monkeypatch.setattr(api_mod, "get_db_engine", lambda: engine)

    def fake_load(engine, contract_type, limit):
        from contracts.dead_letter import DeadLetterEntry
        return [
            DeadLetterEntry(
                id=1,
                event_id=uuid4(),
                contract_type="PullLifecycle",
                payload={
                    "producer_module": "t",
                    "correlation_id": str(uuid4()),
                    "puller_name": "fred",
                    "state": "COMPLETED",
                },
                consumer="x",
                error_type="CONSUMER_EXCEPTION",
                error_detail="x",
                retry_count=0,
                next_retry_at=None,
                failed_at=datetime.now(timezone.utc),
                correlation_id=None,
            )
        ]

    monkeypatch.setattr(api_mod, "_load_filtered", fake_load)
    monkeypatch.setattr(api_mod, "replay_entry", lambda engine, entry: True)

    client = _client()
    r = client.post("/api/v1/contracts/dead-letter/1/replay")
    assert r.status_code == 200
    assert r.json() == {"success": 1, "failed": 0}
