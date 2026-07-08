from __future__ import annotations

import json
import uuid

from fastapi.testclient import TestClient

from agent_hub.app import create_app


class FakeObjectStore:
    def __init__(self, fail: bool = False):
        self.fail = fail
        self.objects: dict[tuple[str, str], tuple[bytes, str]] = {}
        self.put_calls: list[tuple[str, str, bytes, str]] = []

    def put(self, bucket: str, key: str, data: bytes, content_type: str) -> bool:
        if self.fail:
            return False
        self.objects[(bucket, key)] = (data, content_type)
        self.put_calls.append((bucket, key, data, content_type))
        return True

    def check_health(self, bucket: str) -> bool:
        return not self.fail


class FakeReportRepository:
    def __init__(self, db_fail: bool = False):
        self.db_fail = db_fail
        self.rows_by_key: dict[str, dict] = {}
        self.insert_calls: list[dict] = []

    def get_by_idempotency_key(self, idempotency_key: str) -> dict | None:
        return self.rows_by_key.get(idempotency_key)

    def insert_report(self, report: dict) -> dict:
        self.insert_calls.append(report)
        row = {
            "id": str(uuid.uuid4()),
            "report_uri": report["report_uri"],
            "idempotency_key": report["idempotency_key"],
        }
        self.rows_by_key[report["idempotency_key"]] = row
        return row

    def check_health(self) -> bool:
        return not self.db_fail



def _client(repo=None, store=None) -> TestClient:
    return TestClient(
        create_app(
            report_repository=repo or FakeReportRepository(),
            object_store=store or FakeObjectStore(),
            token="test-token",
        )
    )


def _payload() -> dict:
    return {
        "date": "2026-05-07",
        "agent": "codex-loop",
        "host": "gridz4",
        "title": "Nightly GRID report",
        "body_md": "# Report\n\nAll clear.",
        "body_json": {"status": "ok", "count": 3},
        "tags": ["grid", "nightly"],
        "idempotency_key": "codex-loop-gridz4-2026-05-07",
    }


def test_report_ingest_writes_markdown_json_sidecar_and_database_row():
    repo = FakeReportRepository()
    store = FakeObjectStore()
    client = _client(repo=repo, store=store)

    response = client.post(
        "/report",
        json=_payload(),
        headers={"Authorization": "Bearer test-token"},
    )

    assert response.status_code == 200
    body = response.json()
    assert uuid.UUID(body["id"])
    assert body["report_uri"] == (
        "s3://agent-reports/2026-05-07/codex-loop/gridz4/"
        "codex-loop-gridz4-2026-05-07.md"
    )
    assert body["idempotent"] is False

    md_key = (
        "2026-05-07/codex-loop/gridz4/"
        "codex-loop-gridz4-2026-05-07.md"
    )
    json_key = md_key.replace(".md", ".json")
    assert store.objects[("agent-reports", md_key)][0] == b"# Report\n\nAll clear."
    assert store.objects[("agent-reports", md_key)][1] == "text/markdown; charset=utf-8"

    sidecar = json.loads(store.objects[("agent-reports", json_key)][0].decode("utf-8"))
    assert sidecar["title"] == "Nightly GRID report"
    assert sidecar["body_json"] == {"status": "ok", "count": 3}
    assert sidecar["report_uri"] == body["report_uri"]

    assert len(repo.insert_calls) == 1
    inserted = repo.insert_calls[0]
    assert inserted["agent"] == "codex-loop"
    assert inserted["host"] == "gridz4"
    assert inserted["tags"] == ["grid", "nightly"]
    assert inserted["body_json"] == {"status": "ok", "count": 3}


def test_report_ingest_is_idempotent_on_idempotency_key():
    repo = FakeReportRepository()
    store = FakeObjectStore()
    client = _client(repo=repo, store=store)
    headers = {"Authorization": "Bearer test-token"}

    first = client.post("/report", json=_payload(), headers=headers)
    second = client.post("/report", json=_payload(), headers=headers)

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]
    assert second.json()["report_uri"] == first.json()["report_uri"]
    assert second.json()["idempotent"] is True
    assert len(repo.insert_calls) == 1
    assert len(store.put_calls) == 2


def test_report_ingest_rejects_missing_required_fields_before_writes():
    repo = FakeReportRepository()
    store = FakeObjectStore()
    payload = _payload()
    del payload["body_md"]

    response = _client(repo=repo, store=store).post(
        "/report",
        json=payload,
        headers={"Authorization": "Bearer test-token"},
    )

    assert response.status_code == 422
    assert repo.insert_calls == []
    assert store.put_calls == []


def test_report_ingest_requires_bearer_token():
    response = _client().post("/report", json=_payload())

    assert response.status_code == 401


def test_report_ingest_does_not_insert_when_object_store_write_fails():
    repo = FakeReportRepository()
    store = FakeObjectStore(fail=True)

    response = _client(repo=repo, store=store).post(
        "/report",
        json=_payload(),
        headers={"Authorization": "Bearer test-token"},
    )

    assert response.status_code == 503
    assert repo.insert_calls == []


def test_health_check_returns_200_on_success():
    client = _client()
    # test GET
    response_get = client.get("/health")
    assert response_get.status_code == 200
    assert response_get.json()["status"] == "ok"
    assert response_get.json()["details"]["postgres"] == "ok"
    assert response_get.json()["details"]["minio"] == "ok"

    # test HEAD
    response_head = client.head("/health")
    assert response_head.status_code == 200
    assert response_head.text == ""


def test_health_check_returns_503_on_db_failure():
    repo = FakeReportRepository(db_fail=True)
    client = _client(repo=repo)
    response = client.get("/health")
    assert response.status_code == 503
    assert response.json()["detail"]["details"]["postgres"] == "error"
    assert response.json()["detail"]["details"]["minio"] == "ok"


def test_health_check_returns_503_on_s3_failure():
    store = FakeObjectStore(fail=True)
    client = _client(store=store)
    response = client.get("/health")
    assert response.status_code == 503
    assert response.json()["detail"]["details"]["postgres"] == "ok"
    assert response.json()["detail"]["details"]["minio"] == "error"

