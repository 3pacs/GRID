from __future__ import annotations

import json
import os
import re
from datetime import date
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from agent_hub.object_store import MinioReportObjectStore, ReportObjectStore
from agent_hub.repository import PostgresReportRepository, ReportRepository


DEFAULT_BUCKET = "agent-reports"
DEFAULT_TOKEN_FILE = "/etc/agent-hub/token"
_SEGMENT_RE = re.compile(r"[^A-Za-z0-9._-]+")


class AgentReportIn(BaseModel):
    date: date
    agent: str = Field(min_length=1)
    host: str = Field(min_length=1)
    title: str = Field(min_length=1)
    body_md: str = Field(min_length=1)
    body_json: Any | None = None
    tags: list[str] = Field(default_factory=list)
    idempotency_key: str = Field(min_length=1)


class AgentReportOut(BaseModel):
    id: str
    report_uri: str
    idempotency_key: str
    idempotent: bool


def _safe_segment(value: str) -> str:
    segment = _SEGMENT_RE.sub("-", value.strip()).strip(".-")
    return segment[:120] or "unknown"


def object_key_for(report: AgentReportIn, suffix: str) -> str:
    return (
        f"{report.date.isoformat()}/"
        f"{_safe_segment(report.agent)}/"
        f"{_safe_segment(report.host)}/"
        f"{_safe_segment(report.idempotency_key)}{suffix}"
    )


def report_uri(bucket: str, md_key: str) -> str:
    return f"s3://{bucket}/{md_key}"


def load_token(token_file: str = DEFAULT_TOKEN_FILE) -> str:
    env_token = os.getenv("AGENT_HUB_TOKEN", "").strip()
    if env_token:
        return env_token
    token = Path(token_file).read_text(encoding="utf-8").strip()
    if not token:
        raise RuntimeError(f"empty agent hub token file: {token_file}")
    return token


def _check_auth(authorization: str | None, token: str) -> None:
    if authorization != f"Bearer {token}":
        raise HTTPException(status_code=401, detail="invalid bearer token")


def create_app(
    *,
    report_repository: ReportRepository | None = None,
    object_store: ReportObjectStore | None = None,
    token: str | None = None,
) -> FastAPI:
    repo = report_repository or PostgresReportRepository()
    store = object_store or MinioReportObjectStore()
    auth_token = token or load_token(os.getenv("AGENT_HUB_TOKEN_FILE", DEFAULT_TOKEN_FILE))
    bucket = os.getenv("AGENT_HUB_BUCKET", DEFAULT_BUCKET)
    app = FastAPI(title="GRID Agent Reporting Hub", version="1.0.0")

    @app.api_route("/health", methods=["GET", "HEAD"])
    def health() -> dict[str, Any]:
        db_ok = repo.check_health() if hasattr(repo, "check_health") else True
        s3_ok = store.check_health(bucket) if hasattr(store, "check_health") else True

        if not db_ok or not s3_ok:
            raise HTTPException(
                status_code=503,
                detail={
                    "status": "error",
                    "service": "agent-hub",
                    "details": {
                        "postgres": "ok" if db_ok else "error",
                        "minio": "ok" if s3_ok else "error",
                    }
                }
            )

        return {
            "status": "ok",
            "service": "agent-hub",
            "details": {
                "postgres": "ok",
                "minio": "ok"
            }
        }

    @app.post("/report", response_model=AgentReportOut)

    def ingest_report(
        payload: AgentReportIn,
        authorization: str | None = Header(default=None),
    ) -> AgentReportOut:
        _check_auth(authorization, auth_token)

        existing = repo.get_by_idempotency_key(payload.idempotency_key)
        if existing:
            return AgentReportOut(**existing, idempotent=True)

        md_key = object_key_for(payload, ".md")
        json_key = object_key_for(payload, ".json")
        uri = report_uri(bucket, md_key)
        sidecar = payload.model_dump(mode="json")
        sidecar["report_uri"] = uri
        sidecar["object_key"] = md_key
        sidecar["json_object_key"] = json_key

        if not store.put(
            bucket,
            md_key,
            payload.body_md.encode("utf-8"),
            "text/markdown; charset=utf-8",
        ):
            raise HTTPException(status_code=503, detail="object store markdown write failed")

        if not store.put(
            bucket,
            json_key,
            json.dumps(sidecar, sort_keys=True).encode("utf-8"),
            "application/json",
        ):
            raise HTTPException(status_code=503, detail="object store JSON sidecar write failed")

        row = repo.insert_report(
            {
                "date": payload.date,
                "agent": payload.agent,
                "host": payload.host,
                "title": payload.title,
                "body_md": payload.body_md,
                "body_json": payload.body_json,
                "tags": payload.tags,
                "report_uri": uri,
                "idempotency_key": payload.idempotency_key,
            }
        )
        return AgentReportOut(**row, idempotent=False)

    return app

