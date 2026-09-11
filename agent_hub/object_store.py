from __future__ import annotations

import io
import os
from typing import Protocol

from loguru import logger as log


class ReportObjectStore(Protocol):
    def put(self, bucket: str, key: str, data: bytes, content_type: str) -> bool:
        """Store bytes at bucket/key."""

    def check_health(self, bucket: str) -> bool:
        """Check object store connection health."""


class MinioReportObjectStore:
    """Small MinIO writer for agent report markdown + JSON sidecars."""

    def __init__(self) -> None:
        from config import settings
        from minio import Minio

        # Process env (systemd EnvironmentFile=/etc/agent-hub/minio.env) wins over
        # config.py; neither carries a built-in credential.
        endpoint = os.getenv("MINIO_ENDPOINT") or settings.MINIO_ENDPOINT
        access_key = os.getenv("MINIO_ACCESS_KEY") or settings.MINIO_ACCESS_KEY
        secret_key = os.getenv("MINIO_SECRET_KEY") or settings.MINIO_SECRET_KEY
        secure_raw = os.getenv("MINIO_SECURE", str(settings.MINIO_SECURE))
        secure = secure_raw.lower() in {"1", "true", "yes", "on"}
        region = os.getenv("MINIO_REGION") or settings.MINIO_REGION

        if not access_key or not secret_key:
            log.warning(
                "MINIO_ACCESS_KEY / MINIO_SECRET_KEY not set -- agent_hub object store "
                "writes will fail until /etc/agent-hub/minio.env provides them"
            )

        self._client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            region=region,
        )

    def put(self, bucket: str, key: str, data: bytes, content_type: str) -> bool:
        try:
            if not self._client.bucket_exists(bucket):
                self._client.make_bucket(bucket)
                log.info("Created MinIO bucket {bucket}", bucket=bucket)
            self._client.put_object(
                bucket,
                key,
                io.BytesIO(data),
                len(data),
                content_type=content_type,
            )
            return True
        except Exception as exc:
            log.error(
                "agent_hub MinIO write failed for {bucket}/{key}: {err}",
                bucket=bucket,
                key=key,
                err=str(exc),
            )
            return False

    def check_health(self, bucket: str) -> bool:
        try:
            self._client.bucket_exists(bucket)
            return True
        except Exception as exc:
            log.error(
                "agent_hub MinIO health check failed: {err}",
                err=str(exc),
            )
            return False

