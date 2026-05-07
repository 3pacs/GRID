from __future__ import annotations

import io
import os
from typing import Protocol

from loguru import logger as log


class ReportObjectStore(Protocol):
    def put(self, bucket: str, key: str, data: bytes, content_type: str) -> bool:
        """Store bytes at bucket/key."""


class MinioReportObjectStore:
    """Small MinIO writer for agent report markdown + JSON sidecars."""

    def __init__(self) -> None:
        from config import settings
        from minio import Minio

        endpoint = os.getenv("MINIO_ENDPOINT", getattr(settings, "MINIO_ENDPOINT", "localhost:9000"))
        access_key = os.getenv("MINIO_ACCESS_KEY", getattr(settings, "MINIO_ACCESS_KEY", "gridminio"))
        secret_key = os.getenv("MINIO_SECRET_KEY", getattr(settings, "MINIO_SECRET_KEY", "gridminio2026"))
        secure_raw = os.getenv("MINIO_SECURE", str(getattr(settings, "MINIO_SECURE", False)))
        secure = secure_raw.lower() in {"1", "true", "yes", "on"}
        region = os.getenv("MINIO_REGION", getattr(settings, "MINIO_REGION", "us-east-1"))

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
