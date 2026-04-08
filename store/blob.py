"""
S3-compatible blob store backed by MinIO.

Provides put/get/delete/url operations for storing raw filings, evidence,
screenshots, and model artifacts.

Usage:
    from store.blob import blob_store

    # Upload
    blob_store.put("filings", "AAPL/10-K/2026.pdf", pdf_bytes)

    # Download
    data = blob_store.get("filings", "AAPL/10-K/2026.pdf")

    # Pre-signed URL (for frontend)
    url = blob_store.get_url("filings", "AAPL/10-K/2026.pdf", expires=3600)

    # Delete
    blob_store.delete("filings", "AAPL/10-K/2026.pdf")

    # List
    objects = blob_store.list("filings", prefix="AAPL/")
"""

from __future__ import annotations

import io
from datetime import timedelta

from loguru import logger as log


# Buckets
BUCKETS = ("filings", "evidence", "screenshots", "models", "exports")


class BlobStore:
    """MinIO/S3-compatible blob store wrapper."""

    def __init__(self):
        self._client = None
        self._available = None

    def _get_client(self):
        """Lazy-init MinIO client."""
        if self._client is not None:
            return self._client

        try:
            from minio import Minio
            from config import settings

            self._client = Minio(
                settings.MINIO_ENDPOINT,
                access_key=settings.MINIO_ACCESS_KEY,
                secret_key=settings.MINIO_SECRET_KEY,
                secure=settings.MINIO_SECURE,
                region=settings.MINIO_REGION,
            )
            self._available = True
            log.info(f"MinIO connected: {settings.MINIO_ENDPOINT}")
        except Exception as e:
            log.warning(f"MinIO unavailable: {e}")
            self._client = None
            self._available = False

        return self._client

    @property
    def available(self) -> bool:
        """Check if MinIO is available."""
        if self._available is None:
            self._get_client()
        return self._available or False

    def _ensure_bucket(self, bucket: str) -> None:
        """Create bucket if it doesn't exist."""
        client = self._get_client()
        if client and not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            log.info(f"Created bucket: {bucket}")

    def put(
        self,
        bucket: str,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> bool:
        """Upload bytes to bucket/key."""
        client = self._get_client()
        if not client:
            log.warning(f"MinIO unavailable, cannot put {bucket}/{key}")
            return False

        try:
            self._ensure_bucket(bucket)
            client.put_object(
                bucket,
                key,
                io.BytesIO(data),
                len(data),
                content_type=content_type,
            )
            log.debug(f"Stored {bucket}/{key} ({len(data)} bytes)")
            return True
        except Exception as e:
            log.error(f"MinIO put failed {bucket}/{key}: {e}")
            return False

    def get(self, bucket: str, key: str) -> bytes | None:
        """Download bytes from bucket/key."""
        client = self._get_client()
        if not client:
            return None

        try:
            response = client.get_object(bucket, key)
            data = response.read()
            response.close()
            response.release_conn()
            return data
        except Exception as e:
            log.warning(f"MinIO get failed {bucket}/{key}: {e}")
            return None

    def get_url(self, bucket: str, key: str, expires: int = 3600) -> str | None:
        """Generate a pre-signed URL for downloading."""
        client = self._get_client()
        if not client:
            return None

        try:
            return client.presigned_get_object(
                bucket,
                key,
                expires=timedelta(seconds=expires),
            )
        except Exception as e:
            log.warning(f"MinIO presigned URL failed {bucket}/{key}: {e}")
            return None

    def delete(self, bucket: str, key: str) -> bool:
        """Delete an object."""
        client = self._get_client()
        if not client:
            return False

        try:
            client.remove_object(bucket, key)
            log.debug(f"Deleted {bucket}/{key}")
            return True
        except Exception as e:
            log.warning(f"MinIO delete failed {bucket}/{key}: {e}")
            return False

    def list(
        self, bucket: str, prefix: str = "", recursive: bool = True
    ) -> list[dict]:
        """List objects in bucket with optional prefix filter."""
        client = self._get_client()
        if not client:
            return []

        try:
            objects = client.list_objects(bucket, prefix=prefix, recursive=recursive)
            return [
                {
                    "key": obj.object_name,
                    "size": obj.size,
                    "modified": (
                        obj.last_modified.isoformat() if obj.last_modified else None
                    ),
                    "etag": obj.etag,
                }
                for obj in objects
            ]
        except Exception as e:
            log.warning(f"MinIO list failed {bucket}/{prefix}: {e}")
            return []

    def exists(self, bucket: str, key: str) -> bool:
        """Check if an object exists."""
        client = self._get_client()
        if not client:
            return False

        try:
            client.stat_object(bucket, key)
            return True
        except Exception:
            return False

    def health_check(self) -> dict:
        """Health check — list buckets and their object counts."""
        client = self._get_client()
        if not client:
            return {"available": False, "error": "MinIO client not initialized"}

        try:
            buckets = [b.name for b in client.list_buckets()]
            return {
                "available": True,
                "endpoint": str(client._base_url),
                "buckets": buckets,
            }
        except Exception as e:
            return {"available": False, "error": str(e)}


# Singleton instance
blob_store = BlobStore()
