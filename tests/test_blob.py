"""Tests for the MinIO blob store and blob API router."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from store.blob import BlobStore, BUCKETS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_minio_client():
    """Create a mock MinIO client."""
    client = MagicMock()
    client.bucket_exists.return_value = True
    client.list_buckets.return_value = [
        SimpleNamespace(name="filings"),
        SimpleNamespace(name="evidence"),
    ]
    client._base_url = "http://localhost:9000"
    return client


@pytest.fixture
def blob_store_with_client(mock_minio_client):
    """Create a BlobStore with a pre-injected mock client."""
    store = BlobStore()
    store._client = mock_minio_client
    store._available = True
    return store


@pytest.fixture
def unavailable_blob_store():
    """Create a BlobStore that simulates MinIO being unavailable."""
    store = BlobStore()
    store._client = None
    store._available = False
    store._get_client = MagicMock(return_value=None)
    return store


# ---------------------------------------------------------------------------
# BlobStore unit tests
# ---------------------------------------------------------------------------


class TestBlobStoreAvailability:
    """Tests for BlobStore availability checks."""

    def test_available_when_client_connected(self, blob_store_with_client):
        assert blob_store_with_client.available is True

    def test_unavailable_when_client_none(self, unavailable_blob_store):
        assert unavailable_blob_store.available is False

    def test_lazy_init_on_first_available_check(self):
        """available property triggers lazy init when _available is None."""
        store = BlobStore()
        assert store._available is None
        # Calling available will attempt to init (and fail without real MinIO)
        with patch("store.blob.BlobStore._get_client", return_value=None):
            store._available = False
            result = store.available
        assert result is False


class TestBlobStorePut:
    """Tests for BlobStore.put()."""

    def test_put_success(self, blob_store_with_client, mock_minio_client):
        data = b"hello world"
        result = blob_store_with_client.put("filings", "test/file.txt", data)

        assert result is True
        mock_minio_client.put_object.assert_called_once()
        call_args = mock_minio_client.put_object.call_args
        assert call_args[0][0] == "filings"
        assert call_args[0][1] == "test/file.txt"
        assert call_args[0][3] == len(data)

    def test_put_with_content_type(self, blob_store_with_client, mock_minio_client):
        data = b"<html>test</html>"
        blob_store_with_client.put(
            "evidence", "page.html", data, content_type="text/html"
        )

        call_kwargs = mock_minio_client.put_object.call_args
        assert call_kwargs[1]["content_type"] == "text/html"

    def test_put_creates_bucket_if_missing(
        self, blob_store_with_client, mock_minio_client
    ):
        mock_minio_client.bucket_exists.return_value = False
        blob_store_with_client.put("filings", "test.txt", b"data")

        mock_minio_client.make_bucket.assert_called_once_with("filings")

    def test_put_returns_false_when_unavailable(self, unavailable_blob_store):
        result = unavailable_blob_store.put("filings", "test.txt", b"data")
        assert result is False

    def test_put_returns_false_on_exception(
        self, blob_store_with_client, mock_minio_client
    ):
        mock_minio_client.put_object.side_effect = Exception("Connection refused")
        result = blob_store_with_client.put("filings", "test.txt", b"data")
        assert result is False


class TestBlobStoreGet:
    """Tests for BlobStore.get()."""

    def test_get_success(self, blob_store_with_client, mock_minio_client):
        mock_response = MagicMock()
        mock_response.read.return_value = b"file contents"
        mock_minio_client.get_object.return_value = mock_response

        result = blob_store_with_client.get("filings", "test.txt")

        assert result == b"file contents"
        mock_response.close.assert_called_once()
        mock_response.release_conn.assert_called_once()

    def test_get_returns_none_when_unavailable(self, unavailable_blob_store):
        result = unavailable_blob_store.get("filings", "test.txt")
        assert result is None

    def test_get_returns_none_on_exception(
        self, blob_store_with_client, mock_minio_client
    ):
        mock_minio_client.get_object.side_effect = Exception("NoSuchKey")
        result = blob_store_with_client.get("filings", "missing.txt")
        assert result is None


class TestBlobStoreGetUrl:
    """Tests for BlobStore.get_url()."""

    def test_get_url_success(self, blob_store_with_client, mock_minio_client):
        mock_minio_client.presigned_get_object.return_value = (
            "http://localhost:9000/filings/test.txt?X-Amz-Signature=abc"
        )

        url = blob_store_with_client.get_url("filings", "test.txt", expires=7200)

        assert url is not None
        assert "localhost:9000" in url
        mock_minio_client.presigned_get_object.assert_called_once()

    def test_get_url_returns_none_when_unavailable(self, unavailable_blob_store):
        result = unavailable_blob_store.get_url("filings", "test.txt")
        assert result is None

    def test_get_url_returns_none_on_exception(
        self, blob_store_with_client, mock_minio_client
    ):
        mock_minio_client.presigned_get_object.side_effect = Exception("error")
        result = blob_store_with_client.get_url("filings", "test.txt")
        assert result is None


class TestBlobStoreDelete:
    """Tests for BlobStore.delete()."""

    def test_delete_success(self, blob_store_with_client, mock_minio_client):
        result = blob_store_with_client.delete("filings", "test.txt")

        assert result is True
        mock_minio_client.remove_object.assert_called_once_with("filings", "test.txt")

    def test_delete_returns_false_when_unavailable(self, unavailable_blob_store):
        result = unavailable_blob_store.delete("filings", "test.txt")
        assert result is False

    def test_delete_returns_false_on_exception(
        self, blob_store_with_client, mock_minio_client
    ):
        mock_minio_client.remove_object.side_effect = Exception("error")
        result = blob_store_with_client.delete("filings", "test.txt")
        assert result is False


class TestBlobStoreList:
    """Tests for BlobStore.list()."""

    def test_list_success(self, blob_store_with_client, mock_minio_client):
        now = datetime.now(timezone.utc)
        mock_objects = [
            SimpleNamespace(
                object_name="AAPL/10-K/2026.pdf",
                size=1024,
                last_modified=now,
                etag="abc123",
            ),
            SimpleNamespace(
                object_name="AAPL/10-Q/2026.pdf",
                size=2048,
                last_modified=now,
                etag="def456",
            ),
        ]
        mock_minio_client.list_objects.return_value = mock_objects

        result = blob_store_with_client.list("filings", prefix="AAPL/")

        assert len(result) == 2
        assert result[0]["key"] == "AAPL/10-K/2026.pdf"
        assert result[0]["size"] == 1024
        assert result[1]["key"] == "AAPL/10-Q/2026.pdf"

    def test_list_returns_empty_when_unavailable(self, unavailable_blob_store):
        result = unavailable_blob_store.list("filings")
        assert result == []

    def test_list_returns_empty_on_exception(
        self, blob_store_with_client, mock_minio_client
    ):
        mock_minio_client.list_objects.side_effect = Exception("error")
        result = blob_store_with_client.list("filings")
        assert result == []

    def test_list_handles_none_last_modified(
        self, blob_store_with_client, mock_minio_client
    ):
        mock_objects = [
            SimpleNamespace(
                object_name="file.txt",
                size=100,
                last_modified=None,
                etag="aaa",
            ),
        ]
        mock_minio_client.list_objects.return_value = mock_objects

        result = blob_store_with_client.list("filings")
        assert result[0]["modified"] is None


class TestBlobStoreExists:
    """Tests for BlobStore.exists()."""

    def test_exists_true(self, blob_store_with_client, mock_minio_client):
        mock_minio_client.stat_object.return_value = SimpleNamespace(size=100)
        assert blob_store_with_client.exists("filings", "test.txt") is True

    def test_exists_false_not_found(self, blob_store_with_client, mock_minio_client):
        mock_minio_client.stat_object.side_effect = Exception("NoSuchKey")
        assert blob_store_with_client.exists("filings", "missing.txt") is False

    def test_exists_false_when_unavailable(self, unavailable_blob_store):
        assert unavailable_blob_store.exists("filings", "test.txt") is False


class TestBlobStoreHealthCheck:
    """Tests for BlobStore.health_check()."""

    def test_health_check_success(self, blob_store_with_client, mock_minio_client):
        result = blob_store_with_client.health_check()

        assert result["available"] is True
        assert "filings" in result["buckets"]
        assert "evidence" in result["buckets"]

    def test_health_check_unavailable(self, unavailable_blob_store):
        result = unavailable_blob_store.health_check()

        assert result["available"] is False
        assert "error" in result

    def test_health_check_exception(self, blob_store_with_client, mock_minio_client):
        mock_minio_client.list_buckets.side_effect = Exception("Connection refused")
        result = blob_store_with_client.health_check()

        assert result["available"] is False
        assert "Connection refused" in result["error"]


# ---------------------------------------------------------------------------
# BlobStore constants
# ---------------------------------------------------------------------------


class TestBlobStoreConstants:
    """Tests for module-level constants."""

    def test_buckets_tuple(self):
        assert isinstance(BUCKETS, tuple)
        assert "filings" in BUCKETS
        assert "evidence" in BUCKETS
        assert "screenshots" in BUCKETS
        assert "models" in BUCKETS
        assert "exports" in BUCKETS

    def test_buckets_count(self):
        assert len(BUCKETS) == 5


# ---------------------------------------------------------------------------
# API endpoint tests (using FastAPI TestClient)
# ---------------------------------------------------------------------------


def _can_import_blob_router() -> bool:
    """Check if the blob router can be imported (needs python-multipart)."""
    try:
        from api.routers.blob import router  # noqa: F401
        return True
    except (ImportError, RuntimeError):
        return False


_skip_api = pytest.mark.skipif(
    not _can_import_blob_router(),
    reason="python-multipart or fastapi test deps not available",
)


@_skip_api
class TestBlobAPI:
    """Tests for the blob API router endpoints."""

    @pytest.fixture(autouse=True)
    def setup_client(self):
        """Set up FastAPI TestClient with mocked auth and blob store."""
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from api.routers.blob import router

        app = FastAPI()
        app.include_router(router)

        # Patch require_auth to always pass
        async def mock_auth():
            return "test-user"

        from api.routers import blob as blob_module

        # Override the dependency
        from api.auth import require_auth

        app.dependency_overrides[require_auth] = mock_auth

        self.client = TestClient(app)
        self.mock_blob_store = MagicMock()
        self._original_blob_store = blob_module.blob_store
        blob_module.blob_store = self.mock_blob_store
        yield
        blob_module.blob_store = self._original_blob_store

    def test_health_endpoint(self):
        self.mock_blob_store.health_check.return_value = {
            "available": True,
            "endpoint": "http://localhost:9000",
            "buckets": ["filings", "evidence"],
        }

        resp = self.client.get("/api/v1/blob/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["available"] is True

    def test_upload_blob(self):
        self.mock_blob_store.put.return_value = True

        resp = self.client.post(
            "/api/v1/blob/filings/AAPL/10-K/2026.pdf",
            files={"file": ("test.pdf", b"pdf-content", "application/pdf")},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["bucket"] == "filings"
        assert data["key"] == "AAPL/10-K/2026.pdf"
        assert data["size"] == len(b"pdf-content")
        assert data["status"] == "uploaded"

    def test_upload_invalid_bucket(self):
        resp = self.client.post(
            "/api/v1/blob/invalid_bucket/test.txt",
            files={"file": ("test.txt", b"data", "text/plain")},
        )
        assert resp.status_code == 400

    def test_upload_blob_store_unavailable(self):
        self.mock_blob_store.put.return_value = False

        resp = self.client.post(
            "/api/v1/blob/filings/test.txt",
            files={"file": ("test.txt", b"data", "text/plain")},
        )
        assert resp.status_code == 503

    def test_download_blob(self):
        self.mock_blob_store.get.return_value = b"file-contents"

        resp = self.client.get("/api/v1/blob/filings/AAPL/10-K/2026.pdf")
        assert resp.status_code == 200
        assert resp.content == b"file-contents"

    def test_download_not_found(self):
        self.mock_blob_store.get.return_value = None

        resp = self.client.get("/api/v1/blob/filings/missing.txt")
        assert resp.status_code == 404

    def test_download_invalid_bucket(self):
        resp = self.client.get("/api/v1/blob/bad_bucket/test.txt")
        assert resp.status_code == 400

    def test_list_blobs(self):
        self.mock_blob_store.list.return_value = [
            {"key": "AAPL/10-K.pdf", "size": 1024, "modified": None, "etag": "abc"},
        ]

        resp = self.client.get("/api/v1/blob/filings/?prefix=AAPL/")
        assert resp.status_code == 200
        data = resp.json()
        assert data["bucket"] == "filings"
        assert data["count"] == 1
        assert data["objects"][0]["key"] == "AAPL/10-K.pdf"

    def test_list_invalid_bucket(self):
        resp = self.client.get("/api/v1/blob/invalid/")
        assert resp.status_code == 400

    def test_delete_blob(self):
        self.mock_blob_store.delete.return_value = True

        resp = self.client.delete("/api/v1/blob/filings/test.txt")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "deleted"

    def test_delete_blob_unavailable(self):
        self.mock_blob_store.delete.return_value = False

        resp = self.client.delete("/api/v1/blob/filings/test.txt")
        assert resp.status_code == 503

    def test_delete_invalid_bucket(self):
        resp = self.client.delete("/api/v1/blob/bad_bucket/test.txt")
        assert resp.status_code == 400

    def test_presigned_url(self):
        self.mock_blob_store.get_url.return_value = (
            "http://localhost:9000/filings/test.txt?sig=abc"
        )

        resp = self.client.get("/api/v1/blob/filings/test.txt/url?expires=7200")
        assert resp.status_code == 200
        data = resp.json()
        assert "url" in data
        assert data["expires_in"] == 7200

    def test_presigned_url_unavailable(self):
        self.mock_blob_store.get_url.return_value = None

        resp = self.client.get("/api/v1/blob/filings/test.txt/url")
        assert resp.status_code == 503

    def test_presigned_url_invalid_bucket(self):
        resp = self.client.get("/api/v1/blob/bad_bucket/test.txt/url")
        assert resp.status_code == 400

    def test_presigned_url_expires_validation(self):
        """Expires must be between 60 and 86400."""
        resp = self.client.get("/api/v1/blob/filings/test.txt/url?expires=10")
        assert resp.status_code == 422  # FastAPI validation error
