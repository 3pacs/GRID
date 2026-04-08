"""Blob store API — upload, download, and manage stored files."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query
from fastapi.responses import Response
from loguru import logger as log

from api.auth import require_auth
from store.blob import blob_store, BUCKETS

router = APIRouter(prefix="/api/v1/blob", tags=["blob"])


@router.get("/health")
async def blob_health(_=Depends(require_auth)):
    """Check blob store health."""
    return blob_store.health_check()


@router.post("/{bucket}/{key:path}")
async def upload_blob(
    bucket: str,
    key: str,
    file: UploadFile = File(...),
    _=Depends(require_auth),
):
    """Upload a file to the blob store."""
    if bucket not in BUCKETS:
        raise HTTPException(400, f"Invalid bucket. Allowed: {BUCKETS}")

    data = await file.read()
    if len(data) > 100 * 1024 * 1024:  # 100MB limit
        raise HTTPException(413, "File too large (max 100MB)")

    success = blob_store.put(
        bucket, key, data, content_type=file.content_type or "application/octet-stream"
    )
    if not success:
        raise HTTPException(503, "Blob store unavailable")

    return {"bucket": bucket, "key": key, "size": len(data), "status": "uploaded"}


@router.get("/{bucket}/{key:path}/url")
async def get_presigned_url(
    bucket: str,
    key: str,
    expires: int = Query(3600, ge=60, le=86400),
    _=Depends(require_auth),
):
    """Get a pre-signed URL for direct download."""
    if bucket not in BUCKETS:
        raise HTTPException(400, f"Invalid bucket. Allowed: {BUCKETS}")

    url = blob_store.get_url(bucket, key, expires=expires)
    if not url:
        raise HTTPException(503, "Blob store unavailable")

    return {"url": url, "expires_in": expires}


@router.get("/{bucket}/")
async def list_blobs(
    bucket: str,
    prefix: str = Query("", description="Key prefix filter"),
    _=Depends(require_auth),
):
    """List objects in a bucket."""
    if bucket not in BUCKETS:
        raise HTTPException(400, f"Invalid bucket. Allowed: {BUCKETS}")

    objects = blob_store.list(bucket, prefix=prefix)
    return {"bucket": bucket, "prefix": prefix, "objects": objects, "count": len(objects)}


@router.get("/{bucket}/{key:path}")
async def download_blob(
    bucket: str,
    key: str,
    _=Depends(require_auth),
):
    """Download a file from the blob store."""
    if bucket not in BUCKETS:
        raise HTTPException(400, f"Invalid bucket. Allowed: {BUCKETS}")

    data = blob_store.get(bucket, key)
    if data is None:
        raise HTTPException(404, f"Object not found: {bucket}/{key}")

    return Response(content=data, media_type="application/octet-stream")


@router.delete("/{bucket}/{key:path}")
async def delete_blob(
    bucket: str,
    key: str,
    _=Depends(require_auth),
):
    """Delete an object from the blob store."""
    if bucket not in BUCKETS:
        raise HTTPException(400, f"Invalid bucket. Allowed: {BUCKETS}")

    success = blob_store.delete(bucket, key)
    if not success:
        raise HTTPException(503, "Blob store unavailable or object not found")

    return {"bucket": bucket, "key": key, "status": "deleted"}
