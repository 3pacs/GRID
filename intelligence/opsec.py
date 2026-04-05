"""
GRID Intelligence Operations Security (OPSEC) Module.

Handles encryption, access control, and audit logging for sensitive
intelligence data — particularly offshore network cross-references,
rumored connections, and shadow economy estimates.

Architecture:
    - Column-level encryption via pgcrypto (AES-256)
    - 4-tier access control: public, contributor, analyst, operator
    - Audit logging for every sensitive query
    - Encrypted intelligence store for cross-references

The encryption key is derived from GRID_INTEL_KEY env var.
If not set, falls back to a derived key from GRID_JWT_SECRET.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Encryption Key Management ────────────────────────────────────────────

def _get_intel_key() -> str:
    """Get the intelligence encryption key.

    Priority:
        1. GRID_INTEL_KEY env var (recommended)
        2. Derived from GRID_JWT_SECRET (fallback)
        3. Hardcoded dev key (development only, logs warning)
    """
    key = os.getenv("GRID_INTEL_KEY")
    if key:
        return key

    jwt_secret = os.getenv("GRID_JWT_SECRET")
    if jwt_secret:
        return hashlib.sha256(f"grid-intel-{jwt_secret}".encode()).hexdigest()

    log.warning("OPSEC: No GRID_INTEL_KEY or GRID_JWT_SECRET set — using dev key")
    return "dev-intel-key-DO-NOT-USE-IN-PRODUCTION"


# ── Access Tier Definitions ──────────────────────────────────────────────

TIER_HIERARCHY = {
    "public": 0,
    "contributor": 1,
    "analyst": 2,
    "operator": 3,
}

CONFIDENCE_TIERS = {
    "confirmed": "public",      # anyone can see confirmed facts
    "derived": "contributor",    # verified users see derived data
    "estimated": "contributor",  # verified users see estimates
    "rumored": "analyst",        # analysts only for rumors
    "inferred": "analyst",       # analysts only for inferences
}


def user_can_view(user_tier: str, data_confidence: str) -> bool:
    """Check if a user's access tier allows viewing data at this confidence."""
    required = CONFIDENCE_TIERS.get(data_confidence, "operator")
    return TIER_HIERARCHY.get(user_tier, 0) >= TIER_HIERARCHY.get(required, 3)


def get_user_tier(role: str) -> str:
    """Map JWT role to intelligence tier."""
    if role == "admin":
        return "operator"
    if role == "contributor":
        return "contributor"
    return "public"


# ── Audit Logging ────────────────────────────────────────────────────────

class AuditLogger:
    """Logs every access to sensitive intelligence endpoints."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def log_access(
        self,
        username: str,
        role: str,
        action: str,
        endpoint: str = "",
        target: str = "",
        ip_address: str = "",
        details: dict | None = None,
        risk_level: str = "normal",
    ) -> None:
        """Record an access event to the audit log."""
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO security_audit_log "
                        "(username, role, action, endpoint, target, ip_address, "
                        "details, risk_level) "
                        "VALUES (:u, :r, :a, :e, :t, :ip, :d, :rl)"
                    ),
                    {
                        "u": username,
                        "r": role,
                        "a": action,
                        "e": endpoint,
                        "t": target,
                        "ip": ip_address,
                        "d": json.dumps(details or {}),
                        "rl": risk_level,
                    },
                )
        except Exception as exc:
            log.debug("Audit log write failed: {e}", e=str(exc))

    def log_sensitive_query(
        self,
        username: str,
        role: str,
        query_type: str,
        target: str,
        ip: str = "",
    ) -> None:
        """Shortcut for logging sensitive data queries."""
        self.log_access(
            username=username,
            role=role,
            action=f"sensitive_query:{query_type}",
            endpoint=f"/api/v1/intelligence/{query_type}",
            target=target,
            ip_address=ip,
            risk_level="elevated",
        )

    def get_recent(self, limit: int = 50, risk_level: str | None = None) -> list[dict]:
        """Get recent audit log entries."""
        try:
            with self.engine.connect() as conn:
                if risk_level:
                    rows = conn.execute(
                        text(
                            "SELECT ts, username, role, action, endpoint, target, "
                            "ip_address, risk_level "
                            "FROM security_audit_log "
                            "WHERE risk_level = :rl "
                            "ORDER BY ts DESC LIMIT :lim"
                        ),
                        {"rl": risk_level, "lim": limit},
                    ).fetchall()
                else:
                    rows = conn.execute(
                        text(
                            "SELECT ts, username, role, action, endpoint, target, "
                            "ip_address, risk_level "
                            "FROM security_audit_log "
                            "ORDER BY ts DESC LIMIT :lim"
                        ),
                        {"lim": limit},
                    ).fetchall()
                return [
                    {
                        "ts": r[0].isoformat() if r[0] else None,
                        "username": r[1],
                        "role": r[2],
                        "action": r[3],
                        "endpoint": r[4],
                        "target": r[5],
                        "ip": r[6],
                        "risk_level": r[7],
                    }
                    for r in rows
                ]
        except Exception:
            return []


# ── Encrypted Intelligence Store ─────────────────────────────────────────

class EncryptedIntelStore:
    """Store and retrieve encrypted intelligence data.

    Uses PostgreSQL pgcrypto for AES-256 symmetric encryption.
    Data is encrypted at rest — only decryptable with the intel key.
    """

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._key = _get_intel_key()

    def store(
        self,
        category: str,
        subject: str,
        data: dict,
        confidence: str = "rumored",
        min_tier: str = "analyst",
        tags: list[str] | None = None,
        jurisdiction: str | None = None,
        sector: str | None = None,
        source_leak: str | None = None,
        actor_count: int = 0,
        entity_count: int = 0,
    ) -> int | None:
        """Encrypt and store an intelligence record.

        Returns the record ID, or None on failure.
        """
        payload = json.dumps(data, default=str)

        try:
            with self.engine.begin() as conn:
                row = conn.execute(
                    text(
                        "INSERT INTO encrypted_intelligence "
                        "(category, subject, confidence, min_tier, "
                        "encrypted_data, tags, jurisdiction, sector, "
                        "source_leak, actor_count, entity_count) "
                        "VALUES (:cat, :sub, :conf, :tier, "
                        "pgp_sym_encrypt(:payload, :key), "
                        ":tags, :jur, :sec, :src, :ac, :ec) "
                        "RETURNING id"
                    ),
                    {
                        "cat": category,
                        "sub": subject,
                        "conf": confidence,
                        "tier": min_tier,
                        "payload": payload,
                        "key": self._key,
                        "tags": tags or [],
                        "jur": jurisdiction,
                        "sec": sector,
                        "src": source_leak,
                        "ac": actor_count,
                        "ec": entity_count,
                    },
                ).fetchone()
                return row[0] if row else None
        except Exception as exc:
            log.warning("Failed to store encrypted intel: {e}", e=str(exc))
            return None

    def retrieve(
        self,
        record_id: int | None = None,
        category: str | None = None,
        user_tier: str = "public",
        limit: int = 50,
    ) -> list[dict]:
        """Decrypt and retrieve intelligence records.

        Only returns records the user's tier is authorized to see.
        """
        tier_level = TIER_HIERARCHY.get(user_tier, 0)

        try:
            with self.engine.connect() as conn:
                if record_id:
                    rows = conn.execute(
                        text(
                            "SELECT id, category, subject, confidence, min_tier, "
                            "pgp_sym_decrypt(encrypted_data, :key)::text as data, "
                            "tags, jurisdiction, sector, source_leak, "
                            "actor_count, entity_count, created_at "
                            "FROM encrypted_intelligence "
                            "WHERE id = :id"
                        ),
                        {"id": record_id, "key": self._key},
                    ).fetchall()
                elif category:
                    rows = conn.execute(
                        text(
                            "SELECT id, category, subject, confidence, min_tier, "
                            "pgp_sym_decrypt(encrypted_data, :key)::text as data, "
                            "tags, jurisdiction, sector, source_leak, "
                            "actor_count, entity_count, created_at "
                            "FROM encrypted_intelligence "
                            "WHERE category = :cat "
                            "ORDER BY created_at DESC LIMIT :lim"
                        ),
                        {"cat": category, "key": self._key, "lim": limit},
                    ).fetchall()
                else:
                    rows = conn.execute(
                        text(
                            "SELECT id, category, subject, confidence, min_tier, "
                            "pgp_sym_decrypt(encrypted_data, :key)::text as data, "
                            "tags, jurisdiction, sector, source_leak, "
                            "actor_count, entity_count, created_at "
                            "FROM encrypted_intelligence "
                            "ORDER BY created_at DESC LIMIT :lim"
                        ),
                        {"key": self._key, "lim": limit},
                    ).fetchall()

                results = []
                for r in rows:
                    # Check tier authorization
                    required_tier = TIER_HIERARCHY.get(r[4], 3)
                    if tier_level < required_tier:
                        # User can see the metadata but not the payload
                        results.append({
                            "id": r[0],
                            "category": r[1],
                            "subject": r[2],
                            "confidence": r[3],
                            "min_tier": r[4],
                            "data": {"redacted": True, "reason": f"requires {r[4]} tier"},
                            "tags": r[6],
                            "jurisdiction": r[7],
                            "sector": r[8],
                            "actor_count": r[10],
                            "entity_count": r[11],
                        })
                    else:
                        try:
                            payload = json.loads(r[5]) if r[5] else {}
                        except (json.JSONDecodeError, TypeError):
                            payload = {"raw": r[5]}
                        results.append({
                            "id": r[0],
                            "category": r[1],
                            "subject": r[2],
                            "confidence": r[3],
                            "min_tier": r[4],
                            "data": payload,
                            "tags": r[6],
                            "jurisdiction": r[7],
                            "sector": r[8],
                            "source_leak": r[9],
                            "actor_count": r[10],
                            "entity_count": r[11],
                            "created_at": r[12].isoformat() if r[12] else None,
                        })
                return results
        except Exception as exc:
            log.warning("Failed to retrieve encrypted intel: {e}", e=str(exc))
            return []

    def search(
        self,
        tags: list[str] | None = None,
        jurisdiction: str | None = None,
        sector: str | None = None,
        confidence: str | None = None,
        user_tier: str = "public",
        limit: int = 50,
    ) -> list[dict]:
        """Search encrypted intelligence by metadata (no decryption needed for search)."""
        conditions = []
        params: dict[str, Any] = {"lim": limit}

        if tags:
            conditions.append("tags && :tags")
            params["tags"] = tags
        if jurisdiction:
            conditions.append("jurisdiction = :jur")
            params["jur"] = jurisdiction
        if sector:
            conditions.append("sector = :sec")
            params["sec"] = sector
        if confidence:
            conditions.append("confidence = :conf")
            params["conf"] = confidence

        where = " AND ".join(conditions) if conditions else "TRUE"

        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        f"SELECT id, category, subject, confidence, min_tier, "
                        f"tags, jurisdiction, sector, source_leak, "
                        f"actor_count, entity_count, created_at "
                        f"FROM encrypted_intelligence "
                        f"WHERE {where} "
                        f"ORDER BY created_at DESC LIMIT :lim"
                    ),
                    params,
                ).fetchall()

                tier_level = TIER_HIERARCHY.get(user_tier, 0)
                return [
                    {
                        "id": r[0],
                        "category": r[1],
                        "subject": r[2],
                        "confidence": r[3],
                        "accessible": tier_level >= TIER_HIERARCHY.get(r[4], 3),
                        "tags": r[5],
                        "jurisdiction": r[6],
                        "sector": r[7],
                        "source_leak": r[8],
                        "actor_count": r[9],
                        "entity_count": r[10],
                    }
                    for r in rows
                ]
        except Exception as exc:
            log.warning("Encrypted intel search failed: {e}", e=str(exc))
            return []


# ── Middleware: Audit Decorator ───────────────────────────────────────────

def audit_sensitive(action: str, risk_level: str = "elevated"):
    """Decorator for FastAPI endpoints that access sensitive intelligence.

    Usage:
        @router.get("/intelligence/offshore/{entity}")
        @audit_sensitive("offshore_lookup", risk_level="critical")
        async def get_offshore_entity(entity: str, ...):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Extract user info from the request context
            request = kwargs.get("request")
            token_data = kwargs.get("_token_data", {})
            username = token_data.get("sub", "unknown") if isinstance(token_data, dict) else "unknown"
            role = token_data.get("role", "public") if isinstance(token_data, dict) else "public"
            ip = request.client.host if request and request.client else "unknown"

            # Log the access
            try:
                from api.dependencies import get_db_engine
                engine = get_db_engine()
                audit = AuditLogger(engine)
                audit.log_access(
                    username=username,
                    role=role,
                    action=action,
                    endpoint=str(request.url) if request else "",
                    target=str(kwargs.get("ticker", kwargs.get("entity", ""))),
                    ip_address=ip,
                    risk_level=risk_level,
                )
            except Exception as exc:
                log.warning("Opsec audit log write failed: {e}", e=exc)

            return await func(*args, **kwargs)
        return wrapper
    return decorator
