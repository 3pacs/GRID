"""
GRID JWT authentication with role-based access control.

Supports:
  - Master password login (backward compatible) → admin role
  - User accounts with roles (admin, contributor)
  - require_auth: any valid token
  - require_role("admin"): admin-only endpoints
"""

from __future__ import annotations

import os
import shelve
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

import psycopg2
import psycopg2.extras
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from loguru import logger as log
from passlib.context import CryptContext

from api.schemas.auth import (
    CreateUserRequest,
    LoginRequest,
    LoginResponse,
    TokenVerifyResponse,
    UserResponse,
)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer(auto_error=False)

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])

# Rate limiting — persisted to disk so state survives restarts
_RATE_LIMIT_WINDOW = 60  # seconds
_RATE_LIMIT_MAX = 5
_rate_limit_path = str(
    Path(os.getenv("GRID_DATA_DIR", tempfile.gettempdir())) / "grid_rate_limits"
)

VALID_ROLES = ("admin", "contributor")


# ── Settings ──────────────────────────────────────────────────

def _get_settings() -> tuple[str, str, int]:
    """Return (password_hash, jwt_secret, expire_hours) from env."""
    pw_hash = os.getenv("GRID_MASTER_PASSWORD_HASH", "")
    jwt_secret = os.getenv("GRID_JWT_SECRET", "")
    if not jwt_secret:
        environment = os.getenv("ENVIRONMENT", "development")
        if environment != "development":
            raise RuntimeError(
                "GRID_JWT_SECRET must be set in non-development environments. "
                'Generate one with: python -c "import secrets; print(secrets.token_urlsafe(64))"'
            )
        jwt_secret = "dev-secret-change-me"
    expire_hours = int(os.getenv("GRID_JWT_EXPIRE_HOURS", "168"))
    return pw_hash, jwt_secret, expire_hours


def _get_db_conn():
    """Get a psycopg2 connection for user lookups."""
    from config import settings
    return psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
    )


def _ensure_users_table() -> None:
    """Create grid_users table if it doesn't exist."""
    try:
        conn = _get_db_conn()
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS grid_users (
                id          SERIAL PRIMARY KEY,
                username    TEXT NOT NULL UNIQUE,
                pw_hash     TEXT NOT NULL,
                role        TEXT NOT NULL DEFAULT 'contributor'
                            CHECK (role IN ('admin', 'contributor')),
                created_at  TIMESTAMPTZ DEFAULT NOW(),
                last_login  TIMESTAMPTZ
            );
            CREATE INDEX IF NOT EXISTS idx_grid_users_username ON grid_users (username);
        """)
        conn.close()
    except Exception as e:
        log.warning("Could not ensure grid_users table: {e}", e=e)


# Initialize on import
_ensure_users_table()


# ── Password Helpers ──────────────────────────────────────────

def hash_password(password: str) -> str:
    """Return bcrypt hash of password."""
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    """Verify plain password against bcrypt hash."""
    return pwd_context.verify(plain, hashed)


# ── JWT ───────────────────────────────────────────────────────

def create_token(
    role: str = "admin",
    username: str = "operator",
    expires_hours: int | None = None,
) -> str:
    """Create a JWT with role and username claims."""
    _, jwt_secret, default_hours = _get_settings()
    hours = expires_hours or default_hours
    expire = datetime.now(timezone.utc) + timedelta(hours=hours)
    payload = {
        "sub": username,
        "role": role,
        "exp": expire,
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, jwt_secret, algorithm="HS256")


def verify_token(token: str) -> bool:
    """Return True if token is valid and not expired."""
    _, jwt_secret, _ = _get_settings()
    try:
        jwt.decode(token, jwt_secret, algorithms=["HS256"])
        return True
    except JWTError:
        return False


def decode_token(token: str) -> dict | None:
    """Decode and return token payload, or None."""
    _, jwt_secret, _ = _get_settings()
    try:
        return jwt.decode(token, jwt_secret, algorithms=["HS256"])
    except JWTError:
        return None


def get_token_expiry(token: str) -> str | None:
    """Return ISO8601 expiry time from token, or None."""
    payload = decode_token(token)
    if payload and payload.get("exp"):
        return datetime.fromtimestamp(payload["exp"], tz=timezone.utc).isoformat()
    return None


# ── Auth Dependencies ─────────────────────────────────────────

async def require_auth(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
) -> str:
    """FastAPI dependency: require valid JWT. Returns the token."""
    token = None

    if credentials:
        token = credentials.credentials
    else:
        token = request.query_params.get("token")

    if not token or not verify_token(token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return token


def require_role(*roles: str) -> Callable:
    """Factory: return a dependency that checks the token has one of the given roles.

    Usage:
        @router.get("/admin-only", dependencies=[Depends(require_role("admin"))])
    """
    async def _check_role(
        request: Request,
        credentials: HTTPAuthorizationCredentials | None = Depends(security),
    ) -> str:
        token = None
        if credentials:
            token = credentials.credentials
        else:
            token = request.query_params.get("token")

        if not token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired token",
            )

        payload = decode_token(token)
        if not payload:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired token",
            )

        user_role = payload.get("role", "contributor")
        if user_role not in roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Requires role: {', '.join(roles)}. You have: {user_role}",
            )
        return token

    return _check_role


# ── User Lookup ───────────────────────────────────────────────

def _get_user(username: str) -> dict | None:
    """Fetch a user from grid_users by username."""
    try:
        conn = _get_db_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT id, username, pw_hash, role, created_at, last_login "
            "FROM grid_users WHERE username = %s",
            (username,),
        )
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None
    except Exception as e:
        log.warning("User lookup failed: {e}", e=e)
        return None


def _update_last_login(username: str) -> None:
    """Update last_login timestamp."""
    try:
        conn = _get_db_conn()
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            "UPDATE grid_users SET last_login = NOW() WHERE username = %s",
            (username,),
        )
        conn.close()
    except Exception:
        pass


# ── Rate Limiting ─────────────────────────────────────────────

def _check_rate_limit(client_ip: str) -> None:
    """Raise 429 if too many login attempts."""
    now = time.time()
    try:
        with shelve.open(_rate_limit_path) as db:
            attempts: list[float] = db.get(client_ip, [])
            attempts = [t for t in attempts if now - t < _RATE_LIMIT_WINDOW]
            db[client_ip] = attempts
            if len(attempts) >= _RATE_LIMIT_MAX:
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail="Too many login attempts. Try again later.",
                )
    except HTTPException:
        raise
    except Exception as exc:
        log.warning("Rate limit store unavailable: {e}", e=str(exc))


def _record_login_attempt(client_ip: str) -> None:
    """Record a login attempt timestamp for rate limiting."""
    try:
        with shelve.open(_rate_limit_path) as db:
            attempts: list[float] = db.get(client_ip, [])
            attempts.append(time.time())
            db[client_ip] = attempts
    except Exception as exc:
        log.warning("Failed to record login attempt: {e}", e=str(exc))


# ── Routes ────────────────────────────────────────────────────

@router.post("/login", response_model=LoginResponse)
async def login(body: LoginRequest, request: Request) -> LoginResponse:
    """Authenticate with master password or user credentials."""
    client_ip = request.client.host if request.client else "unknown"
    _check_rate_limit(client_ip)
    _record_login_attempt(client_ip)

    # User login (username + password)
    if body.username:
        user = _get_user(body.username)
        if not user or not verify_password(body.password, user["pw_hash"]):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid username or password",
            )
        _update_last_login(body.username)
        _, _, expire_hours = _get_settings()
        token = create_token(
            role=user["role"],
            username=user["username"],
            expires_hours=expire_hours,
        )
        return LoginResponse(
            token=token,
            expires_in=expire_hours * 3600,
            role=user["role"],
            username=user["username"],
        )

    # Master password login (backward compatible)
    pw_hash, _, expire_hours = _get_settings()
    if not pw_hash:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Master password not configured. Run scripts/setup_auth.py first.",
        )
    if not verify_password(body.password, pw_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid password",
        )

    token = create_token(role="admin", username="operator", expires_hours=expire_hours)
    return LoginResponse(
        token=token,
        expires_in=expire_hours * 3600,
        role="admin",
        username="operator",
    )


@router.post("/logout")
async def logout(_token: str = Depends(require_auth)) -> dict:
    """Log out (token expiry handles invalidation)."""
    return {"status": "ok"}


@router.get("/verify", response_model=TokenVerifyResponse)
async def verify(token: str = Depends(require_auth)) -> TokenVerifyResponse:
    """Verify current token is valid."""
    expires_at = get_token_expiry(token) or ""
    payload = decode_token(token) or {}
    return TokenVerifyResponse(
        valid=True,
        expires_at=expires_at,
        role=payload.get("role", "admin"),
        username=payload.get("sub", "operator"),
    )


# ── User Management (admin-only) ─────────────────────────────

@router.post("/users", response_model=UserResponse)
async def create_user(
    body: CreateUserRequest,
    _token: str = Depends(require_role("admin")),
) -> UserResponse:
    """Create a new user account (admin only)."""
    if body.role not in VALID_ROLES:
        raise HTTPException(400, f"Invalid role. Must be one of: {', '.join(VALID_ROLES)}")
    if len(body.username) < 3 or len(body.username) > 50:
        raise HTTPException(400, "Username must be 3-50 characters")
    if len(body.password) < 8:
        raise HTTPException(400, "Password must be at least 8 characters")

    pw_hash = hash_password(body.password)

    try:
        conn = _get_db_conn()
        conn.autocommit = True
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "INSERT INTO grid_users (username, pw_hash, role) "
            "VALUES (%s, %s, %s) RETURNING id, username, role, created_at",
            (body.username, pw_hash, body.role),
        )
        user = dict(cur.fetchone())
        conn.close()
        log.info("User created: {u} (role={r})", u=body.username, r=body.role)
        return UserResponse(
            id=user["id"],
            username=user["username"],
            role=user["role"],
            created_at=user["created_at"].isoformat(),
        )
    except psycopg2.errors.UniqueViolation:
        raise HTTPException(409, f"Username '{body.username}' already exists")
    except Exception as e:
        log.error("Failed to create user: {e}", e=e)
        raise HTTPException(500, "Failed to create user")


@router.get("/users")
async def list_users(
    _token: str = Depends(require_role("admin")),
) -> list[UserResponse]:
    """List all user accounts (admin only)."""
    try:
        conn = _get_db_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT id, username, role, created_at FROM grid_users ORDER BY id"
        )
        users = [
            UserResponse(
                id=r["id"],
                username=r["username"],
                role=r["role"],
                created_at=r["created_at"].isoformat(),
            )
            for r in cur.fetchall()
        ]
        conn.close()
        return users
    except Exception as e:
        log.error("Failed to list users: {e}", e=e)
        raise HTTPException(500, "Failed to list users")


@router.delete("/users/{username}")
async def delete_user(
    username: str,
    _token: str = Depends(require_role("admin")),
) -> dict:
    """Delete a user account (admin only)."""
    try:
        conn = _get_db_conn()
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("DELETE FROM grid_users WHERE username = %s", (username,))
        if cur.rowcount == 0:
            raise HTTPException(404, f"User '{username}' not found")
        conn.close()
        log.info("User deleted: {u}", u=username)
        return {"status": "deleted", "username": username}
    except HTTPException:
        raise
    except Exception as e:
        log.error("Failed to delete user: {e}", e=e)
        raise HTTPException(500, "Failed to delete user")
