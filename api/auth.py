"""
GRID JWT authentication.

Single-operator auth — no user management, just a master password.
"""

from __future__ import annotations

import os
import shelve
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from loguru import logger as log
from passlib.context import CryptContext

from api.schemas.auth import LoginRequest, LoginResponse, TokenVerifyResponse

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer(auto_error=False)

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])

# Rate limiting — persisted to disk so state survives restarts
_RATE_LIMIT_WINDOW = 60  # seconds
_RATE_LIMIT_MAX = 5
_rate_limit_path = str(
    Path(os.getenv("GRID_DATA_DIR", tempfile.gettempdir())) / "grid_rate_limits"
)


def _get_settings() -> tuple[str, str, int]:
    """Return (password_hash, jwt_secret, expire_hours) from env."""
    pw_hash = os.getenv("GRID_MASTER_PASSWORD_HASH", "")
    jwt_secret = os.getenv("GRID_JWT_SECRET", "")
    if not jwt_secret:
        environment = os.getenv("ENVIRONMENT", "development")
        if environment != "development":
            raise RuntimeError(
                "GRID_JWT_SECRET must be set in non-development environments. "
                "Generate one with: python -c \"import secrets; print(secrets.token_urlsafe(64))\""
            )
        jwt_secret = "dev-secret-change-me"
    expire_hours = int(os.getenv("GRID_JWT_EXPIRE_HOURS", "168"))
    return pw_hash, jwt_secret, expire_hours


def hash_password(password: str) -> str:
    """Return bcrypt hash of password."""
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    """Verify plain password against bcrypt hash."""
    return pwd_context.verify(plain, hashed)


def create_token(expires_hours: int | None = None) -> str:
    """Create a JWT with exp claim."""
    _, jwt_secret, default_hours = _get_settings()
    hours = expires_hours or default_hours
    expire = datetime.now(timezone.utc) + timedelta(hours=hours)
    payload = {
        "sub": "grid-operator",
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


def get_token_expiry(token: str) -> str | None:
    """Return ISO8601 expiry time from token, or None."""
    _, jwt_secret, _ = _get_settings()
    try:
        payload = jwt.decode(token, jwt_secret, algorithms=["HS256"])
        exp = payload.get("exp")
        if exp:
            return datetime.fromtimestamp(exp, tz=timezone.utc).isoformat()
        return None
    except JWTError:
        return None


async def require_auth(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
) -> str:
    """FastAPI dependency: require valid JWT.

    Reads from Authorization header or ?token= query param.
    """
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


def _check_rate_limit(client_ip: str) -> None:
    """Raise 429 if too many login attempts.

    Attempts are persisted via ``shelve`` so rate limits survive
    server restarts — attackers cannot reset limits by restarting
    the process.
    """
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
        log.warning("Rate limit store unavailable, falling back to allow: {e}", e=str(exc))


def _record_login_attempt(client_ip: str) -> None:
    """Record a login attempt timestamp for rate limiting."""
    try:
        with shelve.open(_rate_limit_path) as db:
            attempts: list[float] = db.get(client_ip, [])
            attempts.append(time.time())
            db[client_ip] = attempts
    except Exception as exc:
        log.warning("Failed to record login attempt: {e}", e=str(exc))


@router.post("/login", response_model=LoginResponse)
async def login(body: LoginRequest, request: Request) -> LoginResponse:
    """Authenticate with master password."""
    client_ip = request.client.host if request.client else "unknown"
    _check_rate_limit(client_ip)

    pw_hash, _, expire_hours = _get_settings()

    if not pw_hash:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Master password not configured. Run scripts/setup_auth.py first.",
        )

    _record_login_attempt(client_ip)

    if not verify_password(body.password, pw_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid password",
        )

    token = create_token(expire_hours)
    return LoginResponse(
        token=token,
        expires_in=expire_hours * 3600,
    )


@router.post("/logout")
async def logout(_token: str = Depends(require_auth)) -> dict:
    """Log out (token expiry handles invalidation)."""
    return {"status": "ok"}


@router.get("/verify", response_model=TokenVerifyResponse)
async def verify(token: str = Depends(require_auth)) -> TokenVerifyResponse:
    """Verify current token is valid."""
    expires_at = get_token_expiry(token) or ""
    return TokenVerifyResponse(valid=True, expires_at=expires_at)
