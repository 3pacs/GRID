"""JWT backend: PyJWT replaces python-jose in api.auth and astrogrid_api.auth.

These tests pin what the swap must keep:

* tokens round-trip, and the claims and expiry read back unchanged;
* sessions minted by the old python-jose backend still verify, so the deploy
  logs nobody out (the legacy tokens below were minted once with
  python-jose 3.5.0 and are fixed test vectors);
* every bad token is refused by returning False/None, never by raising:
  ``alg: none``, a different algorithm, the wrong key, a tampered payload,
  expiry, and garbage;
* nothing in the tree imports ``jose`` again.
"""

from __future__ import annotations

import base64
import json
import os
import re
import time
from pathlib import Path

import jwt
import pytest

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("DB_PASSWORD", "testpass")

import api.auth as grid_auth  # noqa: E402
import astrogrid_api.auth as astro_auth  # noqa: E402

SECRET = "jwt-backend-test-secret-at-least-32-bytes-long"
LEGACY_SECRET = "legacy-jose-session-secret-for-compat-test-0123456789"
# python-jose 3.5.0, HS256, {"sub": "operator", "role": "admin", "exp": 4102444800, "iat": 1794614400}
LEGACY_LIVE = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJzdWIiOiJvcGVyYXRvciIsInJvbGUiOiJhZG1pbiIsImV4cCI6NDEwMjQ0NDgwMCwiaWF0IjoxNzk0NjE0NDAwfQ."
    "BCDrYlkGoqLe1TwTzrM6e829n1lA5GP63ksk23ZtGjA"
)
# Same claims, but exp 1794614460 (2026-11-13), already past.
LEGACY_EXPIRED = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJzdWIiOiJvcGVyYXRvciIsInJvbGUiOiJhZG1pbiIsImV4cCI6MTc5NDYxNDQ2MCwiaWF0IjoxNzk0NjE0NDAwfQ."
    "vsVV9tqhBJU54CiXjIB8OIDuPBCUtc04lbAtdkJoJWc"
)

AUTH_MODULES = [pytest.param(grid_auth, id="api"), pytest.param(astro_auth, id="astrogrid_api")]


@pytest.fixture(autouse=True)
def _jwt_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRID_JWT_SECRET", SECRET)
    monkeypatch.setenv("GRID_JWT_EXPIRE_HOURS", "1")


def _b64(obj: dict) -> str:
    raw = json.dumps(obj, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


@pytest.mark.parametrize("auth", AUTH_MODULES)
def test_round_trip_keeps_claims(auth) -> None:
    token = auth.create_token(role="contributor", username="alice")
    assert isinstance(token, str)
    assert auth.verify_token(token) is True
    claims = auth.decode_token(token)
    assert claims["sub"] == "alice"
    assert claims["role"] == "contributor"
    assert isinstance(claims["exp"], int) and isinstance(claims["iat"], int)
    assert 3500 <= claims["exp"] - time.time() <= 3600
    assert auth.get_token_expiry(token).endswith("+00:00")


@pytest.mark.parametrize("auth", AUTH_MODULES)
def test_legacy_jose_session_still_verifies(auth, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRID_JWT_SECRET", LEGACY_SECRET)
    assert auth.verify_token(LEGACY_LIVE) is True
    assert auth.decode_token(LEGACY_LIVE) == {
        "sub": "operator",
        "role": "admin",
        "exp": 4102444800,
        "iat": 1794614400,
    }
    assert auth.verify_token(LEGACY_EXPIRED) is False
    assert auth.decode_token(LEGACY_EXPIRED) is None


# The HS512 vector reuses SECRET on purpose (algorithm pinning), so PyJWT warns its key is short for SHA512.
@pytest.mark.filterwarnings("ignore::jwt.warnings.InsecureKeyLengthWarning")
@pytest.mark.parametrize("auth", AUTH_MODULES)
def test_bad_tokens_are_refused_without_raising(auth) -> None:
    now = int(time.time())
    claims = {"sub": "mallory", "role": "admin", "exp": now + 600, "iat": now}
    good = auth.create_token(role="contributor", username="alice")
    header, _payload, signature = good.split(".")
    bad = {
        "alg_none": f"{_b64({'alg': 'none', 'typ': 'JWT'})}.{_b64(claims)}.",
        "other_alg": jwt.encode(claims, SECRET, algorithm="HS512"),
        "wrong_key": jwt.encode(claims, "a-different-secret-that-is-also-32-bytes", algorithm="HS256"),
        "tampered_payload": f"{header}.{_b64({**claims, 'sub': 'alice'})}.{signature}",
        "expired": jwt.encode({**claims, "exp": now - 5}, SECRET, algorithm="HS256"),
        "garbage": "not.a.jwt",
        "empty": "",
    }
    for name, token in bad.items():
        assert auth.verify_token(token) is False, name
        assert auth.decode_token(token) is None, name
        assert auth.get_token_expiry(token) is None, name


def test_stream_ticket_is_not_a_session_and_survives_the_swap() -> None:
    session = grid_auth.create_token(role="admin", username="operator")
    issued = grid_auth.issue_stream_ticket(session, "/api/v1/events/stream")
    ticket = issued["ticket"]
    assert grid_auth.decode_token(ticket) is None
    claims = grid_auth.redeem_stream_ticket(ticket, "/api/v1/events/stream")
    assert claims is not None and claims["sub"] == "operator"
    assert grid_auth.redeem_stream_ticket(ticket, "/api/v1/events/stream") is None


def test_nothing_imports_python_jose() -> None:
    root = Path(__file__).resolve().parents[1]
    pattern = re.compile(r"^\s*(from\s+jose\b|import\s+jose\b)", re.MULTILINE)
    skip = {".git", "node_modules", ".venv", "venv", "pwa_dist"}
    offenders = []
    for path in root.rglob("*.py"):
        if skip.intersection(path.parts) or path.parts[len(root.parts)] == "docs":
            continue
        if pattern.search(path.read_text(encoding="utf-8", errors="ignore")):
            offenders.append(str(path.relative_to(root)))
    assert offenders == []
