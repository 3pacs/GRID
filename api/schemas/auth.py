"""Authentication schemas."""

from __future__ import annotations

from pydantic import BaseModel


class LoginRequest(BaseModel):
    password: str


class LoginResponse(BaseModel):
    token: str
    expires_in: int


class TokenVerifyResponse(BaseModel):
    valid: bool
    expires_at: str
