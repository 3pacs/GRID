"""Authentication schemas."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    password: str
    username: Optional[str] = None  # None = master password login


class LoginResponse(BaseModel):
    token: str
    expires_in: int
    role: str = "admin"
    username: str = "operator"


class TokenVerifyResponse(BaseModel):
    valid: bool
    expires_at: str
    role: str = "admin"
    username: str = "operator"


class RegisterRequest(BaseModel):
    username: str
    password: str


class CreateUserRequest(BaseModel):
    username: str
    password: str
    role: str = "contributor"  # admin or contributor


class UserResponse(BaseModel):
    id: int
    username: str
    role: str
    created_at: str


class StreamTicketRequest(BaseModel):
    path: str = Field(..., min_length=1, max_length=256)


class StreamTicketResponse(BaseModel):
    ticket: str
    expires_in: int
    path: str
