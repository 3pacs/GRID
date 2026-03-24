"""Watchlist schemas."""

from __future__ import annotations

from pydantic import BaseModel, field_validator


class WatchlistItemCreate(BaseModel):
    ticker: str
    display_name: str | None = None
    asset_type: str = "stock"
    notes: str | None = None

    @field_validator("ticker")
    @classmethod
    def validate_ticker(cls, v: str) -> str:
        v = v.strip().upper()
        if not v or len(v) > 20:
            raise ValueError("Ticker must be 1-20 characters")
        return v

    @field_validator("asset_type")
    @classmethod
    def validate_asset_type(cls, v: str) -> str:
        allowed = ("stock", "crypto", "commodity", "etf", "index", "forex")
        if v not in allowed:
            raise ValueError(f"asset_type must be one of: {', '.join(allowed)}")
        return v


class WatchlistItemResponse(BaseModel):
    id: int
    ticker: str
    display_name: str | None = None
    asset_type: str
    added_at: str
    notes: str | None = None

    class Config:
        from_attributes = True
