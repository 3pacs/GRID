"""
GRID — Normalized schemas for the dealer_flow subpackage (GEX V2 §5).

Pydantic v2 models that enforce the canonical normalized contract shape.
Every venue adapter must map raw exchange responses into these schemas
BEFORE any validation, Greek completion, or exposure math runs. Units and
conventions are documented at the field level so downstream math never has
to guess.

Three models:

- ``OptionContract``  — one contract row with ~30 fields covering venue
  identity, strike/expiry, quote state, OI, IV, exchange-provided Greeks
  (optional), timestamps, and data-quality flags.
- ``OptionSnapshot``  — a bundle of contracts for one underlying at one
  timestamp, plus the spot price used to compute exposures.
- ``OptionExposure``  — the structured aggregation output (net GEX/CEX/
  VEX/VOEX/COLEX/ZEX/SPEEDEX, gamma flip, walls, confidence score).

Skeleton only. Field-level validators and cross-field checks are
stubbed here and will be completed in GEX-8 alongside the real adapter.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


OptionType = Literal["call", "put"]
GreekSource = Literal["exchange", "recomputed", "mixed", "missing"]


class OptionContract(BaseModel):
    """Normalized per-contract option row (GEX V2 spec §5.1).

    Holds 30+ fields per the canonical schema. Optional Greeks remain
    ``None`` when the venue does not publish them; the Greek-completion
    layer fills them in and flips ``greek_source_*`` provenance flags.
    """

    model_config = ConfigDict(extra="allow", frozen=False)

    # ── venue identity ─────────────────────────────────────────────
    venue: str = Field(..., description="deribit | okx | bybit | ...")
    symbol: str = Field(..., description="venue-native option symbol")
    underlying: str = Field(..., description="BTC, ETH, SOL, ...")

    # ── contract economics ─────────────────────────────────────────
    expiry_ts_utc: int = Field(..., description="expiration UTC ms")
    strike: float = Field(..., description="strike price")
    option_type: OptionType = Field(..., description="call or put")
    contract_size: float = Field(..., description="underlying or payout size")
    settlement_currency: str = Field(..., description="BTC, ETH, USDC, USD, ...")
    quote_currency: str = Field(..., description="quote denomination")

    # ── quote state ────────────────────────────────────────────────
    mark_price: float | None = None
    bid: float | None = None
    ask: float | None = None
    mid: float | None = None

    # ── open interest & volume ─────────────────────────────────────
    oi_contracts: float = Field(..., description="OI in contracts")
    oi_underlying_units: float | None = None
    volume_24h: float | None = None

    # ── underlying & vol ───────────────────────────────────────────
    underlying_price: float = Field(..., description="normalized spot / index")
    iv_decimal: float | None = Field(None, description="IV as decimal (0.65)")

    # ── exchange-provided Greeks (optional) ────────────────────────
    delta: float | None = None
    gamma: float | None = None
    vanna: float | None = None
    charm: float | None = None
    vomma: float | None = None
    color: float | None = None
    zomma: float | None = None
    speed: float | None = None

    # ── timestamps ─────────────────────────────────────────────────
    source_ts_utc: int = Field(..., description="venue or fetch ts (UTC ms)")
    ingest_ts_utc: int = Field(..., description="local ingest ts (UTC ms)")

    # ── validation + derived ───────────────────────────────────────
    is_expired: bool = False
    data_quality_flags: list[str] = Field(default_factory=list)

    # ── derived (optional, spec §5.2) ──────────────────────────────
    time_to_expiry_years: float | None = None
    dte_days: float | None = None
    distance_from_spot_pct: float | None = None
    spread_bps: float | None = None
    quote_age_ms: int | None = None
    greek_source: GreekSource | None = None
    row_confidence: float | None = None


class OptionSnapshot(BaseModel):
    """Collection of normalized contracts for one underlying at one ts."""

    model_config = ConfigDict(extra="allow")

    snapshot_id: str
    underlying: str
    spot: float
    venues: list[str]
    max_dte_days: int
    source_ts_utc: int
    ingest_ts_utc: int
    contracts: list[OptionContract] = Field(default_factory=list)
    data_quality: dict[str, Any] = Field(default_factory=dict)


class OptionExposure(BaseModel):
    """Aggregated dealer-flow exposure payload (GEX V2 §18)."""

    model_config = ConfigDict(extra="allow")

    snapshot_id: str
    underlying: str
    spot: float
    venues: list[str]
    max_dte_days: int

    # net exposures
    net_gex: float | None = None
    net_cex: float | None = None
    net_vex: float | None = None
    net_voex: float | None = None
    net_colex: float | None = None
    net_zex: float | None = None
    net_speedex: float | None = None

    # structural levels
    gamma_flip: float | None = None
    call_wall: float | None = None
    put_wall: float | None = None
    call_charm_wall: float | None = None
    put_charm_wall: float | None = None

    # provenance + scoring
    confidence_score: float | None = None
    regime_tags: list[str] = Field(default_factory=list)
    data_quality: dict[str, Any] = Field(default_factory=dict)
