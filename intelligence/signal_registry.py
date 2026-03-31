"""
GRID Intelligence — Signal Registry.

Typed, temporal, PIT-correct signal store for all intelligence modules.
Every signal carries valid_from/valid_until so the Oracle can reconstruct
state at any historical as_of timestamp without look-ahead contamination.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


class SignalType(str, Enum):
    DIRECTIONAL = "DIRECTIONAL"
    MAGNITUDE   = "MAGNITUDE"
    REGIME      = "REGIME"
    PATTERN     = "PATTERN"
    NARRATIVE   = "NARRATIVE"


class Direction(str, Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"


@dataclass(frozen=True)
class RegisteredSignal:
    signal_id:       str
    source_module:   str
    signal_type:     SignalType
    direction:       Direction
    value:           float
    confidence:      float
    valid_from:      datetime
    ticker:          str | None        = None
    z_score:         float | None      = None
    valid_until:     datetime | None   = None
    freshness_hours: float | None      = None
    metadata:        dict[str, Any]    = field(default_factory=dict)
    provenance:      str | None        = None

    def __post_init__(self) -> None:
        if not (0.0 <= self.confidence <= 1.0):
            raise ValueError(f"confidence must be in [0, 1], got {self.confidence}")
        if self.valid_from.tzinfo is None:
            raise ValueError("valid_from must be timezone-aware UTC")
        if self.valid_until is not None and self.valid_until.tzinfo is None:
            raise ValueError("valid_until must be timezone-aware UTC")
        if self.valid_until is not None and self.valid_until <= self.valid_from:
            raise ValueError("valid_until must be after valid_from")


def make_signal_id(source_module: str, key: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{source_module}:{key}"))


class SignalRegistry:

    @staticmethod
    def register(signals: list[RegisteredSignal], engine: Engine) -> int:
        if not signals:
            return 0
        inserted = 0
        with engine.begin() as conn:
            for sig in signals:
                result = conn.execute(
                    text("""
                        INSERT INTO signal_registry (
                            signal_id, source_module, signal_type, ticker,
                            direction, value, z_score, confidence,
                            valid_from, valid_until, freshness_hours,
                            metadata, provenance
                        ) VALUES (
                            :signal_id, :source_module, :signal_type, :ticker,
                            :direction, :value, :z_score, :confidence,
                            :valid_from, :valid_until, :freshness_hours,
                            CAST(:metadata AS jsonb), :provenance
                        )
                        ON CONFLICT (signal_id, valid_from) DO NOTHING
                    """),
                    {
                        "signal_id":       sig.signal_id,
                        "source_module":   sig.source_module,
                        "signal_type":     sig.signal_type.value if hasattr(sig.signal_type, 'value') else str(sig.signal_type),
                        "ticker":          sig.ticker,
                        "direction":       sig.direction.value if hasattr(sig.direction, 'value') else str(sig.direction),
                        "value":           sig.value,
                        "z_score":         sig.z_score,
                        "confidence":      sig.confidence,
                        "valid_from":      sig.valid_from,
                        "valid_until":     sig.valid_until,
                        "freshness_hours": sig.freshness_hours,
                        "metadata":        json.dumps(sig.metadata),
                        "provenance":      sig.provenance,
                    },
                )
                inserted += result.rowcount
        log.info(
            "SignalRegistry.register: {n}/{total} new rows from {mod}",
            n=inserted, total=len(signals),
            mod=signals[0].source_module if signals else "unknown",
        )
        return inserted

    @staticmethod
    def query(
        engine: Engine,
        *,
        ticker: str | None = None,
        source_module: str | None = None,
        signal_type: SignalType | None = None,
        as_of: datetime | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        if as_of is None:
            as_of = datetime.now(timezone.utc)
        clauses = [
            "valid_from <= :as_of",
            "(valid_until IS NULL OR valid_until > :as_of)",
        ]
        params: dict[str, Any] = {"as_of": as_of, "limit": limit}
        if ticker is not None:
            clauses.append("ticker = :ticker")
            params["ticker"] = ticker
        if source_module is not None:
            clauses.append("source_module = :source_module")
            params["source_module"] = source_module
        if signal_type is not None:
            clauses.append("signal_type = :signal_type")
            params["signal_type"] = signal_type.value
        where = " AND ".join(clauses)
        sql = text(
            f"SELECT id, signal_id, source_module, signal_type, ticker, "
            f"direction, value, z_score, confidence, created_at, "
            f"valid_from, valid_until, freshness_hours, metadata, provenance "
            f"FROM signal_registry "
            f"WHERE {where} "
            f"ORDER BY valid_from DESC LIMIT :limit"
        )
        with engine.connect() as conn:
            rows = conn.execute(sql, params).mappings().all()
        return [dict(r) for r in rows]

    @staticmethod
    def query_for_ticker(ticker: str, engine: Engine, as_of: datetime | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return SignalRegistry.query(engine, ticker=ticker, as_of=as_of, limit=limit)

    @staticmethod
    def query_by_source(source_module: str, engine: Engine, as_of: datetime | None = None, limit: int = 200) -> list[dict[str, Any]]:
        return SignalRegistry.query(engine, source_module=source_module, as_of=as_of, limit=limit)

    @staticmethod
    def prune_expired(engine: Engine, days_old: int = 7) -> int:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days_old)
        with engine.begin() as conn:
            result = conn.execute(
                text("DELETE FROM signal_registry WHERE valid_until IS NOT NULL AND valid_until < :cutoff"),
                {"cutoff": cutoff},
            )
        deleted = result.rowcount
        log.info("SignalRegistry.prune_expired: removed {n} rows older than {d}d", n=deleted, d=days_old)
        return deleted

    @staticmethod
    def get_signal_count(engine: Engine) -> dict[str, int]:
        now = datetime.now(timezone.utc)
        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT source_module, COUNT(*) AS cnt
                    FROM signal_registry
                    WHERE valid_from <= :now AND (valid_until IS NULL OR valid_until > :now)
                    GROUP BY source_module ORDER BY cnt DESC
                """),
                {"now": now},
            ).fetchall()
        return {r[0]: r[1] for r in rows}
