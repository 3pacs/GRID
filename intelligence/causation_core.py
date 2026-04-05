"""
GRID Intelligence — Causal Connection Engine (core module).

Data classes, schema, constants, and primary entry points:
  find_causes              — all probable causes for a single action
  batch_find_causes        — run find_causes for all recent signal_sources
  get_suspicious_trades    — trades where the cause is likely non-public info
  generate_causal_narrative — LLM or rule-based "why is everyone trading X?"
"""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Data Classes ─────────────────────────────────────────────────────────


@dataclass
class CausalLink:
    """A causal connection between an actor's trade and an upstream event."""

    action_id: str           # the signal_sources row id
    actor: str
    action: str              # BUY / SELL
    ticker: str
    action_date: str
    probable_cause: str      # one-line description
    cause_type: str          # 'contract', 'legislation', 'earnings',
                             # 'insider_knowledge', 'rebalancing', 'unknown'
    evidence: list[dict]
    probability: float       # 0-1
    lead_time_days: float    # how far before the action did the cause occur

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CausalChain:
    """A multi-hop causal chain tracing a path from root cause to final effect.

    Example: lobbying -> legislation -> contract award -> stock price move -> insider sale.
    """

    ticker: str
    chain: list[CausalLink]       # ordered sequence of causes and effects
    total_hops: int
    timespan_days: int            # from first cause to final effect
    total_dollar_flow: float
    key_actors: list[str]
    narrative: str
    confidence: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "chain": [link.to_dict() for link in self.chain],
            "total_hops": self.total_hops,
            "timespan_days": self.timespan_days,
            "total_dollar_flow": self.total_dollar_flow,
            "key_actors": self.key_actors,
            "narrative": self.narrative,
            "confidence": self.confidence,
        }


# ── Schema ───────────────────────────────────────────────────────────────

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS causal_links (
    id              SERIAL PRIMARY KEY,
    signal_id       INT,
    actor           TEXT,
    ticker          TEXT,
    action_date     DATE,
    cause_type      TEXT,
    probable_cause  TEXT,
    evidence        JSONB,
    probability     NUMERIC,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
"""

_CREATE_INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_causal_links_ticker ON causal_links (ticker);",
    "CREATE INDEX IF NOT EXISTS idx_causal_links_signal ON causal_links (signal_id);",
    "CREATE INDEX IF NOT EXISTS idx_causal_links_type ON causal_links (cause_type);",
    "CREATE INDEX IF NOT EXISTS idx_causal_links_date ON causal_links (action_date DESC);",
]

_CREATE_CHAINS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS causal_chains (
    id                SERIAL PRIMARY KEY,
    ticker            TEXT,
    chain             JSONB NOT NULL,
    total_hops        INT,
    timespan_days     INT,
    total_dollar_flow NUMERIC,
    key_actors        JSONB,
    narrative         TEXT,
    confidence        NUMERIC,
    created_at        TIMESTAMPTZ DEFAULT NOW()
);
"""

_CREATE_CHAINS_INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_causal_chains_ticker ON causal_chains (ticker);",
    "CREATE INDEX IF NOT EXISTS idx_causal_chains_hops ON causal_chains (total_hops DESC);",
    "CREATE INDEX IF NOT EXISTS idx_causal_chains_confidence ON causal_chains (confidence DESC);",
    "CREATE INDEX IF NOT EXISTS idx_causal_chains_created ON causal_chains (created_at DESC);",
]


def ensure_table(engine: Engine) -> None:
    """Create the causal_links and causal_chains tables and indexes if they don't exist."""
    with engine.begin() as conn:
        conn.execute(text(_CREATE_TABLE_SQL))
        for idx_sql in _CREATE_INDEX_SQL:
            conn.execute(text(idx_sql))
        conn.execute(text(_CREATE_CHAINS_TABLE_SQL))
        for idx_sql in _CREATE_CHAINS_INDEX_SQL:
            conn.execute(text(idx_sql))
    log.debug("causal_links + causal_chains tables ensured")


# ── Constants ────────────────────────────────────────────────────────────

# Search window: how many days before/after the action to look for causes
_PRE_WINDOW_DAYS = 60
_POST_WINDOW_DAYS = 14

# Known macro event series_id patterns
_MACRO_SERIES_PATTERNS = [
    "FRED:FEDFUNDS",
    "FRED:CPIAUCSL",
    "FRED:UNRATE",
    "FRED:GDP",
    "FRED:PCE",
    "FRED:PAYEMS",
]

# FOMC meeting dates (known well in advance) — rough 2024-2026 schedule
_FOMC_KEYWORDS = {"fomc", "federal open market", "fed meeting", "rate decision"}


# ── General Helpers ──────────────────────────────────────────────────────


def _parse_json(raw: Any) -> dict[str, Any]:
    """Parse a JSON string or dict safely."""
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            return {}
    return {}


def _safe_float(val: Any) -> float | None:
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _macro_series_to_name(series_id: str) -> str:
    """Map a FRED series_id to a human-readable macro event name."""
    name_map = {
        "FEDFUNDS": "Federal Funds Rate (FOMC)",
        "CPIAUCSL": "CPI (Inflation)",
        "UNRATE": "Unemployment Rate",
        "GDP": "GDP",
        "PCE": "Personal Consumption Expenditures",
        "PAYEMS": "Nonfarm Payrolls",
    }
    for key, name in name_map.items():
        if key in series_id:
            return name
    return series_id
