"""
GRID Intelligence — Dollar Flow Normalizer.

Converts all signal sources into estimated USD amounts for apples-to-apples
comparison across congressional trades, insider filings, dark pool activity,
13F position changes, ETF flows, whale options, and prediction markets.

Each signal type has a different native unit; this module normalizes them
all into a single `amount_usd` figure with confidence labels and provenance.

Conversion rules:
  - Congressional: midpoint of reported range (e.g., "$1M-$5M" -> $3M)
  - Insider (Form 4): shares x price at transaction date
  - Dark pool: volume x VWAP estimate from resolved_series
  - 13F: quarterly holdings delta (value_usd from raw_series)
  - ETF flows: daily dollar volume from ETF_FLOW series
  - Whale options: notional premium from signal_value
  - Prediction markets: probability-implied stake if available

Key entry points:
  normalize_all_flows    — scan signal_sources + raw_series, normalize to USD
  get_flows_by_ticker    — filter flows for a single ticker
  get_flows_by_sector    — aggregate flows by GICS sector (via sector_map)
  get_aggregate_flows    — net flow per sector, per actor tier
  get_biggest_movers     — top 10 by absolute dollar amount
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import date, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ─────────────────────────────────────────────────────────────

# Amount range mapping (mirrors congressional.py)
_AMOUNT_RANGES: dict[str, tuple[int, int]] = {
    "A": (1_001, 15_000),
    "B": (15_001, 50_000),
    "C": (50_001, 100_000),
    "D": (100_001, 250_000),
    "E": (250_001, 500_000),
    "F": (500_001, 1_000_000),
    "G": (1_000_001, 5_000_000),
    "H": (5_000_001, 25_000_000),
    "I": (25_000_001, 50_000_000),
    "J": (50_000_001, 999_999_999),
}

# Default VWAP estimate when resolved_series has no data (conservative)
_DEFAULT_VWAP_ESTIMATE: float = 50.0

# Source types we normalize
_SOURCE_TYPES = [
    "congressional",
    "insider",
    "darkpool",
    "options_flow",
    "prediction_market",
]


# ── Table DDL ─────────────────────────────────────────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS dollar_flows (
    id SERIAL PRIMARY KEY,
    source_type TEXT NOT NULL,
    actor_name TEXT,
    ticker TEXT,
    amount_usd NUMERIC NOT NULL,
    direction TEXT NOT NULL,
    confidence TEXT DEFAULT 'estimated',
    evidence JSONB,
    flow_date DATE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dollar_flows_ticker
    ON dollar_flows (ticker, flow_date DESC);
CREATE INDEX IF NOT EXISTS idx_dollar_flows_source
    ON dollar_flows (source_type, flow_date DESC);
CREATE INDEX IF NOT EXISTS idx_dollar_flows_date
    ON dollar_flows (flow_date DESC);
CREATE INDEX IF NOT EXISTS idx_dollar_flows_amount
    ON dollar_flows (amount_usd DESC);
"""


# ── Helpers ───────────────────────────────────────────────────────────────

def _ensure_table(engine: Engine) -> None:
    """Create dollar_flows table if it does not exist."""
    with engine.begin() as conn:
        for stmt in CREATE_TABLE_SQL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(text(stmt))


def _midpoint_amount(amount_range: str) -> float:
    """Get the midpoint dollar value from an amount range code or string.

    Parameters:
        amount_range: Either a code ('A'-'J') or a string like '$1,001 - $15,000'.

    Returns:
        Midpoint dollar value as float.
    """
    if not amount_range:
        return 0.0

    # Try coded range first
    if amount_range.upper() in _AMOUNT_RANGES:
        lo, hi = _AMOUNT_RANGES[amount_range.upper()]
        return (lo + hi) / 2.0

    # Try parsing dollar range string
    nums = re.findall(r"[\d,]+", amount_range.replace(",", ""))
    if len(nums) >= 2:
        try:
            lo = float(nums[0].replace(",", ""))
            hi = float(nums[1].replace(",", ""))
            return (lo + hi) / 2.0
        except ValueError:
            pass

    return 0.0


def _direction_from_signal(signal_type: str) -> str:
    """Map signal_type to 'inflow' or 'outflow'.

    Parameters:
        signal_type: The signal_type field from signal_sources.

    Returns:
        'inflow' or 'outflow'.
    """
    upper = signal_type.upper()
    if any(kw in upper for kw in ("BUY", "PURCHASE", "NEW", "INCREASED", "CALL")):
        return "inflow"
    if any(kw in upper for kw in ("SELL", "SALE", "CLOSED", "DECREASED", "PUT")):
        return "outflow"
    # Default based on context
    return "inflow"


def _get_vwap_estimate(engine: Engine, ticker: str, obs_date: date) -> float:
    """Attempt to get a VWAP estimate from resolved_series for a ticker/date.

    Falls back to _DEFAULT_VWAP_ESTIMATE if unavailable.

    Parameters:
        engine: SQLAlchemy engine.
        ticker: Stock ticker.
        obs_date: Observation date.

    Returns:
        Estimated VWAP as float.
    """
    try:
        with engine.connect() as conn:
            # Look for a price series for this ticker in raw_series
            row = conn.execute(
                text(
                    "SELECT value FROM raw_series "
                    "WHERE series_id LIKE :pattern "
                    "AND obs_date <= :od "
                    "ORDER BY obs_date DESC LIMIT 1"
                ),
                {"pattern": f"%{ticker}%close%", "od": obs_date},
            ).fetchone()
            if row and row[0] and row[0] > 0:
                return float(row[0])
    except Exception:
        pass
    return _DEFAULT_VWAP_ESTIMATE


def _build_sector_lookup() -> dict[str, str]:
    """Build a ticker -> sector name mapping from the SECTOR_MAP.

    Returns:
        Dict mapping uppercase ticker to sector name.
    """
    try:
        from analysis.sector_map import SECTOR_MAP
    except ImportError:
        return {}

    lookup: dict[str, str] = {}
    for sector_name, sector_data in SECTOR_MAP.items():
        etf = sector_data.get("etf")
        if etf:
            lookup[etf.upper()] = sector_name
        for _subsector_name, subsector_data in sector_data.get("subsectors", {}).items():
            for actor in subsector_data.get("actors", []):
                ticker = actor.get("ticker")
                if ticker:
                    lookup[ticker.upper()] = sector_name
    return lookup


_SECTOR_LOOKUP: dict[str, str] | None = None


def _get_sector(ticker: str | None) -> str:
    """Return the sector for a ticker, or 'Unknown'."""
    global _SECTOR_LOOKUP
    if _SECTOR_LOOKUP is None:
        _SECTOR_LOOKUP = _build_sector_lookup()
    if not ticker:
        return "Unknown"
    return _SECTOR_LOOKUP.get(ticker.upper(), "Unknown")


# ── Normalization per source type ─────────────────────────────────────────

def _normalize_congressional(row: dict) -> dict | None:
    """Normalize a congressional trading signal to USD.

    Parameters:
        row: Dict with signal_sources columns.

    Returns:
        Normalized flow dict or None if unparseable.
    """
    sv = row.get("signal_value") or {}
    if isinstance(sv, str):
        try:
            sv = json.loads(sv)
        except (json.JSONDecodeError, TypeError):
            sv = {}

    amount_range = sv.get("amount_range", "")
    midpoint = sv.get("amount_midpoint")

    if midpoint and float(midpoint) > 0:
        amount = float(midpoint)
    else:
        amount = _midpoint_amount(amount_range)

    if amount <= 0:
        return None

    return {
        "source_type": "congressional",
        "actor_name": row.get("source_id", ""),
        "ticker": row.get("ticker"),
        "amount_usd": amount,
        "direction": _direction_from_signal(row.get("signal_type", "")),
        "confidence": "estimated",
        "evidence": {
            "amount_range": amount_range,
            "chamber": sv.get("chamber"),
            "party": sv.get("party"),
            "signal_type": row.get("signal_type"),
        },
        "flow_date": row.get("signal_date"),
    }


def _normalize_insider(row: dict) -> dict | None:
    """Normalize an insider (Form 4) signal to USD.

    Uses shares x price for individual trades, total_value for cluster buys.

    Parameters:
        row: Dict with signal_sources columns.

    Returns:
        Normalized flow dict or None.
    """
    sv = row.get("signal_value") or {}
    if isinstance(sv, str):
        try:
            sv = json.loads(sv)
        except (json.JSONDecodeError, TypeError):
            sv = {}

    signal_type = row.get("signal_type", "")

    # Cluster buy: use total_value directly
    if "CLUSTER" in signal_type.upper():
        amount = float(sv.get("total_value", 0))
        if amount <= 0:
            return None
        return {
            "source_type": "insider",
            "actor_name": row.get("source_id", ""),
            "ticker": row.get("ticker"),
            "amount_usd": amount,
            "direction": "inflow",
            "confidence": "confirmed",
            "evidence": {
                "insider_count": sv.get("insider_count"),
                "signal_type": signal_type,
            },
            "flow_date": row.get("signal_date"),
        }

    # Individual trade: shares x price
    shares = float(sv.get("shares", 0))
    price = float(sv.get("price", 0))
    value = float(sv.get("value", 0))

    # Use pre-computed value if available, otherwise shares * price
    amount = value if value > 0 else (shares * price)
    if amount <= 0:
        return None

    return {
        "source_type": "insider",
        "actor_name": row.get("source_id", ""),
        "ticker": row.get("ticker"),
        "amount_usd": amount,
        "direction": _direction_from_signal(signal_type),
        "confidence": "confirmed",
        "evidence": {
            "shares": shares,
            "price": price,
            "insider_title": sv.get("insider_title"),
            "signal_type": signal_type,
        },
        "flow_date": row.get("signal_date"),
    }


def _normalize_darkpool(row: dict, engine: Engine) -> dict | None:
    """Normalize a dark pool volume spike to estimated USD.

    Multiplies spike volume by a VWAP estimate from resolved_series.

    Parameters:
        row: Dict with signal_sources columns.
        engine: SQLAlchemy engine for VWAP lookup.

    Returns:
        Normalized flow dict or None.
    """
    sv = row.get("signal_value") or {}
    if isinstance(sv, str):
        try:
            sv = json.loads(sv)
        except (json.JSONDecodeError, TypeError):
            sv = {}

    volume = float(sv.get("volume", 0))
    if volume <= 0:
        return None

    ticker = row.get("ticker", "")
    obs_date = row.get("signal_date", date.today())
    vwap = _get_vwap_estimate(engine, ticker, obs_date)

    amount = volume * vwap

    return {
        "source_type": "darkpool",
        "actor_name": row.get("source_id", ""),
        "ticker": ticker,
        "amount_usd": amount,
        "direction": "inflow",  # Dark pool direction is ambiguous
        "confidence": "estimated",
        "evidence": {
            "volume": volume,
            "vwap_estimate": vwap,
            "spike_ratio": sv.get("spike_ratio"),
            "signal_type": row.get("signal_type"),
        },
        "flow_date": obs_date,
    }


def _normalize_whale_options(row: dict) -> dict | None:
    """Normalize a whale options flow signal to USD.

    Uses the notional premium directly (already in USD).

    Parameters:
        row: Dict with signal_sources columns.

    Returns:
        Normalized flow dict or None.
    """
    sv = row.get("signal_value") or {}
    if isinstance(sv, str):
        try:
            sv = json.loads(sv)
        except (json.JSONDecodeError, TypeError):
            sv = {}

    notional = float(sv.get("notional", 0))
    if notional <= 0:
        return None

    direction_str = sv.get("direction", "")
    if "CALL" in direction_str.upper() or "BULL" in direction_str.upper():
        direction = "inflow"
    elif "PUT" in direction_str.upper() or "BEAR" in direction_str.upper():
        direction = "outflow"
    else:
        direction = _direction_from_signal(row.get("signal_type", ""))

    return {
        "source_type": "options_flow",
        "actor_name": row.get("source_id", ""),
        "ticker": row.get("ticker"),
        "amount_usd": notional,
        "direction": direction,
        "confidence": "confirmed",
        "evidence": {
            "notional_premium": notional,
            "oi_ratio": sv.get("oi_ratio"),
            "volume_ratio": sv.get("volume_ratio"),
            "signals": sv.get("signals"),
            "signal_type": row.get("signal_type"),
        },
        "flow_date": row.get("signal_date"),
    }


def _normalize_prediction_market(row: dict) -> dict | None:
    """Normalize a prediction market signal to estimated USD.

    Prediction markets report probability shifts, not direct dollar amounts.
    We estimate the implied capital flow from the probability shift magnitude.
    A 10pp shift on a liquid market implies ~$1M directional capital.

    Parameters:
        row: Dict with signal_sources columns.

    Returns:
        Normalized flow dict or None.
    """
    sv = row.get("signal_value") or {}
    if isinstance(sv, str):
        try:
            sv = json.loads(sv)
        except (json.JSONDecodeError, TypeError):
            sv = {}

    shift = abs(float(sv.get("shift", 0)))
    if shift <= 0:
        return None

    # Estimate: 10pp shift ~ $1M implied capital (rough Polymarket heuristic)
    amount = shift * 100_000

    direction_str = sv.get("direction", "")
    if "UP" in direction_str.upper() or "BULL" in direction_str.upper():
        direction = "inflow"
    elif "DOWN" in direction_str.upper() or "BEAR" in direction_str.upper():
        direction = "outflow"
    else:
        direction = "inflow"

    return {
        "source_type": "prediction_market",
        "actor_name": row.get("source_id", ""),
        "ticker": row.get("ticker"),
        "amount_usd": amount,
        "direction": direction,
        "confidence": "estimated",
        "evidence": {
            "shift_pct": shift,
            "current_prob": sv.get("current_prob"),
            "title": sv.get("title"),
            "signal_type": row.get("signal_type"),
        },
        "flow_date": row.get("signal_date"),
    }


# ── 13F + ETF flow normalization (from raw_series) ───────────────────────

def _normalize_13f_flows(engine: Engine, days: int) -> list[dict]:
    """Extract 13F position changes from raw_series and normalize to USD.

    Parameters:
        engine: SQLAlchemy engine.
        days: Lookback window in days.

    Returns:
        List of normalized flow dicts.
    """
    cutoff = date.today() - timedelta(days=days)
    flows: list[dict] = []

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT series_id, obs_date, value, raw_payload "
                    "FROM raw_series "
                    "WHERE series_id LIKE :pattern "
                    "AND obs_date >= :cutoff "
                    "ORDER BY obs_date DESC"
                ),
                {"pattern": "13F:%", "cutoff": cutoff},
            ).fetchall()

        for row in rows:
            series_id = row[0]
            obs_date = row[1]
            value_usd = float(row[2]) if row[2] else 0.0
            payload = row[3] or {}
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except (json.JSONDecodeError, TypeError):
                    payload = {}

            if abs(value_usd) <= 0:
                continue

            # Parse series_id: 13F:{cik}:{cusip}:{action}
            parts = series_id.split(":")
            action = parts[3] if len(parts) > 3 else "UNKNOWN"
            manager_name = payload.get("manager_name", parts[1] if len(parts) > 1 else "")

            if action in ("NEW", "INCREASED"):
                direction = "inflow"
            elif action in ("CLOSED", "DECREASED"):
                direction = "outflow"
            else:
                direction = "inflow"

            flows.append({
                "source_type": "13f",
                "actor_name": manager_name,
                "ticker": payload.get("issuer_name", ""),
                "amount_usd": abs(value_usd),
                "direction": direction,
                "confidence": "confirmed",
                "evidence": {
                    "cusip": payload.get("cusip"),
                    "action": action,
                    "pct_change": payload.get("pct_change"),
                    "shares": payload.get("shares"),
                    "filing_accession": payload.get("filing_accession"),
                },
                "flow_date": obs_date,
            })
    except Exception as exc:
        log.warning("13F flow normalization failed: {e}", e=str(exc))

    return flows


def _normalize_etf_flows(engine: Engine, days: int) -> list[dict]:
    """Extract ETF flow data from raw_series and normalize to USD.

    Parameters:
        engine: SQLAlchemy engine.
        days: Lookback window in days.

    Returns:
        List of normalized flow dicts.
    """
    cutoff = date.today() - timedelta(days=days)
    flows: list[dict] = []

    try:
        with engine.connect() as conn:
            # ETF_FLOW:{ticker}:5d series contains 5-day rolling $ flows
            rows = conn.execute(
                text(
                    "SELECT series_id, obs_date, value, raw_payload "
                    "FROM raw_series "
                    "WHERE series_id LIKE :pattern "
                    "AND obs_date >= :cutoff "
                    "ORDER BY obs_date DESC"
                ),
                {"pattern": "ETF_FLOW:%:5d", "cutoff": cutoff},
            ).fetchall()

        for row in rows:
            series_id = row[0]
            obs_date = row[1]
            value = float(row[2]) if row[2] else 0.0
            payload = row[3] or {}
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except (json.JSONDecodeError, TypeError):
                    payload = {}

            if abs(value) <= 0:
                continue

            # Parse series_id: ETF_FLOW:{ticker}:5d
            # Note: ETF 5d series is total $ volume, not net flow.
            # Use a fraction (~2%) as estimated net flow; direction from
            # price change would be more accurate but volume is a proxy.
            parts = series_id.split(":")
            ticker = parts[1] if len(parts) > 1 else ""
            estimated_net = value * 0.02  # ~2% of volume is estimated net flow

            flows.append({
                "source_type": "etf_flow",
                "actor_name": f"ETF:{ticker}",
                "ticker": ticker,
                "amount_usd": abs(estimated_net),
                "direction": "inflow",  # volume-based proxy; needs price direction for accuracy
                "confidence": "estimated",
                "evidence": {
                    "series_id": series_id,
                    "raw_value": value,
                },
                "flow_date": obs_date,
            })
    except Exception as exc:
        log.warning("ETF flow normalization failed: {e}", e=str(exc))

    return flows


# ── Core API ──────────────────────────────────────────────────────────────

def normalize_all_flows(engine: Engine, days: int = 90) -> list[dict]:
    """Scan signal_sources + raw_series, normalize all signals to USD, and persist.

    Parameters:
        engine: SQLAlchemy engine.
        days: Lookback window in days (default 90).

    Returns:
        List of all normalized flow dicts.
    """
    _ensure_table(engine)
    cutoff = date.today() - timedelta(days=days)
    all_flows: list[dict] = []

    # 1. Normalize signal_sources rows
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT source_type, source_id, ticker, signal_date, "
                    "signal_type, signal_value "
                    "FROM signal_sources "
                    "WHERE signal_date >= :cutoff "
                    "ORDER BY signal_date DESC"
                ),
                {"cutoff": cutoff},
            ).fetchall()
    except Exception as exc:
        log.warning("Failed to fetch signal_sources: {e}", e=str(exc))
        rows = []

    for row in rows:
        row_dict = {
            "source_type": row[0],
            "source_id": row[1],
            "ticker": row[2],
            "signal_date": row[3],
            "signal_type": row[4],
            "signal_value": row[5],
        }

        normalized = None
        stype = row_dict["source_type"]

        if stype == "congressional":
            normalized = _normalize_congressional(row_dict)
        elif stype == "insider":
            normalized = _normalize_insider(row_dict)
        elif stype == "darkpool":
            normalized = _normalize_darkpool(row_dict, engine)
        elif stype == "options_flow":
            normalized = _normalize_whale_options(row_dict)
        elif stype == "prediction_market":
            normalized = _normalize_prediction_market(row_dict)

        if normalized:
            all_flows.append(normalized)

    # 2. Normalize raw_series-based flows (13F + ETF)
    all_flows.extend(_normalize_13f_flows(engine, days))
    all_flows.extend(_normalize_etf_flows(engine, days))

    # 3. Persist to dollar_flows table
    _persist_flows(engine, all_flows)

    log.info(
        "Normalized {n} dollar flows across {d} days",
        n=len(all_flows),
        d=days,
    )
    return all_flows


def _persist_flows(engine: Engine, flows: list[dict]) -> int:
    """Write normalized flows to the dollar_flows table.

    Uses upsert-like logic: clears existing rows for the date range
    covered by the new flows, then bulk inserts.

    Parameters:
        engine: SQLAlchemy engine.
        flows: List of normalized flow dicts.

    Returns:
        Number of rows inserted.
    """
    if not flows:
        return 0

    inserted = 0
    with engine.begin() as conn:
        # Determine date range
        dates = [f["flow_date"] for f in flows if f.get("flow_date")]
        if not dates:
            return 0

        min_date = min(dates)
        max_date = max(dates)

        # Clear stale rows for this date range
        conn.execute(
            text(
                "DELETE FROM dollar_flows "
                "WHERE flow_date >= :min_date AND flow_date <= :max_date"
            ),
            {"min_date": min_date, "max_date": max_date},
        )

        # Bulk insert
        for f in flows:
            if not f.get("flow_date") or not f.get("amount_usd"):
                continue
            try:
                conn.execute(
                    text(
                        "INSERT INTO dollar_flows "
                        "(source_type, actor_name, ticker, amount_usd, "
                        "direction, confidence, evidence, flow_date) "
                        "VALUES (:stype, :actor, :ticker, :amount, "
                        ":direction, :confidence, :evidence, :fdate)"
                    ),
                    {
                        "stype": f["source_type"],
                        "actor": f.get("actor_name"),
                        "ticker": f.get("ticker"),
                        "amount": f["amount_usd"],
                        "direction": f["direction"],
                        "confidence": f.get("confidence", "estimated"),
                        "evidence": json.dumps(f.get("evidence", {})),
                        "fdate": f["flow_date"],
                    },
                )
                inserted += 1
            except Exception as exc:
                log.debug("Failed to insert flow: {e}", e=str(exc))

    log.info("Persisted {n} dollar_flows rows", n=inserted)
    return inserted


def get_flows_by_ticker(engine: Engine, ticker: str, days: int = 30) -> list[dict]:
    """Retrieve normalized dollar flows for a specific ticker.

    Parameters:
        engine: SQLAlchemy engine.
        ticker: Stock ticker to filter by.
        days: Lookback window in days (default 30).

    Returns:
        List of flow dicts sorted by flow_date descending.
    """
    _ensure_table(engine)
    cutoff = date.today() - timedelta(days=days)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT id, source_type, actor_name, ticker, amount_usd, "
                "direction, confidence, evidence, flow_date, created_at "
                "FROM dollar_flows "
                "WHERE UPPER(ticker) = :ticker "
                "AND flow_date >= :cutoff "
                "ORDER BY flow_date DESC"
            ),
            {"ticker": ticker.upper(), "cutoff": cutoff},
        ).fetchall()

    return [
        {
            "id": r[0],
            "source_type": r[1],
            "actor_name": r[2],
            "ticker": r[3],
            "amount_usd": float(r[4]) if r[4] else 0.0,
            "direction": r[5],
            "confidence": r[6],
            "evidence": r[7] if isinstance(r[7], dict) else {},
            "flow_date": r[8].isoformat() if r[8] else None,
            "created_at": r[9].isoformat() if r[9] else None,
        }
        for r in rows
    ]


def get_flows_by_sector(engine: Engine, sector: str, days: int = 30) -> list[dict]:
    """Retrieve normalized dollar flows for all tickers in a sector.

    Parameters:
        engine: SQLAlchemy engine.
        sector: Sector name (e.g., 'Technology', 'Financials').
        days: Lookback window in days (default 30).

    Returns:
        List of flow dicts for all tickers in the sector.
    """
    _ensure_table(engine)
    sector_lookup = _build_sector_lookup()

    # Collect all tickers belonging to this sector
    sector_tickers = [
        t for t, s in sector_lookup.items()
        if s.lower() == sector.lower()
    ]

    if not sector_tickers:
        return []

    cutoff = date.today() - timedelta(days=days)

    # Use parameterized IN clause
    placeholders = ", ".join(f":t{i}" for i in range(len(sector_tickers)))
    params: dict[str, Any] = {"cutoff": cutoff}
    for i, t in enumerate(sector_tickers):
        params[f"t{i}"] = t

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT id, source_type, actor_name, ticker, amount_usd, "
                f"direction, confidence, evidence, flow_date, created_at "
                f"FROM dollar_flows "
                f"WHERE UPPER(ticker) IN ({placeholders}) "
                f"AND flow_date >= :cutoff "
                f"ORDER BY flow_date DESC"
            ),
            params,
        ).fetchall()

    return [
        {
            "id": r[0],
            "source_type": r[1],
            "actor_name": r[2],
            "ticker": r[3],
            "amount_usd": float(r[4]) if r[4] else 0.0,
            "direction": r[5],
            "confidence": r[6],
            "evidence": r[7] if isinstance(r[7], dict) else {},
            "flow_date": r[8].isoformat() if r[8] else None,
            "created_at": r[9].isoformat() if r[9] else None,
            "sector": sector,
        }
        for r in rows
    ]


def get_aggregate_flows(engine: Engine, days: int = 30) -> dict[str, Any]:
    """Compute aggregate net flows per sector and per actor tier.

    Parameters:
        engine: SQLAlchemy engine.
        days: Lookback window in days (default 30).

    Returns:
        Dict with 'by_sector', 'by_source_type', 'by_actor' aggregations
        and 'total_inflow', 'total_outflow', 'net_flow'.
    """
    _ensure_table(engine)
    cutoff = date.today() - timedelta(days=days)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT source_type, actor_name, ticker, amount_usd, signal_type "
                "FROM dollar_flows "
                "WHERE flow_date >= :cutoff"
            ),
            {"cutoff": cutoff},
        ).fetchall()

    sector_flows: dict[str, dict[str, float]] = defaultdict(
        lambda: {"inflow": 0.0, "outflow": 0.0}
    )
    source_type_flows: dict[str, dict[str, float]] = defaultdict(
        lambda: {"inflow": 0.0, "outflow": 0.0}
    )
    actor_flows: dict[str, dict[str, float]] = defaultdict(
        lambda: {"inflow": 0.0, "outflow": 0.0}
    )

    total_inflow = 0.0
    total_outflow = 0.0

    for r in rows:
        source_type = r[0]
        actor_name = r[1] or "Unknown"
        ticker = r[2]
        amount = float(r[3]) if r[3] else 0.0
        direction = r[4]

        sector = _get_sector(ticker)

        if direction == "inflow":
            total_inflow += amount
            sector_flows[sector]["inflow"] += amount
            source_type_flows[source_type]["inflow"] += amount
            actor_flows[actor_name]["inflow"] += amount
        else:
            total_outflow += amount
            sector_flows[sector]["outflow"] += amount
            source_type_flows[source_type]["outflow"] += amount
            actor_flows[actor_name]["outflow"] += amount

    # Compute net flows
    def _net(d: dict[str, float]) -> dict[str, float]:
        return {
            "inflow": d["inflow"],
            "outflow": d["outflow"],
            "net": d["inflow"] - d["outflow"],
        }

    # Sort actors by absolute net flow, take top 20
    sorted_actors = sorted(
        actor_flows.items(),
        key=lambda x: abs(x[1]["inflow"] - x[1]["outflow"]),
        reverse=True,
    )[:20]

    return {
        "by_sector": {k: _net(v) for k, v in sector_flows.items()},
        "by_source_type": {k: _net(v) for k, v in source_type_flows.items()},
        "by_actor": {k: _net(v) for k, v in sorted_actors},
        "total_inflow": total_inflow,
        "total_outflow": total_outflow,
        "net_flow": total_inflow - total_outflow,
        "days": days,
        "flow_count": len(rows),
    }


def get_biggest_movers(engine: Engine, days: int = 7) -> list[dict]:
    """Return top 10 flows by absolute dollar amount.

    Parameters:
        engine: SQLAlchemy engine.
        days: Lookback window in days (default 7).

    Returns:
        List of the 10 largest flows by amount_usd.
    """
    _ensure_table(engine)
    cutoff = date.today() - timedelta(days=days)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT id, source_type, actor_name, ticker, amount_usd, "
                "direction, confidence, evidence, flow_date "
                "FROM dollar_flows "
                "WHERE flow_date >= :cutoff "
                "ORDER BY amount_usd DESC "
                "LIMIT 10"
            ),
            {"cutoff": cutoff},
        ).fetchall()

    return [
        {
            "id": r[0],
            "source_type": r[1],
            "actor_name": r[2],
            "ticker": r[3],
            "amount_usd": float(r[4]) if r[4] else 0.0,
            "direction": r[5],
            "confidence": r[6],
            "evidence": r[7] if isinstance(r[7], dict) else {},
            "flow_date": r[8].isoformat() if r[8] else None,
            "sector": _get_sector(r[3]),
        }
        for r in rows
    ]
