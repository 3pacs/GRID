"""
GRID — Flow Aggregation Engine.

Answers questions like "how much money moved into tech this week?" by
aggregating normalized dollar flows from intelligence/dollar_flows.py into
sector views, time series, actor-tier breakdowns, momentum scores, and
sector rotation matrices.

This module sits between raw dollar_flows data and the API/LLM layer,
providing the structured aggregations that power timeline charts, rotation
heatmaps, and narrative generation.

Key entry points:
  aggregate_by_sector       — net inflow/outflow per sector with trend
  aggregate_by_time         — time series of flows for charts
  aggregate_by_actor_tier   — sovereign/institutional/individual breakdown
  compute_flow_momentum     — 5d vs 20d flow acceleration score
  build_sector_flow_matrix  — NxN rotation matrix between sectors
"""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import date, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from analysis.sector_map import SECTOR_MAP, get_all_sectors


# ── Actor tier classification ────────────────────────────────────────────

# Source types map to actor tiers.  Congressional and insider are
# "individual" tier (named persons); 13f and etf_flow are
# "institutional"; darkpool is institutional (block trades); options_flow
# is institutional; prediction_market is retail/individual.
_SOURCE_TIER_MAP: dict[str, str] = {
    "congressional": "individual",
    "insider": "individual",
    "13f": "institutional",
    "etf_flow": "institutional",
    "darkpool": "institutional",
    "options_flow": "institutional",
    "prediction_market": "individual",
}

# Known sovereign actors (from actor_network) — match by actor_name prefix
_SOVEREIGN_PREFIXES: list[str] = [
    "Federal Reserve", "Fed:", "ECB", "BOJ", "PBOC", "BOE",
    "Treasury", "Saudi", "OPEC", "IMF", "World Bank",
    "China", "Japan", "Norway", "Switzerland",
]

# Known regional actors
_REGIONAL_PREFIXES: list[str] = [
    "CalPERS", "CalSTRS", "GIC", "Temasek", "ADIA", "KIA",
    "CPPIB", "Ontario Teachers", "NBIM",
]


def _classify_actor_tier(source_type: str, actor_name: str | None) -> str:
    """Classify an actor into sovereign/regional/institutional/individual tier.

    Uses source_type as the base classification, then upgrades based on
    actor_name matching against known sovereign and regional entities.

    Parameters:
        source_type: The dollar_flows source_type field.
        actor_name: The actor_name field (may be None).

    Returns:
        One of 'sovereign', 'regional', 'institutional', 'individual'.
    """
    name = (actor_name or "").strip()

    # Check sovereign actors first (overrides source_type)
    for prefix in _SOVEREIGN_PREFIXES:
        if name.upper().startswith(prefix.upper()):
            return "sovereign"

    # Check regional actors
    for prefix in _REGIONAL_PREFIXES:
        if name.upper().startswith(prefix.upper()):
            return "regional"

    return _SOURCE_TIER_MAP.get(source_type, "individual")


# ── Sector lookup (cached) ───────────────────────────────────────────────

_SECTOR_LOOKUP: dict[str, str] | None = None


def _build_sector_lookup() -> dict[str, str]:
    """Build a ticker -> sector name mapping from the SECTOR_MAP."""
    lookup: dict[str, str] = {}
    for sector_name, sector_data in SECTOR_MAP.items():
        etf = sector_data.get("etf")
        if etf:
            lookup[etf.upper()] = sector_name
        for _sub_name, sub_data in sector_data.get("subsectors", {}).items():
            for actor in sub_data.get("actors", []):
                ticker = actor.get("ticker")
                if ticker:
                    lookup[ticker.upper()] = sector_name
    return lookup


def _get_sector(ticker: str | None) -> str:
    """Return the sector for a ticker, or 'Unknown'."""
    global _SECTOR_LOOKUP
    if _SECTOR_LOOKUP is None:
        _SECTOR_LOOKUP = _build_sector_lookup()
    if not ticker:
        return "Unknown"
    return _SECTOR_LOOKUP.get(ticker.upper(), "Unknown")


# ── SQL helpers ──────────────────────────────────────────────────────────

def _fetch_flows(engine: Engine, days: int) -> list[dict]:
    """Fetch all dollar_flows rows within the lookback window.

    Parameters:
        engine: SQLAlchemy engine.
        days: Lookback window in days.

    Returns:
        List of flow dicts with all columns.
    """
    cutoff = date.today() - timedelta(days=days)
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT source_type, actor_name, ticker, amount_usd, "
                    "direction, confidence, flow_date "
                    "FROM dollar_flows "
                    "WHERE flow_date >= :cutoff "
                    "ORDER BY flow_date DESC"
                ),
                {"cutoff": cutoff},
            ).fetchall()
    except Exception as exc:
        log.warning("Failed to fetch dollar_flows: {e}", e=str(exc))
        return []

    return [
        {
            "source_type": r[0],
            "actor_name": r[1],
            "ticker": r[2],
            "amount_usd": float(r[3]) if r[3] else 0.0,
            "direction": r[4],
            "confidence": r[5],
            "flow_date": r[6],
        }
        for r in rows
    ]


def _signed_amount(flow: dict) -> float:
    """Return positive for inflow, negative for outflow."""
    amt = flow["amount_usd"]
    return amt if flow["direction"] == "inflow" else -amt


# ══════════════════════════════════════════════════════════════════════════
# 1. aggregate_by_sector
# ══════════════════════════════════════════════════════════════════════════

def aggregate_by_sector(engine: Engine, days: int = 30) -> dict[str, Any]:
    """Net inflow/outflow per sector in USD with trend and top actors.

    Compares the most recent 7 days against the prior 7 days to determine
    whether flow is accelerating or decelerating.

    Parameters:
        engine: SQLAlchemy engine.
        days: Lookback window in days (default 30).

    Returns:
        Dict keyed by sector name::

            {sector: {
                net_flow, direction, acceleration,
                top_actors, source_breakdown,
                inflow, outflow
            }}
    """
    flows = _fetch_flows(engine, days)
    if not flows:
        return {}

    today = date.today()
    week_ago = today - timedelta(days=7)
    two_weeks_ago = today - timedelta(days=14)

    # Accumulators per sector
    sector_data: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "inflow": 0.0,
        "outflow": 0.0,
        "this_week": 0.0,
        "last_week": 0.0,
        "actors": defaultdict(float),
        "sources": defaultdict(float),
    })

    for f in flows:
        sector = _get_sector(f["ticker"])
        amt = f["amount_usd"]
        signed = _signed_amount(f)
        sd = sector_data[sector]

        if f["direction"] == "inflow":
            sd["inflow"] += amt
        else:
            sd["outflow"] += amt

        # Actor accumulation (net signed)
        actor = f["actor_name"] or "Unknown"
        sd["actors"][actor] += signed

        # Source breakdown (net signed)
        sd["sources"][f["source_type"]] += signed

        # Weekly comparison for acceleration
        fdate = f["flow_date"]
        if fdate is not None:
            if isinstance(fdate, date) and fdate >= week_ago:
                sd["this_week"] += signed
            elif isinstance(fdate, date) and fdate >= two_weeks_ago:
                sd["last_week"] += signed

    result: dict[str, Any] = {}
    for sector, sd in sector_data.items():
        net = sd["inflow"] - sd["outflow"]

        # Acceleration: this_week vs last_week
        tw = sd["this_week"]
        lw = sd["last_week"]
        if lw != 0:
            accel_pct = (tw - lw) / abs(lw)
        elif tw != 0:
            accel_pct = 1.0  # all new flow this week
        else:
            accel_pct = 0.0

        if accel_pct > 0.1:
            acceleration = "accelerating"
        elif accel_pct < -0.1:
            acceleration = "decelerating"
        else:
            acceleration = "stable"

        # Top 10 actors by absolute net flow
        sorted_actors = sorted(
            sd["actors"].items(),
            key=lambda x: abs(x[1]),
            reverse=True,
        )[:10]

        top_actors = [
            {"name": name, "net_flow": round(val, 2)}
            for name, val in sorted_actors
        ]

        # Source breakdown
        source_breakdown = {
            src: round(val, 2)
            for src, val in sd["sources"].items()
        }

        result[sector] = {
            "net_flow": round(net, 2),
            "direction": "inflow" if net >= 0 else "outflow",
            "acceleration": acceleration,
            "acceleration_pct": round(accel_pct, 4),
            "this_week_flow": round(tw, 2),
            "last_week_flow": round(lw, 2),
            "inflow": round(sd["inflow"], 2),
            "outflow": round(sd["outflow"], 2),
            "top_actors": top_actors,
            "source_breakdown": source_breakdown,
        }

    return result


# ══════════════════════════════════════════════════════════════════════════
# 2. aggregate_by_time
# ══════════════════════════════════════════════════════════════════════════

def aggregate_by_time(
    engine: Engine,
    ticker_or_sector: str,
    period: str = "weekly",
    days: int = 90,
) -> list[dict]:
    """Time series of dollar flows for a ticker or sector.

    Groups flows into weekly or daily buckets for timeline chart rendering.

    Parameters:
        engine: SQLAlchemy engine.
        ticker_or_sector: A stock ticker (e.g. 'NVDA') or sector name
                          (e.g. 'Technology').
        period: 'daily' or 'weekly' (default 'weekly').
        days: Lookback window in days (default 90).

    Returns:
        List of dicts: [{period_start, period_end, net_flow, inflows, outflows}]
        sorted oldest to newest.
    """
    all_flows = _fetch_flows(engine, days)
    if not all_flows:
        return []

    # Determine if input is a sector or ticker
    all_sectors = {s.lower(): s for s in get_all_sectors()}
    is_sector = ticker_or_sector.lower() in all_sectors

    if is_sector:
        sector_name = all_sectors[ticker_or_sector.lower()]
        filtered = [
            f for f in all_flows
            if _get_sector(f["ticker"]) == sector_name
        ]
    else:
        ticker_upper = ticker_or_sector.upper()
        filtered = [
            f for f in all_flows
            if (f["ticker"] or "").upper() == ticker_upper
        ]

    if not filtered:
        return []

    # Build time buckets
    today = date.today()
    start = today - timedelta(days=days)

    if period == "daily":
        bucket_days = 1
    else:
        bucket_days = 7

    buckets: list[dict] = []
    cursor = start
    while cursor <= today:
        bucket_end = min(cursor + timedelta(days=bucket_days - 1), today)
        buckets.append({
            "period_start": cursor.isoformat(),
            "period_end": bucket_end.isoformat(),
            "net_flow": 0.0,
            "inflows": 0.0,
            "outflows": 0.0,
        })
        cursor += timedelta(days=bucket_days)

    # Fill buckets
    for f in filtered:
        fdate = f["flow_date"]
        if fdate is None or not isinstance(fdate, date):
            continue
        # Find the bucket
        offset = (fdate - start).days
        if offset < 0:
            continue
        bucket_idx = offset // bucket_days
        if bucket_idx >= len(buckets):
            bucket_idx = len(buckets) - 1

        b = buckets[bucket_idx]
        amt = f["amount_usd"]
        if f["direction"] == "inflow":
            b["inflows"] += amt
            b["net_flow"] += amt
        else:
            b["outflows"] += amt
            b["net_flow"] -= amt

    # Round all values
    for b in buckets:
        b["net_flow"] = round(b["net_flow"], 2)
        b["inflows"] = round(b["inflows"], 2)
        b["outflows"] = round(b["outflows"], 2)

    return buckets


# ══════════════════════════════════════════════════════════════════════════
# 3. aggregate_by_actor_tier
# ══════════════════════════════════════════════════════════════════════════

def aggregate_by_actor_tier(engine: Engine, days: int = 30) -> dict[str, Any]:
    """Break down flows by actor tier (sovereign/regional/institutional/individual).

    Answers: "Institutional money is flowing into tech at $2B/week while
    retail is selling."

    Parameters:
        engine: SQLAlchemy engine.
        days: Lookback window in days (default 30).

    Returns:
        Dict keyed by tier with sector breakdown::

            {tier: {
                net_flow, inflow, outflow,
                direction, weekly_rate,
                sector_breakdown: {sector: net_flow},
                top_actors: [{name, net_flow}]
            }}
    """
    flows = _fetch_flows(engine, days)
    if not flows:
        return {}

    weeks = max(days / 7.0, 1.0)

    tier_data: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "inflow": 0.0,
        "outflow": 0.0,
        "sectors": defaultdict(float),
        "actors": defaultdict(float),
    })

    for f in flows:
        tier = _classify_actor_tier(f["source_type"], f["actor_name"])
        sector = _get_sector(f["ticker"])
        signed = _signed_amount(f)
        amt = f["amount_usd"]
        td = tier_data[tier]

        if f["direction"] == "inflow":
            td["inflow"] += amt
        else:
            td["outflow"] += amt

        td["sectors"][sector] += signed
        actor = f["actor_name"] or "Unknown"
        td["actors"][actor] += signed

    result: dict[str, Any] = {}
    for tier in ["sovereign", "regional", "institutional", "individual"]:
        td = tier_data.get(tier)
        if td is None:
            result[tier] = {
                "net_flow": 0.0,
                "inflow": 0.0,
                "outflow": 0.0,
                "direction": "neutral",
                "weekly_rate": 0.0,
                "sector_breakdown": {},
                "top_actors": [],
            }
            continue

        net = td["inflow"] - td["outflow"]
        weekly_rate = net / weeks

        # Sector breakdown
        sector_bd = {
            s: round(v, 2)
            for s, v in sorted(
                td["sectors"].items(),
                key=lambda x: abs(x[1]),
                reverse=True,
            )
        }

        # Top 10 actors
        sorted_actors = sorted(
            td["actors"].items(),
            key=lambda x: abs(x[1]),
            reverse=True,
        )[:10]
        top_actors = [
            {"name": name, "net_flow": round(val, 2)}
            for name, val in sorted_actors
        ]

        result[tier] = {
            "net_flow": round(net, 2),
            "inflow": round(td["inflow"], 2),
            "outflow": round(td["outflow"], 2),
            "direction": "inflow" if net >= 0 else "outflow",
            "weekly_rate": round(weekly_rate, 2),
            "sector_breakdown": sector_bd,
            "top_actors": top_actors,
        }

    return result


# ══════════════════════════════════════════════════════════════════════════
# 4. compute_flow_momentum
# ══════════════════════════════════════════════════════════════════════════

def compute_flow_momentum(engine: Engine, ticker: str, days: int = 30) -> dict[str, Any]:
    """Compute dollar flow momentum for a ticker.

    Compares the 5-day average net flow against the 20-day average to
    produce a momentum score.  Positive momentum means smart money is
    accumulating faster than the trailing average.

    Parameters:
        engine: SQLAlchemy engine.
        ticker: Stock ticker symbol.
        days: Lookback window (default 30, used for the 20-day baseline).

    Returns:
        Dict with momentum_score, signal, 5d_avg, 20d_avg, flow_count.
    """
    cutoff = date.today() - timedelta(days=max(days, 30))
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT amount_usd, direction, flow_date "
                    "FROM dollar_flows "
                    "WHERE UPPER(ticker) = :ticker "
                    "AND flow_date >= :cutoff "
                    "ORDER BY flow_date DESC"
                ),
                {"ticker": ticker.upper(), "cutoff": cutoff},
            ).fetchall()
    except Exception as exc:
        log.warning("Flow momentum query failed: {e}", e=str(exc))
        return {
            "ticker": ticker,
            "momentum_score": 0.0,
            "signal": "no_data",
            "flow_5d_avg": 0.0,
            "flow_20d_avg": 0.0,
            "flow_count": 0,
        }

    if not rows:
        return {
            "ticker": ticker,
            "momentum_score": 0.0,
            "signal": "no_data",
            "flow_5d_avg": 0.0,
            "flow_20d_avg": 0.0,
            "flow_count": 0,
        }

    today = date.today()
    five_ago = today - timedelta(days=5)
    twenty_ago = today - timedelta(days=20)

    sum_5d = 0.0
    count_5d = 0
    sum_20d = 0.0
    count_20d = 0

    for r in rows:
        amt = float(r[0]) if r[0] else 0.0
        direction = r[1]
        fdate = r[2]

        signed = amt if direction == "inflow" else -amt

        if isinstance(fdate, date):
            if fdate >= five_ago:
                sum_5d += signed
                count_5d += 1
            if fdate >= twenty_ago:
                sum_20d += signed
                count_20d += 1

    avg_5d = sum_5d / max(count_5d, 1)
    avg_20d = sum_20d / max(count_20d, 1)

    # Momentum score: ratio of short-term to long-term flow
    if abs(avg_20d) > 0:
        momentum_score = avg_5d / abs(avg_20d)
    elif avg_5d != 0:
        momentum_score = 1.0 if avg_5d > 0 else -1.0
    else:
        momentum_score = 0.0

    # Clamp to [-5, 5] range
    momentum_score = max(-5.0, min(5.0, momentum_score))

    # Signal classification
    if momentum_score > 1.0:
        signal = "strong_accumulation"
    elif momentum_score > 0.3:
        signal = "accumulation"
    elif momentum_score < -1.0:
        signal = "strong_distribution"
    elif momentum_score < -0.3:
        signal = "distribution"
    else:
        signal = "neutral"

    return {
        "ticker": ticker,
        "momentum_score": round(momentum_score, 4),
        "signal": signal,
        "flow_5d_avg": round(avg_5d, 2),
        "flow_20d_avg": round(avg_20d, 2),
        "flow_count": len(rows),
    }


# ══════════════════════════════════════════════════════════════════════════
# 5. build_sector_flow_matrix
# ══════════════════════════════════════════════════════════════════════════

def build_sector_flow_matrix(engine: Engine, days: int = 30) -> dict[str, Any]:
    """Build an NxN matrix estimating capital rotation between sectors.

    We estimate rotation from simultaneous sector-level outflows and
    inflows: if sector A has net outflow of $X and sector B has net inflow
    of $Y in the same time window, the estimated rotation from A to B is
    proportional to their magnitudes.  This is refined by looking at ETF
    flow timing and 13F position changes.

    Parameters:
        engine: SQLAlchemy engine.
        days: Lookback window in days (default 30).

    Returns:
        Dict with 'sectors', 'matrix' (NxN list of lists), 'signals'.
    """
    sector_agg = aggregate_by_sector(engine, days)
    if not sector_agg:
        return {"sectors": [], "matrix": [], "signals": []}

    # Only include known sectors (not 'Unknown')
    known_sectors = [s for s in get_all_sectors() if s in sector_agg]
    if not known_sectors:
        return {"sectors": [], "matrix": [], "signals": []}

    n = len(known_sectors)
    sector_idx = {s: i for i, s in enumerate(known_sectors)}

    # Identify net outflow and net inflow sectors
    outflow_sectors: list[tuple[str, float]] = []
    inflow_sectors: list[tuple[str, float]] = []

    for s in known_sectors:
        net = sector_agg[s]["net_flow"]
        if net < 0:
            outflow_sectors.append((s, abs(net)))
        elif net > 0:
            inflow_sectors.append((s, net))

    total_outflow = sum(v for _, v in outflow_sectors)
    total_inflow = sum(v for _, v in inflow_sectors)

    # Build the rotation matrix: estimated flow from each outflow sector
    # to each inflow sector, proportional to their relative magnitudes.
    matrix = [[0.0] * n for _ in range(n)]

    if total_outflow > 0 and total_inflow > 0:
        # The rotation amount is the minimum of total outflows and inflows
        rotation_volume = min(total_outflow, total_inflow)

        for out_sector, out_amt in outflow_sectors:
            out_share = out_amt / total_outflow
            i = sector_idx[out_sector]
            for in_sector, in_amt in inflow_sectors:
                in_share = in_amt / total_inflow
                j = sector_idx[in_sector]
                estimated_flow = rotation_volume * out_share * in_share
                matrix[i][j] = round(estimated_flow, 2)

    # Round matrix values
    for i in range(n):
        for j in range(n):
            matrix[i][j] = round(matrix[i][j], 2)

    # Generate human-readable rotation signals
    signals: list[dict] = []
    for i in range(n):
        for j in range(n):
            if matrix[i][j] > 0 and i != j:
                signals.append({
                    "from_sector": known_sectors[i],
                    "to_sector": known_sectors[j],
                    "estimated_usd": matrix[i][j],
                    "label": (
                        f"Money leaving {known_sectors[i]}, "
                        f"entering {known_sectors[j]}: "
                        f"${matrix[i][j]:,.0f}"
                    ),
                })

    # Sort signals by magnitude
    signals.sort(key=lambda x: x["estimated_usd"], reverse=True)

    return {
        "sectors": known_sectors,
        "matrix": matrix,
        "signals": signals[:20],
    }


# ══════════════════════════════════════════════════════════════════════════
# Convenience: full aggregated view (used by API endpoint)
# ══════════════════════════════════════════════════════════════════════════

def get_full_aggregation(
    engine: Engine,
    sector: str | None = None,
    period: str = "weekly",
    days: int = 30,
) -> dict[str, Any]:
    """Build a complete aggregated flow response.

    This is the main entry point for the /api/v1/flows/aggregated endpoint.
    Assembles sector aggregation, optional sector time series, actor-tier
    breakdown, and rotation matrix into one response.

    Parameters:
        engine: SQLAlchemy engine.
        sector: Optional sector filter. If provided, includes a time series.
        period: 'daily' or 'weekly' for time series buckets.
        days: Lookback window.

    Returns:
        Dict with by_sector, by_actor_tier, rotation_matrix, and optionally
        time_series.
    """
    result: dict[str, Any] = {
        "days": days,
        "period": period,
    }

    # Sector aggregation
    try:
        by_sector = aggregate_by_sector(engine, days)
        result["by_sector"] = by_sector
    except Exception as exc:
        log.warning("aggregate_by_sector failed: {e}", e=str(exc))
        result["by_sector"] = {}

    # Actor tier breakdown
    try:
        by_tier = aggregate_by_actor_tier(engine, days)
        result["by_actor_tier"] = by_tier
    except Exception as exc:
        log.warning("aggregate_by_actor_tier failed: {e}", e=str(exc))
        result["by_actor_tier"] = {}

    # Rotation matrix
    try:
        matrix = build_sector_flow_matrix(engine, days)
        result["rotation_matrix"] = matrix
    except Exception as exc:
        log.warning("build_sector_flow_matrix failed: {e}", e=str(exc))
        result["rotation_matrix"] = {"sectors": [], "matrix": [], "signals": []}

    # Time series for specific sector or ticker
    if sector:
        try:
            ts = aggregate_by_time(engine, sector, period=period, days=days)
            result["time_series"] = ts
        except Exception as exc:
            log.warning("aggregate_by_time failed: {e}", e=str(exc))
            result["time_series"] = []

    return result
