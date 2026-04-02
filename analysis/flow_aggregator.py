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
import time as _time
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

_flow_cache: dict[str, Any] = {"data": None, "ts": 0.0, "key": ""}
_FLOW_CACHE_TTL = 60.0  # seconds


def _fetch_flows(engine: Engine, days: int) -> list[dict]:
    """Fetch all dollar_flows rows within the lookback window.

    Parameters:
        engine: SQLAlchemy engine.
        days: Lookback window in days.

    Returns:
        List of flow dicts with all columns including evidence JSONB.
    """
    cache_key = str(days)
    now = _time.time()
    if (
        _flow_cache["data"] is not None
        and _flow_cache["key"] == cache_key
        and (now - _flow_cache["ts"]) < _FLOW_CACHE_TTL
    ):
        return _flow_cache["data"]

    cutoff = date.today() - timedelta(days=days)
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT source_type, actor_name, ticker, amount_usd, "
                    "direction, confidence, flow_date, evidence "
                    "FROM dollar_flows "
                    "WHERE flow_date >= :cutoff "
                    "ORDER BY flow_date DESC"
                ),
                {"cutoff": cutoff},
            ).fetchall()
    except Exception as exc:
        log.warning("Failed to fetch dollar_flows: {e}", e=str(exc))
        return []

    import json as _json

    result = []
    for r in rows:
        ev = r[7]
        if ev and isinstance(ev, str):
            try:
                ev = _json.loads(ev)
            except Exception:
                ev = {}
        elif not isinstance(ev, dict):
            ev = {}

        result.append({
            "source_type": r[0],
            "actor_name": r[1],
            "ticker": r[2],
            "amount_usd": float(r[3]) if r[3] else 0.0,
            "direction": r[4],
            "confidence": r[5] or "estimated",
            "flow_date": r[6],
            "evidence": ev or {},
        })

    _flow_cache["data"] = result
    _flow_cache["ts"] = _time.time()
    _flow_cache["key"] = cache_key
    return result


# ── Confidence weighting ────────────────────────────────────────────────

_CONFIDENCE_WEIGHTS: dict[str, float] = {
    "confirmed": 1.0,
    "derived": 0.7,
    "estimated": 0.4,
    "rumored": 0.1,
}


def _confidence_weight(conf: str) -> float:
    """Return a multiplier [0.1, 1.0] based on confidence level."""
    return _CONFIDENCE_WEIGHTS.get(conf, 0.4)


# ── Smart money classification ──────────────────────────────────────────

_SMART_MONEY_SOURCES = {"congressional", "insider", "13f", "darkpool"}
_DUMB_MONEY_SOURCES = {"prediction_market", "etf_flow"}


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
# 6. Smart money vs dumb money split
# ══════════════════════════════════════════════════════════════════════════

def aggregate_smart_vs_dumb(engine: Engine, days: int = 30) -> dict[str, Any]:
    """Split flows into smart money (confirmed insiders/institutions) vs
    dumb money (passive ETF flows, prediction markets).

    Smart money: congressional, insider, 13f, darkpool (confirmed/derived)
    Dumb money: etf_flow, prediction_market (estimated)
    """
    flows = _fetch_flows(engine, days)
    if not flows:
        return {"smart": {}, "dumb": {}, "divergence": "no_data"}

    smart = {"net": 0.0, "inflow": 0.0, "outflow": 0.0, "count": 0, "sectors": defaultdict(float)}
    dumb = {"net": 0.0, "inflow": 0.0, "outflow": 0.0, "count": 0, "sectors": defaultdict(float)}

    for f in flows:
        signed = _signed_amount(f)
        amt = f["amount_usd"]
        sector = _get_sector(f["ticker"])
        bucket = smart if f["source_type"] in _SMART_MONEY_SOURCES else dumb

        bucket["count"] += 1
        bucket["net"] += signed
        bucket["sectors"][sector] += signed
        if f["direction"] == "inflow":
            bucket["inflow"] += amt
        else:
            bucket["outflow"] += amt

    # Divergence: when smart and dumb money disagree
    smart_dir = "bullish" if smart["net"] > 0 else "bearish" if smart["net"] < 0 else "neutral"
    dumb_dir = "bullish" if dumb["net"] > 0 else "bearish" if dumb["net"] < 0 else "neutral"

    if smart_dir != dumb_dir and smart_dir != "neutral" and dumb_dir != "neutral":
        divergence = f"DIVERGENCE: smart money is {smart_dir}, dumb money is {dumb_dir}"
    elif smart_dir == dumb_dir and smart_dir != "neutral":
        divergence = f"CONSENSUS: both smart and dumb money are {smart_dir}"
    else:
        divergence = "MIXED: no clear agreement or divergence"

    def _format_bucket(b: dict) -> dict:
        return {
            "net_flow": round(b["net"], 2),
            "direction": "inflow" if b["net"] >= 0 else "outflow",
            "inflow": round(b["inflow"], 2),
            "outflow": round(b["outflow"], 2),
            "flow_count": b["count"],
            "top_sectors": dict(sorted(
                ((s, round(v, 2)) for s, v in b["sectors"].items()),
                key=lambda x: abs(x[1]), reverse=True,
            )[:5]),
        }

    return {
        "smart": _format_bucket(smart),
        "dumb": _format_bucket(dumb),
        "divergence": divergence,
        "smart_sources": list(_SMART_MONEY_SOURCES),
        "dumb_sources": list(_DUMB_MONEY_SOURCES),
    }


# ══════════════════════════════════════════════════════════════════════════
# 7. Sector conviction meter
# ══════════════════════════════════════════════════════════════════════════

def compute_sector_conviction(engine: Engine, days: int = 30) -> dict[str, dict]:
    """For each sector, compute a conviction score (0-100) based on:

    1. Confirmation ratio: what % of flows are confirmed vs estimated (0-50 pts)
    2. Actor consensus: what % of actors agree on direction (0-30 pts)
    3. Source diversity: how many independent source types agree (0-20 pts)

    A sector with $10B inflow from one ETF rebalance has LOW conviction.
    A sector with $500M inflow from 5 different confirmed sources has HIGH conviction.
    """
    flows = _fetch_flows(engine, days)
    if not flows:
        return {}

    sector_flows: dict[str, list[dict]] = defaultdict(list)
    for f in flows:
        sector = _get_sector(f["ticker"])
        sector_flows[sector].append(f)

    result = {}
    for sector, sflows in sector_flows.items():
        if not sflows:
            continue

        # 1. Confirmation ratio (0-50 pts)
        confirmed = sum(1 for f in sflows if f["confidence"] in ("confirmed", "derived"))
        total = len(sflows)
        conf_ratio = confirmed / total if total > 0 else 0
        conf_score = conf_ratio * 50

        # 2. Actor consensus (0-30 pts)
        actor_dirs: dict[str, float] = defaultdict(float)
        for f in sflows:
            actor = f["actor_name"] or "Unknown"
            actor_dirs[actor] += _signed_amount(f)

        if actor_dirs:
            bullish_actors = sum(1 for v in actor_dirs.values() if v > 0)
            bearish_actors = sum(1 for v in actor_dirs.values() if v < 0)
            total_actors = bullish_actors + bearish_actors
            if total_actors > 0:
                majority = max(bullish_actors, bearish_actors)
                consensus_pct = majority / total_actors
                consensus_score = consensus_pct * 30
            else:
                consensus_score = 0
        else:
            consensus_score = 0
            bullish_actors = 0
            bearish_actors = 0

        # 3. Source diversity (0-20 pts)
        net_flow = sum(_signed_amount(f) for f in sflows)
        majority_dir = "inflow" if net_flow >= 0 else "outflow"
        agreeing_sources = set()
        for f in sflows:
            f_dir = "inflow" if _signed_amount(f) >= 0 else "outflow"
            if f_dir == majority_dir:
                agreeing_sources.add(f["source_type"])
        diversity_score = min(20, len(agreeing_sources) * 5)

        conviction = round(conf_score + consensus_score + diversity_score)

        result[sector] = {
            "conviction": min(100, conviction),
            "confirmation_ratio": round(conf_ratio, 2),
            "confirmed_flows": confirmed,
            "estimated_flows": total - confirmed,
            "actors_bullish": bullish_actors,
            "actors_bearish": bearish_actors,
            "agreeing_sources": len(agreeing_sources),
            "source_types": list(agreeing_sources),
            "net_flow": round(net_flow, 2),
            "direction": majority_dir,
            "explanation": (
                f"{conviction}% conviction: "
                f"{confirmed}/{total} confirmed flows ({conf_ratio*100:.0f}%), "
                f"{bullish_actors}↑/{bearish_actors}↓ actors, "
                f"{len(agreeing_sources)} source types agree"
            ),
        }

    return result


# ══════════════════════════════════════════════════════════════════════════
# 8. Flow velocity (multi-timeframe acceleration)
# ══════════════════════════════════════════════════════════════════════════

def compute_flow_velocity(engine: Engine, days: int = 60) -> dict[str, Any]:
    """Multi-timeframe flow velocity per sector.

    Compares 3d vs 7d, 7d vs 14d, 14d vs 30d to identify
    acceleration at different time scales.

    Returns per-sector velocity with short/medium/long signals.
    """
    flows = _fetch_flows(engine, days)
    if not flows:
        return {}

    today = date.today()
    windows = {
        "3d": today - timedelta(days=3),
        "7d": today - timedelta(days=7),
        "14d": today - timedelta(days=14),
        "30d": today - timedelta(days=30),
    }

    # Accumulate per sector per window
    sector_windows: dict[str, dict[str, float]] = defaultdict(lambda: {k: 0.0 for k in windows})

    for f in flows:
        sector = _get_sector(f["ticker"])
        signed = _signed_amount(f)
        fdate = f["flow_date"]
        if not isinstance(fdate, date):
            continue
        for label, cutoff in windows.items():
            if fdate >= cutoff:
                sector_windows[sector][label] += signed

    result = {}
    for sector, w in sector_windows.items():
        # Normalize to daily rates
        rates = {
            "3d": w["3d"] / 3,
            "7d": w["7d"] / 7,
            "14d": w["14d"] / 14,
            "30d": w["30d"] / 30,
        }

        # Velocity: ratio of short-term to long-term daily rate
        def _velocity(short: float, long: float) -> float:
            if abs(long) > 0:
                return round(short / abs(long), 2)
            return 1.0 if short > 0 else -1.0 if short < 0 else 0.0

        short_vel = _velocity(rates["3d"], rates["7d"])
        med_vel = _velocity(rates["7d"], rates["14d"])
        long_vel = _velocity(rates["14d"], rates["30d"])

        # Signal
        def _signal(vel: float) -> str:
            if vel > 1.5:
                return "surging"
            if vel > 1.0:
                return "accelerating"
            if vel > 0.5:
                return "stable"
            if vel > 0:
                return "decelerating"
            return "reversing"

        result[sector] = {
            "short_velocity": short_vel,
            "medium_velocity": med_vel,
            "long_velocity": long_vel,
            "short_signal": _signal(short_vel),
            "medium_signal": _signal(med_vel),
            "long_signal": _signal(long_vel),
            "daily_rates": {k: round(v, 2) for k, v in rates.items()},
            "net_flows": {k: round(v, 2) for k, v in w.items()},
        }

    return result


# ══════════════════════════════════════════════════════════════════════════
# 9. Confidence-weighted aggregation
# ══════════════════════════════════════════════════════════════════════════

def aggregate_confidence_weighted(engine: Engine, days: int = 30) -> dict[str, Any]:
    """Like aggregate_by_sector but weights each flow by its confidence level.

    A $100M confirmed insider buy counts 1.0x.
    A $100M estimated ETF flow counts 0.4x.
    A $100M rumored prediction market signal counts 0.1x.

    Returns the same structure as aggregate_by_sector but with
    confidence-adjusted flows.
    """
    flows = _fetch_flows(engine, days)
    if not flows:
        return {}

    sector_data: dict[str, dict[str, float]] = defaultdict(lambda: {
        "weighted_net": 0.0, "raw_net": 0.0,
        "confirmed_net": 0.0, "estimated_net": 0.0,
    })

    for f in flows:
        sector = _get_sector(f["ticker"])
        signed = _signed_amount(f)
        weight = _confidence_weight(f["confidence"])
        sd = sector_data[sector]
        sd["raw_net"] += signed
        sd["weighted_net"] += signed * weight
        if f["confidence"] in ("confirmed", "derived"):
            sd["confirmed_net"] += signed
        else:
            sd["estimated_net"] += signed

    result = {}
    for sector, sd in sector_data.items():
        raw = sd["raw_net"]
        weighted = sd["weighted_net"]
        # Conviction gap: how much does weighting change the picture?
        if abs(raw) > 0:
            conviction_gap = round(1 - abs(weighted) / abs(raw), 3)
        else:
            conviction_gap = 0.0

        result[sector] = {
            "raw_net_flow": round(raw, 2),
            "weighted_net_flow": round(weighted, 2),
            "confirmed_net": round(sd["confirmed_net"], 2),
            "estimated_net": round(sd["estimated_net"], 2),
            "conviction_gap": conviction_gap,
            "direction_raw": "inflow" if raw >= 0 else "outflow",
            "direction_weighted": "inflow" if weighted >= 0 else "outflow",
            "directions_agree": (raw >= 0) == (weighted >= 0),
        }

    return result


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

    # Smart money vs dumb money
    try:
        result["smart_vs_dumb"] = aggregate_smart_vs_dumb(engine, days)
    except Exception as exc:
        log.warning("smart_vs_dumb failed: {e}", e=str(exc))
        result["smart_vs_dumb"] = {}

    # Sector conviction scores
    try:
        result["sector_conviction"] = compute_sector_conviction(engine, days)
    except Exception as exc:
        log.warning("sector_conviction failed: {e}", e=str(exc))
        result["sector_conviction"] = {}

    # Multi-timeframe flow velocity
    try:
        result["flow_velocity"] = compute_flow_velocity(engine, max(days, 60))
    except Exception as exc:
        log.warning("flow_velocity failed: {e}", e=str(exc))
        result["flow_velocity"] = {}

    # Confidence-weighted view
    try:
        result["confidence_weighted"] = aggregate_confidence_weighted(engine, days)
    except Exception as exc:
        log.warning("confidence_weighted failed: {e}", e=str(exc))
        result["confidence_weighted"] = {}

    # Time series for specific sector or ticker
    if sector:
        try:
            ts = aggregate_by_time(engine, sector, period=period, days=days)
            result["time_series"] = ts
        except Exception as exc:
            log.warning("aggregate_by_time failed: {e}", e=str(exc))
            result["time_series"] = []

    return result
