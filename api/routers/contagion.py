"""Chain contagion simulator endpoint.

Given a shock (supplier outage or commodity price spike) this endpoint
returns the cascading margin / revenue impact on every downstream actor.

The heavy lifting lives in ``intelligence.chain_contagion``; this router is
a thin validation + caching + persistence wrapper over that pure library.

Every successful simulation is logged to ``contagion_predictions`` so the
backtest module can score the prediction 7/14/30 days later against the
actual downstream price moves in ``raw_series``.
"""

from __future__ import annotations

import json
import re
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine
from intelligence.chain_contagion import (
    DEFAULT_PASS_THROUGH,
    VALID_SHOCK_TYPES,
    simulate_contagion,
)
from utils.ttl_cache import TTLCache

router = APIRouter(prefix="/api/v1/contagion", tags=["contagion"])
sector_router = APIRouter(prefix="/api/v1/sectors", tags=["contagion"])

_cache: TTLCache = TTLCache(ttl=600.0, max_size=128)
# Matrix cache holds (scenario_id, sector) → cell list for 1h.
_matrix_cache: TTLCache = TTLCache(ttl=3600.0, max_size=256)
# Scenario-level sim cache (single scenario re-used across many tickers).
_scenario_sim_cache: TTLCache = TTLCache(ttl=3600.0, max_size=256)


# ─── Sub-feature 3: Preset scenario catalog ─────────────────────────────────

SCENARIO_CATALOG: list[dict[str, Any]] = [
    {
        "id": "cocoa_crisis",
        "label": "West Africa cocoa crisis",
        "description": (
            "Ivory Coast + Ghana harvest shortfall drives cocoa prices +50%"
        ),
        "shock": {
            "shock_node": "cocoa_beans",
            "shock_type": "price_increase",
            "magnitude": 0.50,
        },
        "expected_victims_preview": ["hsy", "mdlz", "nsrgy"],
    },
    {
        "id": "taiwan_crisis",
        "label": "Taiwan Strait disruption",
        "description": "TSMC production halt cuts advanced chip supply",
        "shock": {
            "shock_node": "tsmc",
            "shock_type": "supply_disruption",
            "magnitude": 0.30,
        },
        "expected_victims_preview": ["aapl", "nvda", "amd"],
    },
    {
        "id": "fed_hike_100bp",
        "label": "Fed surprise +100bp",
        "description": "Emergency rate hike crushes rate-sensitive sectors",
        "shock": {
            "shock_node": "fed_funds_rate",
            "shock_type": "price_increase",
            "magnitude": 1.00,
        },
        "expected_victims_preview": ["xlre_tickers", "homebuilders"],
    },
    {
        "id": "opec_cut",
        "label": "OPEC+ cuts 2 mb/d",
        "description": "Supply cut drives crude +20%",
        "shock": {
            "shock_node": "oil_crude",
            "shock_type": "price_increase",
            "magnitude": 0.20,
        },
        "expected_victims_preview": ["airline tickers", "trucking"],
    },
    {
        "id": "usd_up_10",
        "label": "USD index +10%",
        "description": "Dollar strength crushes exporter earnings",
        "shock": {
            "shock_node": "usd_index",
            "shock_type": "price_increase",
            "magnitude": 0.10,
        },
        "expected_victims_preview": ["ko", "pg", "nke"],
    },
    {
        "id": "glencore_halt",
        "label": "Glencore mining halt",
        "description": "Cobalt/copper supply disruption hits EV supply chain",
        "shock": {
            "shock_node": "cobalt",
            "shock_type": "supply_disruption",
            "magnitude": 0.40,
        },
        "expected_victims_preview": ["tsla", "lg_energy"],
    },
    {
        "id": "euv_down",
        "label": "ASML EUV halt",
        "description": "EUV lithography supply disruption",
        "shock": {
            "shock_node": "asml",
            "shock_type": "supply_disruption",
            "magnitude": 1.00,
        },
        "expected_victims_preview": ["tsmc", "samsung_foundry", "intel"],
    },
    {
        "id": "neon_shortage",
        "label": "Neon gas shortage",
        "description": "Ukraine crisis cuts 70% global neon supply",
        "shock": {
            "shock_node": "neon_gas",
            "shock_type": "supply_disruption",
            "magnitude": 0.50,
        },
        "expected_victims_preview": ["tsmc", "nvda"],
    },
]

_SCENARIOS_BY_ID: dict[str, dict[str, Any]] = {s["id"]: s for s in SCENARIO_CATALOG}

_NODE_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,80}$")

_VALID_SOURCES: frozenset[str] = frozenset(
    {"api", "news_listener", "scheduled_scenario", "smoke_test", "test"}
)


def _cache_key(
    shock_node: str,
    shock_type: str,
    magnitude: float,
    max_depth: int,
    pass_through: float,
    persist: bool = True,
) -> str:
    return (
        f"{shock_node.lower()}|{shock_type}|{magnitude:.4f}|"
        f"{max_depth}|{pass_through:.3f}|persist={int(persist)}"
    )


def _persist_prediction(
    engine: Any,
    shock_node: str,
    shock_type: str,
    magnitude: float,
    max_depth: int,
    result: dict[str, Any],
    source: str,
    caller_id: str | None,
) -> int | None:
    """Insert a row in ``contagion_predictions`` and return the new id.

    Returns ``None`` on any DB failure — the simulation response is still
    returned to the client, we just lose the audit trail for this call.
    """
    summary = result.get("summary") or {}
    ranked = result.get("ranked_impact") or []
    try:
        with engine.begin() as conn:
            row = conn.execute(
                text(
                    """
                    INSERT INTO contagion_predictions (
                        shock_node, shock_type, magnitude, max_depth,
                        summary, ranked_impact, source, caller_id
                    ) VALUES (
                        :shock_node, :shock_type, :magnitude, :max_depth,
                        CAST(:summary AS JSONB), CAST(:ranked AS JSONB),
                        :source, :caller_id
                    )
                    RETURNING id
                    """
                ),
                {
                    "shock_node": shock_node,
                    "shock_type": shock_type,
                    "magnitude": float(magnitude),
                    "max_depth": int(max_depth),
                    "summary": json.dumps(summary),
                    "ranked": json.dumps(ranked),
                    "source": source,
                    "caller_id": caller_id,
                },
            ).fetchone()
        return int(row[0]) if row else None
    except Exception as exc:
        log.warning(
            "contagion: persist failed for {n}: {e}",
            n=shock_node,
            e=str(exc),
        )
        return None


@router.get("/simulate")
async def simulate(
    shock_node: str = Query(..., min_length=1, max_length=80),
    shock_type: str = Query("price_increase"),
    magnitude: float = Query(0.30, ge=-1.0, le=5.0),
    max_depth: int = Query(4, ge=1, le=6),
    source: str = Query("api"),
    pass_through: float = Query(DEFAULT_PASS_THROUGH, ge=0.0, le=1.0),
    caller_id: str | None = Query(None, max_length=120),
    persist: bool = Query(True),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Simulate a supply-chain shock and return downstream impact.

    The result includes a ``prediction_id`` that clients can use to look up
    the row later in ``contagion_predictions`` (e.g. to correlate against
    backtest scores).

    Parameters
    ----------
    shock_node: id of the shocked node (commodity, supplier, country). Must
        match the lowercase id convention in ``supply_chain_nodes``.
    shock_type: ``price_increase`` | ``supply_disruption``.
    magnitude: fractional shock (0.30 = 30%). Negative values allowed for
        price *decreases* (treated symmetrically as margin relief).
    max_depth: BFS depth cap.
    source: persistence tag — where the call originated from.
    pass_through: fraction of cost shock that lands on downstream margins.
    caller_id: optional user or agent id for audit.
    persist: when false, compute the simulation without writing a prediction row.
    """
    if not _NODE_RE.match(shock_node):
        raise HTTPException(status_code=400, detail="invalid shock_node")
    if shock_type not in VALID_SHOCK_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"shock_type must be one of {sorted(VALID_SHOCK_TYPES)}",
        )
    if source not in _VALID_SOURCES:
        source = "api"

    # When called directly (not via HTTP), Query() defaults may still be
    # unresolved — coerce to primitive scalars.
    try:
        pass_through = float(pass_through)
    except (TypeError, ValueError):
        pass_through = DEFAULT_PASS_THROUGH
    try:
        magnitude = float(magnitude)
    except (TypeError, ValueError):
        magnitude = 0.30
    try:
        max_depth = int(max_depth)
    except (TypeError, ValueError):
        max_depth = 4
    if not isinstance(caller_id, (str, type(None))):
        caller_id = None
    if not isinstance(persist, bool):
        persist = True

    key = _cache_key(
        shock_node,
        shock_type,
        magnitude,
        max_depth,
        pass_through,
        persist,
    )
    hit = _cache.get(key)
    if hit is not None:
        log.debug("contagion cache hit: {k}", k=key)
        return hit

    engine = get_db_engine()
    try:
        result = simulate_contagion(
            engine=engine,
            shock_node_id=shock_node,
            shock_type=shock_type,
            shock_magnitude=float(magnitude),
            max_depth=int(max_depth),
            pass_through=float(pass_through),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        log.warning("contagion simulate failed for {n}: {e}", n=shock_node, e=str(exc))
        raise HTTPException(status_code=500, detail="contagion simulation failed")

    if persist:
        prediction_id = _persist_prediction(
            engine=engine,
            shock_node=shock_node,
            shock_type=shock_type,
            magnitude=float(magnitude),
            max_depth=int(max_depth),
            result=result,
            source=source,
            caller_id=caller_id,
        )
        if prediction_id is not None:
            result["prediction_id"] = prediction_id

    _cache.set(key, result)
    return result


@router.get("/backtest")
async def backtest(
    days: int = Query(90, ge=1, le=365),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return recent backtest accuracy stats for contagion predictions.

    Aggregates ``contagion_backtest_results`` rows scored within the last
    ``days`` days. Cheap read-only query — no live scoring happens here,
    the scheduler does that daily at 05:00 UTC.
    """
    engine = get_db_engine()
    try:
        with engine.connect() as conn:
            windows_rows = conn.execute(
                text(
                    """
                    SELECT scored_at_days,
                           COUNT(*)                                AS n,
                           AVG(accuracy_score)                     AS avg_acc,
                           SUM(CASE WHEN accuracy_score >= 0.5
                                    THEN 1 ELSE 0 END)             AS directional
                    FROM contagion_backtest_results
                    WHERE scored_at >= NOW() - (:days || ' days')::INTERVAL
                    GROUP BY scored_at_days
                    ORDER BY scored_at_days
                    """
                ),
                {"days": int(days)},
            ).fetchall()

            by_shock_rows = conn.execute(
                text(
                    """
                    SELECT p.shock_type,
                           AVG(b.accuracy_score) AS avg_acc,
                           COUNT(*)              AS n
                    FROM contagion_backtest_results b
                    JOIN contagion_predictions p ON p.id = b.prediction_id
                    WHERE b.scored_at >= NOW() - (:days || ' days')::INTERVAL
                    GROUP BY p.shock_type
                    ORDER BY avg_acc DESC NULLS LAST
                    """
                ),
                {"days": int(days)},
            ).fetchall()

            top_rows = conn.execute(
                text(
                    """
                    SELECT b.id, b.prediction_id, b.ticker, b.scored_at_days,
                           b.predicted_margin_impact_pct,
                           b.actual_price_move_pct, b.accuracy_score,
                           p.shock_node, p.shock_type
                    FROM contagion_backtest_results b
                    JOIN contagion_predictions p ON p.id = b.prediction_id
                    WHERE b.scored_at >= NOW() - (:days || ' days')::INTERVAL
                      AND b.accuracy_score IS NOT NULL
                    ORDER BY b.accuracy_score DESC, b.scored_at DESC
                    LIMIT 10
                    """
                ),
                {"days": int(days)},
            ).fetchall()

            worst_rows = conn.execute(
                text(
                    """
                    SELECT b.id, b.prediction_id, b.ticker, b.scored_at_days,
                           b.predicted_margin_impact_pct,
                           b.actual_price_move_pct, b.accuracy_score,
                           p.shock_node, p.shock_type
                    FROM contagion_backtest_results b
                    JOIN contagion_predictions p ON p.id = b.prediction_id
                    WHERE b.scored_at >= NOW() - (:days || ' days')::INTERVAL
                      AND b.accuracy_score IS NOT NULL
                    ORDER BY b.accuracy_score ASC, b.scored_at DESC
                    LIMIT 10
                    """
                ),
                {"days": int(days)},
            ).fetchall()
    except Exception as exc:
        log.warning("contagion backtest query failed: {e}", e=str(exc))
        raise HTTPException(
            status_code=500, detail="contagion backtest query failed"
        )

    windows = [
        {
            "days": int(r[0]),
            "predictions_scored": int(r[1] or 0),
            "avg_accuracy": float(r[2]) if r[2] is not None else None,
            "directionally_correct": int(r[3] or 0),
        }
        for r in windows_rows
    ]
    by_shock_type = [
        {
            "shock_type": r[0],
            "accuracy": float(r[1]) if r[1] is not None else None,
            "sample_size": int(r[2] or 0),
        }
        for r in by_shock_rows
    ]

    def _row_to_dict(r: Any) -> dict[str, Any]:
        return {
            "id": int(r[0]),
            "prediction_id": int(r[1]),
            "ticker": r[2],
            "scored_at_days": int(r[3]),
            "predicted_margin_impact_pct": (
                float(r[4]) if r[4] is not None else None
            ),
            "actual_price_move_pct": float(r[5]) if r[5] is not None else None,
            "accuracy_score": float(r[6]) if r[6] is not None else None,
            "shock_node": r[7],
            "shock_type": r[8],
        }

    return {
        "windows": windows,
        "by_shock_type": by_shock_type,
        "top_accurate_predictions": [_row_to_dict(r) for r in top_rows],
        "worst_predictions": [_row_to_dict(r) for r in worst_rows],
        "lookback_days": int(days),
    }


# ─── Sub-feature 3: Scenarios endpoint ──────────────────────────────────────

@router.get("/scenarios")
async def get_scenarios(
    _token: str = Depends(require_auth),
) -> list[dict[str, Any]]:
    """Return the curated preset contagion scenario catalog.

    Static list — safe to fetch on every page load, the frontend uses it
    for the scenario dropdown on the contagion view.
    """
    # Return a shallow copy so callers can't mutate the module constant.
    return [dict(s) for s in SCENARIO_CATALOG]


# ─── Sub-feature 4: Sector contagion matrix ─────────────────────────────────

def _severity_bucket(margin_pct: float) -> str:
    """Bucket a margin impact into a coarse severity label for the UI."""
    m = abs(margin_pct)
    if m >= 0.03:
        return "high"
    if m >= 0.01:
        return "medium"
    if m > 0.0:
        return "low"
    return "none"


def _sector_tickers(sector_name: str) -> list[str]:
    """Return lowercase ticker ids for every actor in the requested sector.

    Resolves through ``analysis.sector_map.SECTOR_MAP``. Falls back to an
    empty list for unknown sectors — the caller returns an empty matrix.
    """
    try:
        from analysis.sector_map import SECTOR_MAP
    except Exception:
        return []
    sector = SECTOR_MAP.get(sector_name)
    if not sector:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for sub in (sector.get("subsectors") or {}).values():
        for actor in sub.get("actors", []) or []:
            tk = actor.get("ticker")
            if not tk:
                continue
            tk_l = tk.lower()
            if tk_l in seen:
                continue
            seen.add(tk_l)
            out.append(tk_l)
    return out


def _run_scenario_sim(engine: Any, scenario: dict[str, Any]) -> dict[str, Any]:
    """Run (and cache) a single preset scenario simulation.

    Keyed by scenario id so every ticker in a sector hits the same cached
    result — one cold simulation per scenario instead of ``n`` per matrix.
    """
    sid = scenario["id"]
    hit = _scenario_sim_cache.get(sid)
    if hit is not None:
        return hit
    shock = scenario["shock"]
    try:
        result = simulate_contagion(
            engine=engine,
            shock_node_id=shock["shock_node"],
            shock_type=shock["shock_type"],
            shock_magnitude=float(shock["magnitude"]),
            max_depth=4,
        )
    except Exception as exc:
        log.warning(
            "contagion matrix: sim failed for scenario {s}: {e}",
            s=sid,
            e=str(exc),
        )
        result = {"ranked_impact": []}
    _scenario_sim_cache.set(sid, result)
    return result


@sector_router.get("/{sector_name}/contagion-matrix")
async def get_contagion_matrix(
    sector_name: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return a ticker × scenario impact grid for the requested sector.

    For every ticker in ``SECTOR_MAP[sector_name]``, for every preset
    scenario, pull the ticker's ``margin_impact_pct`` out of a cached
    contagion simulation. Cells without a hit are still returned with
    ``margin_impact_pct = 0.0`` so the frontend can render a full grid.

    Heavy caching: one simulation per (scenario_id) is reused across every
    ticker in the sector, and the final assembled matrix is memoized per
    ``(sector_name)`` for 1h.
    """
    cache_key = f"matrix|{sector_name}"
    hit = _matrix_cache.get(cache_key)
    if hit is not None:
        return hit

    tickers = _sector_tickers(sector_name)
    if not tickers:
        empty = {
            "sector": sector_name,
            "tickers": [],
            "scenarios": [s["id"] for s in SCENARIO_CATALOG],
            "cells": [],
            "cached": False,
            "error": f"unknown sector: {sector_name}",
        }
        return empty

    engine = get_db_engine()
    scenario_results: dict[str, dict[str, Any]] = {}
    for scenario in SCENARIO_CATALOG:
        scenario_results[scenario["id"]] = _run_scenario_sim(engine, scenario)

    cells: list[dict[str, Any]] = []
    for tk in tickers:
        for scenario in SCENARIO_CATALOG:
            sid = scenario["id"]
            result = scenario_results.get(sid) or {}
            ranked = result.get("ranked_impact") or []
            match = next(
                (a for a in ranked if str(a.get("id", "")).lower() == tk),
                None,
            )
            margin = float(match["margin_impact_pct"]) if match else 0.0
            cells.append(
                {
                    "ticker": tk,
                    "scenario": sid,
                    "margin_impact_pct": round(margin, 6),
                    "severity": _severity_bucket(margin),
                }
            )

    payload = {
        "sector": sector_name,
        "tickers": tickers,
        "scenarios": [s["id"] for s in SCENARIO_CATALOG],
        "cells": cells,
        "cached": False,
    }
    _matrix_cache.set(cache_key, {**payload, "cached": True})
    return payload
