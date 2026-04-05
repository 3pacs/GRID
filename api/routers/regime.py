"""Regime state endpoints."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from loguru import logger as log
from pydantic import BaseModel
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine, get_pit_store
from api.schemas.regime import (
    RegimeCurrentResponse,
    RegimeDriver,
    RegimeHistoryEntry,
    RegimeHistoryResponse,
    RegimeTransition,
    RegimeTransitionsResponse,
)

router = APIRouter(prefix="/api/v1/regime", tags=["regime"])


# ── Weight tuning endpoints ──────────────────────────────────────


class WeightUpdateRequest(BaseModel):
    weights: dict[str, float]


@router.get("/weights")
async def get_weights(_token: str = Depends(require_auth)) -> dict:
    """Return current FEATURE_WEIGHTS and latest stress index."""
    from scripts.auto_regime import DEFAULT_FEATURE_WEIGHTS, FEATURE_WEIGHTS, WEIGHTS_OVERRIDE_PATH

    # Check which weights have been overridden
    overrides: dict[str, float] = {}
    if WEIGHTS_OVERRIDE_PATH.exists():
        try:
            with open(WEIGHTS_OVERRIDE_PATH) as f:
                overrides = json.load(f)
        except Exception as e:
            log.warning("Regime: failed to load weight overrides: {e}", e=str(e))

    # Get latest stress index from decision_journal
    engine = get_db_engine()
    stress_index = None
    regime_state = None
    confidence = None
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT inferred_state, state_confidence, counterfactual "
                "FROM decision_journal ORDER BY decision_timestamp DESC LIMIT 1"
            )
        ).fetchone()
        if row:
            regime_state = row[0]
            confidence = float(row[1]) if row[1] else None
            # Parse stress index from counterfactual field (format: "S=0.123, dS/dt=0.0045")
            cf = row[2] or ""
            if "S=" in cf:
                try:
                    stress_index = float(cf.split("S=")[1].split(",")[0])
                except (ValueError, IndexError):
                    pass

    return {
        "weights": dict(FEATURE_WEIGHTS),
        "defaults": dict(DEFAULT_FEATURE_WEIGHTS),
        "overrides": overrides,
        "stress_index": stress_index,
        "regime": regime_state,
        "confidence": confidence,
    }


@router.put("/weights")
async def update_weights(
    req: WeightUpdateRequest,
    _token: str = Depends(require_auth),
) -> dict:
    """Save weight overrides and re-run regime classification."""
    from scripts.auto_regime import DEFAULT_FEATURE_WEIGHTS, _load_effective_weights, run_with_weights

    engine = get_db_engine()
    # Merge with defaults
    merged = dict(DEFAULT_FEATURE_WEIGHTS)
    merged.update(req.weights)
    # Clamp to valid range
    for k in merged:
        merged[k] = max(-0.30, min(0.30, merged[k]))

    result = run_with_weights(engine, merged, save=True)
    return {"ok": True, "result": result}


@router.post("/simulate")
async def simulate_weights(
    req: WeightUpdateRequest,
    _token: str = Depends(require_auth),
) -> dict:
    """Preview regime classification with custom weights (no save)."""
    from scripts.auto_regime import DEFAULT_FEATURE_WEIGHTS, run_with_weights

    engine = get_db_engine()
    merged = dict(DEFAULT_FEATURE_WEIGHTS)
    merged.update(req.weights)
    for k in merged:
        merged[k] = max(-0.30, min(0.30, merged[k]))

    result = run_with_weights(engine, merged, save=False)
    return {"ok": True, "result": result}


@router.get("/current", response_model=RegimeCurrentResponse)
async def get_current(_token: str = Depends(require_auth)) -> RegimeCurrentResponse:
    """Return current inferred regime state."""
    engine = get_db_engine()

    # Get latest journal entry — check production model first, fall back to any entry
    with engine.connect() as conn:
        prod = conn.execute(
            text(
                "SELECT id, name, version FROM model_registry "
                "WHERE state = 'PRODUCTION' AND layer = 'REGIME' LIMIT 1"
            )
        ).fetchone()

        if prod is not None:
            latest = conn.execute(
                text(
                    "SELECT inferred_state, state_confidence, transition_probability, "
                    "contradiction_flags, grid_recommendation, baseline_recommendation, "
                    "decision_timestamp "
                    "FROM decision_journal "
                    "WHERE model_version_id = :mid "
                    "ORDER BY decision_timestamp DESC LIMIT 1"
                ),
                {"mid": prod[0]},
            ).fetchone()
        else:
            latest = None

        # Fall back to most recent journal entry regardless of model
        if latest is None:
            latest = conn.execute(
                text(
                    "SELECT inferred_state, state_confidence, transition_probability, "
                    "contradiction_flags, grid_recommendation, baseline_recommendation, "
                    "decision_timestamp "
                    "FROM decision_journal "
                    "ORDER BY decision_timestamp DESC LIMIT 1"
                )
            ).fetchone()

    if latest is None:
        return RegimeCurrentResponse(
            state="UNCALIBRATED",
            confidence=0.0,
            transition_probability=0.0,
            top_drivers=[],
            contradiction_flags=[],
            model_version="none",
            as_of=datetime.now(timezone.utc).isoformat(),
            baseline_comparison="No data — run auto_regime or wait for scheduled detection",
        )

    model_label = f"{prod[1]} v{prod[2]}" if prod else "auto"
    flags = latest[3] if isinstance(latest[3], dict) else {}
    contradiction_list = [f"{k}: {v}" for k, v in flags.items()] if flags else []

    return RegimeCurrentResponse(
        state=latest[0],
        confidence=float(latest[1]),
        transition_probability=float(latest[2]),
        contradiction_flags=contradiction_list,
        model_version=model_label,
        as_of=latest[6].isoformat() if latest[6] else "",
        baseline_comparison=latest[5] or "",
    )


@router.get("/all-active")
async def get_all_active(_token: str = Depends(require_auth)) -> dict:
    """Return all active regime states with their latest readings."""
    engine = get_db_engine()

    with engine.connect() as conn:
        # Get the latest entry for each distinct regime state
        rows = conn.execute(
            text(
                "SELECT DISTINCT ON (inferred_state) "
                "inferred_state, state_confidence, transition_probability, "
                "contradiction_flags, grid_recommendation, decision_timestamp "
                "FROM decision_journal "
                "ORDER BY inferred_state, decision_timestamp DESC"
            )
        ).fetchall()

        # Macro vs strategy classification
        macro_states = {"GROWTH", "NEUTRAL", "FRAGILE", "CRISIS"}
        macro = []
        strategy = []

        for row in rows:
            entry = {
                "state": row[0],
                "confidence": float(row[1]) if row[1] else 0.0,
                "transition_probability": float(row[2]) if row[2] else 0.0,
                "contradiction_flags": row[3] if isinstance(row[3], dict) else {},
                "recommendation": row[4] or "",
                "as_of": row[5].isoformat() if row[5] else "",
            }
            if row[0] in macro_states:
                macro.append(entry)
            else:
                strategy.append(entry)

        # Sort by confidence descending
        macro.sort(key=lambda x: x["confidence"], reverse=True)
        strategy.sort(key=lambda x: x["confidence"], reverse=True)

        # Get feature contributions from the latest clustering result
        feature_contributions = []
        try:
            from discovery.clustering import ClusterDiscovery

            cd = ClusterDiscovery(engine)
            leaders = cd.identify_transition_leaders()
            if leaders:
                for feat_name, importance in sorted(
                    leaders.items(), key=lambda x: abs(x[1]), reverse=True
                )[:15]:
                    feature_contributions.append({
                        "feature": feat_name,
                        "importance": round(float(importance), 4),
                    })
        except Exception as e:
            log.warning("Regime: feature contribution query failed: {e}", e=str(e))

        # Top movers — features with biggest recent changes
        top_movers = []
        try:
            mover_rows = conn.execute(
                text(
                    "SELECT f.name, f.family, "
                    "  (SELECT rs1.value FROM resolved_series rs1 "
                    "   WHERE rs1.feature_id = f.id "
                    "   ORDER BY rs1.obs_date DESC LIMIT 1) as latest_val, "
                    "  (SELECT rs2.value FROM resolved_series rs2 "
                    "   WHERE rs2.feature_id = f.id "
                    "   ORDER BY rs2.obs_date DESC LIMIT 1 OFFSET 20) as prior_val "
                    "FROM feature_registry f "
                    "WHERE f.model_eligible = TRUE "
                    "ORDER BY f.id LIMIT 100"
                )
            ).fetchall()
            for name, family, latest, prior in mover_rows:
                if latest is not None and prior is not None and float(prior) != 0:
                    pct = (float(latest) - float(prior)) / abs(float(prior)) * 100
                    if abs(pct) > 2:  # Only significant movers
                        top_movers.append({
                            "feature": name,
                            "family": family or "",
                            "latest": round(float(latest), 4),
                            "change_pct": round(pct, 2),
                        })
            top_movers.sort(key=lambda x: abs(x["change_pct"]), reverse=True)
            top_movers = top_movers[:12]
        except Exception as e:
            log.warning("Regime: top movers query failed: {e}", e=str(e))

        return {
            "macro": macro,
            "strategy": strategy,
            "feature_contributions": feature_contributions,
            "top_movers": top_movers,
            "total_journal_entries": len(rows),
        }


@router.get("/synthesis")
async def get_synthesis(_token: str = Depends(require_auth)) -> dict:
    """LLM-powered regime synthesis — interprets combined signals."""
    engine = get_db_engine()

    # Gather all regime data
    with engine.connect() as conn:
        regime_rows = conn.execute(
            text(
                "SELECT DISTINCT ON (inferred_state) "
                "inferred_state, state_confidence, grid_recommendation "
                "FROM decision_journal "
                "ORDER BY inferred_state, decision_timestamp DESC"
            )
        ).fetchall()

        # Top movers
        mover_rows = conn.execute(
            text(
                "SELECT f.name, f.family, "
                "  (SELECT rs1.value FROM resolved_series rs1 "
                "   WHERE rs1.feature_id = f.id "
                "   ORDER BY rs1.obs_date DESC LIMIT 1) as latest_val, "
                "  (SELECT rs2.value FROM resolved_series rs2 "
                "   WHERE rs2.feature_id = f.id "
                "   ORDER BY rs2.obs_date DESC LIMIT 1 OFFSET 20) as prior_val "
                "FROM feature_registry f "
                "WHERE f.model_eligible = TRUE "
                "ORDER BY f.id LIMIT 100"
            )
        ).fetchall()

    regime_summary = "\n".join(
        f"  {r[0]}: {float(r[1])*100:.0f}% confidence — recommendation: {r[2] or 'none'}"
        for r in regime_rows
    )

    movers = []
    for name, family, latest, prior in mover_rows:
        if latest is not None and prior is not None and float(prior) != 0:
            pct = (float(latest) - float(prior)) / abs(float(prior)) * 100
            if abs(pct) > 2:
                direction = "UP" if pct > 0 else "DOWN"
                movers.append(f"  {name} ({family}): {direction} {abs(pct):.1f}%")
    def _mover_sort_key(x: str) -> float:
        try:
            return abs(float(x.split()[-1].rstrip('%')))
        except (ValueError, IndexError):
            return 0.0
    movers.sort(key=_mover_sort_key, reverse=True)

    prompt = f"""You are GRID's regime analyst. Analyze the following regime readings and market data to produce a unified interpretation.

ACTIVE REGIME READINGS:
{regime_summary}

TOP FEATURE MOVERS (last ~20 observations):
{chr(10).join(movers[:15]) if movers else '  (no significant movers)'}

Produce a concise analysis with these sections:

1. UNIFIED SIGNAL: What do these regime readings mean together? If GROWTH and CRISIS are both high, explain the contradiction. What is the dominant force?

2. DRIVERS: What real-world forces (monetary policy, earnings, geopolitics, commodity cycles, AI/tech spending, credit conditions) are most likely behind these readings? Be specific about mechanisms, not vague.

3. MOMENTUM: Is the current regime strengthening or weakening? Where is the inertia? What would cause a transition?

4. MISPRICINGS: Based on regime contradictions and feature moves, what might be mispriced? Where are markets not reflecting the regime signal?

5. POSTURE: One-line recommended positioning.

Be direct, specific, and actionable. No hedging or disclaimers. Reference the actual data above."""

    # Try LLM synthesis
    try:
        from ollama.client import get_client
        client = get_client()
        if not client.is_available:
            raise HTTPException(status_code=503, detail="LLM synthesis service not available")

        response = client.chat(
            [
                {"role": "system", "content": "You are a macro strategist synthesizing quantitative regime signals into actionable market intelligence. Be direct and specific."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.4,
            num_predict=1500,
        )

        return {
            "synthesis": response,
            "regime_count": len(regime_rows),
            "mover_count": len(movers),
        }
    except HTTPException:
        raise
    except Exception as exc:
        log.warning("Regime synthesis failed: {e}", e=str(exc))
        raise HTTPException(status_code=500, detail=f"Regime synthesis failed: {exc}") from exc


@router.get("/history", response_model=RegimeHistoryResponse)
async def get_history(
    days: int = Query(default=90, ge=1, le=365),
    _token: str = Depends(require_auth),
) -> RegimeHistoryResponse:
    """Return regime history."""
    engine = get_db_engine()

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT DATE(decision_timestamp) AS dt, "
                "inferred_state, state_confidence "
                "FROM decision_journal "
                "WHERE decision_timestamp >= NOW() - make_interval(days => :days) "
                "ORDER BY decision_timestamp"
            ),
            {"days": days},
        ).fetchall()

    history = [
        RegimeHistoryEntry(
            date=str(row[0]),
            state=row[1],
            confidence=float(row[2]),
        )
        for row in rows
    ]

    return RegimeHistoryResponse(history=history)


@router.get("/transitions", response_model=RegimeTransitionsResponse)
async def get_transitions(
    _token: str = Depends(require_auth),
) -> RegimeTransitionsResponse:
    """Return all detected regime transitions."""
    engine = get_db_engine()

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT decision_timestamp, inferred_state, state_confidence "
                "FROM decision_journal "
                "ORDER BY decision_timestamp"
            )
        ).fetchall()

    transitions: list[RegimeTransition] = []
    for i in range(1, len(rows)):
        if rows[i][1] != rows[i - 1][1]:
            transitions.append(
                RegimeTransition(
                    date=str(rows[i][0]),
                    from_state=rows[i - 1][1],
                    to_state=rows[i][1],
                    confidence=float(rows[i][2]),
                )
            )

    return RegimeTransitionsResponse(transitions=transitions)
