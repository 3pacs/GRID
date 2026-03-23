"""Regime state endpoints."""

from __future__ import annotations

from datetime import date, datetime, timezone

from fastapi import APIRouter, Depends, Query
from loguru import logger as log
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
        except Exception:
            pass

        return {
            "macro": macro,
            "strategy": strategy,
            "feature_contributions": feature_contributions,
            "total_journal_entries": len(rows),
        }


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
