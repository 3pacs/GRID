"""Trial gem hunter endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/trials", tags=["trials"])


def _row_to_dict(row: Any) -> dict:
    """Convert a SQLAlchemy row to a plain dict."""
    d = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
    # Convert non-serializable types to strings
    for k, v in d.items():
        if hasattr(v, "isoformat"):
            d[k] = v.isoformat()
    return d


@router.get("/gems")
def get_gems(_token: str = Depends(require_auth)) -> dict:
    """Return active BUY signals from the trial_gems view."""
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT * FROM trial_gems")).fetchall()
        return {"gems": [_row_to_dict(r) for r in rows], "count": len(rows)}
    except Exception as exc:
        log.warning("Failed to fetch trial gems: {e}", e=str(exc))
        raise HTTPException(status_code=500, detail=f"Failed to fetch trial gems: {exc}") from exc


@router.get("/signals")
def get_signals(
    _token: str = Depends(require_auth),
    signal_type: str | None = Query(None, description="Filter by signal type: BUY, WATCHLIST, AVOID"),
    limit: int = Query(50, ge=1, le=200, description="Max rows to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
) -> dict:
    """Return scored trial signals with optional filters."""
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            # Build count query
            count_sql = "SELECT COUNT(*) FROM trial_signals"
            data_sql = "SELECT * FROM trial_signals"
            params: dict[str, Any] = {"lim": limit, "off": offset}

            if signal_type is not None:
                allowed = {"BUY", "WATCHLIST", "AVOID"}
                if signal_type.upper() not in allowed:
                    raise HTTPException(
                        status_code=400,
                        detail=f"signal_type must be one of {sorted(allowed)}",
                    )
                count_sql += " WHERE signal_type = :sig"
                data_sql += " WHERE signal_type = :sig"
                params["sig"] = signal_type.upper()

            total = conn.execute(
                text(count_sql).bindparams(**{k: v for k, v in params.items() if k == "sig"}),
            ).scalar()

            data_sql += " ORDER BY trial_strength_score DESC LIMIT :lim OFFSET :off"
            rows = conn.execute(text(data_sql).bindparams(**params)).fetchall()

        return {
            "signals": [_row_to_dict(r) for r in rows],
            "total": total,
        }
    except HTTPException:
        raise
    except Exception as exc:
        log.warning("Failed to fetch trial signals: {e}", e=str(exc))
        raise HTTPException(status_code=500, detail=f"Failed to fetch trial signals: {exc}") from exc


@router.get("/catalysts")
def get_catalysts(_token: str = Depends(require_auth)) -> dict:
    """Return upcoming catalyst events from the upcoming_catalysts view."""
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT * FROM upcoming_catalysts LIMIT :lim").bindparams(lim=100),
            ).fetchall()
        return {"catalysts": [_row_to_dict(r) for r in rows], "count": len(rows)}
    except Exception as exc:
        log.warning("Failed to fetch catalysts: {e}", e=str(exc))
        raise HTTPException(status_code=500, detail=f"Failed to fetch catalysts: {exc}") from exc


@router.get("/sponsors")
def get_sponsors(_token: str = Depends(require_auth)) -> dict:
    """Return actors categorized as trial sponsors, ordered by influence."""
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT * FROM actors "
                    "WHERE category IN ("
                    "  :cat1, :cat2, :cat3, :cat4"
                    ") "
                    "ORDER BY influence_score DESC "
                    "LIMIT :lim"
                ).bindparams(
                    cat1="pharmaceutical_sponsor",
                    cat2="academic_research",
                    cat3="government_research",
                    cat4="clinical_sponsor",
                    lim=100,
                ),
            ).fetchall()
        return {"sponsors": [_row_to_dict(r) for r in rows], "count": len(rows)}
    except Exception as exc:
        log.warning("Failed to fetch sponsors: {e}", e=str(exc))
        raise HTTPException(status_code=500, detail=f"Failed to fetch sponsors: {exc}") from exc


@router.get("/stats")
def get_stats(_token: str = Depends(require_auth)) -> dict:
    """Return summary statistics for trial signals."""
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            # Signals by type
            by_type_rows = conn.execute(
                text(
                    "SELECT signal_type, COUNT(*) AS cnt "
                    "FROM trial_signals "
                    "GROUP BY signal_type "
                    "ORDER BY cnt DESC"
                ),
            ).fetchall()
            by_type = {r[0]: r[1] for r in by_type_rows}

            # Signals by regime
            by_regime_rows = conn.execute(
                text(
                    "SELECT regime_at_signal, COUNT(*) AS cnt "
                    "FROM trial_signals "
                    "GROUP BY regime_at_signal "
                    "ORDER BY cnt DESC"
                ),
            ).fetchall()
            by_regime = {r[0]: r[1] for r in by_regime_rows}

            # Top indications
            top_indications_rows = conn.execute(
                text(
                    "SELECT primary_indication, COUNT(*) AS cnt "
                    "FROM trial_signals "
                    "GROUP BY primary_indication "
                    "ORDER BY cnt DESC "
                    "LIMIT :lim"
                ).bindparams(lim=10),
            ).fetchall()
            top_indications = [{"indication": r[0], "count": r[1]} for r in top_indications_rows]

            # Latest run_id
            latest_run = conn.execute(
                text(
                    "SELECT run_id FROM trial_signals "
                    "ORDER BY created_at DESC "
                    "LIMIT 1"
                ),
            ).scalar()

        return {
            "by_type": by_type,
            "by_regime": by_regime,
            "top_indications": top_indications,
            "latest_run_id": latest_run,
            "total_signals": sum(by_type.values()) if by_type else 0,
        }
    except Exception as exc:
        log.warning("Failed to fetch trial stats: {e}", e=str(exc))
        raise HTTPException(status_code=500, detail=f"Failed to fetch trial stats: {exc}") from exc
