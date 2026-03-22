"""GRID v4 Dashboard — standalone FastAPI app on port 8080.

Serves the intelligence dashboard HTML and provides API endpoints
for stats, features, and feature history. Reads directly from PostgreSQL.

Usage:
    source ~/grid_v4/venv/bin/activate
    python dashboard.py
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from sqlalchemy import text

from db import get_engine

app = FastAPI(title="GRID v4 Dashboard")

_HTML_PATH = Path(__file__).parent / "grid_dashboard.html"


@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    """Serve the main dashboard HTML."""
    return HTMLResponse(content=_HTML_PATH.read_text())


@app.get("/api/stats")
async def get_stats():
    """Return aggregate statistics and latest macro values."""
    engine = get_engine()
    with engine.connect() as conn:
        # Total rows and date range from resolved_series
        row = conn.execute(text(
            "SELECT COUNT(*), MIN(obs_date), MAX(obs_date) FROM resolved_series"
        )).fetchone()
        total_rows = row[0]
        min_date = str(row[1]) if row[1] else None
        max_date = str(row[2]) if row[2] else None

        # Feature counts
        total_features = conn.execute(text(
            "SELECT COUNT(*) FROM feature_registry"
        )).scalar()
        eligible_features = conn.execute(text(
            "SELECT COUNT(*) FROM feature_registry WHERE model_eligible = TRUE"
        )).scalar()

        # Hypothesis count
        hyp_count = conn.execute(text(
            "SELECT COUNT(*) FROM hypothesis_registry"
        )).scalar()

        # Latest macro values by feature name
        macro_names = [
            "wti_oil", "treasury_10y", "yield_curve_10y2y", "vix",
            "fed_funds_rate", "m2_money_supply",
        ]
        macro: dict[str, float | None] = {}
        for name in macro_names:
            val_row = conn.execute(text("""
                SELECT rs.value FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name = :name
                ORDER BY rs.obs_date DESC LIMIT 1
            """), {"name": name}).fetchone()
            macro[name] = float(val_row[0]) if val_row else None

    return {
        "total_rows": total_rows,
        "min_date": min_date,
        "max_date": max_date,
        "total_features": total_features,
        "eligible_features": eligible_features,
        "hypothesis_count": hyp_count,
        "macro": macro,
    }


@app.get("/api/features")
async def get_features():
    """Return all features with latest values and row counts."""
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
                fr.id, fr.name, fr.family, fr.description,
                fr.model_eligible, fr.eligible_from_date,
                (SELECT COUNT(*) FROM resolved_series rs WHERE rs.feature_id = fr.id) AS row_count,
                (SELECT MIN(obs_date) FROM resolved_series rs WHERE rs.feature_id = fr.id) AS min_date,
                (SELECT MAX(obs_date) FROM resolved_series rs WHERE rs.feature_id = fr.id) AS max_date,
                (SELECT value FROM resolved_series rs
                 WHERE rs.feature_id = fr.id ORDER BY obs_date DESC LIMIT 1) AS latest_value
            FROM feature_registry fr
            ORDER BY fr.id
        """)).fetchall()

    features: list[dict[str, Any]] = []
    for r in rows:
        d = dict(r._mapping)
        for k in ("eligible_from_date", "min_date", "max_date"):
            if d.get(k) is not None:
                d[k] = str(d[k])
        if d.get("latest_value") is not None:
            d["latest_value"] = float(d["latest_value"])
        features.append(d)

    return {"features": features}


@app.get("/api/feature/{name}/history")
async def get_feature_history(name: str, days: int = Query(default=90)):
    """Return recent time-series data for a named feature."""
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT rs.obs_date, rs.value
            FROM resolved_series rs
            JOIN feature_registry fr ON rs.feature_id = fr.id
            WHERE fr.name = :name
            ORDER BY rs.obs_date DESC
            LIMIT :days
        """), {"name": name, "days": days}).fetchall()

    data = [{"date": str(r[0]), "value": float(r[1])} for r in reversed(rows)]
    return {"name": name, "data": data}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
