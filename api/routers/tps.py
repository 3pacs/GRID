"""Trump-Proximity Score (TPS) endpoints — Phase 0.

GET /api/v1/tps/today   — today's top-25 with full evidence chain.
GET /api/v1/tps/{ticker} — single-ticker drill-down (live recompute).

Snapshots are written to ``tps_snapshots`` by the daily 06:00 ET cron
in ``ingestion/scheduler.py``. This router is read-only over those
snapshots, with a "live recompute" fallback so the PWA works even
before the first cron has fired in a new environment.

Envelope follows the list-endpoint convention from
``.claude/rules/security.md`` (entries / total / limit / offset / has_more).
"""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine
from intelligence.trump_proximity import (
    compute_tps_for_ticker,
    persist_snapshot,
)

router = APIRouter(prefix="/api/v1/tps", tags=["tps"])


def _coerce_jsonb(value: Any) -> Any:
    """Postgres JSONB → Python object regardless of driver."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return None
    return value


def _row_to_entry(row: Any) -> dict[str, Any]:
    d = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
    for key in ("coverage", "evidence", "layer_scores"):
        d[key] = _coerce_jsonb(d.get(key))
    for key in ("as_of_date", "generated_at"):
        if d.get(key) is not None:
            d[key] = str(d[key])
    if d.get("score") is not None:
        d["score"] = float(d["score"])
    return d


@router.get("/today")
def get_today(
    limit: int = Query(default=25, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    min_coverage_layers: int = Query(default=1, ge=0, le=5),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return today's TPS top-N with evidence.

    ``min_coverage_layers`` enforces a minimum number of layers that
    must have data — guards against a top-of-list dominated by single-
    layer noise. Default 1 keeps the endpoint useful before bulk
    backfill completes.
    """
    engine = get_db_engine()
    today = date.today()

    sql = text(
        """
        SELECT ticker, as_of_date, score, coverage, evidence,
               layer_scores, generated_at
        FROM tps_snapshots
        WHERE as_of_date = (
            SELECT MAX(as_of_date) FROM tps_snapshots
        )
          AND score IS NOT NULL
          AND coverage_layer_count(coverage) >= :min_layers
        ORDER BY score DESC NULLS LAST, ticker ASC
        LIMIT :limit OFFSET :offset
        """
    )
    fallback_sql = text(
        """
        SELECT ticker, as_of_date, score, coverage, evidence,
               layer_scores, generated_at
        FROM tps_snapshots
        WHERE as_of_date = (
            SELECT MAX(as_of_date) FROM tps_snapshots
        )
          AND score IS NOT NULL
        ORDER BY score DESC NULLS LAST, ticker ASC
        LIMIT :limit OFFSET :offset
        """
    )
    count_sql = text(
        """
        SELECT COUNT(*) FROM tps_snapshots
        WHERE as_of_date = (SELECT MAX(as_of_date) FROM tps_snapshots)
          AND score IS NOT NULL
        """
    )

    with engine.connect() as conn:
        try:
            rows = conn.execute(
                sql, {"limit": limit, "offset": offset, "min_layers": min_coverage_layers}
            ).fetchall()
        except Exception:
            # coverage_layer_count() helper not present — fall back to
            # the simpler ordering and filter coverage in Python.
            rows = conn.execute(
                fallback_sql, {"limit": limit, "offset": offset}
            ).fetchall()
        try:
            total = conn.execute(count_sql).fetchone()[0]
        except Exception:
            total = len(rows)

    entries: list[dict[str, Any]] = []
    for row in rows:
        entry = _row_to_entry(row)
        cov = entry.get("coverage") or {}
        if isinstance(cov, dict):
            layer_count = sum(1 for v in cov.values() if v)
            if layer_count < min_coverage_layers:
                continue
            entry["coverage_layer_count"] = layer_count
        entries.append(entry)

    return {
        "entries": entries,
        "total": int(total or 0),
        "limit": limit,
        "offset": offset,
        "has_more": (offset + limit) < int(total or 0),
        "as_of": entries[0]["as_of_date"] if entries else today.isoformat(),
    }


@router.get("/{ticker}")
def get_ticker(
    ticker: str,
    live: bool = Query(default=False),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return the most-recent snapshot for ``ticker`` (or live-recompute).

    Pass ``?live=true`` to bypass the snapshot and recompute now (used by
    the per-row drill-down in the PWA so the user always sees fresh data
    after they click a ticker — at a cost of ~50ms of DB time).
    """
    engine = get_db_engine()
    ticker = ticker.strip().upper()
    if not ticker or len(ticker) > 16 or not ticker.replace(".", "").isalnum():
        raise HTTPException(status_code=400, detail="invalid ticker")

    if live:
        result = compute_tps_for_ticker(engine, ticker)
        try:
            persist_snapshot(engine, result)
        except Exception:
            pass
        return _result_to_dict(result)

    sql = text(
        """
        SELECT ticker, as_of_date, score, coverage, evidence,
               layer_scores, generated_at
        FROM tps_snapshots
        WHERE ticker = :ticker
        ORDER BY as_of_date DESC
        LIMIT 1
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {"ticker": ticker}).fetchone()

    if row is None:
        # Fall through to a live recompute so a never-snapshotted ticker
        # still returns something defensible (with score=None if no data).
        result = compute_tps_for_ticker(engine, ticker)
        return _result_to_dict(result)

    return _row_to_entry(row)


def _result_to_dict(result: Any) -> dict[str, Any]:
    return {
        "ticker": result.ticker,
        "as_of_date": result.as_of,
        "score": result.score,
        "coverage": dict(result.coverage),
        "layer_scores": dict(result.layer_scores),
        "evidence": [
            {
                "layer": e.layer,
                "source": e.source,
                "detail": e.detail,
                "amount_usd": e.amount_usd,
                "observed_at": e.observed_at,
            }
            for e in result.evidence
        ],
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }
