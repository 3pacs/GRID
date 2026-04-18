"""Canvas sub-router: board CRUD endpoints."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from loguru import logger as log
from pydantic import BaseModel
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(tags=["canvas"])


# ── Request schemas ──────────────────────────────────────────────────────

class BoardCreate(BaseModel):
    name: str
    description: str | None = None


class BoardUpdate(BaseModel):
    name: str | None = None
    description: str | None = None


# ── Helpers ──────────────────────────────────────────────────────────────

def _row_to_dict(row: Any) -> dict:
    """Convert a SQLAlchemy row to a plain dict with ISO timestamps."""
    d = dict(row._mapping)
    for key in ("created_at", "updated_at"):
        if key in d and isinstance(d[key], datetime):
            d[key] = d[key].isoformat()
    return d


# ── Endpoints ────────────────────────────────────────────────────────────

@router.get("/boards")
async def list_boards(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    _token: str = Depends(require_auth),
) -> dict:
    """Return all canvas boards with pagination."""
    engine = get_db_engine()

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT * FROM canvas_boards"
                " ORDER BY updated_at DESC"
                " LIMIT :limit OFFSET :offset"
            ),
            {"limit": limit, "offset": offset},
        ).fetchall()

        total = conn.execute(
            text("SELECT COUNT(*) FROM canvas_boards")
        ).fetchone()[0]

    items = [_row_to_dict(row) for row in rows]
    return {
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": (offset + limit) < total,
    }


@router.post("/boards", status_code=201)
async def create_board(
    body: BoardCreate,
    _token: str = Depends(require_auth),
) -> dict:
    """Create a new canvas board."""
    if not body.name or not body.name.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Board name cannot be empty",
        )

    engine = get_db_engine()

    with engine.begin() as conn:
        row = conn.execute(
            text(
                "INSERT INTO canvas_boards (name, description)"
                " VALUES (:name, :description)"
                " RETURNING id, name, description, created_at, updated_at"
            ),
            {"name": body.name.strip(), "description": body.description},
        ).fetchone()

    board = _row_to_dict(row)
    board_id = board["id"]
    board_name = board["name"]
    log.info("Canvas board created: {id} — {name}", id=board_id, name=board_name)

    # Auto-populate: seed the board with a matching entity
    try:
        _auto_seed_board(engine, board_id, board_name)
    except Exception as exc:
        log.debug("Auto-seed failed for board {id}: {e}", id=board_id, e=str(exc))

    return board


def _auto_seed_board(engine, board_id: int, query: str) -> None:
    """Seed a new board with matching entity + intelligence context.

    Every board gets:
    1. A matched company/actor (if board name matches anything)
    2. Key market indicators (SPX, VIX, DXY, 10Y)
    3. Recent breaking news signals
    4. Hot signals (insider trades, congressional moves, etc.)
    """
    import json
    import uuid

    def _add_node(conn, bid, node_type, label, x, y, data_dict):
        nid = f"{node_type}-{uuid.uuid4().hex[:8]}"
        conn.execute(
            text("INSERT INTO canvas_nodes (node_id, board_id, node_type, label, position_x, position_y, data)"
                 " VALUES (:nid, :bid, :ntype, :label, :x, :y, :data)"
                 " ON CONFLICT (node_id) DO NOTHING"),
            {"nid": nid, "bid": bid, "ntype": node_type, "label": label,
             "x": x, "y": y, "data": json.dumps(data_dict)},
        )
        return nid

    with engine.begin() as conn:
        center_nid = None

        # ── 1. Match company or actor from board name ──
        cp = conn.execute(
            text("""
                SELECT ticker, name, sector FROM company_profiles
                WHERE ticker ILIKE :q OR name ILIKE :pq
                ORDER BY
                    CASE WHEN LOWER(ticker) = LOWER(:q) THEN 0
                         WHEN LOWER(name) = LOWER(:exact) THEN 1
                         ELSE 2 END,
                    LENGTH(name) ASC
                LIMIT 1
            """),
            {"q": query.strip(), "pq": f"%{query.strip()}%", "exact": query.strip()},
        ).fetchone()

        if cp:
            m = cp._mapping
            center_nid = _add_node(conn, board_id, "company", m["name"] or m["ticker"],
                                   500, 300, {"ticker": m["ticker"], "sector": m["sector"],
                                              "entityId": f"corp_{m['ticker']}"})
            log.info("Auto-seed: placed company {t} on board {b}", t=m["ticker"], b=board_id)
        else:
            actor = conn.execute(
                text("""
                    SELECT id, name, category FROM actors
                    WHERE name ILIKE :pq
                    AND category NOT IN ('icij_entity', 'icij_officer', 'icij_intermediary')
                    ORDER BY
                        CASE WHEN LOWER(name) = LOWER(:exact) THEN 0 ELSE 1 END,
                        CASE WHEN category IN ('government','corporate','institutional') THEN 0 ELSE 1 END,
                        LENGTH(name) ASC
                    LIMIT 1
                """),
                {"pq": f"%{query.strip()}%", "exact": query.strip()},
            ).fetchone()

            if actor:
                m = actor._mapping
                center_nid = _add_node(conn, board_id, "actor", m["name"],
                                       500, 300, {"entityId": str(m["id"]),
                                                  "category": m["category"] or ""})
                log.info("Auto-seed: placed actor {n} on board {b}", n=m["name"], b=board_id)

        # ── 2. Key market indicators (right side) ──
        indicators = [
            ("SPX", "S&P 500", "spx"),
            ("VIX", "Volatility Index", "vix"),
            ("DXY", "US Dollar Index", "dollar_index"),
            ("US10Y", "10Y Treasury Yield", "treasury_10y"),
        ]
        for i, (ticker, label, feature) in enumerate(indicators):
            # Try to get latest value
            val_row = conn.execute(text(
                "SELECT value FROM raw_series "
                "WHERE series_id LIKE :pat AND pull_status = 'SUCCESS' "
                "ORDER BY obs_date DESC LIMIT 1"
            ), {"pat": f"%{feature}%"}).fetchone()
            val = float(val_row[0]) if val_row else None
            _add_node(conn, board_id, "signal", f"{ticker}: {val:.2f}" if val else ticker,
                      850, 150 + i * 100,
                      {"signal_type": "indicator", "ticker": ticker,
                       "direction": "neutral", "magnitude": val,
                       "feature": feature})

        # ── 3. Recent breaking news (left side) ──
        try:
            news_rows = conn.execute(text("""
                SELECT DISTINCT ON (headline) headline, source, ticker,
                       signal_date, direction, confidence
                FROM signal_data
                WHERE signal_type IN ('news', 'breaking_news', 'tiingo_news',
                                      'marketwatch_news', 'polygon_news')
                AND signal_date >= CURRENT_DATE - INTERVAL '3 days'
                AND headline IS NOT NULL AND headline != ''
                ORDER BY headline, signal_date DESC
                LIMIT 5
            """)).fetchall()
        except Exception:
            news_rows = []

        for i, nr in enumerate(news_rows):
            headline = nr[0][:80] if nr[0] else "Breaking"
            _add_node(conn, board_id, "news", headline,
                      100, 100 + i * 110,
                      {"title": nr[0], "source": nr[1] or "GRID",
                       "ticker": nr[2], "published_at": str(nr[3]) if nr[3] else None,
                       "direction": nr[4], "confidence": nr[5]})

        # ── 4. Hot signals — insiders + congress moves (bottom) ──
        try:
            hot_signals = conn.execute(text("""
                SELECT actor, ticker, signal_type, direction, magnitude,
                       signal_date, confidence
                FROM signal_data
                WHERE signal_type IN ('insider', 'quiverquant:insider',
                                      'congressional', 'quiverquant:house',
                                      'quiverquant:senate', 'darkpool')
                AND signal_date >= CURRENT_DATE - INTERVAL '7 days'
                AND actor IS NOT NULL AND ticker IS NOT NULL
                ORDER BY magnitude DESC NULLS LAST
                LIMIT 6
            """)).fetchall()
        except Exception:
            hot_signals = []

        for i, sig in enumerate(hot_signals):
            sig_type = sig[2] or "signal"
            label_parts = []
            if "insider" in sig_type:
                label_parts.append("INSIDER")
            elif "house" in sig_type or "senate" in sig_type or "congressional" in sig_type:
                label_parts.append("CONGRESS")
            elif "darkpool" in sig_type:
                label_parts.append("DARK POOL")
            else:
                label_parts.append(sig_type.upper())
            label_parts.append(f"{sig[0][:20]} → {sig[1]}")
            if sig[4]:
                try:
                    label_parts.append(f"${float(sig[4]):,.0f}")
                except (ValueError, TypeError):
                    pass

            _add_node(conn, board_id, "signal", " | ".join(label_parts),
                      150 + i * 140, 550,
                      {"signal_type": sig_type, "actor": sig[0], "ticker": sig[1],
                       "direction": sig[3], "magnitude": float(sig[4]) if sig[4] else None,
                       "date": str(sig[5]) if sig[5] else None,
                       "confidence": sig[6]})

        seed_count = (1 if center_nid else 0) + len(indicators) + len(news_rows) + len(hot_signals)
        log.info("Auto-seed: placed {n} nodes on board {b}", n=seed_count, b=board_id)


@router.get("/boards/{board_id}")
async def get_board(
    board_id: str,
    _token: str = Depends(require_auth),
) -> dict:
    """Return a single board with all its nodes and edges."""
    engine = get_db_engine()

    with engine.connect() as conn:
        board_row = conn.execute(
            text("SELECT * FROM canvas_boards WHERE id = :board_id"),
            {"board_id": board_id},
        ).fetchone()

        if board_row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Board {board_id} not found",
            )

        nodes = conn.execute(
            text(
                "SELECT * FROM canvas_nodes"
                " WHERE board_id = :board_id"
                " ORDER BY created_at"
            ),
            {"board_id": board_id},
        ).fetchall()

        edges = conn.execute(
            text(
                "SELECT * FROM canvas_edges"
                " WHERE board_id = :board_id"
                " ORDER BY created_at"
            ),
            {"board_id": board_id},
        ).fetchall()

    board = _row_to_dict(board_row)
    board["nodes"] = [_row_to_dict(n) for n in nodes]
    board["edges"] = [_row_to_dict(e) for e in edges]
    return board


@router.put("/boards/{board_id}")
async def update_board(
    board_id: str,
    body: BoardUpdate,
    _token: str = Depends(require_auth),
) -> dict:
    """Update board name and/or description."""
    engine = get_db_engine()

    with engine.begin() as conn:
        # Check existence
        existing = conn.execute(
            text("SELECT id FROM canvas_boards WHERE id = :board_id"),
            {"board_id": board_id},
        ).fetchone()

        if existing is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Board {board_id} not found",
            )

        # Build SET clause dynamically from provided fields
        updates: dict[str, Any] = {"board_id": board_id}
        set_parts: list[str] = ["updated_at = NOW()"]

        if body.name is not None:
            if not body.name.strip():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Board name cannot be empty",
                )
            set_parts.append("name = :name")
            updates["name"] = body.name.strip()

        if body.description is not None:
            set_parts.append("description = :description")
            updates["description"] = body.description

        # set_parts is built from hardcoded column names; user values are bind params
        set_clause = ", ".join(set_parts)
        update_sql = (
            "UPDATE canvas_boards SET " + set_clause
            + " WHERE id = :board_id"
            + " RETURNING id, name, description, created_at, updated_at"
        )
        row = conn.execute(text(update_sql), updates).fetchone()

    board = _row_to_dict(row)
    log.info("Canvas board updated: {id}", id=board["id"])
    return board


@router.delete("/boards/{board_id}")
async def delete_board(
    board_id: str,
    _token: str = Depends(require_auth),
) -> dict:
    """Delete a board and all its nodes/edges (CASCADE)."""
    engine = get_db_engine()

    with engine.begin() as conn:
        result = conn.execute(
            text("DELETE FROM canvas_boards WHERE id = :board_id RETURNING id"),
            {"board_id": board_id},
        ).fetchone()

    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Board {board_id} not found",
        )

    log.info("Canvas board deleted: {id}", id=board_id)
    return {"status": "deleted", "id": board_id}
