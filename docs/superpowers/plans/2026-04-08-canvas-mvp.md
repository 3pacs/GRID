# Canvas MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-[[development]] (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a React Flow investigation workspace where users drag actors, companies, hypotheses, signals, and notes onto a canvas, connect them, and save investigation boards.

**[[architecture|Architecture]]:** Three new DB tables (canvas_boards, canvas_nodes, canvas_edges). [[FastAPI]] backend with facade + 2 sub-routers following the watchlist split pattern. React Flow frontend with custom node types per entity, a [[Zustand]] store slice, and a reusable "Send to Canvas" button wired into existing views.

**Tech Stack:** @xyflow/react (React Flow v12), [[Zustand]], [[FastAPI]], [[SQLAlchemy]] text() queries, [[PostgreSQL]]

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Create | `migrations/versions/a1b2c3d4e5f6_canvas_tables.py` | 3 canvas tables + indexes |
| Create | `api/routers/canvas.py` | Facade router (includes sub-routers) |
| Create | `api/routers/canvas_core.py` | Board CRUD endpoints |
| Create | `api/routers/canvas_graph.py` | Node + edge CRUD endpoints |
| Create | `pwa/src/stores/canvasStore.js` | Zustand slice for canvas state |
| Create | `pwa/src/views/Canvas.jsx` | Main canvas view with React Flow |
| Create | `pwa/src/components/canvas/ActorNode.jsx` | Custom actor node type |
| Create | `pwa/src/components/canvas/CompanyNode.jsx` | Custom company node type |
| Create | `pwa/src/components/canvas/HypothesisNode.jsx` | Custom hypothesis node type |
| Create | `pwa/src/components/canvas/SignalNode.jsx` | Custom signal node type |
| Create | `pwa/src/components/canvas/NoteNode.jsx` | Custom note (free text) node type |
| Create | `pwa/src/components/canvas/nodeStyles.js` | Shared node styling constants |
| Create | `pwa/src/components/SendToCanvas.jsx` | Reusable "Send to Canvas" button |
| Create | `tests/test_canvas_api.py` | Backend API tests |
| Modify | `pwa/src/api.js` | Add canvas API methods |
| Modify | `pwa/src/routes.js` | Add canvas route entry |
| Modify | `pwa/src/app.jsx` | Add canvas lazy import |
| Modify | `api/main.py:412-459` | Register canvas router |
| Modify | `pwa/package.json` | Add @xyflow/react dependency |

---

### Task 1: Database Migration — Canvas Tables

**Files:**
- Create: `migrations/versions/a1b2c3d4e5f6_canvas_tables.py`

- [ ] **Step 1: Create the migration file**

```python
"""canvas investigation boards

Revision ID: a1b2c3d4e5f6
Revises: f1a2b3c4d5e6
Create Date: 2026-04-08 12:00:00.000000

Creates 3 tables for the canvas investigation workspace:
  - canvas_boards: named investigation boards
  - canvas_nodes: positioned entities on a board
  - canvas_edges: connections between nodes
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, Sequence[str], None] = 'f1a2b3c4d5e6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(sa.text("""
        CREATE TABLE IF NOT EXISTS canvas_boards (
            id          SERIAL PRIMARY KEY,
            name        TEXT NOT NULL,
            description TEXT DEFAULT '',
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS canvas_nodes (
            id          SERIAL PRIMARY KEY,
            board_id    INTEGER NOT NULL REFERENCES canvas_boards(id) ON DELETE CASCADE,
            node_id     TEXT NOT NULL,
            node_type   TEXT NOT NULL CHECK (node_type IN ('actor','company','hypothesis','signal','note')),
            entity_id   INTEGER,
            label       TEXT NOT NULL DEFAULT '',
            position_x  DOUBLE PRECISION NOT NULL DEFAULT 0,
            position_y  DOUBLE PRECISION NOT NULL DEFAULT 0,
            width       DOUBLE PRECISION,
            height      DOUBLE PRECISION,
            data        JSONB DEFAULT '{}',
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT uq_canvas_node UNIQUE (board_id, node_id)
        );

        CREATE TABLE IF NOT EXISTS canvas_edges (
            id              SERIAL PRIMARY KEY,
            board_id        INTEGER NOT NULL REFERENCES canvas_boards(id) ON DELETE CASCADE,
            edge_id         TEXT NOT NULL,
            source_node_id  TEXT NOT NULL,
            target_node_id  TEXT NOT NULL,
            edge_type       TEXT DEFAULT 'default',
            label           TEXT DEFAULT '',
            data            JSONB DEFAULT '{}',
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT uq_canvas_edge UNIQUE (board_id, edge_id)
        );

        CREATE INDEX IF NOT EXISTS idx_canvas_nodes_board ON canvas_nodes(board_id);
        CREATE INDEX IF NOT EXISTS idx_canvas_edges_board ON canvas_edges(board_id);
        CREATE INDEX IF NOT EXISTS idx_canvas_nodes_type ON canvas_nodes(node_type);
        CREATE INDEX IF NOT EXISTS idx_canvas_boards_updated ON canvas_boards(updated_at DESC);
    """))


def downgrade() -> None:
    op.execute(sa.text("""
        DROP TABLE IF EXISTS canvas_edges;
        DROP TABLE IF EXISTS canvas_nodes;
        DROP TABLE IF EXISTS canvas_boards;
    """))
```

- [ ] **Step 2: Run migration on local DB (or apply directly via psql on server)**

Run: `cd /Users/anikdang/dev/GRID && python -m alembic upgrade head`

If alembic is not configured locally, apply directly:
```bash
ssh grid-svr "cd /data/grid_v4/grid_repo && PGPASSWORD=gridmaster2026 psql -U grid -d griddb -h localhost -f -" <<'SQL'
CREATE TABLE IF NOT EXISTS canvas_boards (
    id SERIAL PRIMARY KEY, name TEXT NOT NULL, description TEXT DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS canvas_nodes (
    id SERIAL PRIMARY KEY, board_id INTEGER NOT NULL REFERENCES canvas_boards(id) ON DELETE CASCADE,
    node_id TEXT NOT NULL, node_type TEXT NOT NULL CHECK (node_type IN ('actor','company','hypothesis','signal','note')),
    entity_id INTEGER, label TEXT NOT NULL DEFAULT '', position_x DOUBLE PRECISION NOT NULL DEFAULT 0,
    position_y DOUBLE PRECISION NOT NULL DEFAULT 0, width DOUBLE PRECISION, height DOUBLE PRECISION,
    data JSONB DEFAULT '{}', created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_canvas_node UNIQUE (board_id, node_id)
);
CREATE TABLE IF NOT EXISTS canvas_edges (
    id SERIAL PRIMARY KEY, board_id INTEGER NOT NULL REFERENCES canvas_boards(id) ON DELETE CASCADE,
    edge_id TEXT NOT NULL, source_node_id TEXT NOT NULL, target_node_id TEXT NOT NULL,
    edge_type TEXT DEFAULT 'default', label TEXT DEFAULT '', data JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), CONSTRAINT uq_canvas_edge UNIQUE (board_id, edge_id)
);
CREATE INDEX IF NOT EXISTS idx_canvas_nodes_board ON canvas_nodes(board_id);
CREATE INDEX IF NOT EXISTS idx_canvas_edges_board ON canvas_edges(board_id);
CREATE INDEX IF NOT EXISTS idx_canvas_nodes_type ON canvas_nodes(node_type);
CREATE INDEX IF NOT EXISTS idx_canvas_boards_updated ON canvas_boards(updated_at DESC);
SQL
```

- [ ] **Step 3: Verify tables exist**

```bash
ssh grid-svr "PGPASSWORD=gridmaster2026 psql -U grid -d griddb -h localhost -c '\dt canvas_*'"
```
Expected: 3 tables listed.

- [ ] **Step 4: Commit**

```bash
git add migrations/versions/a1b2c3d4e5f6_canvas_tables.py
git commit -m "feat: add canvas_boards, canvas_nodes, canvas_edges tables"
```

---

### Task 2: Backend — Canvas Board CRUD Router

**Files:**
- Create: `api/routers/canvas_core.py`

- [ ] **Step 1: Write the test file**

Create `tests/test_canvas_api.py`:

```python
"""Tests for canvas board + node + edge CRUD."""

import pytest
from sqlalchemy import text

from api.dependencies import get_db_engine


@pytest.fixture
def engine():
    return get_db_engine()


@pytest.fixture(autouse=True)
def cleanup(engine):
    """Clean canvas tables before each test."""
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM canvas_edges"))
        conn.execute(text("DELETE FROM canvas_nodes"))
        conn.execute(text("DELETE FROM canvas_boards"))
    yield
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM canvas_edges"))
        conn.execute(text("DELETE FROM canvas_nodes"))
        conn.execute(text("DELETE FROM canvas_boards"))


def _create_board(engine, name="Test Board", description=""):
    with engine.begin() as conn:
        row = conn.execute(
            text(
                "INSERT INTO canvas_boards (name, description)"
                " VALUES (:name, :desc) RETURNING id, name, description, created_at, updated_at"
            ),
            {"name": name, "desc": description},
        ).fetchone()
    return dict(row._mapping)


class TestCanvasBoards:
    def test_create_board(self, engine):
        board = _create_board(engine, "Investigation Alpha")
        assert board["name"] == "Investigation Alpha"
        assert board["id"] > 0

    def test_list_boards(self, engine):
        _create_board(engine, "Board A")
        _create_board(engine, "Board B")
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT * FROM canvas_boards ORDER BY updated_at DESC")
            ).fetchall()
        assert len(rows) >= 2

    def test_delete_board_cascades(self, engine):
        board = _create_board(engine)
        bid = board["id"]
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO canvas_nodes (board_id, node_id, node_type, label, position_x, position_y)"
                    " VALUES (:bid, 'n1', 'note', 'test', 0, 0)"
                ),
                {"bid": bid},
            )
            conn.execute(text("DELETE FROM canvas_boards WHERE id = :id"), {"id": bid})
            remaining = conn.execute(
                text("SELECT COUNT(*) FROM canvas_nodes WHERE board_id = :bid"),
                {"bid": bid},
            ).scalar()
        assert remaining == 0


class TestCanvasNodes:
    def test_add_node(self, engine):
        board = _create_board(engine)
        with engine.begin() as conn:
            row = conn.execute(
                text(
                    "INSERT INTO canvas_nodes (board_id, node_id, node_type, entity_id, label, position_x, position_y, data)"
                    " VALUES (:bid, :nid, :ntype, :eid, :label, :x, :y, :data::jsonb)"
                    " RETURNING id, node_id, node_type"
                ),
                {"bid": board["id"], "nid": "actor-42", "ntype": "actor", "eid": 42,
                 "label": "JPMorgan", "x": 100.0, "y": 200.0, "data": '{"trust_score": 0.8}'},
            ).fetchone()
        assert row._mapping["node_type"] == "actor"

    def test_unique_node_id_per_board(self, engine):
        board = _create_board(engine)
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO canvas_nodes (board_id, node_id, node_type, label, position_x, position_y)"
                    " VALUES (:bid, 'dup', 'note', 'first', 0, 0)"
                ),
                {"bid": board["id"]},
            )
        with pytest.raises(Exception):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO canvas_nodes (board_id, node_id, node_type, label, position_x, position_y)"
                        " VALUES (:bid, 'dup', 'note', 'second', 0, 0)"
                    ),
                    {"bid": board["id"]},
                )


class TestCanvasEdges:
    def test_add_edge(self, engine):
        board = _create_board(engine)
        bid = board["id"]
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO canvas_nodes (board_id, node_id, node_type, label, position_x, position_y)"
                    " VALUES (:bid, 'a', 'actor', 'A', 0, 0), (:bid, 'b', 'company', 'B', 100, 0)"
                ),
                {"bid": bid},
            )
            row = conn.execute(
                text(
                    "INSERT INTO canvas_edges (board_id, edge_id, source_node_id, target_node_id, edge_type)"
                    " VALUES (:bid, 'e1', 'a', 'b', 'influence') RETURNING id, edge_type"
                ),
                {"bid": bid},
            ).fetchone()
        assert row._mapping["edge_type"] == "influence"
```

- [ ] **Step 2: Run tests to verify they fail (tables may not exist locally)**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_canvas_api.py -v`
Expected: Tests should PASS if DB has the tables, or FAIL if tables don't exist yet.

- [ ] **Step 3: Write canvas_core.py**

```python
"""Canvas sub-router: board CRUD."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(tags=["canvas"])


def _row_to_dict(row) -> dict:
    d = dict(row._mapping)
    for k in ("created_at", "updated_at"):
        if k in d and d[k] is not None:
            d[k] = d[k].isoformat()
    return d


@router.get("/boards")
async def list_boards(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    _token: str = Depends(require_auth),
) -> dict:
    """List all canvas boards, most recently updated first."""
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
        total = conn.execute(text("SELECT COUNT(*) FROM canvas_boards")).scalar()
    return {
        "boards": [_row_to_dict(r) for r in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.post("/boards")
async def create_board(
    body: dict,
    _token: str = Depends(require_auth),
) -> dict:
    """Create a new canvas board."""
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Board name is required")
    description = body.get("description", "")
    engine = get_db_engine()
    with engine.begin() as conn:
        row = conn.execute(
            text(
                "INSERT INTO canvas_boards (name, description)"
                " VALUES (:name, :desc)"
                " RETURNING id, name, description, created_at, updated_at"
            ),
            {"name": name, "desc": description},
        ).fetchone()
    log.info("Canvas board created: {name} (id={id})", name=name, id=row._mapping["id"])
    return _row_to_dict(row)


@router.get("/boards/{board_id}")
async def get_board(
    board_id: int,
    _token: str = Depends(require_auth),
) -> dict:
    """Get a single board with all its nodes and edges."""
    engine = get_db_engine()
    with engine.connect() as conn:
        board = conn.execute(
            text("SELECT * FROM canvas_boards WHERE id = :id"),
            {"id": board_id},
        ).fetchone()
        if not board:
            raise HTTPException(status_code=404, detail="Board not found")

        nodes = conn.execute(
            text("SELECT * FROM canvas_nodes WHERE board_id = :bid ORDER BY created_at"),
            {"bid": board_id},
        ).fetchall()

        edges = conn.execute(
            text("SELECT * FROM canvas_edges WHERE board_id = :bid ORDER BY created_at"),
            {"bid": board_id},
        ).fetchall()

    return {
        **_row_to_dict(board),
        "nodes": [_row_to_dict(n) for n in nodes],
        "edges": [_row_to_dict(e) for e in edges],
    }


@router.put("/boards/{board_id}")
async def update_board(
    board_id: int,
    body: dict,
    _token: str = Depends(require_auth),
) -> dict:
    """Update board name/description."""
    engine = get_db_engine()
    with engine.begin() as conn:
        row = conn.execute(
            text(
                "UPDATE canvas_boards SET name = COALESCE(:name, name),"
                " description = COALESCE(:desc, description),"
                " updated_at = NOW()"
                " WHERE id = :id RETURNING id, name, description, created_at, updated_at"
            ),
            {"id": board_id, "name": body.get("name"), "desc": body.get("description")},
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Board not found")
    return _row_to_dict(row)


@router.delete("/boards/{board_id}")
async def delete_board(
    board_id: int,
    _token: str = Depends(require_auth),
) -> dict:
    """Delete a board and all its nodes/edges (CASCADE)."""
    engine = get_db_engine()
    with engine.begin() as conn:
        result = conn.execute(
            text("DELETE FROM canvas_boards WHERE id = :id"),
            {"id": board_id},
        )
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Board not found")
    log.info("Canvas board deleted: id={id}", id=board_id)
    return {"deleted": True, "id": board_id}
```

- [ ] **Step 4: Run tests**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_canvas_api.py -v`

- [ ] **Step 5: Commit**

```bash
git add api/routers/canvas_core.py tests/test_canvas_api.py
git commit -m "feat: canvas board CRUD router + tests"
```

---

### Task 3: Backend — Canvas Graph Router (Nodes + Edges)

**Files:**
- Create: `api/routers/canvas_graph.py`

- [ ] **Step 1: Write canvas_graph.py**

```python
"""Canvas sub-router: node and edge CRUD."""

from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(tags=["canvas"])


def _row_to_dict(row) -> dict:
    d = dict(row._mapping)
    for k in ("created_at",):
        if k in d and d[k] is not None:
            d[k] = d[k].isoformat()
    if "data" in d and isinstance(d["data"], str):
        d["data"] = json.loads(d["data"])
    return d


def _touch_board(conn, board_id: int) -> None:
    """Bump board updated_at timestamp."""
    conn.execute(
        text("UPDATE canvas_boards SET updated_at = NOW() WHERE id = :id"),
        {"id": board_id},
    )


# ── Nodes ──────────────────────────────────────────────────────────────

@router.post("/boards/{board_id}/nodes")
async def add_node(
    board_id: int,
    body: dict,
    _token: str = Depends(require_auth),
) -> dict:
    """Add a node to a canvas board."""
    node_id = body.get("node_id") or body.get("id")
    node_type = body.get("node_type") or body.get("type")
    if not node_id or not node_type:
        raise HTTPException(status_code=400, detail="node_id and node_type are required")

    engine = get_db_engine()
    with engine.begin() as conn:
        row = conn.execute(
            text(
                "INSERT INTO canvas_nodes"
                " (board_id, node_id, node_type, entity_id, label, position_x, position_y, width, height, data)"
                " VALUES (:bid, :nid, :ntype, :eid, :label, :x, :y, :w, :h, :data::jsonb)"
                " ON CONFLICT (board_id, node_id) DO UPDATE SET"
                "   position_x = EXCLUDED.position_x,"
                "   position_y = EXCLUDED.position_y,"
                "   width = EXCLUDED.width,"
                "   height = EXCLUDED.height,"
                "   data = EXCLUDED.data,"
                "   label = EXCLUDED.label"
                " RETURNING id, node_id, node_type, entity_id, label, position_x, position_y, data, created_at"
            ),
            {
                "bid": board_id,
                "nid": node_id,
                "ntype": node_type,
                "eid": body.get("entity_id"),
                "label": body.get("label", ""),
                "x": body.get("position_x", body.get("x", 0)),
                "y": body.get("position_y", body.get("y", 0)),
                "w": body.get("width"),
                "h": body.get("height"),
                "data": json.dumps(body.get("data", {})),
            },
        ).fetchone()
        _touch_board(conn, board_id)
    return _row_to_dict(row)


@router.put("/boards/{board_id}/nodes/{node_id}")
async def update_node(
    board_id: int,
    node_id: str,
    body: dict,
    _token: str = Depends(require_auth),
) -> dict:
    """Update a node's position, label, or data."""
    engine = get_db_engine()
    with engine.begin() as conn:
        row = conn.execute(
            text(
                "UPDATE canvas_nodes SET"
                " position_x = COALESCE(:x, position_x),"
                " position_y = COALESCE(:y, position_y),"
                " width = COALESCE(:w, width),"
                " height = COALESCE(:h, height),"
                " label = COALESCE(:label, label),"
                " data = COALESCE(:data::jsonb, data)"
                " WHERE board_id = :bid AND node_id = :nid"
                " RETURNING id, node_id, node_type, entity_id, label, position_x, position_y, data, created_at"
            ),
            {
                "bid": board_id,
                "nid": node_id,
                "x": body.get("position_x", body.get("x")),
                "y": body.get("position_y", body.get("y")),
                "w": body.get("width"),
                "h": body.get("height"),
                "label": body.get("label"),
                "data": json.dumps(body.get("data")) if "data" in body else None,
            },
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Node not found")
        _touch_board(conn, board_id)
    return _row_to_dict(row)


@router.delete("/boards/{board_id}/nodes/{node_id}")
async def delete_node(
    board_id: int,
    node_id: str,
    _token: str = Depends(require_auth),
) -> dict:
    """Delete a node and its connected edges."""
    engine = get_db_engine()
    with engine.begin() as conn:
        # Remove edges referencing this node
        conn.execute(
            text(
                "DELETE FROM canvas_edges"
                " WHERE board_id = :bid AND (source_node_id = :nid OR target_node_id = :nid)"
            ),
            {"bid": board_id, "nid": node_id},
        )
        result = conn.execute(
            text("DELETE FROM canvas_nodes WHERE board_id = :bid AND node_id = :nid"),
            {"bid": board_id, "nid": node_id},
        )
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Node not found")
        _touch_board(conn, board_id)
    return {"deleted": True, "node_id": node_id}


# ── Edges ──────────────────────────────────────────────────────────────

@router.post("/boards/{board_id}/edges")
async def add_edge(
    board_id: int,
    body: dict,
    _token: str = Depends(require_auth),
) -> dict:
    """Add an edge between two nodes."""
    edge_id = body.get("edge_id") or body.get("id")
    source = body.get("source_node_id") or body.get("source")
    target = body.get("target_node_id") or body.get("target")
    if not edge_id or not source or not target:
        raise HTTPException(status_code=400, detail="edge_id, source, and target are required")

    engine = get_db_engine()
    with engine.begin() as conn:
        row = conn.execute(
            text(
                "INSERT INTO canvas_edges"
                " (board_id, edge_id, source_node_id, target_node_id, edge_type, label, data)"
                " VALUES (:bid, :eid, :src, :tgt, :etype, :label, :data::jsonb)"
                " ON CONFLICT (board_id, edge_id) DO UPDATE SET"
                "   edge_type = EXCLUDED.edge_type,"
                "   label = EXCLUDED.label,"
                "   data = EXCLUDED.data"
                " RETURNING id, edge_id, source_node_id, target_node_id, edge_type, label, data, created_at"
            ),
            {
                "bid": board_id,
                "eid": edge_id,
                "src": source,
                "tgt": target,
                "etype": body.get("edge_type", "default"),
                "label": body.get("label", ""),
                "data": json.dumps(body.get("data", {})),
            },
        ).fetchone()
        _touch_board(conn, board_id)
    return _row_to_dict(row)


@router.delete("/boards/{board_id}/edges/{edge_id}")
async def delete_edge(
    board_id: int,
    edge_id: str,
    _token: str = Depends(require_auth),
) -> dict:
    """Delete an edge."""
    engine = get_db_engine()
    with engine.begin() as conn:
        result = conn.execute(
            text("DELETE FROM canvas_edges WHERE board_id = :bid AND edge_id = :eid"),
            {"bid": board_id, "eid": edge_id},
        )
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Edge not found")
        _touch_board(conn, board_id)
    return {"deleted": True, "edge_id": edge_id}


# ── Bulk save ──────────────────────────────────────────────────────────

@router.put("/boards/{board_id}/graph")
async def save_graph(
    board_id: int,
    body: dict,
    _token: str = Depends(require_auth),
) -> dict:
    """Bulk save all nodes and edges for a board (full replace)."""
    nodes = body.get("nodes", [])
    edges = body.get("edges", [])
    engine = get_db_engine()

    with engine.begin() as conn:
        # Verify board exists
        board = conn.execute(
            text("SELECT id FROM canvas_boards WHERE id = :id"), {"id": board_id}
        ).fetchone()
        if not board:
            raise HTTPException(status_code=404, detail="Board not found")

        # Clear existing
        conn.execute(text("DELETE FROM canvas_edges WHERE board_id = :bid"), {"bid": board_id})
        conn.execute(text("DELETE FROM canvas_nodes WHERE board_id = :bid"), {"bid": board_id})

        # Insert nodes
        for n in nodes:
            conn.execute(
                text(
                    "INSERT INTO canvas_nodes"
                    " (board_id, node_id, node_type, entity_id, label, position_x, position_y, width, height, data)"
                    " VALUES (:bid, :nid, :ntype, :eid, :label, :x, :y, :w, :h, :data::jsonb)"
                ),
                {
                    "bid": board_id,
                    "nid": n.get("node_id") or n.get("id"),
                    "ntype": n.get("node_type") or n.get("type", "note"),
                    "eid": n.get("entity_id"),
                    "label": n.get("label", ""),
                    "x": n.get("position_x", n.get("x", 0)),
                    "y": n.get("position_y", n.get("y", 0)),
                    "w": n.get("width"),
                    "h": n.get("height"),
                    "data": json.dumps(n.get("data", {})),
                },
            )

        # Insert edges
        for e in edges:
            conn.execute(
                text(
                    "INSERT INTO canvas_edges"
                    " (board_id, edge_id, source_node_id, target_node_id, edge_type, label, data)"
                    " VALUES (:bid, :eid, :src, :tgt, :etype, :label, :data::jsonb)"
                ),
                {
                    "bid": board_id,
                    "eid": e.get("edge_id") or e.get("id"),
                    "src": e.get("source_node_id") or e.get("source"),
                    "tgt": e.get("target_node_id") or e.get("target"),
                    "etype": e.get("edge_type", "default"),
                    "label": e.get("label", ""),
                    "data": json.dumps(e.get("data", {})),
                },
            )

        _touch_board(conn, board_id)

    log.info("Canvas graph saved: board={bid}, {nn} nodes, {ne} edges",
             bid=board_id, nn=len(nodes), ne=len(edges))
    return {"saved": True, "nodes": len(nodes), "edges": len(edges)}
```

- [ ] **Step 2: Create the facade router**

Create `api/routers/canvas.py`:

```python
"""Canvas API — facade router.

Sub-routers:
  canvas_core.py   — board CRUD (list, create, get, update, delete)
  canvas_graph.py  — node/edge CRUD + bulk save
"""

from __future__ import annotations

from fastapi import APIRouter

from api.routers.canvas_core import router as _core_router
from api.routers.canvas_graph import router as _graph_router

router = APIRouter(prefix="/api/v1/canvas", tags=["canvas"])

router.include_router(_core_router)
router.include_router(_graph_router)
```

- [ ] **Step 3: Register in api/main.py**

Add to the router list in `api/main.py` (after the `"sse"` entry around line 459):

```python
    ("canvas", "api.routers.canvas", False),
```

- [ ] **Step 4: Run tests**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_canvas_api.py -v`

- [ ] **Step 5: Commit**

```bash
git add api/routers/canvas.py api/routers/canvas_graph.py api/main.py
git commit -m "feat: canvas node/edge CRUD + facade router + register in main"
```

---

### Task 4: Frontend — Install React Flow + Canvas Store

**Files:**
- Modify: `pwa/package.json`
- Create: `pwa/src/stores/canvasStore.js`

- [ ] **Step 1: Install @xyflow/react**

```bash
cd /Users/anikdang/dev/GRID/pwa && npm install @xyflow/react
```

- [ ] **Step 2: Create canvasStore.js**

```javascript
/**
 * Canvas store slice — board state, nodes, edges, selection.
 */
import { create } from 'zustand';

const useCanvasStore = create((set, get) => ({
    // Board list
    boards: [],
    currentBoardId: null,

    // Graph state (React Flow format)
    nodes: [],
    edges: [],

    // Selection
    selectedNodeId: null,
    selectedEdgeId: null,

    // Loading
    loading: false,

    // Setters
    setBoards: (boards) => set({ boards }),
    setCurrentBoardId: (id) => set({ currentBoardId: id }),
    setNodes: (nodes) => set({ nodes }),
    setEdges: (edges) => set({ edges }),
    setSelectedNodeId: (id) => set({ selectedNodeId: id }),
    setSelectedEdgeId: (id) => set({ selectedEdgeId: id }),
    setLoading: (loading) => set({ loading }),

    // Node operations (immutable)
    updateNodePosition: (nodeId, position) => set((state) => ({
        nodes: state.nodes.map((n) =>
            n.id === nodeId ? { ...n, position } : n
        ),
    })),

    addNode: (node) => set((state) => ({
        nodes: [...state.nodes, node],
    })),

    removeNode: (nodeId) => set((state) => ({
        nodes: state.nodes.filter((n) => n.id !== nodeId),
        edges: state.edges.filter((e) => e.source !== nodeId && e.target !== nodeId),
    })),

    addEdge: (edge) => set((state) => ({
        edges: [...state.edges, edge],
    })),

    removeEdge: (edgeId) => set((state) => ({
        edges: state.edges.filter((e) => e.id !== edgeId),
    })),

    // Convert DB rows to React Flow format
    loadGraph: (dbNodes, dbEdges) => set({
        nodes: dbNodes.map((n) => ({
            id: n.node_id,
            type: n.node_type,
            position: { x: n.position_x, y: n.position_y },
            data: { label: n.label, entityId: n.entity_id, ...n.data },
            ...(n.width ? { width: n.width } : {}),
            ...(n.height ? { height: n.height } : {}),
        })),
        edges: dbEdges.map((e) => ({
            id: e.edge_id,
            source: e.source_node_id,
            target: e.target_node_id,
            type: e.edge_type || 'default',
            label: e.label || '',
            data: e.data || {},
        })),
    }),

    // Convert React Flow state back to DB format for saving
    toDbFormat: () => {
        const { nodes, edges } = get();
        return {
            nodes: nodes.map((n) => ({
                node_id: n.id,
                node_type: n.type,
                entity_id: n.data?.entityId || null,
                label: n.data?.label || '',
                position_x: n.position?.x || 0,
                position_y: n.position?.y || 0,
                width: n.width || null,
                height: n.height || null,
                data: n.data || {},
            })),
            edges: edges.map((e) => ({
                edge_id: e.id,
                source_node_id: e.source,
                target_node_id: e.target,
                edge_type: e.type || 'default',
                label: e.label || '',
                data: e.data || {},
            })),
        };
    },
}));

export default useCanvasStore;
```

- [ ] **Step 3: Commit**

```bash
git add pwa/package.json pwa/package-lock.json pwa/src/stores/canvasStore.js
git commit -m "feat: install @xyflow/react + canvas Zustand store"
```

---

### Task 5: Frontend — Custom Node Types

**Files:**
- Create: `pwa/src/components/canvas/nodeStyles.js`
- Create: `pwa/src/components/canvas/ActorNode.jsx`
- Create: `pwa/src/components/canvas/CompanyNode.jsx`
- Create: `pwa/src/components/canvas/HypothesisNode.jsx`
- Create: `pwa/src/components/canvas/SignalNode.jsx`
- Create: `pwa/src/components/canvas/NoteNode.jsx`

- [ ] **Step 1: Create shared node styles**

```javascript
// pwa/src/components/canvas/nodeStyles.js
export const NODE_COLORS = {
    actor:      { bg: '#1A1030', border: '#8B5CF6', accent: '#A78BFA' },
    company:    { bg: '#0A1A2E', border: '#3B82F6', accent: '#60A5FA' },
    hypothesis: { bg: '#0A2018', border: '#10B981', accent: '#34D399' },
    signal:     { bg: '#1A1508', border: '#F59E0B', accent: '#FBBF24' },
    note:       { bg: '#1A1A1A', border: '#6B7280', accent: '#9CA3AF' },
};

export const baseNodeStyle = {
    padding: '10px 14px',
    borderRadius: '8px',
    fontSize: '12px',
    fontFamily: "'IBM Plex Sans', sans-serif",
    color: '#C8D8E8',
    minWidth: '160px',
    maxWidth: '260px',
};

export const labelStyle = {
    fontWeight: 600,
    fontSize: '13px',
    marginBottom: '4px',
    whiteSpace: 'nowrap',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
};

export const metaStyle = {
    fontSize: '11px',
    opacity: 0.7,
    lineHeight: '1.4',
};

export const badgeStyle = (color) => ({
    display: 'inline-block',
    padding: '1px 6px',
    borderRadius: '4px',
    fontSize: '10px',
    fontWeight: 600,
    background: color + '30',
    color: color,
    marginRight: '4px',
});
```

- [ ] **Step 2: Create ActorNode.jsx**

```jsx
import React from 'react';
import { Handle, Position } from '@xyflow/react';
import { NODE_COLORS, baseNodeStyle, labelStyle, metaStyle, badgeStyle } from './nodeStyles.js';

const c = NODE_COLORS.actor;

export default function ActorNode({ data }) {
    return (
        <div style={{ ...baseNodeStyle, background: c.bg, border: `1px solid ${c.border}` }}>
            <Handle type="target" position={Position.Left} style={{ background: c.accent }} />
            <div style={labelStyle}>{data.label || 'Actor'}</div>
            <div style={metaStyle}>
                {data.category && <span style={badgeStyle(c.accent)}>{data.category}</span>}
                {data.trust_score != null && <span>Trust: {(data.trust_score * 100).toFixed(0)}%</span>}
            </div>
            <Handle type="source" position={Position.Right} style={{ background: c.accent }} />
        </div>
    );
}
```

- [ ] **Step 3: Create CompanyNode.jsx**

```jsx
import React from 'react';
import { Handle, Position } from '@xyflow/react';
import { NODE_COLORS, baseNodeStyle, labelStyle, metaStyle, badgeStyle } from './nodeStyles.js';

const c = NODE_COLORS.company;

export default function CompanyNode({ data }) {
    return (
        <div style={{ ...baseNodeStyle, background: c.bg, border: `1px solid ${c.border}` }}>
            <Handle type="target" position={Position.Left} style={{ background: c.accent }} />
            <div style={labelStyle}>{data.label || 'Company'}</div>
            <div style={metaStyle}>
                {data.ticker && <span style={badgeStyle(c.accent)}>{data.ticker}</span>}
                {data.sector && <span>{data.sector}</span>}
            </div>
            <Handle type="source" position={Position.Right} style={{ background: c.accent }} />
        </div>
    );
}
```

- [ ] **Step 4: Create HypothesisNode.jsx**

```jsx
import React from 'react';
import { Handle, Position } from '@xyflow/react';
import { NODE_COLORS, baseNodeStyle, labelStyle, metaStyle, badgeStyle } from './nodeStyles.js';

export default function HypothesisNode({ data }) {
    const isAntithesis = data.role === 'antithesis';
    const c = isAntithesis
        ? { bg: '#1A0A0A', border: '#EF4444', accent: '#F87171' }
        : NODE_COLORS.hypothesis;

    return (
        <div style={{ ...baseNodeStyle, background: c.bg, border: `1px solid ${c.border}`, maxWidth: '300px' }}>
            <Handle type="target" position={Position.Left} style={{ background: c.accent }} />
            <div style={labelStyle}>{isAntithesis ? 'ANTI' : 'THESIS'}</div>
            <div style={{ ...metaStyle, whiteSpace: 'normal' }}>
                {data.label || data.thesis || ''}
            </div>
            {data.confidence != null && (
                <div style={{ marginTop: '4px' }}>
                    <span style={badgeStyle(c.accent)}>
                        {(data.confidence * 100).toFixed(0)}%
                    </span>
                    {data.status && <span style={badgeStyle('#6B7280')}>{data.status}</span>}
                </div>
            )}
            <Handle type="source" position={Position.Right} style={{ background: c.accent }} />
        </div>
    );
}
```

- [ ] **Step 5: Create SignalNode.jsx**

```jsx
import React from 'react';
import { Handle, Position } from '@xyflow/react';
import { NODE_COLORS, baseNodeStyle, labelStyle, metaStyle, badgeStyle } from './nodeStyles.js';

const c = NODE_COLORS.signal;

export default function SignalNode({ data }) {
    const dirColor = data.direction === 'bullish' ? '#10B981' : data.direction === 'bearish' ? '#EF4444' : c.accent;
    return (
        <div style={{ ...baseNodeStyle, background: c.bg, border: `1px solid ${c.border}` }}>
            <Handle type="target" position={Position.Left} style={{ background: c.accent }} />
            <div style={labelStyle}>{data.label || data.signal_type || 'Signal'}</div>
            <div style={metaStyle}>
                {data.direction && <span style={badgeStyle(dirColor)}>{data.direction}</span>}
                {data.ticker && <span style={badgeStyle(c.accent)}>{data.ticker}</span>}
                {data.magnitude != null && <span>Mag: {data.magnitude.toFixed(2)}</span>}
            </div>
            <Handle type="source" position={Position.Right} style={{ background: c.accent }} />
        </div>
    );
}
```

- [ ] **Step 6: Create NoteNode.jsx**

```jsx
import React, { useState } from 'react';
import { Handle, Position } from '@xyflow/react';
import { NODE_COLORS, baseNodeStyle, labelStyle } from './nodeStyles.js';

const c = NODE_COLORS.note;

export default function NoteNode({ data }) {
    const [editing, setEditing] = useState(false);
    const [text, setText] = useState(data.label || '');

    return (
        <div
            style={{ ...baseNodeStyle, background: c.bg, border: `1px solid ${c.border}`, minWidth: '140px' }}
            onDoubleClick={() => setEditing(true)}
        >
            <Handle type="target" position={Position.Left} style={{ background: c.accent }} />
            {editing ? (
                <textarea
                    autoFocus
                    value={text}
                    onChange={(e) => setText(e.target.value)}
                    onBlur={() => { setEditing(false); data.onLabelChange?.(text); }}
                    onKeyDown={(e) => { if (e.key === 'Escape') setEditing(false); }}
                    style={{
                        background: 'transparent', color: '#C8D8E8', border: 'none',
                        outline: 'none', resize: 'vertical', width: '100%',
                        fontFamily: "'IBM Plex Sans', sans-serif", fontSize: '12px',
                        minHeight: '40px',
                    }}
                />
            ) : (
                <div style={{ ...labelStyle, whiteSpace: 'pre-wrap', fontWeight: 400 }}>
                    {text || 'Double-click to edit...'}
                </div>
            )}
            <Handle type="source" position={Position.Right} style={{ background: c.accent }} />
        </div>
    );
}
```

- [ ] **Step 7: Commit**

```bash
git add pwa/src/components/canvas/
git commit -m "feat: canvas custom node types — actor, company, hypothesis, signal, note"
```

---

### Task 6: Frontend — Canvas View + API Methods + Route Registration

**Files:**
- Create: `pwa/src/views/Canvas.jsx`
- Modify: `pwa/src/api.js` — add canvas methods
- Modify: `pwa/src/routes.js` — add canvas route
- Modify: `pwa/src/app.jsx` — add canvas lazy import

- [ ] **Step 1: Add canvas API methods to api.js**

Append these methods inside the `GRIDApi` class (before the closing `}`):

```javascript
    // ── Canvas ─────────────────────────────────────────────
    async getCanvasBoards() { return this.get('/api/v1/canvas/boards'); }
    async createCanvasBoard(name, description = '') { return this.post('/api/v1/canvas/boards', { name, description }); }
    async getCanvasBoard(boardId) { return this.get(`/api/v1/canvas/boards/${boardId}`); }
    async updateCanvasBoard(boardId, updates) { return this._fetch(`/api/v1/canvas/boards/${boardId}`, { method: 'PUT', body: JSON.stringify(updates) }); }
    async deleteCanvasBoard(boardId) { return this._fetch(`/api/v1/canvas/boards/${boardId}`, { method: 'DELETE' }); }
    async addCanvasNode(boardId, node) { return this.post(`/api/v1/canvas/boards/${boardId}/nodes`, node); }
    async updateCanvasNode(boardId, nodeId, updates) { return this._fetch(`/api/v1/canvas/boards/${boardId}/nodes/${nodeId}`, { method: 'PUT', body: JSON.stringify(updates) }); }
    async deleteCanvasNode(boardId, nodeId) { return this._fetch(`/api/v1/canvas/boards/${boardId}/nodes/${nodeId}`, { method: 'DELETE' }); }
    async addCanvasEdge(boardId, edge) { return this.post(`/api/v1/canvas/boards/${boardId}/edges`, edge); }
    async deleteCanvasEdge(boardId, edgeId) { return this._fetch(`/api/v1/canvas/boards/${boardId}/edges/${edgeId}`, { method: 'DELETE' }); }
    async saveCanvasGraph(boardId, graph) { return this._fetch(`/api/v1/canvas/boards/${boardId}/graph`, { method: 'PUT', body: JSON.stringify(graph) }); }
```

- [ ] **Step 2: Add canvas route to routes.js**

Add in the RESEARCH section (after the `valuation` entry, before the TRADING section comment):

```javascript
    {
        id: 'canvas',
        label: 'Canvas',
        icon: Grid3X3,
        component: './views/Canvas.jsx',
        group: 'research',
        nav: 'drawer',
        desc: 'Investigation boards — connect actors, signals, hypotheses',
    },
```

(`Grid3X3` is already imported in routes.js.)

- [ ] **Step 3: Add canvas lazy import to app.jsx**

Add to the `routeComponents` object (after the `'trial-gems'` entry):

```javascript
    canvas:             React.lazy(() => import('./views/Canvas.jsx')),
    valuation:          React.lazy(() => import('./views/Valuation.jsx')),
```

(Check if `valuation` is already there — add `canvas` only if needed.)

- [ ] **Step 4: Create Canvas.jsx**

```jsx
import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
    ReactFlow,
    Background,
    Controls,
    MiniMap,
    addEdge,
    useNodesState,
    useEdgesState,
    Panel,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { api } from '../api.js';
import useCanvasStore from '../stores/canvasStore.js';
import ActorNode from '../components/canvas/ActorNode.jsx';
import CompanyNode from '../components/canvas/CompanyNode.jsx';
import HypothesisNode from '../components/canvas/HypothesisNode.jsx';
import SignalNode from '../components/canvas/SignalNode.jsx';
import NoteNode from '../components/canvas/NoteNode.jsx';
import { NODE_COLORS } from '../components/canvas/nodeStyles.js';
import { Plus, Save, Trash2, FolderOpen, X } from 'lucide-react';

const nodeTypes = { actor: ActorNode, company: CompanyNode, hypothesis: HypothesisNode, signal: SignalNode, note: NoteNode };

const darkTheme = {
    bg: '#080C10',
    panel: '#0D1117',
    panelBorder: '#1E2A3A',
    text: '#C8D8E8',
    textMuted: '#5A7080',
    accent: '#3B82F6',
};

const btnStyle = {
    padding: '6px 12px', borderRadius: '6px', border: `1px solid ${darkTheme.panelBorder}`,
    background: darkTheme.panel, color: darkTheme.text, cursor: 'pointer',
    fontSize: '12px', fontFamily: "'IBM Plex Sans', sans-serif",
    display: 'flex', alignItems: 'center', gap: '4px',
};

export default function Canvas() {
    const { boards, setBoards, currentBoardId, setCurrentBoardId, setLoading } = useCanvasStore();
    const [nodes, setNodes, onNodesChange] = useNodesState([]);
    const [edges, setEdges, onEdgesChange] = useEdgesState([]);
    const [boardName, setBoardName] = useState('');
    const [showBoardList, setShowBoardList] = useState(false);
    const [dirty, setDirty] = useState(false);
    const saveTimerRef = useRef(null);

    // Load boards on mount
    useEffect(() => {
        api.getCanvasBoards().then((res) => {
            setBoards(res.boards || []);
            if (res.boards?.length && !currentBoardId) {
                loadBoard(res.boards[0].id);
            }
        }).catch(() => {});
    }, []);

    const loadBoard = useCallback(async (boardId) => {
        setLoading(true);
        try {
            const data = await api.getCanvasBoard(boardId);
            setCurrentBoardId(boardId);
            setBoardName(data.name);
            const rfNodes = (data.nodes || []).map((n) => ({
                id: n.node_id,
                type: n.node_type,
                position: { x: n.position_x, y: n.position_y },
                data: { label: n.label, entityId: n.entity_id, ...(n.data || {}) },
            }));
            const rfEdges = (data.edges || []).map((e) => ({
                id: e.edge_id,
                source: e.source_node_id,
                target: e.target_node_id,
                type: e.edge_type || 'default',
                label: e.label || '',
                animated: e.edge_type === 'influence',
            }));
            setNodes(rfNodes);
            setEdges(rfEdges);
            setDirty(false);
        } catch (err) {
            console.error('Failed to load board:', err);
        } finally {
            setLoading(false);
        }
    }, []);

    const createBoard = useCallback(async () => {
        const name = prompt('Board name:');
        if (!name?.trim()) return;
        try {
            const board = await api.createCanvasBoard(name.trim());
            setBoards([board, ...boards]);
            setCurrentBoardId(board.id);
            setBoardName(board.name);
            setNodes([]);
            setEdges([]);
            setDirty(false);
        } catch (err) {
            console.error('Failed to create board:', err);
        }
    }, [boards]);

    const deleteBoard = useCallback(async () => {
        if (!currentBoardId) return;
        if (!confirm('Delete this board?')) return;
        try {
            await api.deleteCanvasBoard(currentBoardId);
            const remaining = boards.filter((b) => b.id !== currentBoardId);
            setBoards(remaining);
            if (remaining.length) {
                loadBoard(remaining[0].id);
            } else {
                setCurrentBoardId(null);
                setNodes([]);
                setEdges([]);
            }
        } catch (err) {
            console.error('Failed to delete board:', err);
        }
    }, [currentBoardId, boards]);

    const saveGraph = useCallback(async () => {
        if (!currentBoardId) return;
        const graph = {
            nodes: nodes.map((n) => ({
                node_id: n.id, node_type: n.type, entity_id: n.data?.entityId || null,
                label: n.data?.label || '', position_x: n.position?.x || 0,
                position_y: n.position?.y || 0, data: n.data || {},
            })),
            edges: edges.map((e) => ({
                edge_id: e.id, source_node_id: e.source, target_node_id: e.target,
                edge_type: e.type || 'default', label: e.label || '', data: e.data || {},
            })),
        };
        try {
            await api.saveCanvasGraph(currentBoardId, graph);
            setDirty(false);
        } catch (err) {
            console.error('Failed to save:', err);
        }
    }, [currentBoardId, nodes, edges]);

    // Auto-save after 3s of inactivity
    useEffect(() => {
        if (!dirty || !currentBoardId) return;
        clearTimeout(saveTimerRef.current);
        saveTimerRef.current = setTimeout(saveGraph, 3000);
        return () => clearTimeout(saveTimerRef.current);
    }, [dirty, nodes, edges]);

    const onConnect = useCallback((params) => {
        const edgeId = `e-${params.source}-${params.target}`;
        setEdges((eds) => addEdge({ ...params, id: edgeId, animated: false }, eds));
        setDirty(true);
    }, []);

    const onNodeDragStop = useCallback(() => setDirty(true), []);

    const addNoteNode = useCallback(() => {
        const id = `note-${Date.now()}`;
        const newNode = {
            id, type: 'note',
            position: { x: 200 + Math.random() * 200, y: 200 + Math.random() * 200 },
            data: { label: '' },
        };
        setNodes((nds) => [...nds, newNode]);
        setDirty(true);
    }, []);

    // Keyboard: Delete selected, Ctrl+S save
    useEffect(() => {
        const handler = (e) => {
            if ((e.metaKey || e.ctrlKey) && e.key === 's') {
                e.preventDefault();
                saveGraph();
            }
        };
        window.addEventListener('keydown', handler);
        return () => window.removeEventListener('keydown', handler);
    }, [saveGraph]);

    const miniMapNodeColor = (node) => NODE_COLORS[node.type]?.border || '#6B7280';

    return (
        <div style={{ width: '100%', height: 'calc(100vh - 60px)', background: darkTheme.bg }}>
            <ReactFlow
                nodes={nodes}
                edges={edges}
                onNodesChange={(changes) => { onNodesChange(changes); setDirty(true); }}
                onEdgesChange={(changes) => { onEdgesChange(changes); setDirty(true); }}
                onConnect={onConnect}
                onNodeDragStop={onNodeDragStop}
                nodeTypes={nodeTypes}
                fitView
                deleteKeyCode={['Backspace', 'Delete']}
                style={{ background: darkTheme.bg }}
                defaultEdgeOptions={{ style: { stroke: '#3B82F6', strokeWidth: 1.5 }, type: 'smoothstep' }}
            >
                <Background color="#1E2A3A" gap={20} size={1} />
                <Controls style={{ background: darkTheme.panel, borderColor: darkTheme.panelBorder }} />
                <MiniMap
                    nodeColor={miniMapNodeColor}
                    maskColor="rgba(0,0,0,0.7)"
                    style={{ background: darkTheme.panel, borderColor: darkTheme.panelBorder }}
                />

                {/* Top toolbar */}
                <Panel position="top-left" style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                    <button style={btnStyle} onClick={() => setShowBoardList(!showBoardList)}>
                        <FolderOpen size={14} />
                        {boardName || 'No board'}
                        {dirty && <span style={{ color: '#F59E0B', marginLeft: '4px' }}>*</span>}
                    </button>
                    <button style={btnStyle} onClick={createBoard}><Plus size={14} /> New</button>
                    <button style={btnStyle} onClick={saveGraph}><Save size={14} /> Save</button>
                    <button style={btnStyle} onClick={addNoteNode}><Plus size={14} /> Note</button>
                    {currentBoardId && (
                        <button style={{ ...btnStyle, borderColor: '#8B1F1F' }} onClick={deleteBoard}>
                            <Trash2 size={14} />
                        </button>
                    )}
                </Panel>

                {/* Board picker dropdown */}
                {showBoardList && (
                    <Panel position="top-left" style={{
                        marginTop: '44px', background: darkTheme.panel,
                        border: `1px solid ${darkTheme.panelBorder}`, borderRadius: '8px',
                        padding: '8px', minWidth: '220px', maxHeight: '300px', overflowY: 'auto',
                    }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '6px' }}>
                            <span style={{ fontSize: '11px', color: darkTheme.textMuted, fontWeight: 600 }}>BOARDS</span>
                            <X size={12} style={{ cursor: 'pointer', color: darkTheme.textMuted }} onClick={() => setShowBoardList(false)} />
                        </div>
                        {boards.map((b) => (
                            <div
                                key={b.id}
                                onClick={() => { loadBoard(b.id); setShowBoardList(false); }}
                                style={{
                                    padding: '6px 8px', borderRadius: '4px', cursor: 'pointer',
                                    fontSize: '12px', color: b.id === currentBoardId ? darkTheme.accent : darkTheme.text,
                                    background: b.id === currentBoardId ? darkTheme.accent + '15' : 'transparent',
                                }}
                            >
                                {b.name}
                            </div>
                        ))}
                        {boards.length === 0 && (
                            <div style={{ fontSize: '11px', color: darkTheme.textMuted, padding: '8px' }}>
                                No boards yet. Click "New" to create one.
                            </div>
                        )}
                    </Panel>
                )}
            </ReactFlow>
        </div>
    );
}
```

- [ ] **Step 5: Commit**

```bash
git add pwa/src/views/Canvas.jsx pwa/src/api.js pwa/src/routes.js pwa/src/app.jsx
git commit -m "feat: Canvas view with React Flow + API methods + route registration"
```

---

### Task 7: Frontend — SendToCanvas Component

**Files:**
- Create: `pwa/src/components/SendToCanvas.jsx`

- [ ] **Step 1: Create the SendToCanvas component**

```jsx
import React, { useState, useEffect } from 'react';
import { api } from '../api.js';
import { Grid3X3 } from 'lucide-react';

/**
 * Reusable "Send to Canvas" button.
 *
 * Usage:
 *   <SendToCanvas type="actor" entityId={42} label="JPMorgan" data={{ category: 'bank' }} />
 */
export default function SendToCanvas({ type, entityId, label, data = {} }) {
    const [boards, setBoards] = useState([]);
    const [open, setOpen] = useState(false);
    const [sent, setSent] = useState(false);

    useEffect(() => {
        if (open && boards.length === 0) {
            api.getCanvasBoards().then((res) => setBoards(res.boards || [])).catch(() => {});
        }
    }, [open]);

    const sendToBoard = async (boardId) => {
        const nodeId = `${type}-${entityId || Date.now()}`;
        try {
            await api.addCanvasNode(boardId, {
                node_id: nodeId,
                node_type: type,
                entity_id: entityId,
                label: label || '',
                position_x: 100 + Math.random() * 400,
                position_y: 100 + Math.random() * 300,
                data,
            });
            setSent(true);
            setTimeout(() => { setSent(false); setOpen(false); }, 1200);
        } catch (err) {
            console.error('Failed to send to canvas:', err);
        }
    };

    const createAndSend = async () => {
        const name = prompt('New board name:');
        if (!name?.trim()) return;
        try {
            const board = await api.createCanvasBoard(name.trim());
            setBoards((prev) => [board, ...prev]);
            await sendToBoard(board.id);
        } catch (err) {
            console.error('Failed to create board:', err);
        }
    };

    return (
        <div style={{ position: 'relative', display: 'inline-block' }}>
            <button
                onClick={() => setOpen(!open)}
                title="Send to Canvas"
                style={{
                    background: 'transparent', border: '1px solid #1E2A3A', borderRadius: '4px',
                    color: sent ? '#10B981' : '#5A7080', cursor: 'pointer', padding: '4px 8px',
                    fontSize: '11px', display: 'flex', alignItems: 'center', gap: '4px',
                }}
            >
                <Grid3X3 size={12} />
                {sent ? 'Sent!' : 'Canvas'}
            </button>
            {open && (
                <div style={{
                    position: 'absolute', top: '100%', right: 0, marginTop: '4px',
                    background: '#0D1117', border: '1px solid #1E2A3A', borderRadius: '8px',
                    padding: '6px', minWidth: '180px', zIndex: 100, boxShadow: '0 4px 12px rgba(0,0,0,0.5)',
                }}>
                    <div style={{ fontSize: '10px', color: '#5A7080', fontWeight: 600, padding: '4px 6px' }}>
                        ADD TO BOARD
                    </div>
                    {boards.map((b) => (
                        <div
                            key={b.id}
                            onClick={() => sendToBoard(b.id)}
                            style={{
                                padding: '5px 8px', borderRadius: '4px', cursor: 'pointer',
                                fontSize: '12px', color: '#C8D8E8',
                            }}
                            onMouseEnter={(e) => e.target.style.background = '#1E2A3A'}
                            onMouseLeave={(e) => e.target.style.background = 'transparent'}
                        >
                            {b.name}
                        </div>
                    ))}
                    <div
                        onClick={createAndSend}
                        style={{
                            padding: '5px 8px', borderRadius: '4px', cursor: 'pointer',
                            fontSize: '12px', color: '#3B82F6', borderTop: '1px solid #1E2A3A',
                            marginTop: '4px', paddingTop: '8px',
                        }}
                    >
                        + New board...
                    </div>
                </div>
            )}
        </div>
    );
}
```

- [ ] **Step 2: Commit**

```bash
git add pwa/src/components/SendToCanvas.jsx
git commit -m "feat: SendToCanvas reusable button component"
```

---

### Task 8: Build, Test, Deploy

- [ ] **Step 1: Build PWA locally to verify no errors**

```bash
cd /Users/anikdang/dev/GRID/pwa && npm run build
```
Expected: Build succeeds with no errors.

- [ ] **Step 2: Run backend tests**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/test_canvas_api.py -v
```

- [ ] **Step 3: Push to GitHub**

```bash
cd /Users/anikdang/dev/GRID && git push origin main
```

- [ ] **Step 4: Deploy to server**

```bash
ssh grid-svr 'cd /data/grid_v4/grid_repo && git pull origin main && cd pwa && npm install && npm run build && sudo systemctl restart grid-api'
```

- [ ] **Step 5: Apply migration on server**

```bash
ssh grid-svr "PGPASSWORD=gridmaster2026 psql -U grid -d griddb -h localhost" <<'SQL'
CREATE TABLE IF NOT EXISTS canvas_boards (...);
CREATE TABLE IF NOT EXISTS canvas_nodes (...);
CREATE TABLE IF NOT EXISTS canvas_edges (...);
-- (Use the full SQL from Task 1)
SQL
```

- [ ] **Step 6: Verify on live site**

Open `https://grid.stepdad.finance/#/canvas` and verify:
- Board creation works
- Adding notes works
- Connecting nodes works
- Save/load works

- [ ] **Step 7: Final commit**

```bash
git add -A && git commit -m "feat: Canvas MVP — React Flow investigation workspace"
```
