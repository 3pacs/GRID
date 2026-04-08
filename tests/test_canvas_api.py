"""Tests for canvas API routers.

Stubs api.auth to avoid heavy transitive deps (psycopg2, jose, passlib)
that may not be installed in lightweight CI environments.

Tests verify:
  - Router structure (prefix, tags, endpoints)
  - Board CRUD via direct SQL (requires PostgreSQL)
  - Node unique constraint and edge creation
"""

from __future__ import annotations

import sys
import uuid
from types import ModuleType

import pytest

# ---------------------------------------------------------------------------
# Stub api.auth before canvas imports it — avoids psycopg2/jose/passlib
# ---------------------------------------------------------------------------

_auth_stub = ModuleType("api.auth")
_auth_stub.require_auth = lambda: None  # type: ignore[attr-defined]
sys.modules.setdefault("api.auth", _auth_stub)

if "api.dependencies" not in sys.modules:
    _deps_stub = ModuleType("api.dependencies")
    _deps_stub.get_db_engine = lambda: None  # type: ignore[attr-defined]
    sys.modules["api.dependencies"] = _deps_stub


# ── Router structure tests (no DB needed) ─────────────────────────────────

class TestCanvasRouter:
    @pytest.fixture(autouse=True)
    def _import_router(self):
        from api.routers.canvas import router
        self.router = router

    def test_router_has_correct_prefix(self):
        assert self.router.prefix == "/api/v1/canvas"

    def test_router_has_tag(self):
        assert "canvas" in self.router.tags

    def test_list_boards_endpoint_exists(self):
        paths = [r.path for r in self.router.routes]
        assert any(p.endswith("/boards") for p in paths)

    def test_create_board_endpoint_exists(self):
        methods = {}
        for r in self.router.routes:
            if hasattr(r, "methods"):
                for m in r.methods:
                    methods.setdefault(r.path, set()).add(m)
        board_paths = [p for p in methods if p.endswith("/boards")]
        assert any("POST" in methods[p] for p in board_paths)

    def test_get_board_endpoint_exists(self):
        paths = [r.path for r in self.router.routes]
        assert any("/boards/{board_id}" in p for p in paths)

    def test_delete_board_endpoint_exists(self):
        methods = {}
        for r in self.router.routes:
            if hasattr(r, "methods"):
                for m in r.methods:
                    methods.setdefault(r.path, set()).add(m)
        board_paths = [p for p in methods if "{board_id}" in p and not "node" in p and not "edge" in p and not "graph" in p]
        assert any("DELETE" in methods[p] for p in board_paths)

    def test_bulk_save_endpoint_exists(self):
        paths = [r.path for r in self.router.routes]
        assert any(p.endswith("/graph") for p in paths)

    def test_add_node_endpoint_exists(self):
        paths = [r.path for r in self.router.routes]
        assert any(p.endswith("/nodes") for p in paths)

    def test_add_edge_endpoint_exists(self):
        paths = [r.path for r in self.router.routes]
        assert any(p.endswith("/edges") for p in paths)


# ── Database integration tests (require PostgreSQL) ───────────────────────

class TestCanvasBoards:
    """Board CRUD via direct SQL against a live database."""

    @pytest.fixture(autouse=True)
    def _setup(self, pg_engine):
        """Use the pg_engine fixture from conftest.py; skip if DB unavailable."""
        self.engine = pg_engine
        # Ensure tables exist
        from sqlalchemy import text
        with self.engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS canvas_boards (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    name TEXT NOT NULL,
                    description TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS canvas_nodes (
                    id TEXT PRIMARY KEY,
                    board_id UUID NOT NULL REFERENCES canvas_boards(id) ON DELETE CASCADE,
                    node_type TEXT NOT NULL DEFAULT 'note',
                    label TEXT,
                    position_x DOUBLE PRECISION NOT NULL DEFAULT 0,
                    position_y DOUBLE PRECISION NOT NULL DEFAULT 0,
                    data JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS canvas_edges (
                    id TEXT PRIMARY KEY,
                    board_id UUID NOT NULL REFERENCES canvas_boards(id) ON DELETE CASCADE,
                    source_node_id TEXT NOT NULL REFERENCES canvas_nodes(id) ON DELETE CASCADE,
                    target_node_id TEXT NOT NULL REFERENCES canvas_nodes(id) ON DELETE CASCADE,
                    edge_type TEXT DEFAULT 'default',
                    label TEXT,
                    data JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """))
        yield
        # Cleanup test data
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM canvas_edges WHERE board_id IN (SELECT id FROM canvas_boards WHERE name LIKE 'test_%%')"))
            conn.execute(text("DELETE FROM canvas_nodes WHERE board_id IN (SELECT id FROM canvas_boards WHERE name LIKE 'test_%%')"))
            conn.execute(text("DELETE FROM canvas_boards WHERE name LIKE 'test_%%'"))

    def test_create_board(self):
        from sqlalchemy import text
        with self.engine.begin() as conn:
            row = conn.execute(
                text(
                    "INSERT INTO canvas_boards (name, description)"
                    " VALUES (:name, :description)"
                    " RETURNING id, name"
                ),
                {"name": "test_board_create", "description": "A test board"},
            ).fetchone()
        assert row is not None
        assert row[1] == "test_board_create"

    def test_list_boards(self):
        from sqlalchemy import text
        # Create two boards
        with self.engine.begin() as conn:
            conn.execute(
                text("INSERT INTO canvas_boards (name) VALUES (:name)"),
                {"name": "test_list_a"},
            )
            conn.execute(
                text("INSERT INTO canvas_boards (name) VALUES (:name)"),
                {"name": "test_list_b"},
            )

        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT * FROM canvas_boards"
                    " WHERE name LIKE 'test_list_%%'"
                    " ORDER BY updated_at DESC"
                )
            ).fetchall()
        assert len(rows) >= 2

    def test_delete_cascades(self):
        from sqlalchemy import text
        # Create board + node + edge, then delete board — everything should cascade
        with self.engine.begin() as conn:
            board = conn.execute(
                text(
                    "INSERT INTO canvas_boards (name) VALUES (:name) RETURNING id"
                ),
                {"name": "test_cascade"},
            ).fetchone()
            board_id = str(board[0])

            conn.execute(
                text(
                    "INSERT INTO canvas_nodes (id, board_id, node_type, label)"
                    " VALUES (:id, :board_id, :node_type, :label)"
                ),
                {"id": "n1", "board_id": board_id, "node_type": "note", "label": "A"},
            )
            conn.execute(
                text(
                    "INSERT INTO canvas_nodes (id, board_id, node_type, label)"
                    " VALUES (:id, :board_id, :node_type, :label)"
                ),
                {"id": "n2", "board_id": board_id, "node_type": "note", "label": "B"},
            )
            conn.execute(
                text(
                    "INSERT INTO canvas_edges (id, board_id, source_node_id, target_node_id)"
                    " VALUES (:id, :board_id, :source, :target)"
                ),
                {"id": "e1", "board_id": board_id, "source": "n1", "target": "n2"},
            )

            # Delete board
            conn.execute(
                text("DELETE FROM canvas_boards WHERE id = :id"),
                {"id": board_id},
            )

        # Verify cascade
        with self.engine.connect() as conn:
            nodes = conn.execute(
                text("SELECT id FROM canvas_nodes WHERE board_id = :bid"),
                {"bid": board_id},
            ).fetchall()
            edges = conn.execute(
                text("SELECT id FROM canvas_edges WHERE board_id = :bid"),
                {"bid": board_id},
            ).fetchall()

        assert len(nodes) == 0
        assert len(edges) == 0


class TestCanvasNodes:
    """Node operations via direct SQL."""

    @pytest.fixture(autouse=True)
    def _setup(self, pg_engine):
        self.engine = pg_engine
        from sqlalchemy import text
        # Ensure tables exist (idempotent)
        with self.engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS canvas_boards (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    name TEXT NOT NULL,
                    description TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS canvas_nodes (
                    id TEXT PRIMARY KEY,
                    board_id UUID NOT NULL REFERENCES canvas_boards(id) ON DELETE CASCADE,
                    node_type TEXT NOT NULL DEFAULT 'note',
                    label TEXT,
                    position_x DOUBLE PRECISION NOT NULL DEFAULT 0,
                    position_y DOUBLE PRECISION NOT NULL DEFAULT 0,
                    data JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """))
        # Create a test board
        with self.engine.begin() as conn:
            row = conn.execute(
                text("INSERT INTO canvas_boards (name) VALUES (:name) RETURNING id"),
                {"name": "test_nodes_board"},
            ).fetchone()
            self.board_id = str(row[0])
        yield
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM canvas_boards WHERE name LIKE 'test_%%'"))

    def test_add_node(self):
        from sqlalchemy import text
        node_id = f"node_{uuid.uuid4().hex[:8]}"
        with self.engine.begin() as conn:
            row = conn.execute(
                text(
                    "INSERT INTO canvas_nodes (id, board_id, node_type, label, position_x, position_y)"
                    " VALUES (:id, :board_id, :node_type, :label, :x, :y)"
                    " RETURNING id, label"
                ),
                {
                    "id": node_id,
                    "board_id": self.board_id,
                    "node_type": "actor",
                    "label": "Test Actor",
                    "x": 100.0,
                    "y": 200.0,
                },
            ).fetchone()
        assert row is not None
        assert row[0] == node_id
        assert row[1] == "Test Actor"

    def test_unique_constraint(self):
        from sqlalchemy import text
        node_id = f"node_{uuid.uuid4().hex[:8]}"
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO canvas_nodes (id, board_id, node_type)"
                    " VALUES (:id, :board_id, :node_type)"
                ),
                {"id": node_id, "board_id": self.board_id, "node_type": "note"},
            )

        # Inserting same id should raise
        with pytest.raises(Exception):
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO canvas_nodes (id, board_id, node_type)"
                        " VALUES (:id, :board_id, :node_type)"
                    ),
                    {"id": node_id, "board_id": self.board_id, "node_type": "note"},
                )


class TestCanvasEdges:
    """Edge operations via direct SQL."""

    @pytest.fixture(autouse=True)
    def _setup(self, pg_engine):
        self.engine = pg_engine
        from sqlalchemy import text
        # Ensure tables exist
        with self.engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS canvas_boards (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    name TEXT NOT NULL,
                    description TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS canvas_nodes (
                    id TEXT PRIMARY KEY,
                    board_id UUID NOT NULL REFERENCES canvas_boards(id) ON DELETE CASCADE,
                    node_type TEXT NOT NULL DEFAULT 'note',
                    label TEXT,
                    position_x DOUBLE PRECISION NOT NULL DEFAULT 0,
                    position_y DOUBLE PRECISION NOT NULL DEFAULT 0,
                    data JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS canvas_edges (
                    id TEXT PRIMARY KEY,
                    board_id UUID NOT NULL REFERENCES canvas_boards(id) ON DELETE CASCADE,
                    source_node_id TEXT NOT NULL REFERENCES canvas_nodes(id) ON DELETE CASCADE,
                    target_node_id TEXT NOT NULL REFERENCES canvas_nodes(id) ON DELETE CASCADE,
                    edge_type TEXT DEFAULT 'default',
                    label TEXT,
                    data JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """))
        # Create board + two nodes
        with self.engine.begin() as conn:
            row = conn.execute(
                text("INSERT INTO canvas_boards (name) VALUES (:name) RETURNING id"),
                {"name": "test_edges_board"},
            ).fetchone()
            self.board_id = str(row[0])

            self.node_a = f"na_{uuid.uuid4().hex[:8]}"
            self.node_b = f"nb_{uuid.uuid4().hex[:8]}"
            conn.execute(
                text(
                    "INSERT INTO canvas_nodes (id, board_id, node_type)"
                    " VALUES (:id, :board_id, 'note')"
                ),
                {"id": self.node_a, "board_id": self.board_id},
            )
            conn.execute(
                text(
                    "INSERT INTO canvas_nodes (id, board_id, node_type)"
                    " VALUES (:id, :board_id, 'note')"
                ),
                {"id": self.node_b, "board_id": self.board_id},
            )
        yield
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM canvas_boards WHERE name LIKE 'test_%%'"))

    def test_add_edge(self):
        from sqlalchemy import text
        edge_id = f"edge_{uuid.uuid4().hex[:8]}"
        with self.engine.begin() as conn:
            row = conn.execute(
                text(
                    "INSERT INTO canvas_edges (id, board_id, source_node_id, target_node_id, edge_type)"
                    " VALUES (:id, :board_id, :source, :target, :edge_type)"
                    " RETURNING id, source_node_id, target_node_id"
                ),
                {
                    "id": edge_id,
                    "board_id": self.board_id,
                    "source": self.node_a,
                    "target": self.node_b,
                    "edge_type": "influences",
                },
            ).fetchone()
        assert row is not None
        assert row[0] == edge_id
        assert row[1] == self.node_a
        assert row[2] == self.node_b
