"""Tests for investigation_evidence table and evidence CRUD endpoints.

Tests verify:
  - Evidence creation with all fields
  - Evidence retrieval by node
  - Evidence deletion
  - Confidence label validation
  - Cascade deletion when parent board is deleted
  - Evidence type validation

Stubs api.auth to avoid heavy transitive deps (psycopg2, jose, passlib)
that may not be installed in lightweight CI environments.
"""

from __future__ import annotations

import sys
import uuid
from types import ModuleType

import pytest

# ---------------------------------------------------------------------------
# Prefer the real api.auth — only stub if heavy deps are unavailable.
# Unconditional stubbing pollutes sys.modules for every later test.
# ---------------------------------------------------------------------------

try:
    import api.auth  # noqa: F401
except Exception:
    _auth_stub = ModuleType("api.auth")
    _auth_stub.require_auth = lambda: None  # type: ignore[attr-defined]
    sys.modules["api.auth"] = _auth_stub

try:
    import api.dependencies  # noqa: F401
except Exception:
    _deps_stub = ModuleType("api.dependencies")
    _deps_stub.get_db_engine = lambda: None  # type: ignore[attr-defined]
    sys.modules["api.dependencies"] = _deps_stub


# ── Router structure tests (no DB needed) ────────────────────────────────


class TestEvidenceRouterStructure:
    """Verify evidence endpoints are registered on the canvas_graph router."""

    @pytest.fixture(autouse=True)
    def _import_router(self):
        from api.routers.canvas_graph import router
        self.router = router

    def test_add_evidence_endpoint_exists(self):
        paths = [r.path for r in self.router.routes]
        assert any("/evidence" in p and "node_id" in p for p in paths)

    def test_get_evidence_endpoint_exists(self):
        methods: dict[str, set[str]] = {}
        for r in self.router.routes:
            if hasattr(r, "methods"):
                for m in r.methods:
                    methods.setdefault(r.path, set()).add(m)
        evidence_paths = [
            p for p in methods
            if "/evidence" in p and "node_id" in p
        ]
        assert any("GET" in methods[p] for p in evidence_paths)

    def test_delete_evidence_endpoint_exists(self):
        methods: dict[str, set[str]] = {}
        for r in self.router.routes:
            if hasattr(r, "methods"):
                for m in r.methods:
                    methods.setdefault(r.path, set()).add(m)
        delete_paths = [
            p for p in methods
            if "/evidence/{evidence_id}" in p
        ]
        assert any("DELETE" in methods[p] for p in delete_paths)


# ── Pydantic model tests (no DB needed) ─────────────────────────────────


class TestEvidenceCreateModel:
    """Validate the EvidenceCreate Pydantic model defaults and fields."""

    def test_defaults(self):
        from api.routers.canvas_graph import EvidenceCreate
        body = EvidenceCreate(evidence_type="signal")
        assert body.evidence_type == "signal"
        assert body.confidence == "derived"
        assert body.content is None
        assert body.source_url is None
        assert body.source_table is None
        assert body.source_id is None
        assert body.metadata is None

    def test_all_fields(self):
        from api.routers.canvas_graph import EvidenceCreate
        body = EvidenceCreate(
            evidence_type="filing",
            content="10-K excerpt about revenue growth",
            source_url="https://sec.gov/filing/123",
            source_table="sec_filings",
            source_id="abc-123",
            confidence="confirmed",
            metadata={"page": 42, "section": "MD&A"},
        )
        assert body.evidence_type == "filing"
        assert body.confidence == "confirmed"
        assert body.metadata["page"] == 42


# ── Validation constant tests (no DB needed) ────────────────────────────


class TestValidationConstants:
    """Verify the allowed confidence and evidence_type sets."""

    def test_valid_confidence_labels(self):
        from api.routers.canvas_graph import VALID_CONFIDENCE_LABELS
        expected = {"confirmed", "derived", "estimated", "rumored", "inferred"}
        assert VALID_CONFIDENCE_LABELS == expected

    def test_valid_evidence_types(self):
        from api.routers.canvas_graph import VALID_EVIDENCE_TYPES
        expected = {"signal", "filing", "quote", "chart", "news", "prediction"}
        assert VALID_EVIDENCE_TYPES == expected


# ── Database integration tests (require PostgreSQL) ─────────────────────


class TestInvestigationEvidenceTable:
    """Direct SQL tests against the investigation_evidence table."""

    @pytest.fixture(autouse=True)
    def _setup(self, pg_engine):
        self.engine = pg_engine
        from sqlalchemy import text

        # Ensure prerequisite tables exist
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
                    node_id TEXT PRIMARY KEY,
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
                CREATE TABLE IF NOT EXISTS investigation_evidence (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    board_id UUID REFERENCES canvas_boards(id) ON DELETE CASCADE,
                    node_id TEXT,
                    evidence_type TEXT NOT NULL,
                    content TEXT,
                    source_url TEXT,
                    source_table TEXT,
                    source_id TEXT,
                    confidence TEXT DEFAULT 'derived',
                    captured_at TIMESTAMPTZ DEFAULT NOW(),
                    metadata JSONB DEFAULT '{}'::jsonb
                )
            """))

        # Create a test board and node
        with self.engine.begin() as conn:
            row = conn.execute(
                text("INSERT INTO canvas_boards (name) VALUES (:name) RETURNING id"),
                {"name": "test_evidence_board"},
            ).fetchone()
            self.board_id = str(row[0])

            self.node_id = f"ev_node_{uuid.uuid4().hex[:8]}"
            conn.execute(
                text(
                    "INSERT INTO canvas_nodes (node_id, board_id, node_type, label)"
                    " VALUES (:node_id, :board_id, 'actor', 'Test Actor')"
                ),
                {"node_id": self.node_id, "board_id": self.board_id},
            )

        yield

        # Cleanup
        with self.engine.begin() as conn:
            conn.execute(text(
                "DELETE FROM investigation_evidence"
                " WHERE board_id IN (SELECT id FROM canvas_boards WHERE name LIKE 'test_%%')"
            ))
            conn.execute(text("DELETE FROM canvas_nodes WHERE board_id IN (SELECT id FROM canvas_boards WHERE name LIKE 'test_%%')"))
            conn.execute(text("DELETE FROM canvas_boards WHERE name LIKE 'test_%%'"))

    def test_create_evidence(self):
        from sqlalchemy import text
        with self.engine.begin() as conn:
            row = conn.execute(
                text(
                    "INSERT INTO investigation_evidence"
                    " (board_id, node_id, evidence_type, content, confidence)"
                    " VALUES (:board_id, :node_id, :etype, :content, :confidence)"
                    " RETURNING id, evidence_type, content, confidence"
                ),
                {
                    "board_id": self.board_id,
                    "node_id": self.node_id,
                    "etype": "signal",
                    "content": "Dark pool print 2M shares",
                    "confidence": "confirmed",
                },
            ).fetchone()
        assert row is not None
        assert row[1] == "signal"
        assert row[2] == "Dark pool print 2M shares"
        assert row[3] == "confirmed"

    def test_create_evidence_with_all_fields(self):
        from sqlalchemy import text
        with self.engine.begin() as conn:
            row = conn.execute(
                text(
                    "INSERT INTO investigation_evidence"
                    " (board_id, node_id, evidence_type, content, source_url,"
                    "  source_table, source_id, confidence, metadata)"
                    " VALUES (:board_id, :node_id, :etype, :content, :source_url,"
                    "  :source_table, :source_id, :confidence, :metadata)"
                    " RETURNING id, evidence_type, source_url, source_table, source_id, confidence"
                ),
                {
                    "board_id": self.board_id,
                    "node_id": self.node_id,
                    "etype": "filing",
                    "content": "Revenue grew 42% YoY",
                    "source_url": "https://sec.gov/filing/xyz",
                    "source_table": "sec_filings",
                    "source_id": "filing-001",
                    "confidence": "confirmed",
                    "metadata": '{"page": 12}',
                },
            ).fetchone()
        assert row is not None
        assert row[1] == "filing"
        assert row[2] == "https://sec.gov/filing/xyz"
        assert row[3] == "sec_filings"
        assert row[4] == "filing-001"
        assert row[5] == "confirmed"

    def test_retrieve_evidence_by_node(self):
        from sqlalchemy import text
        # Insert two evidence items
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO investigation_evidence"
                    " (board_id, node_id, evidence_type, content, confidence)"
                    " VALUES (:board_id, :node_id, 'signal', 'Evidence A', 'confirmed')"
                ),
                {"board_id": self.board_id, "node_id": self.node_id},
            )
            conn.execute(
                text(
                    "INSERT INTO investigation_evidence"
                    " (board_id, node_id, evidence_type, content, confidence)"
                    " VALUES (:board_id, :node_id, 'quote', 'Evidence B', 'rumored')"
                ),
                {"board_id": self.board_id, "node_id": self.node_id},
            )

        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT id, evidence_type, content, confidence"
                    " FROM investigation_evidence"
                    " WHERE board_id = :board_id AND node_id = :node_id"
                    " ORDER BY captured_at DESC"
                ),
                {"board_id": self.board_id, "node_id": self.node_id},
            ).fetchall()

        assert len(rows) >= 2
        types = {r[1] for r in rows}
        assert "signal" in types
        assert "quote" in types

    def test_delete_evidence(self):
        from sqlalchemy import text
        with self.engine.begin() as conn:
            row = conn.execute(
                text(
                    "INSERT INTO investigation_evidence"
                    " (board_id, node_id, evidence_type, content)"
                    " VALUES (:board_id, :node_id, 'chart', 'Chart snapshot')"
                    " RETURNING id"
                ),
                {"board_id": self.board_id, "node_id": self.node_id},
            ).fetchone()
            evidence_id = str(row[0])

        # Delete it
        with self.engine.begin() as conn:
            result = conn.execute(
                text(
                    "DELETE FROM investigation_evidence"
                    " WHERE id = :eid AND board_id = :board_id"
                    " RETURNING id"
                ),
                {"eid": evidence_id, "board_id": self.board_id},
            ).fetchone()
        assert result is not None
        assert str(result[0]) == evidence_id

        # Verify gone
        with self.engine.connect() as conn:
            check = conn.execute(
                text("SELECT id FROM investigation_evidence WHERE id = :eid"),
                {"eid": evidence_id},
            ).fetchone()
        assert check is None

    def test_confidence_default_is_derived(self):
        from sqlalchemy import text
        with self.engine.begin() as conn:
            row = conn.execute(
                text(
                    "INSERT INTO investigation_evidence"
                    " (board_id, node_id, evidence_type, content)"
                    " VALUES (:board_id, :node_id, 'news', 'Breaking news')"
                    " RETURNING confidence"
                ),
                {"board_id": self.board_id, "node_id": self.node_id},
            ).fetchone()
        assert row[0] == "derived"

    def test_all_confidence_values(self):
        from sqlalchemy import text
        for label in ("confirmed", "derived", "estimated", "rumored", "inferred"):
            with self.engine.begin() as conn:
                row = conn.execute(
                    text(
                        "INSERT INTO investigation_evidence"
                        " (board_id, node_id, evidence_type, content, confidence)"
                        " VALUES (:board_id, :node_id, 'signal', :content, :confidence)"
                        " RETURNING confidence"
                    ),
                    {
                        "board_id": self.board_id,
                        "node_id": self.node_id,
                        "content": f"Test {label}",
                        "confidence": label,
                    },
                ).fetchone()
            assert row[0] == label

    def test_cascade_delete_board_removes_evidence(self):
        """Deleting a board must cascade-delete all associated evidence."""
        from sqlalchemy import text

        # Create a separate board for this test
        with self.engine.begin() as conn:
            board_row = conn.execute(
                text("INSERT INTO canvas_boards (name) VALUES (:name) RETURNING id"),
                {"name": "test_cascade_evidence"},
            ).fetchone()
            cascade_board_id = str(board_row[0])

            # Add a node to the board
            cascade_node_id = f"cascade_node_{uuid.uuid4().hex[:8]}"
            conn.execute(
                text(
                    "INSERT INTO canvas_nodes (node_id, board_id, node_type, label)"
                    " VALUES (:node_id, :board_id, 'actor', 'Cascade Actor')"
                ),
                {"node_id": cascade_node_id, "board_id": cascade_board_id},
            )

            # Add evidence to the node
            conn.execute(
                text(
                    "INSERT INTO investigation_evidence"
                    " (board_id, node_id, evidence_type, content)"
                    " VALUES (:board_id, :node_id, 'signal', 'Will be cascaded')"
                ),
                {"board_id": cascade_board_id, "node_id": cascade_node_id},
            )
            conn.execute(
                text(
                    "INSERT INTO investigation_evidence"
                    " (board_id, node_id, evidence_type, content)"
                    " VALUES (:board_id, :node_id, 'filing', 'Also cascaded')"
                ),
                {"board_id": cascade_board_id, "node_id": cascade_node_id},
            )

        # Verify evidence exists
        with self.engine.connect() as conn:
            count_before = conn.execute(
                text(
                    "SELECT COUNT(*) FROM investigation_evidence WHERE board_id = :bid"
                ),
                {"bid": cascade_board_id},
            ).fetchone()[0]
        assert count_before == 2

        # Delete the board
        with self.engine.begin() as conn:
            conn.execute(
                text("DELETE FROM canvas_boards WHERE id = :id"),
                {"id": cascade_board_id},
            )

        # Verify evidence is gone
        with self.engine.connect() as conn:
            count_after = conn.execute(
                text(
                    "SELECT COUNT(*) FROM investigation_evidence WHERE board_id = :bid"
                ),
                {"bid": cascade_board_id},
            ).fetchone()[0]
        assert count_after == 0

    def test_metadata_jsonb_roundtrip(self):
        """Verify JSONB metadata can be stored and retrieved."""
        from sqlalchemy import text
        import json

        meta = {"page": 42, "section": "Risk Factors", "tags": ["SEC", "10-K"]}
        with self.engine.begin() as conn:
            row = conn.execute(
                text(
                    "INSERT INTO investigation_evidence"
                    " (board_id, node_id, evidence_type, content, metadata)"
                    " VALUES (:board_id, :node_id, 'filing', 'Test', :metadata)"
                    " RETURNING metadata"
                ),
                {
                    "board_id": self.board_id,
                    "node_id": self.node_id,
                    "metadata": json.dumps(meta),
                },
            ).fetchone()

        retrieved = row[0] if isinstance(row[0], dict) else json.loads(row[0])
        assert retrieved["page"] == 42
        assert "SEC" in retrieved["tags"]
