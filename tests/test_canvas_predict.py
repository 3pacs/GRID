"""Tests for canvas prediction pipeline (Phase 4.7).

Validates:
  - Prediction creation with valid request
  - Antithesis auto-creation and pair_id linking
  - Canvas node creation for the hypothesis
  - Validation: empty board, invalid direction, invalid confidence clamping
  - LEVER -> CONDITION -> THESIS structure
"""

from __future__ import annotations

import json
import sys
import uuid
from types import ModuleType

import pytest

# ---------------------------------------------------------------------------
# Prefer the real api.auth — only stub if heavy deps are unavailable.
# Unconditional stubbing pollutes sys.modules for every later test in the
# session (e.g. test_celestial.py that needs create_token).
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


# ── Router import tests (no DB needed) ──────────────────────────────────


class TestCanvasPredictRouter:
    """Verify the predict endpoint is registered properly."""

    @pytest.fixture(autouse=True)
    def _import_router(self):
        from api.routers.canvas import router
        self.router = router

    def test_predict_endpoint_exists(self):
        paths = [r.path for r in self.router.routes]
        assert any(p.endswith("/predict") for p in paths), (
            f"No /predict endpoint found. Routes: {paths}"
        )

    def test_predict_endpoint_is_post(self):
        methods = {}
        for r in self.router.routes:
            if hasattr(r, "methods"):
                for m in r.methods:
                    methods.setdefault(r.path, set()).add(m)
        predict_paths = [p for p in methods if p.endswith("/predict")]
        assert any("POST" in methods[p] for p in predict_paths)


# ── Schema / model tests ───────────────────────────────────────────────


class TestPredictionModels:
    """Test Pydantic models serialize correctly."""

    def test_prediction_request_defaults(self):
        from api.routers.canvas_predict import PredictionRequest

        req = PredictionRequest(
            board_id="test-board",
            thesis_text="BTC will rally",
            ticker="BTC",
            direction="bullish",
        )
        assert req.timeframe_days == 30
        assert req.confidence == 0.5
        assert req.lever_node_id is None
        assert req.condition_node_ids == []

    def test_prediction_request_with_lever(self):
        from api.routers.canvas_predict import PredictionRequest

        req = PredictionRequest(
            board_id="board-1",
            thesis_text="ETH follows BTC",
            ticker="ETH",
            direction="bullish",
            lever_node_id="node-abc",
            condition_node_ids=["node-x", "node-y"],
            confidence=0.8,
            timeframe_days=14,
        )
        assert req.lever_node_id == "node-abc"
        assert len(req.condition_node_ids) == 2
        assert req.confidence == 0.8

    def test_prediction_response_model(self):
        from api.routers.canvas_predict import PredictionResponse

        resp = PredictionResponse(
            hypothesis_id="abc-123",
            thesis="Test thesis",
            pattern_type="canvas_investigation",
            confidence=0.75,
            status="active",
            canvas_node_id="hyp-abc12345",
        )
        assert resp.hypothesis_id == "abc-123"
        assert resp.status == "active"


# ── Helper tests ────────────────────────────────────────────────────────


class TestHelpers:
    def test_parse_node_data_none(self):
        from api.routers.canvas_predict import _parse_node_data
        assert _parse_node_data(None) == {}

    def test_parse_node_data_dict(self):
        from api.routers.canvas_predict import _parse_node_data
        data = {"foo": "bar"}
        assert _parse_node_data(data) == {"foo": "bar"}

    def test_parse_node_data_valid_json_str(self):
        from api.routers.canvas_predict import _parse_node_data
        assert _parse_node_data('{"a": 1}') == {"a": 1}

    def test_parse_node_data_invalid_json_str(self):
        from api.routers.canvas_predict import _parse_node_data
        assert _parse_node_data("not json") == {}

    def test_clamp_confidence(self):
        from api.routers.canvas_predict import _clamp_confidence
        assert _clamp_confidence(0.5) == 0.5
        assert _clamp_confidence(-0.2) == 0.0
        assert _clamp_confidence(1.5) == 1.0
        assert _clamp_confidence(0.0) == 0.0
        assert _clamp_confidence(1.0) == 1.0


# ── Database integration tests (require PostgreSQL) ─────────────────────


class TestCanvasPredictDB:
    """Full prediction pipeline against a live database."""

    @pytest.fixture(autouse=True)
    def _setup(self, pg_engine):
        """Create required tables and clean up test data after."""
        self.engine = pg_engine
        from sqlalchemy import text

        with self.engine.begin() as conn:
            # Canvas tables
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
            # Hypothesis table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS discovered_hypotheses (
                    id TEXT PRIMARY KEY,
                    thesis TEXT NOT NULL,
                    pattern_type TEXT,
                    evidence JSONB,
                    test_criteria JSONB,
                    invalidation TEXT,
                    confidence DOUBLE PRECISION DEFAULT 0.5,
                    status TEXT DEFAULT 'active',
                    times_tested INTEGER DEFAULT 0,
                    times_correct INTEGER DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    last_tested TIMESTAMPTZ,
                    role TEXT DEFAULT 'thesis',
                    pair_id TEXT,
                    kill_reason TEXT,
                    killed_at TIMESTAMPTZ
                )
            """))

        yield

        # Cleanup
        with self.engine.begin() as conn:
            conn.execute(text(
                "DELETE FROM canvas_edges WHERE board_id IN "
                "(SELECT id FROM canvas_boards WHERE name LIKE 'test_predict_%%')"
            ))
            conn.execute(text(
                "DELETE FROM canvas_nodes WHERE board_id IN "
                "(SELECT id FROM canvas_boards WHERE name LIKE 'test_predict_%%')"
            ))
            conn.execute(text(
                "DELETE FROM canvas_boards WHERE name LIKE 'test_predict_%%'"
            ))
            conn.execute(text(
                "DELETE FROM discovered_hypotheses WHERE pattern_type = 'canvas_investigation'"
            ))

    def _create_board_with_nodes(self, conn, board_name="test_predict_board"):
        """Helper: create a board with 3 nodes and return (board_id, node_ids)."""
        from sqlalchemy import text

        row = conn.execute(
            text(
                "INSERT INTO canvas_boards (name) VALUES (:name) RETURNING id"
            ),
            {"name": board_name},
        ).fetchone()
        board_id = str(row[0])

        node_ids = []
        for i, (ntype, label) in enumerate([
            ("actor", "Elon Musk"),
            ("signal", "Dark Pool Spike"),
            ("company", "Tesla Inc"),
        ]):
            nid = f"test-node-{i}-{uuid.uuid4().hex[:8]}"
            conn.execute(
                text(
                    "INSERT INTO canvas_nodes (id, board_id, node_type, label, position_x, position_y)"
                    " VALUES (:id, :bid, :ntype, :label, :px, :py)"
                ),
                {
                    "id": nid,
                    "bid": board_id,
                    "ntype": ntype,
                    "label": label,
                    "px": 100.0 + i * 200,
                    "py": 200.0,
                },
            )
            node_ids.append(nid)

        return board_id, node_ids

    def test_create_prediction_full_pipeline(self):
        """Test that a prediction creates thesis, antithesis, and canvas node."""
        from sqlalchemy import text
        from api.routers.canvas_predict import create_prediction, PredictionRequest
        import asyncio

        with self.engine.begin() as conn:
            board_id, node_ids = self._create_board_with_nodes(conn)

        # Monkey-patch get_db_engine to return our test engine
        import api.routers.canvas_predict as mod
        original_get = mod.get_db_engine
        mod.get_db_engine = lambda: self.engine

        try:
            req = PredictionRequest(
                board_id=board_id,
                thesis_text="Tesla will rally on Elon tweet",
                ticker="TSLA",
                direction="bullish",
                timeframe_days=14,
                lever_node_id=node_ids[0],  # Elon Musk
                condition_node_ids=[node_ids[1]],  # Dark Pool Spike
                confidence=0.7,
            )

            result = asyncio.get_event_loop().run_until_complete(
                create_prediction(req, _token="test")
            )

            assert result.hypothesis_id
            assert result.pattern_type == "canvas_investigation"
            assert result.confidence == 0.7
            assert result.status == "active"
            assert result.canvas_node_id.startswith("hyp-")

            # Verify thesis in DB
            with self.engine.connect() as conn:
                thesis_row = conn.execute(
                    text("SELECT * FROM discovered_hypotheses WHERE id = :id"),
                    {"id": result.hypothesis_id},
                ).mappings().fetchone()

                assert thesis_row is not None
                assert thesis_row["role"] == "thesis"
                assert thesis_row["status"] == "active"
                assert thesis_row["confidence"] == 0.7
                assert "LEVER:" in thesis_row["thesis"]
                assert "CONDITION:" in thesis_row["thesis"]
                assert "THESIS:" in thesis_row["thesis"]
                assert thesis_row["pair_id"] is not None

                # Verify evidence JSONB
                evidence = json.loads(thesis_row["evidence"]) if isinstance(thesis_row["evidence"], str) else thesis_row["evidence"]
                assert evidence["ticker"] == "TSLA"
                assert evidence["direction"] == "bullish"
                assert evidence["created_from"] == "canvas"

                # Verify test_criteria
                criteria = json.loads(thesis_row["test_criteria"]) if isinstance(thesis_row["test_criteria"], str) else thesis_row["test_criteria"]
                assert criteria["ticker"] == "TSLA"
                assert criteria["direction"] == "bullish"
                assert criteria["window_days"] == 14

                # Verify antithesis
                anti_id = thesis_row["pair_id"]
                anti_row = conn.execute(
                    text("SELECT * FROM discovered_hypotheses WHERE id = :id"),
                    {"id": anti_id},
                ).mappings().fetchone()

                assert anti_row is not None
                assert anti_row["role"] == "antithesis"
                assert anti_row["pair_id"] == result.hypothesis_id
                assert anti_row["confidence"] == pytest.approx(0.3, abs=0.01)

                # Verify canvas node was created
                canvas_node = conn.execute(
                    text("SELECT * FROM canvas_nodes WHERE id = :id"),
                    {"id": result.canvas_node_id},
                ).mappings().fetchone()

                assert canvas_node is not None
                assert canvas_node["node_type"] == "hypothesis"
                assert "BULLISH" in canvas_node["label"]
                assert "TSLA" in canvas_node["label"]

        finally:
            mod.get_db_engine = original_get

    def test_pair_id_bidirectional_linking(self):
        """Verify thesis.pair_id -> antithesis and antithesis.pair_id -> thesis."""
        from sqlalchemy import text
        from api.routers.canvas_predict import create_prediction, PredictionRequest
        import asyncio

        with self.engine.begin() as conn:
            board_id, node_ids = self._create_board_with_nodes(conn, "test_predict_pair")

        import api.routers.canvas_predict as mod
        original_get = mod.get_db_engine
        mod.get_db_engine = lambda: self.engine

        try:
            req = PredictionRequest(
                board_id=board_id,
                thesis_text="SPY drops on rate hike",
                ticker="SPY",
                direction="bearish",
                confidence=0.6,
            )

            result = asyncio.get_event_loop().run_until_complete(
                create_prediction(req, _token="test")
            )

            with self.engine.connect() as conn:
                thesis = conn.execute(
                    text("SELECT id, pair_id FROM discovered_hypotheses WHERE id = :id"),
                    {"id": result.hypothesis_id},
                ).mappings().fetchone()

                anti = conn.execute(
                    text("SELECT id, pair_id FROM discovered_hypotheses WHERE id = :id"),
                    {"id": thesis["pair_id"]},
                ).mappings().fetchone()

                # Bidirectional linking
                assert thesis["pair_id"] == anti["id"]
                assert anti["pair_id"] == thesis["id"]

        finally:
            mod.get_db_engine = original_get

    def test_bearish_direction(self):
        """Test bearish prediction sets correct thresholds."""
        from sqlalchemy import text
        from api.routers.canvas_predict import create_prediction, PredictionRequest
        import asyncio

        with self.engine.begin() as conn:
            board_id, node_ids = self._create_board_with_nodes(conn, "test_predict_bear")

        import api.routers.canvas_predict as mod
        original_get = mod.get_db_engine
        mod.get_db_engine = lambda: self.engine

        try:
            req = PredictionRequest(
                board_id=board_id,
                thesis_text="Market crash incoming",
                ticker="QQQ",
                direction="bearish",
                confidence=0.9,
            )

            result = asyncio.get_event_loop().run_until_complete(
                create_prediction(req, _token="test")
            )

            with self.engine.connect() as conn:
                row = conn.execute(
                    text("SELECT test_criteria FROM discovered_hypotheses WHERE id = :id"),
                    {"id": result.hypothesis_id},
                ).mappings().fetchone()

                criteria = json.loads(row["test_criteria"]) if isinstance(row["test_criteria"], str) else row["test_criteria"]
                assert criteria["direction"] == "bearish"
                assert criteria["threshold_pct"] == -2.0

        finally:
            mod.get_db_engine = original_get

    def test_empty_board_returns_404(self):
        """Test that an empty board raises 404."""
        from fastapi import HTTPException
        from api.routers.canvas_predict import create_prediction, PredictionRequest
        import asyncio

        # Create a board with no nodes
        from sqlalchemy import text
        with self.engine.begin() as conn:
            row = conn.execute(
                text("INSERT INTO canvas_boards (name) VALUES (:name) RETURNING id"),
                {"name": "test_predict_empty"},
            ).fetchone()
            board_id = str(row[0])

        import api.routers.canvas_predict as mod
        original_get = mod.get_db_engine
        mod.get_db_engine = lambda: self.engine

        try:
            req = PredictionRequest(
                board_id=board_id,
                thesis_text="Should fail",
                ticker="FAIL",
                direction="bullish",
            )

            with pytest.raises(HTTPException) as exc_info:
                asyncio.get_event_loop().run_until_complete(
                    create_prediction(req, _token="test")
                )
            assert exc_info.value.status_code == 404
            assert "no nodes" in str(exc_info.value.detail).lower()

        finally:
            mod.get_db_engine = original_get

    def test_invalid_direction_returns_400(self):
        """Test that an invalid direction raises 400."""
        from fastapi import HTTPException
        from api.routers.canvas_predict import create_prediction, PredictionRequest
        import asyncio

        with self.engine.begin() as conn:
            board_id, _ = self._create_board_with_nodes(conn, "test_predict_baddir")

        import api.routers.canvas_predict as mod
        original_get = mod.get_db_engine
        mod.get_db_engine = lambda: self.engine

        try:
            req = PredictionRequest(
                board_id=board_id,
                thesis_text="Test",
                ticker="AAPL",
                direction="sideways",
            )

            with pytest.raises(HTTPException) as exc_info:
                asyncio.get_event_loop().run_until_complete(
                    create_prediction(req, _token="test")
                )
            assert exc_info.value.status_code == 400

        finally:
            mod.get_db_engine = original_get

    def test_confidence_clamping(self):
        """Test that out-of-range confidence gets clamped to [0, 1]."""
        from sqlalchemy import text
        from api.routers.canvas_predict import create_prediction, PredictionRequest
        import asyncio

        with self.engine.begin() as conn:
            board_id, _ = self._create_board_with_nodes(conn, "test_predict_clamp")

        import api.routers.canvas_predict as mod
        original_get = mod.get_db_engine
        mod.get_db_engine = lambda: self.engine

        try:
            req = PredictionRequest(
                board_id=board_id,
                thesis_text="High confidence",
                ticker="NVDA",
                direction="bullish",
                confidence=1.5,  # Should clamp to 1.0
            )

            result = asyncio.get_event_loop().run_until_complete(
                create_prediction(req, _token="test")
            )
            assert result.confidence == 1.0

            with self.engine.connect() as conn:
                row = conn.execute(
                    text("SELECT confidence FROM discovered_hypotheses WHERE id = :id"),
                    {"id": result.hypothesis_id},
                ).mappings().fetchone()
                assert row["confidence"] == 1.0

        finally:
            mod.get_db_engine = original_get

    def test_no_lever_no_conditions(self):
        """Test prediction without lever or conditions uses plain thesis."""
        from sqlalchemy import text
        from api.routers.canvas_predict import create_prediction, PredictionRequest
        import asyncio

        with self.engine.begin() as conn:
            board_id, _ = self._create_board_with_nodes(conn, "test_predict_plain")

        import api.routers.canvas_predict as mod
        original_get = mod.get_db_engine
        mod.get_db_engine = lambda: self.engine

        try:
            req = PredictionRequest(
                board_id=board_id,
                thesis_text="Simple thesis without structure",
                ticker="GOOG",
                direction="bullish",
            )

            result = asyncio.get_event_loop().run_until_complete(
                create_prediction(req, _token="test")
            )

            # Without lever/conditions, thesis should be plain text
            assert result.thesis == "Simple thesis without structure"
            assert "LEVER:" not in result.thesis

        finally:
            mod.get_db_engine = original_get
