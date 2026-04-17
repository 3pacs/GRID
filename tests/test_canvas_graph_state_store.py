"""Regression tests for Canvas graph_state board storage.

The frontend board CRUD path persists graph state in investigation_boards.
These tests cover the compatibility endpoints that mutate nodes/edges one at a
time so they cannot silently drift back to the legacy canvas_* tables only.
"""

from __future__ import annotations

import asyncio
import uuid

from sqlalchemy import text


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class TestCanvasGraphStateHelpers:
    def test_upsert_update_delete_node_mutates_graph_state(self):
        from api.routers.canvas_board_store import (
            delete_graph_node,
            node_response,
            normalize_graph_state,
            update_graph_node,
            upsert_graph_node,
        )

        state = normalize_graph_state({"nodes": [], "edges": []})
        saved = upsert_graph_node(
            state,
            {
                "node_id": "n-a",
                "node_type": "actor",
                "label": "Actor A",
                "position_x": 10,
                "position_y": 20,
                "data": {"source": "test"},
            },
        )

        assert saved["id"] == "n-a"
        assert state["nodes"][0]["attributes"]["data"] == {"source": "test"}

        updated = update_graph_node(state, "n-a", {"label": "Actor A2", "position_x": 55})
        assert updated is not None
        assert node_response("board-1", updated)["label"] == "Actor A2"
        assert node_response("board-1", updated)["position_x"] == 55

        deleted = delete_graph_node(state, "n-a")
        assert deleted is not None
        assert state["nodes"] == []

    def test_upsert_delete_edge_mutates_graph_state(self):
        from api.routers.canvas_board_store import (
            delete_graph_edge,
            edge_response,
            normalize_graph_state,
            upsert_graph_edge,
        )

        state = normalize_graph_state({"nodes": [], "edges": []})
        saved = upsert_graph_edge(
            state,
            {
                "edge_id": "e-ab",
                "source_node_id": "n-a",
                "target_node_id": "n-b",
                "edge_type": "signal",
            },
        )

        response = edge_response("board-1", saved)
        assert response["id"] == "e-ab"
        assert response["source_node_id"] == "n-a"
        assert response["target_node_id"] == "n-b"

        deleted = delete_graph_edge(state, "e-ab")
        assert deleted is not None
        assert state["edges"] == []


class TestCanvasGraphStateCompatibility:
    def test_legacy_canvas_rows_can_sync_back_to_graph_state(self, pg_engine):
        from api.routers.canvas_board_store import (
            ensure_legacy_canvas_tables,
            sync_board_from_legacy_canvas,
        )

        board_id = str(uuid.uuid4())
        try:
            with pg_engine.begin() as conn:
                ensure_legacy_canvas_tables(conn)
                conn.execute(
                    text(
                        "INSERT INTO canvas_boards (id, name, description) "
                        "VALUES (CAST(:id AS UUID), :name, :description)"
                    ),
                    {"id": board_id, "name": "test_legacy_back_sync", "description": "legacy"},
                )
                conn.execute(
                    text(
                        "INSERT INTO canvas_nodes "
                        "(node_id, board_id, node_type, label, position_x, position_y, data) "
                        "VALUES (:id, CAST(:board_id AS UUID), 'actor', :label, 10, 20, :data)"
                    ),
                    {
                        "id": "legacy-a",
                        "board_id": board_id,
                        "label": "Legacy Actor",
                        "data": '{"entityId": "actor-1"}',
                    },
                )
                conn.execute(
                    text(
                        "INSERT INTO canvas_nodes "
                        "(node_id, board_id, node_type, label, position_x, position_y, data) "
                        "VALUES (:id, CAST(:board_id AS UUID), 'signal', :label, 30, 40, :data)"
                    ),
                    {
                        "id": "legacy-b",
                        "board_id": board_id,
                        "label": "Legacy Signal",
                        "data": '{"signal_id": "sig-1"}',
                    },
                )
                conn.execute(
                    text(
                        "INSERT INTO canvas_edges "
                        "(id, board_id, source_node_id, target_node_id, edge_type, label, data) "
                        "VALUES (:id, CAST(:board_id AS UUID), :source, :target, 'signal', :label, '{}')"
                    ),
                    {
                        "id": "legacy-edge",
                        "board_id": board_id,
                        "source": "legacy-a",
                        "target": "legacy-b",
                        "label": "legacy link",
                    },
                )
                assert sync_board_from_legacy_canvas(conn, board_id) is True

            with pg_engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT name, graph_state "
                        "FROM investigation_boards WHERE id = :id"
                    ),
                    {"id": board_id},
                ).mappings().one()

            assert row["name"] == "test_legacy_back_sync"
            assert [node["id"] for node in row["graph_state"]["nodes"]] == [
                "legacy-a",
                "legacy-b",
            ]
            assert row["graph_state"]["edges"][0]["id"] == "legacy-edge"
        finally:
            with pg_engine.begin() as conn:
                conn.execute(text("DELETE FROM canvas_edges WHERE board_id = CAST(:id AS UUID)"), {"id": board_id})
                conn.execute(text("DELETE FROM canvas_nodes WHERE board_id = CAST(:id AS UUID)"), {"id": board_id})
                conn.execute(text("DELETE FROM canvas_boards WHERE id = CAST(:id AS UUID)"), {"id": board_id})
                conn.execute(text("DELETE FROM investigation_boards WHERE id = :id"), {"id": board_id})

    def test_node_edge_endpoints_update_investigation_board_graph_state(self, pg_engine, monkeypatch):
        import api.routers.canvas_graph as canvas_graph
        from api.routers.canvas_board_store import ensure_investigation_boards_table
        from api.routers.canvas_graph import EdgeCreate, NodeCreate, NodeUpdate

        monkeypatch.setattr(canvas_graph, "get_db_engine", lambda: pg_engine)
        ensure_investigation_boards_table(pg_engine)

        board_id = str(uuid.uuid4())
        with pg_engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO investigation_boards (id, name, description, graph_state) "
                    "VALUES (:id, :name, :description, '{}'::jsonb)"
                ),
                {"id": board_id, "name": "test_graph_state_bridge", "description": "bridge"},
            )

        try:
            node_a = _run(
                canvas_graph.add_node(
                    board_id,
                    NodeCreate(id="n-a", node_type="actor", label="Actor A", x=10, y=20),
                    _token="test",
                ),
            )
            node_b = _run(
                canvas_graph.add_node(
                    board_id,
                    NodeCreate(id="n-b", node_type="ticker", label="Ticker B", x=30, y=40),
                    _token="test",
                ),
            )
            edge = _run(
                canvas_graph.add_edge(
                    board_id,
                    EdgeCreate(id="e-ab", source="n-a", target="n-b", edge_type="signal"),
                    _token="test",
                ),
            )
            updated = _run(
                canvas_graph.update_node(
                    board_id,
                    "n-a",
                    NodeUpdate(label="Actor A updated", position_x=55),
                    _token="test",
                ),
            )

            assert node_a["node_id"] == "n-a"
            assert node_b["node_id"] == "n-b"
            assert edge["source_node_id"] == "n-a"
            assert updated["label"] == "Actor A updated"
            assert updated["position_x"] == 55

            with pg_engine.connect() as conn:
                state = conn.execute(
                    text("SELECT graph_state FROM investigation_boards WHERE id = :id"),
                    {"id": board_id},
                ).scalar_one()
                legacy_nodes = conn.execute(
                    text("SELECT node_id, label FROM canvas_nodes WHERE board_id = CAST(:id AS UUID)"),
                    {"id": board_id},
                ).fetchall()
                legacy_edges = conn.execute(
                    text(
                        "SELECT id, source_node_id, target_node_id "
                        "FROM canvas_edges WHERE board_id = CAST(:id AS UUID)"
                    ),
                    {"id": board_id},
                ).fetchall()

            assert {node["id"] for node in state["nodes"]} == {"n-a", "n-b"}
            assert state["nodes"][0]["label"] == "Actor A updated"
            assert state["edges"][0]["id"] == "e-ab"
            assert {row[0] for row in legacy_nodes} == {"n-a", "n-b"}
            assert legacy_edges == [("e-ab", "n-a", "n-b")]
        finally:
            with pg_engine.begin() as conn:
                conn.execute(text("DELETE FROM canvas_edges WHERE board_id = CAST(:id AS UUID)"), {"id": board_id})
                conn.execute(text("DELETE FROM canvas_nodes WHERE board_id = CAST(:id AS UUID)"), {"id": board_id})
                conn.execute(text("DELETE FROM canvas_boards WHERE id = CAST(:id AS UUID)"), {"id": board_id})
                conn.execute(text("DELETE FROM investigation_boards WHERE id = :id"), {"id": board_id})

    def test_bulk_graph_save_replaces_graph_state_and_legacy_shadow(self, pg_engine, monkeypatch):
        import api.routers.canvas_graph as canvas_graph
        from api.routers.canvas_board_store import ensure_investigation_boards_table
        from api.routers.canvas_graph import BulkGraphSave

        monkeypatch.setattr(canvas_graph, "get_db_engine", lambda: pg_engine)
        ensure_investigation_boards_table(pg_engine)

        board_id = str(uuid.uuid4())
        with pg_engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO investigation_boards (id, name, graph_state) "
                    "VALUES (:id, :name, '{}'::jsonb)"
                ),
                {"id": board_id, "name": "test_bulk_graph_state_bridge"},
            )

        try:
            result = _run(
                canvas_graph.bulk_save_graph(
                    board_id,
                    BulkGraphSave(
                        nodes=[
                            {"id": "n-1", "type": "note", "label": "One"},
                            {"id": "n-2", "type": "note", "label": "Two"},
                        ],
                        edges=[
                            {"id": "e-1", "source": "n-1", "target": "n-2", "type": "link"},
                            {"id": "e-bad", "source": "n-1", "target": "missing"},
                        ],
                    ),
                    _token="test",
                ),
            )

            assert result["status"] == "saved"

            with pg_engine.connect() as conn:
                state = conn.execute(
                    text("SELECT graph_state FROM investigation_boards WHERE id = :id"),
                    {"id": board_id},
                ).scalar_one()
                legacy_edge_ids = conn.execute(
                    text("SELECT id FROM canvas_edges WHERE board_id = CAST(:id AS UUID) ORDER BY id"),
                    {"id": board_id},
                ).fetchall()

            assert [node["id"] for node in state["nodes"]] == ["n-1", "n-2"]
            assert [edge["id"] for edge in state["edges"]] == ["e-1"]
            assert legacy_edge_ids == [("e-1",)]
        finally:
            with pg_engine.begin() as conn:
                conn.execute(text("DELETE FROM canvas_edges WHERE board_id = CAST(:id AS UUID)"), {"id": board_id})
                conn.execute(text("DELETE FROM canvas_nodes WHERE board_id = CAST(:id AS UUID)"), {"id": board_id})
                conn.execute(text("DELETE FROM canvas_boards WHERE id = CAST(:id AS UUID)"), {"id": board_id})
                conn.execute(text("DELETE FROM investigation_boards WHERE id = :id"), {"id": board_id})
