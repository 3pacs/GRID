"""Shared helpers for Canvas investigation board graph storage.

The current board CRUD endpoints persist graph state in
``investigation_boards.graph_state``. Some older Canvas routers still expect
``canvas_boards``/``canvas_nodes``/``canvas_edges`` rows. These helpers keep the
JSONB board state canonical and mirror it into the legacy tables on best effort
so those older paths can keep working while they are phased out.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine


_INVESTIGATION_BOARDS_DDL = """
CREATE TABLE IF NOT EXISTS investigation_boards (
    id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    name TEXT NOT NULL,
    description TEXT,
    graph_state JSONB NOT NULL DEFAULT '{}',
    camera_state JSONB,
    filters JSONB,
    pinned_nodes TEXT[] DEFAULT '{}',
    annotations JSONB DEFAULT '[]',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_investigation_boards_updated
    ON investigation_boards (updated_at DESC);
"""

_LEGACY_CANVAS_DDL = """
CREATE TABLE IF NOT EXISTS canvas_boards (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS canvas_nodes (
    node_id TEXT PRIMARY KEY,
    board_id UUID NOT NULL REFERENCES canvas_boards(id) ON DELETE CASCADE,
    node_type TEXT NOT NULL DEFAULT 'note',
    label TEXT,
    position_x DOUBLE PRECISION NOT NULL DEFAULT 0,
    position_y DOUBLE PRECISION NOT NULL DEFAULT 0,
    data JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS canvas_edges (
    id TEXT PRIMARY KEY,
    board_id UUID NOT NULL REFERENCES canvas_boards(id) ON DELETE CASCADE,
    source_node_id TEXT NOT NULL REFERENCES canvas_nodes(node_id) ON DELETE CASCADE,
    target_node_id TEXT NOT NULL REFERENCES canvas_nodes(node_id) ON DELETE CASCADE,
    edge_type TEXT DEFAULT 'default',
    label TEXT,
    data JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_canvas_nodes_board
    ON canvas_nodes (board_id);
CREATE INDEX IF NOT EXISTS idx_canvas_edges_board
    ON canvas_edges (board_id);
"""

_boards_ensured = False
_legacy_ensured = False


def ensure_investigation_boards_table(engine: Engine) -> None:
    """Create the canonical investigation board table if needed."""
    global _boards_ensured
    if _boards_ensured:
        return
    with engine.begin() as conn:
        _ensure_investigation_boards_on_connection(conn)
    _boards_ensured = True


def _ensure_investigation_boards_on_connection(conn: Connection) -> None:
    for statement in _split_sql_statements(_INVESTIGATION_BOARDS_DDL):
        conn.execute(text(statement))


def ensure_legacy_canvas_tables(conn: Connection) -> None:
    """Create legacy Canvas graph tables if they are missing."""
    global _legacy_ensured
    if _legacy_ensured:
        return
    for statement in _split_sql_statements(_LEGACY_CANVAS_DDL):
        conn.execute(text(statement))
    _legacy_ensured = True


def _split_sql_statements(sql: str) -> list[str]:
    return [statement.strip() for statement in sql.split(";") if statement.strip()]


def parse_json_value(value: Any, default: Any) -> Any:
    """Return a decoded JSON value, accepting DB driver strings or objects."""
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return default
    return value


def normalize_graph_state(raw_state: Any) -> dict[str, Any]:
    """Normalize stored graph state to a dict with list ``nodes`` and ``edges``."""
    state = parse_json_value(raw_state, {})
    if not isinstance(state, dict):
        state = {}
    normalized = dict(state)
    normalized["nodes"] = state.get("nodes") if isinstance(state.get("nodes"), list) else []
    normalized["edges"] = state.get("edges") if isinstance(state.get("edges"), list) else []
    return normalized


def _attrs(item: dict[str, Any]) -> dict[str, Any]:
    attrs = item.get("attributes")
    return attrs if isinstance(attrs, dict) else {}


def node_key(node: dict[str, Any]) -> str | None:
    value = node.get("id") or node.get("key") or node.get("node_id")
    return str(value) if value is not None and str(value) else None


def edge_key(edge: dict[str, Any]) -> str | None:
    value = edge.get("id") or edge.get("key") or edge.get("edge_id")
    return str(value) if value is not None and str(value) else None


def edge_source(edge: dict[str, Any]) -> str | None:
    value = edge.get("source") or edge.get("source_node_id") or edge.get("from")
    return str(value) if value is not None and str(value) else None


def edge_target(edge: dict[str, Any]) -> str | None:
    value = edge.get("target") or edge.get("target_node_id") or edge.get("to")
    return str(value) if value is not None and str(value) else None


def graph_node_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Convert API node payloads to the canonical graph node shape."""
    nid = payload.get("node_id") or payload.get("id") or payload.get("key")
    if nid is None:
        raise ValueError("node id is required")
    nid = str(nid)

    existing_attrs = _attrs(payload)
    node_type = (
        payload.get("node_type")
        or payload.get("nodeType")
        or payload.get("type")
        or existing_attrs.get("nodeType")
        or "note"
    )
    label = (
        payload.get("label")
        or payload.get("name")
        or existing_attrs.get("label")
        or nid
    )
    x = payload.get("position_x")
    if x is None:
        x = payload.get("x", existing_attrs.get("x", 0.0))
    y = payload.get("position_y")
    if y is None:
        y = payload.get("y", existing_attrs.get("y", 0.0))

    attrs = dict(existing_attrs)
    attrs.update({"nodeType": node_type, "label": label})
    if payload.get("data") is not None:
        attrs["data"] = payload["data"]

    node = dict(payload)
    node.update(
        {
            "id": nid,
            "key": nid,
            "type": node_type,
            "nodeType": node_type,
            "label": label,
            "x": x,
            "y": y,
            "attributes": attrs,
        },
    )
    node.pop("node_id", None)
    node.pop("position_x", None)
    node.pop("position_y", None)
    return node


def graph_edge_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Convert API edge payloads to the canonical graph edge shape."""
    source = payload.get("source_node_id") or payload.get("source") or payload.get("from")
    target = payload.get("target_node_id") or payload.get("target") or payload.get("to")
    if not source or not target:
        raise ValueError("source and target are required")

    source = str(source)
    target = str(target)
    eid = payload.get("edge_id") or payload.get("id") or payload.get("key")
    if eid is None:
        eid = f"{source}->{target}"
    eid = str(eid)

    existing_attrs = _attrs(payload)
    edge_type = (
        payload.get("edge_type")
        or payload.get("edgeKind")
        or payload.get("type")
        or existing_attrs.get("edgeKind")
        or "default"
    )

    attrs = dict(existing_attrs)
    attrs["edgeKind"] = edge_type
    if payload.get("data") is not None:
        attrs["data"] = payload["data"]

    edge = dict(payload)
    edge.update(
        {
            "id": eid,
            "key": eid,
            "source": source,
            "target": target,
            "edge_type": edge_type,
            "type": edge_type,
            "attributes": attrs,
        },
    )
    edge.pop("edge_id", None)
    edge.pop("source_node_id", None)
    edge.pop("target_node_id", None)
    return edge


def node_response(board_id: str, node: dict[str, Any]) -> dict[str, Any]:
    attrs = _attrs(node)
    nid = node_key(node)
    return {
        "node_id": nid,
        "id": nid,
        "board_id": board_id,
        "node_type": (
            node.get("node_type")
            or node.get("nodeType")
            or node.get("type")
            or attrs.get("nodeType")
            or "note"
        ),
        "label": node.get("label") or node.get("name") or attrs.get("label") or nid,
        "position_x": node.get("position_x", node.get("x", attrs.get("x", 0.0))),
        "position_y": node.get("position_y", node.get("y", attrs.get("y", 0.0))),
        "data": attrs.get("data", node.get("data")),
    }


def edge_response(board_id: str, edge: dict[str, Any]) -> dict[str, Any]:
    attrs = _attrs(edge)
    eid = edge_key(edge)
    return {
        "id": eid,
        "edge_id": eid,
        "board_id": board_id,
        "source_node_id": edge_source(edge),
        "target_node_id": edge_target(edge),
        "edge_type": (
            edge.get("edge_type")
            or edge.get("edgeKind")
            or edge.get("type")
            or attrs.get("edgeKind")
            or "default"
        ),
        "label": edge.get("label") or attrs.get("label"),
        "data": attrs.get("data", edge.get("data")),
    }


def get_board_graph_state(conn: Connection, board_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        text("SELECT graph_state FROM investigation_boards WHERE id = :board_id"),
        {"board_id": board_id},
    ).fetchone()
    if row is None:
        return None
    return normalize_graph_state(row[0])


def save_board_graph_state(conn: Connection, board_id: str, graph_state: dict[str, Any]) -> bool:
    result = conn.execute(
        text(
            "UPDATE investigation_boards "
            "SET graph_state = CAST(:graph_state AS JSONB), updated_at = NOW() "
            "WHERE id = :board_id"
        ),
        {"board_id": board_id, "graph_state": json.dumps(normalize_graph_state(graph_state))},
    )
    return result.rowcount > 0


def upsert_graph_node(graph_state: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    node = graph_node_from_payload(payload)
    nid = node_key(node)
    nodes = graph_state["nodes"]
    for idx, existing in enumerate(nodes):
        if isinstance(existing, dict) and node_key(existing) == nid:
            attrs = {**_attrs(existing), **_attrs(node)}
            nodes[idx] = {**existing, **node, "attributes": attrs}
            return nodes[idx]
    nodes.append(node)
    return node


def update_graph_node(graph_state: dict[str, Any], node_id: str, updates: dict[str, Any]) -> dict[str, Any] | None:
    nodes = graph_state["nodes"]
    for idx, existing in enumerate(nodes):
        if isinstance(existing, dict) and node_key(existing) == node_id:
            payload = {**existing, **updates, "id": node_id}
            if "position_x" in updates:
                payload["x"] = updates["position_x"]
            if "position_y" in updates:
                payload["y"] = updates["position_y"]
            if "data" in updates:
                attrs = {**_attrs(existing), "data": updates["data"]}
                payload["attributes"] = attrs
            nodes[idx] = graph_node_from_payload(payload)
            return nodes[idx]
    return None


def delete_graph_node(graph_state: dict[str, Any], node_id: str) -> dict[str, Any] | None:
    nodes = graph_state["nodes"]
    deleted = None
    kept_nodes = []
    for node in nodes:
        if isinstance(node, dict) and node_key(node) == node_id:
            deleted = node
        else:
            kept_nodes.append(node)
    if deleted is None:
        return None
    graph_state["nodes"] = kept_nodes
    graph_state["edges"] = [
        edge for edge in graph_state["edges"]
        if not (
            isinstance(edge, dict)
            and (edge_source(edge) == node_id or edge_target(edge) == node_id)
        )
    ]
    return deleted


def upsert_graph_edge(graph_state: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    edge = graph_edge_from_payload(payload)
    eid = edge_key(edge)
    edges = graph_state["edges"]
    for idx, existing in enumerate(edges):
        if isinstance(existing, dict) and edge_key(existing) == eid:
            attrs = {**_attrs(existing), **_attrs(edge)}
            edges[idx] = {**existing, **edge, "attributes": attrs}
            return edges[idx]
    edges.append(edge)
    return edge


def delete_graph_edge(graph_state: dict[str, Any], edge_id: str) -> dict[str, Any] | None:
    edges = graph_state["edges"]
    deleted = None
    kept_edges = []
    for edge in edges:
        if isinstance(edge, dict) and edge_key(edge) == edge_id:
            deleted = edge
        else:
            kept_edges.append(edge)
    if deleted is None:
        return None
    graph_state["edges"] = kept_edges
    return deleted


def sync_legacy_canvas_from_board(conn: Connection, board_id: str, graph_state: dict[str, Any] | None = None) -> None:
    """Mirror a canonical board into legacy canvas_* tables on best effort."""
    try:
        ensure_legacy_canvas_tables(conn)
        row = conn.execute(
            text(
                "SELECT id, name, description, graph_state "
                "FROM investigation_boards WHERE id = :board_id"
            ),
            {"board_id": board_id},
        ).mappings().fetchone()
        if row is None:
            return

        state = normalize_graph_state(graph_state if graph_state is not None else row["graph_state"])
        conn.execute(
            text(
                "INSERT INTO canvas_boards (id, name, description, updated_at) "
                "VALUES (CAST(:id AS UUID), :name, :description, NOW()) "
                "ON CONFLICT (id) DO UPDATE SET "
                "name = EXCLUDED.name, "
                "description = EXCLUDED.description, "
                "updated_at = NOW()"
            ),
            {"id": row["id"], "name": row["name"], "description": row["description"]},
        )
        conn.execute(text("DELETE FROM canvas_edges WHERE board_id = CAST(:id AS UUID)"), {"id": board_id})
        conn.execute(text("DELETE FROM canvas_nodes WHERE board_id = CAST(:id AS UUID)"), {"id": board_id})

        inserted_node_ids: set[str] = set()
        for node in state["nodes"]:
            if not isinstance(node, dict):
                continue
            nid = node_key(node)
            if not nid:
                continue
            response = node_response(board_id, node)
            inserted_node_ids.add(nid)
            conn.execute(
                text(
                    "INSERT INTO canvas_nodes "
                    "(node_id, board_id, node_type, label, position_x, position_y, data) "
                    "VALUES (:node_id, CAST(:board_id AS UUID), :node_type, :label, :x, :y, CAST(:data AS JSONB)) "
                    "ON CONFLICT (node_id) DO UPDATE SET "
                    "board_id = EXCLUDED.board_id, "
                    "node_type = EXCLUDED.node_type, "
                    "label = EXCLUDED.label, "
                    "position_x = EXCLUDED.position_x, "
                    "position_y = EXCLUDED.position_y, "
                    "data = EXCLUDED.data"
                ),
                {
                    "node_id": nid,
                    "board_id": board_id,
                    "node_type": response["node_type"],
                    "label": response["label"],
                    "x": response["position_x"] or 0.0,
                    "y": response["position_y"] or 0.0,
                    "data": json.dumps(response["data"] if response["data"] is not None else {}),
                },
            )

        for edge in state["edges"]:
            if not isinstance(edge, dict):
                continue
            eid = edge_key(edge)
            source = edge_source(edge)
            target = edge_target(edge)
            if not eid or not source or not target:
                continue
            if source not in inserted_node_ids or target not in inserted_node_ids:
                continue
            response = edge_response(board_id, edge)
            conn.execute(
                text(
                    "INSERT INTO canvas_edges "
                    "(id, board_id, source_node_id, target_node_id, edge_type, label, data) "
                    "VALUES (:id, CAST(:board_id AS UUID), :source, :target, :edge_type, :label, CAST(:data AS JSONB)) "
                    "ON CONFLICT (id) DO UPDATE SET "
                    "board_id = EXCLUDED.board_id, "
                    "source_node_id = EXCLUDED.source_node_id, "
                    "target_node_id = EXCLUDED.target_node_id, "
                    "edge_type = EXCLUDED.edge_type, "
                    "label = EXCLUDED.label, "
                    "data = EXCLUDED.data"
                ),
                {
                    "id": eid,
                    "board_id": board_id,
                    "source": source,
                    "target": target,
                    "edge_type": response["edge_type"],
                    "label": response["label"],
                    "data": json.dumps(response["data"] if response["data"] is not None else {}),
                },
            )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "Canvas legacy mirror skipped for board {board}: {error}",
            board=board_id,
            error=str(exc),
        )


def sync_board_from_legacy_canvas(conn: Connection, board_id: str) -> bool:
    """Mirror legacy canvas_* rows back into the canonical board graph_state."""
    try:
        _ensure_investigation_boards_on_connection(conn)
        ensure_legacy_canvas_tables(conn)
        board = conn.execute(
            text(
                "SELECT id::text AS id, name, description "
                "FROM canvas_boards WHERE id = CAST(:board_id AS UUID)"
            ),
            {"board_id": board_id},
        ).mappings().fetchone()
        if board is None:
            return False

        node_rows = conn.execute(
            text(
                "SELECT node_id, node_type, label, position_x, position_y, data "
                "FROM canvas_nodes "
                "WHERE board_id = CAST(:board_id AS UUID) "
                "ORDER BY created_at ASC"
            ),
            {"board_id": board_id},
        ).mappings().fetchall()

        nodes = []
        node_ids = set()
        for row in node_rows:
            nid = str(row["node_id"])
            node_ids.add(nid)
            nodes.append(
                graph_node_from_payload(
                    {
                        "id": nid,
                        "node_type": row["node_type"] or "note",
                        "label": row["label"],
                        "position_x": row["position_x"] or 0.0,
                        "position_y": row["position_y"] or 0.0,
                        "data": parse_json_value(row["data"], {}),
                    },
                ),
            )

        edge_rows = conn.execute(
            text(
                "SELECT id, source_node_id, target_node_id, edge_type, label, data "
                "FROM canvas_edges "
                "WHERE board_id = CAST(:board_id AS UUID) "
                "ORDER BY created_at ASC"
            ),
            {"board_id": board_id},
        ).mappings().fetchall()

        edges = []
        for row in edge_rows:
            source = str(row["source_node_id"])
            target = str(row["target_node_id"])
            if source not in node_ids or target not in node_ids:
                continue
            edges.append(
                graph_edge_from_payload(
                    {
                        "id": str(row["id"]),
                        "source": source,
                        "target": target,
                        "edge_type": row["edge_type"] or "default",
                        "label": row["label"],
                        "data": parse_json_value(row["data"], {}),
                    },
                ),
            )

        graph_state = {"nodes": nodes, "edges": edges}
        conn.execute(
            text(
                "INSERT INTO investigation_boards "
                "(id, name, description, graph_state, updated_at) "
                "VALUES (:id, :name, :description, CAST(:graph_state AS JSONB), NOW()) "
                "ON CONFLICT (id) DO UPDATE SET "
                "name = EXCLUDED.name, "
                "description = EXCLUDED.description, "
                "graph_state = EXCLUDED.graph_state, "
                "updated_at = NOW()"
            ),
            {
                "id": board["id"],
                "name": board["name"],
                "description": board["description"],
                "graph_state": json.dumps(graph_state),
            },
        )
        return True
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "Canvas graph_state mirror skipped for legacy board {board}: {error}",
            board=board_id,
            error=str(exc),
        )
        return False


def delete_legacy_canvas_board(conn: Connection, board_id: str) -> None:
    """Delete the legacy shadow board if the table exists."""
    try:
        ensure_legacy_canvas_tables(conn)
        conn.execute(
            text("DELETE FROM canvas_boards WHERE id = CAST(:board_id AS UUID)"),
            {"board_id": board_id},
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "Canvas legacy shadow delete skipped for board {board}: {error}",
            board=board_id,
            error=str(exc),
        )


def row_to_dict(row: Any) -> dict[str, Any]:
    """Convert a SQLAlchemy row to a plain dict with ISO timestamps."""
    d = dict(row._mapping)
    for key in ("created_at", "updated_at", "captured_at"):
        if key in d and isinstance(d[key], datetime):
            d[key] = d[key].isoformat()
    return d
