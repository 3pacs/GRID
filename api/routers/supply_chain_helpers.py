"""Helpers for the supply_chain router.

Extracted from ``supply_chain.py`` to keep the router under the 450-line
soft cap. All DB lookups, BFS traversal, label resolution, fallback
graph, response assembly, and template narrative live here. The router
itself only owns the FastAPI decorator + endpoint entry point.

Nothing in this module touches FastAPI — it is pure data access + shape
transformation, so it can be unit-tested without spinning up the app.
"""

from __future__ import annotations

import re
from collections import deque
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text

# ─── Constants ────────────────────────────────────────────────────────

_MAX_NODES: int = 200

_EDGE_COLS: str = (
    "upstream_id, downstream_id, relationship, input_type, annual_usd, "
    "pct_upstream_revenue, pct_downstream_cogs, chokepoint_score, "
    "confidence, as_of, source, "
    "relationship_weak, validation_correlation, weak_since"
)

# ─── Shared micro helpers ─────────────────────────────────────────────


def _slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (name or "").lower()).strip("_")


def _table_exists(conn: Any, table_name: str) -> bool:
    try:
        row = conn.execute(
            text("SELECT to_regclass(:n)").bindparams(n=table_name)
        ).fetchone()
        return bool(row and row[0])
    except Exception:
        return False


# ─── Actor resolution ─────────────────────────────────────────────────


def _lookup_sector_actor(
    actor_id: str,
) -> tuple[dict | None, str | None, str | None]:
    """Ticker/slug match against SECTOR_MAP. Returns (actor, sector, subsector)."""
    try:
        from analysis.sector_map import SECTOR_MAP
    except Exception:
        return None, None, None
    aid_upper = (actor_id or "").strip().upper()
    aid_slug = _slug(actor_id)
    for sector_name, sector in SECTOR_MAP.items():
        if not isinstance(sector, dict):
            continue
        for sub_name, sub in (sector.get("subsectors") or {}).items():
            if not isinstance(sub, dict):
                continue
            for actor in sub.get("actors") or []:
                tk = (actor.get("ticker") or "").upper()
                if (tk and tk == aid_upper) or _slug(actor.get("name", "")) == aid_slug:
                    return actor, sector_name, sub_name
    return None, None, None


def _lookup_db_node(conn: Any, actor_id: str) -> dict | None:
    """Three-pass lookup: exact id → case-insensitive id → name match.

    The frontend passes whatever id the canvas graph stores, which can be
    a lowercased ticker, an uppercased one, or the company's display name
    ("NVIDIA Corporation"). All three must resolve to the same supply
    chain node so the lens shows real coverage instead of "data pending".
    """
    if not _table_exists(conn, "supply_chain_nodes"):
        return None
    try:
        # Pass 1: exact id match
        row = conn.execute(
            text(
                "SELECT id, name, type, country, region, chokepoint_flag, notes "
                "FROM supply_chain_nodes WHERE id = :id LIMIT 1"
            ),
            {"id": actor_id},
        ).fetchone()
        # Pass 2: case-insensitive id match
        if not row:
            row = conn.execute(
                text(
                    "SELECT id, name, type, country, region, chokepoint_flag, notes "
                    "FROM supply_chain_nodes WHERE lower(id) = lower(:id) LIMIT 1"
                ),
                {"id": actor_id},
            ).fetchone()
        # Pass 3: name match (case-insensitive, prefix tolerant)
        # Strip common corporate suffixes so "NVIDIA Corporation" / "NVIDIA, Inc."
        # both reach the bare "NVIDIA" name we store.
        if not row:
            cleaned = actor_id.strip()
            for suffix in (" corporation", " corp", " inc.", " inc", " co.", " co",
                           " ltd.", " ltd", " plc", " sa", " ag", ", inc.", ", inc"):
                if cleaned.lower().endswith(suffix):
                    cleaned = cleaned[: -len(suffix)].rstrip(",.")
                    break
            row = conn.execute(
                text(
                    "SELECT id, name, type, country, region, chokepoint_flag, notes "
                    "FROM supply_chain_nodes "
                    "WHERE lower(name) = lower(:name) "
                    "   OR lower(name) LIKE lower(:like_prefix) "
                    "ORDER BY length(name) ASC LIMIT 1"
                ),
                {"name": cleaned, "like_prefix": f"{cleaned}%"},
            ).fetchone()
    except Exception as exc:
        log.debug("supply_chain: node lookup failed: {e}", e=str(exc))
        return None
    if not row:
        return None
    return {
        "id": row[0],
        "label": row[1] or row[0],
        "type": row[2] or "node",
        "country": row[3],
        "region": row[4],
        "chokepoint_flag": bool(row[5]) if row[5] is not None else False,
        "notes": row[6],
    }


def _resolve_actor(engine: Any, actor_id: str) -> tuple[dict, str] | None:
    """Resolve to (actor_meta, seed_id). Tries SECTOR_MAP then supply_chain_nodes.

    The seed loader stores ids in lowercase; SECTOR_MAP uses uppercase tickers.
    We return the lowercase form as the BFS seed so DB lookups succeed, while
    the display label/id stays uppercase for the UI.
    """
    actor, sector, subsector = _lookup_sector_actor(actor_id)
    if actor:
        tk = (actor.get("ticker") or actor_id).upper()
        return (
            {
                "id": tk,
                "label": actor.get("name") or tk,
                "type": actor.get("type") or "ticker",
                "sector": sector,
                "subsector": subsector,
            },
            tk.lower(),
        )
    with engine.connect() as conn:
        for variant in (actor_id, actor_id.lower(), actor_id.upper()):
            db_node = _lookup_db_node(conn, variant)
            if db_node:
                return (
                    {
                        "id": db_node["id"],
                        "label": db_node["label"],
                        "type": db_node["type"],
                    },
                    db_node["id"],
                )
    return None


# ─── Edge shaping / BFS ───────────────────────────────────────────────


def _edge_row(row: Any) -> dict[str, Any]:
    # Tail columns (11..13) may be missing on older DBs that have not yet
    # applied migration 0032. Fall back gracefully so the endpoint keeps
    # working either way.
    relationship_weak = bool(row[11]) if len(row) > 11 and row[11] is not None else False
    validation_correlation = (
        float(row[12]) if len(row) > 12 and row[12] is not None else None
    )
    weak_since = str(row[13]) if len(row) > 13 and row[13] is not None else None
    return {
        "source": row[0],
        "target": row[1],
        "relationship": row[2],
        "input_type": row[3],
        "annual_usd": float(row[4]) if row[4] is not None else None,
        "pct_upstream_revenue": float(row[5]) if row[5] is not None else None,
        "pct_downstream_cogs": float(row[6]) if row[6] is not None else None,
        "chokepoint_score": float(row[7]) if row[7] is not None else None,
        "confidence": row[8],
        "as_of": str(row[9]) if row[9] else None,
        "source_doc": row[10],
        "relationship_weak": relationship_weak,
        "validation_correlation": validation_correlation,
        "weak_since": weak_since,
    }


def _bfs(
    conn: Any, seed: str, depth: int, upstream: bool
) -> tuple[list[dict], dict[str, int]]:
    """BFS one direction. Upstream=True: follow downstream_id=node; tier negative.
    Downstream: follow upstream_id=node; tier positive."""
    edges: list[dict] = []
    tier: dict[str, int] = {seed: 0}
    frontier: deque[tuple[str, int]] = deque([(seed, 0)])
    seen: set[tuple[str, str]] = set()
    filter_col = "downstream_id" if upstream else "upstream_id"
    sign = -1 if upstream else 1
    next_field = "source" if upstream else "target"

    while frontier and len(tier) < _MAX_NODES:
        node, d = frontier.popleft()
        if d >= depth:
            continue
        try:
            # _EDGE_COLS is a module constant; filter_col is from hardcoded ternary
            edge_sql = (
                "SELECT " + _EDGE_COLS + " FROM supply_chain_edges "
                "WHERE " + filter_col + " = :n"
            )
            rows = conn.execute(text(edge_sql), {"n": node}).fetchall()
        except Exception as exc:
            log.debug("supply_chain bfs query failed: {e}", e=str(exc))
            return edges, tier
        for r in rows:
            e = _edge_row(r)
            key = (e["source"], e["target"])
            if key in seen:
                continue
            seen.add(key)
            edges.append(e)
            nxt = e[next_field]
            if nxt not in tier:
                tier[nxt] = sign * (d + 1)
                if len(tier) >= _MAX_NODES:
                    break
                frontier.append((nxt, d + 1))
    return edges, tier


def _resolve_labels(
    conn: Any, node_ids: list[str]
) -> dict[str, dict[str, Any]]:
    """Resolve ids via supply_chain_nodes, then SECTOR_MAP, then fallback title-case."""
    out: dict[str, dict[str, Any]] = {}
    if not node_ids:
        return out
    if _table_exists(conn, "supply_chain_nodes"):
        try:
            rows = conn.execute(
                text(
                    "SELECT id, name, type, country, region, chokepoint_flag, notes "
                    "FROM supply_chain_nodes WHERE id = ANY(:ids)"
                ),
                {"ids": node_ids},
            ).fetchall()
            for r in rows:
                out[r[0]] = {
                    "id": r[0],
                    "label": r[1] or r[0],
                    "type": r[2] or "node",
                    "country": r[3],
                    "region": r[4],
                    "chokepoint_flag": bool(r[5]) if r[5] is not None else False,
                    "notes": r[6],
                }
        except Exception as exc:
            log.debug("supply_chain label batch failed: {e}", e=str(exc))

    for nid in node_ids:
        if nid in out:
            continue
        actor, _s, _sub = _lookup_sector_actor(nid)
        if actor:
            out[nid] = {
                "id": nid,
                "label": actor.get("name") or nid,
                "type": actor.get("type") or "ticker",
                "country": None,
                "chokepoint_flag": False,
            }
        else:
            out[nid] = {
                "id": nid,
                "label": nid.replace("_", " ").title(),
                "type": "unknown",
                "country": None,
                "chokepoint_flag": False,
            }
    return out


# ─── Fallback graph ───────────────────────────────────────────────────


def _fallback_graph(actor_id: str, direction: str, depth: int) -> dict[str, Any]:
    """Build minimal graph from flows.py static maps + sector subsector overlap."""
    try:
        from api.routers.flows import _SUPPLY_CHAIN
    except Exception:
        _SUPPLY_CHAIN = {}
    actor, sector_name, subsector = _lookup_sector_actor(actor_id)
    ticker = (actor.get("ticker") if actor else actor_id or "").upper()
    label = (actor.get("name") if actor else actor_id) or actor_id
    atype = (actor.get("type") if actor else "ticker") or "ticker"

    nodes: dict[str, dict[str, Any]] = {
        ticker: {
            "id": ticker,
            "label": label,
            "type": atype,
            "tier": 0,
            "chokepoint": False,
            "annual_usd_total": 0.0,
        },
    }
    edges: list[dict[str, Any]] = []

    if direction in ("upstream", "both"):
        for commodity in _SUPPLY_CHAIN.get(ticker, []):
            cid = f"commodity_{commodity}"
            nodes[cid] = {
                "id": cid,
                "label": commodity.replace("_", " ").title(),
                "type": "commodity",
                "tier": -1,
                "chokepoint": False,
                "annual_usd_total": 0.0,
            }
            edges.append({
                "source": cid,
                "target": ticker,
                "relationship": "raw_material",
                "input_type": commodity,
                "annual_usd": None,
                "pct_upstream_revenue": None,
                "pct_downstream_cogs": None,
                "chokepoint_score": None,
                "confidence": "inferred",
                "as_of": None,
                "citation": "flows.py _SUPPLY_CHAIN static map",
                "relationship_weak": False,
                "validation_correlation": None,
                "weak_since": None,
            })

    if direction in ("downstream", "both") and sector_name and subsector:
        try:
            from analysis.sector_map import SECTOR_MAP
            sub = (
                (SECTOR_MAP.get(sector_name, {}) or {})
                .get("subsectors", {})
                .get(subsector, {})
            )
            for peer in (sub.get("actors") or [])[:10]:
                pt = (peer.get("ticker") or "").upper()
                if not pt or pt == ticker or pt in nodes:
                    continue
                nodes[pt] = {
                    "id": pt,
                    "label": peer.get("name") or pt,
                    "type": peer.get("type") or "ticker",
                    "tier": 1,
                    "chokepoint": False,
                    "annual_usd_total": 0.0,
                }
                edges.append({
                    "source": ticker,
                    "target": pt,
                    "relationship": "customer",
                    "input_type": None,
                    "annual_usd": None,
                    "pct_upstream_revenue": None,
                    "pct_downstream_cogs": None,
                    "chokepoint_score": None,
                    "confidence": "inferred",
                    "as_of": None,
                    "citation": "sector_map subsector overlap",
                    "relationship_weak": False,
                    "validation_correlation": None,
                    "weak_since": None,
                })
        except Exception as exc:
            log.debug("supply_chain fallback subsector failed: {e}", e=str(exc))

    upstream_nodes = [n for n in nodes.values() if n["tier"] < 0]
    downstream_nodes = [n for n in nodes.values() if n["tier"] > 0]
    narrative = _narrative(
        label, upstream_nodes, downstream_nodes, [], fallback=True
    )

    return {
        "actor": {"id": ticker, "label": label, "type": atype},
        "direction": direction,
        "depth": depth,
        "nodes": list(nodes.values()),
        "edges": edges,
        "chokepoints": [],
        "summary": {
            "upstream_count": len(upstream_nodes),
            "downstream_count": len(downstream_nodes),
            "upstream_annual_usd_total": 0.0,
            "downstream_annual_usd_total": 0.0,
            "largest_upstream": None,
            "largest_downstream": None,
        },
        "narrative": narrative,
        "provenance": {
            "rows": len(edges),
            "source": "fallback",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "confidence_mix": {"inferred": len(edges)},
        },
    }


# ─── DB result assembly ───────────────────────────────────────────────


def _assemble_db_result(
    actor_meta: dict,
    seed_id: str,
    direction: str,
    depth: int,
    up_edges: list[dict],
    down_edges: list[dict],
    up_tier: dict[str, int],
    down_tier: dict[str, int],
    label_map: dict[str, dict],
) -> dict[str, Any]:
    """Build the full response dict from BFS results + label resolution."""
    tier_map: dict[str, int] = dict(down_tier)
    for k, v in up_tier.items():
        if k == seed_id:
            tier_map[k] = 0
        elif k not in tier_map or tier_map[k] >= 0:
            tier_map[k] = v
    all_edges = up_edges + down_edges
    node_ids = list(tier_map.keys())

    chokepoint_flags = {nid: False for nid in node_ids}
    chokepoint_scores: dict[str, float] = {}
    annual_totals = {nid: 0.0 for nid in node_ids}
    for e in all_edges:
        src, tgt = e["source"], e["target"]
        amt = e.get("annual_usd") or 0
        if src in annual_totals:
            annual_totals[src] += amt
        if tgt in annual_totals:
            annual_totals[tgt] += amt
        cs = e.get("chokepoint_score") or 0
        if cs >= 0.5:
            for n in (src, tgt):
                chokepoint_flags[n] = True
                chokepoint_scores[n] = max(chokepoint_scores.get(n, 0.0), cs)
    for nid, meta in label_map.items():
        if meta.get("chokepoint_flag"):
            chokepoint_flags[nid] = True

    nodes_out = [
        {
            "id": nid,
            "label": label_map.get(nid, {}).get("label") or nid,
            "type": label_map.get(nid, {}).get("type") or "unknown",
            "tier": t,
            "country": label_map.get(nid, {}).get("country"),
            "chokepoint": chokepoint_flags.get(nid, False),
            "notes": label_map.get(nid, {}).get("notes"),
            "annual_usd_total": annual_totals.get(nid, 0.0),
        }
        for nid, t in tier_map.items()
    ]

    chokepoints = []
    for nid, score in sorted(
        chokepoint_scores.items(), key=lambda kv: kv[1], reverse=True
    ):
        meta = label_map.get(nid, {})
        downstream_tickers = [
            n["id"]
            for n in nodes_out
            if n["tier"] > tier_map.get(nid, 0) and n["type"] == "ticker"
        ][:10]
        chokepoints.append({
            "id": nid,
            "label": meta.get("label") or nid,
            "score": round(score, 3),
            "reason": meta.get("notes") or "High chokepoint score from edge",
            "downstream_impact": downstream_tickers,
        })

    upstream_nodes = [n for n in nodes_out if n["tier"] < 0]
    downstream_nodes = [n for n in nodes_out if n["tier"] > 0]
    up_usd_total = sum((e.get("annual_usd") or 0) for e in up_edges)
    down_usd_total = sum((e.get("annual_usd") or 0) for e in down_edges)
    largest_up = max(
        (e for e in up_edges if e.get("annual_usd")),
        key=lambda e: e["annual_usd"],
        default=None,
    )
    largest_down = max(
        (e for e in down_edges if e.get("annual_usd")),
        key=lambda e: e["annual_usd"],
        default=None,
    )
    conf_mix: dict[str, int] = {}
    for e in all_edges:
        c = e.get("confidence") or "unknown"
        conf_mix[c] = conf_mix.get(c, 0) + 1

    edges_out = [
        {**{k: v for k, v in e.items() if k != "source_doc"},
         "citation": e.get("source_doc")}
        for e in all_edges
    ]

    return {
        "actor": {
            "id": actor_meta["id"],
            "label": actor_meta["label"],
            "type": actor_meta["type"],
        },
        "direction": direction,
        "depth": depth,
        "nodes": nodes_out,
        "edges": edges_out,
        "chokepoints": chokepoints,
        "summary": {
            "upstream_count": len(upstream_nodes),
            "downstream_count": len(downstream_nodes),
            "upstream_annual_usd_total": float(up_usd_total),
            "downstream_annual_usd_total": float(down_usd_total),
            "largest_upstream": (
                {"id": largest_up["source"], "annual_usd": largest_up["annual_usd"]}
                if largest_up else None
            ),
            "largest_downstream": (
                {"id": largest_down["target"], "annual_usd": largest_down["annual_usd"]}
                if largest_down else None
            ),
        },
        "narrative": _narrative(
            actor_meta["label"], upstream_nodes, downstream_nodes, chokepoints
        ),
        "provenance": {
            "rows": len(all_edges),
            "source": "db",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "confidence_mix": conf_mix,
        },
    }


# ─── Narrative template ───────────────────────────────────────────────


def _narrative(
    actor_label: str,
    upstream: list[dict],
    downstream: list[dict],
    chokepoints: list[dict],
    fallback: bool = False,
) -> str:
    """Template narrative — no LLM."""
    parts: list[str] = []
    if chokepoints:
        top = chokepoints[0]
        parts.append(
            f"{actor_label}'s most acute supply risk is {top.get('label', top['id'])} "
            f"(chokepoint score {top.get('score', 0):.2f}): "
            f"{top.get('reason', 'concentrated exposure')}."
        )
    if upstream:
        top_up = max(upstream, key=lambda n: n.get("annual_usd_total") or 0)
        parts.append(
            f"Upstream dependence runs through {top_up['label']} "
            f"and {len(upstream) - 1} other supplier(s)."
        )
    if downstream:
        top_down = max(downstream, key=lambda n: n.get("annual_usd_total") or 0)
        parts.append(
            f"Largest downstream channel is {top_down['label']} "
            f"across {len(downstream)} counterparty node(s)."
        )
    if not parts:
        return (
            f"No supply-chain graph data found for {actor_label}."
            + (" Fallback graph returned minimal context." if fallback else "")
        )
    return " ".join(parts)
