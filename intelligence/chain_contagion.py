"""Chain contagion simulator.

Propagate an upstream shock (commodity price spike, supplier outage) through
the directed supply_chain_edges DAG and compute cascading gross-margin impact
and USD revenue at risk for each affected downstream actor.

Algorithm
---------
Starting from ``shock_node_id``, BFS strictly downstream (follow
``upstream_id -> downstream_id`` edges). At every edge we compute a per-edge
margin impact for the downstream node:

    edge_impact = incoming_shock
                  * effective_pct_cogs
                  * pass_through_factor
                  * (1 - alt_availability)  # only for supply_disruption shocks

- ``incoming_shock`` starts at ``shock_magnitude`` for the seed node and gets
  attenuated by a fixed ``HOP_ATTENUATION`` factor every hop so effects can't
  accumulate unbounded.
- ``effective_pct_cogs`` uses ``edge.pct_downstream_cogs`` when known, else a
  conservative default (``DEFAULT_PCT_COGS``).
- ``pass_through_factor`` models how much of a supplier cost hike actually
  lands on the buyer's margin (hedges, substitution, retail price hikes
  absorb part of the shock).
- ``alt_availability`` for ``supply_disruption`` shocks derives from the
  number of alternative suppliers (via supply_chokepoints.find_alternatives).
  More alternatives -> less impact.

Margin hits accumulate at each downstream actor across every inbound path.
If the actor has revenue data in ``capital_flows`` we project
``revenue_at_risk_usd = latest_revenue * |margin_impact_pct|``.

Everything is pure: the DB engine is injected, no module-level state, no
LLM calls. Cycle-safe (visited set, max node cap). DB errors are swallowed
and fall through to a best-effort result so callers never see 500s.

Public API
----------
    simulate_contagion(engine, shock_node_id, shock_type, shock_magnitude,
                       max_depth=4) -> dict
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable
from uuid import uuid4

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# Producer module tag for every SignalFired emitted from this module.
# Matches the pattern used by the SYNTH-B detectors so routing and
# observability stay uniform.
_PRODUCER_MODULE = "intelligence.chain_contagion"


# ── Tunables ─────────────────────────────────────────────────────────────────

DEFAULT_PASS_THROUGH: float = 0.70
HOP_ATTENUATION: float = 0.50
DEFAULT_PCT_COGS: float = 0.05
MAX_NODES: int = 500
RANK_TOP_N: int = 50
VALID_SHOCK_TYPES: frozenset[str] = frozenset({"price_increase", "supply_disruption"})
EMPIRICAL_PASS_THROUGH_MIN: float = 0.1
EMPIRICAL_PASS_THROUGH_MAX: float = 1.0


# ── Internal data containers ─────────────────────────────────────────────────

@dataclass(frozen=True)
class ShockSpec:
    node_id: str
    shock_type: str
    magnitude: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "type": self.shock_type,
            "magnitude": self.magnitude,
        }


@dataclass
class ActorImpact:
    """Per-downstream accumulator for BFS traversal."""

    node_id: str
    tier: int = 0
    margin_impact_pct: float = 0.0  # negative = margin hit
    edge_count: int = 0
    paths: list[list[str]] = field(default_factory=list)

    def worst_path(self) -> list[str]:
        return self.paths[0] if self.paths else [self.node_id]


# ── Query helpers ────────────────────────────────────────────────────────────

_EDGE_COLS = (
    "upstream_id, downstream_id, relationship, input_type, annual_usd, "
    "pct_downstream_cogs, chokepoint_score"
)


def _table_exists(conn: Any, name: str) -> bool:
    try:
        row = conn.execute(
            text("SELECT to_regclass(:n)").bindparams(n=name)
        ).fetchone()
        return bool(row and row[0])
    except Exception:
        return False


def _fetch_downstream_edges(conn: Any, node_id: str) -> list[dict[str, Any]]:
    # _EDGE_COLS is a module constant
    edge_sql = (
        "SELECT " + _EDGE_COLS + " FROM supply_chain_edges "
        "WHERE upstream_id = :n"
    )
    try:
        rows = conn.execute(text(edge_sql), {"n": node_id}).fetchall()
    except Exception as exc:
        log.debug("chain_contagion: edge fetch failed for {n}: {e}", n=node_id, e=str(exc))
        return []
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "upstream_id": r[0],
                "downstream_id": r[1],
                "relationship": r[2],
                "input_type": r[3],
                "annual_usd": float(r[4]) if r[4] is not None else None,
                "pct_downstream_cogs": (
                    float(r[5]) if r[5] is not None else None
                ),
                "chokepoint_score": float(r[6]) if r[6] is not None else None,
            }
        )
    return out


def _alt_count(conn: Any, downstream_id: str, input_type: str | None) -> int:
    """Return number of distinct alternative upstream suppliers for the same
    input_type. Returns 0 on failure so the default is max risk.
    """
    try:
        rows = conn.execute(
            text(
                """
                SELECT COUNT(DISTINCT upstream_id)
                FROM supply_chain_edges
                WHERE downstream_id = :d
                  AND (
                        (:it IS NULL AND input_type IS NULL)
                     OR input_type = :it
                  )
                """
            ),
            {"d": downstream_id, "it": input_type},
        ).fetchone()
    except Exception:
        return 1
    return int(rows[0]) if rows and rows[0] is not None else 1


def _empirical_pass_through(
    conn: Any, upstream_id: str, downstream_id: str
) -> float | None:
    """Return the empirical pass-through factor from supply_shock_attributions
    if we have historical data for this (upstream, downstream) pair, else None.

    Uses the most recent lagged_correlation row. The absolute value of the
    correlation is clamped to [EMPIRICAL_PASS_THROUGH_MIN, EMPIRICAL_PASS_THROUGH_MAX]
    so that a measured link of e.g. -0.82 returns 0.82.

    Never raises — on any DB failure we return None and let the caller fall
    back to the flat default.
    """
    if not upstream_id or not downstream_id:
        return None
    if not _table_exists(conn, "supply_shock_attributions"):
        return None
    try:
        row = conn.execute(
            text(
                """
                SELECT correlation FROM supply_shock_attributions
                WHERE upstream_id = :up AND downstream_id = :dn
                  AND method = 'lagged_correlation'
                ORDER BY as_of DESC LIMIT 1
                """
            ).bindparams(up=upstream_id, dn=downstream_id),
        ).fetchone()
    except Exception as exc:
        log.debug(
            "chain_contagion: empirical pass-through lookup failed "
            "({u}->{d}): {e}",
            u=upstream_id,
            d=downstream_id,
            e=str(exc),
        )
        return None
    if row is None or row[0] is None:
        return None
    try:
        value = abs(float(row[0]))
    except (TypeError, ValueError):
        return None
    return max(EMPIRICAL_PASS_THROUGH_MIN, min(EMPIRICAL_PASS_THROUGH_MAX, value))


def _mitigation_for(
    conn: Any,
    downstream_id: str,
    upstream_id: str,
    input_type: str | None,
) -> list[dict[str, Any]]:
    """Return up-to-3 alternate upstream suppliers for the same ``input_type``.

    Pulls every edge feeding ``downstream_id`` with matching ``input_type``
    via ``supply_chokepoints.find_alternatives``, drops the focal upstream,
    joins chokepoint scores, and returns the most-substitutable (lowest
    chokepoint_score) first. Returns an empty list on any failure — the
    simulator never raises out of this path.
    """
    try:
        # Local import to avoid a hard dependency cycle at module import time.
        from intelligence.supply_chokepoints import find_alternatives
    except Exception:
        return []
    try:
        rows = find_alternatives(conn, downstream_id, input_type)
    except Exception:
        return []

    focal = (upstream_id or "").lower()
    alt_ids: list[str] = []
    seen: set[str] = set()
    for r in rows:
        uid = (r.get("upstream_id") or "").lower()
        if not uid or uid == focal or uid in seen:
            continue
        seen.add(uid)
        alt_ids.append(uid)
    if not alt_ids:
        return []

    # Pull chokepoint_score + name in one hop.
    scores: dict[str, float | None] = {}
    labels: dict[str, str] = {}
    try:
        score_rows = conn.execute(
            text(
                """
                SELECT upstream_id, MIN(chokepoint_score)
                FROM supply_chain_edges
                WHERE downstream_id = :d
                  AND upstream_id = ANY(:ids)
                  AND (
                        (:it IS NULL AND input_type IS NULL)
                     OR input_type = :it
                  )
                GROUP BY upstream_id
                """
            ),
            {"d": downstream_id, "ids": alt_ids, "it": input_type},
        ).fetchall()
        for r in score_rows:
            scores[r[0]] = float(r[1]) if r[1] is not None else None
    except Exception:
        pass
    try:
        if _table_exists(conn, "supply_chain_nodes"):
            label_rows = conn.execute(
                text(
                    "SELECT id, name FROM supply_chain_nodes WHERE id = ANY(:ids)"
                ),
                {"ids": alt_ids},
            ).fetchall()
            for r in label_rows:
                labels[r[0]] = r[1] or r[0]
    except Exception:
        pass

    enriched = [
        {
            "id": aid,
            "name": labels.get(aid, aid),
            "chokepoint_score": scores.get(aid),
        }
        for aid in alt_ids
    ]
    # Lowest chokepoint_score first (most substitutable). None scores sort last.
    enriched.sort(
        key=lambda a: (
            a["chokepoint_score"] is None,
            a["chokepoint_score"] if a["chokepoint_score"] is not None else 1.0,
        )
    )
    return enriched[:3]


def _build_mitigations(
    conn: Any,
    affected_inputs: dict[str, set[tuple[str, str | None]]],
    shock_node_id: str,
) -> dict[str, list[dict[str, Any]]]:
    """For each downstream, pick the representative (upstream, input_type)
    pair linked to the shock seed (prefer the seed itself if present) and
    compute its top-3 alternatives. Returns {downstream_id: [mitigations]}.
    """
    out: dict[str, list[dict[str, Any]]] = {}
    for downstream, pairs in affected_inputs.items():
        chosen: tuple[str, str | None] | None = None
        # Prefer the edge sourced directly from the shock node itself.
        for up, it in pairs:
            if up == shock_node_id:
                chosen = (up, it)
                break
        if chosen is None and pairs:
            chosen = next(iter(pairs))
        if chosen is None:
            continue
        mitigations = _mitigation_for(conn, downstream, chosen[0], chosen[1])
        if mitigations:
            out[downstream] = mitigations
    return out


def _load_latest_revenue(conn: Any, actor_ids: Iterable[str]) -> dict[str, float]:
    ids = [i for i in set(actor_ids) if i]
    if not ids:
        return {}
    if not _table_exists(conn, "capital_flows"):
        return {}
    try:
        rows = conn.execute(
            text(
                """
                SELECT DISTINCT ON (actor_id) actor_id, amount_usd
                FROM capital_flows
                WHERE actor_id = ANY(:ids)
                  AND flow_type = 'revenue'
                  AND period_type IN ('annual', 'ttm')
                ORDER BY actor_id, fiscal_period DESC
                """
            ),
            {"ids": ids},
        ).fetchall()
    except Exception as exc:
        log.debug("chain_contagion: revenue lookup failed: {e}", e=str(exc))
        return {}
    out: dict[str, float] = {}
    for r in rows:
        if r[1] is not None:
            try:
                out[r[0]] = float(r[1])
            except (TypeError, ValueError):
                continue
    return out


def _load_labels(conn: Any, node_ids: Iterable[str]) -> dict[str, str]:
    ids = [i for i in set(node_ids) if i]
    if not ids or not _table_exists(conn, "supply_chain_nodes"):
        return {}
    try:
        rows = conn.execute(
            text("SELECT id, name FROM supply_chain_nodes WHERE id = ANY(:ids)"),
            {"ids": ids},
        ).fetchall()
    except Exception:
        return {}
    return {r[0]: (r[1] or r[0]) for r in rows}


# ── Pure impact math ─────────────────────────────────────────────────────────

def _alt_availability(alt_count: int) -> float:
    """0 -> 0.0 (nothing available), 1 -> ~0.0 (the disrupted one is the only source),
    2 -> 0.5, 3 -> 0.667, etc. Used only for supply_disruption shocks.
    """
    if alt_count <= 1:
        return 0.0
    return max(0.0, 1.0 - 1.0 / float(alt_count))


def _edge_impact(
    incoming_shock: float,
    shock_type: str,
    pct_cogs: float | None,
    pass_through: float,
    alt_count: int,
) -> float:
    """Compute the downstream margin hit from a single edge.

    Returns a NEGATIVE number when there is any impact (margin erosion).
    ``incoming_shock`` is the already-attenuated absolute shock magnitude
    arriving at the upstream side of this edge.
    """
    if incoming_shock == 0:
        return 0.0
    effective_pct = pct_cogs if pct_cogs is not None else DEFAULT_PCT_COGS
    if effective_pct > 1.0:  # some seeds may use 0-100 scale
        effective_pct = effective_pct / 100.0
    effective_pct = max(0.0, min(1.0, effective_pct))

    if shock_type == "supply_disruption":
        # A disruption cuts supply, which lands on the buyer as either a
        # lost-revenue or cost-spike event scaled by how much of their COGS
        # depends on that supplier AND how unavailable substitutes are.
        availability = _alt_availability(alt_count)
        disruption_factor = 1.0 - availability
        raw = incoming_shock * effective_pct * disruption_factor
    else:
        # price_increase: cost inflation × pass-through to margin
        raw = incoming_shock * effective_pct * pass_through

    return -abs(raw)


# ── BFS simulator ────────────────────────────────────────────────────────────

def _simulate(
    conn: Any,
    shock: ShockSpec,
    max_depth: int,
    pass_through: float,
    stats: dict[str, int] | None = None,
    affected_inputs: dict[str, set[tuple[str, str | None]]] | None = None,
) -> dict[str, ActorImpact]:
    """BFS downstream from shock.node_id. Returns impacts keyed by node id.

    The seed itself is included with zero impact (it's the source).

    ``stats`` (if provided) accumulates {"empirical": N, "default": N}
    counts so the caller can report how many edges used an empirical prior
    vs. the flat default. ``affected_inputs`` (if provided) collects the set
    of (upstream_id, input_type) pairs that touched each downstream id so
    the caller can build mitigation suggestions.
    """
    impacts: dict[str, ActorImpact] = {
        shock.node_id: ActorImpact(node_id=shock.node_id, tier=0)
    }
    # Frontier entries carry (node, tier, incoming_shock_magnitude, path_so_far).
    frontier: deque[tuple[str, int, float, tuple[str, ...]]] = deque(
        [(shock.node_id, 0, shock.magnitude, (shock.node_id,))]
    )
    visited_edges: set[tuple[str, str, str | None]] = set()

    while frontier and len(impacts) < MAX_NODES:
        node, depth, incoming, path = frontier.popleft()
        if depth >= max_depth:
            continue
        edges = _fetch_downstream_edges(conn, node)
        for e in edges:
            key = (e["upstream_id"], e["downstream_id"], e["input_type"])
            if key in visited_edges:
                continue
            visited_edges.add(key)
            downstream = e["downstream_id"]
            if downstream == shock.node_id:
                continue  # prevent self-loop

            alt_count = 1
            if shock.shock_type == "supply_disruption":
                alt_count = _alt_count(conn, downstream, e["input_type"])

            # Empirical pass-through prior — falls back to the caller's
            # default when we have no cross-lens attribution for this pair.
            empirical = _empirical_pass_through(
                conn, e["upstream_id"], downstream
            )
            edge_pass_through = empirical if empirical is not None else pass_through
            if stats is not None:
                if empirical is not None:
                    stats["empirical"] = stats.get("empirical", 0) + 1
                else:
                    stats["default"] = stats.get("default", 0) + 1

            if affected_inputs is not None:
                affected_inputs.setdefault(downstream, set()).add(
                    (e["upstream_id"], e["input_type"])
                )

            hit = _edge_impact(
                incoming_shock=incoming,
                shock_type=shock.shock_type,
                pct_cogs=e["pct_downstream_cogs"],
                pass_through=edge_pass_through,
                alt_count=alt_count,
            )

            impact = impacts.get(downstream)
            new_tier = depth + 1
            if impact is None:
                impact = ActorImpact(node_id=downstream, tier=new_tier)
                impacts[downstream] = impact
            else:
                impact.tier = min(impact.tier, new_tier) if impact.tier else new_tier
            impact.margin_impact_pct += hit
            impact.edge_count += 1
            impact.paths.append(list(path) + [downstream])

            if len(impacts) >= MAX_NODES:
                break

            # Propagate the downstream cost inflation further, attenuated.
            next_shock = abs(hit) * HOP_ATTENUATION
            if next_shock > 1e-6 and new_tier < max_depth:
                frontier.append(
                    (downstream, new_tier, next_shock, path + (downstream,))
                )

    return impacts


# ── Narrative & shaping ──────────────────────────────────────────────────────

def _narrative(
    shock: ShockSpec,
    ranked: list[dict[str, Any]],
    total_affected: int,
    total_rev_risk: float,
    label_map: dict[str, str],
) -> str:
    if not ranked:
        return (
            f"No downstream exposure found for {shock.node_id} under a "
            f"{int(shock.magnitude * 100)}% {shock.shock_type.replace('_', ' ')} shock."
        )

    label = label_map.get(shock.node_id, shock.node_id.replace("_", " ").title())
    verb = (
        "price increase"
        if shock.shock_type == "price_increase"
        else "supply disruption"
    )
    magnitude_pct = int(round(shock.magnitude * 100))

    # Worst tier-1 victim (actors at tier == 1)
    tier1 = [a for a in ranked if a.get("tier") == 1]
    worst_t1 = tier1[0] if tier1 else ranked[0]
    worst_overall = ranked[0]

    parts: list[str] = []
    parts.append(
        f"A {magnitude_pct}% {verb} in {label} cascades through "
        f"{total_affected} downstream actor(s)."
    )
    parts.append(
        f"First-tier casualty: {label_map.get(worst_t1['id'], worst_t1['id'])} "
        f"absorbs {worst_t1['margin_impact_pct'] * 100:.1f}% gross margin hit."
    )
    if worst_overall["id"] != worst_t1["id"]:
        parts.append(
            f"Worst downstream ticker is {label_map.get(worst_overall['id'], worst_overall['id'])} "
            f"at {worst_overall['margin_impact_pct'] * 100:.1f}% margin impact "
            f"via {' -> '.join(worst_overall['path'])}."
        )
    else:
        parts.append(
            f"It is also the worst downstream ticker via "
            f"{' -> '.join(worst_overall['path'])}."
        )
    if total_rev_risk > 0:
        parts.append(
            f"Estimated aggregate revenue at risk: "
            f"${total_rev_risk / 1e9:.1f}B."
        )
    return " ".join(parts)


def _shape_impact(
    impact: ActorImpact,
    revenue: float | None,
    mitigation: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    rev_risk = (
        abs(impact.margin_impact_pct) * revenue
        if revenue is not None and revenue > 0
        else 0.0
    )
    return {
        "id": impact.node_id,
        "tier": impact.tier,
        "margin_impact_pct": round(impact.margin_impact_pct, 6),
        "edge_count": impact.edge_count,
        "revenue_at_risk_usd": round(rev_risk, 2),
        "path": impact.worst_path(),
        "mitigation": mitigation or [],
    }


def _emit_contagion_signals(
    ranked: list[dict[str, Any]],
    shock_node: str,
) -> None:
    """Emit one ``SignalFired`` per ranked victim.

    Non-fatal: every failure (import error, bus down, audit write fail)
    is swallowed so ``simulate_contagion`` always returns its result dict.
    """
    if not ranked:
        return
    try:
        from contracts.correlation import (
            get_current_correlation_id,
            new_correlation_id,
        )
        from contracts.emit import emit as _emit
        from contracts.schemas import SignalFired
    except Exception as exc:  # pragma: no cover — defensive import guard
        log.debug(
            "chain_contagion: contracts import failed: {e}", e=str(exc)
        )
        return

    try:
        corr_id = get_current_correlation_id() or new_correlation_id()
    except Exception:
        corr_id = None
    if corr_id is None:
        return

    for victim in ranked:
        ticker = victim.get("id")
        margin = victim.get("margin_impact_pct")
        if not ticker or margin is None:
            continue
        try:
            strength = float(margin)
        except (TypeError, ValueError):
            continue
        if strength == 0.0:
            continue
        # Clamp to [-1, 1] — margin_impact_pct is a fraction but can
        # occasionally edge outside in cascade stacking. SignalFired
        # strength downstream consumers expect that envelope.
        strength = max(-1.0, min(1.0, strength))
        try:
            _emit(
                SignalFired(
                    producer_module=_PRODUCER_MODULE,
                    correlation_id=corr_id,
                    signal_id=uuid4(),
                    source=f"chain_contagion:{shock_node}",
                    signal_type="contagion_ranked_impact",
                    strength=strength,
                    ticker=str(ticker),
                    actor_hint=None,
                    raw_row_ids=[],
                )
            )
        except Exception as exc:  # non-fatal per SYNTH-C contract
            log.debug(
                "chain_contagion emit failed for {t}: {e}",
                t=ticker, e=str(exc),
            )


def _group_waves(ranked: list[dict[str, Any]], max_depth: int) -> list[dict[str, Any]]:
    by_tier: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for a in ranked:
        by_tier[int(a["tier"])].append(a)
    waves: list[dict[str, Any]] = []
    for tier in sorted(by_tier):
        if tier == 0 or tier > max_depth:
            continue
        waves.append({"tier": tier, "actors": by_tier[tier]})
    return waves


# ── Public entry point ──────────────────────────────────────────────────────

def simulate_contagion(
    engine: Engine,
    shock_node_id: str,
    shock_type: str = "price_increase",
    shock_magnitude: float = 0.30,
    max_depth: int = 4,
    pass_through: float = DEFAULT_PASS_THROUGH,
) -> dict[str, Any]:
    """Propagate a shock through ``supply_chain_edges`` and rank downstream
    margin / revenue impact. See module docstring for algorithm detail.
    """
    if not shock_node_id:
        raise ValueError("shock_node_id required")
    if shock_type not in VALID_SHOCK_TYPES:
        raise ValueError(
            f"shock_type must be one of {sorted(VALID_SHOCK_TYPES)}, got {shock_type!r}"
        )
    if max_depth < 1:
        raise ValueError("max_depth must be >= 1")
    max_depth = min(max_depth, 6)

    seed = shock_node_id.strip().lower()
    shock = ShockSpec(
        node_id=seed,
        shock_type=shock_type,
        magnitude=float(shock_magnitude),
    )

    impacts: dict[str, ActorImpact] = {}
    label_map: dict[str, str] = {}
    revenues: dict[str, float] = {}
    pass_through_stats: dict[str, int] = {"empirical": 0, "default": 0}
    affected_inputs: dict[str, set[tuple[str, str | None]]] = {}
    mitigations: dict[str, list[dict[str, Any]]] = {}

    try:
        with engine.connect() as conn:
            if not _table_exists(conn, "supply_chain_edges"):
                log.warning("chain_contagion: supply_chain_edges missing")
            else:
                impacts = _simulate(
                    conn,
                    shock,
                    max_depth,
                    pass_through,
                    stats=pass_through_stats,
                    affected_inputs=affected_inputs,
                )
                actor_ids = list(impacts.keys())
                label_map = _load_labels(conn, actor_ids)
                revenues = _load_latest_revenue(conn, actor_ids)
                mitigations = _build_mitigations(
                    conn, affected_inputs, shock.node_id
                )
    except Exception as exc:
        log.warning("chain_contagion: simulation failed: {e}", e=str(exc))

    shaped = [
        _shape_impact(imp, revenues.get(nid), mitigations.get(nid))
        for nid, imp in impacts.items()
        if nid != shock.node_id and imp.margin_impact_pct != 0.0
    ]
    shaped.sort(key=lambda a: abs(a["margin_impact_pct"]), reverse=True)
    ranked = shaped[:RANK_TOP_N]

    waves = _group_waves(shaped, max_depth)
    total_affected = len(shaped)
    total_rev_risk = sum(a["revenue_at_risk_usd"] for a in shaped)

    tier1 = [a for a in shaped if a["tier"] == 1]
    worst_t1 = tier1[0]["id"] if tier1 else None
    worst_ticker = ranked[0]["id"] if ranked else None

    # SYNTH-C / SYNTH-35 — non-fatal SignalFired fanout. One signal per
    # ranked victim so trust_scorer + oracle_signals can pick it up.
    _emit_contagion_signals(ranked, shock.node_id)

    return {
        "shock": shock.as_dict(),
        "waves": waves,
        "ranked_impact": ranked,
        "summary": {
            "total_actors_affected": total_affected,
            "worst_case_tier1": worst_t1,
            "worst_case_ticker": worst_ticker,
            "total_revenue_at_risk_usd": round(total_rev_risk, 2),
            "max_depth": max_depth,
            "pass_through_factor": pass_through,
        },
        "narrative": _narrative(
            shock, ranked, total_affected, total_rev_risk, label_map
        ),
        "provenance": {
            "source": "supply_chain_edges+capital_flows+supply_shock_attributions",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "algorithm_version": "2.0",
            "labels_resolved": len(label_map),
            "revenues_resolved": len(revenues),
            "pass_through_empirical": pass_through_stats.get("empirical", 0),
            "pass_through_default": pass_through_stats.get("default", 0),
            "mitigations_resolved": len(mitigations),
        },
    }


__all__ = [
    "simulate_contagion",
    "ShockSpec",
    "ActorImpact",
    "DEFAULT_PASS_THROUGH",
    "HOP_ATTENUATION",
    "DEFAULT_PCT_COGS",
    "MAX_NODES",
    "VALID_SHOCK_TYPES",
    "EMPIRICAL_PASS_THROUGH_MIN",
    "EMPIRICAL_PASS_THROUGH_MAX",
    "_empirical_pass_through",
    "_mitigation_for",
]
