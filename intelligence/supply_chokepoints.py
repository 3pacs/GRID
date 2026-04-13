"""
GRID Intelligence — Supply Chain Chokepoint Scoring.

Computes a `chokepoint_score` in [0, 1] for every row in `supply_chain_edges`
that does not already carry a hand-curated score, and flips
`supply_chain_nodes.chokepoint_flag = TRUE` for nodes that participate in any
high-score (>= 0.7) edge.

The score is a weighted sum of four factors:

    score = 0.40 * substitution_penalty
          + 0.25 * buyer_concentration
          + 0.20 * geographic_concentration
          + 0.15 * historical_disruption

1. substitution_penalty:   1 / (1 + alt_count) where alt_count is the number of
                           alternative suppliers the downstream has for the
                           same `input_type`. Single-source → 1.0.
2. buyer_concentration:    `pct_downstream_cogs` if present, else normalized
                           annual_usd share across the downstream's upstream
                           edges.
3. geographic_concentration: HHI over alternative-supplier country distribution
                           (for the same input_type). Single-country → 1.0.
4. historical_disruption:  Hardcoded bump list — neon/Ukraine, West Africa
                           cocoa, Taiwan semis, China rare earths. Capped at 1.

Hand-curated scores are preserved — the updater only writes where
`chokepoint_score IS NULL`. Idempotent: re-runs produce the same result.

Public API:
    compute_chokepoint_score(edge, context) -> float
    find_alternatives(conn, downstream_id, input_type) -> list[dict]
    score_all_edges(engine) -> dict[str, int]
    flag_chokepoint_nodes(engine, threshold=0.7) -> dict[str, int]
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable
from uuid import uuid4

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


_PRODUCER_MODULE = "intelligence.supply_chokepoints"


# ── Constants ────────────────────────────────────────────────────────────────

WEIGHT_SUBSTITUTION: float = 0.40
WEIGHT_BUYER_CONC: float = 0.25
WEIGHT_GEO_CONC: float = 0.20
WEIGHT_HIST_DISRUPTION: float = 0.15

HIGH_SCORE_THRESHOLD: float = 0.7

# Historical disruption bumps keyed by substring matches against
# input_type / notes / upstream_id / downstream_id. Cap at 1.0.
HISTORICAL_DISRUPTION_RULES: list[tuple[tuple[str, ...], float, str]] = [
    (("neon",), 0.30, "Ukraine war: Odesa/Mariupol air separation units offline 2022+"),
    (("cocoa", "west_africa"), 0.20, "West Africa cocoa climate/black pod disease 2023-2024"),
    (("cocoa", "ivory_coast"), 0.20, "Cote d'Ivoire cocoa production collapse 2023-2024"),
    (("cocoa", "ghana"), 0.20, "Ghana cocoa smuggling / production collapse 2023-2024"),
    (("taiwan",), 0.30, "Taiwan Strait geopolitical risk: TSMC/semi concentration"),
    (("cowos",), 0.30, "TSMC CoWoS Taiwan single-point advanced packaging"),
    (("hbm",), 0.20, "HBM memory sold out: SK Hynix/Micron/Samsung capacity locked"),
    (("rare_earth", "china"), 0.40, "China rare earth export controls 2024-2025"),
    (("rare_earths",), 0.40, "China ~85% of rare earth processing"),
    (("euv", "asml"), 0.30, "ASML EUV single vendor monopoly"),
    (("plasma",), 0.15, "US plasma collection concentration ~70% global"),
    (("glp1", "bachem", "polypeptide"), 0.20, "GLP-1 peptide CDMO duopoly"),
    (("cobalt", "drc"), 0.20, "DRC cobalt artisanal mining / China refining 70-75%"),
    (("palm_oil", "indonesia"), 0.15, "Indonesia palm oil export restrictions 2022+"),
    (("uranium",), 0.15, "Uranium: Kazakhstan/Russia enrichment concentration"),
    (("co2", "carbon_dioxide"), 0.15, "Food-grade CO2: ammonia byproduct dependency"),
]


# ── Data containers ──────────────────────────────────────────────────────────

@dataclass(frozen=True)
class EdgeContext:
    """Immutable per-edge context needed to compute a chokepoint score.

    All fields are optional so pure unit tests can pass minimal stubs.
    """

    alt_count: int = 0
    pct_downstream_cogs: float | None = None
    annual_usd: float | None = None
    downstream_total_annual_usd: float | None = None
    country_hhi: float = 0.0
    historical_bump: float = 0.0
    notes: str | None = None


@dataclass
class ScoreBreakdown:
    substitution_penalty: float = 0.0
    buyer_concentration: float = 0.0
    geographic_concentration: float = 0.0
    historical_disruption: float = 0.0
    score: float = 0.0

    def as_dict(self) -> dict[str, float]:
        return {
            "substitution_penalty": self.substitution_penalty,
            "buyer_concentration": self.buyer_concentration,
            "geographic_concentration": self.geographic_concentration,
            "historical_disruption": self.historical_disruption,
            "score": self.score,
        }


# ── Pure scoring helpers ─────────────────────────────────────────────────────

def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    if value != value:  # NaN check
        return lo
    return max(lo, min(hi, value))


def substitution_penalty(alt_count: int) -> float:
    """1 / (1 + alt_count). Single supplier -> 1.0, two sources -> 0.5, etc."""
    if alt_count < 0:
        alt_count = 0
    return 1.0 / (1.0 + float(alt_count))


def buyer_concentration(
    pct_downstream_cogs: float | None,
    annual_usd: float | None,
    downstream_total_annual_usd: float | None,
) -> float:
    """Prefer explicit pct_downstream_cogs; else fall back to share of annual USD."""
    if pct_downstream_cogs is not None:
        value = float(pct_downstream_cogs)
        # Some seed rows store % as a 0-1 decimal already, but tolerate 0-100.
        if value > 1.0:
            value = value / 100.0
        return _clamp(value)
    if annual_usd and downstream_total_annual_usd and downstream_total_annual_usd > 0:
        return _clamp(float(annual_usd) / float(downstream_total_annual_usd))
    return 0.0


def geographic_concentration(country_counts: dict[str, int]) -> float:
    """HHI over country shares. Single country -> 1.0.

    country_counts maps ISO/slug country ids to the number of alternative
    suppliers located there (including the focal upstream). When no country
    info is available, returns 0.0.
    """
    total = sum(country_counts.values())
    if total <= 0:
        return 0.0
    hhi = 0.0
    for n in country_counts.values():
        share = float(n) / float(total)
        hhi += share * share
    return _clamp(hhi)


def historical_disruption(
    *fields: str | None,
    rules: list[tuple[tuple[str, ...], float, str]] | None = None,
) -> tuple[float, list[str]]:
    """Scan free-text fields for known disruption keywords. Return (bump, reasons).

    Multiple triggers are additive but the total is capped at 1.0.
    """
    rules = rules if rules is not None else HISTORICAL_DISRUPTION_RULES
    haystack = " ".join(f.lower() for f in fields if f).strip()
    if not haystack:
        return 0.0, []
    total = 0.0
    reasons: list[str] = []
    for keywords, bump, reason in rules:
        if all(k in haystack for k in keywords):
            total += bump
            reasons.append(reason)
    return _clamp(total), reasons


def compute_chokepoint_score(
    edge: dict[str, Any], context: EdgeContext
) -> ScoreBreakdown:
    """Weighted combination. All component scores are in [0, 1]."""
    sub = substitution_penalty(context.alt_count)
    buy = buyer_concentration(
        context.pct_downstream_cogs,
        context.annual_usd,
        context.downstream_total_annual_usd,
    )
    geo = _clamp(context.country_hhi)
    hist = _clamp(context.historical_bump)
    raw = (
        WEIGHT_SUBSTITUTION * sub
        + WEIGHT_BUYER_CONC * buy
        + WEIGHT_GEO_CONC * geo
        + WEIGHT_HIST_DISRUPTION * hist
    )
    final = round(_clamp(raw), 3)
    return ScoreBreakdown(
        substitution_penalty=round(sub, 3),
        buyer_concentration=round(buy, 3),
        geographic_concentration=round(geo, 3),
        historical_disruption=round(hist, 3),
        score=final,
    )


# ── DB queries ───────────────────────────────────────────────────────────────

_FIND_ALTERNATIVES_SQL = text(
    """
    SELECT e.upstream_id, e.annual_usd, n.country
    FROM supply_chain_edges e
    LEFT JOIN supply_chain_nodes n ON n.id = e.upstream_id
    WHERE e.downstream_id = :downstream_id
      AND (
            (:input_type IS NULL AND e.input_type IS NULL)
         OR e.input_type = :input_type
      )
    """
)

_FIND_DOWNSTREAM_TOTAL_SQL = text(
    """
    SELECT COALESCE(SUM(annual_usd), 0) AS total, COUNT(*) AS n
    FROM supply_chain_edges
    WHERE downstream_id = :downstream_id
    """
)

_LOAD_NODE_META_SQL = text(
    "SELECT id, country, notes FROM supply_chain_nodes WHERE id = ANY(:ids)"
)

_SELECT_UNSCORED_EDGES_SQL = text(
    """
    SELECT id, upstream_id, downstream_id, relationship, input_type,
           annual_usd, pct_upstream_revenue, pct_downstream_cogs,
           chokepoint_score, confidence
    FROM supply_chain_edges
    WHERE chokepoint_score IS NULL
    ORDER BY id
    """
)

_SELECT_ALL_EDGES_SQL = text(
    """
    SELECT id, upstream_id, downstream_id, input_type, annual_usd,
           pct_downstream_cogs
    FROM supply_chain_edges
    """
)

_UPDATE_SCORE_SQL = text(
    """
    UPDATE supply_chain_edges
    SET chokepoint_score = :score
    WHERE id = :id AND chokepoint_score IS NULL
    """
)


def find_alternatives(
    conn: Any, downstream_id: str, input_type: str | None
) -> list[dict[str, Any]]:
    """Return the full list of edges feeding `downstream_id` with matching
    `input_type` (including the focal edge). Each row has
    {upstream_id, annual_usd, country}.
    """
    rows = conn.execute(
        _FIND_ALTERNATIVES_SQL,
        {"downstream_id": downstream_id, "input_type": input_type},
    ).fetchall()
    return [
        {
            "upstream_id": r[0],
            "annual_usd": float(r[1]) if r[1] is not None else None,
            "country": r[2],
        }
        for r in rows
    ]


def _downstream_annual_total(conn: Any, downstream_id: str) -> float:
    row = conn.execute(
        _FIND_DOWNSTREAM_TOTAL_SQL, {"downstream_id": downstream_id}
    ).fetchone()
    return float(row[0] or 0) if row else 0.0


def _load_node_meta(conn: Any, ids: Iterable[str]) -> dict[str, dict[str, Any]]:
    ids = [i for i in set(ids) if i]
    if not ids:
        return {}
    rows = conn.execute(_LOAD_NODE_META_SQL, {"ids": ids}).fetchall()
    return {r[0]: {"country": r[1], "notes": r[2]} for r in rows}


def _build_edge_context(
    conn: Any, edge: dict[str, Any], node_meta: dict[str, dict[str, Any]]
) -> EdgeContext:
    downstream_id = edge["downstream_id"]
    upstream_id = edge["upstream_id"]
    input_type = edge.get("input_type")

    alternatives = find_alternatives(conn, downstream_id, input_type)
    # alt_count = number of OTHER suppliers for the same input_type
    upstream_ids_for_input = {a["upstream_id"] for a in alternatives}
    alt_count = max(0, len(upstream_ids_for_input) - 1)

    # Country HHI across the distinct alternative suppliers' countries.
    country_counts: dict[str, int] = {}
    for a in alternatives:
        country = a["country"] or node_meta.get(a["upstream_id"], {}).get("country")
        if country:
            country_counts[country] = country_counts.get(country, 0) + 1
    geo_hhi = geographic_concentration(country_counts)

    # Downstream annual total (sum across all inputs, any input_type).
    total = _downstream_annual_total(conn, downstream_id)

    # Historical disruption keywords
    upstream_notes = node_meta.get(upstream_id, {}).get("notes")
    upstream_country = node_meta.get(upstream_id, {}).get("country")
    bump, _reasons = historical_disruption(
        input_type,
        upstream_id,
        downstream_id,
        upstream_notes,
        upstream_country,
    )

    return EdgeContext(
        alt_count=alt_count,
        pct_downstream_cogs=(
            float(edge["pct_downstream_cogs"])
            if edge.get("pct_downstream_cogs") is not None
            else None
        ),
        annual_usd=(
            float(edge["annual_usd"]) if edge.get("annual_usd") is not None else None
        ),
        downstream_total_annual_usd=total if total > 0 else None,
        country_hhi=geo_hhi,
        historical_bump=bump,
        notes=upstream_notes,
    )


# ── Public orchestration ─────────────────────────────────────────────────────

def score_all_edges(engine: Engine) -> dict[str, int]:
    """Score every edge with a NULL chokepoint_score. Preserves existing values.

    Returns {scanned, scored, skipped, errors}.
    """
    stats = {"scanned": 0, "scored": 0, "skipped": 0, "errors": 0}
    with engine.begin() as conn:
        rows = conn.execute(_SELECT_UNSCORED_EDGES_SQL).fetchall()
        if not rows:
            log.info("supply_chokepoints: no unscored edges found")
            return stats

        # Preload all node metadata in one shot (upstream + downstream).
        all_ids: set[str] = set()
        for r in rows:
            all_ids.add(r[1])
            all_ids.add(r[2])
        node_meta = _load_node_meta(conn, all_ids)

        for r in rows:
            stats["scanned"] += 1
            edge = {
                "id": r[0],
                "upstream_id": r[1],
                "downstream_id": r[2],
                "relationship": r[3],
                "input_type": r[4],
                "annual_usd": r[5],
                "pct_upstream_revenue": r[6],
                "pct_downstream_cogs": r[7],
                "chokepoint_score": r[8],
                "confidence": r[9],
            }
            try:
                ctx = _build_edge_context(conn, edge, node_meta)
                breakdown = compute_chokepoint_score(edge, ctx)
                conn.execute(
                    _UPDATE_SCORE_SQL, {"id": edge["id"], "score": breakdown.score}
                )
                stats["scored"] += 1
            except Exception as exc:
                log.warning(
                    "supply_chokepoints: scoring failed for edge {i}: {e}",
                    i=edge["id"],
                    e=str(exc),
                )
                stats["errors"] += 1
    log.info("supply_chokepoints: {s}", s=stats)
    return stats


def flag_chokepoint_nodes(
    engine: Engine, threshold: float = HIGH_SCORE_THRESHOLD
) -> dict[str, int]:
    """Set chokepoint_flag = TRUE for nodes touching any edge >= threshold.

    Does NOT clear the flag on other nodes — hand-curated flags are preserved.
    Returns {flagged, already_flagged, total_high}.
    """
    sql = text(
        """
        WITH high_edges AS (
            SELECT DISTINCT upstream_id AS node_id
            FROM supply_chain_edges
            WHERE chokepoint_score IS NOT NULL AND chokepoint_score >= :t
            UNION
            SELECT DISTINCT downstream_id AS node_id
            FROM supply_chain_edges
            WHERE chokepoint_score IS NOT NULL AND chokepoint_score >= :t
        )
        UPDATE supply_chain_nodes n
        SET chokepoint_flag = TRUE
        FROM high_edges h
        WHERE n.id = h.node_id
          AND (n.chokepoint_flag IS DISTINCT FROM TRUE)
        RETURNING n.id
        """
    )
    count_sql = text(
        """
        SELECT COUNT(*) FROM supply_chain_nodes WHERE chokepoint_flag = TRUE
        """
    )
    with engine.begin() as conn:
        flipped = conn.execute(sql, {"t": threshold}).fetchall()
        total = conn.execute(count_sql).scalar() or 0
        # Look up the max chokepoint score for each newly-flagged node so
        # the emitted SignalFired carries a real severity. Done in the
        # same transaction so the read is consistent with the UPDATE
        # we just ran.
        flipped_ids = [r[0] for r in flipped]
        node_scores: dict[str, float] = {}
        if flipped_ids:
            rows = conn.execute(
                text(
                    """
                    SELECT node_id, MAX(chokepoint_score) AS max_score
                    FROM (
                        SELECT upstream_id AS node_id, chokepoint_score
                        FROM supply_chain_edges
                        WHERE upstream_id = ANY(:ids)
                          AND chokepoint_score IS NOT NULL
                        UNION ALL
                        SELECT downstream_id AS node_id, chokepoint_score
                        FROM supply_chain_edges
                        WHERE downstream_id = ANY(:ids)
                          AND chokepoint_score IS NOT NULL
                    ) e
                    GROUP BY node_id
                    """
                ),
                {"ids": flipped_ids},
            ).fetchall()
            for nid, score in rows:
                if score is None:
                    continue
                node_scores[str(nid)] = float(score)
    stats = {
        "newly_flagged": len(flipped),
        "total_flagged": int(total),
        "threshold": threshold,
    }

    # SYNTH-C / SYNTH-34 — non-fatal SignalFired fanout. One signal per
    # node that just crossed the flag threshold.
    _emit_chokepoint_signals(flipped_ids, node_scores)

    log.info("supply_chokepoints.flag_nodes: {s}", s=stats)
    return stats


def _emit_chokepoint_signals(
    node_ids: list[Any],
    node_scores: dict[str, float],
) -> None:
    """Emit one ``SignalFired`` per newly flagged chokepoint node.

    Strength is the node's max outgoing chokepoint_score in [0, 1].
    Non-fatal: any import / bus / audit error is swallowed.
    """
    if not node_ids:
        return
    try:
        from contracts.correlation import (
            get_current_correlation_id,
            new_correlation_id,
        )
        from contracts.emit import emit as _emit
        from contracts.schemas import SignalFired
    except Exception as exc:  # pragma: no cover — defensive import guard
        log.debug("supply_chokepoints: contracts import failed: {e}", e=str(exc))
        return

    try:
        corr_id = get_current_correlation_id() or new_correlation_id()
    except Exception:
        return

    for nid in node_ids:
        node_str = str(nid)
        score = node_scores.get(node_str)
        if score is None:
            continue
        strength = max(-1.0, min(1.0, float(score)))
        if strength == 0.0:
            continue
        try:
            _emit(
                SignalFired(
                    producer_module=_PRODUCER_MODULE,
                    correlation_id=corr_id,
                    signal_id=uuid4(),
                    source=f"supply_chokepoints:{node_str}",
                    signal_type="chokepoint_crossing",
                    strength=strength,
                    ticker=node_str,
                    actor_hint=None,
                    raw_row_ids=[],
                )
            )
        except Exception as exc:  # non-fatal per SYNTH-C contract
            log.debug(
                "supply_chokepoints emit failed for {n}: {e}",
                n=node_str, e=str(exc),
            )


__all__ = [
    "EdgeContext",
    "ScoreBreakdown",
    "substitution_penalty",
    "buyer_concentration",
    "geographic_concentration",
    "historical_disruption",
    "compute_chokepoint_score",
    "find_alternatives",
    "score_all_edges",
    "flag_chokepoint_nodes",
    "HIGH_SCORE_THRESHOLD",
    "HISTORICAL_DISRUPTION_RULES",
]
