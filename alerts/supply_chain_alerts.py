"""
GRID Intelligence — Supply Chain Pulse watchdog.

Monitors ``supply_chain_edges`` and ``capital_flows`` for meaningful changes
and emits a digest email via the existing alerts pipeline.

Trigger conditions
------------------
1. **New supplier mention** — an ``upstream_id`` appears for the first time
   (via ``supply_chain_edges.created_at`` within ``since_hours``).
2. **Concentration shift** — an edge's ``pct_downstream_cogs`` has changed by
   >= ``threshold_pp`` (absolute, in the 0..1 space, i.e. 0.05 == 5pp) since
   the previous snapshot.
3. **Chokepoint degradation** — an edge's ``chokepoint_score`` has increased
   by >= ``delta_threshold`` since the previous snapshot.
4. **New high-chokepoint crossing** — an edge crossed from ``<0.7`` to
   ``>=0.7`` compared to the previous snapshot (or is new and already
   ``>=0.7``).
5. **Geographic concentration spike** — a newly created edge whose
   upstream node sits in a country that already hosts ``>=3`` upstream
   nodes for the same ``input_type``.
6. **Large new acquisition** — a ``capital_flows`` row with
   ``flow_type='acquisitions'``, ``period_type='announcement'`` and
   ``amount_usd >= 5e9`` created in the last ``since_hours``.
7. **New chain contagion risk** — if ``intelligence.chain_contagion`` is
   importable and a new edge has ``chokepoint_score >= 0.8``, run a
   contagion simulation and attach the top-3 downstream victims.

State
-----
* ``alert_state`` — dedup: we only alert each finding once. Keyed by
  (alert_type, entity_id).
* ``supply_chain_edge_snapshots`` — previous chokepoint/pct snapshot per
  edge, refreshed at the end of every run.

Public API
----------
* ``detect_new_suppliers(engine, since_hours=24)``
* ``detect_concentration_shifts(engine, threshold_pp=0.05)``
* ``detect_chokepoint_degradation(engine, delta_threshold=0.15)``
* ``detect_new_high_chokepoints(engine, threshold=0.7)``
* ``detect_geographic_spikes(engine, since_hours=24, min_nodes=3)``
* ``detect_large_acquisitions(engine, min_usd=5e9, since_hours=24)``
* ``detect_contagion_risk(engine, score_threshold=0.8)``
* ``refresh_snapshots(engine)``
* ``run_all(engine, since_hours=24, send_email=False)``
* ``render_digest_html(findings)``
* ``send_digest(findings)``
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ─────────────────────────────────────────────────────────────────────────────
# Thresholds
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_SINCE_HOURS: int = 24
DEFAULT_CONCENTRATION_PP: float = 0.05      # 5pp absolute
DEFAULT_DEGRADATION_DELTA: float = 0.15     # +0.15 chokepoint_score
DEFAULT_HIGH_CHOKE_THRESHOLD: float = 0.7
DEFAULT_CONTAGION_TRIGGER: float = 0.8
DEFAULT_MIN_ACQ_USD: float = 5e9
DEFAULT_GEO_MIN_NODES: int = 3

CANVAS_LINK_BASE: str = "https://grid.stepdad.finance/#/canvas"


# ─────────────────────────────────────────────────────────────────────────────
# Finding container
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Finding:
    """Immutable finding produced by a detector.

    ``key`` is used for dedup against ``alert_state``. Two findings of the
    same ``alert_type`` with the same ``key`` will only be alerted once.
    """

    alert_type: str
    key: str
    entity: str            # human-readable headline entity (e.g. ticker)
    headline: str          # short summary
    delta: str             # change summary (e.g. "+0.18", "new")
    context: str           # long-form body
    deep_link: str         # URL to drill in
    payload: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "alert_type": self.alert_type,
            "key": self.key,
            "entity": self.entity,
            "headline": self.headline,
            "delta": self.delta,
            "context": self.context,
            "deep_link": self.deep_link,
            "payload": self.payload,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _edge_key(upstream: str, downstream: str, relationship: str | None) -> str:
    return f"{upstream}|{downstream}|{relationship or ''}"


def _canvas_link(node_id: str | None) -> str:
    if not node_id:
        return CANVAS_LINK_BASE
    return f"{CANVAS_LINK_BASE}/{node_id}/supply"


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _fmt_usd(amount: float | None) -> str:
    if amount is None:
        return "n/a"
    amt = float(amount)
    if amt >= 1e9:
        return f"${amt / 1e9:.2f}B"
    if amt >= 1e6:
        return f"${amt / 1e6:.1f}M"
    if amt >= 1e3:
        return f"${amt / 1e3:.1f}K"
    return f"${amt:.0f}"


def _already_alerted(conn: Any, alert_type: str, key: str) -> bool:
    row = conn.execute(
        text(
            "SELECT 1 FROM alert_state "
            "WHERE alert_type = :t AND entity_id = :k LIMIT 1"
        ).bindparams(t=alert_type, k=key),
    ).fetchone()
    return bool(row)


def _record_alerted(conn: Any, findings: Iterable[Finding]) -> int:
    n = 0
    for f in findings:
        try:
            conn.execute(
                text(
                    "INSERT INTO alert_state (alert_type, entity_id, payload) "
                    "VALUES (:t, :k, CAST(:p AS JSONB)) "
                    "ON CONFLICT (alert_type, entity_id) DO NOTHING"
                ).bindparams(
                    t=f.alert_type,
                    k=f.key,
                    p=json.dumps(f.as_dict(), default=str),
                ),
            )
            n += 1
        except Exception as exc:
            log.warning(
                "supply_chain_alerts: failed to record {t}:{k}: {e}",
                t=f.alert_type, k=f.key, e=str(exc),
            )
    return n


def _load_snapshot(conn: Any, edge_key: str) -> dict[str, Any] | None:
    row = conn.execute(
        text(
            "SELECT edge_key, chokepoint_score, pct_downstream_cogs, "
            "       annual_usd, snapshotted_at "
            "FROM supply_chain_edge_snapshots WHERE edge_key = :k"
        ).bindparams(k=edge_key),
    ).fetchone()
    if not row:
        return None
    return {
        "edge_key": row[0],
        "chokepoint_score": _float_or_none(row[1]),
        "pct_downstream_cogs": _float_or_none(row[2]),
        "annual_usd": _float_or_none(row[3]),
        "snapshotted_at": row[4],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Detectors
# ─────────────────────────────────────────────────────────────────────────────

_SQL_NEW_SUPPLIERS = text(
    """
    SELECT DISTINCT ON (upstream_id)
           upstream_id, downstream_id, relationship, input_type,
           annual_usd, chokepoint_score, created_at
    FROM supply_chain_edges
    WHERE created_at >= NOW() - (:hours || ' hours')::INTERVAL
    ORDER BY upstream_id, created_at ASC
    """
)


def detect_new_suppliers(
    engine: Engine, since_hours: int = DEFAULT_SINCE_HOURS
) -> list[dict[str, Any]]:
    """Return a Finding dict per upstream_id first-seen within ``since_hours``."""
    out: list[dict[str, Any]] = []
    with engine.connect() as conn:
        rows = conn.execute(
            _SQL_NEW_SUPPLIERS.bindparams(hours=str(int(since_hours))),
        ).fetchall()
        for r in rows:
            upstream, downstream, rel, input_type, annual_usd, score, _created = r
            # Suppress if this upstream existed before the window.
            prior = conn.execute(
                text(
                    "SELECT 1 FROM supply_chain_edges "
                    "WHERE upstream_id = :u "
                    "  AND created_at < NOW() - (:h || ' hours')::INTERVAL "
                    "LIMIT 1"
                ).bindparams(u=upstream, h=str(int(since_hours))),
            ).fetchone()
            if prior:
                continue
            key = f"new_supplier:{upstream}"
            if _already_alerted(conn, "new_supplier", key):
                continue
            headline = f"New supplier mention: {upstream}"
            context = (
                f"{upstream} appeared for the first time as an upstream "
                f"of {downstream}"
                + (f" for {input_type}" if input_type else "")
                + (f" at {_fmt_usd(_float_or_none(annual_usd))}/yr" if annual_usd else "")
                + "."
            )
            out.append(
                Finding(
                    alert_type="new_supplier",
                    key=key,
                    entity=upstream,
                    headline=headline,
                    delta="first seen",
                    context=context,
                    deep_link=_canvas_link(downstream),
                    payload={
                        "upstream_id": upstream,
                        "downstream_id": downstream,
                        "relationship": rel,
                        "input_type": input_type,
                        "annual_usd": _float_or_none(annual_usd),
                        "chokepoint_score": _float_or_none(score),
                    },
                ).as_dict()
            )
    return out


_SQL_ALL_EDGES_WITH_SNAP = text(
    """
    SELECT e.upstream_id, e.downstream_id, e.relationship, e.input_type,
           e.pct_downstream_cogs, e.chokepoint_score, e.annual_usd,
           s.chokepoint_score AS prev_score,
           s.pct_downstream_cogs AS prev_pct,
           s.snapshotted_at
    FROM supply_chain_edges e
    LEFT JOIN supply_chain_edge_snapshots s
      ON s.edge_key = e.upstream_id || '|' || e.downstream_id
                    || '|' || COALESCE(e.relationship, '')
    """
)


def _iter_edges_with_snap(conn: Any) -> list[dict[str, Any]]:
    rows = conn.execute(_SQL_ALL_EDGES_WITH_SNAP).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "upstream_id": r[0],
                "downstream_id": r[1],
                "relationship": r[2],
                "input_type": r[3],
                "pct_downstream_cogs": _float_or_none(r[4]),
                "chokepoint_score": _float_or_none(r[5]),
                "annual_usd": _float_or_none(r[6]),
                "prev_score": _float_or_none(r[7]),
                "prev_pct": _float_or_none(r[8]),
                "snapshotted_at": r[9],
            }
        )
    return out


def detect_concentration_shifts(
    engine: Engine, threshold_pp: float = DEFAULT_CONCENTRATION_PP
) -> list[dict[str, Any]]:
    """Edges whose ``pct_downstream_cogs`` changed by >= ``threshold_pp``."""
    out: list[dict[str, Any]] = []
    with engine.connect() as conn:
        for e in _iter_edges_with_snap(conn):
            cur = e["pct_downstream_cogs"]
            prev = e["prev_pct"]
            if cur is None or prev is None:
                continue
            delta = cur - prev
            if abs(delta) < threshold_pp:
                continue
            key = _edge_key(e["upstream_id"], e["downstream_id"], e["relationship"])
            dedup_key = f"concentration_shift:{key}:{round(cur, 3)}"
            if _already_alerted(conn, "concentration_shift", dedup_key):
                continue
            sign = "+" if delta >= 0 else ""
            out.append(
                Finding(
                    alert_type="concentration_shift",
                    key=dedup_key,
                    entity=f"{e['upstream_id']} → {e['downstream_id']}",
                    headline=(
                        f"Concentration shift: {e['upstream_id']} → "
                        f"{e['downstream_id']}"
                    ),
                    delta=f"{sign}{delta * 100:.1f}pp (now {cur * 100:.1f}%)",
                    context=(
                        f"{e['downstream_id']} COGS exposure to {e['upstream_id']} "
                        f"moved from {prev * 100:.1f}% to {cur * 100:.1f}% "
                        + (f"for {e['input_type']}." if e["input_type"] else ".")
                    ),
                    deep_link=_canvas_link(e["downstream_id"]),
                    payload={
                        "edge_key": key,
                        "pct_downstream_cogs": cur,
                        "prev_pct_downstream_cogs": prev,
                        "delta_pp": delta,
                    },
                ).as_dict()
            )
    return out


def detect_chokepoint_degradation(
    engine: Engine, delta_threshold: float = DEFAULT_DEGRADATION_DELTA
) -> list[dict[str, Any]]:
    """Edges whose ``chokepoint_score`` rose by >= ``delta_threshold``."""
    out: list[dict[str, Any]] = []
    with engine.connect() as conn:
        for e in _iter_edges_with_snap(conn):
            cur = e["chokepoint_score"]
            prev = e["prev_score"]
            if cur is None or prev is None:
                continue
            delta = cur - prev
            if delta < delta_threshold:
                continue
            key = _edge_key(e["upstream_id"], e["downstream_id"], e["relationship"])
            dedup_key = f"chokepoint_degradation:{key}:{round(cur, 3)}"
            if _already_alerted(conn, "chokepoint_degradation", dedup_key):
                continue
            out.append(
                Finding(
                    alert_type="chokepoint_degradation",
                    key=dedup_key,
                    entity=f"{e['upstream_id']} → {e['downstream_id']}",
                    headline=(
                        f"Chokepoint degradation: {e['upstream_id']} → "
                        f"{e['downstream_id']}"
                    ),
                    delta=f"+{delta:.2f} (now {cur:.2f})",
                    context=(
                        f"chokepoint_score on {e['upstream_id']} → "
                        f"{e['downstream_id']} rose from {prev:.2f} to {cur:.2f}"
                        + (f" (input: {e['input_type']})." if e["input_type"] else ".")
                    ),
                    deep_link=_canvas_link(e["downstream_id"]),
                    payload={
                        "edge_key": key,
                        "chokepoint_score": cur,
                        "prev_chokepoint_score": prev,
                        "delta": delta,
                    },
                ).as_dict()
            )
    return out


def detect_new_high_chokepoints(
    engine: Engine, threshold: float = DEFAULT_HIGH_CHOKE_THRESHOLD
) -> list[dict[str, Any]]:
    """Edges that crossed from below ``threshold`` to above ``threshold``."""
    out: list[dict[str, Any]] = []
    with engine.connect() as conn:
        for e in _iter_edges_with_snap(conn):
            cur = e["chokepoint_score"]
            prev = e["prev_score"]
            if cur is None or cur < threshold:
                continue
            # crossing = prev < threshold (or no prev snapshot at all)
            if prev is not None and prev >= threshold:
                continue
            key = _edge_key(e["upstream_id"], e["downstream_id"], e["relationship"])
            dedup_key = f"new_high_chokepoint:{key}"
            if _already_alerted(conn, "new_high_chokepoint", dedup_key):
                continue
            prev_txt = f"{prev:.2f}" if prev is not None else "baseline"
            out.append(
                Finding(
                    alert_type="new_high_chokepoint",
                    key=dedup_key,
                    entity=f"{e['upstream_id']} → {e['downstream_id']}",
                    headline=(
                        f"New high chokepoint: {e['upstream_id']} → "
                        f"{e['downstream_id']}"
                    ),
                    delta=f"{prev_txt} → {cur:.2f}",
                    context=(
                        f"{e['upstream_id']} → {e['downstream_id']} crossed "
                        f"the {threshold:.2f} chokepoint threshold "
                        + (f"on {e['input_type']}." if e["input_type"] else ".")
                    ),
                    deep_link=_canvas_link(e["downstream_id"]),
                    payload={
                        "edge_key": key,
                        "chokepoint_score": cur,
                        "prev_chokepoint_score": prev,
                        "threshold": threshold,
                    },
                ).as_dict()
            )
    return out


_SQL_GEO_SPIKE = text(
    """
    SELECT e.upstream_id, e.downstream_id, e.relationship, e.input_type,
           n.country, e.chokepoint_score, e.created_at
    FROM supply_chain_edges e
    JOIN supply_chain_nodes n ON n.id = e.upstream_id
    WHERE e.created_at >= NOW() - (:hours || ' hours')::INTERVAL
      AND e.input_type IS NOT NULL
      AND n.country IS NOT NULL
    """
)

_SQL_COUNTRY_COUNT = text(
    """
    SELECT COUNT(DISTINCT e.upstream_id)
    FROM supply_chain_edges e
    JOIN supply_chain_nodes n ON n.id = e.upstream_id
    WHERE e.input_type = :input_type
      AND n.country = :country
      AND e.upstream_id != :upstream
      AND e.created_at < NOW() - (:hours || ' hours')::INTERVAL
    """
)


def detect_geographic_spikes(
    engine: Engine,
    since_hours: int = DEFAULT_SINCE_HOURS,
    min_nodes: int = DEFAULT_GEO_MIN_NODES,
) -> list[dict[str, Any]]:
    """New edge added in a country already hosting >= ``min_nodes`` upstreams
    for the same ``input_type``.
    """
    out: list[dict[str, Any]] = []
    with engine.connect() as conn:
        rows = conn.execute(
            _SQL_GEO_SPIKE.bindparams(hours=str(int(since_hours)))
        ).fetchall()
        for r in rows:
            upstream, downstream, rel, input_type, country, score, _created = r
            existing = conn.execute(
                _SQL_COUNTRY_COUNT.bindparams(
                    input_type=input_type,
                    country=country,
                    upstream=upstream,
                    hours=str(int(since_hours)),
                ),
            ).scalar() or 0
            if int(existing) < min_nodes:
                continue
            key = f"geo_spike:{country}:{input_type}:{upstream}"
            if _already_alerted(conn, "geo_spike", key):
                continue
            total = int(existing) + 1
            out.append(
                Finding(
                    alert_type="geo_spike",
                    key=key,
                    entity=f"{country}:{input_type}",
                    headline=(
                        f"Geographic concentration spike: {country} / "
                        f"{input_type}"
                    ),
                    delta=f"{total} upstream nodes in {country}",
                    context=(
                        f"New upstream {upstream} added in {country} for "
                        f"{input_type}. That country now hosts {total} "
                        f"upstream nodes for the same input — single-jurisdiction "
                        f"risk is elevated."
                    ),
                    deep_link=_canvas_link(downstream),
                    payload={
                        "upstream_id": upstream,
                        "downstream_id": downstream,
                        "relationship": rel,
                        "input_type": input_type,
                        "country": country,
                        "country_upstream_count": total,
                        "chokepoint_score": _float_or_none(score),
                    },
                ).as_dict()
            )
    return out


_SQL_LARGE_ACQ = text(
    """
    SELECT actor_id, counterparty_id, amount_usd, fiscal_period,
           source_filing, as_of
    FROM capital_flows
    WHERE flow_type = 'acquisitions'
      AND period_type = 'announcement'
      AND amount_usd >= :min_usd
      AND as_of >= NOW() - (:hours || ' hours')::INTERVAL
    ORDER BY amount_usd DESC
    """
)


def detect_large_acquisitions(
    engine: Engine,
    min_usd: float = DEFAULT_MIN_ACQ_USD,
    since_hours: int = DEFAULT_SINCE_HOURS,
) -> list[dict[str, Any]]:
    """Large acquisition announcements."""
    out: list[dict[str, Any]] = []
    with engine.connect() as conn:
        rows = conn.execute(
            _SQL_LARGE_ACQ.bindparams(
                min_usd=float(min_usd), hours=str(int(since_hours))
            ),
        ).fetchall()
        for r in rows:
            actor, counterparty, amount, period, filing, _as_of = r
            amt = _float_or_none(amount) or 0.0
            key = f"large_acquisition:{actor}:{counterparty or 'unknown'}:{period}:{round(amt)}"
            if _already_alerted(conn, "large_acquisition", key):
                continue
            out.append(
                Finding(
                    alert_type="large_acquisition",
                    key=key,
                    entity=actor,
                    headline=(
                        f"Large acquisition announced: {actor} "
                        f"{_fmt_usd(amt)}"
                    ),
                    delta=_fmt_usd(amt),
                    context=(
                        f"{actor} announced an acquisition of "
                        f"{counterparty or 'undisclosed target'} for "
                        f"{_fmt_usd(amt)}"
                        + (f" (source: {filing})." if filing else ".")
                    ),
                    deep_link=_canvas_link(actor),
                    payload={
                        "actor_id": actor,
                        "counterparty_id": counterparty,
                        "amount_usd": amt,
                        "fiscal_period": period.isoformat() if period else None,
                        "source_filing": filing,
                    },
                ).as_dict()
            )
    return out


def detect_contagion_risk(
    engine: Engine, score_threshold: float = DEFAULT_CONTAGION_TRIGGER
) -> list[dict[str, Any]]:
    """For any new edge with chokepoint_score >= threshold, run a contagion
    simulation and attach the top-3 downstream victims.

    Silently returns [] if ``intelligence.chain_contagion`` is unavailable.
    """
    try:
        from intelligence.chain_contagion import simulate_contagion
    except Exception:
        return []

    out: list[dict[str, Any]] = []
    with engine.connect() as conn:
        for e in _iter_edges_with_snap(conn):
            cur = e["chokepoint_score"]
            prev = e["prev_score"]
            if cur is None or cur < score_threshold:
                continue
            if prev is not None and prev >= score_threshold:
                continue  # already alerted previously
            key = _edge_key(e["upstream_id"], e["downstream_id"], e["relationship"])
            dedup_key = f"contagion_risk:{key}"
            if _already_alerted(conn, "contagion_risk", dedup_key):
                continue

            victims: list[dict[str, Any]] = []
            try:
                sim = simulate_contagion(
                    engine,
                    shock_node_id=e["upstream_id"],
                    shock_type="supply_disruption",
                    shock_magnitude=0.5,
                    max_depth=4,
                )
                victims = (sim.get("ranked_impact") or [])[:3]
            except Exception as exc:
                log.warning(
                    "supply_chain_alerts: contagion sim failed for {k}: {e}",
                    k=key, e=str(exc),
                )

            victim_lines = [
                f"{v.get('id', '?')}: {v.get('margin_impact_pct', 0) * 100:+.1f}% margin"
                for v in victims
            ] or ["no downstream victims resolved"]
            out.append(
                Finding(
                    alert_type="contagion_risk",
                    key=dedup_key,
                    entity=e["upstream_id"],
                    headline=(
                        f"Contagion risk: {e['upstream_id']} → "
                        f"{e['downstream_id']} @ {cur:.2f}"
                    ),
                    delta=f"chokepoint {cur:.2f}",
                    context=(
                        f"New edge at chokepoint_score={cur:.2f}. "
                        f"Top victims: " + "; ".join(victim_lines)
                    ),
                    deep_link=_canvas_link(e["upstream_id"]),
                    payload={
                        "edge_key": key,
                        "chokepoint_score": cur,
                        "victims": victims,
                    },
                ).as_dict()
            )
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Snapshot refresh
# ─────────────────────────────────────────────────────────────────────────────

_SQL_REFRESH_SNAPSHOT = text(
    """
    INSERT INTO supply_chain_edge_snapshots
           (edge_key, chokepoint_score, pct_downstream_cogs, annual_usd,
            snapshotted_at)
    VALUES (:edge_key, :score, :pct, :annual, NOW())
    ON CONFLICT (edge_key) DO UPDATE
      SET chokepoint_score    = EXCLUDED.chokepoint_score,
          pct_downstream_cogs = EXCLUDED.pct_downstream_cogs,
          annual_usd          = EXCLUDED.annual_usd,
          snapshotted_at      = NOW()
    """
)


def refresh_snapshots(engine: Engine) -> int:
    """Persist a fresh snapshot row for every edge. Returns count written."""
    n = 0
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                "SELECT upstream_id, downstream_id, relationship, "
                "       chokepoint_score, pct_downstream_cogs, annual_usd "
                "FROM supply_chain_edges"
            )
        ).fetchall()
        for r in rows:
            upstream, downstream, rel, score, pct, annual = r
            conn.execute(
                _SQL_REFRESH_SNAPSHOT.bindparams(
                    edge_key=_edge_key(upstream, downstream, rel),
                    score=_float_or_none(score),
                    pct=_float_or_none(pct),
                    annual=_float_or_none(annual),
                ),
            )
            n += 1
    log.info("supply_chain_alerts: refreshed {n} edge snapshots", n=n)
    return n


# ─────────────────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────────────────

DETECTOR_ORDER: tuple[str, ...] = (
    "new_suppliers",
    "concentration_shifts",
    "chokepoint_degradation",
    "new_high_chokepoints",
    "geographic_spikes",
    "large_acquisitions",
    "contagion_risk",
)


def run_all(
    engine: Engine,
    since_hours: int = DEFAULT_SINCE_HOURS,
    send_email: bool = False,
) -> dict[str, Any]:
    """Run every detector, aggregate findings, optionally send the digest
    email, then refresh the snapshot table.

    Returns ``{"findings": {group: [...]}, "total": N, "sent": bool,
                "snapshots_written": int}``.
    """
    findings: dict[str, list[dict[str, Any]]] = {}

    try:
        findings["new_suppliers"] = detect_new_suppliers(engine, since_hours)
    except Exception as exc:
        log.warning("new_suppliers detector failed: {e}", e=str(exc))
        findings["new_suppliers"] = []

    try:
        findings["concentration_shifts"] = detect_concentration_shifts(engine)
    except Exception as exc:
        log.warning("concentration_shifts detector failed: {e}", e=str(exc))
        findings["concentration_shifts"] = []

    try:
        findings["chokepoint_degradation"] = detect_chokepoint_degradation(engine)
    except Exception as exc:
        log.warning("chokepoint_degradation detector failed: {e}", e=str(exc))
        findings["chokepoint_degradation"] = []

    try:
        findings["new_high_chokepoints"] = detect_new_high_chokepoints(engine)
    except Exception as exc:
        log.warning("new_high_chokepoints detector failed: {e}", e=str(exc))
        findings["new_high_chokepoints"] = []

    try:
        findings["geographic_spikes"] = detect_geographic_spikes(
            engine, since_hours=since_hours
        )
    except Exception as exc:
        log.warning("geographic_spikes detector failed: {e}", e=str(exc))
        findings["geographic_spikes"] = []

    try:
        findings["large_acquisitions"] = detect_large_acquisitions(
            engine, since_hours=since_hours
        )
    except Exception as exc:
        log.warning("large_acquisitions detector failed: {e}", e=str(exc))
        findings["large_acquisitions"] = []

    try:
        findings["contagion_risk"] = detect_contagion_risk(engine)
    except Exception as exc:
        log.warning("contagion_risk detector failed: {e}", e=str(exc))
        findings["contagion_risk"] = []

    total = sum(len(v) for v in findings.values())
    sent = False
    if send_email and total > 0:
        try:
            sent = send_digest(findings)
        except Exception as exc:
            log.warning("digest send failed: {e}", e=str(exc))
            sent = False

        # Record dedup rows only after we actually sent.
        if sent:
            try:
                with engine.begin() as conn:
                    for group in findings.values():
                        _record_alerted(
                            conn,
                            (
                                Finding(
                                    alert_type=f["alert_type"],
                                    key=f["key"],
                                    entity=f["entity"],
                                    headline=f["headline"],
                                    delta=f["delta"],
                                    context=f["context"],
                                    deep_link=f["deep_link"],
                                    payload=f.get("payload", {}),
                                )
                                for f in group
                            ),
                        )
            except Exception as exc:
                log.warning("alert_state write failed: {e}", e=str(exc))

    snaps = 0
    try:
        snaps = refresh_snapshots(engine)
    except Exception as exc:
        log.warning("snapshot refresh failed: {e}", e=str(exc))

    result = {
        "findings": findings,
        "total": total,
        "sent": sent,
        "snapshots_written": snaps,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    log.info(
        "supply_chain_alerts.run_all: total={t} sent={s} snapshots={n}",
        t=total, s=sent, n=snaps,
    )
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Email formatting
# ─────────────────────────────────────────────────────────────────────────────

_SECTION_TITLES: dict[str, tuple[str, str]] = {
    "new_suppliers":          ("New Supplier Mentions",          ""),
    "concentration_shifts":   ("Concentration Shifts",           "amber"),
    "chokepoint_degradation": ("Chokepoint Degradation",         "amber"),
    "new_high_chokepoints":   ("New High Chokepoints",           "red"),
    "geographic_spikes":      ("Geographic Concentration Spikes", "amber"),
    "large_acquisitions":     ("Large Acquisitions",             "purple"),
    "contagion_risk":         ("Contagion Risk",                 "red"),
}


def _escape_html(s: str) -> str:
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _render_finding_row(f: dict[str, Any]) -> str:
    entity = _escape_html(f.get("entity", ""))
    delta = _escape_html(f.get("delta", ""))
    headline = _escape_html(f.get("headline", ""))
    context = _escape_html(f.get("context", ""))
    link = f.get("deep_link", CANVAS_LINK_BASE)
    return (
        f'<div style="padding:10px 0;border-bottom:1px solid #1A2A3A;">'
        f'<div style="font-size:14px;color:#E8F0F8;font-weight:700;">{headline}</div>'
        f'<div style="font-size:13px;color:#1A6EBF;margin-top:2px;">{entity} '
        f'&middot; <span style="color:#F59E0B;">{delta}</span></div>'
        f'<div style="font-size:13px;color:#C8D8E8;margin-top:6px;line-height:1.5;">'
        f'{context}</div>'
        f'<div style="font-size:12px;margin-top:6px;">'
        f'<a href="{link}" style="color:#1A6EBF;text-decoration:none;">Open in canvas &rarr;</a>'
        f'</div></div>'
    )


def _render_section(group: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    title, accent = _SECTION_TITLES.get(group, (group.replace("_", " ").title(), ""))
    body = "".join(_render_finding_row(f) for f in items)
    return {
        "title": f"{title} ({len(items)})",
        "body": body or '<span style="color:#5A7A96;">No findings.</span>',
        "accent": accent,
    }


def render_digest_html(findings: dict[str, list[dict[str, Any]]]) -> str:
    """Render the aggregated findings into a dark-theme HTML digest string.

    Sections with zero findings are skipped. Uses the same ``_render_html``
    primitive as the rest of the alerts pipeline so the look matches every
    other GRID newsletter.
    """
    from alerts.email import _render_html

    sections: list[dict[str, Any]] = []
    total = 0
    for group in DETECTOR_ORDER:
        items = findings.get(group) or []
        if not items:
            continue
        total += len(items)
        sections.append(_render_section(group, items))

    if not sections:
        sections.append(
            {
                "title": "All Clear",
                "body": (
                    '<span style="color:#5A7A96;">No supply chain or capital '
                    "flow alerts in this window.</span>"
                ),
                "accent": "",
            }
        )

    subject = (
        f"[GRID] Supply Chain Pulse — {total} findings "
        f"({datetime.now(timezone.utc).strftime('%Y-%m-%d')})"
    )
    return _render_html(
        subject,
        sections,
        footer_note="GRID Supply Chain Pulse watchdog",
    )


def send_digest(findings: dict[str, list[dict[str, Any]]]) -> bool:
    """Format and dispatch the digest via the shared alerts email pipeline.

    Returns True if the email was queued for sending, False otherwise.
    """
    from alerts.email import _send

    sections: list[dict[str, Any]] = []
    total = 0
    for group in DETECTOR_ORDER:
        items = findings.get(group) or []
        if not items:
            continue
        total += len(items)
        sections.append(_render_section(group, items))

    if not sections:
        log.info("supply_chain_alerts.send_digest: nothing to send")
        return False

    subject = (
        f"[GRID] Supply Chain Pulse — {total} findings "
        f"({datetime.now(timezone.utc).strftime('%Y-%m-%d')})"
    )
    try:
        _send(subject, sections, footer_note="GRID Supply Chain Pulse watchdog")
        return True
    except Exception as exc:
        log.warning("supply_chain_alerts.send_digest failed: {e}", e=str(exc))
        return False


__all__ = [
    "Finding",
    "DETECTOR_ORDER",
    "detect_new_suppliers",
    "detect_concentration_shifts",
    "detect_chokepoint_degradation",
    "detect_new_high_chokepoints",
    "detect_geographic_spikes",
    "detect_large_acquisitions",
    "detect_contagion_risk",
    "refresh_snapshots",
    "run_all",
    "render_digest_html",
    "send_digest",
]
