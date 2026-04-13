"""Edge-validation handler (SYNTH-39).

Consumes ``EdgeValidated`` contracts emitted by
``intelligence/supply_chain_edge_validator.py`` after every edge's
180-day return correlation pass. When an edge flips to
``relationship_weak=True``, every ``cross_lens`` signal row that cites
this edge via ``supply_shock_attributions.edge_id`` has its trust_score
downgraded so downstream oracle cycles stop rewarding stale couplings.

The handler owns no business logic beyond the SQL join — scoring math,
window selection, and recency decay stay in ``intelligence.trust_scorer``.

Non-fatal: any DB failure is logged at ``warning`` and swallowed so the
contract bus keeps flowing.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger as log
from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from contracts.schemas import EdgeValidated


#: Multiplicative trust downgrade applied to cross_lens rows citing a
#: newly-weak edge. Keeps the deprecation gradual so a single weak
#: validation can't zero a signal that recovers on the next pass.
_WEAK_EDGE_TRUST_FACTOR: float = 0.75

#: Minimum floor we clamp the downgraded trust_score to so repeatedly
#: weak validations don't drive the score below what a fresh neutral
#: signal would start at.
_MIN_TRUST_FLOOR: float = 0.05


def on_edge_validated(evt: "EdgeValidated", *, engine: "Engine") -> None:
    """Downgrade cross_lens signal trust when an edge turns weak.

    Strong (``relationship_weak=False``) validations are a no-op —
    trust_scorer's normal recency decay handles the recovery path.
    """
    if not getattr(evt, "relationship_weak", False):
        return

    edge_id = int(getattr(evt, "edge_id", 0) or 0)
    if edge_id <= 0:
        return

    correlation = float(getattr(evt, "validation_correlation", 0.0) or 0.0)

    try:
        with engine.begin() as conn:
            result = conn.execute(
                text(
                    """
                    UPDATE signal_sources AS s
                    SET trust_score = GREATEST(
                            :floor,
                            COALESCE(s.trust_score, 0.5) * :factor
                        )
                    WHERE s.source_type = 'cross_lens_supply_shock'
                      AND s.id IN (
                          SELECT s2.id
                          FROM signal_sources s2
                          JOIN supply_shock_attributions a
                            ON a.signal_source_id = s2.id
                          WHERE a.edge_id = :eid
                      )
                    """
                ),
                {
                    "floor": _MIN_TRUST_FLOOR,
                    "factor": _WEAK_EDGE_TRUST_FACTOR,
                    "eid": edge_id,
                },
            )
            downgraded = getattr(result, "rowcount", 0) or 0
    except Exception as exc:
        log.warning(
            "edges.on_edge_validated(edge_id={eid}): {e}",
            eid=edge_id, e=str(exc),
        )
        return

    log.info(
        "edges.on_edge_validated: edge={eid} corr={c:+.3f} "
        "cross_lens_downgraded={n}",
        eid=edge_id, c=correlation, n=downgraded,
    )
