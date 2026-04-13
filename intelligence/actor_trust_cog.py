"""INTEL-2 — Actor trust-or-cog classifier.

Every actor in `lever_pullers` is scored on a single axis in [-1, +1]:

    +1.0 → pure TRUST    (signals reliably precede market moves)
     0.0 → noise / unknown / mixed
    -1.0 → pure COG      (actor's actions follow other forces, not lead them)

The score is a weighted blend of three factual signals already in the DB:

  1. Lead-precision   — correct_signals / total_signals × tanh(lead_days / 7)
                        from `lever_pullers`. Positive lead days = trust;
                        zero or negative lead days = cog.
  2. Centrality       — pagerank from `actor_analytics`. Highly central actors
                        are wired up — they have more chances to lead.
  3. Say-do alignment — credibility_score from `actor_credibility` (0.5 default).
                        Say-do divergence drags toward cog.

The combined formula is intentionally simple so the contribution of each
input stays auditable. We do NOT use a hidden ML model — the goal is a
defensible per-actor explanation surfaced via `/api/v1/actors/<id>/trust-cog`.

Run via ``intelligence.scheduler._actor_trust_cog_recompute`` weekly. Idempotent.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Tuning constants ───────────────────────────────────────────────────────

# Weights sum to 1.0. Lead-precision dominates because it is the only
# direct outcome-grounded signal; centrality and credibility are priors.
_W_PRECISION = 0.60
_W_CENTRALITY = 0.25
_W_CREDIBILITY = 0.15

# Classification thresholds on the [-1, +1] score.
_THRESHOLD_TRUST = 0.30
_THRESHOLD_COG = -0.30

# Lead-time scale: 7 days saturates the precision multiplier.
_LEAD_SCALE_DAYS = 7.0

# Minimum signal volume before a precision call is meaningful. Below this
# the actor stays "unknown" no matter how good their hit rate looks.
_MIN_SIGNALS_FOR_PRECISION = 3


@dataclass
class TrustCogScore:
    """Per-actor breakdown — every input that fed the score is exposed."""

    lever_id: int
    name: str
    category: str
    precision_component: float
    centrality_component: float
    credibility_component: float
    score: float
    classification: str          # 'trust' | 'cog' | 'mixed' | 'unknown'
    inputs: dict[str, Any] = field(default_factory=dict)


# ── Score computation ──────────────────────────────────────────────────────


def _precision_component(
    correct: int | None, total: int | None, lead_days: float | None
) -> tuple[float, dict[str, Any]]:
    """Return (component, breakdown) for the lead-precision input.

    Range is [-1, +1]. Negative lead time pulls the actor toward cog.
    """
    correct = int(correct or 0)
    total = int(total or 0)
    lead = float(lead_days or 0.0)
    breakdown = {"correct": correct, "total": total, "lead_days": lead}

    if total < _MIN_SIGNALS_FOR_PRECISION:
        breakdown["note"] = "below min signal volume"
        return 0.0, breakdown

    precision = correct / total if total > 0 else 0.0
    # Re-center around 0.5: precision==0.5 → 0.0 contribution.
    centered = (precision - 0.5) * 2.0
    lead_factor = math.tanh(lead / _LEAD_SCALE_DAYS) if lead else 0.0
    raw = centered * (0.5 + 0.5 * abs(lead_factor))
    if lead < 0:
        raw = -abs(raw)
    breakdown["precision"] = round(precision, 4)
    breakdown["lead_factor"] = round(lead_factor, 4)
    return max(-1.0, min(1.0, raw)), breakdown


def _centrality_component(
    pagerank: float | None,
) -> tuple[float, dict[str, Any]]:
    """Return (component, breakdown). High pagerank tilts toward trust."""
    pr = float(pagerank or 0.0)
    breakdown = {"pagerank": round(pr, 6)}
    if pr <= 0:
        return 0.0, breakdown
    # PageRank is bounded by graph normalisation; map [0, 0.01] → [0, 1]
    # and clamp. Most actors fall well below 0.01 in a 3K-node graph.
    scaled = min(1.0, pr / 0.01)
    return scaled, breakdown


def _credibility_component(
    credibility: float | None,
) -> tuple[float, dict[str, Any]]:
    """Return (component, breakdown). credibility is in [0, 1] (0.5 default)."""
    cred = 0.5 if credibility is None else float(credibility)
    breakdown = {"credibility_score": round(cred, 4)}
    # Re-center so 0.5 → 0.0 contribution.
    return (cred - 0.5) * 2.0, breakdown


def _classify(score: float) -> str:
    if score >= _THRESHOLD_TRUST:
        return "trust"
    if score <= _THRESHOLD_COG:
        return "cog"
    return "mixed"


def score_one_actor(
    *,
    lever_id: int,
    name: str,
    category: str,
    correct_signals: int | None,
    total_signals: int | None,
    avg_lead_time_days: float | None,
    pagerank: float | None,
    credibility_score: float | None,
) -> TrustCogScore:
    """Compute the trust-or-cog score for a single actor.

    Pure function — no DB I/O — so it's trivially testable.
    """
    prec, prec_b = _precision_component(
        correct_signals, total_signals, avg_lead_time_days,
    )
    cen, cen_b = _centrality_component(pagerank)
    cred, cred_b = _credibility_component(credibility_score)

    score = (
        _W_PRECISION * prec
        + _W_CENTRALITY * cen
        + _W_CREDIBILITY * cred
    )
    score = max(-1.0, min(1.0, score))

    return TrustCogScore(
        lever_id=lever_id,
        name=name,
        category=category,
        precision_component=round(prec, 4),
        centrality_component=round(cen, 4),
        credibility_component=round(cred, 4),
        score=round(score, 4),
        classification=_classify(score),
        inputs={
            "precision": prec_b,
            "centrality": cen_b,
            "credibility": cred_b,
            "weights": {
                "precision": _W_PRECISION,
                "centrality": _W_CENTRALITY,
                "credibility": _W_CREDIBILITY,
            },
        },
    )


# ── Persistence ────────────────────────────────────────────────────────────


def score_all_actors(engine: Engine) -> dict[str, int]:
    """Recompute trust-or-cog for every row in lever_pullers.

    Joins lever_pullers ⋈ actor_analytics ⋈ actor_credibility on best-effort
    name slug match. Updates trust_or_cog_score / classification / classification_at.

    Returns a count summary {trust, cog, mixed, unknown, total}.
    """
    counts = {"trust": 0, "cog": 0, "mixed": 0, "unknown": 0, "total": 0}
    now = datetime.now(timezone.utc)

    with engine.begin() as conn:
        # Best-effort join: lever_pullers.name → actor_analytics.actor_id (slug)
        # → actor_credibility.actor_name. We use COALESCE + lower(replace) to
        # bridge the slug formats. None of these joins are unique constraints,
        # so the LEFT JOINs gracefully degrade to NULL.
        rows = conn.execute(text(
            """
            SELECT
                lp.id                                 AS lever_id,
                lp.name                               AS name,
                lp.category                           AS category,
                lp.correct_signals                    AS correct_signals,
                lp.total_signals                      AS total_signals,
                lp.avg_lead_time_days                 AS avg_lead_time_days,
                aa.pagerank                           AS pagerank,
                ac.credibility_score                  AS credibility_score
            FROM lever_pullers lp
            LEFT JOIN actor_analytics aa
                ON aa.actor_id = lower(replace(lp.name, ' ', '_'))
            LEFT JOIN actor_credibility ac
                ON ac.actor_name = lp.name
            """
        )).fetchall()

        for row in rows:
            score = score_one_actor(
                lever_id=row.lever_id,
                name=row.name,
                category=row.category,
                correct_signals=row.correct_signals,
                total_signals=row.total_signals,
                avg_lead_time_days=row.avg_lead_time_days,
                pagerank=row.pagerank,
                credibility_score=row.credibility_score,
            )
            conn.execute(
                text(
                    """
                    UPDATE lever_pullers
                    SET trust_or_cog_score = :score,
                        classification     = :cls,
                        classification_at  = :ts
                    WHERE id = :id
                    """
                ).bindparams(
                    score=score.score,
                    cls=score.classification,
                    ts=now,
                    id=score.lever_id,
                ),
            )
            counts[score.classification] = counts.get(score.classification, 0) + 1
            counts["total"] += 1

    log.info(
        "actor_trust_cog: scored {n} actors — trust={t} cog={c} mixed={m} unknown={u}",
        n=counts["total"], t=counts["trust"], c=counts["cog"],
        m=counts["mixed"], u=counts["unknown"],
    )
    return counts


def get_actor_trust_cog(engine: Engine, name_or_id: str) -> dict[str, Any] | None:
    """Look up the latest trust-or-cog row for a single actor.

    Tries exact name match first, then case-insensitive substring.
    Returns the full breakdown dict, or None if not in lever_pullers.
    """
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT
                    lp.id, lp.name, lp.category, lp.influence_rank,
                    lp.trust_score, lp.avg_lead_time_days,
                    lp.total_signals, lp.correct_signals,
                    lp.trust_or_cog_score, lp.classification,
                    lp.classification_at,
                    aa.pagerank,
                    ac.credibility_score
                FROM lever_pullers lp
                LEFT JOIN actor_analytics aa
                    ON aa.actor_id = lower(replace(lp.name, ' ', '_'))
                LEFT JOIN actor_credibility ac
                    ON ac.actor_name = lp.name
                WHERE lp.name = :name
                   OR lower(lp.name) LIKE lower(:like)
                ORDER BY lp.influence_rank DESC NULLS LAST
                LIMIT 1
                """
            ).bindparams(name=name_or_id, like=f"%{name_or_id}%"),
        ).fetchone()

    if row is None:
        return None

    score = score_one_actor(
        lever_id=row.id,
        name=row.name,
        category=row.category,
        correct_signals=row.correct_signals,
        total_signals=row.total_signals,
        avg_lead_time_days=row.avg_lead_time_days,
        pagerank=row.pagerank,
        credibility_score=row.credibility_score,
    )
    return {
        "name": row.name,
        "category": row.category,
        "score": score.score,
        "classification": score.classification,
        "components": {
            "precision": score.precision_component,
            "centrality": score.centrality_component,
            "credibility": score.credibility_component,
        },
        "inputs": score.inputs,
        "stored_classification": row.classification,
        "stored_score": float(row.trust_or_cog_score) if row.trust_or_cog_score is not None else None,
        "stored_at": row.classification_at.isoformat() if row.classification_at else None,
    }
