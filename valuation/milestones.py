"""
GRID — Company Milestone & Goal Tracker.

Tracks public plans, management guidance, analyst expectations, and
market rumors as quantified milestones on a timeline. Each milestone
has a probability weight, target value, and achievement status.

This lets us see whether a company is meeting, exceeding, or falling
behind its stated goals — which directly feeds into the valuation
model's forward-looking component.

Milestone types:
  - EARNINGS_GUIDANCE: Management forward earnings guidance
  - REVENUE_GUIDANCE: Management revenue targets
  - PRODUCT_LAUNCH: Product/service launch deadlines
  - EXPANSION: New market / geography expansion
  - M_AND_A: Mergers, acquisitions, divestitures
  - REGULATORY: FDA approvals, regulatory milestones
  - COST_TARGET: Cost reduction / margin targets
  - BUYBACK: Share repurchase programs
  - DIVIDEND: Dividend changes
  - DEBT_TARGET: Debt reduction targets
  - STRATEGIC: Strategic pivots / restructuring
  - RUMOR: Unconfirmed market rumors
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


VALID_TYPES = {
    "EARNINGS_GUIDANCE", "REVENUE_GUIDANCE", "PRODUCT_LAUNCH", "EXPANSION",
    "M_AND_A", "REGULATORY", "COST_TARGET", "BUYBACK", "DIVIDEND",
    "DEBT_TARGET", "STRATEGIC", "RUMOR",
}

VALID_STATUSES = {
    "PENDING", "ON_TRACK", "AHEAD", "BEHIND", "ACHIEVED", "MISSED",
    "CANCELLED", "SUPERSEDED",
}

VALID_CONFIDENCE_SOURCES = {
    "MANAGEMENT", "ANALYST", "INSIDER", "MARKET", "RUMOR", "CALCULATED",
}


@dataclass
class Milestone:
    """A company milestone / goal / guidance item."""

    ticker: str
    milestone_type: str
    announced_date: date
    description: str
    target_date: date | None = None
    actual_date: date | None = None
    target_value: float | None = None
    target_unit: str | None = None
    actual_value: float | None = None
    achievement_pct: float | None = None
    probability: float = 0.5
    confidence_source: str = "ANALYST"
    value_impact_ps: float | None = None
    value_impact_pct: float | None = None
    status: str = "PENDING"
    source_url: str | None = None
    notes: str | None = None
    id: int | None = None

    def __post_init__(self) -> None:
        if self.milestone_type not in VALID_TYPES:
            raise ValueError(f"Invalid milestone_type: {self.milestone_type}")
        if self.status not in VALID_STATUSES:
            raise ValueError(f"Invalid status: {self.status}")
        if not (0 <= self.probability <= 1):
            raise ValueError(f"Probability must be 0-1, got {self.probability}")


class MilestoneTracker:
    """Manages company milestones, guidance, and rumors.

    Provides CRUD operations, scoring, and timeline generation
    for tracking whether companies are executing on their plans.
    """

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine

    def add(self, milestone: Milestone) -> int:
        """Add a new milestone. Returns the new ID."""
        with self.engine.begin() as conn:
            row = conn.execute(
                text("""
                    INSERT INTO company_milestones (
                        ticker, milestone_type, announced_date, target_date, actual_date,
                        description, target_value, target_unit, actual_value, achievement_pct,
                        probability, confidence_source, value_impact_ps, value_impact_pct,
                        status, source_url, notes
                    ) VALUES (
                        :ticker, :mtype, :ann_date, :tgt_date, :act_date,
                        :desc, :tgt_val, :tgt_unit, :act_val, :ach_pct,
                        :prob, :conf_src, :vi_ps, :vi_pct,
                        :status, :src_url, :notes
                    )
                    RETURNING id
                """),
                {
                    "ticker": milestone.ticker.upper(),
                    "mtype": milestone.milestone_type,
                    "ann_date": milestone.announced_date,
                    "tgt_date": milestone.target_date,
                    "act_date": milestone.actual_date,
                    "desc": milestone.description,
                    "tgt_val": milestone.target_value,
                    "tgt_unit": milestone.target_unit,
                    "act_val": milestone.actual_value,
                    "ach_pct": milestone.achievement_pct,
                    "prob": milestone.probability,
                    "conf_src": milestone.confidence_source,
                    "vi_ps": milestone.value_impact_ps,
                    "vi_pct": milestone.value_impact_pct,
                    "status": milestone.status,
                    "src_url": milestone.source_url,
                    "notes": milestone.notes,
                },
            ).fetchone()
            new_id = row[0]
            log.info("Added milestone #{id} for {t}: {d}", id=new_id,
                     t=milestone.ticker, d=milestone.description[:80])
            return new_id

    def update_status(
        self,
        milestone_id: int,
        status: str,
        actual_value: float | None = None,
        actual_date: date | None = None,
        notes: str | None = None,
    ) -> None:
        """Update a milestone's status and optionally record actuals."""
        if status not in VALID_STATUSES:
            raise ValueError(f"Invalid status: {status}")

        sets = ["status = :status", "updated_at = NOW()"]
        params: dict[str, Any] = {"id": milestone_id, "status": status}

        if actual_value is not None:
            sets.append("actual_value = :act_val")
            params["act_val"] = actual_value
        if actual_date is not None:
            sets.append("actual_date = :act_date")
            params["act_date"] = actual_date
        if notes is not None:
            sets.append("notes = :notes")
            params["notes"] = notes

        # Auto-compute achievement_pct if we have both target and actual
        if actual_value is not None:
            sets.append("""
                achievement_pct = CASE
                    WHEN target_value IS NOT NULL AND target_value != 0
                    THEN (:act_val / target_value) * 100
                    ELSE NULL
                END
            """)

        set_clause = ", ".join(sets)
        with self.engine.begin() as conn:
            conn.execute(
                text(f"UPDATE company_milestones SET {set_clause} WHERE id = :id"),
                params,
            )
        log.info("Updated milestone #{id} -> {s}", id=milestone_id, s=status)

    def get_for_ticker(
        self,
        ticker: str,
        status_filter: list[str] | None = None,
        type_filter: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Get all milestones for a ticker, optionally filtered."""
        conditions = ["ticker = :ticker"]
        params: dict[str, Any] = {"ticker": ticker.upper()}

        if status_filter:
            conditions.append("status = ANY(:statuses)")
            params["statuses"] = status_filter
        if type_filter:
            conditions.append("milestone_type = ANY(:types)")
            params["types"] = type_filter

        where = " AND ".join(conditions)
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(f"""
                    SELECT id, ticker, milestone_type, announced_date, target_date,
                           actual_date, description, target_value, target_unit,
                           actual_value, achievement_pct, probability,
                           confidence_source, value_impact_ps, value_impact_pct,
                           status, source_url, notes, created_at, updated_at
                    FROM company_milestones
                    WHERE {where}
                    ORDER BY COALESCE(target_date, announced_date + INTERVAL '1 year')
                """),
                params,
            ).fetchall()

        return [
            {
                "id": r[0], "ticker": r[1], "milestone_type": r[2],
                "announced_date": str(r[3]), "target_date": str(r[4]) if r[4] else None,
                "actual_date": str(r[5]) if r[5] else None,
                "description": r[6], "target_value": r[7], "target_unit": r[8],
                "actual_value": r[9], "achievement_pct": r[10],
                "probability": r[11], "confidence_source": r[12],
                "value_impact_ps": r[13], "value_impact_pct": r[14],
                "status": r[15], "source_url": r[16], "notes": r[17],
                "created_at": str(r[18]), "updated_at": str(r[19]),
            }
            for r in rows
        ]

    def get_scorecard(self, ticker: str) -> dict[str, Any]:
        """Get the execution scorecard for a ticker."""
        with self.engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT total_milestones, achieved, ahead_of_schedule,
                           missed_or_behind, pending, avg_achievement_pct,
                           execution_score
                    FROM milestone_scorecard
                    WHERE ticker = :ticker
                """),
                {"ticker": ticker.upper()},
            ).fetchone()

        if row is None:
            return {
                "ticker": ticker.upper(),
                "total_milestones": 0,
                "execution_score": None,
                "assessment": "NO_DATA",
            }

        execution_score = row[6]
        if execution_score is None:
            assessment = "NO_COMPLETED_MILESTONES"
        elif execution_score >= 80:
            assessment = "STRONG_EXECUTION"
        elif execution_score >= 60:
            assessment = "ADEQUATE_EXECUTION"
        elif execution_score >= 40:
            assessment = "MIXED_EXECUTION"
        else:
            assessment = "POOR_EXECUTION"

        return {
            "ticker": ticker.upper(),
            "total_milestones": row[0],
            "achieved": row[1],
            "ahead_of_schedule": row[2],
            "missed_or_behind": row[3],
            "pending": row[4],
            "avg_achievement_pct": float(row[5]) if row[5] else None,
            "execution_score": float(execution_score) if execution_score else None,
            "assessment": assessment,
        }

    def get_timeline(self, ticker: str) -> list[dict[str, Any]]:
        """Get milestones as a chronological timeline for the prompt builder."""
        milestones = self.get_for_ticker(ticker)

        timeline = []
        for m in milestones:
            event_date = m["actual_date"] or m["target_date"] or m["announced_date"]
            timeline.append({
                "date": event_date,
                "type": m["milestone_type"],
                "description": m["description"],
                "target_value": m["target_value"],
                "target_unit": m["target_unit"],
                "actual_value": m["actual_value"],
                "achievement_pct": m["achievement_pct"],
                "probability": m["probability"],
                "status": m["status"],
                "value_impact_ps": m["value_impact_ps"],
            })

        return sorted(timeline, key=lambda x: x["date"] or "9999-12-31")

    def probability_weighted_impact(self, ticker: str) -> float:
        """Compute probability-weighted sum of milestone value impacts.

        This is the expected value adjustment to intrinsic value
        from all pending milestones and rumors.
        """
        milestones = self.get_for_ticker(
            ticker,
            status_filter=["PENDING", "ON_TRACK", "AHEAD"],
        )

        total_impact = 0.0
        for m in milestones:
            impact = m.get("value_impact_ps") or 0.0
            prob = m.get("probability") or 0.5
            total_impact += impact * prob

        return total_impact
