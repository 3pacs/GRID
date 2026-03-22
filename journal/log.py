"""
GRID immutable decision journal.

Records every decision the system produces with full context for
retrospective analysis.  Enforces immutability — once a decision is
logged, only the outcome fields (outcome_value, outcome_recorded_at,
verdict) and annotation can be updated.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

_VALID_OPERATOR_CONFIDENCE = ("LOW", "MEDIUM", "HIGH")
_VALID_VERDICTS = ("HELPED", "HARMED", "NEUTRAL", "INSUFFICIENT_DATA")


class DecisionJournal:
    """Immutable, append-only decision journal.

    Every decision the GRID system produces is logged here with full
    context.  Outcomes are recorded later but cannot be overwritten.

    Attributes:
        engine: SQLAlchemy engine for database operations.
    """

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the decision journal.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        self.engine = db_engine
        log.info("DecisionJournal initialised")

    def log_decision(
        self,
        model_version_id: int,
        inferred_state: str,
        state_confidence: float,
        transition_probability: float,
        contradiction_flags: dict[str, Any],
        grid_recommendation: str,
        baseline_recommendation: str,
        action_taken: str,
        counterfactual: str,
        operator_confidence: str,
    ) -> int:
        """Log a new decision to the journal.

        Parameters:
            model_version_id: ID of the model that produced the decision.
            inferred_state: The regime/state the model inferred.
            state_confidence: Confidence in the inferred state (0–1).
            transition_probability: Probability of state transition (0–1).
            contradiction_flags: Dict of any contradictory signals.
            grid_recommendation: The GRID system's recommendation.
            baseline_recommendation: What the baseline would recommend.
            action_taken: The actual action taken by the operator.
            counterfactual: What would have happened with the baseline.
            operator_confidence: Operator's confidence level ('LOW'/'MEDIUM'/'HIGH').

        Returns:
            int: The newly created decision_journal.id.

        Raises:
            ValueError: If operator_confidence is not valid.
            ValueError: If state_confidence or transition_probability is outside [0, 1].
        """
        # Validate inputs
        if operator_confidence not in _VALID_OPERATOR_CONFIDENCE:
            raise ValueError(
                f"Invalid operator_confidence '{operator_confidence}'. "
                f"Must be one of {_VALID_OPERATOR_CONFIDENCE}."
            )

        import math
        if math.isnan(state_confidence) or math.isinf(state_confidence) or not 0 <= state_confidence <= 1:
            raise ValueError(
                f"state_confidence must be a finite number between 0 and 1, got {state_confidence}"
            )

        if math.isnan(transition_probability) or math.isinf(transition_probability) or not 0 <= transition_probability <= 1:
            raise ValueError(
                f"transition_probability must be a finite number between 0 and 1, got {transition_probability}"
            )

        log.info(
            "Logging decision — model={m}, state={s}, confidence={c:.2f}",
            m=model_version_id,
            s=inferred_state,
            c=state_confidence,
        )

        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO decision_journal
                    (model_version_id, inferred_state, state_confidence,
                     transition_probability, contradiction_flags,
                     grid_recommendation, baseline_recommendation,
                     action_taken, counterfactual, operator_confidence)
                    VALUES
                    (:mvid, :state, :sc, :tp, :cf, :gr, :br, :at, :cft, :oc)
                    RETURNING id
                """),
                {
                    "mvid": model_version_id,
                    "state": inferred_state,
                    "sc": state_confidence,
                    "tp": transition_probability,
                    "cf": json.dumps(contradiction_flags),
                    "gr": grid_recommendation,
                    "br": baseline_recommendation,
                    "at": action_taken,
                    "cft": counterfactual,
                    "oc": operator_confidence,
                },
            )
            decision_id = result.fetchone()[0]

        log.info("Decision logged — id={id}", id=decision_id)
        return decision_id

    def record_outcome(
        self,
        decision_id: int,
        outcome_value: float,
        verdict: str,
        annotation: str | None = None,
    ) -> bool:
        """Record the outcome of a previously logged decision.

        Parameters:
            decision_id: The decision_journal.id to update.
            outcome_value: Numerical outcome (e.g. P&L, return).
            verdict: One of 'HELPED', 'HARMED', 'NEUTRAL', 'INSUFFICIENT_DATA'.
            annotation: Optional free-text annotation.

        Returns:
            bool: True on success.

        Raises:
            ValueError: If verdict is not valid.
            ValueError: If outcome was already recorded for this decision.
        """
        if verdict not in _VALID_VERDICTS:
            raise ValueError(
                f"Invalid verdict '{verdict}'. Must be one of {_VALID_VERDICTS}."
            )

        # Check if outcome is already recorded
        with self.engine.connect() as conn:
            existing = conn.execute(
                text(
                    "SELECT outcome_value, outcome_recorded_at "
                    "FROM decision_journal WHERE id = :id"
                ),
                {"id": decision_id},
            ).fetchone()

        if existing is None:
            raise ValueError(f"Decision {decision_id} not found")

        if existing[1] is not None:
            raise ValueError(
                f"Outcome already recorded for decision {decision_id}. "
                "Journal is immutable."
            )

        log.info(
            "Recording outcome for decision {id} — value={v}, verdict={vd}",
            id=decision_id,
            v=outcome_value,
            vd=verdict,
        )

        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE decision_journal
                    SET outcome_value = :ov,
                        outcome_recorded_at = :orat,
                        verdict = :v,
                        annotation = COALESCE(:ann, annotation)
                    WHERE id = :id
                """),
                {
                    "ov": outcome_value,
                    "orat": datetime.now(timezone.utc),
                    "v": verdict,
                    "ann": annotation,
                    "id": decision_id,
                },
            )

        log.info("Outcome recorded for decision {id}", id=decision_id)
        return True

    def get_performance_summary(
        self,
        model_version_id: int | None = None,
        days_back: int = 90,
    ) -> dict[str, Any]:
        """Compute a performance summary from journal entries.

        Parameters:
            model_version_id: Optional model filter. If None, summarises all.
            days_back: Number of days to look back (default: 90).

        Returns:
            dict: Summary with total_decisions, outcomes_recorded,
                  helped/harmed/neutral counts, avg_outcome_value,
                  helped_rate, and breakdowns by state and confidence.
        """
        log.info(
            "Computing performance summary — model={m}, days_back={d}",
            m=model_version_id,
            d=days_back,
        )

        base_query = """
            SELECT * FROM decision_journal
            WHERE decision_timestamp >= NOW() - MAKE_INTERVAL(days => :days)
        """
        params: dict[str, Any] = {"days": days_back}

        if model_version_id is not None:
            base_query += " AND model_version_id = :mvid"
            params["mvid"] = model_version_id

        with self.engine.connect() as conn:
            df = pd.read_sql(text(base_query), conn, params=params)

        if df.empty:
            return {
                "total_decisions": 0,
                "outcomes_recorded": 0,
                "helped": 0,
                "harmed": 0,
                "neutral": 0,
                "avg_outcome_value": 0.0,
                "helped_rate": 0.0,
                "by_state": {},
                "by_operator_confidence": {},
            }

        outcomes = df[df["outcome_recorded_at"].notna()]

        helped = int((outcomes["verdict"] == "HELPED").sum())
        harmed = int((outcomes["verdict"] == "HARMED").sum())
        neutral = int((outcomes["verdict"] == "NEUTRAL").sum())

        total_with_verdict = helped + harmed + neutral

        # By state breakdown
        by_state: dict[str, dict[str, int]] = {}
        for state, group in outcomes.groupby("inferred_state"):
            by_state[state] = {
                "count": len(group),
                "helped": int((group["verdict"] == "HELPED").sum()),
                "harmed": int((group["verdict"] == "HARMED").sum()),
            }

        # By operator confidence breakdown
        by_confidence: dict[str, dict[str, int]] = {}
        for conf, group in outcomes.groupby("operator_confidence"):
            by_confidence[conf] = {
                "count": len(group),
                "helped": int((group["verdict"] == "HELPED").sum()),
                "harmed": int((group["verdict"] == "HARMED").sum()),
            }

        return {
            "total_decisions": len(df),
            "outcomes_recorded": len(outcomes),
            "helped": helped,
            "harmed": harmed,
            "neutral": neutral,
            "avg_outcome_value": round(
                float(outcomes["outcome_value"].mean()) if not outcomes.empty else 0.0,
                6,
            ),
            "helped_rate": round(
                helped / max(total_with_verdict, 1), 4
            ),
            "by_state": by_state,
            "by_operator_confidence": by_confidence,
        }

    def get_recent(self, n: int = 20) -> pd.DataFrame:
        """Return the n most recent decisions.

        Parameters:
            n: Number of recent decisions to return (default: 20).

        Returns:
            pd.DataFrame: Recent decisions with all fields.
        """
        with self.engine.connect() as conn:
            df = pd.read_sql(
                text(
                    "SELECT * FROM decision_journal "
                    "ORDER BY decision_timestamp DESC LIMIT :n"
                ),
                conn,
                params={"n": n},
            )

        log.info("Retrieved {n} recent decisions", n=len(df))
        return df


if __name__ == "__main__":
    from db import get_engine

    journal = DecisionJournal(db_engine=get_engine())

    recent = journal.get_recent(10)
    if not recent.empty:
        print(f"Recent decisions ({len(recent)}):")
        print(recent[["id", "inferred_state", "state_confidence", "verdict"]].to_string())
    else:
        print("No decisions in journal yet")
