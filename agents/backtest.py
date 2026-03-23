"""
Agent decision backtesting.

Replays historical dates through TradingAgents and compares agent
decisions against GRID's decision journal outcomes, producing
accuracy and agreement metrics.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from config import settings


class AgentBacktester:
    """Backtest agent decisions against GRID journal history.

    Compares what agents would have decided on historical dates against
    the actual outcomes recorded in the decision journal.

    Attributes:
        engine: SQLAlchemy database engine.
    """

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        log.info("AgentBacktester initialised")

    def run_backtest(
        self,
        ticker: str | None = None,
        days_back: int = 90,
    ) -> dict[str, Any]:
        """Run a backtest comparing agent runs to journal outcomes.

        Looks at existing agent_runs that have a linked decision_journal_id,
        then checks if the journal entry has recorded outcomes. Computes
        agreement rates and accuracy metrics.

        Parameters:
            ticker: Filter to specific ticker (None = all).
            days_back: How many days of history to analyse.

        Returns:
            dict with backtest metrics and per-run details.
        """
        days_back = min(days_back, settings.AGENTS_BACKTEST_MAX_DAYS)
        log.info("Running agent backtest — days_back={d}, ticker={t}", d=days_back, t=ticker)

        query = """
            SELECT
                ar.id AS run_id,
                ar.run_timestamp,
                ar.ticker,
                ar.as_of_date,
                ar.grid_regime_state,
                ar.grid_confidence,
                ar.final_decision AS agent_decision,
                dj.grid_recommendation,
                dj.baseline_recommendation,
                dj.outcome_value,
                dj.verdict,
                dj.inferred_state,
                dj.state_confidence
            FROM agent_runs ar
            JOIN decision_journal dj ON dj.id = ar.decision_journal_id
            WHERE ar.run_timestamp >= NOW() - make_interval(days => :days)
        """

        params: dict[str, Any] = {"days": days_back}
        if ticker:
            query += " AND ar.ticker = :ticker"
            params["ticker"] = ticker

        query += " ORDER BY ar.run_timestamp DESC"

        with self.engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params=params)

        if df.empty:
            return {
                "total_runs": 0,
                "runs_with_outcomes": 0,
                "metrics": {},
                "details": [],
                "days_back": days_back,
                "ticker": ticker,
            }

        total_runs = len(df)
        has_outcome = df[df["outcome_value"].notna()]
        runs_with_outcomes = len(has_outcome)

        # Agreement: how often agent decision matched GRID recommendation
        agreement_mask = df["agent_decision"] == df["grid_recommendation"]
        agreement_rate = float(agreement_mask.mean()) if len(df) > 0 else 0.0

        # Accuracy: among runs with outcomes and verdicts
        has_verdict = has_outcome[has_outcome["verdict"].notna()]
        agent_helped = 0
        agent_harmed = 0
        agent_neutral = 0

        for _, row in has_verdict.iterrows():
            verdict = row["verdict"]
            if verdict == "HELPED":
                agent_helped += 1
            elif verdict == "HARMED":
                agent_harmed += 1
            elif verdict == "NEUTRAL":
                agent_neutral += 1

        total_verdicts = agent_helped + agent_harmed + agent_neutral
        helped_rate = agent_helped / max(total_verdicts, 1)

        # Average outcome value
        avg_outcome = float(has_outcome["outcome_value"].mean()) if runs_with_outcomes > 0 else 0.0

        # Decision distribution
        decision_counts = df["agent_decision"].value_counts().to_dict()

        # By regime breakdown
        by_regime: dict[str, dict[str, Any]] = {}
        for regime, group in df.groupby("grid_regime_state"):
            regime_outcomes = group[group["outcome_value"].notna()]
            by_regime[str(regime)] = {
                "runs": len(group),
                "avg_outcome": round(float(regime_outcomes["outcome_value"].mean()), 6) if len(regime_outcomes) > 0 else None,
                "decisions": group["agent_decision"].value_counts().to_dict(),
            }

        # Build per-run details
        details = []
        for _, row in df.head(50).iterrows():
            details.append({
                "run_id": int(row["run_id"]),
                "date": row["as_of_date"].isoformat() if hasattr(row["as_of_date"], "isoformat") else str(row["as_of_date"]),
                "ticker": row["ticker"],
                "agent_decision": row["agent_decision"],
                "grid_recommendation": row["grid_recommendation"],
                "agreed": row["agent_decision"] == row["grid_recommendation"],
                "regime": row["grid_regime_state"],
                "outcome_value": float(row["outcome_value"]) if pd.notna(row["outcome_value"]) else None,
                "verdict": row["verdict"],
            })

        return {
            "total_runs": total_runs,
            "runs_with_outcomes": runs_with_outcomes,
            "days_back": days_back,
            "ticker": ticker,
            "metrics": {
                "agreement_rate": round(agreement_rate, 4),
                "helped_rate": round(helped_rate, 4),
                "avg_outcome": round(avg_outcome, 6),
                "agent_helped": agent_helped,
                "agent_harmed": agent_harmed,
                "agent_neutral": agent_neutral,
                "decision_distribution": decision_counts,
            },
            "by_regime": by_regime,
            "details": details,
        }

    def get_comparison_summary(self, days_back: int = 90) -> dict[str, Any]:
        """Quick summary comparing agent vs GRID performance.

        Returns:
            dict with side-by-side comparison metrics.
        """
        safe_days = min(days_back, settings.AGENTS_BACKTEST_MAX_DAYS)
        query = """
            SELECT
                ar.final_decision AS agent_decision,
                dj.baseline_recommendation AS grid_recommendation,
                dj.outcome_value,
                dj.verdict
            FROM agent_runs ar
            JOIN decision_journal dj ON dj.id = ar.decision_journal_id
            WHERE ar.run_timestamp >= NOW() - make_interval(days => :days)
              AND dj.outcome_value IS NOT NULL
        """

        with self.engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params={"days": safe_days})

        if df.empty:
            return {"has_data": False, "message": "No agent runs with outcomes yet"}

        return {
            "has_data": True,
            "total_compared": len(df),
            "agreement_rate": round(float((df["agent_decision"] == df["grid_recommendation"]).mean()), 4),
            "avg_outcome": round(float(df["outcome_value"].mean()), 6),
            "helped_count": int((df["verdict"] == "HELPED").sum()),
            "harmed_count": int((df["verdict"] == "HARMED").sum()),
        }
