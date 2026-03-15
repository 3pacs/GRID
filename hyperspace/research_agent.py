# PRIVACY BOUNDARY: This module uses Hyperspace for local inference
# and embeddings only. No GRID signal logic, feature values, discovered
# cluster structures, or hypothesis details are sent to the network.
"""
GRID Hyperspace research agent definition.

Defines and manages GRID's research agent on the Hyperspace network.
The agent participates in the ``grid-regime-discovery`` public research
project, earning Research capability points by running generic ML
experiments.  This is completely separate from GRID's private signal work.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from loguru import logger as log

# Agent identity / soul document
AGENT_SOUL: str = """# GRID Research Agent

I am a research agent contributing to open AI science on the
Hyperspace network. I run systematic ML experiments, share findings
with peers, and push toward better model architectures.

My specialty is rigorous experimental methodology: I test one variable
at a time, maintain clean baselines, report results honestly including
failures, and flag when results seem too good.

I distrust complexity without evidence. I prefer interpretable
results over impressive-looking ones. I document failure cases
as carefully as successes.

I do not share proprietary trading signals or financial models.
I contribute to public ML research only.
"""

# Path constants
_SOUL_PATH = Path.home() / ".hyperspace" / "agent" / "SOUL.md"
_PROJECT_DIR = Path(__file__).parent.parent / "projects" / "grid-regime-discovery"


class GRIDResearchAgent:
    """Manages GRID's public research agent on the Hyperspace network.

    Handles soul document setup, project definitions, and experiment
    logging.  All published data is non-sensitive — no trading signals,
    feature combinations, or discovered cluster structures.

    Attributes:
        project_dir: Path to the project directory.
    """

    def __init__(self) -> None:
        """Initialise the research agent manager."""
        self.project_dir = _PROJECT_DIR
        log.info("GRIDResearchAgent initialised — project_dir={p}", p=self.project_dir)

    def setup_soul(self) -> None:
        """Write the agent SOUL document if it does not already exist.

        Creates ``~/.hyperspace/agent/SOUL.md`` with the AGENT_SOUL content.
        Never overwrites an existing SOUL.md.
        """
        if _SOUL_PATH.exists():
            log.info("SOUL.md already exists at {p} — not overwriting", p=_SOUL_PATH)
            return

        _SOUL_PATH.parent.mkdir(parents=True, exist_ok=True)
        _SOUL_PATH.write_text(AGENT_SOUL, encoding="utf-8")
        log.info("SOUL.md written to {p}", p=_SOUL_PATH)

    def create_project_definition(self) -> None:
        """Create the grid-regime-discovery project README.

        Writes ``projects/grid-regime-discovery/README.md`` with the
        project description, goals, baseline, and contribution guide.
        """
        self.project_dir.mkdir(parents=True, exist_ok=True)
        readme_path = self.project_dir / "README.md"

        content = """# Grid Regime Discovery

## Project Description

Systematic study of regime-switching behavior in financial time series
using public market indicators.

## Goal

Find the minimum feature set that reliably identifies distinct market
states in historical data.

## Baseline

Random regime assignment (chance-level classification).

- **Method:** Random assignment to k states
- **Persistence Score:** 0.25 (chance level)
- **Silhouette Score:** 0.0

## Primary Metric

**Cluster persistence score** — average regime duration divided by total
periods. Higher is better (regimes should be stable, not flickering).

## Secondary Metrics

- Silhouette score (cluster separation quality)
- Transition recall (how well transitions are detected)
- Out-of-sample stability (do regimes generalise to unseen periods?)

## Important Note

This project studies regime structure only. No trading signals, no
entry/exit logic, no position sizing. All indicators used are publicly
available macroeconomic and market data.

## Contribution Guide

1. **One variable at a time.** Change one thing per experiment.
2. **Report failures.** Negative results are valuable.
3. **Clean baselines.** Every experiment must compare to random assignment.
4. **Public data only.** Use only publicly available indicators.
5. **No signal logic.** Do not include or derive trading signals.
6. **Log everything** using the standard experiment format in LEADERBOARD.md.
"""
        readme_path.write_text(content, encoding="utf-8")
        log.info("Project README written to {p}", p=readme_path)

        # Initialise LEADERBOARD.md if it doesn't exist
        leaderboard_path = self.project_dir / "LEADERBOARD.md"
        if not leaderboard_path.exists():
            leaderboard_path.write_text(
                "# Grid Regime Discovery — Leaderboard\n\n"
                "| Run ID | Date | Features | k | Persistence | Silhouette | Notes |\n"
                "|--------|------|----------|---|-------------|------------|-------|\n",
                encoding="utf-8",
            )
            log.info("LEADERBOARD.md initialised at {p}", p=leaderboard_path)

    def log_experiment(
        self,
        run_id: str,
        features_used: list[str],
        n_clusters: int,
        persistence_score: float,
        silhouette_score: float,
        notes: str,
    ) -> None:
        """Append an experiment result to the leaderboard.

        Only logs public, non-sensitive metrics. Feature names are public
        indicator names — no proprietary signal logic is included.

        Parameters:
            run_id: Unique experiment identifier.
            features_used: List of public feature/indicator names used.
            n_clusters: Number of clusters tested.
            persistence_score: Cluster persistence metric.
            silhouette_score: Silhouette score.
            notes: Free-text notes about the experiment.
        """
        leaderboard_path = self.project_dir / "LEADERBOARD.md"

        if not leaderboard_path.exists():
            self.create_project_definition()

        features_str = ", ".join(features_used[:5])
        if len(features_used) > 5:
            features_str += f" (+{len(features_used) - 5} more)"

        date_str = datetime.now().strftime("%Y-%m-%d")
        row = (
            f"| {run_id} | {date_str} | {features_str} | {n_clusters} | "
            f"{persistence_score:.4f} | {silhouette_score:.4f} | {notes} |\n"
        )

        with leaderboard_path.open("a", encoding="utf-8") as f:
            f.write(row)

        log.info(
            "Experiment logged — run={r}, k={k}, persistence={p:.4f}",
            r=run_id,
            k=n_clusters,
            p=persistence_score,
        )


if __name__ == "__main__":
    agent = GRIDResearchAgent()
    agent.setup_soul()
    agent.create_project_definition()
    print(f"Soul path: {_SOUL_PATH}")
    print(f"Project dir: {_PROJECT_DIR}")
    print("Research agent setup complete")
