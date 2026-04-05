"""
GRID LLM Task Queue — data models, constants, and type definitions.

Contains the LLMTask dataclass, task type constants, and queue configuration.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable


# ---------------------------------------------------------------------------
# Task type constants — used for routing and display
# ---------------------------------------------------------------------------

REALTIME_TYPES = frozenset({
    "trade_review",
    "convergence_alert",
    "regime_change_explanation",
    "user_chat",
})

SCHEDULED_TYPES = frozenset({
    "thesis_narrative",
    "cross_reference_narrative",
    "postmortem_analysis",
    "hypothesis_review",
})

BACKGROUND_TYPES = frozenset({
    "web_scrape_summarize",
    "feature_interpretation",
    "actor_research",
    "hypothesis_generation",
    "market_briefing",
    "anomaly_detection",
    "narrative_history",
    "prediction_refinement",
    "knowledge_building",
    "correlation_discovery",
    "company_analysis",
    "offshore_leak_investigation",
    "panama_papers_research",
})


# ---------------------------------------------------------------------------
# Task dataclass
# ---------------------------------------------------------------------------

@dataclass
class LLMTask:
    """A unit of work for the LLM."""

    id: str
    priority: int                   # 1=realtime, 2=scheduled, 3=background
    task_type: str
    prompt: str
    context: dict
    callback: Callable | None       # called with (task) after completion
    created_at: str
    started_at: str | None = None
    completed_at: str | None = None
    result: str | None = None
    error: str | None = None

    # For heapq ordering: (priority, creation_time, unique_id)
    _sort_key: tuple = field(default=(), repr=False)

    def __post_init__(self) -> None:
        if not self._sort_key:
            object.__setattr__(
                self, "_sort_key", (self.priority, self.created_at, self.id)
            )

    def __lt__(self, other: LLMTask) -> bool:
        return self._sort_key < other._sort_key

    def __le__(self, other: LLMTask) -> bool:
        return self._sort_key <= other._sort_key


# ---------------------------------------------------------------------------
# Queue configuration constants
# ---------------------------------------------------------------------------

# Default timeout for a single LLM call (seconds)
TASK_TIMEOUT_SECONDS = 60

# How many background tasks to generate per refill
BACKGROUND_BATCH_SIZE = 200  # Qwen should never be idle — backlog dominant

# Minimum seconds between background refills (avoid spamming)
BACKGROUND_REFILL_COOLDOWN = 10  # refill more often — never idle

# Max completed tasks kept in memory for status/history
MAX_HISTORY = 500
