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
    "surfacer_data_backfill",
})


# ---------------------------------------------------------------------------
# Task type → LLM tier mapping
# ---------------------------------------------------------------------------
# The queue used to route *every* task through Tier.ORACLE, which sent
# routine analysis (hypothesis_generation, company_analysis) to the
# heaviest model in the cluster. That both wasted the strongest node on
# cheap work and serialised the queue behind multi-minute reasoning calls.
#
# This map honours the router's documented 3-tier taxonomy (see
# llm/router.py): LOCAL for extraction/summarisation, REASON for
# analysis/synthesis, ORACLE for high-stakes signals + deep
# investigations. Values are the Tier enum's string values so this
# module stays import-light (no llm.router dependency at load time).
# Unmapped task types fall back to "reason" — the router's own default.
TASK_TYPE_TIERS: dict[str, str] = {
    # LOCAL — extraction, summarisation, transforms
    "web_scrape_summarize": "local",
    "feature_interpretation": "local",
    "surfacer_data_backfill": "local",
    "narrative_history": "local",
    # REASON — analysis, synthesis, thesis, regime, postmortem
    "hypothesis_generation": "reason",
    "hypothesis_review": "reason",
    "company_analysis": "reason",
    "thesis_narrative": "reason",
    "cross_reference_narrative": "reason",
    "postmortem_analysis": "reason",
    "correlation_discovery": "reason",
    "prediction_refinement": "reason",
    "anomaly_detection": "reason",
    "actor_research": "reason",
    "knowledge_building": "reason",
    "market_briefing": "reason",
    "regime_change_explanation": "reason",
    # ORACLE — high-stakes signals, sleuth investigations, interactive
    "trade_review": "oracle",
    "convergence_alert": "oracle",
    "offshore_leak_investigation": "oracle",
    "panama_papers_research": "oracle",
    "user_chat": "oracle",
}

# Tier used when a task_type has no explicit entry above.
DEFAULT_TASK_TIER: str = "reason"


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
