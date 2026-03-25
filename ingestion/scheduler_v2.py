"""
GRID extended ingestion scheduler (v2) — DEPRECATED.

.. deprecated::
    All functionality has been merged into ``ingestion.scheduler``.
    This module re-exports ``run_pull_group`` and ``backfill_all`` from
    the unified scheduler for backwards compatibility only.

    Direct imports of this module will emit a DeprecationWarning.
"""

from __future__ import annotations

import warnings

warnings.warn(
    "ingestion.scheduler_v2 is deprecated. "
    "Use ingestion.scheduler (run_pull_group, backfill_all) instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export for backwards compatibility
from ingestion.scheduler import backfill_all, run_pull_group  # noqa: F401

__all__ = ["run_pull_group", "backfill_all"]
