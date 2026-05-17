"""Routing helpers for backlog task ownership."""

from __future__ import annotations


SURFACER_BACKFILL_TASK_TYPE = "surfacer_data_backfill"
DEDICATED_BACKLOG_TASK_TYPES = (SURFACER_BACKFILL_TASK_TYPE,)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def generic_backlog_predicate(column: str = "task_type") -> str:
    """SQL predicate for generic workers that must not claim dedicated tasks."""
    dedicated = ", ".join(_sql_literal(task_type) for task_type in DEDICATED_BACKLOG_TASK_TYPES)
    return f"{column} NOT IN ({dedicated})"
