"""The 2026-11-21 dollar-plumbing and regional-survey FRED ids are in the default pull.

Read from the source with ``ast`` (no fedfred, config or database import), the
same way ``tests/test_fred_default_dispatch.py`` reads the list.
"""

from __future__ import annotations

import ast
from pathlib import Path

NEW_FRED_IDS: tuple[str, ...] = (
    "IORB", "WRESBAL", "WLCFLPCL", "RPONTSYD", "CPN3M", "DTB3",
    "GACDISA066MSFRBNY", "GACDFSA066MSFRBPHI", "BACTSAMFRBDAL",
)


def _fred_series_list() -> list[str]:
    fred_path = Path(__file__).resolve().parent.parent / "ingestion" / "fred.py"
    tree = ast.parse(fred_path.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.AnnAssign) and getattr(node.target, "id", None) == "FRED_SERIES_LIST":
            return ast.literal_eval(node.value)
    raise AssertionError("FRED_SERIES_LIST not found")


def test_fred_plumbing_and_regional_surveys_registered() -> None:
    series_list = _fred_series_list()
    for sid in NEW_FRED_IDS:
        assert sid in series_list
    assert len(series_list) == len(set(series_list))


def test_new_ids_are_appended_after_the_existing_defaults() -> None:
    # Appending keeps every existing series ahead of the new ones in the
    # deadline-bounded bulk pull (FRED_JOB_BUDGET_S): if a run runs out of
    # time, the new ids are the ones skipped, not an existing feed.
    series_list = _fred_series_list()
    assert series_list[-len(NEW_FRED_IDS):] == list(NEW_FRED_IDS)
