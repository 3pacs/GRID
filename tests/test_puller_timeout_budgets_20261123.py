"""etf_flows and quiverquant get the time their full runs take.

Measured on grid-svr, 2026-11-23:

* ``etf_flows`` (11 ETFs, then 75 managers' two latest 13F filings read in
  full): 346 s. At 120 s every run was cut off at about manager 26 and logged
  FAILED with 0 rows while its orphaned thread kept writing.
* ``quiverquant`` (10 endpoints): 84-124 s. At 120 s, 5 of 13 runs between
  11-19 and 11-23 were logged FAILED and then stored their rows 4 s later.
"""

from __future__ import annotations

import pytest

import scripts.hermes_operator as ho
from ingestion.smart_scheduler import PULLER_REGISTRY, TICK_TIME_BUDGET_S

MEASURED_FULL_RUN_S = {"etf_flows": 346, "quiverquant": 124}


@pytest.mark.parametrize("name", sorted(MEASURED_FULL_RUN_S))
def test_timeout_covers_the_measured_run_with_margin_and_fits_the_hermes_step(name: str) -> None:
    (entry,) = [p for p in PULLER_REGISTRY if p["name"] == name]
    timeout_s = entry["timeout_s"]

    assert timeout_s >= 1.5 * MEASURED_FULL_RUN_S[name]
    # Same enclosure rule as tests/test_hermes_timeout_budgets.py: a puller
    # started at the end of the tick budget must finish inside the step.
    assert TICK_TIME_BUDGET_S + timeout_s + 60 <= ho.SMART_INGESTION_TIMEOUT_SECONDS
