"""quiverquant gets the time its full run takes.

Measured on grid-svr, 2026-11-23: ``quiverquant`` (10 endpoints) takes
84-124 s. At 120 s, 5 of 13 runs between 11-19 and 11-23 were logged FAILED
and then stored their rows 4 s later.

``etf_flows`` is deliberately not raised here. Its full run takes 346 s
against 120 s, but a timeout does not stop the run (the thread is orphaned
and keeps writing), and until the 13F re-run dedupe is live every completed
run appends the same change rows again. Its limit moves after that lands.
"""

from __future__ import annotations

import pytest

import scripts.hermes_operator as ho
from ingestion.smart_scheduler import PULLER_REGISTRY, TICK_TIME_BUDGET_S

MEASURED_FULL_RUN_S = {"quiverquant": 124}


@pytest.mark.parametrize("name", sorted(MEASURED_FULL_RUN_S))
def test_timeout_covers_the_measured_run_with_margin_and_fits_the_hermes_step(name: str) -> None:
    (entry,) = [p for p in PULLER_REGISTRY if p["name"] == name]
    timeout_s = entry["timeout_s"]

    assert timeout_s >= 1.5 * MEASURED_FULL_RUN_S[name]
    # Same enclosure rule as tests/test_hermes_timeout_budgets.py: a puller
    # started at the end of the tick budget must finish inside the step.
    assert TICK_TIME_BUDGET_S + timeout_s + 60 <= ho.SMART_INGESTION_TIMEOUT_SECONDS


def test_etf_flows_limit_is_not_raised_before_the_13f_dedupe() -> None:
    (entry,) = [p for p in PULLER_REGISTRY if p["name"] == "etf_flows"]
    assert entry["timeout_s"] == 120
