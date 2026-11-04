"""Sources retired by the owner on 2026-11-04 must stay unscheduled.

ecb_tltro, semi_book_to_bill, taiwan_exports, opensecrets and
Baltic_Exchange produced failures every cycle against dead endpoints
(finviz_fundamentals already had no writer). Their source_catalog rows were
set active=FALSE the same day; these tests keep the schedulers from
reintroducing them.
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_baltic_exchange_is_held_in_the_smart_scheduler() -> None:
    from ingestion.smart_scheduler import hold_reason_for

    reason = hold_reason_for("baltic_exchange")
    assert reason and "Retired 2026-11-04" in reason


def test_intelligence_scheduler_no_longer_runs_retired_jobs() -> None:
    src = (ROOT / "intelligence/scheduler.py").read_text(encoding="utf-8")
    for job in ("_semi_book_to_bill_monthly", "_ecb_tltro_weekly", "_taiwan_exports_monthly"):
        assert job not in src


def test_grid_scheduler_no_longer_runs_opensecrets() -> None:
    src = (ROOT / "ingestion/scheduler.py").read_text(encoding="utf-8")
    assert "OpenSecretsPuller" not in src
