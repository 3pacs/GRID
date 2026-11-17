"""grid-scheduler's 20:00Z "daily" group: retired pullers stay out, 0-row runs stay honest.

The 2026-11-17 audit read pull_log.status alone and saw SUCCESS / 0 rows for
about ten pullers in this group. pull_log's CHECK constraint allows only
RUNNING/SUCCESS/PARTIAL/FAILED, so the outcome lives in error_message as a
``SKIPPED:`` / ``NO_NEW_DATA:`` marker -- SmartScheduler's convention, read by
ingestion/source_freshness.run_outcome, pipeline-health and the health
alerter. These tests pin that for the real return shapes those pullers
produce, and pin Dune_Analytics out of the group (registry hold, retired
2026-11-04 in commit 17889eec).
"""

from __future__ import annotations

import ast
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _stub_registration_imports(monkeypatch) -> list[str]:
    """Replace every module _get_pullers_for_group imports with a fixture.

    ingestion.smart_scheduler stays real so its registry holds apply.
    Returns the class names that were constructed, in order.
    """
    constructed: list[str] = []

    def fixture_class(name):
        class FixturePuller:
            def __init__(self, *args, **kwargs):
                constructed.append(name)
        FixturePuller.__name__ = name
        return FixturePuller

    source = (ROOT / "ingestion/scheduler.py").read_text(encoding="utf-8")
    node = next(n for n in ast.parse(source).body
                if isinstance(n, ast.FunctionDef) and n.name == "_get_pullers_for_group")
    for imp in (n for n in ast.walk(node) if isinstance(n, ast.ImportFrom)):
        if imp.module == "ingestion.smart_scheduler":
            continue
        monkeypatch.setitem(sys.modules, imp.module, SimpleNamespace(**{
            alias.name: fixture_class(alias.name) for alias in imp.names
        }))
    return constructed


def test_daily_group_does_not_run_dune(monkeypatch):
    import ingestion.scheduler as sched
    from ingestion.smart_scheduler import hold_reason_for

    assert "Retired 2026-11-04" in (hold_reason_for("dune") or "")
    constructed = _stub_registration_imports(monkeypatch)

    names = [entry[0] for entry in sched._get_pullers_for_group("daily", None, {})]

    assert names, "registration produced no pullers; the stubbing is broken"
    assert "Dune_Analytics" not in names
    assert "DunePuller" not in constructed


def test_dune_hold_is_the_one_switch(monkeypatch):
    """Lifting the SmartScheduler hold is all it takes to run Dune here again."""
    import ingestion.scheduler as sched
    import ingestion.smart_scheduler as smart

    real = smart.hold_reason_for
    monkeypatch.setattr(smart, "hold_reason_for",
                        lambda name: None if name == "dune" else real(name))
    constructed = _stub_registration_imports(monkeypatch)

    names = [entry[0] for entry in sched._get_pullers_for_group("daily", None, {})]

    assert names.count("Dune_Analytics") == 1
    assert constructed.count("DunePuller") == 1


# Return shapes as the 2026-11-16 20:00Z daily run produced them.
_DISCORD_NO_USERS = [{
    "source": "discord", "status": "SKIPPED",
    "error": "No Discord users configured. Set DISCORD_USERS env var.",
    "signals_found": 0, "rows_inserted": 0,
}]
_CF_RADAR_NO_TOKEN = {
    "status": "SKIPPED", "total_inserted": 0, "per_series": {},
    "detected_anomalies": [], "errors": ["no CF Radar token configured"],
}


@pytest.mark.parametrize(
    ("puller_name", "result", "outcome", "marker"),
    [
        ("Discord_Solana_Scanner", _DISCORD_NO_USERS, "SKIPPED",
         "SKIPPED: all 1 items skipped: No Discord users configured"),
        ("Cloudflare_Radar", _CF_RADAR_NO_TOKEN, "SKIPPED",
         "SKIPPED: ['no CF Radar token configured']"),
        ("USPTO_Patents", {"rows_inserted": 0}, "NO_NEW_DATA",
         "NO_NEW_DATA: run completed, 0 rows written"),
        ("nowcast", 0, "NO_NEW_DATA", "NO_NEW_DATA: run completed, 0 rows written"),
    ],
)
def test_zero_row_daily_runs_record_smart_scheduler_markers(
    monkeypatch, puller_name, result, outcome, marker,
):
    import ingestion.scheduler as sched
    from tests.test_scheduler_pull_logging import _FakeEngine, _SuccessfulPuller

    engine = _FakeEngine({puller_name: 900})
    monkeypatch.setattr(
        sched, "_get_pullers_for_group",
        lambda group, db_engine, config: [(puller_name, _SuccessfulPuller(result), "pull", {})],
    )

    summary = sched.run_pull_group("daily", engine, config={})

    assert summary["success_count"] == 0
    assert summary["results"][0]["status"] == outcome
    row = engine.pull_logs[1]
    # SUCCESS is the only zero-row status pull_log's CHECK constraint allows;
    # the outcome is the error_message marker.
    assert (row["status"], row["rows_inserted"]) == ("SUCCESS", 0)
    assert row["error_message"].startswith(marker)
    assert engine.touched_source_ids == []  # not marked fresh
    assert json.loads(engine.events[0]["payload"])["status"] == outcome

    from ingestion.source_freshness import RUN_NO_NEW_DATA, RUN_SKIPPED, run_outcome
    expected = RUN_SKIPPED if outcome == "SKIPPED" else RUN_NO_NEW_DATA
    assert run_outcome(row["status"], row["rows_inserted"], row["error_message"]) == expected
