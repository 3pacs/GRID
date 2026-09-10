"""Source-level pins for the Sprint 2 wiring (LEVER-PACKAGE.md §7).

These jobs are registered inside long-running daemons that cannot be
executed in a test, so the tests assert the registration text itself —
the same way the build-time invariants for Hermes budgets are pinned.
"""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_journal_verdicts_are_scored_daily() -> None:
    src = _read("intelligence/scheduler.py")
    assert "from scripts.backfill_journal_verdicts import run" in src
    assert '_sched.every().day.at("06:45").do(_journal_verdicts_daily)' in src
    # Must run after realized alpha (06:30) so the alpha rows exist first.
    # Registration order in the file is irrelevant to `schedule`; only the
    # clock times matter.
    assert '_sched.every().day.at("06:30").do(_realized_alpha_daily)' in src
    assert "06:30" < "06:45"


def test_long_horizon_sweep_is_registered_weekly() -> None:
    src = _read("intelligence/scheduler.py")
    assert '_sched.every().sunday.at("05:00").do(_long_horizon_sweep)' in src
    assert "horizon_days=90" in src
    assert "persist_ranking(" in src


def test_flow_materializer_runs_in_the_hermes_daily_block() -> None:
    src = _read("scripts/hermes_operator.py")
    assert "from ingestion.flow_materializer import sync_all" in src
    assert '"flow_materialize"' in src
    # Inside the daily block: after source_audit, before the backtest scan.
    i_audit = src.index('"source_audit", run_full_audit')
    i_flow = src.index('"flow_materialize"')
    i_scan = src.index('"backtest_scan", run_full_scan')
    assert i_audit < i_flow < i_scan


def test_ci_runs_the_frontend_unit_tests() -> None:
    wf = _read(".github/workflows/test.yml")
    i_tsc = wf.index("npx tsc --noEmit")
    i_test = wf.index("npm run test")
    i_build = wf.index("npm run build")
    assert i_tsc < i_test < i_build


def test_react_flow_dependency_and_dead_node_set_are_gone() -> None:
    pkg = _read("pwa/package.json")
    assert "@xyflow/react" not in pkg
    assert not (ROOT / "pwa/src/components/canvas").exists()
    assert (ROOT / "pwa/src/canvas/nodeStyles.js").exists()
    for dead in ("pwa/src/api.ts", "pwa/src/store.ts", "pwa/src/styles/shared.ts", "pwa/src/types/index.ts"):
        assert not (ROOT / dead).exists(), dead


def test_trial_gem_chain_is_scheduled_daily_before_realized_alpha() -> None:
    src = _read("intelligence/scheduler.py")
    assert '_sched.every().day.at("05:40").do(_trial_ingestor_daily)' in src
    assert '_sched.every().day.at("05:55").do(_trial_signal_daily)' in src
    assert '_sched.every().day.at("06:05").do(_small_cap_enrichment_daily)' in src
    assert "from grid.ingestors.trial_ingestor import run as _ingest" in src
    assert "from grid.signals.trial_signal import run_daily as _score" in src
    assert "from ingestion.altdata.small_cap_enrichment import pull_all as _enrich" in src
    # ingest → score → enrich → (06:30) realized alpha, by clock time
    assert "05:40" < "05:55" < "06:05" < "06:30"
