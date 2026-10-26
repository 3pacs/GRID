"""Offline P1 contract checks; no DB, provider, timer or collector activation."""
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def load(path):
    spec = importlib.util.spec_from_file_location("gex_design_test", ROOT / path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_disabled_runner_and_true_fail_closed(monkeypatch, capsys):
    runner = load("scripts/run_gamma_watch_ingest.py")
    monkeypatch.delenv("GRID_ENABLE_GAMMA_WATCH_INGEST_JOB", raising=False)
    assert runner.main() == 0
    assert "disabled" in capsys.readouterr().out
    for flag in ("", "1", "yes", "true"):
        monkeypatch.setenv("GRID_ENABLE_GAMMA_WATCH_INGEST_JOB", flag)
        with pytest.raises(SystemExit):
            runner.main()


def test_sources_have_distinct_meanings_and_are_inactive():
    manifest = json.loads((ROOT / "config/gamma_watch_sources.json").read_text())
    assert manifest["default_active"] is False
    sources = manifest["sources"]
    assert len({s['name'] for s in sources}) == len(sources)
    by_name = {s['name']: s for s in sources}
    assert by_name['gamma_watch_yahoo_intraday_unadjusted_v1']['basis'] != by_name['gamma_watch_yahoo_daily_adjusted_v1']['basis']
    assert by_name['gamma_watch_vendor_delayed_gex_v1']['admission'] == 'context_only'
    assert by_name['gamma_watch_rtd_model_gex_v1']['admission'] == 'research_only'
    assert by_name['gamma_watch_frozen_chain_scenarios_v1']['admission'] == 'research_only'


def test_migration_not_discovered_and_guard_precedes_sql(monkeypatch):
    from alembic.config import Config
    from alembic.script import ScriptDirectory
    cfg = Config()
    cfg.set_main_option('script_location', str(ROOT / 'migrations'))
    revisions = {r.revision for r in ScriptDirectory.from_config(cfg).walk_revisions()}
    assert 'gamma_watch_p1_20260928' not in revisions
    migration = load('migrations/proposed/gamma_watch_p1_20260928.py')
    monkeypatch.delenv('GRID_APPROVE_GAMMA_WATCH_SCHEMA', raising=False)
    with pytest.raises(RuntimeError, match='approval'):
        migration.upgrade()
    with pytest.raises(RuntimeError, match='preserve'):
        migration.downgrade()


def test_timer_has_no_enable_target_and_service_is_disabled():
    service = (ROOT / 'deploy/systemd/grid-gamma-watch-ingest.service.template').read_text()
    timer = (ROOT / 'deploy/systemd/grid-gamma-watch-ingest.timer.template').read_text()
    for body in (service, timer):
        active = '\n'.join(l for l in body.splitlines() if not l.startswith('#'))
        assert '[Install]' not in active and 'WantedBy=' not in active
        assert 'ConditionPathExists=/etc/grid/approvals/gamma-watch-ingest' in active
    assert 'Environment=GRID_ENABLE_GAMMA_WATCH_INGEST_JOB=false' in service
    assert 'Persistent=false' in timer
