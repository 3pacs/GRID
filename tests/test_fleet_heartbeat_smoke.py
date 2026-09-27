"""Smoke/import test for intelligence/fleet_heartbeat.py (recovery, 2026-09-27).

This module has *never* existed on origin/main. It has been running live
since 2026-05-25 from a separate, uncommitted legacy checkout
(/data/grid_v4/astrogrid_dedup) via a systemd drop-in
(grid-fleet-heartbeat.service.d/path-fix.conf) that overrides the unit's
WorkingDirectory/PYTHONPATH away from the real grid_repo. `alpha_research/
heartbeat.py` (already on main) is an unrelated alpha-research alert job
(VIX regime / PIT freshness) — not a successor to this fleet/infra monitor.

Pure-logic smoke coverage only — no live DB, no subprocess/ssh, no email.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import intelligence.fleet_heartbeat as mod  # noqa: E402


def test_module_imports_and_exposes_registries():
    assert "grid-svr" in mod.FLEET
    assert mod.ENDPOINTS
    assert mod.FRESHNESS
    assert callable(mod.run_fleet_heartbeat)
    assert callable(mod.ensure_schema)


def test_probe_freshness_reports_up_for_a_fresh_file(tmp_path, monkeypatch):
    fresh_file = tmp_path / "fresh.json"
    fresh_file.write_text("{}")
    monkeypatch.setitem(mod.FRESHNESS, "unit-test-fresh", (str(fresh_file), 60))

    results = mod.probe_freshness()
    row = next(r for r in results if r[0] == "unit-test-fresh")
    assert row[2] == "up"


def test_probe_freshness_reports_down_for_a_missing_file(tmp_path, monkeypatch):
    missing_file = tmp_path / "does-not-exist.json"
    monkeypatch.setitem(mod.FRESHNESS, "unit-test-missing", (str(missing_file), 60))

    results = mod.probe_freshness()
    row = next(r for r in results if r[0] == "unit-test-missing")
    assert row[2] == "down"


def test_probe_freshness_reports_degraded_when_stale(tmp_path, monkeypatch):
    stale_file = tmp_path / "stale.json"
    stale_file.write_text("{}")
    old = time.time() - 120 * 60  # 120 minutes ago
    import os

    os.utime(stale_file, (old, old))
    monkeypatch.setitem(mod.FRESHNESS, "unit-test-stale", (str(stale_file), 60))

    results = mod.probe_freshness()
    row = next(r for r in results if r[0] == "unit-test-stale")
    assert row[2] == "degraded"


def test_email_never_raises_even_when_send_alert_is_missing(monkeypatch):
    """`_email` must swallow failures — a broken alert path must not break the sweep."""
    def _boom(*args, **kwargs):
        raise RuntimeError("no smtp configured in test env")

    monkeypatch.setattr("alerts.email.send_alert", _boom, raising=False)
    mod._email("test subject", "test body", "info")  # must not raise
