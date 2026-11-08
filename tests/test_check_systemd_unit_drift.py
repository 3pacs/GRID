"""scripts/check_systemd_unit_drift.sh: installed units vs shipped templates, with --sync."""
from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "check_systemd_unit_drift.sh"
SERVICE = "[Unit]\nDescription=x\n\n[Service]\nType=oneshot\nExecStart=/bin/true\n"
MANAGED = "# deploy-sync: managed\n"


def run(templates: Path, system: Path, *args: str) -> subprocess.CompletedProcess:
    fake_bin = system.parent / "bin"
    fake_bin.mkdir(exist_ok=True)
    systemctl = fake_bin / "systemctl"
    systemctl.write_text("#!/usr/bin/env bash\necho \"systemctl $*\" >> \"$(dirname \"$0\")/systemctl.log\"\n")
    systemctl.chmod(systemctl.stat().st_mode | stat.S_IEXEC)
    env = dict(os.environ, SUDO="", PATH=f"{fake_bin}:{os.environ['PATH']}")
    return subprocess.run(["bash", str(SCRIPT), "--templates", str(templates), "--system-dir", str(system), *args],
                          capture_output=True, text=True, env=env, check=False)


@pytest.fixture
def dirs(tmp_path: Path) -> tuple[Path, Path]:
    templates, system = tmp_path / "templates", tmp_path / "system"
    templates.mkdir()
    system.mkdir()
    # installed and identical apart from comments/blank lines
    (templates / "grid-a.service.template").write_text("# TEMPLATE comment\n" + MANAGED + SERVICE)
    (system / "grid-a.service").write_text(SERVICE + "\n\n")
    # installed and drifted (new Environment= line in the template)
    (templates / "grid-b.service.template").write_text(MANAGED + SERVICE.replace("ExecStart", "Environment=X=1\nExecStart"))
    (system / "grid-b.service").write_text(SERVICE)
    # plain (non-.template) timer, drifted
    (templates / "grid-c.timer").write_text(MANAGED + "[Timer]\nOnCalendar=*-*-* 11:35:00 UTC\n")
    (system / "grid-c.timer").write_text("[Timer]\nOnCalendar=*-*-* 10:35:00 UTC\n")
    # shipped but not installed: must never be installed
    (templates / "grid-new.service.template").write_text(SERVICE)
    # templated instance unit, drifted: reported, never synced
    (templates / "grid-worker@.service.template").write_text(SERVICE)
    (system / "grid-worker@.service").write_text(SERVICE.replace("/bin/true", "/bin/false"))
    # installed, drifted, but the template did not opt in: reported, never synced
    (templates / "grid-e2.service.template").write_text(SERVICE.replace("Type=oneshot", "User=grid\nType=oneshot"))
    (system / "grid-e2.service").write_text(SERVICE.replace("Type=oneshot", "User=e2board\nType=oneshot"))
    # a README in the templates dir is ignored
    (templates / "README.md").write_text("docs\n")
    return templates, system


def test_report_only_lists_drift_and_changes_nothing(dirs):
    templates, system = dirs
    result = run(templates, system)
    assert result.returncode == 0, result.stderr
    out = result.stdout
    assert "ok     grid-a.service" in out
    assert "DRIFT  grid-b.service" in out and "1 differing line(s)" in out
    assert "Environment=X=1" not in out and "X=1" not in out          # unit contents never reach CI logs by default
    assert "DRIFT  grid-c.timer" in out
    assert "DRIFT  grid-worker@.service (templated instance unit" in out
    assert "grid-new" not in out
    assert "DRIFT  grid-e2.service (unmanaged template" in out
    assert "units checked=5 drifted=4 synced=0 skipped=1 unmanaged=1 remaining=4" in out
    assert (system / "grid-b.service").read_text() == SERVICE          # untouched
    assert not (system / "grid-new.service").exists()
    assert not (system.parent / "bin" / "systemctl.log").exists()      # no daemon-reload without --sync


def test_verbose_prints_the_diff_with_environment_values_redacted(dirs):
    templates, system = dirs
    (templates / "grid-b.service.template").write_text(
        MANAGED + SERVICE.replace("ExecStart", "Environment=TOKEN=s3cr3t-value\nExecStart"))
    out = run(templates, system, "--verbose").stdout
    assert "> Environment=TOKEN=<redacted>" in out and "s3cr3t" not in out
    assert "> User=grid" in out                                         # non-Environment lines are shown as-is


def test_strict_fails_on_remaining_drift_and_plain_mode_exits_zero(dirs):
    templates, system = dirs
    assert run(templates, system, "--strict").returncode == 2
    assert run(templates, system).returncode == 0          # the deploy step passes --strict for that reason


def test_sync_reinstalls_drifted_units_and_reloads(dirs):
    templates, system = dirs
    result = run(templates, system, "--sync", "--strict")
    out = result.stdout
    assert "SYNCED grid-b.service" in out and "SYNCED grid-c.timer" in out
    assert (system / "grid-b.service").read_text() == (templates / "grid-b.service.template").read_text()
    assert (system / "grid-c.timer").read_text() == (templates / "grid-c.timer").read_text()
    assert (system / "grid-b.service").stat().st_mode & 0o777 == 0o644
    assert (system / "grid-worker@.service").read_text().endswith("/bin/false\n")   # never synced
    assert "User=e2board" in (system / "grid-e2.service").read_text()                # unmanaged: never synced
    assert not (system / "grid-new.service").exists()                              # never installed
    assert (system.parent / "bin" / "systemctl.log").read_text() == "systemctl daemon-reload\n"
    assert "units checked=5 drifted=4 synced=2 skipped=1 unmanaged=1 remaining=2" in out
    assert result.returncode == 2          # the templated and the unmanaged unit still drift
    # second run: nothing left to sync, no reload
    (system.parent / "bin" / "systemctl.log").unlink()
    again = run(templates, system, "--sync")
    assert "synced=0" in again.stdout and not (system.parent / "bin" / "systemctl.log").exists()


def test_bad_arguments_and_missing_dirs(tmp_path):
    missing = run(tmp_path / "nope", tmp_path)
    assert missing.returncode == 64 and "templates dir not found" in missing.stderr
    bogus = subprocess.run(["bash", str(SCRIPT), "--what"], capture_output=True, text=True, check=False)
    assert bogus.returncode == 64
