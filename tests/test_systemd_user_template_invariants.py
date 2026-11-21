"""Guard: pinned-checkout user-unit templates in ``deploy/systemd-user/``.

``deploy/systemd/`` templates must run from the release tree
(``tests/test_systemd_template_invariants.py``; that guard is not relaxed and
covers every template in that directory). A unit whose output must not change
when ``main`` does, such as the Desk receipt log, runs instead from a checkout
pinned to one commit. Those templates live in ``deploy/systemd-user/`` under
this guard, which states the pinned contract as strictly as the other states
the release contract:

* the working directory and every repository path are ``@CODE@``, the pinned
  checkout's placeholder: never the release tree, which moves on each deploy,
  and never the stale ``grid_repo`` checkout;
* the environment file is the one the system units use;
* a user unit sets no ``User=`` or ``Group=``;
* the placement is declared in the template's first line;
* a UTC-anchored timer stays out of the nightly backup window.

And the two directories cannot be confused: no ``deploy/systemd/`` template
uses ``@CODE@``, and no unit name appears in both.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
USER_DIR = REPO / "deploy" / "systemd-user"
SYSTEM_DIR = REPO / "deploy" / "systemd"

PLACEHOLDER = "@CODE@"
RELEASE_TREE = "/data/grid_v4/grid_release"
STALE_CHECKOUT = "/home/grid/grid_v4/grid_repo"
REPO_ENV_FILE = "/home/grid/grid_v4/grid_repo/.env"
PLACEMENT_LINE = "# TEMPLATE. placement: pinned-checkout user unit"
REPO_DIRS = ("scripts/", "intelligence/", "analysis/", "ingestion/", "deploy/")
BACKUP_WINDOW_MIN = (3 * 60 + 30, 10 * 60 + 30)  # 03:30-10:30 UTC, as in the system guard
ONCALENDAR_UTC = re.compile(r"^OnCalendar=.*\s(\d{2}):(\d{2}):\d{2}\s+UTC\s*$")


def _services() -> list[Path]:
    return sorted(USER_DIR.glob("*.service.template"))


def _timers() -> list[Path]:
    return sorted(USER_DIR.glob("*.timer.template"))


def _settings(path: Path) -> list[str]:
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.strip().startswith(("#", "["))]


def _value(lines: list[str], key: str) -> str | None:
    return next((line.split("=", 1)[1].strip() for line in lines
                 if line.startswith(f"{key}=")), None)


@pytest.mark.unit
@pytest.mark.parametrize("path", _services(), ids=lambda p: p.name)
def test_user_service_template_runs_from_the_pinned_checkout(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    lines = _settings(path)

    assert text.startswith(PLACEMENT_LINE), f"{path.name}: first line must declare the placement"
    assert _value(lines, "WorkingDirectory") == PLACEHOLDER
    assert _value(lines, "EnvironmentFile") == REPO_ENV_FILE
    assert _value(lines, "User") is None and _value(lines, "Group") is None, (
        f"{path.name}: a user unit sets no User= or Group=")

    exec_start = _value(lines, "ExecStart")
    assert exec_start, f"{path.name}: no ExecStart="
    repo_paths = [token for token in exec_start.split()
                  if any(f"/{name}" in token or token.startswith(name) for name in REPO_DIRS)]
    assert repo_paths, f"{path.name}: ExecStart runs nothing from the checkout"
    for token in repo_paths:
        assert token.startswith(PLACEHOLDER + "/"), (
            f"{path.name}: {token!r} must be under {PLACEHOLDER}, the pinned checkout")

    for line in lines:
        assert RELEASE_TREE not in line, (
            f"{path.name}: a pinned unit must not use the release tree, which moves on "
            f"every deploy: {line!r}")
        if not line.startswith("EnvironmentFile="):
            assert STALE_CHECKOUT not in line, (
                f"{path.name}: the stale checkout used outside EnvironmentFile=: {line!r}")


@pytest.mark.unit
@pytest.mark.parametrize("path", _services(), ids=lambda p: p.name)
def test_user_service_template_renders_to_one_checkout(path: Path) -> None:
    pinned = "/data/agent-home/someone/pinned/0123abc"
    rendered = path.read_text(encoding="utf-8").replace(PLACEHOLDER, pinned)
    lines = [line.strip() for line in rendered.splitlines()
             if line.strip() and not line.strip().startswith("#")]

    assert all(PLACEHOLDER not in line for line in lines)
    assert f"WorkingDirectory={pinned}" in lines
    assert any(line.startswith("ExecStart=") and f"{pinned}/scripts/" in line for line in lines)


@pytest.mark.unit
@pytest.mark.parametrize("path", _timers(), ids=lambda p: p.name)
def test_user_timer_avoids_the_nightly_backup_window(path: Path) -> None:
    for line in _settings(path):
        match = ONCALENDAR_UTC.match(line)
        if match:
            minute = int(match.group(1)) * 60 + int(match.group(2))
            assert not BACKUP_WINDOW_MIN[0] <= minute < BACKUP_WINDOW_MIN[1], (
                f"{path.name}: {line!r} is inside the nightly backup window")


@pytest.mark.unit
def test_the_release_tree_directory_holds_no_pinned_unit() -> None:
    """The pinned contract cannot be used to get around the release-tree guard."""
    offenders = [
        f"{path.name}: {line.strip()}"
        for path in sorted(SYSTEM_DIR.iterdir()) if path.is_file() and path.suffix != ".md"
        for line in path.read_text(encoding="utf-8").splitlines()
        if PLACEHOLDER in line and not line.strip().startswith("#")
    ]
    assert not offenders, "pinned-checkout placeholder in deploy/systemd:\n" + "\n".join(offenders)

    def units(folder: Path) -> set[str]:
        return {path.name.removesuffix(".template") for path in folder.iterdir()
                if path.name.endswith((".service", ".timer", ".service.template",
                                       ".timer.template"))}

    assert not units(USER_DIR) & units(SYSTEM_DIR), "a unit is shipped under both contracts"


@pytest.mark.unit
def test_the_guard_checked_something() -> None:
    assert _services() and _timers()
