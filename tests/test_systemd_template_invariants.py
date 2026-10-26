"""Guard: deploy/systemd unit templates run from the release tree and avoid the backup window.

Two invariants, both learned the hard way:

1. Every writer template in ``deploy/systemd/`` must run from
   ``/data/grid_v4/grid_release`` — the release symlink ``grid-api`` and every
   live ``grid-godview-*`` writer already run from, and which
   ``.github/workflows/deploy.yml`` resets to ``main`` on every deploy. Two
   templates (``grid-causal-links``, ``grid-market-diary``) instead pointed
   ``WorkingDirectory`` at ``/home/grid/grid_v4/grid_repo`` — an older,
   stale/diverged checkout that no live service actually runs from (the same
   finding PR #712/#713/#714 review made for grid-dollar-flows,
   grid-regime-state-vectors and grid-hypothesis-forward-log, which already
   got it right). ``ExecStart`` must agree: a relative script path resolves
   against ``WorkingDirectory`` (fine), an absolute one must be anchored
   under ``/data/grid_v4/grid_release`` (not the stale checkout), and a
   ``python3 -m pkg.module`` invocation is fine either way.

   ``grid-goal-worker.service.template`` is intentionally excluded: it is a
   per-Tailnet-node idle-fleet worker instantiated on hosts other than
   grid-svr (``WorkingDirectory=/opt/grid``, per-node env file under
   ``/etc/grid/``), not part of the grid-svr release pipeline this invariant
   is about.

2. No unit's ``OnCalendar`` may land inside the nightly encrypted Postgres
   backup window. ``grid-pg-backup.timer`` fires 03:30 UTC and recent runs
   have taken until roughly 10:05-10:30 UTC (see PR #712/#713/#714 review
   and docs/handoffs); several templates already carry comments recording a
   time they were moved off specifically to clear this window.
   ``grid-analytics-snapshots.timer.template`` still fired at 07:15 UTC,
   squarely inside it (a live ``timer.d/override.conf`` on grid-svr had
   quietly moved the *running* timer to 09:15 UTC, itself still inside a
   03:30-10:30 UTC reading of the window — the repo template never caught
   up). This test enforces the boundary directly on every daily/weekday
   UTC-anchored ``OnCalendar`` line in ``deploy/systemd/`` so a future
   template can't reintroduce the collision. Weekly, non-UTC calendars
   (``grid-godview-fed``/``grid-godview-cftc``, anchored to
   ``America/New_York`` release events) are exempted by construction: they
   fire on specific weekly astronomical/regulatory events already reviewed
   against the window, not a daily UTC cadence.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SYSTEMD_DIR = REPO / "deploy" / "systemd"

RELEASE_TREE = "/data/grid_v4/grid_release"
REPO_ENV_FILE = "/home/grid/grid_v4/grid_repo/.env"
STALE_CHECKOUT = "/home/grid/grid_v4/grid_repo"

# Instantiated per-Tailnet-node idle-fleet worker: a different deployment
# target entirely (not grid-svr, not the release tree, not the repo .env).
EXCLUDED_FROM_RELEASE_TREE_CHECK = {"grid-goal-worker.service.template"}

# Backup window, inclusive start / exclusive end, in minutes since midnight UTC.
BACKUP_WINDOW_START_MIN = 3 * 60 + 30  # 03:30 UTC
BACKUP_WINDOW_END_MIN = 10 * 60 + 30  # 10:30 UTC

ONCALENDAR_UTC_RE = re.compile(
    r"^OnCalendar=(?:(?P<weekday>[A-Za-z.,]+)\s+)?\*-\*-\*\s+"
    r"(?P<hour>\d{2}):(?P<minute>\d{2}):\d{2}\s+UTC\s*$"
)


def _service_templates() -> list[Path]:
    return sorted(SYSTEMD_DIR.glob("*.service.template"))


def _all_timer_units() -> list[Path]:
    return sorted(SYSTEMD_DIR.glob("*.timer.template")) + sorted(
        SYSTEMD_DIR.glob("*.timer")
    )


def _joined_lines(text: str) -> list[str]:
    """Join backslash line continuations, as systemd does when parsing a unit."""
    joined: list[str] = []
    buf = ""
    for raw in text.splitlines():
        line = raw.rstrip()
        if line.endswith("\\"):
            buf += line[:-1] + " "
            continue
        joined.append(buf + line)
        buf = ""
    if buf:
        joined.append(buf)
    return joined


def _value(lines: list[str], key: str) -> str | None:
    prefix = f"{key}="
    for line in lines:
        stripped = line.strip()
        if stripped.startswith(prefix):
            return stripped[len(prefix) :].strip()
    return None


@pytest.mark.unit
@pytest.mark.parametrize("path", _service_templates(), ids=lambda p: p.name)
def test_service_template_runs_from_release_tree(path: Path) -> None:
    if path.name in EXCLUDED_FROM_RELEASE_TREE_CHECK:
        pytest.skip(f"{path.name}: not a grid-svr release-tree writer (see module docstring)")

    lines = _joined_lines(path.read_text(encoding="utf-8"))

    working_directory = _value(lines, "WorkingDirectory")
    assert working_directory == RELEASE_TREE, (
        f"{path.name}: WorkingDirectory={working_directory!r}, expected "
        f"{RELEASE_TREE!r} (the deployed release tree grid-api and every "
        f"live grid-godview-* writer run from)"
    )

    environment_file = _value(lines, "EnvironmentFile")
    assert environment_file == REPO_ENV_FILE, (
        f"{path.name}: EnvironmentFile={environment_file!r}, expected "
        f"{REPO_ENV_FILE!r} (the same env file grid-scheduler, grid-hermes "
        f"and the godview writers use; it deliberately still lives in the "
        f"repo checkout, not the release tree)"
    )

    user = _value(lines, "User")
    assert user == "grid", f"{path.name}: User={user!r}, expected 'grid'"

    exec_start = _value(lines, "ExecStart")
    assert exec_start is not None, f"{path.name}: no ExecStart="
    assert STALE_CHECKOUT not in exec_start, (
        f"{path.name}: ExecStart references the stale checkout "
        f"{STALE_CHECKOUT!r} instead of running relative to "
        f"WorkingDirectory or from {RELEASE_TREE!r}: {exec_start!r}"
    )
    # A `python3 -m pkg.module` invocation needs no path check. Otherwise,
    # any absolute path in the command must be anchored under the release
    # tree -- never under the stale /home/grid/grid_v4/grid_repo checkout.
    if " -m " not in exec_start:
        for token in exec_start.split():
            if token.startswith("/") and token.endswith(".py"):
                assert token.startswith(RELEASE_TREE + "/"), (
                    f"{path.name}: ExecStart script path {token!r} is not "
                    f"under {RELEASE_TREE!r}"
                )


@pytest.mark.unit
def test_no_service_template_points_at_stale_checkout_as_code_dir() -> None:
    """Belt-and-suspenders: grep every template for the stale checkout path
    outside of EnvironmentFile= (where it is intentional and required)."""
    offenders = []
    for path in _service_templates():
        if path.name in EXCLUDED_FROM_RELEASE_TREE_CHECK:
            continue
        for line in _joined_lines(path.read_text(encoding="utf-8")):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            if STALE_CHECKOUT in stripped and not stripped.startswith("EnvironmentFile="):
                offenders.append(f"{path.name}: {stripped}")
    assert not offenders, "stale checkout path used as a code dir:\n" + "\n".join(offenders)


@pytest.mark.unit
@pytest.mark.parametrize("path", _all_timer_units(), ids=lambda p: p.name)
def test_timer_oncalendar_avoids_nightly_backup_window(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    utc_lines = [
        line.strip()
        for line in text.splitlines()
        if line.strip().startswith("OnCalendar=") and "UTC" in line
    ]
    if not utc_lines:
        pytest.skip(f"{path.name}: no UTC-anchored OnCalendar line (non-UTC calendars are reviewed separately)")

    for line in utc_lines:
        m = ONCALENDAR_UTC_RE.match(line)
        assert m, f"{path.name}: unrecognized UTC OnCalendar syntax: {line!r}"
        minute_of_day = int(m.group("hour")) * 60 + int(m.group("minute"))
        in_window = BACKUP_WINDOW_START_MIN <= minute_of_day < BACKUP_WINDOW_END_MIN
        assert not in_window, (
            f"{path.name}: {line!r} fires at {m.group('hour')}:{m.group('minute')} "
            f"UTC, inside the nightly pg_dump window "
            f"({BACKUP_WINDOW_START_MIN // 60:02d}:{BACKUP_WINDOW_START_MIN % 60:02d}-"
            f"{BACKUP_WINDOW_END_MIN // 60:02d}:{BACKUP_WINDOW_END_MIN % 60:02d} UTC) "
            f"-- move it out"
        )


@pytest.mark.unit
def test_at_least_one_service_template_and_one_timer_were_checked() -> None:
    """Guard against a glob typo silently turning every parametrized test into a no-op."""
    assert len(_service_templates()) >= 5
    assert len(_all_timer_units()) >= 5
