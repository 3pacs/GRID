"""A fake grid-svr for scripts/apply_eligibility_change.py's deployed-code evidence (#1301).

``FakeHost`` stands in for systemctl, ``/proc`` and git. Its default layout mirrors grid-svr: a release root
``grid_release.releases`` holding the current release (grid-api, grid-hermes, grid-coordinator, grid-worker and
grid-intelligence run it through the ``grid_release`` symlink) and an older release that grid-scheduler and
grid-realtime are pinned to. Every tree carries both prerequisite literals unless a test says otherwise, and each
release's git HEAD is its directory name.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from scripts import apply_eligibility_change as elig

POLARITY, HYSTERESIS = elig.PREREQUISITES
GOOD_ORACLE = f'"""Oracle."""\nimport os\n{POLARITY.constant} = "{POLARITY.expected}"\n'
GOOD_REGIME = f'{HYSTERESIS.constant}: str = "{HYSTERESIS.expected}"\n'

CURRENT_SHA = "a" * 40
PINNED_SHA = "b" * 40
NEXT_SHA = "c" * 40
PINNED_UNITS = ("grid-scheduler.service", "grid-realtime.service")
DRY_RUN_AT = datetime(2026, 11, 22, 8, 0, 0, tzinfo=timezone.utc)


def write_tree(root: Path, oracle: Optional[str] = GOOD_ORACLE, regime: Optional[str] = GOOD_REGIME) -> Path:
    """A code tree whose two prerequisite files hold the given module source (None: the file is absent)."""
    root.mkdir(parents=True, exist_ok=True)
    for rel, source in ((POLARITY.path, oracle), (HYSTERESIS.path, regime)):
        if source is not None:
            (root / rel).parent.mkdir(parents=True, exist_ok=True)
            (root / rel).write_text(source)
    return root


class FakeHost:
    """systemctl, /proc and git for one host. Mutate the tables between a dry run and an apply."""

    def __init__(self, tmp_path: Path) -> None:
        releases = tmp_path / "grid_release.releases"
        self.current = write_tree(releases / CURRENT_SHA).resolve()
        self.pinned = write_tree(releases / PINNED_SHA).resolve()
        self.releases = releases.resolve()
        self.live_link = tmp_path / "grid_release"  # what deploy_release_swap.sh repoints
        self.live_link.symlink_to(self.current)
        self.units: dict[str, Any] = {}
        self.cwds: dict[int, Any] = {}
        self.starts: dict[int, Any] = {}
        self.git_overrides: dict[str, Any] = {}
        self.host, self.boot, self.now = "grid-svr", "boot-1", DRY_RUN_AT
        for pid, unit in enumerate(elig.SERVICE_UNITS, start=2001):
            if unit in PINNED_UNITS:
                self.run(unit, pid, self.pinned, self.pinned)
            else:
                self.run(unit, pid, self.live_link, self.current)

    def run(self, unit: str, pid: int, working_directory: Path, cwd: Path, start: str | None = None) -> None:
        """``unit`` active as ``pid``, configured with ``working_directory``, running from ``cwd``."""
        self.units[unit] = {"LoadState": "loaded", "ActiveState": "active", "SubState": "running",
                            "MainPID": str(pid), "WorkingDirectory": str(working_directory),
                            "NeedDaemonReload": "no"}
        self.cwds[pid] = str(cwd)
        self.starts[pid] = start or f"{pid}00"

    def stop(self, unit: str) -> None:
        self.units[unit].update(ActiveState="inactive", SubState="dead", MainPID="0")

    # probes

    def _show(self, unit: str):
        value = self.units.get(unit, {"LoadState": "not-found"})
        if isinstance(value, Exception):
            raise value
        return dict(value)

    def _readlink(self, pid: int) -> str:
        value = self.cwds.get(pid, FileNotFoundError(2, "No such file or directory"))
        if isinstance(value, Exception):
            raise value
        return value

    def _start(self, pid: int) -> str:
        value = self.starts.get(pid, FileNotFoundError(2, "No such file or directory"))
        if isinstance(value, Exception):
            raise value
        if isinstance(value, list):  # successive reads
            return value.pop(0)
        return value

    def _git(self, tree: Path) -> dict[str, Any]:
        override = self.git_overrides.get(str(tree))
        if isinstance(override, Exception):
            raise override
        name = tree.name if tree.parent == self.releases else hashlib.sha1(str(tree).encode()).hexdigest()  # nosec B324
        identity = {"toplevel": str(tree), "head": name, "head_tree": hashlib.sha1(name.encode()).hexdigest(),  # nosec B324
                    "clean": True}
        identity.update(override or {})
        return identity

    def probes(self) -> elig.Probes:
        return elig.Probes(show=self._show, readlink=self._readlink, pid_start=self._start, git=self._git,
                           host=lambda: self.host, boot_id=lambda: self.boot, now=lambda: self.now)
