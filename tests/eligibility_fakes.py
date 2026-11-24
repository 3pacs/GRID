"""A fake grid-svr for scripts/apply_eligibility_change.py's deployed-code evidence (#1301, #1383).

``FakeHost`` stands in for systemctl, ``/proc``, git and the deploy lock. Its default layout mirrors grid-svr: a
release root ``grid_release.releases`` holding the current release (grid-api, grid-hermes, grid-coordinator,
grid-worker and grid-intelligence run it through the ``grid_release`` symlink) and an older release that
grid-scheduler and grid-realtime are pinned to. Each unit is launched as on grid-svr (2026-11-24: the venv's
python with ``-m <module>``, ``-m uvicorn <module>:app`` or ``scripts/<file>.py``), and every tree holds those
entry files. Every tree carries the #1307 literal and contains #1350's merge commit unless a test says otherwise,
each release's git HEAD is its directory name, and its code is dated before every process started.
"""

from __future__ import annotations

import hashlib
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

from scripts import apply_eligibility_change as elig

(HYSTERESIS,) = elig.PREREQUISITES
(POLARITY,) = elig.ANCESTOR_PREREQUISITES
GOOD_REGIME = f'{HYSTERESIS.constant}: str = "{HYSTERESIS.expected}"\n'

CURRENT_SHA = "a" * 40
PINNED_SHA = "b" * 40
NEXT_SHA = "c" * 40
PINNED_UNITS = ("grid-scheduler.service", "grid-realtime.service")
DRY_RUN_AT = datetime(2026, 11, 22, 8, 0, 0, tzinfo=timezone.utc)
BOOTED_AT = datetime(2026, 11, 1, tzinfo=timezone.utc)   # a process's start is BOOTED_AT + ticks / 100 s
CODE_AT = BOOTED_AT - timedelta(hours=1)                 # every tree's code predates every process

PY = "/data/grid_v4/grid_venv-314/bin/python"
LAUNCH = {
    "grid-api.service": ("-m", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"),
    "grid-hermes.service": ("scripts/hermes_operator.py",),
    "grid-scheduler.service": ("-m", "ingestion.scheduler"),
    "grid-coordinator.service": ("-m", "uvicorn", "scripts.compute_coordinator:app", "--host", "0.0.0.0",
                                 "--port", "8100"),
    "grid-worker.service": ("scripts/worker.py", "--coordinator", "http://localhost:8100", "--max-concurrent", "1"),
    "grid-intelligence.service": ("-m", "intelligence.scheduler"),
    "grid-realtime.service": ("-m", "ingestion.realtime.ws_listener"),
}
ENTRY_FILES = ("api/main.py", "scripts/hermes_operator.py", "ingestion/scheduler.py", "scripts/compute_coordinator.py",
               "scripts/worker.py", "intelligence/scheduler.py", "ingestion/realtime/ws_listener.py")


def write_tree(root: Path, regime: Optional[str] = GOOD_REGIME) -> Path:
    """A code tree with every unit's entry file and the #1307 file holding ``regime`` (None: the file is absent)."""
    root.mkdir(parents=True, exist_ok=True)
    for rel in ENTRY_FILES:
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        (root / rel).write_text('"""entry."""\n')
    if regime is not None:
        (root / HYSTERESIS.path).parent.mkdir(parents=True, exist_ok=True)
        (root / HYSTERESIS.path).write_text(regime)
    return root


def exec_start(argv: tuple[str, ...] | list[str]) -> str:
    """``ExecStart`` as ``systemctl show`` prints it."""
    return (f"{{ path={argv[0]} ; argv[]={' '.join(argv)} ; ignore_errors=no ; start_time=[n/a] ; "
            "stop_time=[n/a] ; pid=0 ; code=(null) ; status=0/0 }")


class FakeHost:
    """systemctl, /proc, git and the deploy lock for one host. Mutate the tables between a dry run and an apply."""

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
        self.argvs: dict[int, Any] = {}
        self.envs: dict[int, Any] = {}
        self.git_overrides: dict[str, Any] = {}
        self.code_at: dict[str, Any] = {}        # tree -> datetime or Exception (default CODE_AT)
        self.ancestors: dict[str, Any] = {}      # tree -> bool or Exception (default True)
        self.busy_locks: set[str] = set()        # deploy locks a deploy holds
        self.lock_events: list[tuple[str, str]] = []
        self.on_lock: Optional[Any] = None       # called while a deploy lock is taken (tests inject a change)
        self.host, self.boot, self.now = "grid-svr", "boot-1", DRY_RUN_AT
        for pid, unit in enumerate(elig.SERVICE_UNITS, start=2001):
            if unit in PINNED_UNITS:
                self.run(unit, pid, self.pinned, self.pinned)
            else:
                self.run(unit, pid, self.live_link, self.current)

    @property
    def deploy_lock_path(self) -> str:
        return str(self.releases / ".lock")

    def run(self, unit: str, pid: int, working_directory: Path, cwd: Path, start: str | None = None,
            argv: Optional[list[str]] = None) -> None:
        """``unit`` active as ``pid``, configured with ``working_directory``, running from ``cwd``."""
        configured = [PY, *LAUNCH[unit]]
        self.units[unit] = {"LoadState": "loaded", "ActiveState": "active", "SubState": "running",
                            "MainPID": str(pid), "WorkingDirectory": str(working_directory),
                            "NeedDaemonReload": "no", "ExecStart": exec_start(configured)}
        self.cwds[pid] = str(cwd)
        self.starts[pid] = start or f"{pid}00"
        self.argvs[pid] = list(argv or configured)
        self.envs[pid] = {}

    def stop(self, unit: str) -> None:
        self.units[unit].update(ActiveState="inactive", SubState="dead", MainPID="0")

    # probes

    def _show(self, unit: str):
        value = self.units.get(unit, {"LoadState": "not-found"})
        if isinstance(value, Exception):
            raise value
        return dict(value)

    @staticmethod
    def _value(table: dict, key: Any, default: Any = None):
        value = table.get(key, default if default is not None else FileNotFoundError(2, "No such file or directory"))
        if isinstance(value, Exception):
            raise value
        return value

    def _readlink(self, pid: int) -> str:
        return self._value(self.cwds, pid)

    def _start(self, pid: int) -> str:
        value = self._value(self.starts, pid)
        if isinstance(value, list):  # successive reads
            return value.pop(0)
        return value

    def _cmdline(self, pid: int) -> list[str]:
        return list(self._value(self.argvs, pid))

    def _env(self, pid: int) -> dict[str, str]:
        return dict(self._value(self.envs, pid))

    @staticmethod
    def _clock(ticks: str) -> datetime:
        return BOOTED_AT + timedelta(seconds=int(ticks) / 100)

    def _code_changed_at(self, tree: Path) -> datetime:
        return self._value(self.code_at, str(tree), CODE_AT)

    def _is_ancestor(self, tree: Path, commit: str) -> bool:
        assert commit == POLARITY.commit
        return self._value(self.ancestors, str(tree), True)

    @contextmanager
    def _deploy_lock(self, path: Path) -> Iterator[None]:
        if str(path) in self.busy_locks:
            raise BlockingIOError(11, "Resource temporarily unavailable")
        self.lock_events.append(("lock", str(path)))
        if self.on_lock is not None:
            self.on_lock()
        try:
            yield
        finally:
            self.lock_events.append(("unlock", str(path)))

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
                           host=lambda: self.host, boot_id=lambda: self.boot, now=lambda: self.now,
                           cmdline=self._cmdline, python_env=self._env, clock=self._clock,
                           code_changed_at=self._code_changed_at, is_ancestor=self._is_ancestor,
                           deploy_lock=self._deploy_lock)
