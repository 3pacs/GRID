"""Regression: a stale legacy checkout on ``sys.path`` must never shadow
this repo's real first-party packages when ``scripts/score_oracle_trades.py``
is imported (not run as ``__main__``).

That module used to do ``sys.path.insert(0, "/data/grid_v4/grid_repo")``
unconditionally, at import time -- a compute-node checkout path. On
grid-svr that directory is a real, stale checkout (confirmed at revision
``5facbdf0``) whose own ``oracle`` package predates
``oracle/entry_price_policy.py`` entirely, and it has no ``config.py`` or
``intelligence/postmortem.py`` matching this repo either -- Hermes runs
from ``/data/grid_v4/grid_release``. The insert ran on every import, on
every host where that directory exists, for the rest of the process's
lifetime: any first-party import that happened to run *after* it --
``from config import settings`` at this module's own top level, or a lazy
``from intelligence.postmortem import ...`` triggered much later during
actual scoring -- could resolve from the stale tree instead. Reproduced
twice on a real-PostgreSQL proof host (gridz4, which also has that
directory): ``ModuleNotFoundError: No module named
'oracle.entry_price_policy'`` during pytest collection, and separately a
failure in the real scorer path itself.

Fix, in two parts:

1. Every first-party import this module needs directly (``config``,
   ``oracle.entry_price_policy``) now runs before the insertion.
2. The insertion itself is now conditional on BOTH
   ``__name__ == "__main__"`` (never run merely by importing the module --
   this is what actually closes the hole for good, since anything a lazy
   import like ``intelligence.postmortem`` needs is never at risk if
   ``sys.path`` was never mutated in the first place) AND
   ``importlib.util.find_spec("config") is None`` (even under direct
   execution, prefer whatever already resolves the repo correctly over the
   hardcoded, potentially-stale fallback).

This file exercises the real, unmodified source. It never touches the real
``/data/grid_v4/grid_repo`` path on disk -- a simulated stale checkout is
built under ``tmp_path`` instead, containing decoys for every first-party
package this module or its lazy imports could reach: ``oracle`` (no
``entry_price_policy`` submodule), ``config.py``, ``db.py``, and
``intelligence/postmortem.py``.
"""

from __future__ import annotations

import importlib
import runpy
import sys

import pytest

_LEGACY_CHECKOUT_PATH = "/data/grid_v4/grid_repo"

# Every first-party module this regression cares about: the two this
# module imports directly (oracle.entry_price_policy, config), plus two it
# does not import itself but which must be equally safe (db,
# intelligence.postmortem -- the latter is scripts/score_oracle_trades.py's
# own lazy import inside _record_success_lesson_safe).
_FIRST_PARTY_MODULES = (
    "config",
    "db",
    "oracle.entry_price_policy",
    "intelligence.postmortem",
)


def _build_stale_checkout(tmp_path) -> "Path":  # noqa: F821 - typing only
    """A stale checkout shaped like the one confirmed on grid-svr: an
    `oracle` package with no entry_price_policy submodule, plus decoy
    config.py / db.py / intelligence/postmortem.py -- none of which match
    this repo's real modules (each decoy's own content would raise or
    behave differently if it were ever actually the one imported)."""
    stale_root = tmp_path / "stale_grid_repo"
    stale_oracle_pkg = stale_root / "oracle"
    stale_oracle_pkg.mkdir(parents=True)
    (stale_oracle_pkg / "__init__.py").write_text("", encoding="utf-8")
    # No entry_price_policy.py -- the exact defect confirmed on grid-svr.

    (stale_root / "config.py").write_text(
        "raise ImportError('stale decoy config.py must never be imported')\n",
        encoding="utf-8",
    )
    (stale_root / "db.py").write_text(
        "raise ImportError('stale decoy db.py must never be imported')\n",
        encoding="utf-8",
    )
    stale_intel_pkg = stale_root / "intelligence"
    stale_intel_pkg.mkdir()
    (stale_intel_pkg / "__init__.py").write_text("", encoding="utf-8")
    (stale_intel_pkg / "postmortem.py").write_text(
        "raise ImportError("
        "'stale decoy intelligence/postmortem.py must never be imported')\n",
        encoding="utf-8",
    )
    return stale_root


def _first_party_module_names() -> set[str]:
    names: set[str] = set()
    for mod in _FIRST_PARTY_MODULES:
        parts = mod.split(".")
        for i in range(1, len(parts) + 1):
            names.add(".".join(parts[:i]))
    names.add("scripts.score_oracle_trades")
    return names


@pytest.fixture(autouse=True)
def _restore_modules():
    """Snapshot and restore every module this test forces a fresh import
    of, so a genuinely fresh import (required to reproduce the shadowing
    scenario) never leaves the real process's cached modules replaced."""
    names = _first_party_module_names()
    saved = {name: sys.modules.get(name) for name in names}
    yield
    for name in _first_party_module_names():
        if name not in saved:
            sys.modules.pop(name, None)
    for name, mod in saved.items():
        if mod is not None:
            sys.modules[name] = mod
        else:
            sys.modules.pop(name, None)


def _fresh_import(*names: str) -> None:
    for name in names:
        sys.modules.pop(name, None)


class _RedirectingPathList(list):
    """A ``sys.path``-shaped list that redirects one specific literal
    ``insert`` target to ``redirect_to``, passing every other call through
    to the real ``list.insert`` unchanged."""

    def __init__(self, initial: list[str], *, target: str, redirect_to: str):
        super().__init__(initial)
        self._target = target
        self._redirect_to = redirect_to

    def insert(self, index, path):  # noqa: D102 - list.insert override
        if path == self._target:
            path = self._redirect_to
        super().insert(index, path)


def test_import_as_a_module_never_mutates_sys_path(tmp_path, monkeypatch):
    """The primary guarantee: Hermes, pytest, or anything else that merely
    IMPORTS scripts.score_oracle_trades (never runs it as __main__) must
    never see /data/grid_v4/grid_repo land on sys.path at all -- not just
    "land somewhere harmless". If the guard ever regresses back to an
    unconditional insert, this is the first thing that fails.
    """
    stale_root = _build_stale_checkout(tmp_path)
    monkeypatch.setattr(
        sys, "path",
        _RedirectingPathList(
            sys.path, target=_LEGACY_CHECKOUT_PATH, redirect_to=str(stale_root),
        ),
    )

    _fresh_import(*_first_party_module_names())
    module = importlib.import_module("scripts.score_oracle_trades")

    assert module.__name__ != "__main__"
    assert _LEGACY_CHECKOUT_PATH not in sys.path
    assert str(stale_root) not in sys.path

    # This module's own first-party imports are the real ones.
    assert module.settings is not None
    assert module.SCORE_NOTE_ENTRY_NULL == (
        "No entry price was measured at publish time"
    )
    assert callable(module.entry_price_score_note)
    assert module.entry_price_score_note(None) == module.SCORE_NOTE_ENTRY_NULL
    assert module.entry_price_score_note(150.0) is None


def test_every_first_party_module_resolves_under_the_real_repo(
    tmp_path, monkeypatch,
):
    """Beyond what scripts.score_oracle_trades.py imports itself: config,
    db and intelligence.postmortem (the latter is this module's OWN lazy
    import, triggered only much later during actual scoring, inside
    _record_success_lesson_safe) must all resolve to this repo -- not the
    stale decoys -- whether or not anything ever imports them off the back
    of importing this module.
    """
    stale_root = _build_stale_checkout(tmp_path)
    monkeypatch.setattr(
        sys, "path",
        _RedirectingPathList(
            sys.path, target=_LEGACY_CHECKOUT_PATH, redirect_to=str(stale_root),
        ),
    )

    _fresh_import(*_first_party_module_names())
    importlib.import_module("scripts.score_oracle_trades")

    import config
    import db
    import oracle.entry_price_policy
    import intelligence.postmortem

    for mod in (config, db, oracle.entry_price_policy, intelligence.postmortem):
        assert mod.__file__ is not None
        assert str(stale_root) not in mod.__file__, (
            f"{mod.__name__} resolved from the stale checkout: {mod.__file__}"
        )


def test_running_as_main_skips_the_fallback_when_repo_already_importable(
    tmp_path, monkeypatch,
):
    """Under direct execution (__name__ == "__main__"), the fallback insert
    is still skipped when this repo is already importable -- the second
    guard, `importlib.util.find_spec("config") is None`. Simulated with
    runpy.run_module(..., run_name="__main__"), which executes the module
    exactly as `python scripts/score_oracle_trades.py` would, with
    `__name__` set to "__main__", without actually invoking `main()`
    (guarded by the file's own `if __name__ == "__main__":` at the bottom,
    which runpy also triggers -- so args are pointed at --help via sys.argv
    to make main() exit immediately rather than trying to score anything).
    """
    stale_root = _build_stale_checkout(tmp_path)
    monkeypatch.setattr(
        sys, "path",
        _RedirectingPathList(
            sys.path, target=_LEGACY_CHECKOUT_PATH, redirect_to=str(stale_root),
        ),
    )
    monkeypatch.setattr(sys, "argv", ["score_oracle_trades.py", "--help"])

    _fresh_import(*_first_party_module_names())

    with pytest.raises(SystemExit):
        # argparse's --help exits 0 after printing usage -- this is the
        # cheapest way to run the module as __main__ without it trying to
        # connect to a real database.
        runpy.run_module("scripts.score_oracle_trades", run_name="__main__")

    # config was already importable via the real repo root the whole time
    # (this test process's own sys.path), so the fallback must not have
    # fired even though __name__ == "__main__".
    assert _LEGACY_CHECKOUT_PATH not in sys.path
    assert str(stale_root) not in sys.path
