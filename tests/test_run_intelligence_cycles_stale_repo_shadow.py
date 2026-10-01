"""Regression: a stale legacy checkout on ``sys.path`` must never shadow
this repo's real first-party packages when ``scripts/run_intelligence_cycles.py``
is imported (not run as ``__main__``) -- and the sys.path fallback it does
carry for direct execution must actually be able to run *before* any
first-party import that could need it, not after.

Mirrors tests/test_score_oracle_trades_stale_repo_shadow.py, written for the
identical defect in scripts/score_oracle_trades.py. That module used to do
``sys.path.insert(0, "/data/grid_v4/grid_repo")`` unconditionally, at import
time -- a compute-node checkout path. On grid-svr that directory is a real,
stale checkout whose own first-party packages can disagree with this repo's.
The insert ran on every import, on every host where that directory exists,
for the rest of the process's lifetime: any first-party import that happened
to run *after* it -- ``from config import settings`` at this module's own
top level, or any of its lazy ``from intelligence.* import ...`` calls
inside ``step_thesis``/``step_trust``/``step_forensics``/``step_patterns``,
triggered much later during an actual cycle run -- could resolve from the
stale tree instead.

Fix, in two parts:

1. The fallback guard (``__name__ == "__main__" and
   find_spec("config") is None``) now runs BEFORE any first-party import in
   this module, not after. A guard placed after ``from config import
   settings`` can never fire: if config were not already importable, that
   import would already have raised before the guard was ever reached.
   ``test_guard_precedes_every_first_party_import_in_source`` below is the
   static regression proof for that ordering bug specifically -- it checks
   source order rather than executing the module as ``__main__`` (which
   would run this script's real, DB-touching cycle logic with no
   ``--help``-style short-circuit, unlike ``score_oracle_trades.py``).
2. The guard still never fires on a mere import (only under direct
   execution), so importing this module can never mutate sys.path.

This file exercises the real, unmodified source. It never touches the real
``/data/grid_v4/grid_repo`` path on disk -- a simulated stale checkout is
built under ``tmp_path`` instead, containing decoys for every first-party
package this module or its lazy imports could reach: ``config.py`` and an
``intelligence`` package whose submodules raise if imported.
"""

from __future__ import annotations

import importlib
import re
import sys
from pathlib import Path

import pytest

_LEGACY_CHECKOUT_PATH = "/data/grid_v4/grid_repo"

# Every first-party module this regression cares about: the one this module
# imports directly at module level (config), plus the four it imports
# lazily, inside step_thesis/step_trust/step_forensics/step_patterns.
_FIRST_PARTY_MODULES = (
    "config",
    "intelligence.thesis_tracker",
    "intelligence.trust_scorer",
    "intelligence.forensics",
    "intelligence.event_sequence",
)


def _build_stale_checkout(tmp_path) -> "Path":  # noqa: F821 - typing only
    """A stale checkout shaped like the one confirmed on grid-svr: decoy
    config.py plus a decoy intelligence package whose submodules raise if
    ever actually imported -- none of which match this repo's real
    modules."""
    stale_root = tmp_path / "stale_grid_repo"
    stale_root.mkdir(parents=True)

    (stale_root / "config.py").write_text(
        "raise ImportError('stale decoy config.py must never be imported')\n",
        encoding="utf-8",
    )
    stale_intel_pkg = stale_root / "intelligence"
    stale_intel_pkg.mkdir()
    (stale_intel_pkg / "__init__.py").write_text("", encoding="utf-8")
    for submodule in ("thesis_tracker", "trust_scorer", "forensics", "event_sequence"):
        (stale_intel_pkg / f"{submodule}.py").write_text(
            "raise ImportError("
            f"'stale decoy intelligence/{submodule}.py must never be imported')\n",
            encoding="utf-8",
        )
    return stale_root


def _first_party_module_names() -> set[str]:
    names: set[str] = set()
    for mod in _FIRST_PARTY_MODULES:
        parts = mod.split(".")
        for i in range(1, len(parts) + 1):
            names.add(".".join(parts[:i]))
    names.add("scripts.run_intelligence_cycles")
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
    IMPORTS scripts.run_intelligence_cycles (never runs it as __main__) must
    never see /data/grid_v4/grid_repo land on sys.path at all.
    """
    stale_root = _build_stale_checkout(tmp_path)
    monkeypatch.setattr(
        sys, "path",
        _RedirectingPathList(
            sys.path, target=_LEGACY_CHECKOUT_PATH, redirect_to=str(stale_root),
        ),
    )

    _fresh_import(*_first_party_module_names())
    module = importlib.import_module("scripts.run_intelligence_cycles")

    assert module.__name__ != "__main__"
    assert _LEGACY_CHECKOUT_PATH not in sys.path
    assert str(stale_root) not in sys.path

    # This module's own top-level first-party import is the real one.
    assert module.settings is not None
    assert module.engine is not None


def test_every_lazy_first_party_module_resolves_under_the_real_repo(
    tmp_path, monkeypatch,
):
    """Beyond config (imported at module level): the four intelligence.*
    submodules this module imports lazily, inside step_thesis/step_trust/
    step_forensics/step_patterns, must all resolve to the real repo -- not
    the stale decoys -- whether or not anything has actually called those
    step_* functions yet.
    """
    stale_root = _build_stale_checkout(tmp_path)
    monkeypatch.setattr(
        sys, "path",
        _RedirectingPathList(
            sys.path, target=_LEGACY_CHECKOUT_PATH, redirect_to=str(stale_root),
        ),
    )

    _fresh_import(*_first_party_module_names())
    importlib.import_module("scripts.run_intelligence_cycles")

    import config
    import intelligence.thesis_tracker
    import intelligence.trust_scorer
    import intelligence.forensics
    import intelligence.event_sequence

    for mod in (
        config,
        intelligence.thesis_tracker,
        intelligence.trust_scorer,
        intelligence.forensics,
        intelligence.event_sequence,
    ):
        assert mod.__file__ is not None
        assert str(stale_root) not in mod.__file__, (
            f"{mod.__name__} resolved from the stale checkout: {mod.__file__}"
        )


def test_guard_precedes_every_first_party_import_in_source():
    """Ordering regression, checked statically rather than by executing the
    module as __main__ (this script has no --help-style short-circuit --
    running it for real would attempt its actual, DB-touching cycle logic).

    A guard placed AFTER ``from config import settings`` can never fire: if
    config were not already importable, that import would already have
    raised before the guard was reached, so the fallback could never
    actually rescue it. This asserts the guard's `sys.path.insert(...)`
    line appears, in source order, before every top-level first-party
    import statement in the file.
    """
    path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "run_intelligence_cycles.py"
    )
    source = path.read_text(encoding="utf-8")

    insert_match = re.search(
        r'sys\.path\.insert\(0,\s*["\']' + re.escape(_LEGACY_CHECKOUT_PATH) + r'["\']\)',
        source,
    )
    assert insert_match is not None, (
        "Expected the legacy-checkout sys.path.insert fallback to still be "
        "present (guarded) in run_intelligence_cycles.py; if it was removed "
        "entirely, this test (and its guard-ordering concern) no longer "
        "applies and should be deleted instead."
    )
    insert_pos = insert_match.start()

    # Every top-level (module-scope) first-party import in this file.
    first_party_import_pattern = re.compile(
        r'^from config import|^from intelligence\.\w+ import', re.MULTILINE,
    )
    first_party_imports = list(first_party_import_pattern.finditer(source))

    # This module only imports `config` at module scope; the intelligence.*
    # imports are all lazy (inside step_* functions, i.e. indented -- the
    # `^` anchor with MULTILINE only matches column 0, so indented lazy
    # imports inside functions are correctly excluded here).
    assert first_party_imports, "Expected at least a top-level `from config import ...`"

    for m in first_party_imports:
        assert insert_pos < m.start(), (
            f"The sys.path fallback at offset {insert_pos} must appear "
            f"BEFORE the first-party import {m.group(0)!r} at offset "
            f"{m.start()} -- a guard placed after a first-party import can "
            "never fire, since that import would already have raised."
        )


def test_running_as_main_skips_the_fallback_when_repo_already_importable(
    tmp_path, monkeypatch,
):
    """Under direct execution (__name__ == "__main__"), the fallback insert
    is still skipped when this repo is already importable -- the second
    guard, `importlib.util.find_spec("config") is None`.

    This only exercises the guard's own two-line `if` statement directly
    (via exec in a throwaway namespace with `__name__` forced to
    "__main__"), rather than running the whole script as __main__ --
    unlike score_oracle_trades.py, this script has no `--help` short-circuit
    and would otherwise attempt its real, DB-touching cycle logic.
    """
    stale_root = _build_stale_checkout(tmp_path)
    monkeypatch.setattr(
        sys, "path",
        _RedirectingPathList(
            sys.path, target=_LEGACY_CHECKOUT_PATH, redirect_to=str(stale_root),
        ),
    )

    path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "run_intelligence_cycles.py"
    )
    source = path.read_text(encoding="utf-8")
    insert_match = re.search(
        r'if __name__ == "__main__" and importlib\.util\.find_spec\("config"\) is None:\n'
        r'    sys\.path\.insert\(0, "/data/grid_v4/grid_repo"\)\n',
        source,
    )
    assert insert_match is not None, (
        "Expected to find the exact two-line guarded fallback block; if its "
        "exact text changed, update this test's pattern."
    )

    namespace = {"__name__": "__main__", "sys": sys, "importlib": importlib}
    exec(compile(insert_match.group(0), "<guard-block>", "exec"), namespace)

    # config is already importable via the real repo root the whole time
    # (this test process's own sys.path), so the fallback must not fire even
    # though __name__ == "__main__".
    assert _LEGACY_CHECKOUT_PATH not in sys.path
    assert str(stale_root) not in sys.path
