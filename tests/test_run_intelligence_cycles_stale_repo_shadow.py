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

import ast
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


def _is_first_party(module_name: str, repo_root) -> bool:
    """True if `module_name`'s top-level package/module exists directly
    under the repo root -- i.e. it is one of THIS repo's own packages
    (``config``, ``intelligence``, ...), not a stdlib or installed
    third-party module (``sys``, ``sqlalchemy``, ``loguru``, ...). Derived
    from the filesystem rather than a hardcoded name list, so a new
    first-party import added anywhere in the file is caught automatically."""
    top = module_name.split(".")[0]
    if (repo_root / f"{top}.py").is_file():
        return True
    pkg_dir = repo_root / top
    return pkg_dir.is_dir() and (pkg_dir / "__init__.py").is_file()


def test_guard_precedes_every_first_party_import_in_source():
    """Ordering regression, checked statically via the ``ast`` module rather
    than by executing the module as __main__ (this script has no
    --help-style short-circuit -- running it for real would attempt its
    actual, DB-touching cycle logic) and rather than a plain text/regex
    search (unsound here: this file's own guard comment literally contains
    the substring "from config import settings", so a naive search could be
    fooled by a comment or docstring mention rather than the real import
    statement).

    A guard placed AFTER ``from config import settings`` can never fire: if
    config were not already importable, that import would already have
    raised before the guard was reached, so the fallback could never
    actually rescue it. This parses the real source and asserts:

    1. The guarded ``if ...: sys.path.insert(0, "/data/grid_v4/grid_repo")``
       block exists at module level, and its condition actually references
       both ``__name__`` and ``find_spec`` (ordering alone doesn't prove the
       guard's *condition* survived a future edit).
    2. Every MODULE-LEVEL (``tree.body``, which excludes anything nested
       inside a function -- i.e. the lazy ``intelligence.*`` imports inside
       step_thesis/step_trust/step_forensics/step_patterns are correctly
       excluded, since they run long after the guard anyway) first-party
       import -- determined by filesystem lookup under the repo root, not a
       hardcoded module list -- appears strictly after that guard.
    """
    repo_root = Path(__file__).resolve().parents[1]
    path = repo_root / "scripts" / "run_intelligence_cycles.py"
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))

    def _is_sys_path_insert_call(node: ast.AST) -> bool:
        return (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "insert"
            and isinstance(node.func.value, ast.Attribute)
            and node.func.value.attr == "path"
            and isinstance(node.func.value.value, ast.Name)
            and node.func.value.value.id == "sys"
            and len(node.args) == 2
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value == _LEGACY_CHECKOUT_PATH
        )

    guard_if = None
    for node in tree.body:
        if isinstance(node, ast.If) and any(
            _is_sys_path_insert_call(n) for n in ast.walk(node)
        ):
            guard_if = node
            break

    assert guard_if is not None, (
        "Expected a module-level `if ...: sys.path.insert(0, "
        f'"{_LEGACY_CHECKOUT_PATH}")` guard in run_intelligence_cycles.py; '
        "if it was removed entirely, this test (and its guard-ordering "
        "concern) no longer applies and should be deleted instead."
    )

    condition_src = ast.unparse(guard_if.test)
    assert "__name__" in condition_src, (
        f"Guard condition must check __name__, got: {condition_src!r}"
    )
    assert "find_spec" in condition_src, (
        "Guard condition must check "
        f"importlib.util.find_spec(...), got: {condition_src!r}"
    )

    violations = []
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module:
            if _is_first_party(node.module, repo_root) and node.lineno <= guard_if.lineno:
                violations.append((node.lineno, ast.unparse(node)))
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if _is_first_party(alias.name, repo_root) and node.lineno <= guard_if.lineno:
                    violations.append((node.lineno, ast.unparse(node)))

    assert violations == [], (
        f"First-party import(s) at/before the sys.path guard "
        f"(guard at line {guard_if.lineno}): {violations} -- a guard placed "
        "after a first-party import can never fire, since that import "
        "would already have raised."
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
