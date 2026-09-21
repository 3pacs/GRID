"""Regression: a stale legacy checkout on ``sys.path`` must never shadow
this repo's real ``oracle`` package when ``scripts/score_oracle_trades.py``
is imported (not run as ``__main__``).

That module does ``sys.path.insert(0, "/data/grid_v4/grid_repo")`` at
import time -- a compute-node checkout path, pre-existing on ``main`` and
out of scope for this extraction to remove. On grid-svr that directory is a
real, stale checkout (confirmed at revision ``5facbdf0``) whose own
``oracle`` package predates ``oracle/entry_price_policy.py`` entirely --
Hermes runs from ``/data/grid_v4/grid_release``. If that stale path landed
at ``sys.path[0]`` *before* this module's own ``from oracle.entry_price_policy
import ...`` had resolved, the import would resolve ``oracle`` from the
stale tree instead and raise ``ModuleNotFoundError: No module named
'oracle.entry_price_policy'`` -- exactly what a real-PostgreSQL proof on the
gridz4 host reproduced during pytest collection of
``tests/test_oracle_null_readers.py`` (that host also has
``/data/grid_v4/grid_repo``).

The fix: ``scripts/score_oracle_trades.py`` now imports
``oracle.entry_price_policy`` *before* its own ``sys.path.insert`` runs, so
the real package is already cached in ``sys.modules`` by the time that
insert executes -- caching by fully-qualified name is permanent for the
process, so nothing added to ``sys.path`` afterward can shadow it.

This test exercises the real, unmodified source line -- it does not touch
the real ``/data/grid_v4/grid_repo`` path on disk. Instead it redirects
*only* calls to ``sys.path.insert`` carrying that exact literal string to a
harmless ``tmp_path`` directory built to look exactly like the stale
checkout (an ``oracle`` package with no ``entry_price_policy`` submodule);
every other ``sys.path.insert`` call (pytest's own, etc.) passes through
unchanged.
"""

from __future__ import annotations

import importlib
import sys

import pytest

_LEGACY_CHECKOUT_PATH = "/data/grid_v4/grid_repo"


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


def _oracle_module_names() -> set[str]:
    return {
        name for name in sys.modules
        if name == "oracle" or name.startswith("oracle.")
    }


@pytest.fixture(autouse=True)
def _restore_modules():
    """Snapshot and restore every ``oracle*``/``scripts.score_oracle_trades``
    entry in ``sys.modules`` -- this test forces a genuinely fresh import of
    both to reproduce the shadowing scenario, and must not leave the real
    process's cached modules replaced afterward."""
    names = _oracle_module_names() | {"scripts.score_oracle_trades"}
    saved = {name: sys.modules.get(name) for name in names}
    yield
    for name in _oracle_module_names() | {"scripts.score_oracle_trades"}:
        if name not in saved:
            sys.modules.pop(name, None)
    for name, mod in saved.items():
        if mod is not None:
            sys.modules[name] = mod
        else:
            sys.modules.pop(name, None)


def test_stale_legacy_checkout_cannot_shadow_the_real_oracle_package(
    tmp_path, monkeypatch,
):
    """Import scripts.score_oracle_trades fresh while its hardcoded
    sys.path.insert target is redirected to a simulated stale checkout (an
    `oracle` package with no `entry_price_policy` submodule, the exact
    shape confirmed on grid-svr). The real module must still be the one
    used -- both by the freshly-imported script itself and by anyone else
    in the process who imports oracle.entry_price_policy afterward.
    """
    stale_root = tmp_path / "stale_grid_repo"
    stale_oracle_pkg = stale_root / "oracle"
    stale_oracle_pkg.mkdir(parents=True)
    (stale_oracle_pkg / "__init__.py").write_text("", encoding="utf-8")
    # No entry_price_policy.py in the stale package -- this is the exact
    # defect confirmed on grid-svr's stale checkout.

    monkeypatch.setattr(
        sys, "path",
        _RedirectingPathList(
            sys.path, target=_LEGACY_CHECKOUT_PATH, redirect_to=str(stale_root),
        ),
    )

    # Force a genuinely fresh import: nothing oracle-related may already be
    # cached, or the bug's precondition (oracle not yet in sys.modules when
    # the shadowing insert runs) is not reproduced.
    for name in _oracle_module_names() | {"scripts.score_oracle_trades"}:
        sys.modules.pop(name, None)

    module = importlib.import_module("scripts.score_oracle_trades")

    # The module's own oracle.entry_price_policy import succeeded and is
    # the real one -- not shadowed by the stale tree its own sys.path
    # insert (redirected to stale_root by this test) placed at
    # sys.path[0] moments later.
    assert module.SCORE_NOTE_ENTRY_NULL == (
        "No entry price was measured at publish time"
    )
    assert callable(module.entry_price_score_note)
    assert module.entry_price_score_note(None) == module.SCORE_NOTE_ENTRY_NULL
    assert module.entry_price_score_note(150.0) is None

    # And the stale tree's fake `oracle` package never got a chance to
    # shadow the real one for anyone else in the process either: the real
    # module is the one cached in sys.modules.
    import oracle.entry_price_policy as real_module

    assert hasattr(real_module, "SCORE_NOTE_ENTRY_NULL")
    assert real_module.__file__ is not None
    assert str(stale_root) not in real_module.__file__

    # The redirect actually fired -- otherwise this test would pass
    # vacuously (never having exercised the shadowing scenario at all).
    assert str(stale_root) in sys.path
