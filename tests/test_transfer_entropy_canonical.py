"""Regression guard against `transfer_entropy` body drift.

Prior state (pre-2026-06-19): `physics/transforms.py` and
`analysis/transfer_entropy.py` both defined a top-level
`transfer_entropy` with divergent bodies. The `analysis/` version is the
canonical one (only callers: `analysis/lead_lag_backtest.py` +
`tests/test_transfer_entropy.py`); the `physics/` duplicate had zero
callers and silently shadowed the canonical body whenever someone
imported from `physics.transforms` by accident.

This test locks the invariant: `physics.transforms` either has no
`transfer_entropy` symbol at all (current state), or it is an explicit
re-export of `analysis.transfer_entropy.transfer_entropy`. Two distinct
callables under the same name is the forbidden state.
"""

from __future__ import annotations

import importlib

from analysis import transfer_entropy as analysis_te_module


def test_physics_transforms_does_not_redefine_transfer_entropy() -> None:
    physics_transforms = importlib.import_module("physics.transforms")
    physics_attr = getattr(physics_transforms, "transfer_entropy", None)
    canonical = analysis_te_module.transfer_entropy

    if physics_attr is None:
        return  # deletion path — no duplicate possible

    assert physics_attr is canonical, (
        "physics.transforms.transfer_entropy must be a re-export of "
        "analysis.transfer_entropy.transfer_entropy — never an independent "
        "definition. See docs/PUNCH-LIST-2026-05-13.md (Auditor 2026-06-14 "
        "physics/ P1) for the drift the original duplicate caused."
    )


def test_analysis_transfer_entropy_remains_callable() -> None:
    canonical = analysis_te_module.transfer_entropy
    assert callable(canonical)
    result = canonical([1.0, 2.0, 3.0], [4.0, 5.0, 6.0])
    assert isinstance(result, float)
