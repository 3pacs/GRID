"""The factory's price features are shared with the live recorder, so their text is pinned (3pacs/GRID#1160).

Design Q13 of the module factory: research and live call one function per
feature. gex-signals (3pacs/gex, `gex-signals/factory_features.py`) carries
`_minutes`, `_hm`, `robust_z`, `Session` and `raw_features` copied byte for
byte from `analysis/module_factory.py`, with the sha256 of that text pinned on
its side.

This test pins the same hash here. If you change one of those functions, this
fails: copy the new text into gex-signals, change the hash in both repos in the
same change, and treat any live slot registered on the old text as a
registration of a different feature.
"""

from __future__ import annotations

import ast
import hashlib
from pathlib import Path

from analysis import module_factory as mf

SHARED = ("_minutes", "_hm", "robust_z", "Session", "raw_features")
SHARED_SHA256 = "3dc1721b2f681b0711c931b0c91f074117a131442cbe9d1071add873b13473bd"


def _shared_text() -> str:
    source = Path(mf.__file__).read_text(encoding="utf-8")
    lines = source.splitlines(keepends=True)
    parts = {
        node.name: "".join(lines[node.lineno - 1: node.end_lineno])
        for node in ast.parse(source).body
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in SHARED
    }
    assert set(parts) == set(SHARED), sorted(set(SHARED) - set(parts))
    return "\n\n".join(parts[name] for name in SHARED)


def test_the_shared_feature_text_is_the_text_the_live_recorder_carries():
    assert hashlib.sha256(_shared_text().encode()).hexdigest() == SHARED_SHA256, (
        "a shared feature function changed: update gex-signals/factory_features.py "
        "(3pacs/gex) and this hash together"
    )


def test_the_shared_functions_need_only_the_standard_library():
    """gex-signals runs on python3.10 -I -S -B: no third-party import may enter this text."""
    names = {
        node.id
        for node in ast.walk(ast.parse(_shared_text()))
        if isinstance(node, ast.Name)
    }
    assert not names & {"pd", "np", "numpy", "pandas"}
    assert {"bisect", "statistics"} >= names & {"bisect", "statistics", "math", "json", "sqlite3"}


def test_the_features_listed_for_live_are_the_ones_the_function_returns():
    day = mf.Session({f"{m // 60:02d}:{m % 60:02d}": (1.0, 1.1, 0.9, 1.0, 1) for m in range(570, 960)})
    assert tuple(mf.raw_features(day, day, "10:29")) == mf.FEATURES == ("F1", "F10", "F11", "F1_shift30")
    assert mf.Z_CAP == 4.0
