"""Regression test for the pm-backtest router's DB-engine dependency wiring.

`api/routers/prediction_backtest.py` must depend on the clearable
``get_db_engine`` wrapper (``api/dependencies.py``) rather than the raw
``db.get_engine`` re-export. Importing the raw name bypasses the
singleton-clearing contract documented in ``.claude/rules/security.md`` and
relied on by ``clear_singletons()``.

This is a static source check so it runs without FastAPI / DB deps installed.
"""

from __future__ import annotations

import re
from pathlib import Path

ROUTER = Path(__file__).resolve().parents[1] / "api" / "routers" / "prediction_backtest.py"


def _source() -> str:
    return ROUTER.read_text(encoding="utf-8")


def test_imports_clearable_get_db_engine() -> None:
    src = _source()
    assert "from api.dependencies import get_db_engine" in src


def test_does_not_import_raw_get_engine() -> None:
    src = _source()
    # `get_engine` as a standalone token (not the `get_db_engine` wrapper).
    bare = re.findall(r"(?<!db_)\bget_engine\b", src)
    assert not bare, f"raw get_engine still referenced {len(bare)}x; use get_db_engine"


def test_all_route_depends_use_get_db_engine() -> None:
    src = _source()
    assert "Depends(get_db_engine)" in src
    assert "Depends(get_engine)" not in src
