"""Fail-closed identity of SEC heads and qualified TTM publication state.

The global identity intentionally invalidates peer-relative financial reports
when any contributing issuer changes. It is not a version of market prices or
nonfinancial source tables. No engine/default database discovery occurs here.
"""
from __future__ import annotations

import re
from typing import Any

from sqlalchemy import text

_GENERATION_SQL = text("SELECT generation FROM sec_financial_generation")


def get_financial_generation(conn: Any) -> str:
    """Return a verified current financial generation; never invent a fallback."""
    value = conn.execute(_GENERATION_SQL).scalar_one()
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{32}", value) is None:
        raise RuntimeError("Financial source generation is unavailable")
    return value
