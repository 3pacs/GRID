"""Regression test: pm-backtest router must not build SQL from f-strings.

`api/routers/prediction_backtest.py` used to interpolate a table name into a
raw SQL string via ``text(f"SELECT COUNT(*) FROM {table}")``. Even though the
value came from a hardcoded list, the pattern violates
``.claude/rules/security.md`` ("NEVER use f-strings ... for SQL queries") and
is a foot-gun the moment someone widens the loop input.

This is a static source check so it runs without FastAPI / DB deps installed.
"""

from __future__ import annotations

import re
from pathlib import Path

ROUTER = Path(__file__).resolve().parents[1] / "api" / "routers" / "prediction_backtest.py"


def _source() -> str:
    return ROUTER.read_text(encoding="utf-8")


def test_no_fstring_or_format_inside_text_call() -> None:
    """Fail if any `text(...)` call receives an f-string or a `.format(...)`."""
    src = _source()
    # text(f"..."), text(f'...'), text( f"..."), etc.
    fstring_hits = re.findall(r"\btext\s*\(\s*f[\"']", src)
    assert not fstring_hits, (
        f"text() called with an f-string ({len(fstring_hits)}x). Use a static "
        "SQL string with bound params instead."
    )
    format_hits = re.findall(r"\btext\s*\([^)]*\.format\s*\(", src)
    assert not format_hits, (
        f"text() called with str.format ({len(format_hits)}x). Use bound params."
    )


def test_no_percent_style_string_sql() -> None:
    """Fail if any raw `%s`/`%(name)s` SQL is built via `%` formatting."""
    src = _source()
    percent_hits = re.findall(r"[\"'][^\"']*SELECT[^\"']*[\"']\s*%\s*[\(\w]", src)
    assert not percent_hits, (
        f"Percent-style SQL formatting found ({len(percent_hits)}x). Use bound params."
    )
