"""Tests for pocket-lining conflict detection."""

from __future__ import annotations

from datetime import date
from typing import Any

from intelligence.pocket_lining import assess_pocket_lining


class _Rows:
    def __init__(self, rows: list[tuple[Any, ...]] | None = None) -> None:
        self._rows = rows or []

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows

    def fetchone(self) -> None:
        return None


class _Conn:
    def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> _Rows:
        sql = getattr(stmt, "text", None) or str(stmt)
        if "source_type = 'congressional'" in sql:
            return _Rows([
                ("Patrick McHenry", "JPM", "BUY", date(2026, 6, 15), None),
            ])
        return _Rows()


class _Context:
    def __init__(self, conn: _Conn) -> None:
        self._conn = conn

    def __enter__(self) -> _Conn:
        return self._conn

    def __exit__(self, *args: object) -> bool:
        return False


class _Engine:
    def __init__(self) -> None:
        self._conn = _Conn()

    def begin(self) -> _Context:
        return _Context(self._conn)

    def connect(self) -> _Context:
        return _Context(self._conn)


def test_assess_pocket_lining_flags_politician_committee_overlap():
    flags = assess_pocket_lining(_Engine())

    assert flags == [{
        "detection": "committee_jurisdiction_trade",
        "who": "Patrick McHenry",
        "what": "BUY JPM on 2026-06-15",
        "who_benefits": "Patrick McHenry",
        "overlap": "Committees: financial services; Sector: XLF",
        "confidence": "likely",
        "implication": (
            "Patrick McHenry traded JPM (XLF sector) while serving on committee "
            "with jurisdiction over that sector"
        ),
        "severity": "high",
    }]
