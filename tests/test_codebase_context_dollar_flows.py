"""intelligence.codebase_context dollar-flows summary for LLM context.

amount_usd is stored positive with the sign in ``direction``; the old summary
split on the sign of amount_usd, so every outflow read as an inflow. 13F is
non-directional (owner decision 2026-11-23) and must never count as a flow.
"""
from __future__ import annotations

from types import SimpleNamespace

from intelligence import codebase_context as cc


class _Conn:
    def __init__(self, row):
        self.row = row
        self.sql: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, stmt, params=None):
        self.sql.append(str(stmt))
        return SimpleNamespace(fetchone=lambda: self.row)


def _summary(monkeypatch, row):
    conn = _Conn(row)
    monkeypatch.setattr(cc, "_get_engine", lambda: SimpleNamespace(connect=lambda: conn))
    return cc._get_dollar_flows_summary(), conn.sql[0]


def test_summary_totals_read_direction_and_exclude_13f(monkeypatch):
    text, sql = _summary(monkeypatch, (3, 5_000_000.0, 2_000_000.0))
    assert "direction = 'inflow'" in sql and "direction = 'outflow'" in sql
    assert "amount_usd > 0" not in sql and "amount_usd < 0" not in sql
    assert "source_type <> '13f'" in sql
    assert text == "DOLLAR FLOWS (7d): 3 transactions, $5,000,000 in / $2,000,000 out = $3,000,000 net inflow"


def test_summary_net_outflow_and_empty(monkeypatch):
    text, _ = _summary(monkeypatch, (2, 1_000_000.0, 4_000_000.0))
    assert text.endswith("= $3,000,000 net outflow")
    empty, _ = _summary(monkeypatch, (0, None, None))
    assert empty == ""
