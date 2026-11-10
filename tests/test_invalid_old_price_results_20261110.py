"""Results built on the mixed-basis ``_full`` price panel stay out of performance figures.

The panel was repaired 2026-11-09/10 (docs/reference/PRICE_PANEL_BASIS.md).
Two records computed from it remain as history but must not be served as
results: the adaptive_rotation_live paper book (the only paper_trades input
to realized alpha) and the 242 rotation_variant_* AstroGrid backtest runs.
"""

from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import MagicMock

import alpha_research.realized_alpha as ra
from store.astrogrid import AstroGridStore


def test_paper_trades_query_excludes_invalid_strategies() -> None:
    seen: list[tuple[str, Any]] = []
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value

    def execute(statement: Any, params: Any = None) -> MagicMock:
        seen.append((str(statement), params))
        res = MagicMock()
        res.fetchall.return_value = []
        return res

    conn.execute.side_effect = execute
    assert ra.collect_trades(engine, "paper_trades", date(2026, 11, 10)) == []
    sql, params = next((s, p) for s, p in seen if "FROM paper_trades" in s)
    assert "strategy_id = ANY(:invalid_strategies)" in sql
    assert "strategy_id IS NULL" in sql  # a trade without a strategy is still counted
    assert "adaptive_rotation_live" in params["invalid_strategies"]
    assert set(params["invalid_strategies"]) == set(ra.INVALID_PAPER_STRATEGIES)
    assert all(reason for reason in ra.INVALID_PAPER_STRATEGIES.values())


def test_backtest_results_skip_runs_marked_invalidated() -> None:
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchall.return_value = []
    AstroGridStore(engine).list_backtest_results(strategy_variant="grid_only")
    statement, params = conn.execute.call_args.args
    sql = str(statement)
    assert "summary_payload -> 'invalidated' IS NOT NULL" in sql
    assert "NOT EXISTS" in sql and "inv.id = br.backtest_run_id" in sql
    # the existing filters still apply
    assert "br.target_symbol IS DISTINCT FROM :legacy_char_symbol" in sql
    assert params["strategy_variant"] == "grid_only"
