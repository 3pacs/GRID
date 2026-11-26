"""parallel_postmortem_runner must not let skipped predictions exhaust its row budget (#959, Codex hold on #1486)."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import scripts.parallel_postmortem_runner as runner


class _Engine:
    def __init__(self, pred_ids, seen_sql):
        self._pred_ids = pred_ids
        self._seen = seen_sql

    def connect(self):
        outer = self

        class _Conn:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, stmt, params=None):
                sql = " ".join(str(stmt).split())
                outer._seen.append(sql)
                res = MagicMock()
                res.fetchall.return_value = [(i,) for i in outer._pred_ids] if "oracle_predictions" in sql else []
                return res

        return _Conn()


def _run_worker(monkeypatch, pred_ids, results, max_rows):
    seen_sql: list[str] = []
    called: list[str] = []
    monkeypatch.setenv("GRID_POSTMORTEM_PARTITIONS", "1")
    monkeypatch.setenv("GRID_POSTMORTEM_PARTITION_IDX", "0")
    monkeypatch.setenv("GRID_POSTMORTEM_MAX_ROWS", str(max_rows))
    monkeypatch.setattr(runner, "get_engine", lambda: _Engine(pred_ids, seen_sql))

    import intelligence.postmortem as pm_mod

    def _gen(_engine, pid):
        called.append(pid)
        return results[pid]

    monkeypatch.setattr(pm_mod, "generate_prediction_postmortem", _gen)
    monkeypatch.setattr(pm_mod, "generate_postmortem", lambda *_a, **_k: None)
    assert runner._worker_main() == 0
    return called, seen_sql


def test_candidate_query_excludes_predictions_with_no_recorded_return(monkeypatch):
    _, seen_sql = _run_worker(monkeypatch, [], {}, max_rows=5)
    pred_sql = [q for q in seen_sql if "FROM" in q and "oracle_predictions" in q and "verdict = 'miss'" in q]
    assert pred_sql and all("pnl_pct IS NOT NULL" in q for q in pred_sql)
    assert all("pnl_pct::float8 NOT IN ('NaN'::float8, 'Infinity'::float8, '-Infinity'::float8)" in q for q in pred_sql)


def test_a_skipped_prediction_does_not_spend_the_row_budget(monkeypatch):
    # Codex's reproduction: with a one-row budget a newest candidate that writes nothing
    # (None) must not stop the following valid candidate from being tried.
    called, _ = _run_worker(monkeypatch, ["skipped", "valid"], {"skipped": None, "valid": object()}, max_rows=1)
    assert called == ["skipped", "valid"]


def test_written_postmortems_still_consume_the_budget(monkeypatch):
    called, _ = _run_worker(monkeypatch, ["a", "b", "c"], {k: object() for k in "abc"}, max_rows=2)
    assert called == ["a", "b"]


# ── trade side (#1488, same hold applied to the trade candidates) ───────────────────

def _run_trade_worker(monkeypatch, trade_ids, results, max_rows):
    seen_sql: list[str] = []
    called: list[int] = []

    class _TradeEngine(_Engine):
        def connect(self):
            outer = self

            class _Conn:
                def __enter__(self):
                    return self

                def __exit__(self, *a):
                    return False

                def execute(self, stmt, params=None):
                    sql = " ".join(str(stmt).split())
                    seen_sql.append(sql)
                    res = MagicMock()
                    res.fetchall.return_value = (
                        [(i,) for i in trade_ids] if "FROM options_recommendations" in sql else []
                    )
                    return res

            return _Conn()

    monkeypatch.setenv("GRID_POSTMORTEM_PARTITIONS", "1")
    monkeypatch.setenv("GRID_POSTMORTEM_PARTITION_IDX", "0")
    monkeypatch.setenv("GRID_POSTMORTEM_MAX_ROWS", str(max_rows))
    monkeypatch.setattr(runner, "get_engine", lambda: _TradeEngine([], seen_sql))

    import intelligence.postmortem as pm_mod

    def _gen(_engine, tid):
        called.append(tid)
        return results[tid]

    monkeypatch.setattr(pm_mod, "generate_postmortem", _gen)
    monkeypatch.setattr(pm_mod, "generate_prediction_postmortem", lambda *_a, **_k: None)
    assert runner._worker_main() == 0
    return called, seen_sql


def test_trade_candidate_query_excludes_trades_with_no_recorded_return(monkeypatch):
    _, seen_sql = _run_trade_worker(monkeypatch, [], {}, max_rows=5)
    trade_sql = [q for q in seen_sql if "FROM options_recommendations" in q and "'LOSS', 'EXPIRED'" in q]
    assert trade_sql and all("actual_return IS NOT NULL" in q for q in trade_sql)


def test_a_skipped_trade_does_not_spend_the_row_budget(monkeypatch):
    called, _ = _run_trade_worker(monkeypatch, [1, 2], {1: None, 2: object()}, max_rows=1)
    assert called == [1, 2]
