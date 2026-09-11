"""Read-back of persisted sweeps (``load_latest_ranking`` / ``list_rankings``).

The Sunday long-horizon job writes ``universe_ranking_history``; these
helpers are what ``GET /api/v1/conviction/sweeps`` serves so the canvas can
paint verdicts without re-running the decision stack.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

from intelligence import universe_ranker as ur

_TOP_K = [
    {"ticker": "NVDA", "verdict": "high", "composite_score": 1.3, "sector": "tech"},
    {"ticker": "AMD", "verdict": "moderate", "composite_score": 0.8, "sector": "tech"},
]


def _row(id_: int = 7, horizon: int = 90, top_k_json: bool = True) -> tuple:
    return (
        id_,
        datetime(2026, 9, 6, 5, 0, tzinfo=timezone.utc),
        "custom",
        horizon,
        33,
        31,
        "trending",
        json.dumps(_TOP_K) if top_k_json else _TOP_K,
        json.dumps([{"sector": "tech", "count": 2}]),
        json.dumps(["tech concentration 2/2"]),
        "Two names actionable.",
    )


def _engine(first=None, rows=None, total: int = 0) -> tuple[MagicMock, MagicMock]:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)

    def execute(stmt, params=None):
        result = MagicMock()
        sql = str(stmt)
        if "COUNT(*)" in sql:
            result.scalar.return_value = total
        elif "LIMIT 1" in sql:
            result.first.return_value = first
        else:
            result.fetchall.return_value = rows or []
        return result

    conn.execute.side_effect = execute
    engine = MagicMock()
    engine.connect.return_value = conn
    return engine, conn


def test_row_to_dict_parses_json_columns_and_defaults_horizon() -> None:
    out = ur._ranking_row_to_dict(_row())
    assert out["id"] == 7
    assert out["generated_at"].startswith("2026-09-06T05:00:00")
    assert out["horizon_days"] == 90
    assert out["top_k"][0]["ticker"] == "NVDA"
    assert out["sector_distributions"][0]["sector"] == "tech"
    assert out["concentration_alerts"] == ["tech concentration 2/2"]
    # JSONB may already arrive parsed (psycopg2) — accepted as-is.
    assert ur._ranking_row_to_dict(_row(top_k_json=False))["top_k"] == _TOP_K
    # NULL horizon (pre-migration row) → legacy default.
    assert ur._ranking_row_to_dict(_row(horizon=None))["horizon_days"] == ur.DEFAULT_HORIZON_DAYS


def test_load_latest_ranking_filters_and_returns_dict() -> None:
    engine, conn = _engine(first=_row())
    out = ur.load_latest_ranking(engine, horizon_days=90, universe_name="custom")
    assert out is not None and out["regime_signature"] == "trending"
    stmt, params = conn.execute.call_args.args
    assert "ORDER BY generated_at DESC" in str(stmt)
    assert params == {"horizon_days": 90, "universe_name": "custom"}


def test_load_latest_ranking_none_when_empty_or_db_error() -> None:
    engine, _ = _engine(first=None)
    assert ur.load_latest_ranking(engine) is None
    broken = MagicMock()
    broken.connect.side_effect = RuntimeError("relation does not exist")
    assert ur.load_latest_ranking(broken, horizon_days=90) is None


def test_list_rankings_pagination_contract() -> None:
    engine, conn = _engine(rows=[_row(9), _row(8)], total=5)
    page = ur.list_rankings(engine, limit=2, offset=2, horizon_days=90)
    assert [e["id"] for e in page["entries"]] == [9, 8]
    assert page["total"] == 5 and page["limit"] == 2 and page["offset"] == 2
    assert page["has_more"] is True
    page_params = [c.args[1] for c in conn.execute.call_args_list if "OFFSET" in str(c.args[0])]
    assert page_params == [{"horizon_days": 90, "limit": 2, "offset": 2}]


def test_list_rankings_last_page_and_error_are_empty_not_raised() -> None:
    engine, _ = _engine(rows=[_row(1)], total=3)
    page = ur.list_rankings(engine, limit=2, offset=2)
    assert page["has_more"] is False
    broken = MagicMock()
    broken.connect.side_effect = RuntimeError("db down")
    empty = ur.list_rankings(broken, limit=-4, offset=-1)
    assert empty == {"entries": [], "total": 0, "limit": 1, "offset": 0, "has_more": False}


def test_readback_sql_is_parameterised_and_not_formatted() -> None:
    from pathlib import Path

    src = Path(ur.__file__).read_text(encoding="utf-8")
    block = src[src.index("_LATEST_RANKING_SQL"):src.index("def _ranking_row_to_dict")]
    assert 'f"""' not in block and ".format(" not in block
    assert ":horizon_days" in block and ":limit OFFSET :offset" in block
