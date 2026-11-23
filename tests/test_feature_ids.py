"""Unit tests for store/feature_ids.py (no database).

The PostgreSQL contract for the readers that use these lookups is
tests/test_resolved_series_bound_id_reads_pg.py.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from store.feature_ids import feature_id, feature_ids

_REGISTRY = {"spy_full": 2791, "aapl": 1695, "aapl_full": 1696}


def _conn() -> MagicMock:
    conn = MagicMock()

    def _execute(statement, params):
        sql = str(statement)
        assert "FROM feature_registry" in sql
        assert "resolved_series" not in sql
        result = MagicMock()
        if "name" in params:
            fid = _REGISTRY.get(params["name"])
            result.fetchone.return_value = (fid,) if fid is not None else None
        else:
            # Rows come back in any order; the helper restores the caller's.
            rows = [(fid, n) for n, fid in sorted(_REGISTRY.items()) if n in params["names"]]
            result.fetchall.return_value = rows
        return result

    conn.execute.side_effect = _execute
    return conn


def test_feature_id_binds_the_name_and_returns_an_int() -> None:
    conn = _conn()
    assert feature_id(conn, "spy_full") == 2791
    assert conn.execute.call_args.args[1] == {"name": "spy_full"}


def test_feature_id_unregistered_or_empty_is_none() -> None:
    conn = _conn()
    assert feature_id(conn, "nope") is None
    assert feature_id(conn, "") is None
    assert conn.execute.call_count == 1  # the empty name never reaches the database


def test_feature_ids_keep_caller_order_and_drop_unregistered() -> None:
    conn = _conn()
    assert feature_ids(conn, ["aapl_full", "nope", "aapl"]) == [1696, 1695]
    assert feature_ids(conn, ["aapl", "aapl_full"]) == [1695, 1696]


def test_feature_ids_dedupe_and_skip_empty_names() -> None:
    conn = _conn()
    assert feature_ids(conn, ["spy_full", "", "spy_full", None]) == [2791]
    assert conn.execute.call_args.args[1] == {"names": ["spy_full"]}


def test_feature_ids_no_names_no_query() -> None:
    conn = _conn()
    assert feature_ids(conn, []) == []
    assert feature_ids(conn, ["", None]) == []
    conn.execute.assert_not_called()
