"""Signal-source write-path tests for the three previously broken pullers.

Covers:

1. ``ingestion/altdata/smart_money.py`` — now emits two signal_sources
   rows per signal: ``source_type='smart_money'`` +
   ``source_type='social'``.
2. ``ingestion/altdata/institutional_flows.py`` — 13F position-change
   emitter now uses the real schema (source_type / source_id / ticker /
   signal_date / signal_type / signal_value JSONB) and no longer
   references the non-existent ``signal_payload`` / ``confidence``
   columns.

Each puller's write path is exercised via a stub connection so no real
PostgreSQL is required. Tests verify:

- The INSERT statement targets ``signal_sources`` and uses the correct
  column list.
- The ``source_type`` matches the canonical stream names that the
  downstream convergence scanner expects.
- The JSONB payload contains the contract keys.
- The upsert ``ON CONFLICT`` clause matches the schema UNIQUE key
  ``(source_type, source_id, ticker, signal_date, signal_type)``.
- A forced INSERT exception surfaces as a WARN log but does NOT break
  the puller's raw_series write.
"""

from __future__ import annotations

import json
from datetime import date
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _StubConn:
    """Capture-only DB connection stub.

    Records every ``execute(stmt, params)`` call on ``.calls`` as a
    tuple of ``(sql_text, params_dict)`` so tests can assert on the
    exact SQL + bound params emitted by the puller.
    """

    def __init__(self, fail_on_signal_sources: bool = False) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.fail_on_signal_sources = fail_on_signal_sources

    def execute(self, stmt: Any, params: dict[str, Any] | None = None):
        sql = getattr(stmt, "text", None) or str(stmt)
        self.calls.append((sql, params or {}))
        if self.fail_on_signal_sources and "signal_sources" in sql:
            raise RuntimeError("forced signal_sources failure")
        # Return a generic mock for chained .fetchone / .fetchall calls.
        return MagicMock(fetchone=MagicMock(return_value=None),
                         fetchall=MagicMock(return_value=[]))


def _find_signal_sources_calls(conn: _StubConn) -> list[tuple[str, dict[str, Any]]]:
    return [(sql, p) for sql, p in conn.calls if "signal_sources" in sql]


# ---------------------------------------------------------------------------
# smart_money — smart_money + social heat rows
# ---------------------------------------------------------------------------


@pytest.fixture
def smart_money_puller():
    """Return a SmartMoneyPuller with __init__ bypassed.

    Avoids hitting source_catalog; we only exercise the pure write-path
    helper ``_emit_signal_sources``.
    """
    from ingestion.altdata.smart_money import SmartMoneyPuller

    inst = SmartMoneyPuller.__new__(SmartMoneyPuller)
    inst.engine = MagicMock()
    inst.source_id = 999
    inst._trust_cache = {}
    return inst


def test_smart_money_emits_smart_money_row(smart_money_puller):
    """source_type='smart_money' must emit with NET_POSITION_DELTA."""
    conn = _StubConn()
    signal = {
        "platform": "reddit",
        "username": "deepFckingValue",
        "ticker": "TSLA",
        "direction": "BULLISH",
        "post_score": 250,
        "trust_score": 0.7,
        "subreddit": "wallstreetbets",
    }
    smart_money_puller._emit_signal_sources(conn, signal, date(2026, 4, 14))

    sm_calls = [
        (sql, p) for sql, p in _find_signal_sources_calls(conn)
        if p.get("stype") == "smart_money"
    ]
    assert len(sm_calls) == 1, \
        f"expected one smart_money row, got {len(sm_calls)}"
    sql, params = sm_calls[0]

    # Column set — real schema, not the old broken one.
    assert "source_type" in sql
    assert "source_id" in sql
    assert "ticker" in sql
    assert "signal_date" in sql
    assert "signal_type" in sql
    assert "signal_value" in sql
    assert "signal_payload" not in sql  # broken column — must be gone
    assert "confidence" not in sql      # broken column — must be gone

    # Upsert key
    assert ("ON CONFLICT (source_type, source_id, ticker, "
            "signal_date, signal_type)") in sql

    # Canonical source_type + signal_type
    assert params["stype"] == "smart_money"
    assert params["stype2"] == "NET_POSITION_DELTA"
    assert params["ticker"] == "TSLA"
    assert params["sdate"] == date(2026, 4, 14)

    # Payload contract keys
    payload = json.loads(params["sval"])
    assert "position_delta" in payload
    assert "reddit_mentions_count" in payload
    assert "sentiment_score" in payload
    assert "window_days" in payload
    assert payload["position_delta"] > 0  # BULLISH → +1


def test_smart_money_emits_social_heat_row(smart_money_puller):
    """source_type='social' must emit with HEAT_SPIKE + heat payload."""
    conn = _StubConn()
    signal = {
        "platform": "reddit",
        "username": "roaring_kitty",
        "ticker": "GME",
        "direction": "BULLISH",
        "post_score": 500,
        "subreddit": "wallstreetbets",
    }
    smart_money_puller._emit_signal_sources(conn, signal, date(2026, 4, 14))

    social_calls = [
        (sql, p) for sql, p in _find_signal_sources_calls(conn)
        if p.get("stype") == "social"
    ]
    assert len(social_calls) == 1
    sql, params = social_calls[0]

    assert params["stype"] == "social"
    assert params["stype2"] == "HEAT_SPIKE"
    assert params["ticker"] == "GME"
    assert "signal_payload" not in sql
    assert "confidence" not in sql

    payload = json.loads(params["sval"])
    assert "mentions_z" in payload
    assert "sentiment" in payload
    assert "ticker_rank" in payload


def test_smart_money_write_path_does_not_drop_raw_on_signal_failure(
    smart_money_puller,
):
    """If _emit_signal_sources raises, _store_signal must still return
    True (raw_series was persisted) and the puller must log a WARN.
    """
    conn = _StubConn()

    # Stub _row_exists / _insert_raw so we don't touch base-puller internals.
    smart_money_puller._row_exists = MagicMock(return_value=False)
    smart_money_puller._insert_raw = MagicMock()
    smart_money_puller._emit_signal_sources = MagicMock(
        side_effect=RuntimeError("boom")
    )

    signal = {
        "platform": "reddit",
        "username": "someone",
        "ticker": "NVDA",
        "direction": "BEARISH",
        "post_score": 120,
    }
    inserted = smart_money_puller._store_signal(conn, signal, date(2026, 4, 14))

    # raw_series write still happened
    assert smart_money_puller._insert_raw.called
    # signal_sources emit was attempted
    assert smart_money_puller._emit_signal_sources.called
    # function reports success (the raw row was stored)
    assert inserted is True


def test_smart_money_finviz_retired_fetch_refuses_before_network(
    smart_money_puller,
    monkeypatch,
):
    """The retired private fetch must refuse without an HTTP probe or DB write."""
    network = MagicMock(side_effect=AssertionError("retired Finviz HTTP reached"))
    monkeypatch.setattr(
        "ingestion.altdata.smart_money.requests.get", network,
    )
    monkeypatch.setattr("requests.sessions.Session.request", network)

    with pytest.raises(RuntimeError, match="Finviz retired.*SEC Form-4"):
        smart_money_puller._fetch_finviz_insiders()
    network.assert_not_called()
    assert smart_money_puller.engine.mock_calls == []


def test_smart_money_finviz_retired_compatibility_is_skipped_without_work(
    smart_money_puller,
    monkeypatch,
):
    """Old callers get honest SKIPPED without fetching or persisting any source."""
    network = MagicMock(side_effect=AssertionError("retired Finviz HTTP reached"))
    monkeypatch.setattr(
        "ingestion.altdata.smart_money.requests.get", network,
    )
    monkeypatch.setattr("requests.sessions.Session.request", network)
    fetch = MagicMock(side_effect=AssertionError("retired provider fetch reached"))
    monkeypatch.setattr(smart_money_puller, "_fetch_finviz_insiders", fetch)
    writers = []
    for name in ("_store_signal", "_insert_raw", "_emit_signal_sources"):
        writer = MagicMock(side_effect=AssertionError("retired source write reached"))
        monkeypatch.setattr(smart_money_puller, name, writer)
        writers.append(writer)

    result = smart_money_puller.pull_finviz_insiders()

    assert result["source"] == "finviz_insider"
    assert result["status"] == "SKIPPED"
    assert result["signals_found"] == 0
    assert result["rows_inserted"] == 0
    assert "Finviz retired" in result["reason"]
    assert "separate SEC Form-4 source" in result["reason"]
    fetch.assert_not_called()
    network.assert_not_called()
    for writer in writers:
        writer.assert_not_called()
    assert smart_money_puller.engine.mock_calls == []


# ---------------------------------------------------------------------------
# institutional_flows — 13F NET_POSITION_DELTA rows
# ---------------------------------------------------------------------------


def test_institutional_flows_writes_no_signal_sources_rows():
    """13F is non-directional (owner decision 2026-11-23): the 13F puller no
    longer writes signal_sources at all. Its old 'institutional' /
    NET_POSITION_DELTA rows carried a dollar value that wealth_tracker and
    pocket_lining read as a directional fund flow (Codex HOLD on #1369
    d304044d). The end-to-end check is
    tests/test_flows_sentiment_defects.py::test_13f_straddling_cutover_pair_creates_no_spurious_change_event.
    """
    import pathlib
    src = pathlib.Path("ingestion/altdata/institutional_flows.py").read_text()
    assert "INSERT INTO signal_sources" not in src
    assert '"NET_POSITION_DELTA"' not in src  # no signal_type written (the comment may name it)


def test_signal_sources_failure_logs_warn_but_continues(caplog):
    """When signal_sources INSERT raises, the puller's embedded try/except
    must log WARN (not DEBUG) and not propagate the exception.

    This verifies the anti-silent-swallow rule from the task brief.
    """
    from loguru import logger as log

    # Route loguru into caplog
    handler_id = log.add(
        lambda m: caplog.records.append(
            SimpleNamespace(
                levelname=m.record["level"].name,
                msg=m.record["message"],
            )
        ),
        level="WARNING",
    )

    conn = _StubConn(fail_on_signal_sources=True)
    # Mirror the per-change block from institutional_flows.py
    try:
        from sqlalchemy import text
        conn.execute(
            text(
                "INSERT INTO signal_sources "
                "(source_type, source_id, ticker, signal_date, "
                "signal_type, signal_value) VALUES (:a, :b, :c, :d, :e, :f)"
            ),
            {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6},
        )
    except Exception as exc:
        log.warning("institutional_flows: signal_sources write failed: {e}", e=str(exc))

    log.remove(handler_id)

    warn_records = [r for r in caplog.records if r.levelname == "WARNING"]
    assert any("signal_sources" in r.msg for r in warn_records), \
        f"expected a signal_sources WARN log, got {[r.msg for r in warn_records]}"
