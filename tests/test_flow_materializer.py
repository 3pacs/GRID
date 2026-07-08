"""Tests for ingestion.flow_materializer failure handling."""

from __future__ import annotations

from unittest.mock import MagicMock

from loguru import logger

from ingestion import flow_materializer


def _capture_levels(fn) -> list[tuple[str, str]]:
    records: list[tuple[str, str]] = []
    sink_id = logger.add(
        lambda msg: records.append(
            (msg.record["level"].name, msg.record["message"])
        ),
        level="WARNING",
    )
    try:
        fn()
    finally:
        logger.remove(sink_id)
    return records


def test_sync_all_statement_timeout_logs_warning_not_error(monkeypatch):
    def raise_timeout(_engine):
        raise RuntimeError("canceling statement due to statement timeout")

    monkeypatch.setattr(flow_materializer, "sync_insider_trades", raise_timeout)
    monkeypatch.setattr(flow_materializer, "sync_congressional_trades", lambda _engine: 0)
    monkeypatch.setattr(flow_materializer, "sync_dark_pool_weekly", lambda _engine: 0)
    monkeypatch.setattr(flow_materializer, "sync_etf_flows", lambda _engine: 0)
    monkeypatch.setattr(flow_materializer, "sync_junction_points", lambda _engine: 0)

    result: dict[str, object] = {}
    records = _capture_levels(
        lambda: result.update(flow_materializer.sync_all(MagicMock()))
    )

    assert result["status"] == "PARTIAL"
    assert result["insider_trades"] == 0
    levels = {lvl for lvl, _ in records}
    assert "ERROR" not in levels
    assert "WARNING" in levels


def test_sync_all_code_failure_still_logs_error(monkeypatch):
    def raise_bug(_engine):
        raise AttributeError("missing parser")

    monkeypatch.setattr(flow_materializer, "sync_insider_trades", raise_bug)
    monkeypatch.setattr(flow_materializer, "sync_congressional_trades", lambda _engine: 0)
    monkeypatch.setattr(flow_materializer, "sync_dark_pool_weekly", lambda _engine: 0)
    monkeypatch.setattr(flow_materializer, "sync_etf_flows", lambda _engine: 0)
    monkeypatch.setattr(flow_materializer, "sync_junction_points", lambda _engine: 0)

    records = _capture_levels(lambda: flow_materializer.sync_all(MagicMock()))

    assert "ERROR" in {lvl for lvl, _ in records}
