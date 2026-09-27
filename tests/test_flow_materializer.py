"""Tests for ingestion.flow_materializer failure handling and GD-FIX
honesty fixes (insider_trades.filing_date, honest is_cluster_buy)."""

from __future__ import annotations

from datetime import date
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


# ── GD-FIX: insider_trades.filing_date ──────────────────────────────────

def test_parse_filing_date_parses_iso_string():
    assert flow_materializer._parse_filing_date("2026-03-14") == date(2026, 3, 14)


def test_parse_filing_date_parses_datetime_style_string():
    assert flow_materializer._parse_filing_date("2026-03-14T00:00:00Z") == date(2026, 3, 14)


def test_parse_filing_date_missing_stays_none_never_trade_date():
    """GD-FIX: a missing filing date must stay NULL, never fall back to the
    trade date — the whole point of the fix is that the two are different
    facts and conflating them fabricates a same-day filing."""
    assert flow_materializer._parse_filing_date("") is None
    assert flow_materializer._parse_filing_date(None) is None


def test_parse_filing_date_unparseable_stays_none():
    assert flow_materializer._parse_filing_date("not-a-date") is None


# ── GD-FIX: honest is_cluster_buy (was is_unusual_size) ─────────────────

def test_cluster_windows_from_rows_builds_ticker_windows():
    rows = [
        ("AAPL", date(2026, 3, 20), {"window_days": 10}),
        ("MSFT", date(2026, 4, 1), '{"window_days": 5}'),
    ]
    windows = flow_materializer._cluster_windows_from_rows(rows)
    assert windows["AAPL"] == [(date(2026, 3, 10), date(2026, 3, 20))]
    assert windows["MSFT"] == [(date(2026, 3, 27), date(2026, 4, 1))]


def test_cluster_windows_from_rows_skips_missing_ticker_or_date():
    rows = [(None, date(2026, 3, 20), {}), ("AAPL", None, {})]
    assert flow_materializer._cluster_windows_from_rows(rows) == {}


def test_is_in_cluster_window_true_inside_range():
    windows = {"AAPL": [(date(2026, 3, 10), date(2026, 3, 20))]}
    assert flow_materializer._is_in_cluster_window("AAPL", date(2026, 3, 15), windows) is True


def test_is_in_cluster_window_false_outside_range():
    windows = {"AAPL": [(date(2026, 3, 10), date(2026, 3, 20))]}
    assert flow_materializer._is_in_cluster_window("AAPL", date(2026, 4, 1), windows) is False


def test_is_in_cluster_window_false_for_unmentioned_ticker():
    windows = {"AAPL": [(date(2026, 3, 10), date(2026, 3, 20))]}
    assert flow_materializer._is_in_cluster_window("MSFT", date(2026, 3, 15), windows) is False


def test_is_in_cluster_window_no_longer_reads_is_unusual_size():
    """GD-FIX regression guard: a single large ($500K+) trade with no
    second insider must NOT be flagged as a cluster buy just because it's
    unusually sized — that was exactly the old bug."""
    # An "is_unusual_size" trade with no CLUSTER_BUY row at all for its
    # ticker is not in any cluster window.
    windows: dict = {}
    assert flow_materializer._is_in_cluster_window("TSLA", date(2026, 3, 15), windows) is False
