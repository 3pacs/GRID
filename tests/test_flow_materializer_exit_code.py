"""run_cli maps sync_all status to the systemd exit code (FAILED -> 1)."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from loguru import logger

from ingestion import flow_materializer as fm


def _run(monkeypatch, summary):
    monkeypatch.setattr(fm, "sync_all", lambda engine: summary)
    msgs: list = []
    sink = logger.add(lambda m: msgs.append(m.record), level="WARNING")
    try:
        rc = fm.run_cli(MagicMock())
    finally:
        logger.remove(sink)
    return rc, msgs


def test_failed_exits_nonzero(monkeypatch):
    errs = [{"table": t, "error": "statement timeout"} for t in ("insider_trades", "etf_flows")]
    rc, msgs = _run(monkeypatch, {"status": "FAILED", "errors": errs})
    assert rc == 1
    assert any(m["level"].name == "ERROR" and "FAILED" in m["message"] for m in msgs)


def test_partial_exits_zero_but_logs_warning(monkeypatch):
    rc, msgs = _run(monkeypatch, {"status": "PARTIAL",
                                  "errors": [{"table": "junction_point_readings", "error": "x"}]})
    assert rc == 0
    assert any(m["level"].name == "WARNING" and "PARTIAL" in m["message"]
               and "junction_point_readings" in m["message"] for m in msgs)


def test_success_exits_zero_quietly(monkeypatch):
    rc, msgs = _run(monkeypatch, {"status": "SUCCESS", "errors": []})
    assert rc == 0 and not msgs


def test_main_guard_exits_with_run_cli_code():
    src = open(fm.__file__).read()
    assert "sys.exit(run_cli())" in src
