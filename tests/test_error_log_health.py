"""Tests for the ``scripts.error_log_health`` triage script.

Pin the contract: signatures over the threshold cause exit 1, anything
under threshold (or any empty/missing log) returns 0.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scripts.error_log_health import report


def _write_log(path: Path, entries: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for entry in entries:
            fh.write(json.dumps(entry) + "\n")


def _entry(
    msg: str = "boom",
    module: str = "mod",
    function: str = "fn",
    minutes_ago: float = 0.0,
) -> dict:
    ts = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    return {
        "ts": ts.isoformat(),
        "level": "ERROR",
        "module": module,
        "function": function,
        "line": 1,
        "message": msg,
        "exception": None,
    }


@pytest.mark.unit
class TestErrorLogHealth:
    def test_missing_log_returns_zero(self, tmp_path):
        rc = report(
            log_path=tmp_path / "absent.jsonl",
            hours=24,
            threshold=10,
            top=5,
        )
        assert rc == 0

    def test_under_threshold_returns_zero(self, tmp_path, capsys):
        log = tmp_path / "errors.jsonl"
        _write_log(log, [_entry(msg=f"err-{i}") for i in range(3)])

        rc = report(log_path=log, hours=24, threshold=10, top=5)
        assert rc == 0
        out = capsys.readouterr().out
        assert "[ok]" in out

    def test_over_threshold_returns_one(self, tmp_path, capsys):
        log = tmp_path / "errors.jsonl"
        # 12 identical signatures within the window
        _write_log(log, [_entry(msg="boom") for _ in range(12)])

        rc = report(log_path=log, hours=24, threshold=10, top=5)
        assert rc == 1
        out = capsys.readouterr().out
        assert "[fail]" in out

    def test_window_excludes_old_entries(self, tmp_path, capsys):
        log = tmp_path / "errors.jsonl"
        _write_log(
            log,
            [
                # 50 ancient entries (well outside 1h window)
                *[_entry(msg="ancient", minutes_ago=120) for _ in range(50)],
                # 1 fresh entry
                _entry(msg="fresh"),
            ],
        )

        rc = report(log_path=log, hours=1, threshold=10, top=5)
        assert rc == 0
        out = capsys.readouterr().out
        assert "last 1" in out
        assert "[ok]" in out

    def test_corrupt_lines_are_skipped_not_fatal(self, tmp_path):
        log = tmp_path / "errors.jsonl"
        log.parent.mkdir(parents=True, exist_ok=True)
        with log.open("w", encoding="utf-8") as fh:
            fh.write("not json\n")
            fh.write(json.dumps(_entry(msg="real")) + "\n")
            fh.write("\n")  # blank line

        rc = report(log_path=log, hours=24, threshold=10, top=5)
        assert rc == 0
