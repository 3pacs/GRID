"""Dead hypotheses are kept as the trial ledger (2026-11-06).

``cleanup_hypotheses`` used to delete expired/invalidated
``discovered_hypotheses`` (>90 days) and FAILED/KILLED ``hypothesis_registry``
rows (>180 days) every Sunday. A dead hypothesis is still a trial, and the
overfitting gate deflates by the trial count, so the prune made survivors look
more significant than they are. It now only reports what it would have hit,
and the scheduler no longer runs it.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from intelligence.hypothesis_engine import cleanup_hypotheses

_SCHEDULER = Path(__file__).resolve().parents[1] / "ingestion" / "scheduler.py"


class _RecordingConn:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, stmt, *args, **kwargs):
        sql = str(stmt)
        self.statements.append(sql)
        result = MagicMock()
        result.scalar.return_value = 7 if "discovered_hypotheses" in sql else 3
        return result


def _engine(conn: _RecordingConn) -> MagicMock:
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    engine.begin.return_value.__enter__.return_value = conn
    return engine


@pytest.mark.parametrize("dry_run", [False, True])
def test_cleanup_reports_and_never_deletes(dry_run):
    conn = _RecordingConn()
    engine = _engine(conn)
    result = cleanup_hypotheses(engine, dry_run=dry_run)
    assert result == {"discovered_expired": 7, "registry_failed": 3}
    assert not any("DELETE" in s.upper() for s in conn.statements)
    engine.begin.assert_not_called()


def test_scheduler_no_longer_runs_the_prune():
    source = _SCHEDULER.read_text()
    assert "cleanup_hypotheses" not in source
    assert 'sunday.at("02:30")' not in source
