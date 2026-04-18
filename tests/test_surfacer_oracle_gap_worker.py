from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock


def test_worker_claims_matching_requirements_and_records_last_attempt(monkeypatch):
    from scripts import drain_surfacer_oracle_gap_worker as worker

    execute_calls = []
    claim_rows = [
        SimpleNamespace(
            _mapping={
                "id": 17,
                "ticker": "AAPL",
                "requirement_type": "ticker_direction_calibration",
                "priority": 2,
                "reason": "missing calibration",
                "payload": {"seed": True},
                "volume_rank": 3,
                "dollar_volume": 123.45,
            }
        )
    ]

    class Result:
        def fetchall(self):
            return claim_rows

    conn = MagicMock()
    conn.execute.side_effect = lambda *args, **kwargs: execute_calls.append((args, kwargs)) or Result()
    engine = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn
    engine.begin.return_value.__exit__.return_value = False

    monkeypatch.setattr(worker, "get_engine", lambda: engine)
    monkeypatch.setattr(worker, "_run_oracle_cycle", lambda ticker, timeout: {
        "outcome": "ok",
        "returncode": 0,
        "duration_s": 1.25,
        "command": ["python", "oracle/run_cycle.py", "--tickers", ticker],
        "stdout": '{"new_predictions": 0}',
        "stderr": "",
        "parsed_output": {"new_predictions": 0},
    })
    monkeypatch.setattr(worker.time, "sleep", lambda _: None)

    exit_code = worker.main(["--limit", "1", "--priority-max", "2", "--sleep", "0"])

    assert exit_code == 0
    assert len(execute_calls) == 2

    claim_sql = execute_calls[0][0][0].text
    assert "FOR UPDATE SKIP LOCKED" in claim_sql
    assert "requirement_type = :requirement_type" in claim_sql
    assert "status = 'pending'" in claim_sql
    assert "priority <= :priority_max" in claim_sql

    update_sql = execute_calls[1][0][0].text
    assert "SET status = 'pending'" in update_sql
    assert "last_attempt_at" in execute_calls[1][0][1]["payload"]
    assert execute_calls[1][0][1]["id"] == 17


def test_dry_run_lists_pending_rows_without_claiming(monkeypatch, capsys):
    from scripts import drain_surfacer_oracle_gap_worker as worker

    row = SimpleNamespace(
        _mapping={
            "id": 5,
            "ticker": "MSFT",
            "requirement_type": "ticker_direction_calibration",
            "priority": 1,
            "reason": "missing calibration",
            "payload": {"note": "preview"},
            "volume_rank": 1,
            "dollar_volume": 999.0,
        }
    )

    class Result:
        def fetchall(self):
            return [row]

    conn = MagicMock()
    conn.execute.return_value = Result()
    engine = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn
    engine.begin.return_value.__exit__.return_value = False

    monkeypatch.setattr(worker, "get_engine", lambda: engine)

    exit_code = worker.main(["--limit", "1", "--priority-max", "1", "--dry-run"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert '"dry_run": true' in captured.out
    assert '"ticker": "MSFT"' in captured.out
    assert conn.execute.call_count == 1
