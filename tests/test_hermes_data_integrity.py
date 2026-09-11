"""Tests for the Hermes data-integrity watchdogs.

Every failure these checks exist for was silent: a table stopped being written
and nothing said so. The tests therefore care most about the cases where a
naive implementation would *also* be silent — a first observation with nothing
to compare against, a check that cannot run, and a table name that is not
declared.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from scripts.hermes_data_integrity import (
    EXPECTED_WRITERS,
    check_writer_freshness,
    collect_failed_ranges,
)


class _Conn:
    """Connection double returning scripted results per statement kind."""

    def __init__(self, max_id, prior):
        self._max_id = max_id
        self._prior = prior
        self.executed: list[str] = []

    def execute(self, statement, params=None):
        sql = str(statement)
        self.executed.append(sql)
        result = MagicMock()
        if "max(" in sql:
            result.scalar.return_value = self._max_id
        elif "FROM hermes_writer_watermarks" in sql:
            result.fetchone.return_value = self._prior
        else:
            result.fetchone.return_value = None
            result.scalar.return_value = None
        return result

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Engine:
    def __init__(self, max_id=100, prior=None):
        self.conn = _Conn(max_id, prior)

    def begin(self):
        return self.conn


class TestCheckWriterFreshness:
    def test_first_observation_reports_baseline_not_health(self):
        """With nothing to compare against, a verdict would be invented."""
        with patch("scripts.hermes_data_integrity.log_issue") as logged:
            out = check_writer_freshness(_Engine(max_id=100, prior=None),
                                         table="resolved_series")
        assert out["tables"]["resolved_series"]["status"] == "baseline"
        logged.assert_not_called(), "a baseline must not raise an alarm"

    def test_a_table_that_advanced_is_ok(self):
        prior = (50, datetime.now(timezone.utc) - timedelta(hours=10))
        with patch("scripts.hermes_data_integrity.log_issue") as logged:
            out = check_writer_freshness(_Engine(max_id=100, prior=prior),
                                         table="resolved_series")
        assert out["tables"]["resolved_series"]["status"] == "ok"
        assert out["tables"]["resolved_series"]["advanced_by"] == 50
        logged.assert_not_called()

    def test_a_stalled_table_logs_an_issue(self):
        """The five-month resolved_series stall, caught on the next cycle."""
        quiet_for = EXPECTED_WRITERS["resolved_series"]["max_quiet_hours"] + 1
        prior = (100, datetime.now(timezone.utc) - timedelta(hours=quiet_for))
        with patch("scripts.hermes_data_integrity.log_issue") as logged:
            out = check_writer_freshness(_Engine(max_id=100, prior=prior),
                                         table="resolved_series")
        assert out["tables"]["resolved_series"]["status"] == "stalled"
        assert out["tables"]["resolved_series"]["advanced_by"] == 0
        assert logged.call_count == 1
        kwargs = logged.call_args.kwargs
        assert kwargs["severity"] == "ERROR"
        assert "resolved_series" in kwargs["title"]

    def test_quiet_but_within_threshold_does_not_cry_wolf(self):
        prior = (100, datetime.now(timezone.utc) - timedelta(minutes=30))
        with patch("scripts.hermes_data_integrity.log_issue") as logged:
            out = check_writer_freshness(_Engine(max_id=100, prior=prior),
                                         table="resolved_series")
        assert out["tables"]["resolved_series"]["status"] == "quiet"
        logged.assert_not_called()

    def test_an_undeclared_table_is_refused_not_interpolated(self):
        """A table name cannot be a bound parameter, so the allowlist is the guard."""
        out = check_writer_freshness(_Engine(), table="raw_series; DROP TABLE x")
        assert out["status"] == "error"
        assert "not a declared expected-writer table" in out["error"]

    def test_only_declared_names_reach_the_sql(self):
        engine = _Engine(max_id=5, prior=None)
        check_writer_freshness(engine, table="resolved_series")
        max_stmts = [s for s in engine.conn.executed if "max(" in s]
        assert max_stmts == ["SELECT max(id) FROM resolved_series"]

    def test_a_failing_check_reports_error_rather_than_ok(self):
        """A check that cannot run must never read as 'nothing is wrong'."""
        engine = MagicMock()
        engine.begin.side_effect = [MagicMock(), RuntimeError("connection lost")]
        out = check_writer_freshness(engine, table="resolved_series")
        assert out["tables"]["resolved_series"]["status"] == "error"


class TestCollectFailedRanges:
    def test_extracts_retryable_bounds(self):
        summary = {"failed_ranges": [
            {"since": "2026-04-05T00:00:00", "until": "2026-04-06T00:00:00",
             "error": "timeout", "error_class": "OperationalError"},
        ]}
        assert collect_failed_ranges(summary) == [
            {"since": "2026-04-05T00:00:00", "until": "2026-04-06T00:00:00"},
        ]

    def test_a_clean_run_yields_nothing_to_retry(self):
        assert collect_failed_ranges({"failed_ranges": []}) == []
        assert collect_failed_ranges({}) == []

    def test_malformed_entries_are_dropped_not_retried_blind(self):
        summary = {"failed_ranges": [{"error": "no bounds recorded"}]}
        assert collect_failed_ranges(summary) == []
