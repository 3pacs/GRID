"""Unit tests for ingestion/base.py BasePuller and retry logic."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from ingestion.base import BasePuller, retry_on_failure


# ---------------------------------------------------------------------------
# retry_on_failure decorator tests
# ---------------------------------------------------------------------------


class TestRetryOnFailure:
    def test_succeeds_first_try(self):
        count = 0

        @retry_on_failure(max_attempts=3, backoff=0.01)
        def ok():
            nonlocal count
            count += 1
            return "ok"

        assert ok() == "ok"
        assert count == 1

    def test_retries_on_connection_error(self):
        count = 0

        @retry_on_failure(max_attempts=3, backoff=0.01)
        def fail_twice():
            nonlocal count
            count += 1
            if count < 3:
                raise ConnectionError("down")
            return "recovered"

        assert fail_twice() == "recovered"
        assert count == 3

    def test_gives_up_after_max(self):
        @retry_on_failure(max_attempts=2, backoff=0.01)
        def always_fail():
            raise ConnectionError("permanent")

        with pytest.raises(ConnectionError, match="permanent"):
            always_fail()

    def test_no_retry_on_non_retryable(self):
        count = 0

        @retry_on_failure(max_attempts=3, backoff=0.01)
        def bad_input():
            nonlocal count
            count += 1
            raise ValueError("bad")

        with pytest.raises(ValueError):
            bad_input()
        assert count == 1

    def test_timeout_error_is_retryable(self):
        count = 0

        @retry_on_failure(max_attempts=2, backoff=0.01)
        def timeout_func():
            nonlocal count
            count += 1
            if count < 2:
                raise TimeoutError("timed out")
            return "done"

        assert timeout_func() == "done"
        assert count == 2


# ---------------------------------------------------------------------------
# BasePuller._row_exists tests
# ---------------------------------------------------------------------------


class TestBasePullerRowExists:
    """Verify _row_exists returns True/False correctly."""

    def _make_puller(self, mock_engine):
        """Create a BasePuller without calling _resolve_source_id."""
        puller = BasePuller.__new__(BasePuller)
        puller.engine = mock_engine
        puller.source_id = 1
        return puller

    def test_row_exists_returns_true(self, mock_engine):
        puller = self._make_puller(mock_engine)

        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (1,)
        mock_conn.execute.return_value = mock_result

        assert puller._row_exists("DFF", date(2024, 1, 15), mock_conn) is True

    def test_row_exists_returns_false(self, mock_engine):
        puller = self._make_puller(mock_engine)

        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        mock_conn.execute.return_value = mock_result

        assert puller._row_exists("DFF", date(2024, 1, 15), mock_conn) is False

    def test_row_exists_passes_source_id_in_query(self, mock_engine):
        puller = self._make_puller(mock_engine)
        puller.source_id = 42

        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        mock_conn.execute.return_value = mock_result

        puller._row_exists("DFF", date(2024, 1, 15), mock_conn)

        call_args = mock_conn.execute.call_args
        params = call_args[0][1] if len(call_args[0]) > 1 else call_args[1]
        assert params["src"] == 42
