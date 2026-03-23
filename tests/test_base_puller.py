"""Unit tests for ingestion/base.py BasePuller and retry logic."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
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
# Helper to build mock engines
# ---------------------------------------------------------------------------


def _mock_engine(source_id: int = 1) -> tuple[MagicMock, MagicMock]:
    """Build a mock engine that returns source_id from source_catalog."""
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.fetchone.return_value = (source_id,)
    return engine, conn


# ---------------------------------------------------------------------------
# BasePuller._resolve_source_id tests
# ---------------------------------------------------------------------------


class TestResolveSourceId:
    """BasePuller._resolve_source_id looks up source_catalog."""

    def test_resolve_returns_id(self):
        """Returns the source_catalog.id when found."""
        engine, conn = _mock_engine(source_id=42)

        class TestPuller(BasePuller):
            SOURCE_NAME = "TestSource"

        puller = TestPuller(engine)
        assert puller.source_id == 42

    def test_resolve_raises_when_missing(self):
        """Raises RuntimeError when source not in catalog."""
        engine, conn = _mock_engine()
        conn.execute.return_value.fetchone.return_value = None

        class TestPuller(BasePuller):
            SOURCE_NAME = "MissingSource"

        with pytest.raises(RuntimeError, match="MissingSource source not found"):
            TestPuller(engine)


# ---------------------------------------------------------------------------
# BasePuller._row_exists tests
# ---------------------------------------------------------------------------


class TestRowExists:
    """BasePuller._row_exists checks for recent duplicates."""

    def test_row_exists_true(self):
        """Returns True when a matching row is found."""
        engine, conn = _mock_engine()

        class TestPuller(BasePuller):
            SOURCE_NAME = "Test"

        puller = TestPuller(engine)

        # Configure for _row_exists call
        conn.execute.return_value.fetchone.return_value = (1,)
        assert puller._row_exists("series_1", date(2024, 1, 1), conn) is True

    def test_row_exists_false(self):
        """Returns False when no matching row is found."""
        engine, conn = _mock_engine()

        class TestPuller(BasePuller):
            SOURCE_NAME = "Test"

        puller = TestPuller(engine)

        conn.execute.return_value.fetchone.return_value = None
        assert puller._row_exists("series_1", date(2024, 1, 1), conn) is False


class TestBasePullerRowExists:
    """Verify _row_exists returns True/False correctly (mock_engine fixture)."""

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


# ---------------------------------------------------------------------------
# Inheritance tests
# ---------------------------------------------------------------------------


class TestBasePullerInheritance:
    """Verify that FRED, BLS, yfinance properly inherit from BasePuller."""

    def test_fred_is_base_puller(self):
        """FREDPuller is a subclass of BasePuller."""
        from ingestion.fred import FREDPuller
        assert issubclass(FREDPuller, BasePuller)
        assert FREDPuller.SOURCE_NAME == "FRED"

    def test_bls_is_base_puller(self):
        """BLSPuller is a subclass of BasePuller."""
        from ingestion.bls import BLSPuller
        assert issubclass(BLSPuller, BasePuller)
        assert BLSPuller.SOURCE_NAME == "BLS"

    def test_yfinance_is_base_puller(self):
        """YFinancePuller is a subclass of BasePuller."""
        try:
            from ingestion.yfinance_pull import YFinancePuller
        except ImportError:
            pytest.skip("yfinance not available")
        assert issubclass(YFinancePuller, BasePuller)
        assert YFinancePuller.SOURCE_NAME == "yfinance"


# ---------------------------------------------------------------------------
# safe_inference_context tests
# ---------------------------------------------------------------------------


class TestSafeInferenceContext:
    """PIT safe_inference_context rolls back on lookahead violations."""

    def test_context_yields_data_and_conn(self):
        """Context manager yields (DataFrame, Connection)."""
        engine = MagicMock()
        conn = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        from store.pit import PITStore
        store = PITStore.__new__(PITStore)
        store.engine = engine

        df = pd.DataFrame({
            "feature_id": [1],
            "obs_date": [date(2024, 1, 1)],
            "value": [1.0],
            "release_date": [date(2024, 1, 1)],
            "vintage_date": [date(2024, 1, 1)],
        })

        with patch.object(store, "get_pit", return_value=df):
            with store.safe_inference_context([1], date(2024, 6, 1)) as (result_df, result_conn):
                assert len(result_df) == 1
                assert result_conn is conn

    def test_context_rollback_on_error(self):
        """Transaction is rolled back when ValueError is raised inside context."""
        engine = MagicMock()
        conn = MagicMock()
        exit_mock = MagicMock(return_value=False)
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = exit_mock

        from store.pit import PITStore
        store = PITStore.__new__(PITStore)
        store.engine = engine

        df = pd.DataFrame(columns=["feature_id", "obs_date", "value", "release_date", "vintage_date"])

        with patch.object(store, "get_pit", return_value=df):
            with pytest.raises(ValueError, match="test error"):
                with store.safe_inference_context([1], date(2024, 6, 1)) as (result_df, result_conn):
                    raise ValueError("test error")

        # __exit__ was called (which triggers rollback for begin() context)
        assert exit_mock.called
