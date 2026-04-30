"""Unit tests for ingestion/base.py BasePuller and retry logic."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ingestion.base import BasePuller, log_pull_failure, retry_on_failure


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
# HTTP-aware retry behavior (added 2026-04-28 to silence the 4xx flood in
# .server-logs/errors.jsonl — 64 CBOE 403s, 322 SEC 429s, etc).
# ---------------------------------------------------------------------------


class _FakeResponse:
    def __init__(self, status_code: int, headers: dict | None = None):
        self.status_code = status_code
        self.headers = headers or {}


class _FakeHTTPError(OSError):
    def __init__(self, status_code: int, headers: dict | None = None):
        super().__init__(f"HTTP {status_code}")
        self.response = _FakeResponse(status_code, headers)


class TestRetryHTTPAware:
    def test_403_does_not_retry(self):
        count = 0

        @retry_on_failure(max_attempts=3, backoff=0.01)
        def forbidden():
            nonlocal count
            count += 1
            raise _FakeHTTPError(403)

        with pytest.raises(_FakeHTTPError):
            forbidden()
        assert count == 1, "403 must short-circuit, not retry"

    def test_404_does_not_retry(self):
        count = 0

        @retry_on_failure(max_attempts=3, backoff=0.01)
        def not_found():
            nonlocal count
            count += 1
            raise _FakeHTTPError(404)

        with pytest.raises(_FakeHTTPError):
            not_found()
        assert count == 1

    def test_429_retries_then_gives_up(self):
        count = 0

        @retry_on_failure(max_attempts=3, backoff=0.01)
        def rate_limited():
            nonlocal count
            count += 1
            raise _FakeHTTPError(429, headers={"Retry-After": "0"})

        with pytest.raises(_FakeHTTPError):
            rate_limited()
        assert count == 3, "429 should consume all attempts"

    def test_429_oversized_retry_after_skips(self):
        count = 0

        @retry_on_failure(max_attempts=3, backoff=0.01)
        def long_wait():
            nonlocal count
            count += 1
            # 600s > 30s cap → bail immediately
            raise _FakeHTTPError(429, headers={"Retry-After": "600"})

        with pytest.raises(_FakeHTTPError):
            long_wait()
        assert count == 1

    def test_500_retries_normally(self):
        count = 0

        @retry_on_failure(max_attempts=3, backoff=0.01)
        def server_err():
            nonlocal count
            count += 1
            if count < 2:
                raise _FakeHTTPError(500)
            return "ok"

        assert server_err() == "ok"
        assert count == 2

    def test_status_extracted_from_chained_exception(self):
        count = 0

        @retry_on_failure(max_attempts=3, backoff=0.01)
        def chained():
            nonlocal count
            count += 1
            try:
                raise _FakeHTTPError(403)
            except _FakeHTTPError as inner:
                raise OSError("wrapped") from inner

        with pytest.raises(OSError):
            chained()
        assert count == 1, "403 inside a chained exception still short-circuits"


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


# ---------------------------------------------------------------------------
# log_pull_failure classifier tests
# ---------------------------------------------------------------------------


class TestLogPullFailure:
    """Verify the bug-vs-external classification routes to the right log level.

    Drives the rule that bug-shaped exceptions (KeyError, AttributeError, …)
    log at ERROR — they need to surface in errors.jsonl. Everything else
    (ConnectionError, HTTPError, ValueError-from-parsing, …) logs at WARNING
    so transient upstream issues don't drown the genuine bugs that did the
    `.server-logs/errors.jsonl` 4,560-row flood (PR #61).
    """

    def _capture(self, exc):
        from loguru import logger
        records: list[tuple[str, str]] = []
        sink_id = logger.add(
            lambda msg: records.append(
                (msg.record["level"].name, msg.record["message"])
            ),
            level="WARNING",
        )
        try:
            log_pull_failure("TestSrc", "thing", exc)
        finally:
            logger.remove(sink_id)
        assert records, "expected exactly one log record"
        return records[-1]

    @pytest.mark.parametrize("exc", [
        KeyError("date"),
        AttributeError("'str' object has no attribute 'get'"),
        IndexError("list out of range"),
        NameError("undefined"),
        ImportError("missing dep"),
    ])
    def test_code_bugs_log_at_error(self, exc):
        level, _ = self._capture(exc)
        assert level == "ERROR", (
            f"{type(exc).__name__} must log at ERROR — these are code bugs "
            f"that need to surface in errors.jsonl"
        )

    @pytest.mark.parametrize("exc", [
        ConnectionError("upstream down"),
        TimeoutError("slow"),
        OSError("network"),
        ValueError("bad json from api"),
        RuntimeError("retry exhausted"),
    ])
    def test_external_failures_log_at_warning(self, exc):
        level, _ = self._capture(exc)
        assert level == "WARNING", (
            f"{type(exc).__name__} must log at WARNING — external/transient "
            f"failures should not pollute errors.jsonl"
        )

    def test_message_includes_source_and_target(self):
        _, msg = self._capture(ConnectionError("boom"))
        assert "TestSrc" in msg
        assert "thing" in msg
        assert "boom" in msg


# ---------------------------------------------------------------------------
# BCB defensive parsing — guards against the `'str' object has no attribute
# 'get'` bug that produced 60 ERROR rows in errors.jsonl.
# ---------------------------------------------------------------------------


class TestBCBNonDictGuard:
    def _make_puller(self):
        from ingestion.international.bcb import BCBPuller
        puller = BCBPuller.__new__(BCBPuller)
        puller.source_id = 1
        puller.engine = MagicMock()
        return puller

    def test_string_payload_skips_cleanly(self):
        puller = self._make_puller()
        # BCB sometimes returns a bare error string instead of [{...}]
        with patch.object(puller, "_fetch_series_data", return_value="error"):
            res = puller.pull_series(11, "2024-01-01")
        assert res["status"] == "SKIPPED"
        assert any("Non-list" in e for e in res["errors"])

    def test_dict_payload_skips_cleanly(self):
        puller = self._make_puller()
        # BCB error envelope shape: {"error": "..."}
        with patch.object(puller, "_fetch_series_data", return_value={"error": "x"}):
            res = puller.pull_series(11, "2024-01-01")
        assert res["status"] == "SKIPPED"

    def test_mixed_list_with_non_dict_entries_does_not_crash(self):
        """List of one valid record + one stray string survives without raising."""
        puller = self._make_puller()
        good = {"data": "01/01/2024", "valor": "1.23"}
        # Mixed payload would have crashed pre-fix with AttributeError
        # on `.get` — verify it now skips the non-dict entry instead.
        engine = MagicMock()
        conn = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        puller.engine = engine
        with patch.object(puller, "_fetch_series_data", return_value=[good, "stray"]), \
             patch.object(puller, "_row_exists", return_value=False):
            res = puller.pull_series(11, "2024-01-01")
        assert res["status"] == "SUCCESS"
