"""Unit tests for ingestion/base.py BasePuller and retry logic."""

from __future__ import annotations

import pytest

from ingestion.base import retry_on_failure


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
