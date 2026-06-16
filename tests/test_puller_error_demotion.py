"""Regression tests for ingestion error-log hygiene.

Each test pins the rule from CLAUDE.md (`log.error` is reserved for
unhandled application bugs — transient HTTP / network / parsing
failures must use `log.warning`) for the pullers most likely to drift:

* ``ingestion/altdata/defi_llama_puller.py``
* ``ingestion/altdata/uspto_puller.py``
* ``ingestion/altdata/earnings_puller.py``

Each was previously logging upstream 404/503/JSON-decode failures at
ERROR, contributing dozens of rows to ``.server-logs/errors.jsonl``
per week. We now route through ``ingestion.base.log_pull_failure``
which classifies by exception type. These tests fail if anyone
re-introduces a coarse ``log.error`` around upstream calls.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from loguru import logger


def _capture_levels(fn) -> list[tuple[str, str]]:
    """Run *fn* with a loguru sink installed; return [(level, message), ...]."""
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


def _http_error(status: int) -> requests.HTTPError:
    """Build a requests.HTTPError whose .response has a real status_code."""
    resp = requests.Response()
    resp.status_code = status
    err = requests.HTTPError(f"{status} simulated", response=resp)
    return err


def _mock_engine_with_source(source_id: int = 1):
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.fetchone.return_value = (source_id,)
    conn.execute.return_value.fetchall.return_value = []
    return engine


# ---------------------------------------------------------------------------
# DeFi Llama puller — 404s on /stablecoins and /bridges must not be ERROR
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDefiLlamaErrorDemotion:
    def _build(self):
        from ingestion.altdata.defi_llama_puller import DefiLlamaPuller

        return DefiLlamaPuller(db_engine=_mock_engine_with_source())

    @pytest.mark.parametrize("method_name", ["pull_protocols", "pull_stablecoins", "pull_bridges"])
    def test_upstream_404_logs_warning_not_error(self, method_name):
        puller = self._build()
        with patch.object(puller, "_fetch_json", side_effect=_http_error(404)):
            records = _capture_levels(getattr(puller, method_name))

        assert records, f"{method_name} must log when upstream fails"
        levels = {lvl for lvl, _ in records}
        assert "ERROR" not in levels, (
            f"DeFi Llama {method_name} upstream 404 is transient; logging it "
            f"at ERROR re-introduces the errors.jsonl flood. Use "
            f"log_pull_failure (severity-aware) instead. Got: {records}"
        )
        assert "WARNING" in levels


# ---------------------------------------------------------------------------
# USPTO puller — 503 is upstream maintenance, must be WARNING
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestUSPTOErrorDemotion:
    def test_upstream_503_logs_warning_not_error(self):
        from ingestion.altdata.uspto_puller import USPTOPuller

        puller = USPTOPuller(db_engine=_mock_engine_with_source())

        with patch.object(puller, "_row_exists", return_value=False), \
             patch.object(puller, "_search", side_effect=_http_error(503)), \
             patch("ingestion.altdata.uspto_puller.time.sleep", return_value=None):
            records = _capture_levels(puller.pull)

        levels = {lvl for lvl, _ in records}
        assert "ERROR" not in levels, (
            "USPTO 503s are upstream-maintenance noise; logging them at "
            f"ERROR was the original bug (≈30 rows/wk). Got: {records}"
        )
        # At least one keyword failure should still log at WARNING.
        assert "WARNING" in levels


# ---------------------------------------------------------------------------
# Earnings puller — yfinance noise must not log ERROR; one bad row must not
# poison the per-ticker transaction
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestEarningsErrorDemotion:
    def test_upstream_yfinance_failure_logs_warning_not_error(self):
        from ingestion.altdata.earnings_puller import EarningsPuller

        puller = EarningsPuller(db_engine=_mock_engine_with_source())

        # yfinance throws plain RuntimeError on rate-limit; we want WARNING.
        with patch.object(
            puller, "_fetch_ticker_data",
            side_effect=RuntimeError("yfinance rate limit"),
        ):
            records = _capture_levels(lambda: puller.pull_ticker("AAPL"))

        levels = {lvl for lvl, _ in records}
        assert "ERROR" not in levels, (
            f"yfinance transient failures are upstream noise. Got: {records}"
        )
        assert "WARNING" in levels

    def test_savepoint_isolates_failed_row_from_outer_transaction(self):
        """A failed _insert_raw must not poison the per-ticker transaction.

        Reproduces the May 6-8 `psycopg2.errors.InFailedSqlTransaction`
        bug (6 ERRORs/wk). Before the savepoint fix, one failed insert
        aborted the whole `with engine.begin()` block.
        """
        from ingestion.altdata.earnings_puller import EarningsPuller

        engine = _mock_engine_with_source()
        # The connection's begin_nested() returns a context-managed savepoint.
        sp = MagicMock()
        conn = engine.begin.return_value.__enter__.return_value
        conn.begin_nested.return_value = sp

        puller = EarningsPuller(db_engine=engine)

        # Calling _store_series_point with a failing _insert_raw should
        # return False and call sp.rollback() rather than re-raising.
        with patch.object(puller, "_insert_raw", side_effect=Exception("boom")):
            ok = puller._store_series_point(
                conn=conn,
                ticker="AAPL",
                field="eps_actual",
                obs_date=__import__("datetime").date(2026, 5, 1),
                value=1.23,
            )

        assert ok is False, (
            "savepoint should swallow row-level failure and return False"
        )
        sp.rollback.assert_called_once()
        sp.commit.assert_not_called()


@pytest.mark.unit
class TestInstitutionalFlowsLogHygiene:
    def test_yfinance_internal_logger_is_suppressed(self):
        import inspect
        from ingestion.altdata import institutional_flows

        source = inspect.getsource(institutional_flows)

        assert 'logging.getLogger("yfinance").setLevel(logging.CRITICAL)' in source
