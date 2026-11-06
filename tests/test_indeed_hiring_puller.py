"""Unit tests for ingestion/altdata/indeed_hiring_puller.py."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ingestion.altdata.indeed_hiring_puller import (
    IndeedHiringPuller,
    _normalise_sector,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_engine() -> MagicMock:
    """Create a mock engine that resolves source_id correctly.

    The first connect().execute().fetchone() must return (2,) so that
    _resolve_source_id finds the source on the initial lookup.
    """
    engine = MagicMock()

    # Default mock connection for begin() — used by _insert_raw, etc.
    begin_conn = MagicMock()
    begin_result = MagicMock()
    begin_result.fetchone.return_value = None  # _row_exists returns False
    begin_result.fetchall.return_value = []    # _get_existing_dates returns empty
    begin_conn.execute.return_value = begin_result

    engine.begin.return_value.__enter__ = MagicMock(return_value=begin_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    # connect() is used by _resolve_source_id — must return (id,) on first call
    connect_conn = MagicMock()
    source_result = MagicMock()
    source_result.fetchone.return_value = (2,)
    connect_conn.execute.return_value = source_result

    engine.connect.return_value.__enter__ = MagicMock(return_value=connect_conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    return engine


def _sample_aggregate_df() -> pd.DataFrame:
    """Create a sample aggregate job postings DataFrame."""
    return pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=12, freq="W"),
        "indeed_job_postings_index": [
            100, 102, 104, 103, 105, 107,
            108, 110, 95, 85, 82, 80,  # Sharp drop from 110 -> 95 -> 85
        ],
    })


def _sample_sector_df() -> pd.DataFrame:
    """Create a sample sector-level DataFrame."""
    dates = pd.date_range("2024-01-01", periods=4, freq="W")
    return pd.DataFrame({
        "date": dates,
        "Software Development": [120, 122, 118, 115],
        "Healthcare": [95, 97, 98, 99],
        "Retail & Food Service": [88, 85, 83, 80],
    })


# ---------------------------------------------------------------------------
# _normalise_sector tests
# ---------------------------------------------------------------------------

class TestNormaliseSector:
    def test_basic(self):
        assert _normalise_sector("Software Development") == "software_development"

    def test_with_ampersand(self):
        assert _normalise_sector("Retail & Food Service") == "retail_and_food_service"

    def test_with_slash(self):
        assert _normalise_sector("Arts/Entertainment") == "arts_entertainment"

    def test_with_parens(self):
        assert _normalise_sector("Banking (Finance)") == "banking_finance"

    def test_strips_whitespace(self):
        assert _normalise_sector("  Tech  ") == "tech"


# ---------------------------------------------------------------------------
# IndeedHiringPuller init
# ---------------------------------------------------------------------------

class TestIndeedHiringPullerInit:
    def test_source_name(self):
        engine = _make_mock_engine()
        puller = IndeedHiringPuller(db_engine=engine)
        assert puller.SOURCE_NAME == "Indeed_Hiring_Lab"
        assert puller.source_id == 2

    def test_source_config_is_free(self):
        assert IndeedHiringPuller.SOURCE_CONFIG["cost_tier"] == "FREE"


# ---------------------------------------------------------------------------
# _fetch_csv tests (mocked HTTP)
# ---------------------------------------------------------------------------

class TestFetchCSV:
    @patch("ingestion.altdata.indeed_hiring_puller.requests.get")
    def test_parses_csv(self, mock_get):
        engine = _make_mock_engine()
        puller = IndeedHiringPuller(db_engine=engine)

        csv_content = "date,indeed_job_postings_index\n2024-01-01,100\n2024-01-08,102\n"
        mock_resp = MagicMock()
        mock_resp.text = csv_content
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = puller._fetch_csv("https://example.com/test.csv")
        assert len(df) == 2
        assert "date" in df.columns
        assert "indeed_job_postings_index" in df.columns

    @patch("ingestion.altdata.indeed_hiring_puller.requests.get")
    def test_raises_on_http_error(self, mock_get):
        engine = _make_mock_engine()
        puller = IndeedHiringPuller(db_engine=engine)

        import requests as req
        mock_get.side_effect = req.RequestException("404 Not Found")

        with pytest.raises(req.RequestException):
            puller._fetch_csv("https://example.com/missing.csv")


# ---------------------------------------------------------------------------
# _pull_aggregate tests
# ---------------------------------------------------------------------------

class TestPullAggregate:
    @patch("ingestion.altdata.indeed_hiring_puller.ingest_actor")
    @patch.object(IndeedHiringPuller, "_fetch_csv")
    def test_stores_aggregate_data(self, mock_fetch, mock_ingest):
        engine = _make_mock_engine()
        conn_mock = engine.begin.return_value.__enter__.return_value
        row_check = MagicMock()
        row_check.fetchone.return_value = None
        row_check.fetchall.return_value = []
        conn_mock.execute.return_value = row_check

        puller = IndeedHiringPuller(db_engine=engine)
        mock_fetch.return_value = _sample_aggregate_df()

        result = puller._pull_aggregate(date(2024, 1, 1))
        assert result["source"] == "indeed_aggregate"
        assert result["status"] in ("SUCCESS", "PARTIAL")
        assert result["rows_inserted"] >= 0

    @patch.object(IndeedHiringPuller, "_fetch_csv")
    def test_detects_hiring_freeze(self, mock_fetch):
        engine = _make_mock_engine()
        conn_mock = engine.begin.return_value.__enter__.return_value
        row_check = MagicMock()
        row_check.fetchone.return_value = None
        row_check.fetchall.return_value = []
        conn_mock.execute.return_value = row_check

        puller = IndeedHiringPuller(db_engine=engine)
        mock_fetch.return_value = _sample_aggregate_df()

        result = puller._pull_aggregate(date(2024, 1, 1))
        anomalies = result.get("anomalies", [])
        # 110 -> 95 is a ~13.6% drop, should trigger
        freeze_signals = [a for a in anomalies if a["signal"] == "hiring_freeze"]
        assert len(freeze_signals) >= 1

    @patch.object(IndeedHiringPuller, "_fetch_csv")
    def test_handles_empty_csv(self, mock_fetch):
        engine = _make_mock_engine()
        puller = IndeedHiringPuller(db_engine=engine)
        mock_fetch.return_value = pd.DataFrame()

        result = puller._pull_aggregate(date(2024, 1, 1))
        assert result["status"] == "PARTIAL"
        assert result["rows_inserted"] == 0

    @patch.object(IndeedHiringPuller, "_fetch_csv")
    def test_handles_fetch_failure(self, mock_fetch):
        engine = _make_mock_engine()
        puller = IndeedHiringPuller(db_engine=engine)

        import requests as req
        mock_fetch.side_effect = req.RequestException("Connection refused")

        result = puller._pull_aggregate(date(2024, 1, 1))
        assert result["status"] == "FAILED"


# ---------------------------------------------------------------------------
# _pull_sectors tests
# ---------------------------------------------------------------------------

class TestPullSectors:
    @patch("ingestion.altdata.indeed_hiring_puller.ingest_actor")
    @patch.object(IndeedHiringPuller, "_fetch_csv")
    def test_stores_sector_data(self, mock_fetch, mock_ingest):
        engine = _make_mock_engine()
        conn_mock = engine.begin.return_value.__enter__.return_value
        row_check = MagicMock()
        row_check.fetchone.return_value = None
        row_check.fetchall.return_value = []
        conn_mock.execute.return_value = row_check

        puller = IndeedHiringPuller(db_engine=engine)
        mock_fetch.return_value = _sample_sector_df()

        result = puller._pull_sectors(date(2024, 1, 1))
        assert result["source"] == "indeed_sectors"
        assert result["status"] in ("SUCCESS", "PARTIAL")
        assert "sectors" in result

    @patch("ingestion.altdata.indeed_hiring_puller.ingest_actor")
    @patch.object(IndeedHiringPuller, "_fetch_csv")
    def test_processes_all_sectors(self, mock_fetch, mock_ingest):
        engine = _make_mock_engine()
        conn_mock = engine.begin.return_value.__enter__.return_value
        row_check = MagicMock()
        row_check.fetchone.return_value = None
        row_check.fetchall.return_value = []
        conn_mock.execute.return_value = row_check

        puller = IndeedHiringPuller(db_engine=engine)
        mock_fetch.return_value = _sample_sector_df()

        result = puller._pull_sectors(date(2024, 1, 1))
        sectors = result.get("sectors", [])
        sector_lower = [s.lower() for s in sectors]
        assert "software development" in sector_lower
        assert "healthcare" in sector_lower
        assert "retail & food service" in sector_lower

    @patch("ingestion.altdata.indeed_hiring_puller.ingest_actor")
    @patch.object(IndeedHiringPuller, "_fetch_csv")
    def test_ingests_sector_actors(self, mock_fetch, mock_ingest):
        engine = _make_mock_engine()
        conn_mock = engine.begin.return_value.__enter__.return_value
        row_check = MagicMock()
        row_check.fetchone.return_value = None
        row_check.fetchall.return_value = []
        conn_mock.execute.return_value = row_check

        puller = IndeedHiringPuller(db_engine=engine)
        mock_fetch.return_value = _sample_sector_df()

        puller._pull_sectors(date(2024, 1, 1))
        # Should have called ingest_actor for each sector
        assert mock_ingest.call_count >= 3

    @patch.object(IndeedHiringPuller, "_fetch_csv")
    def test_handles_empty_csv(self, mock_fetch):
        engine = _make_mock_engine()
        puller = IndeedHiringPuller(db_engine=engine)
        mock_fetch.return_value = pd.DataFrame()

        result = puller._pull_sectors(date(2024, 1, 1))
        assert result["status"] == "PARTIAL"

    @patch("ingestion.altdata.indeed_hiring_puller.ingest_actor")
    @patch.object(IndeedHiringPuller, "_fetch_csv")
    def test_logs_when_sector_values_silently_coerced(
        self, mock_fetch, mock_ingest, caplog
    ):
        """ATTENTION.md #13: bad sector values must not be silently dropped.

        Mixed numeric + garbage in a sector column should trigger a
        log.warning naming the coerced count and the sector, so an
        operator sees a malformed CSV instead of a silently shrunken
        dataset.
        """
        from loguru import logger
        import logging

        # Loguru -> stdlib bridge so pytest's caplog captures messages.
        class _Bridge(logging.Handler):
            def emit(self, record):  # noqa: D401
                logging.getLogger(record.name).handle(record)

        handler_id = logger.add(_Bridge(), level="WARNING", format="{message}")
        try:
            caplog.set_level(logging.WARNING)
            engine = _make_mock_engine()
            conn_mock = engine.begin.return_value.__enter__.return_value
            row_check = MagicMock()
            row_check.fetchone.return_value = None
            row_check.fetchall.return_value = []
            conn_mock.execute.return_value = row_check

            puller = IndeedHiringPuller(db_engine=engine)
            dates = pd.date_range("2024-01-01", periods=4, freq="W")
            mock_fetch.return_value = pd.DataFrame({
                "date": dates,
                # Two valid floats, two unparseable strings → coerce to NaN.
                "Software Development": ["120", "n/a", "118", "--"],
            })

            puller._pull_sectors(date(2024, 1, 1))
        finally:
            logger.remove(handler_id)

        messages = [r.getMessage() for r in caplog.records]
        assert any(
            "non-numeric values to NaN for indeed sector" in m
            and "software development" in m
            and "2" in m
            for m in messages
        ), messages

    @patch("ingestion.altdata.indeed_hiring_puller.ingest_actor")
    @patch.object(IndeedHiringPuller, "_fetch_csv")
    def test_no_log_when_all_sector_values_valid(
        self, mock_fetch, mock_ingest, caplog
    ):
        """Conversely, a clean DataFrame must not emit a coerce warning."""
        from loguru import logger
        import logging

        class _Bridge(logging.Handler):
            def emit(self, record):  # noqa: D401
                logging.getLogger(record.name).handle(record)

        handler_id = logger.add(_Bridge(), level="WARNING", format="{message}")
        try:
            caplog.set_level(logging.WARNING)
            engine = _make_mock_engine()
            conn_mock = engine.begin.return_value.__enter__.return_value
            row_check = MagicMock()
            row_check.fetchone.return_value = None
            row_check.fetchall.return_value = []
            conn_mock.execute.return_value = row_check

            puller = IndeedHiringPuller(db_engine=engine)
            mock_fetch.return_value = _sample_sector_df()

            puller._pull_sectors(date(2024, 1, 1))
        finally:
            logger.remove(handler_id)

        messages = [r.getMessage() for r in caplog.records]
        assert not any(
            "non-numeric values to NaN for indeed sector" in m
            for m in messages
        ), messages


# ---------------------------------------------------------------------------
# pull_all integration (mocked)
# ---------------------------------------------------------------------------

class TestPullAll:
    @patch("ingestion.altdata.indeed_hiring_puller.ingest_actor")
    @patch.object(IndeedHiringPuller, "_fetch_csv")
    def test_pull_all_success(self, mock_fetch, mock_ingest):
        engine = _make_mock_engine()
        conn_mock = engine.begin.return_value.__enter__.return_value
        row_check = MagicMock()
        row_check.fetchone.return_value = None
        row_check.fetchall.return_value = []
        conn_mock.execute.return_value = row_check

        puller = IndeedHiringPuller(db_engine=engine)

        # Return aggregate for first call, sectors for second
        mock_fetch.side_effect = [
            _sample_aggregate_df(),
            _sample_sector_df(),
        ]

        results = puller.pull_all(start_date="2024-01-01")
        assert len(results) == 2
        sources = [r["source"] for r in results]
        assert "indeed_aggregate" in sources
        assert "indeed_sectors" in sources

    @patch("ingestion.altdata.indeed_hiring_puller.ingest_actor")
    @patch.object(IndeedHiringPuller, "_fetch_csv")
    def test_pull_all_partial_failure(self, mock_fetch, mock_ingest):
        """One source fails but the other succeeds."""
        engine = _make_mock_engine()
        conn_mock = engine.begin.return_value.__enter__.return_value
        row_check = MagicMock()
        row_check.fetchone.return_value = None
        row_check.fetchall.return_value = []
        conn_mock.execute.return_value = row_check

        puller = IndeedHiringPuller(db_engine=engine)

        import requests as req
        mock_fetch.side_effect = [
            _sample_aggregate_df(),  # aggregate succeeds
            req.RequestException("Sector download failed"),  # sectors fail
        ]

        results = puller.pull_all(start_date="2024-01-01")
        assert len(results) == 2
        statuses = {r["source"]: r["status"] for r in results}
        assert statuses["indeed_aggregate"] in ("SUCCESS", "PARTIAL")
        assert statuses["indeed_sectors"] == "FAILED"

    @patch("ingestion.altdata.indeed_hiring_puller.ingest_actor")
    @patch.object(IndeedHiringPuller, "_fetch_csv")
    def test_pull_all_string_date(self, mock_fetch, mock_ingest):
        engine = _make_mock_engine()
        conn_mock = engine.begin.return_value.__enter__.return_value
        row_check = MagicMock()
        row_check.fetchone.return_value = None
        row_check.fetchall.return_value = []
        conn_mock.execute.return_value = row_check

        puller = IndeedHiringPuller(db_engine=engine)
        mock_fetch.side_effect = [
            _sample_aggregate_df(),
            _sample_sector_df(),
        ]

        results = puller.pull_all(start_date="2024-01-01")
        assert isinstance(results, list)


# ---------------------------------------------------------------------------
# Series ID format
# ---------------------------------------------------------------------------

class TestSeriesIdFormat:
    def test_aggregate_format(self):
        assert "indeed:us:aggregate_postings" == "indeed:us:aggregate_postings"

    def test_sector_format(self):
        sector = _normalise_sector("Software Development")
        series_id = f"indeed:us:sector:{sector}"
        assert series_id == "indeed:us:sector:software_development"

    def test_sector_with_special_chars(self):
        sector = _normalise_sector("Retail & Food Service")
        series_id = f"indeed:us:sector:{sector}"
        assert series_id == "indeed:us:sector:retail_and_food_service"


# ---------------------------------------------------------------------------
# Current Hiring Lab layout (job_postings_tracker, long format) -- 2026-11-06
# ---------------------------------------------------------------------------
#
# The hiring-lab-data-library paths 404 and the tracker files are LONG
# format. Rows below are copied from the live files on 2026-11-06.

_TRACKER_AGGREGATE_CSV = """date,jobcountry,indeed_job_postings_index_SA,indeed_job_postings_index_NSA,variable
2026-10-29,US,103.79,109.18,total postings
2026-10-30,US,103.79,109.16,total postings
2026-10-29,US,99.54,97.92,new postings
2026-10-30,US,99.03,97.29,new postings
"""

_TRACKER_SECTOR_CSV = """date,jobcountry,indeed_job_postings_index,variable,display_name
2026-10-29,US,106.76,total postings,Accounting
2026-10-29,US,75.15,new postings,Accounting
2026-10-29,US,81.49,total postings,Software Development
2026-10-29,US,87.85,new postings,Software Development
2026-10-30,US,106.77,total postings,Accounting
2026-10-30,US,74.67,new postings,Accounting
2026-10-30,US,81.58,total postings,Software Development
2026-10-30,US,87.61,new postings,Software Development
"""


def _tracker_df(text: str) -> pd.DataFrame:
    import io

    return pd.read_csv(io.StringIO(text))


def _tracker_fetch(url: str) -> pd.DataFrame:
    return _tracker_df(_TRACKER_AGGREGATE_CSV if "aggregate" in url else _TRACKER_SECTOR_CSV)


class TestTrackerLayout:
    def test_urls_point_at_the_live_tracker_repo(self):
        from ingestion.altdata import indeed_hiring_puller as mod

        assert mod._AGGREGATE_URL == (
            "https://raw.githubusercontent.com/hiring-lab/job_postings_tracker/master/US/"
            "aggregate_job_postings_US.csv"
        )
        assert mod._SECTOR_URL.endswith("/job_postings_tracker/master/US/job_postings_by_sector_US.csv")

    def test_aggregate_long_format_keeps_sa_total_postings(self):
        from ingestion.altdata.indeed_hiring_puller import _to_wide_aggregate

        wide = _to_wide_aggregate(_tracker_df(_TRACKER_AGGREGATE_CSV))
        assert list(wide.columns) == ["date", "indeed_job_postings_index"]
        assert wide["indeed_job_postings_index"].tolist() == [103.79, 103.79]  # SA, total only

    def test_sector_long_format_pivots_display_names(self):
        from ingestion.altdata.indeed_hiring_puller import _to_wide_sectors

        wide = _to_wide_sectors(_tracker_df(_TRACKER_SECTOR_CSV))
        assert set(wide.columns) == {"date", "Accounting", "Software Development"}
        assert wide["Accounting"].tolist() == [106.76, 106.77]  # total postings only

    def test_legacy_wide_frames_pass_through(self):
        from ingestion.altdata.indeed_hiring_puller import _to_wide_aggregate, _to_wide_sectors

        agg, sec = _sample_aggregate_df(), _sample_sector_df()
        assert _to_wide_aggregate(agg) is agg
        assert _to_wide_sectors(sec) is sec

    @patch("ingestion.altdata.indeed_hiring_puller.ingest_actor")
    @patch.object(IndeedHiringPuller, "_fetch_csv")
    def test_pull_all_on_tracker_files(self, mock_fetch, mock_ingest):
        from ingestion.smart_scheduler import _classify_outcome

        engine = _make_mock_engine()
        puller = IndeedHiringPuller(db_engine=engine)
        mock_fetch.side_effect = _tracker_fetch
        results = puller.pull_all(start_date="2026-10-01")
        by_source = {r["source"]: r for r in results}
        assert by_source["indeed_aggregate"]["status"] == "SUCCESS"
        assert by_source["indeed_aggregate"]["rows_inserted"] == 2
        assert by_source["indeed_sectors"]["status"] == "SUCCESS"
        assert by_source["indeed_sectors"]["rows_inserted"] == 4
        assert sorted(by_source["indeed_sectors"]["sectors"]) == ["accounting", "software development"]
        assert _classify_outcome(results)[:2] == ("SUCCESS", 6)

    @patch("ingestion.altdata.indeed_hiring_puller.ingest_actor")
    @patch.object(IndeedHiringPuller, "_fetch_csv")
    def test_everything_already_stored_is_no_new_data(self, mock_fetch, mock_ingest):
        from ingestion.smart_scheduler import _classify_outcome

        engine = _make_mock_engine()
        puller = IndeedHiringPuller(db_engine=engine)
        mock_fetch.side_effect = _tracker_fetch
        with patch.object(IndeedHiringPuller, "_get_existing_dates",
                          return_value={date(2026, 10, 29), date(2026, 10, 30)}):
            results = puller.pull_all(start_date="2026-10-01")
        assert {r["status"] for r in results} == {"NO_NEW_DATA"}
        assert _classify_outcome(results)[0] == "NO_NEW_DATA"

    @patch.object(IndeedHiringPuller, "_fetch_csv")
    def test_unreadable_values_fail_instead_of_no_new_data(self, mock_fetch):
        engine = _make_mock_engine()
        puller = IndeedHiringPuller(db_engine=engine)
        mock_fetch.return_value = pd.DataFrame({"date": ["2026-10-30"], "jobcountry": ["US"]})
        result = puller._pull_aggregate(date(2026, 10, 1))
        assert result["status"] == "FAILED"
