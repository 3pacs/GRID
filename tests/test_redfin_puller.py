"""Unit tests for ingestion/altdata/redfin_puller.py."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ingestion.altdata.redfin_puller import (
    RedfinPuller,
    _normalise_region,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_engine() -> MagicMock:
    """Create a mock engine that resolves source_id correctly.

    The first connect().execute().fetchone() must return (1,) so that
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
    source_result.fetchone.return_value = (1,)
    connect_conn.execute.return_value = source_result

    engine.connect.return_value.__enter__ = MagicMock(return_value=connect_conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    return engine


def _sample_tsv_df() -> pd.DataFrame:
    """Create a sample DataFrame mimicking Redfin TSV structure."""
    return pd.DataFrame({
        "period_begin": [
            "2024-01-01", "2024-01-01", "2024-01-01",
            "2024-02-01", "2024-02-01", "2024-02-01",
        ],
        "period_end": [
            "2024-01-07", "2024-01-07", "2024-01-07",
            "2024-02-07", "2024-02-07", "2024-02-07",
        ],
        "region": [
            "National", "New York, NY", "Los Angeles, CA",
            "National", "New York, NY", "Los Angeles, CA",
        ],
        "region_type": [
            "national", "metro", "metro",
            "national", "metro", "metro",
        ],
        "property_type": ["All Residential"] * 6,
        "median_sale_price": [400000, 550000, 750000, 410000, 560000, 770000],
        "inventory": [500000, 30000, 25000, 510000, 42000, 26000],
        "days_on_market": [30, 45, 38, 29, 44, 37],
        "homes_sold": [50000, 5000, 4500, 52000, 5200, 4600],
        "new_listings": [60000, 6000, 5500, 61000, 6100, 5600],
    })


# ---------------------------------------------------------------------------
# _normalise_region tests
# ---------------------------------------------------------------------------

class TestNormaliseRegion:
    def test_basic(self):
        assert _normalise_region("New York") == "new_york"

    def test_with_punctuation(self):
        assert _normalise_region("Washington, D.C.") == "washington_dc"

    def test_strips_whitespace(self):
        assert _normalise_region("  Boston  ") == "boston"


# ---------------------------------------------------------------------------
# RedfinPuller init
# ---------------------------------------------------------------------------

class TestRedfinPullerInit:
    def test_source_name(self):
        engine = _make_mock_engine()
        puller = RedfinPuller(db_engine=engine)
        assert puller.SOURCE_NAME == "Redfin"
        assert puller.source_id == 1

    def test_source_config_is_free(self):
        assert RedfinPuller.SOURCE_CONFIG["cost_tier"] == "FREE"


# ---------------------------------------------------------------------------
# _filter_regions tests
# ---------------------------------------------------------------------------

class TestFilterRegions:
    def test_keeps_national(self):
        engine = _make_mock_engine()
        puller = RedfinPuller(db_engine=engine)
        df = _sample_tsv_df()
        filtered = puller._filter_regions(df)
        assert any("national" in str(r).lower() for r in filtered["region"])

    def test_keeps_top_metros(self):
        engine = _make_mock_engine()
        puller = RedfinPuller(db_engine=engine)
        df = _sample_tsv_df()
        filtered = puller._filter_regions(df)
        regions = filtered["region"].str.lower().tolist()
        assert any("new york" in r for r in regions)
        assert any("los angeles" in r for r in regions)

    def test_filters_out_unknown_regions(self):
        engine = _make_mock_engine()
        puller = RedfinPuller(db_engine=engine)
        df = _sample_tsv_df()
        # Add a row for an unknown region
        extra = pd.DataFrame({
            "period_begin": ["2024-01-01"],
            "period_end": ["2024-01-07"],
            "region": ["Smallville, KS"],
            "region_type": ["metro"],
            "property_type": ["All Residential"],
            "median_sale_price": [100000],
            "inventory": [500],
            "days_on_market": [60],
            "homes_sold": [50],
            "new_listings": [60],
        })
        df = pd.concat([df, extra], ignore_index=True)
        filtered = puller._filter_regions(df)
        assert not any("smallville" in str(r).lower() for r in filtered["region"])

    def test_empty_df(self):
        engine = _make_mock_engine()
        puller = RedfinPuller(db_engine=engine)
        df = pd.DataFrame()
        filtered = puller._filter_regions(df)
        assert filtered.empty


# ---------------------------------------------------------------------------
# _detect_inventory_anomalies tests
# ---------------------------------------------------------------------------

class TestDetectInventoryAnomalies:
    def test_detects_30pct_jump(self):
        engine = _make_mock_engine()
        puller = RedfinPuller(db_engine=engine)
        df = _sample_tsv_df()
        # New York inventory: 30000 -> 42000 = 40% jump
        anomalies = puller._detect_inventory_anomalies(df)
        ny_anomalies = [a for a in anomalies if "New York" in a["region"]]
        assert len(ny_anomalies) == 1
        assert ny_anomalies[0]["pct_change"] == 40.0
        assert ny_anomalies[0]["signal"] == "housing_stress"

    def test_no_anomaly_for_small_change(self):
        engine = _make_mock_engine()
        puller = RedfinPuller(db_engine=engine)
        df = _sample_tsv_df()
        # LA inventory: 25000 -> 26000 = 4% — no anomaly
        anomalies = puller._detect_inventory_anomalies(df)
        la_anomalies = [a for a in anomalies if "Los Angeles" in a["region"]]
        assert len(la_anomalies) == 0

    def test_handles_missing_inventory_column(self):
        engine = _make_mock_engine()
        puller = RedfinPuller(db_engine=engine)
        df = pd.DataFrame({
            "region": ["National"],
            "period_begin": ["2024-01-01"],
            "median_sale_price": [400000],
        })
        anomalies = puller._detect_inventory_anomalies(df)
        assert anomalies == []

    def test_handles_single_period(self):
        engine = _make_mock_engine()
        puller = RedfinPuller(db_engine=engine)
        df = pd.DataFrame({
            "region": ["National"],
            "period_begin": ["2024-01-01"],
            "inventory": [500000],
        })
        anomalies = puller._detect_inventory_anomalies(df)
        assert anomalies == []

    def test_logs_when_inventory_silently_coerced(self, caplog):
        """ATTENTION.md #13: bad inventory values must not be silently
        dropped. Mixed numeric + garbage in the column should trigger a
        log.warning naming the coerced count so an operator sees a
        malformed TSV instead of a silently shrunken anomaly set.
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
            puller = RedfinPuller(db_engine=engine)

            df = pd.DataFrame({
                "region": ["National"] * 4,
                "period_begin": [
                    "2024-01-01", "2024-02-01",
                    "2024-03-01", "2024-04-01",
                ],
                # Two rows are valid; two are unparseable strings that
                # pd.to_numeric will silently coerce to NaN.
                "inventory": ["500000", "n/a", "510000", "--"],
            })
            puller._detect_inventory_anomalies(df)
        finally:
            logger.remove(handler_id)

        messages = [r.getMessage() for r in caplog.records]
        assert any(
            "inventory values coerced to NaN" in m and "2" in m
            for m in messages
        ), messages

    def test_no_log_when_all_inventory_valid(self, caplog):
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
            puller = RedfinPuller(db_engine=engine)
            df = _sample_tsv_df()
            puller._detect_inventory_anomalies(df)
        finally:
            logger.remove(handler_id)

        messages = [r.getMessage() for r in caplog.records]
        assert not any(
            "inventory values coerced to NaN" in m for m in messages
        ), messages


# ---------------------------------------------------------------------------
# _fetch_tsv tests (mocked HTTP)
# ---------------------------------------------------------------------------

class TestFetchTSV:
    @patch("ingestion.altdata.redfin_puller.requests.get")
    def test_parses_tsv(self, mock_get):
        engine = _make_mock_engine()
        puller = RedfinPuller(db_engine=engine)

        tsv_content = "region\tperiod_begin\tmedian_sale_price\nNational\t2024-01-01\t400000\n"
        mock_resp = MagicMock()
        mock_resp.text = tsv_content
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = puller._fetch_tsv()
        assert len(df) == 1
        assert "region" in df.columns

    @patch("ingestion.altdata.redfin_puller.requests.get")
    def test_raises_on_http_error(self, mock_get):
        engine = _make_mock_engine()
        puller = RedfinPuller(db_engine=engine)

        import requests as req
        mock_get.side_effect = req.RequestException("503 Service Unavailable")

        with pytest.raises(req.RequestException):
            puller._fetch_tsv()


# ---------------------------------------------------------------------------
# pull_all integration (mocked fetch + DB)
# ---------------------------------------------------------------------------

class TestPullAll:
    @patch("ingestion.altdata.redfin_puller.ingest_actor")
    @patch.object(RedfinPuller, "_fetch_tsv")
    def test_pull_all_success(self, mock_fetch, mock_ingest):
        engine = _make_mock_engine()

        # Make _row_exists return False (no dedup blocking)
        conn_mock = engine.begin.return_value.__enter__.return_value
        row_check = MagicMock()
        row_check.fetchone.return_value = None
        existing_dates = MagicMock()
        existing_dates.fetchall.return_value = []
        conn_mock.execute.return_value = row_check

        puller = RedfinPuller(db_engine=engine)
        mock_fetch.return_value = _sample_tsv_df()

        result = puller.pull_all(start_date="2023-01-01")
        assert result["status"] in ("SUCCESS", "PARTIAL")
        assert result["source"] == "redfin"
        assert "anomalies" in result

    @patch.object(RedfinPuller, "_fetch_tsv")
    def test_pull_all_handles_fetch_failure(self, mock_fetch):
        engine = _make_mock_engine()
        puller = RedfinPuller(db_engine=engine)

        import requests as req
        mock_fetch.side_effect = req.RequestException("Network error")

        result = puller.pull_all(start_date="2023-01-01")
        assert result["status"] == "FAILED"
        assert "error" in result

    @patch("ingestion.altdata.redfin_puller.ingest_actor")
    @patch.object(RedfinPuller, "_fetch_tsv")
    def test_pull_all_empty_tsv(self, mock_fetch, mock_ingest):
        engine = _make_mock_engine()
        puller = RedfinPuller(db_engine=engine)
        mock_fetch.return_value = pd.DataFrame()

        result = puller.pull_all(start_date="2023-01-01")
        assert result["status"] == "PARTIAL"
        assert result["rows_inserted"] == 0

    @patch("ingestion.altdata.redfin_puller.ingest_actor")
    @patch.object(RedfinPuller, "_fetch_tsv")
    def test_pull_all_string_date(self, mock_fetch, mock_ingest):
        engine = _make_mock_engine()
        puller = RedfinPuller(db_engine=engine)
        mock_fetch.return_value = _sample_tsv_df()

        conn_mock = engine.begin.return_value.__enter__.return_value
        row_check = MagicMock()
        row_check.fetchone.return_value = None
        row_check.fetchall.return_value = []
        conn_mock.execute.return_value = row_check

        result = puller.pull_all(start_date="2023-01-01")
        assert result["source"] == "redfin"


# ---------------------------------------------------------------------------
# Series ID format
# ---------------------------------------------------------------------------

class TestSeriesIdFormat:
    def test_national_format(self):
        assert "redfin:national:median_sale_price" == "redfin:national:median_sale_price"

    def test_metro_format(self):
        region = _normalise_region("New York, NY")
        series_id = f"redfin:{region}:inventory"
        assert series_id == "redfin:new_york_ny:inventory"
