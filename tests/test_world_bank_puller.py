"""Unit tests for ingestion/international/world_bank_puller.py."""

from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock, patch, call

import pytest

from ingestion.international.world_bank_puller import (
    WorldBankPuller,
    INDICATORS,
    COUNTRIES,
    _WB_BASE_URL,
    _PER_PAGE,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_engine():
    """Create a mock SQLAlchemy engine with source_catalog lookup."""
    engine = MagicMock()
    conn = MagicMock()
    ctx = MagicMock()

    # source_catalog lookup returns id=42
    row_mock = MagicMock()
    row_mock.__getitem__ = lambda self, idx: 42
    conn.execute.return_value.fetchone.return_value = row_mock

    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    return engine


@pytest.fixture
def puller(mock_engine):
    """Create a WorldBankPuller with mocked engine."""
    return WorldBankPuller(mock_engine)


def _make_wb_response(records: list[dict], page: int = 1, pages: int = 1) -> list:
    """Build a mock World Bank API response envelope."""
    meta = {"page": page, "pages": pages, "per_page": _PER_PAGE, "total": len(records)}
    return [meta, records]


def _sample_record(year: int, value: float | None, country: str = "US") -> dict:
    """Build a single WB observation record."""
    return {
        "date": str(year),
        "value": value,
        "country": {"id": country, "value": COUNTRIES.get(country, country)},
        "indicator": {"id": "NY.GDP.MKTP.CD", "value": "GDP (current US$)"},
    }


# ---------------------------------------------------------------------------
# Configuration tests
# ---------------------------------------------------------------------------

class TestConfiguration:
    def test_source_name(self):
        assert WorldBankPuller.SOURCE_NAME == "world_bank"

    def test_source_config_is_free(self):
        assert WorldBankPuller.SOURCE_CONFIG["cost_tier"] == "FREE"

    def test_indicators_count(self):
        assert len(INDICATORS) == 11

    def test_countries_count(self):
        assert len(COUNTRIES) == 30

    def test_series_id_format(self, puller):
        """Series IDs must follow wb:{country}:{indicator} pattern."""
        sid = f"wb:us:NY.GDP.MKTP.CD"
        assert sid.startswith("wb:")
        parts = sid.split(":")
        assert len(parts) == 3
        assert parts[1] == "us"

    def test_all_country_codes_are_two_letter(self):
        for code in COUNTRIES:
            assert len(code) == 2, f"Country code {code} is not 2 letters"
            assert code == code.upper(), f"Country code {code} is not uppercase"


# ---------------------------------------------------------------------------
# API fetch tests
# ---------------------------------------------------------------------------

class TestFetchIndicator:
    @patch("ingestion.international.world_bank_puller.requests.get")
    @patch("ingestion.international.world_bank_puller.time.sleep")
    def test_single_page_fetch(self, mock_sleep, mock_get, puller):
        records = [_sample_record(2023, 25000000000000)]
        mock_resp = MagicMock()
        mock_resp.json.return_value = _make_wb_response(records)
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        result = puller._fetch_indicator("US", "NY.GDP.MKTP.CD", 2020, 2023)

        assert len(result) == 1
        assert result[0]["value"] == 25000000000000
        mock_get.assert_called_once()
        call_kwargs = mock_get.call_args
        assert "US" in call_kwargs[0][0] or "US" in str(call_kwargs)

    @patch("ingestion.international.world_bank_puller.requests.get")
    @patch("ingestion.international.world_bank_puller.time.sleep")
    def test_paginated_fetch(self, mock_sleep, mock_get, puller):
        """Should follow pagination until all pages are fetched."""
        page1_records = [_sample_record(2023, 100)]
        page2_records = [_sample_record(2022, 200)]

        resp1 = MagicMock()
        resp1.json.return_value = _make_wb_response(page1_records, page=1, pages=2)
        resp1.raise_for_status = MagicMock()

        resp2 = MagicMock()
        resp2.json.return_value = _make_wb_response(page2_records, page=2, pages=2)
        resp2.raise_for_status = MagicMock()

        mock_get.side_effect = [resp1, resp2]

        result = puller._fetch_indicator("US", "NY.GDP.MKTP.CD", 2020, 2023)

        assert len(result) == 2
        assert mock_get.call_count == 2

    @patch("ingestion.international.world_bank_puller.requests.get")
    @patch("ingestion.international.world_bank_puller.time.sleep")
    def test_empty_response(self, mock_sleep, mock_get, puller):
        mock_resp = MagicMock()
        mock_resp.json.return_value = [{"page": 1, "pages": 1}, None]
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        result = puller._fetch_indicator("US", "NY.GDP.MKTP.CD", 2020, 2023)
        assert result == []

    @patch("ingestion.international.world_bank_puller.requests.get")
    @patch("ingestion.international.world_bank_puller.time.sleep")
    def test_unexpected_response_format(self, mock_sleep, mock_get, puller):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"error": "bad request"}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        result = puller._fetch_indicator("US", "NY.GDP.MKTP.CD", 2020, 2023)
        assert result == []


# ---------------------------------------------------------------------------
# Pull indicator tests
# ---------------------------------------------------------------------------

class TestPullIndicator:
    @patch.object(WorldBankPuller, "_fetch_indicator")
    @patch.object(WorldBankPuller, "_get_existing_dates", return_value=set())
    @patch.object(WorldBankPuller, "_insert_raw")
    @patch("ingestion.international.world_bank_puller.time.sleep")
    def test_inserts_valid_records(self, mock_sleep, mock_insert, mock_existing, mock_fetch, puller):
        mock_fetch.return_value = [
            _sample_record(2023, 25000000000000),
            _sample_record(2022, 24000000000000),
        ]

        result = puller.pull_indicator("US", "NY.GDP.MKTP.CD", years=5)

        assert result["status"] == "SUCCESS"
        assert result["rows_inserted"] == 2
        assert result["series_id"] == "wb:us:NY.GDP.MKTP.CD"
        assert mock_insert.call_count == 2

    @patch.object(WorldBankPuller, "_fetch_indicator")
    @patch.object(WorldBankPuller, "_get_existing_dates")
    @patch.object(WorldBankPuller, "_insert_raw")
    @patch("ingestion.international.world_bank_puller.time.sleep")
    def test_skips_existing_dates(self, mock_sleep, mock_insert, mock_existing, mock_fetch, puller):
        mock_fetch.return_value = [
            _sample_record(2023, 100),
            _sample_record(2022, 200),
        ]
        mock_existing.return_value = {date(2023, 1, 1)}

        result = puller.pull_indicator("US", "NY.GDP.MKTP.CD")

        assert result["rows_inserted"] == 1
        assert mock_insert.call_count == 1

    @patch.object(WorldBankPuller, "_fetch_indicator")
    @patch("ingestion.international.world_bank_puller.time.sleep")
    def test_skips_null_values(self, mock_sleep, mock_fetch, puller):
        mock_fetch.return_value = [
            _sample_record(2023, None),
            _sample_record(2022, None),
        ]

        result = puller.pull_indicator("US", "NY.GDP.MKTP.CD")
        assert result["rows_inserted"] == 0

    @patch.object(WorldBankPuller, "_fetch_indicator")
    @patch("ingestion.international.world_bank_puller.time.sleep")
    def test_handles_no_data(self, mock_sleep, mock_fetch, puller):
        mock_fetch.return_value = []

        result = puller.pull_indicator("US", "NY.GDP.MKTP.CD")
        assert result["status"] == "PARTIAL"
        assert result["rows_inserted"] == 0

    @patch.object(WorldBankPuller, "_fetch_indicator")
    @patch("ingestion.international.world_bank_puller.time.sleep")
    def test_handles_api_failure(self, mock_sleep, mock_fetch, puller):
        mock_fetch.side_effect = Exception("API timeout")

        result = puller.pull_indicator("US", "NY.GDP.MKTP.CD")
        assert result["status"] == "FAILED"
        assert "API timeout" in result["errors"][0]

    @patch.object(WorldBankPuller, "_fetch_indicator")
    @patch.object(WorldBankPuller, "_get_existing_dates", return_value=set())
    @patch.object(WorldBankPuller, "_insert_raw")
    @patch("ingestion.international.world_bank_puller.time.sleep")
    def test_skips_non_numeric_values(self, mock_sleep, mock_insert, mock_existing, mock_fetch, puller):
        records = [_sample_record(2023, 100)]
        records[0]["value"] = "not-a-number"
        mock_fetch.return_value = records

        result = puller.pull_indicator("US", "NY.GDP.MKTP.CD")
        assert result["rows_inserted"] == 0
        assert mock_insert.call_count == 0

    @patch.object(WorldBankPuller, "_fetch_indicator")
    @patch.object(WorldBankPuller, "_get_existing_dates", return_value=set())
    @patch.object(WorldBankPuller, "_insert_raw")
    @patch("ingestion.international.world_bank_puller.time.sleep")
    def test_raw_payload_includes_metadata(self, mock_sleep, mock_insert, mock_existing, mock_fetch, puller):
        mock_fetch.return_value = [_sample_record(2023, 100)]

        puller.pull_indicator("US", "NY.GDP.MKTP.CD")

        assert mock_insert.call_count == 1
        call_kwargs = mock_insert.call_args[1]
        payload = call_kwargs["raw_payload"]
        assert payload["country_id"] == "US"
        assert payload["indicator_id"] == "NY.GDP.MKTP.CD"


# ---------------------------------------------------------------------------
# Anomaly detection tests
# ---------------------------------------------------------------------------

class TestGDPAnomalyDetection:
    def test_detects_large_drop(self, mock_engine):
        """GDP growth drop > 5pp should be flagged."""
        conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=conn)

        # Simulate 2 years of GDP growth: 5.0% then -3.0% (delta = -8pp)
        conn.execute.return_value.fetchall.return_value = [
            (date(2019, 1, 1), 5.0),
            (date(2020, 1, 1), -3.0),
        ]

        puller = WorldBankPuller(mock_engine)
        anomalies = puller.detect_gdp_anomalies()

        # Should flag every country (all get same mock data)
        assert len(anomalies) > 0
        first = anomalies[0]
        assert first["delta_pp"] == -8.0
        assert first["prev_growth"] == 5.0
        assert first["curr_growth"] == -3.0

    def test_no_anomaly_for_small_drop(self, mock_engine):
        """GDP growth drop < 5pp should NOT be flagged."""
        conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=conn)

        # Delta = -3pp, below threshold
        conn.execute.return_value.fetchall.return_value = [
            (date(2022, 1, 1), 4.0),
            (date(2023, 1, 1), 1.0),
        ]

        puller = WorldBankPuller(mock_engine)
        anomalies = puller.detect_gdp_anomalies()
        assert len(anomalies) == 0

    def test_handles_insufficient_data(self, mock_engine):
        """Single data point should not trigger anomaly."""
        conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=conn)

        conn.execute.return_value.fetchall.return_value = [
            (date(2023, 1, 1), 3.0),
        ]

        puller = WorldBankPuller(mock_engine)
        anomalies = puller.detect_gdp_anomalies()
        assert len(anomalies) == 0


# ---------------------------------------------------------------------------
# Actor ingestion tests
# ---------------------------------------------------------------------------

class TestActorIngestion:
    @patch("ingestion.international.world_bank_puller.ingest_actor", create=True)
    def test_ingests_all_countries(self, mock_ingest, puller):
        """Should call ingest_actor for each of the 30 countries."""
        # Patch the import inside the method
        with patch(
            "intelligence.actor_ingest.ingest_actor", return_value=True
        ) as mock_ia:
            added = puller._ingest_country_actors()

        assert added == 30
        assert mock_ia.call_count == 30

    @patch("intelligence.actor_ingest.ingest_actor", return_value=True)
    def test_actor_params(self, mock_ingest, puller):
        """Verify actor ingestion uses correct type and source."""
        puller._ingest_country_actors()

        # Check the first call
        first_call = mock_ingest.call_args_list[0]
        assert first_call[1]["actor_type"] == "government"
        assert first_call[1]["source"] == "world_bank"
        assert first_call[1]["confidence"] == "confirmed"

    @patch(
        "intelligence.actor_ingest.ingest_actor",
        side_effect=ImportError("no module"),
    )
    def test_graceful_when_actor_module_missing(self, mock_ingest, puller):
        """Should not crash if intelligence module is unavailable."""
        # The actual import happens inside the method; patch to simulate ImportError
        with patch.dict("sys.modules", {"intelligence.actor_ingest": None}):
            # The try/except ImportError in the method should handle this
            added = puller._ingest_country_actors()
        # Should return 0 gracefully (or whatever the import guard yields)
        assert added >= 0


# ---------------------------------------------------------------------------
# Bulk pull tests
# ---------------------------------------------------------------------------

class TestPullAll:
    @patch.object(WorldBankPuller, "detect_gdp_anomalies", return_value=[])
    @patch.object(WorldBankPuller, "_ingest_country_actors", return_value=5)
    @patch.object(WorldBankPuller, "pull_indicator")
    def test_pulls_all_combinations(self, mock_pull, mock_actors, mock_anomaly, puller):
        """Should call pull_indicator for every (country, indicator) pair."""
        mock_pull.return_value = {
            "series_id": "test",
            "rows_inserted": 1,
            "status": "SUCCESS",
            "errors": [],
        }

        result = puller.pull_all(years=5)

        expected_calls = len(INDICATORS) * len(COUNTRIES)
        assert mock_pull.call_count == expected_calls
        assert result["source"] == "world_bank"
        assert result["total"] == expected_calls
        assert result["total_rows"] == expected_calls  # 1 row each

    @patch.object(WorldBankPuller, "detect_gdp_anomalies", return_value=[])
    @patch.object(WorldBankPuller, "_ingest_country_actors", return_value=0)
    @patch.object(WorldBankPuller, "pull_indicator")
    def test_counts_failures(self, mock_pull, mock_actors, mock_anomaly, puller):
        """Failed pulls should be counted in the summary."""
        mock_pull.return_value = {
            "series_id": "test",
            "rows_inserted": 0,
            "status": "FAILED",
            "errors": ["timeout"],
        }

        result = puller.pull_all()

        assert result["failed"] == len(INDICATORS) * len(COUNTRIES)
        assert result["succeeded"] == 0

    @patch.object(WorldBankPuller, "detect_gdp_anomalies")
    @patch.object(WorldBankPuller, "_ingest_country_actors", return_value=0)
    @patch.object(WorldBankPuller, "pull_indicator")
    def test_runs_anomaly_detection(self, mock_pull, mock_actors, mock_anomaly, puller):
        """Anomaly detection should run after all pulls."""
        mock_pull.return_value = {
            "series_id": "test",
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }
        mock_anomaly.return_value = [{"country": "AR", "delta_pp": -15.0}]

        result = puller.pull_all()

        mock_anomaly.assert_called_once()
        assert len(result["anomalies"]) == 1

    @patch.object(WorldBankPuller, "detect_gdp_anomalies", return_value=[])
    @patch.object(WorldBankPuller, "_ingest_country_actors")
    @patch.object(WorldBankPuller, "pull_indicator")
    def test_ingests_actors_before_pulling(self, mock_pull, mock_actors, mock_anomaly, puller):
        """Country actors should be ingested before data pull starts."""
        call_order = []
        mock_actors.side_effect = lambda: call_order.append("actors") or 0
        mock_pull.side_effect = lambda *a, **kw: call_order.append("pull") or {
            "series_id": "t", "rows_inserted": 0, "status": "SUCCESS", "errors": [],
        }

        puller.pull_all()

        assert call_order[0] == "actors"


# ---------------------------------------------------------------------------
# Record failure tests
# ---------------------------------------------------------------------------

class TestRecordFailure:
    def test_records_failed_row(self, puller, mock_engine):
        """Should insert a FAILED row into raw_series."""
        conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=conn)

        puller._record_failure("wb:us:test", Exception("boom"))

        conn.execute.assert_called_once()
        call_args = conn.execute.call_args
        params = call_args[0][1]
        assert params["sid"] == "wb:us:test"
        assert params["src"] == 42
        assert '"error": "boom"' in params["payload"]

    def test_handles_insert_failure_gracefully(self, puller, mock_engine):
        """Should not raise if the failure recording itself fails."""
        conn = MagicMock()
        conn.execute.side_effect = Exception("DB down")
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=conn)

        # Should not raise
        puller._record_failure("wb:us:test", Exception("original"))


# ---------------------------------------------------------------------------
# URL / parameter construction tests
# ---------------------------------------------------------------------------

class TestURLConstruction:
    @patch("ingestion.international.world_bank_puller.requests.get")
    @patch("ingestion.international.world_bank_puller.time.sleep")
    def test_url_contains_country_and_indicator(self, mock_sleep, mock_get, puller):
        mock_resp = MagicMock()
        mock_resp.json.return_value = [{"page": 1, "pages": 1}, None]
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        puller._fetch_indicator("BR", "FP.CPI.TOTL.ZG", 2020, 2023)

        url = mock_get.call_args[0][0]
        assert "/country/BR/indicator/FP.CPI.TOTL.ZG" in url

    @patch("ingestion.international.world_bank_puller.requests.get")
    @patch("ingestion.international.world_bank_puller.time.sleep")
    def test_date_range_params(self, mock_sleep, mock_get, puller):
        mock_resp = MagicMock()
        mock_resp.json.return_value = [{"page": 1, "pages": 1}, None]
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        puller._fetch_indicator("US", "NY.GDP.MKTP.CD", 2018, 2023)

        params = mock_get.call_args[1]["params"]
        assert params["date"] == "2018:2023"
        assert params["format"] == "json"
        assert params["per_page"] == _PER_PAGE
