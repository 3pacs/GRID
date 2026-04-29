"""Unit tests for ingestion/altdata/cloudflare_radar_puller.py."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch, call

import pytest

from ingestion.altdata.cloudflare_radar_puller import (
    CloudflareRadarPuller,
    TOP_COUNTRIES,
    _ENDPOINTS,
    _TRAFFIC_DROP_THRESHOLD,
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
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value = ctx
    engine.begin.return_value = ctx

    # _resolve_source_id: return source_id=42
    row_mock = MagicMock()
    row_mock.__getitem__ = lambda self, idx: 42
    conn.execute.return_value.fetchone.return_value = row_mock

    return engine


@pytest.fixture
def puller(mock_engine):
    """Create a CloudflareRadarPuller with mocked engine."""
    return CloudflareRadarPuller(mock_engine)


# ---------------------------------------------------------------------------
# Class configuration tests
# ---------------------------------------------------------------------------


class TestCloudflareRadarConfig:
    def test_source_name(self, puller):
        assert puller.SOURCE_NAME == "Cloudflare_Radar"

    def test_source_config_free(self, puller):
        assert puller.SOURCE_CONFIG["cost_tier"] == "FREE"

    def test_source_config_trust(self, puller):
        assert puller.SOURCE_CONFIG["trust_score"] == "HIGH"

    def test_top_countries_count(self):
        assert len(TOP_COUNTRIES) == 30

    def test_top_countries_includes_majors(self):
        for cc in ["US", "CN", "DE", "JP", "GB", "FR", "BR", "IN", "KR", "CA"]:
            assert cc in TOP_COUNTRIES


class TestSeriesId:
    def test_global_http(self, puller):
        assert puller._series_id("http_traffic", "global") == "cf_radar:http_traffic:global"

    def test_country(self, puller):
        assert puller._series_id("traffic", "us") == "cf_radar:traffic:us"

    def test_ddos(self, puller):
        assert puller._series_id("ddos", "global") == "cf_radar:ddos:global"

    def test_anomaly(self, puller):
        assert puller._series_id("anomaly", "evt_123") == "cf_radar:anomaly:evt_123"

    def test_bgp(self, puller):
        assert puller._series_id("bgp", "global") == "cf_radar:bgp:global"


# ---------------------------------------------------------------------------
# HTTP traffic parsing tests
# ---------------------------------------------------------------------------


class TestPullHttpTraffic:
    def test_parses_timestamp_value_format(self, puller):
        """Test parsing when API returns timestamps + values arrays."""
        mock_response = {
            "result": {
                "httpRequests": {
                    "timestamps": [
                        "2026-04-01T00:00:00Z",
                        "2026-04-02T00:00:00Z",
                    ],
                    "values": [100.5, 102.3],
                }
            }
        }
        with patch.object(puller, "_fetch_json", return_value=mock_response):
            rows = puller._pull_http_traffic(days_back=7)

        assert len(rows) == 2
        assert rows[0]["obs_date"] == date(2026, 4, 1)
        assert rows[0]["value"] == 100.5
        assert rows[1]["obs_date"] == date(2026, 4, 2)
        assert rows[1]["value"] == 102.3

    def test_parses_flat_timeseries_format(self, puller):
        """Test parsing when API returns flat timeseries list."""
        mock_response = {
            "result": {
                "timeseries": [
                    {"timestamp": "2026-04-01T00:00:00Z", "value": 95.2},
                    {"timestamp": "2026-04-02T00:00:00Z", "value": 97.8},
                ]
            }
        }
        with patch.object(puller, "_fetch_json", return_value=mock_response):
            rows = puller._pull_http_traffic(days_back=7)

        assert len(rows) == 2
        assert rows[0]["value"] == 95.2

    def test_returns_empty_on_fetch_failure(self, puller):
        """Test graceful degradation on API failure."""
        with patch.object(
            puller, "_fetch_json", side_effect=ConnectionError("down")
        ):
            rows = puller._pull_http_traffic()

        assert rows == []

    def test_skips_bad_timestamps(self, puller):
        """Test that malformed timestamps are skipped."""
        mock_response = {
            "result": {
                "httpRequests": {
                    "timestamps": ["not-a-date", "2026-04-02T00:00:00Z"],
                    "values": [100.0, 102.0],
                }
            }
        }
        with patch.object(puller, "_fetch_json", return_value=mock_response):
            rows = puller._pull_http_traffic(days_back=7)

        assert len(rows) == 1
        assert rows[0]["obs_date"] == date(2026, 4, 2)

    def test_empty_result(self, puller):
        """Test handling of empty API response."""
        mock_response = {"result": {}}
        with patch.object(puller, "_fetch_json", return_value=mock_response):
            rows = puller._pull_http_traffic(days_back=7)

        assert rows == []


# ---------------------------------------------------------------------------
# Country traffic parsing tests
# ---------------------------------------------------------------------------


class TestPullCountryTraffic:
    def test_parses_top_locations(self, puller):
        mock_response = {
            "result": {
                "top_0": [
                    {
                        "clientCountryAlpha2": "US",
                        "clientCountryName": "United States",
                        "value": 25.5,
                    },
                    {
                        "clientCountryAlpha2": "CN",
                        "clientCountryName": "China",
                        "value": 18.2,
                    },
                ]
            }
        }
        with patch.object(puller, "_fetch_json", return_value=mock_response):
            result = puller._pull_country_traffic()

        assert "US" in result
        assert "CN" in result
        assert result["US"][0]["value"] == 25.5

    def test_parses_alternative_key(self, puller):
        """Test parsing when API uses 'locations' key."""
        mock_response = {
            "result": {
                "locations": [
                    {"country": "DE", "name": "Germany", "traffic": 8.1},
                ]
            }
        }
        with patch.object(puller, "_fetch_json", return_value=mock_response):
            result = puller._pull_country_traffic()

        assert "DE" in result
        assert result["DE"][0]["value"] == 8.1

    def test_returns_empty_on_failure(self, puller):
        with patch.object(
            puller, "_fetch_json", side_effect=ConnectionError("down")
        ):
            result = puller._pull_country_traffic()

        assert result == {}

    def test_skips_entries_without_country(self, puller):
        mock_response = {
            "result": {
                "top_0": [
                    {"value": 10.0},  # no country code
                    {"clientCountryAlpha2": "JP", "value": 7.3},
                ]
            }
        }
        with patch.object(puller, "_fetch_json", return_value=mock_response):
            result = puller._pull_country_traffic()

        assert "JP" in result
        assert len(result) == 1


# ---------------------------------------------------------------------------
# DDoS parsing tests
# ---------------------------------------------------------------------------


class TestPullDdosTrends:
    def test_parses_timestamps_values(self, puller):
        mock_response = {
            "result": {
                "timestamps": ["2026-04-01T00:00:00Z"],
                "values": [5000],
            }
        }
        with patch.object(puller, "_fetch_json", return_value=mock_response):
            rows = puller._pull_ddos_trends(days_back=7)

        assert len(rows) == 1
        assert rows[0]["value"] == 5000.0

    def test_parses_flat_timeseries(self, puller):
        mock_response = {
            "result": {
                "timeseries": [
                    {"timestamp": "2026-04-01T00:00:00Z", "attacks": 3200},
                ]
            }
        }
        with patch.object(puller, "_fetch_json", return_value=mock_response):
            rows = puller._pull_ddos_trends(days_back=7)

        assert len(rows) == 1
        assert rows[0]["value"] == 3200.0

    def test_returns_empty_on_failure(self, puller):
        with patch.object(
            puller, "_fetch_json", side_effect=TimeoutError("slow")
        ):
            rows = puller._pull_ddos_trends()

        assert rows == []


# ---------------------------------------------------------------------------
# Anomaly parsing tests
# ---------------------------------------------------------------------------


class TestPullAnomalies:
    def test_parses_anomaly_events(self, puller):
        mock_response = {
            "result": {
                "anomalies": [
                    {
                        "id": "a1",
                        "startDate": "2026-04-01T12:00:00Z",
                        "location": "IR",
                        "status": "ongoing",
                        "magnitude": 85.0,
                        "description": "Internet shutdown in Iran",
                    },
                ]
            }
        }
        with patch.object(puller, "_fetch_json", return_value=mock_response):
            rows = puller._pull_anomalies(days_back=7)

        assert len(rows) == 1
        assert rows[0]["event_id"] == "a1"
        assert rows[0]["obs_date"] == date(2026, 4, 1)
        assert rows[0]["value"] == 85.0
        assert rows[0]["raw_payload"]["location"] == "IR"

    def test_generates_event_id_if_missing(self, puller):
        mock_response = {
            "result": {
                "anomalies": [
                    {
                        "startDate": "2026-04-01T00:00:00Z",
                        "location": "RU",
                        "status": "resolved",
                    },
                ]
            }
        }
        with patch.object(puller, "_fetch_json", return_value=mock_response):
            rows = puller._pull_anomalies(days_back=7)

        assert rows[0]["event_id"] == "evt_0"

    def test_default_magnitude(self, puller):
        """Anomalies without magnitude default to 1.0."""
        mock_response = {
            "result": {
                "anomalies": [
                    {"startDate": "2026-04-01T00:00:00Z"},
                ]
            }
        }
        with patch.object(puller, "_fetch_json", return_value=mock_response):
            rows = puller._pull_anomalies(days_back=7)

        assert rows[0]["value"] == 1.0

    def test_skips_bad_dates(self, puller):
        mock_response = {
            "result": {
                "anomalies": [
                    {"id": "bad", "startDate": "not-a-date"},
                    {"id": "good", "startDate": "2026-04-02T00:00:00Z"},
                ]
            }
        }
        with patch.object(puller, "_fetch_json", return_value=mock_response):
            rows = puller._pull_anomalies(days_back=7)

        assert len(rows) == 1
        assert rows[0]["event_id"] == "good"

    def test_returns_empty_on_failure(self, puller):
        with patch.object(
            puller, "_fetch_json", side_effect=OSError("network")
        ):
            rows = puller._pull_anomalies()

        assert rows == []


# ---------------------------------------------------------------------------
# BGP stats parsing tests
# ---------------------------------------------------------------------------


class TestPullBgpStats:
    def test_parses_routes_total(self, puller):
        mock_response = {
            "result": {
                "routes_total": 950000,
                "routes_origin": 85000,
                "rpki_valid": 40000,
            }
        }
        with patch.object(puller, "_fetch_json", return_value=mock_response):
            rows = puller._pull_bgp_stats()

        assert len(rows) == 1
        assert rows[0]["value"] == 950000.0
        assert rows[0]["obs_date"] == date.today()
        assert rows[0]["raw_payload"]["rpki_valid"] == 40000

    def test_handles_total_key(self, puller):
        mock_response = {"result": {"total": 800000}}
        with patch.object(puller, "_fetch_json", return_value=mock_response):
            rows = puller._pull_bgp_stats()

        assert rows[0]["value"] == 800000.0

    def test_returns_empty_when_no_total(self, puller):
        mock_response = {"result": {"other_field": 123}}
        with patch.object(puller, "_fetch_json", return_value=mock_response):
            rows = puller._pull_bgp_stats()

        assert rows == []

    def test_returns_empty_on_failure(self, puller):
        with patch.object(
            puller, "_fetch_json", side_effect=ConnectionError("err")
        ):
            rows = puller._pull_bgp_stats()

        assert rows == []


# ---------------------------------------------------------------------------
# Anomaly detection logic tests
# ---------------------------------------------------------------------------


class TestDetectTrafficAnomalies:
    def test_detects_large_drop(self, puller):
        """Country with >30% drop should be flagged."""
        country_data = {
            "IR": [
                {"obs_date": date(2026, 4, 1), "value": 100.0},
                {"obs_date": date(2026, 4, 2), "value": 100.0},
                {"obs_date": date(2026, 4, 3), "value": 50.0},  # 50% drop
            ]
        }
        anomalies = puller.detect_traffic_anomalies(country_data)

        assert len(anomalies) == 1
        assert anomalies[0]["country"] == "IR"
        assert anomalies[0]["drop_pct"] == 50.0
        assert anomalies[0]["severity"] == "MEDIUM"

    def test_severe_drop(self, puller):
        """Country with >50% drop should be flagged as HIGH severity."""
        country_data = {
            "SY": [
                {"obs_date": date(2026, 4, 1), "value": 100.0},
                {"obs_date": date(2026, 4, 2), "value": 30.0},  # 70% drop
            ]
        }
        anomalies = puller.detect_traffic_anomalies(country_data)

        assert len(anomalies) == 1
        assert anomalies[0]["severity"] == "HIGH"

    def test_no_anomaly_for_small_drop(self, puller):
        """Country with <30% drop should not be flagged."""
        country_data = {
            "US": [
                {"obs_date": date(2026, 4, 1), "value": 100.0},
                {"obs_date": date(2026, 4, 2), "value": 85.0},  # 15% drop
            ]
        }
        anomalies = puller.detect_traffic_anomalies(country_data)

        assert len(anomalies) == 0

    def test_no_anomaly_for_increase(self, puller):
        """Traffic increase should not trigger anomaly."""
        country_data = {
            "CN": [
                {"obs_date": date(2026, 4, 1), "value": 100.0},
                {"obs_date": date(2026, 4, 2), "value": 120.0},  # increase
            ]
        }
        anomalies = puller.detect_traffic_anomalies(country_data)

        assert len(anomalies) == 0

    def test_skips_single_data_point(self, puller):
        """Need at least 2 data points to detect anomaly."""
        country_data = {
            "XX": [
                {"obs_date": date(2026, 4, 1), "value": 50.0},
            ]
        }
        anomalies = puller.detect_traffic_anomalies(country_data)

        assert len(anomalies) == 0

    def test_skips_zero_mean(self, puller):
        """Avoid division by zero when mean prior is 0."""
        country_data = {
            "XX": [
                {"obs_date": date(2026, 4, 1), "value": 0.0},
                {"obs_date": date(2026, 4, 2), "value": 0.0},
            ]
        }
        anomalies = puller.detect_traffic_anomalies(country_data)

        assert len(anomalies) == 0

    def test_multiple_countries(self, puller):
        """Test detection across multiple countries."""
        country_data = {
            "IR": [
                {"obs_date": date(2026, 4, 1), "value": 100.0},
                {"obs_date": date(2026, 4, 2), "value": 40.0},  # 60% drop
            ],
            "US": [
                {"obs_date": date(2026, 4, 1), "value": 100.0},
                {"obs_date": date(2026, 4, 2), "value": 98.0},  # stable
            ],
            "RU": [
                {"obs_date": date(2026, 4, 1), "value": 100.0},
                {"obs_date": date(2026, 4, 2), "value": 55.0},  # 45% drop
            ],
        }
        anomalies = puller.detect_traffic_anomalies(country_data)

        flagged = {a["country"] for a in anomalies}
        assert "IR" in flagged
        assert "RU" in flagged
        assert "US" not in flagged


# ---------------------------------------------------------------------------
# Full pull integration tests (mocked)
# ---------------------------------------------------------------------------


class TestPullOrchestrator:
    # The puller short-circuits when no CF_RADAR_TOKEN / CLOUDFLARE_API_TOKEN
    # is configured (sensible — every endpoint 403s without one and the old
    # behavior was logging one ERROR per endpoint per cycle). Tests in this
    # class mock the token resolver so they exercise the populated-token
    # happy path, not the SKIPPED short-circuit.
    @pytest.fixture(autouse=True)
    def _stub_cf_token(self):
        with patch(
            "ingestion.altdata.cloudflare_radar_puller._resolve_cf_token",
            return_value="test-token-12345",
        ):
            yield

    def test_pull_inserts_http_traffic(self, puller):
        """Test that pull() stores HTTP traffic rows."""
        http_rows = [
            {"obs_date": date(2026, 4, 1), "value": 100.0, "raw_payload": {}},
        ]

        with patch.object(puller, "_pull_http_traffic", return_value=http_rows), \
             patch.object(puller, "_pull_country_traffic", return_value={}), \
             patch.object(puller, "_pull_ddos_trends", return_value=[]), \
             patch.object(puller, "_pull_anomalies", return_value=[]), \
             patch.object(puller, "_pull_bgp_stats", return_value=[]), \
             patch.object(puller, "_rate_limit"), \
             patch.object(puller, "_get_existing_dates", return_value=set()), \
             patch.object(puller, "_insert_raw") as mock_insert:

            result = puller.pull(days_back=7)

        assert result["status"] == "SUCCESS"
        assert result["rows_inserted"] >= 1
        mock_insert.assert_called()

    def test_pull_deduplicates(self, puller):
        """Test that existing dates are skipped."""
        http_rows = [
            {"obs_date": date(2026, 4, 1), "value": 100.0, "raw_payload": {}},
            {"obs_date": date(2026, 4, 2), "value": 101.0, "raw_payload": {}},
        ]

        with patch.object(puller, "_pull_http_traffic", return_value=http_rows), \
             patch.object(puller, "_pull_country_traffic", return_value={}), \
             patch.object(puller, "_pull_ddos_trends", return_value=[]), \
             patch.object(puller, "_pull_anomalies", return_value=[]), \
             patch.object(puller, "_pull_bgp_stats", return_value=[]), \
             patch.object(puller, "_rate_limit"), \
             patch.object(
                 puller, "_get_existing_dates",
                 return_value={date(2026, 4, 1)},  # already stored
             ), \
             patch.object(puller, "_insert_raw") as mock_insert:

            result = puller.pull(days_back=7)

        # Only Apr 2 should be inserted
        assert result["rows_inserted"] == 1

    def test_pull_respects_flags(self, puller):
        """Test that pull flags disable categories."""
        with patch.object(puller, "_pull_http_traffic", return_value=[]) as mock_http, \
             patch.object(puller, "_pull_country_traffic") as mock_country, \
             patch.object(puller, "_pull_ddos_trends") as mock_ddos, \
             patch.object(puller, "_pull_anomalies") as mock_anom, \
             patch.object(puller, "_pull_bgp_stats") as mock_bgp, \
             patch.object(puller, "_rate_limit"):

            puller.pull(
                pull_countries=False,
                pull_ddos=False,
                pull_anomalies=False,
                pull_bgp=False,
            )

        mock_http.assert_called_once()
        mock_country.assert_not_called()
        mock_ddos.assert_not_called()
        mock_anom.assert_not_called()
        mock_bgp.assert_not_called()

    def test_pull_returns_detected_anomalies(self, puller):
        """Test that detected anomalies appear in pull result."""
        country_data = {
            "IR": [
                {"obs_date": date(2026, 4, 1), "value": 100.0, "raw_payload": {}},
                {"obs_date": date(2026, 4, 2), "value": 40.0, "raw_payload": {}},
            ]
        }

        with patch.object(puller, "_pull_http_traffic", return_value=[]), \
             patch.object(puller, "_pull_country_traffic", return_value=country_data), \
             patch.object(puller, "_pull_ddos_trends", return_value=[]), \
             patch.object(puller, "_pull_anomalies", return_value=[]), \
             patch.object(puller, "_pull_bgp_stats", return_value=[]), \
             patch.object(puller, "_rate_limit"), \
             patch.object(puller, "_get_existing_dates", return_value=set()), \
             patch.object(puller, "_insert_raw"):

            result = puller.pull(days_back=7)

        assert len(result["detected_anomalies"]) == 1
        assert result["detected_anomalies"][0]["country"] == "IR"


# ---------------------------------------------------------------------------
# Fetch retry tests
# ---------------------------------------------------------------------------


class TestFetchRetry:
    def test_fetch_json_calls_requests_get(self, puller):
        """Test that _fetch_json makes proper HTTP call."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"result": {}}
        mock_resp.raise_for_status.return_value = None

        with patch("ingestion.altdata.cloudflare_radar_puller.requests.get", return_value=mock_resp) as mock_get:
            result = puller._fetch_json("https://example.com/api", {"key": "val"})

        mock_get.assert_called_once()
        assert result == {"result": {}}

    def test_fetch_json_raises_on_http_error(self, puller):
        """Test that HTTP errors propagate after retries."""
        import requests as req

        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = req.HTTPError("500")

        with patch(
            "ingestion.altdata.cloudflare_radar_puller.requests.get",
            return_value=mock_resp,
        ):
            with pytest.raises(req.HTTPError):
                puller._fetch_json("https://example.com/api")
