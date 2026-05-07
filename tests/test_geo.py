"""Tests for the geo-spatial data endpoints and helper functions."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import MagicMock


from api.routers.geo import FINANCIAL_CENTERS, _extract_geo


# ── _extract_geo helper tests ──────────────────────────────────────


class TestExtractGeo:
    """Tests for the _extract_geo helper function."""

    def test_direct_coordinates_from_metadata(self):
        meta = {"lat": 40.7128, "lng": -74.006}
        result = _extract_geo(meta, "")
        assert result is not None
        assert result["lat"] == 40.7128
        assert result["lng"] == -74.006

    def test_direct_coordinates_from_json_string(self):
        meta = json.dumps({"lat": 51.5074, "lng": -0.1278})
        result = _extract_geo(meta, "")
        assert result is not None
        assert abs(result["lat"] - 51.5074) < 0.001
        assert abs(result["lng"] - (-0.1278)) < 0.001

    def test_jurisdiction_lookup(self):
        meta = {"jurisdiction": "UK"}
        result = _extract_geo(meta, "")
        assert result is not None
        assert result["lat"] == FINANCIAL_CENTERS["UK"]["lat"]
        assert result["lng"] == FINANCIAL_CENTERS["UK"]["lng"]

    def test_country_code_lookup(self):
        meta = {"country": "JP"}
        result = _extract_geo(meta, "")
        assert result is not None
        assert result["lat"] == FINANCIAL_CENTERS["JP"]["lat"]

    def test_headquarters_country_lookup(self):
        meta = {"headquarters_country": "SG"}
        result = _extract_geo(meta, "")
        assert result is not None
        assert result["lat"] == FINANCIAL_CENTERS["SG"]["lat"]

    def test_name_inference_fed(self):
        result = _extract_geo(None, "Federal Reserve")
        assert result is not None
        assert result["lat"] == FINANCIAL_CENTERS["US"]["lat"]

    def test_name_inference_treasury(self):
        result = _extract_geo(None, "US Treasury Department")
        assert result is not None
        assert result["lat"] == FINANCIAL_CENTERS["US"]["lat"]

    def test_name_inference_boe(self):
        result = _extract_geo(None, "Bank of England")
        assert result is not None
        assert result["lat"] == FINANCIAL_CENTERS["UK"]["lat"]

    def test_name_inference_boj(self):
        result = _extract_geo(None, "Bank of Japan")
        assert result is not None
        assert result["lat"] == FINANCIAL_CENTERS["JP"]["lat"]

    def test_name_inference_pboc(self):
        result = _extract_geo(None, "PBOC")
        assert result is not None
        assert result["lat"] == FINANCIAL_CENTERS["CN"]["lat"]

    def test_name_inference_ecb(self):
        result = _extract_geo(None, "European Central Bank")
        assert result is not None
        assert result["lat"] == FINANCIAL_CENTERS["DE"]["lat"]

    def test_no_match_returns_none(self):
        result = _extract_geo(None, "Unknown Entity XYZ")
        assert result is None

    def test_empty_metadata_no_name(self):
        result = _extract_geo({}, "")
        assert result is None

    def test_none_metadata_none_name(self):
        result = _extract_geo(None, None)
        assert result is None

    def test_invalid_json_string_metadata(self):
        result = _extract_geo("not-json", "")
        assert result is None

    def test_invalid_lat_lng_values(self):
        meta = {"lat": "not_a_number", "lng": "bad"}
        result = _extract_geo(meta, "")
        assert result is None

    def test_lowercase_country_code(self):
        meta = {"jurisdiction": "uk"}
        result = _extract_geo(meta, "")
        assert result is not None
        assert result["lat"] == FINANCIAL_CENTERS["UK"]["lat"]

    def test_long_country_code_truncated(self):
        meta = {"jurisdiction": "USA"}
        result = _extract_geo(meta, "")
        assert result is not None
        assert result["lat"] == FINANCIAL_CENTERS["US"]["lat"]


# ── Financial centers data tests ───────────────────────────────────


class TestFinancialCenters:
    """Verify the financial centers lookup table is well-formed."""

    def test_all_entries_have_required_keys(self):
        for code, data in FINANCIAL_CENTERS.items():
            assert "lat" in data, f"{code} missing lat"
            assert "lng" in data, f"{code} missing lng"
            assert "name" in data, f"{code} missing name"

    def test_lat_lng_are_valid_ranges(self):
        for code, data in FINANCIAL_CENTERS.items():
            assert -90 <= data["lat"] <= 90, f"{code} lat out of range: {data['lat']}"
            assert -180 <= data["lng"] <= 180, f"{code} lng out of range: {data['lng']}"

    def test_key_centers_present(self):
        for code in ["US", "UK", "JP", "CN", "DE", "SG", "HK"]:
            assert code in FINANCIAL_CENTERS, f"{code} missing from FINANCIAL_CENTERS"

    def test_offshore_centers_present(self):
        for code in ["PA", "VG", "KY", "BM"]:
            assert code in FINANCIAL_CENTERS, f"Offshore center {code} missing"


# ── Helper to build mock engine for endpoint tests ────────────────


def _make_mock_engine():
    """Create a mock SQLAlchemy engine with a mock connection."""
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn


def _make_mock_result(rows):
    """Create a mock result object that returns the given rows via .mappings().all()."""
    mock_mappings = MagicMock()
    mock_mappings.all.return_value = rows
    mock_result = MagicMock()
    mock_result.mappings.return_value = mock_mappings
    return mock_result


# ── API endpoint tests (mocked DB) ────────────────────────────────


class TestGeoFlowsEndpoint:
    """Test /api/v1/geo/flows endpoint with mocked database."""

    def test_flows_returns_structure(self):
        engine, conn = _make_mock_engine()

        conn.execute.side_effect = [
            _make_mock_result([
                {
                    "from_actor": "Federal Reserve",
                    "to_entity": "Bank of England",
                    "amount_estimate": 1_000_000_000,
                    "confidence": "estimated",
                    "flow_date": "2026-03-01",
                    "evidence": "test",
                    "from_category": "sovereign",
                    "from_meta": None,
                    "to_category": "sovereign",
                    "to_meta": None,
                }
            ]),
            _make_mock_result([]),  # dollar_flows empty
        ]

        from api.routers.geo import get_geo_flows

        result = asyncio.new_event_loop().run_until_complete(
            get_geo_flows(flow_type="capital", days=90, min_amount=0, engine=engine, _=None)
        )

        assert "flows" in result
        assert "count" in result
        assert result["flow_type"] == "capital"
        assert len(result["flows"]) == 1
        assert result["flows"][0]["from_name"] == "Federal Reserve"
        assert result["flows"][0]["to_name"] == "Bank of England"

    def test_flows_empty_when_no_geo(self):
        engine, conn = _make_mock_engine()

        conn.execute.side_effect = [
            _make_mock_result([
                {
                    "from_actor": "Unknown Corp",
                    "to_entity": "Another Unknown",
                    "amount_estimate": 500_000,
                    "confidence": "rumored",
                    "flow_date": "2026-03-01",
                    "evidence": None,
                    "from_category": None,
                    "from_meta": None,
                    "to_category": None,
                    "to_meta": None,
                }
            ]),
            _make_mock_result([]),
        ]

        from api.routers.geo import get_geo_flows

        result = asyncio.new_event_loop().run_until_complete(
            get_geo_flows(flow_type="capital", days=90, min_amount=0, engine=engine, _=None)
        )

        assert result["count"] == 0

    def test_flows_includes_dollar_flows(self):
        engine, conn = _make_mock_engine()

        conn.execute.side_effect = [
            _make_mock_result([]),  # wealth_flows empty
            _make_mock_result([
                {
                    "source_type": "institutional",
                    "actor_name": "Bank of Japan",
                    "ticker": "SPY",
                    "amount_usd": 500_000_000,
                    "direction": "outflow",
                    "confidence": "derived",
                    "flow_date": "2026-03-15",
                }
            ]),
        ]

        from api.routers.geo import get_geo_flows

        result = asyncio.new_event_loop().run_until_complete(
            get_geo_flows(flow_type="capital", days=90, min_amount=0, engine=engine, _=None)
        )

        assert result["count"] == 1
        assert result["flows"][0]["from_name"] == "Bank of Japan"
        assert result["flows"][0]["to_name"] == "SPY"


class TestGeoActorsEndpoint:
    """Test /api/v1/geo/actors endpoint."""

    def test_actors_returns_structure(self):
        engine, conn = _make_mock_engine()

        conn.execute.return_value = _make_mock_result([
            {
                "id": 1,
                "name": "Federal Reserve",
                "category": "sovereign",
                "tier": 1,
                "influence_score": 0.95,
                "net_worth_estimate": None,
                "metadata": json.dumps({"jurisdiction": "US"}),
            }
        ])

        from api.routers.geo import get_geo_actors

        result = asyncio.new_event_loop().run_until_complete(
            get_geo_actors(min_influence=0.5, category=None, limit=100, engine=engine, _=None)
        )

        assert "actors" in result
        assert "count" in result
        assert len(result["actors"]) == 1
        assert result["actors"][0]["name"] == "Federal Reserve"
        assert result["actors"][0]["lat"] == FINANCIAL_CENTERS["US"]["lat"]

    def test_actors_with_category_filter(self):
        engine, conn = _make_mock_engine()

        conn.execute.return_value = _make_mock_result([])

        from api.routers.geo import get_geo_actors

        result = asyncio.new_event_loop().run_until_complete(
            get_geo_actors(min_influence=0, category="sovereign", limit=50, engine=engine, _=None)
        )

        assert result["count"] == 0
        assert conn.execute.called

    def test_actors_filters_no_geo(self):
        engine, conn = _make_mock_engine()

        conn.execute.return_value = _make_mock_result([
            {
                "id": 99,
                "name": "Mystery Corp",
                "category": None,
                "tier": None,
                "influence_score": 0.5,
                "net_worth_estimate": None,
                "metadata": None,
            }
        ])

        from api.routers.geo import get_geo_actors

        result = asyncio.new_event_loop().run_until_complete(
            get_geo_actors(min_influence=0, category=None, limit=100, engine=engine, _=None)
        )

        # No geo for "Mystery Corp" with no metadata -> filtered out
        assert result["count"] == 0


class TestSignalDensityEndpoint:
    """Test /api/v1/geo/signals/density endpoint."""

    def test_density_returns_structure(self):
        engine, conn = _make_mock_engine()

        conn.execute.return_value = _make_mock_result([
            {
                "actor": "Federal Reserve",
                "signal_count": 42,
                "metadata": None,
                "category": "sovereign",
            }
        ])

        from api.routers.geo import get_signal_density

        result = asyncio.new_event_loop().run_until_complete(
            get_signal_density(days=30, engine=engine, _=None)
        )

        assert "density" in result
        assert "count" in result
        assert len(result["density"]) == 1
        assert result["density"][0]["weight"] == 42
        assert result["density"][0]["actor"] == "Federal Reserve"

    def test_density_filters_no_geo(self):
        engine, conn = _make_mock_engine()

        conn.execute.return_value = _make_mock_result([
            {
                "actor": "Unknown Entity",
                "signal_count": 5,
                "metadata": None,
                "category": None,
            }
        ])

        from api.routers.geo import get_signal_density

        result = asyncio.new_event_loop().run_until_complete(
            get_signal_density(days=30, engine=engine, _=None)
        )

        # No geo for "Unknown Entity", should be filtered out
        assert result["count"] == 0
