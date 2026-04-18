"""Unit tests for ingestion/altdata/jodi_oil.py.

Covers:
* Constants shape and membership
* JODIObservation dataclass roundtrip
* _is_tracked filter (happy + 3 reject paths)
* _parse_jodi_csv: happy / malformed / empty / header drift
* _parse_jodi_sdmx: happy / unexpected shape
* run_jodi_oil_puller: CSV happy, CSV->SDMX fallback, both-fail, idempotent re-run
"""

from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from ingestion.altdata.jodi_oil import (
    JODI_CSV_URL,
    JODI_SDMX_URL,
    JODIObservation,
    JODIOilPuller,
    TRACKED_COUNTRIES,
    TRACKED_FLOWS,
    TRACKED_PRODUCTS,
    _is_tracked,
    _parse_jodi_csv,
    _parse_jodi_sdmx,
    run_jodi_oil_puller,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_engine(source_id: int = 7) -> tuple[MagicMock, MagicMock]:
    """Build a mock SQLAlchemy engine with begin()/connect() context managers."""
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    # source_catalog lookup returns the id, _get_existing_dates returns []
    conn.execute.return_value.fetchone.return_value = (source_id,)
    conn.execute.return_value.fetchall.return_value = []
    return engine, conn


_HAPPY_CSV = (
    "REF_AREA,ENERGY_PRODUCT,FLOW_BREAKDOWN,UNIT_MEASURE,TIME_PERIOD,OBS_VALUE,ASSESSMENT_CODE\n"
    "SAU,CRUDEOIL,CLOSSTLV,KBBL,2026-01,150000,1\n"
    "RUS,CRUDEOIL,PRODUCTION,KBD,2026-01,10500,2\n"
    "USA,GASOLINE,IMPORTS,KBD,2026-01,650,1\n"
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


class TestConstants:
    def test_tracked_countries_is_frozen_tuple_of_15(self):
        assert isinstance(TRACKED_COUNTRIES, tuple)
        assert len(TRACKED_COUNTRIES) == 15
        # ISO-3 codes — all uppercase, length 3
        for code in TRACKED_COUNTRIES:
            assert isinstance(code, str)
            assert len(code) == 3
            assert code == code.upper()
        # Critical OPEC+ producers must be present
        for must_have in ("SAU", "RUS", "ARE", "IRQ", "VEN"):
            assert must_have in TRACKED_COUNTRIES

    def test_tracked_products_minimum_set(self):
        assert isinstance(TRACKED_PRODUCTS, tuple)
        assert "CRUDEOIL" in TRACKED_PRODUCTS
        assert "GASOLINE" in TRACKED_PRODUCTS
        assert "JETKERO" in TRACKED_PRODUCTS

    def test_tracked_flows_minimum_set(self):
        assert isinstance(TRACKED_FLOWS, tuple)
        assert "PRODUCTION" in TRACKED_FLOWS
        assert "CLOSSTLV" in TRACKED_FLOWS

    def test_urls_present(self):
        assert JODI_CSV_URL.startswith("https://")
        assert "jodidata.org" in JODI_CSV_URL
        assert JODI_SDMX_URL.startswith("https://")
        assert "sdmx" in JODI_SDMX_URL.lower()


# ---------------------------------------------------------------------------
# JODIObservation dataclass
# ---------------------------------------------------------------------------


class TestJODIObservation:
    def test_frozen_dataclass_roundtrip(self):
        obs = JODIObservation(
            month_end=date(2026, 1, 1),
            country="SAU",
            product="CRUDEOIL",
            flow="CLOSSTLV",
            value=150000.0,
            unit="KBBL",
            assessment="1",
        )
        assert obs.country == "SAU"
        assert obs.value == 150000.0
        assert obs.month_end == date(2026, 1, 1)

    def test_frozen_dataclass_immutable(self):
        obs = JODIObservation(
            month_end=date(2026, 1, 1),
            country="RUS",
            product="CRUDEOIL",
            flow="PRODUCTION",
            value=10500.0,
            unit="KBD",
            assessment="2",
        )
        with pytest.raises((AttributeError, Exception)):
            obs.value = 9999.0  # type: ignore[misc]


# ---------------------------------------------------------------------------
# _is_tracked
# ---------------------------------------------------------------------------


class TestIsTracked:
    def test_happy_path(self):
        assert _is_tracked("SAU", "CRUDEOIL", "CLOSSTLV") is True

    def test_country_filter_rejects_untracked(self):
        # NZL is a real ISO code but not in TRACKED_COUNTRIES
        assert _is_tracked("NZL", "CRUDEOIL", "CLOSSTLV") is False

    def test_product_filter_rejects_untracked(self):
        # Tracked country + tracked flow but ELEC is not a tracked product
        assert _is_tracked("SAU", "ELEC", "CLOSSTLV") is False

    def test_flow_filter_rejects_untracked(self):
        # SSTLV_DEV (stock-level deviation) is not in TRACKED_FLOWS
        assert _is_tracked("SAU", "CRUDEOIL", "SSTLV_DEV") is False

    def test_empty_strings_rejected(self):
        assert _is_tracked("", "CRUDEOIL", "CLOSSTLV") is False
        assert _is_tracked("SAU", "", "CLOSSTLV") is False
        assert _is_tracked("SAU", "CRUDEOIL", "") is False

    def test_case_insensitive(self):
        assert _is_tracked("sau", "crudeoil", "closstlv") is True


# ---------------------------------------------------------------------------
# _parse_jodi_csv
# ---------------------------------------------------------------------------


class TestParseCSV:
    def test_happy_three_rows(self):
        obs = _parse_jodi_csv(_HAPPY_CSV)
        assert len(obs) == 3

        countries = {o.country for o in obs}
        assert countries == {"SAU", "RUS", "USA"}

        sau = next(o for o in obs if o.country == "SAU")
        assert sau.product == "CRUDEOIL"
        assert sau.flow == "CLOSSTLV"
        assert sau.value == 150000.0
        assert sau.unit == "KBBL"
        assert sau.assessment == "1"
        assert sau.month_end == date(2026, 1, 1)

        usa = next(o for o in obs if o.country == "USA")
        assert usa.product == "GASOLINE"
        assert usa.value == 650.0

    def test_malformed_rows_skipped(self):
        # Mix of valid and broken rows: bad date, sentinel value, untracked country
        broken_csv = (
            "REF_AREA,ENERGY_PRODUCT,FLOW_BREAKDOWN,UNIT_MEASURE,TIME_PERIOD,OBS_VALUE,ASSESSMENT_CODE\n"
            "SAU,CRUDEOIL,CLOSSTLV,KBBL,2026-01,150000,1\n"          # OK
            "RUS,CRUDEOIL,PRODUCTION,KBD,not-a-date,10500,2\n"       # bad date → skip
            "ARE,CRUDEOIL,CLOSSTLV,KBBL,2026-01,..,1\n"              # sentinel → skip
            "ARE,CRUDEOIL,CLOSSTLV,KBBL,2026-01,,1\n"                # empty val → skip
            "NZL,CRUDEOIL,CLOSSTLV,KBBL,2026-01,9999,1\n"            # untracked country → skip
            "KWT,CRUDEOIL,PRODUCTION,KBD,2026-02,2700,1\n"           # OK
        )
        obs = _parse_jodi_csv(broken_csv)
        assert len(obs) == 2
        assert {o.country for o in obs} == {"SAU", "KWT"}

    def test_empty_csv(self):
        assert _parse_jodi_csv("") == []
        assert _parse_jodi_csv("   \n\n  ") == []

    def test_missing_required_columns(self):
        bad = "FOO,BAR,BAZ\n1,2,3\n"
        assert _parse_jodi_csv(bad) == []

    def test_alternate_column_names(self):
        # Old-format JODI columns: COUNTRY/PRODUCT/FLOW/VALUE
        old_format = (
            "COUNTRY,PRODUCT,FLOW,UNIT,TIME_PERIOD,VALUE\n"
            "SAU,CRUDEOIL,CLOSSTLV,KBBL,2026-03,151200\n"
        )
        obs = _parse_jodi_csv(old_format)
        assert len(obs) == 1
        assert obs[0].country == "SAU"
        assert obs[0].value == 151200.0
        assert obs[0].assessment == ""  # optional column missing


# ---------------------------------------------------------------------------
# _parse_jodi_sdmx
# ---------------------------------------------------------------------------


def _make_sdmx_payload() -> dict:
    """Build a minimal SDMX-JSON payload with one Saudi crude observation."""
    return {
        "structure": {
            "dimensions": {
                "series": [
                    {
                        "id": "REF_AREA",
                        "values": [{"id": "SAU"}, {"id": "RUS"}],
                    },
                    {
                        "id": "ENERGY_PRODUCT",
                        "values": [{"id": "CRUDEOIL"}],
                    },
                    {
                        "id": "FLOW_BREAKDOWN",
                        "values": [{"id": "CLOSSTLV"}, {"id": "PRODUCTION"}],
                    },
                    {
                        "id": "UNIT_MEASURE",
                        "values": [{"id": "KBBL"}, {"id": "KBD"}],
                    },
                ],
                "observation": [
                    {
                        "id": "TIME_PERIOD",
                        "values": [{"id": "2026-01"}, {"id": "2026-02"}],
                    }
                ],
            }
        },
        "dataSets": [
            {
                "series": {
                    # SAU(0):CRUDEOIL(0):CLOSSTLV(0):KBBL(0)
                    "0:0:0:0": {
                        "observations": {
                            "0": [150000, "1"],
                            "1": [151000, "2"],
                        }
                    },
                    # RUS(1):CRUDEOIL(0):PRODUCTION(1):KBD(1)
                    "1:0:1:1": {
                        "observations": {
                            "0": [10500, "1"],
                        }
                    },
                }
            }
        ],
    }


class TestParseSDMX:
    def test_happy_path(self):
        obs = _parse_jodi_sdmx(_make_sdmx_payload())
        assert len(obs) == 3
        countries = sorted({o.country for o in obs})
        assert countries == ["RUS", "SAU"]

        sau_jan = next(
            o for o in obs
            if o.country == "SAU" and o.month_end == date(2026, 1, 1)
        )
        assert sau_jan.value == 150000.0
        assert sau_jan.unit == "KBBL"
        assert sau_jan.assessment == "1"
        assert sau_jan.flow == "CLOSSTLV"

        rus = next(o for o in obs if o.country == "RUS")
        assert rus.product == "CRUDEOIL"
        assert rus.flow == "PRODUCTION"
        assert rus.value == 10500.0

    def test_unexpected_shape_returns_empty(self):
        assert _parse_jodi_sdmx({}) == []
        assert _parse_jodi_sdmx({"structure": "not-a-dict"}) == []  # type: ignore[arg-type]
        assert _parse_jodi_sdmx({"dataSets": []}) == []
        # Garbage payloads
        assert _parse_jodi_sdmx({"foo": "bar"}) == []
        # Wrong type at top level
        assert _parse_jodi_sdmx(None) == []  # type: ignore[arg-type]
        assert _parse_jodi_sdmx([]) == []  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# run_jodi_oil_puller — top-level entry point
# ---------------------------------------------------------------------------


class TestRunPullerCSVHappy:
    def test_csv_happy_path(self):
        engine, conn = _mock_engine(source_id=7)

        fake_resp = MagicMock()
        fake_resp.text = _HAPPY_CSV
        fake_resp.raise_for_status = MagicMock()

        with patch(
            "ingestion.altdata.jodi_oil.requests.get",
            return_value=fake_resp,
        ) as mock_get:
            result = run_jodi_oil_puller(engine)

        assert result["fetched"] == 3
        assert result["inserted"] == 3
        assert result["source"] == "csv"
        assert sorted(result["countries_seen"]) == ["RUS", "SAU", "USA"]
        assert result["observations_by_flow"]["CLOSSTLV"] == 1
        assert result["observations_by_flow"]["PRODUCTION"] == 1
        assert result["observations_by_flow"]["IMPORTS"] == 1
        # Verify CSV URL was hit (not SDMX)
        assert mock_get.call_count == 1
        assert mock_get.call_args.args[0] == JODI_CSV_URL


class TestRunPullerSDMXFallback:
    def test_csv_failure_falls_back_to_sdmx(self):
        engine, conn = _mock_engine(source_id=7)

        sdmx_resp = MagicMock()
        sdmx_resp.json.return_value = _make_sdmx_payload()
        sdmx_resp.raise_for_status = MagicMock()

        call_log = []

        def fake_get(url, *args, **kwargs):
            call_log.append(url)
            if url == JODI_CSV_URL:
                raise ConnectionError("CSV unreachable")
            return sdmx_resp

        with patch(
            "ingestion.altdata.jodi_oil.requests.get",
            side_effect=fake_get,
        ):
            result = run_jodi_oil_puller(engine)

        assert result["source"] == "sdmx"
        assert result["fetched"] >= 3
        assert result["inserted"] == result["fetched"]
        assert "SAU" in result["countries_seen"]
        assert "RUS" in result["countries_seen"]
        assert call_log[-1] == JODI_SDMX_URL


class TestRunPullerBothFail:
    def test_both_paths_fail_returns_zero(self):
        engine, conn = _mock_engine(source_id=7)

        with patch(
            "ingestion.altdata.jodi_oil.requests.get",
            side_effect=ConnectionError("offline"),
        ):
            result = run_jodi_oil_puller(engine)

        assert result["fetched"] == 0
        assert result["inserted"] == 0
        assert result["source"] == "none"
        assert result["countries_seen"] == []
        assert result["observations_by_flow"] == {}

    def test_csv_returns_garbage_sdmx_returns_garbage(self):
        engine, conn = _mock_engine(source_id=7)

        garbage_csv = MagicMock()
        garbage_csv.text = "GIBBERISH,NOTHING\nfoo,bar\n"
        garbage_csv.raise_for_status = MagicMock()

        garbage_sdmx = MagicMock()
        garbage_sdmx.json.return_value = {"foo": "bar"}
        garbage_sdmx.raise_for_status = MagicMock()

        def fake_get(url, *args, **kwargs):
            return garbage_csv if url == JODI_CSV_URL else garbage_sdmx

        with patch(
            "ingestion.altdata.jodi_oil.requests.get",
            side_effect=fake_get,
        ):
            result = run_jodi_oil_puller(engine)

        assert result["fetched"] == 0
        assert result["source"] == "none"


class TestIdempotentRerun:
    def test_rerun_with_existing_dates_inserts_zero(self):
        engine, conn = _mock_engine(source_id=7)

        # _get_existing_dates returns the Jan 2026 month for every series,
        # so the second run should insert nothing.
        existing_month = date(2026, 1, 1)
        conn.execute.return_value.fetchall.return_value = [(existing_month,)]

        fake_resp = MagicMock()
        fake_resp.text = _HAPPY_CSV
        fake_resp.raise_for_status = MagicMock()

        with patch(
            "ingestion.altdata.jodi_oil.requests.get",
            return_value=fake_resp,
        ):
            result = run_jodi_oil_puller(engine)

        assert result["fetched"] == 3
        # All 3 observations are for 2026-01, all marked as already-existing
        assert result["inserted"] == 0
