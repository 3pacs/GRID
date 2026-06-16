"""Tests for ingestion.altdata.sec_13f_live.

Covers the edgartools rip: the infotable DataFrame -> position-dict converter
(version-tolerant across edgartools 4.x/5.x column casing), the
edgartools-primary / raw-XML-fallback dispatch in ``fetch_infotable``, the pure
XML parser, the CUSIP->ticker map, and the per-ticker aggregation + upsert.

All SEC network access is mocked — no live endpoints are hit.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from ingestion.altdata import sec_13f_live as m


# ── infotable DataFrame -> positions converter ────────────────────────────────


def test_converter_handles_5x_capitalized_columns():
    """edgartools 5.x emits Issuer/Cusip/Value/SharesPrnAmount/Type."""
    df = pd.DataFrame(
        [
            {
                "Issuer": "APPLE INC",
                "Class": "COM",
                "Cusip": "037833100",
                "Value": 1_000_000,
                "SharesPrnAmount": 500,
                "Type": "Shares",
            }
        ]
    )
    positions = m._infotable_df_to_positions(df)
    assert positions == [
        {
            "name_of_issuer": "APPLE INC",
            "cusip": "037833100",
            "value": 1_000_000,
            "shares": 500,
            "share_type": "Shares",
        }
    ]


def test_converter_handles_4x_lowercase_columns():
    """edgartools 4.x (and the raw parser) emit lower-case column names."""
    df = pd.DataFrame(
        [
            {
                "name_of_issuer": "MSFT",
                "cusip": "594918104",
                "value": 2_000_000,
                "shares": 300,
                "share_type": "SH",
            }
        ]
    )
    positions = m._infotable_df_to_positions(df)
    assert positions[0]["name_of_issuer"] == "MSFT"
    assert positions[0]["cusip"] == "594918104"
    assert positions[0]["value"] == 2_000_000
    assert positions[0]["shares"] == 300


def test_converter_drops_rows_missing_cusip_or_issuer():
    df = pd.DataFrame(
        [
            {"Issuer": "", "Cusip": "", "Value": 0, "SharesPrnAmount": 0},
            {"Issuer": "GOOD", "Cusip": "123456789", "Value": 5, "SharesPrnAmount": 1},
        ]
    )
    positions = m._infotable_df_to_positions(df)
    assert len(positions) == 1
    assert positions[0]["name_of_issuer"] == "GOOD"


def test_converter_coerces_bad_numeric_to_none():
    df = pd.DataFrame(
        [{"Issuer": "X", "Cusip": "111111111", "Value": "n/a", "SharesPrnAmount": None}]
    )
    positions = m._infotable_df_to_positions(df)
    assert positions[0]["value"] is None
    assert positions[0]["shares"] is None


def test_converter_uppercases_cusip():
    df = pd.DataFrame([{"Issuer": "X", "Cusip": "abc833100", "Value": 1}])
    assert m._infotable_df_to_positions(df)[0]["cusip"] == "ABC833100"


# ── fetch_infotable dispatch: edgartools primary, raw fallback ─────────────────


_FILING = m.LatestFiling(
    accession="0001067983-25-000019",
    filing_date=date(2025, 2, 14),
    report_date=date(2024, 12, 31),
    form="13F-HR",
)


def test_fetch_infotable_uses_edgartools_when_available():
    rows = [{"name_of_issuer": "APPLE INC", "cusip": "037833100", "value": 9}]
    with patch.object(m, "_fetch_infotable_edgartools", return_value=rows) as eg, patch.object(
        m, "_fetch_infotable_raw"
    ) as raw:
        out = m.fetch_infotable("1067983", _FILING)
    assert out == rows
    eg.assert_called_once()
    raw.assert_not_called()


def test_fetch_infotable_falls_back_on_edgartools_error():
    rows = [{"name_of_issuer": "RAW CO", "cusip": "999999999", "value": 1}]
    with patch.object(
        m, "_fetch_infotable_edgartools", side_effect=RuntimeError("api drift")
    ), patch.object(m, "_fetch_infotable_raw", return_value=rows) as raw:
        out = m.fetch_infotable("1067983", _FILING)
    assert out == rows
    raw.assert_called_once()


def test_fetch_infotable_falls_back_on_empty_edgartools_result():
    rows = [{"name_of_issuer": "RAW CO", "cusip": "999999999", "value": 1}]
    with patch.object(m, "_fetch_infotable_edgartools", return_value=[]), patch.object(
        m, "_fetch_infotable_raw", return_value=rows
    ) as raw:
        out = m.fetch_infotable("1067983", _FILING)
    assert out == rows
    raw.assert_called_once()


def test_edgartools_path_converts_infotable(monkeypatch):
    """_fetch_infotable_edgartools resolves the filing and converts its table."""
    df = pd.DataFrame(
        [{"Issuer": "APPLE INC", "Cusip": "037833100", "Value": 7, "SharesPrnAmount": 2}]
    )
    fake_obj = MagicMock()
    fake_obj.obj.return_value = MagicMock(infotable=df)

    monkeypatch.setattr(m, "_ensure_identity", lambda: None)
    fake_edgar = MagicMock(find=MagicMock(return_value=fake_obj))
    with patch.dict("sys.modules", {"edgar": fake_edgar}):
        positions = m._fetch_infotable_edgartools(_FILING)

    fake_edgar.find.assert_called_once_with(_FILING.accession)
    assert positions[0]["name_of_issuer"] == "APPLE INC"
    assert positions[0]["value"] == 7


# ── pure XML parser (raw fallback core) ────────────────────────────────────────


def test_parse_infotable_xml_namespaced():
    xml = b"""<?xml version="1.0"?>
    <informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
      <infoTable>
        <nameOfIssuer>NVIDIA CORP</nameOfIssuer>
        <cusip>67066G104</cusip>
        <value>3000000</value>
        <shrsOrPrnAmt><sshPrnamt>123</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
      </infoTable>
    </informationTable>"""
    positions = m.parse_infotable_xml(xml)
    assert positions == [
        {
            "name_of_issuer": "NVIDIA CORP",
            "cusip": "67066G104",
            "value": 3000000,
            "shares": 123,
            "share_type": "SH",
        }
    ]


def test_parse_infotable_xml_bad_input_returns_empty():
    assert m.parse_infotable_xml(b"not xml at all") == []


# ── CUSIP -> ticker map ────────────────────────────────────────────────────────


def test_cusip_map_lookup_and_check_digit_fallback():
    cm = m.CusipTickerMap(data_dirs=[])
    cm._map = {"037833100": "AAPL"}
    assert cm.lookup("037833100") == "AAPL"
    assert cm.lookup("") is None
    assert cm.lookup("000000000") is None
    # 9-char miss retries the 8-char-prefix + check-digit variant.
    cm._map = {"03783310X": "AAPL"}
    # exact miss, but prefix[:8] + last char == "03783310" + "0" -> not present
    assert cm.lookup("037833100") is None


# ── aggregation + upsert ───────────────────────────────────────────────────────


@pytest.fixture
def holdings_engine():
    eng = create_engine("sqlite:///:memory:")
    with eng.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE institutional_holdings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cik TEXT,
                    holder_name TEXT,
                    ticker TEXT,
                    cusip TEXT,
                    shares_held INTEGER,
                    value_usd INTEGER,
                    report_date DATE,
                    filed_date DATE,
                    source TEXT,
                    UNIQUE (holder_name, ticker, report_date)
                )
                """
            )
        )
    return eng


def test_upsert_aggregates_share_classes(holdings_engine):
    ingestor = m.SEC13FLiveIngestor(
        engine=holdings_engine, cusip_map=m.CusipTickerMap(data_dirs=[])
    )
    filer = m.Filer("berkshire_hathaway", "1067983", "Berkshire Hathaway")
    # Two rows for the same ticker (e.g. share classes) must aggregate.
    matched = [
        ({"cusip": "037833100", "shares": 100, "value": 1000}, "AAPL"),
        ({"cusip": "037833100", "shares": 50, "value": 500}, "AAPL"),
    ]
    rows_written = ingestor._upsert_positions(filer, _FILING, matched)
    assert rows_written == 1
    with holdings_engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT shares_held, value_usd, source FROM institutional_holdings "
                "WHERE ticker = 'AAPL'"
            )
        ).one()
    assert row[0] == 150
    assert row[1] == 1500
    assert row[2] == "sec_13f_live"
