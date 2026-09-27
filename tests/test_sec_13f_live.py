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


# ── list_recent_13f_filings / find_latest_13f (bounded catch-up) ──────────────


def _submissions_payload(rows: list[tuple[str, str, str]]) -> dict:
    """Build a fake ``CIK{...}.json`` payload from (form, filingDate, reportDate)."""
    return {
        "filings": {
            "recent": {
                "form": [r[0] for r in rows],
                "accessionNumber": [f"ACC-{i}" for i in range(len(rows))],
                "filingDate": [r[1] for r in rows],
                "reportDate": [r[2] for r in rows],
            }
        }
    }


def test_list_recent_13f_filings_returns_all_report_dates_newest_first():
    payload = _submissions_payload(
        [
            ("13F-HR", "2026-05-15", "2026-03-31"),
            ("10-K", "2026-03-01", "2026-01-01"),  # not a 13F — ignored
            ("13F-HR", "2026-02-14", "2025-12-31"),
            ("13F-HR", "2026-08-14", "2026-06-30"),
        ]
    )
    with patch.object(m, "_get_json", return_value=payload):
        filings = m.list_recent_13f_filings("1067983")

    assert [f.report_date for f in filings] == [
        date(2026, 6, 30),
        date(2026, 3, 31),
        date(2025, 12, 31),
    ]


def test_list_recent_13f_filings_amendment_supersedes_original_same_quarter():
    payload = _submissions_payload(
        [
            ("13F-HR", "2026-05-15", "2026-03-31"),
            ("13F-HR/A", "2026-06-01", "2026-03-31"),  # amends the same quarter
        ]
    )
    with patch.object(m, "_get_json", return_value=payload):
        filings = m.list_recent_13f_filings("1067983")

    assert len(filings) == 1
    assert filings[0].form == "13F-HR/A"
    assert filings[0].filing_date == date(2026, 6, 1)


def test_list_recent_13f_filings_empty_when_no_13f_forms():
    payload = _submissions_payload([("10-K", "2026-03-01", "2026-01-01")])
    with patch.object(m, "_get_json", return_value=payload):
        assert m.list_recent_13f_filings("1067983") == []


def test_find_latest_13f_returns_newest_report_date():
    payload = _submissions_payload(
        [
            ("13F-HR", "2026-02-14", "2025-12-31"),
            ("13F-HR", "2026-08-14", "2026-06-30"),
        ]
    )
    with patch.object(m, "_get_json", return_value=payload):
        filing = m.find_latest_13f("1067983")

    assert filing is not None
    assert filing.report_date == date(2026, 6, 30)


def test_find_latest_13f_returns_none_when_no_filings():
    with patch.object(m, "_get_json", return_value=_submissions_payload([])):
        assert m.find_latest_13f("1067983") is None


# ── _process_filer catch-up (backfill bounded by EDGAR's recent window) ───────


_FILER = m.Filer("berkshire_hathaway", "1067983", "Berkshire Hathaway")


def _seed_known_report_date(engine, holder: str, ticker: str, report_date: date) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO institutional_holdings
                    (holder_name, ticker, report_date, source)
                VALUES (:holder, :ticker, :report_date, 'sec_13f_live')
                """
            ),
            {"holder": holder, "ticker": ticker, "report_date": report_date},
        )


def test_known_report_dates_scoped_to_sec_13f_live_source(holdings_engine):
    _seed_known_report_date(
        holdings_engine, "Berkshire Hathaway", "AAPL", date(2025, 12, 31)
    )
    # A curated/bootstrap row for the same holder+quarter must NOT count as
    # "already ingested" by the live writer — different provenance.
    with holdings_engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO institutional_holdings
                    (holder_name, ticker, report_date, source)
                VALUES ('Berkshire Hathaway', 'MSFT', :report_date, 'sec_13f_curated')
                """
            ),
            {"report_date": date(2024, 12, 31)},
        )
    ingestor = m.SEC13FLiveIngestor(
        engine=holdings_engine, cusip_map=m.CusipTickerMap(data_dirs=[])
    )
    known = ingestor._known_report_dates(_FILER)
    assert known == {date(2025, 12, 31)}


def test_process_filer_backfills_every_missed_quarter(holdings_engine, monkeypatch):
    """A gap of several quarters must not be collapsed to just the latest one."""
    _seed_known_report_date(
        holdings_engine, "Berkshire Hathaway", "AAPL", date(2025, 12, 31)
    )
    q1 = m.LatestFiling(
        accession="ACC-Q1", filing_date=date(2026, 5, 15),
        report_date=date(2026, 3, 31), form="13F-HR",
    )
    q2 = m.LatestFiling(
        accession="ACC-Q2", filing_date=date(2026, 8, 14),
        report_date=date(2026, 6, 30), form="13F-HR",
    )
    monkeypatch.setattr(m, "list_recent_13f_filings", lambda cik: [q2, q1])
    monkeypatch.setattr(m.time, "sleep", lambda *_a, **_k: None)

    def fake_fetch(cik, filing):
        return [{"cusip": "037833100", "value": 10, "shares": 1}]

    monkeypatch.setattr(m, "fetch_infotable", fake_fetch)

    ingestor = m.SEC13FLiveIngestor(
        engine=holdings_engine, cusip_map=m.CusipTickerMap(data_dirs=[])
    )
    ingestor._cusip_map._map = {"037833100": "AAPL"}

    result = ingestor._process_filer(_FILER)

    assert result.status == "ok"
    assert result.filings_processed == 2
    assert result.rows_written == 2  # one upsert per newly-seen quarter
    assert result.filing.report_date == date(2026, 6, 30)  # newest processed

    with holdings_engine.connect() as conn:
        # sqlite's raw text() DATE column hands back ISO strings, not
        # `date` objects (there is no real DATE type to coerce through) —
        # normalize before comparing, same as production code must for
        # Postgres-vs-sqlite portability.
        report_dates = {
            date.fromisoformat(str(row[0])[:10])
            for row in conn.execute(
                text(
                    "SELECT report_date FROM institutional_holdings "
                    "WHERE holder_name = 'Berkshire Hathaway' AND ticker = 'AAPL'"
                )
            )
        }
    # The pre-seeded 2025-12-31 row (simulating the quarter already on file
    # before this run) must survive untouched alongside the two new ones —
    # the backfill only adds rows for previously-missing report_dates.
    assert report_dates == {date(2025, 12, 31), date(2026, 3, 31), date(2026, 6, 30)}


def test_process_filer_up_to_date_skips_fetch_entirely(holdings_engine, monkeypatch):
    only_known = m.LatestFiling(
        accession="ACC-KNOWN", filing_date=date(2026, 2, 14),
        report_date=date(2025, 12, 31), form="13F-HR",
    )
    _seed_known_report_date(
        holdings_engine, "Berkshire Hathaway", "AAPL", date(2025, 12, 31)
    )
    monkeypatch.setattr(m, "list_recent_13f_filings", lambda cik: [only_known])
    fetch_mock = MagicMock()
    monkeypatch.setattr(m, "fetch_infotable", fetch_mock)

    ingestor = m.SEC13FLiveIngestor(
        engine=holdings_engine, cusip_map=m.CusipTickerMap(data_dirs=[])
    )
    result = ingestor._process_filer(_FILER)

    assert result.status == "up_to_date"
    assert result.rows_written == 0
    fetch_mock.assert_not_called()


def test_process_filer_no_filing_when_edgar_has_no_13f(holdings_engine, monkeypatch):
    monkeypatch.setattr(m, "list_recent_13f_filings", lambda cik: [])

    ingestor = m.SEC13FLiveIngestor(
        engine=holdings_engine, cusip_map=m.CusipTickerMap(data_dirs=[])
    )
    result = ingestor._process_filer(_FILER)

    assert result.status == "no_filing"
    assert result.rows_written == 0
