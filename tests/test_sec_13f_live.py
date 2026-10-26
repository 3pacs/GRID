"""Tests for ingestion.altdata.sec_13f_live.

Covers the edgartools rip: the infotable DataFrame -> position-dict converter
(version-tolerant across edgartools 4.x/5.x column casing), the
edgartools-primary / raw-XML-fallback dispatch in ``fetch_infotable``, the pure
XML parser, the CUSIP->ticker map, and the per-ticker aggregation + upsert.

All SEC network access is mocked — no live endpoints are hit.
"""

from __future__ import annotations

import threading
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


# ── HTTP/2 deadlock guard: hard timeout + forcing HTTP/1.1 ─────────────────────
#
# Context: edgartools's HTTP layer deadlocked repeatedly in prod inside an
# httpcore HTTP/2 lock that takes no timeout of its own -- a real hang, never
# an exception, so `except Exception` around a direct call could never catch
# it. `_call_with_timeout` bounds any call from the outside via a daemon
# thread + `Thread.join(timeout=)`; `_ensure_http1_transport` removes the
# HTTP/2 code path entirely (httpxthrottlecache auto-enables HTTP/2 whenever
# `h2` is importable, with no opt-out via edgartools's own `configure_http()`).


def test_call_with_timeout_returns_the_wrapped_result():
    assert m._call_with_timeout(lambda a, b: a + b, 2, 3, timeout=5) == 5


def test_call_with_timeout_reraises_the_wrapped_exception():
    def _boom():
        raise ValueError("api drift")

    with pytest.raises(ValueError, match="api drift"):
        m._call_with_timeout(_boom, timeout=5)


def test_call_with_timeout_raises_timeout_error_on_a_hang_and_does_not_block():
    """A function that never returns (our stand-in for the httpcore deadlock)
    must not hang the caller: join(timeout=) must return, and the leftover
    thread must be daemonic so it can never block interpreter/test exit."""
    released = threading.Event()
    before = {t.ident for t in threading.enumerate()}

    def _hangs_forever():
        released.wait()  # never set; simulates the unkillable httpcore lock

    with pytest.raises(TimeoutError, match="_hangs_forever"):
        m._call_with_timeout(_hangs_forever, timeout=0.05)

    leaked = [t for t in threading.enumerate() if t.ident not in before]
    assert len(leaked) == 1
    assert leaked[0].daemon is True, "a stuck worker must be daemonic or it would block process exit"
    released.set()  # let it finish so it doesn't linger across other tests


def test_fetch_infotable_falls_back_when_edgartools_hangs(monkeypatch):
    """fetch_infotable must not hang forever if edgartools deadlocks -- it
    should time out and fall through to the raw path, same as any other
    edgartools failure."""
    released = threading.Event()

    def _hangs(_filing):
        released.wait()

    rows = [{"name_of_issuer": "RAW CO", "cusip": "999999999", "value": 1}]
    monkeypatch.setattr(m, "_EDGARTOOLS_FETCH_TIMEOUT", 0.05)
    with patch.object(m, "_fetch_infotable_edgartools", side_effect=_hangs), patch.object(
        m, "_fetch_infotable_raw", return_value=rows
    ) as raw:
        out = m.fetch_infotable("1067983", _FILING)
    assert out == rows
    raw.assert_called_once()
    released.set()


def test_ensure_http1_transport_disables_http2_and_recreates_the_client(monkeypatch):
    """Mirrors exactly what edgartools's own configure_http() does when a
    setting changes: flip the flag in httpx_params, close and drop any
    already-created client so the new setting takes effect on the very next
    request."""
    monkeypatch.setattr(m, "_http1_forced", False)
    fake_client = MagicMock()
    fake_mgr = MagicMock(httpx_params={"http2": True}, _client=fake_client)
    fake_httpclient_module = MagicMock(HTTP_MGR=fake_mgr)
    # `from edgar import httpclient` resolves via getattr on whatever object
    # sys.modules["edgar"] holds -- same pattern the existing
    # test_edgartools_path_converts_infotable test uses for `from edgar
    # import find`, so it works regardless of what the real edgar package
    # has already cached as an attribute.
    fake_edgar = MagicMock(httpclient=fake_httpclient_module)

    with patch.dict("sys.modules", {"edgar": fake_edgar}):
        m._ensure_http1_transport()

    assert fake_mgr.httpx_params["http2"] is False
    fake_client.close.assert_called_once()
    assert fake_mgr._client is None


def test_ensure_http1_transport_is_only_applied_once_per_process(monkeypatch):
    monkeypatch.setattr(m, "_http1_forced", False)
    fake_mgr = MagicMock(httpx_params={"http2": True}, _client=None)
    fake_edgar = MagicMock(httpclient=MagicMock(HTTP_MGR=fake_mgr))

    with patch.dict("sys.modules", {"edgar": fake_edgar}):
        m._ensure_http1_transport()
        first_params = dict(fake_mgr.httpx_params)
        m._ensure_http1_transport()

    # The second call is a no-op guarded by _http1_forced -- httpx_params is
    # unchanged from what the first call already set.
    assert dict(fake_mgr.httpx_params) == first_params
    assert m._http1_forced is True


def test_fetch_infotable_edgartools_path_forces_http1(monkeypatch):
    """_fetch_infotable_edgartools must call _ensure_http1_transport alongside
    _ensure_identity, so every edgartools attempt runs on HTTP/1.1."""
    calls: list[str] = []
    monkeypatch.setattr(m, "_ensure_identity", lambda: calls.append("identity"))
    monkeypatch.setattr(m, "_ensure_http1_transport", lambda: calls.append("http1"))

    fake_obj = MagicMock()
    fake_obj.obj.return_value = MagicMock(infotable=pd.DataFrame())
    fake_edgar = MagicMock(find=MagicMock(return_value=fake_obj))
    with patch.dict("sys.modules", {"edgar": fake_edgar}):
        m._fetch_infotable_edgartools(_FILING)

    assert calls == ["identity", "http1"]


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


def _seed_known_report_date(
    engine, holder: str, ticker: str, report_date: date, cik: str = "1067983",
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO institutional_holdings
                    (cik, holder_name, ticker, report_date, source)
                VALUES (:cik, :holder, :ticker, :report_date, 'sec_13f_live')
                """
            ),
            {"cik": cik, "holder": holder, "ticker": ticker, "report_date": report_date},
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


def test_process_filer_sleeps_before_every_infotable_fetch(holdings_engine, monkeypatch):
    """A sleep must precede *every* fetch_infotable call, including the first.

    Regression test: the loop used to only sleep when i > 0, so the first
    fetch_infotable per filer fired immediately after the submissions
    request list_recent_13f_filings() just made — no delay between them.
    """
    q1 = m.LatestFiling(
        accession="ACC-Q1", filing_date=date(2026, 5, 15),
        report_date=date(2026, 3, 31), form="13F-HR",
    )
    q2 = m.LatestFiling(
        accession="ACC-Q2", filing_date=date(2026, 8, 14),
        report_date=date(2026, 6, 30), form="13F-HR",
    )
    monkeypatch.setattr(m, "list_recent_13f_filings", lambda cik: [q2, q1])

    calls: list[str] = []
    monkeypatch.setattr(
        m.time, "sleep", lambda *_a, **_k: calls.append("sleep")
    )

    def fake_fetch(cik, filing):
        calls.append("fetch")
        return [{"cusip": "037833100", "value": 10, "shares": 1}]

    monkeypatch.setattr(m, "fetch_infotable", fake_fetch)

    ingestor = m.SEC13FLiveIngestor(
        engine=holdings_engine, cusip_map=m.CusipTickerMap(data_dirs=[])
    )
    ingestor._cusip_map._map = {"037833100": "AAPL"}

    result = ingestor._process_filer(_FILER)

    assert result.filings_processed == 2
    # Every "fetch" must be immediately preceded by a "sleep" — including
    # the very first one in the loop.
    assert calls == ["sleep", "fetch", "sleep", "fetch"]


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


# ── GD0 §6 item 4 coordinator fix: continuity/upserts must key on cik,  ───────
# ── not the (correctable) display_name -- regression tests, 2026-09-28  ───────


def test_known_report_dates_recognizes_a_renamed_filer_by_cik(holdings_engine):
    # Historical rows were written under the OLD display_name (pre-fix).
    _seed_known_report_date(
        holdings_engine, "Citadel Advisors", "AAPL", date(2025, 12, 31),
        cik="1423053",
    )
    renamed_filer = m.Filer("citadel", "1423053", "Citadel Advisors LLC")
    ingestor = m.SEC13FLiveIngestor(
        engine=holdings_engine, cusip_map=m.CusipTickerMap(data_dirs=[])
    )
    # Continuity must be recognized by cik even though display_name changed.
    known = ingestor._known_report_dates(renamed_filer)
    assert known == {date(2025, 12, 31)}


def test_renamed_filer_rerun_inserts_zero_rows(holdings_engine, monkeypatch):
    """The exact scenario the coordinator flagged: a filer whose
    display_name changed must still be recognized as up to date by cik, and
    a re-run must insert zero rows -- not re-pull and duplicate the whole
    history under the new name."""
    _seed_known_report_date(
        holdings_engine, "Citadel Advisors", "AAPL", date(2025, 12, 31),
        cik="1423053",
    )
    only_known = m.LatestFiling(
        accession="ACC-KNOWN", filing_date=date(2026, 2, 14),
        report_date=date(2025, 12, 31), form="13F-HR",
    )
    renamed_filer = m.Filer("citadel", "1423053", "Citadel Advisors LLC")
    monkeypatch.setattr(m, "list_recent_13f_filings", lambda cik: [only_known])
    fetch_mock = MagicMock()
    monkeypatch.setattr(m, "fetch_infotable", fetch_mock)

    ingestor = m.SEC13FLiveIngestor(
        engine=holdings_engine, cusip_map=m.CusipTickerMap(data_dirs=[])
    )
    result = ingestor._process_filer(renamed_filer)

    assert result.status == "up_to_date"
    assert result.rows_written == 0
    fetch_mock.assert_not_called()

    with holdings_engine.connect() as conn:
        count = conn.execute(
            text("SELECT count(*) FROM institutional_holdings WHERE cik = '1423053'")
        ).scalar()
    # Still exactly the one pre-seeded row -- no duplicate under the new name.
    assert count == 1


def test_upsert_refuses_to_write_under_a_new_name_when_old_name_exists_same_quarter(
    holdings_engine,
):
    """Write-side backstop: even if something bypasses the cik-scoped
    continuity check (e.g. an explicit ``filers=`` override reprocessing an
    already-known quarter), the upsert itself must refuse to create a
    second holder_name key for the same (cik, report_date) instead of
    silently duplicating the underlying 13F fact."""
    _seed_known_report_date(
        holdings_engine, "Citadel Advisors", "AAPL", date(2025, 12, 31),
        cik="1423053",
    )
    renamed_filer = m.Filer("citadel", "1423053", "Citadel Advisors LLC")
    filing = m.LatestFiling(
        accession="ACC-X", filing_date=date(2026, 2, 14),
        report_date=date(2025, 12, 31), form="13F-HR",
    )
    matched = [({"cusip": "037833100", "shares": 100, "value": 1000}, "AAPL")]

    ingestor = m.SEC13FLiveIngestor(
        engine=holdings_engine, cusip_map=m.CusipTickerMap(data_dirs=[])
    )
    rows_written = ingestor._upsert_positions(renamed_filer, filing, matched)

    assert rows_written == 0
    with holdings_engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT holder_name FROM institutional_holdings "
                "WHERE cik = '1423053' AND report_date = '2025-12-31'"
            )
        ).fetchall()
    # Still exactly the one row, still under the OLD name -- no duplicate,
    # no silent overwrite under the new name either.
    assert [r[0] for r in rows] == ["Citadel Advisors"]


def test_upsert_writes_normally_for_a_new_report_date_despite_old_rows_elsewhere(
    holdings_engine,
):
    """The guard is scoped to (cik, report_date), not the filer as a whole
    -- a brand-new quarter must write under the corrected name immediately,
    even though older quarters for the same cik are still under the old
    name (and awaiting the one-time relabel)."""
    _seed_known_report_date(
        holdings_engine, "Citadel Advisors", "MSFT", date(2025, 9, 30),
        cik="1423053",
    )
    renamed_filer = m.Filer("citadel", "1423053", "Citadel Advisors LLC")
    filing = m.LatestFiling(
        accession="ACC-NEW", filing_date=date(2026, 5, 15),
        report_date=date(2026, 3, 31), form="13F-HR",
    )
    matched = [({"cusip": "037833100", "shares": 100, "value": 1000}, "AAPL")]

    ingestor = m.SEC13FLiveIngestor(
        engine=holdings_engine, cusip_map=m.CusipTickerMap(data_dirs=[])
    )
    rows_written = ingestor._upsert_positions(renamed_filer, filing, matched)

    assert rows_written == 1
    with holdings_engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT holder_name FROM institutional_holdings "
                "WHERE cik = '1423053' AND report_date = '2026-03-31'"
            )
        ).one()
    assert row[0] == "Citadel Advisors LLC"


def test_upsert_no_collision_when_no_existing_rows_for_this_cik(holdings_engine):
    """A brand-new filer with no prior rows at all must never be blocked."""
    filer = m.Filer("some_new_filer", "9999999", "Some New Filer LLC")
    filing = m.LatestFiling(
        accession="ACC-BRANDNEW", filing_date=date(2026, 5, 15),
        report_date=date(2026, 3, 31), form="13F-HR",
    )
    matched = [({"cusip": "037833100", "shares": 100, "value": 1000}, "AAPL")]

    ingestor = m.SEC13FLiveIngestor(
        engine=holdings_engine, cusip_map=m.CusipTickerMap(data_dirs=[])
    )
    rows_written = ingestor._upsert_positions(filer, filing, matched)
    assert rows_written == 1
