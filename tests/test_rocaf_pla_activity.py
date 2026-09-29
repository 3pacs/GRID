"""Tests for ingestion/altdata/rocaf_pla_activity.py.

Fixtures in tests/fixtures/rocaf/ are the real air.mnd.gov.tw "Air
activities" list page and the 2026-09-29 report, recorded from grid-svr on
2026-09-29. No network is used.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests
from sqlalchemy import create_engine, text

import ingestion.altdata.rocaf_pla_activity as mod
from ingestion.altdata.rocaf_pla_activity import (
    AF_LIST_URL,
    SERIES_ADIZ,
    SERIES_AIRCRAFT,
    SERIES_OFFICIAL_SHIPS,
    SERIES_PLAN_SHIPS,
    ROCAFPLAActivityPuller,
    parse_list,
    parse_report,
)

FIX = Path(__file__).parent / "fixtures" / "rocaf"
LIST_HTML = (FIX / "air_activities_list_20260929.html").read_text(encoding="utf-8")
REPORT_HTML = (FIX / "air_activity_59269_20260929.html").read_text(encoding="utf-8")
URL_0929 = "https://air.mnd.gov.tw/EN/News/News_Detail.aspx?CID=214&ID=59269"
URL_0928 = "https://air.mnd.gov.tw/EN/News/News_Detail.aspx?CID=214&ID=59267"


def test_parse_list() -> None:
    items = parse_list(LIST_HTML)
    assert len(items) == 12
    assert items[0].url == URL_0929
    assert items[0].published == date(2026, 9, 29)
    assert items[1].url == URL_0928
    assert all("PLA activities" in i.title for i in items)


def test_parse_list_layout_change_is_empty() -> None:
    assert parse_list("<html><body>maintenance</body></html>") == []


def test_parse_report_real_fixture() -> None:
    rep = parse_report(REPORT_HTML, url=URL_0929)
    assert rep is not None
    assert rep.report_date == date(2026, 9, 29)
    assert rep.aircraft_sorties == 3
    assert rep.plan_ships == 6
    assert rep.official_ships == 4
    assert rep.adiz_entries == 1
    assert rep.adiz_sentence_present is True


def test_parse_report_median_line_wording_and_no_official_ships() -> None:
    html = (
        "<div>2026/08/01</div><p>2.PLA activities: 27 sorties of PLA aircraft and 9 PLAN "
        "ships operating around Taiwan were detected as of 6 a.m. (UTC+8) today. "
        "18 out of 27 sorties crossed the median line and entered Taiwan's northern, "
        "central and southwestern ADIZ.</p>"
    )
    rep = parse_report(html)
    assert rep is not None
    assert (rep.aircraft_sorties, rep.plan_ships, rep.adiz_entries) == (27, 9, 18)
    assert rep.official_ships is None  # not stated -> not stored


def test_parse_report_without_adiz_sentence_marks_it() -> None:
    html = (
        "<div>2026/08/02</div><p>2.PLA activities: 5 sorties of PLA aircraft, 7 PLAN ships "
        "and 1 official ship operating around Taiwan were detected as of 6 a.m. today.</p>"
    )
    rep = parse_report(html)
    assert rep is not None
    assert rep.adiz_entries == 0
    assert rep.adiz_sentence_present is False


def test_parse_report_without_sortie_count_is_skipped() -> None:
    assert parse_report("<p>2026/08/03 Press conference on budget.</p>") is None


def _report_html(aircraft: int, adiz_sentence: str) -> str:
    return (
        f"<p>2026/09/29</p><p>2.PLA activities: {aircraft} sorties of PLA aircraft, "
        "6 PLAN ships and 4 official ships operating around Taiwan were detected "
        f"as of 6 a.m. (UTC+8) today. {adiz_sentence}</p>"
    )


@pytest.mark.parametrize(("aircraft", "sentence", "entries"), [
    (12, "All 12 sorties of PLA aircraft entered Taiwan's southwestern ADIZ.", 12),
    (32, "22 of the 32 sorties entered Taiwan's northern and southwestern ADIZ.", 22),
    (32, "22 of 32 sorties entered Taiwan's southwestern ADIZ.", 22),
    (3, "1 out of the 3 sorties entered Taiwan's southwestern ADIZ.", 1),
])
def test_parse_report_mnd_adiz_variants(aircraft: int, sentence: str, entries: int) -> None:
    rep = parse_report(_report_html(aircraft, sentence))
    assert rep is not None
    assert rep.aircraft_sorties == aircraft
    assert rep.adiz_entries == entries
    assert rep.adiz_sentence_present is True


@pytest.mark.parametrize("sentence", [
    "Several sorties entered Taiwan's southwestern ADIZ.",
    "ADIZ entry counts are unavailable.",
    "No sorties entered Taiwan's southwestern ADIZ.",
    "0 out of 3 sorties entered Taiwan's southwestern ADIZ.",
    "All 0 sorties entered Taiwan's southwestern ADIZ.",
    "3 sorties did not enter Taiwan's southwestern ADIZ.",
    pytest.param("Additional information. " * 100 + "ADIZ activity was detected.", id="adiz-beyond-window"),
])
def test_parse_report_unparsed_or_zero_adiz_is_skipped(sentence: str) -> None:
    assert parse_report(_report_html(3, sentence)) is None


@pytest.mark.parametrize("sentence", [
    "All 12 sorties remained outside Taiwan's ADIZ; none entered Taiwan's ADIZ.",
    "22 of the 32 sorties remained outside Taiwan's ADIZ; none entered Taiwan's ADIZ.",
    "All 12 sorties of PLA aircraft never entered Taiwan's southwestern ADIZ.",
    "22 of the 32 sorties did not enter Taiwan's southwestern ADIZ.",
    "All 12 sorties were monitored, while 6 PLAN ships entered the ADIZ.",
    "Not all 12 sorties entered Taiwan's southwestern ADIZ.",
    "If all 12 sorties entered Taiwan's southwestern ADIZ, monitoring would increase.",
    "All 12 sorties entered Taiwan's southwestern ADIZ. ADIZ counts remain unconfirmed.",
    "All 12 sorties entered the ADIZ. 2 of 12 sorties entered the ADIZ.",
])
def test_adiz_count_must_belong_to_one_affirmative_statement(sentence: str) -> None:
    assert parse_report(_report_html(32, sentence)) is None


# ---------------------------------------------------------------------------
# Puller
# ---------------------------------------------------------------------------


class _Resp:
    def __init__(self, text: str, status: int = 200) -> None:
        self.text = text
        self.status_code = status

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class _Session:
    def __init__(self, pages: dict[str, _Resp]) -> None:
        self.pages = pages
        self.headers: dict[str, str] = {}
        self.calls: list[str] = []

    def get(self, url: str, timeout: int = 0) -> _Resp:  # noqa: ARG002
        self.calls.append(url)
        return self.pages.get(url, _Resp("nf", 404))


def _engine(latest: date | None) -> MagicMock:
    engine = MagicMock()
    cconn = MagicMock()

    def _cexec(stmt, params=None):  # noqa: ANN001, ARG001
        res = MagicMock()
        res.fetchone.return_value = (latest,) if "MAX(obs_date)" in str(stmt) else (99,)
        return res

    cconn.execute.side_effect = _cexec
    engine.connect.return_value.__enter__ = MagicMock(return_value=cconn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    bconn = MagicMock()
    bconn.execute.return_value.fetchall.return_value = []
    bconn.execute.return_value.fetchone.return_value = None
    engine.begin.return_value.__enter__ = MagicMock(return_value=bconn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    engine._bconn = bconn
    return engine


def _inserts(engine: MagicMock) -> list[dict]:
    return [c.args[1] for c in engine._bconn.execute.call_args_list
            if "INSERT INTO raw_series" in str(c.args[0])]


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)


def test_incremental_run_fetches_only_new_report() -> None:
    engine = _engine(latest=date(2026, 9, 28))
    session = _Session({AF_LIST_URL: _Resp(LIST_HTML), URL_0929: _Resp(REPORT_HTML)})
    result = ROCAFPLAActivityPuller(engine, session=session).pull()
    assert session.calls == [AF_LIST_URL, URL_0929]
    assert result["status"] == "SUCCESS"
    rows = {r["sid"]: r for r in _inserts(engine)}
    assert {k: v["val"] for k, v in rows.items()} == {
        SERIES_AIRCRAFT: 3.0, SERIES_PLAN_SHIPS: 6.0, SERIES_OFFICIAL_SHIPS: 4.0, SERIES_ADIZ: 1.0,
    }
    for r in rows.values():
        assert r["od"] == date(2026, 9, 29)
        assert r["status"] == "SUCCESS"
        assert r["src"] == 99
        assert "pull_timestamp" not in r  # column default = fetch time


def test_list_blocked_is_failed_and_writes_nothing() -> None:
    engine = _engine(latest=None)
    result = ROCAFPLAActivityPuller(engine, session=_Session({AF_LIST_URL: _Resp("x", 403)})).pull()
    assert result["status"] == "FAILED"
    assert _inserts(engine) == []


def test_all_details_failing_is_failed() -> None:
    engine = _engine(latest=date(2026, 9, 27))
    result = ROCAFPLAActivityPuller(engine, session=_Session({AF_LIST_URL: _Resp(LIST_HTML)})).pull()
    assert result["status"] == "FAILED"
    assert _inserts(engine) == []


def test_adiz_parse_miss_is_failed_and_writes_nothing() -> None:
    engine = _engine(latest=date(2026, 9, 28))
    session = _Session({
        AF_LIST_URL: _Resp(LIST_HTML),
        URL_0929: _Resp(_report_html(3, "Several sorties entered Taiwan's ADIZ.")),
    })
    result = ROCAFPLAActivityPuller(engine, session=session).pull()
    assert result["status"] == "FAILED"
    assert result["rows_inserted"] == 0
    assert "unparseable" in result["error"]
    assert _inserts(engine) == []


def test_report_without_adiz_writes_auditable_zero() -> None:
    engine = _engine(latest=date(2026, 9, 28))
    session = _Session({
        AF_LIST_URL: _Resp(LIST_HTML),
        URL_0929: _Resp(_report_html(3, "ROC Armed Forces monitored the situation.")),
    })
    result = ROCAFPLAActivityPuller(engine, session=session).pull()
    assert result["status"] == "SUCCESS"
    rows = {r["sid"]: r for r in _inserts(engine)}
    assert rows[SERIES_ADIZ]["val"] == 0.0
    assert '"adiz_sentence_present": false' in rows[SERIES_ADIZ]["payload"]


def test_first_run_is_bounded() -> None:
    engine = _engine(latest=None)
    session = _Session({AF_LIST_URL: _Resp(LIST_HTML), URL_0929: _Resp(REPORT_HTML)})
    result = ROCAFPLAActivityPuller(engine, session=session).pull()
    # list + at most INITIAL_MAX_DETAILS detail pages
    assert len(session.calls) == 1 + mod.INITIAL_MAX_DETAILS
    assert result["status"] == "PARTIAL"  # the other 9 fake URLs 404 here


def test_own_source_identity() -> None:
    assert ROCAFPLAActivityPuller.SOURCE_NAME == "rocaf_pla_activity"
    assert SERIES_AIRCRAFT.startswith("pla_activity:")


@pytest.mark.parametrize(("aircraft", "sentence", "entries"), [
    (12, "All 12 sorties remained outside Taiwan's ADIZ; none entered Taiwan's ADIZ.", None),
    (12, "All 12 sorties of PLA aircraft never entered Taiwan's southwestern ADIZ.", None),
    (32, "22 of the 32 sorties were monitored, while 6 PLAN ships entered the ADIZ.", None),
    (12, "All 12 sorties entered the ADIZ. ADIZ counts remain unconfirmed.", None),
    (12, "All 12 sorties of PLA aircraft entered Taiwan's southwestern ADIZ?", None),
    (32, "22 of the 32 sorties entered the ADIZ? Counts remain unconfirmed.", None),
    (12, "If confirmed; All 12 sorties entered the ADIZ.", None),
    (12, "All 12 sorties entered the ADIZ; if confirmed.", None),
    (12, "All 12 sorties entered the ADIZ!", None),
    (12, "All 12 sorties entered the ADIZ??", None),
    (12, "All 12 sorties entered the ADIZ.?", None),
    (12, "All 12 sorties entered the ADIZ...", None),
    (12, "All 12 sorties entered the ADIZ;", None),
    (12, "12 of 8 sorties entered the ADIZ.", None),
    (12, "13 of 12 sorties entered the ADIZ.", None),
    (12, "All 13 sorties entered the ADIZ.", None),
    (12, "8 of 32 sorties entered the ADIZ.", None),
    (12, "All 12 sorties of PLA aircraft entered Taiwan's southwestern ADIZ.", 12),
    (32, "22 of the 32 sorties entered Taiwan's northern and southwestern ADIZ.", 22),
    (128, "103 out of 128 sorties crossed the median line and entered Taiwan's southwestern ADIZ.", 103),
    (128, "All 128 sorties entered Taiwan’s northern, central, and southwestern ADIZ.", 128),
    (12, "All <b>12</b> sorties entered Taiwan&rsquo;s southwestern <em>ADIZ</em>.", 12),
    (12, "All 12 sorties entered the ADIZ", 12),
    (12, "ROC Armed Forces monitored the situation.", 0),
])
def test_affirmative_count_contract_through_real_sqlite_writer(
    monkeypatch: pytest.MonkeyPatch, aircraft: int, sentence: str, entries: int | None,
) -> None:
    """Exercise the actual wrapper/save/BasePuller SQL, including honest zero."""
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE source_catalog (id INTEGER PRIMARY KEY, name TEXT, "
            "last_pull_timestamp TEXT, row_count INTEGER)"
        ))
        conn.execute(text(
            "CREATE TABLE raw_series (id INTEGER PRIMARY KEY, series_id TEXT, "
            "source_id INTEGER, obs_date TEXT, value REAL, raw_payload TEXT, "
            "pull_status TEXT, pull_timestamp TEXT DEFAULT CURRENT_TIMESTAMP)"
        ))
        conn.execute(text(
            "INSERT INTO source_catalog VALUES "
            "(7,'taiwan_strait_osint','2020-01-01',123),"
            "(99,'rocaf_pla_activity','2020-01-02',456)"
        ))
        conn.execute(text(
            "INSERT INTO raw_series (series_id,source_id,obs_date,value,pull_status) "
            "VALUES ('taiwan_strait:aircraft_count',7,'2026-09-28',88,'QUARANTINED')"
        ))

    def snapshot() -> tuple[list[tuple], list[tuple]]:
        with engine.connect() as conn:
            return (
                [tuple(r) for r in conn.execute(text("SELECT * FROM source_catalog ORDER BY id"))],
                [tuple(r) for r in conn.execute(text("SELECT * FROM raw_series ORDER BY id"))],
            )

    before = snapshot()
    listing = (
        '<a href="/EN/News/News_Detail.aspx?CID=214&amp;ID=59269">'
        '<span class="Title">PLA activities in the waters</span>'
        '<span class="Time">2026/09/29</span></a>'
    )
    session = _Session({
        AF_LIST_URL: _Resp(listing),
        URL_0929: _Resp(_report_html(aircraft, sentence)),
    })
    monkeypatch.setattr(mod.requests, "Session", lambda: session)
    result = mod.run_rocaf_pla_activity_puller(engine)
    after = snapshot()
    assert after[0] == before[0]
    if entries is None:
        assert (result["status"], result["rows_inserted"]) == ("FAILED", 0)
        assert after == before
    else:
        assert (result["status"], result["rows_inserted"]) == ("SUCCESS", 4)
        assert after[1][0] == before[1][0]
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT value,raw_payload,pull_status FROM raw_series "
                "WHERE source_id=99 AND series_id=:sid"
            ), {"sid": SERIES_ADIZ}).one()
        assert (row.value, row.pull_status) == (float(entries), "SUCCESS")
        assert json.loads(row.raw_payload)["adiz_sentence_present"] is (entries > 0)
    engine.dispose()
