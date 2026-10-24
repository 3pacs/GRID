"""Tests for ingestion/altdata/pboc_omo_official.py.

Fixtures under tests/fixtures/pboc_omo/ are real pbc.gov.cn pages recorded
from grid-svr on 2026-09-29 (one index page + one announcement). No test
touches the network: HTTP goes through a fake session.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests

from ingestion.altdata import pboc_omo_official as mod
from ingestion.altdata.pboc_omo_official import (
    PBOC_OMO_INDEX_URL,
    PBOCOmoAnnouncementsPuller,
    parse_announcement,
    parse_index,
    series_ids_for,
)

FIXTURES = Path(__file__).parent / "fixtures" / "pboc_omo"
INDEX_HTML = (FIXTURES / "index_20260928.html").read_text(encoding="utf-8")
ANN_HTML = (FIXTURES / "announcement_2026_190.html").read_text(encoding="utf-8")
ANN_190_URL = (
    "https://www.pbc.gov.cn/zhengcehuobisi/125207/125213/125431/125475/"
    "2026092808454683233/index.html"
)


# ---------------------------------------------------------------------------
# Pure parsing
# ---------------------------------------------------------------------------


def test_parse_index_lists_announcements_newest_first() -> None:
    items = parse_index(INDEX_HTML)
    assert len(items) == 20
    assert items[0].url == ANN_190_URL
    assert items[0].published == date(2026, 9, 28)
    assert "第190号" in items[0].title
    assert all(a.published >= b.published for a, b in zip(items, items[1:]))


def test_parse_index_empty_on_layout_change() -> None:
    assert parse_index("<html><body>nothing here</body></html>") == []


def test_parse_announcement_reads_table_and_prose() -> None:
    ann = parse_announcement(ANN_HTML, url=ANN_190_URL, title="t")
    assert ann is not None
    assert ann.op_date == date(2026, 9, 28)
    ops = {o.term: o for o in ann.operations}
    # 7-day: table gives rate 1.40% and winning volume 1390亿元 = 139.0 bn
    assert ops["7d"].amount_cny_bn == pytest.approx(139.0)
    assert ops["7d"].rate_pct == pytest.approx(1.40)
    # Prose-only operations carry an amount but no rate (none published).
    assert ops["on"].amount_cny_bn == pytest.approx(661.0)
    assert ops["on"].rate_pct is None
    assert ops["14d"].amount_cny_bn == pytest.approx(300.0)
    assert ops["14d"].rate_pct is None


def test_parse_announcement_skips_outright_reverse_repo() -> None:
    html = (
        '<div id="zoom"><p>2026年9月10日中国人民银行开展了5000亿元3个月期买断式逆回购操作。'
        "并开展了1000亿元7天期逆回购操作。</p></div>打印本页"
    )
    ann = parse_announcement(html)
    assert ann is not None
    assert [o.term for o in ann.operations] == ["7d"]


def test_parse_announcement_no_operation_day() -> None:
    html = '<div id="zoom"><p>2026年2月1日中国人民银行不开展逆回购操作。</p></div>'
    ann = parse_announcement(html)
    assert ann is not None
    assert ann.no_operation is True
    assert ann.operations == ()


def test_parse_announcement_unparseable_returns_none() -> None:
    assert parse_announcement("<html>maintenance</html>") is None


def test_series_ids() -> None:
    ann = parse_announcement(ANN_HTML)
    assert ann is not None
    seven = next(o for o in ann.operations if o.term == "7d")
    assert series_ids_for(seven) == (
        "pboc_omo_ann:reverse_repo_7d_amount_cny_bn",
        "pboc_omo_ann:reverse_repo_7d_rate_pct",
    )


# ---------------------------------------------------------------------------
# Puller orchestration (fake HTTP, mock engine)
# ---------------------------------------------------------------------------


class _Resp:
    def __init__(self, text: str, status: int = 200) -> None:
        self.text = text
        self.status_code = status
        self.encoding = "utf-8"

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class _FakeSession:
    def __init__(self, pages: dict[str, _Resp]) -> None:
        self.pages = pages
        self.headers: dict[str, str] = {}
        self.calls: list[str] = []

    def get(self, url: str, timeout: int = 0) -> _Resp:  # noqa: ARG002
        self.calls.append(url)
        if url in self.pages:
            return self.pages[url]
        return _Resp("not found", 404)


def _engine(latest: date | None = None, existing_dates: list[date] | None = None) -> MagicMock:
    """Mock engine: source id resolves to 7; MAX(obs_date) -> latest."""
    engine = MagicMock()

    connect_conn = MagicMock()

    def _connect_execute(stmt, params=None):  # noqa: ANN001, ARG001
        res = MagicMock()
        sql = str(stmt)
        if "MAX(obs_date)" in sql:
            res.fetchone.return_value = (latest,)
        else:
            res.fetchone.return_value = (7,)
        return res

    connect_conn.execute.side_effect = _connect_execute
    engine.connect.return_value.__enter__ = MagicMock(return_value=connect_conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    begin_conn = MagicMock()

    def _begin_execute(stmt, params=None):  # noqa: ANN001, ARG001
        res = MagicMock()
        res.fetchall.return_value = [(d,) for d in (existing_dates or [])]
        res.fetchone.return_value = None
        return res

    begin_conn.execute.side_effect = _begin_execute
    engine.begin.return_value.__enter__ = MagicMock(return_value=begin_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    engine._begin_conn = begin_conn
    return engine


def _inserts(engine: MagicMock) -> list[dict]:
    out = []
    for call in engine._begin_conn.execute.call_args_list:
        stmt, params = call.args[0], (call.args[1] if len(call.args) > 1 else None)
        if "INSERT INTO raw_series" in str(stmt):
            out.append(params)
    return out


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)


def test_pull_inserts_only_new_announcements_with_default_pull_timestamp() -> None:
    engine = _engine(latest=date(2026, 9, 27))
    session = _FakeSession({PBOC_OMO_INDEX_URL: _Resp(INDEX_HTML), ANN_190_URL: _Resp(ANN_HTML)})
    result = PBOCOmoAnnouncementsPuller(engine, session=session).pull()

    # since=09-27 → only the 09-28 announcement is fetched.
    assert session.calls == [PBOC_OMO_INDEX_URL, ANN_190_URL]
    assert result["status"] == "SUCCESS"
    rows = _inserts(engine)
    # 7d amount + 7d rate + on amount + 14d amount
    assert result["rows_inserted"] == 4 == len(rows)
    by_sid = {r["sid"]: r for r in rows}
    assert by_sid["pboc_omo_ann:reverse_repo_7d_amount_cny_bn"]["val"] == pytest.approx(139.0)
    assert by_sid["pboc_omo_ann:reverse_repo_7d_rate_pct"]["val"] == pytest.approx(1.40)
    for r in rows:
        assert r["od"] == date(2026, 9, 28)
        assert r["status"] == "SUCCESS"
        assert r["src"] == 7
        # pull_timestamp is NOT supplied, so the column default (fetch time) applies.
        assert "pull_timestamp" not in r


def test_pull_skips_rows_already_stored() -> None:
    engine = _engine(latest=date(2026, 9, 28), existing_dates=[date(2026, 9, 28)])
    session = _FakeSession({PBOC_OMO_INDEX_URL: _Resp(INDEX_HTML), ANN_190_URL: _Resp(ANN_HTML)})
    result = PBOCOmoAnnouncementsPuller(engine, session=session).pull()
    assert result["status"] == "SUCCESS"
    assert result["rows_inserted"] == 0
    assert _inserts(engine) == []


def test_index_failure_is_failed_and_writes_nothing() -> None:
    engine = _engine()
    session = _FakeSession({PBOC_OMO_INDEX_URL: _Resp("blocked", 403)})
    result = PBOCOmoAnnouncementsPuller(engine, session=session).pull()
    assert result["status"] == "FAILED"
    assert result["rows_inserted"] == 0
    assert _inserts(engine) == []


def test_index_layout_change_is_failed() -> None:
    engine = _engine()
    session = _FakeSession({PBOC_OMO_INDEX_URL: _Resp("<html>new layout</html>")})
    result = PBOCOmoAnnouncementsPuller(engine, session=session).pull()
    assert result["status"] == "FAILED"
    assert _inserts(engine) == []


def test_all_details_failing_is_failed_not_success() -> None:
    engine = _engine(latest=date(2026, 9, 27))
    session = _FakeSession({PBOC_OMO_INDEX_URL: _Resp(INDEX_HTML)})  # detail 404s
    result = PBOCOmoAnnouncementsPuller(engine, session=session).pull()
    assert result["status"] == "FAILED"
    assert result["rows_inserted"] == 0
    assert _inserts(engine) == []


def test_some_details_failing_is_partial() -> None:
    engine = _engine(latest=date(2026, 9, 24))
    session = _FakeSession({PBOC_OMO_INDEX_URL: _Resp(INDEX_HTML), ANN_190_URL: _Resp(ANN_HTML)})
    result = PBOCOmoAnnouncementsPuller(engine, session=session).pull()
    assert result["status"] == "PARTIAL"
    assert result["rows_inserted"] == 4
    assert result["errors"]


def test_source_identity_is_separate_from_old_pboc_omo() -> None:
    assert PBOCOmoAnnouncementsPuller.SOURCE_NAME == "pboc_omo_announcements"
    assert PBOCOmoAnnouncementsPuller.SOURCE_NAME != "pboc_omo"
