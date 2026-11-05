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
    ann = parse_announcement(html, url=ANN_190_URL, title="official fixture")
    assert ann is not None
    assert [o.term for o in ann.operations] == ["7d"]


def test_parse_announcement_no_operation_day() -> None:
    html = '<div id="zoom"><p>2026年2月1日中国人民银行不开展逆回购操作。</p></div>'
    ann = parse_announcement(html, url=ANN_190_URL, title="official fixture")
    assert ann is not None
    assert ann.no_operation is True
    assert ann.operations == ()


def test_parse_announcement_unparseable_returns_none() -> None:
    assert parse_announcement("<html>maintenance</html>", url=ANN_190_URL, title="official fixture") is None


def test_series_ids() -> None:
    ann = parse_announcement(ANN_HTML, url=ANN_190_URL, title="official fixture")
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
    def __init__(self, text: str, status: int = 200, url: str | None = None) -> None:
        self.text = text
        self.status_code = status
        self.encoding = "utf-8"
        self.url = url

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
            response = self.pages[url]
            if response.url is None:
                response.url = url  # fake transport's established final URL
            return response
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


def test_guard_foreign_and_missing_provenance() -> None:
    for url in ("", "https://example.invalid/not-pboc", ANN_190_URL.replace("https:", "http:"), ANN_190_URL + "?redirect=elsewhere"):
        assert parse_announcement(ANN_HTML, url=url, title="official fixture") is None
        index = f'<font><a href="{url}">公开市场业务交易公告 [2026]第190号</a></font><span>2026-09-28</span>'
        assert parse_index(index) == []
    assert parse_announcement(ANN_HTML, url=ANN_190_URL, title="  ") is None
    assert parse_announcement(ANN_HTML, url=ANN_190_URL, title="official fixture") is not None


def test_guard_negative_malformed_and_explicit_zero_amounts() -> None:
    for amount in ("-100", "−100", "+100", "1,000", "1.2.3", "1e3", ".5", "9" * 400):
        html = f'<div id="zoom">2026年9月28日开展了{amount}亿元7天期逆回购操作</div>'
        assert parse_announcement(html, url=ANN_190_URL, title="fixture") is None
    for amount, expected in (("0", 0.0), ("0.0", 0.0), ("10.5", 1.05)):
        html = f'<div id="zoom">2026年9月28日开展了{amount}亿元7天期逆回购操作</div>'
        ann = parse_announcement(html, url=ANN_190_URL, title="fixture")
        assert ann is not None
        assert ann.operations[0].amount_cny_bn == pytest.approx(expected)
        assert ann.operations[0].rate_pct is None

    clauses = '<div id="zoom">2026年9月28日开展了100亿元7天期逆回购操作，0亿元14天期逆回购操作</div>'
    ann = parse_announcement(clauses, url=ANN_190_URL, title="fixture")
    assert ann is not None
    assert {op.term: op.amount_cny_bn for op in ann.operations} == {"7d": 10.0, "14d": 0.0}


def test_guard_duplicate_terms_conflict_and_equal_values() -> None:
    for second, valid in (("100", True), ("200", False)):
        html = f'<div id="zoom">2026年9月28日开展了100亿元7天期逆回购操作，开展了{second}亿元7天期逆回购操作</div>'
        ann = parse_announcement(html, url=ANN_190_URL, title="fixture")
        assert (ann is not None) is valid
        if ann is not None:
            assert len(ann.operations) == 1


def test_guard_table_prose_and_duplicate_rate_conflicts() -> None:
    for amount, rate, valid in (("100", "1.4", True), ("200", "1.4", False), ("100", "1.5", False), ("-100", "1.4", False)):
        html = ('<div id="zoom">2026年9月28日开展了100亿元7天期逆回购操作'
                '<table><tr><td>期限</td><td>利率</td><td>中标量</td></tr>'
                '<tr><td>7天</td><td>1.4%</td><td>100亿元</td></tr>'
                f'<tr><td>7天</td><td>{rate}%</td><td>{amount}亿元</td></tr></table></div>')
        assert (parse_announcement(html, url=ANN_190_URL, title="fixture") is not None) is valid


def test_guard_save_checks_whole_batch_before_transaction() -> None:
    valid = parse_announcement(ANN_HTML, url=ANN_190_URL, title="fixture")
    assert valid is not None
    engine = MagicMock()
    puller = object.__new__(PBOCOmoAnnouncementsPuller)
    puller.engine = engine
    bad = mod.Announcement(url="", title="", op_date=valid.op_date, operations=valid.operations)
    with pytest.raises(ValueError, match="official URL and title"):
        puller.save([valid, bad])
    engine.begin.assert_not_called()
    changed = mod.Announcement(url=ANN_190_URL, title="fixture", op_date=valid.op_date,
                              operations=(mod.ReverseRepoOp("7d", 999.0, 1.4),))
    with pytest.raises(ValueError, match="conflicting duplicate"):
        puller.save([valid, changed])
    engine.begin.assert_not_called()


def test_guard_save_negative_nonfinite_and_zero_values() -> None:
    for amount in (-1.0, float("nan"), float("inf")):
        puller = object.__new__(PBOCOmoAnnouncementsPuller)
        puller.engine = MagicMock()
        bad = mod.Announcement(ANN_190_URL, "fixture", date(2026, 9, 28),
                               (mod.ReverseRepoOp("7d", amount, None),))
        with pytest.raises(ValueError, match="finite and nonnegative"):
            puller.save([bad])
        puller.engine.begin.assert_not_called()
    puller = object.__new__(PBOCOmoAnnouncementsPuller)
    puller.engine = MagicMock()
    puller._get_existing_dates = MagicMock(side_effect=lambda *args, **kwargs: set())
    puller._insert_raw = MagicMock()
    zero = mod.Announcement(ANN_190_URL, "fixture", date(2026, 9, 28),
                            (mod.ReverseRepoOp("7d", 0.0, None),))
    assert puller.save([zero]) == 1
    assert puller._insert_raw.call_args.args[3] == 0.0


def test_guard_visible_amount_tokens_entities_and_signs() -> None:
    for amount in ("&#45;100", "&#x2212;100", "－100", "&minus;100", "﹣100", "➖100", "&#x2D;&nbsp;100", "1&#44;000", "<span>&#45;</span>100"):
        html = f'<div id="zoom">2026年9月28日开展了{amount}亿元7天期逆回购操作</div>'
        assert parse_announcement(html, url=ANN_190_URL, title="fixture") is None
        table = ('<div id="zoom">2026年9月28日<table>'
                 '<tr><td>期限</td><td>中标量</td></tr>'
                 f'<tr><td>7天</td><td>{amount}亿元</td></tr></table></div>')
        assert parse_announcement(table, url=ANN_190_URL, title="fixture") is None
    for amount, expected in (("&#48;", 0.0), ("&#49;25.5", 12.55), ("0", 0.0)):
        html = f'<div id="zoom">2026年9月28日开展了{amount}亿元7天期逆回购操作</div>'
        ann = parse_announcement(html, url=ANN_190_URL, title="fixture")
        assert ann is not None
        assert ann.operations[0].amount_cny_bn == pytest.approx(expected)
        assert ann.operations[0].rate_pct is None


def test_guard_get_requires_established_official_final_url() -> None:
    from types import SimpleNamespace

    puller = object.__new__(PBOCOmoAnnouncementsPuller)
    for requested in (PBOC_OMO_INDEX_URL, ANN_190_URL):
        for final in (None, "", "https://example.invalid/foreign", ANN_190_URL.replace("https:", "http:"), ANN_190_URL + "#fragment", 7):
            response = SimpleNamespace(url=final, text=ANN_HTML, encoding="utf-8", raise_for_status=lambda: None)
            puller._session = SimpleNamespace(get=lambda *args, **kwargs: response)
            with pytest.raises(ValueError, match="final URL"):
                puller._get(requested)
        response = SimpleNamespace(text=ANN_HTML, encoding="utf-8", raise_for_status=lambda: None)
        puller._session = SimpleNamespace(get=lambda *args, **kwargs: response)
        with pytest.raises(ValueError, match="final URL"):
            puller._get(requested)
    for requested in (PBOC_OMO_INDEX_URL, ANN_190_URL):
        response = SimpleNamespace(url=requested, text=ANN_HTML, encoding="utf-8", raise_for_status=lambda: None)
        puller._session = SimpleNamespace(get=lambda *args, **kwargs: response)
        assert puller._get(requested) == (ANN_HTML, requested)


def test_guard_official_redirect_records_final_detail_url() -> None:
    redirected = ANN_190_URL.replace("2026092808454683233", "2026092808454683234")
    puller = object.__new__(PBOCOmoAnnouncementsPuller)
    puller._session = _FakeSession({PBOC_OMO_INDEX_URL: _Resp(INDEX_HTML), ANN_190_URL: _Resp(ANN_HTML, url=redirected)})
    announcements, errors = puller.fetch(since=date(2026, 9, 27))
    assert not errors and len(announcements) == 1
    assert announcements[0].url == redirected
    puller.engine = MagicMock()
    puller._get_existing_dates = MagicMock(side_effect=lambda *args, **kwargs: set())
    puller._insert_raw = MagicMock()
    assert puller.save(announcements) == 4
    assert all(call.kwargs["raw_payload"]["announcement_url"] == redirected for call in puller._insert_raw.call_args_list)


def test_guard_foreign_detail_and_index_fail_without_rows() -> None:
    puller = object.__new__(PBOCOmoAnnouncementsPuller)
    puller._get_latest_date = MagicMock(return_value=date(2026, 9, 27))
    puller.save = MagicMock()
    puller._session = _FakeSession({PBOC_OMO_INDEX_URL: _Resp(INDEX_HTML), ANN_190_URL: _Resp(ANN_HTML, url="https://example.invalid/foreign")})
    assert puller.pull()["status"] == "FAILED"
    puller.save.assert_not_called()
    puller._session = _FakeSession({PBOC_OMO_INDEX_URL: _Resp(INDEX_HTML, url="https://example.invalid/foreign")})
    assert puller.pull()["status"] == "FAILED"
    puller.save.assert_not_called()
