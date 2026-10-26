"""news_scraper honest outcomes + live Yahoo Finance feed URL.

grid-hermes journal, grid-svr, 2026-10-20 03:24Z (repeated 04:02Z):

    ingestion.altdata.news_scraper:_fetch_feed:359 — Failed to fetch Yahoo
    Finance: 404 Client Error: Not Found for url:
    https://finance.yahoo.com/news/rssindex
    ... News cnbc: 30 articles scored, 2 stored / marketwatch 6 / bloomberg 2
    / fed 4 / sec_edgar 1  (= 15 rows committed)
    pull_log smart:news_scraper FAILED "puller did not report committed
    write count"

Two defects: the dead feed URL, and a ``pull_all`` summary that carried no
committed write count, so one bad feed made the whole run FAILED with
``rows_inserted = 0`` instead of PARTIAL with the real count. Every test
below fails on main.
"""

from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import requests

from ingestion.altdata import news_scraper as ns
from ingestion.altdata.news_scraper import (
    RSS_FEEDS,
    FeedFetchError,
    NewsArticle,
    NewsScraperPuller,
)
from ingestion.smart_scheduler import (
    OUTCOME_FAILED,
    OUTCOME_NO_NEW_DATA,
    OUTCOME_PARTIAL,
    OUTCOME_SUCCESS,
    _classify_outcome,
)

DEAD_URL = "https://finance.yahoo.com/news/rssindex"
LIVE_PREFIX = "https://feeds.finance.yahoo.com/rss/2.0/headline?"
NOT_FOUND = "404 Client Error: Not Found for url: " + DEAD_URL


# ── fakes ────────────────────────────────────────────────────────────────


def _rss(n: int, prefix: str) -> bytes:
    items = "".join(
        f"<item><title>{prefix} story {i}</title>"
        f"<link>https://example.test/{prefix}/{i}</link>"
        f"<description>d</description></item>"
        for i in range(n)
    )
    return (
        '<?xml version="1.0"?><rss version="2.0"><channel>'
        f"{items}</channel></rss>"
    ).encode()


class _Resp:
    def __init__(self, status: int, content: bytes = b"") -> None:
        self.status_code = status
        self.content = content

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(NOT_FOUND)


class _Session:
    """requests.Session stand-in keyed by URL; unexpected URLs are a bug."""

    def __init__(self, responses: dict[str, _Resp]) -> None:
        self.responses = responses
        self.calls: list[str] = []

    def get(self, url: str, timeout: float | None = None) -> _Resp:
        self.calls.append(url)
        if url not in self.responses:
            raise AssertionError(f"unexpected fetch of {url}")
        return self.responses[url]


def _responses(per_feed: int = 3, **override: _Resp) -> dict[str, _Resp]:
    out = {cfg["url"]: _Resp(200, _rss(per_feed, key)) for key, cfg in RSS_FEEDS.items()}
    for key, resp in override.items():
        out[RSS_FEEDS[key]["url"]] = resp
    return out


def _puller(monkeypatch: pytest.MonkeyPatch, session: _Session) -> NewsScraperPuller:
    """A NewsScraperPuller with no DB: storage is a counting stand-in."""
    p = NewsScraperPuller.__new__(NewsScraperPuller)
    p.engine = MagicMock()
    p.source_id = 297
    p._session = session
    p._seen_hashes = set()
    p._llm = None
    monkeypatch.setattr(ns, "_last_fetch", {})
    monkeypatch.setattr(p, "_ensure_features", lambda: None)
    monkeypatch.setattr(p, "_is_duplicate", lambda _h: False)
    # one news_articles row per article, no raw_series / aggregate rows, so
    # rows_inserted is exactly the article count (15 in the journal's run)
    monkeypatch.setattr(p, "_store_articles", lambda articles: (len(articles), 0))
    monkeypatch.setattr(p, "_store_daily_aggregates", lambda _arts, _d: 0)
    return p


# ── (a) the feed URL ─────────────────────────────────────────────────────


def test_yahoo_feed_is_the_live_headline_endpoint_not_rssindex() -> None:
    url = RSS_FEEDS["yahoo_finance"]["url"]
    assert url != DEAD_URL
    assert url.startswith(LIVE_PREFIX)
    assert "s=" in url and "region=US" in url
    assert NewsScraperPuller.SOURCE_CONFIG["base_url"] != DEAD_URL
    # the other feeds are untouched
    assert RSS_FEEDS["cnbc"]["url"].startswith("https://search.cnbc.com/")
    assert RSS_FEEDS["marketwatch"]["url"] == "https://feeds.marketwatch.com/marketwatch/topstories"
    assert RSS_FEEDS["bloomberg"]["url"] == "https://feeds.bloomberg.com/markets/news.rss"
    assert RSS_FEEDS["fed"]["url"] == "https://www.federalreserve.gov/feeds/press_all.xml"
    assert RSS_FEEDS["sec_edgar"]["url"].startswith("https://www.sec.gov/cgi-bin/browse-edgar")


def test_fetch_feed_raises_instead_of_swallowing_a_404(monkeypatch) -> None:
    session = _Session(_responses(yahoo_finance=_Resp(404)))
    p = _puller(monkeypatch, session)
    with pytest.raises(FeedFetchError) as exc_info:
        p._fetch_feed("yahoo_finance")
    assert exc_info.value.source_key == "yahoo_finance"
    assert "404" in exc_info.value.detail
    with pytest.raises(FeedFetchError):
        p._fetch_feed("no_such_feed")
    # a 200 with 3 items still parses
    assert len(p._fetch_feed("cnbc")) == 3


# ── (b) the write-count contract ─────────────────────────────────────────


def test_dead_feed_is_partial_with_the_committed_count(monkeypatch) -> None:
    """The journal's run: 5 feeds stored rows, Yahoo 404'd -> PARTIAL, 15."""
    session = _Session(_responses(yahoo_finance=_Resp(404)))
    out = _puller(monkeypatch, session).pull_all(use_llm=False, pause_seconds=0)

    assert out["status"] == "PARTIAL"
    assert out["rows_inserted"] == 15
    assert out["articles_inserted"] == 15
    assert (out["feeds_attempted"], out["feeds_succeeded"], out["feeds_failed"]) == (6, 5, 1)
    assert out["errors"] == [f"yahoo_finance: {NOT_FOUND}"]
    assert out["error"].startswith("5 of 6 feeds checked, 15 rows written: yahoo_finance: 404")
    assert out["source_counts"]["yahoo_finance"] == 0

    outcome, rows, note = _classify_outcome(out)
    assert (outcome, rows) == (OUTCOME_PARTIAL, 15)
    assert "did not report committed write count" not in (note or "")
    assert "yahoo_finance: 404" in note


def test_all_feeds_good_is_success_with_the_count(monkeypatch) -> None:
    session = _Session(_responses())
    out = _puller(monkeypatch, session).pull_all(use_llm=False, pause_seconds=0)

    assert out["status"] == "SUCCESS"
    assert out["rows_inserted"] == 18
    assert out["error"] is None and out["errors"] is None
    assert (out["feeds_attempted"], out["feeds_succeeded"], out["feeds_failed"]) == (6, 6, 0)
    assert len(session.calls) == 6
    assert _classify_outcome(out)[:2] == (OUTCOME_SUCCESS, 18)


def test_all_feeds_good_but_nothing_new_is_no_new_data(monkeypatch) -> None:
    session = _Session(_responses(per_feed=0))
    out = _puller(monkeypatch, session).pull_all(use_llm=False, pause_seconds=0)

    assert out["status"] == "SUCCESS" and out["rows_inserted"] == 0
    assert _classify_outcome(out)[:2] == (OUTCOME_NO_NEW_DATA, 0)


def test_every_feed_dead_is_failed_with_zero_rows(monkeypatch) -> None:
    session = _Session({cfg["url"]: _Resp(404) for cfg in RSS_FEEDS.values()})
    out = _puller(monkeypatch, session).pull_all(use_llm=False, pause_seconds=0)

    assert out["status"] == "FAILED"
    assert out["rows_inserted"] == 0
    assert len(out["errors"]) == 6
    outcome, rows, note = _classify_outcome(out)
    assert (outcome, rows) == (OUTCOME_FAILED, 0)
    assert "0 of 6 feeds checked" in note


def test_rows_inserted_sums_articles_raw_series_and_aggregates(monkeypatch) -> None:
    session = _Session(_responses())
    p = _puller(monkeypatch, session)
    monkeypatch.setattr(p, "_store_articles", lambda articles: (len(articles), 2 * len(articles)))
    monkeypatch.setattr(p, "_store_daily_aggregates", lambda arts, _d: 4 if arts else 0)
    out = p.pull_all(use_llm=False, pause_seconds=0)

    assert out["articles_inserted"] == 18
    assert out["raw_series_inserted"] == 36 + 4
    assert out["rows_inserted"] == 18 + 36 + 4
    assert _classify_outcome(out)[:2] == (OUTCOME_SUCCESS, 58)


def test_aggregate_failure_keeps_committed_articles_as_partial(monkeypatch) -> None:
    session = _Session(_responses())
    p = _puller(monkeypatch, session)

    def _boom(_arts, _d):
        raise RuntimeError("raw_series unavailable")

    monkeypatch.setattr(p, "_store_daily_aggregates", _boom)
    out = p.pull_all(use_llm=False, pause_seconds=0)

    assert out["status"] == "PARTIAL" and out["rows_inserted"] == 18
    assert out["errors"] == ["daily_aggregates: raw_series unavailable"]
    assert _classify_outcome(out)[:2] == (OUTCOME_PARTIAL, 18)


def test_rate_limited_feed_is_not_fetched_and_run_is_partial(monkeypatch) -> None:
    session = _Session(_responses())
    p = _puller(monkeypatch, session)
    ns._last_fetch["bloomberg"] = time.time()
    out = p.pull_all(use_llm=False, pause_seconds=0)

    assert RSS_FEEDS["bloomberg"]["url"] not in session.calls
    assert out["feeds_rate_limited"] == ["bloomberg"]
    assert (out["feeds_attempted"], out["feeds_succeeded"], out["feeds_failed"]) == (5, 5, 0)
    assert out["status"] == "PARTIAL" and out["rows_inserted"] == 15
    assert _classify_outcome(out)[:2] == (OUTCOME_PARTIAL, 15)


def test_disabled_feed_is_reported_with_reason_never_fetched(monkeypatch) -> None:
    """The fallback the fix allows: a dead feed is disabled, not 404'd every tick."""
    monkeypatch.setitem(RSS_FEEDS, "yahoo_finance", {
        "url": DEAD_URL, "label": "Yahoo Finance",
        "disabled": "404 since 2026-10-20; no anonymous endpoint",
    })
    responses = _responses()
    responses.pop(DEAD_URL)  # any fetch of it fails the test
    session = _Session(responses)
    out = _puller(monkeypatch, session).pull_all(use_llm=False, pause_seconds=0)

    assert DEAD_URL not in session.calls
    assert out["feeds_disabled"] == {"yahoo_finance": "404 since 2026-10-20; no anonymous endpoint"}
    assert (out["feeds_attempted"], out["feeds_succeeded"], out["feeds_failed"]) == (5, 5, 0)
    assert out["status"] == "SUCCESS" and out["rows_inserted"] == 15
    assert out["source_counts"]["yahoo_finance"] == 0
    assert _classify_outcome(out)[:2] == (OUTCOME_SUCCESS, 15)


# ── _store_articles counts committed rows under savepoints ───────────────


def _article(title: str, tickers: list[str]) -> NewsArticle:
    return NewsArticle(
        title=title, source="cnbc", url="https://example.test/" + title,
        published=None, summary="", tickers=tickers, dedup_hash=title,
    )


def test_store_articles_counts_affected_rows_under_savepoints(monkeypatch) -> None:
    conn = MagicMock()
    conn.begin_nested.return_value.__exit__.return_value = False  # never swallow
    conn.execute.side_effect = [
        SimpleNamespace(rowcount=1),          # inserted
        SimpleNamespace(rowcount=0),          # ON CONFLICT DO NOTHING
        RuntimeError("value too long"),       # statement failed
    ]
    engine = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn

    p = NewsScraperPuller.__new__(NewsScraperPuller)
    p.engine = engine
    p.source_id = 297
    p._seen_hashes = set()
    inserted_raw: list[str] = []
    monkeypatch.setattr(p, "_row_exists", lambda _sid, _d, _c: False)
    monkeypatch.setattr(
        p, "_insert_raw",
        lambda conn, series_id, obs_date, value, raw_payload=None: inserted_raw.append(series_id),
    )
    monkeypatch.setattr(p, "_emit_signal", lambda _c, _a, _d: None)

    articles = [_article("a", ["AAPL"]), _article("b", []), _article("c", ["MSFT"])]
    assert p._store_articles(articles) == (1, 1)
    # one raw_series row for the one committed article; none for the no-op
    assert inserted_raw == ["NEWS:cnbc:AAPL:NEUTRAL"]
    # every article insert and the raw_series insert ran inside a SAVEPOINT,
    # so the failed third statement cannot abort the PG transaction
    assert conn.begin_nested.call_count == 4
    assert p._seen_hashes == {"a", "b"}
    assert p._store_articles([]) == (0, 0)
