"""Per-ticker social series: SocialSentimentPuller.pull_ticker_series (2026-11-17).

No network: HTTP goes through a stubbed session, sockets are refused, and the
database is in-memory SQLite. Fixture shapes follow what api.bsky.app,
apewisdom.io, arctic-shift.photon-reddit.com and tradestie.com returned on
2026-11-18 (fields trimmed to the ones read).
"""

from __future__ import annotations

import inspect
import json
import socket
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

import ingestion.social_sentiment as mod
from ingestion.altdata import arctic_shift, reddit_oauth
from tests.arctic_shift_fakes import COMMENTS_URL, POSTS_URL, FakeArctic, FakeResp
from tests.arctic_shift_fakes import comment as a_comment
from tests.arctic_shift_fakes import post as a_post

FETCH = datetime(2026, 11, 17, 1, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _offline(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    def _refuse(*_a: Any, **_k: Any) -> None:
        raise AssertionError("network access in a unit test")
    monkeypatch.setattr(socket.socket, "connect", _refuse)
    monkeypatch.setattr(mod, "BLUESKY_REQUEST_GAP_S", 0.0)
    monkeypatch.setattr(mod, "APEWISDOM_REQUEST_GAP_S", 0.0)
    monkeypatch.setattr(reddit_oauth, "reddit_credentials_configured", lambda: False)
    monkeypatch.setenv(arctic_shift.LOCK_PATH_ENV, str(tmp_path / "arctic.lock"))
    monkeypatch.setattr(arctic_shift, "MIN_INTERVAL_S", 0.0)


@pytest.fixture()
def engine() -> Any:
    eng = create_engine("sqlite://", poolclass=StaticPool,
                        connect_args={"check_same_thread": False})
    with eng.begin() as conn:
        conn.execute(text(
            "CREATE TABLE source_catalog (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "name TEXT UNIQUE NOT NULL, base_url TEXT, cost_tier TEXT, latency_class TEXT, "
            "pit_available BOOLEAN, revision_behavior TEXT, trust_score TEXT, "
            "priority_rank INTEGER)"))
        conn.execute(text(
            "CREATE TABLE raw_series (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "series_id TEXT NOT NULL, source_id INTEGER NOT NULL, obs_date DATE NOT NULL, "
            "value REAL, raw_payload TEXT, pull_status TEXT NOT NULL, "
            "pull_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"))
    return eng


def _resp(payload: Any = None, status: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = payload
    if status >= 400:
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(f"{status} Client Error")
    return resp


def _post(uri: str, body: str, indexed: datetime, likes: int = 0, reposts: int = 0) -> dict:
    return {"uri": uri, "indexedAt": indexed.isoformat().replace("+00:00", "Z"),
            "record": {"text": body, "createdAt": "2020-01-01T00:00:00Z"},
            "likeCount": likes, "repostCount": reposts, "replyCount": 1, "quoteCount": 0}


def _bsky_nvda() -> dict:
    h = timedelta(hours=1)
    return {"posts": [
        _post("at://a/1", "Trimmed $NVDA into the close", FETCH - h, likes=5, reposts=1),
        _post("at://a/2", "$nvda calls printing", FETCH - 2 * h, likes=2),
        _post("at://a/3", "NVDA without a cashtag is not counted", FETCH - 3 * h),
        _post("at://a/4", "$NVDAX is another symbol", FETCH - 3 * h),
        _post("at://a/5", "Long $NVDA, short $AMD", FETCH - 4 * h),
        _post("at://a/1", "Trimmed $NVDA into the close", FETCH - h, likes=5, reposts=1),  # dupe
        _post("at://a/6", "$NVDA indexed after the fetch", FETCH + h),
        _post("at://a/7", "$NVDA from two days ago", FETCH - 48 * h),
    ], "cursor": "c1", "hitsTotal": 823}


_APE = {
    "wallstreetbets": [
        {"count": 3, "pages": 2, "current_page": 1, "results": [
            {"rank": 1, "ticker": "NVDA", "name": "NVIDIA", "mentions": 1375, "upvotes": 4000,
             "rank_24h_ago": 2, "mentions_24h_ago": 886},
            {"rank": 2, "ticker": "SPY", "name": "SPDR S&amp;P 500", "mentions": "404",
             "upvotes": 900, "rank_24h_ago": 1, "mentions_24h_ago": 172}]},
        {"count": 3, "pages": 2, "current_page": 2, "results": [
            {"rank": 3, "ticker": "SLS", "name": "SELLAS", "mentions": 366, "upvotes": 50,
             "rank_24h_ago": None, "mentions_24h_ago": None}]},
    ],
    "CryptoCurrency": [
        {"count": 1, "pages": 1, "current_page": 1, "results": [
            {"rank": 1, "ticker": "BTC.X", "name": "Bitcoin", "mentions": 57, "upvotes": 300,
             "rank_24h_ago": 1, "mentions_24h_ago": 60}]},
    ],
}


FT = FETCH.timestamp()

# Tradestie's answer for 2026-11-16 (the US/Eastern day before the fetch), trimmed.
_TRADESTIE = [
    {"no_of_comments": 255, "sentiment": "Bullish", "sentiment_score": 0.042, "ticker": "NVDA"},
    {"no_of_comments": 27, "sentiment": "Bearish", "sentiment_score": -0.006, "ticker": "SPY"},
    {"no_of_comments": 9, "sentiment": "Bullish", "sentiment_score": 7.5, "ticker": "AMD"},
    {"no_of_comments": "12", "sentiment": "Bullish", "sentiment_score": 0.1, "ticker": "AAPL"},
    {"no_of_comments": 3, "sentiment": "Bullish", "sentiment_score": 0.1, "ticker": "<script>"},
]


def _arctic() -> FakeArctic:
    h = 3600
    posts = [
        a_post("w1", "wallstreetbets", FT - 1 * h, "NVDA calls into earnings", "loading up"),
        a_post("w2", "wallstreetbets", FT - 2 * h, "Bought $nvda calls", ""),
        a_post("w3", "wallstreetbets", FT - 3 * h, "nvidia and meta", "lowercase nvda"),
        a_post("w4", "wallstreetbets", FT - 5 * h, "Short NVDA, long SPY calls", ""),
        a_post("w5", "wallstreetbets", FT - 30 * h, "NVDA from yesterday", ""),
        a_post("s1", "stocks", FT - 4 * h, "SPY at highs?", ""),
    ]
    comments = [a_comment(f"c{i}", "w1", FT - 60 * (i + 1), body="calls", sub="wallstreetbets")
                for i in range(3)]
    return FakeArctic(posts=posts, comments=comments)


class Router:
    """Stub session: answers by URL and records every request."""

    def __init__(self, bsky: dict[str, Any] | None = None, fail: set[str] = frozenset(),
                 refuse_cursor: bool = False, arctic: FakeArctic | None = None,
                 tradestie: Any = None) -> None:
        self.bsky = bsky if bsky is not None else {"$NVDA": [_bsky_nvda()]}
        self.fail = fail
        self.refuse_cursor = refuse_cursor
        self.arctic = arctic if arctic is not None else _arctic()
        if "arctic" in fail:
            self.arctic.fail = {"posts", "comments"}
        self.tradestie = _TRADESTIE if tradestie is None else tradestie
        self.calls: list[tuple[str, dict]] = []

    def get(self, url: str, params: dict | None = None, **kwargs: Any) -> Any:
        self.calls.append((url, dict(params or {})))
        if self.arctic.handles(url):
            return self.arctic.get(url, params, **kwargs)
        if "reddit.com" in url:
            raise AssertionError("reddit.com must not be contacted")
        if url == mod.TRADESTIE_URL:
            assert kwargs.get("allow_redirects") is False
            if "tradestie" in self.fail:
                return FakeResp({"error": "down"}, status=503)
            return FakeResp(self.tradestie)
        if url == mod.BLUESKY_SEARCH_URL:
            if "bluesky" in self.fail or (self.refuse_cursor and "cursor" in params):
                return _resp(status=403)
            pages = self.bsky.get(params["q"], [{"posts": []}])
            n = sum(1 for u, p in self.calls if u == url and p["q"] == params["q"])
            return _resp(pages[min(n, len(pages)) - 1])
        if "apewisdom.io" in url:
            if "apewisdom" in self.fail:
                return _resp(status=500)
            board, page = url.split("/filter/")[1].split("/page/")
            pages = _APE.get(board, [{"count": 0, "pages": 0, "results": []}])
            return _resp(pages[int(page) - 1])
        raise AssertionError(f"unexpected URL {url}")


def _puller(engine: Any, router: Router, scorer: Any = None) -> mod.SocialSentimentPuller:
    puller = mod.SocialSentimentPuller(db_engine=engine)
    puller._session = MagicMock()
    puller._session.get.side_effect = router.get
    puller._sentiment_scorer = scorer if scorer is not None else _fake_finbert
    return puller


def _fake_finbert(texts: list[str]) -> list[dict]:
    return [{"positive": 0.7, "negative": 0.1, "neutral": 0.2} if "calls" in t
            else {"positive": 0.1, "negative": 0.3, "neutral": 0.6} for t in texts]


def _rows(engine: Any) -> dict[str, tuple[float, str, dict]]:
    with engine.connect() as conn:
        found = conn.execute(text(
            "SELECT series_id, value, obs_date, raw_payload FROM raw_series")).fetchall()
    return {r[0]: (r[1], str(r[2]), json.loads(r[3])) for r in found}


class TestCashtag:
    @pytest.mark.parametrize("body,hit", [
        ("Buying $NVDA", True), ("$nvda.", True), ("($NVDA)", True),
        ("NVDA", False), ("$NVDAX", False), ("$$NVDA", False), ("x$NVDA", False),
    ])
    def test_only_a_whole_cashtag_counts(self, body: str, hit: bool) -> None:
        assert mod.has_cashtag(body, "NVDA") is hit


class TestBluesky:
    def test_request_shape_and_window(self, engine: Any) -> None:
        router = Router()
        posts, meta = _puller(engine, router)._bluesky_cashtag("NVDA", FETCH)
        url, params = router.calls[0]
        assert url == "https://api.bsky.app/xrpc/app.bsky.feed.searchPosts"
        assert params == {"q": "$NVDA", "limit": 100, "sort": "latest",
                          "since": "2026-11-16T01:00:00Z", "until": "2026-11-17T01:00:00Z"}
        # $NVDA x3 inside the window; plain NVDA, $NVDAX, the dupe, the post
        # indexed after the fetch and the one from two days ago are not counted.
        assert [p["uri"] for p in posts] == ["at://a/1", "at://a/2", "at://a/5"]
        assert posts[0]["engagement"] == 5 + 1 + 1
        # The page reached back past the window start: no second page.
        assert len(router.calls) == 1 and meta == {"pages": 1, "results_seen": 8, "capped": False,
                                                   "covered_hours": 24.0}

    def test_a_full_page_inside_the_window_is_capped_without_a_cursor_request(
            self, engine: Any) -> None:
        # Logged-out cursor pages answer 403, so the default is one page.
        page = {"posts": [_post(f"at://r/{i}", "$TSLA" if i % 2 else "tsla",
                                FETCH - timedelta(minutes=6 * (i + 1))) for i in range(100)],
                "cursor": "next"}
        router = Router({"$TSLA": [page]})
        posts, meta = _puller(engine, router)._bluesky_cashtag("TSLA", FETCH)
        assert len(router.calls) == 1 and "cursor" not in router.calls[0][1]
        assert meta["capped"] is True and meta["covered_hours"] == 10.0 and len(posts) == 50

    def test_pages_with_a_cursor_when_allowed(self, engine: Any,
                                              monkeypatch: pytest.MonkeyPatch) -> None:
        recent = {"posts": [_post("at://r/1", "$TSLA", FETCH - timedelta(minutes=6))],
                  "cursor": "next"}
        router = Router({"$TSLA": [recent] * 10})
        monkeypatch.setattr(mod, "BLUESKY_MAX_PAGES", 3)
        posts, meta = _puller(engine, router)._bluesky_cashtag("TSLA", FETCH)
        assert len(router.calls) == 3
        assert router.calls[1][1]["cursor"] == "next"
        assert meta["capped"] is True and meta["covered_hours"] == 0.1 and len(posts) == 1

    def test_a_refused_cursor_page_keeps_the_first(self, engine: Any,
                                                   monkeypatch: pytest.MonkeyPatch) -> None:
        recent = {"posts": [_post("at://r/1", "$TSLA", FETCH - timedelta(hours=2))],
                  "cursor": "next"}
        monkeypatch.setattr(mod, "BLUESKY_MAX_PAGES", 3)
        router = Router({"$TSLA": [recent] * 3}, refuse_cursor=True)
        posts, meta = _puller(engine, router)._bluesky_cashtag("TSLA", FETCH)
        assert len(router.calls) == 2 and len(posts) == 1
        assert meta["capped"] and meta["cursor_refused"] and meta["covered_hours"] == 2.0


class TestApeWisdom:
    def test_reads_every_page(self, engine: Any) -> None:
        router = Router()
        rows, complete = _puller(engine, router)._apewisdom_board("wallstreetbets")
        assert [c[0] for c in router.calls] == [
            "https://apewisdom.io/api/v1.0/filter/wallstreetbets/page/1",
            "https://apewisdom.io/api/v1.0/filter/wallstreetbets/page/2"]
        assert complete and [r["ticker"] for r in rows] == ["NVDA", "SPY", "SLS"]

    def test_page_cap_marks_incomplete(self, engine: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(mod, "APEWISDOM_MAX_PAGES", 1)
        rows, complete = _puller(engine, Router())._apewisdom_board("wallstreetbets")
        assert not complete and len(rows) == 2


class TestPullTickerSeries:
    def test_writes_series_dated_by_the_fetch(self, engine: Any) -> None:
        router = Router()
        out = _puller(engine, router).pull_ticker_series(tickers=["NVDA", "SPY", "BTC.X"],
                                                         now=FETCH)
        rows = _rows(engine)
        assert out["status"] == "SUCCESS" and out["rows_inserted"] == len(rows)
        # Posts were indexed on 2026-11-16; the observation is the fetch date.
        assert {r[1] for r in rows.values()} == {"2026-11-17"}
        assert rows["BSKY:NVDA:posts_24h"][0] == 3
        assert rows["BSKY:NVDA:engagement_24h"][0] == 7 + 3 + 1
        assert rows["BSKY:NVDA:finbert_compound"][0] == pytest.approx((0.6 - 0.2 - 0.2) / 3, abs=1e-6)
        assert rows["BSKY:NVDA:finbert_compound"][2]["n_texts"] == 3
        assert rows["BSKY:NVDA:posts_24h"][2]["window_start"] == "2026-11-16T01:00:00Z"
        assert rows["BSKY:NVDA:posts_per_hour"][0] == pytest.approx(3 / 24, abs=1e-6)
        # A ticker with no cashtag posts is a measured zero; too few texts: no sentiment.
        assert rows["BSKY:SPY:posts_24h"][0] == 0 and rows["BSKY:SPY:posts_per_hour"][0] == 0
        assert "BSKY:SPY:finbert_compound" not in rows
        assert rows["APEWISDOM:wallstreetbets:NVDA:mentions_24h"][0] == 1375
        assert rows["APEWISDOM:wallstreetbets:SPY:upvotes_24h"][0] == 900
        assert rows["APEWISDOM:wallstreetbets:total_mentions_24h"][0] == 1375 + 404 + 366
        assert rows["APEWISDOM:wallstreetbets:tickers_mentioned"][0] == 3
        assert rows["APEWISDOM:CryptoCurrency:BTC:mentions_24h"][0] == 57
        # Unranked on a board is not stored as zero.
        assert "APEWISDOM:stocks:NVDA:mentions_24h" not in rows
        assert "reddit api not configured" in out["reddit"]["skipped"]
        assert not any(k.startswith("REDDIT:") for k in rows)
        assert rows["ARCTICSHIFT:wallstreetbets:posts_24h"][0] == 4
        assert rows["ARCTICSHIFT:wallstreetbets:NVDA:posts_24h"][0] == 3

    def test_arctic_shift_board_counts_and_finbert(self, engine: Any) -> None:
        router = Router()
        out = _puller(engine, router).pull_ticker_series(tickers=["NVDA", "SPY", "META"],
                                                         now=FETCH)
        rows = _rows(engine)
        assert out["status"] == "SUCCESS"
        # The 30 h old post is outside the window; obs_date is the fetch date.
        assert rows["ARCTICSHIFT:wallstreetbets:posts_24h"][0] == 4
        assert rows["ARCTICSHIFT:wallstreetbets:posts_24h"][1] == "2026-11-17"
        assert rows["ARCTICSHIFT:wallstreetbets:posts_per_hour"][0] == pytest.approx(4 / 24, abs=1e-6)
        # Capitals or a cashtag count; "nvidia", lowercase "nvda" and "meta" do not.
        assert rows["ARCTICSHIFT:wallstreetbets:NVDA:posts_24h"][0] == 3
        assert rows["ARCTICSHIFT:wallstreetbets:META:posts_24h"][0] == 0
        assert rows["ARCTICSHIFT:stocks:SPY:posts_24h"][0] == 1
        assert rows["ARCTICSHIFT:investing:NVDA:posts_24h"][0] == 0  # measured zero
        assert rows["ARCTICSHIFT:wallstreetbets:comments_24h"][0] == 3
        nvda = rows["ARCTICSHIFT:NVDA:finbert_compound"]
        assert nvda[0] == pytest.approx((0.6 + 0.6 + 0.6) / 3)  # all three say "calls"
        assert nvda[2]["n_texts"] == 3 and nvda[2]["boards"] == mod.ARCTIC_BOARDS
        assert "ARCTICSHIFT:SPY:finbert_compound" not in rows  # 2 texts < FINBERT_MIN_TEXTS
        # One posts page and one comments page per board; every request bounded in time.
        assert router.arctic.count(POSTS_URL) == len(mod.ARCTIC_BOARDS)
        assert router.arctic.count(COMMENTS_URL) == len(mod.ARCTIC_BOARDS)
        assert all(kw["allow_redirects"] is False for _, _, kw in router.arctic.calls)

    def test_a_board_that_needs_more_pages_than_allowed_gets_a_rate(
            self, engine: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(mod, "ARCTIC_MAX_PAGES", 1)
        posts = [a_post(f"p{i}", "wallstreetbets", FT - 90 * (i + 1), f"take {i}")
                 for i in range(150)]
        comments = [a_comment(f"c{i}", "p1", FT - 36 * (i + 1)) for i in range(120)]
        router = Router(arctic=FakeArctic(posts=posts, comments=comments))
        _puller(engine, router).pull_ticker_series(tickers=["NVDA"], now=FETCH)
        rows = _rows(engine)
        assert "ARCTICSHIFT:wallstreetbets:posts_24h" not in rows
        assert "ARCTICSHIFT:wallstreetbets:NVDA:posts_24h" not in rows  # not a measured zero
        rate = rows["ARCTICSHIFT:wallstreetbets:posts_per_hour"]
        assert rate[0] == pytest.approx(100 / 2.5) and rate[2]["complete"] is False
        assert "ARCTICSHIFT:wallstreetbets:comments_24h" not in rows
        assert rows["ARCTICSHIFT:wallstreetbets:comments_per_hour"][0] == pytest.approx(100 / 1.0)

    def test_tradestie_previous_eastern_day(self, engine: Any) -> None:
        router = Router()
        _puller(engine, router).pull_ticker_series(tickers=["NVDA", "SPY", "AMD", "AAPL", "TSLA"],
                                                   now=FETCH)
        rows = _rows(engine)
        # 01:00Z on 11-17 is 20:00 on 11-16 in New York: the finished day is 11-15.
        calls = [p for u, p in router.calls if u == mod.TRADESTIE_URL]
        assert calls == [{"date": "2026-11-15"}]
        assert rows["TRADESTIE:wallstreetbets:NVDA:comments"][0] == 255
        assert rows["TRADESTIE:wallstreetbets:NVDA:sentiment_score"][0] == pytest.approx(0.042)
        assert rows["TRADESTIE:wallstreetbets:SPY:comments"][2]["sentiment"] == "Bearish"
        assert rows["TRADESTIE:wallstreetbets:SPY:comments"][2]["tradestie_date"] == "2026-11-15"
        # Out-of-range score kept out; malformed rows dropped; absent ticker not a zero.
        assert rows["TRADESTIE:wallstreetbets:AMD:comments"][0] == 9
        assert "TRADESTIE:wallstreetbets:AMD:sentiment_score" not in rows
        assert "TRADESTIE:wallstreetbets:AAPL:comments" not in rows
        assert "TRADESTIE:wallstreetbets:TSLA:comments" not in rows
        assert rows["TRADESTIE:wallstreetbets:top50_comments"][0] == 255 + 27 + 9
        assert rows["TRADESTIE:wallstreetbets:top50_comments"][1] == "2026-11-17"

    def test_tradestie_day_is_stored_once(self, engine: Any) -> None:
        # 06:00Z on 11-17 and 02:00Z on 11-18 are both on 11-17 in New York, so
        # both ask for 11-16: the second (next UTC day) stores no Tradestie row.
        first = datetime(2026, 11, 17, 6, 0, tzinfo=timezone.utc)
        _puller(engine, Router()).pull_ticker_series(tickers=["NVDA"], now=first)
        out = _puller(engine, Router()).pull_ticker_series(
            tickers=["NVDA"], now=first + timedelta(hours=20))
        assert out["tradestie"] == {"date": "2026-11-16", "tickers": 3}
        with engine.connect() as conn:
            n = conn.execute(text("SELECT COUNT(*) FROM raw_series WHERE series_id = "
                                  "'TRADESTIE:wallstreetbets:top50_comments'")).scalar()
        assert n == 1

    def test_tradestie_weekend_is_empty_not_failed(self, engine: Any) -> None:
        out = _puller(engine, Router(tradestie=[])).pull_ticker_series(tickers=["NVDA"], now=FETCH)
        assert out["status"] == "SUCCESS" and out["tradestie"]["tickers"] == 0
        assert not any(k.startswith("TRADESTIE:") for k in _rows(engine))

    def test_arctic_shift_down_is_partial_others_still_stored(self, engine: Any) -> None:
        out = _puller(engine, Router(fail={"arctic"})).pull_ticker_series(
            tickers=["NVDA"], now=FETCH)
        rows = _rows(engine)
        assert out["status"] == "PARTIAL" and "arctic shift r/wallstreetbets posts" in out["error"]
        assert not any(k.startswith("ARCTICSHIFT:") for k in rows)
        assert "BSKY:NVDA:posts_24h" in rows and "TRADESTIE:wallstreetbets:NVDA:comments" in rows

    def test_capped_ticker_gets_a_rate_not_a_24h_count(self, engine: Any) -> None:
        page = {"posts": [_post(f"at://c/{i}", f"$AAPL take {i}",
                                FETCH - timedelta(minutes=3 * (i + 1))) for i in range(100)],
                "cursor": "next"}
        _puller(engine, Router({"$AAPL": [page]})).pull_ticker_series(tickers=["AAPL"], now=FETCH)
        rows = _rows(engine)
        assert "BSKY:AAPL:posts_24h" not in rows and "BSKY:AAPL:engagement_24h" not in rows
        assert rows["BSKY:AAPL:posts_per_hour"][0] == pytest.approx(100 / 5.0)
        assert rows["BSKY:AAPL:posts_per_hour"][2]["capped"] is True
        assert rows["BSKY:AAPL:finbert_compound"][2]["n_texts"] == 100

    def test_second_run_same_day_fetches_nothing_stored(self, engine: Any) -> None:
        _puller(engine, Router()).pull_ticker_series(tickers=["NVDA"], now=FETCH)
        before = _rows(engine)
        router = Router()
        out = _puller(engine, router).pull_ticker_series(
            tickers=["NVDA"], now=FETCH + timedelta(hours=12))
        assert out["status"] == "SUCCESS" and out["rows_inserted"] == 0
        assert router.calls == [] and _rows(engine) == before

    def test_next_day_is_a_new_observation(self, engine: Any) -> None:
        _puller(engine, Router()).pull_ticker_series(tickers=["NVDA"], now=FETCH)
        out = _puller(engine, Router()).pull_ticker_series(
            tickers=["NVDA"], now=FETCH + timedelta(days=1))
        assert out["rows_inserted"] > 0
        with engine.connect() as conn:
            dates = conn.execute(text(
                "SELECT DISTINCT obs_date FROM raw_series ORDER BY obs_date")).fetchall()
        assert [str(d[0]) for d in dates] == ["2026-11-17", "2026-11-18"]

    def test_finbert_failure_keeps_counts_and_is_partial(self, engine: Any) -> None:
        def broken(_texts: list[str]) -> list[dict]:
            raise RuntimeError("model cache missing")
        out = _puller(engine, Router(), scorer=broken).pull_ticker_series(
            tickers=["NVDA"], now=FETCH)
        rows = _rows(engine)
        assert out["status"] == "PARTIAL" and "finbert: model cache missing" in out["error"]
        assert rows["BSKY:NVDA:posts_24h"][0] == 3
        assert "BSKY:NVDA:finbert_compound" not in rows

    def test_bluesky_down_is_partial_apewisdom_still_stored(self, engine: Any) -> None:
        out = _puller(engine, Router(fail={"bluesky"})).pull_ticker_series(
            tickers=["NVDA"], now=FETCH)
        rows = _rows(engine)
        assert out["status"] == "PARTIAL" and "bluesky $NVDA: 403" in out["error"]
        assert not any(k.startswith("BSKY:") for k in rows)
        assert "APEWISDOM:wallstreetbets:NVDA:mentions_24h" in rows

    def test_everything_down_is_failed_and_writes_nothing(self, engine: Any) -> None:
        out = _puller(engine, Router(fail={"bluesky", "apewisdom", "arctic", "tradestie"})).pull_ticker_series(
            tickers=["NVDA"], now=FETCH)
        assert out["status"] == "FAILED" and out["rows_inserted"] == 0
        assert _rows(engine) == {}

    def test_spent_deadline_fetches_nothing(self, engine: Any) -> None:
        import time
        router = Router()
        out = _puller(engine, router).pull_ticker_series(
            tickers=["NVDA"], now=FETCH, deadline_at=time.monotonic())
        assert router.calls == [] and out["status"] == "PARTIAL"
        assert "time budget spent" in out["error"]

    def test_no_engine_is_skipped(self) -> None:
        out = mod.SocialSentimentPuller(db_engine=None).pull_ticker_series(now=FETCH)
        assert out["status"] == "SKIPPED"


class FakeRedditClient:
    def __init__(self, about: dict, pages: list[dict]) -> None:
        self.about, self.pages, self.calls = about, pages, []

    def get_json(self, path: str, params: dict | None = None) -> dict:
        self.calls.append((path, dict(params or {})))
        if path.endswith("/about"):
            return {"data": self.about}
        n = sum(1 for p, _ in self.calls if p.endswith("/new"))
        return {"data": self.pages[min(n, len(self.pages)) - 1]}


def _listing(hours_ago: list[float], comments: int, after: str | None) -> dict:
    return {"children": [{"data": {"created_utc": (FETCH - timedelta(hours=h)).timestamp(),
                                   "num_comments": comments}} for h in hours_ago],
            "after": after}


class TestRedditApi:
    def test_board_stats_from_the_data_api(self) -> None:
        client = FakeRedditClient({"subscribers": 19_000_000, "active_user_count": None,
                                   "accounts_active": 41_000},
                                  [_listing([1, 5], 10, "t3_x"), _listing([20, 30], 4, "t3_y")])
        out = mod.SocialSentimentPuller._reddit_api_board(client, "wallstreetbets", FETCH)
        assert out == {"subscribers": 19_000_000, "active_users": 41_000,
                       "posts_24h": 3, "comments_on_posts_24h": 24}
        assert client.calls[2] == ("/r/wallstreetbets/new", {"limit": 100, "after": "t3_x"})

    def test_listing_that_never_leaves_the_window_has_no_volume(self) -> None:
        client = FakeRedditClient({"subscribers": 5}, [_listing([1], 1, "more")] * 5)
        out = mod.SocialSentimentPuller._reddit_api_board(client, "stocks", FETCH)
        assert out == {"subscribers": 5}
        assert len(client.calls) == 1 + mod.REDDIT_NEW_MAX_PAGES

    def test_configured_client_feeds_the_reddit_features(self, engine: Any) -> None:
        puller = _puller(engine, Router())
        puller._reddit_client = FakeRedditClient(
            {"subscribers": 100, "active_user_count": 7}, [_listing([2, 30], 3, None)])
        out = puller.pull_ticker_series(tickers=["NVDA"], now=FETCH)
        rows = _rows(engine)
        assert out["status"] == "SUCCESS"
        for sub in mod.REDDIT_API_SUBS:
            assert rows[f"REDDIT:{sub}:subscribers"][0] == 100
            assert rows[f"REDDIT:{sub}:active_users"][0] == 7
            assert rows[f"REDDIT:{sub}:posts_24h"][0] == 1


def test_reddit_series_map_to_the_registered_march_features() -> None:
    from normalization.entity_map import NEW_MAPPINGS_V2, SEED_MAPPINGS
    mappings = {**SEED_MAPPINGS, **NEW_MAPPINGS_V2}
    # feature_registry ids 237-246, registered 2026-03-21.
    for sub in ("wallstreetbets", "bitcoin", "cryptocurrency", "stocks", "economics"):
        assert mappings[f"REDDIT:{sub}:active_users"] == f"reddit_{sub}"
        assert mappings[f"REDDIT:{sub}:subscribers"] == f"reddit_{sub}_subs"
        assert sub in mod.REDDIT_API_SUBS


def test_smart_scheduler_registers_the_series_job() -> None:
    from ingestion.smart_scheduler import PULLER_REGISTRY, catalog_name_for
    entry = next(e for e in PULLER_REGISTRY if e["name"] == "social_ticker_series")
    assert (entry["mod"], entry["cls"], entry["method"]) == (
        "ingestion.social_sentiment", "SocialSentimentPuller", "pull_ticker_series")
    assert entry["freq_h"] == 12 and entry["timeout_s"] == 900 and not entry.get("hold_reason")
    assert catalog_name_for("social_ticker_series") == "SocialSentiment"
    # SmartScheduler passes its cooperative deadline to methods that accept it.
    assert "deadline_at" in inspect.signature(mod.SocialSentimentPuller.pull_ticker_series).parameters
    assert mod.SERIES_SCORE_RESERVE_S < entry["timeout_s"]


def test_watchlist_covers_both_social_pullers() -> None:
    from ingestion.altdata.stocktwits import _DEFAULT_TICKERS
    watch = mod.series_watchlist()
    assert len(watch) == len(set(watch))
    assert {t.removesuffix(".X") for t in _DEFAULT_TICKERS} <= set(watch)
    assert set(mod.DEFAULT_TARGET_TICKERS) <= set(watch)


def test_no_llm_on_this_path() -> None:
    src = inspect.getsource(mod)
    assert "llm.router" not in src and "llm/" not in src
