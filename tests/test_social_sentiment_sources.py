"""Source parsing for ingestion/social_sentiment.py (Reddit via Arctic Shift, ApeWisdom, Bluesky).

All HTTP is stubbed. ApeWisdom and Bluesky fixtures mirror what grid-svr
received on 2026-11-05; Arctic Shift records follow its 2026-11-18 answers.
"""

from __future__ import annotations

import socket
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import requests

import ingestion.social_sentiment as mod
from ingestion.altdata import arctic_shift
from tests.arctic_shift_fakes import POSTS_URL, FakeArctic, post


@pytest.fixture(autouse=True)
def _isolated_arctic_slot(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    def _refuse(*_a: Any, **_k: Any) -> None:
        raise AssertionError("network access in a unit test")
    monkeypatch.setattr(socket.socket, "connect", _refuse)
    monkeypatch.setenv(arctic_shift.LOCK_PATH_ENV, str(tmp_path / "arctic.lock"))
    monkeypatch.setattr(arctic_shift, "MIN_INTERVAL_S", 0.0)


NOW = datetime(2026, 11, 5, 16, 0, tzinfo=timezone.utc)
_T = NOW.timestamp()


def _arctic() -> FakeArctic:
    return FakeArctic(posts=[
        post("aaa111", "wallstreetbets", _T - 7200, "NVDA earnings play",
             "Loading NVDA calls &amp; riding the breakout", author="someone"),
        post("bbb222", "Bitcoin", _T - 3600, "BTC dump incoming", "", author="x"),
    ])


_APE = {"count": 2, "pages": 1, "current_page": 1, "results": [
    {"rank": 1, "ticker": "MU", "name": "Micron Technology", "mentions": 616,
     "upvotes": 2255, "rank_24h_ago": 5, "mentions_24h_ago": 32},
    {"rank": 2, "ticker": "SPY", "name": "SPDR S&amp;P 500 ETF Trust", "mentions": "492",
     "upvotes": 1536, "rank_24h_ago": None, "mentions_24h_ago": None},
]}


def _resp(*, content: bytes = b"", payload: Any = None, status: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.content = content
    resp.json.return_value = payload
    if status >= 400:
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(f"{status} Client Error")
    return resp


def _puller(get_side_effect: Any) -> mod.SocialSentimentPuller:
    puller = mod.SocialSentimentPuller(db_engine=None)
    puller._session = MagicMock()
    puller._session.get.side_effect = get_side_effect
    return puller


class TestRedditArcticShift:
    def test_one_request_per_board_on_arctic_shift(self) -> None:
        fake = _arctic()
        puller = _puller(fake.get)
        posts, failures = puller._reddit_hot_all(["wallstreetbets", "bitcoin", "stocks"], now=NOW)
        assert failures == [] and fake.count(POSTS_URL) == 3
        params = [p for _, p, _ in fake.calls]
        assert [p["subreddit"] for p in params] == ["wallstreetbets", "bitcoin", "stocks"]
        assert {p["limit"] for p in params} == {mod.REDDIT_POSTS_PER_BOARD}
        assert all(not url.startswith("https://www.reddit.com") for url, _, _ in fake.calls)
        # Archive case ("Bitcoin") maps back to the configured board name.
        assert sorted(posts) == ["bitcoin", "wallstreetbets"]

    def test_post_fields(self) -> None:
        posts, _ = _puller(_arctic().get)._reddit_hot_all(["wallstreetbets", "bitcoin"], now=NOW)
        wsb = posts["wallstreetbets"][0]
        assert wsb["title"] == "NVDA earnings play"
        assert wsb["text"] == "Loading NVDA calls &amp; riding the breakout"
        assert wsb["created"] == "2026-11-05T14:00:00+00:00"
        assert wsb["id"] == "t3_aaa111" and wsb["subreddit"] == "wallstreetbets"
        assert posts["bitcoin"][0]["text"] == ""

    def test_one_board_down_is_reported_every_board_down_raises(self) -> None:
        fake = _arctic()
        fake.fail = {"posts:stocks"}
        posts, failures = _puller(fake.get)._reddit_hot_all(["wallstreetbets", "stocks"], now=NOW)
        assert list(posts) == ["wallstreetbets"] and len(failures) == 1
        assert "r/stocks" in failures[0]
        fake.fail = {"posts"}
        with pytest.raises(RuntimeError, match="HTTP 500"):
            _puller(fake.get)._reddit_hot_all(["wallstreetbets", "stocks"], now=NOW)


class TestApeWisdom:
    def test_rows_are_normalized(self) -> None:
        puller = _puller([_resp(payload=_APE)])
        rows = puller._reddit_mentions("all-stocks")
        assert puller._session.get.call_args.args[0] == (
            "https://apewisdom.io/api/v1.0/filter/all-stocks/page/1")
        assert rows[0] == {"ticker": "MU", "name": "Micron Technology", "rank": 1,
                           "mentions": 616, "upvotes": 2255, "rank_24h_ago": 5,
                           "mentions_24h_ago": 32}
        assert rows[1]["name"] == "SPDR S&P 500 ETF Trust"
        assert rows[1]["mentions"] == 492 and rows[1]["mentions_24h_ago"] is None

    def test_crypto_suffix_is_stripped(self) -> None:
        payload = {"results": [{"rank": 1, "ticker": "BTC.X", "name": "Bitcoin",
                                "mentions": 144, "upvotes": 722}]}
        rows = _puller([_resp(payload=payload)])._reddit_mentions("all-crypto")
        assert rows[0]["ticker"] == "BTC"


class TestBluesky:
    def test_searches_appview_not_public_cache(self) -> None:
        payload = {"posts": [{"record": {"text": "SPY rally", "createdAt": "2026-11-05T19:00:00Z"},
                              "likeCount": 3, "repostCount": 1, "replyCount": 0}]}
        puller = _puller([_resp(payload=payload)])
        posts = puller._bluesky_search("SPY")
        assert puller._session.get.call_args.args[0] == (
            "https://api.bsky.app/xrpc/app.bsky.feed.searchPosts")
        assert posts == [{"text": "SPY rally", "likes": 3, "reposts": 1, "replies": 0,
                          "created": "2026-11-05T19:00:00Z", "source": "bluesky"}]


class TestPullAll:
    def _run(self, arctic: FakeArctic, ape: Any, bsky: Any) -> dict:
        def get(url: str, *args: Any, **kw: Any) -> Any:
            if arctic.handles(url):
                return arctic.get(url, *args, **kw)
            if "reddit.com" in url:
                raise AssertionError("reddit.com must not be contacted")
            if "apewisdom" in url:
                return ape
            return bsky

        puller = _puller(get)
        puller._google_trends = lambda kw: {}
        # The fixtures are dated 2026-11-05; the scan reads the 24 h before "now".
        with patch.object(mod.time, "sleep"), patch.object(
                mod, "datetime", wraps=datetime) as dt:
            dt.now.return_value = NOW
            return puller.pull_all()

    def test_all_sources_feed_the_scan(self) -> None:
        out = self._run(_arctic(), _resp(payload=_APE), _resp(payload={"posts": []}))
        assert set(out["reddit"]) == {"wallstreetbets", "bitcoin"}
        assert [r["ticker"] for r in out["reddit_mentions"]["stocks"]] == ["MU", "SPY"]
        nvda = out["ticker_sentiment"]["NVDA"]
        assert nvda["mentions"] == 2  # title + self-post body
        assert "SPY" not in out["ticker_sentiment"]  # no post text names SPY
        assert "Most-discussed on Reddit (24h): MU 616 (prev 32), SPY 492." in out["summary"]
        assert out["status"] == "SKIPPED"  # no engine; nothing written

    def test_reddit_wide_counts_attach_to_scored_tickers(self) -> None:
        ape = {"results": [{"rank": 4, "ticker": "NVDA", "name": "NVIDIA", "mentions": 386,
                            "upvotes": 1749, "rank_24h_ago": 8, "mentions_24h_ago": 27}]}
        out = self._run(_arctic(), _resp(payload=ape), _resp(payload={"posts": []}))
        nvda = out["ticker_sentiment"]["NVDA"]
        assert (nvda["reddit_mentions_24h"], nvda["reddit_mentions_prior_24h"],
                nvda["reddit_rank"]) == (386, 27, 4)

    def test_only_apewisdom_answering_still_counts_as_data(self) -> None:
        assert mod._has_data({"reddit_mentions": {"stocks": [{"ticker": "MU"}]}})
        assert not mod._has_data({"reddit": {}, "reddit_mentions": {}, "bluesky": []})
