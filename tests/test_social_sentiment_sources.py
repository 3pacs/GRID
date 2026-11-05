"""Source parsing for ingestion/social_sentiment.py (Reddit RSS, ApeWisdom, Bluesky).

All HTTP is stubbed. Fixtures mirror the shapes grid-svr received on 2026-11-05.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import requests

import ingestion.social_sentiment as mod

_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <author><name>/u/someone</name></author>
    <category term="wallstreetbets" label="r/wallstreetbets"/>
    <content type="html">&lt;!-- SC_OFF --&gt;&lt;div class="md"&gt;&lt;p&gt;Loading NVDA calls &amp;amp; riding the breakout&lt;/p&gt;&lt;/div&gt;&lt;!-- SC_ON --&gt; &amp;#32; submitted by &amp;#32; &lt;a href="https://www.reddit.com/user/someone"&gt; /u/someone &lt;/a&gt; &lt;span&gt;&lt;a href="x"&gt;[comments]&lt;/a&gt;&lt;/span&gt;</content>
    <id>t3_aaa111</id>
    <published>2026-11-05T14:00:00+00:00</published>
    <title>NVDA earnings play</title>
  </entry>
  <entry>
    <category term="Bitcoin" label="r/Bitcoin"/>
    <content type="html">&lt;table&gt;&lt;tr&gt;&lt;td&gt;submitted by /u/x [link] [comments]&lt;/td&gt;&lt;/tr&gt;&lt;/table&gt;</content>
    <id>t3_bbb222</id>
    <published>2026-11-05T15:00:00+00:00</published>
    <title>BTC dump incoming</title>
  </entry>
</feed>"""

_APE = {"count": 2, "pages": 1, "current_page": 1, "results": [
    {"rank": 1, "ticker": "MU", "name": "Micron Technology", "mentions": 616,
     "upvotes": 2255, "rank_24h_ago": 5, "mentions_24h_ago": 32},
    {"rank": 2, "ticker": "SPY", "name": "SPDR S&amp;P 500 ETF Trust", "mentions": "492",
     "upvotes": 1536, "rank_24h_ago": None, "mentions_24h_ago": None},
]}


def _resp(*, content: bytes = b"", payload: Any = None, status: int = 200) -> MagicMock:
    resp = MagicMock()
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


class TestRedditRss:
    def test_one_combined_request_for_every_board(self) -> None:
        puller = _puller([_resp(content=_RSS.encode())])
        posts = puller._reddit_hot_all(["wallstreetbets", "bitcoin", "stocks"])
        assert puller._session.get.call_count == 1
        url = puller._session.get.call_args.args[0]
        assert url == "https://www.reddit.com/r/wallstreetbets+bitcoin+stocks/hot.rss"
        assert puller._session.get.call_args.kwargs["params"] == {"limit": mod.REDDIT_RSS_LIMIT}
        # Feed case ("Bitcoin") maps back to the configured board name.
        assert sorted(posts) == ["bitcoin", "wallstreetbets"]

    def test_entry_fields_and_self_post_body(self) -> None:
        posts = _puller([_resp(content=_RSS.encode())])._reddit_hot_all(["wallstreetbets", "bitcoin"])
        wsb = posts["wallstreetbets"][0]
        assert wsb["title"] == "NVDA earnings play"
        assert wsb["text"] == "Loading NVDA calls & riding the breakout"
        assert wsb["created"] == "2026-11-05T14:00:00+00:00"
        assert wsb["id"] == "t3_aaa111" and wsb["subreddit"] == "wallstreetbets"
        # Link posts carry only the footer, which is not post text.
        assert posts["bitcoin"][0]["text"] == ""

    def test_rate_limited_raises(self) -> None:
        puller = _puller([_resp(status=429)])
        with pytest.raises(requests.exceptions.HTTPError):
            puller._reddit_hot_all(["stocks"])


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
    def _run(self, rss: Any, ape: Any, bsky: Any) -> dict:
        def get(url: str, **_kw: Any) -> MagicMock:
            if "reddit.com" in url:
                return rss
            if "apewisdom" in url:
                return ape
            return bsky

        puller = _puller(get)
        puller._google_trends = lambda kw: {}
        with patch.object(mod.time, "sleep"):
            return puller.pull_all()

    def test_all_sources_feed_the_scan(self) -> None:
        out = self._run(_resp(content=_RSS.encode()), _resp(payload=_APE),
                        _resp(payload={"posts": []}))
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
        out = self._run(_resp(content=_RSS.encode()), _resp(payload=ape),
                        _resp(payload={"posts": []}))
        nvda = out["ticker_sentiment"]["NVDA"]
        assert (nvda["reddit_mentions_24h"], nvda["reddit_mentions_prior_24h"],
                nvda["reddit_rank"]) == (386, 27, 4)

    def test_only_apewisdom_answering_still_counts_as_data(self) -> None:
        assert mod._has_data({"reddit_mentions": {"stocks": [{"ticker": "MU"}]}})
        assert not mod._has_data({"reddit": {}, "reddit_mentions": {}, "bluesky": []})
