"""smart_money Reddit path on the combined hot-post RSS feed (no OAuth)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import ingestion.altdata.smart_money as sm
from ingestion.altdata.reddit_rss import RssEntry


def _puller() -> sm.SmartMoneyPuller:
    p = sm.SmartMoneyPuller.__new__(sm.SmartMoneyPuller)
    p._trust_cache = {}
    p.engine = MagicMock()
    return p


def _entry(eid: str, sub: str, title: str, author: str = "trader", body: str = "") -> RssEntry:
    return RssEntry(id=eid, title=title, author=author, subreddit=sub,
                    published=datetime(2026, 11, 5, 14, tzinfo=timezone.utc), body=body, link="")


def test_one_combined_feed_mapped_to_post_fields(monkeypatch: Any) -> None:
    calls: list[tuple] = []

    def fetch(path: str, params: dict) -> list[RssEntry]:
        calls.append((path, params))
        return [_entry("t3_a", "WallStreetBets", "NVDA calls", body="buying calls"),
                _entry("t1_x", "stocks", "a comment")]

    monkeypatch.setattr(sm.reddit_rss, "fetch_feed", fetch)
    posts = _puller()._fetch_hot_posts(["wallstreetbets", "stocks"])
    assert calls == [("/r/wallstreetbets+stocks/hot.rss", {"limit": sm._MAX_HOT_POSTS})]
    assert posts == [{
        "title": "NVDA calls", "selftext": "buying calls", "author": "trader",
        "subreddit": "wallstreetbets",
        "created_utc": datetime(2026, 11, 5, 14, tzinfo=timezone.utc).timestamp(),
        "score": None, "num_comments": None, "upvote_ratio": None,
    }]


def test_unknown_score_skips_only_the_score_gate() -> None:
    p = _puller()
    post = {"title": "NVDA calls breakout", "selftext": "going long", "author": "trader",
            "score": None, "created_utc": 1_762_351_200}
    signals = p._parse_reddit_post(post, "wallstreetbets")
    assert signals and signals[0]["ticker"] == "NVDA" and signals[0]["post_score"] is None
    # A known low score is still rejected.
    assert p._parse_reddit_post({**post, "score": 3}, "wallstreetbets") == []
    # Low-trust users are still rejected without a score.
    p._trust_cache["trader"] = 0.1
    assert p._parse_reddit_post(post, "wallstreetbets") == []


def test_signals_are_stored(monkeypatch: Any) -> None:
    p = _puller()
    p._load_trust_scores = lambda: None
    p._fetch_hot_posts = lambda subs: [{"title": "AMD puts, crash coming", "selftext": "",
                                        "author": "bear1", "subreddit": "stocks",
                                        "score": None, "created_utc": None}]
    stored: list[dict] = []
    p._store_signal = lambda conn, signal, d: stored.append(signal) or True
    out = p.pull_reddit()
    assert out == {"source": "reddit", "status": "SUCCESS", "signals_found": 1, "rows_inserted": 1}
    assert stored[0]["ticker"] == "AMD" and stored[0]["direction"] == "BEARISH"
