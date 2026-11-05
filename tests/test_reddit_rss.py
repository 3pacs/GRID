"""ingestion/altdata/reddit_rss.py: feed parsing and cross-process request spacing.

HTTP is stubbed; the lock file lives in tmp_path. Feed shapes mirror what
grid-svr received from Reddit on 2026-11-05.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest
import requests

from ingestion.altdata import reddit_rss as rr

_FEED = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <author><name>/u/poster</name></author>
    <category term="wallstreetbets" label="r/wallstreetbets"/>
    <content type="html">&lt;!-- SC_OFF --&gt;&lt;div class="md"&gt;&lt;p&gt;Market open &amp;amp; NVDA&lt;/p&gt;&lt;/div&gt;&lt;!-- SC_ON --&gt; submitted by /u/poster [comments]</content>
    <id>t3_daily</id>
    <link href="https://www.reddit.com/r/wallstreetbets/comments/daily/thread/"/>
    <published>2026-11-05T10:00:00+00:00</published>
    <title>Daily Discussion Thread for November 05, 2026</title>
  </entry>
  <entry>
    <author><name>/u/alice</name></author>
    <content type="html">&lt;div class="md"&gt;&lt;p&gt;$SPY puts&lt;/p&gt;&lt;/div&gt;</content>
    <id>t1_c1</id>
    <updated>2026-11-05T11:00:00Z</updated>
    <title>/u/alice on Daily Discussion Thread</title>
  </entry>
  <entry>
    <content type="html">&lt;table&gt;submitted by [link]&lt;/table&gt;</content>
    <id>t1_c2</id>
    <title>deleted</title>
  </entry>
</feed>"""


@pytest.fixture(autouse=True)
def _lock(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> Any:
    path = tmp_path / "reddit.lock"
    monkeypatch.setenv(rr.LOCK_PATH_ENV, str(path))
    return path


class FakeClock:
    """time.time / time.sleep stand-in; sleeping advances the clock."""

    def __init__(self, start: float = 1_000_000.0) -> None:
        self.now = start
        self.slept: list[float] = []

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    fake = FakeClock()
    monkeypatch.setattr(rr.time, "time", fake.time)
    monkeypatch.setattr(rr.time, "sleep", fake.sleep)
    return fake


def _resp(status: int = 200, content: str = _FEED) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.content = content.encode()
    if status >= 400:
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(f"{status} Client Error")
    return resp


def _session(*responses: MagicMock) -> MagicMock:
    session = MagicMock()
    session.get.side_effect = list(responses)
    return session


class TestParseFeed:
    def test_post_and_comment_entries(self) -> None:
        post, comment, deleted = rr.parse_feed(_FEED)
        assert post.id == "t3_daily" and post.kind == "t3" and post.short_id == "daily"
        assert post.author == "poster" and post.subreddit == "wallstreetbets"
        assert post.body == "Market open & NVDA"
        assert post.published == datetime(2026, 11, 5, 10, tzinfo=timezone.utc)
        assert post.link == "https://www.reddit.com/r/wallstreetbets/comments/daily/thread/"
        # Comment feeds: no category, only <updated>.
        assert comment.kind == "t1" and comment.author == "alice" and comment.subreddit == ""
        assert comment.body == "$SPY puts"
        assert comment.published == datetime(2026, 11, 5, 11, tzinfo=timezone.utc)
        # Deleted author, footer-only content, no timestamps.
        assert (deleted.author, deleted.body, deleted.published, deleted.link) == ("", "", None, "")

    def test_empty_body_is_rejected_xml(self) -> None:
        with pytest.raises(Exception):
            rr.parse_feed(b"")


class TestFetchFeed:
    def test_request_shape(self, clock: FakeClock) -> None:
        session = _session(_resp())
        entries = rr.fetch_feed("/r/a+b/hot.rss", {"limit": 100}, session=session)
        assert [e.id for e in entries] == ["t3_daily", "t1_c1", "t1_c2"]
        args, kwargs = session.get.call_args
        assert args == ("https://www.reddit.com/r/a+b/hot.rss",)
        assert kwargs["params"] == {"limit": 100}
        assert "grid-intelligence" in kwargs["headers"]["User-Agent"]
        assert clock.slept == []  # no earlier request recorded

    def test_back_to_back_requests_are_spaced(self, clock: FakeClock, _lock: Any) -> None:
        session = _session(_resp(), _resp())
        rr.fetch_feed("/r/a/hot.rss", session=session)
        clock.now += 10
        rr.fetch_feed("/r/b/hot.rss", session=session)
        assert clock.slept == [pytest.approx(rr.MIN_INTERVAL_S - 10)]
        assert float(_lock.read_text()) == pytest.approx(clock.now)

    def test_slot_past_budget_sends_nothing(self, clock: FakeClock) -> None:
        first = _session(_resp())
        rr.fetch_feed("/r/a/hot.rss", session=first)
        second = _session(_resp())
        with pytest.raises(rr.RedditRateLimited):
            rr.fetch_feed("/r/b/hot.rss", session=second, max_wait_s=30)
        second.get.assert_not_called()

    def test_429_retries_once_at_next_slot(self, clock: FakeClock) -> None:
        session = _session(_resp(429, ""), _resp())
        entries = rr.fetch_feed("/r/a/hot.rss", session=session)
        assert session.get.call_count == 2 and len(entries) == 3
        assert clock.slept == [pytest.approx(rr.MIN_INTERVAL_S)]

    def test_second_429_raises_http_error(self, clock: FakeClock) -> None:
        session = _session(_resp(429, ""), _resp(429, ""))
        with pytest.raises(requests.exceptions.HTTPError):
            rr.fetch_feed("/r/a/hot.rss", session=session)

    def test_429_retry_that_does_not_fit_budget_is_rate_limited(self, clock: FakeClock) -> None:
        session = _session(_resp(429, ""), _resp())
        with pytest.raises(rr.RedditRateLimited):
            rr.fetch_feed("/r/a/hot.rss", session=session, max_wait_s=20)
        assert session.get.call_count == 1

    def test_failed_request_still_records_its_slot(self, clock: FakeClock, _lock: Any) -> None:
        session = MagicMock()
        session.get.side_effect = requests.exceptions.ConnectionError("reset")
        with pytest.raises(requests.exceptions.ConnectionError):
            rr.fetch_feed("/r/a/hot.rss", session=session)
        assert float(_lock.read_text()) == pytest.approx(clock.now)

    def test_unusable_lock_path_still_fetches(self, clock: FakeClock, tmp_path: Any,
                                              monkeypatch: pytest.MonkeyPatch) -> None:
        target = tmp_path / "elsewhere"
        target.write_text("0")
        link = tmp_path / "link.lock"
        link.symlink_to(target)  # O_NOFOLLOW refuses symlinks
        monkeypatch.setenv(rr.LOCK_PATH_ENV, str(link))
        entries = rr.fetch_feed("/r/a/hot.rss", session=_session(_resp()))
        assert len(entries) == 3 and target.read_text() == "0"
