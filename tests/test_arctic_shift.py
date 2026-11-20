"""ingestion/altdata/arctic_shift.py: request shape, untrusted-answer handling, paging.

No network: sockets are refused and every request goes to a stub session.
"""

from __future__ import annotations

import os
import socket
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from ingestion.altdata import arctic_shift as a
from tests.arctic_shift_fakes import (
    COMMENTS_URL,
    POSTS_URL,
    FakeArctic,
    FakeResp,
    comment,
    post,
)

NOW = datetime(2026, 11, 18, 2, 0, tzinfo=timezone.utc)
T = NOW.timestamp()


@pytest.fixture(autouse=True)
def _offline(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> list[float]:
    def _refuse(*_a: Any, **_k: Any) -> None:
        raise AssertionError("network access in a unit test")
    monkeypatch.setattr(socket.socket, "connect", _refuse)
    monkeypatch.setenv(a.LOCK_PATH_ENV, str(tmp_path / "arctic.lock"))
    monkeypatch.setattr(a, "MIN_INTERVAL_S", 0.0)
    sleeps: list[float] = []
    monkeypatch.setattr(a.time, "sleep", sleeps.append)
    return sleeps


class Seq:
    """Session answering a fixed sequence of responses."""

    def __init__(self, *responses: FakeResp) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, dict, dict]] = []

    def get(self, url: str, params: dict | None = None, **kw: Any) -> FakeResp:
        self.calls.append((url, dict(params or {}), kw))
        return self.responses.pop(0)


class TestSearch:
    def test_fixed_host_no_redirects_streamed_with_user_agent(self) -> None:
        s = Seq(FakeResp({"data": [{"id": "a"}, "junk", 3, None]}))
        out = a.search("posts", {"subreddit": "wallstreetbets", "limit": 5}, session=s)
        assert out == [{"id": "a"}]  # non-dict records dropped
        url, params, kw = s.calls[0]
        assert url == "https://arctic-shift.photon-reddit.com/api/posts/search"
        assert params == {"subreddit": "wallstreetbets", "limit": 5}
        assert kw["allow_redirects"] is False and kw["stream"] is True
        assert kw["headers"]["User-Agent"] == a.USER_AGENT
        assert "@" not in a.USER_AGENT
        assert kw["timeout"] == a.REQUEST_TIMEOUT_S

    @pytest.mark.parametrize("kind,params", [
        ("users", {}),
        ("posts", {"url": "https://elsewhere.example/"}),
        ("posts", {"subreddit": "wsb/../../x"}),
        ("comments", {"link_id": "t3_../x"}),
    ])
    def test_bad_requests_are_refused_before_sending(self, kind: str, params: dict) -> None:
        s = Seq()
        with pytest.raises(ValueError):
            a.search(kind, params, session=s)
        assert s.calls == []

    def test_redirect_is_refused(self) -> None:
        s = Seq(FakeResp(status=302, headers={"Location": "https://elsewhere.example/"}, raw=b""))
        with pytest.raises(a.ArcticShiftError, match="redirect refused"):
            a.search("posts", {}, session=s)
        assert len(s.calls) == 1

    def test_429_waits_for_the_advertised_reset_once(self, _offline: list[float]) -> None:
        s = Seq(FakeResp({"error": "rate"}, status=429, headers={"X-RateLimit-Reset": "7"}),
                FakeResp({"data": []}))
        assert a.search("posts", {}, session=s) == []
        assert 7.5 in _offline and len(s.calls) == 2

    def test_second_429_raises(self) -> None:
        s = Seq(FakeResp({}, status=429, headers={"X-RateLimit-Reset": "1"}),
                FakeResp({}, status=429, headers={"X-RateLimit-Reset": "1"}))
        with pytest.raises(a.ArcticShiftError, match="HTTP 429"):
            a.search("posts", {}, session=s)

    def test_429_reset_past_the_budget_is_busy_without_waiting(self, _offline: list[float]) -> None:
        s = Seq(FakeResp({}, status=429, headers={"X-RateLimit-Reset": "50"}))
        with pytest.raises(a.ArcticShiftBusy):
            a.search("posts", {}, session=s, max_wait_s=10)
        assert _offline == [] and len(s.calls) == 1

    def test_query_timeout_422_is_retried_once(self) -> None:
        slow = {"data": None, "error": "Timeout. Maybe slow down a bit"}
        s = Seq(FakeResp(slow, status=422), FakeResp({"data": [{"id": "x"}]}))
        assert a.search("posts", {}, session=s) == [{"id": "x"}]
        s = Seq(FakeResp(slow, status=422), FakeResp(slow, status=422))
        with pytest.raises(a.ArcticShiftError, match="HTTP 422"):
            a.search("posts", {}, session=s)

    def test_oversized_bodies_are_refused(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(a, "MAX_RESPONSE_BYTES", 100)
        with pytest.raises(a.ArcticShiftError, match="over 100 bytes"):
            a.search("posts", {}, session=Seq(FakeResp({"data": ["x" * 500]})))
        with pytest.raises(a.ArcticShiftError, match="declares"):
            a.search("posts", {}, session=Seq(FakeResp({"data": []},
                                                       headers={"Content-Length": "999"})))

    @pytest.mark.parametrize("body", [b"<html>blocked</html>", b'{"data": null, "error": "x"}',
                                      b'[1, 2]', b'{"data": {"id": 1}}'])
    def test_answers_without_a_data_list_are_errors(self, body: bytes) -> None:
        with pytest.raises(a.ArcticShiftError):
            a.search("posts", {}, session=Seq(FakeResp(raw=body)))

    def test_planted_shared_lock_file_is_not_used(self, tmp_path: Any,
                                                  monkeypatch: pytest.MonkeyPatch) -> None:
        path = tmp_path / "shared.lock"
        path.write_text("9999999999\n")
        os.chmod(path, 0o666)
        monkeypatch.setenv(a.LOCK_PATH_ENV, str(path))
        assert a._open_lock() is None
        assert a.search("posts", {}, session=Seq(FakeResp({"data": []}))) == []


class TestRecords:
    def test_post_fields_are_typed_and_bounded(self) -> None:
        raw = post("abc123", "wallstreetbets", T - 3600, title="  NVDA \n calls ",
                   selftext="x" * 9000, score=1, num_comments=0)
        p = a.post(raw, now=NOW)
        assert p["title"] == "NVDA calls" and len(p["selftext"]) == a.MAX_TEXT_CHARS
        # Inside the 36 h settling window the archive's score is not a vote count yet.
        assert p["score"] is None and p["num_comments"] is None
        old = a.post(post("old1", "stocks", T - 40 * 3600, score=1635, num_comments=389), now=NOW)
        assert (old["score"], old["num_comments"]) == (1635, 389)

    @pytest.mark.parametrize("raw", [
        {"id": "ABC", "created_utc": 1_794_960_000},          # id not base36 lower
        {"id": "abc", "created_utc": "1794960000"},           # string time
        {"id": "abc", "created_utc": True},
        {"id": "abc", "created_utc": float("nan")},
        {"id": "abc", "created_utc": 99},                     # before Reddit existed
        {"id": "a" * 40, "created_utc": 1_794_960_000},
    ])
    def test_records_without_valid_id_or_time_are_dropped(self, raw: dict) -> None:
        assert a.post(raw, now=NOW) is None
        assert a.comment(raw) is None

    def test_deleted_text_and_odd_types_become_empty(self) -> None:
        p = a.post({"id": "d1", "created_utc": int(T - 40 * 3600), "title": "[deleted]",
                    "selftext": {"html": "<b>"}, "author": "bad name!", "score": True,
                    "subreddit": "../etc"}, now=NOW)
        assert (p["title"], p["selftext"], p["author"], p["score"], p["subreddit"]) == \
            ("", "", "", None, "")
        c = a.comment({"id": "c1", "created_utc": int(T), "body": "[removed]",
                       "link_id": "t3_../../x"})
        assert c["body"] == "" and c["link_id"] == ""


class TestPaging:
    def test_pages_back_until_the_window_start(self) -> None:
        posts = [post(f"p{i}", "wallstreetbets", T - 300 * (i + 1)) for i in range(250)]
        posts += [post("late", "wallstreetbets", T + 60),          # after the fetch
                  post("other", "stocks", T - 30)]                  # another board
        fake = FakeArctic(posts=posts)
        got, complete = a.recent_posts("wallstreetbets", since=NOW - timedelta(hours=24),
                                       until=NOW, max_pages=8, session=fake)
        # 24 h / 5 min = 288 slots, but only 250 posts exist: three pages, all of them.
        assert complete is True and len(got) == 250 and fake.count(POSTS_URL) == 3
        assert got[0]["id"] == "p0" and {p["subreddit"] for p in got} == {"wallstreetbets"}
        first = fake.calls[0][1]
        assert first["after"] == int(T) - 86400 and first["before"] == int(T) + 1
        assert fake.calls[1][1]["before"] == int(T) - 300 * 100

    def test_page_cap_marks_the_window_incomplete(self) -> None:
        fake = FakeArctic(posts=[post(f"p{i}", "wallstreetbets", T - 60 * (i + 1))
                                 for i in range(500)])
        got, complete = a.recent_posts("wallstreetbets", since=NOW - timedelta(hours=24),
                                       until=NOW, max_pages=2, session=fake)
        assert complete is False and len(got) == 200

    def test_records_from_another_board_are_never_counted(self) -> None:
        class Liar(FakeArctic):
            def get(self, url: str, params: dict | None = None, **kw: Any) -> FakeResp:
                self.calls.append((url, dict(params or {}), kw))
                return FakeResp({"data": [post("x1", "pennystocks", T - 60),
                                          post("x2", "wallstreetbets", T - 90)]})
        got, _ = a.recent_posts("wallstreetbets", since=NOW - timedelta(hours=1), until=NOW,
                                max_pages=1, session=Liar())
        assert [p["id"] for p in got] == ["x2"]

    def test_thread_comments_newest_first_capped(self) -> None:
        comments = [comment(f"c{i}", "thr1", T - 10 * i, body=f"b{i}", author=f"u{i}")
                    for i in range(260)]
        comments.append(comment("zz", "other", T, body="elsewhere"))
        fake = FakeArctic(comments=comments)
        got = a.thread_comments("thr1", max_comments=250, session=fake)
        assert len(got) == 250 and got[0]["id"] == "c0" and fake.count(COMMENTS_URL) == 3
        assert all(c["link_id"] == "thr1" for c in got)
        assert fake.calls[0][1]["link_id"] == "thr1"

    def test_newest_comments_is_one_page(self) -> None:
        fake = FakeArctic(comments=[comment(f"c{i}", "t", T - 30 * i, sub="stocks")
                                    for i in range(150)])
        got, complete = a.newest_comments("stocks", since=NOW - timedelta(hours=24), until=NOW,
                                          session=fake)
        assert len(got) == 100 and complete is False and fake.count(COMMENTS_URL) == 1
        fake = FakeArctic(comments=[comment("c1", "t", T - 30, sub="economics")])
        got, complete = a.newest_comments("economics", since=NOW - timedelta(hours=24),
                                          until=NOW, session=fake)
        assert len(got) == 1 and complete is True
