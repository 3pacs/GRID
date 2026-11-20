"""Offline stand-ins for the Arctic Shift API (ingestion/altdata/arctic_shift.py).

``FakeArctic`` answers ``/api/posts/search`` and ``/api/comments/search`` from
in-memory records, applying subreddit / link_id / after / before / limit /
sort the way the live API did on 2026-11-18. Record shapes are trimmed copies
of what grid-svr received that day.
"""

from __future__ import annotations

import json
from typing import Any

from ingestion.altdata import arctic_shift

POSTS_URL = f"{arctic_shift.BASE_URL}/api/posts/search"
COMMENTS_URL = f"{arctic_shift.BASE_URL}/api/comments/search"


class FakeResp:
    """A streamed ``requests`` response with a JSON (or raw) body."""

    def __init__(self, payload: Any = None, status: int = 200,
                 headers: dict[str, str] | None = None, raw: bytes | None = None) -> None:
        self.status_code = status
        self.headers = headers or {}
        self._body = raw if raw is not None else json.dumps(payload).encode()
        self.closed = False

    def iter_content(self, chunk_size: int = 65536) -> Any:
        for i in range(0, len(self._body), chunk_size):
            yield self._body[i:i + chunk_size]

    def close(self) -> None:
        self.closed = True


def post(pid: str, sub: str, ts: float, title: str = "", selftext: str = "",
         author: str = "trader", score: int = 1, num_comments: int = 0) -> dict:
    return {"id": pid, "subreddit": sub, "author": author, "title": title,
            "selftext": selftext, "created_utc": int(ts), "score": score,
            "num_comments": num_comments}


def comment(cid: str, link: str, ts: float, body: str = "", author: str = "user",
            sub: str = "wallstreetbets") -> dict:
    return {"id": cid, "link_id": f"t3_{link}", "author": author, "body": body,
            "created_utc": int(ts), "subreddit": sub}


class FakeArctic:
    """Answers Arctic Shift searches from ``posts`` / ``comments``; records every call."""

    def __init__(self, posts: list[dict] | None = None, comments: list[dict] | None = None,
                 fail: set[str] | frozenset[str] = frozenset()) -> None:
        self.posts = posts or []
        self.comments = comments or []
        self.fail = fail
        self.calls: list[tuple[str, dict, dict]] = []

    def handles(self, url: str) -> bool:
        return url.startswith(arctic_shift.BASE_URL)

    def get(self, url: str, params: dict | None = None, **kwargs: Any) -> FakeResp:
        params = dict(params or {})
        self.calls.append((url, params, kwargs))
        if url == POSTS_URL:
            kind, records = "posts", self.posts
        elif url == COMMENTS_URL:
            kind, records = "comments", self.comments
        else:
            raise AssertionError(f"unexpected Arctic Shift URL {url}")
        if kind in self.fail or f"{kind}:{params.get('subreddit')}" in self.fail:
            return FakeResp({"data": None, "error": "Timeout. Maybe slow down a bit"}, status=500)
        out = []
        for r in records:
            if "subreddit" in params and r.get("subreddit", "").lower() != str(params["subreddit"]).lower():
                continue
            if "link_id" in params and r.get("link_id", "").removeprefix("t3_") != \
                    str(params["link_id"]).removeprefix("t3_"):
                continue
            if "after" in params and r["created_utc"] < int(params["after"]):
                continue
            if "before" in params and r["created_utc"] >= int(params["before"]):
                continue
            out.append(r)
        out.sort(key=lambda r: r["created_utc"], reverse=params.get("sort") != "asc")
        return FakeResp({"data": out[:int(params.get("limit", 25))]},
                        headers={"X-RateLimit-Reset": "3"})

    def count(self, url: str) -> int:
        return sum(1 for u, _, _ in self.calls if u == url)
