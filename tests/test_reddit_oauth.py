"""Reddit Data API (OAuth) path for reddit_options_pulse and smart_money.

No network. The OAuth/listing payloads below are SYNTHETIC, shaped after
Reddit's documented Data API responses (no credentials exist yet to record
real ones -- see ingestion/altdata/reddit_oauth.py for the owner setup).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

import ingestion.altdata.reddit_oauth as ro
import ingestion.altdata.reddit_options_pulse as rop
from ingestion.altdata.reddit_oauth import (
    RedditAPIError,
    RedditAppClient,
    RedditNotConfigured,
    reddit_credentials_configured,
)

SEARCH = {
    "kind": "Listing",
    "data": {"children": [{"kind": "t3", "data": {
        "id": "1abcde", "title": "Daily Discussion Thread - September 28, 2026",
        "permalink": "/r/options/comments/1abcde/daily_discussion/",
        "created_utc": 1790582400, "num_comments": 3,
    }}]},
}
THREAD = [
    {"kind": "Listing", "data": {"children": [{"kind": "t3", "data": {"id": "1abcde"}}]}},
    {"kind": "Listing", "data": {"children": [
        {"kind": "t1", "data": {"author": "a1", "body": "Loading NVDA calls, 0dte yolo"}},
        {"kind": "t1", "data": {"author": "a2", "body": "SPY puts, bearish into CPI"}},
        {"kind": "t1", "data": {"author": "a1", "body": "NVDA breakout"}},
    ]}},
]


class _Resp:
    def __init__(self, status: int, body: Any) -> None:
        self.status_code = status
        self._body = body

    def json(self) -> Any:
        return self._body


class _Session:
    def __init__(self, get_map: dict[str, _Resp], token_status: int = 200) -> None:
        self.get_map = get_map
        self.token_status = token_status
        self.posts: list[dict] = []
        self.gets: list[dict] = []

    def post(self, url, auth=None, data=None, headers=None, timeout=0):  # noqa: ANN001, ARG002
        self.posts.append({"url": url, "auth": auth, "data": data, "headers": headers})
        return _Resp(self.token_status, {"access_token": "tok", "token_type": "bearer", "expires_in": 86400})

    def get(self, url, params=None, headers=None, timeout=0):  # noqa: ANN001, ARG002
        self.gets.append({"url": url, "params": params, "headers": headers})
        for suffix, resp in self.get_map.items():
            if url.endswith(suffix):
                return resp
        return _Resp(404, {})


@pytest.fixture
def no_creds(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (ro.ENV_CLIENT_ID, ro.ENV_CLIENT_SECRET, ro.ENV_USERNAME):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(ro, "_setting", lambda name: "")


def _engine() -> MagicMock:
    engine = MagicMock()
    cconn = MagicMock()
    cconn.execute.return_value.fetchone.return_value = (4692,)
    engine.connect.return_value.__enter__ = MagicMock(return_value=cconn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    bconn = MagicMock()
    bconn.execute.return_value.fetchall.return_value = []
    bconn.execute.return_value.fetchone.return_value = None
    engine.begin.return_value.__enter__ = MagicMock(return_value=bconn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    engine._bconn = bconn
    return engine


def _inserts(engine: MagicMock) -> list[dict]:
    return [c.args[1] for c in engine._bconn.execute.call_args_list
            if "INSERT INTO raw_series" in str(c.args[0])]


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


def test_not_configured_raises(no_creds: None) -> None:
    assert reddit_credentials_configured() is False
    with pytest.raises(RedditNotConfigured):
        RedditAppClient()


def test_token_then_bearer_get_on_oauth_host() -> None:
    session = _Session({"/r/options/search": _Resp(200, SEARCH)})
    client = RedditAppClient("cid", "csecret", "griduser", session=session)
    out = client.get_json("/r/options/search", params={"q": "x"})
    out2 = client.get_json("/r/options/search", params={"q": "y"})
    assert out == SEARCH == out2
    assert len(session.posts) == 1  # token cached
    assert session.posts[0]["url"] == "https://www.reddit.com/api/v1/access_token"
    assert session.posts[0]["data"] == {"grant_type": "client_credentials"}
    g = session.gets[0]
    assert g["url"] == "https://oauth.reddit.com/r/options/search"
    assert g["headers"]["Authorization"] == "bearer tok"
    assert g["headers"]["User-Agent"] == "linux:grid-intelligence:1.0 (by /u/griduser)"
    assert g["params"]["raw_json"] == 1


def test_http_error_raises_api_error() -> None:
    session = _Session({"/r/options/search": _Resp(403, {"message": "Forbidden"})})
    client = RedditAppClient("cid", "csecret", "u", session=session)
    with pytest.raises(RedditAPIError) as ei:
        client.get_json("/r/options/search")
    assert ei.value.status == 403


def test_token_failure_raises() -> None:
    client = RedditAppClient("cid", "bad", "u", session=_Session({}, token_status=401))
    with pytest.raises(RedditAPIError):
        client.get_json("/r/options/search")


# ---------------------------------------------------------------------------
# reddit_options_pulse
# ---------------------------------------------------------------------------


def test_pulse_not_configured_is_failed_and_sends_nothing(no_creds: None, monkeypatch: pytest.MonkeyPatch) -> None:
    def _no_network(*a, **k):  # noqa: ANN002, ANN003
        raise AssertionError("must not touch the network")

    monkeypatch.setattr(rop.requests, "get", _no_network)
    engine = _engine()
    summary = rop.run_reddit_options_pulse_puller(engine)
    assert summary["status"] == "FAILED"
    assert "not configured" in summary["error"]
    assert summary["inserted"] == 0
    assert _inserts(engine) == []


def test_pulse_forbidden_is_failed_and_writes_nothing() -> None:
    session = _Session({"/r/options/search": _Resp(403, {})})
    client = RedditAppClient("cid", "csecret", "u", session=session)
    engine = _engine()
    puller = rop.RedditOptionsPulsePuller(engine, client=client)
    assert puller.pull() == []
    assert puller.last_error == "no daily-discussion thread fetched"
    assert _inserts(engine) == []


def test_pulse_happy_path_via_oauth() -> None:
    session = _Session({
        "/r/options/search": _Resp(200, SEARCH),
        "/r/options/comments/1abcde": _Resp(200, THREAD),
    })
    client = RedditAppClient("cid", "csecret", "u", session=session)
    engine = _engine()
    puller = rop.RedditOptionsPulsePuller(engine, client=client)
    pulses = puller.pull()
    assert len(pulses) == 1
    assert pulses[0].comment_count == 3
    assert pulses[0].unique_authors == 2
    inserted = puller.save_to_db(pulses)
    rows = _inserts(engine)
    assert inserted == len(rows) > 0
    assert all("pull_timestamp" not in r for r in rows)  # column default = fetch time
    # Only the OAuth host is used -- never www.reddit.com listing JSON.
    assert all(g["url"].startswith("https://oauth.reddit.com/") for g in session.gets)


# ---------------------------------------------------------------------------
# smart_money Reddit path
# ---------------------------------------------------------------------------


def _smart_money_puller():
    from ingestion.altdata.smart_money import SmartMoneyPuller

    p = SmartMoneyPuller.__new__(SmartMoneyPuller)
    p.engine = _engine()
    p.source_id = 1
    p._load_trust_scores = lambda: None  # type: ignore[method-assign]
    return p


def test_smart_money_reddit_not_configured_is_failed(no_creds: None, monkeypatch: pytest.MonkeyPatch) -> None:
    import ingestion.altdata.smart_money as sm

    monkeypatch.setattr(sm.time, "sleep", lambda s: None)
    result = _smart_money_puller().pull_reddit(subreddits=["options"])
    assert result["status"] == "FAILED"
    assert result["rows_inserted"] == 0


def test_smart_money_all_subreddits_failing_is_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    import ingestion.altdata.smart_money as sm

    monkeypatch.setattr(sm.time, "sleep", lambda s: None)
    p = _smart_money_puller()
    p._reddit_client = RedditAppClient("cid", "csecret", "u", session=_Session({}))  # every GET 404
    result = p.pull_reddit(subreddits=["options", "stocks"])
    assert result["status"] == "FAILED"
    assert "all 2" in result["error"]
