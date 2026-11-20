"""smart_money Reddit path on Arctic Shift (no OAuth, no Reddit RSS)."""

from __future__ import annotations

import socket
import time
from typing import Any
from unittest.mock import MagicMock

import pytest

import ingestion.altdata.smart_money as sm
from ingestion.altdata import arctic_shift
from tests.arctic_shift_fakes import POSTS_URL, FakeArctic, post


@pytest.fixture(autouse=True)
def _offline(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    def _refuse(*_a: Any, **_k: Any) -> None:
        raise AssertionError("network access in a unit test")
    monkeypatch.setattr(socket.socket, "connect", _refuse)
    monkeypatch.setenv(arctic_shift.LOCK_PATH_ENV, str(tmp_path / "arctic.lock"))
    monkeypatch.setattr(arctic_shift, "MIN_INTERVAL_S", 0.0)


def _puller() -> sm.SmartMoneyPuller:
    p = sm.SmartMoneyPuller.__new__(sm.SmartMoneyPuller)
    p._trust_cache = {}
    p.engine = MagicMock()
    return p


def _route(monkeypatch: pytest.MonkeyPatch, fake: FakeArctic) -> None:
    monkeypatch.setattr(arctic_shift.requests, "get", fake.get)


def test_one_request_per_board_mapped_to_post_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    now = time.time()
    fake = FakeArctic(posts=[
        post("a1", "wallstreetbets", now - 600, "NVDA calls", "buying calls", score=1),
        post("old", "wallstreetbets", now - 30 * 3600, "AMD puts"),     # outside 24 h
        post("s1", "stocks", now - 900, "SPY puts", author="bear"),
    ])
    _route(monkeypatch, fake)
    posts = _puller()._fetch_hot_posts(["wallstreetbets", "stocks"])
    assert fake.count(POSTS_URL) == 2
    assert [p["subreddit"] for _, p, _ in fake.calls] == ["wallstreetbets", "stocks"]
    assert {p["limit"] for _, p, _ in fake.calls} == {sm._MAX_HOT_POSTS}
    a1 = next(p for p in posts if p["title"] == "NVDA calls")
    # A 10-minute-old post has no settled score: the corroboration rule applies.
    assert a1 == {"id": "a1", "title": "NVDA calls", "selftext": "buying calls", "author": "trader",
                  "subreddit": "wallstreetbets", "created_utc": float(int(now - 600)),
                  "score": None, "num_comments": None, "upvote_ratio": None}
    assert {p["title"] for p in posts} == {"NVDA calls", "SPY puts"}


def test_one_board_down_is_partial_all_down_is_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    now = time.time()
    fake = FakeArctic(posts=[post(f"b{i}", "stocks", now - 60 * i, "AMD puts, crash coming",
                                  author=f"bear{i}") for i in range(1, 3)],
                      fail={"posts:wallstreetbets"})
    _route(monkeypatch, fake)
    p = _puller()
    p._load_trust_scores = lambda: None
    stored: list[dict] = []
    p._store_signal = lambda conn, signal, d: stored.append(signal) or True
    out = p.pull_reddit(["wallstreetbets", "stocks"])
    assert out["status"] == "PARTIAL" and out["rows_inserted"] == 2
    assert "1 of 2 boards failed: r/wallstreetbets" in out["error"]
    fake.fail = {"posts"}
    p2 = _puller()
    p2._load_trust_scores = lambda: None
    out = p2.pull_reddit(["wallstreetbets", "stocks"])
    assert out["status"] == "FAILED" and "arctic shift" in out["error"]


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


def _post(author: str, title: str) -> dict:
    return {"title": title, "selftext": "", "author": author, "subreddit": "stocks",
            "score": None, "created_utc": None}


def test_corroborated_unscored_signals_are_stored() -> None:
    p = _puller()
    p._load_trust_scores = lambda: None
    p._fetch_hot_posts = lambda subs: [_post("bear1", "AMD puts, crash coming"),
                                       _post("bear2", "AMD dump incoming, buying puts")]
    stored: list[dict] = []
    p._store_signal = lambda conn, signal, d: stored.append(signal) or True
    out = p.pull_reddit()
    assert out == {"source": "reddit", "status": "SUCCESS", "signals_found": 2, "rows_inserted": 2}
    assert {(s["username"], s["ticker"], s["direction"]) for s in stored} == {
        ("bear1", "AMD", "BEARISH"), ("bear2", "AMD", "BEARISH")}


def test_single_author_unscored_signal_is_dropped() -> None:
    p = _puller()
    p._load_trust_scores = lambda: None
    # One author posting twice is still one author.
    p._fetch_hot_posts = lambda subs: [_post("pumper", "GME calls, squeeze to the moon"),
                                       _post("pumper", "GME calls again, breakout")]
    p._store_signal = lambda conn, signal, d: True
    out = p.pull_reddit()
    assert out["signals_found"] == 0 and out["rows_inserted"] == 0


def test_corroboration_leaves_scored_signals_alone() -> None:
    sig = {"username": "a", "ticker": "NVDA", "direction": "BULLISH", "post_score": 50}
    unscored = {"username": "b", "ticker": "TSLA", "direction": "BEARISH", "post_score": None}
    assert sm._corroborate_unscored([sig, unscored]) == [sig]


# ── Trust gate for unscored (young) posts ──────────────────────────────────
# Arctic Shift settles scores ~36 h after a post, so every post in the 24 h
# window is unscored: the score gate cannot screen it. Two gates replace it:
# (a) at least _MIN_UNSCORED_AUTHORS distinct authors per (ticker, direction)
# in the pull, and (d) unscored signals stay out of signal_sources until
# corroborated across runs. (c) holds by construction: arctic_shift.post()
# reports no score before the 36 h settle, so no score logic sees a young post.


def _gate_puller(monkeypatch: pytest.MonkeyPatch, posts: list[dict]) -> tuple[Any, list, list]:
    _route(monkeypatch, FakeArctic(posts=posts))
    p = _puller()
    p.source_id = 7
    p._load_trust_scores = lambda: None
    p._row_exists = lambda series_id, obs_date, conn: False
    raw: list[dict] = []
    emitted: list[dict] = []
    p._insert_raw = lambda conn, series_id, obs_date, value, raw_payload: raw.append(
        {"series_id": series_id, **raw_payload})
    p._emit_signal_sources = lambda conn, signal, obs_date: emitted.append(signal)
    return p, raw, emitted


def test_one_throwaway_author_gives_no_signal_even_with_a_big_young_score(
        monkeypatch: pytest.MonkeyPatch) -> None:
    now = time.time()
    posts = [post(f"t{i}", "wallstreetbets", now - 60 * i, "TSLA calls, squeeze",
                  author="throwaway_1", score=999) for i in range(1, 4)]
    p, raw, emitted = _gate_puller(monkeypatch, posts)
    out = p.pull_reddit(["wallstreetbets"])
    assert out["signals_found"] == 0 and raw == [] and emitted == []


def test_distinct_authors_give_a_signal_kept_out_of_signal_sources(
        monkeypatch: pytest.MonkeyPatch) -> None:
    now = time.time()
    posts = [post(f"d{i}", "stocks", now - 120 * i, "TSLA puts, crash coming", author=f"bear{i}")
             for i in range(1, sm._MIN_UNSCORED_AUTHORS + 1)]
    p, raw, emitted = _gate_puller(monkeypatch, posts)
    out = p.pull_reddit(["stocks"])
    assert out["signals_found"] == sm._MIN_UNSCORED_AUTHORS == out["rows_inserted"]
    assert {r["username"] for r in raw} == {f"bear{i}" for i in range(1, sm._MIN_UNSCORED_AUTHORS + 1)}
    assert {r["trust_tier"] for r in raw} == {"unscored"}
    assert emitted == []  # not yet corroborated across runs


def _stored(author: str, pid: str, created: float, text_: str = "TSLA puts, crash coming",
            ticker: str = "TSLA", direction: str = "BEARISH") -> tuple[str, dict]:
    """A raw_series row as an earlier run wrote it (series_id, raw_payload)."""
    from datetime import datetime, timezone
    return (f"SOCIAL:reddit:{author}:{ticker}:{direction}",
            {"post_id": pid, "post_hash": sm._text_hash(f"{text_} "),
             "post_timestamp": datetime.fromtimestamp(created, tz=timezone.utc).isoformat()})


class _PriorDb:
    """Engine whose prior-run lookup returns ``rows``; records the query parameters."""

    def __init__(self, rows: list[tuple[str, Any]]) -> None:
        self.rows, self.params = rows, []
        conn = MagicMock()

        def execute(sql: Any, params: dict | None = None) -> Any:
            self.params.append(params)
            res = MagicMock()
            res.fetchall.return_value = list(self.rows)
            return res
        conn.execute.side_effect = execute
        self.engine = MagicMock()
        self.engine.begin.return_value.__enter__.return_value = conn


def _run_with_prior(monkeypatch: pytest.MonkeyPatch, posts: list[dict],
                    prior: list[tuple[str, Any]]) -> tuple[list, list]:
    p, raw, emitted = _gate_puller(monkeypatch, posts)
    p.engine = _PriorDb(prior).engine
    p.pull_reddit(["stocks"])
    return raw, emitted


def test_re_pulling_the_same_posts_never_promotes(monkeypatch: pytest.MonkeyPatch) -> None:
    # Run 1 stored bear1/bear2/bear3; hours later the same three posts are still in
    # the 24 h window. All are already stored, so nothing is fresh: no promotion,
    # however many earlier rows there are or how old their pull_timestamp is.
    now = time.time()
    posts = [post(f"d{i}", "stocks", now - 3600 * (8 + i), "TSLA puts, crash coming",
                  author=f"bear{i}") for i in (1, 2, 3)]
    prior = [_stored(f"bear{i}", f"d{i}", now - 3600 * (8 + i)) for i in (1, 2, 3)]
    raw, emitted = _run_with_prior(monkeypatch, posts, prior)
    assert {r["trust_tier"] for r in raw} == {"unscored"} and emitted == []


def test_legacy_rows_without_post_ids_dedupe_by_author_and_text(
        monkeypatch: pytest.MonkeyPatch) -> None:
    now = time.time()
    posts = [post(f"d{i}", "stocks", now - 3600 * (8 + i), "TSLA puts, crash coming",
                  author=f"bear{i}") for i in (1, 2, 3)]
    prior = [(sid, {**pl, "post_id": ""}) for sid, pl in
             (_stored(f"bear{i}", "", now - 3600 * (8 + i)) for i in (1, 2, 3))]
    raw, emitted = _run_with_prior(monkeypatch, posts, prior)
    assert emitted == []


def test_old_authors_reposting_now_do_not_count_as_new(monkeypatch: pytest.MonkeyPatch) -> None:
    now = time.time()
    prior = [_stored("bear1", "o1", now - 20 * 3600), _stored("bear2", "o2", now - 20 * 3600)]
    # New post ids and new text, but the same two authors: no new corroborator.
    posts = [post("n1", "stocks", now - 600, "TSLA puts again, crash", author="bear1"),
             post("n2", "stocks", now - 300, "still TSLA puts, crash", author="bear2")]
    raw, emitted = _run_with_prior(monkeypatch, posts, prior)
    assert emitted == []


def test_prior_posts_must_predate_fresh_posts_by_the_minimum_age(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # Earlier authors posted only 2 h before the new author: one burst, not corroboration.
    now = time.time()
    prior = [_stored("bear1", "o1", now - 3 * 3600), _stored("bear2", "o2", now - 3 * 3600)]
    posts = [post("o1", "stocks", now - 3 * 3600, "TSLA puts, crash coming", author="bear1"),
             post("n3", "stocks", now - 3600, "TSLA puts, crash coming", author="bear3")]
    raw, emitted = _run_with_prior(monkeypatch, posts, prior)
    assert emitted == []


def test_genuinely_later_posts_from_new_authors_promote(monkeypatch: pytest.MonkeyPatch) -> None:
    now = time.time()
    prior = [_stored("bear1", "o1", now - 20 * 3600), _stored("bear2", "o2", now - 19 * 3600)]
    posts = [post("o2", "stocks", now - 19 * 3600, "TSLA puts, crash coming", author="bear2"),
             post("n3", "stocks", now - 1800, "TSLA puts, crash coming", author="bear3"),
             post("n4", "stocks", now - 900, "TSLA puts, crash coming", author="bear4")]
    raw, emitted = _run_with_prior(monkeypatch, posts, prior)
    assert {r["trust_tier"] for r in raw} == {"unscored_promoted"}
    assert {s["username"] for s in emitted} == {"bear2", "bear3", "bear4"}
    assert {r["post_id"] for r in raw} == {"o2", "n3", "n4"}  # ids kept for the next run


def test_one_prior_author_is_not_corroboration(monkeypatch: pytest.MonkeyPatch) -> None:
    now = time.time()
    prior = [_stored("bear1", "o1", now - 20 * 3600)]
    posts = [post("n3", "stocks", now - 1800, "TSLA puts, crash coming", author="bear3"),
             post("n4", "stocks", now - 900, "TSLA puts, crash coming", author="bear4")]
    raw, emitted = _run_with_prior(monkeypatch, posts, prior)
    assert emitted == []


def test_promoted_signal_with_an_existing_row_still_emits(monkeypatch: pytest.MonkeyPatch) -> None:
    p, raw, emitted = _gate_puller(monkeypatch, [])
    p._row_exists = lambda series_id, obs_date, conn: True
    sig = {"platform": "reddit", "username": "bear1", "ticker": "TSLA", "direction": "BEARISH",
           "post_score": None}
    from datetime import date
    assert p._store_signal(MagicMock(), {**sig, "promoted": True}, date(2026, 11, 18)) is False
    assert p._store_signal(MagicMock(), sig, date(2026, 11, 18)) is False
    assert raw == [] and len(emitted) == 1


def test_prior_posts_query_and_parsing() -> None:
    from datetime import datetime, timedelta, timezone
    p = _puller()
    p.source_id = 7
    run = datetime(2026, 11, 18, 12, tzinfo=timezone.utc)
    db = _PriorDb([
        ("SOCIAL:reddit:bear1:TSLA:BEARISH", {"post_id": "a1", "post_hash": "h",
                                             "post_timestamp": "2026-11-18T01:00:00+00:00"}),
        ("SOCIAL:reddit:bear_2:TSLA:BEARISH", '{"post_timestamp": "", "post_hash": "h2"}'),
        ("SOCIAL:reddit:x:TSLAX:BEARISH", {}), ("garbage", {})])
    conn = db.engine.begin.return_value.__enter__.return_value
    got = p._prior_posts(conn, "TSLA", "BEARISH", run)
    assert got == [
        {"author": "bear1", "post_id": "a1", "post_hash": "h",
         "created": datetime(2026, 11, 18, 1, tzinfo=timezone.utc).timestamp()},
        {"author": "bear_2", "post_id": "", "post_hash": "h2", "created": None}]
    params = db.params[0]
    assert params["pat"] == "SOCIAL:reddit:%:TSLA:BEARISH" and params["src"] == 7
    assert params["hi"] == run and params["lo"] == run - timedelta(hours=sm._PROMOTE_LOOKBACK_H)
    conn.execute.side_effect = RuntimeError("db down")
    assert p._prior_posts(conn, "TSLA", "BEARISH", run) == []  # fails closed
