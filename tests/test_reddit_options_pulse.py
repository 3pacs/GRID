"""Unit tests for ingestion/altdata/reddit_options_pulse.py."""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from ingestion.altdata.reddit_options_pulse import (
    BEARISH_TOKENS,
    BULLISH_TOKENS,
    DAILY_THREAD_QUERIES,
    REDDIT_USER_AGENT,
    RedditOptionsPulse,
    RedditOptionsPulsePuller,
    TICKER_FALSE_POSITIVES,
    TICKER_PATTERN,
    TOP_N_TICKERS,
    ZERO_DTE_TOKENS,
    _aggregate_pulse,
    _flatten_comments,
    _rank_tickers,
    compute_bull_bear_ratio,
    count_sentiment_tokens,
    extract_tickers,
    run_reddit_options_pulse_puller,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_engine():
    """Mock SQLAlchemy engine that pretends source_id == 7."""
    engine = MagicMock()
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value = ctx
    engine.begin.return_value = ctx

    # _resolve_source_id() does conn.execute(...).fetchone() and indexes [0].
    row_mock = MagicMock()
    row_mock.__getitem__ = lambda self, idx: 7
    conn.execute.return_value.fetchone.return_value = row_mock
    # _get_existing_dates() does .fetchall() — return empty by default
    conn.execute.return_value.fetchall.return_value = []

    return engine


@pytest.fixture
def puller(mock_engine):
    return RedditOptionsPulsePuller(mock_engine)


# ---------------------------------------------------------------------------
# Dataclass roundtrip
# ---------------------------------------------------------------------------


class TestRedditOptionsPulseDataclass:
    def test_frozen_roundtrip(self):
        p = RedditOptionsPulse(
            thread_date=date(2026, 4, 13),
            thread_id="abc123",
            post_count=1,
            comment_count=42,
            unique_authors=30,
            bullish_count=12,
            bearish_count=4,
            zero_dte_count=3,
            bull_bear_ratio=2.6,
            top_tickers=(("AAPL", 5), ("NVDA", 3)),
            thread_url="https://www.reddit.com/r/options/comments/abc123/",
        )
        assert p.thread_id == "abc123"
        assert p.comment_count == 42
        assert p.top_tickers == (("AAPL", 5), ("NVDA", 3))
        assert p.bull_bear_ratio == pytest.approx(2.6)

        # Frozen — assignment must raise.
        with pytest.raises(Exception):
            p.comment_count = 99  # type: ignore[misc]


# ---------------------------------------------------------------------------
# extract_tickers
# ---------------------------------------------------------------------------


class TestExtractTickers:
    def test_happy_path(self):
        assert extract_tickers("AAPL calls at $180") == ["AAPL"]

    def test_dollar_prefix(self):
        assert extract_tickers("$NVDA to the moon") == ["NVDA"]

    def test_common_word_filter(self):
        # FED, ETC, ETF should all be filtered out.
        out = extract_tickers("FED cut rates, ETC ETF flows higher")
        assert out == []

    def test_multiple_tickers(self):
        out = extract_tickers("AAPL, MSFT, NVDA calls")
        assert out == ["AAPL", "MSFT", "NVDA"]

    def test_lowercase_not_matched(self):
        # Mixed-case "Aapl" must NOT match — the rule is 1-5 *uppercase*.
        assert extract_tickers("Aapl is great") == []
        assert extract_tickers("aapl is great") == []

    def test_too_long_not_matched(self):
        # FAKECOMPANY = 11 chars, exceeds the 5-letter cap.
        out = extract_tickers("FAKECOMPANY just listed today")
        assert "FAKECOMPANY" not in out
        # And a bare 6+ letter run is fully ignored.
        assert extract_tickers("ABCDEF") == []

    def test_empty_string(self):
        assert extract_tickers("") == []


# ---------------------------------------------------------------------------
# count_sentiment_tokens
# ---------------------------------------------------------------------------


class TestCountSentimentTokens:
    def test_bullish_present(self):
        assert count_sentiment_tokens("loaded up on calls", BULLISH_TOKENS) >= 1

    def test_word_boundary_recalls(self):
        # "recalls" must NOT match the "calls" token.
        text = "Tesla recalls 100k vehicles"
        assert count_sentiment_tokens(text, BULLISH_TOKENS) == 0

    def test_mixed_case(self):
        assert count_sentiment_tokens("BULLISH on NVDA", BULLISH_TOKENS) == 1

    def test_multiple_occurrences(self):
        text = "calls calls calls and more calls"
        assert count_sentiment_tokens(text, BULLISH_TOKENS) == 4

    def test_zero_dte_multiword(self):
        text = "I bought 0 dte SPY puts that expire today"
        # "0 dte" + "expire today" + "puts" — but here we only check ZERO_DTE_TOKENS
        assert count_sentiment_tokens(text, ZERO_DTE_TOKENS) >= 2

    def test_empty_text(self):
        assert count_sentiment_tokens("", BULLISH_TOKENS) == 0


# ---------------------------------------------------------------------------
# compute_bull_bear_ratio
# ---------------------------------------------------------------------------


class TestBullBearRatio:
    def test_happy(self):
        assert compute_bull_bear_ratio(10, 5) == pytest.approx(11 / 6)

    def test_zero_bear(self):
        assert compute_bull_bear_ratio(5, 0) == pytest.approx(6.0)

    def test_zero_both(self):
        assert compute_bull_bear_ratio(0, 0) == pytest.approx(1.0)

    def test_zero_bull(self):
        assert compute_bull_bear_ratio(0, 10) == pytest.approx(1 / 11)


# ---------------------------------------------------------------------------
# _rank_tickers
# ---------------------------------------------------------------------------


class TestRankTickers:
    def test_top_20_from_30(self):
        counts = {f"T{i:02d}": 100 - i for i in range(30)}
        out = _rank_tickers(counts, 20)
        assert len(out) == 20
        # Sorted desc by count
        assert [c for _, c in out] == sorted([c for _, c in out], reverse=True)

    def test_tie_breaking_alphabetical(self):
        counts = {"ZZZ": 5, "AAA": 5, "MMM": 5}
        out = _rank_tickers(counts, 3)
        assert [t for t, _ in out] == ["AAA", "MMM", "ZZZ"]

    def test_fewer_than_n(self):
        counts = {"A": 3, "B": 2, "C": 1, "D": 1, "E": 1}
        out = _rank_tickers(counts, 20)
        assert len(out) == 5
        # First three are determined by count; D / E / etc tie-break alpha.
        assert out[0] == ("A", 3)
        assert out[1] == ("B", 2)

    def test_empty(self):
        assert _rank_tickers({}, 10) == []


# ---------------------------------------------------------------------------
# _flatten_comments
# ---------------------------------------------------------------------------


class TestFlattenComments:
    def test_nested_replies_real_format(self):
        # Real Reddit shape: [post_listing, comments_listing]
        thread = [
            {  # post listing
                "kind": "Listing",
                "data": {
                    "children": [
                        {"kind": "t3", "data": {"id": "abc", "title": "Daily Discussion"}}
                    ]
                },
            },
            {  # comments listing
                "kind": "Listing",
                "data": {
                    "children": [
                        {
                            "kind": "t1",
                            "data": {
                                "id": "c1",
                                "body": "AAPL calls",
                                "author": "alice",
                                "replies": {
                                    "kind": "Listing",
                                    "data": {
                                        "children": [
                                            {
                                                "kind": "t1",
                                                "data": {
                                                    "id": "c2",
                                                    "body": "I disagree, puts",
                                                    "author": "bob",
                                                    "replies": "",
                                                },
                                            },
                                            {
                                                "kind": "more",
                                                "data": {"children": []},
                                            },
                                        ]
                                    },
                                },
                            },
                        },
                        {
                            "kind": "t1",
                            "data": {
                                "id": "c3",
                                "body": "NVDA to the moon",
                                "author": "carol",
                                "replies": "",
                            },
                        },
                    ]
                },
            },
        ]
        flat = _flatten_comments(thread)
        bodies = sorted(c.get("body") for c in flat)
        assert bodies == ["AAPL calls", "I disagree, puts", "NVDA to the moon"]
        assert len(flat) == 3

    def test_empty_thread(self):
        assert _flatten_comments([]) == []
        assert (
            _flatten_comments(
                [
                    {"kind": "Listing", "data": {"children": []}},
                    {"kind": "Listing", "data": {"children": []}},
                ]
            )
            == []
        )

    def test_malformed_skipped(self):
        # Comment without a body is skipped; one with a body is kept.
        thread = [
            {"kind": "Listing", "data": {"children": []}},
            {
                "kind": "Listing",
                "data": {
                    "children": [
                        {"kind": "t1", "data": {"id": "x", "author": "a"}},  # no body
                        {
                            "kind": "t1",
                            "data": {"id": "y", "body": "valid", "author": "b"},
                        },
                    ]
                },
            },
        ]
        flat = _flatten_comments(thread)
        assert len(flat) == 1
        assert flat[0]["body"] == "valid"


# ---------------------------------------------------------------------------
# Puller integration tests (fully mocked)
# ---------------------------------------------------------------------------


def _make_canned_thread() -> dict:
    """Build a fake thread payload with ~3 comments mentioning AAPL and NVDA."""
    post = {
        "id": "thread1",
        "title": "Daily Discussion 2026-04-13",
        "permalink": "/r/options/comments/thread1/daily_discussion/",
        "created_utc": float(
            datetime(2026, 4, 13, 14, 30, tzinfo=timezone.utc).timestamp()
        ),
        "selftext": "What are your moves tomorrow?",
        "author": "AutoModerator",
    }
    comments_listing = {
        "kind": "Listing",
        "data": {
            "children": [
                {
                    "kind": "t1",
                    "data": {
                        "id": "c1",
                        "body": "$AAPL calls, $NVDA calls — bullish",
                        "author": "alice",
                        "replies": "",
                    },
                },
                {
                    "kind": "t1",
                    "data": {
                        "id": "c2",
                        "body": "AAPL puts, bearish 0dte",
                        "author": "bob",
                        "replies": "",
                    },
                },
                {
                    "kind": "t1",
                    "data": {
                        "id": "c3",
                        "body": "NVDA breakout, going long",
                        "author": "carol",
                        "replies": "",
                    },
                },
            ]
        },
    }
    post_listing = {
        "kind": "Listing",
        "data": {"children": [{"kind": "t3", "data": post}]},
    }
    thread_json = [post_listing, comments_listing]
    return {"post": post, "thread": thread_json}


class TestPullerHappyPath:
    def test_pull_and_save(self, puller):
        canned = _make_canned_thread()
        with patch(
            "ingestion.altdata.reddit_options_pulse._fetch_daily_thread",
            return_value=canned,
        ):
            pulses = puller.pull()

        assert len(pulses) == 1
        pulse = pulses[0]
        assert pulse.thread_id == "thread1"
        assert pulse.comment_count == 3
        assert pulse.unique_authors == 3
        # bullish > bearish (calls/calls/long/breakout/bullish vs puts/bearish)
        assert pulse.bull_bear_ratio > 1.0
        assert pulse.zero_dte_count >= 1
        # AAPL appears 2x, NVDA 2x
        ticker_dict = dict(pulse.top_tickers)
        assert ticker_dict.get("AAPL", 0) >= 2
        assert ticker_dict.get("NVDA", 0) >= 2

        # save_to_db happy path
        inserted = puller.save_to_db(pulses)
        assert inserted > 0


class TestPullerRateLimited:
    def test_fetch_returns_none(self, puller):
        with patch(
            "ingestion.altdata.reddit_options_pulse._fetch_daily_thread",
            return_value=None,
        ):
            pulses = puller.pull()
        assert pulses == []

    def test_fetch_raises(self, puller):
        with patch(
            "ingestion.altdata.reddit_options_pulse._fetch_daily_thread",
            side_effect=RuntimeError("boom"),
        ):
            pulses = puller.pull()
        assert pulses == []


class TestPullerEmpty:
    def test_empty_payload(self, puller):
        empty_payload = {
            "post": {
                "id": "empty1",
                "permalink": "/r/options/comments/empty1/",
                "created_utc": float(
                    datetime(2026, 4, 13, tzinfo=timezone.utc).timestamp()
                ),
                "selftext": "",
                "title": "Daily Discussion",
            },
            "thread": [
                {"kind": "Listing", "data": {"children": []}},
                {"kind": "Listing", "data": {"children": []}},
            ],
        }
        with patch(
            "ingestion.altdata.reddit_options_pulse._fetch_daily_thread",
            return_value=empty_payload,
        ):
            pulses = puller.pull()
        assert len(pulses) == 1
        p = pulses[0]
        assert p.comment_count == 0
        assert p.unique_authors == 0
        assert p.bull_bear_ratio == pytest.approx(1.0)


class TestPullerIdempotent:
    def test_re_run_no_duplicates(self, mock_engine):
        """Re-running on the same thread_date does not re-insert."""
        puller = RedditOptionsPulsePuller(mock_engine)
        canned = _make_canned_thread()

        # Make _get_existing_dates return the thread date so every series
        # reports the row already present.
        thread_date = date(2026, 4, 13)

        def fake_get_existing(self, sid, conn):  # noqa: ARG001
            return {thread_date}

        with patch.object(
            RedditOptionsPulsePuller,
            "_get_existing_dates",
            new=fake_get_existing,
        ):
            with patch(
                "ingestion.altdata.reddit_options_pulse._fetch_daily_thread",
                return_value=canned,
            ):
                pulses = puller.pull()
                inserted = puller.save_to_db(pulses)

        assert inserted == 0


class TestRunEntrypoint:
    def test_run_happy(self, mock_engine):
        canned = _make_canned_thread()
        with patch(
            "ingestion.altdata.reddit_options_pulse._fetch_daily_thread",
            return_value=canned,
        ):
            summary = run_reddit_options_pulse_puller(mock_engine)

        assert summary["fetched"] == 1
        assert summary["inserted"] > 0
        assert summary["thread_date"] == "2026-04-13"
        assert summary["comment_count"] == 3
        assert summary["bull_bear_ratio"] is not None
        assert summary["top_tickers"] is not None

    def test_run_no_thread(self, mock_engine):
        with patch(
            "ingestion.altdata.reddit_options_pulse._fetch_daily_thread",
            return_value=None,
        ):
            summary = run_reddit_options_pulse_puller(mock_engine)
        assert summary["fetched"] == 0
        assert summary["inserted"] == 0
        assert summary["thread_date"] is None


# ---------------------------------------------------------------------------
# Constants sanity
# ---------------------------------------------------------------------------


class TestConstants:
    def test_token_sets_disjoint(self):
        # Bullish and bearish must not overlap.
        assert BULLISH_TOKENS.isdisjoint(BEARISH_TOKENS)

    def test_user_agent_identifies_grid(self):
        assert "grid" in REDDIT_USER_AGENT.lower()

    def test_daily_queries_nonempty(self):
        assert len(DAILY_THREAD_QUERIES) >= 3

    def test_top_n_is_20(self):
        assert TOP_N_TICKERS == 20

    def test_ticker_pattern_is_regex(self):
        assert TICKER_PATTERN.search("$AAPL") is not None

    def test_false_positives_block_common(self):
        for word in ("FED", "SEC", "ETF", "GDP"):
            assert word in TICKER_FALSE_POSITIVES
