"""
Reddit /r/options daily-discussion pulse — retail options positioning signal.

The /r/options daily discussion thread is auto-posted by Reddit every morning
and is the single highest-density retail-options positioning dataset publicly
available.  It is NOT in the original CAT data catalog and is complementary
to the existing unusual_whales / smart_money pullers (which lean on whale or
insider flow).  This puller is therefore a novel Tier-A source.

Why it matters:

    * Post count + unique commenter count + ticker mention velocity +
      bullish/bearish sentiment ratio + 0DTE reference count together paint
      a picture of retail risk appetite 1-3 days BEFORE it shows up in
      whale flow or dark-pool prints.
    * Extreme pulse readings (top 5% of historical distribution) are
      reliable contrarian signals — when the daily discussion is wall-to-wall
      "calls / yolo / squeeze" the next-day reversal odds are elevated.
    * The 0DTE reference count is itself a leading indicator of intraday
      gamma squeezes and pin risk.

Data source:

    Reddit public JSON API — no auth, no API key, only requires a
    User-Agent header.  Rate limit: ~60 requests / minute / IP.

    * https://www.reddit.com/r/options/search.json
        ?q=title%3A%22daily+discussion%22&restrict_sr=1&sort=new&limit=10
      -> finds the daily discussion threads by title match.

    * https://www.reddit.com/<permalink>.json?limit=500
      -> returns the thread post + the entire comment tree.

Failure modes — graceful degrade only, never crash:

    * 429 / 403 from Reddit -> log warning, return zero rows.
    * Empty / malformed JSON -> log warning, return zero rows.
    * Search returns no daily-discussion match -> log info, return zero rows.

Series stored in raw_series:

    * reddit_options:comment_count
    * reddit_options:unique_authors
    * reddit_options:bull_bear_ratio
    * reddit_options:zero_dte_count
    * reddit_options:top_ticker_rank:1 .. reddit_options:top_ticker_rank:20
      (value = mention_count, raw_payload.ticker = symbol)
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Iterable

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# ── Configuration constants ──────────────────────────────────────────

REDDIT_SUBREDDIT: str = "options"

DAILY_THREAD_QUERIES: tuple[str, ...] = (
    "daily discussion",
    "daily options discussion",
    "what are your moves tomorrow",
)

REDDIT_USER_AGENT: str = "grid-intelligence/0.1 (intelligence platform research)"

REDDIT_BASE_URL: str = "https://www.reddit.com"

_REQUEST_TIMEOUT: int = 30
_BACKFILL_DAYS: int = 5  # how many prior daily threads we are willing to scan

TOP_N_TICKERS: int = 20

# ── Sentiment token sets ─────────────────────────────────────────────
# Lowercase, word-boundary matched.

BULLISH_TOKENS: frozenset[str] = frozenset(
    {
        "calls",
        "long",
        "bullish",
        "buy",
        "moon",
        "yolo",
        "squeeze",
        "breakout",
    }
)

BEARISH_TOKENS: frozenset[str] = frozenset(
    {
        "puts",
        "short",
        "bearish",
        "sell",
        "crash",
        "dump",
        "top",
        "bear",
    }
)

ZERO_DTE_TOKENS: frozenset[str] = frozenset(
    {
        "0dte",
        "0 dte",
        "same day",
        "expire today",
        "friday puts",
        "monday calls",
    }
)

# ── Ticker extraction ────────────────────────────────────────────────
# Match $XYZ or bare XYZ (1-5 uppercase letters) as a whole word.

TICKER_PATTERN: re.Pattern[str] = re.compile(r"(?<![A-Za-z0-9])\$?([A-Z]{1,5})(?![A-Za-z0-9])")

# Common false positives — short uppercase tokens that look like tickers
# but are English words / acronyms / Reddit slang.  Keep this list tight;
# we would rather have a noisy ticker ranking than throw away a real ticker.
TICKER_FALSE_POSITIVES: frozenset[str] = frozenset(
    {
        "I", "ME", "MY", "A", "AN", "THE", "IT", "IS", "US", "EU", "UK",
        "IRS", "SEC", "FED", "ETC", "ETF", "IV", "HV", "OP", "PM", "AM",
        "PE", "EPS", "PS", "PR", "FDA", "GDP", "CPI", "YTD",
    }
)

# Sentiment tokens are matched word-boundary.  Multi-word tokens fall back to
# substring match inside a normalised whitespace string.
_WORD_BOUNDARY_CACHE: dict[str, re.Pattern[str]] = {}


def _word_boundary_regex(token: str) -> re.Pattern[str]:
    """Compile (and cache) a word-boundary regex for *token*.

    Multi-word tokens (e.g. "0 dte") cannot use \\b on both sides because
    digits / spaces break word boundaries — we use a non-alphanumeric
    look-around instead so that "0dte" inside a longer word does not match
    but "0 dte" inside a sentence does.
    """
    if token not in _WORD_BOUNDARY_CACHE:
        escaped = re.escape(token)
        _WORD_BOUNDARY_CACHE[token] = re.compile(
            r"(?<![A-Za-z0-9])" + escaped + r"(?![A-Za-z0-9])",
            flags=re.IGNORECASE,
        )
    return _WORD_BOUNDARY_CACHE[token]


# ── Data class ───────────────────────────────────────────────────────


@dataclass(frozen=True)
class RedditOptionsPulse:
    """A single day's snapshot of /r/options retail positioning.

    All counts are computed across the full comment tree of one daily
    discussion thread.  ``bull_bear_ratio`` is Laplace-smoothed and is
    therefore always strictly positive.
    """

    thread_date: date
    thread_id: str
    post_count: int
    comment_count: int
    unique_authors: int
    bullish_count: int
    bearish_count: int
    zero_dte_count: int
    bull_bear_ratio: float
    top_tickers: tuple[tuple[str, int], ...] = field(default_factory=tuple)
    thread_url: str = ""


# ── Pure helpers (testable in isolation) ─────────────────────────────


def extract_tickers(text: str) -> list[str]:
    """Extract candidate ticker symbols from *text*.

    Rules:
        * 1-5 uppercase ASCII letters as a whole word
        * Optional leading $ is consumed
        * Filtered against TICKER_FALSE_POSITIVES (English words, slang)

    Mixed-case tokens like ``Aapl`` are intentionally NOT matched — the
    1-5 *uppercase* rule is what prevents the regex from latching onto
    every capitalised word in the corpus.

    Parameters:
        text: Raw post / comment body text.

    Returns:
        List of ticker symbols, in order of first appearance.  Duplicates
        are preserved because callers usually want a Counter-style tally.
    """
    if not text:
        return []
    out: list[str] = []
    for match in TICKER_PATTERN.finditer(text):
        symbol = match.group(1)
        if symbol in TICKER_FALSE_POSITIVES:
            continue
        out.append(symbol)
    return out


def count_sentiment_tokens(text: str, token_set: frozenset[str]) -> int:
    """Count occurrences of any token from *token_set* in *text*.

    Lowercase, word-boundary safe (so ``recalls`` does NOT match
    ``calls``).  Multi-word tokens such as ``"0 dte"`` are handled by
    a non-alphanumeric look-around regex.

    Parameters:
        text: Raw text to scan.
        token_set: A frozenset of lowercase tokens.

    Returns:
        Total number of matches across all tokens (sum, not unique).
    """
    if not text or not token_set:
        return 0
    total = 0
    for token in token_set:
        pattern = _word_boundary_regex(token)
        total += len(pattern.findall(text))
    return total


def compute_bull_bear_ratio(bull: int, bear: int) -> float:
    """Laplace-smoothed bull / bear ratio.

    ``(bull + 1) / (bear + 1)``  — always strictly positive, never
    undefined, and decays toward 1.0 in the absence of evidence.

    Parameters:
        bull: Number of bullish token matches.
        bear: Number of bearish token matches.

    Returns:
        Floating-point ratio.  >1 means net-bullish, <1 means net-bearish.
    """
    return (float(bull) + 1.0) / (float(bear) + 1.0)


def _rank_tickers(
    ticker_counts: dict[str, int],
    top_n: int,
) -> list[tuple[str, int]]:
    """Return the top-``top_n`` tickers sorted by mention count desc.

    Tie-breaker: alphabetical (ascending).  If fewer than ``top_n``
    tickers are present, return all of them.

    Parameters:
        ticker_counts: Mapping of ticker symbol -> mention count.
        top_n: Maximum number of tickers to return.

    Returns:
        List of (ticker, count) tuples.
    """
    if not ticker_counts:
        return []
    items = sorted(ticker_counts.items(), key=lambda kv: (-kv[1], kv[0]))
    return items[: max(top_n, 0)]


def _flatten_comments(thread_json: dict[str, Any]) -> list[dict[str, Any]]:
    """Walk a Reddit comment-tree JSON response and return every comment.

    Reddit returns a thread fetch as a list of two Listings — the first
    contains the link/post, the second contains top-level comments.  Each
    comment can have nested ``replies`` which is itself a Listing.  This
    function recurses into ``replies`` and collects every comment record
    that has a ``body`` field.  ``more`` placeholders (``kind == "more"``)
    are skipped — they represent un-loaded comments that would require a
    second API call to fetch.  Skipping them is intentional because the
    daily discussion thread is fetched with ``limit=500`` and Reddit
    returns nearly all comments inline; the trailing "load more" tail is
    only a few percent of the volume.

    Parameters:
        thread_json: Decoded JSON returned by Reddit's
            ``/comments/<id>.json`` endpoint.  Either a list of two
            Listings (the real Reddit format) or a dict with a single
            "data" key (testing convenience).

    Returns:
        Flat list of comment dicts (each with at least a ``body`` key).
    """
    # Reddit real format: [post_listing, comments_listing]
    listings: list[dict[str, Any]] = []
    if isinstance(thread_json, list):
        listings = [item for item in thread_json if isinstance(item, dict)]
    elif isinstance(thread_json, dict):
        listings = [thread_json]
    else:
        return []

    out: list[dict[str, Any]] = []

    def _recurse(node: Any) -> None:
        if isinstance(node, dict):
            kind = node.get("kind")
            data = node.get("data")
            if kind == "more":
                return  # skip un-loaded comment placeholders
            if kind == "Listing" and isinstance(data, dict):
                children = data.get("children", [])
                if isinstance(children, list):
                    for child in children:
                        _recurse(child)
                return
            if kind == "t1" and isinstance(data, dict):
                # t1 is a Reddit Comment
                if "body" in data:
                    out.append(data)
                replies = data.get("replies")
                if isinstance(replies, dict):
                    _recurse(replies)
                elif isinstance(replies, list):
                    for r in replies:
                        _recurse(r)
                return
            # Bare comment dict (test convenience): {"body": "...", "replies": [...]}
            if "body" in node:
                out.append(node)
                replies = node.get("replies")
                if isinstance(replies, list):
                    for r in replies:
                        _recurse(r)
                elif isinstance(replies, dict):
                    _recurse(replies)
                return
            # Generic dict — recurse into every value
            for v in node.values():
                _recurse(v)
        elif isinstance(node, list):
            for item in node:
                _recurse(item)

    for listing in listings:
        _recurse(listing)

    return out


# ── Network helpers ──────────────────────────────────────────────────


def _http_get_json(
    url: str,
    user_agent: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any] | list[Any] | None:
    """Issue a single GET to Reddit and return parsed JSON.

    Returns ``None`` on any error (rate-limit, network, decode).
    """
    headers = {
        "User-Agent": user_agent,
        "Accept": "application/json",
    }
    try:
        resp = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=_REQUEST_TIMEOUT,
        )
    except requests.RequestException as exc:
        log.warning("reddit_options_pulse: request to {u} failed: {e}", u=url, e=str(exc))
        return None

    if resp.status_code == 429:
        log.warning("reddit_options_pulse: rate-limited (429) on {u}", u=url)
        return None
    if resp.status_code == 403:
        log.warning("reddit_options_pulse: forbidden (403) on {u}", u=url)
        return None
    if resp.status_code != 200:
        log.warning(
            "reddit_options_pulse: HTTP {s} on {u}",
            s=resp.status_code, u=url,
        )
        return None

    try:
        return resp.json()
    except ValueError as exc:
        log.warning("reddit_options_pulse: JSON decode failed: {e}", e=str(exc))
        return None


def _fetch_daily_thread(user_agent: str) -> dict[str, Any] | None:
    """Find and fetch the latest /r/options daily discussion thread.

    Two-step process:

        1. Hit the search endpoint with each query in DAILY_THREAD_QUERIES
           until we find a post whose title contains "daily discussion".
        2. Fetch that post's full comment tree via
           ``/r/options/comments/<id>.json?limit=500``.

    Returns:
        A dict shaped as ``{"post": <link_data>, "thread": <raw_thread_json>}``
        on success, or ``None`` on any failure path.  The shape is chosen so
        that downstream code can call ``_flatten_comments(d["thread"])`` and
        also read ``d["post"]`` for thread metadata (id, permalink, created).
    """
    for query in DAILY_THREAD_QUERIES:
        search_url = f"{REDDIT_BASE_URL}/r/{REDDIT_SUBREDDIT}/search.json"
        params = {
            "q": f'title:"{query}"',
            "restrict_sr": 1,
            "sort": "new",
            "limit": 10,
            "raw_json": 1,
        }
        search_payload = _http_get_json(search_url, user_agent, params=params)
        if not isinstance(search_payload, dict):
            continue

        children = (
            search_payload.get("data", {}).get("children", [])
            if isinstance(search_payload.get("data"), dict)
            else []
        )

        for child in children:
            if not isinstance(child, dict):
                continue
            data = child.get("data")
            if not isinstance(data, dict):
                continue
            title = (data.get("title") or "").lower()
            if "daily discussion" not in title and "moves tomorrow" not in title:
                continue

            permalink = data.get("permalink")
            if not permalink:
                continue

            thread_url = f"{REDDIT_BASE_URL}{permalink}.json"
            thread_payload = _http_get_json(
                thread_url, user_agent, params={"limit": 500, "raw_json": 1}
            )
            if thread_payload is None:
                continue

            return {"post": data, "thread": thread_payload}

    log.info("reddit_options_pulse: no daily-discussion thread found")
    return None


# ── Aggregation ──────────────────────────────────────────────────────


def _aggregate_pulse(
    post_data: dict[str, Any],
    comments: list[dict[str, Any]],
) -> RedditOptionsPulse:
    """Compute the full pulse snapshot from a post + its flat comment list."""
    thread_id = str(post_data.get("id") or "")
    permalink = post_data.get("permalink") or ""
    thread_url = f"{REDDIT_BASE_URL}{permalink}" if permalink else ""

    created_utc = post_data.get("created_utc")
    if isinstance(created_utc, (int, float)):
        try:
            thread_date = datetime.fromtimestamp(
                float(created_utc), tz=timezone.utc
            ).date()
        except (ValueError, OSError, OverflowError):
            thread_date = date.today()
    else:
        thread_date = date.today()

    bullish_total = 0
    bearish_total = 0
    zero_dte_total = 0
    ticker_counter: Counter[str] = Counter()
    authors: set[str] = set()

    # The post body itself counts too.
    post_body = post_data.get("selftext") or ""
    if post_body:
        bullish_total += count_sentiment_tokens(post_body, BULLISH_TOKENS)
        bearish_total += count_sentiment_tokens(post_body, BEARISH_TOKENS)
        zero_dte_total += count_sentiment_tokens(post_body, ZERO_DTE_TOKENS)
        for tk in extract_tickers(post_body):
            ticker_counter[tk] += 1

    for comment in comments:
        body = comment.get("body") or ""
        if not body:
            continue
        bullish_total += count_sentiment_tokens(body, BULLISH_TOKENS)
        bearish_total += count_sentiment_tokens(body, BEARISH_TOKENS)
        zero_dte_total += count_sentiment_tokens(body, ZERO_DTE_TOKENS)
        for tk in extract_tickers(body):
            ticker_counter[tk] += 1
        author = comment.get("author")
        if author and author not in ("[deleted]", "AutoModerator", ""):
            authors.add(str(author))

    top = _rank_tickers(dict(ticker_counter), TOP_N_TICKERS)

    return RedditOptionsPulse(
        thread_date=thread_date,
        thread_id=thread_id,
        post_count=1,
        comment_count=len(comments),
        unique_authors=len(authors),
        bullish_count=bullish_total,
        bearish_count=bearish_total,
        zero_dte_count=zero_dte_total,
        bull_bear_ratio=compute_bull_bear_ratio(bullish_total, bearish_total),
        top_tickers=tuple(top),
        thread_url=thread_url,
    )


# ── Puller class ─────────────────────────────────────────────────────


class RedditOptionsPulsePuller(BasePuller):
    """Daily puller for the /r/options daily-discussion pulse signal."""

    SOURCE_NAME = "reddit_options_pulse"
    SOURCE_CONFIG = {
        "base_url": "https://www.reddit.com/r/options/",
        "cost_tier": "FREE",
        "latency_class": "DAILY",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "MED",
        "priority_rank": 55,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        self._user_agent = REDDIT_USER_AGENT

    # ── Public API ───────────────────────────────────────────────────

    def pull(self) -> list[RedditOptionsPulse]:
        """Fetch the latest /r/options daily-discussion pulse(s).

        Returns:
            List of ``RedditOptionsPulse`` snapshots, newest first.  Empty
            if Reddit failed or no daily-discussion thread could be found.
        """
        try:
            payload = _fetch_daily_thread(self._user_agent)
        except Exception as exc:  # never crash the scheduler
            log.warning("reddit_options_pulse: fetch raised {e}", e=str(exc))
            return []

        if not payload:
            return []

        post_data = payload.get("post") if isinstance(payload, dict) else None
        thread_json = payload.get("thread") if isinstance(payload, dict) else None
        if not isinstance(post_data, dict) or thread_json is None:
            return []

        try:
            comments = _flatten_comments(thread_json)
            pulse = _aggregate_pulse(post_data, comments)
        except Exception as exc:
            log.warning("reddit_options_pulse: aggregation raised {e}", e=str(exc))
            return []

        return [pulse]

    def save_to_db(self, pulses: Iterable[RedditOptionsPulse]) -> int:
        """Upsert pulse snapshots into raw_series.

        Idempotent: if a series already has a row for the same
        ``thread_date`` it is not re-inserted.

        Parameters:
            pulses: Iterable of ``RedditOptionsPulse``.

        Returns:
            Total number of raw_series rows inserted.
        """
        pulses_list = list(pulses)
        if not pulses_list:
            return 0

        inserted = 0

        # Collect all series_ids touched so we can batch-load existing dates.
        series_ids: set[str] = {
            "reddit_options:comment_count",
            "reddit_options:unique_authors",
            "reddit_options:bull_bear_ratio",
            "reddit_options:zero_dte_count",
        }
        for rank in range(1, TOP_N_TICKERS + 1):
            series_ids.add(f"reddit_options:top_ticker_rank:{rank}")

        with self.engine.begin() as conn:
            existing: dict[str, set[date]] = {}
            for sid in series_ids:
                existing[sid] = self._get_existing_dates(sid, conn)

            for pulse in pulses_list:
                inserted += self._save_one(conn, pulse, existing)

        return inserted

    def _save_one(
        self,
        conn: Any,
        pulse: RedditOptionsPulse,
        existing: dict[str, set[date]],
    ) -> int:
        """Insert all series rows for a single pulse snapshot."""
        rows = 0
        d = pulse.thread_date

        scalar_series: list[tuple[str, float]] = [
            ("reddit_options:comment_count", float(pulse.comment_count)),
            ("reddit_options:unique_authors", float(pulse.unique_authors)),
            ("reddit_options:bull_bear_ratio", float(pulse.bull_bear_ratio)),
            ("reddit_options:zero_dte_count", float(pulse.zero_dte_count)),
        ]

        base_payload = {
            "thread_id": pulse.thread_id,
            "thread_url": pulse.thread_url,
            "post_count": pulse.post_count,
            "comment_count": pulse.comment_count,
            "unique_authors": pulse.unique_authors,
            "bullish_count": pulse.bullish_count,
            "bearish_count": pulse.bearish_count,
            "zero_dte_count": pulse.zero_dte_count,
            "bull_bear_ratio": pulse.bull_bear_ratio,
        }

        for sid, value in scalar_series:
            if d in existing.get(sid, set()):
                continue
            self._insert_raw(
                conn=conn,
                series_id=sid,
                obs_date=d,
                value=value,
                raw_payload=base_payload,
            )
            existing.setdefault(sid, set()).add(d)
            rows += 1

        for rank, (ticker, count) in enumerate(pulse.top_tickers, start=1):
            if rank > TOP_N_TICKERS:
                break
            sid = f"reddit_options:top_ticker_rank:{rank}"
            if d in existing.get(sid, set()):
                continue
            payload = dict(base_payload)
            payload["ticker"] = ticker
            payload["rank"] = rank
            payload["mention_count"] = count
            self._insert_raw(
                conn=conn,
                series_id=sid,
                obs_date=d,
                value=float(count),
                raw_payload=payload,
            )
            existing.setdefault(sid, set()).add(d)
            rows += 1

        return rows


# ── Scheduler entry point ────────────────────────────────────────────


def run_reddit_options_pulse_puller(engine: Engine) -> dict[str, Any]:
    """Scheduler entry point — fetch the latest pulse and persist it.

    Returns:
        A summary dict with at minimum::

            {
              "fetched":        int,
              "inserted":       int,
              "thread_date":    "YYYY-MM-DD" | None,
              "comment_count":  int | None,
              "bull_bear_ratio": float | None,
              "top_tickers":    list[[ticker, count]] | None,
            }
    """
    summary: dict[str, Any] = {
        "fetched": 0,
        "inserted": 0,
        "thread_date": None,
        "comment_count": None,
        "bull_bear_ratio": None,
        "top_tickers": None,
    }

    try:
        puller = RedditOptionsPulsePuller(engine)
    except Exception as exc:
        log.warning("reddit_options_pulse: puller init failed: {e}", e=str(exc))
        return summary

    try:
        pulses = puller.pull()
    except Exception as exc:
        log.warning("reddit_options_pulse: pull raised {e}", e=str(exc))
        return summary

    summary["fetched"] = len(pulses)
    if not pulses:
        return summary

    try:
        inserted = puller.save_to_db(pulses)
    except Exception as exc:
        log.warning("reddit_options_pulse: save raised {e}", e=str(exc))
        return summary

    summary["inserted"] = inserted
    head = pulses[0]
    summary["thread_date"] = head.thread_date.isoformat()
    summary["comment_count"] = head.comment_count
    summary["bull_bear_ratio"] = head.bull_bear_ratio
    summary["top_tickers"] = [list(t) for t in head.top_tickers]
    return summary


__all__ = [
    "REDDIT_SUBREDDIT",
    "DAILY_THREAD_QUERIES",
    "REDDIT_USER_AGENT",
    "BULLISH_TOKENS",
    "BEARISH_TOKENS",
    "ZERO_DTE_TOKENS",
    "TICKER_PATTERN",
    "TICKER_FALSE_POSITIVES",
    "TOP_N_TICKERS",
    "RedditOptionsPulse",
    "RedditOptionsPulsePuller",
    "extract_tickers",
    "count_sentiment_tokens",
    "compute_bull_bear_ratio",
    "_rank_tickers",
    "_flatten_comments",
    "_fetch_daily_thread",
    "_aggregate_pulse",
    "run_reddit_options_pulse_puller",
]
