"""
Social sentiment ingestor — Reddit, Bluesky, Google Trends.

Reads public social media for sentiment signals around tracked
tickers, themes, and product launches. No API keys or logins:

- Reddit posts: the newest posts of every tracked board from the Arctic
  Shift archive API (``ingestion/altdata/arctic_shift.py``, free, no key).
  Reddit itself answers logged-out ``.json`` with 403 and, since about
  17:50Z 2026-11-17, every ``.rss`` feed with 404, so the RSS feed this used
  (``reddit_rss.py``) is gone.
- Reddit-wide ticker mention counts from ApeWisdom's free public API
  (https://apewisdom.io/api/): mentions, upvotes and rank now vs 24h ago,
  aggregated across the stock and crypto subreddits.
- Bluesky post search via the AppView (``api.bsky.app``); the
  ``public.api.bsky.app`` cache host returns 403 for search.

Provides:
- Volume: how much is being discussed
- Sentiment: bullish/bearish/neutral ratio
- Momentum: is discussion accelerating or decelerating
- Notable posts: high-engagement posts worth reading

Used to gauge whether a product launch or earnings will beat/miss expectations
based on social buzz relative to historical patterns.

Honest outcomes (2026-09-29 write contract, fixed 2026-11-05): SmartScheduler
runs ``pull_all``, which used to return only the scan and never persisted it
(only the intelligence loop called ``save_to_db``), so every scheduled run was
recorded FAILED "puller did not report committed write count". ``pull_all``
now stores the day's scan and returns ``status`` plus the committed
``rows_inserted`` (1 for a new day row, 0 when today's row already exists).
A scan in which every Reddit and Bluesky request failed is FAILED with the
reason, never an empty success.
"""

from __future__ import annotations

import html
import json
import re
import time
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log

from ingestion.altdata import arctic_shift


# Tracked subreddits by theme
SUBREDDITS = {
    "market": ["wallstreetbets", "stocks", "investing", "options"],
    "crypto": ["cryptocurrency", "bitcoin", "ethereum", "solana"],
    "tech": ["technology", "artificial", "MachineLearning", "nvidia"],
    "macro": ["economics", "finance", "personalfinance"],
}

REDDIT_POSTS_PER_BOARD = 10  # newest posts per board (14 boards; RSS used to give 100 in all)
REDDIT_POSTS_WINDOW_H = 24
BLUESKY_SEARCH_URL = "https://api.bsky.app/xrpc/app.bsky.feed.searchPosts"
APEWISDOM_URL = "https://apewisdom.io/api/v1.0/filter/{filter}/page/1"
APEWISDOM_PAGE_URL = "https://apewisdom.io/api/v1.0/filter/{filter}/page/{page}"
APEWISDOM_FILTERS = {"stocks": "all-stocks", "crypto": "all-crypto"}

# Bluesky search terms
BLUESKY_TERMS = [
    "stock market", "SPY", "NVDA", "bitcoin", "recession",
    "fed rate", "earnings", "AI", "semiconductor",
]

# Simple sentiment keywords
BULLISH_WORDS = {
    "moon", "bull", "buy", "long", "calls", "rocket", "squeeze", "breakout",
    "undervalued", "growth", "beat", "surge", "rally", "pump", "green",
    "upgrade", "outperform", "strong", "boom", "soar",
}
BEARISH_WORDS = {
    "bear", "sell", "short", "puts", "crash", "dump", "overvalued",
    "recession", "miss", "tank", "plunge", "red", "downgrade",
    "underperform", "weak", "bust", "collapse", "drop", "fade",
}

# Default focus list for the daily scan (pull_all) and the per-ticker series.
DEFAULT_TARGET_TICKERS = [
    "SPY", "NVDA", "AAPL", "MSFT", "GOOGL", "META", "AMZN", "TSLA",
    "BTC", "ETH", "SOL", "AMD", "INTC", "AVGO",
]

# ── Per-ticker social series (pull_ticker_series, 2026-11-17) ───────────
#
# pull_all keeps one JSON scan per day; nothing numeric per ticker reached
# raw_series from Reddit or Bluesky. pull_ticker_series writes one number per
# (series, UTC fetch date), source row SocialSentiment:
#
#   BSKY:<T>:posts_per_hour     Bluesky posts carrying the cashtag $<T> per hour,
#                               over the span the search page covered (24 h, or
#                               less when the page filled before reaching back)
#   BSKY:<T>:posts_24h          the count itself, only when the page covered 24 h
#   BSKY:<T>:engagement_24h     likes + reposts + replies + quotes on them (same rule)
#   BSKY:<T>:finbert_compound   mean FinBERT (positive - negative) over up to
#                               FINBERT_MAX_TEXTS of them (>= FINBERT_MIN_TEXTS)
#   APEWISDOM:<board>:<T>:mentions_24h / :upvotes_24h
#                               ApeWisdom's 24 h Reddit counts per board
#   APEWISDOM:<board>:total_mentions_24h / :tickers_mentioned
#                               every ranked ticker on the board, all pages
#   ARCTICSHIFT:<sub>:posts_per_hour / :posts_24h
#                               posts created on the board in the 24 h before the fetch,
#                               counted from Arctic Shift (posts_24h only when the pages
#                               reached back 24 h; otherwise the rate over the span covered)
#   ARCTICSHIFT:<sub>:comments_per_hour / :comments_24h
#                               from one page of the board's newest comments (24 h count
#                               only when the page held the whole day)
#   ARCTICSHIFT:<sub>:<T>:posts_24h
#                               posts naming $<T> or <T> in capitals (complete boards only;
#                               zero is a measured zero there)
#   ARCTICSHIFT:<T>:finbert_compound
#                               mean FinBERT over those posts' title + body, all boards
#   TRADESTIE:wallstreetbets:top50_comments / :<T>:comments / :<T>:sentiment_score
#                               Tradestie's r/wallstreetbets top-50 tickers for the previous
#                               US/Eastern day (their comment counts and their own sentiment
#                               score, not FinBERT); stored once per Tradestie day
#   REDDIT:<sub>:subscribers / :active_users / :posts_24h / :comments_24h
#                               Reddit Data API (OAuth) only; see below
#
# PIT: obs_date is the UTC date of the fetch and pull_timestamp the fetch
# time, never a post's own (earlier, and author-set) timestamp. Bluesky posts
# are windowed on indexedAt, the time the AppView saw them.
#
# Reddit: www.reddit.com answers 403 to logged-out .json and, since about
# 17:50Z 2026-11-17, an Atom "Not Found" (HTTP 404) to every subreddit .rss
# (seen from grid-svr and from a second network). old.reddit.com redirects
# to login. The sanctioned route left is the Data API with an owner-registered
# app (ingestion/altdata/reddit_oauth.py); until REDDIT_CLIENT_ID/SECRET exist
# the REDDIT:* series are skipped with that reason and nothing is sent.
# ApeWisdom (https://apewisdom.io/api/, free, no key) still publishes per-board
# Reddit ticker counts. Raw posts and comments come from Arctic Shift
# (ingestion/altdata/arctic_shift.py), a free community archive API that was
# minutes behind Reddit on 2026-11-18, and per-ticker r/wallstreetbets comment
# sentiment from Tradestie's free API (https://tradestie.com/apps/reddit/api/,
# 20 requests/min anonymous; one request per run here). Candidates rejected the
# same day: PullPush (429 "does not provide free scraping resources for
# agents"), SwaggyStocks (no public API), Apify/RapidAPI wrappers (account).
SERIES_WATCHLIST_EXTRA = ["QQQ", "GLD", "TLT", "XLE", "XLF", "XLV"]  # StockTwits' list
SERIES_WINDOW_H = 24
BLUESKY_PAGE_LIMIT = 100
# One page: logged-out searchPosts answers 403 "Request forbidden by
# administrative rules" to any cursor (checked 2026-11-18 from grid-svr and a
# second network). Deeper pages would need an authenticated account; GRID does
# not work around the rule. A page that fills before reaching back 24 h is
# "capped": posts_per_hour then uses the span it did cover.
BLUESKY_MAX_PAGES = 1
BLUESKY_MIN_SPAN_H = 0.25       # a capped page covering less than this yields no rate
BLUESKY_REQUEST_GAP_S = 0.5     # AppView allows thousands of reads per 5 min per IP
APEWISDOM_BOARDS = ["wallstreetbets", "stocks", "investing", "options", "CryptoCurrency"]
APEWISDOM_MAX_PAGES = 15
APEWISDOM_REQUEST_GAP_S = 1.0
ARCTIC_BOARDS = ["wallstreetbets", "stocks", "investing", "options", "economics", "CryptoCurrency"]
ARCTIC_MAX_PAGES = 8            # 800 newest posts; r/wallstreetbets had ~420 in 24 h on 2026-11-18
ARCTIC_MIN_SPAN_H = 0.25        # a sample covering less than this yields no rate
ARCTIC_FINBERT_CHARS = 1000     # title + body characters scored per post
TRADESTIE_URL = "https://tradestie.com/api/v1/apps/reddit"
TRADESTIE_MAX_BYTES = 200_000   # 50 rows are ~4.5 KB
TRADESTIE_TZ = "America/New_York"
_TRADESTIE_TICKER_RE = re.compile(r"^[A-Z]{1,5}$")
REDDIT_API_SUBS = [
    "wallstreetbets", "stocks", "investing", "options", "economics",
    "cryptocurrency", "bitcoin",
]
REDDIT_NEW_MAX_PAGES = 3        # 300 newest posts per board
FINBERT_MIN_TEXTS = 3
FINBERT_MAX_TEXTS = 100         # newest matched posts scored per ticker
FINBERT_BATCH = 32
SERIES_TIME_BUDGET_S = 780.0    # used when no deadline_at is passed (SmartScheduler passes one)
SERIES_SCORE_RESERVE_S = 150.0  # stop fetching this long before the deadline: FinBERT + store


def _has_data(result: dict) -> bool:
    """True when the scan collected anything worth storing."""
    return bool(result.get("ticker_sentiment") or result.get("reddit")
                or result.get("reddit_mentions") or result.get("bluesky"))


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _source_id(conn: Any) -> int | None:
    """The SocialSentiment source_catalog id, creating the row on first use."""
    from sqlalchemy import text
    conn.execute(text(
        "INSERT INTO source_catalog "
        "(name, base_url, cost_tier, latency_class, pit_available, "
        "revision_behavior, trust_score, priority_rank) "
        "VALUES (:name, :url, :cost, :latency, :pit, :rev, :trust, :rank) "
        "ON CONFLICT (name) DO NOTHING"
    ), {
        "name": "SocialSentiment",
        "url": "https://reddit.com",
        "cost": "FREE",
        "latency": "EOD",
        "pit": False,
        "rev": "NEVER",
        "trust": "MED",
        "rank": 60,
    })
    src = conn.execute(text(
        "SELECT id FROM source_catalog WHERE name = 'SocialSentiment'"
    )).fetchone()
    return int(src[0]) if src else None


def _success_row_exists(conn: Any, series_id: str, source_id: int, obs_date: Any) -> bool:
    from sqlalchemy import text
    return conn.execute(text(
        "SELECT 1 FROM raw_series WHERE series_id = :sid AND source_id = :src "
        "AND obs_date = :d AND pull_status = 'SUCCESS' LIMIT 1"
    ), {"sid": series_id, "src": source_id, "d": obs_date}).fetchone() is not None


def series_watchlist() -> list[str]:
    """Tickers for the per-ticker series: pull_all's list plus StockTwits' (no dupes)."""
    out: list[str] = []
    for t in DEFAULT_TARGET_TICKERS + SERIES_WATCHLIST_EXTRA:
        if t not in out:
            out.append(t)
    return out


def _parse_ts(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def has_cashtag(text_: str, ticker: str) -> bool:
    """True when the text carries ``$<ticker>`` as a whole cashtag.

    Bluesky search drops the ``$``: a query for ``$KO`` returned 99 posts on
    2026-11-18, none of them about Coca-Cola. Only a literal cashtag counts.
    """
    return re.search(r"(?<![\w$])\$" + re.escape(ticker) + r"(?![\w])", text_ or "",
                     re.IGNORECASE) is not None


def mentions_ticker(text_: str, ticker: str) -> bool:
    """True for the cashtag ``$<ticker>`` (any case) or the bare symbol in capitals.

    Reddit posts often skip the ``$``; requiring capitals keeps words like
    "meta" or "sol" from counting.
    """
    return has_cashtag(text_, ticker) or re.search(
        r"(?<![A-Za-z0-9$])" + re.escape(ticker) + r"(?![A-Za-z0-9])", text_ or "") is not None


def _payload_dict(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    try:
        out = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return out if isinstance(out, dict) else {}


def _iso_z(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class SocialSentimentPuller:
    """Pull sentiment from Reddit, Bluesky, and Google Trends."""

    def __init__(self, db_engine: Any = None) -> None:
        self.engine = db_engine
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GRID/4.0 (research; grid.stepdad.finance)",
        })
        # Injection points for tests: texts -> FinBERT score dicts, and a
        # Reddit Data API client. None means "build the real one when needed".
        self._sentiment_scorer: Callable[[list[str]], list[dict]] | None = None
        self._reddit_client: Any = None

    def pull_all(self, tickers: list[str] | None = None) -> dict:
        """Pull sentiment from all sources.

        Parameters:
            tickers: Optional list of tickers to focus on.

        Returns:
            Dict with reddit, bluesky, trends, and composite scores.
        """
        result = {
            "date": date.today().isoformat(),
            "reddit": {},
            "reddit_mentions": {},
            "bluesky": [],
            "trends": {},
            "ticker_sentiment": {},
            "summary": "",
        }

        # Per-request failures: a failed request is lost coverage, not "no posts".
        failures: list[str] = []
        attempts = 0

        succeeded = 0

        # Reddit: newest posts of every tracked board (Arctic Shift, one request each)
        attempts += 1
        all_subs = [sub for subs in SUBREDDITS.values() for sub in subs]
        try:
            result["reddit"], board_failures = self._reddit_hot_all(all_subs)
            succeeded += 1
            if board_failures:
                failures.append(f"reddit: {len(board_failures)} of {len(all_subs)} boards "
                                f"failed: {board_failures[0]}")
        except Exception as exc:
            failures.append(f"reddit (arctic shift): {exc}")
            log.debug("Reddit via Arctic Shift failed: {e}", e=str(exc))

        # Reddit-wide ticker mention counts (ApeWisdom)
        for label, flt in APEWISDOM_FILTERS.items():
            attempts += 1
            try:
                result["reddit_mentions"][label] = self._reddit_mentions(flt)
                succeeded += 1
            except Exception as exc:
                failures.append(f"apewisdom {flt}: {exc}")
                log.debug("ApeWisdom {f} failed: {e}", f=flt, e=str(exc))

        # Bluesky
        for term in BLUESKY_TERMS[:5]:
            attempts += 1
            try:
                posts = self._bluesky_search(term)
                result["bluesky"].extend(posts)
                succeeded += 1
                time.sleep(1)
            except Exception as exc:
                failures.append(f"bluesky '{term}': {exc}")
                log.debug("Bluesky search '{t}' failed: {e}", t=term, e=str(exc))

        # Google Trends
        try:
            trends = self._google_trends(tickers or [
                "NVDA", "AAPL", "TSLA", "Bitcoin", "recession", "AI stocks",
            ])
            result["trends"] = trends
        except Exception as exc:
            log.debug("Google Trends failed: {e}", e=str(exc))

        # Score tickers
        all_text = self._collect_all_text(result)
        target_tickers = tickers or DEFAULT_TARGET_TICKERS

        reddit_wide = self._mentions_by_ticker(result["reddit_mentions"])
        for ticker in target_tickers:
            score = self._score_ticker(ticker, all_text)
            wide = reddit_wide.get(ticker)
            if wide:
                score["reddit_mentions_24h"] = wide["mentions"]
                score["reddit_mentions_prior_24h"] = wide["mentions_24h_ago"]
                score["reddit_rank"] = wide["rank"]
            if score["mentions"] > 0:
                result["ticker_sentiment"][ticker] = score

        # Summary
        total_mentions = sum(s["mentions"] for s in result["ticker_sentiment"].values())
        bullish_tickers = [t for t, s in result["ticker_sentiment"].items() if s["sentiment"] == "bullish"]
        bearish_tickers = [t for t, s in result["ticker_sentiment"].items() if s["sentiment"] == "bearish"]

        result["summary"] = (
            f"Social sentiment scan: {total_mentions} mentions across {len(result['ticker_sentiment'])} tickers. "
            f"Bullish: {', '.join(bullish_tickers[:5]) or 'none'}. "
            f"Bearish: {', '.join(bearish_tickers[:5]) or 'none'}."
        )
        top = result["reddit_mentions"].get("stocks", [])[:5]
        if top:
            result["summary"] += " Most-discussed on Reddit (24h): " + ", ".join(
                f"{r['ticker']} {r['mentions']}"
                + (f" (prev {r['mentions_24h_ago']})" if r["mentions_24h_ago"] is not None else "")
                for r in top
            ) + "."

        log.info(
            "Social sentiment: {m} mentions, {b} bullish, {br} bearish tickers",
            m=total_mentions, b=len(bullish_tickers), br=len(bearish_tickers),
        )
        return self._with_outcome(result, failures, attempts, succeeded)

    def _with_outcome(
        self, result: dict, failures: list[str], attempts: int, succeeded: int,
    ) -> dict:
        """Persist the scan and add the scheduler write-contract fields.

        ``rows_inserted`` is what the committed transaction wrote: 1 for a new
        day row, 0 when today's row already exists. Collection failures make
        the run PARTIAL (some sources answered) or FAILED (none did).
        """
        failure_note = (
            f"{len(failures)} of {attempts} social requests failed: {failures[0]}"
            if failures else None
        )
        if not self.engine:
            result.update(status="SKIPPED", rows_inserted=0,
                          skipped_reason="no database engine")
            return result
        if not succeeded:
            result.update(status="FAILED", rows_inserted=0,
                          error=failure_note or "no social source returned data")
            return result
        if not _has_data(result):
            # Requests answered with no posts: a completed check, nothing to store.
            result.update(status="PARTIAL" if failures else "SUCCESS", rows_inserted=0)
            if failure_note:
                result["error"] = failure_note
            return result
        try:
            rows = self._store(result)
        except Exception as exc:
            log.warning("Failed to save social sentiment: {e}", e=str(exc))
            result.update(status="FAILED", rows_inserted=0, error=f"store failed: {exc}")
            return result
        if rows is None:
            result.update(status="FAILED", rows_inserted=0,
                          error="SocialSentiment source_catalog row missing")
            return result
        result.update(status="PARTIAL" if failures else "SUCCESS", rows_inserted=rows)
        if failure_note:
            result["error"] = failure_note
        return result

    def _reddit_hot_all(
        self, subreddits: list[str], now: datetime | None = None,
    ) -> tuple[dict[str, list[dict]], list[str]]:
        """Newest posts of every board (Arctic Shift), keyed by board, and per-board failures.

        Arctic Shift has no "hot" ranking and settles vote counts only ~36 h
        after a post, so this is the newest ``REDDIT_POSTS_PER_BOARD`` posts of
        the last ``REDDIT_POSTS_WINDOW_H`` hours; ApeWisdom's per-ticker
        upvotes cover engagement. Raises when every board failed.
        """
        until = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        since = until - timedelta(hours=REDDIT_POSTS_WINDOW_H)
        posts: dict[str, list[dict]] = {}
        failures: list[str] = []
        for sub in subreddits:
            try:
                rows, _ = arctic_shift.recent_posts(
                    sub, since=since, until=until, max_pages=1,
                    page_limit=REDDIT_POSTS_PER_BOARD, session=self._session)
            except Exception as exc:
                failures.append(f"reddit r/{sub} (arctic shift): {exc}")
                continue
            for p in rows:
                posts.setdefault(sub, []).append({
                    "title": p["title"],
                    "text": p["selftext"][:2000],
                    "created": datetime.fromtimestamp(p["created_utc"], tz=timezone.utc).isoformat(),
                    "subreddit": sub,
                    "id": f"t3_{p['id']}",
                })
        if subreddits and len(failures) == len(subreddits):
            raise RuntimeError(failures[0])
        return posts, failures

    def _reddit_mentions(self, api_filter: str) -> list[dict]:
        """Top Reddit tickers for one ApeWisdom filter (page 1, ranked by mentions)."""
        resp = self._session.get(APEWISDOM_URL.format(filter=api_filter), timeout=15)
        resp.raise_for_status()
        return self._apewisdom_rows(resp.json())

    @staticmethod
    def _apewisdom_rows(payload: Any) -> list[dict]:
        """Normalized rows of one ApeWisdom results page."""
        rows = []
        for r in (payload or {}).get("results", []) or []:
            ticker = str(r.get("ticker") or "").upper()
            if not ticker:
                continue
            rows.append({
                "ticker": ticker.removesuffix(".X"),  # crypto rows are "BTC.X"
                "name": html.unescape(str(r.get("name") or "")),
                "rank": _int_or_none(r.get("rank")),
                "mentions": _int_or_none(r.get("mentions")) or 0,
                "upvotes": _int_or_none(r.get("upvotes")) or 0,
                "rank_24h_ago": _int_or_none(r.get("rank_24h_ago")),
                "mentions_24h_ago": _int_or_none(r.get("mentions_24h_ago")),
            })
        return rows

    @staticmethod
    def _mentions_by_ticker(reddit_mentions: dict[str, list[dict]]) -> dict[str, dict]:
        """First (highest-ranked) ApeWisdom row per ticker across filters."""
        by_ticker: dict[str, dict] = {}
        for rows in reddit_mentions.values():
            for row in rows:
                by_ticker.setdefault(row["ticker"], row)
        return by_ticker

    def _bluesky_search(self, query: str, limit: int = 25) -> list[dict]:
        """Search Bluesky posts through the public AppView."""
        params = {"q": query, "limit": limit, "sort": "latest"}
        resp = self._session.get(BLUESKY_SEARCH_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        posts = []
        for post in data.get("posts", []):
            record = post.get("record", {})
            posts.append({
                "text": record.get("text", ""),
                "likes": post.get("likeCount", 0),
                "reposts": post.get("repostCount", 0),
                "replies": post.get("replyCount", 0),
                "created": record.get("createdAt", ""),
                "source": "bluesky",
            })
        return posts

    def _google_trends(self, keywords: list[str]) -> dict:
        """Fetch Google Trends interest over past 7 days for keywords."""
        try:
            from pytrends.request import TrendReq
            pytrends = TrendReq(hl='en-US', tz=360, timeout=(10, 25))

            # Google Trends allows max 5 keywords at once
            batch = keywords[:5]
            pytrends.build_payload(batch, timeframe='now 7-d')
            interest = pytrends.interest_over_time()

            if interest.empty:
                return {}

            result = {}
            for kw in batch:
                if kw in interest.columns:
                    vals = interest[kw].tolist()
                    current = vals[-1] if vals else 0
                    avg = sum(vals) / len(vals) if vals else 0
                    trend = "rising" if current > avg * 1.2 else "falling" if current < avg * 0.8 else "stable"
                    result[kw] = {
                        "current": int(current),
                        "avg_7d": round(avg, 1),
                        "trend": trend,
                        "peak": int(max(vals)) if vals else 0,
                    }

            log.info("Google Trends: {n} keywords tracked", n=len(result))
            return result
        except ImportError:
            log.debug("pytrends not installed")
            return {}
        except Exception as exc:
            log.debug("Google Trends error: {e}", e=str(exc))
            return {}

    def _collect_all_text(self, result: dict) -> str:
        """Collect all post text into one string for analysis."""
        parts = []
        for sub, posts in result.get("reddit", {}).items():
            for p in posts:
                parts.append(p.get("title", ""))
                parts.append(p.get("text", ""))
        for p in result.get("bluesky", []):
            parts.append(p.get("text", ""))
        return " ".join(parts).lower()

    def _score_ticker(self, ticker: str, all_text: str) -> dict:
        """Score a ticker's social sentiment."""
        tk_lower = ticker.lower()
        # Count mentions (case-insensitive, word boundary)
        mentions = len(re.findall(r'\b' + re.escape(tk_lower) + r'\b', all_text))
        if ticker.startswith("$"):
            mentions += len(re.findall(r'\$' + re.escape(tk_lower[1:]) + r'\b', all_text))

        # Find context around mentions
        bullish_count = 0
        bearish_count = 0
        for match in re.finditer(r'\b' + re.escape(tk_lower) + r'\b', all_text):
            start = max(0, match.start() - 100)
            end = min(len(all_text), match.end() + 100)
            context = all_text[start:end]
            bullish_count += sum(1 for w in BULLISH_WORDS if w in context)
            bearish_count += sum(1 for w in BEARISH_WORDS if w in context)

        total_signal = bullish_count + bearish_count
        if total_signal > 0:
            bull_ratio = bullish_count / total_signal
            sentiment = "bullish" if bull_ratio > 0.6 else "bearish" if bull_ratio < 0.4 else "neutral"
        else:
            sentiment = "neutral"
            bull_ratio = 0.5

        return {
            "mentions": mentions,
            "bullish_signals": bullish_count,
            "bearish_signals": bearish_count,
            "sentiment": sentiment,
            "bull_ratio": round(bull_ratio, 3),
        }

    def save_to_db(self, result: dict) -> bool:
        """Append the day's sentiment scan to raw_series (one SUCCESS row per day).

        Nothing is written when the scan collected nothing at all (every
        source failed): that is not an observation of "0 tickers mentioned".
        A day that already has a SUCCESS row is left alone -- raw_series is
        append-only, never rewritten. True when the day's row is stored (now
        or earlier).
        """
        if not self.engine:
            return False
        try:
            return self._store(result) is not None
        except Exception as exc:
            log.warning("Failed to save social sentiment: {e}", e=str(exc))
            return False

    def _store(self, result: dict) -> int | None:
        """Insert the day's scan row; rows committed (1 new, 0 already there).

        None when nothing was collected or the source row cannot be resolved.
        Exceptions propagate (the transaction rolled back, nothing committed).
        """
        if not _has_data(result):
            return None
        from sqlalchemy import text
        scan = {k: v for k, v in result.items()
                if k not in ("status", "rows_inserted", "error", "skipped_reason")}
        with self.engine.begin() as conn:
            src = _source_id(conn)
            if src is None:
                return None
            series_id = f"social_sentiment_{scan['date']}"
            if _success_row_exists(conn, series_id, src, scan["date"]):
                return 0
            # pull_timestamp: schema DEFAULT NOW().
            conn.execute(text(
                "INSERT INTO raw_series (series_id, source_id, obs_date, value, raw_payload, pull_status) "
                "VALUES (:sid, :src, :d, :v, :payload, 'SUCCESS')"
            ), {
                "sid": series_id,
                "src": src,
                "d": scan["date"],
                "v": len(scan.get("ticker_sentiment", {})),
                "payload": json.dumps(scan, default=str),
            })
        # Reached only after the transaction committed.
        return 1

    # ── Per-ticker series (see the module constants above) ─────────────

    def pull_ticker_series(self, tickers: list[str] | None = None,
                           now: datetime | None = None,
                           deadline_at: float | None = None) -> dict:
        """Store today's per-ticker Bluesky, ApeWisdom and Reddit API series.

        One row per (series, UTC fetch date). A ticker or board whose rows for
        today already exist is not fetched again, so a second run in the same
        UTC day only fills what the first one missed. ``deadline_at`` is the
        SmartScheduler's cooperative ``time.monotonic()`` deadline; fetching
        stops SERIES_SCORE_RESERVE_S before it. Returns the scheduler write
        contract: ``status``, committed ``rows_inserted``, ``error``.
        """
        fetched_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        obs = fetched_at.date()
        end = deadline_at if deadline_at is not None else time.monotonic() + SERIES_TIME_BUDGET_S
        deadline = end - SERIES_SCORE_RESERVE_S
        watch: list[str] = []
        for t in tickers or series_watchlist():
            t = str(t).upper().removesuffix(".X")
            if t and t not in watch:
                watch.append(t)
        result: dict[str, Any] = {
            "date": obs.isoformat(), "fetched_at": fetched_at.isoformat(),
            "bluesky": {}, "apewisdom": {}, "arcticshift": {}, "tradestie": {},
            "reddit": {}, "already_stored": [],
        }
        if not self.engine:
            result.update(status="SKIPPED", rows_inserted=0,
                          skipped_reason="no database engine")
            return result
        with self.engine.begin() as conn:
            src = _source_id(conn)
        if src is None:
            result.update(status="FAILED", rows_inserted=0,
                          error="SocialSentiment source_catalog row missing")
            return result

        failures: list[str] = []
        attempts = succeeded = 0
        rows: list[tuple[str, float, dict]] = []
        window = {"window_start": _iso_z(fetched_at - timedelta(hours=SERIES_WINDOW_H)),
                  "window_end": _iso_z(fetched_at)}

        # Bluesky cashtags
        fetched: list[tuple[str, list[dict], dict]] = []
        for t in watch:
            if time.monotonic() > deadline:
                failures.append(f"time budget spent before bluesky ${t}")
                break
            if self._has_row(f"BSKY:{t}:posts_per_hour", src, obs):
                result["already_stored"].append(f"bluesky:{t}")
                continue
            attempts += 1
            try:
                posts, meta = self._bluesky_cashtag(t, fetched_at)
            except Exception as exc:
                failures.append(f"bluesky ${t}: {exc}")
                continue
            succeeded += 1
            fetched.append((t, posts, meta))
            result["bluesky"][t] = {"posts_24h": len(posts), **meta}

        scores, finbert_error = self._finbert_by_ticker(fetched)
        if finbert_error:
            failures.append(f"finbert: {finbert_error}")
        for t, posts, meta in fetched:
            payload = {"query": f"${t}", "basis": "indexedAt", **window, **meta}
            if meta["covered_hours"] >= BLUESKY_MIN_SPAN_H:
                rows.append((f"BSKY:{t}:posts_per_hour",
                             round(len(posts) / meta["covered_hours"], 6), payload))
            if not meta["capped"]:
                rows.append((f"BSKY:{t}:posts_24h", float(len(posts)), payload))
                rows.append((f"BSKY:{t}:engagement_24h",
                             float(sum(p["engagement"] for p in posts)), payload))
            if t in scores:
                result["bluesky"][t]["finbert_compound"] = scores[t]["compound"]
                rows.append((f"BSKY:{t}:finbert_compound", scores[t]["compound"],
                             {**payload, "model": "ProsusAI/finbert",
                              "n_texts": scores[t]["n_texts"]}))

        # Reddit per-board ticker counts via ApeWisdom
        for board in APEWISDOM_BOARDS:
            if time.monotonic() > deadline:
                failures.append(f"time budget spent before apewisdom {board}")
                break
            if self._has_row(f"APEWISDOM:{board}:total_mentions_24h", src, obs):
                result["already_stored"].append(f"apewisdom:{board}")
                continue
            attempts += 1
            try:
                board_rows, complete = self._apewisdom_board(board)
            except Exception as exc:
                failures.append(f"apewisdom {board}: {exc}")
                continue
            succeeded += 1
            result["apewisdom"][board] = {"tickers": len(board_rows), "complete": complete}
            base = {"board": board, "fetched_at": fetched_at.isoformat()}
            if complete:
                rows.append((f"APEWISDOM:{board}:total_mentions_24h",
                             float(sum(r["mentions"] for r in board_rows)), base))
                rows.append((f"APEWISDOM:{board}:tickers_mentioned",
                             float(len(board_rows)), base))
            by_ticker: dict[str, dict] = {}
            for r in board_rows:
                by_ticker.setdefault(r["ticker"], r)
            for t in watch:
                r = by_ticker.get(t)
                if r is None:
                    continue  # unranked is not a measured zero
                extra = {**base, "rank": r["rank"], "rank_24h_ago": r["rank_24h_ago"],
                         "mentions_24h_ago": r["mentions_24h_ago"]}
                rows.append((f"APEWISDOM:{board}:{t}:mentions_24h", float(r["mentions"]), extra))
                rows.append((f"APEWISDOM:{board}:{t}:upvotes_24h", float(r["upvotes"]), extra))

        # Reddit posts and comments via Arctic Shift (free archive API, no key)
        since = fetched_at - timedelta(hours=SERIES_WINDOW_H)
        arctic_texts: dict[str, list[tuple[float, str]]] = {t: [] for t in watch}
        arctic_boards: list[str] = []
        for board in ARCTIC_BOARDS:
            if time.monotonic() > deadline:
                failures.append(f"time budget spent before arctic shift r/{board}")
                break
            if self._has_row(f"ARCTICSHIFT:{board}:posts_per_hour", src, obs):
                result["already_stored"].append(f"arcticshift:{board}")
                continue
            attempts += 1
            try:
                posts, complete = arctic_shift.recent_posts(
                    board, since=since, until=fetched_at, max_pages=ARCTIC_MAX_PAGES,
                    session=self._session, max_wait_s=self._wait_budget(deadline))
            except Exception as exc:
                failures.append(f"arctic shift r/{board} posts: {exc}")
                continue
            succeeded += 1
            arctic_boards.append(board)
            base = {"subreddit": board, "basis": "created_utc", "source": "arctic-shift",
                    **window, "complete": complete}
            covered = float(SERIES_WINDOW_H)
            if not complete:
                oldest = min((p["created_utc"] for p in posts), default=fetched_at.timestamp())
                covered = round((fetched_at.timestamp() - oldest) / 3600.0, 4)
            if covered >= ARCTIC_MIN_SPAN_H:
                rows.append((f"ARCTICSHIFT:{board}:posts_per_hour",
                             round(len(posts) / covered, 6), {**base, "covered_hours": covered}))
            if complete:
                rows.append((f"ARCTICSHIFT:{board}:posts_24h", float(len(posts)), base))
            for t in watch:
                hits = [p for p in posts if mentions_ticker(f"{p['title']} {p['selftext']}", t)]
                arctic_texts[t].extend(
                    (p["created_utc"], f"{p['title']}. {p['selftext']}"[:ARCTIC_FINBERT_CHARS])
                    for p in hits)
                if complete:
                    rows.append((f"ARCTICSHIFT:{board}:{t}:posts_24h", float(len(hits)), base))
            info: dict[str, Any] = {"posts": len(posts), "complete": complete}
            attempts += 1
            try:
                comments, c_complete = arctic_shift.newest_comments(
                    board, since=since, until=fetched_at,
                    session=self._session, max_wait_s=self._wait_budget(deadline))
            except Exception as exc:
                failures.append(f"arctic shift r/{board} comments: {exc}")
            else:
                succeeded += 1
                info["comments_sample"] = len(comments)
                c_base = {**base, "complete": c_complete, "sample": len(comments)}
                if c_complete:
                    rows.append((f"ARCTICSHIFT:{board}:comments_24h", float(len(comments)), c_base))
                    rows.append((f"ARCTICSHIFT:{board}:comments_per_hour",
                                 round(len(comments) / SERIES_WINDOW_H, 6), c_base))
                elif comments:
                    span = (fetched_at.timestamp() - min(c["created_utc"] for c in comments)) / 3600.0
                    if span >= ARCTIC_MIN_SPAN_H:
                        rows.append((f"ARCTICSHIFT:{board}:comments_per_hour",
                                     round(len(comments) / span, 6),
                                     {**c_base, "covered_hours": round(span, 4)}))
            result["arcticshift"][board] = info

        if arctic_boards:
            texts = [(t, [{"text": txt} for _, txt in sorted(v, key=lambda x: -x[0])], {})
                     for t, v in arctic_texts.items()]
            a_scores, a_error = self._finbert_by_ticker(texts)
            if a_error:
                failures.append(f"finbert (arctic shift): {a_error}")
            for t, sc in a_scores.items():
                rows.append((f"ARCTICSHIFT:{t}:finbert_compound", sc["compound"],
                             {**window, "boards": arctic_boards, "model": "ProsusAI/finbert",
                              "n_texts": sc["n_texts"], "source": "arctic-shift"}))

        # Tradestie: r/wallstreetbets top-50 tickers, previous US/Eastern day
        if time.monotonic() > deadline:
            failures.append("time budget spent before tradestie")
        elif self._has_row("TRADESTIE:wallstreetbets:top50_comments", src, obs):
            result["already_stored"].append("tradestie")
        else:
            day = self._tradestie_day(fetched_at)
            attempts += 1
            try:
                t_rows = self._tradestie(day)
            except Exception as exc:
                failures.append(f"tradestie {day.isoformat()}: {exc}")
            else:
                succeeded += 1
                result["tradestie"] = {"date": day.isoformat(), "tickers": len(t_rows)}
                if t_rows and self._last_tradestie_day(src) != day.isoformat():
                    t_base = {"tradestie_date": day.isoformat(), "subreddit": "wallstreetbets",
                              "fetched_at": fetched_at.isoformat(),
                              "scope": "Tradestie top 50 tickers by comments, US/Eastern day"}
                    rows.append(("TRADESTIE:wallstreetbets:top50_comments",
                                 float(sum(r["comments"] for r in t_rows)),
                                 {**t_base, "tickers": len(t_rows)}))
                    by_t = {}
                    for r in t_rows:
                        by_t.setdefault(r["ticker"], r)
                    for t in watch:
                        r = by_t.get(t)
                        if r is None:
                            continue  # outside the top 50 is not a measured zero
                        extra = {**t_base, "rank": r["rank"], "sentiment": r["sentiment"]}
                        rows.append((f"TRADESTIE:wallstreetbets:{t}:comments",
                                     float(r["comments"]), extra))
                        if r["score"] is not None:
                            rows.append((f"TRADESTIE:wallstreetbets:{t}:sentiment_score",
                                         r["score"], extra))

        # Reddit Data API (owner-registered app only)
        client, reason = self._reddit_api_client()
        if client is None:
            result["reddit"] = {"skipped": reason}
        else:
            for sub in REDDIT_API_SUBS:
                if time.monotonic() > deadline:
                    failures.append(f"time budget spent before reddit r/{sub}")
                    break
                if self._has_row(f"REDDIT:{sub}:subscribers", src, obs):
                    result["already_stored"].append(f"reddit:{sub}")
                    continue
                attempts += 1
                try:
                    values = self._reddit_api_board(client, sub, fetched_at)
                except Exception as exc:
                    failures.append(f"reddit api r/{sub}: {exc}")
                    continue
                succeeded += 1
                result["reddit"][sub] = values
                for field, value in values.items():
                    rows.append((f"REDDIT:{sub}:{field}", float(value),
                                 {"subreddit": sub, **window}))

        failure_note = (f"{len(failures)} social series problems: {'; '.join(failures[:3])}"
                        if failures else None)
        if attempts and not succeeded:
            result.update(status="FAILED", rows_inserted=0,
                          error=failure_note or "no social source answered")
            return result
        try:
            inserted = self._store_series(rows, src, obs)
        except Exception as exc:
            log.warning("Failed to store social ticker series: {e}", e=str(exc))
            result.update(status="FAILED", rows_inserted=0, error=f"store failed: {exc}")
            return result
        result.update(status="PARTIAL" if failures else "SUCCESS", rows_inserted=inserted)
        if failure_note:
            result["error"] = failure_note
        log.info(
            "Social ticker series: {n} rows ({b} Bluesky tickers, {a} ApeWisdom boards, "
            "{s} Arctic Shift boards, Tradestie {td}, reddit api: {r}){f}",
            n=inserted, b=len(result["bluesky"]), a=len(result["apewisdom"]),
            s=len(result["arcticshift"]), td=result["tradestie"].get("date", "-"),
            r=result["reddit"].get("skipped") or f"{len(result['reddit'])} boards",
            f=f"; {failure_note}" if failure_note else "",
        )
        return result

    @staticmethod
    def _wait_budget(deadline: float) -> float:
        """Seconds an Arctic Shift call may wait for its slot before the fetch deadline."""
        return max(1.0, min(arctic_shift.DEFAULT_MAX_WAIT_S, deadline - time.monotonic()))

    @staticmethod
    def _tradestie_day(fetched_at: datetime) -> date:
        """The US/Eastern calendar day before the fetch: a finished Tradestie day."""
        from zoneinfo import ZoneInfo
        return fetched_at.astimezone(ZoneInfo(TRADESTIE_TZ)).date() - timedelta(days=1)

    def _tradestie(self, day: date) -> list[dict]:
        """Tradestie's r/wallstreetbets rows for ``day``, validated; [] when none were published.

        Weekends and holidays answer an empty list. Redirects are refused and
        the body is capped; a row with an odd ticker or count is dropped.
        """
        resp = self._session.get(TRADESTIE_URL, params={"date": day.isoformat()}, timeout=15,
                                 allow_redirects=False, stream=True,
                                 headers={"Accept": "application/json"})
        try:
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code}")
            data = arctic_shift.read_json_bounded(resp, TRADESTIE_MAX_BYTES)
        finally:
            close = getattr(resp, "close", None)
            if callable(close):
                close()
        if not isinstance(data, list):
            raise ValueError("answer is not a list")
        rows: list[dict] = []
        for rank, r in enumerate(data[:100], start=1):
            if not isinstance(r, dict):
                continue
            ticker = r.get("ticker")
            comments = r.get("no_of_comments")
            if (not isinstance(ticker, str) or not _TRADESTIE_TICKER_RE.match(ticker)
                    or isinstance(comments, bool) or not isinstance(comments, int) or comments < 0):
                continue
            score = r.get("sentiment_score")
            if isinstance(score, bool) or not isinstance(score, (int, float)) \
                    or not -1.0 <= float(score) <= 1.0:
                score = None
            sentiment = r.get("sentiment")
            rows.append({"ticker": ticker, "rank": rank, "comments": comments,
                         "score": None if score is None else float(score),
                         "sentiment": sentiment if sentiment in ("Bullish", "Bearish") else ""})
        return rows

    def _last_tradestie_day(self, source_id: int) -> str | None:
        """The Tradestie day of the newest stored top50 row (one row per Tradestie day)."""
        from sqlalchemy import text
        with self.engine.connect() as conn:
            row = conn.execute(text(
                "SELECT raw_payload FROM raw_series WHERE series_id = :sid AND source_id = :src "
                "AND pull_status = 'SUCCESS' ORDER BY obs_date DESC LIMIT 1"
            ), {"sid": "TRADESTIE:wallstreetbets:top50_comments", "src": source_id}).fetchone()
        return _payload_dict(row[0]).get("tradestie_date") if row else None

    def _has_row(self, series_id: str, source_id: int, obs_date: date) -> bool:
        with self.engine.connect() as conn:
            return _success_row_exists(conn, series_id, source_id, obs_date)

    def _store_series(self, rows: list[tuple[str, float, dict]],
                      source_id: int, obs_date: date) -> int:
        """Insert rows not yet stored for ``obs_date``; the count committed."""
        from sqlalchemy import text
        inserted = 0
        with self.engine.begin() as conn:
            for series_id, value, payload in rows:
                if _success_row_exists(conn, series_id, source_id, obs_date):
                    continue
                # pull_timestamp: schema DEFAULT NOW() -- the fetch, never a post time.
                conn.execute(text(
                    "INSERT INTO raw_series (series_id, source_id, obs_date, value, "
                    "raw_payload, pull_status) "
                    "VALUES (:sid, :src, :d, :v, :payload, 'SUCCESS')"
                ), {"sid": series_id, "src": source_id, "d": obs_date, "v": value,
                    "payload": json.dumps(payload, default=str)})
                inserted += 1
        return inserted

    def _bluesky_cashtag(self, ticker: str, fetched_at: datetime) -> tuple[list[dict], dict]:
        """Posts with the cashtag ``$<ticker>`` indexed in the window before ``fetched_at``.

        Pages newest-first until a page reaches back past the window start.
        ``capped`` means the pages ran out first (BLUESKY_MAX_PAGES, or a 403 on
        a cursor page): ``covered_hours`` is then the span from the oldest
        result seen to the fetch, and only that span was counted.
        """
        since = fetched_at - timedelta(hours=SERIES_WINDOW_H)
        params: dict[str, Any] = {"q": f"${ticker}", "limit": BLUESKY_PAGE_LIMIT,
                                  "sort": "latest", "since": _iso_z(since),
                                  "until": _iso_z(fetched_at)}
        found: dict[str, dict] = {}
        pages = seen = 0
        capped = refused = False
        oldest_seen: datetime | None = None
        while True:
            resp = self._session.get(BLUESKY_SEARCH_URL, params=dict(params), timeout=15)
            if pages and resp.status_code == 403:
                capped = refused = True  # cursor pages refused; keep what came
                break
            resp.raise_for_status()
            data = resp.json() or {}
            pages += 1
            batch = data.get("posts") or []
            seen += len(batch)
            oldest: datetime | None = None
            for post in batch:
                indexed = _parse_ts(post.get("indexedAt"))
                if indexed is None:
                    continue
                oldest = indexed if oldest is None or indexed < oldest else oldest
                if oldest_seen is None or indexed < oldest_seen:
                    oldest_seen = indexed
                if not since <= indexed <= fetched_at:
                    continue
                body = (post.get("record") or {}).get("text") or ""
                if not has_cashtag(body, ticker):
                    continue
                uri = str(post.get("uri") or f"{ticker}:{len(found)}")
                found.setdefault(uri, {
                    "uri": uri,
                    "text": body,
                    "indexed_at": indexed,
                    "engagement": sum(_int_or_none(post.get(k)) or 0 for k in (
                        "likeCount", "repostCount", "replyCount", "quoteCount")),
                })
            cursor = data.get("cursor")
            if not cursor or not batch or (oldest is not None and oldest < since):
                break
            if pages >= BLUESKY_MAX_PAGES:
                capped = True
                break
            params["cursor"] = cursor
            time.sleep(BLUESKY_REQUEST_GAP_S)
        posts = sorted(found.values(), key=lambda p: p["indexed_at"], reverse=True)
        covered = float(SERIES_WINDOW_H)
        if capped:
            start = max(oldest_seen or fetched_at, since)
            covered = round((fetched_at - start).total_seconds() / 3600.0, 4)
        meta = {"pages": pages, "results_seen": seen, "capped": capped,
                "covered_hours": covered}
        if refused:
            meta["cursor_refused"] = True
        return posts, meta

    def _finbert_by_ticker(
        self, fetched: list[tuple[str, list[dict], dict]],
    ) -> tuple[dict[str, dict], str | None]:
        """Mean FinBERT compound per ticker; the error that stopped scoring, if any.

        Local model only (ingestion/ml/finbert_scorer.py, device from
        FINBERT_DEVICE as for the daily scorer); no LLM, paid or local.
        """
        work = {t: [p["text"] for p in posts[:FINBERT_MAX_TEXTS]]
                for t, posts, _ in fetched if len(posts) >= FINBERT_MIN_TEXTS}
        out: dict[str, dict] = {}
        if not work:
            return out, None
        try:
            score = self._sentiment_scorer or self._load_finbert()
            for t, texts in work.items():
                scored: list[dict] = []
                for i in range(0, len(texts), FINBERT_BATCH):
                    scored.extend(score(texts[i:i + FINBERT_BATCH]))
                if len(scored) != len(texts):
                    raise RuntimeError(f"{len(scored)} scores for {len(texts)} texts")
                compound = sum(float(s["positive"]) - float(s["negative"]) for s in scored)
                out[t] = {"compound": round(compound / len(scored), 6), "n_texts": len(scored)}
        except Exception as exc:
            return out, str(exc) or type(exc).__name__
        return out, None

    def _load_finbert(self) -> Callable[[list[str]], list[dict]]:
        from ingestion.ml.finbert_scorer import FinBERTScorer
        scorer = FinBERTScorer(db_engine=self.engine)
        self._sentiment_scorer = scorer.score_batch
        return scorer.score_batch

    def _apewisdom_board(self, board: str) -> tuple[list[dict], bool]:
        """Every ranked ticker on one ApeWisdom board; ``complete`` when all pages came."""
        rows: list[dict] = []
        page = 1
        while True:
            resp = self._session.get(APEWISDOM_PAGE_URL.format(filter=board, page=page),
                                     timeout=15)
            resp.raise_for_status()
            data = resp.json() or {}
            rows.extend(self._apewisdom_rows(data))
            pages = _int_or_none(data.get("pages")) or 1
            if page >= pages:
                return rows, True
            if page >= APEWISDOM_MAX_PAGES:
                return rows, False
            page += 1
            time.sleep(APEWISDOM_REQUEST_GAP_S)

    def _reddit_api_client(self) -> tuple[Any, str | None]:
        """The Data API client, or None and why (nothing is sent without one)."""
        if self._reddit_client is not None:
            return self._reddit_client, None
        from ingestion.altdata import reddit_oauth
        if not reddit_oauth.reddit_credentials_configured():
            return None, ("reddit api not configured (REDDIT_CLIENT_ID/REDDIT_CLIENT_SECRET "
                          "absent; logged-out .json is 403 and .rss is 404)")
        self._reddit_client = reddit_oauth.RedditAppClient()
        return self._reddit_client, None

    @staticmethod
    def _reddit_api_board(client: Any, sub: str, fetched_at: datetime) -> dict[str, int]:
        """Subscribers, users online, and posts/comments of the trailing window for one board.

        ``posts_24h`` / ``comments_on_posts_24h`` (comments so far on those
        posts) are kept only when the listing reached back past the window.
        """
        about = (client.get_json(f"/r/{sub}/about") or {}).get("data") or {}
        out: dict[str, int] = {}
        subscribers = _int_or_none(about.get("subscribers"))
        if subscribers is not None:
            out["subscribers"] = subscribers
        active = _int_or_none(about.get("active_user_count"))
        if active is None:
            active = _int_or_none(about.get("accounts_active"))
        if active is not None:
            out["active_users"] = active
        since = fetched_at - timedelta(hours=SERIES_WINDOW_H)
        posts = comments = 0
        after: str | None = None
        for _ in range(REDDIT_NEW_MAX_PAGES):
            params: dict[str, Any] = {"limit": 100}
            if after:
                params["after"] = after
            listing = (client.get_json(f"/r/{sub}/new", params) or {}).get("data") or {}
            children = listing.get("children") or []
            oldest: datetime | None = None
            for child in children:
                d = child.get("data") or {}
                try:
                    created = datetime.fromtimestamp(float(d["created_utc"]), tz=timezone.utc)
                except (KeyError, TypeError, ValueError):
                    continue
                oldest = created if oldest is None or created < oldest else oldest
                if since <= created <= fetched_at:
                    posts += 1
                    comments += _int_or_none(d.get("num_comments")) or 0
            after = listing.get("after")
            if not after or not children or (oldest is not None and oldest < since):
                out["posts_24h"] = posts
                out["comments_on_posts_24h"] = comments
                break
        return out
