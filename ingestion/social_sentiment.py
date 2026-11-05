"""
Social sentiment ingestor — Reddit, Bluesky, Google Trends.

Scrapes public social media for sentiment signals around tracked
tickers, themes, and product launches. No API keys required for
Reddit (public JSON) and Bluesky (public AT Protocol).

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
reason -- from grid-svr both return HTTP 403 to unauthenticated requests
(probed 2026-11-05) -- never an empty success.
"""

from __future__ import annotations

import json
import re
import time
from datetime import date
from typing import Any

import requests
from loguru import logger as log


# Tracked subreddits by theme
SUBREDDITS = {
    "market": ["wallstreetbets", "stocks", "investing", "options"],
    "crypto": ["cryptocurrency", "bitcoin", "ethereum", "solana"],
    "tech": ["technology", "artificial", "MachineLearning", "nvidia"],
    "macro": ["economics", "finance", "personalfinance"],
}

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


class SocialSentimentPuller:
    """Pull sentiment from Reddit, Bluesky, and Google Trends."""

    def __init__(self, db_engine: Any = None) -> None:
        self.engine = db_engine
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GRID/4.0 (research; grid.stepdad.finance)",
        })

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
            "bluesky": [],
            "trends": {},
            "ticker_sentiment": {},
            "summary": "",
        }

        # Per-request failures: a failed request is lost coverage, not "no posts".
        failures: list[str] = []
        attempts = 0

        # Reddit
        for theme, subs in SUBREDDITS.items():
            for sub in subs:
                attempts += 1
                try:
                    posts = self._reddit_hot(sub)
                    result["reddit"][sub] = posts
                    time.sleep(2)  # Rate limit
                except Exception as exc:
                    failures.append(f"reddit r/{sub}: {exc}")
                    log.debug("Reddit r/{s} failed: {e}", s=sub, e=str(exc))

        # Bluesky
        bluesky_ok = 0
        for term in BLUESKY_TERMS[:5]:
            attempts += 1
            try:
                posts = self._bluesky_search(term)
                result["bluesky"].extend(posts)
                bluesky_ok += 1
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
        target_tickers = tickers or [
            "SPY", "NVDA", "AAPL", "MSFT", "GOOGL", "META", "AMZN", "TSLA",
            "BTC", "ETH", "SOL", "AMD", "INTC", "AVGO",
        ]

        for ticker in target_tickers:
            score = self._score_ticker(ticker, all_text)
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

        log.info(
            "Social sentiment: {m} mentions, {b} bullish, {br} bearish tickers",
            m=total_mentions, b=len(bullish_tickers), br=len(bearish_tickers),
        )
        return self._with_outcome(result, failures, attempts, len(result["reddit"]) + bluesky_ok)

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
        if not (result["ticker_sentiment"] or result["reddit"] or result["bluesky"]):
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

    def _reddit_hot(self, subreddit: str, limit: int = 25) -> list[dict]:
        """Fetch hot posts from a subreddit via public JSON API."""
        url = f"https://www.reddit.com/r/{subreddit}/hot.json?limit={limit}"
        resp = self._session.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        posts = []
        for child in data.get("data", {}).get("children", []):
            post = child.get("data", {})
            posts.append({
                "title": post.get("title", ""),
                "score": post.get("score", 0),
                "comments": post.get("num_comments", 0),
                "upvote_ratio": post.get("upvote_ratio", 0),
                "created": post.get("created_utc", 0),
                "subreddit": subreddit,
            })
        return posts

    def _bluesky_search(self, query: str, limit: int = 25) -> list[dict]:
        """Search Bluesky public API for posts."""
        url = "https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts"
        params = {"q": query, "limit": limit, "sort": "latest"}
        resp = self._session.get(url, params=params, timeout=10)
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
        if not (result.get("ticker_sentiment") or result.get("reddit") or result.get("bluesky")):
            return None
        from sqlalchemy import text
        scan = {k: v for k, v in result.items()
                if k not in ("status", "rows_inserted", "error", "skipped_reason")}
        with self.engine.begin() as conn:
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
            if not src:
                return None
            series_id = f"social_sentiment_{scan['date']}"
            exists = conn.execute(text(
                "SELECT 1 FROM raw_series WHERE series_id = :sid AND source_id = :src "
                "AND obs_date = :d AND pull_status = 'SUCCESS' LIMIT 1"
            ), {"sid": series_id, "src": src[0], "d": scan["date"]}).fetchone()
            if exists:
                return 0
            # pull_timestamp: schema DEFAULT NOW().
            conn.execute(text(
                "INSERT INTO raw_series (series_id, source_id, obs_date, value, raw_payload, pull_status) "
                "VALUES (:sid, :src, :d, :v, :payload, 'SUCCESS')"
            ), {
                "sid": series_id,
                "src": src[0],
                "d": scan["date"],
                "v": len(scan.get("ticker_sentiment", {})),
                "payload": json.dumps(scan, default=str),
            })
        # Reached only after the transaction committed.
        return 1
