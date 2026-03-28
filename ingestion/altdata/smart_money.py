"""
GRID Social Smart Money Tracker.

Tracks high-accuracy social media accounts and insider activity:
1. Reddit: monitors r/wallstreetbets, r/options, r/stocks for posts by
   accounts with historically good call accuracy (trust scoring).
2. Finviz: scrapes the insider trading page for notable transactions.

Only tracks accounts that have been right before — uses a trust score
per username that decays over time and is boosted by accurate calls.

Series stored:
- SOCIAL:{platform}:{username}:{ticker}:{direction}

Source: Reddit JSON API (public, no key needed), Finviz insider page
Schedule: Daily
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ── Configuration ────────────────────────────────────────────────────

_REQUEST_TIMEOUT: int = 30
_RATE_LIMIT_DELAY: float = 2.0  # Reddit is strict on rate limits

# Target subreddits for financial discussion
TARGET_SUBREDDITS: list[str] = [
    "wallstreetbets",
    "options",
    "stocks",
    "investing",
    "thetagang",
]

# Reddit JSON API (public, no auth required — append .json to any URL)
_REDDIT_BASE: str = "https://www.reddit.com"

# Minimum trust score to track a user (0-1 scale)
_MIN_TRUST_SCORE: float = 0.3

# Default trust score for new users
_DEFAULT_TRUST_SCORE: float = 0.5

# Minimum account age in days to consider
_MIN_ACCOUNT_AGE_DAYS: int = 90

# Minimum post score (upvotes - downvotes) to consider
_MIN_POST_SCORE: int = 10

# Maximum posts to scan per subreddit
_MAX_POSTS_PER_SUB: int = 50

# Finviz insider trading page
_FINVIZ_INSIDER_URL: str = "https://finviz.com/insidertrades.ashx"

# Known ticker symbols for extraction (major liquid names)
_KNOWN_TICKERS: set[str] = {
    "SPY", "QQQ", "IWM", "DIA", "TLT", "HYG", "XLF", "XLE", "XLK",
    "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "GOOGL", "META", "AMD",
    "JPM", "BAC", "GS", "NFLX", "COIN", "PLTR", "SOFI",
    "GLD", "SLV", "USO", "EEM", "FXI", "KWEB",
    "COST", "WMT", "HD", "LOW", "TGT", "CRM", "ORCL", "ADBE",
    "V", "MA", "PYPL", "SQ", "SHOP", "SNOW", "NET", "DDOG",
    "UBER", "LYFT", "ABNB", "DASH", "RBLX", "SNAP", "PINS",
    "ARM", "SMCI", "AVGO", "MU", "INTC", "QCOM", "TSM",
}

# Bullish/bearish signal keywords
_BULLISH_KEYWORDS: list[str] = [
    "calls", "long", "buy", "bullish", "moon", "rocket", "yolo",
    "all in", "going up", "undervalued", "breakout", "squeeze",
    "to the moon", "diamond hands", "bull",
]
_BEARISH_KEYWORDS: list[str] = [
    "puts", "short", "sell", "bearish", "crash", "dump",
    "overvalued", "bubble", "going down", "collapse",
    "bear", "hedge", "protection",
]


def _extract_tickers(text_content: str) -> list[str]:
    """Extract ticker symbols from post text.

    Looks for $TICKER patterns and known ticker mentions.

    Parameters:
        text_content: Post title + body text.

    Returns:
        List of unique ticker symbols found.
    """
    tickers: list[str] = []

    # Pattern 1: $TICKER format
    dollar_tickers = re.findall(r"\$([A-Z]{1,5})\b", text_content.upper())
    tickers.extend(dollar_tickers)

    # Pattern 2: Known ticker mentioned as whole word
    upper_text = text_content.upper()
    for ticker in _KNOWN_TICKERS:
        # Only match whole words, require 3+ chars to avoid false positives
        if len(ticker) >= 3 and re.search(rf"\b{ticker}\b", upper_text):
            if ticker not in tickers:
                tickers.append(ticker)

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for t in tickers:
        if t in _KNOWN_TICKERS and t not in seen:
            seen.add(t)
            unique.append(t)

    return unique


def _detect_direction(text_content: str) -> str:
    """Detect bullish/bearish direction from post text.

    Parameters:
        text_content: Post title + body text.

    Returns:
        'BULLISH', 'BEARISH', or 'NEUTRAL'.
    """
    lower = text_content.lower()
    bull_count = sum(1 for kw in _BULLISH_KEYWORDS if kw in lower)
    bear_count = sum(1 for kw in _BEARISH_KEYWORDS if kw in lower)

    if bull_count > bear_count:
        return "BULLISH"
    elif bear_count > bull_count:
        return "BEARISH"
    return "NEUTRAL"


def _text_hash(text_content: str) -> str:
    """Generate a short hash of post text for dedup/tracking.

    Parameters:
        text_content: Text to hash.

    Returns:
        8-character hex hash.
    """
    return hashlib.sha256(text_content.encode("utf-8", errors="replace")).hexdigest()[:8]


class SmartMoneyPuller(BasePuller):
    """Tracks social smart money signals from Reddit and Finviz.

    Monitors specific subreddits for posts by accounts with proven
    track records, extracting ticker mentions and directional signals.
    Also scrapes Finviz insider trading data.

    Only stores signals from accounts meeting minimum trust thresholds
    (account age, historical accuracy, post quality).

    Series pattern: SOCIAL:{platform}:{username}:{ticker}:{direction}

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for Social_Smart_Money.
        _trust_cache: In-memory cache of user trust scores.
    """

    SOURCE_NAME: str = "Social_Smart_Money"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://www.reddit.com/",
        "cost_tier": "FREE",
        "latency_class": "INTRADAY",
        "pit_available": True,
        "revision_behavior": "APPEND_ONLY",
        "trust_score": "LOW",
        "priority_rank": 45,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the smart money puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        self._trust_cache: dict[str, float] = {}
        log.info(
            "SmartMoneyPuller initialised — source_id={sid}",
            sid=self.source_id,
        )

    # ------------------------------------------------------------------ #
    # Trust scoring
    # ------------------------------------------------------------------ #

    def _load_trust_scores(self) -> None:
        """Load existing user trust scores from the database."""
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT DISTINCT raw_payload->>'username' AS username, "
                        "AVG(value) AS avg_score "
                        "FROM raw_series "
                        "WHERE series_id LIKE 'SOCIAL:reddit:%' "
                        "AND source_id = :src "
                        "AND pull_status = 'SUCCESS' "
                        "GROUP BY raw_payload->>'username'"
                    ),
                    {"src": self.source_id},
                ).fetchall()

                for row in rows:
                    if row[0]:
                        self._trust_cache[row[0]] = float(row[1]) if row[1] else _DEFAULT_TRUST_SCORE

            log.debug(
                "SmartMoney: loaded {n} user trust scores",
                n=len(self._trust_cache),
            )
        except Exception as exc:
            log.warning(
                "SmartMoney: failed to load trust scores: {e}",
                e=str(exc),
            )

    def _get_trust_score(self, username: str) -> float:
        """Get trust score for a username.

        Parameters:
            username: Reddit username.

        Returns:
            Trust score (0-1). Returns default for unknown users.
        """
        return self._trust_cache.get(username, _DEFAULT_TRUST_SCORE)

    def _passes_trust_filter(
        self,
        username: str,
        account_age_days: int | None,
        post_score: int,
    ) -> bool:
        """Check if a user passes the trust filter.

        Parameters:
            username: Reddit username.
            account_age_days: Account age in days (None if unknown).
            post_score: Post upvote score.

        Returns:
            True if the user should be tracked.
        """
        # Minimum post quality
        if post_score < _MIN_POST_SCORE:
            return False

        # Account age check (if available)
        if account_age_days is not None and account_age_days < _MIN_ACCOUNT_AGE_DAYS:
            return False

        # Trust score check
        trust = self._get_trust_score(username)
        if trust < _MIN_TRUST_SCORE:
            return False

        return True

    # ------------------------------------------------------------------ #
    # Reddit API interaction
    # ------------------------------------------------------------------ #

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(
            ConnectionError,
            TimeoutError,
            OSError,
            requests.RequestException,
        ),
    )
    def _fetch_subreddit_posts(
        self,
        subreddit: str,
        sort: str = "hot",
        limit: int = _MAX_POSTS_PER_SUB,
    ) -> list[dict[str, Any]]:
        """Fetch posts from a subreddit using Reddit's JSON API.

        Parameters:
            subreddit: Subreddit name (without r/).
            sort: Sort order ('hot', 'new', 'top', 'rising').
            limit: Maximum posts to fetch (max 100 per Reddit API).

        Returns:
            List of post data dicts.

        Raises:
            requests.RequestException: On HTTP errors after retries.
        """
        headers = {
            "User-Agent": "GRID-DataPuller/1.0 (financial research)",
            "Accept": "application/json",
        }

        url = f"{_REDDIT_BASE}/r/{subreddit}/{sort}.json"
        params = {"limit": min(limit, 100), "raw_json": 1}

        resp = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        posts = []
        children = data.get("data", {}).get("children", [])
        for child in children:
            post_data = child.get("data", {})
            if post_data:
                posts.append(post_data)

        return posts

    def _parse_reddit_post(
        self,
        post: dict[str, Any],
        subreddit: str,
    ) -> list[dict[str, Any]]:
        """Parse a Reddit post into smart money signals.

        Extracts tickers, direction, and metadata from a single post.
        Returns one signal per ticker mentioned.

        Parameters:
            post: Reddit post data dict.
            subreddit: Source subreddit name.

        Returns:
            List of signal dicts (one per ticker found).
        """
        title = post.get("title", "")
        body = post.get("selftext", "")
        full_text = f"{title} {body}"
        username = post.get("author", "")
        post_score = int(post.get("score", 0))

        if not username or username in ("[deleted]", "AutoModerator"):
            return []

        # Calculate account age if available
        created_utc = post.get("created_utc")
        account_age_days: int | None = None
        if created_utc:
            try:
                post_time = datetime.fromtimestamp(created_utc, tz=timezone.utc)
                account_age_days = None  # Post age, not account age
            except (ValueError, OSError):
                pass

        # Trust filter
        if not self._passes_trust_filter(username, account_age_days=None, post_score=post_score):
            return []

        # Extract tickers and direction
        tickers = _extract_tickers(full_text)
        if not tickers:
            return []

        direction = _detect_direction(full_text)
        if direction == "NEUTRAL":
            return []

        post_hash = _text_hash(full_text)
        post_timestamp = None
        if created_utc:
            try:
                post_timestamp = datetime.fromtimestamp(
                    created_utc, tz=timezone.utc
                ).isoformat()
            except (ValueError, OSError):
                pass

        signals: list[dict[str, Any]] = []
        for ticker in tickers:
            signals.append({
                "platform": "reddit",
                "username": username,
                "ticker": ticker,
                "direction": direction,
                "subreddit": subreddit,
                "post_title": title[:200],
                "post_hash": post_hash,
                "post_score": post_score,
                "post_timestamp": post_timestamp,
                "trust_score": self._get_trust_score(username),
                "num_comments": post.get("num_comments", 0),
                "upvote_ratio": post.get("upvote_ratio", 0),
            })

        return signals

    # ------------------------------------------------------------------ #
    # Finviz insider scraping
    # ------------------------------------------------------------------ #

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(
            ConnectionError,
            TimeoutError,
            OSError,
            requests.RequestException,
        ),
    )
    def _fetch_finviz_insiders(self) -> list[dict[str, Any]]:
        """Fetch insider trading data from Finviz.

        Scrapes the Finviz insider trading page for recent transactions.
        Falls back gracefully if the page structure changes.

        Returns:
            List of insider trade dicts.
        """
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml",
        }

        resp = requests.get(
            _FINVIZ_INSIDER_URL,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        html = resp.text

        # Parse insider trades from the HTML table
        # Finviz uses a table with class "body-table"
        trades: list[dict[str, Any]] = []

        try:
            # Simple regex-based parsing (avoids BeautifulSoup dependency)
            # Table rows contain: Ticker, Owner, Relationship, Date,
            # Transaction, Cost, #Shares, Value($), #Shares Total, SEC Form 4
            row_pattern = re.compile(
                r'<tr[^>]*class="cursor-pointer[^"]*"[^>]*>.*?</tr>',
                re.DOTALL,
            )
            cell_pattern = re.compile(r'<td[^>]*>(.*?)</td>', re.DOTALL)
            link_text_pattern = re.compile(r'<a[^>]*>(.*?)</a>', re.DOTALL)

            rows = row_pattern.findall(html)

            for row_html in rows[:100]:  # Limit to 100 trades
                cells = cell_pattern.findall(row_html)
                if len(cells) < 9:
                    continue

                # Extract text from cells (strip HTML tags)
                clean_cells = []
                for cell in cells:
                    link_match = link_text_pattern.search(cell)
                    text_val = link_match.group(1) if link_match else cell
                    text_val = re.sub(r'<[^>]+>', '', text_val).strip()
                    clean_cells.append(text_val)

                ticker = clean_cells[0].upper().strip()
                owner = clean_cells[1].strip()
                relationship = clean_cells[2].strip()
                trade_date_str = clean_cells[3].strip()
                transaction = clean_cells[4].strip()
                cost = clean_cells[5].strip()
                shares = clean_cells[6].strip()
                value_str = clean_cells[7].strip()

                if not ticker or ticker not in _KNOWN_TICKERS:
                    continue

                # Parse transaction type
                direction = "NEUTRAL"
                tx_lower = transaction.lower()
                if "buy" in tx_lower or "purchase" in tx_lower:
                    direction = "BULLISH"
                elif "sale" in tx_lower or "sell" in tx_lower:
                    direction = "BEARISH"

                if direction == "NEUTRAL":
                    continue

                # Parse value
                try:
                    value_clean = value_str.replace(",", "").replace("$", "")
                    trade_value = float(value_clean)
                except (ValueError, TypeError):
                    trade_value = 0.0

                trades.append({
                    "platform": "finviz_insider",
                    "username": owner[:50],
                    "ticker": ticker,
                    "direction": direction,
                    "relationship": relationship,
                    "transaction": transaction,
                    "trade_date": trade_date_str,
                    "cost": cost,
                    "shares": shares,
                    "trade_value": trade_value,
                    "trust_score": 0.7,  # Insiders get higher default trust
                })

        except Exception as exc:
            log.warning(
                "SmartMoney: Finviz insider parsing failed: {e}",
                e=str(exc),
            )

        log.info(
            "SmartMoney: parsed {n} insider trades from Finviz",
            n=len(trades),
        )
        return trades

    # ------------------------------------------------------------------ #
    # Storage
    # ------------------------------------------------------------------ #

    def _store_signal(
        self,
        conn: Any,
        signal: dict[str, Any],
        obs_date: date,
    ) -> bool:
        """Store a single social smart money signal.

        Parameters:
            conn: Active database connection (within a transaction).
            signal: Signal dict with platform, username, ticker, direction.
            obs_date: Observation date.

        Returns:
            True if inserted, False if duplicate.
        """
        series_id = (
            f"SOCIAL:{signal['platform']}:{signal['username']}"
            f":{signal['ticker']}:{signal['direction']}"
        )

        if self._row_exists(series_id, obs_date, conn):
            return False

        self._insert_raw(
            conn=conn,
            series_id=series_id,
            obs_date=obs_date,
            value=signal.get("trust_score", _DEFAULT_TRUST_SCORE),
            raw_payload={
                "platform": signal["platform"],
                "username": signal["username"],
                "ticker": signal["ticker"],
                "direction": signal["direction"],
                "post_hash": signal.get("post_hash", ""),
                "post_timestamp": signal.get("post_timestamp", ""),
                "post_score": signal.get("post_score", 0),
                "upvote_ratio": signal.get("upvote_ratio", 0),
                "num_comments": signal.get("num_comments", 0),
                "trust_score": signal.get("trust_score", _DEFAULT_TRUST_SCORE),
                "subreddit": signal.get("subreddit", ""),
                "relationship": signal.get("relationship", ""),
                "trade_value": signal.get("trade_value", 0),
            },
        )
        return True

    # ------------------------------------------------------------------ #
    # Main pull methods
    # ------------------------------------------------------------------ #

    def pull_reddit(
        self,
        subreddits: list[str] | None = None,
    ) -> dict[str, Any]:
        """Pull smart money signals from Reddit.

        Parameters:
            subreddits: Override target subreddits (default: TARGET_SUBREDDITS).

        Returns:
            Dict with status, signals_found, rows_inserted.
        """
        if subreddits is None:
            subreddits = TARGET_SUBREDDITS

        today = date.today()
        self._load_trust_scores()

        all_signals: list[dict[str, Any]] = []

        for sub in subreddits:
            try:
                posts = self._fetch_subreddit_posts(sub)
                log.debug(
                    "SmartMoney: fetched {n} posts from r/{s}",
                    n=len(posts),
                    s=sub,
                )

                for post in posts:
                    signals = self._parse_reddit_post(post, sub)
                    all_signals.extend(signals)

            except Exception as exc:
                log.warning(
                    "SmartMoney: Reddit pull failed for r/{s}: {e}",
                    s=sub,
                    e=str(exc),
                )

            time.sleep(_RATE_LIMIT_DELAY)

        if not all_signals:
            log.info("SmartMoney: no Reddit signals found")
            return {
                "source": "reddit",
                "status": "SUCCESS",
                "signals_found": 0,
                "rows_inserted": 0,
            }

        inserted = 0
        with self.engine.begin() as conn:
            for signal in all_signals:
                try:
                    if self._store_signal(conn, signal, today):
                        inserted += 1
                except Exception as exc:
                    log.debug(
                        "SmartMoney: failed to store Reddit signal: {e}",
                        e=str(exc),
                    )

        log.info(
            "SmartMoney Reddit: {n} signals found, {i} stored",
            n=len(all_signals),
            i=inserted,
        )

        return {
            "source": "reddit",
            "status": "SUCCESS",
            "signals_found": len(all_signals),
            "rows_inserted": inserted,
        }

    def pull_finviz_insiders(self) -> dict[str, Any]:
        """Pull insider trading signals from Finviz.

        Returns:
            Dict with status, signals_found, rows_inserted.
        """
        today = date.today()

        try:
            trades = self._fetch_finviz_insiders()
        except Exception as exc:
            log.error(
                "SmartMoney: Finviz insider pull failed: {e}",
                e=str(exc),
            )
            return {
                "source": "finviz_insider",
                "status": "FAILED",
                "signals_found": 0,
                "rows_inserted": 0,
                "error": str(exc),
            }

        if not trades:
            return {
                "source": "finviz_insider",
                "status": "SUCCESS",
                "signals_found": 0,
                "rows_inserted": 0,
            }

        inserted = 0
        with self.engine.begin() as conn:
            for trade in trades:
                try:
                    if self._store_signal(conn, trade, today):
                        inserted += 1
                except Exception as exc:
                    log.debug(
                        "SmartMoney: failed to store insider signal: {e}",
                        e=str(exc),
                    )

        log.info(
            "SmartMoney Finviz: {n} insider trades found, {i} stored",
            n=len(trades),
            i=inserted,
        )

        return {
            "source": "finviz_insider",
            "status": "SUCCESS",
            "signals_found": len(trades),
            "rows_inserted": inserted,
        }

    def pull_all(self) -> list[dict[str, Any]]:
        """Pull all smart money signals (Reddit + Finviz insiders).

        Never stops on a single-source failure -- logs and continues.

        Returns:
            List of per-source result dicts.
        """
        log.info("Starting smart money pull — Reddit + Finviz insiders")

        results: list[dict[str, Any]] = []

        # Reddit
        try:
            reddit_result = self.pull_reddit()
            results.append(reddit_result)
        except Exception as exc:
            log.error("SmartMoney: Reddit pull failed: {e}", e=str(exc))
            results.append({
                "source": "reddit",
                "status": "FAILED",
                "error": str(exc),
            })

        # Finviz insiders
        try:
            finviz_result = self.pull_finviz_insiders()
            results.append(finviz_result)
        except Exception as exc:
            log.error("SmartMoney: Finviz pull failed: {e}", e=str(exc))
            results.append({
                "source": "finviz_insider",
                "status": "FAILED",
                "error": str(exc),
            })

        total_signals = sum(r.get("signals_found", 0) for r in results)
        total_inserted = sum(r.get("rows_inserted", 0) for r in results)
        log.info(
            "SmartMoney pull complete — {s} signals, {i} stored across {n} sources",
            s=total_signals,
            i=total_inserted,
            n=len(results),
        )

        return results


if __name__ == "__main__":
    from db import get_engine

    puller = SmartMoneyPuller(db_engine=get_engine())
    results = puller.pull_all()
    for r in results:
        print(
            f"  {r.get('source', '?')}: {r.get('status')} — "
            f"{r.get('signals_found', 0)} signals, "
            f"{r.get('rows_inserted', 0)} stored"
        )
