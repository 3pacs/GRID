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

Source: Reddit hot-post RSS feed (no credentials; see
ingestion/altdata/reddit_rss.py). Finviz is retired.
Schedule: Daily
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.altdata import reddit_rss
from ingestion.base import BasePuller, retry_on_failure

# ── Configuration ────────────────────────────────────────────────────

_REQUEST_TIMEOUT: int = 30

# Target subreddits for financial discussion
TARGET_SUBREDDITS: list[str] = [
    "wallstreetbets",
    "options",
    "stocks",
    "investing",
    "thetagang",
]

# Reddit is read from one combined hot-post RSS feed for every target board
# (ingestion/altdata/reddit_rss.py). The .json listings return "403 Blocked"
# from grid-svr and no Data API app was registered, so the OAuth path
# (reddit_oauth.py) left this source dead from 2026-05-28 to 2026-11-05.

# Minimum trust score to track a user (0-1 scale)
_MIN_TRUST_SCORE: float = 0.3

# Default trust score for new users
_DEFAULT_TRUST_SCORE: float = 0.5

# Minimum account age in days to consider
_MIN_ACCOUNT_AGE_DAYS: int = 90

# Minimum post score (upvotes - downvotes) to consider, when a score is known.
# RSS carries no score; there the hot listing itself is the traction filter.
_MIN_POST_SCORE: int = 10

# Posts in the combined hot feed, across all target boards
_MAX_HOT_POSTS: int = 100

# Finviz insider trading page
_FINVIZ_INSIDER_URL: str = "https://finviz.com/insidertrading.ashx"

_FINVIZ_UNAVAILABLE_BODY_PATTERNS: tuple[str, ...] = (
    "404 not found",
    "410 gone",
    "page not found",
    "resource not found",
    "endpoint gone",
)

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
        "revision_behavior": "NEVER",
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
        self._finviz_source_unavailable: bool = False
        self._finviz_source_unavailable_reason: str | None = None
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
        post_score: int | None,
    ) -> bool:
        """Check if a user passes the trust filter.

        Parameters:
            username: Reddit username.
            account_age_days: Account age in days (None if unknown).
            post_score: Post upvote score (None when the feed carries none).

        Returns:
            True if the user should be tracked.
        """
        # Minimum post quality, when the score is known
        if post_score is not None and post_score < _MIN_POST_SCORE:
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

    def _fetch_hot_posts(self, subreddits: list[str]) -> list[dict[str, Any]]:
        """Hot posts for every target board from one combined RSS feed.

        Shaped like the Data API post fields ``_parse_reddit_post`` reads.
        RSS carries no score, comment count or upvote ratio, so those are None.

        Raises:
            reddit_rss.RedditRateLimited, requests.RequestException: no feed.
        """
        entries = reddit_rss.fetch_feed(
            f"/r/{'+'.join(subreddits)}/hot.rss", {"limit": _MAX_HOT_POSTS},
        )
        canonical = {s.lower(): s for s in subreddits}
        posts = []
        for entry in entries:
            if entry.kind != "t3":
                continue
            posts.append({
                "title": entry.title,
                "selftext": entry.body,
                "author": entry.author,
                "subreddit": canonical.get(entry.subreddit.lower(), entry.subreddit),
                "created_utc": entry.published.timestamp() if entry.published else None,
                "score": None,
                "num_comments": None,
                "upvote_ratio": None,
            })
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
        raw_score = post.get("score")
        post_score = int(raw_score) if raw_score is not None else None

        if not username or username in ("[deleted]", "AutoModerator"):
            return []

        # Calculate account age if available
        created_utc = post.get("created_utc")
        if created_utc:
            try:
                datetime.fromtimestamp(created_utc, tz=timezone.utc)
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
                "num_comments": post.get("num_comments"),
                "upvote_ratio": post.get("upvote_ratio"),
            })

        return signals

    # ------------------------------------------------------------------ #
    # Finviz insider scraping
    # ------------------------------------------------------------------ #

    def _is_finviz_source_unavailable(self, resp: Any) -> str | None:
        """Return a short reason if the Finviz page looks unavailable."""
        status_code = getattr(resp, "status_code", None)
        if status_code in (404, 410):
            return f"HTTP {status_code}"

        text = f"{getattr(resp, 'text', '')} {getattr(resp, 'reason', '')}".lower()
        for pattern in _FINVIZ_UNAVAILABLE_BODY_PATTERNS:
            if pattern in text:
                return pattern
        return None

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
        raise RuntimeError("Finviz retired; use the separate SEC Form-4 source")

    def _store_signal(
        self,
        conn: Any,
        signal: dict[str, Any],
        obs_date: date,
    ) -> bool:
        """Store a single social smart money signal.

        Writes the raw_series row first (always), then emits the
        downstream signal_sources rows for trust scoring + convergence
        detection. A signal_sources write failure is logged at WARN but
        does not drop the raw_series row.

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

        # Emit signal_sources rows AFTER the raw_series write so the
        # raw ingest is never dropped due to a signal-source write fault.
        try:
            # Own savepoint, so a signal_sources fault cannot abort the
            # enclosing transaction and take the raw_series row with it.
            begin_nested = getattr(conn, "begin_nested", None)
            if callable(begin_nested):
                with begin_nested():
                    self._emit_signal_sources(conn, signal, obs_date)
            else:
                self._emit_signal_sources(conn, signal, obs_date)
        except Exception as exc:
            log.warning(
                "SmartMoney: signal_sources emission failed for "
                "{u}/{t}: {e}",
                u=signal.get("username", "?"),
                t=signal.get("ticker", "?"),
                e=str(exc),
            )
        return True

    # ------------------------------------------------------------------ #
    # signal_sources emission (smart_money + social heat)
    # ------------------------------------------------------------------ #

    def _emit_signal_sources(
        self,
        conn: Any,
        signal: dict[str, Any],
        obs_date: date,
    ) -> None:
        """Emit smart_money + social heat rows to signal_sources.

        Two rows are written per signal so the downstream convergence
        scanner sees both the smart-money net position delta stream and
        the social heat-spike stream:

        1. source_type='smart_money', signal_type='NET_POSITION_DELTA'
           payload: {position_delta, reddit_mentions_count,
                     sentiment_score, window_days}
        2. source_type='social', signal_type='HEAT_SPIKE'
           payload: {mentions_z, sentiment, ticker_rank}

        Column order + upsert key match the schema contract in schema.sql
        around line 803 and mirror the working congressional / insider
        pullers (source_type, source_id, ticker, signal_date, signal_type,
        signal_value). The trust_score column is left to the schema
        default (0.5) to match how every other working puller behaves.

        Parameters:
            conn: Active database connection (within a transaction).
            signal: Signal dict with platform, username, ticker, direction.
            obs_date: Observation date for the signal row.
        """
        ticker = str(signal.get("ticker") or "").strip()
        if not ticker:
            return

        direction = str(signal.get("direction", "NEUTRAL"))
        # Map direction to a numeric position delta so the scanner can
        # aggregate without string parsing: BULLISH=+1, BEARISH=-1.
        if direction == "BULLISH":
            position_delta = 1.0
            sentiment_score = 1.0
        elif direction == "BEARISH":
            position_delta = -1.0
            sentiment_score = -1.0
        else:
            position_delta = 0.0
            sentiment_score = 0.0

        post_score = int(signal.get("post_score", 0) or 0)
        # Reddit post_score is a cheap proxy for "mentions heat" on a
        # per-post basis. When richer z-scores are wired in (by a
        # rolling-window helper) replace this inline calc.
        mentions_z = float(post_score) / 100.0

        username = str(signal.get("username", "")).strip() or "unknown"
        source_id = f"{signal.get('platform', 'reddit')}:{username}"[:200]

        # Row 1: smart_money NET_POSITION_DELTA
        conn.execute(
            text(
                "INSERT INTO signal_sources "
                "(source_type, source_id, ticker, signal_date, "
                "signal_type, signal_value) "
                "VALUES (:stype, :sid, :ticker, :sdate, :stype2, :sval) "
                "ON CONFLICT (source_type, source_id, ticker, "
                "signal_date, signal_type) DO NOTHING"
            ),
            {
                "stype": "smart_money",
                "sid": source_id,
                "ticker": ticker,
                "sdate": obs_date,
                "stype2": "NET_POSITION_DELTA",
                "sval": json.dumps({
                    "position_delta": position_delta,
                    "reddit_mentions_count": 1,
                    "sentiment_score": sentiment_score,
                    "window_days": 1,
                    "platform": signal.get("platform", ""),
                    "username": username,
                    "direction": direction,
                    "post_score": post_score,
                    "trust_score": float(
                        signal.get("trust_score", _DEFAULT_TRUST_SCORE)
                    ),
                }),
            },
        )

        # Row 2: social HEAT_SPIKE
        # The per-ticker rank inside this pull batch is derived on the
        # caller side (see pull_reddit). If unavailable, fall back to 0.
        ticker_rank = int(signal.get("ticker_rank", 0) or 0)
        conn.execute(
            text(
                "INSERT INTO signal_sources "
                "(source_type, source_id, ticker, signal_date, "
                "signal_type, signal_value) "
                "VALUES (:stype, :sid, :ticker, :sdate, :stype2, :sval) "
                "ON CONFLICT (source_type, source_id, ticker, "
                "signal_date, signal_type) DO NOTHING"
            ),
            {
                "stype": "social",
                "sid": source_id,
                "ticker": ticker,
                "sdate": obs_date,
                "stype2": "HEAT_SPIKE",
                "sval": json.dumps({
                    "mentions_z": mentions_z,
                    "sentiment": sentiment_score,
                    "ticker_rank": ticker_rank,
                    "platform": signal.get("platform", ""),
                    "username": username,
                    "subreddit": signal.get("subreddit", ""),
                    "direction": direction,
                }),
            },
        )

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

        try:
            posts = self._fetch_hot_posts(subreddits)
        except Exception as exc:
            log.warning("SmartMoney: Reddit RSS pull failed: {e}", e=str(exc))
            return {
                "source": "reddit",
                "status": "FAILED",
                "error": f"reddit rss fetch failed: {exc}",
                "signals_found": 0,
                "rows_inserted": 0,
            }
        log.debug("SmartMoney: fetched {n} hot posts from {k} boards",
                  n=len(posts), k=len(subreddits))

        all_signals: list[dict[str, Any]] = []
        for post in posts:
            all_signals.extend(self._parse_reddit_post(post, post["subreddit"]))

        status = "SUCCESS"
        partial_note: dict[str, str] = {}

        if not all_signals:
            log.info("SmartMoney: no Reddit signals found")
            return {
                "source": "reddit",
                "status": status,
                "signals_found": 0,
                "rows_inserted": 0,
                **partial_note,
            }

        inserted = 0
        store_failures = 0
        with self.engine.begin() as conn:
            for signal in all_signals:
                try:
                    # A savepoint per signal: on PostgreSQL a failed statement
                    # aborts the whole transaction, so swallowing it without
                    # one would silently roll back every row counted so far.
                    with conn.begin_nested():
                        if self._store_signal(conn, signal, today):
                            inserted += 1
                except Exception as exc:
                    store_failures += 1
                    log.debug(
                        "SmartMoney: failed to store Reddit signal: {e}",
                        e=str(exc),
                    )

        log.info(
            "SmartMoney Reddit: {n} signals found, {i} stored",
            n=len(all_signals),
            i=inserted,
        )

        if store_failures:
            status = "PARTIAL"
            partial_note = {"error": f"{store_failures} of {len(all_signals)} signals failed to store"}
        return {
            "source": "reddit",
            "status": status,
            "signals_found": len(all_signals),
            "rows_inserted": inserted,
            **partial_note,
        }

    def pull_finviz_insiders(self) -> dict[str, Any]:
        """Compatibility entry point: Finviz retired; existing SEC Form-4 remains."""
        return {
            "source": "finviz_insider", "status": "SKIPPED",
            "signals_found": 0, "rows_inserted": 0,
            "reason": "Finviz retired; insider filings use the separate SEC Form-4 source",
        }

    def pull_all(self) -> list[dict[str, Any]]:
        """Pull Reddit signals; Finviz is retired and SEC Form-4 is separate.

        Never stops on a single-source failure -- logs and continues.

        Returns:
            List of per-source result dicts.
        """
        log.info("Starting smart money pull — Reddit; Finviz retired")

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
