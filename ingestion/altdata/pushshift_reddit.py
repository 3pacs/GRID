"""
Pushshift Reddit historical backfill ingestor.

Parses zstandard-compressed NDJSON dump files from the Pushshift Reddit
archive (available via Academic Torrents) and extracts daily sentiment
aggregates per subreddit.  Streams files line by line to handle multi-GB
dumps without blowing memory.

Produces series:  pushshift.{subreddit}.daily_sentiment
Stored in raw_series via BasePuller._insert_raw().

Reference: github.com/Watchful1/PushshiftDumps
"""

from __future__ import annotations

import re
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import orjson
import zstandard as zstd
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# ── Target subreddits ────────────────────────────────────────────────

TARGET_SUBREDDITS: set[str] = {
    "wallstreetbets",
    "stocks",
    "investing",
    "cryptocurrency",
    "options",
    "thetagang",
    "SPACs",
    "Superstonk",
    "Bitcoin",
    "solana",
    "ethtrader",
    "algotrading",
}

# Lowercase lookup for case-insensitive matching
_TARGET_LOWER: set[str] = {s.lower() for s in TARGET_SUBREDDITS}

# ── Sentiment keywords (mirrored from social_sentiment.py) ──────────

BULLISH_WORDS: set[str] = {
    "moon", "bull", "buy", "long", "calls", "rocket", "squeeze", "breakout",
    "undervalued", "growth", "beat", "surge", "rally", "pump", "green",
    "upgrade", "outperform", "strong", "boom", "soar",
}
BEARISH_WORDS: set[str] = {
    "bear", "sell", "short", "puts", "crash", "dump", "overvalued",
    "recession", "miss", "tank", "plunge", "red", "downgrade",
    "underperform", "weak", "bust", "collapse", "drop", "fade",
}

# ── Ticker extraction ────────────────────────────────────────────────

# Cashtag pattern: $AAPL, $BTC — 1-5 uppercase letters after $
_CASHTAG_RE = re.compile(r"\$([A-Z]{1,5})\b")

# Known tickers to also match without $ prefix (common, high-signal only)
_KNOWN_TICKERS: set[str] = {
    "SPY", "QQQ", "AAPL", "MSFT", "GOOGL", "GOOG", "META", "AMZN", "TSLA",
    "NVDA", "AMD", "INTC", "AVGO", "NFLX", "DIS", "BA", "JPM", "GS",
    "BTC", "ETH", "SOL", "DOGE", "XRP", "ADA", "DOT", "MATIC",
    "GME", "AMC", "BBBY", "PLTR", "SOFI", "RIVN", "LCID",
}

# Word-boundary pattern for known tickers (compiled once)
_KNOWN_RE = re.compile(
    r"\b(" + "|".join(re.escape(t) for t in sorted(_KNOWN_TICKERS)) + r")\b"
)

# ── Zstandard streaming config ───────────────────────────────────────

_ZST_READ_SIZE = 2 ** 17  # 128 KiB chunks for streaming decompression
_BATCH_INSERT_SIZE = 500   # Flush to DB every N daily aggregates


class PushshiftRedditPuller(BasePuller):
    """Parse Pushshift Reddit dump files and insert daily sentiment aggregates."""

    SOURCE_NAME = "pushshift_reddit"
    SOURCE_CONFIG = {
        "base_url": "https://academictorrents.com/",
        "cost_tier": "FREE",
        "latency_class": "HISTORICAL",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "MED",
        "priority_rank": 60,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        # Accumulator: (subreddit_lower, date_str) -> list[post_dict]
        self._day_buckets: dict[tuple[str, str], list[dict]] = defaultdict(list)

    # ── Public API ───────────────────────────────────────────────────

    def ingest_directory(self, dirpath: str | Path) -> dict[str, Any]:
        """Process all .zst files in *dirpath*.

        Parameters:
            dirpath: Directory containing Pushshift .zst dump files.

        Returns:
            Summary dict with files_processed, rows_inserted, errors.
        """
        dirpath = Path(dirpath)
        if not dirpath.is_dir():
            raise FileNotFoundError(f"Directory not found: {dirpath}")

        zst_files = sorted(dirpath.glob("*.zst"))
        log.info(
            "Pushshift ingest — found {n} .zst files in {d}",
            n=len(zst_files), d=str(dirpath),
        )

        summary: dict[str, Any] = {
            "files_processed": 0,
            "files_skipped": 0,
            "rows_inserted": 0,
            "errors": [],
        }

        for zst_file in zst_files:
            try:
                result = self.process_dump_file(zst_file)
                summary["files_processed"] += 1
                summary["rows_inserted"] += result["rows_inserted"]
                log.info(
                    "Processed {f} — {p} posts, {r} rows inserted",
                    f=zst_file.name,
                    p=result["posts_parsed"],
                    r=result["rows_inserted"],
                )
            except Exception as exc:
                summary["errors"].append({"file": str(zst_file), "error": str(exc)})
                log.error("Failed to process {f}: {e}", f=zst_file.name, e=str(exc))

        # Flush any remaining accumulated data
        flushed = self._flush_buckets()
        summary["rows_inserted"] += flushed

        log.info(
            "Pushshift ingest complete — {fp} files, {ri} rows, {err} errors",
            fp=summary["files_processed"],
            ri=summary["rows_inserted"],
            err=len(summary["errors"]),
        )
        return summary

    def process_dump_file(self, filepath: str | Path) -> dict[str, Any]:
        """Read a zstandard-compressed NDJSON file and accumulate posts.

        Streams the file line by line — never loads the entire file into
        memory.  Posts from target subreddits are bucketed by (subreddit, date)
        for later aggregation.

        Parameters:
            filepath: Path to a .zst compressed NDJSON file.

        Returns:
            Summary dict with posts_parsed, posts_matched, rows_inserted.
        """
        filepath = Path(filepath)
        if not filepath.exists():
            raise FileNotFoundError(f"Dump file not found: {filepath}")

        posts_parsed = 0
        posts_matched = 0
        rows_inserted = 0

        dctx = zstd.ZstdDecompressor(max_window_size=2 ** 31)

        with open(filepath, "rb") as fh:
            reader = dctx.stream_reader(fh)
            text_stream = _LineReader(reader, read_size=_ZST_READ_SIZE)

            for line in text_stream:
                if not line.strip():
                    continue

                try:
                    obj = orjson.loads(line)
                except (orjson.JSONDecodeError, ValueError):
                    continue

                posts_parsed += 1

                # Filter to target subreddits
                subreddit = obj.get("subreddit", "")
                if subreddit.lower() not in _TARGET_LOWER:
                    continue

                posts_matched += 1

                # Extract timestamp
                created_utc = obj.get("created_utc")
                if created_utc is None:
                    continue

                try:
                    ts = int(created_utc)
                    obs_date = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
                except (ValueError, TypeError, OSError):
                    continue

                # Build lightweight post record
                title = obj.get("title", "")
                body = obj.get("selftext", "") or obj.get("body", "")
                text_blob = f"{title} {body}".strip()

                post_record = {
                    "id": obj.get("id", ""),
                    "title": title[:300],
                    "text_snippet": text_blob[:500],
                    "score": obj.get("score", 0),
                    "num_comments": obj.get("num_comments", 0),
                    "created_utc": ts,
                }

                key = (subreddit.lower(), obs_date)
                self._day_buckets[key].append(post_record)

                # Periodic flush to avoid unbounded memory growth
                if len(self._day_buckets) > 5000:
                    rows_inserted += self._flush_buckets()

                if posts_parsed % 500_000 == 0:
                    log.debug(
                        "  ... {n} lines parsed, {m} matched, {b} day-buckets",
                        n=posts_parsed, m=posts_matched, b=len(self._day_buckets),
                    )

        # Flush remaining
        rows_inserted += self._flush_buckets()

        return {
            "posts_parsed": posts_parsed,
            "posts_matched": posts_matched,
            "rows_inserted": rows_inserted,
        }

    # ── Ticker extraction ────────────────────────────────────────────

    @staticmethod
    def extract_ticker_mentions(text: str) -> dict[str, int]:
        """Find $TICKER and known ticker mentions in text.

        Parameters:
            text: Raw post/comment text.

        Returns:
            Dict mapping ticker symbol to mention count.
        """
        counts: dict[str, int] = defaultdict(int)

        # Cashtag matches: $AAPL, $BTC, etc.
        for match in _CASHTAG_RE.finditer(text):
            counts[match.group(1)] += 1

        # Known ticker matches (word boundary, case-sensitive)
        for match in _KNOWN_RE.finditer(text):
            counts[match.group(1)] += 1

        return dict(counts)

    # ── Sentiment scoring ────────────────────────────────────────────

    @staticmethod
    def score_sentiment(text: str) -> dict[str, Any]:
        """Score text for bullish/bearish sentiment using keyword matching.

        Uses the same bull/bear keyword approach as social_sentiment.py.
        Also stores the raw text snippet for future FinBERT scoring.

        Parameters:
            text: Raw post/comment text.

        Returns:
            Dict with bullish_count, bearish_count, net_score, label,
            and text_snippet (first 512 chars for future model scoring).
        """
        lower = text.lower()
        bullish_count = sum(1 for w in BULLISH_WORDS if w in lower)
        bearish_count = sum(1 for w in BEARISH_WORDS if w in lower)

        net = bullish_count - bearish_count
        total = bullish_count + bearish_count

        if total == 0:
            label = "neutral"
            ratio = 0.5
        else:
            ratio = bullish_count / total
            label = "bullish" if ratio > 0.6 else "bearish" if ratio < 0.4 else "neutral"

        return {
            "bullish_count": bullish_count,
            "bearish_count": bearish_count,
            "net_score": net,
            "bull_ratio": round(ratio, 3),
            "label": label,
            "text_snippet": text[:512],  # preserved for future FinBERT
        }

    # ── Daily aggregation ────────────────────────────────────────────

    def aggregate_daily(
        self,
        subreddit: str,
        obs_date: str,
        posts: list[dict],
    ) -> dict[str, Any]:
        """Aggregate all posts for a subreddit on a given date.

        Parameters:
            subreddit: Lowercase subreddit name.
            obs_date: Date string (YYYY-MM-DD).
            posts: List of post records from the dump.

        Returns:
            Dict with series_id, value (net sentiment), and raw_payload.
        """
        all_ticker_counts: dict[str, int] = defaultdict(int)
        total_bullish = 0
        total_bearish = 0
        total_score = 0
        sample_titles: list[str] = []
        text_snippets: list[str] = []

        for post in posts:
            text_blob = f"{post.get('title', '')} {post.get('text_snippet', '')}".strip()

            # Ticker extraction
            tickers = self.extract_ticker_mentions(text_blob)
            for ticker, count in tickers.items():
                all_ticker_counts[ticker] += count

            # Sentiment
            sentiment = self.score_sentiment(text_blob)
            total_bullish += sentiment["bullish_count"]
            total_bearish += sentiment["bearish_count"]
            total_score += post.get("score", 0)

            # Sample titles (top by score)
            if post.get("title"):
                sample_titles.append(post["title"][:200])

            # Store snippets for future FinBERT (limited to save space)
            if len(text_snippets) < 20 and sentiment["text_snippet"]:
                text_snippets.append(sentiment["text_snippet"])

        # Compute normalized net sentiment: (bull - bear) / (bull + bear)
        total_signals = total_bullish + total_bearish
        if total_signals > 0:
            net_sentiment = (total_bullish - total_bearish) / total_signals
        else:
            net_sentiment = 0.0

        # Top tickers by mention count
        top_tickers = sorted(
            all_ticker_counts.items(), key=lambda x: x[1], reverse=True
        )[:20]

        # Sample titles — pick top-scored ones
        sample_titles_sorted = sample_titles[:10]

        series_id = f"pushshift.{subreddit}.daily_sentiment"

        raw_payload = {
            "post_count": len(posts),
            "mention_counts": dict(top_tickers),
            "top_tickers": [t[0] for t in top_tickers[:10]],
            "avg_score": round(total_score / len(posts), 2) if posts else 0,
            "bullish_total": total_bullish,
            "bearish_total": total_bearish,
            "sample_titles": sample_titles_sorted,
            "text_snippets": text_snippets,  # for future FinBERT
        }

        return {
            "series_id": series_id,
            "obs_date": obs_date,
            "value": round(net_sentiment, 6),
            "raw_payload": raw_payload,
        }

    # ── Internal helpers ─────────────────────────────────────────────

    def _flush_buckets(self) -> int:
        """Aggregate and insert all accumulated day-buckets into raw_series.

        Returns:
            Number of rows inserted.
        """
        if not self._day_buckets:
            return 0

        rows_inserted = 0
        buckets = dict(self._day_buckets)
        self._day_buckets.clear()

        # Pre-fetch existing dates per series to avoid per-row dedup queries
        series_existing: dict[str, set[date]] = {}

        with self.engine.begin() as conn:
            for (subreddit, obs_date_str), posts in buckets.items():
                agg = self.aggregate_daily(subreddit, obs_date_str, posts)
                series_id = agg["series_id"]

                # Lazy-load existing dates for this series
                if series_id not in series_existing:
                    series_existing[series_id] = self._get_existing_dates(
                        series_id, conn
                    )

                obs_d = date.fromisoformat(obs_date_str)
                if obs_d in series_existing[series_id]:
                    continue

                self._insert_raw(
                    conn=conn,
                    series_id=agg["series_id"],
                    obs_date=obs_d,
                    value=agg["value"],
                    raw_payload=agg["raw_payload"],
                )
                series_existing[series_id].add(obs_d)
                rows_inserted += 1

        log.debug("Flushed {n} daily aggregates to raw_series", n=rows_inserted)
        return rows_inserted


class _LineReader:
    """Stream lines from a zstandard reader without loading entire file.

    Reads fixed-size chunks and yields complete lines.  Handles lines
    split across chunk boundaries.
    """

    def __init__(self, reader: Any, read_size: int = 2 ** 17) -> None:
        self._reader = reader
        self._read_size = read_size
        self._buf = b""
        self._exhausted = False

    def __iter__(self):
        return self

    def __next__(self) -> str:
        while True:
            # Check if we have a complete line in the buffer
            newline_pos = self._buf.find(b"\n")
            if newline_pos != -1:
                line = self._buf[:newline_pos]
                self._buf = self._buf[newline_pos + 1:]
                return line.decode("utf-8", errors="replace")

            # Need more data
            if self._exhausted:
                if self._buf:
                    line = self._buf
                    self._buf = b""
                    return line.decode("utf-8", errors="replace")
                raise StopIteration

            chunk = self._reader.read(self._read_size)
            if not chunk:
                self._exhausted = True
            else:
                self._buf += chunk


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    from db import get_engine

    if len(sys.argv) < 2:
        print("Usage: python -m ingestion.altdata.pushshift_reddit <path_to_dir_or_file>")
        print()
        print("Examples:")
        print("  python -m ingestion.altdata.pushshift_reddit /data/pushshift/")
        print("  python -m ingestion.altdata.pushshift_reddit /data/pushshift/wallstreetbets_submissions.zst")
        sys.exit(1)

    target = Path(sys.argv[1])
    engine = get_engine()
    puller = PushshiftRedditPuller(db_engine=engine)

    if target.is_dir():
        result = puller.ingest_directory(target)
    elif target.is_file() and target.suffix == ".zst":
        result = puller.process_dump_file(target)
    else:
        print(f"Error: {target} is not a directory or .zst file")
        sys.exit(1)

    log.info("Result: {r}", r=result)
