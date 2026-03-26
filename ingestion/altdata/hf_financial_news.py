"""
GRID HuggingFace Financial News Multi-Source ingestion module.

Pulls financial news from the oliverwang15/financial-news-multisource
dataset on HuggingFace. This dataset contains 57M+ rows across 24
subsets spanning 1990-2025, covering Yahoo Finance, Reddit, NYT, and
other financial news sources.

Uses HuggingFace ``datasets`` library in streaming mode to avoid
blowing up memory. Rows are batched (1000 at a time) for efficient
insertion into raw_series.

Series stored:
- hf_news.{subset_name}: One series per subset (e.g., hf_news.yahoo_finance)
  value = sentiment score if available, else None
  raw_payload = {title, text_snippet, source, subset}

Data source:
- https://huggingface.co/datasets/oliverwang15/financial-news-multisource
"""

from __future__ import annotations

import json
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# ---- Dataset config ----
_HF_DATASET_ID: str = "oliverwang15/financial-news-multisource"

# Series ID prefix
_SERIES_PREFIX: str = "hf_news"

# Priority subsets (most valuable first)
PRIORITY_SUBSETS: list[str] = [
    "yahoo_finance",
    "reddit_finance",
    "nyt",
    "finsen",
]

# All known subsets (extend as needed)
ALL_SUBSETS: list[str] = PRIORITY_SUBSETS + [
    "bloomberg",
    "reuters",
    "cnbc",
    "guardian",
    "ft",
    "economist",
    "wsj",
    "marketwatch",
    "investopedia",
    "seekingalpha",
    "benzinga",
    "thestreet",
    "barrons",
    "fortune",
    "forbes",
    "bbc_business",
    "cnn_business",
    "ap_business",
    "nasdaq",
    "motley_fool",
]

# Text snippet length for raw_payload
_TEXT_SNIPPET_LEN: int = 500

# Batch size for DB inserts
_BATCH_SIZE: int = 1000

# Delay between subset downloads to be polite
_SUBSET_DELAY: float = 2.0

# Feature definitions
HF_NEWS_FEATURES: dict[str, str] = {
    "yahoo_finance": "Yahoo Finance news articles with sentiment",
    "reddit_finance": "Reddit finance community posts with sentiment",
    "nyt": "New York Times financial/business articles",
    "finsen": "FinSen financial sentiment dataset",
}


def _parse_date_field(raw_date: Any) -> date | None:
    """Parse various date formats found in the HF dataset.

    The dataset contains dates in multiple formats across subsets:
    YYYY-MM-DD, YYYY/MM/DD, Unix timestamps, datetime strings, etc.

    Parameters:
        raw_date: Raw date value from the dataset row.

    Returns:
        Parsed date or None if unparseable.
    """
    if raw_date is None:
        return None

    # Already a date or datetime
    if isinstance(raw_date, datetime):
        return raw_date.date()
    if isinstance(raw_date, date):
        return raw_date

    raw_str = str(raw_date).strip()
    if not raw_str:
        return None

    # Try ISO format first (YYYY-MM-DD)
    try:
        return date.fromisoformat(raw_str[:10])
    except (ValueError, IndexError):
        pass

    # Try common datetime formats
    for fmt in ("%Y/%m/%d", "%m/%d/%Y", "%d-%m-%Y", "%B %d, %Y"):
        try:
            return datetime.strptime(raw_str[:20], fmt).date()
        except (ValueError, IndexError):
            continue

    # Try unix timestamp (seconds)
    try:
        ts = float(raw_str)
        if 0 < ts < 3e10:  # reasonable range
            return datetime.fromtimestamp(ts, tz=timezone.utc).date()
    except (ValueError, OverflowError):
        pass

    return None


def _extract_sentiment(row: dict[str, Any]) -> float | None:
    """Extract a sentiment score from dataset row if available.

    Different subsets use different field names for sentiment.

    Parameters:
        row: Single row dict from the HF dataset.

    Returns:
        Float sentiment score or None.
    """
    for field in ("sentiment", "sentiment_score", "label", "score", "polarity"):
        val = row.get(field)
        if val is None:
            continue
        try:
            return float(val)
        except (ValueError, TypeError):
            # Map string labels to numeric
            if isinstance(val, str):
                label_map = {
                    "positive": 1.0,
                    "negative": -1.0,
                    "neutral": 0.0,
                    "bullish": 1.0,
                    "bearish": -1.0,
                }
                mapped = label_map.get(val.lower().strip())
                if mapped is not None:
                    return mapped
    return None


class HFFinancialNewsPuller(BasePuller):
    """Pulls financial news from HuggingFace financial-news-multisource.

    Streams data from the oliverwang15/financial-news-multisource dataset
    using the HuggingFace ``datasets`` library in streaming mode to keep
    memory usage bounded. Inserts in batches of 1000 rows.

    Features:
    - hf_news.yahoo_finance: Yahoo Finance news with sentiment
    - hf_news.reddit_finance: Reddit finance posts with sentiment
    - hf_news.nyt: New York Times financial articles
    - hf_news.finsen: FinSen financial sentiment data

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for hf_financial_news.
    """

    SOURCE_NAME: str = "hf_financial_news"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://huggingface.co/datasets/oliverwang15/financial-news-multisource",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "MED",
        "priority_rank": 45,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the HuggingFace Financial News puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        log.info(
            "HFFinancialNewsPuller initialised -- source_id={sid}",
            sid=self.source_id,
        )

    def _series_id(self, subset_name: str) -> str:
        """Build the full series_id for a subset.

        Parameters:
            subset_name: Subset name (e.g., 'yahoo_finance').

        Returns:
            Full series_id (e.g., 'hf_news.yahoo_finance').
        """
        return f"{_SERIES_PREFIX}.{subset_name}"

    def _insert_batch(
        self,
        conn: Any,
        rows: list[dict[str, Any]],
        series_id: str,
        existing_dates: set[date],
    ) -> int:
        """Insert a batch of rows into raw_series, skipping duplicates.

        Parameters:
            conn: Active database connection (within a transaction).
            rows: List of parsed row dicts with obs_date, value, raw_payload.
            series_id: Series identifier for these rows.
            existing_dates: Set of dates already in the database.

        Returns:
            Number of rows actually inserted.
        """
        inserted = 0
        for row in rows:
            obs_date = row["obs_date"]
            if obs_date in existing_dates:
                continue

            self._insert_raw(
                conn=conn,
                series_id=series_id,
                obs_date=obs_date,
                value=row["value"],
                raw_payload=row["raw_payload"],
            )
            existing_dates.add(obs_date)
            inserted += 1

        return inserted

    def pull_subset(
        self,
        subset_name: str,
        start_date: str | date | None = None,
    ) -> dict[str, Any]:
        """Pull one subset from the HuggingFace dataset.

        Uses streaming mode to avoid loading the entire subset into
        memory. Rows are batched for efficient DB insertion.

        Parameters:
            subset_name: Name of the dataset subset (e.g., 'yahoo_finance').
            start_date: Only ingest rows on or after this date. If None,
                        uses incremental mode (checks latest date in DB).

        Returns:
            dict with status, rows_inserted, subset.
        """
        try:
            from datasets import load_dataset
        except ImportError:
            log.error("datasets library not installed -- pip install datasets")
            return {
                "status": "FAILED",
                "rows_inserted": 0,
                "subset": subset_name,
                "error": "datasets library not installed",
            }

        sid = self._series_id(subset_name)

        # Determine start date for incremental pull
        if start_date is None:
            latest = self._get_latest_date(sid)
            if latest is not None:
                # Overlap by 1 day to catch late-arriving data
                start_date = latest - timedelta(days=1)
                log.info(
                    "HF news {s}: incremental from {d}",
                    s=subset_name,
                    d=start_date,
                )
            else:
                start_date = date(1990, 1, 1)
                log.info(
                    "HF news {s}: full pull from {d}",
                    s=subset_name,
                    d=start_date,
                )
        elif isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)

        # Load dataset in streaming mode
        log.info(
            "HF news: loading subset '{s}' in streaming mode",
            s=subset_name,
        )
        try:
            ds = load_dataset(
                _HF_DATASET_ID,
                name=subset_name,
                split="train",
                streaming=True,
                trust_remote_code=False,
            )
        except Exception as exc:
            log.error(
                "HF news: failed to load subset '{s}': {e}",
                s=subset_name,
                e=str(exc),
            )
            return {
                "status": "FAILED",
                "rows_inserted": 0,
                "subset": subset_name,
                "error": str(exc),
            }

        total_inserted = 0
        total_skipped = 0
        batch: list[dict[str, Any]] = []

        with self.engine.begin() as conn:
            existing_dates = self._get_existing_dates(sid, conn)

            for row in ds:
                # Parse date
                obs_date = _parse_date_field(
                    row.get("date") or row.get("Date") or row.get("timestamp")
                )
                if obs_date is None:
                    total_skipped += 1
                    continue

                if obs_date < start_date:
                    continue

                # Extract fields
                title = str(row.get("title") or row.get("Title") or "")[:500]
                text = str(row.get("text") or row.get("Text") or row.get("content") or "")
                text_snippet = text[:_TEXT_SNIPPET_LEN] if text else ""
                source = str(row.get("source") or row.get("Source") or subset_name)
                sentiment = _extract_sentiment(row)

                batch.append({
                    "obs_date": obs_date,
                    "value": sentiment,
                    "raw_payload": {
                        "title": title,
                        "text_snippet": text_snippet,
                        "source": source,
                        "subset": subset_name,
                    },
                })

                # Flush batch
                if len(batch) >= _BATCH_SIZE:
                    inserted = self._insert_batch(conn, batch, sid, existing_dates)
                    total_inserted += inserted
                    batch.clear()

                    if total_inserted > 0 and total_inserted % 10000 == 0:
                        log.info(
                            "HF news {s}: {n} rows inserted so far",
                            s=subset_name,
                            n=total_inserted,
                        )

            # Flush remaining
            if batch:
                inserted = self._insert_batch(conn, batch, sid, existing_dates)
                total_inserted += inserted
                batch.clear()

        log.info(
            "HF news {s}: complete -- {n} rows inserted, {sk} skipped (no date)",
            s=subset_name,
            n=total_inserted,
            sk=total_skipped,
        )

        return {
            "status": "SUCCESS",
            "rows_inserted": total_inserted,
            "subset": subset_name,
            "skipped_no_date": total_skipped,
        }

    def pull_all(
        self,
        subsets: list[str] | None = None,
        start_date: str | date | None = None,
    ) -> list[dict[str, Any]]:
        """Pull all priority subsets (or a custom list).

        Iterates subsets sequentially with a delay between each to avoid
        hammering the HuggingFace servers.

        Parameters:
            subsets: List of subset names. Defaults to PRIORITY_SUBSETS.
            start_date: Passed through to pull_subset for date filtering.

        Returns:
            List of result dicts (one per subset).
        """
        if subsets is None:
            subsets = PRIORITY_SUBSETS

        results: list[dict[str, Any]] = []

        for i, subset_name in enumerate(subsets):
            log.info(
                "HF news: pulling subset {i}/{n} -- {s}",
                i=i + 1,
                n=len(subsets),
                s=subset_name,
            )

            result = self.pull_subset(
                subset_name=subset_name,
                start_date=start_date,
            )
            results.append(result)

            # Rate limit between subsets
            if i < len(subsets) - 1:
                time.sleep(_SUBSET_DELAY)

        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        total_rows = sum(r["rows_inserted"] for r in results)
        log.info(
            "HF news pull_all -- {ok}/{total} subsets, {rows} rows total",
            ok=succeeded,
            total=len(results),
            rows=total_rows,
        )
        return results
