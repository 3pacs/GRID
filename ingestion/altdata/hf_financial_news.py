"""
GRID HuggingFace Financial News ingestion module.

Pulls financial news sentiment from publicly available HuggingFace datasets.
Primary: zeroshot/twitter-financial-news-sentiment (11.9K tweets, 3-class)
Fallback: takala/financial_phrasebank (4.8K sentences, 3-class)

Uses HuggingFace ``datasets`` library in streaming mode to avoid
blowing up memory. Rows are batched (1000 at a time) for efficient
insertion into raw_series.

Series stored:
- hf_news.{subset_name}: One series per subset
  value = sentiment score (1.0=positive/bullish, 0.0=neutral, -1.0=negative/bearish)
  raw_payload = {title, text_snippet, source, subset}

Data sources:
- https://huggingface.co/datasets/zeroshot/twitter-financial-news-sentiment
- https://huggingface.co/datasets/takala/financial_phrasebank
"""

from __future__ import annotations

import json
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# ---- Dataset config ----
_SERIES_PREFIX: str = "hf_news"

# Available datasets and their configs
# Ordered by value: large article datasets first, then sentiment-labeled sets.
DATASET_CONFIGS: dict[str, dict] = {
    # ── Large article corpora (100K+ rows each) ──────────────────────
    "bloomberg_financial_news": {
        "hf_id": "danidanou/Bloomberg_Financial_News",
        "subset": None,
        "text_field": "Article",
        "date_field": "Date",
        "title_field": "Headline",
        "label_field": None,  # no sentiment labels — mine with NLP
        "label_map": {},
        "split": "train",
    },
    "reuters_financial_articles": {
        "hf_id": "ashraq/financial-news-articles",
        "subset": None,
        "text_field": "text",
        "date_field": None,  # dates embedded in article text, parsed from content
        "title_field": "title",
        "label_field": None,
        "label_map": {},
        "split": "train",
        "per_article": True,  # store every article, not one per date
    },

    # ── Sentiment-labeled datasets ───────────────────────────────────
    "twitter_financial_sentiment": {
        "hf_id": "zeroshot/twitter-financial-news-sentiment",
        "subset": None,
        "text_field": "text",
        "date_field": None,
        "title_field": None,
        "label_field": "label",
        "label_map": {0: -1.0, 1: 0.0, 2: 1.0},  # Bearish/Neutral/Bullish
        "split": "train",
    },
    "twitter_financial_sentiment_val": {
        "hf_id": "zeroshot/twitter-financial-news-sentiment",
        "subset": None,
        "text_field": "text",
        "date_field": None,
        "title_field": None,
        "label_field": "label",
        "label_map": {0: -1.0, 1: 0.0, 2: 1.0},
        "split": "validation",
    },
    "twitter_financial_topic": {
        "hf_id": "zeroshot/twitter-financial-news-topic",
        "subset": None,
        "text_field": "text",
        "date_field": None,
        "title_field": None,
        "label_field": "label",
        "label_map": {},  # 20 topic categories — store raw
        "split": "train",
    },
    "twitter_financial_topic_val": {
        "hf_id": "zeroshot/twitter-financial-news-topic",
        "subset": None,
        "text_field": "text",
        "date_field": None,
        "title_field": None,
        "label_field": "label",
        "label_map": {},
        "split": "validation",
    },
}

# Priority order for pull_all — big corpora first
PRIORITY_SUBSETS: list[str] = [
    "bloomberg_financial_news",       # ~446K articles
    "reuters_financial_articles",     # ~300K articles
    "twitter_financial_sentiment",    # ~9.5K labeled
    "twitter_financial_sentiment_val",# ~2.4K labeled
    "twitter_financial_topic",        # ~17K topic-labeled
    "twitter_financial_topic_val",    # ~4K topic-labeled
]

# Text snippet length for raw_payload
_TEXT_SNIPPET_LEN: int = 500

# Batch size for DB inserts
_BATCH_SIZE: int = 5000  # bigger batches for large datasets

# Delay between subset downloads to be polite
_SUBSET_DELAY: float = 2.0

# Feature definitions
HF_NEWS_FEATURES: dict[str, str] = {
    "bloomberg_financial_news": "Bloomberg financial news 2006-2013 (446K articles with dates)",
    "reuters_financial_articles": "Reuters financial articles 2017-2023 (~300K with title+text)",
    "twitter_financial_sentiment": "Twitter financial sentiment (9.5K, Bearish/Neutral/Bullish)",
    "twitter_financial_sentiment_val": "Twitter financial sentiment validation (2.4K)",
    "twitter_financial_topic": "Twitter financial topic classification (17K, 20 categories)",
    "twitter_financial_topic_val": "Twitter financial topic validation (4K)",
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

    # Try extracting "Month DD, YYYY" from longer text (e.g. Reuters articles)
    import re
    month_match = re.search(
        r'((?:January|February|March|April|May|June|July|August|'
        r'September|October|November|December)\s+\d{1,2},?\s+\d{4})',
        raw_str[:100],
    )
    if month_match:
        date_str = month_match.group(1)
        for fmt in ("%B %d, %Y", "%B %d %Y"):
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
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
        per_article: bool = False,
    ) -> int:
        """Insert a batch of rows into raw_series, skipping duplicates.

        Parameters:
            conn: Active database connection (within a transaction).
            rows: List of parsed row dicts with obs_date, value, raw_payload.
            series_id: Series identifier for these rows.
            existing_dates: Set of dates already in the database.
            per_article: If True, create a unique series_id per article
                         (for large article datasets where we want every row).

        Returns:
            Number of rows actually inserted.
        """
        import hashlib

        inserted = 0
        for row in rows:
            obs_date = row["obs_date"]

            if per_article:
                # Unique series_id per article: hash of title+text+date
                title = row["raw_payload"].get("title", "")
                snippet = row["raw_payload"].get("text_snippet", "")
                article_hash = hashlib.md5(
                    f"{title}:{snippet[:100]}:{obs_date}".encode()
                ).hexdigest()[:12]
                row_sid = f"{series_id}.{article_hash}"

                conn.execute(
                    text(
                        "INSERT INTO raw_series "
                        "(series_id, source_id, obs_date, value, raw_payload, pull_status) "
                        "VALUES (:sid, :src, :od, :val, :payload, 'SUCCESS') "
                        "ON CONFLICT DO NOTHING"
                    ),
                    {
                        "sid": row_sid,
                        "src": self.source_id,
                        "od": obs_date,
                        "val": float(row["value"]) if row["value"] is not None else 0.0,
                        "payload": json.dumps(row["raw_payload"]),
                    },
                )
                inserted += 1
            else:
                if obs_date in existing_dates:
                    continue

                self._insert_raw(
                    conn=conn,
                    series_id=series_id,
                    obs_date=obs_date,
                    value=row["value"] if row["value"] is not None else 0.0,
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
        """Pull one dataset from HuggingFace using DATASET_CONFIGS.

        Uses streaming mode to avoid loading the entire dataset into
        memory. Rows are batched for efficient DB insertion.

        Parameters:
            subset_name: Config key from DATASET_CONFIGS.
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

        ds_cfg = DATASET_CONFIGS.get(subset_name)
        if not ds_cfg:
            return {
                "status": "FAILED",
                "rows_inserted": 0,
                "subset": subset_name,
                "error": f"Unknown dataset config: {subset_name}",
            }

        sid = self._series_id(subset_name)

        # Determine start date for incremental pull
        # Per-article datasets use hashed series_ids, so the base sid
        # doesn't reflect actual article dates — always full pull.
        is_per_article = ds_cfg.get("per_article", ds_cfg.get("date_field") is not None)
        if start_date is None:
            if is_per_article:
                start_date = date(1990, 1, 1)
                log.info(
                    "HF news {s}: full pull (per-article mode)",
                    s=subset_name,
                )
            else:
                latest = self._get_latest_date(sid)
                if latest is not None:
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
        hf_id = ds_cfg["hf_id"]
        hf_subset = ds_cfg.get("subset")
        hf_split = ds_cfg.get("split", "train")
        text_field = ds_cfg["text_field"]
        title_field = ds_cfg.get("title_field")
        date_field = ds_cfg.get("date_field")
        label_field = ds_cfg.get("label_field")
        label_map = ds_cfg.get("label_map", {})

        log.info(
            "HF news: loading '{hf}' subset='{sub}' split='{sp}'",
            hf=hf_id, sub=hf_subset, sp=hf_split,
        )
        try:
            load_kwargs: dict[str, Any] = {
                "path": hf_id,
                "split": hf_split,
                "streaming": True,
                "trust_remote_code": False,
            }
            if hf_subset:
                load_kwargs["name"] = hf_subset
            ds = load_dataset(**load_kwargs)
        except Exception as exc:
            log.error(
                "HF news: failed to load '{hf}': {e}",
                hf=hf_id, e=str(exc),
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

        # Per-article mode: store every article with unique ID (for large corpora)
        # Either explicitly set in config, or auto-enabled for datasets with dates
        has_dates = date_field is not None
        use_per_article = ds_cfg.get("per_article", has_dates)

        with self.engine.begin() as conn:
            # Per-article mode uses hashed sids — skip existing_dates check
            existing_dates = set() if use_per_article else self._get_existing_dates(sid, conn)

            for row in ds:
                # Parse date — use configured field first, then generic fallback
                obs_date = None
                if date_field:
                    obs_date = _parse_date_field(row.get(date_field))
                if obs_date is None:
                    obs_date = _parse_date_field(
                        row.get("date") or row.get("Date") or row.get("timestamp")
                    )
                # Try parsing date from article text (e.g., Reuters: "January 2, 2018 / 9:31 PM")
                if obs_date is None and use_per_article:
                    text_val_tmp = str(row.get(text_field, ""))[:100]
                    obs_date = _parse_date_field(text_val_tmp)
                if obs_date is None:
                    if not has_dates:
                        obs_date = date.today()
                    else:
                        total_skipped += 1
                        continue

                if obs_date < start_date:
                    continue

                # Extract text and title
                text_val = str(row.get(text_field, ""))
                text_snippet = text_val[:_TEXT_SNIPPET_LEN] if text_val else ""
                title_val = str(row.get(title_field, ""))[:300] if title_field else text_val[:200]

                # Extract sentiment via label map or fallback
                raw_label = row.get(label_field) if label_field else None
                if raw_label is not None and label_map:
                    sentiment = label_map.get(raw_label)
                    if sentiment is None:
                        sentiment = _extract_sentiment(row)
                else:
                    sentiment = _extract_sentiment(row)

                batch.append({
                    "obs_date": obs_date,
                    "value": sentiment,
                    "raw_payload": {
                        "title": title_val,
                        "text_snippet": text_snippet,
                        "source": hf_id,
                        "subset": subset_name,
                        "raw_label": raw_label,
                    },
                })

                # Flush batch
                # Large article datasets (with dates) get per-article IDs
                use_per_article = has_dates
                if len(batch) >= _BATCH_SIZE:
                    inserted = self._insert_batch(
                        conn, batch, sid, existing_dates,
                        per_article=use_per_article,
                    )
                    total_inserted += inserted
                    batch.clear()

                    if total_inserted > 0 and total_inserted % 10000 == 0:
                        log.info(
                            "HF news {s}: {n:,d} rows inserted so far",
                            s=subset_name,
                            n=total_inserted,
                        )

            # Flush remaining
            if batch:
                inserted = self._insert_batch(
                    conn, batch, sid, existing_dates,
                    per_article=use_per_article,
                )
                total_inserted += inserted
                batch.clear()

        log.info(
            "HF news {s}: complete -- {n} rows inserted, {sk} skipped",
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
