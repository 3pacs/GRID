"""
GRID Federal Reserve communications ingestion module.

Pulls and scores Federal Reserve speeches and FOMC communications using
simple keyword-based NLP (no LLM dependency). Fed communications are
among the most market-moving events — hawkish/dovish tone shifts can
precede rate decisions by weeks.

Data source: https://www.federalreserve.gov/json/ne-speeches.json (public)
FOMC calendar: hardcoded schedule (updated annually).
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# Hawkish / dovish keyword dictionaries for scoring
_HAWKISH_WORDS: list[str] = [
    "inflation", "tighten", "tightening", "restrictive", "vigilant",
    "persistent", "overheating", "price stability", "above target",
    "upside risk", "strong labor", "robust demand", "rate increase",
    "further increase", "higher for longer", "insufficiently restrictive",
    "wage pressure", "excess demand",
]

_DOVISH_WORDS: list[str] = [
    "accommodate", "accommodative", "support", "supportive", "patience",
    "gradual", "slowdown", "downside risk", "below target", "labor market softening",
    "easing", "rate cut", "disinflation", "progress", "balanced risk",
    "sufficient progress", "data dependent", "appropriate time",
]

# FOMC meeting dates for 2024-2026 (8 per year, scheduled in advance)
# Each tuple is (meeting_end_date, statement_release_date)
_FOMC_DATES: list[str] = [
    # 2024
    "2024-01-31", "2024-03-20", "2024-05-01", "2024-06-12",
    "2024-07-31", "2024-09-18", "2024-11-07", "2024-12-18",
    # 2025
    "2025-01-29", "2025-03-19", "2025-05-07", "2025-06-18",
    "2025-07-30", "2025-09-17", "2025-10-29", "2025-12-17",
    # 2026
    "2026-01-28", "2026-03-18", "2026-05-06", "2026-06-17",
    "2026-07-29", "2026-09-16", "2026-10-28", "2026-12-16",
]

# Rate limiting
_RATE_LIMIT_DELAY: float = 1.0
_REQUEST_TIMEOUT: int = 30

# Fed speech JSON endpoint
_SPEECHES_URL: str = "https://www.federalreserve.gov/json/ne-speeches.json"


class FedSpeechPuller(BasePuller):
    """Pulls and scores Federal Reserve communications.

    Data source: https://www.federalreserve.gov/json/ne-speeches.json (public)

    Features:
    - fomc_hawkish_score: NLP-derived hawkishness (-1 to 1) from latest statement
    - fomc_days_since_meeting: trading days since last FOMC decision
    - fomc_days_to_meeting: trading days until next FOMC decision
    - fed_speech_frequency: count of Fed speeches in last 7 days
    - fed_tone_7d_avg: rolling 7-day average hawkishness score

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for Fed speeches.
    """

    SOURCE_NAME: str = "FedSpeeches"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://www.federalreserve.gov",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 15,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the Fed speech puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        self._fomc_dates = [date.fromisoformat(d) for d in _FOMC_DATES]
        log.info(
            "FedSpeechPuller initialised — source_id={sid}", sid=self.source_id
        )

    @staticmethod
    def score_text(text_content: str) -> float:
        """Score text on a hawkish-dovish scale using keyword counting.

        Parameters:
            text_content: Raw text to score.

        Returns:
            Float from -1 (very dovish) to +1 (very hawkish).
            Returns 0.0 if no keywords found.
        """
        lower = text_content.lower()

        hawkish_count = sum(1 for w in _HAWKISH_WORDS if w in lower)
        dovish_count = sum(1 for w in _DOVISH_WORDS if w in lower)
        total = hawkish_count + dovish_count

        if total == 0:
            return 0.0

        score = (hawkish_count - dovish_count) / total
        # Clamp to [-1, 1]
        return max(-1.0, min(1.0, score))

    def _trading_days_between(self, d1: date, d2: date) -> int:
        """Count approximate trading days between two dates.

        Parameters:
            d1: Start date.
            d2: End date.

        Returns:
            Number of weekdays between d1 and d2 (absolute value).
        """
        if d1 > d2:
            d1, d2 = d2, d1
        count = 0
        current = d1
        while current < d2:
            current += timedelta(days=1)
            if current.weekday() < 5:  # Mon-Fri
                count += 1
        return count

    def _days_since_last_fomc(self, ref_date: date) -> int:
        """Trading days since the most recent FOMC meeting on or before ref_date.

        Parameters:
            ref_date: Reference date.

        Returns:
            Trading days since last FOMC meeting, or 999 if none found.
        """
        past = [d for d in self._fomc_dates if d <= ref_date]
        if not past:
            return 999
        last = max(past)
        return self._trading_days_between(last, ref_date)

    def _days_to_next_fomc(self, ref_date: date) -> int:
        """Trading days until the next FOMC meeting after ref_date.

        Parameters:
            ref_date: Reference date.

        Returns:
            Trading days until next FOMC meeting, or 999 if none found.
        """
        future = [d for d in self._fomc_dates if d > ref_date]
        if not future:
            return 999
        nxt = min(future)
        return self._trading_days_between(ref_date, nxt)

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _fetch_speeches(self) -> list[dict[str, Any]]:
        """Fetch the Fed speeches JSON feed.

        Returns:
            List of speech dicts with 'd' (date) and 't' (title) keys.

        Raises:
            requests.RequestException: On HTTP errors.
        """
        headers = {
            "User-Agent": "GRID-DataPuller/1.0",
            "Accept": "application/json",
        }
        resp = requests.get(
            _SPEECHES_URL, headers=headers, timeout=_REQUEST_TIMEOUT
        )
        resp.raise_for_status()

        data = resp.json()
        # The feed may be a list directly or nested under a key
        if isinstance(data, dict):
            # Try common keys
            for key in ("speeches", "items", "data"):
                if key in data:
                    return data[key]
            # Return values of first list-type value
            for v in data.values():
                if isinstance(v, list):
                    return v
        elif isinstance(data, list):
            return data

        log.warning("Unexpected Fed speeches JSON structure")
        return []

    def _parse_speech_date(self, speech: dict[str, Any]) -> date | None:
        """Extract and parse the date from a speech dict.

        Parameters:
            speech: Raw speech dict from the JSON feed.

        Returns:
            Parsed date or None if unparseable.
        """
        # Try common date field names
        for key in ("d", "date", "Date", "speechDate"):
            raw = speech.get(key)
            if raw:
                try:
                    # Handle various formats
                    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%B %d, %Y"):
                        try:
                            return datetime.strptime(str(raw).strip(), fmt).date()
                        except ValueError:
                            continue
                    # Try pandas as last resort
                    parsed = pd.to_datetime(str(raw), errors="coerce")
                    if pd.notna(parsed):
                        return parsed.date()
                    else:
                        log.warning(
                            "Fed speech date coerced to NaT: {v}", v=raw
                        )
                except Exception:
                    continue
        return None

    def _get_speech_text(self, speech: dict[str, Any]) -> str:
        """Extract scoreable text from a speech dict.

        Uses title + description/summary fields (full text is not in the feed).

        Parameters:
            speech: Raw speech dict.

        Returns:
            Combined text for NLP scoring.
        """
        parts = []
        for key in ("t", "title", "Title", "s", "summary", "description"):
            val = speech.get(key)
            if val and isinstance(val, str):
                parts.append(val)
        return " ".join(parts)

    def pull_all(
        self,
        start_date: str | date = "2015-01-01",
        days_back: int = 365,
    ) -> list[dict[str, Any]]:
        """Pull all Fed speech features and FOMC calendar signals.

        Parameters:
            start_date: Earliest date to store.
            days_back: Number of days back to process.

        Returns:
            List of result dicts per feature type.
        """
        results: list[dict[str, Any]] = []

        # --- Fetch and score speeches ---
        try:
            speeches = self._fetch_speeches()
        except Exception as exc:
            log.error("Failed to fetch Fed speeches: {e}", e=str(exc))
            speeches = []

        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)

        cutoff = date.today() - timedelta(days=days_back)
        effective_start = max(start_date, cutoff)

        # Parse and score each speech
        scored_speeches: list[dict[str, Any]] = []
        for speech in speeches:
            speech_date = self._parse_speech_date(speech)
            if speech_date is None or speech_date < effective_start:
                continue
            speech_text = self._get_speech_text(speech)
            if not speech_text.strip():
                continue
            score = self.score_text(speech_text)
            scored_speeches.append({
                "date": speech_date,
                "score": score,
                "text": speech_text[:200],
            })

        # Group by date for aggregation
        by_date: dict[date, list[float]] = {}
        for s in scored_speeches:
            by_date.setdefault(s["date"], []).append(s["score"])

        # --- Store per-speech hawkish scores (daily average) ---
        score_rows = 0
        try:
            with self.engine.begin() as conn:
                for obs_date, scores in sorted(by_date.items()):
                    avg_score = float(np.mean(scores))

                    if self._row_exists("fomc_hawkish_score", obs_date, conn):
                        continue

                    self._insert_raw(
                        conn=conn,
                        series_id="fomc_hawkish_score",
                        obs_date=obs_date,
                        value=avg_score,
                        raw_payload={"n_speeches": len(scores)},
                    )
                    score_rows += 1
        except Exception as exc:
            log.error("Fed hawkish score insert failed: {e}", e=str(exc))

        results.append({
            "feature": "fomc_hawkish_score",
            "status": "SUCCESS" if score_rows > 0 or not scored_speeches else "NO_DATA",
            "rows_inserted": score_rows,
        })

        # --- Store FOMC calendar signals (for each day in range) ---
        cal_rows = 0
        freq_rows = 0
        tone_rows = 0
        try:
            with self.engine.begin() as conn:
                current = effective_start
                while current <= date.today():
                    # Skip weekends
                    if current.weekday() >= 5:
                        current += timedelta(days=1)
                        continue

                    # fomc_days_since_meeting
                    days_since = self._days_since_last_fomc(current)
                    if not self._row_exists("fomc_days_since_meeting", current, conn):
                        self._insert_raw(
                            conn=conn,
                            series_id="fomc_days_since_meeting",
                            obs_date=current,
                            value=float(days_since),
                        )
                        cal_rows += 1

                    # fomc_days_to_meeting
                    days_to = self._days_to_next_fomc(current)
                    if not self._row_exists("fomc_days_to_meeting", current, conn):
                        self._insert_raw(
                            conn=conn,
                            series_id="fomc_days_to_meeting",
                            obs_date=current,
                            value=float(days_to),
                        )
                        cal_rows += 1

                    # fed_speech_frequency: count of speeches in prior 7 days
                    window_start = current - timedelta(days=7)
                    speech_count = sum(
                        len(scores)
                        for d, scores in by_date.items()
                        if window_start <= d <= current
                    )
                    if not self._row_exists("fed_speech_frequency", current, conn):
                        self._insert_raw(
                            conn=conn,
                            series_id="fed_speech_frequency",
                            obs_date=current,
                            value=float(speech_count),
                        )
                        freq_rows += 1

                    # fed_tone_7d_avg: rolling 7-day average hawkishness
                    window_scores = [
                        s
                        for d, scores in by_date.items()
                        if window_start <= d <= current
                        for s in scores
                    ]
                    if window_scores:
                        avg_tone = float(np.mean(window_scores))
                    else:
                        avg_tone = 0.0

                    if not self._row_exists("fed_tone_7d_avg", current, conn):
                        self._insert_raw(
                            conn=conn,
                            series_id="fed_tone_7d_avg",
                            obs_date=current,
                            value=avg_tone,
                            raw_payload={"n_speeches_7d": len(window_scores)},
                        )
                        tone_rows += 1

                    current += timedelta(days=1)

        except Exception as exc:
            log.error("Fed calendar signal insert failed: {e}", e=str(exc))

        results.append({
            "feature": "fomc_calendar",
            "status": "SUCCESS",
            "rows_inserted": cal_rows,
        })
        results.append({
            "feature": "fed_speech_frequency",
            "status": "SUCCESS",
            "rows_inserted": freq_rows,
        })
        results.append({
            "feature": "fed_tone_7d_avg",
            "status": "SUCCESS",
            "rows_inserted": tone_rows,
        })

        total_rows = score_rows + cal_rows + freq_rows + tone_rows
        log.info(
            "FedSpeech pull_all — {n} total rows ({score} scores, {cal} calendar, "
            "{freq} frequency, {tone} tone)",
            n=total_rows,
            score=score_rows,
            cal=cal_rows,
            freq=freq_rows,
            tone=tone_rows,
        )
        return results
