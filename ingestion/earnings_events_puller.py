"""
SEC EDGAR + AlphaVantage earnings events puller (Task #151).

Canonical rollup of 10-K / 10-Q / 8-K filings AND earnings-call transcripts
per ticker. Writes to the ``earnings_events`` table on griddb.

Sources (in order of preference):
1. SEC EDGAR submissions JSON  (FREE, official, never-revised archive)
     https://data.sec.gov/submissions/CIK<10digit>.json
2. SEC EDGAR company_tickers.json (one-shot ticker -> CIK map at startup)
     https://www.sec.gov/files/company_tickers.json
3. AlphaVantage EARNINGS_CALL_TRANSCRIPT (free tier, 5 req/min)
     https://www.alphavantage.co/query?function=EARNINGS_CALL_TRANSCRIPT&symbol=AAPL&quarter=2024Q1

Why a separate puller instead of extending edgar_transcripts.py?
- edgar_transcripts.py writes to raw_series for milestone/guidance extraction
  (already wired through hermes_operator). This puller writes to the canonical
  earnings_events rollup that the catalyst-timeline UI, cross-class
  confirmation, and gem-hunter consume.
- edgar_transcripts.py is limited to 30 hardcoded mega-caps and 8-K only.
  This puller covers the watchlist + 10-K / 10-Q / 8-K and pulls the
  AlphaVantage transcript channel.

Rate limits:
- SEC EDGAR: 10 req/sec hard limit. We sleep 0.15s between requests
  AND set a stable, identifiable User-Agent per their fair-access policy.
- AlphaVantage free tier: 5 req/min, 500 req/day. We cap to 25 transcript
  requests per run by default and sleep 13s between calls.

Author: GRID intelligence platform.
"""

from __future__ import annotations

import os
import re
import time
from datetime import date, datetime, timedelta
from typing import Any, Iterable

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# LLM-based sentiment classifier (Task #175). Imported lazily inside
# _classify_transcript_sentiment so a misconfigured Ollama endpoint or a
# transient import error never breaks the rest of the puller. The
# classifier hits qwen2.5:14b on p9d via Ollama and returns
# label + confidence + signed score; falls back gracefully on failure.


SEC_TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik10}.json"
SEC_ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession_nodash}/{primary_doc}"
SEC_ARCHIVE_INDEX_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession_nodash}/"

AV_URL = "https://www.alphavantage.co/query"

# SEC EDGAR fair-access policy requires a stable User-Agent that identifies
# us. Per their docs, format is "<company> <email>".
DEFAULT_UA = "GRID Intelligence stepdadfinance@gmail.com"

# Form types we actually care about for earnings/material events.
INTERESTING_FORMS: frozenset[str] = frozenset({
    "10-K", "10-K/A",
    "10-Q", "10-Q/A",
    "8-K", "8-K/A",
    "20-F", "20-F/A",
    "40-F", "40-F/A",
    "S-1", "S-1/A",
    "S-3", "S-3/A",
    "S-4", "S-4/A",
    "DEF 14A",
})


def _normalize_form(form: str) -> str:
    """Strip /A amendments to bucket into our event_type CHECK list."""
    return form.split("/")[0].strip()


def _quarter_label(period_end: date | None) -> str | None:
    """Convert period_end date to fiscal-quarter label like '2024Q3'."""
    if period_end is None:
        return None
    return f"{period_end.year}Q{((period_end.month - 1) // 3) + 1}"


class EarningsEventsPuller(BasePuller):
    """Pull SEC EDGAR filings + AlphaVantage transcripts into earnings_events.

    Idempotent via the (ticker, event_type, filing_date, fiscal_quarter)
    UNIQUE constraint. Safe to re-run repeatedly.
    """

    SOURCE_NAME = "earnings_events"
    SOURCE_CONFIG = {
        "base_url": "https://data.sec.gov/submissions",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 12,
    }

    # ------------------------------------------------------------------
    # init / config
    # ------------------------------------------------------------------
    def __init__(
        self,
        db_engine: Engine,
        user_agent: str | None = None,
        av_api_key: str | None = None,
        av_max_calls: int = 25,
    ) -> None:
        super().__init__(db_engine)
        self.user_agent = user_agent or os.getenv("SEC_EDGAR_UA", DEFAULT_UA)
        self.av_api_key = av_api_key or os.getenv("ALPHAVANTAGE_API_KEY", "")
        self.av_max_calls = av_max_calls
        self._headers_sec = {
            "User-Agent": self.user_agent,
            "Accept": "application/json",
        }
        self._ticker_cik_map: dict[str, str] | None = None

    # ------------------------------------------------------------------
    # CIK map (one-shot at startup)
    # ------------------------------------------------------------------
    @retry_on_failure(max_attempts=3, retryable_exceptions=(ConnectionError, TimeoutError, OSError))
    def _sec_get(self, url: str) -> requests.Response:
        resp = requests.get(url, headers=self._headers_sec, timeout=30)
        time.sleep(0.15)  # 10 req/sec ceiling
        resp.raise_for_status()
        return resp

    def load_cik_map(self, force: bool = False) -> dict[str, str]:
        """Fetch SEC's master ticker -> CIK map (cached for the run).

        Format from SEC is ``{"0": {"cik_str": 320193, "ticker": "AAPL", ...}, ...}``.
        Returns a dict of UPPERCASE ticker -> zero-padded 10-digit CIK string.
        """
        if self._ticker_cik_map is not None and not force:
            return self._ticker_cik_map

        try:
            resp = self._sec_get(SEC_TICKER_MAP_URL)
            data = resp.json()
        except Exception as exc:
            log.warning("Failed to load SEC ticker map: {e}", e=str(exc))
            self._ticker_cik_map = {}
            return self._ticker_cik_map

        result: dict[str, str] = {}
        for _, row in data.items():
            try:
                ticker = str(row["ticker"]).upper()
                cik = str(row["cik_str"]).zfill(10)
                # First entry wins (some tickers share CIKs across share classes)
                result.setdefault(ticker, cik)
            except (KeyError, TypeError):
                continue
        self._ticker_cik_map = result
        log.info("Loaded SEC ticker->CIK map: {n} entries", n=len(result))
        return result

    # ------------------------------------------------------------------
    # EDGAR submissions per ticker
    # ------------------------------------------------------------------
    def fetch_submissions(self, cik10: str) -> dict[str, Any] | None:
        url = SEC_SUBMISSIONS_URL.format(cik10=cik10)
        try:
            resp = self._sec_get(url)
            return resp.json()
        except requests.HTTPError as exc:
            log.debug("EDGAR submissions {cik} -> {e}", cik=cik10, e=str(exc))
            return None
        except Exception as exc:
            log.warning("EDGAR submissions {cik} failed: {e}", cik=cik10, e=str(exc))
            return None

    def extract_filings(
        self,
        submissions: dict[str, Any],
        cutoff: date,
    ) -> list[dict[str, Any]]:
        """Extract recent interesting filings from submissions JSON.

        Iterates filings.recent (a column-oriented dict) and emits one
        dict per filing whose form is in INTERESTING_FORMS and filed
        date >= cutoff.
        """
        recent = (submissions.get("filings") or {}).get("recent") or {}
        forms = recent.get("form") or []
        dates = recent.get("filingDate") or []
        periods = recent.get("reportDate") or []
        accessions = recent.get("accessionNumber") or []
        primary_docs = recent.get("primaryDocument") or []
        primary_desc = recent.get("primaryDocDescription") or []
        items = recent.get("items") or []

        out: list[dict[str, Any]] = []
        for i, form in enumerate(forms):
            norm = _normalize_form(form)
            if norm not in {"10-K", "10-Q", "8-K", "20-F", "40-F",
                            "S-1", "S-3", "S-4", "DEF 14A"}:
                continue
            try:
                filed = datetime.strptime(dates[i], "%Y-%m-%d").date()
            except Exception:
                continue
            if filed < cutoff:
                continue
            try:
                period = datetime.strptime(periods[i], "%Y-%m-%d").date() if periods[i] else None
            except Exception:
                period = None
            out.append({
                "form": form,
                "norm_form": norm,
                "filed": filed,
                "period": period,
                "accession": accessions[i] if i < len(accessions) else None,
                "primary_doc": primary_docs[i] if i < len(primary_docs) else None,
                "description": primary_desc[i] if i < len(primary_desc) else None,
                "items": items[i] if i < len(items) else None,
            })
        return out

    # ------------------------------------------------------------------
    # AlphaVantage earnings call transcripts
    # ------------------------------------------------------------------
    def fetch_av_transcript(self, ticker: str, quarter: str) -> dict[str, Any] | None:
        """Fetch a single earnings call transcript from AlphaVantage.

        Returns the parsed JSON or None if quota exceeded / no data.
        """
        if not self.av_api_key:
            return None
        params = {
            "function": "EARNINGS_CALL_TRANSCRIPT",
            "symbol": ticker,
            "quarter": quarter,
            "apikey": self.av_api_key,
        }
        try:
            resp = requests.get(AV_URL, params=params, timeout=30)
            time.sleep(13.0)  # AlphaVantage free tier 5 req/min
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            log.debug("AV transcript {t} {q} failed: {e}", t=ticker, q=quarter, e=str(exc))
            return None

        if not isinstance(data, dict):
            return None
        # AV returns {"Information": "..."} on quota exhaustion
        if "Information" in data and "transcript" not in data:
            log.warning("AV quota exhausted: {msg}", msg=data["Information"][:120])
            return None
        if "Note" in data and "transcript" not in data:
            log.warning("AV throttled: {msg}", msg=str(data["Note"])[:120])
            return None
        if not data.get("transcript"):
            return None
        return data

    @staticmethod
    def _summarize_transcript(transcript_entries: list[dict[str, Any]],
                              max_chars: int = 1200) -> tuple[str, str, str]:
        """Build a compact summary, full_text concat, and rough AV-derived label.

        This is the cheap pass: averages AV's per-entry sentiment numbers
        into a coarse positive/neutral/negative label. The LLM classifier
        (_classify_transcript_sentiment) runs alongside it at insert time
        and overwrites label + confidence with higher-quality values.
        Kept as a fallback for the case where the LLM endpoint is down.
        """
        sentiments: list[float] = []
        chunks: list[str] = []
        for e in transcript_entries:
            content = str(e.get("content", "")).strip()
            if content:
                chunks.append(content)
            try:
                s = float(e.get("sentiment", "0"))
                sentiments.append(s)
            except (ValueError, TypeError):
                pass

        full_text = "\n\n".join(chunks)
        summary = full_text[:max_chars]
        if sentiments:
            avg = sum(sentiments) / len(sentiments)
            if avg > 0.15:
                label = "positive"
            elif avg < -0.15:
                label = "negative"
            else:
                label = "neutral"
        else:
            label = "neutral"
        return summary, full_text, label

    @staticmethod
    def _classify_transcript_sentiment(full_text: str) -> tuple[str | None, float | None]:
        """Run the LLM sentiment classifier on a full transcript / filing text.

        Returns (label, confidence). label is "positive" / "neutral" /
        "negative" to match the existing column convention. Returns
        (None, None) if the classifier is unreachable or the text is
        empty, so callers can fall back to the AV-derived label.

        Task #175 — wires the +0.2 cross-class confirmation leg for the
        filings lane (#167) by giving it a real |score| signal instead
        of the previous flat confidence 0.8 across every call.
        """
        if not full_text or not full_text.strip():
            return None, None
        try:
            # Lazy import to keep the puller boot fast and avoid pulling
            # the LLM client into test runs that don't need it.
            from ingestion.ml.earnings_sentiment_llm import classify_or_none
        except Exception as exc:  # pragma: no cover - import smoke
            log.warning("sentiment classifier import failed: {e}", e=exc)
            return None, None
        try:
            label, confidence, _ = classify_or_none(full_text)
            return label, confidence
        except Exception as exc:  # pragma: no cover - network / model
            log.warning("sentiment classifier raised: {e}", e=str(exc)[:200])
            return None, None

    # ------------------------------------------------------------------
    # Upsert
    # ------------------------------------------------------------------
    def _upsert_event(
        self,
        *,
        ticker: str,
        cik: str | None,
        event_type: str,
        filing_date: date,
        period_end: date | None,
        fiscal_quarter: str | None,
        accession: str | None,
        url: str | None,
        title: str | None,
        summary: str | None,
        full_text: str | None,
        sentiment: str | None,
        confidence: float | None,
        raw_payload: dict[str, Any] | None,
        source: str = "SEC_EDGAR",
    ) -> bool:
        """Insert one row, idempotent on the UNIQUE constraint."""
        import json as _json
        sql = """
            INSERT INTO earnings_events
                (ticker, cik, event_type, filing_date, period_end,
                 fiscal_quarter, accession, url, title, summary,
                 full_text, sentiment, confidence, raw_payload, source)
            VALUES
                (:ticker, :cik, :event_type, :filing_date, :period_end,
                 :fiscal_quarter, :accession, :url, :title, :summary,
                 :full_text, :sentiment, :confidence,
                 CAST(:raw_payload AS JSONB), :source)
            ON CONFLICT (ticker, event_type, filing_date, fiscal_quarter)
            DO UPDATE SET
                cik           = EXCLUDED.cik,
                period_end    = COALESCE(EXCLUDED.period_end, earnings_events.period_end),
                accession     = COALESCE(EXCLUDED.accession,  earnings_events.accession),
                url           = COALESCE(EXCLUDED.url,        earnings_events.url),
                title         = COALESCE(EXCLUDED.title,      earnings_events.title),
                summary       = COALESCE(EXCLUDED.summary,    earnings_events.summary),
                full_text     = COALESCE(EXCLUDED.full_text,  earnings_events.full_text),
                sentiment     = COALESCE(EXCLUDED.sentiment,  earnings_events.sentiment),
                confidence    = COALESCE(EXCLUDED.confidence, earnings_events.confidence),
                raw_payload   = COALESCE(EXCLUDED.raw_payload, earnings_events.raw_payload),
                source        = EXCLUDED.source
            RETURNING (xmax = 0) AS inserted
        """
        with self.engine.begin() as conn:
            row = conn.execute(text(sql), {
                "ticker": ticker,
                "cik": cik,
                "event_type": event_type,
                "filing_date": filing_date,
                "period_end": period_end,
                "fiscal_quarter": fiscal_quarter,
                "accession": accession,
                "url": url,
                "title": title,
                "summary": summary,
                "full_text": full_text,
                "sentiment": sentiment,
                "confidence": confidence,
                "raw_payload": _json.dumps(raw_payload) if raw_payload is not None else None,
                "source": source,
            }).fetchone()
        return bool(row[0]) if row else False

    # ------------------------------------------------------------------
    # Ticker universe
    # ------------------------------------------------------------------
    def _load_universe(
        self,
        override: list[str] | None,
        *,
        ticker_source: str = "signal_registry",
        lookback_days: int = 120,
        min_signals: int = 3,
        max_tickers: int = 1000,
    ) -> list[str]:
        """Resolve the ticker universe for this run.

        Task #185 (2026-05-17): the legacy hard-coded ``watchlist`` table
        only had 21 tickers, so earnings_events covered <10% of the gem
        universe. We now default to ``signal_registry`` which unions the
        watchlist + gem_alerts backtest set + recently active signal
        tickers, expanding coverage from ~21 -> ~580 names.

        Args:
            override: Explicit ticker list (from CLI ``--tickers``).
                Wins over everything else.
            ticker_source: ``"signal_registry"`` (new default), or
                ``"watchlist"`` for legacy behaviour.
            lookback_days, min_signals, max_tickers: passed through to
                ``watchlist_resolver.resolve_universe``.
        """
        from ingestion.watchlist_resolver import resolve_universe

        return resolve_universe(
            self.engine,
            source="cli" if override else ticker_source,
            cli_tickers=override,
            lookback_days=lookback_days,
            min_signals=min_signals,
            max_tickers=max_tickers,
        )

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # Incremental-mode state (Task #182 followup, 2026-05-17)
    # ------------------------------------------------------------------
    #
    # AlphaVantage's free-tier transcript budget is 500 req/day. With a
    # ~580-ticker universe a "full" pass blows past the cap on day 1,
    # then the rest of the universe never gets pulled. ``incremental``
    # mode keeps a per-ticker freshness check (the max filing_date for
    # ``EARNINGS_CALL`` rows already in ``earnings_events``) and only
    # spends AV calls on tickers whose freshest transcript is older
    # than ``incremental_max_age_days``. Daily call budget capped via
    # ``av_max_calls`` (default 400 — leaves 100 budget for retries
    # and the SEC sentiment LLM fallback).
    INCREMENTAL_DEFAULT_MAX_AGE_DAYS = 90

    def _load_last_transcript_dates(
        self,
        tickers: list[str],
    ) -> dict[str, date]:
        """Return ``{ticker: max(filing_date)}`` for EARNINGS_CALL rows.

        Used in incremental mode to skip tickers we've already pulled
        recently. A ticker missing from the result has never had a
        transcript stored and must be pulled.
        """
        if not tickers:
            return {}
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT ticker, MAX(filing_date) "
                        "FROM earnings_events "
                        "WHERE event_type = 'EARNINGS_CALL' "
                        "  AND source = 'ALPHAVANTAGE' "
                        "  AND ticker = ANY(:tk) "
                        "GROUP BY ticker"
                    ),
                    {"tk": list(tickers)},
                ).fetchall()
            return {r[0]: r[1] for r in rows if r[0] and r[1]}
        except Exception as exc:
            log.warning(
                "earnings_events: last-transcript lookup failed ({e}) — "
                "treating all tickers as stale (full pull)",
                e=str(exc),
            )
            return {}

    def pull(
        self,
        tickers: list[str] | None = None,
        days_back: int = 90,
        include_transcripts: bool = True,
        ticker_source: str = "signal_registry",
        lookback_days: int = 120,
        min_signals: int = 3,
        max_tickers: int = 1000,
        mode: str = "incremental",
        incremental_max_age_days: int = INCREMENTAL_DEFAULT_MAX_AGE_DAYS,
    ) -> dict[str, Any]:
        """Pull recent filings + transcripts for the given tickers.

        Args:
            tickers: Override the resolved universe. Uppercase US tickers.
            days_back: How many days of filings to backfill (default 90).
            include_transcripts: If True and ALPHAVANTAGE_API_KEY is set,
                also pull earnings-call transcripts.
            ticker_source: Where to resolve the universe from when
                ``tickers`` is not supplied (Task #185). One of
                ``"signal_registry"`` (default, ~580 tickers) or
                ``"watchlist"`` (legacy ~21 tickers).
            lookback_days, min_signals, max_tickers: tuning knobs for
                signal_registry resolution.
            mode: ``"incremental"`` (default) only spends AlphaVantage
                transcript calls on tickers whose most recent stored
                EARNINGS_CALL filing_date is older than
                ``incremental_max_age_days`` (or absent). ``"full"``
                attempts every ticker (subject to ``av_max_calls``).
                SEC EDGAR filings ALWAYS run for every ticker regardless
                of mode — they're idempotent and cheap.
            incremental_max_age_days: A ticker is considered fresh (and
                its transcript pull is skipped) when its newest stored
                EARNINGS_CALL is within this many days of today.
                Default 90 — one quarter, which lines up with the
                cadence of new earnings calls.

        Returns:
            Summary dict with per-ticker counts and totals.
        """
        universe = self._load_universe(
            tickers,
            ticker_source=ticker_source,
            lookback_days=lookback_days,
            min_signals=min_signals,
            max_tickers=max_tickers,
        )
        cik_map = self.load_cik_map()
        cutoff = date.today() - timedelta(days=days_back)

        # Build the AV-eligible subset based on mode + freshness.
        mode_norm = (mode or "incremental").lower()
        if mode_norm not in ("incremental", "full"):
            log.warning(
                "earnings_events: unknown mode '{m}' — defaulting to incremental",
                m=mode,
            )
            mode_norm = "incremental"

        if include_transcripts and mode_norm == "incremental":
            last_dates = self._load_last_transcript_dates(universe)
            fresh_cutoff = date.today() - timedelta(days=incremental_max_age_days)
            av_eligible: set[str] = {
                t for t in universe
                if last_dates.get(t) is None or last_dates[t] < fresh_cutoff
            }
            log.info(
                "earnings_events incremental: {n}/{m} tickers stale "
                "(>{d}d) — will spend up to {b} AV calls",
                n=len(av_eligible), m=len(universe),
                d=incremental_max_age_days, b=self.av_max_calls,
            )
        else:
            av_eligible = set(universe)
            if include_transcripts:
                log.info(
                    "earnings_events full mode: every ticker AV-eligible "
                    "({n} tickers, cap {b} calls)",
                    n=len(universe), b=self.av_max_calls,
                )

        filings_seen = 0
        filings_inserted = 0
        transcripts_inserted = 0
        per_ticker: dict[str, dict[str, int]] = {}
        av_calls = 0
        av_skipped_fresh = 0

        for ticker in universe:
            cik = cik_map.get(ticker)
            counts = {"filings_inserted": 0, "filings_seen": 0,
                      "transcripts_inserted": 0, "cik": cik or ""}

            if cik:
                subs = self.fetch_submissions(cik)
                if subs:
                    filings = self.extract_filings(subs, cutoff)
                    counts["filings_seen"] = len(filings)
                    filings_seen += len(filings)

                    for f in filings:
                        norm = f["norm_form"]
                        if norm not in {"10-K", "10-Q", "8-K", "20-F",
                                        "40-F", "S-1", "S-3", "S-4", "DEF 14A"}:
                            continue
                        accession = f["accession"] or ""
                        accession_nodash = accession.replace("-", "")
                        primary = f["primary_doc"] or ""
                        url = (
                            SEC_ARCHIVE_URL.format(
                                cik=int(cik),
                                accession_nodash=accession_nodash,
                                primary_doc=primary,
                            )
                            if accession and primary
                            else SEC_ARCHIVE_INDEX_URL.format(
                                cik=int(cik),
                                accession_nodash=accession_nodash,
                            )
                            if accession
                            else None
                        )
                        title = f["description"] or f["form"]
                        items_field = f.get("items")
                        summary_parts: list[str] = []
                        if items_field:
                            summary_parts.append(f"Items: {items_field}")
                        summary_parts.append(f"Form {f['form']} filed {f['filed']}")
                        summary = " — ".join(summary_parts)
                        inserted = self._upsert_event(
                            ticker=ticker,
                            cik=cik,
                            event_type=norm,
                            filing_date=f["filed"],
                            period_end=f["period"],
                            fiscal_quarter=_quarter_label(f["period"]),
                            accession=accession or None,
                            url=url,
                            title=title,
                            summary=summary,
                            full_text=None,
                            sentiment=None,
                            confidence=0.95 if norm in {"10-K","10-Q"} else 0.85,
                            raw_payload={
                                "form": f["form"],
                                "items": items_field,
                                "description": f["description"],
                                "primary_doc": primary,
                            },
                            source="SEC_EDGAR",
                        )
                        if inserted:
                            counts["filings_inserted"] += 1
                            filings_inserted += 1

            # AlphaVantage transcripts (most-recent 2 quarters).
            # In incremental mode we skip tickers whose freshest stored
            # EARNINGS_CALL is within ``incremental_max_age_days``.
            if include_transcripts and ticker not in av_eligible:
                av_skipped_fresh += 1
            if (
                include_transcripts
                and self.av_api_key
                and ticker in av_eligible
                and av_calls < self.av_max_calls
            ):
                today = date.today()
                cur_q = (today.month - 1) // 3 + 1
                cur_year = today.year
                quarters = []
                for back in range(2):
                    q = cur_q - back
                    y = cur_year
                    if q <= 0:
                        q += 4
                        y -= 1
                    quarters.append(f"{y}Q{q}")
                for q in quarters:
                    if av_calls >= self.av_max_calls:
                        break
                    av_calls += 1
                    payload = self.fetch_av_transcript(ticker, q)
                    if not payload:
                        continue
                    entries = payload.get("transcript") or []
                    if not entries:
                        continue
                    summary, full_text, sent_label = self._summarize_transcript(entries)
                    # Task #175: replace coarse AV label with LLM classifier
                    # output when reachable. Falls back to AV-derived label
                    # at confidence 0.8 if the LLM call fails.
                    llm_label, llm_conf = self._classify_transcript_sentiment(full_text)
                    if llm_label is not None and llm_conf is not None:
                        final_label = llm_label
                        final_confidence = float(llm_conf)
                        sentiment_source = "qwen2.5:14b"
                    else:
                        final_label = sent_label
                        final_confidence = 0.8
                        sentiment_source = "alphavantage_avg"
                    try:
                        # Quarter -> period_end approximation:
                        # last day of fiscal quarter.
                        y_str, q_str = q.split("Q")
                        year = int(y_str)
                        qi = int(q_str)
                        month = qi * 3
                        if month in (1, 3, 5, 7, 8, 10, 12):
                            day = 31
                        elif month == 2:
                            day = 29 if year % 4 == 0 else 28
                        else:
                            day = 30
                        period_end = date(year, month, day)
                    except Exception:
                        period_end = None
                    inserted = self._upsert_event(
                        ticker=ticker,
                        cik=cik,
                        event_type="EARNINGS_CALL",
                        filing_date=period_end or date.today(),
                        period_end=period_end,
                        fiscal_quarter=q,
                        accession=None,
                        url=f"https://www.alphavantage.co/query?function=EARNINGS_CALL_TRANSCRIPT&symbol={ticker}&quarter={q}",
                        title=f"{ticker} Earnings Call Transcript {q}",
                        summary=summary,
                        full_text=full_text,
                        sentiment=final_label,
                        confidence=final_confidence,
                        raw_payload={
                            "symbol": payload.get("symbol", ticker),
                            "quarter": q,
                            "entry_count": len(entries),
                            "sentiment_source": sentiment_source,
                        },
                        source="ALPHAVANTAGE",
                    )
                    if inserted:
                        counts["transcripts_inserted"] += 1
                        transcripts_inserted += 1

            per_ticker[ticker] = counts
            if (
                counts["filings_inserted"]
                or counts["transcripts_inserted"]
                or counts["filings_seen"]
            ):
                log.debug(
                    "earnings_events {t}: {fs} seen, {fi} new filings, {tr} new transcripts",
                    t=ticker,
                    fs=counts["filings_seen"],
                    fi=counts["filings_inserted"],
                    tr=counts["transcripts_inserted"],
                )

        # Update last_pull_at in source_catalog
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text("UPDATE source_catalog SET last_pull_at = now() "
                         "WHERE name = :n"),
                    {"n": self.SOURCE_NAME},
                )
        except Exception as exc:
            log.debug("source_catalog touch failed: {e}", e=str(exc))

        log.info(
            "earnings_events: {fi} new filings, {tr} new transcripts "
            "across {n} tickers ({fs} filings seen, {av} AV calls, "
            "{sk} AV-skipped-fresh, mode={mo})",
            fi=filings_inserted, tr=transcripts_inserted,
            n=len(universe), fs=filings_seen, av=av_calls,
            sk=av_skipped_fresh, mo=mode_norm,
        )
        return {
            "tickers": len(universe),
            "filings_seen": filings_seen,
            "filings_inserted": filings_inserted,
            "transcripts_inserted": transcripts_inserted,
            "av_calls": av_calls,
            "av_skipped_fresh": av_skipped_fresh,
            "mode": mode_norm,
            "per_ticker": per_ticker,
        }


# ----------------------------------------------------------------------
# CLI / systemd entry point
# ----------------------------------------------------------------------
def main() -> None:
    """Run the puller from the command line / systemd timer."""
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="GRID earnings_events puller (Task #151, #185)")
    parser.add_argument("--tickers", help="Comma-separated ticker list (overrides --ticker-source)")
    parser.add_argument("--ticker-source",
                        choices=["signal_registry", "watchlist", "cli"],
                        default="signal_registry",
                        help="Where to load the ticker universe from (Task #185). "
                             "Default 'signal_registry' expands coverage from "
                             "~21 (watchlist) to ~580 active tickers.")
    parser.add_argument("--lookback-days", type=int, default=120,
                        help="signal_registry activity window (default 120 days)")
    parser.add_argument("--min-signals", type=int, default=3,
                        help="Min signal_registry rows for a ticker to qualify (default 3)")
    parser.add_argument("--max-tickers", type=int, default=1000,
                        help="Hard cap on universe size for safety (default 1000)")
    parser.add_argument("--days-back", type=int, default=90)
    parser.add_argument("--no-transcripts", action="store_true",
                        help="Skip AlphaVantage transcripts (SEC EDGAR only)")
    parser.add_argument("--av-max-calls", type=int, default=400,
                        help="Cap on AlphaVantage calls per run (default 400 — "
                             "AV free tier is 500/day, leave 100 budget for "
                             "retries + LLM sentiment fallback)")
    parser.add_argument("--mode", choices=["full", "incremental"],
                        default="incremental",
                        help="incremental (default) only pulls AV transcripts "
                             "for tickers whose freshest stored EARNINGS_CALL "
                             "is older than --incremental-max-age-days (or "
                             "missing). 'full' attempts every ticker subject "
                             "to --av-max-calls. SEC EDGAR filings always "
                             "run for every ticker regardless of mode.")
    parser.add_argument("--incremental-max-age-days", type=int, default=90,
                        help="In incremental mode, a ticker is 'fresh' (and "
                             "AV-skipped) when its newest EARNINGS_CALL is "
                             "within this many days. Default 90 (one quarter).")
    args = parser.parse_args()

    # Defer import so this module can be reasoned about without DB config
    from db import get_engine

    engine = get_engine()
    puller = EarningsEventsPuller(engine, av_max_calls=args.av_max_calls)

    override = None
    if args.tickers:
        override = [t.strip() for t in args.tickers.split(",") if t.strip()]

    result = puller.pull(
        tickers=override,
        days_back=args.days_back,
        include_transcripts=not args.no_transcripts,
        ticker_source=args.ticker_source,
        lookback_days=args.lookback_days,
        min_signals=args.min_signals,
        max_tickers=args.max_tickers,
        mode=args.mode,
        incremental_max_age_days=args.incremental_max_age_days,
    )
    log.info("earnings_events puller done: {r}",
             r={k: v for k, v in result.items() if k != "per_ticker"})
    # Non-zero exit if we got literally nothing AND the universe is non-empty
    # (signals a config / network issue worth investigating)
    if result["tickers"] > 0 and result["filings_seen"] == 0:
        log.warning("No filings seen across {n} tickers — check SEC_EDGAR_UA / network",
                    n=result["tickers"])
        sys.exit(2)


if __name__ == "__main__":
    main()
