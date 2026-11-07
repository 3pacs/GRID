"""
SEC EDGAR + AlphaVantage earnings events puller (Task #151).

Canonical rollup of 10-K / 10-Q / 8-K filings AND earnings-call transcripts
per ticker. Writes to the ``earnings_events`` table on griddb.

Sources (in order of preference):
1. SEC EDGAR submissions JSON  (FREE, official, never-revised archive)
     https://data.sec.gov/submissions/CIK<10digit>.json
2. SEC EDGAR company_tickers.json (one-shot ticker -> CIK map at startup)
     https://www.sec.gov/files/company_tickers.json
3. AlphaVantage EARNINGS_CALL_TRANSCRIPT (account entitlement required)
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
- AlphaVantage: explicit per-run cap and configurable pacing, with optional
  shared-file UTC-day cap. Defaults do not imply any account entitlement.
  State is persistent, so empty responses cannot starve later symbols.

Author: GRID intelligence platform.
"""

from __future__ import annotations

import os
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure
from ingestion.av_transcript_plan import TranscriptPeriod, TranscriptState, verified_periods

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
    """Legacy SEC calendar bucket; never use this as an AV fiscal request label."""
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
        av_requests_per_minute: float = 5.0,
        av_daily_limit: int | None = None,
        av_state_path: str | Path | None = None,
    ) -> None:
        if av_max_calls < 0:
            raise ValueError("AV max calls must be nonnegative")
        super().__init__(db_engine)
        self.user_agent = user_agent or os.getenv("SEC_EDGAR_UA", DEFAULT_UA)
        self.av_api_key = av_api_key or os.getenv("ALPHAVANTAGE_API_KEY", "")
        self.av_max_calls = av_max_calls
        self.av_requests_per_minute = av_requests_per_minute
        self.av_daily_limit = av_daily_limit
        self.av_state_path = av_state_path or os.getenv(
            "GRID_AV_TRANSCRIPT_STATE", str(Path.home() / ".local/state/grid/av_transcripts.sqlite3")
        )
        self._av_stop_reason: str | None = None
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
            if resp.status_code in (401, 403, 429):
                self._av_stop_reason = f"http_{resp.status_code}"
                return None
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            # Exception strings may include the request URL and API key.
            log.warning("AV transcript {t} {q} failed ({kind})", t=ticker, q=quarter,
                        kind=type(exc).__name__)
            return None
        if not isinstance(data, dict):
            return None
        if "Information" in data or "Note" in data:
            # Notices also cover entitlement errors, not only quota exhaustion.
            self._av_stop_reason = "provider_notice"
            log.warning("AV provider notice; stopping this transcript run")
            return None
        if data.get("symbol") != ticker or data.get("quarter") != quarter:
            log.warning("AV transcript identity mismatch for {t} {q}", t=ticker, q=quarter)
            return None
        entries = data.get("transcript")
        if not isinstance(entries, list) or not entries or not all(isinstance(e, dict) for e in entries):
            return None
        if not any(isinstance(e.get("content"), str) and e["content"].strip() for e in entries):
            return None
        return data

    def _load_verified_periods(self, ticker: str, filings: list[dict[str, Any]]) -> list[TranscriptPeriod]:
        """Reuse SEC fundamentals already ingested; no extra provider calls.

        Exact accession/report-date matching is essential: SEC fy/fp on an old
        comparative fact describes its newer filing, not that historical period.
        Missing metadata is reported as a coverage gap instead of guessed.
        """
        sid_ticker = ticker.replace("-", ".")
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT r.raw_payload FROM raw_series r "
                    "JOIN source_catalog s ON s.id = r.source_id "
                    "WHERE s.name = :source AND r.pull_status = 'SUCCESS' "
                    "AND r.series_id = ANY(:series) AND r.obs_date >= :cutoff "
                    "AND r.pull_timestamp <= :as_of"
                ), {"source": "SEC_EDGAR_Fundamentals",
                    "series": [f"sec_filed_fundamentals.{sid_ticker}.{field}" for field in
                               ("total_assets", "net_income", "eps_diluted")],
                    "cutoff": date.today() - timedelta(days=550),
                    "as_of": datetime.now(timezone.utc)}).fetchall()
            return verified_periods([r[0] for r in rows], filings, date.today())
        except Exception as exc:
            log.warning("AV fiscal-period lookup failed for {t} ({kind}); skipping", t=ticker,
                        kind=type(exc).__name__)
            return []

    def _stored_transcript_periods(self, ticker: str, periods: list[TranscriptPeriod]) -> tuple[set[tuple[str, date]], set[tuple[str, date]]]:
        """Separate verified coverage from legacy rows requiring reconciliation."""
        import json
        with self.engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT fiscal_quarter, period_end, filing_date, raw_payload FROM earnings_events "
                "WHERE ticker = :ticker AND event_type = 'EARNINGS_CALL' "
                "AND source = 'ALPHAVANTAGE' AND full_text IS NOT NULL "
                "AND length(trim(full_text)) > 0"
            ), {"ticker": ticker}).fetchall()
        expected = {(p.quarter, p.period_end): p for p in periods}
        verified, legacy = set(), set()
        for quarter, end, filed, raw in rows:
            key = (quarter, end)
            period = expected.get(key)
            try:
                payload = json.loads(raw) if isinstance(raw, str) else raw
                observed = datetime.fromisoformat(payload["observed_at"])
                valid = (period is not None and filed == period.filed
                         and payload.get("filing_date_basis") == "matched_sec_filing_not_call_date"
                         and payload.get("fiscal_period_accession") == period.accession
                         and observed.tzinfo is not None
                         and observed <= datetime.now(timezone.utc))
            except (KeyError, TypeError, ValueError):
                valid = False
            (verified if valid else legacy).add(key)
        return verified, legacy - verified

    def _pull_planned_transcripts(self, plans, per_ticker, cik_map):
        """Run bounded, resumable requests only after SEC collection completes."""
        self._av_stop_reason = None
        calls = inserted_count = 0
        if not plans or not self.av_api_key or not self.av_max_calls:
            return calls, inserted_count
        try:
            state = TranscriptState(self.av_state_path, self.av_requests_per_minute, self.av_daily_limit)
            remaining = dict(plans)
            while remaining and calls < self.av_max_calls and not self._av_stop_reason:
                reservation = state.reserve(list(remaining))
                if reservation is None:
                    self._av_stop_reason = "local_daily_budget_or_day_boundary"
                    break
                key, wait = reservation
                ticker, period = remaining.pop(key)
                time.sleep(wait)
                calls += 1
                payload = self.fetch_av_transcript(ticker, period.quarter)
                if not payload:
                    continue
                entries = payload["transcript"]
                summary, full_text, sent_label = self._summarize_transcript(entries)
                llm_label, llm_conf = self._classify_transcript_sentiment(full_text)
                inserted = self._upsert_event(
                    ticker=ticker, cik=cik_map.get(ticker), event_type="EARNINGS_CALL",
                    # This is the matched SEC filing date, NOT an invented call date.
                    # First-observed time is retained separately in the raw payload.
                    filing_date=period.filed, period_end=period.period_end,
                    fiscal_quarter=period.quarter, accession=None,
                    url=f"{AV_URL}?function=EARNINGS_CALL_TRANSCRIPT&symbol={ticker}&quarter={period.quarter}",
                    title=f"{ticker} Earnings Call Transcript {period.quarter}",
                    summary=summary, full_text=full_text,
                    sentiment=llm_label if llm_label is not None else sent_label,
                    confidence=float(llm_conf) if llm_conf is not None else 0.8,
                    raw_payload={"symbol": ticker, "quarter": period.quarter,
                                 "entry_count": len(entries),
                                 "fiscal_period_source": "SEC_EDGAR_Fundamentals",
                                 "fiscal_period_accession": period.accession,
                                 "filing_date_basis": "matched_sec_filing_not_call_date",
                                 "observed_at": datetime.now(timezone.utc).isoformat(),
                                 "sentiment_source": "llm" if llm_label is not None else "alphavantage_avg"},
                    source="ALPHAVANTAGE",
                )
                if inserted:
                    inserted_count += 1
                    per_ticker[ticker]["transcripts_inserted"] += 1
        except Exception as exc:
            # State/DB failures must not fall back to an unbounded alphabetical run.
            self._av_stop_reason = "state_or_storage_error"
            log.warning("AV transcript run stopped ({kind})", kind=type(exc).__name__)
        return calls, inserted_count

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
    # Legacy argument retained for old wrappers. Scheduling now deduplicates
    # exact verified fiscal periods, not ticker-level calendar age.
    INCREMENTAL_DEFAULT_MAX_AGE_DAYS = 90

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
            mode: Legacy compatibility label ("incremental" or "full"). Both
                request missing verified periods and preserve SEC collection.
            incremental_max_age_days: Deprecated compatibility argument; a
                recent transcript never suppresses a different fiscal quarter.

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

        # Keep legacy CLI arguments accepted, but freshness is per verified fiscal
        # period. A recent transcript must not hide a newly filed quarter.
        plans: dict[str, tuple[str, TranscriptPeriod]] = {}
        av_skipped_period_unknown = 0
        av_skipped_stored = 0
        av_legacy_unverified = 0

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

            filings = []
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

            if include_transcripts and self.av_api_key and self.av_max_calls:
                # Parse the available recent filing index, not only the UI lookback.
                recent_filings = self.extract_filings(subs, date.today() - timedelta(days=550)) if cik and subs else []
                periods = self._load_verified_periods(ticker, recent_filings)
                if not periods:
                    av_skipped_period_unknown += 1
                    counts["av_period_unknown"] = 1
                try:
                    stored, legacy = self._stored_transcript_periods(ticker, periods)
                except Exception as exc:
                    log.warning("AV stored-period lookup failed for {t} ({kind}); skipping", t=ticker,
                                kind=type(exc).__name__)
                    counts["av_storage_lookup_failed"] = 1
                    periods = []
                    stored, legacy = set(), set()
                for period in periods:
                    if (period.quarter, period.period_end) in legacy:
                        # Changing filing_date changes the existing upsert identity.
                        # Do not silently create a duplicate or rewrite historical rows.
                        av_legacy_unverified += 1
                        counts["av_legacy_unverified"] = counts.get("av_legacy_unverified", 0) + 1
                        continue
                    if (period.quarter, period.period_end) in stored:
                        av_skipped_stored += 1
                        continue
                    # Stable lexical order, with newest periods first per symbol.
                    key = f"{ticker}|{9999 - int(period.quarter[:4]):04d}|{4 - int(period.quarter[-1])}"
                    plans[key] = (ticker, period)

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

        av_calls, transcripts_inserted = self._pull_planned_transcripts(plans, per_ticker, cik_map)

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
            "{unknown} unknown fiscal periods, stop={stop}, mode={mo})",
            fi=filings_inserted, tr=transcripts_inserted,
            n=len(universe), fs=filings_seen, av=av_calls,
            unknown=av_skipped_period_unknown, stop=self._av_stop_reason, mo=mode_norm,
        )
        return {
            "tickers": len(universe),
            "filings_seen": filings_seen,
            "filings_inserted": filings_inserted,
            "transcripts_inserted": transcripts_inserted,
            "av_calls": av_calls,
            "av_skipped_fresh": av_skipped_fresh,
            "av_skipped_stored_periods": av_skipped_stored,
            "av_legacy_unverified_periods": av_legacy_unverified,
            "av_skipped_period_unknown": av_skipped_period_unknown,
            "av_planned": len(plans),
            "av_stop_reason": self._av_stop_reason,
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
                        help="Local per-run call cap (default 400); not an account entitlement")
    parser.add_argument("--av-requests-per-minute", type=float, default=5.0,
                        help="Pacing allocation for this worker; confirm shared-account budget before raising")
    parser.add_argument("--av-daily-limit", type=int, default=None,
                        help="Optional local UTC-day cap across runs sharing --av-state-path")
    parser.add_argument("--av-state-path", default=None,
                        help="Durable local SQLite ledger; defaults to GRID_AV_TRANSCRIPT_STATE or ~/.local/state/grid/av_transcripts.sqlite3")
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental",
                        help="Legacy mode label; both modes now deduplicate exact verified fiscal periods")
    parser.add_argument("--incremental-max-age-days", type=int, default=90,
                        help="Deprecated compatibility argument; exact fiscal-period coverage controls scheduling")
    args = parser.parse_args()

    # Defer import so this module can be reasoned about without DB config
    from db import get_engine

    engine = get_engine()
    puller = EarningsEventsPuller(
        engine, av_max_calls=args.av_max_calls,
        av_requests_per_minute=args.av_requests_per_minute,
        av_daily_limit=args.av_daily_limit, av_state_path=args.av_state_path,
    )

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
