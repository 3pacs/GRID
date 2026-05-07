"""
GRID Intelligence — Earnings Transcript Analyzer.

Deep analysis of earnings call transcripts stored by edgar_transcripts puller.
Goes beyond header-level extraction to analyze:
    - Management tone (optimistic/cautious/defensive)
    - Section-by-section sentiment (prepared remarks vs Q&A)
    - Key phrase extraction (guidance language, risk mentions)
    - Forward-looking statement detection
    - Hedging language detection (weasel words)
    - Comparison with prior quarter tone (tone shift detection)
    - Q&A sentiment (analyst bear/bull question ratio)

Sources: raw_series where series_id LIKE 'edgar:8k:%' (from edgar_transcripts.py)
Schedule: Daily (runs after edgar_transcripts puller)
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ────────────────────────────────────────────────────────────

# Positive management tone words
_POSITIVE_WORDS = frozenset({
    "strong", "robust", "excellent", "outstanding", "exceptional", "record",
    "accelerat", "momentum", "growth", "improved", "expanding", "confident",
    "optimistic", "pleased", "excited", "ahead", "outperform", "surpass",
    "tailwind", "upside", "beat", "exceed", "milestone", "opportunity",
    "transformative", "innovative", "breakthrough",
})

# Negative/cautious management tone words
_NEGATIVE_WORDS = frozenset({
    "challenging", "headwind", "uncertain", "cautious", "soft", "weak",
    "decline", "pressure", "risk", "concerned", "disappoint", "shortfall",
    "below", "miss", "deteriorat", "contraction", "downturn", "adverse",
    "restructur", "impair", "writedown", "charge", "loss", "negative",
    "delay", "postpone", "suspend",
})

# Hedging language (weasel words that signal uncertainty)
_HEDGE_WORDS = frozenset({
    "may", "might", "could", "possibly", "potentially", "approximately",
    "generally", "somewhat", "relatively", "subject to", "depending on",
    "if conditions", "assuming", "barring", "absent any", "to the extent",
    "we believe", "we expect", "we anticipate", "we estimate",
})

# Forward-looking indicators
_FORWARD_LOOKING = frozenset({
    "guidance", "outlook", "expect", "forecast", "project", "anticipate",
    "plan", "target", "goal", "initiative", "pipeline", "backlog",
    "next quarter", "full year", "fiscal year", "second half", "going forward",
    "looking ahead", "on track", "positioned",
})

# Q&A section markers
_QA_START_PATTERNS = [
    re.compile(r"(?:question|Q\s*&\s*A|questions?\s+and\s+answers?)\s+(?:session|period|portion)", re.I),
    re.compile(r"(?:operator|moderator).*(?:first question|open.*for questions)", re.I),
    re.compile(r"(?:we'll|we will|let's)\s+(?:now\s+)?(?:open|take|begin)\s+(?:the\s+)?(?:call|line|floor)\s+(?:for|to)\s+questions", re.I),
]


# ── Data Classes ─────────────────────────────────────────────────────────

@dataclass
class TranscriptAnalysis:
    """Complete analysis of an earnings transcript."""
    analysis_id: str
    ticker: str
    filing_date: date
    # Tone scores [-1.0 to +1.0]
    overall_tone: float
    prepared_remarks_tone: float
    qa_tone: float
    tone_label: str              # optimistic, cautious, defensive, neutral
    # Counts
    positive_count: int
    negative_count: int
    hedge_count: int
    forward_looking_count: int
    # Key phrases
    guidance_phrases: list[str]
    risk_phrases: list[str]
    forward_statements: list[str]
    hedge_phrases: list[str]
    # Comparison
    tone_shift: float | None     # vs prior quarter (positive = more optimistic)
    prior_tone: float | None
    # Metadata
    word_count: int
    qa_word_count: int
    confidence: float
    computed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["filing_date"] = self.filing_date.isoformat()
        d["computed_at"] = self.computed_at.isoformat()
        return d


# ── Tone Scorer ──────────────────────────────────────────────────────────

class ToneScorer:
    """Score management tone from transcript text."""

    def score_text(self, text: str) -> dict[str, Any]:
        """Score a block of text for tone indicators.

        Args:
            text: Text to analyze.

        Returns:
            Dict with counts and computed tone score.
        """
        words = text.lower().split()
        word_count = len(words)

        if word_count == 0:
            return {
                "tone": 0.0,
                "positive": 0,
                "negative": 0,
                "hedges": 0,
                "forward_looking": 0,
                "word_count": 0,
            }

        positive = sum(1 for w in words if any(w.startswith(p) for p in _POSITIVE_WORDS))
        negative = sum(1 for w in words if any(w.startswith(n) for n in _NEGATIVE_WORDS))
        hedges = sum(1 for w in words if w in _HEDGE_WORDS)

        # Forward-looking needs bigram check
        text_lower = text.lower()
        forward = sum(1 for fl in _FORWARD_LOOKING if fl in text_lower)

        # Tone: normalized (positive - negative) / total sentiment words
        total_sentiment = positive + negative
        if total_sentiment > 0:
            tone = (positive - negative) / total_sentiment
        else:
            tone = 0.0

        # Hedge penalty: high hedging pulls tone toward neutral
        if hedges > word_count * 0.02:
            tone *= 0.7

        return {
            "tone": round(max(-1.0, min(1.0, tone)), 4),
            "positive": positive,
            "negative": negative,
            "hedges": hedges,
            "forward_looking": forward,
            "word_count": word_count,
        }

    def classify_tone(self, tone: float) -> str:
        """Classify numeric tone into a label.

        Args:
            tone: Tone score [-1.0, +1.0].

        Returns:
            Label string.
        """
        if tone >= 0.3:
            return "optimistic"
        elif tone >= 0.05:
            return "confident"
        elif tone >= -0.05:
            return "neutral"
        elif tone >= -0.3:
            return "cautious"
        else:
            return "defensive"


# ── Phrase Extractor ─────────────────────────────────────────────────────

class PhraseExtractor:
    """Extract key phrases from transcript text."""

    _GUIDANCE_PATTERNS = [
        re.compile(r"(?:revenue|sales)\s+(?:guidance|outlook|expect|forecast|project)[^.]{5,150}", re.I),
        re.compile(r"(?:earnings|EPS|earnings per share)\s+(?:guidance|outlook|expect|forecast)[^.]{5,150}", re.I),
        re.compile(r"(?:margin|margins)\s+(?:guidance|outlook|expect|target)[^.]{5,150}", re.I),
        re.compile(r"(?:full.?year|FY\d{2,4}|fiscal year)\s+(?:guidance|outlook|expect|revenue|earnings)[^.]{5,150}", re.I),
        re.compile(r"(?:raise|raised|increase|lower|cut|narrow|reaffirm)\w*\s+(?:our\s+)?(?:guidance|outlook|forecast|target)[^.]{5,150}", re.I),
    ]

    _RISK_PATTERNS = [
        re.compile(r"(?:risk|threat|headwind|challenge|pressure|concern|uncertainty)[^.]{5,150}", re.I),
        re.compile(r"(?:tariff|trade war|sanction|geopolitical|regulatory\s+risk)[^.]{5,150}", re.I),
        re.compile(r"(?:supply\s+chain|shortage|disruption|inflation\s+pressure)[^.]{5,150}", re.I),
    ]

    _FORWARD_PATTERNS = [
        re.compile(r"(?:we\s+expect|we\s+anticipate|we\s+plan|we\s+project|we\s+target|we\s+forecast)[^.]{10,200}", re.I),
        re.compile(r"(?:looking\s+ahead|going\s+forward|in\s+the\s+(?:coming|next)\s+(?:quarter|year))[^.]{10,200}", re.I),
        re.compile(r"(?:on\s+track\s+to|positioned\s+to|committed\s+to)[^.]{10,150}", re.I),
    ]

    _HEDGE_PATTERNS = [
        re.compile(r"(?:subject\s+to|depending\s+on|assuming\s+|barring\s+|to\s+the\s+extent|if\s+conditions)[^.]{10,150}", re.I),
    ]

    def extract_guidance(self, text: str) -> list[str]:
        """Extract guidance-related phrases."""
        return self._extract_all(text, self._GUIDANCE_PATTERNS)

    def extract_risks(self, text: str) -> list[str]:
        """Extract risk-related phrases."""
        return self._extract_all(text, self._RISK_PATTERNS)

    def extract_forward(self, text: str) -> list[str]:
        """Extract forward-looking statements."""
        return self._extract_all(text, self._FORWARD_PATTERNS)

    def extract_hedges(self, text: str) -> list[str]:
        """Extract hedging language."""
        return self._extract_all(text, self._HEDGE_PATTERNS)

    def _extract_all(self, text: str, patterns: list[re.Pattern]) -> list[str]:
        """Run all patterns and return unique matches."""
        results: list[str] = []
        seen: set[str] = set()
        for pattern in patterns:
            for match in pattern.findall(text):
                cleaned = match.strip()[:200]
                key = cleaned[:50].lower()
                if key not in seen:
                    seen.add(key)
                    results.append(cleaned)
        return results[:15]  # Cap at 15 per category


# ── Section Splitter ─────────────────────────────────────────────────────

class SectionSplitter:
    """Split transcript into prepared remarks and Q&A sections."""

    def split(self, text: str) -> tuple[str, str]:
        """Split transcript text into (prepared_remarks, qa_section).

        Args:
            text: Full transcript text.

        Returns:
            Tuple of (prepared_remarks, qa_section). If no Q&A section
            found, returns (full_text, "").
        """
        for pattern in _QA_START_PATTERNS:
            match = pattern.search(text)
            if match:
                split_pos = match.start()
                return text[:split_pos], text[split_pos:]

        # Fallback: look for "Question" as a standalone section header
        question_idx = text.lower().find("\nquestion")
        if question_idx > len(text) * 0.3:  # Must be past 30% of text
            return text[:question_idx], text[question_idx:]

        return text, ""


# ── Main Analyzer ────────────────────────────────────────────────────────

class EarningsTranscriptAnalyzer:
    """Analyze earnings call transcripts for tone, key phrases, and shifts.

    Consumes transcripts stored by the edgar_transcripts puller in raw_series
    and produces structured analysis stored in the earnings_analysis table.

    Attributes:
        engine: SQLAlchemy engine.
        scorer: ToneScorer instance.
        extractor: PhraseExtractor instance.
        splitter: SectionSplitter instance.
    """

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self.scorer = ToneScorer()
        self.extractor = PhraseExtractor()
        self.splitter = SectionSplitter()
        self._ensure_table()

    def _ensure_table(self) -> None:
        """Create earnings_analysis table if it doesn't exist."""
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS earnings_analysis (
                        id SERIAL PRIMARY KEY,
                        analysis_id TEXT UNIQUE NOT NULL,
                        ticker TEXT NOT NULL,
                        filing_date DATE NOT NULL,
                        overall_tone DOUBLE PRECISION DEFAULT 0.0,
                        prepared_remarks_tone DOUBLE PRECISION DEFAULT 0.0,
                        qa_tone DOUBLE PRECISION DEFAULT 0.0,
                        tone_label TEXT DEFAULT 'neutral',
                        positive_count INTEGER DEFAULT 0,
                        negative_count INTEGER DEFAULT 0,
                        hedge_count INTEGER DEFAULT 0,
                        forward_looking_count INTEGER DEFAULT 0,
                        guidance_phrases JSONB DEFAULT '[]',
                        risk_phrases JSONB DEFAULT '[]',
                        forward_statements JSONB DEFAULT '[]',
                        hedge_phrases JSONB DEFAULT '[]',
                        tone_shift DOUBLE PRECISION,
                        prior_tone DOUBLE PRECISION,
                        word_count INTEGER DEFAULT 0,
                        qa_word_count INTEGER DEFAULT 0,
                        confidence DOUBLE PRECISION DEFAULT 0.5,
                        computed_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_earnings_analysis_ticker
                    ON earnings_analysis (ticker)
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_earnings_analysis_date
                    ON earnings_analysis (filing_date DESC)
                """))
                conn.execute(text("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_earnings_analysis_ticker_date
                    ON earnings_analysis (ticker, filing_date)
                """))
        except Exception as exc:
            log.warning("Failed to create earnings_analysis table: {e}", e=str(exc))

    def analyze_transcript(
        self,
        ticker: str,
        filing_date: date,
        transcript_text: str,
    ) -> TranscriptAnalysis | None:
        """Run full analysis on a single transcript.

        Args:
            ticker: Ticker symbol.
            filing_date: Date of the filing.
            transcript_text: Full transcript text.

        Returns:
            TranscriptAnalysis or None if text too short.
        """
        if len(transcript_text) < 200:
            return None

        # Split into sections
        prepared, qa = self.splitter.split(transcript_text)

        # Score tone for each section
        overall_scores = self.scorer.score_text(transcript_text)
        prepared_scores = self.scorer.score_text(prepared)
        qa_scores = self.scorer.score_text(qa) if qa else {
            "tone": 0.0, "positive": 0, "negative": 0,
            "hedges": 0, "forward_looking": 0, "word_count": 0,
        }

        # Extract key phrases
        guidance_phrases = self.extractor.extract_guidance(transcript_text)
        risk_phrases = self.extractor.extract_risks(transcript_text)
        forward_statements = self.extractor.extract_forward(transcript_text)
        hedge_phrases = self.extractor.extract_hedges(transcript_text)

        # Get prior quarter tone for shift detection
        prior_tone = self._get_prior_tone(ticker, filing_date)
        tone_shift = None
        if prior_tone is not None:
            tone_shift = round(overall_scores["tone"] - prior_tone, 4)

        # Confidence based on text length and phrase extraction
        confidence = min(1.0, 0.3 + len(transcript_text) / 20000 * 0.4 + len(guidance_phrases) * 0.03)

        analysis_id = hashlib.sha256(
            f"transcript:{ticker}:{filing_date}".encode()
        ).hexdigest()[:20]

        return TranscriptAnalysis(
            analysis_id=analysis_id,
            ticker=ticker.upper(),
            filing_date=filing_date,
            overall_tone=overall_scores["tone"],
            prepared_remarks_tone=prepared_scores["tone"],
            qa_tone=qa_scores["tone"],
            tone_label=self.scorer.classify_tone(overall_scores["tone"]),
            positive_count=overall_scores["positive"],
            negative_count=overall_scores["negative"],
            hedge_count=overall_scores["hedges"],
            forward_looking_count=overall_scores["forward_looking"],
            guidance_phrases=guidance_phrases,
            risk_phrases=risk_phrases,
            forward_statements=forward_statements,
            hedge_phrases=hedge_phrases,
            tone_shift=tone_shift,
            prior_tone=prior_tone,
            word_count=overall_scores["word_count"],
            qa_word_count=qa_scores["word_count"],
            confidence=round(confidence, 3),
        )

    def _get_prior_tone(self, ticker: str, current_date: date) -> float | None:
        """Get the tone score from the previous quarter's analysis.

        Args:
            ticker: Ticker symbol.
            current_date: Current filing date.

        Returns:
            Prior overall_tone or None.
        """
        try:
            with self.engine.connect() as conn:
                row = conn.execute(
                    text("""
                        SELECT overall_tone
                        FROM earnings_analysis
                        WHERE ticker = :ticker
                          AND filing_date < :current
                        ORDER BY filing_date DESC
                        LIMIT 1
                    """),
                    {"ticker": ticker.upper(), "current": current_date},
                ).fetchone()

            return float(row[0]) if row else None
        except Exception:
            return None

    def _store_analysis(self, analysis: TranscriptAnalysis) -> bool:
        """Persist analysis to the database.

        Args:
            analysis: The TranscriptAnalysis to store.

        Returns:
            True if stored successfully.
        """
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO earnings_analysis
                        (analysis_id, ticker, filing_date, overall_tone,
                         prepared_remarks_tone, qa_tone, tone_label,
                         positive_count, negative_count, hedge_count,
                         forward_looking_count, guidance_phrases, risk_phrases,
                         forward_statements, hedge_phrases, tone_shift,
                         prior_tone, word_count, qa_word_count, confidence,
                         computed_at)
                        VALUES
                        (:aid, :ticker, :fdate, :otone,
                         :ptone, :qtone, :tlabel,
                         :pos, :neg, :hedge, :fwd,
                         :gphrases, :rphrases, :fstmts, :hphrases,
                         :tshift, :ptone_val, :wc, :qwc, :conf, :cat)
                        ON CONFLICT (analysis_id) DO UPDATE SET
                            overall_tone = EXCLUDED.overall_tone,
                            tone_label = EXCLUDED.tone_label,
                            confidence = EXCLUDED.confidence,
                            computed_at = EXCLUDED.computed_at
                    """),
                    {
                        "aid": analysis.analysis_id,
                        "ticker": analysis.ticker,
                        "fdate": analysis.filing_date,
                        "otone": analysis.overall_tone,
                        "ptone": analysis.prepared_remarks_tone,
                        "qtone": analysis.qa_tone,
                        "tlabel": analysis.tone_label,
                        "pos": analysis.positive_count,
                        "neg": analysis.negative_count,
                        "hedge": analysis.hedge_count,
                        "fwd": analysis.forward_looking_count,
                        "gphrases": json.dumps(analysis.guidance_phrases),
                        "rphrases": json.dumps(analysis.risk_phrases),
                        "fstmts": json.dumps(analysis.forward_statements),
                        "hphrases": json.dumps(analysis.hedge_phrases),
                        "tshift": analysis.tone_shift,
                        "ptone_val": analysis.prior_tone,
                        "wc": analysis.word_count,
                        "qwc": analysis.qa_word_count,
                        "conf": analysis.confidence,
                        "cat": analysis.computed_at,
                    },
                )
            return True
        except Exception as exc:
            log.warning(
                "Failed to store earnings analysis for {t}: {e}",
                t=analysis.ticker, e=str(exc),
            )
            return False

    def run_analysis(
        self,
        tickers: list[str] | None = None,
        days_back: int = 90,
    ) -> dict[str, Any]:
        """Run transcript analysis for all recent filings.

        Fetches transcript text from raw_series (stored by edgar_transcripts
        puller) and runs tone/phrase analysis.

        Args:
            tickers: Optional ticker list. If None, discovers from raw_series.
            days_back: Days to look back for filings.

        Returns:
            Summary dict.
        """
        cutoff = (date.today() - timedelta(days=days_back)).isoformat()

        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT series_id, obs_date, raw_payload
                        FROM raw_series
                        WHERE series_id LIKE 'edgar:8k:%'
                          AND obs_date >= :cutoff
                        ORDER BY obs_date DESC
                    """),
                    {"cutoff": cutoff},
                ).fetchall()
        except Exception as exc:
            log.warning("Transcript analysis query failed: {e}", e=str(exc))
            return {"error": str(exc)}

        analyzed = 0
        stored = 0

        for row in rows:
            row[0]
            obs_date = row[1]
            payload = row[2] if isinstance(row[2], dict) else {}

            ticker = payload.get("ticker", "")
            if tickers and ticker not in tickers:
                continue

            transcript_text = payload.get("text_preview", "")

            # Try to get full text if preview is truncated
            if len(transcript_text) < 500:
                continue

            analysis = self.analyze_transcript(ticker, obs_date, transcript_text)
            if analysis:
                analyzed += 1
                if self._store_analysis(analysis):
                    stored += 1

        result = {
            "filings_found": len(rows),
            "analyzed": analyzed,
            "stored": stored,
        }

        log.info(
            "Earnings transcript analysis: {f} filings → {a} analyzed, {s} stored",
            f=len(rows), a=analyzed, s=stored,
        )

        return result

    def get_analysis(
        self,
        ticker: str,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        """Get recent transcript analyses for a ticker.

        Args:
            ticker: Ticker symbol.
            limit: Max results.

        Returns:
            List of analysis dicts.
        """
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT analysis_id, ticker, filing_date, overall_tone,
                               prepared_remarks_tone, qa_tone, tone_label,
                               positive_count, negative_count, hedge_count,
                               forward_looking_count, guidance_phrases,
                               risk_phrases, forward_statements, hedge_phrases,
                               tone_shift, prior_tone, word_count, qa_word_count,
                               confidence
                        FROM earnings_analysis
                        WHERE ticker = :ticker
                        ORDER BY filing_date DESC
                        LIMIT :lim
                    """),
                    {"ticker": ticker.upper(), "lim": limit},
                ).fetchall()
        except Exception as exc:
            log.warning("Get analysis query failed: {e}", e=str(exc))
            return []

        return [
            {
                "analysis_id": r[0],
                "ticker": r[1],
                "filing_date": r[2].isoformat() if r[2] else None,
                "overall_tone": r[3],
                "prepared_remarks_tone": r[4],
                "qa_tone": r[5],
                "tone_label": r[6],
                "positive_count": r[7],
                "negative_count": r[8],
                "hedge_count": r[9],
                "forward_looking_count": r[10],
                "guidance_phrases": r[11],
                "risk_phrases": r[12],
                "forward_statements": r[13],
                "hedge_phrases": r[14],
                "tone_shift": r[15],
                "prior_tone": r[16],
                "word_count": r[17],
                "qa_word_count": r[18],
                "confidence": r[19],
            }
            for r in rows
        ]

    def get_tone_shifts(self, min_shift: float = 0.2) -> list[dict[str, Any]]:
        """Get all tickers with significant tone shifts vs prior quarter.

        Args:
            min_shift: Minimum absolute tone shift to include.

        Returns:
            List of dicts with ticker, tone_shift, and labels.
        """
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT DISTINCT ON (ticker)
                               ticker, filing_date, overall_tone, tone_label,
                               tone_shift, prior_tone
                        FROM earnings_analysis
                        WHERE tone_shift IS NOT NULL
                          AND ABS(tone_shift) >= :min_shift
                        ORDER BY ticker, filing_date DESC
                    """),
                    {"min_shift": min_shift},
                ).fetchall()
        except Exception as exc:
            log.warning("Tone shifts query failed: {e}", e=str(exc))
            return []

        return [
            {
                "ticker": r[0],
                "filing_date": r[1].isoformat() if r[1] else None,
                "current_tone": r[2],
                "tone_label": r[3],
                "tone_shift": r[4],
                "prior_tone": r[5],
                "shift_direction": "more_optimistic" if (r[4] or 0) > 0 else "more_cautious",
            }
            for r in rows
        ]
