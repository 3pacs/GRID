"""
GRID — Deterministic claim extraction from LLM output text.

Parses atomic claims using regex patterns: prices, percentages, dates,
ticker mentions, directional language, and numeric values.  No LLM
dependency — pure deterministic parsing.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Literal

ClaimType = Literal[
    "price", "percentage", "direction", "indicator", "narrative", "date",
]


@dataclass(frozen=True)
class Claim:
    """Single atomic claim extracted from LLM output."""

    text: str
    claim_type: ClaimType
    ticker: str | None = None
    value: float | None = None
    confidence: float = 0.8
    source_span: tuple[int, int] = (0, 0)


# ── Ticker catalogue (extend as needed) ──────────────────────────────────

_KNOWN_TICKERS: frozenset[str] = frozenset({
    "SPY", "SPX", "QQQ", "IWM", "DIA", "VIX", "VVIX",
    "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "NVDA", "META", "TSLA",
    "BTC", "ETH", "SOL", "XRP", "BNB", "ADA", "DOGE",
    "GLD", "SLV", "USO", "TLT", "HYG", "LQD",
    "DXY", "EURUSD", "USDJPY",
})

# ── Compiled patterns ────────────────────────────────────────────────────

_PRICE_RE = re.compile(
    r"\$\s?([\d,]+(?:\.\d{1,2})?)\s*"
    r"(?:(trillion|billion|million|thousand|T|B|M|K)(?![a-z]))?",
    re.IGNORECASE,
)

_PERCENTAGE_RE = re.compile(
    r"([-+]?\d+(?:\.\d+)?)\s*%",
)

_TICKER_MENTION_RE = re.compile(
    r"\b([A-Z]{2,5})\b",
)

_DATE_RE = re.compile(
    r"\b(\d{4}[-/]\d{1,2}[-/]\d{1,2})\b"
    r"|\b((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*"
    r"\s+\d{1,2},?\s+\d{4})\b",
    re.IGNORECASE,
)

_DIRECTION_WORDS: dict[str, str] = {
    "surged": "up", "rallied": "up", "jumped": "up", "soared": "up",
    "climbed": "up", "gained": "up", "rose": "up", "increased": "up",
    "spiked": "up", "exploded": "up", "mooned": "up", "pumped": "up",
    "dropped": "down", "fell": "down", "plunged": "down", "crashed": "down",
    "declined": "down", "slid": "down", "tumbled": "down", "tanked": "down",
    "collapsed": "down", "dumped": "down", "sank": "down", "retreated": "down",
}

_DIRECTION_RE = re.compile(
    r"\b(" + "|".join(_DIRECTION_WORDS.keys()) + r")\b",
    re.IGNORECASE,
)

_NUMERIC_VALUE_RE = re.compile(
    r"\b(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*"
    r"(T|B|M|K|trillion|billion|million|thousand)?\b",
    re.IGNORECASE,
)

_UNIT_MULTIPLIERS: dict[str, float] = {
    "t": 1e12, "trillion": 1e12,
    "b": 1e9, "billion": 1e9,
    "m": 1e6, "million": 1e6,
    "k": 1e3, "thousand": 1e3,
}


# ── Helpers ──────────────────────────────────────────────────────────────

def _parse_numeric(raw: str, unit: str | None = None) -> float:
    """Convert a raw numeric string + optional unit suffix to a float."""
    cleaned = raw.replace(",", "")
    value = float(cleaned)
    if unit:
        multiplier = _UNIT_MULTIPLIERS.get(unit.lower(), 1.0)
        value *= multiplier
    return value


def _nearest_ticker(text: str, pos: int) -> str | None:
    """Find the nearest known ticker within +-80 chars of *pos*."""
    window = text[max(0, pos - 80): pos + 80]
    for m in _TICKER_MENTION_RE.finditer(window):
        candidate = m.group(1).upper()
        if candidate in _KNOWN_TICKERS:
            return candidate
    return None


# ── Sentence splitter (lightweight, no spaCy) ───────────────────────────

_SENTENCE_RE = re.compile(r"(?<=[.!?])\s+(?=[A-Z\d$])")


def _split_sentences(text: str) -> list[str]:
    """Split text into sentences using punctuation heuristics."""
    return [s.strip() for s in _SENTENCE_RE.split(text) if s.strip()]


# ── Main extraction ─────────────────────────────────────────────────────

def extract_claims(text: str) -> list[Claim]:
    """Parse LLM output into a list of atomic Claim objects.

    Uses deterministic regex patterns — no LLM call.
    """
    if not text or not text.strip():
        return []

    claims: list[Claim] = []
    seen_spans: set[tuple[int, int]] = set()

    def _add(claim: Claim) -> None:
        if claim.source_span not in seen_spans:
            seen_spans.add(claim.source_span)
            claims.append(claim)

    # --- Price claims ($XXX.XX) ---
    for m in _PRICE_RE.finditer(text):
        value = _parse_numeric(m.group(1), m.group(2))
        ticker = _nearest_ticker(text, m.start())
        sentence = _enclosing_sentence(text, m.start(), m.end())
        _add(Claim(
            text=sentence,
            claim_type="price",
            ticker=ticker,
            value=value,
            confidence=0.9,
            source_span=(m.start(), m.end()),
        ))

    # --- Percentage claims ---
    for m in _PERCENTAGE_RE.finditer(text):
        value = float(m.group(1))
        ticker = _nearest_ticker(text, m.start())
        sentence = _enclosing_sentence(text, m.start(), m.end())
        _add(Claim(
            text=sentence,
            claim_type="percentage",
            ticker=ticker,
            value=value,
            confidence=0.85,
            source_span=(m.start(), m.end()),
        ))

    # --- Direction claims ---
    for m in _DIRECTION_RE.finditer(text):
        direction_word = m.group(1).lower()
        direction = _DIRECTION_WORDS[direction_word]
        ticker = _nearest_ticker(text, m.start())
        sentence = _enclosing_sentence(text, m.start(), m.end())
        _add(Claim(
            text=sentence,
            claim_type="direction",
            ticker=ticker,
            value=1.0 if direction == "up" else -1.0,
            confidence=0.7,
            source_span=(m.start(), m.end()),
        ))

    # --- Date claims ---
    for m in _DATE_RE.finditer(text):
        raw_date = m.group(1) or m.group(2)
        sentence = _enclosing_sentence(text, m.start(), m.end())
        _add(Claim(
            text=sentence,
            claim_type="date",
            ticker=_nearest_ticker(text, m.start()),
            value=None,
            confidence=0.8,
            source_span=(m.start(), m.end()),
        ))

    return claims


def _enclosing_sentence(text: str, start: int, end: int) -> str:
    """Return the sentence containing the span [start, end)."""
    # Walk backwards to sentence boundary
    s = start
    while s > 0 and text[s - 1] not in ".!?\n":
        s -= 1
    # Walk forwards to sentence boundary
    e = end
    while e < len(text) and text[e] not in ".!?\n":
        e += 1
    return text[s:e].strip()
