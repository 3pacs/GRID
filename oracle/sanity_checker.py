"""
GRID — Deterministic sanity checks on verified claims.

No LLM calls.  Validates price ranges, percentage math, direction
consistency, date sanity, unit sanity, and cross-claim consistency.
Imports range data from ingestion/sanity_ranges.py.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal

from loguru import logger as log

from oracle.claim_verifier import VerifiedClaim

SanityFlag = Literal["pass", "fail", "warn"]


@dataclass(frozen=True)
class SanityResult:
    """Result of a single sanity check."""

    check_name: str
    flag: SanityFlag
    message: str


@dataclass(frozen=True)
class CheckedClaim:
    """A verified claim plus its sanity check results."""

    verified: VerifiedClaim
    checks: tuple[SanityResult, ...]
    critical_fail: bool = False


# ── Price range table ────────────────────────────────────────────────────
# Extends ingestion/sanity_ranges.py with claim-level ticker ranges.

_TICKER_PRICE_RANGES: dict[str, tuple[float, float]] = {
    "SPY": (100.0, 1500.0),
    "SPX": (1000.0, 15000.0),
    "QQQ": (50.0, 1500.0),
    "IWM": (50.0, 800.0),
    "DIA": (100.0, 1200.0),
    "VIX": (5.0, 90.0),
    "VVIX": (50.0, 250.0),
    "BTC": (10000.0, 500000.0),
    "ETH": (100.0, 50000.0),
    "SOL": (1.0, 5000.0),
    "GLD": (80.0, 500.0),
    "TLT": (50.0, 200.0),
    "DXY": (70.0, 140.0),
}

# Try to import the canonical ranges from ingestion
try:
    from ingestion.sanity_ranges import SERIES_OVERRIDES as _SERIES_OVERRIDES
except ImportError:
    _SERIES_OVERRIDES: dict[str, tuple[float, float]] = {}


# ── Unit sanity: catch Q (quadrillion) misuse ────────────────────────────

_BAD_UNIT_RE = re.compile(
    r"\$\s?[\d,.]+\s*Q\b",
    re.IGNORECASE,
)

# ── Percentage math pattern: "from X to Y, Z%" ──────────────────────────

_PCT_MATH_RE = re.compile(
    r"from\s+\$?([\d,.]+)\s+to\s+\$?([\d,.]+).*?([-+]?\d+(?:\.\d+)?)\s*%",
    re.IGNORECASE,
)

# ── Date detection for future-date check ─────────────────────────────────

_YEAR_RE = re.compile(r"\b(20\d{2})\b")

# ── Direction words for cross-claim consistency ──────────────────────────

_BULLISH_WORDS = frozenset({
    "rallied", "surged", "jumped", "soared", "climbed", "gained",
    "rose", "bull", "bullish", "risk on", "market rallied",
})
_BEARISH_WORDS = frozenset({
    "dropped", "fell", "plunged", "crashed", "declined", "tumbled",
    "tanked", "bear", "bearish", "selloff", "sell-off", "risk off",
})


# ── Individual check functions ───────────────────────────────────────────

def _check_price_range(claim: VerifiedClaim) -> SanityResult | None:
    """Validate price is within plausible range for the ticker."""
    if claim.claim.claim_type != "price" or claim.claim.value is None:
        return None
    ticker = claim.claim.ticker
    if not ticker:
        return None

    value = claim.claim.value
    lo, hi = _TICKER_PRICE_RANGES.get(ticker.upper(), (None, None))

    # Fallback to ingestion ranges
    if lo is None:
        t_lower = ticker.lower()
        for key in [f"{t_lower}_full", f"{t_lower}_usd_full", t_lower]:
            if key in _SERIES_OVERRIDES:
                lo, hi = _SERIES_OVERRIDES[key]
                break

    if lo is None:
        return SanityResult("price_range", "pass", f"No range data for {ticker}")

    if value < lo or value > hi:
        return SanityResult(
            "price_range", "fail",
            f"{ticker} price ${value:,.2f} outside plausible range [{lo:,.2f}, {hi:,.2f}]",
        )
    return SanityResult("price_range", "pass", f"{ticker} ${value:,.2f} within range")


def _check_pct_math(claim: VerifiedClaim) -> SanityResult | None:
    """If the text says 'from X to Y, Z%' verify the math."""
    if claim.claim.claim_type != "percentage":
        return None

    m = _PCT_MATH_RE.search(claim.claim.text)
    if not m:
        return None

    try:
        val_from = float(m.group(1).replace(",", ""))
        val_to = float(m.group(2).replace(",", ""))
        claimed_pct = float(m.group(3))
    except (ValueError, IndexError):
        return None

    if val_from == 0:
        return None

    actual_pct = (val_to - val_from) / val_from * 100
    diff = abs(claimed_pct - actual_pct)
    if diff > 3.0:
        return SanityResult(
            "pct_math", "fail",
            f"Claimed {claimed_pct:+.1f}% but {val_from} → {val_to} is actually {actual_pct:+.1f}%",
        )
    return SanityResult("pct_math", "pass", "Percentage math checks out")


def _check_direction_consistency(claim: VerifiedClaim) -> SanityResult | None:
    """If verdict is contradicted on a direction claim, flag it."""
    if claim.claim.claim_type != "direction":
        return None
    if claim.verdict == "contradicted":
        return SanityResult(
            "direction_consistency", "fail",
            f"Direction contradicted by evidence: {claim.reason}",
        )
    return SanityResult("direction_consistency", "pass", "Direction consistent")


def _check_date_sanity(claim: VerifiedClaim) -> SanityResult | None:
    """Flag future dates used in past-tense claims."""
    if claim.claim.claim_type != "date":
        return None

    text_lower = claim.claim.text.lower()
    # Check for past-tense language
    is_past_tense = any(w in text_lower for w in [
        "was", "were", "had", "rose", "fell", "dropped",
        "surged", "crashed", "gained", "lost", "traded",
    ])
    if not is_past_tense:
        return None

    # Extract year from claim text
    year_match = _YEAR_RE.search(claim.claim.text)
    if not year_match:
        return None

    claimed_year = int(year_match.group(1))
    current_year = date.today().year
    if claimed_year > current_year:
        return SanityResult(
            "date_sanity", "fail",
            f"Past-tense claim references future year {claimed_year}",
        )
    return SanityResult("date_sanity", "pass", "Date is consistent with tense")


def _check_unit_sanity(claim: VerifiedClaim) -> SanityResult | None:
    """Catch $XQ (quadrillion) — likely meant $XT or $XB."""
    if claim.claim.claim_type != "price":
        return None
    if _BAD_UNIT_RE.search(claim.claim.text):
        return SanityResult(
            "unit_sanity", "fail",
            "Uses 'Q' (quadrillion) — likely should be T (trillion) or B (billion)",
        )
    return SanityResult("unit_sanity", "pass", "Units look correct")


# ── Cross-claim consistency ──────────────────────────────────────────────

def _check_cross_claim_consistency(
    claims: list[VerifiedClaim],
) -> list[tuple[int, SanityResult]]:
    """Detect contradictory directional language across claims.

    Returns list of (claim_index, SanityResult) for flagged claims.
    """
    has_bullish = False
    has_bearish = False
    bullish_indices: list[int] = []
    bearish_indices: list[int] = []

    for i, vc in enumerate(claims):
        text_lower = vc.claim.text.lower()
        if any(w in text_lower for w in _BULLISH_WORDS):
            has_bullish = True
            bullish_indices.append(i)
        if any(w in text_lower for w in _BEARISH_WORDS):
            has_bearish = True
            bearish_indices.append(i)

    results: list[tuple[int, SanityResult]] = []
    if has_bullish and has_bearish:
        # Only flag if talking about the SAME broad market (no ticker or same ticker)
        bull_tickers = {claims[i].claim.ticker for i in bullish_indices}
        bear_tickers = {claims[i].claim.ticker for i in bearish_indices}
        overlap = bull_tickers & bear_tickers
        # None means "broad market" — flag if both reference None or same ticker
        if None in overlap or (overlap - {None}):
            for i in bullish_indices + bearish_indices:
                results.append((i, SanityResult(
                    "cross_claim", "warn",
                    "Text contains both bullish and bearish language for the same market/ticker",
                )))
    return results


# ── Main entry point ────────────────────────────────────────────────────

_SINGLE_CHECKS = [
    _check_price_range,
    _check_pct_math,
    _check_direction_consistency,
    _check_date_sanity,
    _check_unit_sanity,
]


def run_sanity_checks(claims: list[VerifiedClaim]) -> list[CheckedClaim]:
    """Run all deterministic sanity checks on verified claims.

    Returns a CheckedClaim for each input, with sanity results attached.
    """
    # Per-claim checks
    per_claim_checks: list[list[SanityResult]] = []
    for vc in claims:
        results: list[SanityResult] = []
        for check_fn in _SINGLE_CHECKS:
            result = check_fn(vc)
            if result is not None:
                results.append(result)
        per_claim_checks.append(results)

    # Cross-claim checks
    cross_results = _check_cross_claim_consistency(claims)
    for idx, result in cross_results:
        if idx < len(per_claim_checks):
            per_claim_checks[idx].append(result)

    # Build output
    checked: list[CheckedClaim] = []
    for vc, checks in zip(claims, per_claim_checks):
        has_critical = any(r.flag == "fail" for r in checks)
        checked.append(CheckedClaim(
            verified=vc,
            checks=tuple(checks),
            critical_fail=has_critical,
        ))
    return checked
