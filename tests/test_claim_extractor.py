"""Unit tests for oracle/claim_extractor.py.

Covers the deterministic regex pipeline that powers
`api/routers/chat.py:1409` (LLM-output firewall) — price / percentage /
direction / date extraction, ticker proximity, unit suffix multipliers,
and span de-duplication.  No DB or LLM dependency.

TIER 4 punch-list item — docs/PUNCH-LIST-2026-05-13.md [P1] item 6.
"""
from __future__ import annotations

import pytest

from oracle.claim_extractor import Claim, extract_claims


# ── Empty / boundary input ──────────────────────────────────────────────


@pytest.mark.parametrize("text", ["", "   ", "\n\n\t"])
def test_empty_or_whitespace_returns_empty(text: str) -> None:
    assert extract_claims(text) == []


# ── Price claims ────────────────────────────────────────────────────────


def test_price_plain_dollar_value() -> None:
    claims = extract_claims("SPY hit $450.25 yesterday.")
    prices = [c for c in claims if c.claim_type == "price"]
    assert len(prices) == 1
    assert prices[0].value == pytest.approx(450.25)
    assert prices[0].ticker == "SPY"
    assert prices[0].confidence == 0.9


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("$1.5B", 1.5e9),
        ("$2T", 2e12),
        ("$500M", 5e8),
        ("$10K", 1e4),
        ("$1.2 billion", 1.2e9),
        ("$3 trillion", 3e12),
    ],
)
def test_price_unit_suffix_multiplies(raw: str, expected: float) -> None:
    claims = extract_claims(f"The trade was {raw} in size.")
    prices = [c for c in claims if c.claim_type == "price"]
    assert len(prices) == 1
    assert prices[0].value == pytest.approx(expected)


def test_price_comma_thousands_separator() -> None:
    claims = extract_claims("NVDA closed at $1,450.50 on Friday.")
    prices = [c for c in claims if c.claim_type == "price"]
    assert len(prices) == 1
    assert prices[0].value == pytest.approx(1450.50)
    assert prices[0].ticker == "NVDA"


def test_price_without_nearby_ticker_has_none() -> None:
    claims = extract_claims("The figure came in at $99.99.")
    prices = [c for c in claims if c.claim_type == "price"]
    assert len(prices) == 1
    assert prices[0].ticker is None


# ── Percentage claims ───────────────────────────────────────────────────


def test_percentage_positive() -> None:
    claims = extract_claims("QQQ gained 2.5% on the day.")
    pcts = [c for c in claims if c.claim_type == "percentage"]
    assert len(pcts) == 1
    assert pcts[0].value == pytest.approx(2.5)
    assert pcts[0].ticker == "QQQ"
    assert pcts[0].confidence == 0.85


def test_percentage_negative_signed() -> None:
    claims = extract_claims("TSLA closed -3.7% lower.")
    pcts = [c for c in claims if c.claim_type == "percentage"]
    assert len(pcts) == 1
    assert pcts[0].value == pytest.approx(-3.7)
    assert pcts[0].ticker == "TSLA"


def test_percentage_explicit_plus_sign() -> None:
    claims = extract_claims("BTC was up +12% this week.")
    pcts = [c for c in claims if c.claim_type == "percentage"]
    assert len(pcts) == 1
    assert pcts[0].value == pytest.approx(12.0)
    assert pcts[0].ticker == "BTC"


# ── Direction claims ────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "word",
    ["surged", "rallied", "jumped", "soared", "climbed",
     "gained", "rose", "increased", "spiked", "exploded",
     "mooned", "pumped"],
)
def test_direction_up_words(word: str) -> None:
    claims = extract_claims(f"NVDA {word} on heavy volume.")
    directions = [c for c in claims if c.claim_type == "direction"]
    assert len(directions) == 1
    assert directions[0].value == 1.0
    assert directions[0].ticker == "NVDA"
    assert directions[0].confidence == 0.7


@pytest.mark.parametrize(
    "word",
    ["dropped", "fell", "plunged", "crashed", "declined",
     "slid", "tumbled", "tanked", "collapsed", "dumped",
     "sank", "retreated"],
)
def test_direction_down_words(word: str) -> None:
    claims = extract_claims(f"AAPL {word} after the print.")
    directions = [c for c in claims if c.claim_type == "direction"]
    assert len(directions) == 1
    assert directions[0].value == -1.0
    assert directions[0].ticker == "AAPL"


def test_direction_is_case_insensitive() -> None:
    claims = extract_claims("META SURGED on the news.")
    directions = [c for c in claims if c.claim_type == "direction"]
    assert len(directions) == 1
    assert directions[0].value == 1.0


# ── Date claims ─────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw",
    ["2026-05-13", "2026/05/13", "2026-5-3", "2026/12/31"],
)
def test_date_iso_like(raw: str) -> None:
    claims = extract_claims(f"The readout is scheduled for {raw}.")
    dates = [c for c in claims if c.claim_type == "date"]
    assert len(dates) == 1
    assert dates[0].value is None
    assert dates[0].confidence == 0.8


@pytest.mark.parametrize(
    "raw",
    ["May 13, 2026", "January 5 2026", "December 31, 2025"],
)
def test_date_named_month(raw: str) -> None:
    claims = extract_claims(f"Earnings drop on {raw} after close.")
    dates = [c for c in claims if c.claim_type == "date"]
    assert len(dates) == 1


# ── Ticker proximity (±80 char window) ──────────────────────────────────


def test_ticker_attached_only_when_within_80_chars() -> None:
    # 100 filler chars between the ticker and the value
    filler = "x " * 60  # ~120 chars
    text = f"SPY {filler} was up 5% today."
    claims = extract_claims(text)
    pcts = [c for c in claims if c.claim_type == "percentage"]
    assert len(pcts) == 1
    assert pcts[0].ticker is None  # outside ±80 window


def test_unknown_ticker_not_attached() -> None:
    # ZZZZ is uppercase 2-5 chars but not in _KNOWN_TICKERS
    claims = extract_claims("ZZZZ traded at $50.")
    prices = [c for c in claims if c.claim_type == "price"]
    assert len(prices) == 1
    assert prices[0].ticker is None


# ── De-duplication on source span ───────────────────────────────────────


def test_same_span_not_double_counted() -> None:
    # Direction + ticker reference share the sentence but not the span;
    # de-dupe should keep distinct claim types even when they overlap.
    claims = extract_claims("SPY surged 2% today.")
    claim_types = sorted(c.claim_type for c in claims)
    assert "direction" in claim_types
    assert "percentage" in claim_types
    # All spans unique
    spans = [c.source_span for c in claims]
    assert len(spans) == len(set(spans))


# ── Mixed-claim integration ─────────────────────────────────────────────


def test_mixed_claims_in_one_paragraph() -> None:
    text = (
        "On 2026-05-13, NVDA surged 8.3% to a record $1,250.00 after "
        "the print today."
    )
    claims = extract_claims(text)
    types = {c.claim_type for c in claims}
    assert {"price", "percentage", "direction", "date"} <= types

    pct_values = sorted(c.value for c in claims if c.claim_type == "percentage")
    assert pytest.approx(8.3) in pct_values

    prices = [c for c in claims if c.claim_type == "price"]
    assert any(c.value == pytest.approx(1250.0) for c in prices)


def test_claim_is_frozen_dataclass() -> None:
    claim = Claim(text="x", claim_type="price")
    with pytest.raises((AttributeError, Exception)):
        claim.value = 1.0  # type: ignore[misc]


def test_source_span_points_into_original_text() -> None:
    text = "SPY hit $450 today."
    claims = extract_claims(text)
    prices = [c for c in claims if c.claim_type == "price"]
    assert len(prices) == 1
    s, e = prices[0].source_span
    assert "$450" in text[s:e]
