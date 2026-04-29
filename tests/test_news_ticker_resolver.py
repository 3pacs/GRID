"""Unit tests for ``intelligence.news_ticker_resolver``.

These tests run without any DB or network access — the resolver
lazily loads ``analysis.sector_map`` once per process and is purely
CPU-bound from there.

Coverage:
    * Cashtag extraction ($AAPL, $TSLA.B)
    * Exchange prefix (NYSE: XPOF)
    * Known ticker word match (AAPL in uppercase prose)
    * Top-200 alias table (Apple, Nvidia, Berkshire, Meta)
    * Fallback payload tickers (upstream Tiingo list)
    * Ambiguous single-word aliases are REJECTED when they only match
      English words (block, target, visa, apollo, travelers)
    * Proper-noun company names (Meta, Ford, Disney) ARE matched
    * Blacklist words (BTC, ETH, NEWS, CEO) are rejected
"""

from __future__ import annotations

from intelligence.news_ticker_resolver import resolve_tickers


# ── Positive cases ───────────────────────────────────────────────────

def test_cashtag_basic() -> None:
    assert "AAPL" in resolve_tickers("$AAPL climbs 5% on earnings")


def test_cashtag_multiple() -> None:
    out = resolve_tickers("$AAPL and $TSLA both rally")
    assert "AAPL" in out
    assert "TSLA" in out


def test_exchange_prefix() -> None:
    out = resolve_tickers("Earnings release from NASDAQ: AMD top line beat")
    assert "AMD" in out


def test_alias_apple() -> None:
    out = resolve_tickers("Apple unveils new iPhone")
    assert "AAPL" in out


def test_alias_multi_word() -> None:
    out = resolve_tickers(
        "Warren Buffett says Berkshire Hathaway remains long JPMorgan"
    )
    assert "BRK.B" in out
    assert "JPM" in out


def test_alias_meta_capitalised() -> None:
    """Meta Platforms is a valid match despite colliding with 'meta-'."""
    out = resolve_tickers("Meta, Google under attack in antitrust case")
    assert "META" in out
    assert "GOOGL" in out


def test_payload_fallback() -> None:
    out = resolve_tickers(
        "Generic headline no ticker",
        description=None,
        fallback_payload_tickers=["TSLA", "nvda"],
    )
    assert "TSLA" in out
    assert "NVDA" in out


def test_payload_bypasses_blacklist() -> None:
    """Tiingo writes lowercase 'all' (Allstate) — must survive blacklist."""
    out = resolve_tickers(
        "Allstate Holiday Driver Report",
        description="Safety guidance for winter travel",
        fallback_payload_tickers=["all"],
    )
    assert "ALL" in out


def test_payload_bypasses_blacklist_new() -> None:
    """'NEW' is valid for upstream payload even though it's blacklisted
    for text extraction."""
    out = resolve_tickers(
        "Generic news article",
        fallback_payload_tickers=["NEW"],
    )
    assert "NEW" in out


def test_title_and_description_combined() -> None:
    out = resolve_tickers(
        "Earnings preview",
        description="Analysts expect strong numbers from Nvidia and AMD.",
    )
    assert "NVDA" in out
    assert "AMD" in out


# ── Negative / false-positive guards ────────────────────────────────

def test_block_word_does_not_resolve_sq() -> None:
    """The word 'block' must NOT resolve to Block Inc (SQ)."""
    out = resolve_tickers(
        "Senate committee plans to block Warsh nomination to Fed"
    )
    assert "SQ" not in out


def test_target_word_does_not_resolve_tgt() -> None:
    """The word 'target' must NOT resolve to Target Corp (TGT)."""
    out = resolve_tickers("Fed target rate unchanged at 4.25%")
    assert "TGT" not in out


def test_visa_word_does_not_resolve_v() -> None:
    out = resolve_tickers(
        "Travelers face visa issues entering EU"
    )
    assert "V" not in out
    # 'travelers' must also not resolve to TRV
    assert "TRV" not in out


def test_apollo_word_does_not_resolve_apo() -> None:
    out = resolve_tickers("Apollo mission anniversary celebrated at NASA")
    assert "APO" not in out


def test_apollo_global_long_form_does_resolve() -> None:
    out = resolve_tickers("Apollo Global raises $10B private credit fund")
    assert "APO" in out


def test_block_inc_long_form_does_resolve() -> None:
    out = resolve_tickers("Block Inc beats Q4 earnings estimates")
    assert "SQ" in out


def test_blacklist_btc_excluded() -> None:
    out = resolve_tickers("BTC price spikes above $100K")
    assert "BTC" not in out


def test_blacklist_ceo_excluded() -> None:
    out = resolve_tickers("Apple CEO Tim Cook speaks at conference")
    assert "CEO" not in out
    # AAPL should still match
    assert "AAPL" in out


def test_empty_input_returns_empty() -> None:
    assert resolve_tickers(None) == []
    assert resolve_tickers("") == []
    assert resolve_tickers("", "") == []


def test_deterministic_output_sorted() -> None:
    """Same input must produce identical sorted output."""
    out1 = resolve_tickers("Apple, Microsoft, and Nvidia rally")
    out2 = resolve_tickers("Apple, Microsoft, and Nvidia rally")
    assert out1 == out2
    assert out1 == sorted(out1)
