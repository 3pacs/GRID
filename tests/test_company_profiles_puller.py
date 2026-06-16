"""Tests for the company_profiles market-cap enrichment puller.

All HTTP is mocked — these exercise the pure row-shaping (which maps FMP
quote/profile responses onto the existing company_profiles schema, storing
market cap inside the JSONB profile column) and the fetch→shape glue.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from ingestion.altdata.company_profiles_puller import (
    CompanyProfilesPuller,
    shape_profile_row,
)


# ── shape_profile_row (pure, offline) ──

def test_shape_uses_quote_marketcap_and_profile_identity():
    quote = {"marketCap": 1_250_000_000, "price": 42.5, "name": "Acme Bio"}
    profile = {
        "companyName": "Acme Biopharma Inc.",
        "sector": "Healthcare",
        "industry": "Biotechnology",
        "exchangeShortName": "NASDAQ",
    }
    row = shape_profile_row("acme", quote, profile)
    assert row is not None
    assert row["ticker"] == "ACME"  # upper-cased
    assert row["name"] == "Acme Biopharma Inc."  # profile name wins over quote
    assert row["sector"] == "Healthcare"
    # Market cap lives inside the JSONB profile (no schema change).
    assert row["profile"]["market_cap"] == 1_250_000_000.0
    assert row["profile"]["price"] == 42.5
    assert row["profile"]["industry"] == "Biotechnology"
    assert row["profile"]["exchange"] == "NASDAQ"
    assert row["profile"]["source"] == "fmp"


def test_shape_falls_back_to_profile_mktcap_when_quote_missing():
    profile = {"companyName": "BioCo", "mktCap": "800000000", "sector": "Healthcare"}
    row = shape_profile_row("BIO", None, profile)
    assert row is not None
    assert row["profile"]["market_cap"] == 800_000_000.0
    assert row["name"] == "BioCo"


def test_shape_returns_none_for_blank_ticker():
    assert shape_profile_row("", {"marketCap": 1}, {}) is None
    assert shape_profile_row("   ", None, None) is None


def test_shape_returns_none_when_no_identity_and_no_marketcap():
    # Delisted/empty ticker — nothing worth persisting.
    assert shape_profile_row("ZZZZ", {}, {}) is None
    assert shape_profile_row("ZZZZ", {"price": 1.0}, {}) is None


def test_shape_keeps_row_with_marketcap_but_no_name():
    row = shape_profile_row("XYZ", {"marketCap": 500_000_000}, None)
    assert row is not None
    assert row["name"] is None
    assert row["profile"]["market_cap"] == 500_000_000.0


def test_shape_coerces_bad_numeric_to_none():
    row = shape_profile_row(
        "ABC", {"marketCap": "not-a-number", "price": None},
        {"companyName": "ABC Corp"},
    )
    assert row is not None
    assert row["profile"]["market_cap"] is None
    assert row["profile"]["price"] is None


# ── enrich_ticker (fetch glue, mocked HTTP) ──

def _puller_no_db() -> CompanyProfilesPuller:
    """Construct without running real __init__ (no DB / no FMP key needed)."""
    p = CompanyProfilesPuller.__new__(CompanyProfilesPuller)
    p.engine = MagicMock()
    p.fmp = MagicMock()
    return p


def test_enrich_ticker_combines_quote_and_profile():
    p = _puller_no_db()
    p.fmp.pull_quote.return_value = {"marketCap": 2_000_000_000, "price": 10.0}
    p.fmp.pull_profile.return_value = {"companyName": "Trial Co", "sector": "Healthcare"}
    row = p.enrich_ticker("TRIAL")
    assert row["ticker"] == "TRIAL"
    assert row["profile"]["market_cap"] == 2_000_000_000.0
    assert row["name"] == "Trial Co"
    p.fmp.pull_quote.assert_called_once_with("TRIAL")
    p.fmp.pull_profile.assert_called_once_with("TRIAL")


def test_pull_disabled_without_api_key():
    p = _puller_no_db()
    p.fmp.api_key = ""
    out = p.pull(tickers=["AAA"])
    assert out["status"] == "DISABLED"
    assert out["rows_upserted"] == 0


def test_pull_upserts_only_shapeable_rows():
    p = _puller_no_db()
    p.fmp.api_key = "key"

    # AAA -> good; BBB -> empty (skipped, returns None from shaping).
    def fake_quote(tk):
        return {"marketCap": 1_000_000_000} if tk == "AAA" else {}

    def fake_profile(tk):
        return {"companyName": "Alpha"} if tk == "AAA" else {}

    p.fmp.pull_quote.side_effect = fake_quote
    p.fmp.pull_profile.side_effect = fake_profile

    conn = MagicMock()
    p.engine.begin.return_value.__enter__.return_value = conn

    with patch(
        "ingestion.altdata.company_profiles_puller.ensure_table"
    ), patch("ingestion.altdata.company_profiles_puller.time.sleep"):
        out = p.pull(tickers=["AAA", "BBB"])

    assert out["status"] == "SUCCESS"
    assert out["tickers_attempted"] == 2
    assert out["rows_upserted"] == 1  # only AAA persisted
    assert conn.execute.call_count == 1
