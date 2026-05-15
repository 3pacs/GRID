"""Unit tests for oracle/claim_verifier.py.

Covers the DB-evidence verdict path consumed by `gate_decision`
(`oracle/publisher_gate.py:42`) and the chat firewall
(`oracle/firewall.py::verify_output`). All four verdict states
(supported / contradicted / insufficient / ambiguous) are exercised
plus tolerance edges and engine-exception fallthroughs.

TIER 4 punch-list item — docs/PUNCH-LIST-2026-05-13.md [P1] item 7.
The MagicMock engine pattern mirrors PR #161 (`tests/test_psi_model.py`):
``engine.connect().__enter__().execute().fetchone()`` chain, with
`side_effect` keyed on the bound ``:name`` parameter.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from oracle.claim_extractor import Claim
from oracle.claim_verifier import (
    VerifiedClaim,
    _lookup_feature_value,
    _lookup_latest_value,
    _lookup_price_change,
    _verify_direction,
    _verify_generic,
    _verify_percentage,
    _verify_price,
    verify_claims,
)


# ── Engine-mock helpers ──────────────────────────────────────────────────


def _make_engine_for_fetchone(rows: dict[str, tuple | None]) -> MagicMock:
    """Build a MagicMock engine whose .execute().fetchone() returns
    ``rows[feature_name]`` based on the bound ``:name`` param."""
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value

    def _execute(_sql, params):
        result = MagicMock()
        result.fetchone.return_value = rows.get(params["name"])
        return result

    conn.execute.side_effect = _execute
    return engine


def _make_engine_for_fetchall(rows: dict[str, list[tuple]]) -> MagicMock:
    """Build a MagicMock engine whose .execute().fetchall() returns
    ``rows[feature_name]`` (list of row tuples) based on bound ``:name``."""
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value

    def _execute(_sql, params):
        result = MagicMock()
        result.fetchall.return_value = rows.get(params["name"], [])
        return result

    conn.execute.side_effect = _execute
    return engine


def _make_engine_raising(exc: Exception) -> MagicMock:
    """Engine that raises on connect()."""
    engine = MagicMock()
    engine.connect.side_effect = exc
    return engine


# ── _lookup_latest_value ─────────────────────────────────────────────────


def test_lookup_latest_value_returns_first_candidate_hit() -> None:
    # "spy" hits before "spy_full" / "spy_usd_full".
    engine = _make_engine_for_fetchone(
        {"spy": (450.0, "2026-05-13", "spy")},
    )
    value, obs_date, source = _lookup_latest_value(engine, "SPY")
    assert value == pytest.approx(450.0)
    assert obs_date == "2026-05-13"
    assert source == "spy"


def test_lookup_latest_value_falls_through_to_full_suffix() -> None:
    # Bare "btc" misses (None); "btc_full" is the fallback.
    engine = _make_engine_for_fetchone(
        {"btc": None, "btc_full": (65000.0, "2026-05-13", "btc_full")},
    )
    value, obs_date, source = _lookup_latest_value(engine, "BTC")
    assert value == pytest.approx(65000.0)
    assert source == "btc_full"


def test_lookup_latest_value_falls_through_to_usd_full_suffix() -> None:
    # Bare and *_full miss; *_usd_full is the last candidate.
    engine = _make_engine_for_fetchone(
        {
            "eth": None,
            "eth_full": None,
            "eth_usd_full": (3000.0, "2026-05-13", "eth_usd_full"),
        },
    )
    value, _obs_date, source = _lookup_latest_value(engine, "ETH")
    assert value == pytest.approx(3000.0)
    assert source == "eth_usd_full"


def test_lookup_latest_value_returns_none_triple_when_all_miss() -> None:
    engine = _make_engine_for_fetchone({})  # every candidate -> None
    assert _lookup_latest_value(engine, "ZZZ") == (None, None, None)


def test_lookup_latest_value_skips_row_with_null_value() -> None:
    # A row with value=None must not short-circuit; verifier expects None triple
    # so the caller falls through to "insufficient".
    engine = _make_engine_for_fetchone(
        {"spy": (None, "2026-05-13", "spy")},
    )
    assert _lookup_latest_value(engine, "SPY") == (None, None, None)


def test_lookup_latest_value_returns_none_triple_on_engine_exception() -> None:
    engine = _make_engine_raising(RuntimeError("DB down"))
    assert _lookup_latest_value(engine, "SPY") == (None, None, None)


# ── _lookup_feature_value ────────────────────────────────────────────────


def test_lookup_feature_value_returns_pair_on_hit() -> None:
    engine = _make_engine_for_fetchone({"vix_spot": (22.5, "2026-05-13")})
    value, obs_date = _lookup_feature_value(engine, "vix_spot")
    assert value == pytest.approx(22.5)
    assert obs_date == "2026-05-13"


def test_lookup_feature_value_returns_none_pair_on_miss() -> None:
    engine = _make_engine_for_fetchone({"vix_spot": None})
    assert _lookup_feature_value(engine, "vix_spot") == (None, None)


def test_lookup_feature_value_returns_none_pair_on_engine_exception() -> None:
    engine = _make_engine_raising(RuntimeError("DB down"))
    assert _lookup_feature_value(engine, "anything") == (None, None)


# ── _lookup_price_change ─────────────────────────────────────────────────


def test_lookup_price_change_returns_latest_and_oldest_when_enough_rows() -> None:
    # DESC-ordered rows: [latest, ..., previous]. Function returns (rows[0], rows[-1]).
    engine = _make_engine_for_fetchall(
        {"spy": [(460.0,), (455.0,), (450.0,)]},
    )
    latest, previous = _lookup_price_change(engine, "SPY", periods=3)
    assert latest == pytest.approx(460.0)
    assert previous == pytest.approx(450.0)


def test_lookup_price_change_returns_none_pair_when_only_one_row() -> None:
    engine = _make_engine_for_fetchall({"spy": [(460.0,)]})
    assert _lookup_price_change(engine, "SPY") == (None, None)


def test_lookup_price_change_falls_through_to_full_suffix() -> None:
    engine = _make_engine_for_fetchall(
        {"btc": [], "btc_full": [(65000.0,), (64000.0,)]},
    )
    latest, previous = _lookup_price_change(engine, "BTC")
    assert latest == pytest.approx(65000.0)
    assert previous == pytest.approx(64000.0)


def test_lookup_price_change_returns_none_pair_on_engine_exception() -> None:
    engine = _make_engine_raising(RuntimeError("DB down"))
    assert _lookup_price_change(engine, "SPY") == (None, None)


# ── _verify_price ────────────────────────────────────────────────────────


def _price_claim(ticker: str | None, value: float | None) -> Claim:
    return Claim(text="x", claim_type="price", ticker=ticker, value=value)


def test_verify_price_insufficient_when_ticker_missing() -> None:
    out = _verify_price(_price_claim(None, 100.0), MagicMock())
    assert out.verdict == "insufficient"
    assert "No ticker" in out.reason


def test_verify_price_insufficient_when_value_missing() -> None:
    out = _verify_price(_price_claim("SPY", None), MagicMock())
    assert out.verdict == "insufficient"


def test_verify_price_insufficient_when_no_db_data() -> None:
    engine = _make_engine_for_fetchone({})  # every candidate -> None
    out = _verify_price(_price_claim("ZZZ", 100.0), engine)
    assert out.verdict == "insufficient"
    assert "No DB data for ZZZ" in out.reason


def test_verify_price_supported_when_within_tolerance() -> None:
    # Claimed 102, actual 100 -> 2% diff (within 5% threshold).
    engine = _make_engine_for_fetchone({"spy": (100.0, "2026-05-13", "spy")})
    out = _verify_price(_price_claim("SPY", 102.0), engine)
    assert out.verdict == "supported"
    assert out.confidence == pytest.approx(0.9)
    assert out.evidence_value == pytest.approx(100.0)
    assert out.evidence_source == "spy"


def test_verify_price_contradicted_when_outside_tolerance() -> None:
    # Claimed 120, actual 100 -> 20% diff (>5%).
    engine = _make_engine_for_fetchone({"spy": (100.0, "2026-05-13", "spy")})
    out = _verify_price(_price_claim("SPY", 120.0), engine)
    assert out.verdict == "contradicted"
    assert out.evidence_value == pytest.approx(100.0)


def test_verify_price_boundary_at_5_percent_is_supported() -> None:
    # Exactly 5% diff -> supported (the comparison is <=).
    engine = _make_engine_for_fetchone({"spy": (100.0, "2026-05-13", "spy")})
    out = _verify_price(_price_claim("SPY", 105.0), engine)
    assert out.verdict == "supported"


def test_verify_price_zero_actual_short_circuits_to_supported() -> None:
    # `_lookup_latest_value`'s `row[0] is not None` guard lets 0.0 through
    # (only NULL is filtered), so `_verify_price` sees actual=0.0 and the
    # `actual != 0` ternary short-circuits pct_diff to 0 -> "supported".
    # Pins behavior to surface the edge case; a future caller that treats
    # 0.0 as missing data must update this test along with the guard.
    engine = _make_engine_for_fetchone({"spy": (0.0, "2026-05-13", "spy")})
    out = _verify_price(_price_claim("SPY", 100.0), engine)
    assert out.verdict == "supported"
    assert out.evidence_value == pytest.approx(0.0)


# ── _verify_percentage ───────────────────────────────────────────────────


def _pct_claim(ticker: str | None, value: float | None) -> Claim:
    return Claim(text="x", claim_type="percentage", ticker=ticker, value=value)


def test_verify_percentage_insufficient_when_ticker_missing() -> None:
    out = _verify_percentage(_pct_claim(None, 5.0), MagicMock())
    assert out.verdict == "insufficient"


def test_verify_percentage_insufficient_when_only_one_row() -> None:
    engine = _make_engine_for_fetchall({"spy": [(100.0,)]})
    out = _verify_percentage(_pct_claim("SPY", 5.0), engine)
    assert out.verdict == "insufficient"


def test_verify_percentage_supported_within_3pp_tolerance() -> None:
    # latest=105, previous=100 -> +5.0%. Claim +5.0% -> 0pp diff (supported).
    engine = _make_engine_for_fetchall({"spy": [(105.0,), (100.0,)]})
    out = _verify_percentage(_pct_claim("SPY", 5.0), engine)
    assert out.verdict == "supported"
    assert out.confidence == pytest.approx(0.85)
    assert out.evidence_value == pytest.approx(5.0)


def test_verify_percentage_contradicted_outside_3pp_tolerance() -> None:
    # actual +5%, claim +15% -> 10pp diff (>3pp).
    engine = _make_engine_for_fetchall({"spy": [(105.0,), (100.0,)]})
    out = _verify_percentage(_pct_claim("SPY", 15.0), engine)
    assert out.verdict == "contradicted"


def test_verify_percentage_boundary_at_3pp_is_supported() -> None:
    # actual +5%, claim +8% -> 3pp diff (<=).
    engine = _make_engine_for_fetchall({"spy": [(105.0,), (100.0,)]})
    out = _verify_percentage(_pct_claim("SPY", 8.0), engine)
    assert out.verdict == "supported"


# ── _verify_direction ────────────────────────────────────────────────────


def _dir_claim(ticker: str | None, value: float | None) -> Claim:
    return Claim(text="x", claim_type="direction", ticker=ticker, value=value)


def test_verify_direction_insufficient_when_ticker_missing() -> None:
    out = _verify_direction(_dir_claim(None, 1.0), MagicMock())
    assert out.verdict == "insufficient"


def test_verify_direction_insufficient_when_no_price_data() -> None:
    engine = _make_engine_for_fetchall({"spy": []})
    out = _verify_direction(_dir_claim("SPY", 1.0), engine)
    assert out.verdict == "insufficient"


def test_verify_direction_supported_both_up() -> None:
    engine = _make_engine_for_fetchall({"spy": [(110.0,), (100.0,)]})  # actual up
    out = _verify_direction(_dir_claim("SPY", 1.0), engine)  # claim up
    assert out.verdict == "supported"
    assert "up" in out.reason


def test_verify_direction_supported_both_down() -> None:
    engine = _make_engine_for_fetchall({"spy": [(90.0,), (100.0,)]})  # actual down
    out = _verify_direction(_dir_claim("SPY", -1.0), engine)  # claim down
    assert out.verdict == "supported"
    assert "down" in out.reason


def test_verify_direction_contradicted_when_mismatch() -> None:
    engine = _make_engine_for_fetchall({"spy": [(90.0,), (100.0,)]})  # actual down
    out = _verify_direction(_dir_claim("SPY", 1.0), engine)  # claim up
    assert out.verdict == "contradicted"


# ── _verify_generic ──────────────────────────────────────────────────────


def test_verify_generic_returns_ambiguous() -> None:
    claim = Claim(text="x", claim_type="narrative")
    out = _verify_generic(claim)
    assert out.verdict == "ambiguous"
    assert out.confidence == pytest.approx(0.3)
    assert "narrative" in out.reason


# ── verify_claims dispatcher ─────────────────────────────────────────────


def test_verify_claims_dispatches_by_claim_type() -> None:
    # Mixed batch: price -> _verify_price (supported),
    #              direction -> _verify_direction (supported),
    #              narrative -> _verify_generic (ambiguous).
    fetchone_rows = {"spy": (100.0, "2026-05-13", "spy")}
    fetchall_rows = {"spy": [(110.0,), (100.0,)]}

    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value

    def _execute(_sql, params):
        result = MagicMock()
        result.fetchone.return_value = fetchone_rows.get(params["name"])
        result.fetchall.return_value = fetchall_rows.get(params["name"], [])
        return result

    conn.execute.side_effect = _execute

    claims = [
        Claim(text="SPY is $100", claim_type="price", ticker="SPY", value=100.0),
        Claim(text="SPY went up", claim_type="direction", ticker="SPY", value=1.0),
        Claim(text="bullish narrative", claim_type="narrative"),
    ]
    out = verify_claims(claims, engine)
    assert [v.verdict for v in out] == ["supported", "supported", "ambiguous"]
    assert all(isinstance(v, VerifiedClaim) for v in out)


def test_verify_claims_unknown_type_falls_through_to_generic() -> None:
    # Any claim_type not in _VERIFIERS hits _verify_generic.
    claim = Claim(text="x", claim_type="indicator")
    out = verify_claims([claim], MagicMock())
    assert len(out) == 1
    assert out[0].verdict == "ambiguous"


def test_verify_claims_preserves_order_and_length() -> None:
    claims = [
        Claim(text=f"c{i}", claim_type="narrative") for i in range(5)
    ]
    out = verify_claims(claims, MagicMock())
    assert len(out) == 5
    assert [v.claim.text for v in out] == [f"c{i}" for i in range(5)]
