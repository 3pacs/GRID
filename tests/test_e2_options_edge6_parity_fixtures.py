"""Synthetic parity fixtures for a future E2 options cost class (proposal only).

Nothing here imports or changes ``evals.e2``. The fixture holds synthetic
four-leg quotes and the per-row values that the GEX edge6 ``price()`` (vrp_edge6.py
at 35fa182, blob 692a979a) returns for them. A future ``us_equity_option_0dte``
implementation must reproduce those values to 4 decimals. No market data is read.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "e2_options" / "edge6_parity_synthetic.json"
FIXTURE_SHA256 = "b61f7d5144de6dac943ad1cdc3df75e6f172629dace11065348f25e6a18a6d2a"
FEE, WING = 0.0003, 5


def _load() -> dict:
    return json.loads(FIXTURE.read_text())


def _quotes(raw: dict) -> dict:
    return {(k.split(":")[0], float(k.split(":")[1])): tuple(v) for k, v in raw.items()}


def reference_row(q: dict, K: float, settle: float):
    """The v2 per-row rule, in edge6's exact float expression order."""
    legs = [q.get(("C", K)), q.get(("P", K)), q.get(("C", K + WING)), q.get(("P", K - WING))]
    if any(v is None for v in legs):
        return None
    (cb, ca), (pb, pa), (_cwb, cwa), (_pwb, pwa) = legs
    if not all(x is not None and x > 0 for x in (cb, pb, cwa, pwa, ca, pa)):
        return None
    straddle_pay = max(settle - K, 0.0) + max(K - settle, 0.0)
    wing_pay = max(settle - K - WING, 0.0) + max(K - WING - settle, 0.0)
    credit = cb + pb - cwa - pwa
    return {"credit": round(credit, 4), "fly": round(credit - straddle_pay + wing_pay - 4 * FEE, 4)}


def test_fixture_bytes_are_pinned():
    assert hashlib.sha256(FIXTURE.read_bytes()).hexdigest() == FIXTURE_SHA256


def test_fixture_is_synthetic_and_names_its_oracle():
    data = _load()
    assert data["synthetic"] is True
    assert "35fa182" in data["oracle"] and data["fee"] == FEE and data["wing"] == WING


@pytest.mark.parametrize("case", _load()["cases"], ids=lambda c: c["name"])
def test_reference_row_matches_edge6(case):
    got = reference_row(_quotes(case["quotes"]), case["K"], case["settle"])
    want = case["edge6"]
    if want is None:
        assert got is None
    else:
        assert got == {"credit": want["credit"], "fly": want["fly"]}


def test_fixture_covers_the_shapes_d0_names():
    cases = {c["name"]: c["edge6"] for c in _load()["cases"]}
    assert cases["settle_on_short_strike_payoff_zero"] is not None  # exit payoff 0 is a valid row
    assert cases["nonpositive_credit"]["credit"] < 0  # a losing credit stays a row
    assert cases["zero_credit_exact"]["credit"] == 0
    assert cases["missing_leg_returns_none"] is None  # failed section is excluded, not zero
    assert cases["nonpositive_bid_returns_none"] is None


def test_fee_is_cash_on_four_entry_legs_not_a_return():
    # Same quotes, settle on the short strike: fly = credit - 4*FEE exactly at 4 dp.
    case = next(c for c in _load()["cases"] if c["name"] == "settle_on_short_strike_payoff_zero")
    assert case["edge6"]["fly"] == round(case["edge6"]["credit"] - 4 * FEE, 4)
