"""The 100x digest must survive a NULL field on one opportunity.

Regression for the grid-hermes journal entry 2026-10-20 04:28:43Z:

    __main__:run_cycle:3856 — 100x digest failed:
    unsupported format string passed to NoneType.__format__

``MispricingOpportunity.iv_atm`` is ``float | None`` (nullable since #476,
``options_daily_signals.iv_atm`` is NULL for a thin chain — DNTH's
2026-10-16 row on that run). ``run_100x_digest`` copies it into a dict as a
*present* key, so ``opp.get("iv_atm", 0)`` returns ``None`` (the default only
covers an absent key) and ``f"{None:.1%}"`` raised inside
``_llm_sanity_review``, outside its try block, taking down the whole digest.
"""

from __future__ import annotations

import sys
import types

import pytest

from alerts import hundredx_digest as hx


def _opp(**overrides) -> dict:
    """Shape of the dict ``run_100x_digest`` builds from a MispricingOpportunity."""
    base = {
        "ticker": "DNTH",
        "direction": "PUT",
        "score": 6.4,
        "payoff_multiple": 42.0,
        "thesis": "IV skew dislocated",
        "spot_price": 72.09,
        "iv_atm": None,  # NULL column, present key
        "signals": {"pcr": {"raw_value": 1.3}},
        "strikes": [65.0],
        "expiry": "2026-11-20",
        "confidence": "MEDIUM",
        "is_100x": False,
    }
    base.update(overrides)
    return base


# ── _fmt ────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    ("value", "spec", "expected"),
    [
        (None, ".1%", "N/A"),
        (float("nan"), ".2f", "N/A"),
        (0.25, ".1%", "25.0%"),
        (72.09, ".2f", "72.09"),
        (0, ".0f", "0"),
        ("1.3", "", "1.3"),
    ],
)
def test_fmt_renders_missing_as_placeholder(value, spec, expected):
    assert hx._fmt(value, spec) == expected


def test_fmt_custom_placeholder():
    assert hx._fmt(None, ".1f", missing="n/a") == "n/a"


# ── LLM review prompt line (the crash site) ─────────────────────────────────

def test_summary_line_with_null_iv_atm_does_not_raise():
    line = hx._opportunity_summary_line(1, _opp())
    assert line.startswith("1. DNTH PUTS — score 6.4/10, payoff est 42x, spot $72.09, IV ATM N/A")


def test_summary_line_with_every_nullable_field_none():
    line = hx._opportunity_summary_line(
        2, _opp(score=None, payoff_multiple=None, spot_price=None, iv_atm=None, thesis=None)
    )
    assert line == "2. DNTH PUTS — score N/A/10, payoff est N/Ax, spot $N/A, IV ATM N/A, thesis: "


class _Resp:
    def __init__(self, status_code: int, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload


def test_llm_sanity_review_survives_null_iv_atm(monkeypatch):
    """End-to-end through ``_llm_sanity_review`` with the LLM stubbed as reachable.

    On main this raised ``TypeError: unsupported format string passed to
    NoneType.__format__`` while building the prompt.
    """
    import requests

    fake_config = types.SimpleNamespace(
        settings=types.SimpleNamespace(LLAMACPP_BASE_URL="http://llm.invalid")
    )
    monkeypatch.setitem(sys.modules, "config", fake_config)

    prompts: list[str] = []

    def fake_get(url, timeout=None, **_):
        assert url.endswith("/props")
        return _Resp(200)

    def fake_post(url, json=None, timeout=None, **_):
        prompts.append(json["messages"][0]["content"])
        content = '[{"index": 1, "verdict": "SUSPECT", "reason": "iv missing"}, ' \
                  '{"index": 2, "verdict": "PASS", "reason": "ok"}]'
        return _Resp(200, {"choices": [{"message": {"content": content}}]})

    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr(requests, "post", fake_post)

    opps = [_opp(), _opp(ticker="BMRN", iv_atm=0.6009561389160156, score=7.1)]
    reviewed = hx._llm_sanity_review(opps, engine=None)

    assert [o["ticker"] for o in reviewed] == ["DNTH", "BMRN"]
    assert reviewed[0]["llm_verdict"] == "SUSPECT"
    assert reviewed[1]["llm_verdict"] == "PASS"
    assert len(prompts) == 1
    assert "1. DNTH PUTS — score 6.4/10, payoff est 42x, spot $72.09, IV ATM N/A" in prompts[0]
    assert "2. BMRN PUTS — score 7.1/10, payoff est 42x, spot $72.09, IV ATM 60.1%" in prompts[0]


# ── Email card ──────────────────────────────────────────────────────────────

def test_opportunity_card_with_null_fields_renders_placeholders():
    opp = _opp(score=None, payoff_multiple=None, spot_price=None, iv_atm=None)
    verification = {"verified": False, "issues": [], "corrections": [], "bonus_data": [],
                    "live_spot": None, "recommended_strikes": []}

    card = hx._format_opportunity_card(opp, verification, 3)

    assert card["title"] == "#3 — DNTH PUTS"
    body = card["body"]
    assert "N/A/10" in body          # score
    assert "N/Ax" in body            # payoff header
    assert "NoneType" not in body
    assert "None" not in body


def test_opportunity_card_null_iv_atm_only():
    opp = _opp()
    verification = {"verified": True, "issues": [], "corrections": [], "bonus_data": [],
                    "live_spot": 72.5, "recommended_strikes": []}

    body = hx._format_opportunity_card(opp, verification, 1)["body"]

    assert "42x" in body
    assert "6.4/10" in body
    assert "$72.50" in body
    assert "None" not in body


# ── Whole digest ────────────────────────────────────────────────────────────

def _stub_llm(monkeypatch, verdicts: str):
    import requests

    monkeypatch.setitem(sys.modules, "config", types.SimpleNamespace(
        settings=types.SimpleNamespace(LLAMACPP_BASE_URL="http://llm.invalid")))
    monkeypatch.setattr(requests, "get", lambda url, timeout=None, **_: _Resp(200))
    monkeypatch.setattr(requests, "post", lambda url, json=None, timeout=None, **_: _Resp(
        200, {"choices": [{"message": {"content": verdicts}}]}))


def _run_digest_with(monkeypatch, opportunities):
    """Drive ``run_100x_digest`` with scanner/DB/yfinance/email stubbed out."""
    import discovery.options_scanner as scanner_mod

    class _Scanner:
        def __init__(self, db_engine=None):
            pass

        def scan_all(self):
            return opportunities

    class _Conn:
        def __enter__(self):
            raise RuntimeError("no DB in unit test")

        def __exit__(self, *a):
            return False

    class _Engine:
        def connect(self):
            return _Conn()

    sent: dict = {}
    monkeypatch.setattr(scanner_mod, "OptionsScanner", _Scanner)
    monkeypatch.setattr(hx, "_get_engine", lambda: _Engine())
    monkeypatch.setattr(hx, "_verify_opportunity", lambda t, o, e: {
        "verified": True, "issues": [], "corrections": [], "bonus_data": [],
        "live_spot": None, "live_chain": None, "recommended_strikes": []})
    monkeypatch.setattr(hx.time, "sleep", lambda *_: None)
    monkeypatch.setattr(hx, "_send_in_thread",
                        lambda subject, html, plain: sent.update(subject=subject, html=html, plain=plain))
    result = hx.run_100x_digest()
    return result, sent


def _real_opps():
    from datetime import date

    from discovery.options_scanner import MispricingOpportunity

    return [
        MispricingOpportunity(
            ticker="DNTH", scan_date=date(2026, 10, 20), score=6.4,
            estimated_payoff_multiple=42.0, direction="PUT", thesis="IV skew dislocated",
            signals={"pcr": {"raw_value": 1.3, "value": 1.3}}, strikes=[65.0],
            expiry="2026-11-20", spot_price=72.09, iv_atm=None, confidence="MEDIUM",
        ),
        MispricingOpportunity(
            ticker="BMRN", scan_date=date(2026, 10, 20), score=7.1,
            estimated_payoff_multiple=55.0, direction="PUT", thesis="PCR extreme",
            signals={"pcr": {"raw_value": 2.0, "value": 2.0}}, strikes=[50.0],
            expiry="2026-11-20", spot_price=54.67, iv_atm=0.6009561389160156, confidence="HIGH",
        ),
    ]


def test_run_100x_digest_sends_with_null_iv_atm_row(monkeypatch):
    """The 2026-10-20 04:28Z shape: one NULL-iv_atm row among reviewed opps."""
    _stub_llm(monkeypatch, '[{"index": 1, "verdict": "SUSPECT", "reason": "iv missing"}, '
                           '{"index": 2, "verdict": "PASS", "reason": "ok"}]')

    result, sent = _run_digest_with(monkeypatch, _real_opps())

    assert result["sent"] is True
    assert result["opportunities"] == 2
    assert sent["subject"].endswith("0 100x + 2 High-Score Opportunities")
    assert "DNTH PUTS" in sent["html"] and "BMRN PUTS" in sent["html"]
    assert "60.1%" in sent["html"]
    assert "None" not in sent["plain"]


def test_run_100x_digest_annotates_a_card_that_fails(monkeypatch):
    """A card that still raises is replaced by an annotated section; the rest ship."""
    _stub_llm(monkeypatch, '[{"index": 1, "verdict": "PASS", "reason": "ok"}, '
                           '{"index": 2, "verdict": "PASS", "reason": "ok"}]')
    real_card = hx._format_opportunity_card

    def boom(opp, v, i):
        if opp["ticker"] == "DNTH":
            raise TypeError("unsupported format string passed to NoneType.__format__")
        return real_card(opp, v, i)

    monkeypatch.setattr(hx, "_format_opportunity_card", boom)

    result, sent = _run_digest_with(monkeypatch, _real_opps())

    assert result["sent"] is True
    assert result["opportunities"] == 2
    assert "Card not rendered (missing/invalid fields)" in sent["plain"]
    assert "BMRN PUTS" in sent["html"]
