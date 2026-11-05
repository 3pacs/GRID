"""Selection contract for the 100x digest (``alerts/hundredx_digest.py``).

``run_100x_digest`` picks what gets verified and in what order it ships:

1. scanner rows are sorted by score (descending);
2. rows failing ``_sanity_check`` are dropped;
3. at most 15 of the remaining rows are verified (``all_opps[:15]``);
4. the cards are emitted 100x rows first, then by score (descending).

These tests pin steps 1-4 with synthetic rows. Every collaborator is an
in-test adapter: the scanner module, the DB engine, the LLM HTTP client, the
``config`` module, per-row verification and the email sender. No provider,
database, SMTP or yfinance call can happen — the adapters raise if reached.
"""

from __future__ import annotations

import dataclasses
import re
import smtplib
import sys
import types
from dataclasses import dataclass, field
from typing import Any

import pytest

from alerts import hundredx_digest as hx

VERIFY_CAP = 15

_CARD_TITLE = re.compile(r"^\[#(\d+) — (\S+) (CALL|PUT)S\]$", re.MULTILINE)


# ── Synthetic scanner rows ──────────────────────────────────────────────────

@dataclass(frozen=True)
class _Opp:
    """Duck-typed stand-in for ``discovery.options_scanner.MispricingOpportunity``.

    Defaults pass ``_sanity_check``: max pain (``strikes[0]``) sits at spot,
    IV is in range, PCR is low, and the score is never exactly 5.0.
    """

    ticker: str
    score: float
    estimated_payoff_multiple: float = 20.0
    direction: str = "CALL"
    thesis: str = "synthetic selection fixture"
    signals: dict[str, Any] = field(default_factory=lambda: {"pcr": {"value": 1.0, "raw_value": 1.0}})
    strikes: list[float] = field(default_factory=lambda: [100.0])
    expiry: str = "2026-11-20"
    spot_price: float = 100.0
    iv_atm: float | None = 0.40
    confidence: str = "MEDIUM"

    @property
    def is_100x(self) -> bool:
        return bool(self.estimated_payoff_multiple >= 100.0)


def _universe(n: int, hundredx_ranks: frozenset[int] = frozenset()) -> list[_Opp]:
    """``n`` sanity-clean rows with distinct scores; rank 0 scores highest.

    Returned in a scrambled order so the digest's own score sort is exercised
    rather than inherited from the fixture.
    """
    rows = [
        _Opp(
            ticker=f"R{rank:02d}",
            score=round(9.9 - 0.1 * rank, 2),  # 9.9 down to 5.1 for n<=49; never 5.0
            estimated_payoff_multiple=150.0 if rank in hundredx_ranks else 20.0,
        )
        for rank in range(n)
    ]
    return rows[1::2] + rows[0::2]


# ── Isolated adapters ───────────────────────────────────────────────────────

class _NoDbEngine:
    """Engine adapter: any connection attempt fails like an unreachable DB."""

    def connect(self):
        raise RuntimeError("database access is not allowed in this test")


class _Resp:
    def __init__(self, status_code: int, payload: dict | None = None) -> None:
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict:
        return self._payload


def _offline_requests() -> types.ModuleType:
    """HTTP adapter for an unreachable LLM: ``/props`` fails, POST must never run."""
    mod = types.ModuleType("requests")

    def get(url, timeout=None, **_):
        raise ConnectionError(f"provider call blocked in test: GET {url}")

    def post(url, **_):
        raise AssertionError(f"provider call blocked in test: POST {url}")

    mod.get, mod.post = get, post
    return mod


def _scripted_llm_requests(verdicts_json: str, calls: list[str]) -> types.ModuleType:
    """HTTP adapter for a reachable LLM that answers with a fixed verdict array."""
    mod = types.ModuleType("requests")

    def get(url, timeout=None, **_):
        assert url == "http://llm.invalid/props"
        calls.append("GET /props")
        return _Resp(200)

    def post(url, json=None, timeout=None, **_):
        assert url == "http://llm.invalid/v1/chat/completions"
        calls.append("POST /v1/chat/completions")
        return _Resp(200, {"choices": [{"message": {"content": verdicts_json}}]})

    mod.get, mod.post = get, post
    return mod


@dataclass
class _Run:
    result: dict
    verified: list[str]
    cards: list[tuple[int, str]]
    sent: dict


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    """Block every outbound side effect for every test in this file."""
    monkeypatch.setitem(sys.modules, "requests", _offline_requests())
    monkeypatch.setitem(sys.modules, "config", types.SimpleNamespace(
        settings=types.SimpleNamespace(LLAMACPP_BASE_URL="http://llm.invalid")))
    monkeypatch.setattr(hx, "_get_engine", lambda: _NoDbEngine())
    monkeypatch.setattr(hx.time, "sleep", lambda *_: None)

    def _no_smtp(*a, **k):
        raise AssertionError("SMTP is not allowed in this test")

    monkeypatch.setattr(smtplib, "SMTP", _no_smtp)
    monkeypatch.setattr(smtplib, "SMTP_SSL", _no_smtp)


def _run(monkeypatch, rows: list[_Opp]) -> _Run:
    """Drive ``run_100x_digest`` over ``rows`` and record what it selected."""
    scanner_mod = types.ModuleType("discovery.options_scanner")

    class OptionsScanner:
        def __init__(self, db_engine=None) -> None:
            assert isinstance(db_engine, _NoDbEngine)

        def scan_all(self) -> list[_Opp]:
            return list(rows)

    scanner_mod.OptionsScanner = OptionsScanner
    monkeypatch.setitem(sys.modules, "discovery.options_scanner", scanner_mod)

    verified: list[str] = []

    def fake_verify(ticker, opp, engine):
        verified.append(ticker)
        return {"verified": True, "issues": [], "corrections": [], "bonus_data": [],
                "live_spot": None, "live_chain": None, "recommended_strikes": []}

    monkeypatch.setattr(hx, "_verify_opportunity", fake_verify)

    sent: dict = {}
    monkeypatch.setattr(hx, "_send_in_thread",
                        lambda subject, html, plain: sent.update(subject=subject, html=html, plain=plain))

    result = hx.run_100x_digest()
    cards = [(int(i), t) for i, t, _ in _CARD_TITLE.findall(sent.get("plain", ""))]
    return _Run(result=result, verified=verified, cards=cards, sent=sent)


def _r(*ranks: int) -> list[str]:
    return [f"R{k:02d}" for k in ranks]


# ── Fixture contract ────────────────────────────────────────────────────────

def test_fixture_mirrors_the_scanner_row_shape():
    """Guards the stand-in against drift from the real dataclass it replaces."""
    from discovery.options_scanner import MispricingOpportunity

    real = {f.name for f in dataclasses.fields(MispricingOpportunity)}
    fake = {f.name for f in dataclasses.fields(_Opp)}
    assert fake <= real  # only scan_date is left out; the digest never reads it

    from datetime import date

    for payoff in (99.99, 100.0, 150.0):
        real_row = MispricingOpportunity(
            ticker="X", scan_date=date(2026, 10, 20), score=7.0,
            estimated_payoff_multiple=payoff, direction="CALL", thesis="t",
        )
        assert _Opp("X", 7.0, estimated_payoff_multiple=payoff).is_100x is real_row.is_100x


# ── 15-row verification cap ─────────────────────────────────────────────────

@pytest.mark.parametrize("n", [1, 7, 14, 15, 16, 40])
def test_verification_is_capped_at_15_rows(monkeypatch, n):
    run = _run(monkeypatch, _universe(n))

    expected = min(n, VERIFY_CAP)
    assert len(run.verified) == expected
    assert run.result["opportunities"] == expected
    assert len(run.cards) == expected


def test_cap_keeps_the_15_highest_scores_in_score_order(monkeypatch):
    run = _run(monkeypatch, _universe(40))

    assert run.verified == _r(*range(15))
    assert {"R15", "R39"}.isdisjoint(run.verified)
    assert run.result["total_scanned"] == 40


def test_sanity_rejects_do_not_consume_cap_slots(monkeypatch):
    rows = _universe(20)
    # Make the three best-scoring rows fail the PCR sanity bound (>20).
    garbage = {"R00", "R01", "R02"}
    rows = [dataclasses.replace(o, signals={"pcr": {"value": 25.0}}) if o.ticker in garbage else o
            for o in rows]

    run = _run(monkeypatch, rows)

    assert run.verified == _r(*range(3, 18))
    assert garbage.isdisjoint(run.verified)


def test_cap_is_applied_by_score_before_100x_priority(monkeypatch):
    """Current behaviour: a 100x row ranked 16th or lower by score is never verified.

    The cap slices the score-sorted list; 100x priority only reorders what
    survived it. Changing that is a product decision — this test makes it a
    deliberate one.
    """
    run = _run(monkeypatch, _universe(20, hundredx_ranks=frozenset({3, 17})))

    assert "R17" not in run.verified
    assert "R17" not in [t for _, t in run.cards]
    assert run.result["100x_count"] == 1


# ── Final ordering: 100x first, then score ──────────────────────────────────

def test_cards_are_100x_first_then_by_score(monkeypatch):
    run = _run(monkeypatch, _universe(20, hundredx_ranks=frozenset({3, 9, 14, 17})))

    expected = _r(3, 9, 14) + _r(0, 1, 2, 4, 5, 6, 7, 8, 10, 11, 12, 13)
    assert [t for _, t in run.cards] == expected
    assert [i for i, _ in run.cards] == list(range(1, 16))
    assert run.result["100x_count"] == 3
    assert run.sent["subject"] == "GRID Intelligence — 3 100x + 12 High-Score Opportunities"


def test_html_body_carries_the_same_order(monkeypatch):
    run = _run(monkeypatch, _universe(6, hundredx_ranks=frozenset({4})))

    titles = re.findall(r'<div class="card-title">#\d+ — (\S+) CALLS</div>', run.sent["html"])
    assert titles == _r(4, 0, 1, 2, 3, 5)


def test_100x_rows_are_ordered_by_score_among_themselves(monkeypatch):
    run = _run(monkeypatch, _universe(5, hundredx_ranks=frozenset({0, 2, 4})))

    assert [t for _, t in run.cards] == _r(0, 2, 4, 1, 3)


def test_ordering_is_reapplied_after_llm_review(monkeypatch):
    """A reachable LLM answers in verification (score) order and drops FAILs;
    the shipped order is still 100x first, then score."""
    calls: list[str] = []
    # Verification order is R00..R05; index N refers to R0(N-1).
    verdicts = ('[{"index": 1, "verdict": "PASS", "reason": "ok"}, '
                '{"index": 2, "verdict": "FAIL", "reason": "bad"}, '
                '{"index": 3, "verdict": "SUSPECT", "reason": "check"}, '
                '{"index": 4, "verdict": "FAIL", "reason": "bad"}, '
                '{"index": 5, "verdict": "PASS", "reason": "ok"}, '
                '{"index": 6, "verdict": "PASS", "reason": "ok"}]')
    monkeypatch.setitem(sys.modules, "requests", _scripted_llm_requests(verdicts, calls))

    run = _run(monkeypatch, _universe(6, hundredx_ranks=frozenset({1, 3, 5})))

    assert calls == ["GET /props", "POST /v1/chat/completions"]
    assert run.verified == _r(0, 1, 2, 3, 4, 5)
    assert [t for _, t in run.cards] == _r(5, 0, 2, 4)
    assert run.result["100x_count"] == 1
