"""Selection contract for the 100x digest (``alerts/hundredx_digest.py``).

``run_100x_digest`` picks what gets verified and in what order it ships:

1. scanner rows are sorted by score (descending);
2. rows failing ``_sanity_check`` are dropped;
3. ``_selection_gate`` withholds the 100x tier, suppressed directions (CALL)
   and stale signal rows — none of them consumes a verification slot;
4. at most 15 of the remaining rows are verified;
5. a row whose verification names no contract is withheld;
6. contracts emailed inside the re-send window are withheld, and nothing is
   sent when nothing new is left;
7. every remaining call is written to the ledger before the email is sent,
   and a ledger read or write failure sends nothing;
8. the cards ship in score order (descending).

These tests pin steps 1-8 with synthetic rows. Every collaborator is an
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

_CARD_TITLE = re.compile(r"^\[#(\d+) — (\S+) (CALL|PUT)S(?: \(UNPROVEN\))?\]$", re.MULTILINE)


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
    direction: str = "PUT"
    thesis: str = "synthetic selection fixture"
    signals: dict[str, Any] = field(default_factory=lambda: {"pcr": {"value": 1.0, "raw_value": 1.0}})
    strikes: list[float] = field(default_factory=lambda: [100.0])
    expiry: str = "2026-11-20"
    spot_price: float = 100.0
    iv_atm: float | None = 0.40
    confidence: str = "MEDIUM"
    signal_date: Any = None
    signal_captured_at: Any = None

    @property
    def is_100x(self) -> bool:
        return bool(self.estimated_payoff_multiple >= 100.0)


def _universe(n: int, hundredx_ranks: frozenset[int] = frozenset()) -> list[_Opp]:
    """``n`` sanity-clean PUT rows with distinct scores; rank 0 scores highest.

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
    persisted: list[dict]
    events: list[str]


def _contract(ticker: str, strike: float = 98.0) -> dict:
    from datetime import datetime, timezone

    return {"strike": strike, "bid": 2.00, "ask": 2.20, "iv": 0.40, "oi": 500, "volume": 20,
            "last_trade": None, "expiry": "2026-12-18",
            "quote_at": datetime(2026, 11, 3, 15, 0, tzinfo=timezone.utc),
            "quote_source": "test chain"}


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


def _run(monkeypatch, rows: list[_Opp], *, recent: list[dict] | None = None,
         no_contract: frozenset[str] = frozenset(), ledger_read_error: bool = False,
         ledger_write_error: bool = False, force: bool = False, scanner_error: bool = False) -> _Run:
    """Drive ``run_100x_digest`` over ``rows`` and record what it selected."""
    monkeypatch.setattr(hx, "_digest_paused", lambda: None)
    scanner_mod = types.ModuleType("discovery.options_scanner")

    class OptionsScanner:
        def __init__(self, db_engine=None) -> None:
            assert isinstance(db_engine, _NoDbEngine)

        def scan_all(self) -> list[_Opp]:
            if scanner_error:
                raise RuntimeError("synthetic scanner failure")
            return list(rows)

    scanner_mod.OptionsScanner = OptionsScanner
    monkeypatch.setitem(sys.modules, "discovery.options_scanner", scanner_mod)

    verified: list[str] = []
    events: list[str] = []

    def fake_verify(ticker, opp, engine):
        verified.append(ticker)
        contract = None if ticker in no_contract else _contract(ticker)
        return {"verified": contract is not None, "issues": [], "corrections": [],
                "bonus_data": [], "live_spot": 100.0, "spot_source": "test",
                "spot_at": None, "live_chain": None, "contract": contract,
                "recommended_strikes": [contract] if contract else []}

    monkeypatch.setattr(hx, "_verify_opportunity", fake_verify)

    def fake_recent(engine, since):
        if ledger_read_error:
            raise RuntimeError('relation "options_digest_calls" does not exist')
        return list(recent or [])

    persisted: list[dict] = []

    def fake_persist(engine, run_id, calls):
        if ledger_write_error:
            raise RuntimeError("permission denied for table options_digest_calls")
        events.append("persist")
        persisted.extend(calls)
        return len(calls)

    monkeypatch.setattr(hx, "_load_recent_calls", fake_recent)
    monkeypatch.setattr(hx, "_persist_calls", fake_persist)
    monkeypatch.setattr(hx, "_load_track_record", lambda engine, as_of: {
        "calls": [], "settled": 0, "settled_wins": 0, "settled_hit_rate": None,
        "settled_median_return": None, "open": 0, "open_median_return": None, "unpriced": 0})
    monkeypatch.setattr(hx, "_catalyst_before_expiry",
                        lambda engine, t, today, exp: {"status": "none_on_file", "detail": "test"})

    sent: dict = {}

    def fake_send(subject, html, plain):
        events.append("send")
        sent.update(subject=subject, html=html, plain=plain)

    monkeypatch.setattr(hx, "_send_in_thread", fake_send)

    result = hx.run_100x_digest(force=force)
    cards = [(int(i), t) for i, t, _ in _CARD_TITLE.findall(sent.get("plain", ""))]
    return _Run(result=result, verified=verified, cards=cards, sent=sent,
                persisted=persisted, events=events)


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
    assert VERIFY_CAP == hx.VERIFY_CAP
    assert len(run.verified) == expected
    assert run.result["opportunities"] == expected
    assert len(run.cards) == expected
    assert len(run.persisted) == expected


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


# ── Measured selection gates ────────────────────────────────────────────────

def test_100x_tier_is_withheld_and_does_not_consume_slots(monkeypatch):
    run = _run(monkeypatch, _universe(20, hundredx_ranks=frozenset({0, 3, 17})))

    assert {"R00", "R03", "R17"}.isdisjoint(run.verified)
    assert run.verified == _r(1, 2, *range(4, 17))
    assert run.result["gated"]["lottery_tier"] == 3
    assert run.result["100x_count"] == 0


def test_calls_are_sent_and_labelled_unproven(monkeypatch):
    # 2026-11-04 owner decision: send CALLs labelled, don't withhold them.
    rows = _universe(6)
    calls = {"R00", "R02"}
    rows = [dataclasses.replace(o, direction="CALL") if o.ticker in calls else o for o in rows]

    run = _run(monkeypatch, rows)

    assert calls <= set(run.verified)
    assert run.result["gated"].get("direction_suppressed", 0) == 0
    assert "CALLS (UNPROVEN)" in run.sent["plain"]
    assert "42.0% of the time over 5 days" in run.sent["plain"]
    assert {c["direction"] for c in run.persisted} == {"CALL", "PUT"}


def test_call_direction_gate_still_withholds_when_enabled(monkeypatch):
    from alerts import hundredx_digest as hd

    monkeypatch.setattr(hd, "SUPPRESSED_DIRECTIONS", frozenset({"CALL"}))
    rows = _universe(6)
    calls = {"R00", "R02"}
    rows = [dataclasses.replace(o, direction="CALL") if o.ticker in calls else o for o in rows]

    run = _run(monkeypatch, rows)

    assert calls.isdisjoint(run.verified)
    assert run.result["gated"]["direction_suppressed"] == 2
    assert "CALL withheld" in run.sent["plain"]
    assert all(c["direction"] == "PUT" for c in run.persisted)


def test_stale_signal_row_is_withheld(monkeypatch):
    from datetime import date, timedelta

    today = date.today()
    rows = [dataclasses.replace(o, signal_date=today - timedelta(days=30 if o.ticker == "R01" else 0))
            for o in _universe(4)]

    run = _run(monkeypatch, rows)

    assert "R01" not in run.verified
    assert run.result["gated"]["stale_signal"] == 1


def test_row_without_an_executable_contract_is_withheld(monkeypatch):
    run = _run(monkeypatch, _universe(5), no_contract=frozenset({"R00", "R03"}))

    assert run.verified == _r(0, 1, 2, 3, 4)  # all priced ...
    assert [t for _, t in run.cards] == _r(1, 2, 4)  # ... two had no contract
    assert run.result["gated"]["no_executable_contract"] == 2


def test_nothing_is_sent_when_every_row_is_withheld(monkeypatch):
    # Every row in the still-withheld 100x tier (CALLs are sent since 2026-11-04).
    rows = _universe(4, hundredx_ranks=frozenset(range(4)))

    run = _run(monkeypatch, rows)

    assert run.result["sent"] is False
    assert run.result["reason"] == "no_new_calls"
    assert run.verified == []
    assert run.events == []


# ── Re-send window ──────────────────────────────────────────────────────────

def test_contract_emailed_inside_the_window_is_not_resent(monkeypatch):
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    recent = [
        {"ticker": "R00", "direction": "PUT", "expiry": "2026-12-18", "strike": 98.0,
         "emailed_at": now - timedelta(hours=4)},
        # Outside the window: sent again.
        {"ticker": "R01", "direction": "PUT", "expiry": "2026-12-18", "strike": 98.0,
         "emailed_at": now - timedelta(hours=hx.RESEND_COOLDOWN_HOURS + 1)},
    ]

    run = _run(monkeypatch, _universe(3), recent=recent)

    assert [t for _, t in run.cards] == _r(1, 2)
    assert run.result["gated"]["already_emailed"] == 1


def test_nothing_new_sends_nothing(monkeypatch):
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    recent = [{"ticker": t, "direction": "PUT", "expiry": "2026-12-18", "strike": 98.0,
               "emailed_at": now - timedelta(hours=1)} for t in _r(0, 1)]

    run = _run(monkeypatch, _universe(2), recent=recent)

    assert run.result == {**run.result, "sent": False, "reason": "no_new_calls"}
    assert run.events == []


def test_direction_flip_is_noted_on_the_card(monkeypatch):
    from datetime import datetime, timedelta, timezone

    recent = [{"ticker": "R00", "direction": "CALL", "expiry": "2026-11-20", "strike": 110.0,
               "emailed_at": datetime.now(timezone.utc) - timedelta(days=3)}]

    run = _run(monkeypatch, _universe(1), recent=recent)

    assert "direction flipped: CALL emailed" in run.sent["plain"]


# ── Ledger before email ─────────────────────────────────────────────────────

def test_every_call_is_persisted_before_the_email_is_sent(monkeypatch):
    run = _run(monkeypatch, _universe(3))

    assert run.events == ["persist", "send"]
    assert [c["ticker"] for c in run.persisted] == _r(0, 1, 2)
    row = run.persisted[0]
    assert row["contract"]["ask"] == 2.20 and row["contract"]["strike"] == 98.0
    assert row["economics"]["breakeven"] == pytest.approx(98.0 - 2.20)
    assert row["provenance"]["gates"]["min_dte"] == hx.MIN_DTE_DAYS
    assert row["provenance"]["quote_at_kind"] == "local_retrieval_time"
    assert row["provenance"]["spot_at_kind"] == "local_retrieval_time"
    assert row["provenance"]["provider_bid_ask_timestamp"] is None


def test_unreadable_ledger_sends_nothing(monkeypatch):
    run = _run(monkeypatch, _universe(3), ledger_read_error=True)

    assert run.result["sent"] is False
    assert run.result["reason"] == "call_ledger_unavailable"
    assert run.events == []


def test_ledger_write_failure_sends_nothing(monkeypatch):
    run = _run(monkeypatch, _universe(3), ledger_write_error=True)

    assert run.result["sent"] is False
    assert run.result["reason"] == "call_ledger_write_failed"
    assert "send" not in run.events


# ── Final ordering: by score ────────────────────────────────────────────────

def test_cards_ship_in_score_order(monkeypatch):
    run = _run(monkeypatch, _universe(6))

    assert [t for _, t in run.cards] == _r(0, 1, 2, 3, 4, 5)
    assert [i for i, _ in run.cards] == list(range(1, 7))
    assert run.sent["subject"].startswith("GRID Options — 6 calls: R00 PUT, R01 PUT")


def test_html_body_carries_the_same_order(monkeypatch):
    run = _run(monkeypatch, _universe(4))

    titles = re.findall(r'<div class="card-title">#\d+ — (\S+) PUTS(?: \(UNPROVEN\))?</div>', run.sent["html"])
    assert titles == _r(0, 1, 2, 3)


def test_llm_fail_drops_the_row(monkeypatch):
    """A reachable LLM answers in verification (score) order and drops FAILs."""
    calls: list[str] = []
    verdicts = ('[{"index": 1, "verdict": "PASS", "reason": "ok"}, '
                '{"index": 2, "verdict": "FAIL", "reason": "bad"}, '
                '{"index": 3, "verdict": "SUSPECT", "reason": "check"}, '
                '{"index": 4, "verdict": "FAIL", "reason": "bad"}]')
    monkeypatch.setitem(sys.modules, "requests", _scripted_llm_requests(verdicts, calls))

    run = _run(monkeypatch, _universe(4))

    assert calls == ["GET /props", "POST /v1/chat/completions"]
    assert run.verified == _r(0, 1, 2, 3)
    assert [t for _, t in run.cards] == _r(0, 2)
    assert "SUSPECT: check" in run.sent["plain"]


# Summary-first presentation must describe the bounded selection funnel.
def test_digest_summary_reports_candidate_and_quote_check_scope(monkeypatch):
    run = _run(monkeypatch, _universe(40))
    plain = run.sent["plain"]
    assert "15 option ideas shown" in plain
    assert "40 scanner candidates returned" in plain
    assert "15 quote-checked" in plain
    assert "25 not quote-checked due to the 15-candidate cap" in plain
    assert "15 PUT / 0 CALL" in plain
    assert "UNPROVEN directions shown: PUT" in plain
    assert "not probabilities or expected returns" in plain
    assert plain.index("15 option ideas shown") < plain.index("Track Record")
    assert "font-size:32px" in run.sent["html"]


def test_digest_summary_preserves_withheld_and_missing_quote_counts(monkeypatch):
    run = _run(monkeypatch, _universe(5), no_contract=frozenset({"R00", "R03"}))
    assert "3 option ideas shown" in run.sent["plain"]
    assert "5 quote-checked" in run.sent["plain"]
    assert "Withheld 2" in run.sent["plain"]
    assert "3/3 shown ideas have a retrieved two-sided quote" in run.sent["plain"]



def test_forced_digest_keeps_scanner_failure_visible(monkeypatch):
    run = _run(monkeypatch, [], force=True, scanner_error=True)
    assert run.result["sent"] is True
    assert "Scanner failed: candidate coverage is unavailable" in run.sent["plain"]
    assert "0 scanner candidates returned" not in run.sent["plain"]


class TestPause:
    """While OPTIONS_DIGEST_ENABLED is false the digest scans, records and sends nothing."""

    def test_default_setting_is_paused(self):
        # Read the source: other tests in a run may have replaced the ``config`` module with a stub.
        from pathlib import Path

        source = (Path(__file__).resolve().parent.parent / "config.py").read_text()
        assert re.search(r"^\s+OPTIONS_DIGEST_ENABLED: bool = False$", source, re.MULTILINE)

    def test_paused_run_touches_nothing_even_when_forced(self, monkeypatch):
        touched: list[str] = []
        monkeypatch.setattr(hx, "_digest_paused", lambda: "paused: test")
        monkeypatch.setattr(hx, "_get_engine", lambda: touched.append("engine"))
        monkeypatch.setattr(hx, "_send_in_thread", lambda *a, **k: touched.append("send"))
        for force in (False, True):
            assert hx.run_100x_digest(force=force) == {"sent": False, "reason": "paused: test", "scanned": 0}
        assert touched == []

    def test_setting_controls_the_pause(self, monkeypatch):
        import config

        monkeypatch.setattr(config.settings, "OPTIONS_DIGEST_ENABLED", False, raising=False)
        assert "OPTIONS_DIGEST_ENABLED is false" in hx._digest_paused()
        monkeypatch.setattr(config.settings, "OPTIONS_DIGEST_ENABLED", True, raising=False)
        assert hx._digest_paused() is None
