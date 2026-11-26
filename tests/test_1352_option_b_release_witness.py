"""Release witness for #1352 Option B: no signed receipt, no ticket.

This is the RELEASE WITNESS that Codex asked for in lane entry
GRID-1352-OPTION-B-RELEASE-CRITERIA-20261125 (review at ``625b84cd``).
Before any Option B release proposal, Codex asked for the exact merge
composition that would ship to be pinned, with a focused offline
gateway-to-ticket witness kept on it. Its criteria:
(a) a strong matching vote without a true receipt gives no ticket, with an
observable no-receipt marker; (b) the exact expiry edge; (c) future and
(d) research refusals; (e) no fallback and no alternate direct generator
bypass.

It is NOT a re-enable. Production builds no ``SignedFamilyReceipt``
(``signal_provenance._family_receipts`` returns none: Anik, "Abstain now
(B)", 2026-11-24), so every live report abstains. Nothing here changes a
production path. Tests marked CONTRACT hand the live builder a
test-constructed receipt, to show where the floor would open once an
engine change exports receipts. No live path builds one.

Pinned composition: ``claude/fp-provenance-stack-20261122`` at merge
``8c9c4f97`` (tree ``4082adc0``), which is ``625b84cd`` plus origin/main
``b2629045`` (#1401). The commit that adds this file changes nothing
else, so the production code it witnesses is that merge's.

Criteria and tests:
  (a) ``TestAStrongMatchingVoteWithoutReceipt``
  (b) ``TestBExactExpiryEdge`` (CONTRACT)
  (c) ``TestCFutureRefusal``
  (d) ``TestDResearchRefusal``
  (e) ``TestENoFallbackOrBypass``

Real code: ``decision_gateway.should_i_trade``; ``oracle.engine.
EnsemblePredictor`` (its constructor, ``predict``, the real
``SignalAggregator`` and the vote, confidence and interval math);
``build_provenance_report`` with its clock, fences and verdict policy;
``run_stress_test``; the gateway ticket stage; ``generate_ticket``; the
conviction API routes; ``pair_conviction``. Stubbed, data reads only: the
oracle registry and ``signal_registry`` rows (``ModelFactory``); the heads'
hit-rate and bucket reads; the oracle's context reads (catalyst, liquidity
regime, regime router, FCI, crowdedness and its penalty); the ``oracle_models`` family
rows; the two scorecard tables; the eleven adjuster reads; the pattern
library; the red-team LLM; and the circuit breaker, a fresh one per run.
There is no DB, and a socket guard fails any network attempt.
"""
from __future__ import annotations

import ast
import socket
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.auth import require_auth
from api.dependencies import get_db_engine
from api.routers.conviction import router as conviction_router
from features.per_signal_brier import SignalScorecard
from intelligence import decision_gateway as dg
from intelligence import llm_red_team
from intelligence import pair_conviction as pc
from intelligence import signal_provenance as sp
from intelligence.signal_convergence_scanner import ConvergenceReport
from oracle.engine import EnsemblePrediction, EnsemblePredictor
from oracle.model_factory import ModelFactory, ModelSpec
from oracle.risk import CircuitBreaker
from trading import trade_ticket_generator as tg

_REPO = Path(__file__).resolve().parents[1]

# The fixture: three oracle heads, each on the one ``credit`` family, each
# reading one bullish credit input at confidence 0.92 (bearish for AMD, the
# pair short leg). Their vote is unanimous and the family's card is strong.
_FAMILY = "credit"
_HEADS: tuple[str, ...] = ("credit_a", "credit_b", "credit_c")
_HEAD_CONFIDENCE = 0.92
_HIT_RATE = 0.6
_CALLS: dict[str, str] = {"NVDA": "bullish", "AMD": "bearish"}
_STRONG_WEIGHT = 1.3
# What each unfenced adjuster read answers: not neutral, so a neutral
# number on the report shows the clock fence held.
_UNFENCED_LAYER_VALUE = 0.9
_ACCOUNT = 100_000.0
_PRICE = 120.0
_VOL = 0.35

_FLOOR = sp.NO_ELIGIBLE_DIRECTIONAL_EVIDENCE      # "no_eligible_directional_evidence"
_NO_RECEIPT = sp.NO_SIGNED_FAMILY_RECEIPT         # "no_signed_family_receipt"
_ONE_MICROSECOND = timedelta(microseconds=1)


# ── Offline guard ─────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _no_network(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Refuse and record every network attempt; the witness is offline."""
    attempts: list[str] = []

    def _refuse(*args: Any, **_kwargs: Any) -> Any:
        attempts.append(repr(args[:2]))
        raise OSError("release witness is offline: network attempt refused")

    monkeypatch.setattr(socket.socket, "connect", _refuse)
    monkeypatch.setattr(socket.socket, "connect_ex", _refuse)
    monkeypatch.setattr(socket, "create_connection", _refuse)
    monkeypatch.setattr(socket, "getaddrinfo", _refuse)
    yield
    assert attempts == [], f"network attempted: {attempts}"


# ── The offline composition ───────────────────────────────────────────────


@dataclass
class _Clock:
    """``signal_provenance._utc_now`` during a gateway run, relative to the
    decision instant that ``predict()`` stamps from its own wall clock.

    The registry stub records that instant when ``predict()`` reads signals.
    The provenance builder then sees ``decision + build``. The pattern
    library stage runs after provenance and before stress and the ticket
    stage, so its stub moves the clock to ``decision + issue``, as a slow
    stage would delay issuance.
    """

    build: timedelta = timedelta(0)
    issue: timedelta = timedelta(0)
    decision: datetime | None = None
    offset: timedelta = timedelta(0)

    def __call__(self) -> datetime:
        assert self.decision is not None, "provenance read the clock before predict()"
        return self.decision + self.offset


def _signal_rows(ticker: str, as_of: datetime) -> list[dict[str, Any]]:
    """One ``signal_registry`` row for the family, valid a day before ``as_of``."""
    return [
        {
            "id": f"{ticker}-{_FAMILY}-1",
            "source_module": _FAMILY,
            "signal_type": "spread_z",
            "ticker": ticker,
            "direction": _CALLS[ticker],
            "value": 0.8,
            "strength": 0.8,
            "z_score": 2.0,
            "confidence": _HEAD_CONFIDENCE,
            "valid_from": as_of - timedelta(days=1),
            "valid_until": None,
            "metadata": {},
        }
    ]


def _oracle_models_engine() -> MagicMock:
    """An engine whose ``oracle_models`` read maps every head to the family."""
    engine = MagicMock(name="witness-engine")
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchall.return_value = [
        (head, [_FAMILY], None) for head in _HEADS
    ]
    return engine


def _patch_oracle_reads(stack: ExitStack, clock: _Clock | None) -> None:
    """The oracle's reads: the registry, signal rows, hit rates and context."""
    specs = [ModelSpec(name=head, signal_sources=[_FAMILY], min_signals=1) for head in _HEADS]

    def _init(self: ModelFactory, engine: Any) -> None:
        self.engine = engine

    def _signals(self: ModelFactory, name: str, as_of: datetime, ticker: str | None = None) -> Any:
        if clock is not None:
            clock.decision, clock.offset = as_of, clock.build
        return _signal_rows(str(ticker), as_of)

    stack.enter_context(patch.object(ModelFactory, "__init__", _init))
    stack.enter_context(patch.object(ModelFactory, "list_active_models", lambda self: list(specs)))
    stack.enter_context(patch.object(ModelFactory, "get_signals_for_model", _signals))
    stack.enter_context(
        patch.object(
            ModelFactory, "active_model_names", staticmethod(lambda conn, names, **kw: set(names))
        )
    )
    stack.enter_context(
        patch.object(EnsemblePredictor, "_get_hit_rate", lambda self, name, *, horizon=None: _HIT_RATE)
    )
    stack.enter_context(
        patch.object(EnsemblePredictor, "_get_bucket_weight", lambda self, name, *, horizon=None: 1.0)
    )
    for target, value in (
        ("intelligence.catalyst_aggregator.proximity_score", {"score": 0.0, "catalyst_type": None}),
        ("intelligence.liquidity_regime.classify_current_regime", None),
        ("oracle.regime_router.RegimeRouter.model_regime_weight", 1.0),
        ("intelligence.financial_conditions_index.compute_fci", SimpleNamespace(score=0.0, regime="NEUTRAL")),
        (
            "intelligence.consensus_crowdedness.compute_crowdedness",
            SimpleNamespace(score=0.0, crowd_direction=None),
        ),
        ("intelligence.consensus_crowdedness.compute_penalty", SimpleNamespace(multiplier=1.0, aligned=False)),
    ):
        stack.enter_context(patch(target, return_value=value))


def _strong_card(stamp: datetime) -> SignalScorecard:
    return SignalScorecard(
        signal_source=_FAMILY,
        horizon_days=7,
        scored_count=50,
        running_brier=0.08,
        running_ece=0.06,
        hit_rate=0.65,
        last_updated=stamp,
        is_calibrated=True,
        conviction_weight=_STRONG_WEIGHT,
    )


def _patch_provenance_reads(stack: ExitStack) -> None:
    """The scorecard tables and the eleven adjuster reads behind provenance."""
    # Scored a day before any run's decision instant, so the card is fenced.
    card = _strong_card(datetime.now(timezone.utc) - timedelta(days=1))
    stack.enter_context(
        patch(
            "features.regime_conditional_brier.get_regime_conditional_scorecard",
            lambda _e, src, _h, _r: card if src == _FAMILY else None,
        )
    )
    stack.enter_context(
        patch(
            "features.regime_conditional_brier.get_signal_scorecard",
            lambda _e, src, _h: card if src == _FAMILY else None,
        )
    )

    def fn(*_a: Any, **_k: Any) -> float:
        return _UNFENCED_LAYER_VALUE

    for name in (
        "get_lift_multiplier",
        "conviction_multiplier_for_bucket",
        "scenario_conviction_multiplier",
        "null_hypothesis_penalty",
        "get_aggregate_weight_multiplier",
        "contra_conviction_multiplier",
        "squeeze_conviction_multiplier",
        "arbitrage_conviction_multiplier",
        "money_flow_conviction_multiplier",
        "memory_lesson_conviction_multiplier",
    ):
        stack.enter_context(patch.object(sp, name, fn))
    stack.enter_context(
        patch(
            "intelligence.signal_convergence_scanner.scan_convergence",
            return_value=ConvergenceReport(
                ticker="NVDA",
                as_of="2026-11-24",
                window_days=7,
                target_direction="bullish",
                stream_signals=(),
                n_active_streams=2,
                n_aligned=2,
                n_opposed=0,
                convergence_score=0.55,
                conviction_multiplier=_UNFENCED_LAYER_VALUE,
                evidence_chain=(),
                advisory="Active convergence",
                missing_stream_count=6,
            ),
        )
    )


def _contract_receipts(
    prediction: Any, *, ticker: str, clock: datetime
) -> dict[str, sp.SignedFamilyReceipt]:
    """CONTRACT FIXTURE, never live: a complete receipt for the family, signed
    the call's way by every contributing head, at this ticker and clock."""
    call = str(getattr(prediction, "direction", ""))
    return {
        _FAMILY: sp.SignedFamilyReceipt(
            signal_source=_FAMILY,
            direction=call,
            ticker=ticker,
            decision_clock=clock,
            consumed_signals=(f"{ticker}-{_FAMILY}-1",),
            head_directions=tuple((head, call) for head in _HEADS),
            complete_contributing_set=True,
        )
    }


@contextmanager
def _offline(*, receipts: bool = False, clock: _Clock | None = None) -> Iterator[MagicMock]:
    """Install the offline composition; yields the engine to pass on.

    ``receipts=True`` makes the run a CONTRACT test (``_contract_receipts``).
    ``clock`` replaces the provenance wall clock (``_Clock``); without it
    every clock is the real wall clock, as in production.
    """
    engine = _oracle_models_engine()

    def _red_team(**kw: Any) -> llm_red_team.RedTeamReport:
        return llm_red_team.RedTeamReport(
            ticker=kw.get("ticker", ""),
            prediction_score=float(kw.get("score", 0.0)),
            counters=(llm_red_team.CounterArgument("Valuation risk", 0.2, 0.2, 0.2),),
            epistemic_risk_score=0.2,
        )

    def _pattern(_engine: Any, *, ticker: str) -> Any:
        if clock is not None:
            clock.offset = clock.issue
        return SimpleNamespace(confidence_signal=0.0)

    with ExitStack() as stack:
        _patch_oracle_reads(stack, clock)
        _patch_provenance_reads(stack)
        stack.enter_context(patch("intelligence.llm_red_team.red_team_prediction", _red_team))
        stack.enter_context(patch("intelligence.pattern_library.build_pattern_match_report", _pattern))
        stack.enter_context(patch("oracle.risk.get_global_circuit_breaker", lambda: CircuitBreaker()))
        if clock is not None:
            stack.enter_context(patch.object(sp, "_utc_now", clock))
        if receipts:
            stack.enter_context(patch.object(sp, "_family_receipts", _contract_receipts))
        yield engine


def _gateway(
    ticker: str = "NVDA", *, receipts: bool = False, clock: _Clock | None = None
) -> dg.DecisionResponse:
    """``should_i_trade`` as the ``/conviction/ticker`` route calls it."""
    with _offline(receipts=receipts, clock=clock) as engine:
        return dg.should_i_trade(
            engine, ticker, account_size_usd=_ACCOUNT, current_price=_PRICE, vol_30d=_VOL
        )


def _client(engine: Any) -> TestClient:
    """The real conviction router, with the witness engine and a test token."""
    app = FastAPI()
    app.include_router(conviction_router)
    app.dependency_overrides[get_db_engine] = lambda: engine
    app.dependency_overrides[require_auth] = lambda: "witness-token"
    return TestClient(app)


def _decision(report: Any) -> datetime:
    clock, refused = sp.parse_decision_instant(report.decision_clock)
    assert refused is None, refused
    return clock


def _stage(report: Any, now: datetime | None = None) -> tuple[Any, str | None]:
    """The gateway's ticket stage on ``report``, at ``now`` (wall clock if None)."""
    with ExitStack() as stack:
        stack.enter_context(patch("oracle.risk.get_global_circuit_breaker", lambda: CircuitBreaker()))
        if now is not None:
            stack.enter_context(patch.object(sp, "_utc_now", lambda: now))
        return dg._run_trade_ticket(
            report,
            account_size_usd=_ACCOUNT,
            current_price=_PRICE,
            vol_30d=_VOL,
            instrument="equity",
            unified_verdict=report.verdict,
        )


def _direct(report: Any, now: datetime | None = None) -> Any:
    """``generate_ticket`` called directly on ``report``, at ``now``."""
    with ExitStack() as stack:
        stack.enter_context(patch("oracle.risk.get_global_circuit_breaker", lambda: CircuitBreaker()))
        if now is not None:
            stack.enter_context(patch.object(sp, "_utc_now", lambda: now))
        return tg.generate_ticket(
            report, account_size_usd=_ACCOUNT, current_price=_PRICE, vol_30d=_VOL
        )


def _outcome(resp: dg.DecisionResponse) -> tuple[str, str, bool]:
    return (resp.provenance_report.verdict, resp.unified_verdict, resp.trade_ticket is not None)


def _contract_report() -> sp.TradeProvenanceReport:
    """CONTRACT: the gateway's live report with a test-constructed receipt."""
    resp = _gateway(receipts=True)
    assert _outcome(resp) == ("medium", "medium", True), resp.stage_errors
    return resp.provenance_report


# ── (a) A strong matching vote without a true receipt gives no ticket ──────


class TestAStrongMatchingVoteWithoutReceipt:
    """(a) A fenced strong card and a unanimous agreeing head vote at 0.92,
    through the real ensemble, give no ticket. The no-receipt marker is in
    the output an operator reads."""

    def test_a_real_ensemble_strong_card_unanimous_vote_gives_no_ticket(self) -> None:
        resp = _gateway()
        pred = resp.prediction
        prov = resp.provenance_report
        assert prov is not None, resp.stage_errors

        # The real predict(): three heads, unanimous, at confidence 0.92.
        assert isinstance(pred, EnsemblePrediction)
        assert pred.direction == "bullish"
        assert pred.confidence == pytest.approx(_HEAD_CONFIDENCE)
        assert pred.model_count == len(_HEADS) == 3
        assert {v["direction"] for v in pred.model_votes} == {"bullish"}
        assert pred.coherence == pytest.approx(1.0)

        # A live report on predict()'s own instant, with a fenced strong card
        # and the unanimous head vote on the row, but no receipt.
        assert (prov.actionable, prov.basis) == (True, sp.REPORT_BASIS_LIVE)
        assert _decision(prov) == pred.as_of
        (row,) = prov.signal_evidence
        assert (row.signal_source, row.classification) == (_FAMILY, "strong")
        assert row.scorecard.last_updated <= pred.as_of
        assert row.signed_input.direction == "bullish"
        assert set(row.signed_input.models) == set(_HEADS)
        assert row.receipt is None
        assert sp.count_eligible_directional_evidence(
            prov.signal_evidence,
            direction=prov.direction,
            decision_clock=prov.decision_clock,
            ticker=prov.ticker,
        ) == 0

        # No ticket: LOW in the 0.92 band, stress robust, the floor named.
        assert _outcome(resp) == ("low", "low", False)
        assert prov.verdict_reason.startswith(f"{_FLOOR}: 0 eligible rows")
        assert f"{_NO_RECEIPT}: 1 of 1 rows" in prov.verdict_reason
        assert resp.stress_report.robustness_label == "robust"
        assert resp.stress_report.live_parity is True
        assert resp.stage_errors == {"trade_ticket": "verdict=low below ticket threshold"}

    def test_a_the_no_receipt_marker_is_in_the_serialized_decision(self) -> None:
        out = _gateway().to_dict()
        prov = out["provenance_report"]
        assert out["trade_ticket"] is None
        assert out["unified_verdict"] == prov["verdict"] == "low"
        assert prov["verdict_reason"].startswith(_FLOOR)
        assert f"{_NO_RECEIPT}: 1 of 1 rows" in prov["verdict_reason"]
        (row,) = prov["signal_evidence"]
        assert row["receipt"] is None
        assert row["signed_input"]["basis"] == sp.AGGREGATE_HEAD_VOTE_BASIS
        assert row["signed_input"]["direction"] == "bullish"
        assert out["stage_errors"]["trade_ticket"] == "verdict=low below ticket threshold"

    def test_a_the_marker_is_in_the_conviction_route_payload(self) -> None:
        with _offline() as engine:
            res = _client(engine).get(
                "/api/v1/conviction/ticker/NVDA",
                params={"current_price": _PRICE, "vol_30d": _VOL},
            )
        assert res.status_code == 200, res.text
        body = res.json()
        assert body["trade_ticket"] is None
        assert body["unified_verdict"] == "low"
        assert body["provenance_report"]["verdict_reason"].startswith(_FLOOR)
        assert _NO_RECEIPT in body["provenance_report"]["verdict_reason"]
        assert [r["receipt"] for r in body["provenance_report"]["signal_evidence"]] == [None]
        assert body["stage_errors"] == {"trade_ticket": "verdict=low below ticket threshold"}

    def test_a_forced_medium_the_ticket_stage_and_generator_refuse_and_say_why(self) -> None:
        forced = replace(_gateway().provenance_report, verdict="medium")
        ticket, err = _stage(forced)
        assert ticket is None
        assert err.startswith(f"{_FLOOR}: 0 eligible rows (floor 1)")
        assert f"{_NO_RECEIPT}: 1 of 1 rows" in err and err.endswith("; no ticket")
        assert _direct(forced) is None

    def test_a_contract_the_same_run_with_a_receipt_tickets(self) -> None:
        # CONTRACT: the one difference from the live run is the receipt.
        resp = _gateway(receipts=True)
        assert _outcome(resp) == ("medium", "medium", True)
        assert resp.stage_errors == {}
        assert resp.trade_ticket.direction == "bullish"
        prov = resp.provenance_report
        assert prov.signal_evidence[0].receipt is not None
        assert prov.verdict_reason == "confidence 0.92 in the saturated > 0.85 zone"
        assert sp.actionable_refusal_reason(prov, now=_decision(prov)) is None


# ── (b) The exact expiry edge (CONTRACT) ──────────────────────────────────


class TestBExactExpiryEdge:
    """(b) CONTRACT: issuance exactly ``ISSUANCE_MAX_AGE`` after the decision
    instant is allowed; one microsecond later is ``decision_expired``. The
    live report, with no receipt, is refused on both sides of the edge."""

    def test_b_the_bound_is_one_hour(self) -> None:
        assert sp.ISSUANCE_MAX_AGE == timedelta(hours=1)
        assert sp.DECISION_EXPIRED == "decision_expired"

    def test_b_contract_ticket_stage_and_generator_at_the_exact_edge(self) -> None:
        report = _contract_report()
        edge = _decision(report) + sp.ISSUANCE_MAX_AGE

        ticket, err = _stage(report, edge)
        assert ticket is not None and err is None
        assert _direct(report, edge) is not None
        assert sp.actionable_refusal_reason(report, now=edge) is None

        late = edge + _ONE_MICROSECOND
        ticket, err = _stage(report, late)
        assert ticket is None
        assert err.startswith(sp.DECISION_EXPIRED) and err.endswith("; no ticket")
        assert _direct(report, late) is None
        assert sp.actionable_refusal_reason(report, now=late).startswith(sp.DECISION_EXPIRED)

    @pytest.mark.parametrize(
        ("issue", "issued"),
        [
            pytest.param(sp.ISSUANCE_MAX_AGE, True, id="exactly-max-age"),
            pytest.param(sp.ISSUANCE_MAX_AGE + _ONE_MICROSECOND, False, id="max-age-plus-1us"),
        ],
    )
    def test_b_contract_full_gateway_delayed_issuance(
        self, issue: timedelta, issued: bool
    ) -> None:
        # Built on its decision instant, issued ``issue`` later.
        clock = _Clock(build=timedelta(0), issue=issue)
        resp = _gateway(receipts=True, clock=clock)
        assert resp.provenance_report.generated_at == clock.decision.isoformat()
        assert resp.unified_verdict == "medium"
        assert (resp.trade_ticket is not None) is issued
        if issued:
            assert resp.stage_errors == {}
        else:
            assert resp.stage_errors["trade_ticket"].startswith(sp.DECISION_EXPIRED)

    def test_b_live_without_a_receipt_no_ticket_on_either_side(self) -> None:
        report = _gateway().provenance_report
        edge = _decision(report) + sp.ISSUANCE_MAX_AGE
        assert sp.actionable_refusal_reason(report, now=edge).startswith(_FLOOR)
        assert sp.actionable_refusal_reason(
            report, now=edge + _ONE_MICROSECOND
        ).startswith(sp.DECISION_EXPIRED)


# ── (c) Future refusal ────────────────────────────────────────────────────


class TestCFutureRefusal:
    """(c) A decision instant beyond ``LIVE_CLOCK_MAX_FUTURE_SKEW`` ahead of
    the reader's clock is refused: by the builder, which no ticket can
    follow, and by the ticket stage and the generator."""

    def test_c_the_skew_is_sixty_seconds(self) -> None:
        assert sp.LIVE_CLOCK_MAX_FUTURE_SKEW == timedelta(seconds=60)
        assert sp.DECISION_CLOCK_IN_FUTURE == "decision_clock_in_future"

    def test_c_full_gateway_builder_refuses_a_future_decision_instant(self) -> None:
        # Receipts are on (CONTRACT), so only the clock can refuse.
        skew = sp.LIVE_CLOCK_MAX_FUTURE_SKEW
        ahead = _Clock(build=-(skew + _ONE_MICROSECOND), issue=-(skew + _ONE_MICROSECOND))
        resp = _gateway(receipts=True, clock=ahead)
        assert resp.provenance_report is None
        assert "live-only" in resp.stage_errors["provenance"]
        assert "in the future" in resp.stage_errors["provenance"]
        assert resp.unified_verdict == "no_trade"
        assert resp.trade_ticket is None
        assert resp.stage_errors["trade_ticket"] == "skipped — no provenance report"

        # CONTRACT control: exactly the skew ahead is allowed and tickets.
        at_skew = _gateway(receipts=True, clock=_Clock(build=-skew, issue=-skew))
        assert _outcome(at_skew) == ("medium", "medium", True)

    def test_c_contract_ticket_stage_and_generator_refuse_a_future_instant(self) -> None:
        report = _contract_report()
        decision = _decision(report)
        skew = sp.LIVE_CLOCK_MAX_FUTURE_SKEW

        at_skew = decision - skew
        ticket, err = _stage(report, at_skew)
        assert ticket is not None and err is None
        assert _direct(report, at_skew) is not None

        early = at_skew - _ONE_MICROSECOND
        ticket, err = _stage(report, early)
        assert ticket is None and err.startswith(sp.DECISION_CLOCK_IN_FUTURE)
        assert _direct(report, early) is None

        # A report relabelled a day ahead is refused at the wall clock.
        forged = replace(report, decision_clock=(decision + timedelta(days=1)).isoformat())
        ticket, err = _stage(forged)
        assert ticket is None and err.startswith(sp.DECISION_CLOCK_IN_FUTURE)
        assert _direct(forged) is None


# ── (d) Research refusal ──────────────────────────────────────────────────


def _walk_forward_report(created: datetime) -> sp.TradeProvenanceReport:
    """``walk_forward_validate``'s time-frozen research report on the family."""
    from scripts import walk_forward_validate as wfv

    row = {
        "id": "witness-1",
        "ticker": "NVDA",
        "created_at": created,
        "expiry": created + timedelta(days=7),
        "confidence": _HEAD_CONFIDENCE,
        "verdict": "hit",
        "model_name": None,
        "signals": {"direction": "bullish", "regime": "EXPANSION", "fci_regime": "EASY"},
        "signal_contributions": {_FAMILY: 1.0},
        "model_weights": None,
    }
    return wfv.build_time_frozen_provenance(
        row, {_FAMILY: _strong_card(created - timedelta(days=1))}
    )


class TestDResearchRefusal:
    """(d) A clockless or research report (``actionable=False``) never
    tickets, through the ticket stage or ``generate_ticket`` directly."""

    def test_d_walk_forward_research_report_never_tickets(self) -> None:
        contract = _contract_report()
        decision = _decision(contract)
        report = _walk_forward_report(decision - timedelta(days=30))
        assert (report.actionable, report.basis, report.decision_clock) == (
            False,
            sp.REPORT_BASIS_RESEARCH,
            "",
        )
        assert _direct(report) is None
        # Forced to a strong MEDIUM, given a valid live clock and the
        # contract's complete, agreeing receipt: still research, no ticket.
        (row,) = report.signal_evidence
        forced = replace(
            report,
            verdict="medium",
            aggregate_conviction=_STRONG_WEIGHT,
            decision_clock=contract.decision_clock,
            signal_evidence=[replace(row, receipt=contract.signal_evidence[0].receipt)],
        )
        for now in (None, decision):
            ticket, err = _stage(forced, now)
            assert ticket is None and err.startswith(sp.NOT_ACTIONABLE)
            assert _direct(forced, now) is None
            assert sp.actionable_refusal_reason(forced, now=now).startswith(sp.NOT_ACTIONABLE)

    @pytest.mark.parametrize(
        ("change", "code"),
        [
            pytest.param({"actionable": False}, sp.NOT_ACTIONABLE, id="actionable-false"),
            pytest.param(
                {"actionable": False, "basis": sp.REPORT_BASIS_RESEARCH},
                sp.NOT_ACTIONABLE,
                id="research-basis",
            ),
            pytest.param({"decision_clock": ""}, sp.DECISION_CLOCK_BLANK, id="clockless-blank"),
            pytest.param({"decision_clock": None}, sp.DECISION_CLOCK_MISSING, id="clockless-none"),
        ],
    )
    def test_d_contract_report_made_research_or_clockless_never_tickets(
        self, change: dict[str, Any], code: str
    ) -> None:
        report = _contract_report()
        decision = _decision(report)
        assert _stage(report, decision)[0] is not None  # the control tickets
        forged = replace(report, **change)
        ticket, err = _stage(forged, decision)
        assert ticket is None and err.startswith(code)
        assert _direct(forged, decision) is None

    def test_d_a_hand_built_report_defaults_to_research(self) -> None:
        live = _contract_report()
        kept = {
            name: getattr(live, name)
            for name in sp.TradeProvenanceReport.__dataclass_fields__
            if name not in ("actionable", "basis")
        }
        bare = sp.TradeProvenanceReport(**kept)
        assert (bare.actionable, bare.basis) == (False, sp.REPORT_BASIS_RESEARCH)
        assert _direct(bare, _decision(live)) is None

    def test_d_call_a_trade_is_research_and_prints_no_ticket(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        from scripts import call_a_trade

        assert call_a_trade.main() == 1
        out = capsys.readouterr().out
        assert sp.REPORT_BASIS_RESEARCH in out and "not actionable" in out
        assert "TRADE TICKET" not in out


# ── (e) No fallback and no alternate generator ────────────────────────────

# Every production call of a ticket generator or of the gateway that feeds
# one, found by an AST scan of every non-test ``.py`` file:
# (file, enclosing function, callee). ``generate_ticket``, ``TradeTicket``
# and ``_run_trade_ticket`` are the gated provenance ticket path;
# ``should_i_trade`` callers receive the gateway's gated ticket;
# ``generate_pair_ticket``, ``PairTradeTicket`` and ``scan_candidate_pairs``
# are the pair path, which builds its own ticket from two gateway responses.
_TICKET_CALLEES: frozenset[str] = frozenset(
    {
        "generate_ticket",
        "TradeTicket",
        "_run_trade_ticket",
        "should_i_trade",
        "generate_pair_ticket",
        "PairTradeTicket",
        "scan_candidate_pairs",
    }
)
_PINNED_CALL_SITES: frozenset[tuple[str, str, str]] = frozenset(
    {
        ("api/routers/conviction.py", "get_pair_candidates", "scan_candidate_pairs"),
        ("api/routers/conviction.py", "get_pair_ticket", "generate_pair_ticket"),
        ("api/routers/conviction.py", "get_ticker_conviction", "should_i_trade"),
        ("api/routers/conviction.py", "get_ticker_narrative", "should_i_trade"),
        ("intelligence/decision_gateway.py", "_run_trade_ticket", "generate_ticket"),
        ("intelligence/decision_gateway.py", "should_i_trade", "_run_trade_ticket"),
        ("intelligence/pair_conviction.py", "_run_leg", "should_i_trade"),
        ("intelligence/pair_conviction.py", "generate_pair_ticket", "PairTradeTicket"),
        ("intelligence/pair_conviction.py", "scan_candidate_pairs", "generate_pair_ticket"),
        ("intelligence/universe_ranker.py", "_run_one_ticker", "should_i_trade"),
        ("scripts/call_a_trade.py", "main", "generate_ticket"),
        ("scripts/walk_forward_validate.py", "walk_forward", "generate_ticket"),
        ("trading/trade_ticket_generator.py", "generate_ticket", "TradeTicket"),
    }
)
# Every production import from the ticket modules: (file, module, name).
_TICKET_MODULES: frozenset[str] = frozenset(
    {"trading.trade_ticket_generator", "intelligence.pair_conviction", "intelligence.decision_gateway"}
)
_PINNED_IMPORTS: frozenset[tuple[str, str, str]] = frozenset(
    {
        ("api/routers/conviction.py", "intelligence.decision_gateway", "should_i_trade"),
        ("api/routers/conviction.py", "intelligence.pair_conviction", "DEFAULT_PAIR_CANDIDATES"),
        ("api/routers/conviction.py", "intelligence.pair_conviction", "generate_pair_ticket"),
        ("api/routers/conviction.py", "intelligence.pair_conviction", "scan_candidate_pairs"),
        ("intelligence/decision_gateway.py", "trading.trade_ticket_generator", "generate_ticket"),
        ("intelligence/pair_conviction.py", "intelligence.decision_gateway", "should_i_trade"),
        ("intelligence/universe_ranker.py", "intelligence.decision_gateway", "should_i_trade"),
        ("scripts/call_a_trade.py", "trading.trade_ticket_generator", "generate_ticket"),
        ("scripts/walk_forward_validate.py", "trading.trade_ticket_generator", "generate_ticket"),
        ("trading/options_recommender.py", "trading.trade_ticket_generator", "MAX_KELLY_PER_TICKET"),
    }
)
# Ticket products outside the provenance stack: they read contagion and
# options data, never a provenance report or a gateway decision, so they
# cannot stand in for a refused gateway ticket, and #1352 does not change
# them. Pinned so a release kit names them as out of scope.
_SEPARATE_TICKET_MODULES: tuple[str, ...] = (
    "trading/contagion_to_ticket.py",
    "trading/options_recommender.py",
)
_GATEWAY_NAMES: frozenset[str] = frozenset(
    {
        "should_i_trade",
        "build_provenance_report",
        "TradeProvenanceReport",
        "generate_ticket",
        "TradeTicket",
        "_run_trade_ticket",
        "actionable_refusal_reason",
    }
)


def _production_files() -> Iterator[tuple[str, str]]:
    """``(repo-relative path, source)`` for every non-test ``.py`` file."""
    skip = {".git", "node_modules", "__pycache__", "tests"}
    for path in sorted(_REPO.rglob("*.py")):
        rel = path.relative_to(_REPO)
        if any(p in skip or p.startswith(".") for p in rel.parts[:-1]):
            continue
        if rel.name.startswith("test_") or rel.name == "conftest.py":
            continue
        yield rel.as_posix(), path.read_text(encoding="utf-8", errors="replace")


def _call_sites(tree: ast.AST, rel: str, callees: frozenset[str]) -> set[tuple[str, str, str]]:
    """``(rel, enclosing scope, callee)`` for each call of a name in ``callees``."""
    found: set[tuple[str, str, str]] = set()

    def visit(node: ast.AST, scope: tuple[str, ...]) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                visit(child, scope + (child.name,))
                continue
            if isinstance(child, ast.Call):
                func = child.func
                name = (
                    func.id if isinstance(func, ast.Name)
                    else func.attr if isinstance(func, ast.Attribute)
                    else None
                )
                if name in callees:
                    found.add((rel, ".".join(scope) or "<module>", name))
            visit(child, scope)

    visit(tree, ())
    return found


def _scan() -> tuple[set[tuple[str, str, str]], set[tuple[str, str, str]]]:
    """(call sites, imports) of the ticket names across production code."""
    calls: set[tuple[str, str, str]] = set()
    imports: set[tuple[str, str, str]] = set()
    module_tails = {m.rsplit(".", 1)[1] for m in _TICKET_MODULES}
    for rel, src in _production_files():
        hit_call = any(name in src for name in _TICKET_CALLEES)
        hit_import = any(tail in src for tail in module_tails)
        if not (hit_call or hit_import):
            continue
        tree = ast.parse(src)
        calls |= _call_sites(tree, rel, _TICKET_CALLEES)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module in _TICKET_MODULES:
                imports |= {(rel, node.module, a.name) for a in node.names}
            elif isinstance(node, ast.Import):
                imports |= {(rel, a.name, "*") for a in node.names if a.name in _TICKET_MODULES}
            elif isinstance(node, ast.ImportFrom) and node.module in ("trading", "intelligence"):
                imports |= {
                    (rel, f"{node.module}.{a.name}", "*")
                    for a in node.names
                    if f"{node.module}.{a.name}" in _TICKET_MODULES
                }
    return calls, imports


def _function_call_lines(rel: str, function: str) -> dict[str, list[int]]:
    """``{callee: [line, ...]}`` of the calls inside one top-level function."""
    tree = ast.parse((_REPO / rel).read_text(encoding="utf-8"))
    (fn,) = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == function]
    lines: dict[str, list[int]] = {}
    for node in ast.walk(fn):
        if isinstance(node, ast.Call):
            func = node.func
            name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
            if name:
                lines.setdefault(name, []).append(node.lineno)
    return lines


_REAL_SHOULD_I_TRADE = dg.should_i_trade


class TestENoFallbackOrBypass:
    """(e) Every production path to a ticket goes through
    ``actionable_refusal_reason``; there is no fallback and no alternate
    generator.

    Call sites (``_PINNED_CALL_SITES``):
      - ``trading/trade_ticket_generator.py::generate_ticket`` builds the only
        ``TradeTicket``, after ``actionable_refusal_reason``.
      - ``intelligence/decision_gateway.py::_run_trade_ticket`` (the
        gateway's ticket stage, called only by ``should_i_trade``) refuses on
        ``actionable_refusal_reason`` before it calls ``generate_ticket``.
      - ``scripts/call_a_trade.py::main`` and
        ``scripts/walk_forward_validate.py::walk_forward`` call
        ``generate_ticket`` on research reports (refused, criterion (d)).
      - ``api/routers/conviction.py`` (``/ticker``, ``/narrative``) and
        ``intelligence/universe_ranker.py::_run_one_ticker`` call
        ``should_i_trade`` and only read its gated ticket.
      - ``intelligence/pair_conviction.py::generate_pair_ticket`` builds a
        ``PairTradeTicket`` from two ``should_i_trade`` responses, served by
        ``/conviction/pair/{long}/{short}`` and ``/conviction/pair/candidates``
        (``scan_candidate_pairs``). It must abstain when its legs abstain:
        ``_pair_leg_refusal`` applies ``actionable_refusal_reason`` to each
        leg's report before any ``PairTradeTicket`` is built (owner-approved
        pair gate, 2026-11-25).
    """

    def test_e_the_pair_path_checks_the_gate_before_it_builds_a_pair_ticket(self) -> None:
        lines = _function_call_lines("intelligence/pair_conviction.py", "generate_pair_ticket")
        (gate,) = lines["_pair_leg_refusal"]
        (build,) = lines["PairTradeTicket"]
        assert gate < build
        refusal_lines = _function_call_lines("intelligence/pair_conviction.py", "_pair_leg_refusal")
        assert refusal_lines.get("actionable_refusal_reason"), "pair gate must call actionable_refusal_reason"

    def test_e_production_ticket_call_sites_and_imports_are_pinned(self) -> None:
        calls, imports = _scan()
        assert calls == set(_PINNED_CALL_SITES)
        assert imports == set(_PINNED_IMPORTS)

    def test_e_generate_ticket_checks_the_gate_before_it_builds_a_ticket(self) -> None:
        lines = _function_call_lines("trading/trade_ticket_generator.py", "generate_ticket")
        (gate,) = lines["actionable_refusal_reason"]
        (build,) = lines["TradeTicket"]
        assert gate < build

        report = _contract_report()
        decision = _decision(report)
        refusal = "witness: forced refusal"
        spy = MagicMock(return_value=refusal)
        builder = MagicMock(wraps=tg.TradeTicket)
        with patch.object(tg, "actionable_refusal_reason", spy), patch.object(tg, "TradeTicket", builder):
            assert _direct(report, decision) is None
        spy.assert_called_once_with(report)
        builder.assert_not_called()

    def test_e_the_gateway_stage_checks_the_gate_before_the_generator(self) -> None:
        lines = _function_call_lines("intelligence/decision_gateway.py", "_run_trade_ticket")
        (gate,) = lines["actionable_refusal_reason"]
        (generate,) = lines["generate_ticket"]
        assert gate < generate
        assert "generate_ticket" not in _function_call_lines(
            "intelligence/decision_gateway.py", "should_i_trade"
        )

        report = _contract_report()
        refusal = "witness: forced refusal"
        generator = MagicMock(side_effect=AssertionError("generator reached"))
        with patch.object(sp, "actionable_refusal_reason", MagicMock(return_value=refusal)), patch.object(
            tg, "generate_ticket", generator
        ):
            ticket, err = _stage(report, _decision(report))
        assert (ticket, err) == (None, f"{refusal}; no ticket")
        generator.assert_not_called()

    def test_e_direct_generate_ticket_with_a_medium_report_and_no_receipt_returns_none(
        self,
    ) -> None:
        live = _gateway().provenance_report
        medium = replace(live, verdict="medium")
        # Every other precondition holds: conviction, direction, price, clock.
        assert medium.aggregate_conviction >= tg.MIN_CONVICTION_FOR_TICKET
        assert medium.direction == "bullish"
        assert all(ev.receipt is None for ev in medium.signal_evidence)
        decision = _decision(medium)
        assert _direct(medium, decision) is None
        reason = sp.actionable_refusal_reason(medium, now=decision)
        assert reason.startswith(_FLOOR) and _NO_RECEIPT in reason

    def test_e_no_fallback_when_the_gate_the_generator_or_provenance_fails(self) -> None:
        report = _contract_report()
        decision = _decision(report)
        boom = RuntimeError("witness: component failure")

        # The generator fails inside the stage: no ticket, no other generator.
        with patch.object(tg, "generate_ticket", MagicMock(side_effect=boom)):
            ticket, err = _stage(report, decision)
        assert ticket is None and err.startswith("trade_ticket_generator failed")

        # The gate fails: both entry points fail closed.
        with patch.object(sp, "actionable_refusal_reason", MagicMock(side_effect=boom)):
            ticket, err = _stage(report, decision)
        assert ticket is None and err is not None
        gate = MagicMock(side_effect=boom)
        with patch.object(tg, "actionable_refusal_reason", gate), pytest.raises(RuntimeError):
            _direct(report, decision)

        # Provenance fails: the gateway has no other report to ticket from.
        with _offline(receipts=True) as engine, patch.object(
            sp, "build_provenance_report", MagicMock(side_effect=boom)
        ):
            resp = dg.should_i_trade(
                engine, "NVDA", account_size_usd=_ACCOUNT, current_price=_PRICE, vol_30d=_VOL
            )
        assert resp.provenance_report is None and resp.trade_ticket is None
        assert resp.unified_verdict == "no_trade"
        assert resp.stage_errors["trade_ticket"] == "skipped — no provenance report"

    def test_e_separate_ticket_products_do_not_touch_the_gateway(self) -> None:
        for rel in _SEPARATE_TICKET_MODULES:
            tree = ast.parse((_REPO / rel).read_text(encoding="utf-8"))
            names = {
                alias.name
                for node in ast.walk(tree)
                if isinstance(node, ast.ImportFrom)
                for alias in node.names
            }
            modules = {
                node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
            } | {
                alias.name
                for node in ast.walk(tree)
                if isinstance(node, ast.Import)
                for alias in node.names
            }
            assert not names & _GATEWAY_NAMES, rel
            assert not modules & {"intelligence.decision_gateway", "intelligence.signal_provenance"}, rel

    @pytest.mark.parametrize("entry", ["generate_pair_ticket", "scan_candidate_pairs", "pair_route"])
    def test_e_the_pair_ticket_abstains_when_both_legs_abstain(self, entry: str) -> None:
        legs: dict[str, dg.DecisionResponse] = {}

        def _leg(engine: Any, ticker: str, **kw: Any) -> dg.DecisionResponse:
            legs[ticker] = _REAL_SHOULD_I_TRADE(engine, ticker, **kw)
            return legs[ticker]

        with _offline() as engine, patch.object(dg, "should_i_trade", _leg):
            if entry == "generate_pair_ticket":
                issued = pc.generate_pair_ticket(engine, "NVDA", "AMD", account_size_usd=_ACCOUNT)
            elif entry == "scan_candidate_pairs":
                found = pc.scan_candidate_pairs(
                    engine,
                    [pc.PairCandidate("NVDA", "AMD", "witness", "witness")],
                    account_size_usd=_ACCOUNT,
                )
                issued = found[0] if found else None
            else:
                res = _client(engine).get("/api/v1/conviction/pair/NVDA/AMD")
                assert res.status_code == 200, res.text
                body = res.json()
                issued = None if body.get("ticket", "") is None else body

        # Both legs ran the real gateway and abstained, for want of a receipt.
        assert set(legs) == {"NVDA", "AMD"}
        for ticker, resp in legs.items():
            assert resp.trade_ticket is None, ticker
            assert resp.unified_verdict == "low", ticker
            assert resp.provenance_report.direction == _CALLS[ticker]
            assert resp.provenance_report.verdict_reason.startswith(_FLOOR), ticker
            assert _NO_RECEIPT in resp.provenance_report.verdict_reason, ticker

        shown = issued.to_dict() if hasattr(issued, "to_dict") else issued
        assert issued is None, (
            "alternate generator bypass: the pair path issued a ticket while both "
            f"legs abstained ({_FLOOR}, {_NO_RECEIPT}): "
            f"{ {k: shown.get(k) for k in ('pair_name', 'verdict', 'pair_conviction_score')} }"
        )
