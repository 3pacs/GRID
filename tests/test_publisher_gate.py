"""Unit tests for oracle.publisher_gate.gate_decision.

Covers the deterministic publish / review / reject branches:
  - empty-claims pass-through
  - hard reject on contradiction or critical sanity failure
  - auto-publish (all supported, no flags, avg confidence >= 0.85)
  - all-insufficient pass-through at score 0.5
  - human-review on any flagged claim
  - default publish for mixed-but-clean verdicts

gate_decision is the shared gate feeding both the chat firewall
(oracle/firewall.py) and the astrogrid publish path, so each branch
is pinned here against silent threshold drift.
"""

from __future__ import annotations

import pytest

from oracle.claim_extractor import Claim
from oracle.claim_verifier import VerifiedClaim
from oracle.publisher_gate import PublishDecision, gate_decision
from oracle.sanity_checker import CheckedClaim, SanityResult


# ── Fixtures / builders ──────────────────────────────────────────────────


def _claim(claim_type: str = "price", ticker: str = "SPY", value: float = 100.0) -> Claim:
    return Claim(text=f"{ticker} at {value}", claim_type=claim_type, ticker=ticker, value=value)


def _verified(
    verdict: str,
    confidence: float = 0.9,
    reason: str = "",
    claim_type: str = "price",
) -> VerifiedClaim:
    return VerifiedClaim(
        claim=_claim(claim_type=claim_type),
        verdict=verdict,
        confidence=confidence,
        reason=reason,
    )


def _sanity(flag: str, name: str = "range_check", message: str = "") -> SanityResult:
    return SanityResult(check_name=name, flag=flag, message=message or f"{name}:{flag}")


def _checked(
    verdict: str,
    confidence: float = 0.9,
    checks: tuple[SanityResult, ...] = (),
    critical_fail: bool = False,
    reason: str = "",
    claim_type: str = "price",
) -> CheckedClaim:
    return CheckedClaim(
        verified=_verified(verdict, confidence, reason, claim_type),
        checks=checks,
        critical_fail=critical_fail,
    )


# ── Empty input ──────────────────────────────────────────────────────────


class TestEmptyClaims:
    def test_empty_list_publishes_passthrough(self):
        d = gate_decision([])
        assert d.decision == "publish"
        assert d.score == 1.0
        assert d.claims == ()
        assert "pass-through" in d.reasons[0]


# ── Hard reject ──────────────────────────────────────────────────────────


class TestHardReject:
    def test_single_contradicted_rejects(self):
        d = gate_decision([_checked("contradicted", reason="DB shows 200, claim said 100")])
        assert d.decision == "reject"
        assert d.score == 0.0
        assert any("CONTRADICTED" in r for r in d.reasons)

    def test_contradicted_among_supported_still_rejects(self):
        claims = [
            _checked("supported"),
            _checked("supported"),
            _checked("contradicted", reason="bad"),
        ]
        d = gate_decision(claims)
        assert d.decision == "reject"
        assert d.score == 0.0

    def test_critical_sanity_failure_rejects(self):
        fail = _sanity("fail", name="price_range", message="SPY price 99999 out of range")
        d = gate_decision([_checked("supported", checks=(fail,), critical_fail=True)])
        assert d.decision == "reject"
        assert d.score == 0.0
        assert any("SANITY FAIL" in r for r in d.reasons)

    def test_contradicted_takes_precedence_over_critical_fail(self):
        # A claim both contradicted and critical-fail: contradicted branch returns first.
        fail = _sanity("fail", name="price_range", message="out of range")
        claims = [_checked("contradicted", reason="evidence mismatch", checks=(fail,), critical_fail=True)]
        d = gate_decision(claims)
        assert d.decision == "reject"
        assert any("CONTRADICTED" in r for r in d.reasons)

    def test_contradicted_takes_precedence_over_flags(self):
        warn = _sanity("warn")
        claims = [
            _checked("supported", checks=(warn,)),
            _checked("contradicted", reason="bad"),
        ]
        assert gate_decision(claims).decision == "reject"


# ── Auto-publish ─────────────────────────────────────────────────────────


class TestAutoPublish:
    def test_all_supported_high_confidence_auto_publishes(self):
        claims = [_checked("supported", 0.9), _checked("supported", 0.95)]
        d = gate_decision(claims)
        assert d.decision == "publish"
        assert d.score == pytest.approx(0.925)
        assert "All verifiable claims supported" in d.reasons[0]

    def test_confidence_exactly_at_threshold_auto_publishes(self):
        claims = [_checked("supported", 0.85)]
        d = gate_decision(claims)
        assert d.decision == "publish"
        assert d.score == pytest.approx(0.85)
        assert "All verifiable claims supported" in d.reasons[0]

    def test_confidence_just_below_threshold_falls_to_default_publish(self):
        # supported, no flags, but avg confidence < 0.85 -> not auto-publish,
        # falls through to the default "mixed verdicts" publish branch.
        claims = [_checked("supported", 0.84)]
        d = gate_decision(claims)
        assert d.decision == "publish"
        assert d.score == pytest.approx(0.84)
        assert "Mixed verdicts" in d.reasons[0]

    def test_supported_with_warn_flag_does_not_auto_publish(self):
        warn = _sanity("warn")
        d = gate_decision([_checked("supported", 0.95, checks=(warn,))])
        assert d.decision == "review"

    def test_supported_plus_insufficient_blocks_auto_publish_when_avg_low(self):
        # verifiable == supported (insufficient excluded), so supported == verifiable,
        # but the insufficient claim's 0.5 confidence drags avg below 0.85.
        claims = [_checked("supported", 0.95), _checked("insufficient", 0.5)]
        d = gate_decision(claims)
        assert d.decision == "publish"
        assert "Mixed verdicts" in d.reasons[0]
        assert d.score == pytest.approx(0.725)


# ── All insufficient ─────────────────────────────────────────────────────


class TestAllInsufficient:
    def test_all_insufficient_publishes_at_half_score(self):
        claims = [_checked("insufficient", 0.4), _checked("insufficient", 0.6)]
        d = gate_decision(claims)
        assert d.decision == "publish"
        assert d.score == 0.5
        assert "insufficient data" in d.reasons[0]

    def test_single_insufficient_publishes_at_half_score(self):
        d = gate_decision([_checked("insufficient", 0.9)])
        assert d.decision == "publish"
        assert d.score == 0.5


# ── Human review ─────────────────────────────────────────────────────────


class TestReview:
    def test_single_flagged_claim_triggers_review(self):
        warn = _sanity("warn", message="date is in the future")
        d = gate_decision([_checked("supported", 0.9, checks=(warn,))])
        assert d.decision == "review"
        assert d.score == pytest.approx(0.9)
        assert any("1/1 claims flagged" in r for r in d.reasons)

    def test_non_critical_fail_flag_triggers_review_not_reject(self):
        # flag == "fail" but critical_fail is False -> flagged, not a hard reject.
        fail = _sanity("fail", name="unit_check", message="unit mismatch")
        d = gate_decision([_checked("supported", 0.9, checks=(fail,), critical_fail=False)])
        assert d.decision == "review"

    def test_any_flag_among_many_clean_still_reviews(self):
        warn = _sanity("warn")
        claims = [
            _checked("supported", 0.9),
            _checked("supported", 0.9),
            _checked("supported", 0.9, checks=(warn,)),
        ]
        d = gate_decision(claims)
        assert d.decision == "review"
        assert any("1/3 claims flagged" in r for r in d.reasons)

    def test_flagged_insufficient_mix_reviews(self):
        warn = _sanity("warn")
        claims = [_checked("supported", 0.9), _checked("insufficient", 0.5, checks=(warn,))]
        d = gate_decision(claims)
        assert d.decision == "review"

    def test_pass_only_checks_do_not_flag(self):
        ok = _sanity("pass")
        d = gate_decision([_checked("supported", 0.95, checks=(ok,))])
        assert d.decision == "publish"
        assert "All verifiable claims supported" in d.reasons[0]


# ── Default publish (mixed but clean) ────────────────────────────────────


class TestDefaultPublish:
    def test_supported_plus_insufficient_no_flags_publishes(self):
        claims = [_checked("supported", 0.7), _checked("insufficient", 0.7)]
        d = gate_decision(claims)
        assert d.decision == "publish"
        assert d.score == pytest.approx(0.7)
        assert "Mixed verdicts" in d.reasons[0]


# ── Result shape ─────────────────────────────────────────────────────────


class TestPublishDecisionShape:
    def test_claims_tuple_preserves_order_and_content(self):
        c1 = _checked("supported", 0.9)
        c2 = _checked("insufficient", 0.5)
        d = gate_decision([c1, c2])
        assert d.claims == (c1, c2)

    def test_decision_is_frozen(self):
        d = gate_decision([_checked("supported", 0.95)])
        with pytest.raises(AttributeError):
            d.decision = "reject"  # type: ignore[misc]

    def test_returns_publish_decision_instance(self):
        assert isinstance(gate_decision([]), PublishDecision)
