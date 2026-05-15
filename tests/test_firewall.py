"""Unit tests for oracle/firewall.py.

Covers `verify_output` — the single entry point invoked by
`api/routers/chat.py:1409` to gate every LLM response — and its two
private helpers `_mark_unverified` and `_audit_claims`. All three
gate-decision branches (publish / review / reject), the empty-claims
short-circuit, the [UNVERIFIED] annotation in reverse-span order, and
the non-blocking audit-write failure mode are exercised.

TIER 4 punch-list item — docs/PUNCH-LIST-2026-05-13.md [P1] item 4.

Mocking strategy: patches `extract_claims`, `verify_claims`,
`run_sanity_checks`, and `gate_decision` at the
`oracle.firewall.*` import site so the test runs without DB and
without re-running the upstream pipeline. The engine is a MagicMock
mirroring the
``engine.connect().__enter__().execute()`` chain established in
PR #161 (`tests/test_psi_model.py`) and PR #162
(`tests/test_claim_verifier.py``).
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from oracle.claim_extractor import Claim
from oracle.claim_verifier import VerifiedClaim
from oracle.firewall import (
    FirewallResult,
    _SAFE_FALLBACK,
    _audit_claims,
    _mark_unverified,
    verify_output,
)
from oracle.publisher_gate import PublishDecision
from oracle.sanity_checker import CheckedClaim, SanityResult


# ── Fixture helpers ──────────────────────────────────────────────────────


def _claim(text: str, ctype: str = "price", span: tuple[int, int] = (0, 0)) -> Claim:
    return Claim(text=text, claim_type=ctype, source_span=span)


def _verified(
    claim: Claim,
    verdict: str = "supported",
    confidence: float = 0.9,
    reason: str = "",
) -> VerifiedClaim:
    return VerifiedClaim(claim=claim, verdict=verdict, confidence=confidence, reason=reason)


def _checked(
    verified: VerifiedClaim,
    checks: tuple[SanityResult, ...] = (),
    critical_fail: bool = False,
) -> CheckedClaim:
    return CheckedClaim(verified=verified, checks=checks, critical_fail=critical_fail)


def _decision(
    decision: str,
    score: float = 1.0,
    claims: tuple[CheckedClaim, ...] = (),
    reasons: tuple[str, ...] = (),
) -> PublishDecision:
    return PublishDecision(
        decision=decision, score=score, claims=claims, reasons=reasons
    )


def _mock_engine() -> MagicMock:
    """Build a MagicMock engine with a stub
    ``connect().__enter__().execute()`` chain. The default leaves
    ``execute`` as a plain MagicMock; tests override per-case."""
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = MagicMock()
    return engine


# ── verify_output — short-circuit on zero claims ─────────────────────────


class TestVerifyOutputEmptyClaims:
    def test_no_claims_returns_publish_with_pass_through_text(self) -> None:
        engine = _mock_engine()
        with patch("oracle.firewall.extract_claims", return_value=[]):
            result = verify_output("Some narrative text.", engine)
        assert isinstance(result, FirewallResult)
        assert result.decision.decision == "publish"
        assert result.claim_count == 0
        assert result.flagged_count == 0
        assert result.original_text == "Some narrative text."
        assert result.output_text == "Some narrative text."

    def test_no_claims_skips_verify_sanity_and_audit(self) -> None:
        engine = _mock_engine()
        with patch("oracle.firewall.extract_claims", return_value=[]), \
            patch("oracle.firewall.verify_claims") as v, \
            patch("oracle.firewall.run_sanity_checks") as s, \
            patch("oracle.firewall._audit_claims") as a:
            verify_output("anything", engine)
        v.assert_not_called()
        s.assert_not_called()
        a.assert_not_called()


# ── verify_output — publish branch ───────────────────────────────────────


class TestVerifyOutputPublish:
    def test_all_supported_returns_publish_with_original_text(self) -> None:
        engine = _mock_engine()
        text = "SPY closed at $500."
        claim = _claim(text, "price", span=(0, len(text)))
        checked = _checked(_verified(claim, verdict="supported"))
        with patch("oracle.firewall.extract_claims", return_value=[claim]), \
            patch("oracle.firewall.verify_claims", return_value=[checked.verified]), \
            patch("oracle.firewall.run_sanity_checks", return_value=[checked]), \
            patch("oracle.firewall.gate_decision",
                  return_value=_decision("publish", claims=(checked,))):
            result = verify_output(text, engine)
        assert result.decision.decision == "publish"
        assert result.output_text == text  # untouched
        assert result.original_text == text
        assert result.claim_count == 1
        assert result.flagged_count == 0


# ── verify_output — reject branch ────────────────────────────────────────


class TestVerifyOutputReject:
    def test_reject_returns_safe_fallback_text(self) -> None:
        engine = _mock_engine()
        text = "QQQ skyrocketed 50% today!"
        claim = _claim(text, "percentage", span=(4, 10))
        checked = _checked(
            _verified(claim, verdict="contradicted", reason="evidence shows +0.5%"),
        )
        with patch("oracle.firewall.extract_claims", return_value=[claim]), \
            patch("oracle.firewall.verify_claims", return_value=[checked.verified]), \
            patch("oracle.firewall.run_sanity_checks", return_value=[checked]), \
            patch("oracle.firewall.gate_decision",
                  return_value=_decision("reject", score=0.1, claims=(checked,))), \
            patch("oracle.firewall._audit_claims"):
            result = verify_output(text, engine)
        assert result.decision.decision == "reject"
        assert result.output_text == _SAFE_FALLBACK
        assert result.output_text != text
        assert result.flagged_count == 1  # contradicted counts as flagged

    def test_reject_flagged_count_includes_critical_fail(self) -> None:
        engine = _mock_engine()
        text = "abc"
        claim = _claim(text, "price")
        checked = _checked(_verified(claim, verdict="supported"), critical_fail=True)
        with patch("oracle.firewall.extract_claims", return_value=[claim]), \
            patch("oracle.firewall.verify_claims", return_value=[checked.verified]), \
            patch("oracle.firewall.run_sanity_checks", return_value=[checked]), \
            patch("oracle.firewall.gate_decision",
                  return_value=_decision("reject", claims=(checked,))), \
            patch("oracle.firewall._audit_claims"):
            result = verify_output(text, engine)
        assert result.flagged_count == 1


# ── verify_output — review branch ────────────────────────────────────────


class TestVerifyOutputReview:
    def test_review_inserts_unverified_markers(self) -> None:
        engine = _mock_engine()
        text = "GLD at $200 and BTC at $50000."
        c1 = _claim("$200", "price", span=(7, 11))      # ⌟ flagged: contradicted
        c2 = _claim("$50000", "price", span=(23, 29))   # ⌟ unflagged
        checked1 = _checked(_verified(c1, verdict="contradicted", reason="actual=$180"))
        checked2 = _checked(_verified(c2, verdict="supported"))
        with patch("oracle.firewall.extract_claims", return_value=[c1, c2]), \
            patch("oracle.firewall.verify_claims",
                  return_value=[checked1.verified, checked2.verified]), \
            patch("oracle.firewall.run_sanity_checks",
                  return_value=[checked1, checked2]), \
            patch("oracle.firewall.gate_decision",
                  return_value=_decision("review", score=0.6,
                                         claims=(checked1, checked2))), \
            patch("oracle.firewall._audit_claims"):
            result = verify_output(text, engine)
        assert result.decision.decision == "review"
        assert "[UNVERIFIED]" in result.output_text
        # Original text unmodified
        assert result.original_text == text
        # Only contradicted claim should have a marker — supported one should not
        assert result.output_text.count("[UNVERIFIED]") == 1


# ── verify_output — audit invocation ─────────────────────────────────────


class TestVerifyOutputCallsAudit:
    def test_audit_called_with_checked_and_decision(self) -> None:
        engine = _mock_engine()
        text = "x"
        claim = _claim(text)
        checked = _checked(_verified(claim))
        decision = _decision("publish", claims=(checked,))
        with patch("oracle.firewall.extract_claims", return_value=[claim]), \
            patch("oracle.firewall.verify_claims", return_value=[checked.verified]), \
            patch("oracle.firewall.run_sanity_checks", return_value=[checked]), \
            patch("oracle.firewall.gate_decision", return_value=decision), \
            patch("oracle.firewall._audit_claims") as audit:
            verify_output(text, engine)
        audit.assert_called_once_with(engine, [checked], decision)


# ── _mark_unverified — string-mutation logic ─────────────────────────────


class TestMarkUnverified:
    def test_flagged_contradicted_inserts_marker(self) -> None:
        text = "SPY at $500."
        cc = _checked(_verified(_claim("$500", "price", span=(7, 11)),
                                verdict="contradicted"))
        result = _mark_unverified(text, [cc])
        assert "[UNVERIFIED]" in result
        # Marker is inserted at the span start (before the dollar sign).
        assert result.index("[UNVERIFIED]") == 7

    def test_supported_no_markers(self) -> None:
        text = "SPY at $500."
        cc = _checked(_verified(_claim("$500", "price", span=(7, 11)),
                                verdict="supported"))
        result = _mark_unverified(text, [cc])
        assert "[UNVERIFIED]" not in result
        assert result == text

    def test_critical_fail_inserts_marker_even_if_supported(self) -> None:
        text = "SPY at $500."
        cc = _checked(
            _verified(_claim("$500", "price", span=(7, 11)), verdict="supported"),
            critical_fail=True,
        )
        result = _mark_unverified(text, [cc])
        assert "[UNVERIFIED]" in result

    def test_ambiguous_verdict_inserts_marker(self) -> None:
        text = "SPY at $500."
        cc = _checked(_verified(_claim("$500", "price", span=(7, 11)),
                                verdict="ambiguous"))
        result = _mark_unverified(text, [cc])
        assert "[UNVERIFIED]" in result

    def test_warn_sanity_check_inserts_marker(self) -> None:
        text = "SPY at $500."
        cc = _checked(
            _verified(_claim("$500", "price", span=(7, 11)), verdict="supported"),
            checks=(SanityResult("range", "warn", "near upper bound"),),
        )
        result = _mark_unverified(text, [cc])
        assert "[UNVERIFIED]" in result

    def test_pass_sanity_check_no_marker(self) -> None:
        text = "SPY at $500."
        cc = _checked(
            _verified(_claim("$500", "price", span=(7, 11)), verdict="supported"),
            checks=(SanityResult("range", "pass", "ok"),),
        )
        result = _mark_unverified(text, [cc])
        assert "[UNVERIFIED]" not in result

    def test_multiple_flagged_inserted_in_reverse_order_preserves_spans(self) -> None:
        # Two flagged claims: spans (3, 6) and (10, 13). Reverse-insertion
        # guarantees the second one's span is not shifted by the first's marker.
        text = "abc123 def456 xyz"
        cc1 = _checked(_verified(_claim("123", "percentage", span=(3, 6)),
                                 verdict="contradicted"))
        cc2 = _checked(_verified(_claim("456", "percentage", span=(10, 13)),
                                 verdict="contradicted"))
        result = _mark_unverified(text, [cc1, cc2])
        # Both markers present
        assert result.count("[UNVERIFIED]") == 2
        # Original characters all preserved
        assert "123" in result and "456" in result
        # Marker positions: first one before "123", second before "456".
        m1 = result.index("[UNVERIFIED] 123")
        m2 = result.index("[UNVERIFIED] 456")
        assert m1 < m2

    def test_span_start_zero_prepends_marker(self) -> None:
        text = "+5% today"
        cc = _checked(_verified(_claim("+5%", "percentage", span=(0, 3)),
                                verdict="contradicted"))
        result = _mark_unverified(text, [cc])
        assert result.startswith("[UNVERIFIED] ")

    def test_span_start_past_end_of_text_silently_skipped(self) -> None:
        text = "short"
        # span_start beyond len(text) — should not raise, just leave text alone
        cc = _checked(_verified(_claim("xx", "price", span=(999, 1001)),
                                verdict="contradicted"))
        result = _mark_unverified(text, [cc])
        assert result == text


# ── _audit_claims — DB write path ────────────────────────────────────────


class TestAuditClaims:
    def test_inserts_one_row_per_claim(self) -> None:
        engine = MagicMock()
        conn = engine.connect.return_value.__enter__.return_value
        c1 = _claim("$500", "price")
        c2 = _claim("+5%", "percentage")
        cc1 = _checked(_verified(c1, verdict="supported", confidence=0.9))
        cc2 = _checked(_verified(c2, verdict="contradicted", confidence=0.4))
        decision = _decision("publish", claims=(cc1, cc2))
        _audit_claims(engine, [cc1, cc2], decision)
        assert conn.execute.call_count == 2
        conn.commit.assert_called_once()

    def test_published_flag_true_when_decision_is_publish(self) -> None:
        engine = MagicMock()
        conn = engine.connect.return_value.__enter__.return_value
        cc = _checked(_verified(_claim("$500", "price")))
        _audit_claims(engine, [cc], _decision("publish", claims=(cc,)))
        params = conn.execute.call_args[0][1]
        assert params["published"] is True

    def test_published_flag_false_when_decision_is_review_or_reject(self) -> None:
        engine = MagicMock()
        conn = engine.connect.return_value.__enter__.return_value
        cc = _checked(_verified(_claim("$500", "price")))
        _audit_claims(engine, [cc], _decision("review", claims=(cc,)))
        assert conn.execute.call_args[0][1]["published"] is False

        engine2 = MagicMock()
        conn2 = engine2.connect.return_value.__enter__.return_value
        _audit_claims(engine2, [cc], _decision("reject", claims=(cc,)))
        assert conn2.execute.call_args[0][1]["published"] is False

    @pytest.mark.parametrize(
        "claim_type,expected",
        [
            ("price", "high"),
            ("percentage", "high"),
            ("direction", "medium"),
            ("indicator", "medium"),
            ("narrative", "medium"),
            ("date", "medium"),
        ],
    )
    def test_materiality_high_for_price_and_pct_else_medium(
        self, claim_type: str, expected: str,
    ) -> None:
        engine = MagicMock()
        conn = engine.connect.return_value.__enter__.return_value
        cc = _checked(_verified(_claim("x", claim_type)))
        _audit_claims(engine, [cc], _decision("publish", claims=(cc,)))
        params = conn.execute.call_args[0][1]
        assert params["materiality"] == expected

    def test_claim_text_truncated_at_500_chars(self) -> None:
        engine = MagicMock()
        conn = engine.connect.return_value.__enter__.return_value
        long_text = "A" * 800
        cc = _checked(_verified(_claim(long_text, "narrative")))
        _audit_claims(engine, [cc], _decision("publish", claims=(cc,)))
        params = conn.execute.call_args[0][1]
        assert len(params["claim_text"]) == 500
        assert params["claim_text"] == "A" * 500

    def test_evidence_payload_includes_value_date_source_reason(self) -> None:
        import json as _json

        engine = MagicMock()
        conn = engine.connect.return_value.__enter__.return_value
        v = VerifiedClaim(
            claim=_claim("$500", "price"),
            verdict="supported",
            confidence=0.9,
            evidence_value=500.0,
            evidence_date="2026-05-13",
            evidence_source="YF:SPY:close",
            reason="within 1%",
        )
        cc = _checked(v)
        _audit_claims(engine, [cc], _decision("publish", claims=(cc,)))
        params = conn.execute.call_args[0][1]
        evidence = _json.loads(params["evidence"])
        assert evidence == {
            "value": 500.0,
            "date": "2026-05-13",
            "source": "YF:SPY:close",
            "reason": "within 1%",
        }

    def test_sanity_checks_payload_is_serialised(self) -> None:
        import json as _json

        engine = MagicMock()
        conn = engine.connect.return_value.__enter__.return_value
        cc = _checked(
            _verified(_claim("$500", "price")),
            checks=(
                SanityResult("range_check", "pass", "ok"),
                SanityResult("pct_math", "warn", "rounded"),
            ),
        )
        _audit_claims(engine, [cc], _decision("publish", claims=(cc,)))
        params = conn.execute.call_args[0][1]
        sanity = _json.loads(params["sanity_checks"])
        assert sanity == [
            {"check": "range_check", "flag": "pass", "msg": "ok"},
            {"check": "pct_math", "flag": "warn", "msg": "rounded"},
        ]

    def test_db_failure_is_swallowed_and_logged(self) -> None:
        """Audit failures must be non-blocking — chat firewall stays up
        even if the claim_audit table is unreachable."""
        engine = MagicMock()
        engine.connect.side_effect = RuntimeError("DB down")
        cc = _checked(_verified(_claim("$500", "price")))
        # Should NOT raise
        _audit_claims(engine, [cc], _decision("publish", claims=(cc,)))

    def test_empty_claims_list_does_not_call_execute(self) -> None:
        engine = MagicMock()
        conn = engine.connect.return_value.__enter__.return_value
        _audit_claims(engine, [], _decision("publish"))
        conn.execute.assert_not_called()
        conn.commit.assert_called_once()


# ── End-to-end smoke: verify_output flagged_count semantics ──────────────


class TestFlaggedCountSemantics:
    def test_flagged_count_counts_contradicted_plus_critical_fail(self) -> None:
        engine = _mock_engine()
        c1 = _claim("a", "price")  # contradicted
        c2 = _claim("b", "price")  # critical_fail
        c3 = _claim("c", "price")  # supported, no fail → not flagged
        cc1 = _checked(_verified(c1, verdict="contradicted"))
        cc2 = _checked(_verified(c2, verdict="supported"), critical_fail=True)
        cc3 = _checked(_verified(c3, verdict="supported"))
        with patch("oracle.firewall.extract_claims", return_value=[c1, c2, c3]), \
            patch("oracle.firewall.verify_claims",
                  return_value=[cc1.verified, cc2.verified, cc3.verified]), \
            patch("oracle.firewall.run_sanity_checks",
                  return_value=[cc1, cc2, cc3]), \
            patch("oracle.firewall.gate_decision",
                  return_value=_decision("review", claims=(cc1, cc2, cc3))), \
            patch("oracle.firewall._audit_claims"):
            result = verify_output("abc", engine)
        # contradicted + critical_fail = 2 flagged; supported untouched = not flagged
        assert result.flagged_count == 2
        assert result.claim_count == 3

    def test_flagged_count_zero_when_all_clean(self) -> None:
        engine = _mock_engine()
        c1 = _claim("a", "price")
        cc1 = _checked(_verified(c1, verdict="supported"))
        with patch("oracle.firewall.extract_claims", return_value=[c1]), \
            patch("oracle.firewall.verify_claims", return_value=[cc1.verified]), \
            patch("oracle.firewall.run_sanity_checks", return_value=[cc1]), \
            patch("oracle.firewall.gate_decision",
                  return_value=_decision("publish", claims=(cc1,))), \
            patch("oracle.firewall._audit_claims"):
            result = verify_output("a", engine)
        assert result.flagged_count == 0
