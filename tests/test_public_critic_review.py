"""Invented examples only: two funds, 20%/negative returns, one of two rising."""
import copy
import hashlib
import importlib.util
from pathlib import Path
import unittest

SOURCE = Path(__file__).resolve().parents[1] / "scripts/verify_public_critic_review.py"
spec = importlib.util.spec_from_file_location("critic_guard", SOURCE)
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


def fixture():
    text = "Synthetic A NAV: Monday100, Tuesday120. Synthetic B: Monday100, Tuesday90."
    ctx = {"scope": "public_source_only", "originals_available": False,
           "pit_outcomes_available": False, "passages": {"P": "Synthetic fund A rose20%."},
           "sources": {"S": {"url": "https://example.invalid/primary", "text": text,
                                "sha256": hashlib.sha256(text.encode()).hexdigest()}},
           "horizons": {"H": {"start": "2030-01-07", "end": "2030-01-08", "sessions": 1}},
           "observations": {}, "count_universe": ["A", "B"], "count_pairs": {}}
    for fund, end in [("A", "120"), ("B", "90")]:
        for suffix, value, date in [("0", "100", "2030-01-07"), ("1", end, "2030-01-08")]:
            ctx["observations"][fund + suffix] = {"value": value, "basis": "nav_price_level",
                "unit": "syntheticUSD", "session": date, "instrument": fund}
        ctx["count_pairs"][fund] = {"prior": fund + "0", "current": fund + "1"}
    review = {"scope": "public_source_only", "performance_status": "unscorable",
              "missing_originals_or_pit": True,
              "claims": [{"id": "C", "passage_id": "P", "exact_quote": "fund A rose20%",
                          "inference": "observation", "disposition": "supported",
                          "citations": [{"source_id": "S", "url": "https://example.invalid/primary",
                                         "exact_excerpt": "Monday100, Tuesday120"}]}],
              "comparisons": [{"prior": "A0", "current": "A1", "basis": "nav_price_level",
                               "horizon": "H", "decimal_places": 2, "value_pct": "20.00"}],
              "counts": [{"members": ["A", "B"], "horizon": "H", "basis": "nav_price_level",
                          "numerator": 1, "denominator": 2}]}
    return ctx, review


class CriticGuardTests(unittest.TestCase):
    def setUp(self):
        self.ctx, self.review = fixture()

    def check(self):
        return guard.verify(self.ctx, self.review, guard.digest(self.ctx))

    def reject(self, expected):
        with self.assertRaisesRegex(ValueError, expected):
            self.check()

    def test_valid_arithmetic_and_coverage_still_requires_semantics(self):
        self.assertEqual(self.check()["semantic_review"], "REQUIRED")

    def test_causal_attribution_cannot_be_fact(self):
        self.review["claims"][0]["inference"] = "causal_attribution"
        self.reject("causal attribution")

    def test_causal_hypothesis_or_rejection_is_retained(self):
        for disposition in ["hypothesis", "rejected"]:
            self.review["claims"][0].update(inference="causal_attribution", disposition=disposition)
            self.assertEqual(self.check()["deterministic_checks"], "PASS")

    def test_paraphrase_is_not_an_exact_quote(self):
        self.review["claims"][0]["exact_quote"] = "A gained twenty percent"
        self.reject("quote mismatch")

    def test_wrong_citation_url(self):
        self.review["claims"][0]["citations"][0]["url"] = "https://example.invalid/rewrite"
        self.reject("citation URL")

    def test_fabricated_citation_excerpt(self):
        self.review["claims"][0]["citations"][0]["exact_excerpt"] = "NAV500"
        self.reject("citation excerpt")

    def test_missing_citation(self):
        self.review["claims"][0]["citations"] = []
        self.reject("citation missing")

    def test_tampered_source(self):
        self.ctx["sources"]["S"]["text"] += " revised"
        self.reject("source text pin")

    def test_external_pin_prevents_context_rewrite(self):
        pin = guard.digest(self.ctx)
        self.ctx["observations"]["A1"]["value"] = "140"
        with self.assertRaisesRegex(ValueError, "context pin"):
            guard.verify(self.ctx, self.review, pin)

    def test_price_cannot_be_substituted_for_NAV(self):
        self.ctx["observations"]["A1"]["basis"] = "market_close_price"
        self.reject("mixed price/NAV")

    def test_explicit_market_price_method_is_allowed(self):
        for obs in self.ctx["observations"].values():
            obs["basis"] = "market_close_price"
        for item in self.review["comparisons"] + self.review["counts"]:
            item["basis"] = "market_close_price"
        self.assertEqual(self.check()["deterministic_checks"], "PASS")

    def test_price_levels_cannot_be_total_returns(self):
        self.review["comparisons"][0]["basis"] = "total_return"
        self.reject("unsupported basis")

    def test_different_instrument_denominator(self):
        self.review["comparisons"][0]["prior"] = "B0"
        self.reject("instrument mismatch")

    def test_wrong_window(self):
        self.ctx["observations"]["A1"]["session"] = "2030-01-09"
        self.reject("window mismatch")

    def test_multisession_is_not_single_session(self):
        self.ctx["horizons"]["H"]["sessions"] = 2
        self.reject("single-session")

    def test_wrong_percentage(self):
        self.review["comparisons"][0]["value_pct"] = "19.00"
        self.reject("arithmetic mismatch")

    def test_nonfinite_or_boolean_value(self):
        for value in ["NaN", "Infinity", True]:
            self.ctx["observations"]["A1"]["value"] = value
            self.reject("finite")

    def test_count_denominator_cannot_drop_losing_fund(self):
        self.review["counts"][0].update(members=["A"], numerator=1, denominator=1)
        self.reject("complete pinned universe")

    def test_duplicate_count_member(self):
        self.review["counts"][0]["members"] = ["A", "B", "A"]
        self.reject("complete pinned universe")

    def test_rising_pair_cannot_be_reused_for_falling_member(self):
        self.ctx["count_pairs"]["B"] = dict(self.ctx["count_pairs"]["A"])
        self.review["counts"][0]["numerator"] = 2
        self.reject("count member/instrument")

    def test_swapped_member_pairs_are_rejected_even_with_correct_count(self):
        self.ctx["count_pairs"]["A"], self.ctx["count_pairs"]["B"] = (
            self.ctx["count_pairs"]["B"], self.ctx["count_pairs"]["A"])
        self.reject("count member/instrument")

    def test_aliased_instrument_binding_is_rejected(self):
        for key in ("B0", "B1"):
            self.ctx["observations"][key]["instrument"] = "A"
        self.reject("count member/instrument")

    def test_zero_return_member_remains_valid_and_not_positive(self):
        self.ctx["observations"]["B1"]["value"] = "100"
        self.assertEqual(self.check()["deterministic_checks"], "PASS")

    def test_wrong_count(self):
        self.review["counts"][0]["numerator"] = 2
        self.reject("count arithmetic")

    def test_missing_originals_or_pit_cannot_be_scored(self):
        for originals, pit in [(False, True), (True, False), (False, False)]:
            self.ctx.update(originals_available=originals, pit_outcomes_available=pit)
            self.review["performance_status"] = "scored"
            self.reject("cannot admit performance")

    def test_missing_evidence_cannot_be_hidden(self):
        self.review["missing_originals_or_pit"] = False
        self.reject("missing evidence hidden")

    def test_duplicate_claim(self):
        self.review["claims"].append(copy.deepcopy(self.review["claims"][0]))
        self.reject("duplicate claim")


if __name__ == "__main__":
    unittest.main()
