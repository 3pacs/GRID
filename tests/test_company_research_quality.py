"""Standard-library-only regressions: python -m unittest discover -s tests -p test_company_research_quality.py."""
import copy
import hashlib
import unittest

from company_research_quality import evaluate_company_research


class QualityTests(unittest.TestCase):
    def setUp(self):
        text = "Revenue was 10 million dollars. One purchase commitment is 50 million."
        self.packet = {
            "company_id": "issuer:example", "required_source_kinds": ["filing"],
            "documents": [{"company_id": "issuer:example", "snapshot_id": "s1",
                           "source_kind": "filing", "source_url": "https://example.test/filing",
                           "parser_version": "test-v1", "parse_status": "parsed",
                           "published_at": "2026-11-01T00:00:00Z", "retrieved_at": "2026-11-02T00:00:00Z",
                           "content_sha256": hashlib.sha256(text.encode()).hexdigest(),
                           "text_sha256": hashlib.sha256(text.encode()).hexdigest(), "text": text}],
            "claims": [{"id": "c1", "kind": "fact", "text": "Revenue was 10 million dollars.",
                        "evidence": [{"snapshot_id": "s1", "quote": "Revenue was 10 million dollars."}],
                        "metric": "revenue", "value": "10", "scale": "1000000", "unit": "USD",
                        "period_type": "duration", "period_start": "2026-07-01", "period_end": "2026-09-30",
                        "fiscal_year": 2026, "fiscal_period": "Q3", "scope": "consolidated", "basis": "GAAP"}],
        }

    def evaluate(self):
        return evaluate_company_research(self.packet, as_of="2026-11-09T00:00:00Z", max_age_days={"filing": 120})

    def codes(self):
        return {x["code"] for x in self.evaluate()["issues"]}

    def test_clean_is_eligible_not_true(self):
        result = self.evaluate()
        self.assertTrue(result["eligible_for_synthesis"])
        self.assertFalse(result["truth_assessed"])
        self.assertFalse(result["latest_source_confirmed"])

    def test_missing_is_not_zero(self):
        self.packet["documents"] = []
        self.assertIn("coverage_missing", self.codes())

    def test_unknown_date_is_not_retrieval(self):
        self.packet["documents"][0]["published_at"] = None
        self.assertIn("date_unknown_or_invalid", self.codes())
        self.assertEqual(self.evaluate()["coverage"]["filing"]["status"], "unusable")

    def test_future_retrieval_blocks_pit(self):
        self.packet["documents"][0]["retrieved_at"] = "2026-11-10T00:00:00Z"
        self.assertIn("after_cutoff", self.codes())

    def test_stale_uses_publication_not_recent_fetch(self):
        self.packet["documents"][0]["published_at"] = "2025-01-01T00:00:00Z"
        self.assertIn("stale", self.codes())

    def test_wrong_entity(self):
        self.packet["documents"][0]["company_id"] = "another"
        self.assertIn("company_mismatch", self.codes())

    def test_hash_tamper(self):
        self.packet["documents"][0]["text"] += " Extra text."
        self.assertIn("text_hash_mismatch", self.codes())

    def test_duplicate_snapshot(self):
        self.packet["documents"] *= 2
        self.assertIn("snapshot_identity_invalid", self.codes())

    def test_quote_required(self):
        self.packet["claims"][0]["evidence"][0]["quote"] = "fabricated"
        self.assertIn("quote_not_found", self.codes())

    def test_inference_requires_reasoning(self):
        self.packet["claims"][0]["kind"] = "inference"
        self.assertIn("inference_reasoning_missing", self.codes())

    def test_units_and_fiscal_context_required(self):
        for field in ("unit", "scale", "fiscal_year", "fiscal_period", "basis", "scope"):
            with self.subTest(field=field):
                original = self.packet["claims"][0].pop(field)
                self.assertIn("metric_context_missing", self.codes())
                self.packet["claims"][0][field] = original

    def test_invalid_numbers(self):
        for value in ("NaN", "Infinity", True, None):
            self.packet["claims"][0]["value"] = value
            self.assertIn("metric_number_invalid", self.codes())

    def test_conflicting_values(self):
        other = copy.deepcopy(self.packet["claims"][0])
        other.update(id="c2", value="11")
        self.packet["claims"].append(other)
        self.assertIn("metric_conflict", self.codes())

    def test_equal_values_different_scale(self):
        other = copy.deepcopy(self.packet["claims"][0])
        other.update(id="c2", value="10000000", scale="1")
        self.packet["claims"].append(other)
        self.assertNotIn("metric_conflict", self.codes())

    def test_different_basis_not_comparable(self):
        other = copy.deepcopy(self.packet["claims"][0])
        other.update(id="c2", value="11", basis="non-GAAP")
        self.packet["claims"].append(other)
        self.assertNotIn("metric_conflict", self.codes())

    def test_repeated_commitment_no_auto_sum(self):
        self.packet["claims"][0]["commitment_id"] = "purchase-001"
        other = copy.deepcopy(self.packet["claims"][0])
        other["id"] = "c2"
        self.packet["claims"].append(other)
        self.assertIn("commitment_repeated", self.codes())

    def test_period_order(self):
        self.packet["claims"][0]["period_start"] = "2026-10-01"
        self.assertIn("period_invalid", self.codes())

    def test_explicit_policy_required(self):
        result = evaluate_company_research(self.packet, as_of="2026-11-09T00:00:00Z", max_age_days={})
        self.assertIn("freshness_policy_missing", {x["code"] for x in result["issues"]})

    def test_no_mutation(self):
        original = copy.deepcopy(self.packet)
        self.evaluate()
        self.assertEqual(self.packet, original)

    def test_no_claims_not_complete(self):
        self.packet["claims"] = []
        self.assertIn("claims_missing", self.codes())

    def test_bad_shape_rejected(self):
        for field in ("documents", "claims", "required_source_kinds"):
            with self.subTest(field=field):
                original = self.packet[field]
                self.packet[field] = {}
                with self.assertRaises(ValueError):
                    self.evaluate()
                self.packet[field] = original


if __name__ == "__main__":
    unittest.main()
