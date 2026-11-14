import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from analysis.desk_evidence import DeskEvidence, EvidenceKind, evidence_card


class DeskEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 11, 14, 12, tzinfo=timezone.utc)
        self.item = DeskEvidence(
            "slot-a", EvidenceKind.FORWARD, "receipt-a", self.now, self.now, 10
        )

    def card(self, **changes):
        return evidence_card(
            replace(self.item, **changes), as_of=self.now, max_age=timedelta(hours=1)
        )

    def test_forward_is_unproven(self):
        self.assertEqual(self.card()["label"], "unproven, 10 of 100")

    def test_sample_count_never_promotes(self):
        for n in (100, 101, 10000):
            c = self.card(samples=n)
            self.assertIn("validation pending", c["label"])
            self.assertFalse(c["promotion_allowed"])

    def test_development_not_forward(self):
        self.assertIn(
            "not forward evidence", self.card(kind=EvidenceKind.DEVELOPMENT)["label"]
        )

    def test_lead_requires_test(self):
        self.assertIn("undeclared_test", self.card(kind=EvidenceKind.LEAD)["reasons"])
        self.assertEqual(
            self.card(kind=EvidenceKind.LEAD, declared_test="test-a")["quality"],
            "clock_eligible",
        )

    def test_future_receipt_withheld(self):
        self.assertIn(
            "future_evidence",
            self.card(available_at=self.now + timedelta(seconds=1))["reasons"],
        )

    def test_future_observation_withheld(self):
        self.assertIn(
            "future_evidence",
            self.card(observed_at=self.now + timedelta(seconds=1))["reasons"],
        )

    def test_unknown_clock(self):
        for key in ("observed_at", "available_at"):
            self.assertIn("missing_clock", self.card(**{key: None})["reasons"])

    def test_latest_ingest_does_not_refresh_stale_observation(self):
        self.assertIn(
            "stale_observation",
            self.card(observed_at=self.now - timedelta(days=1))["reasons"],
        )

    def test_boundary(self):
        self.assertEqual(
            self.card(observed_at=self.now - timedelta(hours=1))["quality"],
            "clock_eligible",
        )

    def test_reversed_clocks(self):
        self.assertIn(
            "availability_before_observation",
            self.card(available_at=self.now - timedelta(seconds=1))["reasons"],
        )

    def test_naive_clock_rejected(self):
        with self.assertRaises(ValueError):
            self.card(observed_at=datetime(2026, 1, 1))

    def test_bad_counts(self):
        for n in (-1, 1.5, True):
            with self.assertRaises(ValueError):
                self.card(samples=n)
        with self.assertRaises(ValueError):
            self.card(required_samples=0)

    def test_identity_required(self):
        for key in ("receipt_id", "item_id"):
            with self.assertRaises(ValueError):
                self.card(**{key: ""})

    def test_unknown_kind(self):
        with self.assertRaises(ValueError):
            self.card(kind="validated")

    def test_explicit_policy(self):
        with self.assertRaises(ValueError):
            evidence_card(self.item, as_of=self.now, max_age=timedelta(0))

    def test_bad_declared_test(self):
        for value in (" ", [], "x" * 2049):
            with self.assertRaises(ValueError):
                self.card(declared_test=value)

    def test_deterministic_no_mutation(self):
        self.assertEqual(self.card(), self.card())
        self.assertEqual(self.item.samples, 10)


if __name__ == "__main__":
    unittest.main()
