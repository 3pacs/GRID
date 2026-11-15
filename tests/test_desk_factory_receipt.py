"""Synthetic receipt/report seam; no actual ledgers, prices or holdouts."""
import copy
import hashlib
import json
import unittest
from datetime import datetime, timedelta, timezone

from analysis.desk_factory_receipt import receipt_bound_factory_cards


def fixture():
    row = {"trial_id": "a", "family": "f", "feature": "F1", "n": 100,
           "r": -1.0, "net_dollars": -0.04, "t": -2.0,
           "p": 0.9, "adjusted_p": 1.0, "selected": False}
    report = {"run_id": "synthetic", "state": "INTRADAY_DEV_RANKING_ONLY",
              "promotion_allowed": False, "frozen_sha256": "a" * 64,
              "target_unit": "bps", "trial_count": 1, "tested": 1,
              "untestable": 0, "selected": [], "ranking": [row]}
    receipt = {"kind": "factory_run_result", "promotion_allowed": False,
               "run_id": "synthetic", "frozen_sha256": "a" * 64,
               "recorded_at": "2026-11-15T00:00:00+00:00",
               "trial_count": 1, "tested_count": 1, "selected_count": 0,
               "trials": [{**row, "status": "tested"}]}
    return report, receipt


class ReceiptCallerTest(unittest.TestCase):
    def setUp(self):
        self.report, self.receipt = fixture()
        self.now = datetime(2026, 11, 15, tzinfo=timezone.utc)

    def raw(self):
        report = json.dumps(self.report, indent=2, sort_keys=True).encode()
        self.receipt["report_sha256"] = hashlib.sha256(
            json.dumps(self.report, sort_keys=True, allow_nan=False).encode()
        ).hexdigest()
        receipt = json.dumps(self.receipt, sort_keys=True,
                             separators=(",", ":"), allow_nan=False).encode()
        return report, receipt

    def call(self, report=None, receipt=None, **overrides):
        if report is None:
            report, receipt = self.raw()
        args = dict(expected_result_sha256=hashlib.sha256(receipt).hexdigest(),
                    receipt_available_at=self.now, as_of=self.now,
                    max_age=timedelta(hours=1))
        args.update(overrides)
        return receipt_bound_factory_cards(report, receipt, **args)

    def test_native_pretty_bytes_differ_from_canonical_digest(self):
        raw, receipt = self.raw()
        self.assertNotEqual(hashlib.sha256(raw).hexdigest(), self.receipt["report_sha256"])
        result = self.call(raw, receipt)
        self.assertEqual(result["report_sha256"], hashlib.sha256(raw).hexdigest())
        self.assertEqual(result["report_canonical_sha256"], self.receipt["report_sha256"])
        self.assertEqual(result["outcome"], "no_selected_candidates")
        self.assertEqual(result["ranking"][0]["reported_metrics"]["net_dollars"], -0.04)

    def test_changed_report_rejected_against_original_receipt(self):
        raw, receipt = self.raw()
        self.report["ranking"][0]["r"] = 100
        with self.assertRaisesRegex(ValueError, "canonical-object"):
            self.call(json.dumps(self.report).encode(), receipt)

    def test_changed_receipt_rejected_against_external_pin(self):
        raw, receipt = self.raw()
        old_pin = hashlib.sha256(receipt).hexdigest()
        self.receipt["run_id"] = "tampered"
        _, changed = self.raw()
        with self.assertRaisesRegex(ValueError, "receipt hash"):
            self.call(raw, changed, expected_result_sha256=old_pin)

    def test_receipt_must_be_canonical(self):
        raw, receipt = self.raw()
        with self.assertRaisesRegex(ValueError, "not canonical"):
            self.call(raw, receipt + b"\n")

    def test_missing_availability_withheld(self):
        result = self.call(receipt_available_at=None)
        self.assertIn("missing_clock", result["summary"]["reasons"])

    def test_record_clock_not_file_clock(self):
        self.receipt["recorded_at"] = "2026-11-14T00:00:00+00:00"
        result = self.call()
        self.assertIn("stale_observation", result["summary"]["reasons"])
        self.assertEqual(result["clock_scope"], "result_record_observation_and_receipt_availability")

    def test_future_and_naive_record_time(self):
        self.receipt["recorded_at"] = "2026-11-16T00:00:00+00:00"
        self.assertIn("future_evidence", self.call()["summary"]["reasons"])
        self.receipt["recorded_at"] = "2026-11-15T00:00:00"
        with self.assertRaises(ValueError):
            self.call()

    def test_missing_record_clock_withheld(self):
        del self.receipt["recorded_at"]
        self.assertIn("missing_clock", self.call()["summary"]["reasons"])

    def test_run_frozen_and_state_mismatch(self):
        for key, value in (("run_id", "other"), ("frozen_sha256", "b" * 64),
                           ("kind", "factory_run_declared"), ("promotion_allowed", True)):
            self.report, self.receipt = fixture()
            self.receipt[key] = value
            with self.assertRaises(ValueError):
                self.call()

    def test_counts_and_duplicate_trial(self):
        for key in ("trial_count", "tested_count", "selected_count"):
            self.report, self.receipt = fixture()
            self.receipt[key] = True
            with self.assertRaises(ValueError):
                self.call()
        self.report, self.receipt = fixture()
        self.receipt["trials"] *= 2
        with self.assertRaises(ValueError):
            self.call()

    def test_ranked_metric_mismatch(self):
        self.receipt["trials"][0]["net_dollars"] = 100
        with self.assertRaisesRegex(ValueError, "ranked trial disagrees"):
            self.call()

    def test_omitted_selected_identity_reconciled(self):
        self.report.update(trial_count=2, tested=2, selected=["b"])
        other = {**self.receipt["trials"][0], "trial_id": "b", "selected": True}
        self.receipt["trials"].append(other)
        self.receipt.update(trial_count=2, tested_count=2, selected_count=1)
        result = self.call()
        self.assertEqual(result["selected_not_in_ranking"], ["b"])
        self.assertTrue(result["selected_ids_verified_against_result_receipt"])
        self.assertFalse(result["selected_ids_verified_against_discovery"])
        self.assertFalse(result["ledger_chain_verified"])
        self.assertFalse(result["frozen_and_ledger_verified"])
        self.assertFalse(result["promotion_allowed"])

    def test_phantom_selected_identity_rejected(self):
        self.report.update(trial_count=2, tested=2, selected=["phantom"])
        self.receipt["trials"].append({**self.receipt["trials"][0], "trial_id": "b"})
        self.receipt.update(trial_count=2, tested_count=2)
        with self.assertRaisesRegex(ValueError, "selection/tested"):
            self.call()

    def test_untested_selection_rejected(self):
        self.receipt["trials"][0].update(status="insufficient", selected=True)
        with self.assertRaisesRegex(ValueError, "untested"):
            self.call()

    def test_receipt_duplicate_keys_rejected(self):
        raw, receipt = self.raw()
        receipt = receipt[:-1] + b',"run_id":"synthetic"}'
        with self.assertRaisesRegex(ValueError, "duplicate"):
            self.call(raw, receipt)

    def test_all_untestable_preserved(self):
        self.report.update(tested=0, untestable=1, ranking=[])
        self.receipt.update(tested_count=0)
        self.receipt["trials"][0]["status"] = "insufficient"
        self.assertEqual(self.call()["outcome"], "no_testable_trials")

    def test_unranked_receipt_count_and_metrics_validated(self):
        for field, value in (("n", True), ("n", 10 ** 400), ("n", -1),
                             ("r", 10 ** 400), ("p", 2), ("t", True)):
            self.report, self.receipt = fixture()
            self.report.update(trial_count=2, tested=2, selected=["b"])
            other = {**self.receipt["trials"][0], "trial_id": "b", "selected": True}
            other[field] = value
            self.receipt["trials"].append(other)
            self.receipt.update(trial_count=2, tested_count=2, selected_count=1)
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    self.call()

    def test_no_input_mutation(self):
        raw, receipt = self.raw()
        before = copy.deepcopy((self.report, self.receipt))
        self.call(raw, receipt)
        self.assertEqual(before, (self.report, self.receipt))


if __name__ == "__main__":
    unittest.main()
