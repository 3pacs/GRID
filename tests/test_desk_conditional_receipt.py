"""Synthetic receipt-to-conditional seam; no real ledgers or holdouts."""
import copy
import hashlib
import json
import unittest
from datetime import datetime, timedelta, timezone

from analysis.desk_conditional_receipt import receipt_bound_conditional_cards

def cut(n=40, net=-2.5):
    return {"nights": n, "gross_bp": 1.5, "net_bp": net, "sd_bp": 10.0, "t_gross": 0.95}


def fixture():
    return {
        "run_id": "synthetic", "state": "CROSS_SECTION_DEV_RANKING_ONLY",
        "promotion_allowed": False, "frozen_sha256": "a" * 64,
        "target_unit": "net return per $1 long and $1 short per traded night (fraction)",
        "trial_count": 1, "tested": 1, "untestable": 0, "selected": [],
        "ranking": [{"trial_id": "a", "family": "xs", "feature": "synthetic",
                     "n": 100, "r": -0.1, "net_dollars": -0.00025, "t": -1.0,
                     "p": 0.9, "adjusted_p": 1.0, "selected": False}],
        "method": {"cost_bp_per_side": 1.0, "sides_per_night": {"overnight": 4}},
        "coverage": {"sector_assignment": {"historic": "return_correlation"}},
        "cards": {"a": {
            "overall": cut(100), "state_rows_matched": 200,
            "year": {"2020": cut(), "2021": {"nights": 20}},
            "market_phase": {"rising": cut(net=4.0), "falling": cut(net=-9.0)},
            "sector": {"tech": cut(net=-5.0)}, "spike_state": {},
        }},
    }




class ConditionalReceiptTest(unittest.TestCase):
    def setUp(self):
        self.report = fixture()
        self.receipt = {
            "kind": "factory_run_result", "promotion_allowed": False,
            "run_id": "synthetic", "frozen_sha256": "a" * 64,
            "recorded_at": "2026-11-15T00:00:00+00:00",
            "trial_count": 1, "tested_count": 1, "selected_count": 0,
            "trials": [{**self.report["ranking"][0], "status": "tested"}],
        }
        self.now = datetime(2026, 11, 15, tzinfo=timezone.utc)

    def raw(self):
        self.receipt["report_sha256"] = hashlib.sha256(
            json.dumps(self.report, sort_keys=True, allow_nan=False).encode()).hexdigest()
        return (json.dumps(self.report, indent=2).encode(),
                json.dumps(self.receipt, sort_keys=True, separators=(",", ":")).encode())

    def call(self, raw=None, **kw):
        report, receipt = raw or self.raw()
        args = dict(expected_result_sha256=hashlib.sha256(receipt).hexdigest(),
                    receipt_available_at=self.now, as_of=self.now, max_age=timedelta(days=1))
        args.update(kw)
        return receipt_bound_conditional_cards(report, receipt, **args)

    def test_preserves_cuts_and_receipt_provenance(self):
        result = self.call()
        self.assertEqual(result["cards"][0]["cuts"]["market_phase"],
                         self.report["cards"]["a"]["market_phase"])
        self.assertTrue(result["base"]["selected_ids_verified_against_result_receipt"])
        self.assertFalse(result["base"]["ledger_chain_verified"])
        self.assertEqual(result["base"]["integrity"],
                         "report_canonical_object_matches_pinned_result_receipt")
        self.assertFalse(result["promotion_allowed"])
        self.assertEqual(result["cards"][0]["confirmation_status"], "not_supplied")
        self.assertEqual(result["cards"][0]["forward_status"], "not_supplied")

    def test_changed_cut_fails_receipt_binding(self):
        raw, receipt = self.raw()
        self.report["cards"]["a"]["overall"]["net_bp"] = 123
        with self.assertRaisesRegex(ValueError, "canonical-object"):
            self.call((json.dumps(self.report).encode(), receipt))

    def test_wrong_anchor_rejects(self):
        with self.assertRaisesRegex(ValueError, "receipt hash"):
            self.call(expected_result_sha256="0" * 64)

    def test_missing_stale_future_clocks_withheld_everywhere(self):
        cases = [(None, self.now), ("2026-11-12T00:00:00+00:00", self.now),
                 ("2026-11-16T00:00:00+00:00", self.now),
                 ("2026-11-15T00:00:00+00:00", None)]
        for observed, available in cases:
            with self.subTest(observed=observed, available=available):
                self.receipt["recorded_at"] = observed
                result = self.call(receipt_available_at=available)
                self.assertEqual(result["base"]["summary"]["quality"], "withheld")
                self.assertEqual(result["cards"][0]["evidence"]["quality"], "withheld")

    def test_receipt_trial_mismatch_rejects(self):
        self.receipt["trials"][0]["net_dollars"] = 12
        with self.assertRaisesRegex(ValueError, "ranked trial disagrees"):
            self.call()

    def test_valid_receipt_does_not_bypass_conditional_checks(self):
        self.report["cards"]["a"]["overall"]["nights"] = 99
        with self.assertRaisesRegex(ValueError, "overall nights"):
            self.call()

    def test_unranked_selected_identity_retains_verified_status(self):
        self.report.update(trial_count=2, tested=2, selected=["b"])
        self.receipt.update(trial_count=2, tested_count=2, selected_count=1)
        self.receipt["trials"].append(
            {**self.receipt["trials"][0], "trial_id": "b", "selected": True})
        result = self.call()
        self.assertEqual(result["base"]["selected_not_in_ranking"], ["b"])
        self.assertTrue(result["base"]["selected_ids_verified_against_result_receipt"])
        self.assertFalse(result["base"]["selected_ids_verified_against_discovery"])

    def test_no_caller_clock_override(self):
        with self.assertRaises(TypeError):
            self.call(observed_at=self.now)

    def test_originals_and_outputs_independent(self):
        raw = self.raw()
        before = copy.deepcopy((self.report, self.receipt))
        result = self.call(raw)
        result["cards"][0]["overall"]["net_bp"] = 999
        self.assertEqual(before, (self.report, self.receipt))
        self.assertEqual(self.call(raw)["cards"][0]["overall"]["net_bp"], -2.5)

if __name__ == "__main__":
    unittest.main()
