"""Synthetic-only contract tests; never opens market data or research ledgers."""
import copy
import hashlib
import json
import unittest
from datetime import datetime, timedelta, timezone

from analysis.desk_factory_report import factory_report_cards, MAX_REPORT_BYTES


def fixture():
    return {
        "run_id": "synthetic-run",
        "state": "INTRADAY_DEV_RANKING_ONLY", "promotion_allowed": False,
        "frozen_sha256": "a" * 64, "target_unit": "bp",
        "trial_count": 2, "tested": 1, "untestable": 1,
        "selected": [],
        "ranking": [{
            "trial_id": "trial-a", "family": "synthetic", "feature": "F1",
            "n": 1000, "r": -1.5, "net_dollars": -0.04, "t": -2.0,
            "p": 0.9, "adjusted_p": 1.0, "selected": False,
        }],
    }


class FactoryDeskTest(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 11, 15, tzinfo=timezone.utc)
        self.rep = fixture()

    def adapt(self, rep=None, raw=None, **kwargs):
        if raw is None:
            raw = json.dumps(self.rep if rep is None else rep).encode()
        args = dict(expected_sha256=hashlib.sha256(raw).hexdigest(),
                    receipt_id="synthetic-receipt", observed_at=self.now,
                    available_at=self.now, as_of=self.now,
                    max_age=timedelta(days=1))
        args.update(kwargs)
        return factory_report_cards(raw, **args)

    def test_negative_result_preserved(self):
        result = self.adapt()
        self.assertEqual(result["outcome"], "no_selected_candidates")
        self.assertEqual(result["ranking"][0]["reported_metrics"]["net_dollars"], -0.04)
        self.assertEqual(result["selected_count"], 0)
        self.assertEqual(result["untestable_trials"], 1)

    def test_development_fires_never_forward_samples(self):
        result = self.adapt()
        row = result["ranking"][0]
        self.assertEqual(row["development_fires"], 1000)
        self.assertEqual(row["evidence"]["samples"], 0)
        self.assertEqual(row["forward_samples"], 0)
        self.assertIn("not forward evidence", row["evidence"]["label"])
        self.assertFalse(result["promotion_allowed"])

    def test_empty_run_and_all_untestable(self):
        for untestable in (0, 2):
            self.rep.update(trial_count=untestable, tested=0,
                            untestable=untestable, ranking=[])
            self.assertEqual(self.adapt()["outcome"], "no_testable_trials")

    def test_candidate_never_promoted(self):
        self.rep["selected"] = ["trial-a"]
        self.rep["ranking"][0]["selected"] = True
        result = self.adapt()
        self.assertEqual(result["outcome"], "development_candidates_only")
        self.assertFalse(result["promotion_allowed"])
        self.assertFalse(result["ranking"][0]["evidence"]["promotion_allowed"])

    def test_truncated_ranking_is_not_all_candidates(self):
        self.rep.update(trial_count=3, tested=2, selected=["outside-top"])
        result = self.adapt()
        self.assertFalse(result["ranking_complete"])
        self.assertEqual(result["selected_not_in_ranking"], ["outside-top"])

    def test_complete_ranking_cannot_miss_selected(self):
        self.rep["selected"] = ["outside-top"]
        with self.assertRaises(ValueError):
            self.adapt()

    def test_hash_mismatch(self):
        with self.assertRaisesRegex(ValueError, "hash mismatch"):
            self.adapt(expected_sha256="b" * 64)

    def test_hash_is_not_frozen_verification_or_data_freshness(self):
        result = self.adapt()
        self.assertFalse(result["frozen_and_ledger_verified"])
        self.assertEqual(result["source_market_data_freshness"], "not_assessed")

    def test_clocks_withhold(self):
        cases = [
            ({"observed_at": None}, "missing_clock"),
            ({"available_at": None}, "missing_clock"),
            ({"available_at": self.now + timedelta(seconds=1)}, "future_evidence"),
            ({"observed_at": self.now - timedelta(days=2)}, "stale_observation"),
            ({"available_at": self.now - timedelta(seconds=1)}, "availability_before_observation"),
        ]
        for args, reason in cases:
            with self.subTest(reason=reason):
                result = self.adapt(**args)
                self.assertEqual(result["summary"]["quality"], "withheld")
                self.assertIn(reason, result["summary"]["reasons"])
                self.assertIn(reason, result["ranking"][0]["evidence"]["reasons"])

    def test_naive_clock_rejected(self):
        with self.assertRaises(ValueError):
            self.adapt(observed_at=datetime(2026, 11, 15))

    def test_bad_states(self):
        for key, value in (("state", "VALIDATED"), ("promotion_allowed", True),
                           ("promotion_allowed", 0)):
            rep = fixture()
            rep[key] = value
            with self.assertRaises(ValueError):
                self.adapt(rep)

    def test_bad_counts(self):
        for key in ("trial_count", "tested", "untestable"):
            for value in (-1, True, 1.5):
                rep = fixture()
                rep[key] = value
                with self.assertRaises(ValueError):
                    self.adapt(rep)
        self.rep["trial_count"] = 9
        with self.assertRaises(ValueError):
            self.adapt()

    def test_duplicate_ids_and_selection_mismatch(self):
        for selected in (["trial-a", "trial-a"], ["trial-a"]):
            self.rep["selected"] = selected
            with self.assertRaises(ValueError):
                self.adapt()
        self.rep = fixture()
        self.rep.update(trial_count=3, tested=2)
        self.rep["ranking"] *= 2
        with self.assertRaises(ValueError):
            self.adapt()

    def test_malformed_json_and_duplicate_keys(self):
        for raw in (b"[]", b"null", b"{", b'{"x":1,"x":2}', b'{"x":NaN}'):
            with self.assertRaises(ValueError):
                self.adapt(raw=raw)

    def test_size_bound(self):
        for raw in (b"", b" " * (MAX_REPORT_BYTES + 1)):
            with self.assertRaises(ValueError):
                self.adapt(raw=raw)

    def test_bad_metrics(self):
        for key in ("r", "net_dollars", "t", "p", "adjusted_p"):
            for value in (None, True, "0", float("inf"), float("nan")):
                rep = fixture()
                rep["ranking"][0][key] = value
                with self.assertRaises(ValueError):
                    self.adapt(rep)
        for value in (-0.1, 1.1):
            self.rep["ranking"][0]["adjusted_p"] = value
            with self.assertRaises(ValueError):
                self.adapt()

    def test_bounded_identity(self):
        for value in ("", " " * 3, "x" * 513, 9):
            with self.assertRaises(ValueError):
                self.adapt(receipt_id=value)

    def test_no_mutation_and_deterministic(self):
        original = copy.deepcopy(self.rep)
        self.assertEqual(self.adapt(), self.adapt())
        self.assertEqual(self.rep, original)

    def test_unknown_narrative_not_propagated(self):
        self.rep["note"] = "BUY NOW"
        self.rep["ranking"][0]["advice"] = "BUY"
        self.assertNotIn("BUY", json.dumps(self.adapt()))


if __name__ == "__main__":
    unittest.main()
