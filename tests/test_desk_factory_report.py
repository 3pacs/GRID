"""Synthetic-only contract tests; never opens market data or research ledgers."""
import copy
import hashlib
import json
import unittest
from unittest.mock import patch
from datetime import datetime, timedelta, timezone

from analysis.desk_factory_report import factory_report_cards, MAX_REPORT_BYTES, MAX_COUNT


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
        self.assertFalse(result["selected_ids_verified_against_discovery"])
        self.assertEqual(result["selected_not_in_ranking_status"], "producer_claim_only")

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

    def test_oversized_integer_metric_normalized(self):
        self.rep["ranking"][0]["r"] = 10 ** 400
        with self.assertRaisesRegex(ValueError, "finite numeric"):
            self.adapt()

    def test_deep_json_normalized(self):
        raw = b"[" * 2000 + b"0" + b"]" * 2000
        with self.assertRaises(ValueError):
            self.adapt(raw=raw)
        with patch("analysis.desk_factory_report.json.loads", side_effect=RecursionError):
            with self.assertRaisesRegex(ValueError, "invalid report JSON"):
                self.adapt()

    def test_count_interoperability_bounds(self):
        for value in (MAX_COUNT + 1, 10 ** 400):
            for key in ("trial_count", "tested", "untestable", "n"):
                rep = fixture()
                if key == "n":
                    rep["ranking"][0][key] = value
                else:
                    rep[key] = value
                with self.subTest(key=key, value_bits=value.bit_length()):
                    with self.assertRaisesRegex(ValueError, "JSON-safe"):
                        self.adapt(rep)
        self.rep["ranking"][0]["n"] = MAX_COUNT
        self.assertEqual(self.adapt()["ranking"][0]["development_fires"], MAX_COUNT)
        self.assertEqual(int(float(MAX_COUNT)), MAX_COUNT)

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


    def test_cross_section_state_preserved_without_promotion(self):
        self.rep["state"] = "CROSS_SECTION_DEV_RANKING_ONLY"
        self.rep["target_unit"] = "net return per $1 long and $1 short per traded night (fraction)"
        self.rep["selected"] = ["trial-a"]
        self.rep["ranking"][0]["selected"] = True
        result = self.adapt()
        self.assertEqual(result["source_state"], self.rep["state"])
        self.assertEqual(result["target_unit"], self.rep["target_unit"])
        self.assertEqual(result["ranking"][0]["forward_samples"], 0)
        self.assertFalse(result["promotion_allowed"])
        self.assertEqual(result["outcome"], "development_candidates_only")

    def test_cross_section_negative_and_clocks_preserved(self):
        self.rep["state"] = "CROSS_SECTION_DEV_RANKING_ONLY"
        result = self.adapt(observed_at=None)
        self.assertEqual(result["outcome"], "no_selected_candidates")
        self.assertEqual(result["summary"]["quality"], "withheld")
        self.assertEqual(result["ranking"][0]["reported_metrics"]["net_dollars"], -0.04)

    def test_unadapted_coverage_explicit_without_copying_narrative(self):
        self.rep.update(state="CROSS_SECTION_DEV_RANKING_ONLY",
                        cards={"trial-a": {"note": "BUY NOW"}},
                        method={"selection": "max_t_sign_flip"}, coverage={})
        result = self.adapt()
        self.assertEqual(result["adapter_scope"], "ranking_summary_only")
        self.assertEqual(result["unadapted_report_fields"], ["cards", "coverage", "method"])
        self.assertNotIn("BUY NOW", json.dumps(result))
        self.assertNotIn("max_t_sign_flip", json.dumps(result))

    def test_cross_section_still_requires_explicit_development_state(self):
        for state in ("CROSS_SECTION_VALIDATED", "CROSS_SECTION", "", None, []):
            self.rep["state"] = state
            with self.subTest(state=state), self.assertRaises(ValueError):
                self.adapt()
        self.rep["state"] = "CROSS_SECTION_DEV_RANKING_ONLY"
        self.rep["promotion_allowed"] = True
        with self.assertRaises(ValueError):
            self.adapt()


if __name__ == "__main__":
    unittest.main()
