"""Synthetic cuts and a synthetic factory run only; never opens a research artifact."""
import copy
import hashlib
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from analysis import cross_section_factory as X
from analysis.desk_conditional_cards import conditional_factory_cards, render_conditional_cards
from tests.test_cross_section_factory import RUN, build, ledger_at, write_run


def cut(n=40, net=-2.5):
    return {"nights": n, "gross_bp": 1.5, "net_bp": net, "sd_bp": 10.0, "t_gross": 0.95}


def entry(dim, group, net, n=40):
    return {"cut": dim, "value": group, "nights": n, "net_bp": net, "t_gross": 0.95}


def paid_split():
    """What the factory's paid_split would list for fixture() if its floor were 40 nights."""
    return {"where_it_paid": [entry("market_phase", "rising", 4.0)],
            "where_it_did_not": [entry("market_phase", "falling", -9.0), entry("sector", "tech", -5.0),
                                 entry("year", "2020", -2.5)]}


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


class ConditionalTest(unittest.TestCase):
    def setUp(self):
        self.rep = fixture()
        self.now = datetime(2026, 11, 15, tzinfo=timezone.utc)

    def adapt(self, render=False, **kw):
        raw = json.dumps(self.rep).encode()
        args = dict(expected_sha256=hashlib.sha256(raw).hexdigest(),
                    receipt_id="synthetic", observed_at=self.now,
                    available_at=self.now, as_of=self.now, max_age=timedelta(days=1))
        args.update(kw)
        return (render_conditional_cards if render else conditional_factory_cards)(raw, **args)

    def test_losing_and_winning_cuts_preserved(self):
        result = self.adapt()
        self.assertEqual(result["cards"][0]["cuts"]["market_phase"], self.rep["cards"]["a"]["market_phase"])
        self.assertEqual(result["cards"][0]["overall"], cut(100))
        self.assertFalse(result["promotion_allowed"])
        self.assertEqual(result["base"]["outcome"], "no_selected_candidates")

    def test_small_sample_and_null_statistic_not_invented(self):
        self.rep["cards"]["a"]["overall"]["t_gross"] = None
        result = self.adapt()
        self.assertEqual(result["cards"][0]["cuts"]["year"]["2021"], {"nights": 20})
        self.assertIsNone(result["cards"][0]["overall"]["t_gross"])
        self.assertIn("not reported", self.adapt(render=True))

    def test_cost_units_and_provenance_retained(self):
        result = self.adapt()
        self.assertEqual(result["source_metadata"]["method"], self.rep["method"])
        self.assertEqual(result["source_metadata"]["coverage"], self.rep["coverage"])
        self.assertEqual(result["base"]["target_unit"], self.rep["target_unit"])
        self.assertEqual(result["execution_cost_measurement"], "not_verified")
        self.assertEqual(result["point_in_time_sector_validity"], "not_verified")
        self.assertIn("gross mean, not net mean", result["statistic_scope"])

    def test_missing_and_stale_clocks_stay_withheld(self):
        for kw in ({"observed_at": None}, {"observed_at": self.now - timedelta(days=3)}):
            result = self.adapt(**kw)
            self.assertEqual(result["cards"][0]["evidence"]["quality"], "withheld")
            self.assertIn("quality=withheld", self.adapt(render=True, **kw))

    def test_unknown_confirmation_and_forward_never_promoted(self):
        card = self.adapt()["cards"][0]
        self.assertEqual(card["confirmation_status"], "not_supplied")
        self.assertEqual(card["forward_status"], "not_supplied")
        self.assertFalse(card["promotion_allowed"])

    def test_counts_and_metrics_rejected(self):
        for key, value in (("nights", True), ("nights", 101), ("gross_bp", float("inf")),
                           ("net_bp", 10 ** 400), ("sd_bp", -1), ("t_gross", "1")):
            self.rep = fixture()
            self.rep["cards"]["a"]["year"]["2020"][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.adapt()

    def test_missing_metrics_and_fake_small_sample_stats_rejected(self):
        del self.rep["cards"]["a"]["overall"]["net_bp"]
        with self.assertRaises(ValueError):
            self.adapt()
        self.rep = fixture()
        self.rep["cards"]["a"]["year"]["2021"]["net_bp"] = 1
        with self.assertRaises(ValueError):
            self.adapt()

    def test_phantom_card_and_overall_count_rejected(self):
        self.rep["cards"]["phantom"] = self.rep["cards"].pop("a")
        with self.assertRaises(ValueError):
            self.adapt()
        self.rep = fixture()
        self.rep["cards"]["a"]["overall"]["nights"] = 99
        with self.assertRaises(ValueError):
            self.adapt()

    def test_unknown_dimension_rejected_not_silently_lost(self):
        self.rep["cards"]["a"]["unknown_regime"] = {"good": cut()}
        with self.assertRaises(ValueError):
            self.adapt()

    def test_paid_split_checked_against_cuts_and_not_carried(self):
        self.assertEqual(self.adapt()["cards"][0]["producer_paid_split"], "not_supplied")
        self.rep["cards"]["a"].update(paid_split())
        result = self.adapt()
        card = result["cards"][0]
        self.assertEqual(card["producer_paid_split"], "checked_against_cuts_not_carried")
        self.assertEqual(card["cuts"]["market_phase"], self.rep["cards"]["a"]["market_phase"])
        # The favorable cuts are never re-listed in size order, in the dict or the text.
        self.assertNotIn("where_it_paid", json.dumps(result))
        self.assertNotIn("where_it_did_not", json.dumps(result))
        self.assertNotIn("where_it", self.adapt(render=True))

    def test_paid_split_that_is_not_a_copy_of_a_cut_rejected(self):
        def net_changed(s): s["where_it_paid"][0]["net_bp"] = 4.5
        def nights_changed(s): s["where_it_did_not"][0]["nights"] = 39
        def t_changed(s): s["where_it_did_not"][0]["t_gross"] = None
        def missing_cut(s): s["where_it_paid"].append(entry("market_phase", "sideways", 1.0))
        def missing_dimension(s): s["where_it_paid"].append(entry("unknown_regime", "good", 1.0))
        def unscored_cut(s): s["where_it_paid"].append({**entry("year", "2021", 1.0), "nights": 20})
        def wrong_side(s): s["where_it_paid"].append(s["where_it_did_not"].pop(0))
        def listed_twice(s): s["where_it_did_not"].append(dict(s["where_it_did_not"][0]))
        def extra_field(s): s["where_it_paid"][0]["rank"] = 1
        def non_text_label(s): s["where_it_paid"][0]["value"] = ["rising"]
        def not_a_list(s): s["where_it_paid"] = {"0": s["where_it_paid"][0]}
        def one_side_only(s): del s["where_it_did_not"]
        for change in (net_changed, nights_changed, t_changed, missing_cut, missing_dimension, unscored_cut,
                       wrong_side, listed_twice, extra_field, non_text_label, not_a_list, one_side_only):
            self.rep = fixture()
            split = paid_split()
            change(split)
            self.rep["cards"]["a"].update(split)
            with self.subTest(change=change.__name__), self.assertRaises(ValueError):
                self.adapt()

    def test_missing_and_empty_cards_distinct(self):
        del self.rep["cards"]
        with self.assertRaises(ValueError):
            self.adapt()
        self.rep["cards"] = {}
        result = self.adapt()
        self.assertFalse(result["cards_complete_for_ranking"])
        self.assertEqual(result["ranked_without_cards"], ["a"])

    def test_render_includes_losses_all_strata_and_no_advice(self):
        text = self.adapt(render=True)
        self.assertIn("falling | 40 | 1.5 | -9.0", text)
        self.assertIn("2021 | 20 | not reported", text)
        self.assertIn("No promotion", text)
        self.assertIn("Confirmation and forward records: not supplied", text)

    def test_render_escapes_source_labels(self):
        self.rep["cards"]["a"]["sector"] = {"<script>|[link](x)\nnew": cut()}
        text = self.adapt(render=True)
        self.assertNotIn("<script>", text)
        self.assertNotIn("[link]", text)
        self.assertNotIn("(x)\nnew", text)

    def test_input_and_originals_not_mutated(self):
        before = copy.deepcopy(self.rep)
        result = self.adapt()
        result["cards"][0]["cuts"]["year"]["2020"]["net_bp"] = 999
        self.assertEqual(self.rep, before)
        self.assertEqual(self.adapt()["cards"][0]["cuts"]["year"]["2020"]["net_bp"], -2.5)

    def test_hash_rejects_tampering(self):
        with self.assertRaises(ValueError):
            self.adapt(expected_sha256="0" * 64)

    def test_metadata_bound(self):
        self.rep["method"] = {"cost": "x" * 2049}
        with self.assertRaises(ValueError):
            self.adapt()

    def test_intraday_cannot_be_relabelled(self):
        self.rep["state"] = "INTRADAY_DEV_RANKING_ONLY"
        with self.assertRaises(ValueError):
            self.adapt()


    def test_night_partition_gaps_explicit(self):
        coverage = self.adapt()["cards"][0]["partition_coverage"]
        self.assertEqual(coverage["market_phase"], {
            "status": "reported_partition_counts", "classified_nights": 80,
            "unclassified_nights": 20, "total_nights": 100})
        self.assertEqual(coverage["spike_state"]["unclassified_nights"], 100)
        self.assertEqual(coverage["fed_balance_sheet"], {"status": "not_supplied"})
        self.assertIn("unclassified_nights", self.adapt(render=True))

    def test_partition_overcount_rejected(self):
        self.rep["cards"]["a"]["year"]["2022"] = cut(60)
        with self.assertRaisesRegex(ValueError, "partition"):
            self.adapt()

    def test_all_optional_dimensions_preserved_without_overlap_sum(self):
        card = self.rep["cards"]["a"]
        for dim in ("fed_balance_sheet", "vix_third", "sector_phase", "group", "sector_vs_market"):
            card[dim] = {"one": cut(60), "two": cut(40)}
        # Sector groups can overlap in time: 160 memberships across100 nights.
        card["sector"] = {"tech": cut(80), "utilities": cut(80)}
        result = self.adapt()["cards"][0]
        for dim in ("fed_balance_sheet", "vix_third", "sector_phase", "group", "sector_vs_market", "sector"):
            self.assertEqual(result["cuts"][dim], card[dim])
        self.assertNotIn("sector", result["partition_coverage"])


class FactoryReportTest(unittest.TestCase):
    """The adapter reads the report the cross-section factory actually writes, so the two cannot drift apart."""

    @classmethod
    def setUpClass(cls):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            build(root)
            cls.report = X.run(write_run(root, RUN), root, root / "state.db", ledger_at(root), root / "out")
            cls.raw = (root / "out" / "report.json").read_bytes()
        cls.now = datetime.now(timezone.utc)

    def adapt(self, raw, render=False):
        return (render_conditional_cards if render else conditional_factory_cards)(
            raw, expected_sha256=hashlib.sha256(raw).hexdigest(), receipt_id="r1", observed_at=self.now,
            available_at=self.now, as_of=self.now, max_age=timedelta(days=1))

    def test_a_factory_report_is_adapted_with_every_cut_and_no_paid_ranking(self):
        result = self.adapt(self.raw)
        self.assertTrue(result["cards_complete_for_ranking"])
        self.assertEqual(len(result["cards"]), self.report["trial_count"])
        self.assertTrue(any(self.report["cards"][c["trial_id"]]["where_it_paid"] for c in result["cards"]))
        for card in result["cards"]:
            source = self.report["cards"][card["trial_id"]]
            self.assertEqual(card["producer_paid_split"], "checked_against_cuts_not_carried")
            self.assertEqual(card["overall"], source["overall"])
            self.assertEqual(card["cuts"], {k: v for k, v in source.items() if k in card["cuts"]})
            self.assertEqual(set(source) - set(card["cuts"]),
                             {"overall", "state_rows_matched", "where_it_paid", "where_it_did_not"})
        self.assertNotIn("where_it_paid", json.dumps(result))
        self.assertIn("# Development conditional outcomes", self.adapt(self.raw, render=True))

    def test_an_unknown_field_on_a_factory_card_is_still_refused(self):
        rep = json.loads(self.raw)
        next(iter(rep["cards"].values()))["unknown_regime"] = {"good": {"nights": 5}}
        with self.assertRaisesRegex(ValueError, "unknown conditional card fields"):
            self.adapt(json.dumps(rep, indent=2).encode())


if __name__ == "__main__":
    unittest.main()
