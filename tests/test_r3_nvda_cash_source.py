import copy
import json
import unittest
from pathlib import Path
from analysis.r3_nvda_cash_source import first_value, derive_q2, reconcile, extract

DATA = json.loads(
    (
        Path(__file__).resolve().parents[1] / "docs/research/r3-nvda/cash-reconciliation-companion-v1.json"
    ).read_text()
)


class AdapterTests(unittest.TestCase):
    def test_q1_reconciles(self):
        self.assertEqual(reconcile(DATA["facts"]["Q1"])["issuer_style_fcf"], 48554)

    def test_h1_reconciles(self):
        self.assertEqual(reconcile(DATA["facts"]["H1"])["issuer_style_fcf"], 69895)

    def test_q2_reconciles(self):
        self.assertEqual(
            reconcile(derive_q2(DATA["facts"]["Q1"], DATA["facts"]["H1"]))[
                "issuer_style_fcf"
            ],
            21341,
        )

    def test_signed(self):
        self.assertEqual(first_value("Other ( 94 ) ( 98 )", "Other"), -94)

    def test_ambiguous_label(self):
        with self.assertRaises(ValueError):
            first_value("Other long-term liabilities 1,217 350", "Other")

    def test_source_mutation(self):
        with self.assertRaises(ValueError):
            extract(b"changed", "Q1")

    def test_units(self):
        h = copy.deepcopy(DATA["facts"]["H1"])
        h["ar"]["unit"] = "billions"
        with self.assertRaises(ValueError):
            derive_q2(DATA["facts"]["Q1"], h)

    def test_period(self):
        h = copy.deepcopy(DATA["facts"]["H1"])
        h["ar"]["period_start"] = "2026-04-27"
        with self.assertRaises(ValueError):
            derive_q2(DATA["facts"]["Q1"], h)

    def test_residual(self):
        h = copy.deepcopy(DATA["facts"]["H1"])
        h["ar"]["value"] += 1
        with self.assertRaises(ValueError):
            reconcile(h)

    def test_provenance(self):
        for p in ["Q1", "H1"]:
            for x in DATA["facts"][p].values():
                self.assertEqual(len(x["source_sha256"]), 64)
                self.assertTrue(x["source_url"].startswith("https://www.sec.gov/"))
                self.assertTrue(x["row_text"])
                self.assertEqual(x["kind"], "observed")

    def test_q2_availability(self):
        for x in DATA["facts"]["Q2"].values():
            self.assertEqual(x["available_at"], "2026-08-26T16:36:00-04:00")
            self.assertEqual(len(x["inputs"]), 2)


if __name__ == "__main__":
    unittest.main()
