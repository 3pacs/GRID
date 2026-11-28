import json
import unittest
from analysis.research_trackers.nyfed_repo_pair import pair_rates


CLOCKS = dict(sofr_seen_at="2026-11-20T13:05:00Z", tgcr_seen_at="2026-11-20T13:07:00Z")


def payload(typ, day="2026-11-19", rate=3.91, revision="", footnote=None):
    return json.dumps({"refRates": [{"type": typ, "effectiveDate": day, "percentRate": rate,
                                   "revisionIndicator": revision, "footnoteId": footnote}]}).encode()


class Pairs(unittest.TestCase):
    def test_exact_decimal_pair_is_not_first_release(self):
        result = pair_rates(payload("SOFR"), payload("TGCR", rate=3.88), **CLOCKS)
        row = result["records"][0]
        self.assertEqual(row["sofr_minus_tgcr_bp"], "3.00")
        self.assertFalse(row["first_release_verified"])
        self.assertEqual(row["available_no_earlier_than"], "2026-11-20T13:07:00+00:00")

    def test_unmatched_never_fills(self):
        r = pair_rates(payload("SOFR"), payload("TGCR", day="2026-11-18"), **CLOCKS)
        self.assertEqual(r["records"], [])
        self.assertEqual(len(r["quarantine"]), 2)

    def test_flag_or_future_quarantines(self):
        for s,t in ((payload("SOFR", revision="Y"), payload("TGCR")),
                    (payload("SOFR"), payload("TGCR", footnote=2)),
                    (payload("SOFR", day="2026-11-20"), payload("TGCR", day="2026-11-20"))):
            with self.subTest(s=s,t=t):
                self.assertEqual(pair_rates(s,t, **CLOCKS)["records"], [])

    def test_invalid_schema(self):
        dup = json.loads(payload("SOFR"))["refRates"] * 2
        for row in (payload("TGCR"), payload("SOFR", rate=True), json.dumps({"refRates": dup}).encode()):
            with self.subTest(row=row), self.assertRaises(ValueError):
                pair_rates(row, payload("TGCR"), **CLOCKS)
