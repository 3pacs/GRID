import json
import unittest
from analysis.research_trackers.usda_barge_rates import normalize

SEEN = "2026-11-22T04:50:00Z"


def fixture(*changes):
    row = dict(date="2026-11-17T00:00:00.000", week="46", month="11", year="2026",
               location="St. Louis", rate="529.7222")
    data = json.dumps([row | change for change in changes]).encode()
    return normalize(data, first_seen_at=SEEN)


class Barge(unittest.TestCase):
    def test_units_and_no_release_date_inference(self):
        batch = fixture({})
        self.assertIsNone(batch["upstream_release_at"])
        self.assertEqual(batch["records"][0]["rate_pct_of_route_tariff"], "529.7222")

    def test_keeps_unavailable_not_zero(self):
        result = fixture({"rate": None})["records"][0]
        self.assertEqual((result["availability"], result["rate_pct_of_route_tariff"]), ("unavailable", None))

    def test_duplicate_quarantines_entire_key(self):
        batch = fixture({}, {"rate": "700"})
        self.assertFalse(batch["records"])
        self.assertEqual(batch["quarantine"][0]["reason"], "duplicate_key")

    def test_malformed_and_zero_do_not_pass(self):
        for changes in ({"year": "2025"}, {"location": "Unknown"}, {"rate": "-12"}, {"rate": "0"},
                        {"date": "2026-11-24T00:00:00.000"}):
            with self.subTest(changes=changes):
                self.assertFalse(fixture(changes)["records"])
