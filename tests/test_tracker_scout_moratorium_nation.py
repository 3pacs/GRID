import csv
import hashlib
import io
import unittest

from analysis.research_trackers.moratorium_nation import FIELDS, adapt

OPTIONS = dict(release_tag="v2026.11.11.post13", first_seen_at="2026-11-22T00:00:00-08:00")


def row(rid="al-fairfield-2026", state="AL", name="Fairfield", status="active", sectors='["data_center"]'):
    return (rid, state, name, "City", "2026-07-27", "exact", status, sectors, "",
            "Disclosed address 7201; ordinance references 7199 and 7160.\nSame parcel unverified.")


def fixture(*rows):
    f = io.StringIO(newline="")
    w = csv.writer(f)
    w.writerow((*FIELDS, "current_end_date_iso", "affected_projects"))
    w.writerows(rows)
    return f.getvalue().encode()


def adapt_fixture(data, **changes):
    return adapt(data, **{**OPTIONS, "expected_sha256": hashlib.sha256(data).hexdigest(), **changes})


class MoratoriumTests(unittest.TestCase):
    def test_fairfield_reported_active_is_not_a_verified_active_or_delay_claim(self):
        data = fixture(row())
        got = adapt_fixture(data)
        self.assertEqual(got, adapt_fixture(data))
        lead = got["records"][0]
        self.assertEqual(got["status"], "ok")
        self.assertEqual(lead["id"], "al-fairfield-2026")
        self.assertEqual(lead["jurisdiction"], {"state": "AL", "name": "Fairfield", "type": "City"})
        self.assertEqual(lead["evidence_class"], "research_lead")
        self.assertEqual(lead["reported_status"], "active")
        self.assertEqual(lead["reported_adoption_date"], "2026-07-27")
        for field in ("active_now", "operative_start", "observed_delay", "reported_end_date"):
            self.assertIsNone(lead[field])
        self.assertEqual(lead["project_applicability"], "unverified")
        self.assertEqual(lead["source"]["first_seen_at"], "2026-11-22T08:00:00Z")
        self.assertTrue(lead["source"]["url"].endswith("/v2026.11.11.post13/data/moratorium_inventory.csv"))
        self.assertEqual(lead["source"]["sha256"], hashlib.sha256(data).hexdigest())

    def test_two_scopes_do_not_become_one_affected_project(self):
        got = adapt_fixture(fixture(row("az-pima-2026", "AZ", "Pima County"), row("az-tucson-2026", "AZ", "Tucson")))
        self.assertEqual(len(got["records"]), 2)
        self.assertEqual([r["jurisdiction"]["name"] for r in got["records"]], ["Pima County", "Tucson"])
        self.assertTrue(all(r["project_applicability"] == "unverified" for r in got["records"]))

    def test_quarantines_entire_duplicate_set_and_malformed_row(self):
        data = fixture(row(), row(name="Different Fairfield"), row("tn-gallatin-2026", "TN", "Gallatin", "pending"),
                       row("az-pima-2026", "AZ", "Pima County", sectors="not-json"), row("bad id"))
        got = adapt_fixture(data)
        self.assertEqual(got["status"], "warn")
        self.assertEqual([r["id"] for r in got["records"]], ["tn-gallatin-2026"])
        self.assertEqual({x["defect"] for x in got["quarantine"]},
                         {"ambiguous_duplicate", "malformed_record", "invalid_identifier"})
        self.assertNotIn("evidence_class", got["quarantine"][0])
        self.assertEqual(got, adapt_fixture(data))

    def test_bad_hash_naive_clock_and_mutable_tag_refused(self):
        data = fixture(row())
        for changes in ({"expected_sha256": "0" * 64}, {"first_seen_at": "2026-11-22T00:00:00"},
                        {"release_tag": "main"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                adapt_fixture(data, **changes)

    def test_required_headers_and_csv_quote_errors_fail_closed(self):
        with self.assertRaises(ValueError):
            adapt_fixture(b"moratorium_id\nexample\n")
        with self.assertRaises(csv.Error):
            adapt_fixture(fixture() + b'"truncated')

    def test_non_iso_date_is_not_treated_as_adoption(self):
        bad = list(row())
        bad[FIELDS.index("date_enacted_iso")] = "20260727"
        got = adapt_fixture(fixture(bad))
        self.assertEqual((got["status"], got["records"]), ("warn", []))

    def test_month_and_year_are_retained_without_a_day(self):
        for value, precision in (("2026-08", "month"), ("2026", "year")):
            partial = list(row())
            partial[FIELDS.index("date_enacted_iso")] = value
            lead = adapt_fixture(fixture(partial))["records"][0]
            self.assertIsNone(lead["reported_adoption_date"])
            self.assertIsNone(lead["operative_start"])
            self.assertEqual((lead["reported_adoption_period"], lead["reported_adoption_precision"]),
                             (value, precision))
        partial[FIELDS.index("date_enacted_iso")] = "2026-13"
        self.assertEqual(adapt_fixture(fixture(partial))["status"], "warn")


if __name__ == "__main__":
    unittest.main()
