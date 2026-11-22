import hashlib
import unittest

from analysis.research_trackers.ercot_large_load import parse_monthly

HEADING = "Loads Approved to Energize - Observations\n"


def pair(month="September", value="4,132", approved="9,456"):
    return (
        f"• Of the {approved} MW that have received Approval to Energize, ERCOT has observed "
        f"a non-simultaneous monthly\npeak consumption of {value} MW in {month} 2026.\n"
    )


def meta(month="September", key="2026-09", release="2026-10-16", page=9, suffix=""):
    path = release.replace("-", "/")
    return dict(
        source_url=f"https://www.ercot.com/files/docs/{path}/ERCOT-Monthly-Operational-Overview-{month}{suffix}.pdf",
        released_on=release, first_seen_at="2026-11-22T00:00:00Z", report_month=key, page=page,
    )


class Monthly(unittest.TestCase):
    def test_three_vintages(self):
        for month, key, release, page, value, suffix in (
            ("July", "2026-07", "2026-08-17", 10, 4370, "-2026"),
            ("August", "2026-08", "2026-09-16", 9, 4316, "-2026"),
            ("September", "2026-09", "2026-10-16", 9, 4132, ""),
        ):
            with self.subTest(month=month):
                raw = ("Queue elsewhere: 466,500 MW\f" + HEADING + pair(month, f"{value:,}") + "\f").encode()
                row = parse_monthly(raw, **meta(month, key, release, page, suffix))
                self.assertEqual(row["approved_to_energize_mw"], 9456)
                self.assertEqual(row["observed_sum_of_individual_monthly_maxima_mw"], value)
                self.assertEqual(row["extracted_text_sha256"], hashlib.sha256(raw).hexdigest())
                self.assertEqual(row["lane"], "research_only")

    def test_wrong_section_or_conflicting_values_refused(self):
        for s in (
            pair(), HEADING, HEADING + "\f" + pair(), HEADING + pair() + pair(value="4,999"),
            HEADING + pair(value="41,32"), HEADING + pair(value="9,457"), HEADING + pair(month="July"),
        ):
            with self.subTest(s=s), self.assertRaises(ValueError):
                parse_monthly(s.encode(), **meta())

    def test_metadata_must_be_distinct_and_valid(self):
        raw = (HEADING + pair()).encode()
        for override in (
            dict(first_seen_at="2026-09-01T00:00:00Z"), dict(first_seen_at="2026-11-22T00:00:00"),
            dict(released_on="2026-10-17"), dict(source_url="https://example.com/ercot.pdf"), dict(page=True),
        ):
            with self.subTest(override=override), self.assertRaises(ValueError):
                parse_monthly(raw, **(meta() | override))

    def test_layout_public_label_and_page_footer(self):
        raw = ("other page\fPUBLIC   " + HEADING + pair()
               + "\nSum of the highest readings for individual loads.\n         9\n\f").encode()
        self.assertEqual(parse_monthly(raw, **meta())["source_page"], 9)
        with self.assertRaisesRegex(ValueError, "printed report page"):
            parse_monthly(raw, **meta(page=10))
