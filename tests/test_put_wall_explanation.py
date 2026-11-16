"""Presentation regression: no network, persistence or sending."""
import unittest

from intelligence.forced_flow_monitor import GammaRegimeSnapshot, scan_thresholds
from alerts.waterfall_watch import _format_threshold_line


class PutWallExplanationTest(unittest.TestCase):
    def threshold(self, spot, wall):
        regime = GammaRegimeSnapshot(
            regime="SHORT_GAMMA", spot=spot, gamma_flip=102.0,
            put_wall=wall, call_wall=110.0, aggregate_gex=-1.0,
            snapshot_date="2026-11-13",
        )
        return next(x for x in scan_thresholds(regime, []) if x.name == "put_wall_proximity")

    def test_direction_and_distance_boundaries_unchanged(self):
        for spot, wall, expected, distance in (
            (100.0, 99.0, False, 1.0),
            (100.0, 100.0, False, 0.0),
            (100.0, 101.0, True, 1.0),
            (100.0, 102.0, True, 2.0),
            (100.0, 103.0, False, 3.0),
            (100.0, None, False, None),
            (0.0, 100.0, False, None),
        ):
            with self.subTest(spot=spot, wall=wall):
                th = self.threshold(spot, wall)
                self.assertIs(th.tripped, expected)
                self.assertEqual(th.current_value, distance)
                self.assertEqual(th.threshold_value, 2.0)

    def test_both_predicates_and_supplied_inputs_visible(self):
        th = self.threshold(100.0, 99.0)
        self.assertIn("strictly below", th.description)
        self.assertIn("AND absolute distance <= 2.0%", th.description)
        self.assertIn("spot=100.0, put_wall=99.0", th.description)
        self.assertIn("proximity alone does not trip", th.description)
        self.assertNotIn("break = short-gamma", th.description)

    def test_render_retains_not_tripped_with_explanation(self):
        line = _format_threshold_line(self.threshold(100.0, 99.0).to_dict(), "SHORT_GAMMA")
        self.assertIn("NOT TRIPPED", line)
        self.assertIn("strictly below", line)
        self.assertIn("current=1.00, threshold=2.00", line)
        self.assertIn("put_wall=99.0", line)
        self.assertIn("&lt;=", line)

    def test_missing_wall_is_not_fabricated(self):
        th = self.threshold(100.0, None)
        self.assertIn("put_wall=None", th.description)
        self.assertIn("MISSING", _format_threshold_line(th.to_dict(), "SHORT_GAMMA"))

    def test_rounded_email_case_synthetic_not_claimed_actual(self):
        # Synthetic wall reproduces rounded email values, not the actual receipt.
        th = self.threshold(757.12, 750.0)
        self.assertEqual(f"{th.current_value:.2f}", "0.94")
        self.assertFalse(th.tripped)
        self.assertIn("put_wall=750.0", th.description)


if __name__ == "__main__":
    unittest.main()
