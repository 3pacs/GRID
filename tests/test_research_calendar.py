import unittest
from importlib.util import find_spec
from ingestion.realtime.research_calendar import XNYSCalendar


@unittest.skipUnless(
    find_spec("exchange_calendars"), "optional pinned calendar environment required"
)
class TestResearchCalendar(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.calendar = XNYSCalendar(start="2024-01-01", end="2027-12-31")

    def test_normal_and_dst(self):
        summer = self.calendar("XNYS:2026-07-06:RTH")
        winter = self.calendar("XNYS:2026-11-10:RTH")
        self.assertEqual(summer.opens.hour, 13)
        self.assertEqual(winter.opens.hour, 14)
        self.assertEqual((winter.closes - winter.opens).total_seconds(), 390 * 60)
        self.assertIn("4.11.2", winter.calendar_version)

    def test_early_close(self):
        session = self.calendar("XNYS:2026-11-27:RTH")
        self.assertEqual(session.closes.hour, 18)
        self.assertEqual((session.closes - session.opens).total_seconds(), 210 * 60)

    def test_holidays_and_weekend_refuse(self):
        for key in ("2026-11-26", "2026-07-03", "2026-11-08"):
            with self.assertRaises(ValueError):
                self.calendar(f"XNYS:{key}:RTH")

    def test_ad_hoc_closure(self):
        with self.assertRaises(ValueError):
            self.calendar("XNYS:2025-01-09:RTH")

    def test_calendar_to_retention_to_rollup(self):
        import tempfile
        from pathlib import Path
        from dataclasses import replace
        from datetime import timedelta
        from ingestion.realtime.minute_retention import (
            initialize,
            append,
            sqlite_loader,
        )
        from ingestion.realtime.research_intervals import rollup
        from tests.test_research_intervals import bars

        session = self.calendar("XNYS:2026-11-27:RTH")
        source = [
            replace(
                b,
                ts=session.opens + timedelta(minutes=i),
                available_at=session.opens + timedelta(minutes=i + 1),
            )
            for i, b in enumerate(bars(210))
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "research.db"
            initialize(path)
            append(path, source)
            rows = sqlite_loader(path)("SPY", "fixture", session, session.closes)
            out = rollup(rows, session, "60m", as_of=session.closes)
            self.assertEqual(len(out), 4)
            self.assertEqual(out[-1].expected_minutes, 30)
            self.assertTrue(all(b.complete for b in out))

    def test_bounds(self):
        with self.assertRaises(ValueError):
            self.calendar("XNYS:2023-01-03:RTH")

    def test_other_sessions_refuse(self):
        for key in ("CME:2026-11-10:RTH", "XNYS:2026-11-10:EXT", "2026-11-10"):
            with self.assertRaises(ValueError):
                self.calendar(key)


if __name__ == "__main__":
    unittest.main()
