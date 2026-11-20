import unittest
from datetime import datetime, timedelta, timezone
from analysis.desk_input_qualification import qualify_tape, qualify_calendar

T = datetime(2026, 11, 13, 15, 0, tzinfo=timezone.utc)


class QualificationTests(unittest.TestCase):
    def tape(self, **changes):
        values = dict(source="synthetic", evidence_id="fixture-1", available_at=T,
                      decision_at=T, bar_start=T-timedelta(minutes=1), bar_end=T,
                      bid=100., ask=100.01, volume=0., volume_kind="interval",
                      max_age=timedelta(minutes=1))
        values.update(changes)
        return qualify_tape(**values)

    def calendar(self, **changes):
        values = dict(source="synthetic", evidence_id="schedule-1", available_at=T,
                      decision_at=T, coverage_start=T-timedelta(days=1),
                      coverage_end=T+timedelta(days=1), window_start=T,
                      window_end=T+timedelta(hours=1), coverage_complete=True,
                      version="v1", event_times=[], max_age=timedelta(hours=1))
        values.update(changes)
        return qualify_calendar(**values)

    def test_valid_tape_zero_volume(self):
        self.assertTrue(self.tape().eligible)

    def test_unavailable(self):
        for change in [dict(available_at=None), dict(available_at=T+timedelta(seconds=1))]:
            self.assertFalse(self.tape(**change).eligible)

    def test_incomplete_and_stale_bars(self):
        self.assertIn("bar_incomplete", self.tape(bar_end=T+timedelta(seconds=1)).reasons)
        self.assertIn("bar_stale", self.tape(bar_start=T-timedelta(minutes=4), bar_end=T-timedelta(minutes=2)).reasons)

    def test_capture_must_follow_bar(self):
        self.assertIn("capture_before_completion", self.tape(available_at=T-timedelta(seconds=1)).reasons)

    def test_volume_semantics(self):
        for kind in (None, "cumulative", "trade_count"):
            self.assertFalse(self.tape(volume_kind=kind).eligible)

    def test_numbers(self):
        for field in ("bid", "ask", "volume"):
            for value in (None, float("nan"), float("inf"), -1, True, 10**1000):
                self.assertFalse(self.tape(**{field: value}).eligible)
        self.assertFalse(self.tape(ask=99).eligible)

    def test_metadata(self):
        for field in ("source", "evidence_id"):
            self.assertFalse(self.tape(**{field: ""}).eligible)

    def test_naive_clocks(self):
        self.assertFalse(self.tape(bar_end=T.replace(tzinfo=None)).eligible)
        with self.assertRaises(ValueError):
            self.tape(decision_at=T.replace(tzinfo=None))

    def test_empty_calendar_needs_coverage(self):
        self.assertTrue(self.calendar().eligible)
        for value in (False, None, 1):
            self.assertFalse(self.calendar(coverage_complete=value).eligible)
        self.assertFalse(self.calendar(event_times=None).eligible)

    def test_schedule_can_name_future_event(self):
        self.assertTrue(self.calendar(event_times=[T+timedelta(minutes=30)]).eligible)

    def test_no_future_schedule_revision(self):
        self.assertFalse(self.calendar(available_at=T+timedelta(seconds=1)).eligible)

    def test_calendar_coverage_and_version(self):
        for change in [dict(coverage_end=T), dict(version=""), dict(coverage_start=None),
                       dict(window_end=T), dict(event_times=[T+timedelta(days=2)])]:
            self.assertFalse(self.calendar(**change).eligible)

    def test_stale_calendar(self):
        self.assertIn("calendar_stale", self.calendar(available_at=T-timedelta(hours=2)).reasons)

    def test_timezone_equivalence(self):
        self.assertTrue(self.calendar(available_at=T.astimezone(timezone(timedelta(hours=-5)))).eligible)

    def test_bad_policy(self):
        with self.assertRaises(ValueError):
            self.tape(max_age=timedelta(seconds=-1))


if __name__ == "__main__":
    unittest.main()
