from dataclasses import replace
from datetime import datetime, timedelta, timezone
import unittest
from ingestion.realtime.research_intervals import MinuteBar, Session, rollup, INTERVALS

T = datetime(2026, 11, 10, 14, 30, tzinfo=timezone.utc)
S = Session("NYSE:2026-11-10:RTH", T, T + timedelta(minutes=390), "fixture-calendar-v1")


def bars(n=390):
    return [
        MinuteBar(
            "SPY",
            "fixture",
            T + timedelta(minutes=i),
            T + timedelta(minutes=i + 1),
            100 + i,
            102 + i,
            99 + i,
            101 + i,
            10,
            str(i),
        )
        for i in range(n)
    ]


class TestResearchIntervals(unittest.TestCase):
    def test_all_intervals(self):
        for interval in INTERVALS:
            with self.subTest(interval=interval):
                out = rollup(bars(), S, interval, as_of=S.closes)
                self.assertEqual(sum(x.observed_minutes for x in out), 390)
                self.assertTrue(all(x.complete for x in out))
                self.assertEqual(sum(x.volume for x in out), 3900)

    def test_three_minute_ohlcv(self):
        x = rollup(bars(3), S, "3m", as_of=T + timedelta(minutes=3))[0]
        self.assertEqual(
            (x.open, x.high, x.low, x.close, x.volume), (100, 104, 99, 103, 30)
        )

    def test_no_future_window(self):
        self.assertEqual(rollup(bars(3), S, "3m", as_of=T + timedelta(minutes=2)), [])

    def test_future_receipt_is_missing(self):
        data = bars(3)
        data[1] = replace(data[1], available_at=T + timedelta(minutes=4))
        x = rollup(data, S, "3m", as_of=T + timedelta(minutes=3))[0]
        self.assertFalse(x.complete)
        self.assertIsNone(x.close)
        self.assertEqual(x.observed_minutes, 2)

    def test_gap_not_filled(self):
        x = rollup(bars(1) + bars(3)[2:], S, "3m", as_of=T + timedelta(minutes=3))[0]
        self.assertFalse(x.complete)
        self.assertIsNone(x.open)
        self.assertIsNone(x.volume)

    def test_missingness_not_backdated(self):
        assessed = T + timedelta(minutes=3)
        x = rollup(bars(1), S, "3m", as_of=assessed)[0]
        self.assertEqual(x.assessed_at, assessed)
        self.assertEqual(x.available_at, assessed)
        self.assertEqual(x.last_source_available_at, T + timedelta(minutes=1))

    def test_missing_volume_not_zero(self):
        data = bars(3)
        data[1] = replace(data[1], volume=None)
        x = rollup(data, S, "3m", as_of=T + timedelta(minutes=3))[0]
        self.assertTrue(x.complete)
        self.assertIsNone(x.volume)

    def test_early_close_final_window(self):
        s = replace(S, closes=T + timedelta(minutes=210))
        x = rollup(bars(210), s, "60m", as_of=s.closes)[-1]
        self.assertEqual(x.expected_minutes, 30)
        self.assertTrue(x.complete)

    def test_session_anchor(self):
        x = rollup(bars(60), S, "60m", as_of=T + timedelta(minutes=60))[0]
        self.assertEqual(x.start, T)
        self.assertEqual(x.end.hour, 15)
        self.assertEqual(x.end.minute, 30)

    def test_daily_is_session(self):
        x = rollup(bars(), S, "1d", as_of=S.closes)[0]
        self.assertEqual(x.start, T)
        self.assertEqual(x.end, S.closes)

    def test_duplicate_refuses(self):
        with self.assertRaises(ValueError):
            rollup(bars(3) + bars(1), S, "3m", as_of=S.closes)

    def test_mixed_source_refuses(self):
        with self.assertRaises(ValueError):
            rollup(
                bars(1) + [replace(bars(2)[1], source="other")], S, "3m", as_of=S.closes
            )

    def test_sampled_refuses(self):
        with self.assertRaises(ValueError):
            rollup(
                [replace(bars(1)[0], quality="receipt_sampled")],
                S,
                "3m",
                as_of=S.closes,
            )

    def test_naive_refuses(self):
        with self.assertRaises(ValueError):
            rollup(bars(1), S, "3m", as_of=datetime(2026, 11, 10))

    def test_unclosed_refuses(self):
        with self.assertRaises(ValueError):
            rollup([replace(bars(1)[0], available_at=T)], S, "3m", as_of=S.closes)

    def test_bad_price_refuses(self):
        with self.assertRaises(ValueError):
            rollup([replace(bars(1)[0], high=1)], S, "3m", as_of=S.closes)

    def test_empty_no_coverage(self):
        self.assertEqual(rollup([], S, "3m", as_of=S.closes), [])

    def test_unsupported_refuses(self):
        for label in ("1s", "tick", "1w", "1mo"):
            with self.assertRaises(ValueError):
                rollup(bars(1), S, label, as_of=S.closes)


if __name__ == "__main__":
    unittest.main()
