from dataclasses import replace
from datetime import datetime, timedelta, timezone
import unittest
from ingestion.realtime.research_intervals import Session, MinuteBar
from ingestion.realtime.native_volume import native_volume_view


class NativeVolumeTests(unittest.TestCase):
    def setUp(self):
        self.open = datetime(2026, 11, 10, 14, 30, tzinfo=timezone.utc)
        self.current = Session(
            "today", self.open, self.open + timedelta(minutes=390), "calendar:test"
        )
        self.prior = tuple(
            Session(
                str(i),
                self.open - timedelta(days=11 - i),
                self.open - timedelta(days=11 - i) + timedelta(minutes=390),
                "calendar:test",
            )
            for i in range(1, 11)
        )
        self.now = self.open + timedelta(minutes=5, seconds=6)
        self.rows = []
        for s in (*self.prior, self.current):
            n = 5 if s == self.current else 390
            for i in range(n):
                ts = s.opens + timedelta(minutes=i)
                self.rows.append(
                    MinuteBar(
                        "SPY",
                        "schwab_pricehistory",
                        ts,
                        self.now - timedelta(seconds=1),
                        2,
                        3,
                        1,
                        2,
                        10,
                        f"{s.key}:{i}",
                        price_basis="provider_unspecified",
                    )
                )

    def view(self, rows=None, **kw):
        args = dict(
            session=self.current,
            prior_sessions=self.prior,
            symbol="SPY",
            source="schwab_pricehistory",
            price_basis="provider_unspecified",
            as_of=self.now,
        )
        args.update(kw)
        return native_volume_view(self.rows if rows is None else rows, **args)

    def test_arithmetic(self):
        x = self.view()
        self.assertEqual(x["status"], "ready")
        self.assertEqual(x["rvol"], 1)
        self.assertEqual(x["bar_vwap_estimate"], 2)
        self.assertEqual(x["cumulative_shares"], 50)
        self.assertFalse(x["exact_trade_vwap"])
        self.assertFalse(x["signal_confirmation"])

    def test_current_gap(self):
        self.assertEqual(self.view(self.rows[:-1])["reason"], "current_session_gap")

    def test_baseline_gap(self):
        x = self.view(self.rows[1:])
        self.assertEqual(x["status"], "degraded")
        self.assertIsNone(x["rvol"])
        self.assertEqual(x["bar_vwap_estimate"], 2)

    def test_missing_session_refused(self):
        with self.assertRaises(ValueError):
            self.view(prior_sessions=self.prior[1:])

    def test_wrong_source(self):
        self.assertEqual(self.view(source="yahoo")["reason"], "current_session_gap")

    def test_future_receipt_excluded(self):
        self.assertEqual(
            self.view(
                [
                    replace(r, available_at=self.now + timedelta(seconds=1))
                    for r in self.rows
                ]
            )["reason"],
            "current_session_gap",
        )

    def test_stale_receipts(self):
        rows = [replace(r, available_at=r.ts + timedelta(minutes=1)) for r in self.rows]
        self.assertIn(
            self.view(rows, max_receipt_age=1)["reason"],
            ("receipt_stale", "event_stale"),
        )

    def test_backfill_no_event_freshness(self):
        self.assertEqual(
            self.view(as_of=self.now + timedelta(minutes=10))["reason"],
            "current_session_gap",
        )

    def test_early_close_baseline(self):
        prior = (
            replace(self.prior[0], closes=self.prior[0].opens + timedelta(minutes=3)),
            *self.prior[1:],
        )
        x = self.view(prior_sessions=prior)
        self.assertIsNone(x["rvol"])
        self.assertEqual(
            x["baseline_errors"]["1"], "early_close_insufficient_elapsed_minutes"
        )

    def test_current_early_close(self):
        x = self.view(
            session=replace(
                self.current, closes=self.current.opens + timedelta(minutes=3)
            ),
            max_receipt_age=180,
        )
        self.assertEqual(x["completed_minutes"], 3)

    def test_calendar_mismatch(self):
        with self.assertRaises(ValueError):
            self.view(
                prior_sessions=(
                    replace(self.prior[0], calendar_version="other"),
                    *self.prior[1:],
                )
            )

    def test_ambiguous_revision(self):
        with self.assertRaises(ValueError):
            self.view(
                self.rows + [replace(self.rows[-1], volume=20, revision="conflict")]
            )

    def test_zero_volume(self):
        x = self.view([replace(r, volume=0) for r in self.rows])
        self.assertIsNone(x["rvol"])
        self.assertIsNone(x["bar_vwap_estimate"])

    def test_zero_current_positive_baseline(self):
        rows = [replace(r, volume=0) if r.ts >= self.open else r for r in self.rows]
        result = self.view(rows)
        self.assertEqual(result["status"], "degraded")
        self.assertEqual(result["rvol"], 0)
        self.assertIsNone(result["bar_vwap_estimate"])

    def test_fractional_volume(self):
        with self.assertRaises(ValueError):
            self.view(self.rows[:-1] + [replace(self.rows[-1], volume=0.5)])

    def test_prior_event_fresh_download_refused(self):
        old = replace(self.current, closes=self.current.opens + timedelta(minutes=1))
        self.assertEqual(self.view(session=old)["reason"], "event_stale")

    def test_preopen(self):
        self.assertEqual(self.view(as_of=self.open)["reason"], "no_completed_minute")


if __name__ == "__main__":
    unittest.main()
