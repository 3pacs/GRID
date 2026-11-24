"""Pure fixture tests, independent of providers, reports and research outcomes."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext
from unittest import TestCase
from zoneinfo import ZoneInfo

from spy_vol_quote_diagnostics import Quote, diagnose_quote_mark

MARK = datetime(2099, 1, 12, 20, tzinfo=timezone.utc)
LEGS = ("short_call", "short_put", "long_call", "long_put")
GEN = "fixture-generation"


def quote(leg, *, age=15, known_age=10, bid="1.00", ask="1.10", generation=GEN):
    return Quote(leg, generation, MARK - timedelta(seconds=age), MARK - timedelta(seconds=known_age),
                 Decimal(bid) if bid is not None else None, Decimal(ask) if ask is not None else None)


class TestQuoteMark(TestCase):
    def check(self, rows):
        return diagnose_quote_mark(LEGS, rows, mark=MARK, generation=GEN)

    def setUp(self):
        self.rows = [quote(leg) for leg in LEGS]

    def test_four_quotes_never_executable_including_exact_cutoffs(self):
        rows = [quote(LEGS[0], age=60, known_age=0, bid="0.10", ask="0.30")] + self.rows[1:]
        result = self.check(rows)
        self.assertEqual(result["status"], "FOUR_QUOTES_QUALIFIED")
        self.assertEqual(result["execution"], "NOT_ESTABLISHED")

    def test_late_revision_cannot_change_earlier_mark(self):
        late = quote(LEGS[0], age=2, known_age=-1, ask="0.50")
        self.assertEqual(self.check([late, *self.rows]), self.check(self.rows))

    def test_stale_leg_cannot_borrow_other_generation(self):
        stale = quote(LEGS[0], age=61)
        fresh = quote(LEGS[0], generation="other")
        result = self.check([stale, fresh, *self.rows[1:]])
        self.assertEqual(result["reasons"], {LEGS[0]: "stale_quote"})
        self.assertEqual(result["quotes"], ())

    def test_known_latest_invalid_never_borrows_older_market(self):
        for bid, ask in (("1.01", "0.99"), ("0", "1"), ("NaN", "1"), (None, "1")):
            with self.subTest(bid=bid, ask=ask):
                latest = quote(LEGS[0], age=4, known_age=1, bid=bid, ask=ask)
                self.assertEqual(self.check([latest, *self.rows])["reasons"], {LEGS[0]: "invalid_market"})

    def test_spread_uses_larger_of_fixed_and_relative_threshold(self):
        threshold = quote(LEGS[0], bid="0.75", ask="1.25")
        wide = replace(threshold, ask=Decimal("1.2501"))
        self.assertEqual(self.check([threshold, *self.rows[1:]])["reasons"], {})
        self.assertEqual(self.check([wide, *self.rows[1:]])["reasons"], {LEGS[0]: "wide_spread"})

    def test_missing_leg_and_future_available_leg_do_not_fill(self):
        only_late = quote(LEGS[0], age=5, known_age=-1)
        self.assertEqual(self.check([only_late, *self.rows[2:]])["reasons"],
                         {leg: "missing_at_mark" for leg in LEGS[:2]})

    def test_tied_conflicting_update_order_invariant(self):
        disagree = replace(self.rows[0], ask=Decimal("1.11"))
        for rows in ([disagree, *self.rows], [*self.rows, disagree]):
            self.assertEqual(self.check(rows)["reasons"], {LEGS[0]: "conflicting_latest_update"})
        self.assertEqual(len(self.check([*self.rows, self.rows[0]])["quotes"]), 4)

    def test_inverted_or_unknown_clock_not_masked_by_good_row(self):
        for row in (quote(LEGS[0], age=4, known_age=8),
                    replace(self.rows[0], available_at=MARK.replace(tzinfo=None))):
            self.assertEqual(self.check([row, *self.rows])["reasons"], {LEGS[0]: "unqualified_clock"})

    def test_offset_fold_is_checked_by_instant_not_wall_clock(self):
        et = ZoneInfo("America/New_York")
        mark = datetime(2026, 11, 1, 1, 30, 0, tzinfo=et, fold=1)
        earlier_fold = mark.replace(fold=0)
        rows = [Quote(leg, GEN, earlier_fold, earlier_fold, Decimal("1"), Decimal("1.1")) for leg in LEGS]
        result = diagnose_quote_mark(LEGS, rows, mark=mark, generation=GEN)
        self.assertEqual(result["reasons"], dict.fromkeys(LEGS, "stale_quote"))

    def test_locked_positive_market_is_allowed(self):
        locked = quote(LEGS[0], bid="1", ask="1")
        result = self.check([locked, *self.rows[1:]])
        self.assertEqual(result["status"], "FOUR_QUOTES_QUALIFIED")

    def test_later_available_same_observation_conflict_is_not_a_revision_order(self):
        delayed = replace(self.rows[0], ask=Decimal("1.11"), available_at=MARK)
        for rows in ([delayed, *self.rows], [*self.rows, delayed]):
            self.assertEqual(self.check(rows)["reasons"], {LEGS[0]: "conflicting_latest_update"})

    def test_same_observation_and_prices_preserve_earliest_availability(self):
        delayed = replace(self.rows[0], available_at=MARK)
        for rows in ([delayed, *self.rows], [*self.rows, delayed]):
            self.assertEqual(self.check(rows)["quotes"][0], self.rows[0])

    def test_signaling_nan_duplicate_and_extreme_decimals_are_refused(self):
        for bid in ("sNaN", "1e999999", "1e-999999"):
            bad = quote(LEGS[0], age=4, known_age=1, bid=bid)
            self.assertEqual(self.check([bad, *self.rows, bad])["reasons"], {LEGS[0]: "invalid_market"})

    def test_spread_calculation_does_not_use_ambient_decimal_precision(self):
        wide = quote(LEGS[0], bid="0.75", ask="1.2501")
        with localcontext() as ctx:
            ctx.prec = 1
            self.assertEqual(self.check([wide, *self.rows[1:]])["reasons"], {LEGS[0]: "wide_spread"})
