"""Synthetic metadata only; no holdout/forward file or real registry access."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest import TestCase

from research_receipt_conformance import (
    FrozenRun, Opportunity, OpportunityReceipt, RunReceipt, check_frozen_run, reconcile_opportunities,
)

R = datetime(2099, 1, 20, 12, tzinfo=UTC)
HOUR = timedelta(hours=1)
M, A, B = "a" * 64, "b" * 64, "c" * 64


class TestFrozenRun(TestCase):
    def setUp(self):
        self.pin = FrozenRun("fixture-trial", M, 100, A, R)
        self.original = RunReceipt("fixture-trial", M, 100, A, R)

    def test_nonmatching_corrections_never_replace_pin_even_if_visible_in_time(self):
        for seen in (R - HOUR, R + HOUR):
            successor = RunReceipt("fixture-trial", M, 101, B, seen)
            self.assertEqual(check_frozen_run(self.pin, [successor]), "HOLD_NO_FROZEN_RUN_RECEIPT")
            self.assertEqual(check_frozen_run(self.pin, [successor, self.original]), "FIXTURE_IDENTITY_MATCH")

    def test_matching_hash_manifest_and_run_belonging_to_other_trial_hold(self):
        other = replace(self.original, trial_id="different-fixture-trial")
        self.assertEqual(check_frozen_run(self.pin, [other]), "HOLD_NO_FROZEN_RUN_RECEIPT")
        self.assertEqual(check_frozen_run(self.pin, [other, self.original]), "FIXTURE_IDENTITY_MATCH")

    def test_same_id_different_hash_or_manifest_and_late_exact_receipt_hold(self):
        mismatches = [
            replace(self.original, review_sha256=B),
            replace(self.original, manifest_sha256=B),
            replace(self.original, witnessed_at=R + HOUR),
        ]
        self.assertEqual(check_frozen_run(self.pin, mismatches), "HOLD_CONFLICTING_RUN_RECEIPT")

    def test_timely_contradiction_cannot_hide_behind_a_match(self):
        for field in ("review_sha256", "manifest_sha256"):
            conflict = replace(self.original, **{field: B})
            for rows in ([conflict, self.original], [self.original, conflict]):
                self.assertEqual(check_frozen_run(self.pin, rows), "HOLD_CONFLICTING_RUN_RECEIPT")

    def test_future_contradiction_does_not_rewrite_frozen_match(self):
        future = replace(self.original, review_sha256=B, witnessed_at=R + HOUR)
        self.assertEqual(check_frozen_run(self.pin, [future, self.original, self.original]),
                         "FIXTURE_IDENTITY_MATCH")


class TestOpportunityCoverage(TestCase):
    def setUp(self):
        self.roster = [Opportunity("opp1", R + HOUR), Opportunity("opp2", R + 2 * HOUR),
                       Opportunity("opp3", R + 3 * HOUR)]
        self.rows = [
            OpportunityReceipt(item.opportunity_id, M, state, digest,
                               item.decision_cutoff - HOUR / 2, item.decision_cutoff)
            for item, state, digest in zip(self.roster, ("DECISION", "NONTRADE", "UNAVAILABLE"), (M, A, B))
        ]

    def reconcile(self, rows, audit=R + 4 * HOUR):
        return reconcile_opportunities(self.roster, rows, manifest_sha256=M, frozen_at=R, audit_at=audit)

    def test_all_dispositions_count_once_and_exact_cutoff_qualifies(self):
        result = self.reconcile(self.rows + self.rows[::-1])
        self.assertEqual(result["status"], "FIXTURE_RECONCILED")
        self.assertEqual(result["opportunities"], dict(zip(
            ("opp1", "opp2", "opp3"), ("DECISION", "NONTRADE", "UNAVAILABLE"),
        )))

    def test_unavailable_and_nontrade_are_not_missing(self):
        value = self.reconcile(self.rows[1:])
        self.assertEqual(value["status"], "HOLD_INCOMPLETE")
        self.assertEqual(value["opportunities"], {"opp1": "MISSING", "opp2": "NONTRADE", "opp3": "UNAVAILABLE"})

    def test_captured_in_time_but_witnessed_after_cutoff_cannot_backfill(self):
        late = replace(self.rows[0], witnessed_at=R + 2 * HOUR)
        res = self.reconcile([late, *self.rows[1:]])
        self.assertEqual(res["opportunities"]["opp1"], "MISSING")
        self.assertEqual(res["ignored"], {"not_visible_by_decision_cutoff": 1})

    def test_future_correction_cannot_change_as_of_report(self):
        audit = R + HOUR
        future = replace(self.rows[1], opportunity_id="unknown", manifest_sha256=A)
        before = self.reconcile(self.rows[:1], audit)
        self.assertEqual(before, self.reconcile([future, *self.rows], audit))
        self.assertEqual(before["opportunities"]["opp2"], "PENDING")

    def test_conflicting_receipts_never_choose_a_winner(self):
        other = replace(self.rows[0], disposition="NONTRADE", receipt_sha256=A)
        for rows in ([other, *self.rows], [*self.rows, other]):
            self.assertEqual(self.reconcile(rows)["opportunities"]["opp1"], "CONFLICT")

    def test_wrong_manifest_does_not_resolve_opportunity(self):
        other = replace(self.rows[0], manifest_sha256=A)
        self.assertEqual(self.reconcile([other, *self.rows[1:]])["opportunities"]["opp1"], "MISSING")

    def test_witness_redelivery_does_not_duplicate_receipt(self):
        second_readback = replace(self.rows[0], witnessed_at=R + HOUR / 2)
        self.assertEqual(self.reconcile([second_readback, *self.rows]), self.reconcile(self.rows))

    def test_late_exact_readback_is_a_noop_only_with_timely_evidence(self):
        late = replace(self.rows[0], witnessed_at=R + 2 * HOUR)
        for rows in ([late, *self.rows], [*self.rows, late]):
            self.assertEqual(self.reconcile(rows), self.reconcile(self.rows))
        for other in (replace(late, receipt_sha256=B), replace(late, disposition="NONTRADE"),
                      replace(late, decision_at=late.decision_at + HOUR / 4)):
            report = self.reconcile([other, *self.rows])
            self.assertEqual(report["status"], "HOLD_INCOMPLETE")
            self.assertEqual(report["ignored"], {"not_visible_by_decision_cutoff": 1})

    def test_roster_must_be_nonempty_fixed_and_unique(self):
        self.assertEqual(reconcile_opportunities([], [], manifest_sha256=M, frozen_at=R,
                                                 audit_at=R)["status"], "HOLD_EMPTY_ROSTER")
        with self.assertRaises(ValueError):
            reconcile_opportunities([self.roster[0], self.roster[0]], [], manifest_sha256=M, frozen_at=R,
                                    audit_at=R)

    def test_all_early_receipts_stay_provisional_until_their_cutoff(self):
        early = [replace(row, decision_at=R + HOUR / 4, witnessed_at=R + HOUR / 2) for row in self.rows]
        before_first = self.reconcile(early, R + HOUR / 2)
        self.assertEqual(before_first["status"], "HOLD_INCOMPLETE")
        self.assertEqual(before_first["opportunities"], dict.fromkeys(("opp1", "opp2", "opp3"), "PROVISIONAL"))
        at_first = self.reconcile(early, R + HOUR)
        self.assertEqual(at_first["opportunities"], {"opp1": "DECISION", "opp2": "PROVISIONAL", "opp3": "PROVISIONAL"})
        self.assertEqual(at_first["status"], "HOLD_INCOMPLETE")
        self.assertEqual(self.reconcile(early, R + 3 * HOUR)["status"], "FIXTURE_RECONCILED")

    def test_freeze_must_strictly_precede_decision(self):
        simultaneous = replace(self.rows[0], decision_at=R)
        after = replace(self.rows[0], decision_at=R + timedelta(microseconds=1))
        rejected = self.reconcile([simultaneous, *self.rows[1:]])
        self.assertEqual(rejected["opportunities"]["opp1"], "MISSING")
        self.assertEqual(rejected["ignored"], {"not_visible_by_decision_cutoff": 1})
        self.assertEqual(self.reconcile([after, *self.rows[1:]])["status"], "FIXTURE_RECONCILED")
