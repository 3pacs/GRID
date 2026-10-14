"""Pure declaration controls; no database or production-module imports."""

from dataclasses import FrozenInstanceError, replace
from itertools import product
import unittest

from analysis.materialization_accounting import (
    CommitOutcome,
    CoverageDeclaration,
    CoverageState,
    RunDeclaration,
    TransactionDeclaration,
    diagnose_coverage,
    diagnose_run,
    diagnose_transaction,
)


def transaction(**updates):
    fields = dict(
        transaction_id="ticker-A/attempt-1",
        outcome=CommitOutcome.COMMITTED,
        attempted_rows=3,
        staged_rows=2,
        declared_errors=0,
        declared_committed_rows=2,
        clean_pit_empty=False,
    )
    fields.update(updates)
    return TransactionDeclaration(**fields)


def coverage(**updates):
    fields = dict(
        expected_items=3,
        attempted_items=3,
        succeeded_items=2,
        clean_empty_items=1,
        failed_items=0,
        unknown_items=0,
        deferred_items=0,
    )
    fields.update(updates)
    return CoverageDeclaration(**fields)


class TransactionTests(unittest.TestCase):
    def test_explicit_commit_count(self):
        value = transaction()
        result = diagnose_transaction(value)
        self.assertTrue(result.consistent)
        self.assertEqual(result.usable_declared_committed_rows, 2)
        self.assertIs(result.declaration, value)
        self.assertEqual(result.empty_reason, "NOT_DECLARED")

    def test_no_staging_inference(self):
        result = diagnose_transaction(transaction(declared_committed_rows=None))
        self.assertTrue(result.consistent)
        self.assertIsNone(result.usable_declared_committed_rows)

    def test_unknown_commit_preserves_unknown(self):
        result = diagnose_transaction(
            transaction(outcome=CommitOutcome.UNKNOWN, declared_committed_rows=None)
        )
        self.assertTrue(result.consistent)
        self.assertIsNone(result.usable_declared_committed_rows)

    def test_unknown_with_any_count_is_contradictory(self):
        for count in (0, 1, 2):
            with self.subTest(count=count):
                result = diagnose_transaction(
                    transaction(outcome=CommitOutcome.UNKNOWN, declared_committed_rows=count)
                )
                self.assertIn("UNKNOWN_COMMIT_HAS_KNOWN_COUNT", result.issues)
                self.assertIsNone(result.usable_declared_committed_rows)

    def test_rollback_cannot_report_positive(self):
        result = diagnose_transaction(transaction(outcome=CommitOutcome.ROLLED_BACK))
        self.assertIn("NONCOMMITTED_OUTCOME_HAS_POSITIVE_COUNT", result.issues)
        self.assertIsNone(result.usable_declared_committed_rows)

    def test_rollback_zero_and_unknown_are_separate(self):
        for count in (0, None):
            with self.subTest(count=count):
                result = diagnose_transaction(
                    transaction(outcome=CommitOutcome.ROLLED_BACK, declared_committed_rows=count)
                )
                self.assertTrue(result.consistent)
                self.assertEqual(result.usable_declared_committed_rows, count)

    def test_not_attempted_refuses_activity(self):
        result = diagnose_transaction(
            transaction(outcome=CommitOutcome.NOT_ATTEMPTED, declared_committed_rows=0)
        )
        self.assertIn("NOT_ATTEMPTED_HAS_ROW_ACTIVITY", result.issues)
        self.assertIsNone(result.usable_declared_committed_rows)

    def test_not_attempted_refuses_positive_count(self):
        result = diagnose_transaction(
            transaction(outcome=CommitOutcome.NOT_ATTEMPTED, attempted_rows=0,
                        staged_rows=0, declared_committed_rows=1)
        )
        self.assertFalse(result.consistent)
        self.assertIn("NONCOMMITTED_OUTCOME_HAS_POSITIVE_COUNT", result.issues)

    def test_not_attempted_missing_count_not_inferred(self):
        result = diagnose_transaction(
            transaction(outcome=CommitOutcome.NOT_ATTEMPTED, attempted_rows=0,
                        staged_rows=0, declared_committed_rows=None)
        )
        self.assertTrue(result.consistent)
        self.assertIsNone(result.usable_declared_committed_rows)
        self.assertEqual(result.empty_reason, "NOT_DECLARED")

    def test_clean_pit_empty_is_explicit(self):
        result = diagnose_transaction(
            transaction(outcome=CommitOutcome.NOT_ATTEMPTED, attempted_rows=0,
                        staged_rows=0, declared_committed_rows=0, clean_pit_empty=True)
        )
        self.assertTrue(result.consistent)
        self.assertEqual(result.empty_reason, "CLEAN_PIT_EMPTY")
        self.assertEqual(result.usable_declared_committed_rows, 0)

    def test_pit_empty_rejects_error_or_missing_count(self):
        for changes in (
            {"declared_errors": 1}, {"declared_committed_rows": None},
            {"outcome": CommitOutcome.ROLLED_BACK}, {"attempted_rows": 1},
            {"outcome": CommitOutcome.UNKNOWN}, {"staged_rows": 1},
        ):
            with self.subTest(changes=changes):
                value = transaction(outcome=CommitOutcome.NOT_ATTEMPTED, attempted_rows=0,
                                    staged_rows=0, declared_committed_rows=0, clean_pit_empty=True)
                result = diagnose_transaction(replace(value, **changes))
                self.assertIn("CLEAN_PIT_EMPTY_CONTRADICTION", result.issues)
                self.assertEqual(result.empty_reason, "INCONSISTENT")

    def test_lookup_error_is_not_empty(self):
        result = diagnose_transaction(
            transaction(outcome=CommitOutcome.NOT_ATTEMPTED, attempted_rows=0,
                        staged_rows=0, declared_errors=1, declared_committed_rows=0)
        )
        self.assertTrue(result.consistent)
        self.assertEqual(result.empty_reason, "ERRORS_DECLARED")

    def test_commit_and_error_keep_distinct_declared_facts(self):
        result = diagnose_transaction(transaction(declared_errors=1))
        self.assertTrue(result.consistent)
        self.assertEqual(result.usable_declared_committed_rows, 2)
        self.assertEqual(result.empty_reason, "ERRORS_DECLARED")

    def test_staged_cannot_exceed_attempted(self):
        result = diagnose_transaction(transaction(attempted_rows=1))
        self.assertIn("STAGED_EXCEEDS_ATTEMPTED", result.issues)
        self.assertIsNone(result.usable_declared_committed_rows)

    def test_committed_cannot_exceed_staged(self):
        result = diagnose_transaction(transaction(declared_committed_rows=3))
        self.assertIn("COMMITTED_EXCEEDS_STAGED", result.issues)
        self.assertIsNone(result.usable_declared_committed_rows)

    def test_smaller_declared_commit_and_zero_are_allowed(self):
        for count in (0, 1, 2):
            with self.subTest(count=count):
                result = diagnose_transaction(transaction(declared_committed_rows=count))
                self.assertTrue(result.consistent)
                self.assertEqual(result.usable_declared_committed_rows, count)
                self.assertNotEqual(result.empty_reason, "CLEAN_PIT_EMPTY")

    def test_invalid_transaction_count_types(self):
        for field in ("attempted_rows", "staged_rows", "declared_errors", "declared_committed_rows"):
            for value in (True, False, -1, 1.0, "1", [], {}):
                with self.subTest(field=field, value=value):
                    with self.assertRaises(ValueError):
                        transaction(**{field: value})

    def test_only_committed_count_is_optional(self):
        for field in ("attempted_rows", "staged_rows", "declared_errors"):
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    transaction(**{field: None})

    def test_invalid_enums_flags_and_ids(self):
        for field, values in (
            ("outcome", ("COMMITTED", None, True, 1)),
            ("clean_pit_empty", (0, 1, "true", None)),
            ("transaction_id", ("", " ", None, 1, True)),
        ):
            for value in values:
                with self.subTest(field=field, value=value):
                    with self.assertRaises(ValueError):
                        transaction(**{field: value})

    def test_boundary_count_matrix(self):
        for attempted, staged, committed in product(range(4), repeat=3):
            with self.subTest(attempted=attempted, staged=staged, committed=committed):
                result = diagnose_transaction(transaction(
                    attempted_rows=attempted, staged_rows=staged,
                    declared_committed_rows=committed))
                self.assertEqual(result.consistent, committed <= staged <= attempted)
                self.assertEqual(result.usable_declared_committed_rows,
                                 committed if committed <= staged <= attempted else None)


class CoverageTests(unittest.TestCase):
    def test_complete_partition(self):
        value = coverage()
        result = diagnose_coverage(value)
        self.assertEqual(result.state, CoverageState.COMPLETE)
        self.assertEqual(result.unaccounted_items, 0)
        self.assertIs(result.declaration, value)

    def test_complete_failed_partition_is_not_task_success(self):
        result = diagnose_coverage(coverage(succeeded_items=0, clean_empty_items=0, failed_items=3))
        self.assertEqual(result.state, CoverageState.COMPLETE)
        self.assertEqual(result.declaration.failed_items, 3)

    def test_deferred_partition(self):
        result = diagnose_coverage(coverage(expected_items=5, deferred_items=2))
        self.assertEqual(result.state, CoverageState.DEFERRED)
        self.assertEqual(result.unaccounted_items, 0)

    def test_missing_universe_is_unknown(self):
        result = diagnose_coverage(coverage(expected_items=None))
        self.assertEqual(result.state, CoverageState.UNKNOWN)
        self.assertIsNone(result.unaccounted_items)

    def test_unaccounted_not_silently_deferred(self):
        result = diagnose_coverage(coverage(expected_items=5))
        self.assertEqual(result.state, CoverageState.UNKNOWN)
        self.assertEqual(result.unaccounted_items, 2)
        self.assertEqual(result.declaration.deferred_items, 0)

    def test_unknown_attempts_prevent_complete(self):
        result = diagnose_coverage(coverage(succeeded_items=1, unknown_items=1))
        self.assertEqual(result.state, CoverageState.UNKNOWN)
        self.assertEqual(result.unaccounted_items, 0)

    def test_unknown_with_deferred_retains_both(self):
        result = diagnose_coverage(coverage(expected_items=5, succeeded_items=1,
                                            unknown_items=1, deferred_items=2))
        self.assertEqual(result.state, CoverageState.UNKNOWN)
        self.assertEqual(result.declaration.deferred_items, 2)
        self.assertEqual(result.declaration.unknown_items, 1)

    def test_overlapping_attempt_partition_inconsistent(self):
        result = diagnose_coverage(coverage(failed_items=1))
        self.assertEqual(result.state, CoverageState.INCONSISTENT)
        self.assertIn("ATTEMPTED_PARTITION_MISMATCH", result.issues)
        self.assertIsNone(result.unaccounted_items)

    def test_missing_attempt_partition_inconsistent(self):
        result = diagnose_coverage(coverage(succeeded_items=1))
        self.assertEqual(result.state, CoverageState.INCONSISTENT)
        self.assertIn("ATTEMPTED_PARTITION_MISMATCH", result.issues)

    def test_accounted_over_total_inconsistent(self):
        result = diagnose_coverage(coverage(expected_items=2))
        self.assertEqual(result.state, CoverageState.INCONSISTENT)
        self.assertIn("ACCOUNTED_EXCEEDS_EXPECTED", result.issues)

    def test_empty_universe_is_explicit(self):
        result = diagnose_coverage(coverage(expected_items=0, attempted_items=0,
                                            succeeded_items=0, clean_empty_items=0))
        self.assertEqual(result.state, CoverageState.COMPLETE)
        result2 = diagnose_coverage(replace(result.declaration, expected_items=None))
        self.assertEqual(result2.state, CoverageState.UNKNOWN)

    def test_invalid_coverage_count_types(self):
        for field in CoverageDeclaration.__dataclass_fields__:
            for value in (True, False, -1, 1.0, "1", [], {}):
                with self.subTest(field=field, value=value):
                    with self.assertRaises(ValueError):
                        coverage(**{field: value})

    def test_missing_category_not_defaulted_to_zero(self):
        with self.assertRaises(TypeError):
            CoverageDeclaration(expected_items=1, attempted_items=1, succeeded_items=1)
        for field in tuple(CoverageDeclaration.__dataclass_fields__)[1:]:
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    coverage(**{field: None})

    def test_small_partition_matrix(self):
        for success, empty, failed, unknown, deferred in product(range(2), repeat=5):
            with self.subTest(values=(success, empty, failed, unknown, deferred)):
                attempted = success + empty + failed + unknown
                value = CoverageDeclaration(attempted + deferred, attempted, success,
                                            empty, failed, unknown, deferred)
                expected = (CoverageState.UNKNOWN if unknown else
                            CoverageState.DEFERRED if deferred else CoverageState.COMPLETE)
                self.assertEqual(diagnose_coverage(value).state, expected)


class RunTests(unittest.TestCase):
    def test_prior_committed_ticker_survives_later_rollback(self):
        first = transaction()
        second = transaction(transaction_id="ticker-B/attempt-1", outcome=CommitOutcome.ROLLED_BACK,
                             declared_committed_rows=0, declared_errors=1)
        value = RunDeclaration((first, second), coverage())
        result = diagnose_run(value)
        self.assertEqual(result.known_declared_rows_subtotal, 2)
        self.assertEqual(result.declared_rows_total_for_supplied_transactions, 2)
        self.assertEqual(result.unknown_transaction_count, 0)
        self.assertIs(result.declaration, value)
        self.assertEqual(tuple(x.declaration for x in result.transactions), (first, second))

    def test_unknown_total_keeps_known_subtotal(self):
        unknown = transaction(transaction_id="B", outcome=CommitOutcome.UNKNOWN,
                              declared_committed_rows=None)
        result = diagnose_run(RunDeclaration((transaction(), unknown), coverage()))
        self.assertEqual(result.known_declared_rows_subtotal, 2)
        self.assertIsNone(result.declared_rows_total_for_supplied_transactions)
        self.assertEqual(result.unknown_transaction_count, 1)
        self.assertEqual(result.inconsistent_transaction_count, 0)
        self.assertIs(result.transactions[1].declaration, unknown)

    def test_inconsistent_count_not_aggregated(self):
        inconsistent = transaction(transaction_id="B", outcome=CommitOutcome.ROLLED_BACK)
        result = diagnose_run(RunDeclaration((transaction(), inconsistent), coverage()))
        self.assertEqual(result.known_declared_rows_subtotal, 2)
        self.assertIsNone(result.declared_rows_total_for_supplied_transactions)
        self.assertEqual(result.inconsistent_transaction_count, 1)
        self.assertEqual(result.unknown_transaction_count, 1)
        self.assertEqual(len(result.transactions), 2)
        self.assertIs(result.transactions[1].declaration, inconsistent)

    def test_all_unknown_not_zero_total(self):
        result = diagnose_run(RunDeclaration((transaction(declared_committed_rows=None),), coverage()))
        self.assertEqual(result.known_declared_rows_subtotal, 0)
        self.assertIsNone(result.declared_rows_total_for_supplied_transactions)

    def test_known_supplied_total_does_not_close_deferred_coverage(self):
        result = diagnose_run(RunDeclaration((transaction(),), coverage(expected_items=5, deferred_items=2)))
        self.assertEqual(result.declared_rows_total_for_supplied_transactions, 2)
        self.assertEqual(result.coverage.state, CoverageState.DEFERRED)

    def test_no_inferred_relationship_between_items_and_transactions(self):
        result = diagnose_run(RunDeclaration((), coverage()))
        self.assertEqual(result.declared_rows_total_for_supplied_transactions, 0)
        self.assertEqual(result.coverage.declaration.attempted_items, 3)
        self.assertEqual(result.coverage.state, CoverageState.COMPLETE)
        self.assertEqual(result.transactions, ())

    def test_mutable_containers_wrong_types_and_duplicate_ids_rejected(self):
        first = transaction()
        for items in ([first], None, (None,), ({},), (first, first)):
            with self.subTest(items=items):
                with self.assertRaises(ValueError):
                    RunDeclaration(items, coverage())
        with self.assertRaises(ValueError):
            RunDeclaration((first,), {})

    def test_outputs_and_declarations_are_frozen(self):
        run = RunDeclaration((transaction(),), coverage())
        result = diagnose_run(run)
        for obj, name, value in (
            (run, "transactions", ()), (run.transactions[0], "staged_rows", 99),
            (run.coverage, "expected_items", 99), (result, "known_declared_rows_subtotal", 99),
            (result.transactions[0], "issues", ()), (result.coverage, "state", CoverageState.COMPLETE),
        ):
            with self.subTest(type=type(obj).__name__):
                with self.assertRaises(FrozenInstanceError):
                    setattr(obj, name, value)
        self.assertIs(result.declaration, run)
        self.assertIs(result.coverage.declaration, run.coverage)

    def test_diagnostic_inputs_are_exact_declared_types(self):
        for fn in (diagnose_transaction, diagnose_coverage, diagnose_run):
            for value in (None, 0, True, {}, []):
                with self.subTest(fn=fn.__name__, value=value):
                    with self.assertRaises(ValueError):
                        fn(value)


if __name__ == "__main__":
    unittest.main()
