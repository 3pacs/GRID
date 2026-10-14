"""Pure diagnostics for supplied accounting declarations, never persistence proof.

No database, scheduler, callback, clock, or I/O is involved. Counts refer only to
the supplied declarations. Even consistent COMMITTED declarations are unverified.
"""

from dataclasses import dataclass
from enum import Enum

__all__ = (
    "CommitOutcome",
    "CoverageState",
    "TransactionDeclaration",
    "TransactionDiagnostic",
    "CoverageDeclaration",
    "CoverageDiagnostic",
    "RunDeclaration",
    "RunDiagnostic",
    "diagnose_transaction",
    "diagnose_coverage",
    "diagnose_run",
)


class CommitOutcome(str, Enum):
    COMMITTED = "COMMITTED"
    ROLLED_BACK = "ROLLED_BACK"
    UNKNOWN = "UNKNOWN"
    NOT_ATTEMPTED = "NOT_ATTEMPTED"


class CoverageState(str, Enum):
    COMPLETE = "COMPLETE"
    DEFERRED = "DEFERRED"
    UNKNOWN = "UNKNOWN"
    INCONSISTENT = "INCONSISTENT"


def _count(value: int | None, name: str, *, optional: bool = False) -> None:
    if optional and value is None:
        return
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be an exact nonnegative integer")


@dataclass(frozen=True, slots=True)
class TransactionDeclaration:
    transaction_id: str
    outcome: CommitOutcome
    attempted_rows: int
    staged_rows: int
    declared_errors: int
    declared_committed_rows: int | None
    clean_pit_empty: bool = False

    def __post_init__(self) -> None:
        if type(self.transaction_id) is not str or not self.transaction_id.strip():
            raise ValueError("transaction_id must be a nonempty string")
        if type(self.outcome) is not CommitOutcome:
            raise ValueError("outcome must be a CommitOutcome")
        for name in ("attempted_rows", "staged_rows", "declared_errors"):
            _count(getattr(self, name), name)
        _count(self.declared_committed_rows, "declared_committed_rows", optional=True)
        if type(self.clean_pit_empty) is not bool:
            raise ValueError("clean_pit_empty must be a boolean")


@dataclass(frozen=True, slots=True)
class TransactionDiagnostic:
    declaration: TransactionDeclaration
    issues: tuple[str, ...]
    usable_declared_committed_rows: int | None
    empty_reason: str

    @property
    def consistent(self) -> bool:
        return not self.issues


def diagnose_transaction(value: TransactionDeclaration) -> TransactionDiagnostic:
    if type(value) is not TransactionDeclaration:
        raise ValueError("expected TransactionDeclaration")
    issues: list[str] = []
    count = value.declared_committed_rows
    if value.staged_rows > value.attempted_rows:
        issues.append("STAGED_EXCEEDS_ATTEMPTED")
    if count is not None and count > value.staged_rows:
        issues.append("COMMITTED_EXCEEDS_STAGED")
    if value.outcome in (CommitOutcome.ROLLED_BACK, CommitOutcome.NOT_ATTEMPTED):
        if count not in (None, 0):
            issues.append("NONCOMMITTED_OUTCOME_HAS_POSITIVE_COUNT")
    if value.outcome is CommitOutcome.UNKNOWN and count is not None:
        issues.append("UNKNOWN_COMMIT_HAS_KNOWN_COUNT")
    if value.outcome is CommitOutcome.NOT_ATTEMPTED and (
        value.attempted_rows or value.staged_rows
    ):
        issues.append("NOT_ATTEMPTED_HAS_ROW_ACTIVITY")
    if value.clean_pit_empty and not (
        value.outcome is CommitOutcome.NOT_ATTEMPTED
        and value.attempted_rows == 0
        and value.staged_rows == 0
        and value.declared_errors == 0
        and count == 0
    ):
        issues.append("CLEAN_PIT_EMPTY_CONTRADICTION")
    usable = None if issues or value.outcome is CommitOutcome.UNKNOWN else count
    empty_reason = "NOT_DECLARED"
    if issues:
        empty_reason = "INCONSISTENT"
    elif value.declared_errors:
        empty_reason = "ERRORS_DECLARED"
    elif value.clean_pit_empty:
        empty_reason = "CLEAN_PIT_EMPTY"
    return TransactionDiagnostic(value, tuple(issues), usable, empty_reason)


@dataclass(frozen=True, slots=True)
class CoverageDeclaration:
    expected_items: int | None
    attempted_items: int
    succeeded_items: int
    clean_empty_items: int
    failed_items: int
    unknown_items: int
    deferred_items: int

    def __post_init__(self) -> None:
        _count(self.expected_items, "expected_items", optional=True)
        for name in (
            "attempted_items",
            "succeeded_items",
            "clean_empty_items",
            "failed_items",
            "unknown_items",
            "deferred_items",
        ):
            _count(getattr(self, name), name)


@dataclass(frozen=True, slots=True)
class CoverageDiagnostic:
    declaration: CoverageDeclaration
    state: CoverageState
    issues: tuple[str, ...]
    unaccounted_items: int | None


def diagnose_coverage(value: CoverageDeclaration) -> CoverageDiagnostic:
    if type(value) is not CoverageDeclaration:
        raise ValueError("expected CoverageDeclaration")
    issues: list[str] = []
    partition = (
        value.succeeded_items
        + value.clean_empty_items
        + value.failed_items
        + value.unknown_items
    )
    if partition != value.attempted_items:
        issues.append("ATTEMPTED_PARTITION_MISMATCH")
    accounted = value.attempted_items + value.deferred_items
    expected = value.expected_items
    if expected is not None and accounted > expected:
        issues.append("ACCOUNTED_EXCEEDS_EXPECTED")
    unaccounted = None if expected is None or issues else expected - accounted
    if issues:
        state = CoverageState.INCONSISTENT
    elif expected is None or unaccounted or value.unknown_items:
        state = CoverageState.UNKNOWN
    elif value.deferred_items:
        state = CoverageState.DEFERRED
    else:
        state = CoverageState.COMPLETE
    return CoverageDiagnostic(value, state, tuple(issues), unaccounted)


@dataclass(frozen=True, slots=True)
class RunDeclaration:
    transactions: tuple[TransactionDeclaration, ...]
    coverage: CoverageDeclaration

    def __post_init__(self) -> None:
        if type(self.transactions) is not tuple or any(
            type(item) is not TransactionDeclaration for item in self.transactions
        ):
            raise ValueError("transactions must be a tuple of TransactionDeclaration")
        if type(self.coverage) is not CoverageDeclaration:
            raise ValueError("coverage must be a CoverageDeclaration")
        identities = tuple(item.transaction_id for item in self.transactions)
        if len(set(identities)) != len(identities):
            raise ValueError("duplicate transaction_id")


@dataclass(frozen=True, slots=True)
class RunDiagnostic:
    declaration: RunDeclaration
    transactions: tuple[TransactionDiagnostic, ...]
    coverage: CoverageDiagnostic
    known_declared_rows_subtotal: int
    declared_rows_total_for_supplied_transactions: int | None
    unknown_transaction_count: int
    inconsistent_transaction_count: int


def diagnose_run(value: RunDeclaration) -> RunDiagnostic:
    if type(value) is not RunDeclaration:
        raise ValueError("expected RunDeclaration")
    transactions = tuple(diagnose_transaction(item) for item in value.transactions)
    known = tuple(
        item.usable_declared_committed_rows
        for item in transactions
        if item.usable_declared_committed_rows is not None
    )
    subtotal = sum(known)
    unknown = len(transactions) - len(known)
    total = None if unknown else subtotal
    return RunDiagnostic(
        value,
        transactions,
        diagnose_coverage(value.coverage),
        subtotal,
        total,
        unknown,
        sum(not item.consistent for item in transactions),
    )
