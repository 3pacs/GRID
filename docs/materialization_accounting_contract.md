# Materialization accounting declarations

This module diagnoses consistency of immutable caller-supplied declarations.
It performs no I/O, database transaction, callback, clock access, scheduler work,
or source selection. Its results do not authenticate observations or establish
actual persisted rows, durability, coverage, freshness, or task success. The
existing materializer and both schedulers are unchanged and are not wired to it.

## API and validation

`TransactionDeclaration`, `CoverageDeclaration` and `RunDeclaration` are frozen,
slotted dataclasses. All counts must be exact nonnegative Python integers;
booleans, floats, strings, negatives and missing required category counts are
rejected. Optional counts explicitly use `None`. Outcomes must be `CommitOutcome`
members, not a string that happens to match. Transaction identities must be
nonempty strings and unique within the supplied run. No coercion, trimming,
automatic revision selection, inferred universe or default missing category is
applied. Frozen records provide normal Python assignment protection, not an
authentication or tamper-proof boundary against a caller using Python internals.

`diagnose_transaction`, `diagnose_coverage` and `diagnose_run` preserve the exact
original declaration objects. Contradictory validly typed declarations remain
inspectable in the result and receive issues, rather than being silently dropped.
Results themselves are also frozen. The exported diagnostic DTO constructors do
not confer authority; neither consistent declarations nor a manually constructed
DTO are runtime proof.

## Transaction semantics

`attempted_rows` counts declared attempted row statements; `staged_rows` counts
declared successfully staged row statements. They are not row persistence counts.
`declared_errors` is a declared count of errors, including lookup or write errors;
it is not restricted to the attempted row count. For example, a lookup error may
be declared before any row statement is attempted.

`declared_committed_rows` is the supplied count of affected rows at declared
commit, or `None` when unknown. It cannot exceed staged rows; staged rows cannot
exceed attempted rows. This contract intentionally does not require each staged
statement to affect a row: conflicts/no-op statements can give a smaller count.
`COMMITTED` with an unknown count keeps the count unknown. `UNKNOWN` commit with
any known count, including zero, is inconsistent; no attempt or staging count
fills in the missing commit result. `ROLLED_BACK` and `NOT_ATTEMPTED` cannot report
positive committed rows. Their explicit zero is usable as a declaration, while
their missing count stays unknown. `NOT_ATTEMPTED` cannot have row activity.

The `usable_declared_committed_rows` result is `None` for contradictions or
unknown commit, otherwise the original supplied count. “Usable” here means usable
for declaration arithmetic only. A consistent `COMMITTED` plus errors may retain
a positive declared count and `ERRORS_DECLARED`; the helper cannot decide whether
that transaction actually happened or whether a caller's partial-commit policy
was appropriate.

`clean_pit_empty=True` requires `NOT_ATTEMPTED`, explicit zero committed rows,
zero attempted/staged rows and zero declared errors. This distinguishes a clean
declared PIT-empty from an error or an omitted count. Zero writes alone never
imply PIT-empty. `empty_reason` is `CLEAN_PIT_EMPTY`, `ERRORS_DECLARED`,
`INCONSISTENT` or `NOT_DECLARED`, without assessing any production PIT query.

## Coverage semantics

Coverage counts use one declared item universe, independently of transaction row
counts. Attempted items partition exactly into succeeded, clean-empty, failed and
unknown items. Deferred items are explicitly unattempted. An optional expected
item count bounds attempted plus deferred items. No relationship between a
transaction and an item is invented; callers must bind that relationship outside
this module before any future integration.

| State | Meaning of the supplied declaration |
| --- | --- |
| `INCONSISTENT` | Attempt partition mismatch or accounted items exceed expected |
| `UNKNOWN` | Missing expected count, unaccounted items or unknown attempted outcomes |
| `DEFERRED` | Exact known partition includes explicit deferred items |
| `COMPLETE` | Exact known partition, no unknown or deferred items |

Unknown takes precedence over deferred in the single state, but both original
counts remain preserved. `unaccounted_items` is the difference of the declared
expected and accounted counts when coherent; it is not silently relabelled as
deferred. It is `None` when expected coverage is absent or inconsistent.
`COMPLETE` describes an accounting partition only: a complete partition may
contain failed items and is never a task-success or freshness indication.

## Run arithmetic

`diagnose_run` retains every transaction in input order. Its
`known_declared_rows_subtotal` adds only consistent, explicitly known declared
counts; unknown/inconsistent declarations remain in diagnostics and their number
is reported. `declared_rows_total_for_supplied_transactions` is `None` if any
supplied transaction has unknown or inconsistent count. Otherwise it equals the
known subtotal. An explicitly empty transaction tuple has a supplied arithmetic
total of zero, without inferring any attempted work or closed coverage.

The supplied transaction total is not an expected-universe row total. Deferred
or unknown item coverage remains separate even when every supplied transaction
count is known. A prior declared committed transaction remains visible alongside
a later rolled-back transaction, without reconstructing actual database history.

## Provenance and held integration

These are exactly three new additions against current-main
`85cc57bb64014e9732d2beed0e3ad7e7c8ca3ac9`, tree
`e3a6c98c9ac81dec63ae5cfd12bf1aa6bdb74afa`. The existing `_write_rows`, caller,
scheduler classifier, registry policies, PIT/SQL and per-observation ADR logic
remain protected. Existing classifier capabilities were inspected and are not
reimplemented here. Old candidate commit `5dd470163b166aa5380177fbd1cd8bbe3b7badd0`
and its four historical native cases remain historical evidence, not validation
of this helper or current runtime. Production accounting/cancellation integration,
PR ownership reconciliation, independent source acceptance, full CI and native
runtime verification remain separate held work.

Tests use literal declaration examples and small independent count matrices,
including positive and unknown counts, rollback/error/clean-empty separation,
coverage partitions/deferred omissions, aggregate subtotal versus unknown total,
type rejection and ordinary output immutability. Source mutants must fail
functional assertions without unexpected errors; no AST-only rejection is counted.
