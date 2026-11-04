# PR918 explicit schema preparation and compatible HOLD

This source successor replaces lazy schema creation in ordinary publication,
tracker and second-writer paths with catalog-only readiness on the actual
operation connection. Preliminary resolution is never write authorization:
the tracker pins its resolved namespace and rechecks the operation connection.
Required generated IDs, timestamp defaults, PK/FK and nullable no-default unit
receipt are functional requirements; performance indexes are not readiness keys.

The only preparation entry point for the combined boundary is
`intelligence.runtime_schema.prepare_runtime_schema(engine,
thesis_schema=explicit_namespace, publication_schema=explicit_namespace)`.
Both identifiers must be explicitly bound by the release owner before use;
there is no default schema, engine/credential factory, runtime fallback or grant.
It runs the thesis and publication preparation in one transaction, sets local
lock timeout 500 ms, statement timeout 2 s and idle-transaction timeout 5 s,
and checks a cooperative 5 s budget before/after operations and before commit.
Connection acquisition, driver/network stalls and commit uncertainty require
the existing executor's external lifecycle bound. No retries are automatic.

Preparation preflights existing functional contracts, refuses incompatible
shapes, and may create missing tables/indexes or add nullable TEXT
`conviction_unit` with no default. It performs no row UPDATE/DELETE/backfill,
namespace creation, privilege grant or historical unit inference. A nullable
addition can still take ACCESS EXCLUSIVE; ordinary index creation can block
writes. The transaction-local bounds limit waits, not installed-state proof.
An incompatible existing table must be reconciled separately, never repaired
by silently changing existing types, defaults, keys or records.

Release order, conditional on the existing production approval and holds:

1. Bind installed namespace/role/schema, actual writer and consumer processes,
   approved lock budget and operator lifecycle bound. No source test supplies
   these facts. Preserve existing owner holds and the held scoring flag.
2. Prepare the explicitly bound thesis and publication schemas once under the
   approved hold. Confirm terminal commit or rollback. If outcome is uncertain,
   keep the hold; do not blindly repeat DDL or writes.
3. Adopt the exact unit-aware adapter, both writers, publication and history/API
   source together; a filesystem swap alone does not prove process adoption.
   Ordinary operations now refuse missing/incompatible readiness rather than
   doing DDL. Existing unknown unit receipts remain unknown.
4. Perform only separately authorized bounded postchecks and release holds
   through the sole controller. No scheduler/scoring activation is implied.

The matching HOLD artifact is a separate commit derived from this forward
source. It refuses tracker snapshots before DB/provider work, returns None
from the second snapshot writer, and makes the full thesis cycle report an
observable compatibility hold without effects. It retains the unit-aware
adapter, history/API readers, explicit schema code, publication safeguards and
all additive receipts. Publication persistence remains readiness-checked; HOLD
is a thesis-writer hold, not a global service or publication shutdown.

Rollback does not drop the receipt, infer historical units, change rows,
undo committed schema, or restore a unit-unaware adapter. Keep the prepared
schema and persisted rows; switch to the exact matching HOLD source under
existing controller authority and verify loaded-process adoption. A committed
preparation survives a failed filesystem swap. Any uncertain transaction or
partial process adoption keeps the hold pending reconciliation.

No revised-head publication, hosted CI, migration execution or production
release is authorized by this local preparation. Earlier #918 CI and #919
forward/HOLD receipts are historical evidence, not results for this tree.
Calendar remains qualified; publication/PIT clocks and original publications
are not rewritten. Synthetic tests do not establish production safety,
forecast accuracy, calibration or semantic completeness.
