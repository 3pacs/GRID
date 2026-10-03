# Options bounded publication preparation

This is a separate prerequisite for the later protected scheduler activation.
It does not release #806 or change append-only role ownership. Production
cutover is unapproved to this worker; root assesses and sequences the concrete
packet under the existing follow-up authorization. No new blanket owner gate
is proposed.

## Contract

Fetch every contract in the requested complete expiry scope, normalize and
deduplicate outside DATA transactions, and compute signals before publication.
The existing default expiry cap, provenance labels, session/quote/date gates,
source-wide freshness rules and first-per-day resolved semantics are retained.
The old writer's minimum transaction is `1 header + N contracts`; the 122-row
static witness exceeds 50 without signals or registry/resolved writes. No actual
over-50 negative transaction is executed in this preparation.

Rename the immutable header table to `options_capture_batches_all`. Existing
FKs, indexes, triggers, header images and sources follow the rename unchanged.
Add a constant-default `requires_publication=false` column without backfilling
legacy captures. The new writer inserts a prepared header with true in a
one-row transaction, then writes all contracts in independent transactions of
at most 50 rows. These prepared artifacts remain append-only evidence.

Keep `options_capture_batches` as the public registration name, now a view. It
exposes original atomic headers and prepared headers only after an immutable
completion receipt. `registered_at` on a prepared capture is the server clock
sample at receipt insertion, before COMMIT; it is not an exact COMMIT or client
ACK timestamp. Provider completion and quote timestamps remain unchanged. The
view exposes the receipt only after server COMMIT. Exact commit-time retrospective PIT across
the receipt-insertion/COMMIT interval needs a separate reviewed contract.
`options_snapshots` selects only published captures when superseding a previous
complete capture. Unregistered legacy rows retain their original fallback.

The final transaction takes the existing per-ticker/day advisory lock, preserves
the newest ordinal's signals, writes at most one signal + ten registry + ten
resolved rows and one completion receipt (22 aggregate DATA rows maximum).
Older overlapping captures still publish history without overwriting newer
signals. The completion trigger checks the full declared contract count and
metadata under a header row lock. The contract trigger takes the same lock and
refuses appends after completion, preventing a concurrent late append.

Each transaction uses explicit COMMIT handling, 5-second statement/idle limits,
a 5-second capture lock limit (the existing catalog limit stays 3 seconds), and
a cooperative 15-second deadline. PG14 cannot impose an absolute
wall-time transaction or WAL/COMMIT deadline; connection checkout is also outside
the DATA transaction. No provider work, SAVEPOINT, replay, or retry is inside
this publisher. Failure stops the remaining ticker scope. Acknowledged rows
survive; unknown COMMIT ACK yields an unknown current outcome plus a known
prefix; post-ACK cleanup retains the acknowledged result and stops later scope.
An actually committed completion may be reader-visible after its ACK is lost:
the server has a complete capture, while the client reports UNKNOWN and stops.

Opt-in row guards count DATA writes across the touched relations. Every bounded
transaction checks the reviewed trigger closure under relation locks and counts
actual inserted/updated/deleted rows globally using `pg_stat_xact_user_tables`.
Known triggers have zero DATA sidewrites. Added, altered, disabled, or missing
triggers fail closed before writes. New audit sidewrites require a new reviewed
closure and a smaller batch reservation; silently accepting them is forbidden.
The contract ledger is append-only, not a role/privileged-user security boundary.

## Reader inventory

| Consumer | Existing selection | Consequence |
|---|---|---|
| API derivatives batch metadata | registered headers | unchanged name/columns; prepared headers hidden |
| DealerGamma default | options_snapshots | prior complete remains visible during preparation |
| DealerGamma exact replay | raw contracts JOIN registered headers | incomplete IDs cannot replay |
| gex_batch_replay | resolve registered header/count, then raw chain | incomplete IDs fail resolution |
| gex_levels chain/tested_walls | options_snapshots | frozen reader query contract retained |
| vol_surface/options_recommender/API chain readers | options_snapshots | complete captures only |
| GEM validation | registered headers + raw contracts | incomplete batches excluded |
| GEM schema preflight | four immutable triggers + original FK/check names | locate renamed header storage, retain old-layout fallback |
| #809 Hermes suppression | registered positive non-backfilled scheduler headers | no code/query or retry/provenance change needed |
| daily signals / general resolved PIT consumers | final transaction writes | no premature signal/value update |

Raw storage and private prepared headers are forensic surfaces, not published
capture readers. None of the inspected runtime raw readers bypasses its public
registration gate. Direct database users must make that distinction explicitly.

## Root cutover dependency

The SQL is deliberately outside Alembic and cannot apply automatically through
the normal deploy workflow. Root must first turn this reviewed packet into its
controlled migration/cutover, including the schema version stamp and exact
existing ACL preservation. Creating the replacement view does not inherit the
renamed table's SELECT/INSERT grants; restore the reviewed existing runtime
grants on the new view and grant only the needed INSERT/SELECT permissions on
the new storage/receipt/contract relations. Preserve owner-held role ownership.
Count the 15 trigger-contract ledger rows, any audit sidewrites and the migration
stamp together; if the reviewed administration adds more rows, split before 50.
There is no legacy DATA backfill in this packet.

1. Freeze exact composition with #809 and the then-current main; fresh independent
   review must cover the SQL, writer, trigger closure, reader contract and grants.
2. Root verifies the actual old scheduler/API/Hermes/GEM writer inventory and
   in-flight captures. This worker did not inspect production state or assume
   those processes are stopped. The schema change blocks briefly on active
   transactions and fails after its short lock timeout; an uncertain apply ACK
   is inspected by root, never automatically replayed.
3. Root applies the schema contract in the permitted release window before new
   code can run. Its constant default and insertable registration view let the
   old protected scheduler keep its existing atomic header/contract shape.
4. Deploy the reviewed API/Hermes code only after schema/grant verification.
   Existing readers retain public registration names. The old protected
   scheduler still has the unbounded writer: compatibility is not cap compliance.
5. Root uses the authorized protected scheduler activation/preservation workflow
   to select the bounded writer, and checks every remaining executable options
   entry point (including older GEM copies) before claiming universal <=50 writes.
   No worker stop/restart, migration, merge, deploy, or activation occurred.

Before any prepared artifact exists, a root-reviewed catalog-only rollback may
restore the old layout after removing only new empty relations/functions/views.
After preparation starts, do not drop, rewrite, clean or downgrade these evidence
relations. Preserve the completion-gated schema and stop the affected new writer;
forward correction is the default. An old binary can still use the public view,
but cannot earn a bounded-write claim. The old append-only migration's downgrade
expects a table under the registration name and is no longer a valid rollback.

## Fresh reviewer gates

Independently reproduce >50 and >100 complete synthetic chains and actual global
transaction counts. Check prefix accounting, no partial reader visibility, prior
complete/PIT reads, receipt sealing/concurrency, real server COMMIT plus injected
lost ACK, post-ACK cleanup, scope STOP, source freshness, legacy source/image
preservation, current-main/#809 composition and full focused tests. The private
minimal source/feature/resolved fixture schema verifies writer mechanics; it is
not a full-production-schema or grant certification. Root owns native reporting,
canonical TODO/report sinks, production decisions and release verification.
