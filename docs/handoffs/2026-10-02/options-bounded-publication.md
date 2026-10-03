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

## Captured production shape and narrow guard correction

The root read-only catalog captured at 2026-10-03T00:43:13Z is pinned by
SHA256 `7ae5deaf4e7e54c8c3e74421a98b1cfcaee4d320ef8578fc221c5b177d8e3785`.
It applied nothing. Its private contents, reconstructed schema and raw logs stay
outside Git. The SQL publishes opaque hashes of deterministic catalog images,
plus original relation/function OIDs, rather than the private catalog itself.
The initial image covers actual columns/defaults, checks and both inbound/outbound
FKs, indexes, triggers and their functions, rules/view definitions, owners, RLS
and relation ACLs. All new version objects must be absent. Six fresh exact DATA
counts must be supplied in `grid.options_expected_counts` and rechecked under
short locks. A changed guard, schema, ACL, count, or version refuses before ALTER
or DATA. Unreviewed applicable default grants also refuse; preserve them and
review their consequences, never revoke them to make this packet pass.

The frozen four-function whitelist excluded the enabled
`trg_feature_registry_transformation_version`. Its reviewed function
`feature_registry_check_transformation_version()` is invoker/read-only: it checks
existing version consistency and returns or raises, with zero DATA sidewrites.
Its exact `pg_get_functiondef` SHA256 is
`57671d10b24505bd6294a85e6ce801a2dd4f474281518d7e98c9d0402135d3bd`;
the unchanged append guard SHA256 is
`dfbf6c305268f9c74d8e740892ff78a73354b46dd1f570900c333ba811a5162f`.
Only this reviewed guard is added to the existing zero-sidewrite whitelist.
Original body pins are checked before destructive DDL, immediately before ledger
DATA, and in runtime closure assertions. Names alone never authorize replacement
bodies. The 16 immutable closure rows plus one immutable schema/version stamp
consume 17 aggregate DATA rows, with no legacy DATA backfill or audit sidewrites.
The post-DDL stamp pins actual OIDs, defaults, trigger/function identity and ACLs;
the writer pins the five new function bodies independently and checks the full
closure before work and again before COMMIT. This remains a drift contract under
current ownership, not held #806 privilege hardening.

New metric registration retains existing RAW/version-1, family, ZSCORE and
FORWARD_FILL conventions. Eligibility starts on its actual registration date;
the old fixed 2024 eligibility claim is removed only for newly inserted metrics.
Existing metadata is unchanged on conflict. The actual transformation guard still
refuses a conflicting RAW version. No CHECK is weakened or provenance invented.

## Schema and exact ACL packet

SQL stays outside Alembic, so normal deployment cannot apply it. Captured scoped
relations are owned by `grid`; their ACLs reveal no separate runtime service
principal. Root must verify actual executable login roles against that evidence
before cutover. The renamed storage retains its ACL and owner; replacement views
do not inherit table grants. The packet explicitly grants only these operations
to the existing `grid` principal, and preserves every other existing grant:

| Object | Necessary operations |
|---|---|
| replacement `options_capture_batches` view | SELECT, INSERT |
| renamed `options_capture_batches_all` storage | SELECT, INSERT |
| new `options_capture_publications` receipt | SELECT, INSERT |
| new trigger ledger and schema/version stamp | SELECT |
| existing contracts, signals, registry, resolved and source tables | retain captured owner/ACL; existing writer operations remain |
| existing `options_snapshots` view | CREATE OR REPLACE retains its captured ACL |

New functions remain invoker functions owned by `grid`, with captured default
function ACL behavior verified before creation and pinned afterward. No generic
grants/revokes, role changes, security-definer escalation or ownership changes
are part of this packet. An additional service principal or default privilege is
a concrete compatibility fact for root/reviewer to resolve, not inferred approval.

## Root cutover ordering and existing boundaries

1. Obtain a NEW independent review of the final full code, schema/ACL packet,
   current-main/#809 composition, private proofs and exact-head CI. Skipped or
   pending CI is neither GREEN nor approval. Preserve #806 and other owner holds.
2. Root inventories ALL actual old scheduler/API/Hermes/GEM executables, source
   paths, identities and in-flight captures. Historical protected `892822`/`cdf`
   identities are not asserted stopped or upgraded by this author. The public
   view accommodates an old atomic INSERT shape, but cannot make that writer
   comply with the 50-row cap. A catalog-only inspection proves no activation.
3. Before a merge/push can auto-deploy new API/Hermes code, root schedules and
   verifies the schema/ACL prerequisite against the then-current actual catalog,
   exact counts, owned role and release window. Use one explicit transaction with
   ON_ERROR_STOP and short 5s lock/statement/idle limits plus the cooperative 15s
   elapsed check. First execute a deliberate dry run with known ROLLBACK, retain
   its receipts, then perform the separately controlled once-only apply. A lock
   failure or unknown COMMIT ACK means STOP and inspect; there is no automatic
   apply retry. New writer activation must wait for acknowledged schema/grants
   and an independent read-only verification of stamp, closure, counts and views.
4. Deploy only reviewed, fresh-CI code after that schema readiness evidence.
   Routine `deploy.yml` pushes/restarts cover API/Hermes; they do not implicitly
   activate the protected scheduler. Its existing `activate_scheduler=true` and
   `acknowledge_scheduler_interruption=true` gate remains mandatory, together
   with root's preservation/interruption procedure. Never substitute ops-exec,
   SSH, a manual capture or a worker restart for that protected workflow.
5. Root activates and verifies each separately authorized executable through its
   original service boundary, including remaining GEM/Hermes/API copies, before
   claiming all options writes are bounded. Verify live source identity and real
   outcomes separately from local fixture/CI evidence. This author performs no
   production application, merge, deployment, capture, stop, retry or activation.

A known rollback of the schema transaction leaves the original layout intact.
An UNKNOWN apply ACK requires catalog inspection, never replay. After an
acknowledged apply, a catalog-only reverse cutover is possible only if there are
no prepared captures/receipts, no other new evidence DATA, no activated new writer
or concurrent reader dependency, and root has reviewed exact inverse DDL and
restoration of original ACLs. Never silently delete the 16 ledger rows or stamp.
After preparation begins, preserve all immutable prefixes/receipts and use a
reviewed forward correction. The old append-only Alembic downgrade expects a
table at the registration name and is not a valid reverse cutover here.

## Evidence and reviewer reproduction

The frozen earlier 132 offline / 14 PG / 15 composed results remain historical
mechanics evidence. Their minimal fixture could not certify the captured guard,
constraints or ACL shape. A NEW explicitly owned ordinary-role PG14 fixture
requires private pinned inputs and otherwise skips; it never falls back to a
production/test database URL. Original public names, owners, defaults, checks,
FK edges, indexes, trigger/function logic, views and ACLs are reconstructed.
Only private database/path/port/OIDs and harmless external FK-parent keys adapt;
external child tables retain the relevant FK edges and contain no business DATA.

The author packet records the unchanged old SQL's actual refusal, the corrected
packet/writer proofs, genuine server COMMIT with injected lost ACK, cleanup STOP,
sealing/concurrency, legacy images/PIT and before-DATA drift refusal. Counters
cover setup, ledger/stamp, successful, failed and rolled-back user-table DATA
operations across the fixture, including conservative bounds when PG14 cannot
read counters in an aborted transaction. System-catalog DDL and sequence identity
bookkeeping are distinct from business DATA. Raw failures/archives/databases
are retained privately. Root owns canonical reporting and final release evidence.