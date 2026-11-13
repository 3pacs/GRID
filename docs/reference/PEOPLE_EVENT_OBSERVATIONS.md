# Committed people-event observations (candidate)

This is an explicit, unscheduled observer over canonical `people_events` and
`people_event_revisions`, not a new event ledger or a replacement writer.
`capture_observation(engine, request_id=..., acts=[(channel, dedup_key), ...])`
uses a fresh connection and an atomic READ COMMITTED statement to freeze the
exact full event/version rows and matching revisions. Mutable identity/source
fields are frozen as observed, never projected backward. The payload includes
source database/schema, PostgreSQL snapshot identity, source statement clock,
canonical IDs, version/content fields, revision IDs, source refs and provenance.

## Clocks and limits

The database observation clock is sampled **after** source retrieval. It proves
those captured source bytes were committed and observed by that time. It is not
the source-publication time, true first availability, historical event time,
receipt-commit clock, or downstream consumer-arrival time. The source statement
clock describes the snapshot boundary. A source revision can commit between
that statement and the later observation clock: the receipt intentionally keeps
the earlier consistent state, and a later request observes the change. Do not
claim the snapshot remained current throughout the capture window.

Payload SHA-256 binds exact PostgreSQL JSON text, without lossy reserialization.
A second receipt SHA-256 binds request identity, scope, contract, observation
clock and payload hash. Hashes detect accidental corruption, not hostile database
administration or prove source truth. Production clock synchronization and source
instance identity/custody remain operational requirements.

Scope is explicit, at most 100 canonical act keys. Capture caps are 10,000 event
rows, 10,000 revision rows and 4 MB serialized payload; exceeding any cap refuses
the entire receipt. These are acceptance limits, not a promise the server never
allocates more memory before checking size. SQL timeout bounds calls to 15s and
lock waits to 5s. Missing keys yield an explicit empty/partial snapshot, not a
negative signal or a claim of complete market coverage.

## Durable retry and failure behavior

- A new request ID means a new observation, never an update to an old receipt.
- Same ID + same normalized scope returns identical committed bytes and clock.
  Use the same ID after uncertain/lost acknowledgment; do not recapture under a
  new ID merely to recover an uncertain call.
- Same ID + different scope refuses. Same-ID concurrent calls serialize.
- The source reader never participates in or sees uncommitted writer data.
- Failure before receipt commit rolls back; retry observes again with a later
  clock. This does not reconstruct the lost observation.
- Receipt writes force transaction-local `synchronous_commit=on`, even if the
  session default is asynchronous. Durability still assumes PostgreSQL fsync and
  underlying storage honor WAL flushes; this does not promise survival of arbitrary
  hardware/storage corruption or synchronous replica availability.
- Commit followed by process death/ACK loss leaves the receipt recoverable.
- Corrections and retractions appear in subsequent receipts alongside full
  canonical version and revision identities. Prior snapshots stay immutable.
- The table refuses UPDATE, DELETE and TRUNCATE. Downgrade locks the table and
  refuses nonempty data. No runtime CREATE TABLE or automatic migration.

`read_observation` is SELECT-only and verifies both hashes. It does not qualify
historical evidence or produce a trade signal.

## Integration remains held

The foundation reader's `commit_visibility_unproven` gate is unchanged, and this
contract deliberately differs from its synthetic `people-events-post-commit-receipt-v1`
input. No automatic conversion to that qualifier is provided. A later design must
specify exact decision-time capture/consumption, handling missing observation
intervals and within-window revisions, source instance identity, coverage and
correction history; prove that end-to-end behavior before admitting a separately
registered prospective construction. This implementation cannot repair preexisting
history or retroactively certify known_at/ingested_at/recorded_at.

No job, writer, scheduler, frozen study or provider call invokes this code.
Migration graph and user-run apply remain review/release tasks. Claude verifies
the kit; Anik applies any eventual release.


## Current-main integration (2026-11-13)

Reconciled onto main `e7d2b029c7ca6209960cddf040f4c74d3515a125`.
This candidate adds only the observer files; it does not replay the older
foundation branch or overwrite its readers. The additive migration now follows
`finra_dix_feature_20261113`, preserving one Alembic head. The regression suite
checks the revision path and tests actual observation DDL on disposable PostgreSQL.
This is not a full all-history migration rehearsal, nor production activation.
