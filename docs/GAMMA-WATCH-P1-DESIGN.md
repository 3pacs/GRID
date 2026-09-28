# Gamma Watch P1: canonical receipt ingestion design

Status: **design for coordinator review; no writer or activation**. This PR
does not change the deployed collector, bridge, trading scores or frozen
GEX-levels v1 inputs. The independent collector source-import PR is a baseline,
not implementation of this design. Paid APIs/LLMs remain off; no orders.

## Sources are meanings, not vendors

`config/gamma_watch_sources.json` is the reviewable source-catalog contract.
Database-assigned IDs are resolved by unique name, never hard-coded. Each
meaning has a separate source_id: vendor-delayed GEX, partial RTD-model GEX,
frozen-chain scenarios, Yahoo unadjusted intraday and Yahoo adjusted daily.
Additional distinct contracts cover RTD quotes, structural RTD, Cboe delayed
chains, each official provider, rebalance sensitivity, sampled responses and
option scenarios. A new meaning/sign model/price basis requires a new contract
version; corrections append evidence and never redefine an existing source.

The proposed migration seeds these sources inactive, priority 1000, low trust,
PIT-unavailable pending acceptance, revision behavior FREQUENT. FREE means no
incremental paid API wired here, **not** unrestricted redistribution or free
broker entitlements. RTD entitlement/license verification remains an activation
gate. Catalog latency has only REALTIME/EOD/WEEKLY/MONTHLY; REALTIME is used
solely as its coarse intraday bucket, never as a freshness claim. The immutable
companion contract's `latency` and `basis` are mandatory for reads/display.
Readers incapable of expressing delayed/unknown latency must withhold these
sources. No enum expansion or silent relabeling of existing feeds in this PR.

Canonical scalar series use a `GW1:` namespace, e.g.
`GW1:SPY:vendor:call_wall:USD`, `GW1:SPY:rtd_model:paired_otm:iv0:gex:USD_1PCT`,
`GW1:SPY:yahoo_intraday:close:USD`. Full contract, expiry, strike, side, field,
units, price basis and model version must be unambiguous; curves/chain arrays
remain immutable captures rather than flattened into misleading scalar rows.
No `SPY`, `YF_ADJ:` or existing dealer-engine series IDs are reused. Vendor GEX
units must be established from its schema before scalar admission; the label
alone is insufficient. Gamma Watch local GEX is USD per 1% spot change, with
its displayed billions converted to USD. GRID's existing dealer calculation
uses a different scale; P2 must normalize and compare matched inputs.

## Timestamp lineage

Every envelope keeps these distinct fields:

| Field | Meaning |
|---|---|
| source_time_raw | Provider's exact timestamp string, even when timezone unknown |
| source_event_at | Parsed UTC event/publication time only when semantics and timezone verified; otherwise null |
| collector_received_at | GRID-owned collector's receipt of raw bytes, before normalization; clock trust must be recorded |
| journal_received_at | Durable append receipt; not a provider event or backdated market observation |
| available_at | Earliest **proven** GRID receipt at which this exact value existed |
| canonical_ingested_at | Time of canonical transaction, including replay/backfill |
| computed_at / chain_as_of | Derived output generation and input dates in lineage; neither replaces receipt availability |

For new captures with monitored/verified collector clock, available_at is
collector_received_at. Otherwise use trusted server journal receipt. Derived
availability cannot precede the computation receipt or any parent input's
availability. All timestamps are timezone-aware UTC; unexpected future clocks
quarantine the value, rather than truncating timestamps to now. Store the raw
time and clock evidence. An RTD callback is not an exchange quote time.

Legacy SQLite has no UUID or hash chain and does not archive all snapshots.
Freeze a read-only SQLite backup/export with an approved identity manifest,
schema hash and complete-row hashes; do not write metadata into the source.
Existing row receipt may be used only after audit establishes its provenance;
otherwise available_at is the new admission receipt (not historical known-at).
Canonical-ingested-at always remains the actual new write time. Source dates
alone never justify replay availability. This conservative legacy path cannot
be used for a historical directional-edge claim.

`raw_series.pull_timestamp` must equal accepted available_at, not replay wall
clock, because `store.observations` filters it for as-of reads. Save actual
ingestion time in the capture/admission linkage. `obs_date` is the documented
market/publication date; future auction dates stay payload fields, not future
price observations. Existing readers collapse intraday values by obs_date;
intraday consumers need an explicit receipt-granularity API in a later PR.
Do not feed daily resolver outputs to an intraday backtest. Preserve
FIRST_RELEASE/LATEST_AS_OF semantics per source and explicit source filters.

## Success and quarantine

Receipt persistence and scalar admission are different decisions. Keep original
bytes, source error and absence even when nothing qualifies for raw_series.
`raw_series.value` is NOT NULL: missing values never become zero or a made-up
FAILED numeric row. Record FAILED/PARTIAL/QUARANTINED receipt evidence and skip
the absent scalar. A real numeric zero is admissible when valid.

- SUCCESS: valid finite value, declared unit/basis, valid source identity,
  complete required lineage, proven availability, acceptable per-field age and
  coverage for that specific meaning. SUCCESS means usable **for that declared
  purpose**; modeled SUCCESS remains research-only, never dealer inventory.
- PARTIAL: intact response with missing fields/partial chain. Individual fields
  may be admitted only under an explicit field policy; no aggregate defaulting.
- FAILED: transport/provider failure or missing response. Store error receipt,
  never recycle last successful data with a new receipt timestamp.
- QUARANTINED: malformed/nonfinite values, unknown units/identity, provenance or
  clock contradiction, expired/incorrect subscription scope, invalid model
  dependencies, or integrity conflict. Preserve bytes and reason codes.

Hash/identity/cursor conflicts stop the whole stream before cursor advancement.
For other rejected records, persist the complete rejection atomically and allow
the cursor to advance; replay must not silently skip evidence. Discovery of a
bad previously admitted scalar uses GRID's reviewed QUARANTINED/retraction
path; do not delete captures or overwrite their initial classification. Later
admission/retraction decisions need append-only audit evidence in the writer
implementation. No such production update is authorized by this design.

RTD-model policy must exclude expired series and require age/coverage per
relevant field, not merely a 90% subscription count. OI date and exchange
quote time currently unknown: retain limitations, withhold claims requiring
them. Frozen scenarios may be valid historical research while stale for live
decisions. Official empty arrays are no-records-returned, not zero flow.
Rebalance estimates remain per-initial-billion sensitivities, not actual orders.
Archive status never grants statistical significance or trading permission.

## Database identity, replay and idempotency

Future journal metadata: random database_uuid plus epoch_uuid, schema version,
collector version, creation receipt, public identity hash (no secrets). A file
replacement/recreation or stream reset starts a new epoch; path, mtime or file
size are not identity. A restored prefix is never treated as new unseen data.
Legacy frozen manifests receive an explicit import identity and immutable
manifest hash; no generic live-rowid replay is permitted for unaudited legacy DBs.

Identity is `(database_uuid, epoch_uuid, stream, sequence)`. Keep payload SHA256,
previous-record SHA256, per-stream highest sequence and last hash. Validate
canonical JSON encoding for envelopes, hash raw bytes separately, reject
duplicate sequence with different bytes, unexpected sequence gaps, shrinkage,
changed prior hashes, missing parents and clock changes. Exact duplicate
identity/hash is a no-op; same bytes at a genuinely new receipt is a new event.

Future writer transaction:
1. Read immutable source page; verify database/epoch and previously acknowledged
   boundary hash. Lock one cursor row/advisory key for this stream only.
2. Compare existing capture on conflict: identical hash/metadata is idempotent;
   difference rolls back and alerts. Never `ON CONFLICT DO UPDATE` evidence.
3. Insert captures, dependency links and valid namespaced scalar raw rows plus
   admission mapping. Each field identity is unique; scalar write and mapping
   occur in the same transaction. No raw_series ID FK is presumed (existing
   schema's id does not universally have a unique constraint).
4. Update cursor using compare-and-swap against old sequence/hash, after all
   records persist, within the same transaction. Commit once. Crash before
   commit leaves no cursor advancement; crash after commit replays as no-op.

Bound page size, bytes and transaction duration. Backfill only actual recorded
rows; no regeneration of historical curves using today's time/spot/IV. No
options_snapshots writes or locks, no source mixing, no generic resolver
activation. Scalar resolver participation and authorized intraday read APIs
are separate acceptance gates after the writer exists.

## Immutable raw and model capture

Capture raw successful and failed provider responses **before** summarization;
capture RTD fields with individual callback times/validity and the exact
subscription manifest. Never store credentials, cookies, account data or PHI.
Record request identity without authentication headers. Preserve raw bytes and
hash, parsed-envelope version, provider/source metadata, server receipt and
collector code/config hashes. Publish via append + durable flush/atomic commit;
never replace yesterday's file or use overwritten snapshot.json as history.

For every model result capture input IDs/hashes, spot basis/receipt, expiry
calendar/version, contract multiplier, r/q, IV policy/recovery donors, OI date
or explicit unknown, dealer-sign scenario, field exclusions/coverage, units,
grid/root algorithm, code/config/dependency versions, computation receipt and
full output. A model must reference already committed input captures. Persist
successful and withheld results, including reasons. Persist full ZeroGEX,
Cboe and Yahoo input versions; existing journal summaries are insufficient.
Do not archive just a mutable API envelope with a refreshed `served_at` clock.

The proposed PostgreSQL schema retains exact bytes and immutable relationships;
UPDATE/DELETE/TRUNCATE triggers guard evidence tables. Application-role grants
must additionally exclude DDL, trigger disable, deletion and evidence updates;
owner/superuser actions require auditable controls. These permissions and hash
verification are not implemented by the design-only entrypoint. Model DAG
ordering and parent availability checks are writer acceptance tests, not implied
by FK existence. Cursors are mutable only by the transactional writer.

## Migration and orchestration: intentionally inert

`migrations/proposed/gamma_watch_p1_20260928.py` is an Alembic-format migration
outside the active versions directory. It is **not applied**, not part of
`alembic upgrade head`, and raises without explicit schema approval even if
called manually. It adds capture/identity/source-contract/dependency/cursor/
admission tables, append-only guards, and inactive source catalog entries.
Promotion requires rebase of down_revision onto then-current main, isolated
PostgreSQL contract tests, owner migration GO, backup/dry-run/GO for data
changes, and coordinator review. Downgrade fails closed to preserve evidence.
No destructive rollback and no scratch DB on the production cluster.

Both systemd templates have no Install/WantedBy section and an approval-file
condition. Service runs from the release tree as grid, with the existing env
file location. The entrypoint defaults false, rejects blank/invalid booleans,
and **also refuses true** because the writer is not implemented. No import of
DB/collector modules, no IO on disabled execution. The timer proposes a bounded
22:45Z weekday replay, outside backup, Persistent=false. Intraday cadence is
a later reviewed change; this template is not a claimed real-time integration.
No install script, CI deployment hook or enable/start command is added.

## Acceptance before implementation/activation

- SQLite fixtures must include source_catalog when exercising observations reads;
  test zeros/nulls, explicit source filters, as-of boundary, adjusted/raw isolation.
- Crash/replay, duplicate identical record, conflicting payload, changed DB/epoch,
  rollback, gaps/shrinkage and concurrent cursor writers; no state on failure.
- Hash exact bytes, persist failed input, reject absent/future parent and changed
  model version; stable replay independent of today's clock/market data.
- PostgreSQL FK/check/append-only/permission tests on an isolated test server,
  inactive catalog seed collision rollback, no UPDATE of existing meanings.
- Full field-age, expired subscriptions, partial chain, unknown OI age, clock
  skew, DST/early-close tests; stale remains visible and missing never becomes 0.
- Source/payload manifest tied to approved collector version; no new paid API,
  no trading, no GEM/options_snapshots/frozen-v1 writes; independent review.
- Point-in-time consumers opt in explicitly. FDR/preregistry/holdout/net-cost
  outcomes are P3, not supplied by infrastructure or a source SUCCESS status.

Open PR only. Coordinator reviews and merges. September 29 from 13:30Z until
owner confirms GEM containment: no main merge. No migration or activation
is authorized by accepting this design or merging the separate source import.
