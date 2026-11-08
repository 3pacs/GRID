# SEC-derived signal invalidation: evidence-preserving implementation plan

Status: draft design, based on source head 43c6ab542. This document is not a
migration, production audit, repair authorization, or claim of complete coverage.
No production rows were read. Affected row counts and dates are unknown.

## Decision

Do not attach today's SEC generation to historical signals. The stored evidence
cannot prove which snapshots or even which input signals produced many mixed
outputs. Selective retrospective source binding is not safely recoverable from
current schemas. Keep divergence fanout suppressed. Implement a staged
current-read quarantine and explicit rebuild/publication contract before any
reactivation; preserve original signals, outcomes and decision history.

The unavoidable wider change is the lineage/publication boundary across adapters,
registry, Oracle predictions, derived stores and their caches. A patch to the
three SignalFired handlers or a row outcome of EXPIRED cannot close this boundary.

## Proven facts and selective identification limits

- `contracts/handlers/oracle_signals.py` writes canonical
  `signal_sources.source_type = fundamental_divergence`; `handlers/trust.py`
  also writes the event source such as `fundamental_divergence:Technology`.
  Existing contract IDs can identify direct rows, but neither family proves the
  financial snapshot used. Pending/delayed historical events need blocking too.
- `intelligence/signal_extractor.py::extract_from_signal_sources` preserves the
  family in `signal_data.signal_type` and a composite source_id. Those directly
  identifiable rows can be quarantined from current consumption without deletion.
  The extractor does not preserve the original signal_sources row ID or generation.
- `intelligence/adapters/trust_scorer_adapter.py::_convergence` stores source_count
  but no contributor IDs/types. Clean inputs and financial-contaminated inputs
  can produce exactly identical RegisteredSignal objects. The executable fixture
  in `tests/test_sec_derived_signal_lineage_limits.py` demonstrates this collision.
  Selective recovery from the registry payload alone is therefore impossible.
- Trust source-summary and lever-puller adapter rows often retain source_type in
  metadata and can be directly attributed. Missing metadata is not proof of safety.
- `SignalRegistry.register` inserts and never publishes an empty-result withdrawal.
  Old rows may survive an empty recomputation until TTL expiry. Some lever-puller
  signals remain valid for 14 days. TTL expiry is not an end-to-end repair.
- `contracts/handlers/journal.py` stores source/producer in contradiction_flags
  and source_contract_id. Direct provisional journal entries can be annotated.
  Actual human decisions and realized outcomes must remain readable as history;
  invalidating a supporting signal does not erase an action or its outcome.
- Oracle registry conversion drops input row IDs/metadata. Convergence influences
  confidence before only signals[:10] are persisted. An upsert can retain the
  maximum historical confidence while replacing the visible signal list. A
  clean-looking latest payload cannot prove its accumulated confidence is clean.
- Journal contamination can become market-wide: oracle/engine.py and
  oracle/trace_evolver.py, analysis/thesis_scorer.py and
  intelligence/sentiment_scorer.py select latest journal rows as regime without
  excluding provisional signal journals. Oracle also consumes aggregate journal
  feedback across tickers. A ticker-only quarantine is insufficient.
- Oracle model hit/PnL aggregates and verdict lessons, sentiment snapshots and
  thesis/convergence snapshots can carry mixed influence without complete input
  IDs. Quarantine must gate both current serving and subsequent learning/scoring
  consumption until verified rebuilds. Do not change genuine realized outcomes
  merely because their previous interpretation used an unsupported input.

Generic readers of raw signal_sources include trust, sentiment, edge weighting,
actor/causation modules and watchlist/MCP APIs. Raw signal_data is read across
canvas/feed/intel/actor bridges. Raw registry has callers outside SignalRegistry
(e.g. oracle/model_factory.py, oracle/engine.py and api/routers/canvas.py).
Further inventory requirements from transitive review:
- `trading/signal_executor.py` boosts paper-trade sizing/strength from convergence
  without passing contributor IDs into open_trade. Preserve execution facts;
  gate unsupported evaluative/training reuse rather than rewriting the trade.
- `intelligence/hypothesis_engine.py` persists Oracle-convergence hypotheses with
  model names/date rather than parent prediction IDs; historical ancestry is
  ambiguous and needs conservative withholding or independent reconstruction.
- reasoning_lessons, intelligence_embeddings, trade_postmortems and
  failed_prediction_postmortems retain parent prediction IDs. Those links enable
  precise exclusion once the parent prediction's qualification is established.
- oracle_models, confidence_bucket_history, meta_learning_matrix and
  signal_cooccurrence_history contain shared learned aggregates without complete
  per-input membership. Rebuild new verified clean learning epochs/baselines;
  a freshly SEC-tagged result using an old contaminated model is not clean.

- Journal entries also become generic prediction events in event_sequence.py;
  pattern_engine.py mines/persists them and pattern_adapter.py republishes them
  without original contributors. Sleuth actor-pattern evidence is truncated and
  its adapter retains only partial lead identifiers. These mixed families cannot
  be cleared merely because their output has no financial label.
- trust_scorer.py serves source_trust_scores_cached by age, and briefing reads
  persisted sentiment scores. Both need qualified publication/cache boundaries,
  not only filtered future inputs.

Inventory is not proof of contamination: the credit-cycle-only registry query
in oracle/engine.py and options-only registry query in canvas.py have no
established divergence dependency from this review. Do not blanket-quarantine
unrelated options scans. Check separate recommendation/journal paths individually.

Journal readers include journal history, regime/watchlist summaries, associations
and exports. Some are historical audit views, others are current decision inputs;
they must not be indiscriminately replaced by one view.

## Milestones and minimum acceptance requirements

### M1. Read-only inventory and policy manifest

Create an explicit inventory of each producer, live reader, historical reader,
materializer, training/scoring consumer and cache. Record dependency direction,
identity retained, refresh/TTL and output table. Use only a separately approved
read-only production inventory later to quantify rows and availability impact.
Do not infer contamination dates or affected counts from source history alone.

The manifest has three classes: directly attributable, mixed ancestry unknown,
and independently proven unrelated. Unknown is never promoted to unrelated by
absence of a financial label. Sign off the affected output families and the
expected temporary loss of coverage before activation.

### M2. Non-destructive quarantine and replay barrier

Add an append-only invalidation/qualification ledger keyed by immutable table-row
identity, reason, evidence reference, policy version and detection time. Keep raw
rows, outcome history and original metadata unchanged. Direct-family predicates
must use exact canonical name or colon-delimited prefix, not a broad substring.
Use producer/contract evidence when available; conflicting/malformed evidence
requires review rather than optimistic acceptance.

Provide separate current-qualified projections and historical annotated access.
Reject both canonical and sector-prefixed historical divergence events at all
three fanout handlers, including replays, while producer suppression remains on.
Route every current consumer through the projections or a shared qualification
API; audit/history interfaces retain records with explicit qualification status.
No raw-table fallback is allowed on missing views or qualification failures.

For ambiguous legacy aggregates, quarantine the affected writer family/epoch,
not guessed individual tickers. Define an independently verified clean publication
boundary; timestamps alone must not confer clean ancestry. Maintain this as a
reversible policy activation, not destructive updates or blanket deletion.

### M3. Complete future lineage and publication

At computation start capture the SEC/TTM generation and all input publication
identities. Propagate that original generation through SignalFired, both signal
writers, extraction, registry, journal context and dependent materializations.
Persist exact input row/version IDs and computation/policy version, or a
content-addressed dependency manifest; counts and display names are insufficient.

Publish adapter runs transactionally with a run ID, source generation, status and
complete scope. A successful empty run explicitly replaces/withdraws the previous
current publication. Failed, partial or superseded runs never replace it. Check
source identity before/after compute and publication, including postcommit status;
late writes are invisible and leave scheduler work retryable. Mixed-source
aggregates require complete dependency closure, not a fabricated single-row label.

Do not let historical max-confidence upserts contaminate a clean rebuild. Publish
new versioned prediction results and retain old predictions as annotated history.
Invalidate/rebuild dependent confidence, edges, actor summaries, feature stores,
API precomputes and caches from verified inputs. Preserve genuine outcome facts.
Exclude provisional signal journals from regime inference independently of source
family; retain them in annotated history. Include regime.py's unrestricted
fallback and current regime reads in watchlist_overview.py and dad.py. Explicitly gate legacy Oracle, thesis
and sentiment outputs from serving and learning while ancestry remains unknown.

### M4. Disposable qualification and controlled rollout

Run a full schema replay and populated-upgrade preservation test, then fixture
rebuilds in a disposable PostgreSQL database. Verify least-privilege grants,
indexes/query plans, latency, cache isolation and refresh cadence at representative
scale. A separate approved production rollout must coordinate schema, code,
quarantine activation and recomputation; do not drop qualification protections
as a rollback shortcut. Re-enable divergence fanout only after end-to-end current
read qualification passes. No historical row is automatically rebound.

## Required regression and impact matrix

1. Canonical, sector-prefixed, contract-attributed, malformed and unrelated rows;
   all direct historical evidence remains byte-for-byte unchanged.
2. Direct signal_data and provisional journal recognition; human journal history
   remains visible with qualification warnings, never silently rewritten.
3. Mixed trust convergence with identical clean/tainted payloads; broad ambiguous
   family quarantine is explicit, with measured coverage cost.
4. Generation changes after event creation, during handler delivery, during
   materialization, after cache hit and during commit; late writes stay hidden.
5. Replayed/duplicate events, old worker finishes last, partial handler failures,
   unavailable generation view and repeat observations of unchanged source data.
6. Empty successful refresh withdraws previous publication; failed refresh does
   not certify stale data. Scheduler failures remain due and retryable.
7. Oracle truncation and max-confidence history cannot carry old financial
   influence into a clean publication; cross-module convergence is rebuilt.
8. All live readers/caches use qualified publication identities; historical/PIT
   reads preserve what was observed then and expose later qualification status.
   Today's head must not be used to fabricate historical point-in-time evidence.
9. Schema replay, populated migration, roles/grants, rollback policy, independent
   provider outages and unchanged unrelated-signal behavior.

Expected impact: immediate reductions in financial signal coverage and possibly
all ambiguous trust convergence/Oracle-derived coverage until verified rebuilds.
Exact impact cannot be stated without the read-only inventory. Historical audits
remain available; current recommendations must disclose unavailable components.
No assertion that this draft design alone makes the deployed system safe is made.


## First implemented slice (draft, not deployed)

A machine-readable, explicitly incomplete source inventory is in
`SEC_DERIVED_SIGNAL_INVENTORY.json`. It records reviewed paths and dependencies,
which subpaths this slice guards, preserved history and unresolved materializers.
It contains no production counts and is not a completed transitive inventory.

`contracts/financial_signal_guard.py` recognizes the canonical financial event
by exact signal type, canonical/colon-prefixed source or producer module. All
three SignalFired handlers return before imports/provider/database work for that
family, including delayed/replayed events. This is a suppression barrier, not an
acceptance gate: adding a current generation to a payload does not bypass it.
Unrelated signal families keep their existing behavior.

`store/journal_qualification.py` provides a shared read-only predicate excluding
explicit provisional journal signatures and structured financial-event markers.
It is applied to the reviewed current-regime and Oracle journal-feedback reads,
including the regime endpoint's fallback and current weight context. It does
not filter journal history APIs, mutate original rows, erase outcomes, change
operator actions or certify the ancestry of remaining nonprovisional records.
A source_contract_id alone is deliberately insufficient for exclusion.

This first slice has no schema migration or production executor. Existing
persisted/cached predictions, source signals, registry rows, thesis/sentiment
snapshots, patterns and learned state remain outside its current-read guarantee.
Previously cached/materialized regime values also need their own publication
qualification. It therefore cannot establish end-to-end cleanup or justify
reactivating divergence fanout. The M1-M4 rollout gates above remain open.

Product decisions before broader activation: approve the explicitly measured
coverage reduction for ambiguous writer families; choose how unqualified history
is labeled; define clean learning-state rebuild/retention and qualified historical
PIT semantics. These decisions must not be replaced by silently deleting evidence
or stamping today's generation onto old results.


Local first-slice validation: 676 offline tests passed, 9 optional skips; 71
private PostgreSQL tests passed, including 36 predicate/preservation cases.
Replay, current-read fallback, empty synthesis and preserved history are covered.
Lint, syntax and whitespace checks pass. Exact-head CI remains a separate gate.


### Derived-signal qualification correction

The first G3/G5 run correctly rejected an outdated forward thesis digest before
executing tests. The 12 guarded reads now use fixed-column SQLAlchemy clauses and
bound values, preserving predicate/history semantics. Only the forward thesis
pin and new helper dependency are updated; all 14 HOLD seals, other forward
seals, test counts, isolation and rollback checks stay unchanged. Expanded local
validation passes 759 offline tests (9 optional skips) and 71 disposable PG tests.
All 15 forward and 14 HOLD source blobs verify. Fresh exact-head CI is required;
the failed predecessor is not accepted. No production access or deployment.
