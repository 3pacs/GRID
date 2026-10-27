# Actor-news batches yield on budget or GDELT429

The serial intelligence scheduler's daily top200 and weekly tail actor-news
jobs opt into `ActorNewsPuller.pull_batch(actors, sources, *, budget_seconds=30.0)`.
Daily/weekly registrations, actor selection, source selection, held-job gates,
provider request parameters/timeouts, SQL, parsers and provenance stay unchanged.
Standalone `pull_one_actor(actor, sources)` retains its per-source return shape
and existing fetch-failure behavior. Batch state is cleared on every exit; a
puller/session is not supported for concurrent calls or reentrant batches.

In batch mode the first actual GDELT HTTP429 raises a private cooperative-yield
exception before raise_for_status. That exception is neither RequestException
nor ValueError, so the local catch and shared retry decorator cannot absorb or
retry it. The whole batch ends: no remaining source/actor requests, no Retry-After
sleep in the shared loop. Retry-After is returned as observed text (bounded to
128characters); it is not a durable cooldown. A newly constructed puller and a
later scheduled/standalone call do not inherit a provider cooldown. This change
does not claim a cross-process Retry-After scheduling policy.

The batch deadline uses monotonic time. It is checked before actors, sources,
new writes and throttle sleeps, and after throttle sleep before starting HTTP.
An exact deadline is exhausted; a throttle wait reaching/passing it is skipped.
This is a safe-point budget, not a hard wall-time bound or cancellation facility.
A running requests call or SQL transaction cannot be interrupted by it.
Requests timeout20seconds is an inactivity/connect timeout, not an absolute
wall-time cap; a slow response/SQL/CPU operation may overrun the30-second target.
No worker/thread, event-loop rewrite or nominal heartbeat thread is introduced.
The remaining serial jobs can run after the actor job returns; no exact hourly
completion or scheduler latency guarantee is asserted.

The result separates actors_requested/actors_attempted/actors_completed,
failed_actor_ids, deferred_actor_ids and incomplete_actors. On cooperative stop,
deferred IDs include the interrupted actor (if started) and every unstarted tail
actor. A deadline reached between actors has no incomplete current actor.
A regular actor exception records failure and continues at the next safe point;
completed means the existing actor routine returned, not that every provider
succeeded or any record was fresh. stop_reason=completed means enumeration ended,
not all actors succeeded. Counts reconcile completed+failed+deferred=requested
for valid actor inputs, but attempted additionally includes an interrupted actor.

source_rows_fetched totals per-source results across completed and incomplete
actors. It is not SQL affected/inserted row count: existing INSERT uses
ON CONFLICT DO NOTHING. Incomplete records retain their per-source fetched
counts and news/bio write_attempted/write_completed flags. Those flags describe
writer calls, not row-level commit receipts or a rollback guarantee. GDELT429
after current-actor Google retrieval occurs before the existing grouped news
write: fetched Google rows are unpersisted and discarded with the incomplete
actor. Budget expiry before a new write similarly leaves fetched rows unwritten.
If the news writer returned before budget expiry at the Wikipedia boundary,
news may already exist while the actor remains incomplete; no rollback is
attempted or implied. A failed/in-flight write has no new persistence attestation.
Scheduler logs now sum the actual source keys and label counts fetched, fixing
the previous news/bios lookups against nonexistent keys (which logged zero).

Deferred IDs are reported, not durably queued or automatically resumed.
Subsequent calls start the same actor ordering, so lower-ranked actors and the
weekly tail can starve under repeated budgets/rate limits. Fairness/continuation
requires a separate design; this patch claims neither eventual full coverage nor
complete news ingestion. No cadence/storage/source-quality policy changes here.

Historical basis: retained journal captured148GDELT warnings,141HTTP429, from
2026-10-17T03:30:25Z through03:52:52Z while the earlier scheduled actor-news job
occupied the serial loop across the hourly due window. This supports a backlog
mechanism, not an observed hourly-engine failure or a claim about current runtime.
Journal SHA2560370fd383c896e4d95b2783a0486507e3ecb4a3482c8e702f54ee71d9cce2534.

Prepared tests: `tests/test_actor_news_scheduler_budget.py` has17functions and
24parametrized cases, all UNRUN. Fakes replace requests.Session, time and writers;
no provider or DB is used. Cases cover first/late429/no subsequent HTTP/no backoff,
exact deadline/throttle/oversleep, prewrite expiry and partial existing writes,
legacy behavior/reset, errors/invalid budgets/reentry/empty batches, exact nested
scheduler wrapper source/count wiring and original registration/held-gate ASTs.
Original tests and ingestion/base.py are immutable. Future validation needs a
separately allocated bounded offline process with requests/SQLAlchemy/loguru and
pytest available (or independently reviewed faithful projections); preparation
contains no Python syntax/import/test execution and no claim that cases pass.
No provider run, runtime adoption, service restart, merge or publication is part
of this source-only candidate.
