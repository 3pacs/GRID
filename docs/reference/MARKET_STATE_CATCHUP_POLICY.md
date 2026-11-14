# #1191 source readiness and catch-up design

Status: isolated **non-wired policy prototype**; not a live scheduler or source
adapter. It does not change the verified readiness kit at 6e61b400. Existing
writer, refresh, PIT lags, timers and all stored rows remain unchanged.

## Owner check (2026-11-14)

The original author checkout is clean at b040f3f0. Latest Claude first-run ACK
accepts the findings as reported but says it made no source or server recheck,
and it did not relay to an author. No source-readiness integration ACK was in
the inbox at inspection. This is additional isolated work, not evidence the
other author is integrating it. Integration must check the author head again.

## Source lineage still missing in the actual path

Inspected `intelligence/regime/market_state.py`, `store/observations.py` and
`store/fred_vintages.py`, not production data:

| Input | Existing selection | Necessary adapter evidence |
|---|---|---|
| Raw macro, DIX | `read_window_known_at` returns Observation provenance, then `_Reads.series` collapses it to date/value | Retain source ID/name, series, actual selected row keys/pull timestamps/status, lag-policy version and ordered window hash; distinguish actual capture-before-cutoff from lag-inferred revised backfill. |
| ALFRED news | `load` returns obs/realtime_start/realtime_end/value; `values_as_of` chooses the valid vintage, limited to 45 days | Preserve selected vintage keys, canonical window bytes/hash, load/capture receipt and documented date-resolution semantics. Current query drops `fetched_at`. Do not fabricate an exact publication timestamp from a date. |
| Dealer gamma | Query reads date/value/regime/spot, not upstream capture provenance | Exact target SPY row plus underlying capture receipt, source identity and time bounds; a row dated correctly is necessary but not enough. |
| SPY trend | Resolved feature or yfinance fallback, one named basis per record | Retain chosen raw rows, corporate-action basis, resolver/version and capture lineage; basis name alone is not full evidence. |
| Event calendar | Loaded JSON dates | Preserve calendar bytes/version, recorded acquisition time and additions/corrections; hindsight calendar cannot prove the event was knowable then. |

No adapter is entitled to set `receipt_verified` merely because a hash string
exists. It must hash immutable actual bytes, validate the source identity and
selection/policy, and carry any timestamp uncertainty. A pull receipt does not
prove that its financial value was first public then. If only a publication
date is known, retain date precision in the future adapter rather than invent
an intraday instant; with this existing end-of-UTC-day contract, only a safely
bounded interval before cutoff may satisfy eligibility.

## Prototype contract

`intelligence/regime/capture_policy.py` is a pure, I/O-free planning function.
The caller supplies a verified, bounded complete session calendar, per-session
source requirements, verified receipt evidence and existing target dates.
Source expected dates are explicit: dealer SPY requires exact session, whereas
weekly/monthly/FRED/news must use their own publication calendar/vintage rules.
Do not apply a blanket same-day test or loosen current conservative lags.

Missing or duplicate requirements hold the target. Source mismatch, stale/wrong
observation, unverified receipt/vintage, post-cutoff publication and impossible
capture ordering hold it. Verified old vintages captured later can be replayed,
but are labeled historical replay, never contemporary capture. Every outcome
retains `forward_qualified=false`: source evidence alone cannot certify timer
origin, complete baseline-window lineage or five-session qualification.

A future production adapter must require the complete fixed source-policy
registry, not just whatever subset happens to be available, and verify the
entire selected calculation windows, not only the newest observation. That
registry and source adapters are intentionally NOT activated here.

## Session-aware schedule and catch-up

- Keep the existing exclusive cutoff at next-day **UTC 00:00**. Do not silently
  redefine it as US market close, even on half days.
- A daily UTC wake may inspect a bounded explicit session range after cutoff.
  The wake is not proof of data readiness and does not select a date via local
  machine timezone. No timer syntax or cadence is applied by this prototype.
- Friday is eligible Saturday after cutoff. Holidays produce no invented row;
  half days remain sessions. DST changes do not shift UTC cutoff. A frozen,
  independently verified complete calendar supplies sessions and coverage.
- Use a fixed inception/session range, not `max(as_of_date)+1`: later existing
  backfills must not conceal older missing sessions. Prototype range limit is
  366 calendar days, batch maximum20, default5, oldest eligible first.
- Report every target: already_present, not_due, policy hold, source hold,
  eligible_insert or deferred_batch_limit. Do not advance a global watermark
  past holes, and do not overwrite or requalify already-present rows.
- Future actual writes still need ON CONFLICT DO NOTHING and stored-winner
  identity. Planning is not a lease, database transaction or write receipt.
- A held target may be considered on a later scheduled wake without making a
  new provider request. Persistent missing-source policy should alert once with
  an exact source/target, not spin or rewrite history.
- Operational lateness and original knowledge time are separate: later receipt
  availability must not move a historical cutoff forward to make tests pass.

## Integration acceptance before any activation

1. Wire actual source adapters retaining selected immutable window lineage.
2. Freeze complete required-source policies including frequency/release rules.
3. Confirm session calendar manifest coverage and unusual closure corrections.
4. Add real disposable-Postgres planner→writer tests and concurrency coverage;
   keep current 42 SQLite/PIT +8 PostgreSQL tests intact.
5. Claude verifies integrated kit and exact check; Anik applies. Then prove five
   qualifying sessions with timer/run identity and stored receipt lineage.

Tests now cover weekly-vs-daily readiness; invalid/missing/revised/future inputs;
UTC midnight, DST, Friday/Saturday, holiday/halfday session fixtures;
existing-backfill preservation; holes and batch limits; calendar coverage;
and no forward claims. These are synthetic calendar fixtures, NOT a validation
of a live exchange calendar, real source capture, or production readiness.
