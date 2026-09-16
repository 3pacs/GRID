# TODO: grid-realtime candle persistence across a restart

**Status:** OPEN — tracked, not blocking (grid-realtime activation is opt-in
until this lands; see `scripts/realtime_activation_gate.sh`)
**Discovered:** 2026-09-16 during PR #514 review
**Owner:** Anik (decisions) + agent (implementation)

## The current, actual problem (unchanged by PR #514)

`ingestion/realtime/flusher.py`'s `INSERT_SQL` is
`INSERT ... ON CONFLICT (symbol, interval, ts) DO NOTHING` -- the same
persistence semantics as `main`, not touched by the #514 deployment-path
repair (WorkingDirectory fix, drop-in backup/rollback, DB-connection
lifetime semaphore, opt-in activation gate, verification). "Idempotent"
(no duplicate or corrupt row can ever result) is true of this; "the most
complete candle wins" is NOT.

A candle truncated by a shutdown (`CandleBuilder.flush_all()`) keeps the
*interval-start* `ts_bucket` it was assigned at creation, not a
"time of shutdown" timestamp. So a truncated pre-restart candle and a
later, complete candle for the same bucket share an identical
`(symbol, interval, ts)` primary key -- and because the write is
`DO NOTHING`, whichever one lands FIRST wins permanently. That is provably
the truncated one: the old process's shutdown flush always completes,
successfully or not, strictly before a new process's candle for the same
bucket even starts.

**Proven against a real Postgres, not just described:**
`tests/test_realtime_shutdown_semantics.py::
test_truncated_candle_blocks_a_later_complete_candle_for_the_same_bucket`
writes a 1-tick truncated candle, then a 4-tick complete candle for the
identical bucket, and confirms the 1-tick truncated row survives
permanently -- the complete candle's extra 3 ticks are silently and
irrecoverably discarded.

This is a real, PRE-EXISTING risk -- it happens on every unplanned crash
`Restart=always` already recovers from today, not something #514
introduces. #514 does not fix it either; it only makes the restart itself
safer (correct working directory, bounded DB connections, backed-up/
rollback-able config, process-start + freshness verification) and gates
activation behind explicit human acknowledgment
(`activate_realtime` + `acknowledge_realtime_interruption`) rather than
asserting this pre-existing risk has been resolved.

## A proposed fix was explored, and reverted -- explained for context

An earlier round of #514 replaced `INSERT_SQL` with a source-aware merge
(`DO UPDATE`: additive for Binance, `GREATEST` for everything else,
`close` decided by write order). Two real, tested gaps were found in that
proposal on review:

1. **Binance close-selection under genuine reverse delivery.** Plain
   last-write-wins is correct for the only reachable production ordering
   (see above), but is not a general solution to true reverse delivery --
   nothing prevents or could detect a genuinely out-of-order write, since
   neither Binance's trade ID (`data["t"]`) nor its per-trade timestamp
   (`data["T"]`) survives past `CandleBuilder.ingest()` (confirmed by
   reading `feeds/binance.py` directly, not assumed -- the parser reads
   `data["T"]` only to compute the bucket floor and never reads
   `data["t"]` at all). An even earlier version of the same proposal tried
   to infer completeness from `trade_count` instead of write order, which
   was actively worse (trade count has no relationship to recency).
2. **Yahoo's `GREATEST`-based volume/trade_count merge under-counts
   genuinely distinct minutes.** `GREATEST` correctly turns a same-minute
   revision into a replace, but when the pre- and post-restart segments
   cover genuinely *different* minutes within the same 5-minute bucket,
   the true total is their sum, and `GREATEST` silently keeps only the
   larger side -- `CandleState` has no per-minute breakdown to tell the
   two cases apart.

Per review, this proposal was reverted out of #514 entirely -- the
deployment-path repair does not need it, and shipping it would have
quietly swapped one problem (`DO NOTHING` truncation, understood and
tested) for a different, only-partially-solved one (a merge with two of
its own open gaps), without a schema-level fix for either gap. **Do not
resurrect this proposal's tests as evidence that the design is finished**
-- two of its seven tests assert deliberately WRONG outcomes to document
the gaps above; they are a record of open problems, not acceptance
criteria.

The full proposal (SQL + all 7 original tests, including the two
known-limitation ones) is preserved at
`docs/realtime_candle_merge_proposal_tests.py` -- outside `tests/`
(pytest's `testpaths`), so it is not collected or run by CI. It's a
concrete starting point for whoever picks up the real fix below, not a
draft to merge as-is.

## What would actually fix this

Needs real identity/order data to survive aggregation, not a cleverer
UPSERT on the aggregates that are already there:

- **Binance**: add `last_trade_id` and/or `last_trade_ts` (and maybe
  `first_trade_id`) to `CandleState` / `realtime_candles`, threaded through
  from `feeds/binance.py`'s `data["t"]`/`data["T"]`. Close-selection and
  duplicate detection could then use real trade identity/order instead of
  write order and aggregate-equality.
- **Yahoo**: track per-minute state within the 5-minute bucket (e.g. a
  small `minute -> (volume, close)` map persisted alongside the candle, or
  a separate finer-grained table) so a same-minute re-poll can be
  recognized as a revision (replace) and a genuinely new minute can be
  recognized as additive (accumulate), instead of collapsing everything
  into one running total that can't tell the two apart.
- Once identity/order data exists, a merge SQL can correctly decide both
  `close` and `volume`/`trade_count` -- at that point
  `docs/realtime_candle_merge_proposal_tests.py` is a reasonable starting
  shape to adapt, not a reason to skip designing against the real data.

Both are schema + ingestion-path changes, not SQL-only.

## Notes for the agent picking this up

- Read `tests/test_realtime_shutdown_semantics.py::
  test_truncated_candle_blocks_a_later_complete_candle_for_the_same_bucket`
  first -- it's the current, accurate acceptance test for the PROBLEM.
  `docs/realtime_candle_merge_proposal_tests.py` shows one PROPOSED
  direction and exactly where it fell short; treat its two
  known-limitation tests as the bar a real fix must clear, not as already
  passing.
- Don't reach for another aggregate-only heuristic (a third notion of
  "which side is more complete") -- that's exactly the pattern that
  produced the trade_count bug in the reverted proposal. The fix needs
  more identity/order data captured at ingest time, not a smarter
  comparison of the data already being thrown away.
- Once a real fix lands and is tested against real Postgres (replacing
  `test_truncated_candle_blocks_a_later_complete_candle_for_the_same_bucket`
  with tests proving correct reconstruction instead), `scripts/
  realtime_activation_gate.sh` and the `activate_realtime`/
  `acknowledge_realtime_interruption` gate in `deploy.yml` can be removed
  (or the gate's stated reason updated, if some other bounded risk
  remains) -- grid-realtime would then belong back on the unconditional
  gate with grid-api/grid-hermes.
