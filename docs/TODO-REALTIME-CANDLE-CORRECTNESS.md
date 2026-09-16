# TODO: grid-realtime candle-merge correctness across a restart

**Status:** OPEN — tracked, not blocking (grid-realtime activation is opt-in
until this lands; see `scripts/realtime_activation_gate.sh`)
**Discovered:** 2026-09-16 during PR #514 review
**Owner:** Anik (decisions) + agent (implementation)

## The problem

`ingestion/realtime/flusher.py`'s `INSERT_SQL` merges a truncated
pre-restart candle with the post-restart continuation for the same
`(symbol, interval, ts)` bucket. It can only operate on the aggregate
OHLCV values each side already computed (`CandleState` in
`candle_builder.py`) — it has no access to the individual trades or polls
that produced them, because that identity is discarded before it ever
reaches the merge:

- **Binance** (`feeds/binance.py`): each `@trade` message carries its own
  trade ID (`data["t"]`) and execution timestamp (`data["T"]`). The parser
  reads `data["T"]` only to compute the candle's bucket floor and never
  stores it; `data["t"]` is never read at all. Once
  `CandleBuilder.ingest()` folds a tick into a `CandleState`, there is no
  remaining record of which specific trades contributed to it, or which
  one produced the close.
- **Yahoo** (`feeds/yahoo.py`): each 60s poll re-downloads the *latest*
  1-minute bar and ingests it as one tick. `CandleState` tracks only the
  5-minute bucket's running OHLCV totals — nothing at the individual-minute
  level, so two segments covering different minutes within the same
  5-minute bucket look identical, structurally, to two segments reporting
  revisions of the *same* minute.

Two concrete, tested consequences (see
`tests/test_realtime_shutdown_semantics.py`):

1. **Close-selection is "last write wins," not "true latest wins."** For
   the one reachable production ordering (a restart: the old process's
   shutdown flush provably completes — successfully or not — strictly
   before the new process's first write), write order coincides with
   chronological order, so this is correct in practice. But it is provably
   *not* a general solution: nothing prevents (and nothing could detect) a
   genuinely out-of-order write, since no trade-level timestamp survives to
   check against. An earlier version of this merge tried to infer
   completeness from `trade_count` instead of write order, which is
   actively worse — trade count has no relationship to recency (a later,
   correct segment can easily have fewer trades than an earlier one in a
   quiet period), so that version could retain a stale close even in the
   normal, non-reordered case. Reverted to plain last-write-wins.
2. **Yahoo's `GREATEST`-based volume/trade_count merge under-counts
   genuinely distinct minutes.** `GREATEST` correctly turns a same-minute
   revision into a replace (a strictly-growing re-report of one minute
   naturally has the larger volume, so `GREATEST` picks it) — but when the
   pre- and post-restart segments cover *different* minutes within the
   same 5-minute bucket, the true total is their sum, and `GREATEST`
   returns only the larger side, silently dropping the other side's
   distinct contribution. Never inflates (the property the original fix
   was for), but does not fully reconstruct the bucket either.

Also worth being honest about: the "Binance trades are delivered exactly
once" assumption behind summing its volume/trade_count is a property
claimed of the *upstream* WebSocket feed and the parser's own resubscribe
behavior, not something this codebase independently verifies. Without
persisted trade IDs there is no dedup check that would catch an accidental
redelivery (a reconnect race, a duplicate message) — the exact-duplicate
no-op guard in `INSERT_SQL` only catches a *literal* resend of an
identical aggregate batch (e.g. a retry after an ambiguous write failure),
and equality of the four aggregate fields is a heuristic proxy for "same
batch resent," not proof of identical underlying trades.

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

Both are schema + ingestion-path changes, not SQL-only. Scoped out of the
PR #514 deployment-path repair (drop-in backup/rollback, DB-connection
bound, WorkingDirectory fix) on purpose — that repair proceeds now with
grid-realtime activation kept opt-in (`activate_realtime` +
`acknowledge_realtime_interruption`, mirroring grid-scheduler's existing
gate) rather than asserting routine restarts are fully safe for candle
data, which is not yet true.

## Notes for the agent picking this up

- Read `tests/test_realtime_shutdown_semantics.py` first — the four
  scenarios there (disjoint pre-/post-restart, exact-duplicate replay,
  Yahoo minute-revision-vs-distinct-minute, Binance close-under-reverse-
  delivery) are the acceptance criteria; a real fix should turn the two
  documented-limitation tests into documented-fixed ones without breaking
  the others.
- Don't reach for another aggregate-only heuristic (a third notion of
  "which side is more complete") — that's exactly the pattern that
  produced the trade_count bug this doc replaces. The fix needs more
  identity/order data captured at ingest time, not a smarter comparison of
  the data already being thrown away.
- Once this lands and the new tests are green, `scripts/
  realtime_activation_gate.sh` and the `activate_realtime`/
  `acknowledge_realtime_interruption` gate in `deploy.yml` can be removed
  (or the gate's stated reason updated, if some other bounded risk
  remains) — grid-realtime would then belong back on the unconditional
  gate with grid-api/grid-hermes.
