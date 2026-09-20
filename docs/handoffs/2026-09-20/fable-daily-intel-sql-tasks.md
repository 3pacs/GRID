# Daily-intel SQL tasks: slow-query diagnosis, done_late ledger outcome (review-only)

Branch: `fable/daily-intel-sql-tasks-20260920` off `origin/main` @ `ac10aff8`
Scope: development only. **Draft PR, no merge, no production access, no DB/SSH
used to build this.** Evidence below is a read-only production journal
excerpt the controller supplied; nothing here was re-derived against a live
database.

Controller's instruction: do **not** simply raise the two SQL-task budgets
(`capital_flow_rollups`, `fundamental_divergence`). Use the execution
evidence to tell slow query apart from contention, establish actual
completion times, and fix the query (or make a bounded, evidence-justified
budget change) instead of papering over the timeout — and stop the
"abandoned — exiting without publishing" pattern from silently repeating
the same successful late write three times per cycle.

## Evidence (production journal, 2026-09-20, three attempts, identical pattern)

```
07:49:13  corporate_actions: 0 rows from 38 filings
07:50:13  Step 'daily_intel:capital_flow_rollups' timed out after 60s
07:51:13  Step 'daily_intel:fundamental_divergence' timed out after 60s
07:51:13  compute_ttm failed: (psycopg2.errors.QueryCanceled) canceling
          statement due to statement timeout
07:51:29  capital_flow_rollups.fold_announcements: 90 rolled rows
07:51:29  daily_intel task capital_flow_rollups abandoned — exiting
          without publishing
07:52:16  fundamental_divergence: scored 1 tickers across 16 sectors
07:52:17  fundamental_divergence: wrote 1/1 rows; long=0 short=0 aligned=1
07:52:17  daily_intel task fundamental_divergence abandoned — exiting
          without publishing
```
Repeated identically at 08:06:41/08:08:41 and 08:21:19/08:23:19 — three
attempts, same shape, same 90-row fold and 1-ticker divergence result every
time.

DB-checkout holds during the attempts: `capital_flow_rollups`' worker held
120.1s then 15.0s (matches the 120s cancel + 16s fold-to-completion split
above); `fundamental_divergence`'s worker held 122.7s in ONE hold, no
`QueryCanceled` logged. No lock waits were observed anywhere in the window,
and the unrelated flow-materializer systemd timer ran entirely outside
these attempts (08:05:09-08:05:25) — ruling out lock contention for both
tasks. Table facts: `capital_flows` 238 MB / ~470k rows (quarter 309,915,
annual 162,298, ttm 4,200, announcement 105); `fundamental_divergence` 14
MB / 36k rows; `institutional_holdings` 24 MB / 70k.

`db.py`'s engine sets `statement_timeout=120000` (120s) on every
connection (`GRID_DB_STATEMENT_TIMEOUT_MS`, `db.py` around
`create_engine`) — that is what cancels `compute_ttm`'s statement at
exactly 120s from task start, independent of the 60s `_run_with_timeout`
wrapper budget that abandons the *step* 60s earlier.

## Diagnosis per task

### `capital_flow_rollups` — intelligence/company_financial_rollups.py

**Slow query, not contention.** `compute_ttm`'s `_TTM_UPSERT_SQL` (before
this change) ran a `ROW_NUMBER() OVER (...)` dedup pass followed by a
`SUM(...) OVER (... ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)` trailing
window, **over every `period_type='quarter'` row in the table** — all
309,915 of them, for all ~3,500 actors, on **every single daily-intel
cycle**, regardless of whether an actor's quarterly data had changed since
the previous run. There is no filter narrowing the computation to actors
with new data; the existing index
(`idx_capital_flows_actor_period(actor_id, period_type, fiscal_period
DESC)`, migration 0046) helps a per-actor lookup, not a whole-table
window/sort. That full recompute is what statement_timeout cancels at
120s, every attempt, without exception (`ttm=0` logged all three times).

**Established completion time:** `compute_ttm` never completes — cancelled
at exactly +120s from task start on all three attempts
(`QueryCanceled ... statement timeout`). `fold_announcements` (which only
scans the tiny `period_type='announcement'` table — 105 rows — and was
never the slow part) completes 16s later (+136s total). That 16s
"successful late write" (90 rolled rows) is the one that was repeating
identically three times per cycle, since the orphaned worker always
reaches the same fold result and the ledger never recorded it.

**Fix:** bound `compute_ttm` to actors with a new/updated
`period_type='quarter'` row (by `as_of`) in a trailing lookback window
(`TTM_LOOKBACK_DAYS = 3`) via a new `changed_actors` CTE, using the
existing `capital_flows.as_of` column (already written by every
quarterly-row writer — no new column, no migration). The window
computation still needs each changed actor's *full* quarterly history to
sum a correct trailing-4-quarter total — only the *actor set* is bounded,
not the per-actor lookback, so correctness (and idempotency — ON CONFLICT
DO UPDATE is untouched) is unaffected. `compute_ttm(engine,
lookback_days=None)` is kept as an explicit full-recompute escape hatch
(see the corrected classification below — it is not currently wired to
any `scripts/run_capital_flow_rollups.py` CLI flag). No index change was needed: the `changed_actors` CTE's
`WHERE period_type='quarter' AND as_of >= ...` filter is an unindexed
sequential scan, but at 238 MB / ~310k quarter rows that is a
sub-second cost — the removed cost was the window/sort computation across
the whole table, not the scan itself. See "Migration" below for why no
index was added regardless.

### `fundamental_divergence` — intelligence/fundamental_divergence.py

**Slow-by-volume N+1 query pattern, not one slow statement, not
contention.** `compute_divergence` iterates `_load_universe()` (SECTOR_MAP
flattened: 3,533 actors / 262 subsectors, ~1,500 tickers after ticker
dedup) and calls, **per ticker**: `_load_ticker_fundamentals` (1 query
against `capital_flows`) then `_load_ticker_price_cagr` (up to 3 queries
against `raw_series`: COUNT, latest-close, prior-close). That is
~4,500-6,000 sequential round trips on one connection for a full universe
pass. No `QueryCanceled` was ever logged for this task across all three
attempts — consistent with no single statement ever approaching the 120s
statement_timeout; the ~120-124s DB-checkout hold is round-trip/parse/plan
overhead multiplied by thousands of small, individually-fast, correctly
indexed queries. This is the textbook N+1 shape, not a missing index and
not lock waits (none observed in the evidence window).

**Established completion time:** ~124s for a full pass (one 122.7s DB
checkout hold, no cancellation) — safely over the 60s wrapper budget every
time, landing the exact same result (1 ticker scored, NVDA, 1 row written)
on all three attempts because the universe and data were unchanged between
attempts within the window.

**Fix:** replace the per-ticker loop with `_load_batch_fundamentals` and
`_load_batch_price_cagrs` — the same SEC-over-seed dedup ranking and the
same count/latest-close/prior-close logic, each now issued **once for the
whole universe** (one `capital_flows` query with `UPPER(actor_id) =
ANY(:tickers)` grouped by ticker, one `raw_series` COUNT(*) query, two
`raw_series` `DISTINCT ON (series_id)` queries) instead of once (or three
times) per ticker. `raw_series` already carries
`idx_raw_series_series_obs(series_id, obs_date DESC)` (schema.sql), so the
batched `DISTINCT ON` queries are index-backed with **no migration
needed**; `capital_flows`' annual rows (~162k, a fraction of the table)
are cheap to sequential-scan once per cycle instead of once per ticker.
`_load_ticker_fundamentals` / `_load_ticker_price_cagr` are kept,
untouched, for `tests/test_fundamental_divergence_sec_priority.py`'s
drift guard (it asserts on `_load_ticker_fundamentals`'s embedded SQL
text) and any manual/debug per-ticker use — they are simply no longer on
`compute_divergence`'s hot path.

**Trade-off disclosed:** batching loses the old code's per-ticker
isolation — a bad row for one ticker no longer costs only that ticker; a
batch-query exception now costs the whole universe that metric for the
cycle. Both batch loaders keep the same fail-soft shape (log + return
`None`/empty) at that coarser granularity, matching how `_table_exists`
already gates the whole function today.

## Stale-write classification restated (post-review, `TTM_LOOKBACK_DAYS=3`)

CI review flagged that the diagnosis above doesn't say plainly what
`compute_ttm`'s bounding does to `ttm` freshness, so restating without
softening it:

- `capital_flow_rollups` is now an **INCREMENTAL writer for `ttm` rows**,
  not a full recompute: `run_all`'s `compute_ttm(engine)` call (the only
  one on the daily hermes path) recomputes a `ttm` row only for actors
  with a `period_type='quarter'` row whose `as_of` falls inside the
  trailing `TTM_LOOKBACK_DAYS = 3`-day window.
- A `ttm` key for an actor with **no** new quarterly row in that window
  goes stale and **nothing rewrites it automatically** — not the next
  daily cycle, and not `scripts/run_capital_flow_rollups.py` either:
  every path that script exposes today (bare invocation → `run_all`,
  `--ttm-only`, `--rollup-only` skips `compute_ttm` entirely) still calls
  `compute_ttm(engine)` with the same bounded 3-day default. The
  `compute_ttm(engine, lookback_days=None)` full-recompute escape hatch
  exists as a function argument (and is unit-tested), but **no CLI flag
  wires it up** — the docstring's claim that
  `scripts/run_capital_flow_rollups.py` is how you'd reach it is
  currently wrong. Today the only way to force a full `ttm` recompute is
  calling `compute_ttm(engine, lookback_days=None)` directly (e.g. a
  one-off Python/REPL invocation) — there is no packaged/CLI path. That
  gap is disclosed here, not fixed, since wiring a CLI flag is outside
  this PR's three scoped tasks.
- `fold_announcements` is **unaffected and remains a full recompute every
  run** — `_ROLL_UPSERT_SQL` groups ALL `period_type='announcement'` rows
  unconditionally on every call, with no changed-actor scoping. So
  `announcement_rolled` (`period_type='annual'`) rows do not have this
  staleness class; only `ttm` rows do.

## Migration: not included, deliberately

Both fixes remove full-table **window/sort computation** or **per-row
round trips**; neither fix depends on a new index to hit budget by my
reasoning above (unindexed sequential scans over 162k-310k rows are
sub-second; the removed costs were orders of magnitude larger). Adding one
would be a guarded path in a review-only draft PR for a benefit I can't
size without production `EXPLAIN ANALYZE`, which I don't have access to
here (no DB, per the controller's constraints). Disclosed, not shipped: if
production profiling after this lands shows the `changed_actors`/batched
scans themselves are non-trivial, the natural follow-ups are
`idx_capital_flows_quarter_as_of ON capital_flows(as_of) WHERE
period_type='quarter'` and a functional index on `UPPER(actor_id)` —
neither is included here.

## Ledger fix: `done_late` outcome (scripts/hermes_operator.py)

Root cause of the *3x-repeated identical late write*: `_run_daily_intel_block`'s
attempt-token bump ran **eagerly**, the instant `_run_with_timeout` reported
`ok=False` (the 60s wrapper timeout) — before any retry had actually
started. That made every late-but-successful orphan return look
indistinguishable from "a retry already started, reject it," so the
belated success (`fold_announcements`'s 90 rows; `fundamental_divergence`'s
1-ticker write) was *never* recorded in the ledger, `daily_intel_done`
stayed unset, and the next cycle retried the identical work from scratch —
forever, three times in the captured window alone.

Fix: the driver no longer bumps the token on a wrapper timeout. It instead
sets a new `entry["timed_out"]` flag. The orphaned worker's own
`_run_task` epilogue (which already runs, in the orphan's own thread, the
instant `task.fn` returns — however late) now checks, under the same lock:

- **token no longer current** (a genuine NEW attempt registered since —
  the only thing that still bumps the token) → reject, exactly as before:
  "abandoned — exiting without publishing." This is the real overlap case.
- **token still current, `timed_out` set, `task.fn` returned
  successfully, same period, not already done** → `state.daily_intel_done[name]
  = period_iso`, `state.daily_intel_task_outcome[name] = "done_late"`.
  The task stops being re-attempted. The `results` payload is **still
  never published** — nothing downstream is waiting to consume it.
- **token still current, `timed_out` set, `task.fn` raised** (or the
  period rolled over, or it's already done) → reject, unchanged: a late
  FAILURE never marks done.

### Tests (`tests/test_hermes_daily_intel_resumable.py`)

- `TestDoneLateLedgerOutcome::test_g_late_return_after_timeout_marks_done_late_not_done`
  — (a) timeout then late success → `done_late`, no retry on the next call.
- `TestDoneLateLedgerOutcome::test_superseded_late_return_is_rejected_not_done_late`
  — (b) timeout, a genuine retry registers a new token, THEN the old
  worker's belated result lands → rejected, as today. (This ordering is
  unreachable via real threading alone in this design — a new attempt
  only starts once the no-overlap guard sees the old thread is no longer
  alive, which means its epilogue already ran — so the test drives the
  token-supersession branch directly, documented inline.)
- `TestDoneLateLedgerOutcome::test_late_failure_never_marks_done` — (c) a
  late failure never marks done.
- `TestDoneLateLedgerOutcome::test_summary_line_reports_done_late_count_separately`
  — (d) `done_late=<n>` appears as its own count in both the per-cycle and
  completion-only summary log lines.

Two **pre-existing** tests asserted the *old* (now superseded) behavior —
that a late-but-successful orphan return could never mark its task done —
and were updated to assert `done_late` instead, since that was precisely
the bug being fixed:
`TestInFlightOverlapGuard::test_second_run_skips_in_flight_task_and_proceeds_to_next`
and
`TestAbandonmentDoesNotPreventTaskEffects::test_abandoned_task_performs_its_effect_but_ledger_stays_unchanged`.
Both still assert everything else about their original scenarios (DB
writes happen regardless of abandonment; the `results` payload is never
published; in-flight skips don't double-attempt).

## Per-task budgets

New dedicated constants (not a shared-constant bump — that would loosen
the other 6 `DAILY_INTEL_SQL_TASK_BUDGET_S`-class tasks with no evidence
behind it):

- `DAILY_INTEL_CAPITAL_FLOW_ROLLUPS_BUDGET_S = 90` (up from the shared
  60s default). The only production number ever actually measured to
  *complete* for this task is `fold_announcements` alone — 16s. Bounded
  `compute_ttm` should be cheap on a normal day but is evidence-uncertain
  on a catch-up day (more actors fall inside the 3-day lookback at once
  after a missed cycle). 90s keeps ~5.6x headroom over the one measured
  baseline while staying a small fraction of the 480s cycle budget — not
  "wait longer for the same unbounded scan," since the scan itself is now
  bounded.
- `DAILY_INTEL_FUNDAMENTAL_DIVERGENCE_BUDGET_S = 60` (**unchanged**).
  Batching collapses ~4,500-6,000 round trips into 4 queries for the whole
  universe; there is no evidence this needs more time than the existing
  SQL-class default, so it stays there.

Both remain far under the `DAILY_INTEL_CYCLE_BUDGET_SECONDS = 480`
per-cycle pin (unchanged), which `tests/test_hermes_timeout_budgets.py`
continues to check.

## Deployment effects

- No migration. No schema change. No new environment variable.
- `compute_ttm`'s SQL signature gained a bound parameter
  (`lookback_days`, bound via `make_interval(days => :lookback_days)` per
  `.claude/rules/security.md`'s SQL-safety rule — no dynamic string
  formatting of the interval); default behavior on the daily hermes path
  changes from "recompute every actor every day" to "recompute actors
  with new quarterly data in the trailing 3 days" — idempotent either way
  (ON CONFLICT DO UPDATE unchanged).
- `fundamental_divergence.compute_divergence`'s internal query shape
  changes (batched instead of per-ticker); its public return shape
  (`list[dict]`) and `snapshot_all`'s upsert/signal-emit behavior are
  unchanged.
- `_run_daily_intel_block`'s ledger gains a new possible
  `daily_intel_task_outcome` value, `"done_late"`, alongside the existing
  `held` / `in_flight` / `done` / `done_queued` / `skipped_for_period`.
  Any downstream consumer of that dict that enumerates outcome values by
  an exhaustive allowlist (none found in this repo) would need to add it.

## Follow-up (same day, same branch, development only): durable TTM
## recompute tracking, done_late correctness proofs, PG validation

Controller instruction for this pass: replace the fixed
`TTM_LOOKBACK_DAYS = 3` window above with restart-safe tracking that
covers BOTH new AND corrected quarterly rows, regardless of fiscal
period; prove `done_late`'s guarantees with targeted tests; and validate
the SQL from the original pass against real Postgres. Still draft PR, no
merge, no production access, no DB/SSH used to build this (the new PG
tests are written to run against a disposable database the coordinator
provisions separately — never executed here).

### Corrected-row signal: established from code

The only writer of `capital_flows` `period_type='quarter'` rows is
`ingestion/altdata/sec_xbrl_financials.py::_write_rows` (confirmed by
grepping every `INSERT INTO capital_flows` site in the repo —
`ingestion/altdata/corporate_actions_parser.py` only ever writes
`period_type='announcement'`, and `scripts/load_supply_capital_seed.py`
is an offline one-off seed script, not part of the daily ingest path).
`_write_rows` DELETEs the exact
`(actor_id, fiscal_period, period_type, flow_type, source_filing)` row
(if any exists) and then plain-INSERTs a fresh one with `as_of = NOW()`
on **every** call — there is no `ON CONFLICT DO NOTHING` short-circuit
that could leave `as_of` untouched. **Answer: yes, a re-ingested/
corrected row moves `as_of`**, identically to a brand-new row, regardless
of which fiscal period it corrects. That means the "best available"
per-actor content-fingerprint fallback the brief allowed for was not
needed — a single scalar high-water-mark watermark over `as_of` is
sufficient and strictly simpler.

### What was implemented: a single persisted watermark

`intelligence/company_financial_rollups.py`:
- `TTM_LOOKBACK_DAYS` is gone. `compute_ttm(engine, watermark: str | None
  = None) -> TtmResult(rows_written, watermark)`. `watermark` is an
  ISO-8601 string (or `None`, meaning "no watermark yet" — unconditional
  full recompute, used both for the first-ever run and as the explicit
  full-recompute escape hatch `scripts/run_capital_flow_rollups.py`
  already exposed as a CLI flag, `--watermark` omitted).
- The `changed_actors` CTE's filter changed from
  `as_of >= NOW() - make_interval(days => :lookback_days)` to
  `CAST(:watermark AS timestamptz) IS NULL OR as_of > CAST(:watermark AS
  timestamptz)` — an absolute cursor, not a relative N-day window, so a
  downtime gap of any length (not just >3 days) is caught the same way.
  The CTE still has no `fiscal_period` predicate at all (pinned by
  `tests/test_capital_flow_rollups_tracking.py::
  test_changed_actors_cte_has_no_fiscal_period_predicate`), which is
  exactly what makes a correction to an OLD fiscal period indistinguishable,
  at this gate, from a brand-new row.
- A companion query, `_TTM_NEW_WATERMARK_SQL`
  (`SELECT MAX(as_of) FROM capital_flows WHERE period_type='quarter' ...
  AND (same watermark predicate)`), runs inside the SAME
  `engine.begin()` transaction as the UPSERT. `compute_ttm` returns the
  new watermark only after that transaction has committed — if the
  UPSERT raises, the transaction rolls back and the exception propagates
  BEFORE any watermark is computed, so a failed run's caller never
  receives (and therefore can never persist) an advanced watermark. A
  retry with the same watermark recomputes the identical actor set.

### Where the watermark is persisted

`OperatorState.capital_flow_ttm_watermark: str | None`
(`scripts/hermes_health.py`) — a single scalar, so it lives directly on
`OperatorState` (serialised/hydrated under the same "only restore if
currently unset" rule as every other daily-intel ledger field) rather
than a new table. A per-actor table was considered and rejected: there is
exactly one scalar of durable state for the whole rollup, not one row per
actor, so a table would add a migration and a query for no correctness
benefit. `scripts/hermes_operator.py::_daily_intel_capital_flow_rollups`
reads it as `ttm_watermark` into `run_all`, and advances it —
unconditionally, the instant `run_all` reports `ttm_ok: True` — because
the DB write it corresponds to already committed inside `compute_ttm`'s
own transaction, independent of whatever this ledger later decides about
crediting the *attempt* (done/done_late/abandoned; see "Abandonment
truth" on `_run_daily_intel_block`).

### `run_all` partial-failure semantics (fixes a real gap in the
### original pass)

The original `run_all` swallowed both `compute_ttm` and
`fold_announcements` exceptions into a stats dict and never raised — so
`_daily_intel_capital_flow_rollups` never raised either, and a cancelled
`compute_ttm` with a successful `fold_announcements` (exactly the
production evidence quoted at the top of this doc) would have let
`_run_with_timeout` see `ok=True` and the ledger mark the task **done**,
hiding the fact that no TTM rows were written that cycle. Fixed:
- `run_all(engine, ttm_watermark=None) -> dict` still attempts both
  sub-steps independently (fold still runs even when TTM fails — matches
  the production evidence) and never raises itself, but now returns
  `"ttm_ok"`, `"fold_ok"`, and `"ok"` (`= ttm_ok and fold_ok`) alongside
  the existing row counts, plus `"ttm_watermark"` (the value to persist
  next — unchanged from the input when `ttm_ok` is `False`).
- `_daily_intel_capital_flow_rollups` now RAISES when `cf_stats["ok"]`
  is `False`, so `_run_with_timeout` reports `ok=False` and the
  daily-intel ledger counts a genuine attempt toward
  `DAILY_INTEL_MAX_ATTEMPTS`/`skipped_for_period` — never `done`, never
  `done_late`. Pinned by
  `tests/test_hermes_daily_intel_resumable.py::
  TestCapitalFlowRollupsPartialFailure::
  test_ttm_cancelled_fold_ok_is_never_done_or_done_late` (uses the REAL
  task function with `run_all` monkeypatched to the exact
  ttm-cancelled/fold-ok shape from the original evidence).

### done_late guarantees: what was already true vs. what needed a new test

Re-reading `_run_daily_intel_block`/`_run_task` (added in the original
pass, this doc's "Ledger fix" section above) against the controller's
four correctness properties:
- **(a) credits only the attempt that timed out and only its own due
  period** — the code already compared `state.daily_intel_period` (live,
  checked at the moment the late worker's `finally` block runs) against
  `period_iso` (closure-captured at the ORIGINAL attempt's start) as part
  of the `same_period` check. This was correct but UNTESTED — added
  `tests/test_hermes_daily_intel_resumable.py::TestDoneLatePeriodBoundary::
  test_a_late_return_after_period_rollover_does_not_credit_new_period`,
  fully real-thread (a period rollover happens naturally via a second
  `_run_daily_intel_block` call for a later `now` while the first
  worker is still blocked).
- **(b) never overwrites a newer attempt's outcome** — the pre-existing
  `TestDoneLateLedgerOutcome::
  test_superseded_late_return_is_rejected_not_done_late` covered "a newer
  attempt has REGISTERED a token" (not yet completed). Added
  `TestDoneLateNeverOverwritesNewerAttempt::
  test_bd_newer_attempt_already_done_is_not_overwritten_by_stale_late_return`
  for the stronger case: the newer attempt has ALREADY recorded a real
  `"done"` outcome and a real published result before the stale worker's
  belated result lands — confirms neither is clobbered.
- **(c) never hides a partially failed task** — `TestDoneLateLedgerOutcome::
  test_late_failure_never_marks_done` already covered a late EXCEPTION.
  The capital_flow_rollups partial-failure test above covers the other
  shape: a task that returns normally but reports partial failure through
  its own return contract, now converted to a raised exception by this
  pass's `_daily_intel_capital_flow_rollups` fix — same mechanism, wired
  end to end.
- **(d) not credited when the worker's token was superseded** — same
  mechanism as (b); the new test above doubles as this proof (a fresh
  token was minted for the newer attempt, and the stale worker's `finally`
  block observes its own token no longer matches the entry's current one).

### PostgreSQL-backed validation (new files, real Postgres, no SQL-text
### assertions — behavior only)

`tests/test_capital_flow_rollups_pg.py` — real `compute_ttm` against
`public.capital_flows` (unique `rollup_test_<uuid>` actor ids, cleaned up
per test):
- only the actor with a row newer than the watermark is recomputed (two
  actors, both with a full 4-quarter window; only one gets a post-
  watermark correction to its Q4 row);
- the written `ttm` amount equals a plain independent sum of the four
  quarterly amounts;
- a call that fails mid-transaction (forced via a watermark value that
  fails `CAST(... AS timestamptz)`) writes nothing, and a later call is
  unaffected — the PG-level proof that "failure leaves nothing to have
  advanced past";
- `watermark=None` still recomputes an actor whose rows are ALL old (the
  literal `TTM_LOOKBACK_DAYS=3` regression this design replaces).

`tests/test_fundamental_divergence_pg.py` — real `_load_batch_price_cagrs`
/ `_load_ticker_price_cagr` / `_load_batch_fundamentals` /
`_load_ticker_fundamentals` against `public.raw_series` /
`public.capital_flows` (unique `PG<hex>` tickers / `fd_pg_test_<uuid>`
actor ids, a throwaway `source_catalog` row per test, all cleaned up):
- `pull_status='FAILED'` rows (even with a bogus value and a newer
  `pull_timestamp` than the real data) are excluded from the CAGR;
- a measured `SUCCESS` value of `0` is treated as a real observation
  (CAGR resolves to exactly `-1.0`, not `None`) — and, since this
  specific dataset has no FAILED rows, `_load_batch_price_cagrs` and the
  pre-batching `_load_ticker_price_cagr` are asserted equal, per the
  brief;
- competing vintages (two SUCCESS rows, same `obs_date`, different
  `pull_timestamp`) — the latest pull wins, and the batched and
  per-ticker functions are asserted equal;
- bonus: `_load_batch_fundamentals` vs. `_load_ticker_fundamentals` on
  the same annual `capital_flows` data (byte-equal dict, not just a
  matching CAGR).

**Real bug found and fixed while writing the competing-vintages test**:
`_load_ticker_price_cagr` (`intelligence/fundamental_divergence.py`) had
`ORDER BY obs_date DESC LIMIT 1` with NO `pull_timestamp` tiebreak, unlike
the batched loader's deterministic `DISTINCT ON (series_id) ... ORDER BY
series_id, obs_date DESC, pull_timestamp DESC`. Two SUCCESS rows sharing
an `obs_date` (a same-day price correction) made this legacy function's
"latest pull wins" behavior Postgres-plan-dependent, not deterministic —
violating the same SUCCESS + latest-`pull_timestamp` policy
`.claude/rules/data-integrity.md` documents for `store/observations.py`.
Fixed by adding `, pull_timestamp DESC` to both of its `ORDER BY`
clauses (latest-close and prior-close). This is the ONLY behavioral
change made to `_load_ticker_price_cagr` in this pass — its missing
`pull_status='SUCCESS'` filter (the batched loader has one, this
function still doesn't) was deliberately left alone: fixing it is a
larger, separately-reviewable change, and none of this pass's PG tests
depend on it (the "failed observations" test only calls the batched
loader, not the legacy one, specifically to sidestep this pre-existing
gap). Flagged here, not silently left for someone to rediscover.

The existing `tests/test_fundamental_divergence_sec_priority.py`
(SQLite, `_load_ticker_fundamentals`'s SEC-over-seed dedup) is unchanged
and still the fast/no-PG regression guard for that ranking logic — the
new PG file is the "real Postgres, real table shapes, real functions"
complement, not a replacement.

### No-DB tracking unit tests

`tests/test_capital_flow_rollups_tracking.py` — hand-rolled fake
SQLAlchemy engine (no `pg_engine`, always runs): watermark passed through
unbound by any reintroduced day-count constant; `None` watermark forces
an unconditional full recompute; the `changed_actors` CTE and the new-
watermark query both have no `fiscal_period` predicate (structural drift
guards); a raising UPSERT never yields a watermark and `run_all` keeps
the caller's watermark unchanged on failure so a retry recomputes the
identical set; `OperatorState` hydration restores the watermark only
when currently unset and never overwrites a live in-process value.

### Coordinator: running the PG tests

Point `GRID_TEST_DB_URL` at a disposable Postgres database with the
project's `schema.sql` (+ migrations) applied, then:

```
GRID_TEST_DB_URL=postgresql://user:pass@host:5432/disposable_db \
DB_PASSWORD=x PYTHONUTF8=1 python -m pytest \
  tests/test_capital_flow_rollups_pg.py \
  tests/test_fundamental_divergence_pg.py \
  tests/test_capital_flow_rollups.py \
  -v
```

(`tests/test_capital_flow_rollups.py` is the pre-existing PG file, now
updated for the `watermark`-based `compute_ttm` signature — includes it
here as a regression check on the same disposable DB.) Every test in
both new files creates and cleans up its own uniquely-prefixed rows; none
of them assume any pre-seeded `source_catalog` row, table, or production
data.

### What was NOT done (disclosed)

- `_load_ticker_price_cagr`'s missing `pull_status='SUCCESS'` filter
  (see "Real bug found" above) — left as-is, flagged for a separate
  reviewed change.
- No migration/CLI change beyond the pre-existing
  `scripts/run_capital_flow_rollups.py --watermark` flag added in this
  pass (replaces the old, never-wired `lookback_days=None` escape
  hatch with an actual CLI knob) — no other script or endpoint was
  touched.
- The PG tests in this pass have not been executed against a real
  database by this agent (no DB access here, per the controller's
  constraints) — they are written, reviewed for SQL/behavioral
  correctness against `schema.sql`, and confirmed to skip cleanly with
  no Postgres reachable; the coordinator runs them for real.
