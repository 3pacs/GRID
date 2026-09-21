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

## SUPERSEDED — see "Third follow-up" at the end of this document

Everything below this point through the end of the "Follow-up ... durable
TTM recompute tracking" section describes the scalar `as_of` watermark
design (first and second-draft follow-ups, same day). The controller
subsequently established that design is **not commit-order safe** and it
was replaced with a durable per-actor content fingerprint. The precise,
non-contradictory final statement of what is and is not guaranteed lives
in the "Third follow-up" section at the end of this file — read that
section for the current design; the sections below are kept for history
(what was tried, and why it was wrong) but must not be taken as the
current behavior. In particular: the "Stale-write classification
restated" section below says both that corrected rows always advance the
watermark AND that a stale `ttm` key can survive indefinitely with
nothing to remove it — those two statements are in tension (the first
implies coverage the second denies), and the "Third follow-up" section
resolves it precisely rather than restating either half.

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

## Third follow-up (same day, same branch, development only): the scalar
## watermark was commit-order unsafe — replaced with a durable per-actor
## content fingerprint

**Controller verdict on the second follow-up's design:** a scalar
high-water-mark on `capital_flows.as_of` is NOT established to be
commit-order safe, and "a passing sequential test does not establish
commit-order safety." In PostgreSQL, `now()`/`CURRENT_TIMESTAMP` (what
`ingestion/altdata/sec_xbrl_financials.py::_write_rows` binds into
`as_of`) is the **transaction START time**, not commit time. A writer
transaction that starts before a `compute_ttm` run's snapshot but commits
after it carries an `as_of` older than the watermark that run persists —
`as_of > watermark` then skips that row **forever**, not just for one
cycle, because the watermark can advance past that row's `as_of` from
*other* traffic committing in between, before the slow writer ever
finally commits. A second, narrower hole: two rows with the exact same
`as_of` fall on either side of strict `>` depending only on which one
happened to set the watermark first. Both were proved with two
INDEPENDENT PostgreSQL connections/transactions (real concurrency, not
sequential seeding) — a sequential test cannot exercise "started before,
committed after" at all, which is exactly why the second follow-up's
(sequential) PG tests passing did not establish safety.

A third, separate hole in the second follow-up's design, unrelated to
commit ordering: it only ever ADDED `ttm` rows. An actor whose quarterly
rows were deleted or reclassified (`period_type` changed away from
`'quarter'`) produced no new row and no `as_of` movement at all, so
nothing in `compute_ttm` would ever remove or update its now-stale `ttm`
row — the exact "stale key can remain indefinitely" behavior the
"Stale-write classification restated" section above (correctly)
described, in direct tension with that same design's "corrected rows
always advance the watermark" claim. Both halves are now moot: the
replacement below closes both gaps with the same mechanism.

### What replaced it: a durable per-actor quarter-set content fingerprint
### (Design B from the controller's brief)

`intelligence/company_financial_rollups.py` — full design rationale is in
that module's own docstring; summary:

- New table `capital_flows_ttm_state (actor_id TEXT PRIMARY KEY,
  quarter_fingerprint TEXT, computed_at TIMESTAMPTZ NOT NULL DEFAULT
  NOW())`, added by migration `capital_flow_ttm_state_20260920`
  (`down_revision = 'god_view_market_tables_20260918'`, the prior single
  alembic head — `tests/test_alembic_single_head.py` still passes with
  exactly one head). **This is the migration the coordinator must apply**
  — see "Coordinator" below.
- Every `compute_ttm` call fingerprints EVERY actor's current
  `period_type='quarter'` rows: `md5(string_agg(...))` over
  `fiscal_period, flow_type, direction, counterparty_id (coalesced to
  '__none__'), amount_usd, currency, source_filing, confidence`, in that
  deterministic sort order. An actor is dirty when that live fingerprint
  `IS DISTINCT FROM` (NULL-safe) what is stored for it — covering a
  first-time actor (no stored row), a changed actor (fingerprint differs),
  and an actor whose quarterly rows are now ALL gone (live fingerprint is
  NULL, stored one is not).
- This is commit-order safe **by construction**: it compares committed
  table CONTENT on each run, never a timestamp. It cannot matter whether
  a competing writer's transaction started before or after this run's
  snapshot — only whether it had committed by the time this run's query
  executed. A commit this run's query missed is, by definition, still
  uncommitted as far as this run is concerned; the NEXT run's query will
  see it and flag the actor dirty then. There is no leapfrogging: nothing
  here is a cursor that can advance past a value it never actually saw.
- Stale `ttm` rows are now DELETEd: for every dirty actor, any existing
  `period_type='ttm', source_filing='ttm_rollup'` row whose
  `(flow_type, direction, counterparty_id, fiscal_period)` group is not
  present in that run's freshly computed 4-quarter windows is removed —
  closes case 3 (below) using the compute_ttm's existing, unchanged
  `n_quarters = 4` qualification rule (read from the code, not
  reinvented): a group that does not have exactly 4 trailing quarters
  within a 320-day span does not qualify, same as before.
- ALL of the fingerprint comparison, the stale-`ttm` delete, the
  per-actor state upsert, and the `ttm` write itself run inside ONE SQL
  statement (multiple data-modifying CTEs sharing one query snapshot),
  inside ONE `engine.begin()` transaction — not two separate statements.
  This matters: a second, separately-executed "now record the
  fingerprint" statement would re-read `capital_flows` under READ
  COMMITTED's per-statement snapshot and could durably record a
  fingerprint that this run's `ttm` write never actually matched, if a
  concurrent write landed in the gap between the two statements. One
  statement, one snapshot, closes that race entirely.
- **Why not also Design A (`pg_visible_in_snapshot`)**: real and
  commit-order safe for inserts, but blind to deletes/reclassifications
  (a `DELETE` or a `period_type` change leaves no new, not-yet-visible
  `xmin` to catch) — case 3 below. Since the fingerprint already covers
  inserts, corrections, deletions, and reclassifications with ONE
  mechanism, Design A would only add a second dependency (PostgreSQL 13+
  for `pg_current_snapshot()`/`pg_visible_in_snapshot()` — confirmed
  satisfied; production griddb runs PostgreSQL 15 per
  `docs/SERVER-SERVICES.md`) for a case already covered. Not used.
- **Cost, disclosed**: unlike the scalar watermark, the fingerprint step
  must read every `period_type='quarter'` row on every call (a single
  `GROUP BY actor_id` aggregate scan — no per-row round trips, no window
  function over the full table; that part is still bounded to dirty
  actors only, same as before). At the ~310k-row scale referenced
  earlier in this doc this is a sub-second sequential scan for a
  once-daily job. If the table grows enough for this to matter, the
  natural follow-up is a trigger-maintained fingerprint column on
  `capital_flows` itself rather than reverting to a commit-order-unsafe
  shortcut — not done here, disclosed as a future option only.

### The `watermark` parameter and `OperatorState.capital_flow_ttm_watermark`
### are now vestigial, not removed

`compute_ttm(engine, watermark=None)`, `run_all(engine,
ttm_watermark=None)`, `scripts/hermes_operator.py::
_daily_intel_capital_flow_rollups`, `scripts/run_capital_flow_rollups.py
--watermark`, and `OperatorState.capital_flow_ttm_watermark` are all
UNCHANGED in shape. The parameter/field is accepted and passed through
exactly as before, but plays NO role in deciding which actors get
recomputed — that state lives entirely in `capital_flows_ttm_state`,
committed atomically with the `ttm` rows it governs. `TtmResult.watermark`
/ `stats["ttm_watermark"]` is now just the ISO-8601 wall-clock time the
run completed, kept so existing callers that persist it for telemetry
don't need to change. Docstrings in `intelligence/
company_financial_rollups.py`, `scripts/hermes_operator.py`, `scripts/
hermes_health.py`, and `scripts/run_capital_flow_rollups.py` were all
updated to say this plainly — no code path anywhere still claims the
watermark gates anything.

One consequence, strictly stronger than before: the old "crash between DB
commit and operator-state persistence" scenario is now trivially
harmless, not just "harmless because a retry recomputes the same set." A
crash before `state.capital_flow_ttm_watermark` is written changes
NOTHING about what the next run recomputes, because nothing outside
`compute_ttm`'s own transaction is needed to gate it. Proved by
`tests/test_capital_flow_rollups_pg.py::
test_replay_after_crash_before_watermark_persist_is_a_harmless_noop`.
`compute_ttm`'s and `run_all`'s docstrings say plainly: operator-state
persistence happens at cycle end, after the database commit, and a crash
between them causes exactly this harmless replay (a true no-op — nothing
dirty, not merely an idempotent overwrite).

### The precise guarantee (resolves the "always advances" vs. "can remain
### indefinitely" contradiction above)

- **Guaranteed to trigger recomputation, with the mechanism**: any change
  to a `period_type='quarter'` row's `fiscal_period`, `flow_type`,
  `direction`, `counterparty_id`, `amount_usd`, `currency`,
  `source_filing`, or `confidence` — insert, correction (delete+
  re-insert, matching the one real writer, `_write_rows`), in-place
  update, deletion, or reclassification of `period_type` away from
  `'quarter'` — changes that actor's fingerprint and marks it dirty on
  the NEXT `compute_ttm` call. This holds regardless of commit order,
  regardless of `as_of`, and regardless of which fiscal period is
  touched.
- **Guaranteed removal, with the mechanism**: once dirty, an actor's
  existing `ttm` group that no longer has exactly 4 trailing quarterly
  rows within a 320-day span (compute_ttm's pre-existing, unchanged
  qualification rule) is DELETEd in the same statement/transaction as the
  recompute — including the case where an actor has zero quarterly rows
  left at all.
- **The exact unsupported case(s), stated precisely — no blanket
  "incremental" acceptance request**: the fingerprint covers exactly the
  eight columns listed above, scoped to `period_type='quarter'` rows. A
  write that changes some OTHER column of an existing quarter row (not
  reachable through the one real writer, `_write_rows`, which always
  DELETEs and re-INSERTs the full row — every fingerprinted column moves
  together on every write) would not be detected. Concretely: a
  hypothetical future writer, or an operator hand-editing the table
  directly with SQL, that runs `UPDATE capital_flows SET
  <some-non-fingerprinted-column> = ... WHERE period_type='quarter'`
  without touching any of the eight fingerprinted columns leaves the
  fingerprint unchanged and that actor NOT marked dirty. No trigger is
  used (Design C from the brief was not needed), so there is no
  "triggers disabled" exception to state separately — the whole mechanism
  is a plain query `compute_ttm` runs itself, always active whenever
  `compute_ttm` is called.

### The four PG tests (real concurrency, two independent connections/
### transactions per commit-order case)

All in `tests/test_capital_flow_rollups_pg.py` (plus the pre-existing
`tests/test_capital_flow_rollups.py` and the no-DB
`tests/test_capital_flow_rollups_tracking.py`, both updated to match —
see below):

1. **Late-committing earlier as_of** —
   `test_late_committing_earlier_as_of_is_recomputed` (connection A opens
   a transaction, corrects a quarter row, holds it uncommitted while an
   unrelated actor commits with a LATER as_of and connection B runs
   `compute_ttm`, THEN A commits, THEN `compute_ttm` runs again — the
   correction is picked up) and its variant
   `test_late_committing_row_with_explicit_older_as_of_is_recomputed`
   (same proof with an explicitly backdated `as_of`, sequential — the
   fingerprint design doesn't read `as_of` at all, so no concurrency is
   even needed to demonstrate this half).
2. **Equal timestamps** —
   `test_equal_as_of_timestamps_do_not_skip_the_second_actor` (two
   actors share the exact same `as_of` on their newest quarter row, one
   processed in run 1, the other inserted after with the identical
   `as_of` — recomputed on run 2 regardless of `>` vs `>=`).
3. **Removal / reclassification** —
   `test_deleted_quarter_row_without_reinsert_triggers_recompute_and_removes_ttm`
   (3a, plain DELETE), `test_reclassified_quarter_row_triggers_recompute_and_removes_ttm`
   (3b, `UPDATE ... SET period_type='annual'`),
   `test_all_quarter_rows_deleted_removes_stale_ttm_row` (3c, every
   quarter row gone — asserts the stored fingerprint goes to NULL, not
   just stops updating).
4. **Replay harmlessness** —
   `test_replay_after_crash_before_watermark_persist_is_a_harmless_noop`
   (run 1 commits and returns a new tracking value; the "crash" is
   simulated by calling `compute_ttm` again with the OLD, pre-run-1
   value; asserts `rows_written == 0` on the replay — a true no-op, not
   merely idempotent — and byte-identical rows).

Two pre-existing baseline tests were also kept, updated for the new
design: `test_first_time_actor_is_recomputed_regardless_of_watermark_param`
(was `test_only_actor_with_row_newer_than_watermark_is_recomputed` —
inverted, since the old assertion, "an actor with no row newer than the
watermark is skipped," is exactly the behavior being replaced) and
`test_failed_call_writes_nothing_and_a_later_call_is_unaffected` (kept,
but its failure-forcing mechanism changed: the old `:watermark`-cast
trick no longer applies since `:watermark` isn't bound by the SQL any
more, so it now monkeypatches `_TTM_UPSERT_SQL` to a statement that
raises a genuine PostgreSQL error while still consuming the real bind
parameters). `tests/test_capital_flow_rollups.py`'s
`test_compute_ttm_watermark_skips_actor_with_no_row_since_watermark` was
similarly inverted and renamed
`test_compute_ttm_watermark_param_does_not_gate_recompute`.
`tests/test_capital_flow_rollups_tracking.py` (no-DB, fake engine) was
rewritten: its old structural guards asserted the RETIRED design's SQL
shape (a `_TTM_NEW_WATERMARK_SQL` companion query, a `changed_actors` CTE
with no `fiscal_period` predicate) — replaced with guards for the new
shape (`capital_flows_ttm_state`, `quarter_fingerprint`,
`IS DISTINCT FROM` present; `:watermark` bind-parameter absent; exactly
ONE statement per `compute_ttm` call). Its `run_all` failure/retry and
`OperatorState` hydration tests did not depend on internal SQL shape and
are unchanged.

### Coordinator: applying the migration and running the PG tests

Apply the new migration to the disposable database first (idempotent —
`CREATE TABLE IF NOT EXISTS`):

```
alembic -c alembic.ini -x db_url=$GRID_TEST_DB_URL upgrade head
```

(or however the coordinator's existing disposable-DB setup applies
migrations — this is the one new file, `migrations/versions/
capital_flow_ttm_state_20260920.py`, `down_revision =
'god_view_market_tables_20260918'`). Then:

```
GRID_TEST_DB_URL=postgresql://user:pass@host:5432/disposable_db \
DB_PASSWORD=x PYTHONUTF8=1 python -m pytest \
  tests/test_capital_flow_rollups_pg.py -v
```

Every test creates and cleans up its own uniquely-prefixed
(`rollup_test_<uuid>`) rows in both `capital_flows` and the new
`capital_flows_ttm_state`; none assume pre-seeded data.

### What was NOT done (disclosed)

- No trigger-maintained fingerprint (Design C) — not needed, Design B
  alone passes all four required cases; disclosed as a future
  optimization only if the full-quarter-table scan cost above ever
  becomes measured (not just estimated) to matter.
- `pg_visible_in_snapshot`/Design A was evaluated and deliberately not
  used (see "Why not also Design A" above) — not a gap, a documented
  choice.
- `OperatorState.capital_flow_ttm_watermark` and the `watermark`/
  `ttm_watermark` parameters were left in place, unused for gating,
  rather than removed — removing them would touch
  `scripts/hermes_operator.py`, `scripts/hermes_health.py`, and
  `scripts/run_capital_flow_rollups.py`'s call signatures and every
  test that constructs `OperatorState` for this field, for no
  correctness benefit. Disclosed, not silently left as dead-looking
  code: every touched docstring says plainly that it is now vestigial.

## Fingerprint coverage proof (2026-09-20, THIRD follow-up)

`tests/test_capital_flow_ttm_fingerprint_coverage.py` (new) makes the
fingerprint-coverage claim above mechanically checked instead of merely
asserted:

- **Covered columns, re-derived from the SQL, not hand-typed**:
  `fiscal_period, flow_type, direction, counterparty_id, amount_usd,
  currency, source_filing, confidence` — parsed straight out of
  `_TTM_UPSERT_SQL`'s `string_agg` concatenation expression and asserted
  equal to the documented set
  (`test_fingerprint_concatenation_covers_exactly_the_documented_columns`).
- **Every column the whole TTM statement reads** is asserted to be either
  in that fingerprinted set or on a short, justified allow-list —
  `actor_id` (the GROUP BY key, not per-row content), `period_type` (a
  WHERE-clause constant within this computation, not a varying value),
  `id` and `as_of` (read only as the LAST two `ROW_NUMBER()` tie-breaks,
  reachable only between rows already forced identical on every
  fingerprinted column by the `capital_flows_dedup_nullable_cp_key`
  UNIQUE index from migration 0024 — see the allow-list's inline comment
  in the test file for the full argument)
  (`test_every_column_the_ttm_sql_reads_is_covered`). This is the test
  that fails if a future column addition gets wired into the TTM
  computation without being fingerprinted — verified by hand: injecting
  a synthetic extra referenced column makes the assertion fail as
  expected.
- **Unsupported case, restated precisely**: identical to what the module
  docstring already says — a direct `UPDATE` that touches a quarter row's
  `id` or (hypothetically) some future column outside the eight
  fingerprinted ones, without changing any of those eight, would not be
  detected. Not reachable through the one real write path
  (`_write_rows` always DELETEs + re-INSERTs the full row).
- **Order-independence**: `test_string_agg_order_by_makes_ties_impossible_given_the_unique_index`
  is a no-DB structural proof — the `string_agg` `ORDER BY` clause
  (`fiscal_period, flow_type, direction, counterparty_id, source_filing,
  confidence`) covers the variable part of the real UNIQUE index
  (`capital_flows_dedup_nullable_cp_key`: actor_id, fiscal_period,
  period_type, flow_type, cp_key, source_filing — actor_id/period_type
  constant within one actor's scan), so no two distinct rows can ever tie
  on ORDER BY and PostgreSQL cannot concatenate them in an
  insertion-dependent order.
  `test_same_rows_different_insertion_order_same_fingerprint` (PG,
  skips cleanly without a reachable database) proves it for real:
  identical 4 quarters inserted forward under one actor_id and reversed
  under another both durably record the same `quarter_fingerprint`.

## Representative-scale timing harness (2026-09-20, THIRD follow-up)

`tests/test_daily_intel_scale_pg.py` (new) — only runs with both
`GRID_TEST_DB_URL` and `GRID_SCALE_TESTS=1` set (skips cleanly otherwise,
including against a default local Postgres, since it seeds well over a
million rows). Seeds ~5,000 synthetic TTM actors x ~60 quarter rows
(~300k rows), the REAL production ticker universe
(`analysis.sector_map.SECTOR_MAP`, 1,268 tickers as of 2026-09-20 — close
to the ~1,500 target and literally what `snapshot_all`'s `_load_universe()`
reads, so the "snapshot_all end to end" measurement exercises the actual
production code path) with ~160k annual rows and raw_series price history
clearing `MIN_PRICE_OBS`, and ~150 announcement rows — all via chunked
`COPY ... FROM STDIN`, not per-row round trips. It times (a) `compute_ttm`
first run, (b) steady state, (c) after 50 actors change, (d)
`fold_announcements`, (e) the batched divergence loaders and
`snapshot_all` end to end; asserts each against the real budget constants
imported from `scripts/hermes_operator.py`
(`DAILY_INTEL_CAPITAL_FLOW_ROLLUPS_BUDGET_S=90`,
`DAILY_INTEL_FUNDAMENTAL_DIVERGENCE_BUDGET_S=60`) and against the DB's own
120s `statement_timeout` (reproduced on this test's own engine exactly as
`db.py::get_engine()` sets it); prints `EXPLAIN (ANALYZE, BUFFERS)` for
the fingerprint aggregate and the TTM window query (scoped to the 50
changed actors) so a missing index shows up in the coordinator's output.

**Not yet run against a live database** — this worktree has no DB, no
SSH, and no production access by design (see the scope line at the top of
this doc), so every number in this section is a query-plan/design
argument, not a measured one. The coordinator runs it with:

```
alembic -c alembic.ini -x db_url=$GRID_TEST_DB_URL upgrade head   # applies capital_flow_ttm_state_20260920
GRID_TEST_DB_URL=postgresql://user:pass@host:5432/disposable_db \
GRID_SCALE_TESTS=1 DB_PASSWORD=x PYTHONUTF8=1 \
python -m pytest tests/test_daily_intel_scale_pg.py -q -s
```

Seeding is expected to take well under the task's 2-3 minute allowance
(all bulk loads are chunked `COPY`, not per-row inserts) — expect the
`raw_series` load (~1.5M rows across 1,268 tickers x ~1,155 days) to
dominate seeding time. No index was added speculatively: the existing
`idx_capital_flows_actor_period(actor_id, period_type, fiscal_period
DESC)` (migration 0046) already covers the TTM window query's per-actor
scan shape, and the fingerprint aggregate's `WHERE period_type='quarter'
AND amount_usd IS NOT NULL GROUP BY actor_id` has no obviously-missing
index to propose without first seeing whether PostgreSQL's planner
actually needs one at this row count — that is exactly what this
harness's `EXPLAIN (ANALYZE, BUFFERS)` output will show. If the
coordinator's run comes back slow, the natural next step (per the task
brief) is an idempotent `CREATE INDEX IF NOT EXISTS` added to
`migrations/versions/capital_flow_ttm_state_20260920.py`, re-measured
with the same harness — not a budget increase.

## Coordinator's representative-scale run + snapshot write batching
## (2026-09-20, FOURTH follow-up)

The coordinator ran the harness above on a disposable PostgreSQL 14
database at representative scale (300,000 quarter rows / 5,000 actors,
162,304 annual rows, 1,268 real `SECTOR_MAP` tickers, 1,465,808
`raw_series` price rows; seeding 69s), over an SSH tunnel with a measured
~32.5ms round trip per statement:

| phase | seconds | budget |
|---|---|---|
| ttm_first_run (210,000 ttm rows written) | 9.25 | < 90 |
| ttm_steady_state | 0.73 | < 90 |
| ttm_after_50_changed (2,100 rows) | 3.60 | < 90 |
| fold_announcements | 0.21 | < 90 |
| divergence_load_batch_fundamentals | 2.62 | < 60 |
| divergence_load_batch_price_cagrs | 2.35 | < 60 |
| **divergence_snapshot_all (1,268 tickers, 1,268 rows written)** | **153.79** | **< 60 → FAILED** |

Every phase passed except `divergence_snapshot_all`. `EXPLAIN` confirmed
the *read* side was already fixed and fast (the fingerprint aggregate
0.54s via `idx_capital_flows_actor`, the 50-actor TTM window query
29ms) — the failure was entirely in `snapshot_all`'s **write** phase: a
per-ticker `INSERT ... ON CONFLICT` inside the loop over `compute_divergence`'s
result rows, i.e. the same N+1 pattern already fixed on the read side
(batched loaders above), just not yet applied to the write. ~1,268
tickers x one round trip each x ~32ms measured tunnel RTT ≈ the 150s
observed; on production (localhost, ~0.1–0.3ms RTT) the absolute number
would be much smaller, but the *pattern* — O(n) statements for n tickers
— doesn't stop being a bug just because production RTT is cheap, so it
was fixed rather than budget-raised.

**Fix**: `intelligence/fundamental_divergence.py::snapshot_all` now
batches its write phase into ONE multi-row `INSERT ... ON CONFLICT
(ticker, as_of) DO UPDATE` statement per
`_DIVERGENCE_UPSERT_CHUNK_SIZE` (500)-row chunk, via the new
`_build_divergence_upsert_sql` / `_upsert_divergence_chunk` helpers, all
inside the same single `engine.begin()` transaction as before. All row
values stay bound parameters (`:ticker0`, `:as_of0`, ... per row in the
chunk) — only the *count* of VALUES tuples varies with chunk size, so
this remains fully parameterized per `.claude/rules/security.md`.
Outcome semantics are unchanged: `counts` still tallies every computed
row by classification regardless of write outcome, `written` still
reflects rows actually upserted (now at chunk granularity — a chunk
failure is logged and skips that chunk's 500-row credit, the same
fail-soft trade-off already documented for the batched *read* loaders,
just at a smaller blast radius), and the summary dict shape is
unchanged. `compute_divergence` itself was already grep-verified to
issue no per-ticker SQL (its only `conn.execute` calls are the two
batched loaders, called once each for the whole universe) — nothing
else needed batching there.

`_emit_divergence_signal` (SYNTH-26 `SignalFired` fanout) intentionally
**stays per-row**: it writes to `contracts_audit` via its own separate
engine (`contracts/emit.py::_get_engine()`, not the connection/
transaction `snapshot_all` uses for `fundamental_divergence`) and
`pg_notify`s per event so the SSE listener gets one notification per
signal — collapsing that into a batch would change delivery semantics
for downstream consumers, not just performance, so it was left alone
and documented in-line instead.

New fake-engine test
`tests/test_fundamental_divergence.py::test_snapshot_all_write_phase_statement_count_is_chunked_not_per_ticker`
asserts statement count for the write phase is `1 (existence check) +
ceil(n_tickers / chunk_size)` — i.e. a function of chunk count, not
ticker count — for two ticker counts spanning multiple chunks (50 and
`2*chunk + 137`), and a companion
`test_snapshot_all_chunk_failure_is_fail_soft_and_does_not_abort_run`
proves a chunk failure is caught, logged, and doesn't stop later
chunks or the emit fanout. The scale harness's
`divergence_snapshot_all` assertion message was extended (bound kept at
`< 60s`) to point at this fix if the phase regresses again.

## Deployment effects of #587

Exactly what changes in production if this PR merges and deploys:

- **Migration**: one new file,
  `migrations/versions/capital_flow_ttm_state_20260920.py` — creates
  `capital_flows_ttm_state (actor_id TEXT PRIMARY KEY,
  quarter_fingerprint TEXT, computed_at TIMESTAMPTZ NOT NULL DEFAULT
  NOW())`, idempotent (`CREATE TABLE IF NOT EXISTS`), plus a
  conditional `GRANT ALL ... TO grid` (guarded on the `grid` role
  existing). **No index was added** in this PR — see the scale-harness
  section above for why (no measured evidence yet that one is needed).
- **First post-deploy daily period**: `capital_flows_ttm_state` starts
  empty, so `compute_ttm`'s `changed_actors` CTE treats every actor that
  has ever had a `period_type='quarter'` row as "never seen before" and
  performs **one full TTM recompute for every actor** (~5,000 actors at
  the scale this harness models; production's real actor count is
  whatever currently has quarter rows — not independently counted in
  this review-only branch). Expected duration: bounded by the
  `DAILY_INTEL_CAPITAL_FLOW_ROLLUPS_BUDGET_S=90` budget with the
  headroom argument in that constant's own comment in
  `scripts/hermes_operator.py` (16s measured fold_announcements baseline
  x ~5.6); the scale harness above is what turns that into an actual
  measured number once the coordinator runs it against the disposable
  DB.
- **Every later run**: `compute_ttm` recomputes ONLY actors whose
  `period_type='quarter'` row content fingerprint differs from what
  `capital_flows_ttm_state` has stored — a cheap `GROUP BY actor_id`
  aggregate scan of the quarter table every time (unavoidable — see the
  module docstring's "Cost, disclosed" section), full TTM window
  recompute only for the actors that are actually dirty.
- **`done_late` ledger semantics** (from the earlier follow-up on this
  same branch, see "Ledger fix" above, unchanged by today's work): a
  daily-intel task that times out at the wrapper level but then finishes
  successfully in its orphaned worker thread — same attempt token still
  current, no newer attempt registered — is now recorded as
  `daily_intel_task_outcome[name] = "done_late"` and
  `daily_intel_done[name] = period_iso`, instead of being silently
  dropped and re-attempted from scratch on every subsequent cycle. The
  task's `results` payload is still never published downstream. A late
  FAILURE, or a late success after a genuine newer attempt has already
  registered, is still rejected exactly as before — only the
  late-success/no-newer-attempt case changed.
- **Budget values** (all imported constants in
  `scripts/hermes_operator.py`, unchanged by today's work, restated
  here for completeness): `DAILY_INTEL_CAPITAL_FLOW_ROLLUPS_BUDGET_S =
  90`, `DAILY_INTEL_FUNDAMENTAL_DIVERGENCE_BUDGET_S = 60` (same as the
  shared `DAILY_INTEL_SQL_TASK_BUDGET_S` default, unchanged), DB
  `statement_timeout` = 120s (`db.py`, unchanged), per-cycle daily-intel
  wall-time budget `DAILY_INTEL_CYCLE_BUDGET_SECONDS = 480` (unchanged).
- **What is NOT changed by this PR**: no other daily-intel task's
  budget, timeout, or scheduling; no task is held back or gated behind
  this one; `DAILY_INTEL_CYCLE_BUDGET_SECONDS` (480s) is untouched; no
  statement or wrapper timeout was raised anywhere — the whole point of
  the fingerprint redesign was to make the existing 90s/60s/120s
  ceilings sufficient by doing less work per call, not to loosen the
  ceilings.
- **Exact production reads/writes of the first run**: reads every
  `capital_flows` row with `period_type='quarter' AND amount_usd IS NOT
  NULL` (one sequential/index scan, `GROUP BY actor_id`) plus, for every
  actor (all of them, since none are in `capital_flows_ttm_state` yet),
  every one of that actor's `period_type='quarter'` rows again for the
  window computation (`q_ranked`/`windowed`/`ttm` CTEs). Writes: one row
  per actor into `capital_flows_ttm_state` (INSERT, since the table
  starts empty — `ON CONFLICT DO UPDATE` never fires on this first run),
  and `period_type='ttm', source_filing='ttm_rollup'` rows into
  `capital_flows` for every actor/flow_type/direction/counterparty
  combination with a qualifying trailing-4-quarter window (INSERT or
  `ON CONFLICT DO UPDATE` depending on whether a stale `ttm` row from
  the old design already exists at that key), plus a `DELETE` of any
  existing `ttm, source_filing='ttm_rollup'` row whose group no longer
  qualifies. All of it inside ONE transaction per `compute_ttm()` call
  (the whole thing is a single parameterized SQL statement — see
  `_TTM_UPSERT_SQL`'s own comment for why it has to be one statement,
  not several). `fold_announcements` is unaffected by this migration —
  it reads/writes `period_type='announcement'`/`'annual'` rows exactly
  as before.

## Coordinator's re-measurement at 9f671d38 + core/emitter split (FIFTH follow-up)

The coordinator re-ran the scale harness at `9f671d38` (the batched-write
fix above) on the same disposable-DB shape (1,268 `SECTOR_MAP` tickers,
1.47M price rows, 162k annual rows), tunnel RTT ~30.6ms/statement, this
time with `contracts_audit` actually present and the emitter's own
config-resolved engine pointed through the same tunnel (an earlier
"emitter skipped" run undercounted this — it was an artefact of an
ad-hoc script shadowing the real `contracts` package with a local
`tests/contracts/`, not a real measurement). Corrected numbers:
`divergence_snapshot_all` total **100.8s** = **CORE 5.98s** (10 engine
statements: 6 SELECT 2.55s, 1 WITH 1.23s, 3 INSERT 0.29s; 1,268 rows
written) **+ EMITTER 94.85s** for 531 emitted signals = **178.6ms per
signal** (≈5.8 round trips' worth each — audit INSERT + `pg_notify` +
connection/transaction overhead on `_get_engine()`'s own pool, not a
fixed "3 statements"), 531 `contracts_audit` rows (exactly 1 per
signal), 0 errors. So the batched loads + batched upsert this PR
actually owns are ~6s at this scale, comfortably under the 60s budget.
The remaining ~95s is entirely the pre-existing per-event
`_emit_divergence_signal` → `contracts.emit.emit` fanout — unchanged by
this PR, RTT-bound rather than slow-query-bound. On production
(localhost, sub-millisecond RTT) the same fanout is expected to cost
roughly 1–3s, not 95s.

Important design note the coordinator's correction surfaced: the
emitter does **not** run through the same SQLAlchemy `Engine` object
this harness passes to `snapshot_all` / `compute_divergence`.
`contracts.emit._get_engine()` resolves its own engine from `DB_*`
config (`api.dependencies.get_db_engine()`), independently pooled and
potentially pointed at a different database entirely. A test cannot
safely assume that engine lands on the scratch DB just because
`GRID_TEST_DB_URL` does, so the harness cannot count "statements sent to
the emitter's engine" the way it can for its own — doing so would
either instrument the wrong connection or silently write real audit
rows / `pg_notify` traffic to whatever DB is otherwise configured.

`tests/test_daily_intel_scale_pg.py::test_representative_scale_timings`
was split to measure and report these separately instead of conflating
them:

- **`divergence_snapshot_all_core`** — `snapshot_all` with
  `_emit_divergence_signal` monkeypatched to a counting no-op. Asserted
  `< 60s` (the actual `DAILY_INTEL_FUNDAMENTAL_DIVERGENCE_BUDGET_S` this
  PR is responsible for) — the only time-bounded assertion in this
  split.
- **`divergence_emitter`** — the real `contracts.emit` path run once,
  with `contracts_audit`/`contracts_dead_letter` created on the scratch
  DB from `scripts/migrations/20260411_contracts_infrastructure.sql`
  (applied idempotently in the harness's own setup, `CREATE ... IF NOT
  EXISTS` throughout). Before running it, the harness fetches
  `contracts.emit._get_engine()` and checks its host/port/database
  against the scratch DB's own; the real emit path only runs when they
  match (coordinator's environment responsibility to point `DB_*` config
  at the same scratch DB as `GRID_TEST_DB_URL` — otherwise the
  measurement is skipped with a printed explanation rather than risking
  a write to the wrong database). Printed as `emitter: N events, T
  seconds, ms per signal, audit_rows=R (ran=...)`; when it ran, asserted
  only `audit_rows == events` (1 audit row per emitted signal — the
  pre-existing per-event design; NOT a statement count, and NOT bounded
  by any time budget — the elapsed seconds are reported for visibility
  only, since this leg is RTT-bound on an engine this harness doesn't
  control). `contracts_audit` rows this measurement writes are deleted
  in the harness's teardown (`producer_module =
  'intelligence.fundamental_divergence' AND emitted_at >= <run start>`).

**Net assessment**: the SQL-task work in this PR (batched fingerprint
reads, batched TTM upsert, batched divergence-snapshot upsert) is done
and measured at ~6s for the divergence side at representative scale —
no further query work needed there. The one remaining per-row loop
touched by this task's scope is the `contracts.emit` fanout inside
`_emit_divergence_signal`, and it is **intentionally** left per-row (see
"stays per-row" above) — this is disclosed here as the known,
RTT-sensitive-but-not-slow-query cost, not silently absorbed into the
same number as the fix this PR makes. A future batch-emit change to
`contracts.emit` would alter `SignalFired` delivery semantics for
downstream SSE consumers and is out of scope here; it belongs in its own
reviewed contract-design change, not folded into this SQL-task PR.

## Coordinator's scratch-DB rerun: statistics sensitivity + harness cleanup gap (SIXTH follow-up)

The coordinator reran `tests/test_daily_intel_scale_pg.py` against a
disposable scratch DB after the fifth follow-up above. Evidence, not a
request to reproduce: post-seed (bulk `COPY` loads + deletes/reloads),
the scratch `capital_flows` table showed **672,420 dead tuples vs
210,150 live**, and an autoanalyze had fired while it held **0 quarter
rows** — a snapshot autovacuum happened to catch mid-reload. On that
statistics state the first-run `compute_ttm` statement **exceeded the
120s `statement_timeout`** the harness mirrors from production; earlier
runs against representative statistics completed in **9-12s**. Same
statement, same data volume, only the planner's row estimates differed.
This is consistent with the "zero rows estimated" state driving the
planner toward a nested-loop plan for `q_ranked`'s scan of
`capital_flows` against the `changed_actors` set (cheap when the
planner believes few actors are dirty and the quarter table is
~empty; catastrophic once ~5,000 actors are actually dirty on a
first run and the table actually holds ~300k quarter rows) instead of
a hash-based join that scans `capital_flows` once. Production stays
off this cliff because autovacuum's autoanalyze runs continuously
against live traffic and never observes a "just bulk-loaded, nothing
counted yet" state the way one-shot harness/migration seeding can.

The same rerun also found the harness's own cleanup incomplete:
"every seeded row, nothing else" deleted rows by `source_filing`, but
`compute_ttm` writes `period_type='ttm', source_filing='ttm_rollup'`
rows (a derived filing tag, not one of the three seeded ones) and
`fold_announcements` writes `period_type='annual',
source_filing='announcement_rolled'` rows — neither matched the
delete's `source_filing IN (...)` list, so **210,150 live derived
`ttm` rows** (`period_type='ttm'`, actor_id `scale_ttm_%`) were left
behind on the scratch DB after the run.

**Fixes landed in this PR:**

- `tests/test_daily_intel_scale_pg.py` now runs `ANALYZE capital_flows`
  and `ANALYZE raw_series` (on an `AUTOCOMMIT` connection) immediately
  after seeding and before any timed phase, with the elapsed time
  folded into the `[seed]` print line. This makes the harness reproduce
  production's steady-state statistics (kept current by autovacuum)
  rather than the post-bulk-load zero-estimate state production code
  never actually runs against.
- The harness's `finally` cleanup now also deletes
  `capital_flows WHERE period_type = 'ttm' AND actor_id LIKE
  'scale_ttm_%'` and `capital_flows WHERE period_type = 'annual' AND
  source_filing = 'announcement_rolled' AND actor_id LIKE
  'scale_ttm_%'` — both are exact, seed-only predicates because the
  `scale_ttm_%` actor_id namespace is exclusively synthetic in this
  harness (see the added inline comments for why each predicate is
  distinguishable).
- `scripts/run_capital_flow_rollups.py` gained `--explain-ttm`: prints
  `EXPLAIN` (plain — no `ANALYZE`, executes nothing, writes nothing,
  documented as such in `--help`) of the exact `compute_ttm` UPSERT
  statement, using the same bind parameters, then exits 0 without
  running any task. The EXPLAINed SQL text and the executed statement
  now share one source — `intelligence/company_financial_rollups.py::
  ttm_statement_sql()` — so they cannot drift apart. Covered by
  `tests/test_run_capital_flow_rollups_explain.py` against a fake
  engine (no DB): asserts exactly one statement is issued, that it
  begins with `EXPLAIN ` (not `EXPLAIN (ANALYZE`), and that none of
  `compute_ttm`/`fold_announcements`/`run_all` are called.
- **Release gate addition**: before a production release that touches
  `compute_ttm`/`_TTM_UPSERT_SQL`, the operator/coordinator should run
  `python scripts/run_capital_flow_rollups.py --explain-ttm` read-only
  against production and confirm the plan is **not** a nested loop over
  `capital_flows` keyed by the dirty-actor set — reasoning from the SQL
  under representative (post-ANALYZE) statistics, a healthy plan scans
  `capital_flows` once for `current_fp`'s `GROUP BY actor_id` aggregate
  (Seq Scan or Index-Only Scan + a Hash/GroupAggregate), then joins
  `changed_actors` into the `q_ranked` scan via a Hash Join / Hash Semi
  Join (build the dirty-actor set once, probe once) rather than a
  Nested Loop that re-scans or re-probes `capital_flows` once per dirty
  actor — the latter is the shape that produced this rerun's timeout.
  The coordinator will record the actual EXPLAIN output from
  production at gate time rather than this document asserting a plan
  it did not itself observe.
