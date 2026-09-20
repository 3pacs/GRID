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
for `scripts/run_capital_flow_rollups.py` (manual backfill use, not the
daily hermes path). No index change was needed: the `changed_actors` CTE's
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
