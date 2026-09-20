# Hermes daily-intel batch: independently resumable tasks (review-only)

Branch: `fable/daily-intel-resumable-20260920` off `origin/main` @ `266b9235`
Scope: development only, **draft PR, no merge, no production access, no DB/SSH used to build this**.

## The defect (traced in production; not re-audited here)

`scripts/hermes_operator.py::run_intelligence_tasks` runs under
`_run_with_timeout("intelligence_tasks", ..., INTELLIGENCE_TASKS_TIMEOUT_SECONDS = 900)`.
Its "Daily at 2:00 AM" block ran ~20 sequential tasks — source audit,
backtest scan, postmortem batch, options improvement, hypothesis review,
flow materialisation, hypothesis discovery (`auto_discover`), RAG index
refresh, actor research, ICIJ cross-reference, milestone scoring,
attention anomaly, EDGAR transcripts, corporate actions, capital-flow
rollups, fundamental divergence, holder/deal overlap, and three
insight/briefing/errors.jsonl cleanups — each in its own bare
`try/except`, none with its own timeout.

In production the step was abandoned at 900s on every post-02:00Z cycle
**before** reaching `state.last_daily_intel = now`. Because that watermark
never advanced, the block restarted from the top as catch-up (`is_catch_up
= True`) on every subsequent cycle, and nothing past the first ~10 minutes
of the list ever ran. `hypothesis_discovery` (`auto_discover`) — roughly
task 8 of ~21 — starved from 2026-09-17 onward.

Controller's instructions for this task: make the tasks independently
resumable; do **not** raise `INTELLIGENCE_TASKS_TIMEOUT_SECONDS` or
`CYCLE_TIMEOUT_SECONDS`; do not change what any task *does*.

## Design

### `DAILY_INTEL_TASKS` — ordered task table (`scripts/hermes_operator.py`)

Each entry is a `DailyIntelTask(name, fn, budget_s)` NamedTuple.
`fn(engine, state, now, results)` is the pre-existing task body moved
into its own small function — same imports, same log lines, same
`results[...]` keys. The one behavioral change: the bare
`try/except Exception: log.warning(...)` that used to wrap each task is
gone; `_run_with_timeout` now provides both the timeout AND the exception
handling, which is what makes a task's success/failure visible to the
per-period ledger below. (Six tasks route through the pre-existing
`_run_intel_task` helper, which itself swallows exceptions and records
`state.task_status` instead of raising — `_daily_intel_raise_if_task_
status_failed` re-raises when that status shows failure, so
`_run_with_timeout` still sees it. This is the only other behavioral
addition; the work each task performs is unchanged.)

| # | Task name | Budget | Class |
|---|---|---|---|
| 1 | `storage_maintenance_subagent` | 60s | dispatch (queues a subagent, no LLM call here) |
| 2 | `source_audit` | 180s | LLM-backed |
| 3 | `flow_materialize` | 60s | SQL-only |
| 4 | `backtest_scan` | 180s | LLM-backed (sanity check) |
| 5 | `postmortem_batch` | 180s | LLM-backed, work-bounded by `POSTMORTEM_BATCH_LIMIT=20` |
| 6 | `options_improvement` | 180s | LLM-backed |
| 7 | `hypothesis_review` | 180s | LLM-backed |
| 8 | `hypothesis_discovery` | 180s | LLM-backed (`auto_discover`) — the starved task |
| 9 | `rag_index` | 180s | LLM-adjacent (embeddings) |
| 10 | `actor_research` | 180s | LLM-backed |
| 11 | `icij_linking` | 60s | SQL/fuzzy-match, CPU |
| 12 | `milestone_scoring` | 60s | SQL/computation |
| 13 | `attention_anomaly` | 60s | network I/O (Wikipedia/Trends), no LLM |
| 14 | `edgar_transcripts` | 180s | LLM-backed (guidance extraction) |
| 15 | `corporate_actions` | 60s | regex/SQL |
| 16 | `capital_flow_rollups` | 60s | SQL rollups |
| 17 | `fundamental_divergence` | 60s | SQL/computation |
| 18 | `holder_deal_overlap` | 60s | SQL cross-reference |
| 19 | `insight_cleanup` | 30s | filesystem cleanup |
| 20 | `briefing_cleanup` | 30s | filesystem cleanup |
| 21 | `errors_jsonl_cleanup` | 30s | filesystem cleanup |

Budget defaults are documented constants: `DAILY_INTEL_LLM_TASK_BUDGET_S=180`,
`DAILY_INTEL_SQL_TASK_BUDGET_S=60`, `DAILY_INTEL_CLEANUP_TASK_BUDGET_S=30`,
plus `DAILY_INTEL_DISPATCH_TASK_BUDGET_S` (aliases the SQL default — dispatch
only, no LLM call in-step) and `DAILY_INTEL_POSTMORTEM_TASK_BUDGET_S`
(aliases the LLM default — the batch itself is bounded on the work axis by
the pre-existing `POSTMORTEM_BATCH_LIMIT`, not by a separate time constant).

Ordering is unchanged from the pre-existing inline block — `capital_flow_rollups`,
`fundamental_divergence` and `holder_deal_overlap` still assume the tasks
before them in the tuple already ran this period, same as before.

### Persisted per-period ledger (`OperatorState`, `scripts/hermes_health.py`)

- `daily_intel_period: str | None` — ISO date of the due period
  (`_period_boundary(now, DAILY_INTEL_BOUNDARY_HOUR=2)`, the same helper
  `#577`'s sector-health scheduler uses with `boundary_hour=3`) the
  ledger below currently belongs to.
- `daily_intel_done: dict[str, str]` — task name → ISO date of the due
  period it completed (or was skipped_for_period) for. Checked to decide
  "already done this period, skip it."
- `daily_intel_skipped_for_period: dict[str, str]` — task name → ISO date
  it was marked skipped_for_period for (distinguishes "gave up" from
  "succeeded" for the summary log and tests; every entry here also has a
  matching entry in `daily_intel_done`).
- `daily_intel_attempts: dict[str, int]` — task name → attempts made in
  the CURRENT period.

All four are serialised in `to_dict()` and restored in
`hydrate_from_snapshot()` under the same "only if currently unset" rule
as `repair_backlog`/`last_sector_health`, so a restart resumes the ledger
from the last persisted `analytical_snapshots` row instead of starting
over.

**Ledger semantics, checked per due period.** A task is "done for the
period" purely by `daily_intel_done.get(name) == <current period ISO>` —
a stale entry from a previous period does not block a new period (no
separate reset step is needed for `daily_intel_done` itself); the
explicit clear-and-reset in `_run_daily_intel_block` (triggered when
`state.daily_intel_period` no longer matches the current period) exists
because `daily_intel_attempts` has no per-entry date and must be zeroed
on rollover.

**Idempotent redo is safe.** State is only persisted at the END of a
cycle (`save_cycle_snapshot`'s `analytical_snapshots` write), not after
each task inside `_run_daily_intel_block`. A mid-cycle process restart
can therefore re-run a task this call already finished but hadn't yet
had a chance to persist. This is safe because every task's own DB writes
were already idempotent upserts/inserts-with-dedupe before this task —
that property was explicitly NOT changed — so a redo just repeats the
same write.

### Execution (`_run_daily_intel_block`, same `daily_due` trigger as before)

Iterates `DAILY_INTEL_TASKS` in order:

1. Skip a task already `daily_intel_done` for the current period.
2. Stop the block for THIS cycle if cumulative wall time already spent in
   this call (`budget_used`, a local variable — resets every call) is
   `>= DAILY_INTEL_CYCLE_BUDGET_SECONDS = 480` — checked before starting
   the next task, not mid-task. The next `daily_due` evaluation (next
   cycle) resumes at the first undone task.
3. Otherwise run the task under
   `_run_with_timeout(f"daily_intel:{name}", ..., task.budget_s, state)`.
   - `ok=True` → mark done for the period.
   - `ok=False` (timeout or exception) → increment its attempt count;
     at `DAILY_INTEL_MAX_ATTEMPTS = 3` mark it `skipped_for_period` (also
     recorded in `daily_intel_done`, so it can never block tasks behind
     it) — either way, continue to the NEXT task, never abort the block.
4. `state.last_daily_intel = now` is set **only** when every task is done
   or skipped_for_period.
5. One summary line per cycle:
   `daily_intel: period=<date> done=<n>/<total> ran=[...] skipped_for_period=[...] remaining=[...] budget_used=<s>s`.

An abandoned (timed-out) task's orphaned worker thread (per
`_run_with_timeout`'s own docstring, it cannot be killed, only abandoned)
may still finish later — this cannot mark the task done, because the only
writer of `daily_intel_done` is this loop, driven by `_run_with_timeout`'s
*synchronous* return value at the timeout boundary. There is no callback
path from an orphaned worker back into the ledger.

**Budget pin**: `ACTIVE_HYPO_SCORING_MAX_RUNTIME_S (240) + DAILY_INTEL_CYCLE_BUDGET_SECONDS (480) + 60 <= INTELLIGENCE_TASKS_TIMEOUT_SECONDS (900)`
→ 780 ≤ 900. The `+60` covers the earnings-calendar-sync SQL call and
active-hypo-scoring bookkeeping that run ahead of both inside the same
step. Pinned in `tests/test_hermes_timeout_budgets.py`
(`test_active_hypo_scorer_fits_inside_intelligence_step_with_daily_batch`,
updated to use the new constant) and re-checked in
`tests/test_hermes_daily_intel_resumable.py::TestBudgetPins`. The old
`DAILY_INTEL_BATCH_OBSERVED_S = 360` constant is kept (nothing computes
with it anymore) since it's a documented historical measurement other
notes reference, but the pin now uses the hard cap the code actually
enforces instead of an observed value from the monolithic block that
preceded it.

### `cooldowns.can_retry` — deliberately not consulted

Same reasoning `_run_sector_and_intelligence_steps` documents for its own
split: the blacklist entry `_run_with_timeout` writes on a timeout is
only *honoured* by call sites that explicitly check
`state.cooldowns.can_retry(<name>)` before running — exactly four in this
module (`oracle_cycle`, `signal_classification`, `anomaly_narration`,
`knowledge_mapping`). Daily-intel task names are not among them. This
ledger's own `DAILY_INTEL_MAX_ATTEMPTS` (3, per period) is the throttle
for a persistently-failing task; layering the 24h `can_retry` blacklist
on top would mean a single timeout blocks that task for a full day,
overriding the much shorter, per-period skip this design intends.

## What is NOT changed

- `INTELLIGENCE_TASKS_TIMEOUT_SECONDS` (900) and `CYCLE_TIMEOUT_SECONDS`
  (4500) — untouched, per the controller's instruction.
- What each task computes, writes, or logs on its success path — every
  function body is the pre-existing code, moved, not rewritten.
- The `daily_due` scheduling condition in `run_intelligence_tasks`
  (`is_daily_window` / `is_catch_up` / the `_hours_since(...) >= 20`
  guard) — unchanged; only what happens once triggered has changed.
- Sector-health placement/reasoning (dispatched before intelligence_tasks
  as its own step, `#577`) — untouched; the trailing NOTE comment in
  `run_intelligence_tasks` is updated to mention this fix but still says
  not to re-add a sector-health call there.

## Tests

New `tests/test_hermes_daily_intel_resumable.py`, no DB/network, fake
engine (`MagicMock`), monkeypatched task tables:

- (a) a blocked first task (real timeout via a patched 0.05s budget) does
  not prevent tasks 2/3 from running in the same cycle.
- (b) restart mid-period (ledger hydrated with a done task) does not
  re-run it.
- (c) period rollover clears `daily_intel_done` and resets
  `daily_intel_attempts`.
- (d) a task hitting `DAILY_INTEL_MAX_ATTEMPTS` is marked
  `skipped_for_period`, and the block completes (`last_daily_intel` set)
  even though it never succeeded.
- (e) `last_daily_intel` stays unset while any task is neither done nor
  skipped.
- (f) cycle-budget exhaustion (deterministic via a monkeypatched
  `time.monotonic`) stops the block mid-list; the next call resumes at
  the right task without re-running earlier ones.
- (g) an abandoned (real) timed-out task that finishes later (proven via
  a `threading.Event`) does not retroactively mark itself done.
- (h) budget pins, positive per-task budgets, unique task names.
- Plus a summary-log-line assertion and a sanity check that
  `DAILY_INTEL_TASKS`' real order matches this doc's table.

`DB_PASSWORD=x PYTHONUTF8=1 python -m pytest tests/test_hermes_*.py -q`:
**283 passed, 1 failed** — the failure is
`tests/test_hermes_fixers.py::test_fix_output_dirs_skill_creates_common_output_directories`,
the pre-existing Windows-only failure the task brief flagged as known and
unrelated to this change (a path-separator assertion in an unrelated
fixer test, not touched by this diff).

`ruff check` on changed files: `hermes_operator.py` went from 107 to 86
pre-existing lint findings (net improvement — several bare
`except Exception` blocks were removed), `hermes_health.py` unchanged at
12 (no new findings), and both new/edited test files are clean.

## Deployment effects

- API/Hermes process restart required to pick up the new code (as with
  any Hermes deploy).
- Alembic: no-op — no schema change. The ledger lives inside the existing
  `OperatorState` JSON payload in `analytical_snapshots`
  (subcategory `hermes_operator`), same persistence path as
  `repair_backlog`/`last_sector_health`.
- On the first post-restart cycle after 02:00Z UTC, the block resumes
  from an EMPTY ledger for the current period (nothing hydrates, since
  `daily_intel_period`/`daily_intel_done` did not exist in older
  snapshots) and progresses a few tasks per cycle, bounded by
  `DAILY_INTEL_CYCLE_BUDGET_SECONDS`. No forced/manual run needed —
  it drains naturally over the following cycles the same way the
  sector-health due-period scheduler does.
- Tasks that previously never ran in weeks under the old defect will run
  for the first time once the ledger reaches them. One-line summary of
  what each writes, so the controller can decide whether that's wanted
  before this merges:
  - `hypothesis_discovery` (`auto_discover`) — inserts new rows into
    `hypothesis_registry` (candidate trading hypotheses).
  - `rag_index` — inserts/updates embedding rows for RAG retrieval (no
    trading-relevant writes; index-only tables).
  - `actor_research` — updates/inserts `actor_profiles` rows (LLM
    enrichment) and can create new actor entities ("rabbit holes").
  - `icij_linking` — inserts fuzzy-match rows linking actors to ICIJ
    offshore-entity records.
  - `milestone_scoring` — writes execution scorecards per company
    (milestone tracker tables).
  - `attention_anomaly` — read-only detection; only logs, no DB writes of
    its own beyond what `get_alerts` already persists upstream.
  - `edgar_transcripts` — inserts 8-K-derived guidance/milestone rows.
  - `corporate_actions` — inserts `capital_flows` rows
    (`period_type='announcement'`) from regex-mined 8-Ks.
  - `capital_flow_rollups` — writes TTM and annual-rolled `capital_flows`
    rows derived from XBRL + the announcement rows above.
  - `fundamental_divergence` — upserts one snapshot row per ticker into
    `fundamental_divergence`.
  - `holder_deal_overlap` — inserts pre-positioning detection rows into
    `holder_deal_overlap`.
  - Cleanups (`insight_cleanup`, `briefing_cleanup`,
    `errors_jsonl_cleanup`) — deletions/truncation only, no new data.

No forced run is included in this change — the ledger drains on its own
schedule once deployed.
