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

## Review amendments (2026-09-20, same branch, development only)

Three gaps identified in review of the original PR: (1) the resumable
task table above authorised running *every* task, including several that
touch learning/scoring surfaces the controller has not yet cleared for a
schedule; (2) the no-overlap and late-publish protections `#580`
(sector-health) and `#582` (repair pulls) already have did not exist for
daily-intel tasks, so a timed-out task's orphaned worker could still
collide with the next cycle's retry of the SAME task; (3) "done" and
"skipped_for_period" were both folded into `daily_intel_done`, with no
persisted signal distinguishing a clean period from one that finished
only because a task gave up. Still development-only: draft PR, no merge,
no production access, no DB/SSH used to build this.

### 1. `DAILY_INTEL_INITIAL_ALLOWLIST` — safe initial task set

`scripts/hermes_operator.py` adds `DAILY_INTEL_INITIAL_ALLOWLIST:
frozenset[str]` (13 of 21 tasks) and `DAILY_INTEL_HOLD_REASONS: dict[str,
str]` (the other 8, each with the file:line evidence for its hold). A
task not in the allow-list is "held": `_run_daily_intel_block` never
calls its `fn`, records `daily_intel_task_outcome[name] = "held"` every
time the loop reaches it, and excludes it from the period-completion
check (`total`/`done_count` are computed over `DAILY_INTEL_INITIAL_
ALLOWLIST` tasks only) — a held task can never be mistaken for done or
skipped, and can never block or fake period completion. Enabling a held
task later means editing the frozenset in its own reviewed change.

Classification method: read each task's wrapped function body (not just
its pre-existing budget-constant name) for what it writes and whether it
calls `llm.router` anywhere in its own file.

| Task | Writes | Class | Verdict |
|---|---|---|---|
| storage_maintenance_subagent | goal_queue row (dispatch only) | dispatch, no LLM | **allow** |
| source_audit | source_accuracy, source_discrepancies, source_catalog.priority_rank | deterministic audit/rollup, no LLM (reclassified — see below) | **allow** |
| flow_materialize | dark_pool_weekly, etf_flows, insider_trades, congressional_trades, junction_point_readings | deterministic SQL projection | **allow** |
| rag_index | intelligence_embeddings | local embeddings, no LLM (reclassified — see below) | **allow** |
| icij_linking | ICIJ match rows | deterministic fuzzy match | **allow** |
| attention_anomaly | none (read-only this step) | deterministic detection | **allow** |
| corporate_actions | capital_flows (announcement) | deterministic regex/SQL | **allow** |
| capital_flow_rollups | capital_flows (ttm/rolled) | deterministic SQL rollup | **allow** |
| fundamental_divergence | fundamental_divergence | deterministic SQL | **allow** |
| holder_deal_overlap | holder_deal_overlap | deterministic SQL | **allow** |
| insight_cleanup / briefing_cleanup / errors_jsonl_cleanup | deletions/truncation only | bounded cleanup | **allow** |
| hypothesis_discovery | discovered_hypotheses, hypothesis_postmortems, hypothesis_boost_log | learning write (hypothesis registry), no LLM but standing-hold category | **hold** |
| hypothesis_review | hypothesis_registry state/kill_reason | learning write, LLM (Tier.ORACLE) | **hold** |
| backtest_scan | hypothesis_registry (via winners) | backtest + learning write, LLM (Tier.ORACLE) | **hold** |
| postmortem_batch | trade_postmortems | postmortem feeding learning, LLM (Tier.REASON) | **hold** |
| options_improvement | scanner_weights (model registry), options_recommendations scoring | model registry write + scorer, LLM (report only) | **hold** |
| milestone_scoring | none found (read-only in current code) | scorer execution — standing hold by category, not by current write footprint | **hold** |
| actor_research | actors, raw_series (new leads) | LLM-driven (Tier.REASON) entity write | **hold** |
| edgar_transcripts | raw_series (guidance/milestone data points) | LLM-driven (Tier.REASON + local Gemma) data extraction | **hold** |

**Reclassified from the controller's default-hold assumption:**
`source_audit` and `rag_index` were named in the controller's brief as
presumptively "LLM-driven — hold unless shown otherwise." Reading the
code: neither `intelligence/source_audit.py` nor `intelligence/rag.py`
contains any `llm.router`/`Tier` reference — `source_audit` writes only
`source_accuracy`/`source_discrepancies` (audit/derived tables) plus a
deterministic `source_catalog.priority_rank` re-rank from the accuracy
scores it just computed; `rag_index` embeds via a local
sentence-transformers/TF-IDF/word-freq backend (not a generative call)
into `intelligence_embeddings` (a retrieval index, not a learning or
trading table). Both moved to `allow` on that evidence.
`actor_research` and `edgar_transcripts` stayed **hold** — both call
`llm.router` (Tier.REASON) and write data (new actor entities / extracted
guidance figures) that feeds further research or downstream analysis,
not just audit metadata.

Pin test: `TestDailyIntelAllowlistClassification` in
`tests/test_hermes_daily_intel_resumable.py` — every `DAILY_INTEL_TASKS`
name is in exactly one of `DAILY_INTEL_INITIAL_ALLOWLIST` /
`DAILY_INTEL_HOLD_REASONS`, and pins the exact allow/hold sets above so
an accidental reclassification fails CI.

### 2. No-overlap + late-publish guard for daily-intel workers

Reuses the two patterns already in this module rather than inventing a
third: `#582`'s `_REPAIRS_IN_FLIGHT` no-overlap registry
(`scripts/hermes_fixers.py`) and `#580`'s capture-a-token-at-start /
commit-only-if-current pattern (`_SECTOR_HEALTH_STATE_LOCK` /
`sector_health_attempt_token` in `_maybe_run_sector_health_snapshot`).

- `_DAILY_INTEL_LOCK` (RLock) + `_DAILY_INTEL_IN_FLIGHT: dict[task_name,
  {"token": int, "thread": int | None, "started": float | None}]`, both
  module-level in `scripts/hermes_operator.py`.
- Before starting a task, `_run_daily_intel_block` checks the existing
  entry: if its `"thread"` ident is still alive
  (`_daily_intel_thread_alive`, same `threading.enumerate()` check as
  `hermes_fixers._thread_is_alive`), the retry is skipped — logged
  `in_flight`, `daily_intel_task_outcome[name] = "in_flight"` — and
  counts as **neither an attempt nor a completion**; the loop proceeds to
  the next task. Otherwise a fresh token is minted and registered before
  `_run_with_timeout` is called.
- Each attempt's `fn` runs against a **local** `results` dict via a small
  `_run_task` closure, not the shared one. The closure records its own
  thread ident into the entry the moment it starts, and — in a `finally`
  block that runs whether `fn` returned or raised — checks under the
  lock whether its token is still current; if not, it logs `daily_intel
  task <name> abandoned — exiting without publishing` and does nothing
  further (the local results are discarded, never merged).
- Back in the driver, after `_run_with_timeout` returns: on `ok=True` the
  local results are merged into the shared `results` and the task is
  marked done; on `ok=False` the entry's token is invalidated **first**
  (minting a token this attempt does not hold) before the
  attempt/skipped-for-period bookkeeping runs — this is what makes the
  timeout path itself fence a late return even when no retry ever
  starts, mirroring `_run_sector_and_intelligence_steps`'s timeout-path
  bump of `sector_health_attempt_token`. The in-flight entry is not
  deleted on timeout — the thread ident stays so the next attempt's
  no-overlap check can still detect the orphan.
- **Race found and fixed during testing**: a worker's own `finally`
  block also clears `entry["thread"] = None` the instant `fn` returns
  (success OR a plain synchronous exception, not just a timeout) —
  relying solely on `threading.enumerate()`/`is_alive()` at the next
  call raced the OS thread's own teardown timing and produced a
  false-positive `in_flight` skip on a fast, back-to-back retry in
  testing (`test_d`/`TestOutcomeSemantics::test_one_task_exhausted_...`
  both failed this way before the fix). Same fix shape as
  `_retry_source`'s own `finally` deleting its `_REPAIRS_IN_FLIGHT` entry
  before returning, rather than trusting `is_alive()` for the
  normal-completion case.

Real-thread tests (`TestInFlightOverlapGuard` in
`tests/test_hermes_daily_intel_resumable.py`): a task blocked past a
patched budget times out; while its worker is still blocked (proven with
a `threading.Event`, released only after the assertions), a second call
to `_run_daily_intel_block` for the same period skips it as `in_flight`
(not counted as an attempt) and proceeds to the next task; releasing the
worker lets it finish and confirms nothing was published (ledger
unchanged, shared `results` untouched); a final call after the registry
stops reporting the thread alive retries normally and succeeds.

### 3. `daily_intel_task_outcome` / `daily_intel_period_outcome`

`OperatorState` (`scripts/hermes_health.py`) adds:
- `daily_intel_task_outcome: dict[str, str]` — task name → `"done"` |
  `"skipped_for_period"` | `"held"` | `"in_flight"`, for the CURRENT
  period (reset to `{}` on the same rollover as `daily_intel_done`/
  `daily_intel_skipped_for_period`/`daily_intel_attempts`). Written at
  the exact same points those dicts already are, plus the two new states
  neither of them could represent.
- `daily_intel_period_outcome: str | None` — `"complete"` |
  `"complete_with_skips"` | `None`, set alongside `state.last_daily_intel
  = now` the moment every allow-listed task has a `daily_intel_done`
  entry: `"complete"` if none of them got there via
  `daily_intel_skipped_for_period`, `"complete_with_skips"` otherwise.
  Reset to `None` on period rollover.

Both are added to `to_dict()`/`hydrate_from_snapshot()` under the same
"only restore if currently unset" rule as the other ledger fields. The
summary log line gained `held=[...]` and `in_flight=[...]` alongside the
existing `ran`/`skipped_for_period`/`remaining` fields.

Tests (`TestOutcomeSemantics`, `TestHeldTasksNeverRun` in
`tests/test_hermes_daily_intel_resumable.py`): all-enabled-done →
`"complete"`; one task exhausted → `"complete_with_skips"` with its
outcome `"skipped_for_period"`; hydration round-trips both new fields; a
held task's outcome is always `"held"` and never `"done"`/
`"skipped_for_period"`.

### Tests and lint

`DB_PASSWORD=x PYTHONUTF8=1 python -m pytest tests/test_hermes_*.py
tests/test_postmortem_feedback.py -q`: **308 passed, 1 failed** — the
failure is the same pre-existing Windows-only
`tests/test_hermes_fixers.py::test_fix_output_dirs_skill_creates_common_output_directories`
flagged as known/unrelated in the original task brief; nothing in this
amendment touches that file or that fixer.
`tests/test_hermes_daily_intel_resumable.py` alone: 24 tests, including
9 new ones for this amendment (`TestDailyIntelAllowlistClassification`
×4, `TestHeldTasksNeverRun` ×2, `TestInFlightOverlapGuard` ×1,
`TestOutcomeSemantics` ×4 — one shared with the classification count).

### Deployment effects (amended)

After this amendment, a fresh deploy runs only the 13 allow-listed tasks
above once their ledger reaches them — the 8 held tasks
(`hypothesis_discovery`, `hypothesis_review`, `backtest_scan`,
`postmortem_batch`, `options_improvement`, `milestone_scoring`,
`actor_research`, `edgar_transcripts`) do **not** run, and therefore none
of their writes listed under "Deployment effects" above (hypothesis
registry inserts, `scanner_weights`, `trade_postmortems`, actor/raw_series
writes, etc.) happen until a separate reviewed change edits
`DAILY_INTEL_INITIAL_ALLOWLIST`. No schema change; no forced run.

## Release-controller pre-approval review (2026-09-20, deliverables A–F)

Development only, same draft PR, same branch, no merge/production/DB/SSH.
Everything below is traced from the code the 13 allow-listed task bodies
actually call (imports followed into `intelligence/`, `ingestion/`,
`outputs/`, `ollama/`, `store/`, `scripts/goal_worker.py`), not assumed.

### A/B. Per-task effects table

Columns: **DB writes** (exact table + statement kind) · **dispatched
child work** (what's enqueued, which process executes it) · **external
calls** (host, if literal) · **file writes/deletions** (exact dir,
retention, exclusions) · **effects an abandoned run can still perform**
(part B — see the paragraph below the table first).

| Task | DB writes | Dispatched child work | External calls | File writes/deletions | Abandoned-run effects |
|---|---|---|---|---|---|
| `storage_maintenance_subagent` | none in this step itself | INSERT 1 row into `goal_queue` (`enqueue_goal`, dedupe_window=`hermes:storage_maintainer`, goal_type=`hermes_storage_maintenance`, cpu tier, allow_cloud=False) — executed later, on any `cpu`-tier node, by `scripts/goal_worker.py::handle_hermes_storage_maintenance` → `_inspect_storage_maintenance` → `storage_curator.run_storage_maintenance` (report-only; conditionally INSERTs into `operator_issues` via `log_issue` when status≠"ok") | none | none in this step; the dispatched child writes `outputs/storage_maintenance/storage_maintenance_<UTCstamp>.{json,md}` + refreshes `storage_maintenance_latest.{json,md}` | all of the above — the goal_queue INSERT is already committed by the time this 60s-budget step could time out |
| `source_audit` | INSERT `source_accuracy` (plain INSERT, no ON CONFLICT — append-only audit row per compared source pair, NOT deduped); INSERT `source_discrepancies` (same); UPDATE `source_catalog.priority_rank` (deterministic re-rank from scores just computed) | none | none | none | all of the above (retried attempts append additional audit rows rather than colliding, since source_accuracy/source_discrepancies have no dedupe key) |
| `flow_materialize` | UPSERT (`INSERT ... ON CONFLICT DO UPDATE`) into `insider_trades`, `congressional_trades`, `dark_pool_weekly`, `etf_flows`, `junction_point_readings` | none | none | none | all of the above |
| `rag_index` | DELETE FROM `intelligence_embeddings WHERE source_type='snapshot'` (then bulk INSERT), same DELETE+INSERT for `source_type='actor'` — full delete-then-rebuild per source_type | none | none — embeddings are local (sentence-transformers → sklearn TF-IDF → word-freq fallback, in that priority order); the module's only `requests.post` (to a local llama.cpp completion endpoint) lives in the unrelated `ask_question`/CLI-`ask` path, never called by `index_snapshots`/`index_actors` | none | all of the above |
| `icij_linking` | INSERT `icij_actor_matches ... ON CONFLICT DO NOTHING` | none | none | none | same INSERT |
| `attention_anomaly` | none (read-only SELECT against `attention_anomaly` + `resolved_series`) | none | none (Wikipedia/Trends ingestion happens upstream, in a separate module, not this step) | none | none — re-running is a pure read |
| `corporate_actions` | UPDATE `capital_flows` (pre-step: expands a NULL-`fiscal_period` row in place); INSERT `capital_flows ... ON CONFLICT (...) DO UPDATE`, `period_type='announcement'` | none | YES — `httpx.Client` GET to `https://www.sec.gov/files/company_tickers.json`, `https://data.sec.gov/submissions/CIK{cik}.json`, `https://www.sec.gov/Archives/edgar/data/...` (8-K filing docs), per ticker, last 30 days | none | all of the above, including in-flight SEC EDGAR requests |
| `capital_flow_rollups` | INSERT `capital_flows ... ON CONFLICT DO UPDATE`, `period_type='ttm'` (compute_ttm, one executemany); INSERT `capital_flows ... ON CONFLICT DO UPDATE`, `period_type='annual'`/`source_filing='announcement_rolled'` (fold_announcements) | none | none | none | both UPSERTs |
| `fundamental_divergence` | UPSERT `fundamental_divergence ... ON CONFLICT (ticker, as_of) DO UPDATE`, one row per ticker | none | none | none | same UPSERTs (per-ticker, so a partial abandoned pass is still individually idempotent) |
| `holder_deal_overlap` | UPSERT `holder_deal_overlap ... ON CONFLICT DO UPDATE` | none | none | none | same UPSERTs |
| `insight_cleanup` | none | none | none | **deletes** (`Path.unlink`) `outputs/llm_insights/*.md` older than **30 days** (age from the filename's embedded `<...>_<YYYYMMDD>_<HHMMSS>.md` timestamp, via `rsplit("_", 2)`); exclusion: any file whose stem doesn't split into ≥3 parts, or whose trailing two parts don't parse as that timestamp format, is silently skipped (never deleted); only `*.md` globbed | same deletions (idempotent — an already-deleted file is just not found next time) |
| `briefing_cleanup` | none | none | none | **deletes** `outputs/market_briefings/*.md` older than **90 days**, same filename-timestamp parse rule and same silent-skip exclusion as `insight_cleanup` | same deletions |
| `errors_jsonl_cleanup` | none | none | none | **truncates** (never deletes the file) `<repo_root>/.server-logs/errors.jsonl` to its **last 5000 lines**, gated on current size **> 1,000,000 bytes** (size-triggered, not purely age-based); writes to `errors.jsonl.tmp` then atomically `.replace()`s the original; exclusion: single named file only, no glob | same truncate-and-replace (already atomic; an abandoned run still performs the full read → rewrite → replace) |

**Part B — abandonment truth, stated plainly.** The attempt-token check in
`_run_daily_intel_block` runs AFTER `task.fn(...)` has already returned —
for a task that blows through its budget, `_run_with_timeout` **abandons**
the worker thread rather than killing it (Python's `concurrent.futures`
cannot kill a running thread), so `task.fn` keeps running to completion in
that orphaned thread and performs **every one of the effects in the table
above**, unchanged. What is actually withheld is narrower and entirely
bookkeeping-side: (1) this ledger's `daily_intel_done`/
`daily_intel_task_outcome` update for that attempt (the late-publish guard
discards the orphan's local `results` and skips the ledger write), and (2)
a concurrent retry of the *same* task colliding with the still-running
orphan (the in-flight registry). Neither of those stops the task's own
work. None of the 13 tasks' `fn` accepts a `should_continue`/cooperative-
cancellation parameter — every `DailyIntelTask.fn` signature is
`fn(engine, state, now, results)` — so there is no cooperative exit point
an abandoned run could even observe; none of the 13 is cooperatively
cancellable. This is documented in code (not just here) in
`_run_daily_intel_block`'s "Abandonment truth" docstring paragraph and in
`OperatorState.daily_intel_task_outcome`'s comment
(`scripts/hermes_health.py`), and pinned by
`tests/test_hermes_daily_intel_resumable.py::
TestAbandonmentDoesNotPreventTaskEffects::
test_abandoned_task_performs_its_effect_but_ledger_stays_unchanged` — a
task that blocks past its budget, then performs a fake write after
release, asserted to have performed the write while the ledger stayed
unchanged. The words "cancel"/"stopped" are deliberately not used for this
token check anywhere in the code or here.

### C. Subagent dispatch semantics and held-task bypass

`storage_maintenance_subagent`'s own step is synchronous and finishes the
moment `enqueue_goal` returns (one INSERT into `goal_queue`) — it does
**not** wait for the queued goal to execute. `_run_daily_intel_block` now
marks that task's own outcome `daily_intel_task_outcome[...] =
"done_queued"` (a distinct value from `"done"`, driven by the new
`DailyIntelTask.reports_done_queued` field — true only for this task) —
"done (queued)", not "done (child work finished)". The queued goal's own
completion is tracked separately, by `goal_queue.state` and the
`goal_results` table (`intelligence/goal_queue.py`), never by this ledger;
neither this ledger nor `_run_daily_intel_block` ever learns whether the
goal later succeeds, fails, or sits unclaimed.

Held-category reachability: traced `enqueue_goal`'s `goal_type=
"hermes_storage_maintenance"` to its one consumer, `scripts/goal_worker.py`
(`HANDLERS["hermes_storage_maintenance"] = handle_hermes_storage_
maintenance`) → `scripts/hermes_fixers.py::_inspect_storage_maintenance` →
`scripts/storage_curator.py::run_storage_maintenance` →
`build_storage_maintenance_report` (a single read-only `engine.connect()`
SELECT scan — no INSERT/UPDATE/DELETE anywhere in that call chain) +
`write_storage_maintenance_report` (writes the JSON/MD report files) +,
only on non-"ok" status, one `log_issue` INSERT into `operator_issues`.
That chain never imports or calls anything in `hypothesis_registry`/
`discovered_hypotheses`/`scanner_weights`/`trade_postmortems`/model-
registry code — the HELD categories. Grepped `enqueue_goal` call sites
project-wide: only `scripts/hermes_operator.py` (this task, via
`_dispatch_daily_storage_maintenance`) and `scripts/hermes_fixers.py`
(`_dispatch_subagent`, the general `DISPATCH_SUBAGENT` command) call it;
none of the other 12 allow-listed tasks enqueue anything. **Finding:
`storage_maintenance_subagent`'s dispatched child work cannot reach a
HELD category — kept in the initial allow-list, with `"done_queued"`
making its true (dispatch-only) semantics explicit rather than implying
the subagent's work completed.**

### D. Smallest useful initial subset — unchanged, confirmed

The existing 13-task `DAILY_INTEL_INITIAL_ALLOWLIST` (see the table two
sections up) is confirmed as the smallest subset whose effects AND retry
behaviour are fully understood from this review: every write is either a
keyed UPSERT/`ON CONFLICT DO NOTHING` (idempotent re-run safe) or an
append-only audit insert (`source_audit` — safe to rerun, just adds rows,
not silently overwritten data) or a full delete-then-rebuild
(`rag_index` — idempotent); every abandoned-run effect is an accepted,
already-happening write/deletion, not a new risk introduced by this task.
The 8 held tasks (`hypothesis_discovery`, `hypothesis_review`,
`backtest_scan`, `postmortem_batch`, `options_improvement`,
`milestone_scoring`, `actor_research`, `edgar_transcripts`) stay held —
each writes a learning/scoring/model-registry-adjacent table or is
LLM-driven (see `DAILY_INTEL_HOLD_REASONS` in `scripts/hermes_operator.py`
for the file:line evidence per task); no new information from this review
changes that. `rag_index` and `source_audit` were double-checked against
this review's specific worry ("does it call an embedding *service*, are
its writes idempotent?") and confirmed clean (see the table).

### E. Period wording — implemented

`daily_intel_period_outcome` now has four values instead of two:
`"complete"` / `"complete_with_skips"` (bare — reserved for the
hypothetical case of zero held tasks) and `"complete_for_enabled_tasks"` /
`"complete_for_enabled_tasks_with_skips"` (used whenever any task is
held — true today, 13 of 21 allow-listed). A new completion-only log line,
emitted once per call that completes the period (after the pre-existing
per-cycle progress line, which is unchanged):

```
daily_intel: period=<date> complete_for_enabled_tasks enabled=13 done=<a> done_queued=<b> skipped_for_period=<c> held=8
```

`done`, `done_queued`, `skipped_for_period`, and `held` are four separate
counts (never folded together) — `done_queued` isolates
`storage_maintenance_subagent`; `held` (never-attempted, standing
controller hold) is kept distinct from `skipped_for_period` (attempted
`DAILY_INTEL_MAX_ATTEMPTS` times, then gave up). See
`OperatorState.daily_intel_period_outcome`'s docstring
(`scripts/hermes_health.py`) for the full matrix and
`tests/test_hermes_daily_intel_resumable.py::
TestPeriodOutcomeWordingWithHeldTasks` (uses the REAL 13-allowed/8-held
table with no-op fns) for the pin.

### F. Tests and lint

`DB_PASSWORD=x PYTHONUTF8=1 python -m pytest tests/test_hermes_*.py
tests/test_postmortem_feedback.py -q`: **313 passed, 1 failed** — the
failure is the same pre-existing Windows-only
`tests/test_hermes_fixers.py::test_fix_output_dirs_skill_creates_common_output_directories`
flagged as known/unrelated in the original task brief and every prior
amendment; nothing in this review touches that file or that fixer.
`tests/test_hermes_daily_intel_resumable.py` alone: 29 tests (5 new for
this review — `TestDoneQueuedOutcome` ×2, `TestPeriodOutcomeWordingWithHeldTasks`
×2, `TestAbandonmentDoesNotPreventTaskEffects` ×1 — plus 1 pre-existing
assertion updated from `"complete"` to `"complete_for_enabled_tasks"` now
that a held task is present in that fixture).
`ruff check` on changed files: `hermes_operator.py` unchanged at 86
findings, `hermes_health.py` unchanged at 12 (no new findings from this
review's edits — both are comment/logic additions, not new blind-except
patterns), both edited test files clean.

## Coordinator decision (2026-09-20 05:5xZ): smallest useful initial subset = 11 tasks

After the effects table above, two of the thirteen reviewed tasks are **held for the initial subset** even though neither uses an LLM, because their retry/abandonment behaviour is not yet acceptable:

- `source_audit` — appends plain-INSERT rows to `source_accuracy` / `source_discrepancies` on every run (no `ON CONFLICT`), so an abandoned run plus a retry duplicates audit rows; it also rewrites `source_catalog.priority_rank`, which steers ingestion priority.
- `rag_index` — rebuilds `intelligence_embeddings` by DELETE-then-bulk-INSERT per `source_type`; an abandoned run keeps executing in its orphan thread while readers see a partially emptied index, and a retry repeats the full delete/rebuild.

Initial allow-list (11): `storage_maintenance_subagent` (done_queued; child work traced to a read-only storage report + conditional `operator_issues` insert, no held category reachable), `flow_materialize`, `icij_linking`, `attention_anomaly` (read-only), `corporate_actions` (SEC HTTP reads + idempotent `capital_flows` upserts), `capital_flow_rollups`, `fundamental_divergence`, `holder_deal_overlap`, `insight_cleanup`, `briefing_cleanup`, `errors_jsonl_cleanup`. Held (10): the eight standing-hold tasks plus the two above. With held tasks present the period is reported as **complete for enabled tasks**, with skipped and held counts kept separate.

Abandonment, restated: the attempt-token check runs only after a task returns; it suppresses the late ledger update and the in-flight registry prevents a concurrent retry. It does **not** stop the task: every underlying effect in the table (DB writes, the enqueue, SEC HTTP calls, file deletions/truncation) can still be performed by an abandoned run.

## Controller narrowing (2026-09-20, later same day): 8 non-cleanup tasks, late-write corruption analysis

Development only, same draft PR, same branch, no merge/production/DB/SSH. Two changes from the "11 tasks" decision above:

1. **The three cleanups are now held, not allowed.** `insight_cleanup`, `briefing_cleanup`, `errors_jsonl_cleanup` move to `DAILY_INTEL_HOLD_REASONS` with reason "held for the initial subset by the controller (2026-09-20): file deletion/truncation policy (directories, retention, exclusions) to be accepted separately." This is a policy hold, not a correctness finding — each cleanup's own deletion/truncation logic (see the effects table above) is deterministic, bounded, and idempotent; the hold exists because the retention windows/directories/exclusions themselves haven't been separately reviewed and accepted.
2. **The late-write corruption analysis below (item 4) was run per enabled task** to confirm the remaining 8 belong in the initial subset on correctness grounds, not just "no LLM."

**New allow-list (8):** `storage_maintenance_subagent`, `flow_materialize`, `icij_linking`, `attention_anomaly`, `corporate_actions`, `capital_flow_rollups`, `fundamental_divergence`, `holder_deal_overlap`.
**New hold-list (13):** the 8 standing-hold tasks (`hypothesis_discovery`, `hypothesis_review`, `backtest_scan`, `postmortem_batch`, `options_improvement`, `milestone_scoring`, `actor_research`, `edgar_transcripts`) + `source_audit` + `rag_index` + the 3 cleanups (`insight_cleanup`, `briefing_cleanup`, `errors_jsonl_cleanup`).

### Storage-dispatch row, confirmed correct

`storage_maintenance_subagent`'s own DB write is an **INSERT into `goal_queue`** (a database write, deduped by `enqueue_goal`'s dedupe window — see `intelligence/goal_queue.py::enqueue_goal`, `ON CONFLICT DO NOTHING`). The dispatched child work is executed later, by **goal_worker's `hermes_storage_maintenance` handler** (`scripts/goal_worker.py::handle_hermes_storage_maintenance` → `scripts/hermes_fixers.py::_inspect_storage_maintenance` → `scripts/storage_curator.py::run_storage_maintenance`), which **may INSERT an `operator_issues` row** (via `log_issue`, only when the storage report's status is not "ok"). This step's own outcome is `done_queued` — distinct from the dispatched child's completion, which is tracked by the goal queue (`goal_queue.state` / `goal_results`, `intelligence/goal_queue.py`), not by this ledger. This is already stated in the `DailyIntelTask` comment for this entry (`_daily_intel_storage_maintenance`'s docstring, `scripts/hermes_operator.py`) and in part C above; restated here per the controller's request to double-check it.

### Disclosures

- **Timed-out tasks continue their underlying work in the orphan thread; the attempt-token check only suppresses the late ledger update.** `_run_with_timeout` cannot kill a running thread — on a timeout it abandons the worker, which keeps executing `task.fn` to completion (every DB write, file write/deletion, dispatched enqueue happens exactly as if it had returned on time). The attempt-token check that runs afterward decides only whether *this ledger* records that return; it never decides whether the call happened. See "Abandonment truth" (part B above) and `_run_daily_intel_block`'s own docstring.
- **In-flight tracking prevents concurrent retries only within its supported scope: the same task name, in the same Hermes process, while the worker thread is alive as seen by `threading.enumerate()`.** It does **not** cover: other writers of the same tables (see the per-task "other writers" column below — the XBRL ingestor, `grid-scheduler`-style ingestion pullers, the API routers, `goal_worker.py`, or one-off `scripts/` loaders/backfills); child work dispatched by a task (e.g. the `hermes_storage_maintenance` goal, which `goal_worker` executes completely outside this registry); or a task from a *previous* Hermes process — `_DAILY_INTEL_IN_FLIGHT` is an in-memory `dict`, so it (and any in-flight entry in it) dies with the process at restart, same as the orphan thread it was tracking.
- **Idempotent upserts do not by themselves prevent an older result from overwriting a newer one.** Every enabled task's `ON CONFLICT DO UPDATE` (or `DO NOTHING`) has no version/`as_of`/`updated_at` *guard* — `as_of` is written unconditionally as `NOW()` on every successful write, it is never compared against the existing row's `as_of` before deciding whether to write. A late-returning orphan's `DO UPDATE` therefore writes whatever amount/value *that* run computed, unconditionally, regardless of whether a newer run (or a newer write from a different writer of the same row) already landed. What makes each of the 8 enabled tasks safe anyway is analyzed per task below — not the upsert shape itself.

### 4. Late-write corruption analysis, per enabled task

Method: grepped each table name across `ingestion/`, `intelligence/`, `api/`, `scripts/` (excluding `__pycache__`) for every writer, then read each writer's conflict key and `DO UPDATE`/`DO NOTHING` clause. "Other writers" excludes read-only `api/routers/*` GET endpoints and files that only reference the table name in prose/imports.

| Task | Conflict key & does `DO UPDATE` overwrite | Other writers of the same rows | Can a late write overwrite a NEWER value (restart case)? | Version/`as_of` guard | Verdict |
|---|---|---|---|---|---|
| `storage_maintenance_subagent` | `goal_queue` INSERT, `ON CONFLICT DO NOTHING` (`enqueue_goal`, `intelligence/goal_queue.py`) — can never overwrite an existing row | `scripts/hermes_fixers.py::_dispatch_subagent` (general `DISPATCH_SUBAGENT` command), `scripts/goal_worker.py` (consumer — updates `goal_queue.state`, not a competing producer), `scripts/seed_goals_hypo_scoring.py` (manual one-shot seed) | No — `DO NOTHING` means a late/abandoned enqueue that lands after a newer row already exists for the same dedupe key is a silent no-op, not an overwrite | N/A (nothing to guard; `DO NOTHING` is itself the guard) | **safe** — read-only w.r.t. existing rows (`DO NOTHING`) |
| `flow_materialize` | `INSERT ... ON CONFLICT DO UPDATE` on 5 tables, each keyed on its own natural key (e.g. `(ticker, report_date)` for `dark_pool_weekly`, `(ticker, flow_date, source)` for `etf_flows`); `DO UPDATE` overwrites value columns unconditionally | None found — `ingestion/altdata/institutional_flows.py` (the raw puller feeding this projection) writes only `signal_sources`/`raw_series`, never these 5 tables directly; no other writer in `ingestion/`, `intelligence/`, `api/`, `scripts/` | Only within the process-restart case, and only until the next scheduled run: `ingestion/flow_materializer.py::sync_all` is the sole writer and recomputes each row deterministically from current `signal_sources`/`raw_series` state every run, so a late write just re-derives whatever the source data said at ITS read time — self-corrects on the next run | No — `as_of`-style columns (where present) are set unconditionally, not compared | **safe** — sole writer, deterministic recompute, stale window at most one period |
| `icij_linking` | `INSERT INTO icij_actor_matches ... ON CONFLICT DO NOTHING` (`intelligence/icij_linker.py::_store_matches`) — can never overwrite an existing row | None found (`intelligence/actor_researcher.py` only mentions the table name in a comment; does not write it) | No — `DO NOTHING` | N/A | **safe** — read-only w.r.t. existing rows (`DO NOTHING`) |
| `attention_anomaly` | None — this step's own body (`intelligence/attention_anomaly.py::get_alerts`) is a read-only `SELECT`; it performs no `INSERT`/`UPDATE`/`DELETE` itself | N/A (nothing this step writes) | N/A | N/A | **safe** — read-only |
| `corporate_actions` | `capital_flows`, conflict key `(actor_id, fiscal_period, period_type, flow_type, counterparty_id, source_filing)`; `source_filing = "8-K {date} {accession}"` (unique per filing). `DO UPDATE` overwrites `amount_usd`/`as_of` unconditionally, `counterparty_id` via `COALESCE(existing-non-null, EXCLUDED)` (never regresses a resolved value to NULL). A pre-step `UPDATE ... WHERE counterparty_id IS NULL OR ''` back-fills only still-unresolved rows | `ingestion/altdata/sec_xbrl_financials.py` (writes `period_type IN ('annual','quarter')`, disjoint from `'announcement'` — confirmed by reading `_pick_period_type`); `intelligence/company_financial_rollups.py` (writes `period_type IN ('ttm','annual')` with constant `source_filing` values `'ttm_rollup'`-style / `'announcement_rolled'`, both disjoint from live `"8-K ..."` strings); `scripts/backfill_announcement_counterparties.py` (manual one-shot, same `period_type='announcement'` rows, but its own `UPDATE` is guarded to `counterparty_id IS NULL` — cannot regress an already-resolved value, symmetric with corporate_actions' own guard); `scripts/load_supply_capital_seed.py` (manual one-shot hand-curated seed loader, uses seed-file `source_filing` values, not live accession strings) | No collision with the other *scheduled* writers (disjoint `period_type`/`source_filing`). Against itself: `source_filing` embeds the immutable 8-K accession number, and the regex parse of that filing's static text is deterministic, so a late-returning parse of the SAME filing reproduces the SAME `amount_usd` — not a corruption. The counterparty back-fill is one-directional (NULL→resolved only), so a late run with a stale alias dict cannot un-resolve a value a newer run already set | No — `as_of` set unconditionally | **safe** — sole writer of `period_type='announcement'`; disjoint keys from every other writer; deterministic re-parse of immutable filing text |
| `capital_flow_rollups` | `capital_flows`, same 6-column conflict key as above but with `(COALESCE(NULLIF(counterparty_id,''), '__none__'))` as the functional conflict column; TTM rows use constant `source_filing=TTM_SOURCE_FILING`, rolled-announcement rows use constant `source_filing='announcement_rolled'` (`intelligence/company_financial_rollups.py::ROLLED_SOURCE_FILING`). `DO UPDATE` overwrites `amount_usd`/`direction`/`confidence`/`currency`/`as_of` unconditionally | None found writing `period_type='ttm'` or `source_filing='announcement_rolled'` anywhere else in `ingestion/`, `intelligence/`, `api/`, `scripts/` (`scripts/run_capital_flow_rollups.py` is a CLI wrapper calling the same `compute_ttm`/`fold_announcements` functions, not a separate writer) | Only the process-restart case: sole writer of these two constant-`source_filing` subsets, deterministically recomputed from current `capital_flows` quarter/announcement rows every scheduled run — a late write reflects the input state at its own read time; the next run recomputes and overwrites again | No — `as_of` set unconditionally | **safe** — sole writer, deterministic recompute, stale window at most one period |
| `fundamental_divergence` | `fundamental_divergence`, conflict key `(ticker, as_of)`; `DO UPDATE` overwrites the snapshot columns unconditionally (`intelligence/fundamental_divergence.py::snapshot_all`) | None found — only `intelligence/trust_scorer.py` (unrelated weight-table constants referencing the string) and `api/routers/divergence.py` (read-only) mention the table | Only the process-restart case: sole writer, deterministic recompute from current fundamentals+price data every scheduled run | No | **safe** — sole writer, deterministic recompute, stale window at most one period |
| `holder_deal_overlap` | `holder_deal_overlap`, conflict key `(deal_announcement_date, acquirer_ticker, target_ticker, filer_name)`; `DO UPDATE` overwrites position/flag/narrative columns unconditionally (`intelligence/holder_deal_overlap.py`) | None found — only `api/routers/actor_detail.py` (read-only, conditionally imports the module to render UI) mentions the table | Only the process-restart case: sole writer, deterministic recompute from current 13F snapshots + `capital_flows` announcement rows every scheduled run | No | **safe** — sole writer, deterministic recompute, stale window at most one period |

**Net result:** all 8 enabled tasks verdict `safe` on late-write grounds — this confirms (does not change) the allow-list; no additional task needed a hold from this analysis. The two open questions the controller specifically flagged are answered: **no**, the XBRL ingestor does not write `corporate_actions`' `period_type='announcement'` rows (it writes `annual`/`quarter`, a disjoint `period_type`); and the `ttm`/`announcement_rolled` period types `capital_flow_rollups` writes are not written by any ingestor — `capital_flow_rollups` is their sole writer.

> **Superseded by "Release-controller rejection response" below (2026-09-20, amendment 2).** The controller rejected this table for asserting "sole writer" per *table* without checking whether the same *functions* are invoked from other paths/processes the in-flight registry can't see, and for leaning on "deterministic computation" as a conclusion rather than a derivation. The table above is kept for history; treat the reconciled table below as authoritative for the verdicts and bounds.

### Allow-list and pin tests, updated

`DAILY_INTEL_INITIAL_ALLOWLIST` in `scripts/hermes_operator.py` now holds exactly the 8 tasks above; `DAILY_INTEL_HOLD_REASONS` gained the 3 cleanup entries (13 total). `tests/test_hermes_daily_intel_resumable.py` updated: `TestDailyIntelAllowlistClassification::test_expected_allow_set` (8-task set), `test_standing_hold_categories_are_represented` (13-task hold set including the 3 cleanups), and `TestPeriodOutcomeWordingWithHeldTasks::test_real_allowlist_with_13_held_reports_complete_for_enabled_tasks` (renamed from `..._with_8_held...`; counts `n=8, d=7, dq=1, s=0, h=13`). Two stale prose comments referencing the allow-list size ("13 of 21 allow-listed") in `scripts/hermes_operator.py` and `scripts/hermes_health.py` updated to "8 of 21 allow-listed" so no hard-coded count disagrees with the frozenset.

### Tests

`DB_PASSWORD=x PYTHONUTF8=1 python -m pytest tests/test_hermes_*.py tests/test_postmortem_feedback.py -q`: **313 passed, 1 failed** — the failure is the same pre-existing Windows-only `tests/test_hermes_fixers.py::test_fix_output_dirs_skill_creates_common_output_directories` flagged as known/unrelated in every prior section of this doc; this narrowing does not touch that file or that fixer. `tests/test_hermes_daily_intel_resumable.py` alone: 29 tests, all passing with the new 8-allowed/13-held split.

## Release-controller rejection response (2026-09-20, amendment 2): overlapping-writer risk redone per invocation path

Development only, same draft PR (#584), same branch (`fable/daily-intel-resumable-20260920`), no merge/production/DB/SSH.

**The rejection.** The controller rejected the "8 non-cleanup tasks" analysis above (section 4) for two reasons: (1) it asserted "sole writer" per *table* by grepping the table name, without checking whether the same *function* each Hermes task calls is also invoked from a path in a *different process* that `_DAILY_INTEL_IN_FLIGHT` cannot coordinate, specifically naming a hypothetical flow-materializer timer as an example; (2) it leaned on "deterministic computation" as a stated conclusion rather than deriving, from code, when a stale write is actually guaranteed to be corrected.

**Method, redone.** For each of the 8 allow-listed tasks underlying function: grepped every `.py` caller across the whole repo (`ingestion/`, `intelligence/`, `api/`, `scripts/`, `analysis/`, `trading/`, `contracts/`, excluding `tests/`), then separately grepped every `server_setup/*.service` and `*.timer` file, `ingestion/scheduler.py` (the process `grid-scheduler.service` runs), `ingestion/smart_scheduler.py`, `intelligence/scheduler.py` (the process `grid-intelligence.service` runs), `api/main.py`'s deferred-startup block, and `scripts/goal_worker.py`'s `HANDLERS` dict, for the function name, not the table name, to find every PROCESS that can call it and on what CADENCE. Then read each writer's `ON CONFLICT` key and whether the query producing its rows is date/limit-windowed (incremental) or scans its full source table with no filter (full recompute), to derive the actual stale-write bound.

**1. Invocation paths per task, with process and cadence:**

| Task | Function | All invocation paths found (process, cadence) |
|---|---|---|
| `storage_maintenance_subagent` | `scripts/hermes_fixers.py::_dispatch_daily_storage_maintenance` calling `intelligence/goal_queue.py::enqueue_goal(goal_type="hermes_storage_maintenance")` | grid-hermes, daily (`DAILY_INTEL_TASKS`, only caller in the repo). Downstream, a separate unit of work not tracked by this ledger: `scripts/goal_worker.py::handle_hermes_storage_maintenance`, run by whichever node's `goal_worker.py` process next polls a `cpu`-tier goal (no `server_setup/*.service` found for `goal_worker.py` in this repo snapshot, documented as unverified rather than guessed) |
| `flow_materialize` | `ingestion/flow_materializer.py::sync_all` | grid-hermes, daily (only caller found outside `tests/`). Confirmed NOT called by `ingestion/scheduler.py` (grid-scheduler.service) or `ingestion/smart_scheduler.py`: `smart_scheduler.py`'s `etf_flows` registry entry is `ingestion.altdata.institutional_flows.InstitutionalFlowsPuller.pull_all`, a different function writing only `signal_sources`, never `sync_all`'s 5 target tables. This directly answers the controllers named hypothesis: there is no flow-materializer timer |
| `icij_linking` | `intelligence/icij_linker.py::link_actors` | grid-hermes, daily (only caller found) |
| `attention_anomaly` | `intelligence/attention_anomaly.py::get_alerts` | grid-hermes, daily; also `api/routers/intelligence_actors.py` line 704, grid-api, on-demand per HTTP request (a real second process, but see write key below) |
| `corporate_actions` | `ingestion/altdata/corporate_actions_parser.py::CorporateActionsParser.pull` | grid-hermes, daily, `days_back=30`. `scripts/run_corporate_actions.py`, manual CLI, human-run only, default `days_back=1500` (no `server_setup/*.service`, `*.timer`, or `.github/*.yml` trigger found for this script anywhere in the repo). `scripts/backfill_announcement_counterparties.py`, manual, one-shot, NULL-counterparty-only `UPDATE` (not the same INSERT path) |
| `capital_flow_rollups` | `intelligence/company_financial_rollups.py::run_all` | grid-hermes, daily. `scripts/run_capital_flow_rollups.py`, manual CLI wrapper around the same `run_all`, no schedule found |
| `fundamental_divergence` | `intelligence/fundamental_divergence.py::snapshot_all` | grid-hermes, daily. `scripts/run_fundamental_divergence.py`, manual CLI wrapper around the same `snapshot_all`, no schedule found |
| `holder_deal_overlap` | `intelligence/holder_deal_overlap.py::run` | grid-hermes, daily. `scripts/run_holder_deal_overlap.py`, manual CLI wrapper around the same `run`, no schedule found |

Also checked and ruled out as write-path overlaps: `intelligence/scheduler.py` (`grid-intelligence.service`, confirmed a real, separate, always-restarting systemd process running a `schedule`-library loop with 15min/1h/4h/daily/weekly timers) — its `_capital_flow_refresh` task (every 4h) calls `analysis/capital_flows.py::CapitalFlowResearchEngine.run_research`, which writes `capital_flow_snapshots` keyed on `snapshot_date`, a table disjoint from `capital_flows` (no key overlap with `corporate_actions` or `capital_flow_rollups`); `api/main.py::_sync_deferred_startup` (grid-api startup, once) touches `oracle_models`, the spider graph cache, the dashboard cache, and the sector-flow cache, none of the 8 tasks' tables.

**2. Registry scope, stated precisely.** Written into the code comment on `_DAILY_INTEL_IN_FLIGHT` in `scripts/hermes_operator.py`, not softened: it coordinates only same-named tasks within the same grid-hermes process while the worker thread is alive. It does not coordinate grid-scheduler, grid-api, grid-intelligence, `goal_worker.py`, a manual script, or a previous grid-hermes process.

**3 and 5. Stale-write bound, derived from code, and verdict, reconciled table:**

| Task | Write key (`ON CONFLICT` target) | Full recompute or incremental | Stale-write bound (from code) | Verdict |
|---|---|---|---|---|
| `storage_maintenance_subagent` | `goal_queue` INSERT, `ON CONFLICT DO NOTHING` (dedupe-window partial unique index) | N/A, single dedup-guarded insert | N/A, `DO NOTHING` means a duplicate enqueue is silently dropped, never an overwrite | allow |
| `flow_materialize` | 5 natural keys, one per sub-table: `(ticker, report_date)` dark_pool_weekly, `(ticker, flow_date, source)` etf_flows, `(ticker, trade_date, insider_name, trade_type)` insider_trades, `(ticker, disclosure_date, representative, transaction_type)` congressional_trades, `(series_key, obs_date)` junction_point_readings | Incremental: each sub-materializer reads `ORDER BY ... DESC LIMIT N` (N = 5,000 to 50,000) off `signal_sources`, a row-count window, not a calendar one | At most the next successful grid-hermes run of `flow_materialize` (daily), as long as the event's raw row stays inside the LIMIT-N-most-recent slice; true across many consecutive daily runs at observed volumes, not a proven full-history guarantee. Single automated writer, no cross-process race found | allow |
| `icij_linking` | `icij_actor_matches`, `ON CONFLICT DO NOTHING` | Incremental subset per run (`actors LIMIT 1000`, `limit=500` matches) | N/A for overwrite: `DO NOTHING` means the first successful write for a key is permanent; safe here because the computed match score is a pure function of two largely-static text columns, so there is no legitimate fresher value being blocked. Residual: full-universe coverage is gradual, a completeness gap, not a staleness risk | allow |
| `attention_anomaly` | none, read-only in both invocation paths (`get_alerts` calling `score_attention`, a `SELECT`) | N/A | N/A, nothing is written, so nothing can be stale | allow |
| `corporate_actions` | `capital_flows`, `(actor_id, fiscal_period, period_type='announcement', flow_type, counterparty_id, source_filing)`; `source_filing` embeds the immutable 8-K accession number | Incremental: grid-hermes's own call is windowed to the trailing 30 days of 8-Ks; a filing drops out of its reprocessing scope about 30 days after filing | At most the next successful grid-hermes run while the filing is under 30 days old: `source_filing` is keyed by the immutable accession and the regex extraction is a pure function of that filing's static text, so any writer (grid-hermes or a manual script) that reprocesses the SAME filing computes the SAME value; order and count of writers do not matter within the window. Residual, stated explicitly, not smoothed over: a filing whose only successful write happened during an abandoned run near that 30-day boundary, with no manual script reprocessing it in time, keeps that value indefinitely once past 30 days old. This is a same-writer, incremental-window edge case (no automated second process found), not an uncoordinated cross-process race | allow, residual noted, does not meet the HOLD bar (no second automated writer and the bound is derivable, just not indefinite in the boundary case) |
| `capital_flow_rollups` | `capital_flows`, same 6-column key with constant `source_filing` of either `ttm_rollup` or `announcement_rolled` | Full recompute: `compute_ttm` scans ALL `period_type='quarter'` rows, `fold_announcements` scans ALL `period_type='announcement'` rows, no date filter in either query | At most the next successful run of `run_all` (grid-hermes daily, or an ad hoc manual run): full recompute rewrites every key it owns from current state on every run, so a stale value cannot outlive the next successful run by any writer | allow |
| `fundamental_divergence` | `fundamental_divergence`, `(ticker, as_of=today)` | Full recompute: `_load_universe()` has no date or limit filter; every eligible ticker is rescored and upserted every run | At most the next successful run today (same `as_of` key); a new day changes the key entirely, so there is no cross-day carryover risk either | allow |
| `holder_deal_overlap` | `holder_deal_overlap`, `(deal_announcement_date, acquirer_ticker, target_ticker, filer_name)` | Full recompute: `find_deals()` returns every acquisition announcement with a non-null target (no date filter); `run()`'s own docstring calls itself a full detection pass | At most the next successful run of `run`: full recompute over every known deal rewrites every key from current 13F and `capital_flows` state | allow |

**4. Verdict and allow-list.** All 8 tasks' overlapping-writer risk is resolved (read-only, `DO NOTHING` against a time-invariant value, or full-recompute with a stated bound) even accounting for every invocation path found, including the two genuine second processes (`api/routers/intelligence_actors.py` calling `attention_anomaly.get_alerts`, read-only; `intelligence/scheduler.py`'s 4h capital-flow refresh, disjoint table). No task moves. `DAILY_INTEL_INITIAL_ALLOWLIST` stays the same 8 tasks; `DAILY_INTEL_HOLD_REASONS` stays the same 13. The `corporate_actions` residual above is recorded in the "Overlapping-writer analysis" code comment in `scripts/hermes_operator.py` as a documented, accepted residual, not a reason to hold, per the controller's own bar: it requires a same-writer near-boundary failure with no automated second writer, not an incremental writer with another writer or process.

**Net result:** the flow-materializer-timer hypothesis is directly refuted by code — no such timer exists; `smart_scheduler.py`'s `etf_flows` entry is a different function on a different table. A real second process was found for exactly one task (`attention_anomaly`, via `api/routers/intelligence_actors.py`) and is read-only. A real, separate, always-on second scheduler process was found (`grid-intelligence.service` / `intelligence/scheduler.py`) that does write capital-flow-adjacent data, but to a disjoint table (`capital_flow_snapshots`, not `capital_flows`). Every sole-writer claim in the superseded table above is now qualified by an explicit invocation-path grep rather than a table-name grep, and every safe verdict is now backed by a full-recompute-vs-incremental classification and a derived bound instead of the phrase "deterministic computation."

### Tests, amendment 2

No production code behavior changed: this amendment only adds documentation (code comments in `scripts/hermes_operator.py` above `DAILY_INTEL_INITIAL_ALLOWLIST` and on `_DAILY_INTEL_IN_FLIGHT`) and this doc section; `DAILY_INTEL_INITIAL_ALLOWLIST` and `DAILY_INTEL_HOLD_REASONS` are unchanged, so no pin-test assertion needed to change. `tests/test_hermes_daily_intel_resumable.py::TestDailyIntelAllowlistClassification::test_expected_allow_set`'s comment was updated to point at this section instead of citing "deterministic recompute" as the reason. Full suite: `DB_PASSWORD=x PYTHONUTF8=1 python -m pytest tests/test_hermes_*.py tests/test_postmortem_feedback.py -q`, see report footer for the exact count from this amendment's run.
