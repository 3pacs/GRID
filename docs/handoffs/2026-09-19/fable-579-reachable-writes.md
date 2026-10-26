# PR #579 reachable-write inventory (read-only)

Scope: branch `fable/autoresearch-subtype-20260919` (head `6ad426f8`),
worktree `C:/Users/owner/dev/GRID-fable-wt-hermes-research`, inspected
read-only — nothing changed there. All file:line citations are against
that worktree unless marked `[#570]`, which cites
`origin/fable/write-fencing-20260918` (head `38a9ca7d`), read via `git show`
/ `git diff`, not checked out.

## Why this inventory exists

On current `main` (and on #579's branch, before its own fix), every
Hermes autoresearch run fails at data-load phase `feature_list`:
`scripts/autoresearch.py:437-472`'s `get_feature_list()` selected
`COALESCE(f.subfamily, '')`, and production `feature_registry` has no
`subfamily` column — `psycopg2.errors.UndefinedColumn` on every call,
inside `_load_research_context()` (`scripts/autoresearch.py:501-532`),
which `run_autoresearch()` (`scripts/autoresearch.py:742-758`) catches and
turns into a `_failed_result(phase="feature_list", ...)` before any
hypothesis, backtest, model, or notification code ever runs. #579 changes
the query to `COALESCE(f.signal_subtype, '')` (`scripts/autoresearch.py:456-471`).
Once #579 is deployed, that query works and every line below becomes
reachable for the first time in production.

## Gates that decide whether a run starts at all

1. **`run_cycle`** (`scripts/hermes_operator.py:2134`): `state.cycle_count
   % 12 == 0 and health.get("overall_healthy") and hermes_ok`. Fires once
   per 12 operator cycles (~hourly) only while Hermes reports itself
   healthy.
2. **`maybe_run_autoresearch`** (`scripts/hermes_fixers.py:1703-1707`,
   this task's worktree pre-dates the AUTORESEARCH_ENABLED gate added in
   Task 2 of this same session): returns `None` (no-op) unless
   `state.last_autoresearch` is `None` or ≥12h old; also short-circuits on
   `dry_run` (`scripts/hermes_fixers.py:1709-1710`).
3. **`config.Settings.AUTORESEARCH_ENABLED`** (`config.py:502`, default
   `True` on this branch as read) — confirmed **NOT consulted anywhere**:
   `grep -rn "AUTORESEARCH_ENABLED" --include=*.py .` returns exactly one
   hit, the declaration itself. No caller reads it. (This gap is what
   Task 2 of this session's branch, `fable/research-gate-20260919`,
   closes — out of scope for this read-only inventory.)

None of these three gates is touched by #579.

## Fencing mechanisms in play

- **`_AutoresearchGenerationTracker`** (`scripts/hermes_operator.py:290-340`)
  + **`_generation_current()`** (`scripts/autoresearch.py:290-315`): an
  in-process monotonic counter. `run_cycle` mints a `generation` before
  handing work to `_run_with_timeout` (`scripts/hermes_operator.py:2138`);
  every write site in `run_autoresearch()` re-checks
  `is_current_generation(generation)` first. Scope, per its own docstring
  (`scripts/hermes_operator.py:317-320`): fences a stale **thread within
  the same process** (e.g. an abandoned worker after `_run_with_timeout`
  gives up). It does **not** fence a second Hermes process or a worker
  that survives a process restart.
- **PR #570** (`fable/write-fencing-20260918`) adds `governance/leases.py`
  (`research_leases` table, `SELECT ... FOR UPDATE` row lock, `acquire` /
  `heartbeat` / `release` / `guarded_write(_dbapi)`) — a cross-process
  lease. `scripts/autoresearch.py` on that branch wraps every real write
  through `_guarded_or_direct()` (`/tmp` copy line 319-342 of that
  branch's `scripts/autoresearch.py`, i.e. the file at that path on
  `fable/write-fencing-20260918`) when a `lease_generation` is supplied.

**What #570 explicitly does NOT do** (from its own `governance/leases.py`
module docstring and `scripts/autoresearch.py` comments on that branch):
it prevents an **abandoned/stale worker** (orphaned thread past a
timeout, or a second/restarted Hermes process) from committing a write
after it has lost ownership. It does **not** decide **whether** a
research run is allowed to start, whether a hypothesis is allowed to
reach PASS, or whether a PASS is allowed to send an email — a live,
still-owning worker sails through every `guarded_write` check and
performs every write and every notification below exactly as it does
today. Gating "is research allowed at all" is `AUTORESEARCH_ENABLED`'s
job (Task 2), not #570's.

## Reachable writes, in execution order

Legend for column (b) fence status: **fenced in-process** = checked via
`_generation_current`/`_AutoresearchGenerationTracker` only; **checked
before call, not at write time** = a plain existence/state check (e.g.
`SELECT ... LIMIT 1`) with no lock, so a race is possible even
in-process; **unfenced** = no fencing mechanism at all on `main`
(#579's branch).

### 1. `research_run` status trail (best-effort, always allowed)

| # | Call site | Table | Condition | (b) Fence (this branch) | (c) Would #570 fence it? | (d) Data / readers |
|---|---|---|---|---|---|---|
| 1a | `scripts/autoresearch.py:702-705` (`_record_research_run(..., "started", phase="init")`) | `analytical_snapshots` (category=`research_run`, subcategory=`autoresearch`), via `store/snapshots.py:275` INSERT | Every call, right after `get_engine()` | unfenced (deliberately — it's the status trail itself) | **No.** #570's branch comment (`scripts/autoresearch.py` on that branch, near the final `_record_research_run` call) states this write is "NOT lease-guarded (deliberately... gating it behind the same lease it is reporting the loss of would be circular)". Stays best-effort on both branches. | Operator/status dashboards reading `analytical_snapshots` by category=`research_run`; no downstream automation reads it. |
| 1b | `scripts/autoresearch.py:714-719` | same | Only if Ollama unavailable (`status="failed"`) | unfenced | No (same as above) | same |
| 1c | `scripts/autoresearch.py:732-737` | same | Only if the psycopg2 connect fails (`status="failed"`) | unfenced | No | same |
| 1d | `scripts/autoresearch.py:753-758` | same | Only if `_load_research_context` raises `AutoresearchDataError` (`status="failed"`) — this is the branch #579 fixes; on `main` today this is the ONLY row ever written | unfenced | No | same |
| 1e | `scripts/autoresearch.py:778-782` (`status="running", phase="context_loaded"`) | same | Once context loads successfully — first row only reachable after #579 | unfenced | No | same |
| 1f | `scripts/autoresearch.py:823-827` (`status="running", phase="iteration"`) | same | Once per loop iteration (1..`max_iterations`, default 5), AFTER the per-iteration fencing check at `scripts/autoresearch.py:810` | unfenced (the checkpoint write itself); the loop's continuation is fenced in-process | No | same |
| 1g | `scripts/autoresearch.py:1188-1193` (terminal `status` = `ok`/`abandoned`) | same | End of every run | unfenced | No | same |
| 1h | `scripts/hermes_operator.py:2160-2168` (`_record_research_run(..., "timeout", ...)`) | same | Only if `_run_with_timeout` reports the autoresearch step exceeded `AUTORESEARCH_TIMEOUT_SECONDS` (default 1800s, `scripts/autoresearch.py:56`) | unfenced (written by the operator itself, not the abandoned worker) | No | same |

### 2. `hypothesis_registry` INSERT

| # | Call site | Condition | (b) Fence | (c) #570 | (d) Data / readers |
|---|---|---|---|---|---|
| 2 | `scripts/autoresearch.py:939-952` — `INSERT INTO hypothesis_registry (statement, layer, feature_ids, lag_structure, proposed_metric, proposed_threshold, state='TESTING')` | Only when no existing row matches `(statement, layer)` (dedup check at `scripts/autoresearch.py:922-927`); reached once per NEW hypothesis per iteration, after the per-iteration generation check at line 810 | **fenced in-process** — gated by the loop's line-810 check for that iteration, but the INSERT itself (line 939) has no fresh re-check immediately before it beyond that iteration-start check | **Yes.** On #570's branch this insert runs through `_guarded_or_direct(cur, pg, lease_generation, _insert_hypothesis)` (that branch's `scripts/autoresearch.py:1007`) — a cross-process `SELECT...FOR UPDATE` check-then-write in one transaction | Read by the backtester (`hypothesis_id` FK), the API's hypothesis list/detail routes, and re-read by the next iteration's dedup `SELECT` (line 923). |

### 3. `hypothesis_registry` UPDATE (state transitions)

| # | Call site | Condition | (b) Fence | (c) #570 | (d) |
|---|---|---|---|---|---|
| 3a | `scripts/autoresearch.py:1058-1061` — `SET state='FAILED', kill_reason=...` | Only if `backtester.run_validation()` raises an exception | **unfenced** — no generation check between the exception and this UPDATE | Yes — guarded via `_guarded_or_direct` (`_mark_failed`) on #570's branch | Read by API hypothesis views, by the loop's own dedup `SELECT` on retry |
| 3b | `scripts/autoresearch.py:1090-1093` — `SET state=%s (PASSED/FAILED), kill_reason=..., updated_at=NOW()` | Every completed backtest (verdict PASS/FAIL/CONDITIONAL → PASSED or FAILED), reached after the pre-backtest fencing check at line 1031 | **checked before call, not at write time** — the check is before `run_validation()` is invoked (line 1031), not immediately before this UPDATE which runs after the (possibly slow) backtest completes | Yes — guarded | same |

### 4. `validation_results` INSERT

| # | Call site | Condition | (b) Fence | (c) #570 | (d) |
|---|---|---|---|---|---|
| 4 | `validation/backtest.py:340-362` (`WalkForwardBacktest._store_result`, called internally by `run_validation()` at `validation/backtest.py:356`, invoked from `scripts/autoresearch.py:1048-1055`) | Every completed walk-forward backtest | **unfenced on `main`** — `run_validation()` on this branch takes no write-guard parameter at all (`validation/backtest.py:62-89` constructor has no `write_guard` param on `main`); `scripts/autoresearch.py`'s own comment at line 1024-1030 notes it fences only "at the call boundary" (the line-1031 check before calling `run_validation`), not inside `_store_result` itself | **Yes.** #570 adds a `write_guard` constructor param to `WalkForwardBacktest` (`validation/backtest.py:66,89` on that branch) and `_store_result` calls `self._write_guard(_do_insert)` when set (that branch's `validation/backtest.py:820-821`); `scripts/autoresearch.py` on #570 passes a closure wrapping `governance.leases.run_guarded` (`scripts/autoresearch.py:752-756` on that branch) | Read by API model/validation routers, by `_create_model_from_hypothesis` (item 5, via a `SELECT ... ORDER BY run_timestamp DESC LIMIT 1`), by the REFINE_PROMPT's critique step next iteration. |

### 5. `model_registry` INSERT (auto-CANDIDATE creation)

| # | Call site | Condition | (b) Fence | (c) #570 | (d) |
|---|---|---|---|---|---|
| 5a | `scripts/autoresearch.py:624-634` (`_create_model_from_hypothesis`, `INSERT INTO model_registry ... state='CANDIDATE'`), invoked from `scripts/autoresearch.py:995` | Reused-PASSED-hypothesis idempotent-retry path: only if a hypothesis reused from a prior attempt was already `PASSED` and has no existing `model_registry` row yet (`scripts/autoresearch.py:989-992`) | **checked before call, not at write time** — gate is `_generation_current(...)` at line 993 plus the pre-existing-row `SELECT` at line 989-992; no re-check between that `SELECT` and the `INSERT` inside `_create_model_from_hypothesis` | Yes — wrapped via `_guarded_or_direct` on #570's branch (that branch's lines 1063-1064) | Read by API model registry views; downstream promotion workflow (CANDIDATE→SHADOW→STAGING→PRODUCTION, `governance/registry.py`) — **not itself triggered here**, only the CANDIDATE row is created. |
| 5b | `scripts/autoresearch.py:1134-1145`, same `_create_model_from_hypothesis` call, invoked from the fresh-PASS branch | Only on a genuinely NEW hypothesis that PASSES this run, and only if no existing `model_registry` row for it yet (`scripts/autoresearch.py:1134-1138`), gated by the line-1126 in-iteration check | **checked before call, not at write time** — same shape as 5a: `_generation_current` checked at 1126, then a `SELECT` at 1134-1137, then the INSERT; no lock across those three steps on `main` | Yes — guarded on #570's branch (lines 1298-1299 there) | same |

### 6. Outbound email notification (SMTP)

| # | Call site | Condition | (b) Fence | (c) #570 | (d) |
|---|---|---|---|---|---|
| 6 | `scripts/autoresearch.py:1151-1155` → `scripts.notify.notify_on_pass(attempt)` (`scripts/notify.py:373-394`) → `send_insight_email()` (`scripts/notify.py:71-...`) → `smtplib.SMTP`/`SMTP_SSL` connect+send at `scripts/notify.py:119,121` | Only on a genuinely new PASS this run (the reused-PASSED idempotent-retry branch at line 999-1004 explicitly skips this call with a comment explaining why) | **UNFENCED on `main`** — no generation/lease check between the model-creation step above and this `notify_on_pass` call; it fires unconditionally once code reaches this line, even if model creation at 5b just failed for reasons unrelated to fencing | **Partially, on #570's branch, but not directly.** #570's `scripts/autoresearch.py` (that branch) introduces a `model_creation_fenced` flag: it suppresses `notify_on_pass` when the preceding model-creation step was skipped as fenced (either in-process generation stale, or `governance.leases.OwnershipLost`) — see that branch's comment "a notification is a real-world side effect... with no transactional rollback, so the guard's job here is to prevent the call from ever happening, not to wrap it." There is **no lease check immediately before the SMTP send itself** — it rides on the model-creation check's outcome, so a lease lost in the narrow window between that check and the send would still notify. | Recipient: `ALERT_EMAIL_TO`/`GRID_NOTIFY_EMAIL` (human operator inbox) — real-world side effect, not reversible by any DB rollback. |

### 7. Git subprocess (read-only, not a write)

| # | Call site | Condition | Notes |
|---|---|---|---|
| 7 | `scripts/autoresearch.py:202-221` (`_get_code_sha`) — `subprocess.run(["git", "rev-parse", "HEAD"], ...)` | Every `run_autoresearch()` call, before the DB connection | Read-only (`git rev-parse HEAD`), 5s timeout, best-effort (any failure → `None`). Not a write; included for completeness since the task asked for "any git/subprocess." No other `subprocess`/`git` calls exist in the reachable path — verified via `grep -n "subprocess\|git\." scripts/autoresearch.py`. |

### 8. Outbound LLM calls (Ollama, not a DB write but a real network call)

| # | Call site | Condition | Notes |
|---|---|---|---|
| 8a | `scripts/autoresearch.py:888-893` — `ollama.chat(messages, ...)`, implemented as `requests.post(...)` in `ollama/client.py` (e.g. line 134/460 depending on which client class `get_client()` returns — see `ollama/client.py:587`) | Every iteration, hypothesis GENERATE or REFINE step | Local Ollama HTTP endpoint, not internet-facing per `CLAUDE.md`'s LLM section. No fencing applies (not a write); `ollama.is_available` is checked once up front (`scripts/autoresearch.py:712`). |
| 8b | `scripts/autoresearch.py:858-864` — `reasoner.critique_backtest_result(...)` (`ollama/reasoner.py:168`), itself calling the same Ollama client | Only on iterations ≥2 when the prior attempt has a `statement` (i.e., after a FAIL, before the REFINE prompt) | Same as 8a |

## What #570 changes about "is a run allowed to happen"

Nothing. #570's `maybe_run_autoresearch` (that branch,
`scripts/hermes_fixers.py:1680-1710`) adds `lease_generation` /
`lease_owner_id` parameters that are purely forwarded to
`run_autoresearch()` — the 12h-cooldown / `dry_run` checks are byte-for-
byte the same as on `main`. The lease is **acquired** by
`scripts/hermes_operator.py`'s cycle-6 gate (that branch, line ~2054,
`governance.leases.acquire(engine, "autoresearch", owner_id)`) only AFTER
the exact same `cycle_count % 12 == 0 and health["overall_healthy"] and
hermes_ok` condition that gates `main` today — it is a second, cross-
process lock on top of an already-permitted run, not a new permission
gate. Likewise it does not gate whether a PASS is allowed to email — see
item 6 above.

## Summary (10 lines)

1. #579 alone makes writes 1e onward (research-run "running" checkpoints)
   reachable for the first time in production; on `main` today only 1a-1d
   are ever written (all "failed"/"started" rows).
2. `hypothesis_registry` INSERT/UPDATE (items 2, 3a, 3b): in-process
   fenced only, no re-check at the literal statement.
3. `validation_results` INSERT (item 4): completely unfenced on `main`
   today — no write-guard hook exists in `validation/backtest.py` at all.
4. `model_registry` CANDIDATE INSERT (items 5a, 5b): checked before the
   call, not atomically with it — a TOCTOU gap exists on `main`.
5. The PASS email (item 6) is the least-guarded real-world side effect:
   unconditional on `main`, and on #570 only indirectly suppressed via
   the preceding model-creation check's outcome, not its own lease check.
6. The `research_run` status trail (item 1) is deliberately never fenced
   on either branch — it is the mechanism that reports fencing, not
   something fencing protects.
7. #570 is a cross-process upgrade of the SAME in-process mechanism
   already on `main` (`_AutoresearchGenerationTracker`) — it closes the
   "second process / restarted worker" gap, nothing else.
8. #570 does NOT decide whether research is allowed to start, whether a
   hypothesis may PASS, or whether a PASS email may send — a live,
   still-owning worker performs every write and every notification above
   exactly as on `main` today.
9. `AUTORESEARCH_ENABLED` (`config.py:502`) is declared but read nowhere
   — confirmed by exhaustive grep — so today NOTHING stops a healthy
   Hermes cycle from invoking autoresearch once #579 lands, cooldown
   gates aside.
10. Existing gates before any of the above is reached: `run_cycle`'s
    cycle-modulo/health check (`scripts/hermes_operator.py:2134`) and
    `maybe_run_autoresearch`'s 12h/dry_run check
    (`scripts/hermes_fixers.py:1703-1710`) — both untouched by #579 and
    by #570.
