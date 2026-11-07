# Hypothesis scoring v2 — one-look trials, BH per batch, S11 ledger

Code: `intelligence/hypothesis_trials.py`, `analysis/ledger_steered_exploration.py`
(`record_hypothesis_batch`). Hold: `ACTIVE_HYPOTHESIS_SCORING_HELD` in
`intelligence/hypothesis_engine.py` (still `True`; lifting it is the owner's call).

## What a verdict means now

A `discovered_hypotheses` row leaves `active` only as a declared trial in a
`hypothesis_batch` record of the S11 ledger:

| pattern | test | can confirm? |
|---|---|---|
| convergence (`ticker` + `expected_direction`) | signed log excess return over SPY, first completed close after `created_at` → last close inside `window_days`; one-sided p vs the ticker's pre-creation same-horizon excess returns (≥250) | yes, if BH q ≤ ledger q **and** raw move ≥ `min_move_pct` in the declared direction |
| lead_lag (`watch_signal` → `expect_signal`) | declared lag and sign only, the 90 calendar days after creation only (3-day settle), block-permutation null on the target | yes, if BH q ≤ ledger q; **ineligible** if either side is GRID output |
| volume_anomaly (`watch_category`) | outcome is `analytical_snapshots` row counts = GRID telemetry | never (ineligible, `SELF_TELEMETRY`) |
| actor_shift (`watch_actor`) | outcome is "the actor shows up again" — no market outcome | never (untestable, `NO_MARKET_OUTCOME`) |

GRID output = `snap:*` (every `analytical_snapshots` category), anything with
`llm`, `telemetry`, `pipeline`, `astro`, `oracle`, `hypothes`, `prediction`,
`conviction`, `sleuth`, `agent`, `hermes`, `heartbeat`, `self_`, `grid_` in the key
(`is_self_generated`).

Rules:

- **One look.** A row is declared once, when its window has closed. A row whose
  data has not arrived is *pending* (not declared) until twice its window, then
  declared `untestable` / `NO_DATA`. The ledger refuses a second declaration.
- **FDR.** The ledger computes Benjamini-Hochberg q-values over every declared
  trial of the batch (untestable/ineligible at p = 1 count in the denominator)
  at its frozen `q` (≤ 0.10). Batches draw nothing from S11's alpha-spending
  schedule; their trials do count in `declared_trial_count`, so the S11 overfit
  gate's DSR deflates by them and a gate passed before a batch is stale after it.
- **Ledger before DB.** The batch record is appended (under a file lock) before
  any row changes; a crash between the two is replayed on the next run
  (`_replay_unapplied`, idempotent: only `active` rows change).
- **Single-row callers defer.** `HypothesisGenerator.score_hypothesis` (goal_worker)
  never writes a verdict; Hermes' 30-minute `score_due_active_hypotheses` and
  `score_all` run one batch.
- New columns (added on first live write, ALTER only when missing):
  `scoring_version`, `trial_p_value`, `trial_q_value`, `trial_ledger_sha256`.
  NULL `scoring_version` = a v1 verdict.

## Lifting the hold (owner)

1. Create the S11 ledger file + anchor (anchor on a separate append-only
   location) — a dedicated ledger, or the canonical research ledger if the
   research lane creates one, so one DSR count covers both.
2. Set `HYPOTHESIS_TRIAL_LEDGER_PATH` / `HYPOTHESIS_TRIAL_LEDGER_ANCHOR_PATH`
   in the service env of grid-hermes (and anything else that runs the batch).
3. Flip `ACTIVE_HYPOTHESIS_SCORING_HELD = False` (separate reviewed commit).

Dry run (read-only, any time): `python -m intelligence.hypothesis_trials dry-run
--trials-out trials.jsonl` — forces `default_transaction_read_only = on` on every
pooled connection and checks it, records into an in-memory ledger only.

## Legacy rows — proposed additive relabel (not executed)

Nothing below rewrites history; every proposal is a reader filter or an
append-only side table.

**`discovered_hypotheses` v1 verdicts** (133,089 confirmed / 71,476 invalidated
on 2026-11-06; 114,826 of the confirmed lead-lag rows involve `snap:*`, 97,717
mention `llm_task`). Every v1 terminal row has `scoring_version IS NULL`. Readers
that surface "confirmed" hypotheses should count only
`scoring_version IS NOT NULL` as trial-confirmed and show v1 rows as
`legacy_v1_unsound`. No UPDATE needed.

**`hypothesis_registry`** (separate producer chain; see the PR for the code trace):

- `PASSED` 10,609 rows = **35 distinct statements**, all TACTICAL, all
  `backtest_scanner` ("When X moves >1%, Y follows within 1 day (Sharpe 9.3…)").
  In-sample 504-day Sharpe/win-rate pass, no holdout, no multiple-testing
  correction, re-inserted up to 10 per run without dedup.
- `TESTING` 30,821 rows = 185 distinct statements; 30,814 are PASSED rows the
  LLM reviewer flagged ("Unrealistic assumptions (Sharpe 9.6…)", "Duplicate",
  "Self-correlation disguised as a signal"); `hypothesis_tester` only reads
  `CANDIDATE`, so nothing ever revisits TESTING. 7 are tester-inconclusive.
- `FAILED` 85 is shrunk: the weekly prune deleted FAILED rows > 180 days until
  #1005.

Proposal: an append-only `hypothesis_registry_labels` table
(`registry_id, prior_state, label, reason, decided_by, created_at`) with
`LEGACY_UNVALIDATED` for every PASSED row and `LEGACY_LLM_FLAGGED` /
`LEGACY_INCONCLUSIVE` for TESTING, and the PASSED consumers
(`trading/paper_engine.register_all_passed`, `validation/gates.py` CANDIDATE→SHADOW,
`api/routers/discovery.py` promote, `scripts/train_regime_model.py`,
`scripts/notify.py`) reading through it. The 35 + 185 distinct statements can
be re-declared as fresh v2-style trials with a forward holdout from the date of
re-declaration.

## Production dry run (2026-11-07 00:5xZ, read-only, release f5b0510e + this branch)

`python -m intelligence.hypothesis_trials dry-run` on griddb, every connection
`default_transaction_read_only = on`, in-memory ledger:

- 4,954 active rows scanned (all `times_tested = 0`: v1 never looked at them).
- 4,750 declared trials, 204 pending (external lead-lag rows created
  2026-09-19; their one look is due 2026-12-21).
- 4,060 ineligible `SELF_TELEMETRY` (3,590 `snap:*` lead-lag, 470 category
  watches), 16 `NO_MARKET_OUTCOME`, 20 `NO_DATA`, 4 `NO_DIRECTION`,
  4 `INSUFFICIENT_HISTORY`.
- 646 tested (323 convergence theses, all "down", created 2026-09-13..19, and
  their 323 antitheses) over 150 tickers. Min p 0.0079; 31 raw p < 0.05
  (25 theses, 6 antitheses: about chance for 646 one-sided tests).
- **Confirmed: 0** at BH q = 0.10 over the batch (min q = 1.0), and 0 with BH
  over the 646 tested trials alone (min q = 0.91).
