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

## Trial ledger — dedicated, not canonical (owner decision 2026-11-07)

Scored hypothesis batches go to a **dedicated** S11 ledger, `grid-hypothesis-trials`.
The canonical research ledger `grid-hypothesis-loop` has never had its genesis
(no file on grid-svr or gridz4); its genesis waits until the research lane
designs its catalog. Nothing in the scorer assumes `CANONICAL_LEDGER_ID`:
`open_trial_ledger` opens whatever ledger the env names, and the id comes
from that file's genesis.

| | |
|---|---|
| ledger | `/data/grid_ledgers/hypotheses/ledger/grid-hypothesis-trials.jsonl` |
| anchor | `/data/grid_ledgers/hypotheses/anchor/grid-hypothesis-trials.anchors.jsonl` |
| owner / modes | dirs `grid:grid 0750` (the ledger dir must be writable: the scorer creates `grid-hypothesis-trials.jsonl.lock` beside the ledger); files `grid:grid 0640` |
| q | 0.10 (frozen at genesis; the BH level of every batch) |
| catalog frozen at genesis | `families=("HYPOTHESIS_BATCHES\|change\|fwd1",)`, `features=("none\|x",)`, `classes=(("none\|x", "hypothesis_batches_only"),)`, `self_lag=()` — a placeholder that satisfies `Catalog.validate()`. Hypothesis batches use only `q`; no S11 allocation should ever run on this ledger. |

Run, in order (as Anik; the genesis refuses an existing anchor or ledger file, so a
second run fails instead of resetting anything):

```bash
sudo install -d -m 0750 -o grid -g grid /data/grid_ledgers/hypotheses/ledger /data/grid_ledgers/hypotheses/anchor
```

```bash
sudo -u grid bash -c 'cd /data/grid_v4/grid_release && umask 0027 && PYTHONPATH=. /data/grid_v4/grid_venv-314/bin/python -c "
from analysis.ledger_steered_exploration import Catalog, Ledger
cat = Catalog(families=(\"HYPOTHESIS_BATCHES|change|fwd1\",), features=(\"none|x\",), classes=((\"none|x\", \"hypothesis_batches_only\"),))
led = Ledger.create(\"/data/grid_ledgers/hypotheses/ledger/grid-hypothesis-trials.jsonl\", anchor=\"/data/grid_ledgers/hypotheses/anchor/grid-hypothesis-trials.anchors.jsonl\", catalog=cat, ledger_id=\"grid-hypothesis-trials\", q=0.10)
print(led.genesis[\"ledger_id\"], led.head)
"'
```

Verify (read-only; re-walks the hash chain and the anchor):

```bash
sudo -u grid bash -c 'cd /data/grid_v4/grid_release && PYTHONPATH=. /data/grid_v4/grid_venv-314/bin/python -c "
import json
from analysis.ledger_steered_exploration import Ledger, summarize_hypothesis_batches
led = Ledger(\"/data/grid_ledgers/hypotheses/ledger/grid-hypothesis-trials.jsonl\", anchor=\"/data/grid_ledgers/hypotheses/anchor/grid-hypothesis-trials.anchors.jsonl\")
led.verify(full=True)
g = led.genesis
print(json.dumps({\"ledger_id\": g[\"ledger_id\"], \"q\": g[\"q\"], \"catalog_sha256\": g[\"catalog_sha256\"], \"records\": len(led.records), **summarize_hypothesis_batches(led)}, indent=1))
"'
```

Expected: `ledger_id` `grid-hypothesis-trials`, `q` 0.1, `catalog_sha256`
`4e097c95a2951bcd54a74affb6ed6a894d786505d81c2f9862b63f37c304a9ec`, 1 record,
0 batches.

Then point only grid-hermes at it (drop-in, not the shared `.env`) and restart:

```bash
printf '[Service]\nEnvironment=HYPOTHESIS_TRIAL_LEDGER_PATH=/data/grid_ledgers/hypotheses/ledger/grid-hypothesis-trials.jsonl\nEnvironment=HYPOTHESIS_TRIAL_LEDGER_ANCHOR_PATH=/data/grid_ledgers/hypotheses/anchor/grid-hypothesis-trials.anchors.jsonl\n' | sudo tee /etc/systemd/system/grid-hermes.service.d/zz-hypothesis-ledger.conf
```

```bash
sudo systemctl daemon-reload && sudo systemctl restart grid-hermes
```

Optional hardening after genesis: `sudo chattr +a` on the two `.jsonl` files
(appends still work; truncate/rewrite refused). Not on the directories — the
lock file must be creatable. Keep an off-host copy of the anchor.

While `ACTIVE_HYPOTHESIS_SCORING_HELD` is True the configured ledger is never
written; without the env the scorer writes nothing (`TrialLedgerNotConfigured`).

### Two ledgers, one trial count (open requirement)

`declared_trial_count` (the DSR trial count the S11 overfit gate deflates by)
counts only the ledger it is given. With hypothesis batches in the dedicated
ledger, the canonical research ledger's gate would under-count. **Once
`grid-hypothesis-loop` exists, the overfit gate (`evaluate_overfit_gate` /
`require_overfit_gate`) must add this ledger's hypothesis trials**
(`summarize_hypothesis_batches(...)["hypothesis_trials"]`, read from the
anchored dedicated ledger) to its `n_trials_declared`, and its staleness check
must also fail when this ledger has grown since the gate ran. Until then no
research promotion is possible anyway (no canonical ledger, no gate record).

## Lifting the hold (owner)

1. Create the dedicated ledger and point grid-hermes at it (above).
2. Flip `ACTIVE_HYPOTHESIS_SCORING_HELD = False` (separate reviewed commit;
   branch `claude/hypothesis-lift-hold-20261106`).

Dry run (read-only, any time): `python -m intelligence.hypothesis_trials dry-run
--trials-out trials.jsonl` — forces `default_transaction_read_only = on` on every
pooled connection and checks it, records into an in-memory ledger only.

## Legacy rows — additive relabel

Nothing rewrites history: v1 verdicts are told apart by a reader rule, and
registry verdicts by an append-only side table.

**`discovered_hypotheses` v1 verdicts** (133,089 confirmed / 71,476 invalidated
on 2026-11-06; 114,826 of the confirmed lead-lag rows involve `snap:*`, 97,717
mention `llm_task`). Every v1 terminal row has `scoring_version IS NULL`.
`get_stats` counts only `scoring_version IS NOT NULL` as confirmed/invalidated
and reports v1 rows under `legacy_v1_unsound`; display readers (canvas,
surfacer, valuation) show them as `legacy_v1_unsound`; `oracle/model_evolver`
seeds from active or trial-confirmed rows only.

**`hypothesis_registry`** (separate producer chain):

- `PASSED` 10,609 rows = **35 distinct statements**, all TACTICAL, all
  `backtest_scanner` ("When X moves >1%, Y follows within 1 day (Sharpe 9.3…)").
  In-sample 504-day Sharpe/win-rate pass, no holdout, no multiple-testing
  correction, re-inserted up to 10 per run without dedup.
- `TESTING` 30,821 rows = 185 distinct statements: 30,782 are PASSED rows the
  LLM reviewer flagged ("Unrealistic assumptions (Sharpe 9.6…)", "Duplicate",
  "Self-correlation disguised as a signal"), 39 are tester inconclusives or
  AutoAgent rows with no verdict. `hypothesis_tester` only reads `CANDIDATE`,
  so nothing ever revisits TESTING.
- `FAILED` 85 is shrunk: the weekly prune deleted FAILED rows > 180 days until
  #1005.

`hypothesis_registry_labels` (migration `hyp_registry_labels_20261107`,
append-only, UPDATE/DELETE/TRUNCATE refused) holds one label per row and
`label_version`: `LEGACY_UNVALIDATED` (PASSED), `LEGACY_LLM_FLAGGED` and
`LEGACY_INCONCLUSIVE` (TESTING). `scripts/label_legacy_hypotheses.py` writes
them (dry run by default; `--apply --decided-by ...`; idempotent).

Every PASSED consumer reads a row as validated only when it is PASSED, created
at or after `governance.hypothesis_labels.LEGACY_CUTOFF` (2026-11-07) and
unlabelled — so legacy rows are excluded even before the labels are written:
`trading/paper_engine.register_all_passed`, `validation/gates.py`
CANDIDATE→SHADOW, `api/routers/discovery.py` promote,
`scripts/train_regime_model.py` (unreachable stub), `scripts/notify.py`.

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
