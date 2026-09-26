# Offline research-loop contract proof

Run `python -m scripts.demo_offline_research_proof NEW_OUTPUT_DIRECTORY`.
Run `python -m pytest tests/test_offline_research_proof.py tests/test_offline_research_stats_core.py tests/test_offline_research_replay_v2.py -q`.
Replay the vault #101 panel: `python -m scripts.replay_vein_scan_v2 tests/fixtures/offline_research/grid-vein-scan-signals-20260923.csv`.

This is an executable **synthetic fixture** plus an **exploratory CSV replay**.
It is not a finding, a market backtest, a registered forward test, an enabled
Hermes loop, or permission to promote weights. It makes no DB or provider call.
Existing live engine behavior is unchanged.

The deterministic fixture declares four trials: planted independent signal,
independent noise, constant input, and excluded internal telemetry. Every trial
appears in the discovery ledger, including refusal and insufficient-data rows.
GRID's existing `analysis.hypothesis_tester.compute_lagged_correlation` is reused
only as its pure numeric lag-zero primitive. Its database fetch and
state-changing orchestrator are never called. Lag search is not hidden inside
this proof.

Discovery accepts only discovery rows. It rejects feature known-at after
decision, labels or outcome availability crossing the holdout boundary,
overlapping outcomes (unless the null accounts for them, see (b)), mixed
horizons, missing columns, nonfinite values and unlabeled or real origins. A
feature value of `None` is an explicit abstention. A NaN or inf is a silent
defect and is refused.

The discovery manifest is hashed and written before holdout evaluation. Holdout
tests only frozen discovery survivors, with a Bonferroni correction over that
frozen family and the same effect sign. Its horizon must match discovery. The
local candidate spec records family, direction, horizon, manifest and earliest
forward start. Its hash and status remain `FORWARD_EVIDENCE_PENDING`, never
PASSED/confirmed/promoted. All results report zero forward evidence, and no
promotion API exists. The output directory must be new, so a consumed local
holdout receipt cannot be overwritten by accident. This is not a global
anti-rerun service: an operator can deliberately create another directory.

## Statistical core (S08, 2026-09-26)

These four pieces come from Claude's corrected vault #101 vein-scan prototype
v2 (obsidian-vault `532883a68`, `grid-vein-scan-prototype.py`). They are
integrated into `analysis/offline_research_proof.py` itself, not added as a
parallel module.

- **(a) Split first, then label.** `build_family_rows()` blanks every price
  outside the evaluation window before it computes any forward label.
  Discovery sees `[start, split)` and holdout sees `[split, end)`. A discovery
  label that would need a price on or after the holdout start is never
  produced, which is the purge. `validate_rows()` still refuses a crossing
  label that arrives by any other route. The tests show the discovery rows and
  manifest are byte-identical when every holdout price is replaced with noise.
  Labels built on the full series first (the v1 defect) do read those prices.
- **(b) Horizon-spaced sampling.** With `sampling="horizon_spaced"` (the
  default), decisions are drawn every `max(step, horizon)` sessions and any
  overlapping outcome window is refused. `fixed_step_block_null` keeps the
  fixed step. It allows overlap only when the permutation block is at least
  the measured overlap depth + 1. With v2's weekly step this gives blocks of
  1/1/2/4 for 1/5/10/20-day horizons, and a shorter declared block is refused.
- **(c) Block-permutation null.** The p-value is two-sided against permutations
  of contiguous target blocks. The feature is never permuted, so its
  autocorrelation is kept. Permutations are exact, and a short final block
  moves as one unit. They are seeded only by `(seed, n, block, perms)`, so the
  result does not depend on trial order. Resolution is `1/(perms+1)` and is
  recorded as `min_attainable_p`. The IID `pearsonr` p-value is gone.
  `statistic` is `pearson` (the default) or `spearman`, computed as Pearson on
  ranks through the same primitive.
- **(d) BH-FDR over the whole run.** A run declares `families` (one per
  target/horizon) × `features`. Every declared trial is in one BH family,
  including excluded, insufficient, constant, NaN and empty-family trials,
  each at p = 1.0. `selected` means `adjusted_p <= fdr_q` (default 0.10).

### Reproduction on the v2 panel

Input: the v2 39-series CSV, committed as
`tests/fixtures/offline_research/grid-vein-scan-signals-20260923.csv`
(sha256 `b8014421…a07a`, LF). It was run with origin `exploratory_replay`,
Spearman, 10,000 permutations and seed 20260924. v2's panel loading and
feature engineering are copied into `scripts/replay_vein_scan_v2.py`, so the
trial universe matches. Labels, sampling, the null and BH come from the library.

| | v2 (vault run log) | this contract |
|---|---|---|
| trials attempted | 2,640 | 2,640 (110 features × 24 families) |
| horizon-spaced: testable | 1,140 (fwd10/fwd20 all untestable) | 1,140 (fwd10/fwd20 660/660 untestable) |
| horizon-spaced: BH-10% / 5% survivors | 0 / 0 | 0 / 0 |
| weekly + block null: testable | 2,280 | 2,280 (blocks 1, 2, 4) |
| weekly + block null: BH-10% / 5% survivors | 0 / 0 | 0 / 0 |
| discovery / holdout start | 2025-03-03 / 2026-02-09 | 2025-03-03 / 2026-02-09 |

Checking per trial against v2's discovery ledger (`--v2-ledger`), every one of
the 2,640 trials matches `n` and testable status in both modes. The largest
|rho| gap is 5.0e-5, which is the 4-decimal rounding done by
`compute_lagged_correlation`. The counts are identical, and three things
differ by design:

1. **BH denominator.** v2 applied BH over testable trials only (1,140 / 2,280).
   This contract uses all 2,640, which can only reject fewer. The replay also
   reports the testable-only count as a diagnostic, and it is 0 in both modes.
2. **Horizon-spaced p-values.** v2 used analytic Spearman p there. This
   contract uses the permutation null with block 1. Raw p < 0.05 is 59 here
   against v2's 58, and the minimum p is 0.0018.
3. **Block-null draws.** The RNG scheme differs: v2 used a cache keyed by
   order, and this contract seeds per `(seed, n, block, perms)`. Raw p < 0.05
   is 126 here against v2's 125, and the minimum p is 0.0007.

Holdout: nothing was selected, so there are 0 checks and 0 candidates. That
matches v2. v2's ledger also lists 2,640 holdout rows per method. This contract
measures only frozen selections in the holdout, by design. The test
`tests/test_offline_research_replay_v2.py` asserts all of the above and takes
about 35 s locally.

## Lineage and boundaries

Claude's corrected vault #101 retracts its original leaking 12/3 survivors and
reports zero survivors on exploratory historical data. This contract reproduces
that zero. It does not turn those retrospective rows into prospective evidence:
replay results carry state `EXPLORATORY_REPLAY_ONLY`. The replay CSV is a
latest-vintage hindsight pull with no per-source known-at contract. Its
known-at is taken as the observation date, which is not a PIT guarantee.

#563's batched PIT changes can alter default target column order and remain a
separate dependency. #568 carries #562's evaluator migration and skipped PG
acceptance dependencies; neither is transplanted. This additive contract has no
migration, route, timer, registry persistence or production importer. It never
reads `hypothesis_registry`, `discovered_hypotheses` or `scanner_weights`.

## Missing before real-data / forward integration

- Verified per-source observed, publication, known-at and revision timestamps;
  an audited point-in-time selector (not latest-vintage historical hindsight).
- Explicit raw/adjusted price basis, source identity and exchange-session/horizon
  contracts, including duplicated-source handling and abstention on missing data.
- Costs, a target-return definition and a sample-size/power policy. With
  10,000 permutations and 2,640 trials, one trial alone can never pass BH at
  10% (2,640/10,001 > 0.10), and at least 3 trials must reach the minimum p.
  More permutations or a smaller declared universe are protocol decisions.
- The block null preserves within-block target dependence and the feature's
  autocorrelation. It is not an exact test for every dependence structure: in
  the calibration test it cuts a 29% IID false-positive rate to about 9%, not
  to 5%.
- A durable cross-run trial budget and single-use holdout registry; authenticated
  code/protocol/input hashes; no repeated tuning against the same holdout.
- Prospective prediction logging before outcomes, frozen sample-size and stopping
  rules, forward scoring and an independently reviewed promotion policy.

Those requirements are intentionally unresolved here. Synthetic mechanism
success and a zero-survivor replay do not authorize live research, learning,
weights or historical rescoring.
