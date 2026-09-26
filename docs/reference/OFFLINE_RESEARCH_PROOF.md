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

## S09: fixes from the #658 review, PIT origin, real-panel adapter (2026-09-26)

Run the tests with `python -m pytest tests/test_offline_research_stats_core.py tests/test_research_real_panel.py tests/test_run_real_panel_scan.py -q`.

- **Fixed-step runs are diagnostic only.** At its default block (overlap
  depth + 1), `fixed_step_block_null` is anti-conservative. The #658 reviewer
  measured 8.5% at n=60 and 10.75% at n=240 for a nominal 5%. It still reports
  every p-value and BH-adjusted p, but it never sets `selected`, so it produces
  no holdout checks and no candidates. The manifest records
  `candidate_eligible: false` and the caveat under `caveats`, and the `method`
  string ends with `CAVEAT: ...`. A re-signed fixed-step manifest that carries
  a selection is refused. Candidates come only from `horizon_spaced` runs.
- **Horizon-spaced block-1 calibration test.** An independent AR(1)
  (phi 0.95) feature against serially independent outcomes gives 4.0% at
  n=60 and 3.9% at n=240, with 800 simulations and 499 permutations. The
  exact size of `p < 0.05` at that resolution is 4.8%. The reviewer measured
  4.25%.
- **`change` labels.** `build_family_rows(..., label="change")` labels
  `end - start` for rates, spreads and indexes that can be 0 or negative. The
  label is part of the horizon identity, so a family cannot mix label kinds.
- **Distinct PIT origin `pit_vintage_read`.** `exploratory_replay` is a
  self-declared label, so this origin is gated differently. A `Protocol` with
  this origin must carry `pit_receipt`, the sha256 of a `PitPanel` receipt,
  and no other origin may carry one. `analysis/research_real_panel.py` is the
  only constructor of a `PitPanel` (`load_pit_panel`, guarded by a capability
  token). It reads each declared series once through
  `store.observations.read_window` with `as_of` and `as_of_ts`: `SUCCESS`
  rows only, the latest vintage per date, nothing pulled after `as_of_ts`.
  `discover` and `evaluate_holdout` re-derive every row from the verified
  panel and refuse:
  - the label without a panel;
  - a look-alike object;
  - a mismatched receipt;
  - tampered rows;
  - a panel changed after its read;
  - a window past `as_of`;
  - a panel passed with any other origin.
- **Adapter availability rules.** A feature observation dated `d` becomes
  usable at the first business-day session on or after `d + lag_days`, where
  `lag_days` is a conservative per-series publication lag: 1 for daily
  H.15/ICE/VIX, 8 for H.10 FX, 2 for H.4.1. It is then carried forward at
  most `stale_sessions` sessions. A target's label becomes known at the first
  session on or after `label_end + lag_days`, and a label not known inside
  its window is purged.
- **Adapter refusals.** The adapter refuses `snap:*`, LLM/telemetry counters,
  astro/celestial ids, yfinance ids (`YF:`/`YF_ADJ:`, see S07/#642) and
  `revised=True` series. It builds no SQL of its own and never names
  `discovered_hypotheses` or `hypothesis_registry`.
- **`scripts/run_real_panel_scan.py`.** The scan engine is read-only: a
  NullPool engine with `default_transaction_read_only=on`,
  `statement_timeout` of at most 60 s and autocommit. It writes
  `summary.json`, `trial-ledger.csv`, `frozen-candidates.json` and the
  contract receipts to a new directory.

### First real-panel ledger (grid-svr, read-only, commit `ef0d564b`)

Artifact: `wha/outputs/hypothesis-loop-20260926/` (operator outputs folder).

**Setup:**

- **Universe:** 30 FRED/AAII series. Each is single-source with 0
  multi-valued dates in a bounded pre-run probe, and none is materially
  revised. With chg5/chg20/z60 transforms that gives 90 features.
- **Targets:** forward *changes* over 1/5/20 sessions in VIXCLS, DGS2, T10Y2Y
  and BAMLH0A0HYM2, for 12 families. There are no price targets.
- **Windows:** discovery 2004-01-02 to 2018-01-02; holdout 2018-01-02 to
  2026-09-26 (`as_of` 2026-09-25).
- **Protocol:** 20,000 permutations and BH q=0.10.

**Results:**

- **Trials:** 1,080, all of them testable.
- **BH rejections:** 89 (critical p = 0.0081; the first-rank cut is 9.3e-5).
- **Holdout:** 8 retrospective survivors, which are frozen as
  `FORWARD_EVIDENCE_PENDING` with `promotion_allowed: false`.

This is not the near-zero of the ETF-return replay. The targets are
non-traded levels with known serial structure: the HY OAS 5-session change has
acf1 = +0.34 and the VIX 5-session change has acf1 = -0.18. The selections are
a few known effects counted many times:

- credit-spread momentum;
- VIX mean reversion and vol-state spillover;
- short-rate policy drift.

A post-hoc re-read of the identical panel (receipt hash matched; not a scan,
nothing frozen) keeps 102, 102 and 108 selections with blocks 2, 4 and 8, so
the result is not an artifact of the block-1 null. The machinery detects real
dependence. Tradable targets still need the price-basis split, and those scans
should exclude each target's own-family proxies.

**Known limits.**

- **Revisions are hindsight.** Every row of this universe was pulled on or
  after 2026-03-24 (backfill). `as_of_ts` makes the read reproducible, but it
  does not give first-release values.
- **Sessions are business days**, not an exchange calendar.
- **Publication lags are declared constants**, not per-release timestamps.

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
  to 5%. Since S09, fixed-step runs are diagnostic only for this reason.
- A durable cross-run trial budget and single-use holdout registry; authenticated
  code/protocol/input hashes; no repeated tuning against the same holdout.
- Prospective prediction logging before outcomes, frozen sample-size and stopping
  rules, forward scoring and an independently reviewed promotion policy.

Those requirements are intentionally unresolved here. Synthetic mechanism
success and a zero-survivor replay do not authorize live research, learning,
weights or historical rescoring.
