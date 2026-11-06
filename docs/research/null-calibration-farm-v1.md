# Null-calibration farm v1: the measured size and power of GRID's tests (S7, 2026-11-06)

**What this measures.** How often GRID's statistical tests reject when nothing is
there (empirical size at nominal 5%, target 4–6%), and how often they detect a small
planted effect (power). Seeded synthetic worlds with a known answer are pushed through
the real code, unchanged:

| test | code path |
|---|---|
| `gauntlet_perm` | `alpha_research.validation.gauntlet.permutation_test` (#979 block null, long-short Sharpe), plus the one-trial DSR exactly as `run_gauntlet` computes it |
| `gauntlet_full` | `run_gauntlet` end to end (verdict, permutation p, DSR, CV, subsample, decay) |
| `dsr_best_of` | best of N trial Sharpe ratios → `deflated_sharpe_ratio` with the cross-trial variance (`trial_sharpe_variance`), as `run_gauntlet(trial_sharpes=...)` does |
| `s09_discover` | `analysis.offline_research_proof.build_family_rows` → `discover` (S09/S09b data-driven or declared block, block-permutation p, BH run selection or S11 ledger Holm). Used read-only; the module is hash-pinned and was not changed |

Worlds (`alpha_research/validation/null_farm.py::WORLDS`): `iid`; `persistent_feature`
(feature AR(1) φ=0.95, iid target); `ar1_target` (target AR(1) φ=0.35, or 0.7 / −0.35 /
0.2 where noted, against a persistent feature); `regime_switch` (sticky 2-state Markov
chain, regime-dependent per-asset drift and volatility; an independent regime-shifting
feature); `jump_diffusion` (Student-t(4) plus common and idiosyncratic jumps). Overlapping
horizons are a cell option: `horizon=5` makes the gauntlet's forward returns overlapping
5-day sums and S09 sample fixed-step or horizon-spaced. Signal worlds plant a
cross-sectional IC (gauntlet), a per-step correlation (S09) or a true annualized Sharpe
(DSR).

**Sweep.** 130 cells, 1,245 shards, about **86 CPU-hours** (x99-hex 50.5, i7gen10 22.8,
threadripper 12.4 inside `grid-worker.slice`, CPUQuota 800%, Nice=10). No DB reads, no DB
writes; results are files. Base seed 20261106. Gauntlet replicates are expensive (150
long-short passes each, ~55–100 s), so gauntlet cells have 100–300 replicates; DSR and S09
cells 1,000–4,000. Monte Carlo SE is shown with every rate.

**Exact nominal levels.** Permutation p-values live on a grid. The gauntlet cells use 149
permutations, so a perfectly calibrated `p < 0.05` rejects 7/150 = **4.67%**. The
default 999 permutations would give 4.9% at about 6.7x the cost; the null distribution
does not depend on the count. S09 cells use 1,999 permutations, where `p <= 0.05` is exactly
5.0%.

## Headline

1. **The gauntlet's permutation test is invalid on persistent signals at its default
   costs.** With `cost_bps=10` (the default, and what `scripts/run_gauntlet_vpd.py`
   passes) it rejects **71–83%** of null worlds whenever the signal is persistent. With a
   non-persistent signal it rejects 5.0%. The full gauntlet labels **26.7%** of
   persistent nulls MARGINAL-or-better and 3.3% ROBUST.
2. **Without costs it is close but still runs high on persistent signals.** It rejects
   7.7% at n=250 and 6.0% at n=500. The data-driven block (from the lag-1 autocorrelation
   of the long-short returns) stays at ~1 because those returns are white under the null,
   while the signal itself is persistent. A declared block of 20 brings it into the band
   (6.0% even with costs on).
3. **The one-trial DSR (PSR against 0) holds size on iid, regime-volatility and large-n
   jump returns.** It is anti-conservative on autocorrelated returns: **9.0–9.5%** for
   AR(1) φ=0.2, and 6.7–6.8% for overlapping 5-period returns even as the best of 10.
   It also runs a little high on small-sample jumps (6.3% at n=250).
4. **The multi-trial DSR is very conservative, so it is not a 5% test.** With 10 or 100
   trials it rejects 0.1–1.2%, and power collapses. With 10 trials, a true annual Sharpe of
   1.0 over 1,000 days is detected **2.2%** of the time. A Sharpe of 2.0 is detected 42.5%.
5. **S09 discovery (data-driven block) is in band** on iid, persistent-feature,
   jump-diffusion and regime worlds for n = 60–1000. The exception is autocorrelated
   targets at small n: 7.5% at n=60 for φ=0.35, and 10.5% / 6.7% at n=60 / 120 for
   φ=0.7. Declaring a fixed block is worse: block 1 rejects **15–17%** under φ=0.35 at
   every n, and block 5 rejects 6.6–8.4%, worsening with n.
6. **S11 ledger Holm family-wise error** at α=0.05 (3 trials, AR(1) target) is 8.6% at
   n=60 and 5.1–5.4% at n ≥ 120.

## 1. Gauntlet permutation test (n = 250 dates, 20 assets, top/bottom 5)

| world | costs | block rule | reps | rejection rate | MC SE | status |
|---|---|---|---|---|---|---|
| iid (iid signal) | 10 bps | data-driven (mean 1.1) | 300 | 0.050 | 0.013 | in band |
| persistent_feature | 10 bps | data-driven (1.2) | 100 | **0.800** | 0.040 | high |
| regime_switch | 10 bps | data-driven (1.5) | 100 | **0.710** | 0.045 | high |
| jump_diffusion | 10 bps | data-driven (1.2) | 100 | **0.830** | 0.038 | high |
| ar1_target (φ=0.35) | 10 bps | data-driven (11.5) | 300 | 0.067 | 0.014 | high (CI 4.4–10.1%) |
| persistent_feature | 10 bps | declared 20 | 300 | 0.060 | 0.014 | in band |
| persistent_feature | 0 | data-driven (1.1) | 300 | 0.077 | 0.015 | high |
| persistent_feature, n=500 | 0 | data-driven (1.1) | 300 | 0.060 | 0.014 | in band (edge) |
| ar1_target | 0 | data-driven (11.9) | 300 | 0.053 | 0.013 | in band |
| ar1_target | 0 | declared 5 | 300 | 0.057 | 0.013 | in band |
| ar1_target | 0 | declared 20 | 300 | 0.043 | 0.012 | in band |
| persistent, 5-day overlapping returns, overlap declared | 0 | data-driven (31) | 300 | 0.050 | 0.013 | in band |
| same, overlap not declared | 0 | data-driven (31) | 300 | 0.073 | 0.015 | high |

Full gauntlet (`run_gauntlet`, persistent_feature, 10 bps, 60 reps): permutation
rejection 0.783, **MARGINAL-or-better 0.267, ROBUST 0.033**, DSR passed 0.017.

**Why costs break it.** The null re-orders the signal's dates. A data-driven block of ~1
turns a slow signal into one that reshuffles its book every day, so every null draw pays
roughly 15 bps a day of turnover cost that the real, slow signal never pays. The real
signal then "beats" the null on cost savings alone. An iid signal pays the same turnover
in both, which is why the iid world holds size. The extra cost-free excess (7.7%) comes from the
shared realized returns. A persistent signal holds the same few assets for weeks and
inherits their realized sample means, so its statistic has a wider spread given the
returns than that of a one-day-block shuffle. A block that covers the signal's own
persistence fixes both effects.

**Power** (persistent signal, n=250, 100 reps): cross-sectional IC 0.02 → 0.29 (costs
off); IC 0.05 → 0.91 (costs off) and 1.00 (costs on, inflated by the size defect above).

## 2. Deflated Sharpe ratio

Size, rejection rate of `dsr > 0.95` for the best of N null trials (1,000–4,000 reps,
MC SE ≤ 0.005):

| world | n | N=1 | N=10 | N=100 |
|---|---|---|---|---|
| iid | 250 | 0.051 | 0.001 | 0.001 |
| iid | 1000 | 0.045 | 0.002 | 0.001 |
| jump_diffusion | 250 | **0.063** | 0.012 | 0.009 |
| jump_diffusion | 1000 | 0.054 | 0.010 | 0.005 |
| AR(1) φ=0.2 returns | 250 | **0.095** | 0.005 | 0.005 |
| AR(1) φ=0.2 returns | 1000 | **0.090** | 0.003 | 0.004 |
| volatility regimes | 250 / 1000 | 0.049 / 0.052 | 0.002 / 0.001 | 0.001 / 0.001 |
| iid, 5-period overlapping returns | 250 / 1000 | — | **0.068 / 0.067** | — |
| iid, trials correlated ρ=0.5 | 250 / 1000 | — | 0.015 / 0.018 | — |

Power, iid, n=1000 (2,000 reps): true annual Sharpe 0.5 → 0.256 / 0.001 / 0.001 for N =
1 / 10 / 100; Sharpe 1.0 → 0.636 / 0.022 / 0.010; Sharpe 2.0 → 0.991 / 0.425 / 0.338.

The multi-trial DSR compares the best trial with the *expected* maximum of N null
Sharpes, using one trial's standard error. The maximum of N normals is much tighter
than one normal, so the best of N nulls almost never clears it. That is the intended
"probability of skill" reading, not a test with 5% size. Positive autocorrelation
inflates both the one-trial PSR and the multi-trial DSR, because the Sharpe standard error
assumes independent returns.

## 3. S09 / S09b / S11 discovery (`build_family_rows` → `discover`, 1,999 perms)

Trial-0 rejection rate (`p <= 0.05`), 4,000 reps per null cell (MC SE ≈ 0.0035):

| world | block rule | n=60 | n=120 | n=240 | n=500 | n=1000 |
|---|---|---|---|---|---|---|
| iid | data-driven | 0.056 | 0.049 | 0.043 | 0.051 | |
| persistent_feature | data-driven | 0.048 | 0.052 | 0.055 | 0.052 | |
| persistent_feature | declared 1 / 5 | 0.050 / 0.054 | 0.047 / 0.048 | 0.051 / 0.053 | 0.055 / 0.048 | |
| jump_diffusion | data-driven | 0.052 | 0.054 | 0.048 | 0.052 | |
| regime_switch | data-driven | 0.057 | 0.058 | 0.059 | 0.063 | 0.054 |
| regime_switch | declared 1 / 5 (20 at n=1000) | 0.058 / 0.052 | 0.056 / 0.057 | 0.054 / 0.061 | 0.054 / 0.050 | 0.051 |
| ar1_target φ=0.35 | data-driven | **0.075** | 0.060 | 0.057 | 0.056 | 0.058 |
| ar1_target φ=0.35 | declared 1 | **0.153** | **0.153** | **0.157** | **0.166** | |
| ar1_target φ=0.35 | declared 5 | 0.066 | 0.067 | **0.076** | **0.084** | 0.071 |
| ar1_target φ=0.7 | data-driven | **0.105** | 0.067 | 0.060 | 0.052 | |
| ar1_target φ=−0.35 | data-driven | 0.034 | 0.042 | 0.048 | 0.039 | |
| persistent, h=5, horizon_spaced | data-driven | 0.052 | | 0.047 | | |
| persistent, h=5, fixed_step (diagnostic) | data-driven | 0.079 | | 0.053 | | |

The S09b block caveat fired in 100% of n=60 runs and in 49% of AR(0.35) runs at n=120,
so the anti-conservative small-n cells are already flagged in the manifest. Fixed-step
runs never selected (`any_selected` 0), as designed.

Family-level errors (3 independent null features per run): BH run selection at q=0.10
selects something in 7–13% of null runs wherever the per-trial size is in band. Under the global null, BH controls the
family-wise error at q, so this is as designed. S11 `ledger_holm` at α=0.05 (AR(0.35)
target): n=60 **0.086**, n=120 0.054, n=240 0.051, n=500 0.052.

Power (2,000 reps), trial-0 `p <= 0.05`, per-step correlation r:

| feature / target | n | r=0.1 | r=0.2 | r=0.3 |
|---|---|---|---|---|
| persistent / iid | 60 | 0.138 | 0.342 | 0.636 |
| persistent / iid | 240 | 0.365 | 0.872 | 1.000 |
| persistent / AR(0.35) | 60 | 0.121 | 0.225 | 0.415 |
| persistent / AR(0.35) | 240 | 0.228 | 0.604 | 0.930 |

## Recommended calibrations (nothing frozen was changed)

1. **Gauntlet: run the permutation null on gross returns.** Call
   `long_short_returns(..., cost_bps=0)` inside the null, or charge every null draw the
   real signal's own turnover cost. Keep costs in the net-Sharpe threshold and the DSR. Until
   then, treat any `permutation_p` from `run_gauntlet` with `cost_bps > 0` on a persistent
   signal as invalid, and any MARGINAL verdict as uninformative (26.7% of nulls earn one).
2. **Gauntlet block rule: size the block from the signal's persistence, not only from the
   long-short returns.** Use, for example, `max(current rule, ceil(signal rank half-life))`,
   with the rank half-life from the mean lag-1 rank autocorrelation of the signal across
   assets (φ=0.95 → half-life ≈ 14, block ≈ 20 holds size here). Keep the overlap floor
   and the `MIN_BLOCKS` cap. Always declare `overlap_depth = horizon − 1` for overlapping
   forward returns (7.3% without it, 5.0% with it).
3. **Gauntlet minimum n:** at least 250 dates, and 500 where the signal is persistent
   and the block rule above is not yet in place (6.0% at n=500 vs 7.7% at n=250 with costs
   off).
4. **DSR: compute it on non-overlapping returns, or replace `sqrt(T − 1)` with an
   effective sample size,** e.g. `T_eff = T (1 − ρ1) / (1 + ρ1)` or a HAC/Lo (2002)
   Sharpe standard error, whenever the return series is autocorrelated (φ=0.2 doubles the
   size to 9%). Read DSR > 0.95 with N ≥ 10 as a conservative screen, not a 5%
   test. Where a calibrated multi-trial decision is needed, use per-trial
   block-permutation p-values with Holm, as S11 does, or a max-statistic null.
5. **S09 candidate eligibility: require n ≥ 240 sampled decisions** whenever the
   discovery target's |acf1| is outside the 2/√n band (φ=0.35: 5.7% at 240; φ=0.7: 6.0%
   at 240 and 5.2% at 500). Equivalently, a family carrying the S09b block caveat should
   not produce candidates. That also fixes the ledger-Holm FWER at n=60 (8.6%).
6. **S09: never declare a fixed block shorter than the data-driven one for an
   autocorrelated target.** Block 1 gives 15–17%, and block 5 gets worse as n grows. The
   default `block=0` is the calibrated choice.
7. **Long-memory targets:** the lag-1 rule misses slow regime drifts (6.3% at n=500 with a
   data-driven block of ~1.3, against 5.0–5.4% with a declared block of 1–5). The excess is
   small, but a variance-ratio or several-lag criterion (or an n^(1/3) floor) would close
   it.

## Reproduce

```bash
python scripts/null_farm.py plan --grid first_sweep --out plan.json
# per box (weights = relative throughput; results land in files, resumable):
OMP_NUM_THREADS=1 nice -n 10 python scripts/null_farm.py run --plan plan.json \
    --out results/$(hostname) --jobs 24 --host-index 0 --host-count 3 --weights 50,35,15
python scripts/null_farm.py summarize --plan plan.json --results results/* \
    --json summary.json --markdown summary.md
# top up chosen cells without redoing finished shards:
python scripts/null_farm.py plan --grid first_sweep --gauntlet-reps 300 --only <cell ids> \
    --done results/* --out plan-topup.json
```

On threadripper the run went in `grid-worker.slice` (`sudo systemd-run --slice=grid-worker.slice
--uid=grid -p Nice=10 ...`), so GRID's 8-thread cap covers the sweep and the worker
together. Once this module is on the workers' release tree, a shard can also be queued as
a coordinator `SIMULATION` job with `params = {"kind": "null_calibration", "shard":
<shard from plan.json>}`. That path reads no DB, and the records come back in the job
output. CI coverage: `tests/test_null_farm.py` (seconds, seeded).

Per-cell results (all 130 cells, rates, MC SE, Wilson 95% CI, block lengths and secondary
rates): `docs/research/null-calibration-farm-v1-summary.json`. The five persistent-signal
gauntlet cells at 10 bps (and the full-gauntlet cell) stopped at 100 / 60 replicates
because their answer was already decisive. Their `reps_planned` of 300 in the JSON is the
top-up plan's figure.

## Appendix: every cell

| test | world | n | block rule | params | kind | rate | MC SE | 95% CI | exact nominal | status | block used (mean) | secondary |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| gauntlet_perm | iid | 250 | data-driven | perms=149 | size (300) | 0.050 | 0.013 | 0.031-0.081 | 0.047 | in_band | 1.1 | dsr_passed 0.000 |
| gauntlet_perm | persistent_feature | 250 | data-driven | perms=149 | size (100) | 0.800 | 0.040 | 0.711-0.867 | 0.047 | high_sig | 1.2 | dsr_passed 0.000 |
| gauntlet_perm | ar1_target | 250 | data-driven | perms=149 | size (300) | 0.067 | 0.014 | 0.044-0.101 | 0.047 | high | 11.5 | dsr_passed 0.013 |
| gauntlet_perm | regime_switch | 250 | data-driven | perms=149 | size (100) | 0.710 | 0.045 | 0.615-0.790 | 0.047 | high_sig | 1.5 | dsr_passed 0.000 |
| gauntlet_perm | jump_diffusion | 250 | data-driven | perms=149 | size (100) | 0.830 | 0.038 | 0.745-0.891 | 0.047 | high_sig | 1.2 | dsr_passed 0.000 |
| gauntlet_perm | persistent_feature | 250 | data-driven | cost_bps=0.0, perms=149 | size (300) | 0.077 | 0.015 | 0.052-0.112 | 0.047 | high | 1.1 | dsr_passed 0.073 |
| gauntlet_perm | ar1_target | 250 | data-driven | cost_bps=0.0, perms=149 | size (300) | 0.053 | 0.013 | 0.033-0.085 | 0.047 | in_band | 11.9 | dsr_passed 0.157 |
| gauntlet_perm | persistent_feature | 250 | declared 20 | perms=149 | size (300) | 0.060 | 0.014 | 0.038-0.093 | 0.047 | in_band | 20.0 | dsr_passed 0.000 |
| gauntlet_perm | ar1_target | 250 | declared 5 | cost_bps=0.0, perms=149 | size (300) | 0.057 | 0.013 | 0.036-0.089 | 0.047 | in_band | 5.0 | dsr_passed 0.110 |
| gauntlet_perm | ar1_target | 250 | declared 20 | cost_bps=0.0, perms=149 | size (300) | 0.043 | 0.012 | 0.025-0.073 | 0.047 | in_band | 20.0 | dsr_passed 0.107 |
| gauntlet_perm | persistent_feature | 250 | data-driven | cost_bps=0.0, horizon=5, perms=149 | size (300) | 0.050 | 0.013 | 0.031-0.081 | 0.047 | in_band | 31.0 | dsr_passed 0.220 |
| gauntlet_perm | persistent_feature | 250 | data-driven | cost_bps=0.0, declare_overlap=False, horizon=5, perms=149 | size (300) | 0.073 | 0.015 | 0.049-0.109 | 0.047 | high | 31.0 | dsr_passed 0.250 |
| gauntlet_full | persistent_feature | 250 | data-driven | perms=149 | size (60) | 0.783 | 0.053 | 0.664-0.869 | 0.047 | high_sig | 1.2 | dsr_passed 0.017; robust 0.033; marginal_or_better 0.267 |
| gauntlet_perm | persistent_feature | 250 | data-driven | cost_bps=0.0, effect=0.02, perms=149 | power (100) | 0.290 | 0.045 | 0.210-0.385 | 0.047 | - | 1.2 | dsr_passed 0.320 |
| gauntlet_perm | persistent_feature | 250 | data-driven | cost_bps=0.0, effect=0.05, perms=149 | power (100) | 0.910 | 0.029 | 0.838-0.952 | 0.047 | - | 1.2 | dsr_passed 0.960 |
| gauntlet_perm | persistent_feature | 250 | data-driven | effect=0.05, perms=149 | power (100) | 1.000 | 0.000 | 0.963-1.000 | 0.047 | - | 1.2 | dsr_passed 0.790 |
| gauntlet_perm | persistent_feature | 500 | data-driven | cost_bps=0.0, perms=149 | size (300) | 0.060 | 0.014 | 0.038-0.093 | 0.047 | in_band | 1.1 | dsr_passed 0.067 |
| dsr_best_of | iid | 250 | data-driven | n_trials=1 | size (4000) | 0.051 | 0.003 | 0.045-0.059 | 0.050 | in_band | - | best_is_planted 1.000 |
| dsr_best_of | iid | 250 | data-driven | n_trials=10 | size (4000) | 0.001 | 0.000 | 0.000-0.002 | 0.050 | low_sig | - | best_is_planted 0.090 |
| dsr_best_of | iid | 250 | data-driven | n_trials=100 | size (4000) | 0.001 | 0.000 | 0.000-0.002 | 0.050 | low_sig | - | best_is_planted 0.012 |
| dsr_best_of | iid | 1000 | data-driven | n_trials=1 | size (4000) | 0.045 | 0.003 | 0.039-0.052 | 0.050 | in_band | - | best_is_planted 1.000 |
| dsr_best_of | iid | 1000 | data-driven | n_trials=10 | size (4000) | 0.002 | 0.001 | 0.001-0.003 | 0.050 | low_sig | - | best_is_planted 0.100 |
| dsr_best_of | iid | 1000 | data-driven | n_trials=100 | size (4000) | 0.001 | 0.000 | 0.000-0.002 | 0.050 | low_sig | - | best_is_planted 0.009 |
| dsr_best_of | jump_diffusion | 250 | data-driven | n_trials=1 | size (4000) | 0.063 | 0.004 | 0.056-0.071 | 0.050 | high | - | best_is_planted 1.000 |
| dsr_best_of | jump_diffusion | 250 | data-driven | n_trials=10 | size (4000) | 0.012 | 0.002 | 0.009-0.016 | 0.050 | low_sig | - | best_is_planted 0.095 |
| dsr_best_of | jump_diffusion | 250 | data-driven | n_trials=100 | size (4000) | 0.009 | 0.001 | 0.007-0.012 | 0.050 | low_sig | - | best_is_planted 0.006 |
| dsr_best_of | jump_diffusion | 1000 | data-driven | n_trials=1 | size (4000) | 0.054 | 0.004 | 0.047-0.061 | 0.050 | in_band | - | best_is_planted 1.000 |
| dsr_best_of | jump_diffusion | 1000 | data-driven | n_trials=10 | size (4000) | 0.010 | 0.002 | 0.007-0.013 | 0.050 | low_sig | - | best_is_planted 0.105 |
| dsr_best_of | jump_diffusion | 1000 | data-driven | n_trials=100 | size (4000) | 0.005 | 0.001 | 0.003-0.008 | 0.050 | low_sig | - | best_is_planted 0.008 |
| dsr_best_of | ar1_target | 250 | data-driven | n_trials=1, target={"phi":0.2} | size (4000) | 0.095 | 0.005 | 0.087-0.105 | 0.050 | high_sig | - | best_is_planted 1.000 |
| dsr_best_of | ar1_target | 250 | data-driven | n_trials=10, target={"phi":0.2} | size (4000) | 0.005 | 0.001 | 0.003-0.008 | 0.050 | low_sig | - | best_is_planted 0.098 |
| dsr_best_of | ar1_target | 250 | data-driven | n_trials=100, target={"phi":0.2} | size (4000) | 0.005 | 0.001 | 0.003-0.008 | 0.050 | low_sig | - | best_is_planted 0.011 |
| dsr_best_of | ar1_target | 1000 | data-driven | n_trials=1, target={"phi":0.2} | size (4000) | 0.090 | 0.005 | 0.081-0.099 | 0.050 | high_sig | - | best_is_planted 1.000 |
| dsr_best_of | ar1_target | 1000 | data-driven | n_trials=10, target={"phi":0.2} | size (4000) | 0.003 | 0.001 | 0.002-0.005 | 0.050 | low_sig | - | best_is_planted 0.098 |
| dsr_best_of | ar1_target | 1000 | data-driven | n_trials=100, target={"phi":0.2} | size (4000) | 0.004 | 0.001 | 0.002-0.006 | 0.050 | low_sig | - | best_is_planted 0.012 |
| dsr_best_of | regime_switch | 250 | data-driven | n_trials=1, target={"mu_sd":0.0} | size (4000) | 0.049 | 0.003 | 0.042-0.056 | 0.050 | in_band | - | best_is_planted 1.000 |
| dsr_best_of | regime_switch | 250 | data-driven | n_trials=10, target={"mu_sd":0.0} | size (4000) | 0.002 | 0.001 | 0.001-0.003 | 0.050 | low_sig | - | best_is_planted 0.093 |
| dsr_best_of | regime_switch | 250 | data-driven | n_trials=100, target={"mu_sd":0.0} | size (4000) | 0.001 | 0.000 | 0.000-0.002 | 0.050 | low_sig | - | best_is_planted 0.011 |
| dsr_best_of | regime_switch | 1000 | data-driven | n_trials=1, target={"mu_sd":0.0} | size (4000) | 0.052 | 0.004 | 0.046-0.060 | 0.050 | in_band | - | best_is_planted 1.000 |
| dsr_best_of | regime_switch | 1000 | data-driven | n_trials=10, target={"mu_sd":0.0} | size (4000) | 0.001 | 0.000 | 0.000-0.002 | 0.050 | low_sig | - | best_is_planted 0.104 |
| dsr_best_of | regime_switch | 1000 | data-driven | n_trials=100, target={"mu_sd":0.0} | size (4000) | 0.001 | 0.001 | 0.001-0.003 | 0.050 | low_sig | - | best_is_planted 0.009 |
| dsr_best_of | iid | 250 | data-driven | horizon=5, n_trials=10 | size (4000) | 0.068 | 0.004 | 0.060-0.076 | 0.050 | high_sig | - | best_is_planted 0.103 |
| dsr_best_of | iid | 250 | data-driven | n_trials=10, trial_rho=0.5 | size (4000) | 0.015 | 0.002 | 0.012-0.019 | 0.050 | low_sig | - | best_is_planted 0.105 |
| dsr_best_of | iid | 1000 | data-driven | horizon=5, n_trials=10 | size (4000) | 0.067 | 0.004 | 0.060-0.075 | 0.050 | high | - | best_is_planted 0.103 |
| dsr_best_of | iid | 1000 | data-driven | n_trials=10, trial_rho=0.5 | size (4000) | 0.018 | 0.002 | 0.014-0.022 | 0.050 | low_sig | - | best_is_planted 0.101 |
| dsr_best_of | iid | 1000 | data-driven | effect_sr=0.5, n_trials=1 | power (2000) | 0.256 | 0.010 | 0.237-0.275 | 0.050 | - | - | best_is_planted 1.000 |
| dsr_best_of | iid | 1000 | data-driven | effect_sr=0.5, n_trials=10 | power (2000) | 0.001 | 0.001 | 0.000-0.004 | 0.050 | - | - | best_is_planted 0.340 |
| dsr_best_of | iid | 1000 | data-driven | effect_sr=0.5, n_trials=100 | power (2000) | 0.001 | 0.001 | 0.000-0.004 | 0.050 | - | - | best_is_planted 0.082 |
| dsr_best_of | iid | 1000 | data-driven | effect_sr=1.0, n_trials=1 | power (2000) | 0.636 | 0.011 | 0.615-0.657 | 0.050 | - | - | best_is_planted 1.000 |
| dsr_best_of | iid | 1000 | data-driven | effect_sr=1.0, n_trials=10 | power (2000) | 0.022 | 0.003 | 0.017-0.030 | 0.050 | - | - | best_is_planted 0.668 |
| dsr_best_of | iid | 1000 | data-driven | effect_sr=1.0, n_trials=100 | power (2000) | 0.010 | 0.002 | 0.006-0.015 | 0.050 | - | - | best_is_planted 0.304 |
| dsr_best_of | iid | 1000 | data-driven | effect_sr=2.0, n_trials=1 | power (2000) | 0.991 | 0.002 | 0.985-0.994 | 0.050 | - | - | best_is_planted 1.000 |
| dsr_best_of | iid | 1000 | data-driven | effect_sr=2.0, n_trials=10 | power (2000) | 0.425 | 0.011 | 0.404-0.447 | 0.050 | - | - | best_is_planted 0.987 |
| dsr_best_of | iid | 1000 | data-driven | effect_sr=2.0, n_trials=100 | power (2000) | 0.338 | 0.011 | 0.317-0.359 | 0.050 | - | - | best_is_planted 0.914 |
| s09_discover | iid | 60 | data-driven | - | size (4000) | 0.056 | 0.004 | 0.049-0.064 | 0.050 | in_band | 1.2 | any_selected 0.102; x0_selected 0.041; block_caveat 1.000 |
| s09_discover | iid | 120 | data-driven | - | size (4000) | 0.049 | 0.003 | 0.043-0.056 | 0.050 | in_band | 1.3 | any_selected 0.095; x0_selected 0.036; block_caveat 0.000 |
| s09_discover | iid | 240 | data-driven | - | size (4000) | 0.043 | 0.003 | 0.037-0.050 | 0.050 | in_band | 1.2 | any_selected 0.102; x0_selected 0.033; block_caveat 0.000 |
| s09_discover | iid | 500 | data-driven | - | size (4000) | 0.051 | 0.003 | 0.045-0.058 | 0.050 | in_band | 1.1 | any_selected 0.096; x0_selected 0.037; block_caveat 0.000 |
| s09_discover | persistent_feature | 60 | data-driven | - | size (4000) | 0.048 | 0.003 | 0.042-0.055 | 0.050 | in_band | 1.2 | any_selected 0.091; x0_selected 0.039; block_caveat 1.000 |
| s09_discover | persistent_feature | 60 | declared 1 | - | size (4000) | 0.050 | 0.003 | 0.043-0.057 | 0.050 | in_band | 1.0 | any_selected 0.094; x0_selected 0.041; block_caveat 0.000 |
| s09_discover | persistent_feature | 60 | declared 5 | - | size (4000) | 0.054 | 0.004 | 0.048-0.062 | 0.050 | in_band | 5.0 | any_selected 0.103; x0_selected 0.044; block_caveat 0.000 |
| s09_discover | persistent_feature | 120 | data-driven | - | size (4000) | 0.052 | 0.004 | 0.046-0.059 | 0.050 | in_band | 1.3 | any_selected 0.091; x0_selected 0.039; block_caveat 0.000 |
| s09_discover | persistent_feature | 120 | declared 1 | - | size (4000) | 0.047 | 0.003 | 0.041-0.054 | 0.050 | in_band | 1.0 | any_selected 0.101; x0_selected 0.039; block_caveat 0.000 |
| s09_discover | persistent_feature | 120 | declared 5 | - | size (4000) | 0.048 | 0.003 | 0.041-0.055 | 0.050 | in_band | 5.0 | any_selected 0.098; x0_selected 0.036; block_caveat 0.000 |
| s09_discover | persistent_feature | 240 | data-driven | - | size (4000) | 0.055 | 0.004 | 0.048-0.062 | 0.050 | in_band | 1.2 | any_selected 0.097; x0_selected 0.039; block_caveat 0.000 |
| s09_discover | persistent_feature | 240 | declared 1 | - | size (4000) | 0.051 | 0.003 | 0.045-0.059 | 0.050 | in_band | 1.0 | any_selected 0.102; x0_selected 0.042; block_caveat 0.000 |
| s09_discover | persistent_feature | 240 | declared 5 | - | size (4000) | 0.053 | 0.004 | 0.046-0.060 | 0.050 | in_band | 5.0 | any_selected 0.102; x0_selected 0.041; block_caveat 0.000 |
| s09_discover | persistent_feature | 500 | data-driven | - | size (4000) | 0.052 | 0.004 | 0.046-0.059 | 0.050 | in_band | 1.1 | any_selected 0.101; x0_selected 0.039; block_caveat 0.000 |
| s09_discover | persistent_feature | 500 | declared 1 | - | size (4000) | 0.055 | 0.004 | 0.048-0.062 | 0.050 | in_band | 1.0 | any_selected 0.101; x0_selected 0.043; block_caveat 0.000 |
| s09_discover | persistent_feature | 500 | declared 5 | - | size (4000) | 0.048 | 0.003 | 0.042-0.055 | 0.050 | in_band | 5.0 | any_selected 0.098; x0_selected 0.039; block_caveat 0.000 |
| s09_discover | ar1_target | 60 | data-driven | - | size (4000) | 0.075 | 0.004 | 0.067-0.083 | 0.050 | high_sig | 5.0 | any_selected 0.152; x0_selected 0.065; block_caveat 1.000 |
| s09_discover | ar1_target | 60 | declared 1 | - | size (4000) | 0.153 | 0.006 | 0.142-0.165 | 0.050 | high_sig | 1.0 | any_selected 0.306; x0_selected 0.149; block_caveat 0.000 |
| s09_discover | ar1_target | 60 | declared 5 | - | size (4000) | 0.066 | 0.004 | 0.059-0.074 | 0.050 | high | 5.0 | any_selected 0.135; x0_selected 0.056; block_caveat 0.000 |
| s09_discover | ar1_target | 120 | data-driven | - | size (4000) | 0.060 | 0.004 | 0.053-0.068 | 0.050 | in_band | 12.6 | any_selected 0.117; x0_selected 0.046; block_caveat 0.492 |
| s09_discover | ar1_target | 120 | declared 1 | - | size (4000) | 0.153 | 0.006 | 0.143-0.165 | 0.050 | high_sig | 1.0 | any_selected 0.326; x0_selected 0.150; block_caveat 0.000 |
| s09_discover | ar1_target | 120 | declared 5 | - | size (4000) | 0.067 | 0.004 | 0.059-0.075 | 0.050 | high | 5.0 | any_selected 0.126; x0_selected 0.057; block_caveat 0.000 |
| s09_discover | ar1_target | 240 | data-driven | - | size (4000) | 0.057 | 0.004 | 0.050-0.064 | 0.050 | in_band | 17.0 | any_selected 0.109; x0_selected 0.045; block_caveat 0.031 |
| s09_discover | ar1_target | 240 | declared 1 | - | size (4000) | 0.157 | 0.006 | 0.146-0.169 | 0.050 | high_sig | 1.0 | any_selected 0.344; x0_selected 0.149; block_caveat 0.000 |
| s09_discover | ar1_target | 240 | declared 5 | - | size (4000) | 0.076 | 0.004 | 0.068-0.085 | 0.050 | high_sig | 5.0 | any_selected 0.152; x0_selected 0.066; block_caveat 0.000 |
| s09_discover | ar1_target | 500 | data-driven | - | size (4000) | 0.056 | 0.004 | 0.049-0.064 | 0.050 | in_band | 17.0 | any_selected 0.107; x0_selected 0.042; block_caveat 0.000 |
| s09_discover | ar1_target | 500 | declared 1 | - | size (4000) | 0.166 | 0.006 | 0.154-0.177 | 0.050 | high_sig | 1.0 | any_selected 0.352; x0_selected 0.157; block_caveat 0.000 |
| s09_discover | ar1_target | 500 | declared 5 | - | size (4000) | 0.084 | 0.004 | 0.076-0.093 | 0.050 | high_sig | 5.0 | any_selected 0.150; x0_selected 0.070; block_caveat 0.000 |
| s09_discover | ar1_target | 60 | data-driven | target={"phi":0.7} | size (4000) | 0.105 | 0.005 | 0.096-0.115 | 0.050 | high_sig | 7.0 | any_selected 0.201; x0_selected 0.091; block_caveat 1.000 |
| s09_discover | ar1_target | 120 | data-driven | target={"phi":0.7} | size (4000) | 0.067 | 0.004 | 0.059-0.075 | 0.050 | high | 15.0 | any_selected 0.133; x0_selected 0.052; block_caveat 1.000 |
| s09_discover | ar1_target | 240 | data-driven | target={"phi":0.7} | size (4000) | 0.060 | 0.004 | 0.053-0.068 | 0.050 | in_band | 30.0 | any_selected 0.121; x0_selected 0.050; block_caveat 1.000 |
| s09_discover | ar1_target | 500 | data-driven | target={"phi":0.7} | size (4000) | 0.052 | 0.004 | 0.046-0.060 | 0.050 | in_band | 62.0 | any_selected 0.106; x0_selected 0.042; block_caveat 0.999 |
| s09_discover | ar1_target | 60 | data-driven | target={"phi":-0.35} | size (4000) | 0.034 | 0.003 | 0.029-0.040 | 0.050 | low | 5.7 | any_selected 0.066; x0_selected 0.026; block_caveat 1.000 |
| s09_discover | ar1_target | 120 | data-driven | target={"phi":-0.35} | size (4000) | 0.042 | 0.003 | 0.036-0.049 | 0.050 | in_band | 13.2 | any_selected 0.085; x0_selected 0.031; block_caveat 0.579 |
| s09_discover | ar1_target | 240 | data-driven | target={"phi":-0.35} | size (4000) | 0.048 | 0.003 | 0.042-0.055 | 0.050 | in_band | 17.6 | any_selected 0.087; x0_selected 0.035; block_caveat 0.040 |
| s09_discover | ar1_target | 500 | data-driven | target={"phi":-0.35} | size (4000) | 0.039 | 0.003 | 0.034-0.046 | 0.050 | low | 17.4 | any_selected 0.093; x0_selected 0.031; block_caveat 0.000 |
| s09_discover | regime_switch | 60 | data-driven | - | size (4000) | 0.057 | 0.004 | 0.050-0.064 | 0.050 | in_band | 1.5 | any_selected 0.106; x0_selected 0.047; block_caveat 1.000 |
| s09_discover | regime_switch | 60 | declared 1 | - | size (4000) | 0.058 | 0.004 | 0.051-0.066 | 0.050 | in_band | 1.0 | any_selected 0.107; x0_selected 0.046; block_caveat 0.000 |
| s09_discover | regime_switch | 60 | declared 5 | - | size (4000) | 0.052 | 0.004 | 0.045-0.059 | 0.050 | in_band | 5.0 | any_selected 0.107; x0_selected 0.042; block_caveat 0.000 |
| s09_discover | regime_switch | 120 | data-driven | - | size (4000) | 0.058 | 0.004 | 0.051-0.066 | 0.050 | in_band | 1.7 | any_selected 0.110; x0_selected 0.046; block_caveat 0.004 |
| s09_discover | regime_switch | 120 | declared 1 | - | size (4000) | 0.056 | 0.004 | 0.049-0.063 | 0.050 | in_band | 1.0 | any_selected 0.110; x0_selected 0.044; block_caveat 0.000 |
| s09_discover | regime_switch | 120 | declared 5 | - | size (4000) | 0.057 | 0.004 | 0.050-0.065 | 0.050 | in_band | 5.0 | any_selected 0.121; x0_selected 0.047; block_caveat 0.000 |
| s09_discover | regime_switch | 240 | data-driven | - | size (4000) | 0.059 | 0.004 | 0.052-0.066 | 0.050 | in_band | 1.5 | any_selected 0.117; x0_selected 0.049; block_caveat 0.000 |
| s09_discover | regime_switch | 240 | declared 1 | - | size (4000) | 0.054 | 0.004 | 0.048-0.062 | 0.050 | in_band | 1.0 | any_selected 0.116; x0_selected 0.044; block_caveat 0.000 |
| s09_discover | regime_switch | 240 | declared 5 | - | size (4000) | 0.061 | 0.004 | 0.054-0.069 | 0.050 | high | 5.0 | any_selected 0.110; x0_selected 0.048; block_caveat 0.000 |
| s09_discover | regime_switch | 500 | data-driven | - | size (4000) | 0.063 | 0.004 | 0.056-0.071 | 0.050 | high | 1.3 | any_selected 0.120; x0_selected 0.050; block_caveat 0.000 |
| s09_discover | regime_switch | 500 | declared 1 | - | size (4000) | 0.054 | 0.004 | 0.047-0.061 | 0.050 | in_band | 1.0 | any_selected 0.118; x0_selected 0.043; block_caveat 0.000 |
| s09_discover | regime_switch | 500 | declared 5 | - | size (4000) | 0.050 | 0.003 | 0.044-0.057 | 0.050 | in_band | 5.0 | any_selected 0.102; x0_selected 0.041; block_caveat 0.000 |
| s09_discover | jump_diffusion | 60 | data-driven | - | size (4000) | 0.052 | 0.004 | 0.046-0.060 | 0.050 | in_band | 1.2 | any_selected 0.097; x0_selected 0.043; block_caveat 1.000 |
| s09_discover | jump_diffusion | 120 | data-driven | - | size (4000) | 0.054 | 0.004 | 0.047-0.061 | 0.050 | in_band | 1.3 | any_selected 0.101; x0_selected 0.043; block_caveat 0.001 |
| s09_discover | jump_diffusion | 240 | data-driven | - | size (4000) | 0.048 | 0.003 | 0.042-0.055 | 0.050 | in_band | 1.1 | any_selected 0.090; x0_selected 0.037; block_caveat 0.000 |
| s09_discover | jump_diffusion | 500 | data-driven | - | size (4000) | 0.052 | 0.004 | 0.046-0.059 | 0.050 | in_band | 1.1 | any_selected 0.095; x0_selected 0.040; block_caveat 0.000 |
| s09_discover | persistent_feature | 60 | data-driven | horizon=5, sampling=horizon_spaced | size (4000) | 0.052 | 0.004 | 0.046-0.059 | 0.050 | in_band | 1.3 | any_selected 0.098; x0_selected 0.040; block_caveat 1.000 |
| s09_discover | persistent_feature | 60 | data-driven | horizon=5, sampling=fixed_step_block_null | size (4000) | 0.079 | 0.004 | 0.071-0.088 | 0.050 | high_sig | 7.0 | any_selected 0.000; x0_selected 0.000; block_caveat 1.000 |
| s09_discover | ar1_target | 60 | data-driven | selection=ledger_holm | size (4000) | 0.082 | 0.004 | 0.074-0.091 | 0.050 | high_sig | 5.0 | any_selected 0.086; x0_selected 0.036; block_caveat 1.000 |
| s09_discover | persistent_feature | 240 | data-driven | horizon=5, sampling=horizon_spaced | size (4000) | 0.047 | 0.003 | 0.041-0.054 | 0.050 | in_band | 1.2 | any_selected 0.098; x0_selected 0.035; block_caveat 0.000 |
| s09_discover | persistent_feature | 240 | data-driven | horizon=5, sampling=fixed_step_block_null | size (4000) | 0.053 | 0.004 | 0.046-0.060 | 0.050 | in_band | 30.0 | any_selected 0.000; x0_selected 0.000; block_caveat 1.000 |
| s09_discover | ar1_target | 240 | data-driven | selection=ledger_holm | size (4000) | 0.048 | 0.003 | 0.042-0.055 | 0.050 | in_band | 17.0 | any_selected 0.051; x0_selected 0.015; block_caveat 0.034 |
| s09_discover | persistent_feature | 60 | data-driven | effect=0.1 | power (2000) | 0.117 | 0.007 | 0.103-0.131 | 0.050 | - | 1.2 | any_selected 0.150; x0_selected 0.096; block_caveat 1.000 |
| s09_discover | ar1_target | 60 | data-driven | effect=0.1 | power (2000) | 0.121 | 0.007 | 0.107-0.136 | 0.050 | - | 5.1 | any_selected 0.189; x0_selected 0.105; block_caveat 1.000 |
| s09_discover | persistent_feature | 60 | data-driven | effect=0.2 | power (2000) | 0.320 | 0.010 | 0.299-0.340 | 0.050 | - | 1.3 | any_selected 0.331; x0_selected 0.284; block_caveat 1.000 |
| s09_discover | ar1_target | 60 | data-driven | effect=0.2 | power (2000) | 0.225 | 0.009 | 0.207-0.244 | 0.050 | - | 5.3 | any_selected 0.258; x0_selected 0.195; block_caveat 1.000 |
| s09_discover | persistent_feature | 60 | data-driven | effect=0.3 | power (2000) | 0.628 | 0.011 | 0.607-0.649 | 0.050 | - | 1.4 | any_selected 0.599; x0_selected 0.577; block_caveat 1.000 |
| s09_discover | ar1_target | 60 | data-driven | effect=0.3 | power (2000) | 0.415 | 0.011 | 0.394-0.437 | 0.050 | - | 5.7 | any_selected 0.423; x0_selected 0.376; block_caveat 1.000 |
| s09_discover | persistent_feature | 240 | data-driven | effect=0.1 | power (2000) | 0.365 | 0.011 | 0.344-0.386 | 0.050 | - | 1.2 | any_selected 0.348; x0_selected 0.316; block_caveat 0.000 |
| s09_discover | ar1_target | 240 | data-driven | effect=0.1 | power (2000) | 0.210 | 0.009 | 0.193-0.229 | 0.050 | - | 17.5 | any_selected 0.236; x0_selected 0.175; block_caveat 0.038 |
| s09_discover | persistent_feature | 240 | data-driven | effect=0.2 | power (2000) | 0.872 | 0.007 | 0.857-0.886 | 0.050 | - | 1.3 | any_selected 0.859; x0_selected 0.852; block_caveat 0.000 |
| s09_discover | ar1_target | 240 | data-driven | effect=0.2 | power (2000) | 0.614 | 0.011 | 0.592-0.635 | 0.050 | - | 19.3 | any_selected 0.578; x0_selected 0.555; block_caveat 0.083 |
| s09_discover | persistent_feature | 240 | data-driven | effect=0.3 | power (2000) | 1.000 | 0.000 | 0.998-1.000 | 0.050 | - | 1.9 | any_selected 0.999; x0_selected 0.999; block_caveat 0.000 |
| s09_discover | ar1_target | 240 | data-driven | effect=0.3 | power (2000) | 0.926 | 0.006 | 0.914-0.937 | 0.050 | - | 21.9 | any_selected 0.894; x0_selected 0.890; block_caveat 0.186 |
| s09_discover | regime_switch | 1000 | data-driven | - | size (4000) | 0.054 | 0.004 | 0.048-0.062 | 0.050 | in_band | 1.2 | any_selected 0.116; x0_selected 0.042; block_caveat 0.000 |
| s09_discover | regime_switch | 1000 | declared 20 | - | size (4000) | 0.051 | 0.003 | 0.045-0.059 | 0.050 | in_band | 20.0 | any_selected 0.103; x0_selected 0.039; block_caveat 0.000 |
| s09_discover | ar1_target | 1000 | data-driven | - | size (4000) | 0.058 | 0.004 | 0.051-0.065 | 0.050 | in_band | 17.1 | any_selected 0.107; x0_selected 0.044; block_caveat 0.000 |
| s09_discover | ar1_target | 1000 | declared 5 | - | size (4000) | 0.071 | 0.004 | 0.063-0.079 | 0.050 | high_sig | 5.0 | any_selected 0.140; x0_selected 0.058; block_caveat 0.000 |
| s09_discover | ar1_target | 120 | data-driven | selection=ledger_holm | size (4000) | 0.050 | 0.003 | 0.044-0.057 | 0.050 | in_band | 12.5 | any_selected 0.054; x0_selected 0.017; block_caveat 0.486 |
| s09_discover | ar1_target | 500 | data-driven | selection=ledger_holm | size (4000) | 0.058 | 0.004 | 0.051-0.065 | 0.050 | in_band | 17.0 | any_selected 0.052; x0_selected 0.019; block_caveat 0.000 |
