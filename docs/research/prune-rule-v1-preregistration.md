# Pre-registered prune/abstain rule — `prune_rule_v1`

Status: **pre-registered**. Written 2026-11-05 (US/Pacific; 2026-11-06 UTC) for plan
slice W2-4 / S16, and committed **before** any forward edge number was computed.
The rule below is frozen. If the edge numbers make it look wrong, the fix is a new
version (`prune_rule_v2`) with its own pre-registration. This file is not edited
after the numbers are in.

Order of work, recorded in the PR:

1. This file (the rule) is committed first.
2. Code implementing the rule, the edge report and the trust-score switch follows.
3. Only then is the edge report run read-only on production, and its numbers are
   published in `docs/research/edge-report-forward-*.md`.

## What it decides

For each **unit** it gives a verdict of `EDGE`, `NO_EDGE`, `ANTI_EDGE` or
`INSUFFICIENT`. The units are:

| Family | Unit | Source of outcomes |
|---|---|---|
| `signal_source_type` | `signal_sources.source_type` | `signal_source_outcomes` (`sig-eval-4-sessions`) |
| `signal_feed` | `(source_type, feed_source_id(source_id))` | same |
| `oracle_model` | `oracle_predictions.model_name` (`dedup_keep` rows) | official `verdict` |
| `astrogrid_model` | `astrogrid.prediction_run.model_version` (e.g. `grid-crypto-v1`) | effective `prediction_score` rows |

## Outcome per row (binary "direction right")

- **Signals:** `CORRECT` = 1, `WRONG` = 0. `NO_MOVE` (the move stayed inside the ±1%
  dead band) is **excluded** from the hit rate and reported separately as the
  no-move share. `INELIGIBLE` and `UNRESOLVED` (no row yet) are excluded entirely.
- **Oracle:** `hit` or `partial` = 1 (both mean the expiry close was on the called
  side of entry), `miss` = 0. `no_data`, `pending` and anything else are excluded.
  The target-hit rate (`hit` only) is reported too, but the rule uses direction.
- **AstroGrid (crypto-v1 and other model versions):** direction from the call text
  (`store.astrogrid._prediction_direction`), and `realized_return`. 1 if
  `sign(direction) * realized_return > 0`, 0 if `< 0`. Neutral calls, a missing
  return and an exactly-zero return are excluded. The stored `verdict` hit rate is
  reported too.

**Baseline** for every family: **0.5** (a coin flip on direction). Market drift can
push an always-long unit above 0.5 without skill. The report shows the share of
long calls per unit so a reader can see it. The rule does not adjust for drift.

## Forward-only data

Only the **forward** column is used by the rule:

- **Signals:** a corrected outcome row (`CORRECT`/`WRONG`/`NO_MOVE`) whose
  `signal_sources.created_at` is known, timezone-aware, and earlier than 16:00
  America/New_York on the entry-bar date. The evaluator anchors entry no earlier
  than the day the row became known, so this holds by construction. The check
  stays in the query so a violation shows up in the exploratory column instead of
  silently passing. The corrected rule reuses the legacy horizons
  (`EVALUATION_WINDOWS`) and the legacy 1% threshold, both fixed before these
  signals were recorded. Only the treatment of a sub-threshold move changed
  (`NO_MOVE` instead of `WRONG`). The share of rows scored live (by the forward
  scorer within `FINAL_GRACE_DAYS` + 2 days of the exit) rather than by the 11-06
  replay is reported per unit.
- **Oracle:** an official verdict row in `oracle_predictions` with
  `created_at < expiry 16:00 America/New_York` and `scored_at` not before that
  close. Rows in `oracle_prediction_rescores` (`exploratory_hindsight`, #976) are
  **exploratory** only.
- **AstroGrid:** effective base `prediction_score` rows (the SPY-graded legacy rows
  excluded exactly as `store/astrogrid.py` excludes them) with
  `prediction_run.created_at` earlier than the score time. Rows in
  `prediction_score_legacy_rescore` (#973) are **exploratory** only.

## Statistics (per unit, forward column)

- `N`: rows with a binary outcome. `D`: distinct cluster dates (signal date for
  signals, `created_at` date in UTC for oracle and AstroGrid).
- **Effective N clustered by date:** `N_eff = p(1-p) / Var_CR(p̂)`, where
  `Var_CR = D/(D-1) · Σ_d (h_d − p̂·n_d)² / N²` (the cluster-robust variance of
  the hit rate). It is capped at `N`. When `p̂ ∈ {0, 1}`, `N_eff = D`.
- **Clustered Wilson 95% CI:** a Wilson interval with `n = N_eff`. The naive
  Wilson CI (with `n = N`) is reported alongside it.
- **Block bootstrap:** a circular block bootstrap over the date-ordered per-date
  `(hits, n)` pairs, with B = 1999 resamples and a seed from
  `evals.e2.scoring.seed_for(family, unit)`. The block length is
  `analysis.offline_research_proof.autocorrelation_block` on the per-date hit-rate
  series, with overlap depth = horizon in days − 1, never below 1 and never above
  D // 4. Two-sided p =
  `(1 + #{|p*_b − p̂| ≥ |p̂ − 0.5|}) / (B + 1)`.
- **Multiple testing:** Benjamini–Hochberg (`analysis.offline_research_proof.bh_adjusted`)
  within each family, over every unit in that family. An `INSUFFICIENT` unit
  enters the BH set with p = 1.0.
- **Brier score**, where a probability exists (oracle `confidence` in [0, 1]
  against direction-right), together with the climatological Brier
  `p̄(1 − p̄)`. Brier is **informational** and not part of the verdict.

## Verdict (forward column only)

Thresholds: `MIN_EFFECTIVE_N = 30`, `MIN_DATES = 20`, `ALPHA = 0.05` (BH-adjusted).

1. `INSUFFICIENT` if `N_eff < 30` or `D < 20`.
2. `EDGE` if the clustered Wilson lower bound is > 0.5 **and** BH-adjusted p < 0.05.
3. `ANTI_EDGE` if the clustered Wilson upper bound is < 0.5 **and** BH-adjusted
   p < 0.05.
4. `NO_EDGE` otherwise, which means the interval includes 0.5 or the result does
   not survive BH.

## What a verdict does (behind flags, default off, no automatic reweighting)

- `GRID_TRUST_PRUNE_NO_EDGE` (default `false`) turns the filter on.
  `GRID_TRUST_PRUNE_LIST_PATH` names an **operator-approved** JSON list. The
  edge-report CLI writes only a *candidate* list. Nothing takes effect until an
  operator copies that list to the configured path and turns the flag on.
  Promotion stays manual.
- A `signal_source_type` or `signal_feed` unit listed as `NO_EDGE` or `ANTI_EDGE`
  gets **weight 0** in:
  - the conviction stack's convergence layer
    (`intelligence/signal_convergence_scanner.scan_convergence`): the stream is
    dropped and counted as pruned;
  - `intelligence/trust_scorer.detect_convergence`: the signal does not count
    towards the 3-source threshold or the combined confidence;
  - `intelligence/trust_scorer.get_trusted_sources`: the unit is not returned as
    trusted.
- `ANTI_EDGE` is **never** flipped into a contrarian signal automatically.
- `EDGE` and `INSUFFICIENT` units keep their current weight. Nothing is up-weighted
  by this rule.
- `oracle_model` and `astrogrid_model` verdicts are reported and carried in the
  list, but this version wires no automatic effect into the oracle ensemble or
  AstroGrid weights. Oracle calibration belongs to the W2-2 slice. Any model
  pruning there is a separate, manual decision.

## Trust-score input switch (S16 item 1, recorded here for completeness)

`GRID_TRUST_USE_CORRECTED_OUTCOMES` (default `false`) makes `update_trust_scores`
read `signal_source_outcomes` (`sig-eval-4-sessions`) instead of the legacy
`signal_sources.outcome`:

- `CORRECT` counts as a hit and `WRONG` as a miss. Both keep the existing 90-day
  recency decay and the `source_trust_config` prior.
- `NO_MOVE` counts as **neither**. A move inside the dead band is not evidence for
  or against a direction call. Counting it as a miss would bring back the legacy
  bias (10.9% vs 50.7%), and counting it as a hit would reward calls that did not
  move. It is reported as `no_move_count` in the breakdown.
- `INELIGIBLE` counts as neither, and so does the absence of a row (`UNRESOLVED`).
  A source whose corrected outcomes are all `NO_MOVE`/`INELIGIBLE` falls back to its
  prior trust instead of keeping a stale legacy score.
