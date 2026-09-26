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
forward start. Its status is never PASSED/confirmed/promoted. Since S10 it is
tagged by origin: only a `latest_vintage_read` candidate is
`FORWARD_EVIDENCE_PENDING`; a synthetic or replay "candidate" carries
`SYNTHETIC_PROOF_ONLY` / `EXPLORATORY_REPLAY_ONLY`. All results report zero forward evidence, and no
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
  Since S09b, overlap depth + 1 is only the floor: the default block is
  data-driven from the discovery target's lag-1 autocorrelation (see S09b).
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

Since S09b the default block is data-driven, so the replay now uses blocks
1 and 6 (horizon-spaced) and 1, 2, 4, 5 and 6 (weekly). Both modes still give
0 / 0 survivors.

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

## S09: fixes from the #658 review, read origin, real-panel adapter (2026-09-26)

S09 named its origin `pit_vintage_read`, its panel `PitPanel` and the protocol
field `pit_receipt`. S09b renamed them to `latest_vintage_read`,
`LatestVintagePanel` and `read_receipt`, because the data is not
point-in-time (see S09b). The S09 text below uses the new names.

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
  4.25%. This holds only for a serially independent target. S09b shows that
  block 1 is anti-conservative when the target is autocorrelated.
- **`change` labels.** `build_family_rows(..., label="change")` labels
  `end - start` for rates, spreads and indexes that can be 0 or negative. The
  label is part of the horizon identity, so a family cannot mix label kinds.
- **Distinct read origin `latest_vintage_read`.** `exploratory_replay` is a
  self-declared label, so this origin is gated differently. A `Protocol` with
  this origin must carry `read_receipt`, the sha256 of a `LatestVintagePanel`
  receipt, and no other origin may carry one. `analysis/research_real_panel.py`
  is the only constructor of a `LatestVintagePanel`
  (`load_latest_vintage_panel`, guarded by a capability token). It reads each
  declared series once through
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
- **Adapter availability rules (S09, superseded by S09b).** A feature
  observation dated `d` became usable at the first session on or after
  `d + lag_days` calendar days, with a per-series lag (1 for daily
  H.15/ICE/VIX, 8 for H.10 FX, 2 for H.4.1). It was then carried forward at
  most `stale_sessions` sessions. Every feature's `known_at` was stamped
  00:00Z of its session, which is about 20 hours before H.15 publishes. A
  target's label became known at the first session on or after
  `label_end + lag_days`, and a label not known inside its window was purged.
- **Adapter refusals.** The adapter refuses `snap:*`, LLM/telemetry counters,
  astro/celestial ids and yfinance ids (`YF:`/`YF_ADJ:`, see S07/#642). In S09
  revised series were refused only when the caller declared `revised=True`;
  S09b replaced that flag with a denylist inside the adapter. It builds no SQL
  of its own and never names `discovered_hypotheses` or `hypothesis_registry`.
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
nothing frozen) kept 102, 102 and 108 selections with blocks 2, 4 and 8. S09
read this as "not an artifact of the block-1 null". S09b corrects that
reading (see "Why longer blocks selected more" below): more selections under
longer blocks is expected when the target labels are negatively
autocorrelated, so it is not a robustness check. Tradable targets still need
the price-basis split.

**Review of this ledger (#660).** An independent review found that at least 5
of the 8 frozen candidates are a target predicting itself or a near-copy of
itself: 3 are VIX → VIX, and 2 are DGS1 → DGS2 and DGS1 → T10Y2Y. The other 3
(HY OAS z-scores → VIX) reflect shared volatility state. The data is
latest-vintage hindsight, and feature `known_at` was stamped before
publication. `frozen-candidates.json` must not be consumed. S09b writes a
relabelled copy beside it.

**Known limits.**

- **Revisions are hindsight.** Every row of this universe was pulled on or
  after 2026-03-24 (backfill). `as_of_ts` makes the read reproducible, but it
  does not give first-release values.
- **Sessions are business days**, not an exchange calendar.
- **Publication lags are declared constants**, not per-release timestamps.
  S09b declares a lag and a time of day per source, but these are still
  schedules, not the timestamps of individual releases.

## S09b: follow-ups from the #660 review (2026-09-26)

Run the tests with `python -m pytest tests/test_offline_research_stats_core.py tests/test_research_real_panel.py tests/test_run_real_panel_scan.py -q`.
Relabel a pre-S09b candidate file with
`python -m scripts.relabel_frozen_candidates PATH/frozen-candidates.json`.

### 1. Proxy groups and `self_lag` trials

In S09 all 4 targets were also features, and nothing flagged own-series or
near-copy features. `analysis/research_real_panel.py::PROXY_GROUPS` now
declares, per target series, the series that are its own value or a
near-copy of it. The groups are keyed by target, not found by matching series
ids, and they follow one rule. Rules (a) to (d) are computed from a declared
leg map (`SPREAD_LEGS`, e.g. `T10YIE = DGS10 - DFII10`) and tenor ladder
(`TREASURY_TENORS`) by `required_proxies`:

- **(a)** the target's own series and its legs (a level is its own leg);
- **(b)** every series that shares a leg with the target;
- **(c)** for a spread target, the other leg of every spread that shares a
  leg with it (T10YIE and DFII10 together rebuild the 10-year leg of T10Y2Y);
- **(d)** for each Treasury leg, the next shorter and next longer quoted
  tenor. Where that tenor is absent from the universe, the nearest tenor that
  is present on that side is added too (the universe has no DGS3 or DGS7, so
  DGS5 counts for both the 2-year and the 10-year leg, and DGS30 for the
  10-year leg);
- **(e)** by declaration: the rating sub-indices and the yield or total-return
  versions of the same credit index, and the other indices in the same
  implied-volatility family.

| Target | Proxy group |
|---|---|
| `VIXCLS` | VIXCLS, VXVCLS, VXOCLS, VIX3M, VIX9D |
| `DGS2` | DGS2, DGS1, DGS3, DGS5, T10Y2Y |
| `T10Y2Y` | T10Y2Y; legs DGS10, DGS2; tenors DGS7, DGS20, DGS1, DGS3, DGS5, DGS30; shared-leg spreads T10Y3M, T10Y1Y, T10YIE; their other legs DGS3MO, DFII10 |
| `BAMLH0A0HYM2` | the HY master, its BB/B/CCC sub-indices, and their effective-yield and total-return series |

The adapter refuses a target that has no declared group. It also refuses a
group that lacks a member rules (a) to (d) require for the declared universe,
so the groups cannot silently drift from the leg map. A test also checks the
leg-sharing condition directly from `SPREAD_LEGS`. The adapter derives the
`(family, feature)` pairs whose feature series is in the family target's
group, and the protocol must carry exactly those pairs as `self_lag`. The
contract refuses a protocol that drops or changes them. `discover` still
measures each such trial and records `self_lag_r`/`self_lag_p`. It gives the
trial status `self_lag` and p = 1.0 in the BH denominator, so the trial can
never be selected. `evaluate_holdout` refuses a manifest that selects one,
even if the manifest is re-signed. Over the declared scan universe, 153 of
the 1,044 trials are `self_lag` (17 target-series pairs × 3 transforms × 3
horizons).

Relabelled candidates from the ef0d564b scan (written to
`scan-ef0d564b/frozen-candidates.relabelled.json`; the original receipts are
unchanged):

| Family | Feature | Label |
|---|---|---|
| VIXCLS fwd1 | VIXCLS chg5 | SELF_LAG |
| VIXCLS fwd1 | VIXCLS z60 | SELF_LAG |
| VIXCLS fwd5 | VIXCLS z60 | SELF_LAG |
| DGS2 fwd5 | DGS1 chg20 | SELF_LAG |
| T10Y2Y fwd5 | DGS1 z60 | SELF_LAG |
| VIXCLS fwd5 | BAMLH0A0HYM2 z60 | cross-series |
| VIXCLS fwd5 | BAMLH0A1HYBB z60 | cross-series |
| VIXCLS fwd5 | BAMLH0A2HYB z60 | cross-series |

The 3 cross-series candidates remain candidates only under the proxy rule.
Their run predates the publication-time `known_at` and the data-driven block,
so the copy marks them `RESCAN_REQUIRED`; they need a new scan under S09b
before any forward logging. The SELF_LAG ones are marked
`SELF_LAG_NEVER_A_CANDIDATE`.
`promotion_allowed` stays false for all 8.

### 2. `latest_vintage_read` and the revised-series denylist

The origin `pit_vintage_read` overclaimed. Every row is the latest vintage, all
backfilled on or after 2026-03-24. S09 also refused revised series only when
the caller declared `revised=True`, and the default was False. S09b makes
these changes:

- **Rename.** The origin is now `latest_vintage_read` and its state is
  `LATEST_VINTAGE_READ_EXPLORATORY`, in the code, the manifest, the docs and
  the tests. The receipt records
  `vintage: "latest vintage per obs_date (hindsight), not first release"`.
  The old label is refused as an unknown origin.
- **Denylist.** The adapter enforces it; the caller declares nothing, and
  `SeriesSpec` no longer has a `revised` field. `REVISED_SERIES` lists single
  ids and `REVISED_PREFIXES` lists whole families (`NFCI*`, `ANFCI*`,
  `STLFSI*`). The list covers:
  - Chicago Fed NFCI/ANFCI, CFNAI and KCFSI, whose history is re-estimated
    each release;
  - the Weekly Economic Index;
  - DOL claims, which go from advance to revised and get annual seasonal
    factors;
  - BLS CES/CPS/CPI/JOLTS;
  - BEA PCE/NIPA;
  - Census activity series;
  - Fed G.17, H.6, H.8 and G.19;
  - the Michigan preliminary sentiment;
  - the Fed broad dollar indices (`DTWEXBGS`/`AFEGS`/`EMEGS`), whose history
    is revised when trade weights are updated.

  The list is kept by hand from those publishers' revision policies
  (`REVISED_SOURCES`). ALFRED vintage counts are the check before any id is
  removed. `DTWEXBGS` was dropped from the scan universe, which now has 29
  series.
- **Limit.** Being absent from the list does not prove a series is never
  revised. H.15, ICE BofA and CBOE closes are treated as unrevised because
  that is their publishers' practice.

### 3. Publication-time `known_at`

In S09 every feature value carried `known_at` = 00:00Z of its decision
session. A calendar lag of 1 day made H.15's Monday value usable at Tuesday
00:00Z, about 20 hours before H.15 publishes it. Friday values became usable
on Monday, before the Monday publication.

S09b gives every series a publication `source` (`PUBLICATIONS`). An
observation dated `d` is known at `d + lag` at a declared UTC time of day.
Business-day lags use the US federal holiday calendar. The value is usable
from the first 00:00Z session at or after that stamp. Each feature value now
carries that stamp as its `known_at`, and the target label is known at the
publication stamp of the observation dated `label_end`.

| Source | Series | Lag | Time (UTC) | Basis |
|---|---|---|---|---|
| `FRB_H15` | DGS*, DFII10, DFF | 1 business day | 21:17 | Reviewer-verified ~20:17Z in EDT; +1 h covers EST |
| `FRED_H15_SPREAD` | T10Y2Y, T10Y3M, T10YIE, T5YIE | 1 business day | 23:59 | FRED computes these from H.15 legs after H.15 posts; time not verified |
| `ICE_BOFA` | BAML* | 1 business day | 23:59 | Time not verified |
| `CBOE_VIX` | VIXCLS | 1 business day | 23:59 | Time not verified |
| `FRB_H10` | DEX* | 8 calendar days | 21:15 | Weekly Monday post; reviewer-verified lag 8 |
| `FRB_H41` | WALCL, WTREGEN | 2 calendar days | 21:30 | Wednesday level, Thursday 16:30 ET release; reviewer-verified lag 2 |
| `NYFED_RRP` | RRPONTSYD | 1 business day | 23:59 | Time not verified |
| `FREDDIE_PMMS` | MORTGAGE30US | 1 calendar day | 17:00 | Thursday same-day release; reviewer-verified lag 1 |
| `AAII` | aaii.bull_bear_spread | 1 calendar day | 23:59 | Thursday-dated, pulled Friday; reviewer-verified lag 1 |

In practice a daily H.15 value dated Monday is first used at Wednesday
00:00Z. After a Monday holiday, a Friday value is first used on Wednesday.
The tests check four things:

- every feature `known_at` is at or before its decision, and every observed
  value carries a real publication stamp;
- every declared source is known strictly after the next day's 00:00Z
  decision;
- the holiday and weekend cases resolve as described;
- a value stamped the S09 way is refused by `validate_rows`.

### 4. Calibration under an autocorrelated target, and the data-driven block

S09's block-1 calibration used a serially independent target. S09b adds a
target that is AR(1) with phi = 0.35 at the sampling spacing (the HY OAS fwd5
label has acf1 = +0.335), against an independent AR(1) 0.95 feature. The
tests use 800 simulations, 499 permutations and a nominal 5%:

| Target | n | Block 1 | Data-driven block (median) |
|---|---|---|---|
| AR(1) phi = 0.35 | 60 | 15.9% | 7.0% (7) |
| AR(1) phi = 0.35 | 240 | 14.6% | 5.0% (16) |
| AR(1) phi = -0.18 | 240 | 2.0% | 4.25% (6) |
| independent | 240 | 4.4% | 4.4% (1) |

Block 1 is badly anti-conservative for a positively autocorrelated target, so
the default block (`block=0`) is now data-driven
(`offline_research_proof.autocorrelation_block`):

- It uses the lag-1 autocorrelation phi of the sampled target, computed on
  discovery rows only.
- If |phi| is inside the 2/sqrt(n) band, the block is the overlap floor
  (depth + 1).
- Otherwise the block is `ceil(|phi| / ((1 - |phi|)^2 * 0.05))`. This bounds
  the block null's Bartlett-weight bias for an AR(1) of that |phi|.
- The block is capped so that at least 8 blocks remain, and it is never below
  the overlap floor.

The per-family basis (n, acf1, band, rule, block) is written into the
manifest as `block_basis`. The holdout reuses the frozen discovery block,
under the same cap, and never re-estimates it from holdout labels. A declared
`block` is used as is. At n=60 the cap (7) leaves a residual 7.0%. Short
families therefore stay somewhat anti-conservative, and their p-value
resolution is coarse.

That residual is recorded in the manifest, not only here. A family's
`block_basis` carries a `caveat`, and the manifest's `caveats` list carries
`"<family>: ..."`, in either of two cases:

- the 8-block cap binds, so the block is shorter than the rule wants (the
  caveat cites the 7.0% calibration);
- |acf1| is inside a 2/sqrt(n) band wider than 0.2, i.e. n < 100, where
  dependence of that size cannot be detected and the floor block is used.

A family whose n is below `min_n` has no testable trials and gets no caveat.
When any family is caveated, the payload `method` string ends with `CAVEAT:
data-driven block may be anti-conservative in K of F families ...`. A caveat
is a warning, not a refusal: `candidate_eligible` is unchanged. At n=60 with
AR(1) 0.35, a family is always caveated. Either its acf1 is outside the band
(0.258) and the block it wants exceeds 7, or its acf1 is inside the band.

**Why longer blocks selected more in the post-hoc diagnostic.** The permutation
null variance of a correlation is roughly
(1 + 2 Σ_k w_k ρ_x(k) ρ_y(k)) / n, where w_k is the share of lag-k pairs the
block keeps. Block 1 keeps none, so its null variance is 1/n. The true null
variance keeps every lag. The features here (z60 and overlapping chg20) are
persistent, so ρ_x > 0. The VIX change labels are mean-reverting: acf1 is
-0.176 at fwd5 and -0.096 at fwd20. For those families the true null is
*narrower* than block 1 assumes, so block 1 is conservative. That is the 2.0%
against 4.25% in the table. Longer blocks keep the negative dependence,
tighten the null and select more.

The ledger bears this out. The total rose from 89 to 108, and the three VIX
families account for more than all of the rise: they went from 37 selections
at block 1 to 58 at block 8. VIX fwd5 went 16 → 25 → 28 and VIX fwd20 went
15 → 19 → 22 at blocks 1, 2 and 8. The HY OAS fwd5 family has acf1 = +0.335,
so block 1 was anti-conservative there, and it moved the other way, from 16
to 12. Its strongest p-values were already at the permutation floor and did
not move. "Longer blocks selected more" therefore shows that block 1 was
mis-calibrated in both directions. It is not evidence that the selections are
robust.

## S10: prospective forward log (2026-09-26)

Rules: `docs/paper_log/hypothesis-forward-v1-preregistration.md` (its LF
sha256 is pinned as `research_forward_log.PREREG_SHA256` and written into the
log's header record). Code: `analysis/research_forward_log.py`, CLI
`scripts/research_forward_log.py`, cron wrapper
`deploy/paper_log/hypothesis_forward_v1.sh`. Tests:
`python -m pytest tests/test_research_forward_log.py -q`.

- **Log.** Append-only JSONL, same pattern as the GEX paper log: every record
  carries `prev_sha256` (sha256 of the previous canonical JSON line), the
  code commit and its run time; the first record is a header with the
  pre-registration hash. `verify` walks the chain; `run` and `admit` refuse to
  append to a broken chain, and `STATUS.md` says BROKEN.
- **Tamper evidence, honestly.** The chain alone cannot detect truncation of
  trailing lines or a recompute of the whole file. Every append also appends
  `(records, head_sha256)` to the chained anchor file
  `hypothesis_forward_v1.anchors.jsonl`. `verify` checks every anchored
  prefix locally and against a copy given with `--anchor`. The log is
  tamper-evident only relative to an anchor copy held off-host. `STATUS.md` is
  regenerated from the log and is **not** an anchor. Until an off-host mirror
  exists, the anchor file catches accidents, not a deliberate same-host
  rewrite. The mirror is a follow-up that needs owner approval.
- **One forward test per scientific pair.** Admission is keyed on (target,
  label, horizon, feature series, transform), not on the scan or candidate id.
  A rescan that re-freezes an open or decided pair is refused. Error control:
  Bonferroni 0.05/K within a scan, so P(any false supported verdict) is at
  most 0.05 x admitted scans. `STATUS.md` reports that bound.
- **Consumers (S11).** Each verdict record carries `candidate_id`, the
  scientific `identity`, family, feature, direction, windows, n, rho, p,
  alpha, `prereg_sha256` and `log_head_sha256` (its `prev_sha256`).
- **Code commit and sessions.** `code_sha` has no override: it is the
  archive's `VERSION` or `HEAD` of a clean checkout. Sessions skip US federal
  holidays, the same calendar as the publication lags.
- **Starts empty.** `run` on an empty log writes the header only. Candidates
  enter only through `admit` from one scan directory, all or none. The scan
  must be `latest_vintage_read` on code that descends from the #661 head
  `4506ce48` (checked with `git merge-base --is-ancestor` in a named clone,
  and the scan's recorded file sha256s must equal the files at its commit);
  its summary must rebuild the signed read receipt; every candidate must be a
  selected, holdout-surviving, non-`self_lag` trial in state
  `FORWARD_EVIDENCE_PENDING`; and the candidate set must be exactly the
  holdout survivors (it fixes the Bonferroni family). Synthetic, replay,
  `pit_vintage_read`, `SELF_LAG*`, `RESCAN_REQUIRED` and relabelled scans are
  refused, and the ef0d564b manifest is refused by name.
- **Forward evaluation.** Decisions are business days at 00:00Z strictly after
  the admission (freeze), every `max(step, h)` sessions. A prediction is the
  feature value GRID held at the decision instant (`read_window` with
  `as_of_ts` = the decision, plus the declared publication time), logged
  before its label's publication time; otherwise it is excluded as
  `late_prediction`. An outcome is read only after that publication time.
  The evaluation re-checks all of this from the log for every pair.
- **Stop rule.** One look on the first `min_n` valid pairs: one-sided
  block-permutation test (9,999 perms, seed 20260926, frozen discovery block
  capped at `min_n // 8`), alpha 0.05 / (candidates admitted from the scan).
  `FORWARD_SUPPORTED_REVIEW_REQUIRED`, `FORWARD_FAILED`, or
  `FORWARD_INCONCLUSIVE_STOPPED` after `2 * min_n` decisions. Every state keeps
  `promotion_allowed: false`. `status` shows activity only until the verdict.
- **Operator steps (not automated).** Install the code like the GEX log (an
  immutable `git -c core.autocrlf=false archive` under
  `/data/grid/paper_log/code/<sha>/` with a `VERSION` file), then add one
  crontab line (see the wrapper's header; grid-svr cron runs in UTC). Admit a
  post-#661 scan with `python -m scripts.research_forward_log admit --log-dir
  /data/grid/paper_log/hypothesis_forward_v1 --scan-dir SCAN --repo CLONE`.

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
- An independently reviewed promotion policy. (Prospective prediction logging,
  frozen sample-size and stopping rules and forward scoring exist since S10,
  in code; the grid-svr cron is not installed until the owner approves it.)

Those requirements are intentionally unresolved here. Synthetic mechanism
success and a zero-survivor replay do not authorize live research, learning,
weights or historical rescoring.
