# VS1 v8 pre-registration: realistic-power confirmatory design

This document is written before any VS1 v8 discovery or holdout outcome is opened, and before any v7 Stage-0, discovery or holdout record existed. The UTF-8 LF body strictly between the markers is immutable once registered. Its SHA-256 is pinned in `analysis/panel_insider_density_v8.py` and the two-record v8 registry. Any change to this body requires a new version, registry, and off-host witness. Promotion is prohibited.

<!-- PREREG-BODY-START -->

## 0. Custody and prior evidence

VS1 v7 was registered and witnessed with two records and is stopped unopened. Its terminal `STOP_SUPERSEDED_BY_V8_UNOPENED` record is bound to the exact bytes of the E0 v1 machinery-calibration scorecard (SHA-256 `4b47f918433524d4ab36b93e6472ee557e6617bbfe7567e5c182d4834a7cd62a`, E0 manifest `75489d5091d82af64312f4523c41bdb52b0951a11e017644a022baa08a172822`). On the registered v7 design (A90|fwd5, 414 dates, 202 issuers, raw threshold 0.0125) E0's synthetic outcome models gave power 0.496 (Gaussian), 0.446 (market plus three AR(1) factors, GARCH(1,1), Student-t(4) tails) and 0.414 (the same plus a 0.3 style tilt of factor-1 loadings toward insider-buy propensity) at planted rank IC 0.01, below the 0.50 gate. v7's post-admission price probe, Stage-0, freeze, discovery and holdout never ran. The v7 terminal head, record count and off-host vault-main witness must be verified before v8 registration; the head is bound into v8 code and registry after witnessing. The v7 STOP contains no v8 hash.

V1–v5 Technology and sectors-v2–v4 registration witnesses must each remain at exactly two records; v6 at exactly its three-record STOP; v7 at exactly its three-record STOP. Sectors-v5 was never registered; it names v7 and can never open. Any later multi-sector generalization needs a new sectors registration linked to v8.

## 1. Design changes from v7 and the order they were chosen

All three changes were chosen from non-outcome information only: admission reasons, coverage, sample sizes and synthetic power. No actual forward return, label, rank IC, discovery or holdout result of any VS1 version was read.

1. **Discovery start 2008-01-01.** The discovery interval is `[2008-01-01, 2020-01-01)`. The holdout stays `[2020-01-01, 2026-07-01)`. The selection rule, fixed before the screen was read, was: the latest first-of-month start whose tilted-model power in the screen below reaches 0.80 with margin, and whose 730-day point-in-time Section 16 activity lookback lies inside the SEC Form 3/4/5 structured data (filings from 2006-01-03; 2006-01-02 was a market holiday). 2009-01-01 sits exactly at 0.800 with no margin for admission loss; 2008-01-01 is the earliest start with a complete activity lookback. Earlier starts (2007-01, 2006-04) show realistic-model powers within Monte Carlo error of 2008-01 (standard error about 0.016) but with a truncated activity lookback, so they were not chosen.
2. **One confirmatory test.** `A90|fwd5` is the sole confirmatory trial. H1 is a positive mean cross-sectional rank IC, the direction of the published prior that open-market insider purchases precede positive abnormal returns (Lakonishok and Lee 2001; Jeng, Metrick and Zeckhauser 2003; Cohen, Malloy and Pomorski 2012) and of every earlier VS1 version's pre-registered direction. The discovery test is the unchanged block sign-flip on the per-date rank IC, one-sided in the pre-registered positive direction, at alpha 0.05. The trial is selected when it is testable, its mean IC is positive and its one-sided p is at most 0.05. `A30|fwd5`, `A90|fwd20` and `A30|fwd20` are exploratory: they are computed, reported with their two-sided p, Holm- and BH-adjusted p, and are never selected, never gated and never evaluated as holdout survivors.
3. **Stage-0 gate on two outcome models.** Post-admission Stage-0 power is computed for the confirmatory test on the final v8 admitted Technology panel at planted rank IC 0.01 under both (a) the v1 Stage-0 Gaussian idiosyncratic model (200 simulations, 999 sign flips, seed 20260927) and (b) the E0 v1 `factor_t_garch_exposed` model (500 simulations, 999 sign flips, E0 seeds, E0 manifest `75489d50…` verified at run time, production machinery). **Both must be at least 0.50.** The design target is 0.80 under (b); it is reported, not a gate. E0 `factor_t_garch` power is reported as a sensitivity. The v1 four-trial table at the old raw threshold is reported for continuity only.

The null size of the one-sided 0.05 test in the design screen (500 null worlds each) at the selected 2008-01-01 start was 0.036 (Gaussian), 0.046 (factor) and 0.054 (tilted); across all screened starts it ranged 0.036–0.072, the top of that range about 2.3 standard errors above nominal. E0 v1 also found slight over-rejection (0.066 at nominal 0.05, two-sided) under the style tilt, which inflates rather than deflates tilted power; the tilted model gates because it is the most pessimistic realistic model for power, not because it corrects size. The screen also computed two-sided 0.0125, 0.025 and 0.05 and one-sided 0.025 rates; most of the gain over v7 comes from the one-sided 0.05 confirmatory test (0.496 to 0.818 E0 Gaussian on the v7 geometry), and the earlier start adds the rest.

## 2. Design evidence (synthetic outcomes only)

Feature geometry: the v6 price-admitted 202 Technology issuers held fixed (a design assumption; the v8 gate uses the new v8 admission), E0 v1 generator, 500 simulations, 999 flips, planted IC 0.01, `A90|fwd5`.

| Start | Usable dates | E0 Gaussian raw 0.0125 | E0 Gaussian 1-sided 0.05 | E0 factor+GARCH+t 1-sided 0.05 | E0 + style tilt 1-sided 0.05 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2011-10-01 (v7) | 414 | 0.496 | 0.818 | 0.780 | 0.730 |
| 2009-01-01 | 552 | 0.598 | 0.866 | 0.858 | 0.800 |
| **2008-01-01 (selected)** | **603** | 0.628 | **0.886** | 0.850 | **0.850** |
| 2007-01-01 | 653 | 0.634 | 0.884 | 0.896 | 0.844 |
| 2006-04-01 | 690 | 0.656 | 0.902 | 0.902 | 0.866 |

Admission-loss stress at 2008-01-01, seeded random removal of admitted issuers (one-sided 0.05): 15% removed (172 issuers) gives E0 Gaussian 0.832 and tilted 0.820; 30% removed (141 issuers) gives 0.756 and 0.754. The 2011-10-01 row reproduces E0's published v7 cross-check exactly (0.496 Gaussian, 0.446 factor, 0.414 tilted at raw 0.0125). The gated Gaussian model (a) is the v1 planted-IC simulator, which is slightly more optimistic than E0's Gaussian (0.535 versus 0.496 on the v7 design). These are design estimates, not price admission and not evidence of predictive edge.

## 3. Question, universe, features and admission (unchanged)

The v6/v7 question and directional hypothesis remain: does the count of distinct open-market Form 4 insider buyers in an SIC-expanded Technology issuer predict a positive forward issuer return relative to XLK? Technology is the sole sector. Every v6 §2.1 Form 4 event rule, known-at convention, Section 16 activity rule, issuer/SIC/sector-map pin, 782-candidate construction, current ticker tie rule, point-in-time admission rule, C1 ticker interval, feature definition, horizon-spaced decision grid, split-first labels and XLK-relative forward returns are unchanged.

Price admission remains exactly v6 §2.3 (TIINGO source id 524 split- and dividend-adjusted closes, XLK benchmark, successful selected vintages only, zero unexplained calendar gaps, split consistency, pull-batch splice tolerance, Tiingo metadata entity and listing checks, the same per-ticker TwelveData `adjust=all`/`adjust=none` agreement thresholds). The earlier start requires a fresh non-outcome admission and basis probe spanning **2007-11-02 through 2019-12-31 inclusive** (60 calendar days of warm-up), with TwelveData and Tiingo metadata fetched or validated for that exact span. The v6 and v7 vendor receipts are not v8 admission. No synthetic or substituted series may fill a missing price.

## 4. Testing, holdout and verdict

Discovery: the confirmatory rule in §1.2. Exploratory trials use the unchanged statistic and are reported only; they are never selected, but they still enter the unchanged calibration (CONTRARY, CONSISTENT, WEAK_POSITIVE, ABSENT) and MACHINERY_SUSPECT rules through the Holm-adjusted negative one-sided test over all four trials, exactly as in v6/v7. No later version may make an exploratory trial confirmatory on any part of this discovery window. Holdout: only the confirmatory trial can be a selected trial; it is evaluated on the unchanged holdout with the unchanged frozen-selection Bonferroni rule (one selected trial, two-sided p at most 0.05 with the discovery sign). The primary is always reported on the holdout. Results are research only and `promotion_allowed=false`.

## 5. Binding order

1. Verify the v7 terminal STOP chain and vault-main anchor; finalize this body and reviewed merged code SHA; create exactly one v8 header and one preregistration record linking the v7 body, terminal head, record count, witness path and earlier pins. Publish and independently verify the v8 anchor on vault `main` before any probe or power.
2. Run the 2007-11-02..2019-12-31 non-outcome admission probe with v6 rules. Any missing exact-window report, failed identity/basis/source check or unavailable vendor span excludes that ticker. No authentic recovery is presumed.
3. Compute the §1.3 Stage-0 once on the final v8 admitted panel of the single binding probe; that first sealed Stage-0 binds and may not be recomputed on another manifest. If either gated model is below 0.50, append and witness a STOP and do not freeze or open. There is no `accept_underpowered` override. The E0 model is E0 v1 only (manifest `75489d50…` at the registered code); any other manifest refuses the run (a refusal, not a STOP).
4. If both pass, freeze exact SEC files, admission manifest, probe and cross-check receipts, the Stage-0 result, code SHA and snapshot. Append and witness `discovery_opened` before any discovery price read; seal and witness the discovery; require explicit holdout authorization and a fresh holdout-period basis report before `holdout_opened`; witness the holdout opening before any holdout price read; evaluate once and witness the terminal result.

An unknown witness, older registry growth, unverified v7 STOP, v8 registration fork, missing off-host anchor, changed body or code pin, window mismatch, underpowered Stage-0 or contaminated opening refuses.

<!-- PREREG-BODY-END -->

Body SHA-256: computed from the LF body and pinned in the v8 code at review. The v7 terminal head and registration pins remain fail-closed until independently witnessed and bound.
