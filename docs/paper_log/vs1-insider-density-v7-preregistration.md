# VS1 v7 pre-registration: first-of-month discovery extension

This document is written before any VS1 v7 discovery or holdout outcome is opened. The UTF-8 LF body strictly between the markers is immutable once registered. Its SHA-256 is pinned in `analysis/panel_insider_density_v7.py` and the two-record v7 registry. Any change to this body requires a new version, registry, and off-host witness. Promotion is prohibited.

<!-- PREREG-BODY-START -->

## 0. Custody and prior evidence

VS1 v6 is stopped and superseded by v7 before any v6 discovery or holdout opening. Its sealed post-admission Stage-0 synthetic power was 0.480 for planted rank IC 0.01 on `A90|fwd5`, below its 0.50 gate. A v6 price basis and coverage probe occurred; it read no event-aligned return, forward label, discovery result, or holdout result. V6's terminal `STOP_FOR_OWNER_SUPERSEDED_BY_V7_UNOPENED` record, immutable power receipt, exact registry head/count, and off-host vault-main witness must be verified before v7 registration. The terminal head is bound into v7 code and registry after witnessing; an unknown or extra v6 record refuses. The v6 STOP does not contain a v7 hash.

V1–v5 Technology and sectors-v2–v4 registration witnesses must each remain at exactly two records. The sectors-v4 body remains immutable. A distinct sectors-v5 registration can be witnessed after v7 registration; it cannot open until v7 has a witnessed terminal `holdout_result`.

## 1. Owner-selected change and selection order

The sole design change from v6 is the discovery start, from 2012-01-01 to **2011-10-01** UTC. The discovery interval is `[2011-10-01, 2020-01-01)`. The holdout remains `[2020-01-01, 2026-07-01)`, or 2020-01-01 through 2026-06-30 inclusive. No outcome from either interval was inspected to choose this date.

The owner considered (a) authentic recovery of ten `calendar_gaps`-only tickers, then (b) a first-of-month extension at fixed current admission. No ticker had an authentic recovery, so (a) did not change admission. The ten-name simulation is a hypothetical upper bound, not an admission decision. For (b), the candidate starts were screened in descending monthly order and stopped at the latest start with simulated power at least 0.50: 2011-12 failed, 2011-11 failed, 2011-10 passed. This is the smallest passing extension on the first-of-month grid tested. The 0.80 power aim was not attained and does not replace the binding 0.50 gate.

Every candidate below uses feature-only synthetic outcomes with the current 202 v6 price-admitted Technology issuers unless the row explicitly says hypothetical 212. All use `A90|fwd5`, planted rank IC 0.01, 200 simulations, 999 sign flips, seed 20260927, Holm threshold 0.0125, four trials and fixed holdout. These are design estimates, not price admission for the earlier interval, nor evidence of predictive edge. The noise model is optimistic; the 0.535 estimate is 107/200 hits, just seven over the gate.

| Scenario | Discovery start | Issuers assumed | Usable 5-session dates | Synthetic power |
| --- | --- | ---: | ---: | ---: |
| v6 reproduced | 2012-01-01 | 202 | 402 | 0.480 |
| all ten gaps-only names, hypothetical | 2012-01-01 | 212 | 402 | 0.535 |
| fixed v6 admission | 2011-12-01 | 202 | 406 | 0.425 |
| fixed v6 admission | 2011-11-01 | 202 | 410 | 0.480 |
| **selected fixed v6 admission** | **2011-10-01** | **202** | **414** | **0.535** |
| fixed v6 admission sensitivity | 2011-01-01 | 202 | 452 | 0.485 |
| fixed v6 admission sensitivity | 2010-01-01 | 202 | 502 | 0.490 |
| fixed v6 admission sensitivity | 2009-12-01 | 202 | 506 | 0.550 |
| fixed v6 admission sensitivity | 2009-11-01 | 202 | 510 | 0.605 |
| fixed v6 admission sensitivity | 2009-10-01 | 202 | 514 | 0.610 |
| fixed v6 admission sensitivity | 2009-07-01 | 202 | 527 | 0.625 |
| fixed v6 admission sensitivity | 2009-04-01 | 202 | 540 | 0.575 |
| fixed v6 admission sensitivity | 2009-01-01 | 202 | 552 | 0.615 |
| fixed v6 admission sensitivity | 2008-01-01 | 202 | 603 | 0.620 |
| fixed v6 admission sensitivity | 2007-01-01 | 202 | 653 | 0.600 |
| fixed v6 admission sensitivity | 2006-01-01 | 202 | 700 | 0.690 |

The larger sensitivity starts were inspected only to characterize the design; their nonmonotone estimates do not alter the selected latest passing monthly start. The design receipts and input hashes are recorded in `VS1-V7-POWER-DESIGN-20260929.md`. This table contains no actual return, label, discovery IC, or holdout outcome.

## 2. Question, universe and admission

The v6 question and directional hypothesis remain: does the count of distinct open-market Form 4 insider buyers in an SIC-expanded Technology issuer predict a positive forward issuer return relative to XLK? H1 is positive rank IC; H0 is mean cross-sectional rank IC zero. Technology is the sole sector in this run.

Every v6 §2.1 Form 4 event rule, known-at convention, Section 16 activity rule, issuer/SIC/sector-map pin, 782-candidate construction, current ticker tie rule, and point-in-time admission rule remains binding. The C1 same-security ticker interval gates both features and prices. The issuer's Form 4 history and Section 16 activity are evaluated at each decision instant. The new interval changes which decision instants exist; it does not backfill knowledge or loosen any admission rule.

Price admission remains exactly v6 §2.3: TIINGO `YF:{ticker}:adj_close` with split and dividend adjustment, source id 524; XLK benchmark; selected successful vintages only; no QUARANTINED or other source admitted; zero unexplained calendar gaps; split consistency; pull-batch splice tolerance; Tiingo `startDate` versus SEC identity and listing checks; the C1 ticker-interval bound; and the same per-ticker TwelveData `adjust=all`/`adjust=none` return agreement thresholds (at least 250 comparable pairs, 99% within 10 bps, excluded adjustment-date share at most 10%, relative factor-move threshold 1 bp). The v6 exclusion and abstention rules, low-price handling, missing labels and delisting sensitivities are unchanged. No synthetic or substituted series may fill a missing price.

The earlier discovery start requires a fresh non-outcome admission and basis probe spanning **2011-08-02 through 2019-12-31 inclusive** (60 calendar days of warm-up before 2011-10-01). The v6 probe began 2011-11-02 and cannot be reused as v7 admission. Fetch/validate TwelveData and Tiingo metadata for the exact earlier span and preserve source, snapshot, pull vintage and per-ticker reasons. The 202 current admitted issuers are a synthetic design assumption only. The actual v7 Stage-0 panel must be recomputed from the newly admitted set. The holdout-period basis report remains deferred until `open-holdout` and uses the unchanged 2020-01-01 through 2026-06-30 interval. A basis/coverage probe is not a discovery or holdout opening.

## 3. Trials, statistic and testing

The same four v6 trials, with `A90|fwd5` the sole primary and the other three secondary, run only in Technology. The feature, 5- and 20-session horizons, horizon-spaced decision dates, split-first labels, XLK-relative forward returns, Spearman rank IC, block sign-flip null, 999 permutation settings, sensitivity nulls, Holm selection and reported BH values are unchanged. The split purges any label crossing a window boundary; discovery reads no price on or after 2020-01-01. Run k=1 has alpha 0.05 and the four-trial Holm first-step threshold 0.0125. Holdout uses the v6 frozen-selection Bonferroni and same-sign rules. All untestable trials remain in multiplicity with p=1. Results are research only and `promotion_allowed=false`.

## 4. Binding Stage-0 and one-shot order

1. Verify the exact v6 terminal STOP chain and vault-main anchor. Finalize v7 body and reviewed merged code SHA; create exactly one v7 header and one preregistration record, with parent v6 body, terminal head/count/witness path and earlier v1–v5 pins. Publish and independently verify the v7 anchor on vault `main` before probe or power.
2. Run the new 2011-08-02..2019-12-31 non-outcome admission probe with v6 rules. Any missing exact-window report, failed identity/basis/source check, or unavailable vendor span excludes that ticker. No authentic recovery is presumed.
3. Recompute post-admission Stage-0 synthetic power on the final v7 admitted Technology panel, using the four trials, 200 simulations, 999 sign flips, seed 20260927. **The raw `A90|fwd5` power at planted IC 0.01 must be at least 0.50.** If it is below 0.50, append and witness STOP, return to owner, and do not freeze or open. Do not use an `accept_underpowered` override.
4. If the gate passes, freeze exact SEC files, admission manifest/probe and cross-check receipts, synthetic power result, code SHA and snapshot. Append/witness `discovery_opened` before any discovery price read; read only the bounded admitted TIINGO prices. Seal and witness the frozen discovery, then require explicit holdout authorization and a fresh holdout-period basis report before `holdout_opened`. Append/witness the holdout opening before any holdout price read. Evaluate the one shot holdout and witness its terminal result.

An unknown witness, older registry growth, unverified v6 STOP, v7 registration fork, missing off-host anchor, changed body/code pin, early/late window mismatch, underpowered Stage-0, or contaminated opening refuses. No v6 discovery or holdout may be resumed. Sectors-v5 may register after v7's witnessed registration, but may not open until an exact witnessed v7 `holdout_result` exists.

<!-- PREREG-BODY-END -->

Body SHA-256: computed from the LF body and pinned in the v7 code at review. Registration head, v6 STOP terminal head and merged code SHA remain fail-closed until independently witnessed and bound.
