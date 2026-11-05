# VS1 sectors-v6 pre-registration: the ten-sector generalization test bound to v8

This is a new registration. The sectors-v4 body and two-record chain stay immutable; sectors-v5 was never registered. The LF UTF-8 body between the markers is hashed and pinned in `analysis/panel_insider_density_sectors_v6.py`; any change requires a new version. Registration is allowed only while the VS1 v8 witness covers exactly v8's two registration records. Opening waits for a verified, witnessed v8 terminal record.

<!-- PREREG-BODY-START -->

## 0. Custody and boundary

VS1 v8 (body SHA-256 `74679d001564bc29d9f44fda1f288fa935c95ab80585994ed414d8738e0efb25`, two-record registration head `69a7d3276da1fffc10f0ea023151ff283dd0e154b9ee3509495c670ddff42bb5`, witness `05-GRID/Paper-Log/vs1/granular_panel_prereg_v8.anchors.jsonl`) is the sole Technology run. VS1 v7 is a witnessed three-record STOP (head `5d8d7c9c2fc5c943fadc083c347e766586c6137352f609424e60d1e89c0440b4`) and VS1 v6 a witnessed three-record STOP (head `b9d9ab5a3eb82df3d7cd3e5be177cc058b28cb92ea86b7ab124673a43309d284`); neither was opened. Sectors-v4 (body `e3f41ace1bfbfed12c82e16b3b438a62ba759bc1c54dfdf743fb8b2d27b4e712`, head `0512baf5cbae66310130e7219d589e3d992c438dd2ea2ae612903b456da58f5f`) stays a two-record registration that names v6 and can never open. Sectors-v5 was never registered; it names v7 and can never open. Sectors-v6 supersedes sectors-v4 and sectors-v5 for the ten-sector generalization test only.

Sectors-v6 is registered and witnessed while the v8 witness covers exactly v8's two registration records, as v8's own body section 0 requires; a sectors-v6 registration attempted after any later v8 record (`inputs_frozen`, a STOP or `discovery_opened`) refuses. Its registration records link the v8 body, the exact v8 registration head and witness path, both STOP heads, the sectors-v4 body and head, the per-sector Stage-0 settings and the GateSpec v1 hash. V1–v5 Technology and sectors-v2–v4 witnesses stay at exactly two records; v6 and v7 at exactly three.

## 1. Question and family

The question and directional hypothesis are those of sectors-v4: does point-in-time Form 4 distinct insider-buyer density have a positive cross-sectional relationship with the issuer's forward sector-ETF-relative return outside Technology? H1 is a positive mean cross-sectional rank IC in the published direction (Lakonishok and Lee 2001; Jeng, Metrick and Zeckhauser 2003; Cohen, Malloy and Pomorski 2012). The ten non-Technology sectors, their SIC-range membership, the sector-map tie rules, the benchmark ETFs and the XLRE/XLC pre-inception benchmark rule, the Form 4 event rules, `known_at` convention, Section 16 activity rule, C1 ticker interval, the A30/A90 features, the 5- and 20-session horizons, horizon-spaced decision dates, split-first labels, the Spearman rank IC and the block sign-flip null all inherit sectors-v4 and VS1 v8 exactly. No sector is pooled into Technology or into another sector.

## 2. Windows and admission

Windows are v8's: discovery `[2008-01-01, 2020-01-01)` and holdout `[2020-01-01, 2026-07-01)`. Each sector needs its own fresh non-outcome admission and basis probe over 2007-11-02 through 2019-12-31 inclusive, under the v6/v8 section 2.3 price-admission rules with the sector ETF as benchmark (TIINGO source id 524 split- and dividend-adjusted closes, successful selected vintages only, zero unexplained calendar gaps, split consistency, pull-batch splice tolerance, Tiingo metadata entity and listing checks, the TwelveData `adjust=all`/`adjust=none` agreement thresholds). No v6, v7, v8 or sectors-v4 admission is carried forward. No synthetic or substituted series may fill a missing price. The holdout-period basis report runs only at that sector's holdout opening.

## 3. Confirmatory design

1. **One confirmatory test per sector.** In each sector `A90|fwd5` is the sole confirmatory trial: the unchanged block sign-flip on the per-date rank IC, one-sided in the positive direction. `A30|fwd5`, `A90|fwd20` and `A30|fwd20` are exploratory in every sector: computed and reported with two-sided, Holm- and BH-adjusted p across the sector's four trials, never selected, never counted. They still enter the unchanged calibration and MACHINERY_SUSPECT rules.
2. **Per-sector Stage-0.** On each sector's final admitted panel, power of the confirmatory test at planted rank IC 0.01 and one-sided alpha 0.05 must reach at least 0.50 under both the v1 Gaussian planted-IC simulator (200 simulations, 999 sign flips, seed 20260927) and the E0 v1 `factor_t_garch_exposed` model (500 simulations, 999 sign flips, E0 seeds, E0 manifest `75489d5091d82af64312f4523c41bdb52b0951a11e017644a022baa08a172822` verified at run time). A sector below 0.50 on either model is untestable: it is reported, never opened, and counts as a non-survivor. The first sealed Stage-0 per sector binds.
3. **Per-sector discovery and holdout.** Each testable sector's discovery is reported (mean IC, one-sided p, the four-trial calibration state). Each testable sector's confirmatory trial is then evaluated once on the holdout, regardless of its discovery result. A sector **survives** when its holdout mean IC is positive and its one-sided holdout p is below 0.10; this is exactly GateSpec v1's survival rule. Discovery calibration (CONTRARY, MACHINERY_SUSPECT) does not change survival or the gate; any sector whose discovery calibration is CONTRARY is named in the family report as a machinery alarm that the owner review must address.
4. **Untestable sectors.** A sector is untestable, after exactly one attempt and with no retry, when its admission probe, benchmark admission or vendor coverage fails, or when its Stage-0 is below 0.50 on either gated model. Each untestable sector gets a witnessed terminal `stage0_untestable` record in the sectors-v6 chain and is a non-survivor.
5. **Family claim.** No per-sector result is a family claim. The family-level claim "general" is decided only by `analysis.generalization_gate.evaluate_gate` under GD10b `GateSpec` v1 (spec SHA-256 `fa2fa2bd1b1ef5cb81168f393135795824991e08829b60f66a109decdf401174`), passed as the expected spec hash and computed once, after v8 and every sector are terminal and witnessed. Where this summary and GateSpec v1 differ, GateSpec v1 governs:
   - **Breadth:** survivors (rule 3) must reach max(k_binomial, k_permutation): k_binomial is 4 of 11 (P(X ≥ 4 | 11, 0.10) = 0.0185), the smallest count with binomial chance at most 0.05; k_permutation is the smallest count whose rate is at most 0.05 under GateSpec v1's centred joint block sign-flip of the sealed per-date holdout IC series on the union decision grid.
   - **v8 STOP branch, pre-declared:** if v8 ends in a witnessed STOP, Technology enters as terminal kind `stop` and the gate runs over the ten sectors with threshold 4 of 10 (P(X ≥ 4 | 10, 0.10) = 0.0128) and the same permutation rule.
   - **No dominance:** leave-one-sector-out pooled IC keeps the positive sign with p < 0.05 by GateSpec v1's cluster-robust date-block t, and within every surviving sector the top entity contributes less than 25% of the IC sum.
   - **Forward:** `FORWARD_SUPPORTED_REVIEW_REQUIRED` in at least 2 surviving sectors. **Coverage honesty:** survivors re-survive on the coverage-stable IC series.
   - **Inputs.** All eleven `SectorResult`s carry this body's SHA-256 as their prereg hash and direction +1. Technology's result is derived deterministically from v8's witnessed terminal record: its terminal record hash, kind `holdout_result` or `stop`, and for a holdout the sealed one-sided holdout p, per-date IC series and entity contributions of v8's confirmatory `A90|fwd5` holdout check. Survival therefore uses rule 3, not v8's own selection rule; v8's verdict is reported beside it. Any field not sealed by v8 may be recomputed only from v8's frozen holdout inputs, recorded as a `prices_read` in the sectors-v6 chain before the read, and only if the recomputed mean IC and one-sided p equal v8's sealed values; otherwise, or if v8 is contaminated, superseded or not terminal, the gate is not run and only per-sector results are reported.
   - Verdicts are GateSpec v1's. `promotion_allowed=false` throughout.

## 4. Design evidence and disclosures (non-outcome only)

Per-sector synthetic power of the confirmatory test was screened on SEC Form 3/4/5 feature geometry only: the sectors-v2 universe construction over the filings data, the v6 feature and admission rules, the 2008-01-01 start, planted IC 0.01, one-sided 0.05, the v1 Gaussian simulator (200 simulations) and the E0 v1 generator (500 simulations). Price admission is unknown before the probes, so seeded random issuer losses of 35% and 50% were also screened.

Each cell gives v1 Gaussian / E0 `factor_t_garch_exposed` (gated) / E0 `factor_t_garch` power.

| Sector | Filings issuers | Median issuers per date | All issuers | 35% lost | 50% lost |
| --- | ---: | ---: | --- | --- | --- |
| Healthcare | 1110 | 206 | 0.975 / 0.932 / 0.974 | 0.900 / 0.844 / 0.888 | 0.805 / 0.764 / 0.814 |
| Financials | 832 | 331 | 1.000 / 0.972 / 0.996 | 0.985 / 0.934 / 0.984 | 0.945 / 0.876 / 0.934 |
| Industrials | 832 | 259 | 0.995 / 0.960 / 0.998 | 0.960 / 0.868 / 0.942 | 0.870 / 0.844 / 0.852 |
| Consumer Discretionary | 585 | 191 | 0.960 / 0.934 / 0.948 | 0.895 / 0.838 / 0.880 | 0.810 / 0.780 / 0.806 |
| Materials | 316 | 113 | 0.830 / 0.806 / 0.816 | 0.625 / 0.596 / 0.596 | 0.600 / 0.556 / 0.536 |
| Real Estate | 286 | 125 | 0.800 / 0.802 / 0.842 | 0.680 / 0.650 / 0.690 | 0.585 / 0.540 / 0.562 |
| Consumer Staples | 246 | 92 | 0.775 / 0.740 / 0.776 | 0.605 / 0.582 / 0.584 | 0.575 / 0.518 / 0.498 |
| Energy | 224 | 72 | 0.730 / 0.612 / 0.602 | 0.570 / 0.468 / 0.484 | 0.485 / 0.432 / 0.462 |
| Utilities | 128 | 60 | 0.555 / 0.602 / 0.578 | 0.420 / 0.438 / 0.468 | 0.325 / 0.356 / 0.348 |
| Communication Services | 170 | 41 | 0.455 / 0.456 / 0.470 | 0.370 / 0.316 / 0.360 | 0.220 / 0.210 / 0.190 |

E0 null size of the one-sided 0.05 test with all issuers was 0.036–0.060 across sectors. On these estimates four large sectors stay above 0.75 on both gated models even at 50% admission loss; Materials, Real Estate, Consumer Staples and Energy are near the 0.50 gate after plausible losses; Utilities and Communication Services are likely untestable. The sizing receipt is `/data/sec/sectors6_design/sizing_run1.json` (SHA-256 `ae5570801f25083aa3a8e62fc3a9d3d81d8b0cad8353aa1eb26ebbe8ea22a844`, script `4c87763d3adde64819fefe739cbf53ea49a9b2e538e4f3cbcc674733a1ecc2dd`).

These estimates explain why the per-sector Stage-0 is gated rather than assumed: small sectors may be untestable after admission. They are not price admission and not evidence of edge.

Disclosures: E0 v1's known-effect replication read non-Technology TIINGO closes before 2011 for weekly and one-month reversal and 12-1 momentum; these are not insider-density labels and not this construct, and no design choice here used them. Sectors-v2 through sectors-v4 were registered and never opened. No VS1 or sectors registry has read a non-Technology sector-relative outcome in any VS1 window. The design was fixed from the screen above, SEC feature geometry, and the v8 body.

## 5. One-shot custody and openings

Register exactly one header and one preregistration record in `granular_panel_prereg_sectors_v6.jsonl` while v8 is at exactly its two registration records; publish its exact anchor at `05-GRID/Paper-Log/vs1/granular_panel_prereg_sectors_v6.anchors.jsonl` on vault `main` before any later v8 record (`inputs_frozen`, a STOP or `discovery_opened`) is witnessed, and verify it off-host. The joint-run harness must confirm from vault-main history that in the first commit carrying the sectors-v6 anchor the v8 witness was exactly v8's registration anchor, and refuses otherwise. Registration opens nothing.

Before any sector probe, Stage-0, freeze or opening, a separately reviewed sectors-v6 joint-run harness must verify at one vault-main tip: v8's terminal record (its witnessed STOP status or its witnessed `holdout_result`, each matched against the local chain and the anchor); v6 and v7 at exactly their three-record STOPs; v1–v5 and sectors-v2–v4 at exactly two records; sectors-v6 at exactly its two registration records. Because v8's own checks refuse once sectors-v6 grows, the harness records that v8 terminal proof in the sectors-v6 chain before its first opening. Each sector then follows its own one-shot freeze, discovery-opening, witness, holdout-opening and witness steps. An unknown witness, any older registry growth, an unverified v8 terminal record, a changed body or code pin, a window mismatch, or a sector below its Stage-0 gate refuses. Nothing here authorizes a price read, promotion or trading.

<!-- PREREG-BODY-END -->

The GateSpec v1 hash is filled from GD10b PR #784 and the v8 registration head from the witnessed v8 registration (vault main d83c8594). The body SHA-256 is pinned in code; the sectors-v6 registry head and merged code SHA are pinned only after registration and independent verification. They remain fail-closed placeholders until then.
