# VS1 sectors-v5 pre-registration: generalization after v7

This is a new registration. The sectors-v4 body and its two-record chain remain immutable. The LF UTF-8 body between markers is hashed and pinned in `analysis/panel_insider_density_sectors_v5.py`; any change requires a new version. Registration is allowed only after the v7 two-record registration is verified on vault `main`. Opening waits for a witnessed v7 terminal `holdout_result`.

<!-- PREREG-BODY-START -->

## 0. Dependencies and boundary

VS1 v6 stopped without discovery or holdout opening. VS1 v7 is the Technology run and begins discovery on 2011-10-01, with its holdout fixed at 2020-01-01 through 2026-06-30. Sectors-v4 is registered but cannot be repointed because its immutable body binds v6 in §§2.2, 7 and 12. Sectors-v5 supersedes sectors-v4 only for a future generalization test. It has its own body, registry, off-host anchor and code pins. Neither a passing v7 Stage-0 gate nor frozen v7 inputs permits sectors-v5 opening. The exact v7 terminal `holdout_result` must be witnessed in the canonical vault-main anchor first.

The sectors-v5 registration records the v7 preregistration body hash, exact two-record v7 registration head and witness path, v6 terminal STOP head/count/witness path, and sectors-v4 body/head. Until those hashes and the reviewed merged code SHA are bound, registration refuses. An unknown witness or growth of any older frozen registry refuses.

## 1. Question and run family

The generalization question and directional H1/H0 are those of sectors-v4: does the point-in-time Form 4 distinct insider-buyer density have a positive cross-sectional relationship with the issuer's forward sector-ETF-relative return outside Technology? The ten non-Technology sectors remain separate. Their membership maps and benchmark ETFs, universe construction, event and price admission rules, feature definitions, source/basis constraints, C1 ticker interval, TwelveData checks, exclusions, split-first labels, 5- and 20-session horizons, rank IC statistic, sign-flip null, sensitivity analyses and reporting inherit sectors-v4 exactly. No sector is pooled into Technology, and no change to the v7 Technology admission is inferred.

Each of the ten sectors has the same four trials and `A90|fwd5` 5-session primary as sectors-v4. The other three trials stay secondary. The generalization family uses ledger run k=2, alpha=0.10/6, with Holm across all declared sector trials and untestable p=1. Holdout checks retain the frozen-selection Bonferroni and same-sign requirements. Technology v7 is the eleventh sector only in the eventual generalization gate; its result is linked by its exact witnessed registry result and is never rerun or folded into the ten-sector discovery family. The discovery/holdout split and latest end are those of the v7 Technology run: `[2011-10-01, 2020-01-01)` and `[2020-01-01, 2026-07-01)`. For each non-Technology sector, a new exact earlier-window admission probe and post-admission Stage-0 are required before its own freeze; no v6 or sectors-v4 probe admission is silently carried forward.

## 2. One-shot custody

Register exactly one header and one preregistration record in `granular_panel_prereg_sectors_v5.jsonl`, then publish and verify its exact anchor at `05-GRID/Paper-Log/vs1/granular_panel_prereg_sectors_v5.anchors.jsonl` on vault `main`. The registration does not open a price window. Before any later `open-discovery`, verify at one vault-main tip: v6 exact three-record terminal STOP; v1–v5 and sectors-v2–v4 at two records; sectors-v5 at its exact two-record registration; and the v7 chain ending in an exact witnessed `holdout_result`. A v7 power pass, frozen inputs, discovery result, or unanchored holdout result is insufficient. A changed v7 body/head, unknown witness, later growth, or missing canonical anchor refuses.

Any future opening must use a separately reviewed sectors-v5 joint-run harness and explicit one-shot freeze/open/witness steps. This registration alone does not authorize a sector probe, price read, outcome calculation, promotion or trading. `promotion_allowed=false` throughout.

<!-- PREREG-BODY-END -->

Body SHA-256, v7 registration head, sectors-v5 registry head and merged code SHA are pinned only after independent verification. They remain fail-closed placeholders until then.
