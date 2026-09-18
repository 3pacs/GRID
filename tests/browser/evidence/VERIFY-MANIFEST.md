# Verification-tree manifest (2026-09-18)

The browser-acceptance verification trees are local compositions; they are never pushed as
integration branches. Rebuild any of them from this recipe, or check out the named local ref
`fable/verify-20260918` in the shared GRID repo (points at the latest tree below).

| Tree | SHA | Recipe |
|---|---|---|
| Recovered baseline | `a2f71acfac567a77b1b5ff341b52c9bd96997cf4` | origin/main after #548 |
| Composition h (incident lead) | `8be08c036dec10405dec9b02a373bdb227fdc49a` | a2f71acf + merges in order 721992fd (#536), 4094c50b (#535), 8e6d1729 (#534), a043c83a (#538), ce1a7f55 (#545 rebased), 351e9ca2 (#547 rebased), ac2551bb (#546 rebased stack top: #541 588b15c1 → #540 7ac4bc6f → #542 9f667f16 → #539 5e30f82e → #544 d7cc5756 → #537 cda5ca60 → #546); the last merge conflicts in 10 files, each resolved to the version in g `44019a439c4697b860c5b571fc821acf1ba76b83`. Check: `git diff --stat 44019a43 8be08c03` = exactly `earnings_pred_move_basis_0918.py` (4 lines) + `god_view_market_tables_20260918.py` (+260). |
| d0257b03 | `d0257b03f3935c3cb519294ef782a0af1cc3c785` | h + `git cherry-pick -x` a691d9b0 (#557), a6526b12 (#559), 2b889ad2 (#555), e539c700 (#555) |
| a8f03a47 | `a8f03a47c32d90599b08f02beee03370242b7550` | d0257b03 + cherry-pick f2dea63f (#555) |

Each cherry-pick carries `(cherry picked from commit …)` in its message, so `git log` on the tree
reproduces the recipe. Evidence directories name the tree they were captured on; from runner
`262c6390` onward each attempt writes into its own `run-<UTC timestamp>` subdirectory so a failed
attempt cannot contaminate a later verdict.
