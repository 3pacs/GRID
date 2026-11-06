# Forward edge report — 2026-11-06

Rule: `prune_rule_v1`, pre-registered in `docs/research/prune-rule-v1-preregistration.md` before these numbers were computed. Generated 2026-11-06T02:13:39+00:00 (read-only, production). Reproduce: `python scripts/edge_report.py report` (see `--help`).

Baseline 0.50 (a coin flip on direction). A unit needs N_eff ≥ 30 and ≥ 20 dates for a verdict. EDGE / ANTI_EDGE need the clustered Wilson 95% CI to exclude 0.5 **and** a Benjamini–Hochberg p < 0.05 (circular block bootstrap, B = 1999, BH within each family). Hit = direction right. Signals: CORRECT/(CORRECT+WRONG), with NO_MOVE excluded. Oracle: (hit+partial)/(hit+partial+miss). AstroGrid: sign(direction)·return > 0.

Only the **forward** tables feed the verdict. Hindsight rescores (#976 oracle, #973 crypto) and rows recorded after the outcome was knowable appear only in the exploratory tables.

## Headline

- **Signal sources:** forward N = 343,808, pooled hit 50.7%; verdicts EDGE 0, NO_EDGE 11, ANTI_EDGE 0, INSUFFICIENT 0. Exploratory N = 0, hit —.
- **Oracle models:** forward N = 2,817, pooled hit 49.2%; verdicts EDGE 0, NO_EDGE 0, ANTI_EDGE 0, INSUFFICIENT 105. Exploratory N = 15,980, hit 50.6%.
- **AstroGrid models:** forward N = 2,329, pooled hit 80.3%; verdicts EDGE 0, NO_EDGE 0, ANTI_EDGE 0, INSUFFICIENT 3. Exploratory N = 1,592, hit 28.1%.
- **Signal feeds:** verdicts EDGE 103, NO_EDGE 2345, ANTI_EDGE 18, INSUFFICIENT 6300.

Corrected signal outcome mix (`sig-eval-4-sessions`): CORRECT 174,450, INELIGIBLE 588,427, NO_MOVE 88,477, WRONG 169,358.

AstroGrid stored verdicts (effective scores, legacy SPY-graded rows excluded): `astrogrid-guru-v1` stored-verdict hit 0.0% of 1 hit/partial/miss; `astrogrid-seed-v1` stored-verdict hit 30.8% of 5,140 hit/partial/miss; `psi-oracle-v1` stored-verdict hit 0.0% of 23 hit/partial/miss.

## Signal sources by source type

### Forward

| Unit | N | Dates | N_eff | Hit | Wilson (naive) | Wilson (clustered) | Block | Boot p | BH p | Long | No-move | Live-scored | Verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `options_flow` | 223013 | 193 | 1762 | 51.5% | 51.3–51.7 | 49.1–53.8 | 24 | 0.4930 | 0.6779 | 74% | 22% | 5% | **NO_EDGE** |
| `quiverquant:gov_contracts` | 70046 | 135 | 1694 | 49.1% | 48.8–49.5 | 46.7–51.5 | 16 | 0.7285 | 0.8904 | 100% | 18% | 0% | **NO_EDGE** |
| `quiverquant:insider` | 14134 | 418 | 2617 | 47.7% | 46.8–48.5 | 45.7–49.6 | 7 | 0.1885 | 0.5899 | 11% | 17% | 4% | **NO_EDGE** |
| `quiverquant:wsb` | 13981 | 132 | 13981 | 51.3% | 50.5–52.2 | 50.5–52.2 | 16 | 0.2145 | 0.5899 | 57% | 15% | 6% | **NO_EDGE** |
| `insider` | 7055 | 201 | 1426 | 47.9% | 46.7–49.0 | 45.3–50.5 | 14 | 0.3975 | 0.6246 | 12% | 10% | 2% | **NO_EDGE** |
| `quiverquant:house` | 4311 | 349 | 858 | 48.6% | 47.1–50.0 | 45.2–51.9 | 7 | 0.3530 | 0.6246 | 45% | 16% | 1% | **NO_EDGE** |
| `quiverquant:senate` | 3264 | 894 | 1195 | 49.8% | 48.0–51.5 | 46.9–52.6 | 7 | 0.8675 | 0.8985 | 48% | 18% | 0% | **NO_EDGE** |
| `smart_money` | 3255 | 44 | 1973 | 54.2% | 52.5–55.9 | 52.0–56.4 | 7 | 0.0385 | 0.4235 | 65% | 17% | 0% | **NO_EDGE** |
| `social` | 3243 | 44 | 1484 | 53.0% | 51.3–54.7 | 50.4–55.5 | 5 | 0.1030 | 0.5665 | 65% | 17% | 0% | **NO_EDGE** |
| `gov_contract` | 993 | 635 | 316 | 49.5% | 46.4–52.7 | 44.1–55.0 | 7 | 0.8985 | 0.8985 | 100% | 16% | 2% | **NO_EDGE** |
| `congressional` | 513 | 80 | 311 | 52.8% | 48.5–57.1 | 47.3–58.3 | 20 | 0.3160 | 0.6246 | 47% | 10% | 0% | **NO_EDGE** |

### Exploratory (not used by the rule)

_No exploratory rows._

## Oracle by model

### Forward

| Unit | N | Dates | N_eff | Hit | Wilson (naive) | Wilson (clustered) | Block | Boot p | BH p | Long | Brier (clim.) | Verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `contagion` | 246 | 7 | 246 | 43.1% | 37.1–49.3 | 37.1–49.3 | 1 | 1.0000 | 1.0000 | 100% | 0.278 (0.245) | **INSUFFICIENT** |
| `fundamental` | 228 | 7 | 228 | 39.0% | 32.9–45.5 | 32.9–45.5 | 1 | 1.0000 | 1.0000 | 100% | 0.258 (0.238) | **INSUFFICIENT** |
| `options_flow_mut_5sajor` | 39 | 7 | 39 | 56.4% | 41.0–70.7 | 41.0–70.7 | 1 | 1.0000 | 1.0000 | 82% | 0.321 (0.246) | **INSUFFICIENT** |
| `options_flow_mut_9o0682` | 39 | 6 | 32 | 41.0% | 27.1–56.6 | 25.9–58.0 | 1 | 1.0000 | 1.0000 | 90% | 0.295 (0.242) | **INSUFFICIENT** |
| `options_flow_mut_pqepjg` | 39 | 7 | 22 | 51.3% | 36.2–66.1 | 31.9–70.3 | 1 | 1.0000 | 1.0000 | 90% | 0.300 (0.250) | **INSUFFICIENT** |
| `hyp_hyp_c4cb11b0` | 38 | 7 | 31 | 55.3% | 39.7–69.9 | 38.2–71.2 | 1 | 1.0000 | 1.0000 | 0% | 0.360 (0.247) | **INSUFFICIENT** |
| `cross_options_fl_contagion_5hsh` | 36 | 7 | 36 | 36.1% | 22.5–52.4 | 22.5–52.4 | 1 | 1.0000 | 1.0000 | 92% | 0.296 (0.231) | **INSUFFICIENT** |
| `options_flow_mut_ecsxj2` | 36 | 7 | 36 | 44.4% | 29.5–60.4 | 29.5–60.4 | 1 | 1.0000 | 1.0000 | 86% | 0.337 (0.247) | **INSUFFICIENT** |
| `options_flow_mut_6nbbp2` | 34 | 7 | 12 | 47.1% | 31.5–63.3 | 23.3–72.2 | 1 | 1.0000 | 1.0000 | 94% | 0.327 (0.249) | **INSUFFICIENT** |
| `options_flow_mut_h9s9h8` | 34 | 7 | 34 | 50.0% | 34.1–65.9 | 34.1–65.9 | 1 | 1.0000 | 1.0000 | 85% | 0.245 (0.250) | **INSUFFICIENT** |
| `options_flow_mut_hnp1m7` | 34 | 7 | 34 | 47.1% | 31.5–63.3 | 31.5–63.3 | 1 | 1.0000 | 1.0000 | 85% | 0.294 (0.249) | **INSUFFICIENT** |
| `cross_options_fl_contagion_r22d` | 33 | 7 | 29 | 42.4% | 27.2–59.2 | 26.4–60.2 | 1 | 1.0000 | 1.0000 | 85% | 0.290 (0.244) | **INSUFFICIENT** |
| `cross_options_fl_contagion_hgs7` | 32 | 7 | 32 | 50.0% | 33.6–66.4 | 33.6–66.4 | 1 | 1.0000 | 1.0000 | 88% | 0.305 (0.250) | **INSUFFICIENT** |
| `cross_options_fl_contagion_mpwf` | 32 | 7 | 30 | 37.5% | 22.9–54.7 | 22.5–55.3 | 1 | 1.0000 | 1.0000 | 91% | 0.239 (0.234) | **INSUFFICIENT** |
| `options_flow_mut_2dr50e` | 32 | 7 | 32 | 53.1% | 36.4–69.1 | 36.4–69.1 | 1 | 1.0000 | 1.0000 | 94% | 0.385 (0.249) | **INSUFFICIENT** |
| `options_flow_mut_efbt1x` | 32 | 7 | 29 | 37.5% | 22.9–54.7 | 22.3–55.6 | 1 | 1.0000 | 1.0000 | 88% | 0.231 (0.234) | **INSUFFICIENT** |
| `options_flow_mut_wb5je5` | 32 | 7 | 23 | 40.6% | 25.5–57.7 | 23.3–60.6 | 1 | 1.0000 | 1.0000 | 94% | 0.342 (0.241) | **INSUFFICIENT** |
| `regime_contrarian` | 32 | 6 | 32 | 59.4% | 42.3–74.5 | 42.3–74.5 | 1 | 1.0000 | 1.0000 | 0% | 0.457 (0.241) | **INSUFFICIENT** |
| `hyp_hyp_1fb5d744` | 31 | 7 | 31 | 61.3% | 43.8–76.3 | 43.8–76.3 | 1 | 1.0000 | 1.0000 | 0% | 0.399 (0.237) | **INSUFFICIENT** |
| `hyp_hyp_f5730f57` | 31 | 7 | 31 | 61.3% | 43.8–76.3 | 43.8–76.3 | 1 | 1.0000 | 1.0000 | 0% | 0.402 (0.237) | **INSUFFICIENT** |
| `options_flow` | 31 | 7 | 22 | 29.0% | 16.1–46.6 | 14.5–49.7 | 1 | 1.0000 | 1.0000 | 94% | 0.207 (0.206) | **INSUFFICIENT** |
| `options_flow_mut_3c7it0` | 31 | 7 | 22 | 58.1% | 40.8–73.6 | 37.8–75.9 | 1 | 1.0000 | 1.0000 | 90% | 0.341 (0.243) | **INSUFFICIENT** |
| `options_flow_mut_hav5pw` | 31 | 7 | 31 | 58.1% | 40.8–73.6 | 40.8–73.6 | 1 | 1.0000 | 1.0000 | 84% | 0.325 (0.243) | **INSUFFICIENT** |
| `cross_options_fl_contagion_la0k` | 30 | 7 | 27 | 53.3% | 36.1–69.8 | 35.2–70.6 | 1 | 1.0000 | 1.0000 | 90% | 0.338 (0.249) | **INSUFFICIENT** |
| `hyp_hyp_0be6854a` | 30 | 7 | 30 | 53.3% | 36.1–69.8 | 36.1–69.8 | 1 | 1.0000 | 1.0000 | 0% | 0.352 (0.249) | **INSUFFICIENT** |
| `cross_options_fl_contagion_fqwh` | 29 | 7 | 29 | 51.7% | 34.4–68.6 | 34.4–68.6 | 1 | 1.0000 | 1.0000 | 86% | 0.295 (0.250) | **INSUFFICIENT** |
| `cross_options_fl_contagion_x708` | 29 | 7 | 23 | 24.1% | 12.2–42.1 | 11.3–44.3 | 1 | 1.0000 | 1.0000 | 90% | 0.229 (0.183) | **INSUFFICIENT** |
| `cross_options_fl_contagion_z7g7` | 29 | 7 | 14 | 44.8% | 28.4–62.5 | 22.8–69.1 | 1 | 1.0000 | 1.0000 | 90% | 0.268 (0.247) | **INSUFFICIENT** |
| `flow_momentum` | 29 | 7 | 29 | 51.7% | 34.4–68.6 | 34.4–68.6 | 1 | 1.0000 | 1.0000 | 14% | 0.335 (0.250) | **INSUFFICIENT** |
| `hyp_hyp_93f9bb7b` | 29 | 7 | 29 | 48.3% | 31.4–65.6 | 31.4–65.6 | 1 | 1.0000 | 1.0000 | 0% | 0.313 (0.250) | **INSUFFICIENT** |
| `options_flow_mut_5uv8nb` | 29 | 6 | 29 | 48.3% | 31.4–65.6 | 31.4–65.6 | 1 | 1.0000 | 1.0000 | 90% | 0.289 (0.250) | **INSUFFICIENT** |
| `options_flow_mut_s9t9ov` | 29 | 7 | 23 | 65.5% | 47.3–80.1 | 45.0–81.5 | 1 | 1.0000 | 1.0000 | 3% | 0.285 (0.226) | **INSUFFICIENT** |
| `options_flow_mut_z24xrf` | 29 | 7 | 29 | 65.5% | 47.3–80.1 | 47.3–80.1 | 1 | 1.0000 | 1.0000 | 90% | 0.342 (0.226) | **INSUFFICIENT** |
| `cross_options_fl_contagion_k52z` | 28 | 6 | 28 | 35.7% | 20.7–54.2 | 20.7–54.2 | 1 | 1.0000 | 1.0000 | 89% | 0.291 (0.230) | **INSUFFICIENT** |
| `hyp_hyp_9533512d` | 28 | 6 | 24 | 67.9% | 49.3–82.1 | 47.7–83.0 | 1 | 1.0000 | 1.0000 | 0% | 0.413 (0.218) | **INSUFFICIENT** |
| `options_flow_mut_ueznc7` | 28 | 7 | 22 | 60.7% | 42.4–76.4 | 40.3–78.0 | 1 | 1.0000 | 1.0000 | 4% | 0.306 (0.239) | **INSUFFICIENT** |
| `options_flow_mut_yet32f` | 28 | 7 | 25 | 46.4% | 29.5–64.2 | 28.7–65.2 | 1 | 1.0000 | 1.0000 | 93% | 0.280 (0.249) | **INSUFFICIENT** |
| `cross_asset` | 27 | 7 | 20 | 51.9% | 34.0–69.3 | 31.5–71.6 | 1 | 1.0000 | 1.0000 | 78% | 0.408 (0.250) | **INSUFFICIENT** |
| `cross_options_fl_fundamenta_tuxp` | 27 | 7 | 27 | 59.3% | 40.7–75.5 | 40.7–75.5 | 1 | 1.0000 | 1.0000 | 81% | 0.367 (0.241) | **INSUFFICIENT** |
| `hyp_hyp_75a9e366` | 27 | 7 | 23 | 66.7% | 47.8–81.4 | 46.4–82.2 | 1 | 1.0000 | 1.0000 | 0% | 0.430 (0.222) | **INSUFFICIENT** |
| `options_flow_mut_cdfchq` | 27 | 7 | 23 | 55.6% | 37.3–72.4 | 36.1–73.5 | 1 | 1.0000 | 1.0000 | 93% | 0.321 (0.247) | **INSUFFICIENT** |
| `options_flow_mut_mn7th9` | 27 | 7 | 18 | 29.6% | 15.9–48.5 | 13.7–52.8 | 1 | 1.0000 | 1.0000 | 81% | 0.273 (0.209) | **INSUFFICIENT** |
| `cross_options_fl_contagion_81x1` | 26 | 7 | 18 | 50.0% | 32.1–67.9 | 29.1–70.9 | 1 | 1.0000 | 1.0000 | 77% | 0.371 (0.250) | **INSUFFICIENT** |
| `hyp_hyp_45db960b` | 26 | 7 | 26 | 57.7% | 38.9–74.5 | 38.9–74.5 | 1 | 1.0000 | 1.0000 | 0% | 0.382 (0.244) | **INSUFFICIENT** |
| `hyp_hyp_55ff59c7` | 26 | 7 | 26 | 61.5% | 42.5–77.6 | 42.5–77.6 | 1 | 1.0000 | 1.0000 | 0% | 0.408 (0.237) | **INSUFFICIENT** |
| `options_flow_mut_09cmzr` | 26 | 7 | 26 | 46.2% | 28.8–64.5 | 28.8–64.5 | 1 | 1.0000 | 1.0000 | 81% | 0.306 (0.249) | **INSUFFICIENT** |
| `options_flow_mut_tq1szn` | 26 | 6 | 13 | 73.1% | 53.9–86.3 | 46.4–89.5 | 1 | 1.0000 | 1.0000 | 92% | 0.411 (0.197) | **INSUFFICIENT** |
| `cross_options_fl_contagion_1wbi` | 25 | 7 | 16 | 56.0% | 37.1–73.3 | 33.2–76.5 | 1 | 1.0000 | 1.0000 | 84% | 0.342 (0.246) | **INSUFFICIENT** |
| `cross_options_fl_contagion_6php` | 25 | 7 | 25 | 36.0% | 20.2–55.5 | 20.2–55.5 | 1 | 1.0000 | 1.0000 | 76% | 0.260 (0.230) | **INSUFFICIENT** |
| `hyp_hyp_1fb7a73c` | 25 | 7 | 13 | 60.0% | 40.7–76.6 | 34.1–81.3 | 1 | 1.0000 | 1.0000 | 0% | 0.391 (0.240) | **INSUFFICIENT** |
| `hyp_hyp_3cc523c9` | 25 | 7 | 25 | 64.0% | 44.5–79.8 | 44.5–79.8 | 1 | 1.0000 | 1.0000 | 0% | 0.396 (0.230) | **INSUFFICIENT** |
| `hyp_hyp_cd811e27` | 25 | 7 | 19 | 60.0% | 40.7–76.6 | 38.0–78.6 | 1 | 1.0000 | 1.0000 | 0% | 0.375 (0.240) | **INSUFFICIENT** |
| `options_flow_mut_j78auc` | 25 | 7 | 18 | 40.0% | 23.4–59.3 | 21.2–62.2 | 1 | 1.0000 | 1.0000 | 92% | 0.277 (0.240) | **INSUFFICIENT** |
| `holder_overlap` | 24 | 7 | 24 | 45.8% | 27.9–64.9 | 27.9–64.9 | 1 | 1.0000 | 1.0000 | 0% | 0.265 (0.248) | **INSUFFICIENT** |
| `hyp_hyp_f6924823` | 24 | 6 | 24 | 62.5% | 42.7–78.8 | 42.7–78.8 | 1 | 1.0000 | 1.0000 | 0% | 0.323 (0.234) | **INSUFFICIENT** |
| `options_flow_mut_563j3d` | 24 | 7 | 24 | 45.8% | 27.9–64.9 | 27.9–64.9 | 1 | 1.0000 | 1.0000 | 4% | 0.279 (0.248) | **INSUFFICIENT** |
| `options_flow_mut_7g7lxd` | 24 | 7 | 24 | 50.0% | 31.4–68.6 | 31.4–68.6 | 1 | 1.0000 | 1.0000 | 0% | 0.213 (0.250) | **INSUFFICIENT** |
| `options_flow_mut_g34098` | 24 | 7 | 24 | 50.0% | 31.4–68.6 | 31.4–68.6 | 1 | 1.0000 | 1.0000 | 92% | 0.287 (0.250) | **INSUFFICIENT** |
| `options_flow_mut_mnkr0n` | 24 | 7 | 24 | 66.7% | 46.7–82.0 | 46.7–82.0 | 1 | 1.0000 | 1.0000 | 25% | 0.301 (0.222) | **INSUFFICIENT** |
| `options_flow_mut_z4pjt9` | 24 | 7 | 10 | 37.5% | 21.2–57.3 | 15.3–66.5 | 1 | 1.0000 | 1.0000 | 88% | 0.315 (0.234) | **INSUFFICIENT** |
| `options_flow_mut_zwahvq` | 24 | 7 | 14 | 50.0% | 31.4–68.6 | 26.6–73.4 | 1 | 1.0000 | 1.0000 | 92% | 0.271 (0.250) | **INSUFFICIENT** |
| `hyp_hyp_2c05a526` | 23 | 6 | 23 | 60.9% | 40.8–77.8 | 40.8–77.8 | 1 | 1.0000 | 1.0000 | 0% | 0.385 (0.238) | **INSUFFICIENT** |
| `hyp_hyp_6e218326` | 23 | 7 | 20 | 43.5% | 25.6–63.2 | 24.6–64.5 | 1 | 1.0000 | 1.0000 | 0% | 0.279 (0.246) | **INSUFFICIENT** |
| `hyp_hyp_db751bef` | 23 | 6 | 19 | 52.2% | 33.0–70.8 | 31.3–72.3 | 1 | 1.0000 | 1.0000 | 0% | 0.273 (0.250) | **INSUFFICIENT** |
| `cross_options_fl_contagion_t39x` | 22 | 6 | 22 | 50.0% | 30.7–69.3 | 30.7–69.3 | 1 | 1.0000 | 1.0000 | 82% | 0.299 (0.250) | **INSUFFICIENT** |
| `hyp_hyp_2a6a3166` | 22 | 6 | 12 | 68.2% | 47.3–83.6 | 40.4–87.2 | 1 | 1.0000 | 1.0000 | 0% | 0.352 (0.217) | **INSUFFICIENT** |
| `hyp_hyp_58f3b700` | 22 | 7 | 20 | 36.4% | 19.7–57.0 | 19.3–57.8 | 1 | 1.0000 | 1.0000 | 0% | 0.262 (0.231) | **INSUFFICIENT** |
| `hyp_hyp_6cc82e35` | 22 | 5 | 22 | 59.1% | 38.7–76.7 | 38.7–76.7 | 1 | 1.0000 | 1.0000 | 0% | 0.331 (0.242) | **INSUFFICIENT** |
| `options_flow_mut_20zc3l` | 22 | 7 | 17 | 31.8% | 16.4–52.7 | 14.9–55.4 | 1 | 1.0000 | 1.0000 | 36% | 0.240 (0.217) | **INSUFFICIENT** |
| `options_flow_mut_as10xg` | 22 | 7 | 22 | 40.9% | 23.3–61.3 | 23.3–61.3 | 1 | 1.0000 | 1.0000 | 82% | 0.218 (0.242) | **INSUFFICIENT** |
| `hyp_hyp_1be4686a` | 21 | 7 | 14 | 52.4% | 32.4–71.7 | 28.8–75.0 | 1 | 1.0000 | 1.0000 | 0% | 0.354 (0.249) | **INSUFFICIENT** |
| `hyp_hyp_7162e156` | 21 | 6 | 21 | 57.1% | 36.5–75.5 | 36.5–75.5 | 1 | 1.0000 | 1.0000 | 0% | 0.344 (0.245) | **INSUFFICIENT** |
| `hyp_hyp_f761a726` | 21 | 6 | 21 | 47.6% | 28.3–67.6 | 28.3–67.6 | 1 | 1.0000 | 1.0000 | 0% | 0.280 (0.249) | **INSUFFICIENT** |
| `options_flow_mut_ylnbrl` | 21 | 7 | 16 | 33.3% | 17.2–54.6 | 15.7–57.3 | 1 | 1.0000 | 1.0000 | 86% | 0.232 (0.222) | **INSUFFICIENT** |
| `hyp_hyp_2ffe2009` | 20 | 6 | 20 | 55.0% | 34.2–74.2 | 34.2–74.2 | 1 | 1.0000 | 1.0000 | 0% | 0.309 (0.247) | **INSUFFICIENT** |
| `hyp_hyp_c978b645` | 20 | 7 | 12 | 85.0% | 64.0–94.8 | 57.5–96.0 | 1 | 1.0000 | 1.0000 | 0% | 0.535 (0.128) | **INSUFFICIENT** |
| `hyp_hyp_e77d3fba` | 20 | 7 | 17 | 60.0% | 38.7–78.1 | 37.0–79.3 | 1 | 1.0000 | 1.0000 | 0% | 0.391 (0.240) | **INSUFFICIENT** |
| `news_energy` | 19 | 6 | 8 | 57.9% | 36.3–76.9 | 27.6–83.2 | 1 | 1.0000 | 1.0000 | 11% | 0.381 (0.244) | **INSUFFICIENT** |
| `options_flow_mut_rttym8` | 19 | 7 | 11 | 47.4% | 27.3–68.3 | 23.0–73.0 | 1 | 1.0000 | 1.0000 | 0% | 0.289 (0.249) | **INSUFFICIENT** |
| `cross_options_fl_contagion_apdy` | 18 | 7 | 18 | 38.9% | 20.3–61.4 | 20.3–61.4 | 1 | 1.0000 | 1.0000 | 94% | 0.272 (0.238) | **INSUFFICIENT** |
| `hyp_hyp_20215192` | 18 | 6 | 14 | 50.0% | 29.0–71.0 | 26.5–73.5 | 1 | 1.0000 | 1.0000 | 0% | 0.328 (0.250) | **INSUFFICIENT** |
| `hyp_hyp_8981fd29` | 18 | 6 | 18 | 61.1% | 38.6–79.7 | 38.6–79.7 | 1 | 1.0000 | 1.0000 | 0% | 0.394 (0.238) | **INSUFFICIENT** |
| `hyp_hyp_5c57f02b` | 16 | 4 | 16 | 50.0% | 28.0–72.0 | 28.0–72.0 | 1 | 1.0000 | 1.0000 | 0% | 0.267 (0.250) | **INSUFFICIENT** |
| `hyp_hyp_74ba8555` | 16 | 6 | 15 | 43.8% | 23.1–66.8 | 22.4–67.7 | 1 | 1.0000 | 1.0000 | 0% | 0.245 (0.246) | **INSUFFICIENT** |
| `cross_options_fl_contagion_73lr` | 15 | 6 | 15 | 46.7% | 24.8–69.9 | 24.8–69.9 | 1 | 1.0000 | 1.0000 | 80% | 0.281 (0.249) | **INSUFFICIENT** |
| `hyp_hyp_f71ac4df` | 14 | 6 | 14 | 50.0% | 26.8–73.2 | 26.8–73.2 | 1 | 1.0000 | 1.0000 | 0% | 0.287 (0.250) | **INSUFFICIENT** |
| `hyp_hyp_fb8c60c6` | 14 | 4 | 10 | 64.3% | 38.8–83.7 | 35.1–85.7 | 1 | 1.0000 | 1.0000 | 0% | 0.328 (0.230) | **INSUFFICIENT** |
| `hyp_hyp_4a0eb53d` | 13 | 4 | 9 | 92.3% | 66.7–98.6 | 60.2–99.0 | 1 | 1.0000 | 1.0000 | 0% | 0.394 (0.071) | **INSUFFICIENT** |
| `hyp_hyp_17851ba6` | 12 | 3 | 12 | 41.7% | 19.3–68.0 | 19.3–68.0 | 1 | 1.0000 | 1.0000 | 0% | 0.245 (0.243) | **INSUFFICIENT** |
| `hyp_hyp_ae8d385b` | 12 | 4 | 4 | 58.3% | 32.0–80.7 | 19.2–89.2 | 1 | 1.0000 | 1.0000 | 0% | 0.327 (0.243) | **INSUFFICIENT** |
| `hyp_hyp_e9aa2eaf` | 12 | 4 | 9 | 41.7% | 19.3–68.0 | 17.3–70.9 | 1 | 1.0000 | 1.0000 | 0% | 0.246 (0.243) | **INSUFFICIENT** |
| `hyp_hyp_50d685d3` | 11 | 4 | 11 | 81.8% | 52.3–94.9 | 52.3–94.9 | 1 | 1.0000 | 1.0000 | 0% | 0.351 (0.149) | **INSUFFICIENT** |
| `hyp_hyp_9c9ab734` | 11 | 5 | 8 | 27.3% | 9.7–56.6 | 8.0–61.6 | 1 | 1.0000 | 1.0000 | 0% | 0.198 (0.198) | **INSUFFICIENT** |
| `hyp_hyp_e2115452` | 10 | 4 | 8 | 40.0% | 16.8–68.7 | 15.2–71.3 | 1 | 1.0000 | 1.0000 | 0% | 0.241 (0.240) | **INSUFFICIENT** |
| `hyp_hyp_2e0d2f94` | 7 | 3 | 7 | 14.3% | 2.6–51.3 | 2.6–51.3 | 1 | 1.0000 | 1.0000 | 0% | 0.174 (0.122) | **INSUFFICIENT** |
| `hyp_hyp_7309a1da` | 7 | 2 | 7 | 42.9% | 15.8–75.0 | 15.8–75.0 | 1 | 1.0000 | 1.0000 | 0% | 0.249 (0.245) | **INSUFFICIENT** |
| `hyp_hyp_4374a756` | 3 | 1 | 1 | 66.7% | 20.8–93.9 | 9.4–97.5 | 1 | 1.0000 | 1.0000 | 0% | 0.311 (0.222) | **INSUFFICIENT** |
| `hyp_hyp_3db5f8e4` | 2 | 1 | 1 | 0.0% | 0.0–65.8 | 0.0–79.3 | 1 | 1.0000 | 1.0000 | 0% | 0.136 (0.000) | **INSUFFICIENT** |
| `hyp_hyp_5444fb6f` | 2 | 1 | 1 | 50.0% | 9.5–90.5 | 5.5–94.5 | 1 | 1.0000 | 1.0000 | 0% | 0.268 (0.250) | **INSUFFICIENT** |
| `hyp_hyp_7a0c6487` | 2 | 1 | 1 | 0.0% | 0.0–65.8 | 0.0–79.3 | 1 | 1.0000 | 1.0000 | 0% | 0.136 (0.000) | **INSUFFICIENT** |
| `hyp_hyp_86a395f1` | 2 | 1 | 1 | 0.0% | 0.0–65.8 | 0.0–79.3 | 1 | 1.0000 | 1.0000 | 0% | 0.136 (0.000) | **INSUFFICIENT** |
| `hyp_hyp_9ec0c69a` | 2 | 1 | 1 | 0.0% | 0.0–65.8 | 0.0–79.3 | 1 | 1.0000 | 1.0000 | 0% | 0.137 (0.000) | **INSUFFICIENT** |
| `hyp_hyp_185699a8` | 1 | 1 | 1 | 100.0% | 20.7–100.0 | 20.7–100.0 | 1 | 1.0000 | 1.0000 | 0% | 0.398 (0.000) | **INSUFFICIENT** |
| `hyp_hyp_829acf81` | 1 | 1 | 1 | 100.0% | 20.7–100.0 | 20.7–100.0 | 1 | 1.0000 | 1.0000 | 0% | 0.399 (0.000) | **INSUFFICIENT** |
| `hyp_hyp_a9720382` | 1 | 1 | 1 | 100.0% | 20.7–100.0 | 20.7–100.0 | 1 | 1.0000 | 1.0000 | 0% | 0.397 (0.000) | **INSUFFICIENT** |

Target-hit rate (`hit` only, the legacy headline): `contagion` 30.1%, `fundamental` 27.6%, `options_flow_mut_5sajor` 30.8%, `options_flow_mut_9o0682` 15.4%, `options_flow_mut_pqepjg` 20.5%, `hyp_hyp_c4cb11b0` 50.0%, `cross_options_fl_contagion_5hsh` 13.9%, `options_flow_mut_ecsxj2` 19.4%, `options_flow_mut_6nbbp2` 20.6%, `options_flow_mut_h9s9h8` 14.7%, `options_flow_mut_hnp1m7` 29.4%, `cross_options_fl_contagion_r22d` 24.2%.

### Exploratory (not used by the rule)

| Unit | N | Dates | Hit | Wilson (naive) | Target-hit |
|---|---|---|---|---|---|
| `timeseries_enhanced [rescore:hindsight]` | 6385 | 91 | 46.9% | 45.6–48.1 | 5.0% |
| `holder_overlap [rescore:hindsight]` | 2751 | 89 | 61.3% | 59.5–63.1 | 56.5% |
| `hyp_hyp_a1ba89b0 [rescore:hindsight]` | 2050 | 81 | 46.7% | 44.5–48.8 | 40.3% |
| `hyp_hyp_6ca57178 [rescore:hindsight]` | 464 | 40 | 49.8% | 45.3–54.3 | 15.3% |
| `options_flow_mut_g34098 [not forward]` | 422 | 19 | 51.7% | 46.9–56.4 | 7.6% |
| `options_flow [not forward]` | 320 | 28 | 61.3% | 55.8–66.4 | 25.9% |
| `options_flow_mut_mnkr0n [not forward]` | 304 | 18 | 49.0% | 43.4–54.6 | 41.8% |
| `hyp_hyp_6827c0ff [rescore:hindsight]` | 293 | 68 | 46.8% | 41.1–52.5 | 9.9% |
| `cross_asset [not forward]` | 261 | 28 | 48.7% | 42.7–54.7 | 23.8% |
| `contagion [not forward]` | 212 | 18 | 58.5% | 51.8–64.9 | 40.6% |
| `fundamental [not forward]` | 157 | 13 | 61.8% | 54.0–69.0 | 27.4% |
| `options_flow_mut_7g7lxd [rescore:hindsight]` | 157 | 23 | 61.1% | 53.3–68.4 | 12.7% |
| `news_energy [not forward]` | 155 | 22 | 54.8% | 47.0–62.5 | 36.1% |
| `news_energy [rescore:hindsight]` | 148 | 29 | 68.2% | 60.4–75.2 | 64.9% |
| `flow_momentum [not forward]` | 127 | 23 | 34.6% | 26.9–43.3 | 15.7% |

## AstroGrid by model version (crypto-v1 = `grid-crypto-v1`)

### Forward

| Unit | N | Dates | N_eff | Hit | Wilson (naive) | Wilson (clustered) | Block | Boot p | BH p | Long | Verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `astrogrid-seed-v1` | 2329 | 2 | 2329 | 80.3% | 78.7–81.9 | 78.7–81.9 | 1 | 1.0000 | 1.0000 | 100% | **INSUFFICIENT** |
| `astrogrid-guru-v1` | 0 | 0 | 0 | — | — | — | 1 | 1.0000 | 1.0000 | — | **INSUFFICIENT** |
| `psi-oracle-v1` | 0 | 0 | 0 | — | — | — | 1 | 1.0000 | 1.0000 | — | **INSUFFICIENT** |

### Exploratory (not used by the rule)

| Unit | N | Dates | Hit | Wilson (naive) | Target-hit |
|---|---|---|---|---|---|
| `grid-crypto-v1 [legacy_rescore]` | 1592 | 70 | 28.1% | 25.9–30.3 | 8.9% |

## Signal feeds (source_type / feed id)

8,766 feeds have forward rows; 2,466 clear the N_eff/date floor. Top 40 by N:

| Unit | N | Dates | N_eff | Hit | Wilson (naive) | Wilson (clustered) | Block | Boot p | BH p | Long | No-move | Live-scored | Verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `quiverquant:gov_contracts / qq_gov_contracts` | 70046 | 135 | 1694 | 49.1% | 48.8–49.5 | 46.7–51.5 | 16 | 0.7085 | 1.0000 | 100% | 18% | 0% | **NO_EDGE** |
| `quiverquant:insider / qq_insider_trading` | 14134 | 418 | 2617 | 47.7% | 46.8–48.5 | 45.7–49.6 | 7 | 0.1895 | 1.0000 | 11% | 17% | 4% | **NO_EDGE** |
| `quiverquant:wsb / qq_wsb` | 13981 | 132 | 13981 | 51.3% | 50.5–52.2 | 50.5–52.2 | 16 | 0.2100 | 1.0000 | 57% | 15% | 6% | **NO_EDGE** |
| `quiverquant:house / qq_house_trading` | 4311 | 349 | 858 | 48.6% | 47.1–50.0 | 45.2–51.9 | 7 | 0.3675 | 1.0000 | 45% | 16% | 1% | **NO_EDGE** |
| `quiverquant:senate / qq_senate_trading` | 3264 | 894 | 1195 | 49.8% | 48.0–51.5 | 46.9–52.6 | 7 | 0.8790 | 1.0000 | 48% | 18% | 0% | **NO_EDGE** |
| `smart_money / reddit:oddfinnish1` | 353 | 32 | 273 | 65.2% | 60.0–69.9 | 59.3–70.6 | 7 | 0.0015 | 0.0871 | 97% | 15% | 0% | **NO_EDGE** |
| `social / reddit:oddfinnish1` | 350 | 32 | 219 | 62.6% | 57.4–67.5 | 56.0–68.7 | 5 | 0.0135 | 0.3906 | 97% | 16% | 0% | **NO_EDGE** |
| `gov_contract / General Services Administration` | 175 | 141 | 136 | 51.4% | 44.1–58.7 | 43.1–59.7 | 7 | 0.7870 | 1.0000 | 100% | 15% | 2% | **NO_EDGE** |
| `gov_contract / Department of Homeland Security` | 169 | 138 | 133 | 48.5% | 41.1–56.0 | 40.2–56.9 | 7 | 0.8090 | 1.0000 | 100% | 17% | 3% | **NO_EDGE** |
| `options_flow / whale_coin_140` | 166 | 166 | 165 | 47.0% | 39.5–54.6 | 39.5–54.6 | 20 | 0.7170 | 1.0000 | 55% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_coin_150` | 166 | 166 | 165 | 53.0% | 45.4–60.5 | 45.4–60.5 | 20 | 0.4985 | 1.0000 | 55% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_coin_160` | 166 | 166 | 165 | 48.8% | 41.3–56.3 | 41.3–56.4 | 20 | 0.9115 | 1.0000 | 66% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_coin_165` | 166 | 166 | 165 | 48.2% | 40.7–55.7 | 40.7–55.8 | 20 | 0.8520 | 1.0000 | 72% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_coin_170` | 166 | 166 | 165 | 54.2% | 46.6–61.6 | 46.6–61.6 | 20 | 0.3425 | 1.0000 | 81% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_coin_175` | 166 | 166 | 165 | 42.8% | 35.5–50.4 | 35.5–50.4 | 20 | 0.1475 | 1.0000 | 77% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_coin_180` | 166 | 166 | 165 | 50.0% | 42.5–57.5 | 42.5–57.5 | 20 | 1.0000 | 1.0000 | 88% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_coin_185` | 166 | 166 | 165 | 38.6% | 31.5–46.1 | 31.5–46.2 | 20 | 0.0600 | 1.0000 | 78% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_coin_190` | 166 | 166 | 165 | 40.4% | 33.2–48.0 | 33.2–48.0 | 20 | 0.1520 | 1.0000 | 88% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_coin_195` | 166 | 166 | 165 | 42.2% | 34.9–49.8 | 34.9–49.8 | 15 | 0.1220 | 1.0000 | 72% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_coin_200` | 166 | 166 | 165 | 44.0% | 36.6–51.6 | 36.6–51.6 | 20 | 0.3715 | 1.0000 | 98% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_coin_210` | 166 | 166 | 165 | 38.6% | 31.5–46.1 | 31.5–46.2 | 20 | 0.0665 | 1.0000 | 88% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_coin_220` | 166 | 166 | 165 | 42.8% | 35.5–50.4 | 35.5–50.4 | 20 | 0.2825 | 1.0000 | 99% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_coin_100` | 165 | 165 | 164 | 56.4% | 48.7–63.7 | 48.7–63.7 | 20 | 0.4045 | 1.0000 | 14% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_coin_145` | 165 | 165 | 164 | 51.5% | 43.9–59.0 | 43.9–59.0 | 20 | 0.8475 | 1.0000 | 52% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_coin_250` | 165 | 165 | 164 | 43.6% | 36.3–51.3 | 36.3–51.3 | 20 | 0.3640 | 1.0000 | 98% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_pltr_100` | 164 | 164 | 163 | 37.8% | 30.7–45.4 | 30.7–45.4 | 20 | 0.0145 | 0.4087 | 61% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_pltr_110` | 164 | 164 | 163 | 54.3% | 46.6–61.7 | 46.6–61.7 | 20 | 0.5985 | 1.0000 | 93% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_pltr_115` | 164 | 164 | 163 | 59.8% | 52.1–67.0 | 52.1–67.0 | 20 | 0.1125 | 1.0000 | 71% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_pltr_120` | 164 | 164 | 163 | 54.3% | 46.6–61.7 | 46.6–61.7 | 20 | 0.5120 | 1.0000 | 70% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_pltr_125` | 164 | 164 | 163 | 59.1% | 51.5–66.4 | 51.5–66.4 | 20 | 0.1730 | 1.0000 | 74% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_pltr_130` | 164 | 164 | 163 | 56.1% | 48.4–63.5 | 48.4–63.5 | 20 | 0.3660 | 1.0000 | 84% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_pltr_135` | 164 | 164 | 163 | 51.8% | 44.2–59.3 | 44.2–59.4 | 20 | 0.8385 | 1.0000 | 96% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_pltr_140` | 164 | 164 | 163 | 53.0% | 45.4–60.5 | 45.4–60.6 | 20 | 0.7025 | 1.0000 | 94% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_pltr_145` | 164 | 164 | 163 | 50.0% | 42.4–57.6 | 42.4–57.6 | 20 | 1.0000 | 1.0000 | 97% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_pltr_150` | 164 | 164 | 163 | 51.8% | 44.2–59.3 | 44.2–59.4 | 20 | 0.8295 | 1.0000 | 99% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_pltr_155` | 164 | 164 | 163 | 47.6% | 40.1–55.2 | 40.0–55.2 | 20 | 0.7330 | 1.0000 | 94% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_pltr_160` | 164 | 164 | 163 | 51.2% | 43.6–58.8 | 43.6–58.8 | 20 | 0.8995 | 1.0000 | 99% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_pltr_165` | 164 | 164 | 163 | 51.8% | 44.2–59.3 | 44.2–59.4 | 20 | 0.8360 | 1.0000 | 93% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_pltr_170` | 164 | 164 | 163 | 51.8% | 44.2–59.3 | 44.2–59.4 | 20 | 0.8545 | 1.0000 | 100% | 8% | 6% | **NO_EDGE** |
| `options_flow / whale_pltr_200` | 164 | 164 | 163 | 51.8% | 44.2–59.3 | 44.2–59.4 | 20 | 0.8375 | 1.0000 | 100% | 8% | 6% | **NO_EDGE** |

## What happens next

`scripts/edge_report.py report --candidate-out PATH` writes the NO_EDGE / ANTI_EDGE units as a *candidate* prune list. Nothing changes until an operator copies it to `GRID_TRUST_PRUNE_LIST_PATH` and sets `GRID_TRUST_PRUNE_NO_EDGE=true`. Those units then get weight 0 in the convergence conviction layer, `detect_convergence` and `get_trusted_sources`. Nothing is up-weighted, and no unit is flipped to contrarian.

---

## Notes (hand-written after the numbers; the rule above was not changed)

This is a snapshot. The `signal_source_outcomes` backfill was still running, and forward
signal N grew from 313,869 to 343,808 between two runs about four minutes apart. Rerun the
CLI before acting on anything.

**Signal sources.** All 11 source types are `NO_EDGE`, with hit rates of 47.7–54.2%. Two
clustered intervals exclude 0.5 (`smart_money` 54.2% [52.0, 56.4] and `quiverquant:wsb` 51.3%
[50.5, 52.2]), but neither survives BH, so the pre-registered verdict is `NO_EDGE`. Pooled,
the forward hit rate is 50.7%, which matches the #980 headline.

**The candidate list would zero the whole convergence layer.** It contains all 11 source
types plus 2,345 `NO_EDGE` and 18 `ANTI_EDGE` feeds. If the list is installed as-is and
`GRID_TRUST_PRUNE_NO_EDGE` is turned on, every stream in `scan_convergence` and every source
in `detect_convergence` gets weight 0. That is what the rule says, since no source has shown
edge, but it is a product decision for Anik, not a default.

**Feed-level `EDGE` and `ANTI_EDGE` look like drift.** Nearly all of them are `options_flow`
whale feeds keyed per strike (for example `whale_aapl_250`…`whale_aapl_290`: 91–96% long and
68–79% hit). One underlying move counts once per strike, and the bootstrap runs per feed, so
it cannot see that the strikes are correlated. The `ANTI_EDGE` feeds are the mirror case
(NFLX, SOFI and TSLA strikes in down-moves). Under v1, `EDGE` does nothing, because nothing is
up-weighted. Treat the feed-level `ANTI_EDGE` / `NO_EDGE` entries with the same suspicion. A
v2 that adjusts for drift and underlying (an always-long baseline per underlying, clustered
by underlying × date) needs its own pre-registration.

**Oracle has no forward verdicts.** Only 6,064 `dedup_keep` rows carry an official verdict,
and all of them were created between 2026-03-26 and 2026-05-14. 3,247 of those were
**scored before their expiry close**: 2,056 on expiry day before 16:00 ET and 1,191 the day
before. They are exploratory (hit 50.7%). The remaining 2,817 forward rows span only 7
creation dates (04-18 → 04-25), so every model is `INSUFFICIENT`. Their pooled direction hit
is 49.2% and their target-hit (`hit` only) is 35.2%. Since mid-May the oracle has produced
no official verdicts, only `pending` / `no_data`. The #976 hindsight rescores (12,733 rows)
show a direction hit of 50.5% and a target-hit of 24.0%.

**AstroGrid / crypto-v1 has no forward verdicts either.** No post-fix `grid-crypto-v1` row
has been scored yet (it waits on the S14 24/7 prices). The #973 legacy rescores (1,592 rows,
exploratory) have a direction hit of 28.1% and a target-hit of 8.9%, consistent with the
~10% baseline. `astrogrid-seed-v1` shows 80.3% on 2,329 rows, but they come from only 2
creation dates, essentially one market move, so it is `INSUFFICIENT`.

## Trust-score switch: legacy vs corrected outcomes (read-only, production, 2026-11-06 02:14Z)

`python scripts/edge_report.py trust-diff` recomputes `compute_trust_scores` under both
bases. Nothing is written.

| | Sources | Mean | p10 | p50 | p90 | Share ≥ 0.6 (the `get_trusted_sources` bar) |
|---|---|---|---|---|---|---|
| Legacy (`signal_sources.outcome`) | 13,072 | 0.426 | 0.249 | 0.453 | 0.553 | 3.1% |
| Corrected (`sig-eval-4-sessions`) | 14,746 | 0.499 | 0.427 | 0.500 | 0.572 | 7.0% |

- 13,024 sources appear in both. 1,722 appear only under corrected; these are sources whose
  rows were never CORRECT/WRONG under legacy, or whose outcomes are all neutral, so they now
  sit at their prior. 48 appear only under legacy. They have no corrected rows yet because
  the backfill is still running, and they would keep their stale legacy score if the flag
  were flipped now.
- **Biggest risers** are `options_flow` whale feeds that legacy scored 0 for N (for example
  `whale_tlt_78` 0/90 → 45/11, trust 0.04 → 0.77). They are also the drift-suspect feeds
  above. The QuiverQuant feeds move from 0.00–0.19 to 0.46–0.51, and the mean for
  `options_flow` goes from 0.32 to 0.50.
- **Biggest fallers** are insiders whose legacy CORRECT rows the corrected scorer finds
  INELIGIBLE (priced before the filing was known) or WRONG. For example, `CHANG FRED FACHING`
  goes from 31/3 to 0/0 with 27 INELIGIBLE (0.85 → 0.50), and `Silver Lake Technology
  Investors IV` from 24/0 to 7/12 (0.85 → 0.46). The switch removes this look-ahead credit.
