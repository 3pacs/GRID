# E0 v2: machinery calibration with a calibrated outcome model

E0 v2 is a **sibling** of `evals/e0`. **`evals/e0` stays e0-v1 for good**, because VS1 v8 verifies the e0-v1 manifest (`75489d50…`) at run time and pins `evals/e0` in its code files.

v2 imports the frozen v1 modules unchanged: the generator, machinery adapter, runners, scorer, structure and replication helpers. It also uses v1's committed data files: the v7 Technology structure npz, the replication npz and the deny-list. Before every run it checks that `evals/e0` is exactly e0-v1 (`manifest.verify_builds_on`).

```
python -m evals.e0v2 verify
python -m evals.e0v2 run --out DIR --profile full --jobs 4     # writes DIR/scorecard.json once
python -m evals.e0v2 run --out DIR --profile ic01               # IC 0.01 recovery profile
```

## What changed versus e0-v1

| | e0-v1 | e0-v2 |
|---|---|---|
| Realistic outcome model | hand-set `factor_t_garch` | **`factor_t_garch_calibrated`**, from the EVAL-E0C2 indirect-inference fit |
| Style exposure | 0 and 0.3 | sensitivity grid {0, 0.15, 0.30, 0.45} |
| Headline scenario | `factor_t_garch_exposed` | `factor_t_garch_calibrated_exposed_030` (owner confirms at release) |
| Profiles | full, ci, smoke | plus `ic01` (60 sims, 199 flips, IC {0, 0.01}) |
| VS1 cross-check | v7 design (raw 0.0125) | v8 confirmatory rule (one-sided 0.05), **information only** |
| Replication window | 1993-06 to 2010-12 | 1993-06 to 2007-10 (cutoff 2007-11-01) |

The structure, machinery, seeds, selection and planted-effect calibration are unchanged. v1's three scenarios are carried unchanged in `config.json` for continuity, and their published e0-v1 scorecard is the v1 column of the v2 report.

### Calibrated parameters

| Parameter | Value | Receipt fit (90% CI) | e0-v1 |
|---|---|---|---|
| market_vol | 0.0075 | 0.00747 (0.0066–0.0098) | 0.010 |
| beta_sd | 0.314 | 0.314 (0.27–0.40) | 0.3 |
| factor_vol | 0.0034 | 0.00344 (0.0021–0.0070) | 0.006 |
| factor_ar1 | **0** | −0.09 (−0.78–0.40, not identified) | 0.05 |
| tail_df | 3 | 3 (3–12) | 4 |
| GARCH α / β | 0.049 / 0.898 | 0.0488 / 0.898 (α+β 0.905–0.970) | 0.08 / 0.90 |
| idio_vol | 0.0195 | 0.0195 (0.017–0.022) | 0.020 |
| idio_vol_dispersion | 0.40 | 0.400 (0.37–0.46) | 0.4 |
| label_missing_rate | 0.002 | 6.5e-6 (survivor panel) | 0.002 |

- `factor_ar1` is 0 because the data do not identify it. This was an owner decision on 2026-10-01.
- `label_missing_rate` keeps v1's 0.002 floor, because the survivor panel cannot show delisting missingness.

## Calibration data rule: dates before 2007-11-01 only

The calibration (`data/calibration_receipt.json`, sha256 pinned in `config.json`) and v2's replication use **only dates before 2007-11-01**. They also use only non-Technology tickers that are not on the 908-ticker VS1 Technology deny-list.

The reason: VS1 v8 starts Technology discovery on 2008-01-01, with an admission probe from 2007-11-02. The sectors-v6 registration bound to v8 makes non-Technology 2008–2010 a registered window. v2 therefore reads none of that window.

The receipt's input panel is the committed v1 replication npz (sha256 `d0b6968c…`). The calibration code is `scripts/e0_calibrate_outcome_model.py`, and the method and results are in its report, `GRID-E0-CALIBRATION-20261001.md`. Nothing here is a trading signal or an effect-size estimate.

## VS1 v8 cross-check: information, not a gate

v8's binding Stage-0 is fixed by its registered body: E0 v1 `factor_t_garch_exposed` plus the v1 Gaussian simulator. v2 changes nothing about it.

The scorecard runs v8's confirmatory rule (`analysis.panel_insider_density_v8.e0_power`: A90|fwd5 alone, one-sided positive p ≤ 0.05) on the committed v7 geometry. That geometry is v8's registered 2011-10-01 screen row, with 414 dates.

v1's two realistic scenarios are re-run as reproduction controls; their registered values are 0.780 and 0.730. The selected 2008-01-01 geometry (603 dates) is not committed here.

## Versioning

Every file here is pinned in `MANIFEST.sha256` (same format as v1). The released hash is an entry in `evals/RELEASED.json`. A pinned change is a new version, which needs a new sibling or a version bump plus a new released entry. Never edit `evals/e0`.
