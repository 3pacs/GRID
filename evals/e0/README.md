# E0: machinery-calibration benchmark (v1)

E0 is the first eval of the GRID evals plan (milestone M0). It checks whether the VS1
panel research machinery can find what's there and ignore what isn't. The statistical
path is the one VS1 discovery uses:

- a per-date Spearman rank IC;
- the data-driven block and block sign-flip null;
- Holm at the S11 ledger alpha, with BH over every declared trial.

E0 scores three things:

1. **Power:** the detection rate of a planted rank IC (grid 0.005 to 0.05) in the primary trial.
2. **Empirical FDR / FWER:** under exact nulls, compared with the declared q = 0.10 (BH) and alpha = 0.05 (Holm).
3. **Null p-value calibration:** a KS distance from U(0,1), and the empirical size at 0.01, 0.05 and 0.10.

It also runs a **known-effect replication** on real prices: weekly and one-month reversal, and 12-1 momentum.

E0 grades the machinery; it does not re-implement it. `machinery.py` calls
`analysis.panel_insider_density.measure_trial` and `analysis.offline_research_proof.holm_adjusted`
/ `bh_adjusted` directly. Every scorecard records the sha256 of those files.

## Data rules (research integrity)

- **Planted and null runs** use the real VS1 v7 Technology panel **structure**, in
  `data/vs1_v7_technology_structure.npz`:
  - 202 price-admitted issuers;
  - 2011-10-01..2019-12-31 proxy sessions;
  - four declared trials;
  - Form 4 insider-buy density and abstentions, anonymised.

  It is rebuilt by `extract_structure.py` from SEC files only. Outcomes are **synthetic**:
  a daily factor model with market and industry factors, GARCH volatility clustering and
  Student-t tails, plus an optional style tilt toward the feature. No price, return or IC
  of the VS1 universe in any VS1 window is read or computed.
- **The replication** uses real split- and dividend-adjusted TIINGO closes. They are
  restricted to before 2011-01-01 and to non-Technology companies, and are cached in
  `data/replication_pre2011_nontech_grid5.npz`. `replication.py` refuses any date on or
  after 2011-01-01 and any ticker in `data/vs1_technology_denylist.json`.

## Commands

```bash
python -m evals.e0 verify                                    # manifest check
python -m evals.e0 run --out DIR --profile full --jobs 3     # full run (about 20-40 min)
python -m evals.e0 run --out DIR --profile ci                # small N, about a minute
```

`run` verifies the manifest first. It writes `DIR/scorecard.json` once and refuses to overwrite it.
The same config and seeds give a byte-identical scorecard.

## Frozen and versioned: who may change what

- Every file under `evals/e0/` is pinned by sha256 in `MANIFEST.sha256`: code, `config.json`
  (seeds, IC grid, scenarios, selection rules) and data.
- `tests/test_e0_manifest_guard.py` fails CI in two cases:
  - a pinned file differs from the manifest;
  - the manifest differs from the one released for its version (`RELEASED_MANIFESTS`, append-only).
- Proposers (agents, the hill-climbing harness, any machinery change) may **import and run**
  E0. They may **not edit** it: a machinery change is judged by E0 and must not degrade the scorecard.
- Changing E0 itself is a new benchmark version and an owner-approved event:
  1. Bump `evals.e0.VERSION` and `config.json` to `e0-vN`.
  2. Run `python -m evals.e0 manifest --write --version e0-vN`.
  3. Append the new manifest hash to `RELEASED_MANIFESTS`.
  4. Keep old versions runnable from their commit.
- Recommended: put `evals/` and `tests/test_e0_*` under CODEOWNERS review by the owner.

Nothing here is a trading signal.
