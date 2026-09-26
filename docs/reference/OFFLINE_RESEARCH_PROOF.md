# Offline research-loop contract proof

Run `python -m scripts.demo_offline_research_proof NEW_OUTPUT_DIRECTORY`.
Run `python -m pytest tests/test_offline_research_proof.py -q`.

This is an executable **synthetic fixture**, not a finding, market backtest,
registered forward test, enabled Hermes loop, or permission to promote weights.
It makes no DB/provider call. Existing live engine behavior is unchanged.

The deterministic fixture declares four trials: planted independent signal,
independent noise, constant input, and excluded internal telemetry. Every trial
appears in the discovery ledger, including refusal/insufficient-data rows.
GRID's existing `analysis.hypothesis_tester.compute_lagged_correlation` is reused
only as its pure numeric lag-zero primitive. Its database fetch/state-changing
orchestrator is never called. Lag search is not hidden inside this proof.

Discovery accepts only discovery rows. It rejects feature known-at after decision,
labels/outcome availability crossing the holdout boundary, overlapping outcomes,
mixed horizons, missing columns, nonfinite values and unlabeled/real origins.
The full trial denominator is frozen; Bonferroni correction counts excluded and
untestable trials too. This deliberately conservative proof uses IID Pearson
p-values with synthetic independent observations, not market significance claims.

The discovery manifest is hashed and written before holdout evaluation. Holdout
tests only frozen discovery survivors with another family correction and the
same effect sign. Its horizon must match discovery. The local candidate spec
records direction, horizon, manifest and earliest forward start; its hash and
status remain `FORWARD_EVIDENCE_PENDING`, never PASSED/confirmed/promoted.
All results explicitly report zero forward evidence. No promotion API exists.
The output directory must be new, preventing accidental overwrite of a consumed
local holdout receipt. This is not a global anti-rerun enforcement service: an
operator can deliberately create another directory, and cross-run trial budgets
and holdout reuse must be enforced before any real research integration.

## Lineage and boundaries

Claude's corrected vault #101 retracts its original leaking 12/3 survivors and
reports zero nominal survivors on exploratory historical data. This proof does
not re-use that CSV or turn those retrospective rows into prospective evidence.
It implements the corrected split/purge principle and adds explicit known-at,
frozen receipts, refusal tests, and a no-promotion boundary.

#563's batched PIT changes can alter default target column order and remain a
separate dependency. #568 carries #562's evaluator migration and skipped PG
acceptance dependencies; neither is transplanted. This additive proof has no
migration, route, timer, registry persistence or production importer.

## Missing before real-data / forward integration

- Verified per-source observed, publication, known-at and revision timestamps;
  an audited point-in-time selector (not latest-vintage historical hindsight).
- Explicit raw/adjusted price basis, source identity and exchange-session/horizon
  contracts, including duplicated-source handling and abstention on missing data.
- Serial-dependence-aware null calibration, costs, target-return definition and
  sample-size/power policy; non-overlap alone does not establish independence.
- A durable cross-run trial budget and single-use holdout registry; authenticated
  code/protocol/input hashes; no repeated tuning against the same holdout.
- Prospective prediction logging before outcomes, frozen sample-size and stopping
  rules, forward scoring and an independently reviewed promotion policy.

Those requirements are intentionally unresolved here; synthetic mechanism
success does not authorize live research, learning, weights or historical rescoring.
