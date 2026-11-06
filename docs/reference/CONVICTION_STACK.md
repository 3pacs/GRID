# Conviction Stack — 13-layer adjuster chain (reference)

> Load when: touching `intelligence/signal_provenance.py`, `oracle/engine.py`, the
> calibration tables, or any per-prediction multiplier. Last updated 2026-04-14.

Every live prediction runs through
`intelligence.signal_provenance.build_provenance_report`, which stacks 13 independent
multipliers into `compute_aggregate_conviction`. All 13 are defensive — they wrap their
DB calls in try/except and return neutral `1.0` on any failure, so a missing upstream can
never break the live path.

| Layer | Module | Range | Scope |
|---|---|---|---|
| disagreement | oracle/engine | [0.60, 1.00] | per-prediction |
| fragility | oracle/engine (Shapley) | [0.50, 1.50] | per-prediction |
| red_team | intelligence/llm_red_team | [0.50, 1.00] | per-prediction |
| fudge_alerts | intelligence/cross_reference | [0.10, 1.00] | per-sector |
| cooccurrence_lift | intelligence/signal_cooccurrence | [0.75, 1.25] | per-signal-pair |
| confidence_bucket | intelligence/confidence_bucket_tracker | [0.60, 1.08] | per-horizon × 0.05-bucket |
| historical_scenario | intelligence/historical_scenario_library | [0.70, 1.10] | per-macro-snapshot |
| null_hypothesis | intelligence/null_hypothesis_forecaster | [0.50, 1.00] | per-horizon global |
| meta_learning_edge | intelligence/meta_learning_matrix | [0.40, 1.50] | per-signal × condition-cube |
| contra_indicator | intelligence/contra_indicator_ensemble | [0.85, 1.15] | global crowd |
| short_squeeze | intelligence/short_squeeze_composite | [0.90, 1.15] | per-ticker |
| prediction_market_arb | intelligence/prediction_market_arbitrage | [0.95, 1.10] | per-ticker × horizon |
| convergence | intelligence/signal_convergence_scanner | [0.92, 1.25] | per-ticker × direction × 7d |

Run `python3 -m scripts.audit_conviction_stack` for the full offline puzzle map
(taxonomy, entry points, [[Orthogonality Audit|orthogonality]] hypothesis per layer,
redundancy check). Run `python3 -m scripts.call_a_trade` to see a worked TSM example with
every adjuster shown in the `adjusters:` ticket line.

**Data state on grid-svr as of 2026-04-14:** 31,793 oracle_predictions · 1,312 scored ·
61k signal_sources · 2.2M [[Resolved Series Table|resolved_series]] (1947→2026) · 1,188
eligible features. Calibration tables populating: per_signal_brier=1 (aggregate only —
oracle doesn't yet write Shapley contributions), confidence_bucket=3,
signal_cooccurrence=410, regime_brier=0 (blocked on oracle enrichment), meta_learning=0
(same block).

**Status (checked 2026-11-06, superseding the old "known gap"):** since 2026-04-14
(`923c019f`) `OracleEngine._store_predictions` writes
`signals.{regime,fci_regime,vix_level,signal_contributions}` on every row. Production:
100% of rows written 10-21 → 11-02 carry a non-empty `signal_contributions`, as do
2,283,710 of 2,314,508 scored rows. Two real defects remained:

- **Lossy contributions (fixed 2026-11-06).** The dict was `{s.name: s.weight}`, so ten
  `sector_network` items collapsed to the last weight, and values were not normalized.
  New rows now sum duplicate names and normalize to 1.0
  (`oracle.prediction_context.aggregate_signal_weights`), stamped
  `signals.signal_contributions_basis = "signal_weight_share_v2"`. Stored rows are not
  rewritten. Readers (`bootstrap_per_signal_brier`, `regime_conditional_brier`)
  already renormalize, so old rows stay readable.
- **No live consumer (open).** `features.per_signal_brier.record_scored_prediction` is
  called only by `scripts/bootstrap_per_signal_brier.py`. `per_signal_brier_history`
  was last updated 2026-05-16 (183 rows). The calibrators stay frozen until the scorer
  (`score_expired_predictions`) or a scheduled replay feeds them.
