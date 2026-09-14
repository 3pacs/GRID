# Raw SQL migration ledger

`migrations/*.sql` is a **legacy mechanism**. Nothing in CI or deploy runs it.
`.github/workflows/deploy.yml` runs `python3 -m alembic upgrade head` and only
that; every file listed below was applied, when it was applied at all, by a
person typing `sudo -u postgres psql griddb -f migrations/<file>`.

That is the defect this ledger exists to close. A migration mechanism whose
apply step is "ask a human to remember" produces exactly what it produced
here: `0062_register_gdelt_scheduled_features.sql` merged on 2026-09-11 with
its apply step recorded in a TODO ("Anik: apply it by hand after #452
merges"), was never applied, and the news card on grid.stepdad.finance stayed
empty for months because `GET /physics/momentum` could not find the features
it registers.

**Write new migrations as alembic revisions under `migrations/versions/`.**
`tests/test_raw_sql_migrations_ledger.py` fails on any `migrations/*.sql`
file that is not listed here, and its failure message says so.

## What the statuses mean

| status | meaning |
|---|---|
| `legacy-applied` | Effect verified present on griddb. Retained as history; never runs again. |
| `ported` | Was outstanding. Its effect is now carried by the named alembic revision, written idempotently so it is a no-op wherever the raw file was already applied by hand. |
| `legacy-data-fix` | A one-off data repair against specific primary keys. Not reproducible on another database and not portable to alembic. Retained as the record of what was done. |
| `legacy-superseded` | Outstanding — the effect is **not** on griddb — and deliberately not ported, because what it would create is already produced another way or has no consumer left. The evidence column says which. |

## How the audit was done

Read-only, through `.github/workflows/ops-exec.yml` against griddb on
**2026-09-14** (runs 308 and 309). For each file, its effect was looked up in
the catalog rather than assumed: `to_regclass()` for every table and index,
`information_schema.columns` for every `ADD COLUMN`, `pg_constraint` /
`pg_trigger` / `pg_proc` / `pg_extension` for the rest, and a `SELECT` on the
target table for every `INSERT`. No DDL was executed and nothing was written.

Every file numbered `0020` through `0061`, plus `add_event_bus`,
`add_pgvector_rag`, `add_pull_log`, `add_query_data_gaps` and
`add_signal_forecasts`, came back fully present — the hand-application did in
fact happen for 48 of the 52. Four did not, and they are not the same case as
each other: two are genuinely outstanding and are ported here, one is
outstanding but redundant, and one is a data fix that has nothing left to do.

## Ledger

| file | status | alembic revision | evidence (griddb, 2026-09-14) |
|---|---|---|---|
| `0020_institutional_holdings.sql` | legacy-applied | - | `institutional_holdings` present |
| `0021_supply_chain_and_capital_flows.sql` | legacy-applied | - | `supply_chain_nodes`, `supply_chain_edges`, `capital_flows` all present |
| `0022_supply_shock_attributions.sql` | legacy-applied | - | `supply_shock_attributions` present |
| `0023_alert_state.sql` | legacy-applied | - | `alert_state`, `supply_chain_edge_snapshots` present |
| `0024_capital_flows_currency.sql` | legacy-applied | - | `capital_flows.currency` present; `capital_flows_dedup_nullable_cp_key` present |
| `0025_ticker_metrics_daily.sql` | legacy-applied | - | `ticker_metrics_daily` present |
| `0026_contagion_predictions.sql` | legacy-applied | - | `contagion_predictions`, `contagion_backtest_results` present |
| `0027_supply_chain_enrichment_log.sql` | legacy-applied | - | `supply_chain_enrichment_log` present |
| `0028_sector_health_snapshots.sql` | legacy-applied | - | `sector_health_snapshots` present |
| `0029_contagion_prediction_trigger.sql` | legacy-applied | - | `contagion_predictions.trigger_news_id`, `.trigger_url` present |
| `0030_supply_chain_edge_adjustments.sql` | legacy-applied | - | `supply_chain_edge_adjustments` present; `supply_chain_edges.backtest_validated`, `.last_backtest_at` present |
| `0031_regulatory_events.sql` | legacy-applied | - | `regulatory_events` present |
| `0032_supply_chain_edge_validation.sql` | legacy-applied | - | `supply_chain_edges.relationship_weak`, `.validation_correlation` present |
| `0033_fundamental_divergence.sql` | legacy-applied | - | `fundamental_divergence` present |
| `0034_holder_deal_overlap.sql` | legacy-applied | - | `holder_deal_overlap` present |
| `0035_actor_news.sql` | legacy-applied | - | `actor_news`, `actor_bio` present |
| `0036_user_intel.sql` | legacy-applied | - | `user_intel`, `user_intel_votes` present |
| `0037_options_v2_schema.sql` | legacy-applied | - | `option_contracts_normalized`, `option_snapshots_raw`, `option_exposures` present |
| `0038_oracle_running_metrics.sql` | legacy-applied | - | `oracle_models.running_brier`, `.running_ece`, `.scored_prediction_count` present |
| `0039_synth_b_model_heads.sql` | legacy-applied | - | `oracle_models` rows `holder_overlap`, `fundamental` present |
| `0040_synth_c_seeds.sql` | legacy-applied | - | `oracle_models` row `contagion` present; `decision_journal.source_contract_id` present |
| `0041_actor_trust_or_cog.sql` | legacy-applied | - | `lever_pullers.trust_or_cog_score`, `.classification` present |
| `0042_oracle_horizon_aware.sql` | legacy-applied | - | `oracle_models.horizon_buckets` present |
| `0043_oracle_calibration_history.sql` | legacy-applied | - | `oracle_calibration_history` present |
| `0044_feature_importance_horizon.sql` | legacy-applied | - | `feature_importance_log.horizon_days` present |
| `0045_oracle_regime_buckets.sql` | legacy-applied | - | `oracle_models.regime_buckets` present |
| `0046_canvas_speed_indexes.sql` | legacy-applied | - | all 7 CONCURRENTLY indexes present and valid |
| `0047_system_health_indexes.sql` | legacy-applied | - | `idx_raw_series_pull_timestamp`, `idx_raw_series_status_source_pull` present and valid. Not portable anyway — `raw_series` is 511 GB / 1.93e9 rows |
| `0048_service_cursors.sql` | legacy-applied | - | `service_cursors` present |
| `0049_connected_dot_cards.sql` | legacy-applied | - | `connected_dot_cards` present |
| `0050_agent_reports.sql` | legacy-applied | - | `agent_reports` present; `pgcrypto` installed |
| `0051_options_rec_signals.sql` | legacy-applied | - | `options_recommendations.signals`, `.opposing_signals` present |
| `0052_transformation_version_invariant.sql` | legacy-applied | - | constraint `chk_transformation_version_positive`, trigger `trg_feature_registry_transformation_version`, function `feature_registry_check_transformation_version` all present |
| `0053_reasoning_lessons.sql` | legacy-applied | - | `reasoning_lessons` present; `vector` extension installed |
| `0053_signal_subtype.sql` | legacy-applied | - | `signal_data.signal_subtype` present |
| `0054_mini_model_outputs.sql` | legacy-applied | - | `anomaly_narratives`, `signal_knowledge_entries` present |
| `0055_community_summary.sql` | legacy-applied | - | `community_summary` present; function `refresh_community_summary` present |
| `0056_oracle_predictions_dedup_guard_index.sql` | legacy-applied | - | `oracle_predictions_dedup_unique`, `oracle_predictions_dedup_keep_created` present and valid |
| `0057_realized_alpha.sql` | legacy-applied | - | `realized_alpha_daily`, `realized_alpha_trades` present |
| `0058_horizon_days.sql` | legacy-applied | - | `oracle_predictions.horizon_days`, `universe_ranking_history.horizon_days` present |
| `0059_long_plays_board.sql` | legacy-applied | - | `long_plays_board` present |
| `0060_sponsor_ticker_map.sql` | legacy-applied | - | `sponsor_ticker_map` present |
| `0061_actors_merged_into.sql` | legacy-applied | - | `actors.merged_into` present |
| `0062_register_gdelt_scheduled_features.sql` | ported | raw_sql_port_20260914 | NOT applied: all 14 `gdelt_actor_*_tone` / `gdelt_tension_*` rows absent from `feature_registry`. The only `gdelt_*` rows present are the 13 canonical ones from `scripts/parse_gdelt.py` |
| `0063_register_ghost_mapped_features.sql` | ported | raw_sql_port_20260914 | NOT applied: all 9 rows absent (`eurusd_ecb_daily`, `shy_full`, `ief_full`, `emb_full`, `jnk_full`, `mub_full`, `smh_close`, `icln_close`, `lit_close`) |
| `add_event_bus.sql` | legacy-applied | - | `event_bus`, `task_queue` present |
| `add_missing_indexes_and_wave_tables.sql` | legacy-superseded | - | NOT applied — and every object in it is already produced elsewhere or has no consumer. `idx_decision_journal_model_version_id` duplicates `schema.sql:290`'s `idx_decision_journal_model ON decision_journal (model_version_id)`, same table and column under a different name; `idx_decision_journal_outcome_recorded_at` likewise duplicates `schema.sql:298/343`'s `idx_decision_journal_outcome_recorded ON decision_journal (outcome_recorded_at)`. `idx_resolved_series_conflict` **is** present, created by `schema.sql:160/357` — which is why it was the one object in this file the audit found. `paper_strategy_breaker_state` is absent but `trading/circuit_breaker.py::_ensure_table()` creates it with identical DDL at runtime, as the `grid` role. `agent_runs.conviction_score` / `.persona` / `.debate_rounds` and `idx_agent_runs_persona` are absent and have **no reader or writer anywhere in the tree**; `agent_runs` holds 0 rows. Porting would add two duplicate indexes and three dead columns to production |
| `add_pgvector_rag.sql` | legacy-applied | - | `actors`, `embeddings`, `icij_entities`, `attention_anomaly` present; `pg_trgm` installed |
| `add_pull_log.sql` | legacy-applied | - | `pull_log` present |
| `add_query_data_gaps.sql` | legacy-applied | - | `query_data_gaps` present |
| `add_signal_forecasts.sql` | legacy-applied | - | `signal_forecasts` present |
| `fix_source_catalog_duplicates.sql` | legacy-data-fix | - | Its goal holds: 160 catalog entries, 106 active, and **no** active `lower(name)` duplicate group. But it reached that state by a different route — the 10 loser ids the file names (41, 72, 42, 36, 34, 62, 403, 44, 39, 171) do not exist at all rather than being `active = false`, so re-running the file today would update zero rows. Two of its keepers (166 `Crucix`, 186 `defillama`) are themselves inactive now. A one-off repair against literal primary keys on one database; nothing to port |
