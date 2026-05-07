---
source: /Users/anikdang/grid_obsidian/Architecture/ML-Inference.md
promoted_at: 2026-04-13
promoted_via: OBSIDIAN-2 (task #76)
---
# ML & Inference Layer

Machine learning model training, ensemble classification, [[Live Inference|live inference]], and the self-improving Oracle prediction engine.

## Inference Module (`inference/`, 11 files)

### Core Pipeline
- **`training.py`** — PIT-correct model training. Builds datasets from [[PIT-Store]], trains with [[Walk-Forward Backtesting|walk-forward]] validation to prevent [[PIT Store|lookahead bias]]. Supports XGBoost, RandomForest, RuleBased.
- **`ensemble.py`** — `EnsembleClassifier` combining multiple models via weighted probability averaging. Default weights: XGBoost 0.45, RF 0.30, RuleBased 0.25. Includes disagreement detection — reduces confidence when models conflict.
- **`live.py`** — `LiveInference` runs production models against latest PIT data. Loads features via [[Feature-Lab]], generates recommendations per layer.
- **`trained_models.py`** — `TrainedModelBase` ABC and concrete implementations (XGBoost, RandomForest, RuleBased).

### Supporting
- **`calibration.py`** — Probability calibration for model outputs
- **`circuit_breaker.py`** — Inference circuit breaker (kills models behaving erratically)
- **`failure_analysis.py`** — Post-failure analysis of model errors
- **`trade_logger.py`** — Logs trade decisions from inference output
- **`tuning.py`** — Hyperparameter tuning
- **`timesfm_service.py`** — TimesFM forecasting service integration

## Oracle Engine (`oracle/`, 14 files)

Self-improving prediction loop. Every prediction logged with full provenance, scored against reality after expiry.

### Core
- **`engine.py`** — `OracleEngine`: signal assembly → anti-signal check → model ensemble → prediction generation → immutable logging → post-expiry scoring → weight evolution
- **`psi_oracle.py`** — PSI Oracle: Planetary Stress Index market timing. Proven configs: GLD PSI<5.25+VIX<22 → Sharpe 2.59, QQQ PSI>2.00 → Sharpe 2.01
- **`ensemble.py`** — Oracle ensemble combining multiple oracle models
- **`model_factory.py`** — Creates oracle model instances by name
- **`model_evolver.py`** — Evolves model weights based on track record (winning models get more weight)
- **`signal_aggregator.py`** — Aggregates signals from all sources for oracle input
- **`calibration.py`** — Oracle prediction calibration
- **`scoreboard.py`** — Shared scoreboard helpers for predictions
- **`publish.py`** — Publishes comparable oracle records
- **`report.py`** — Formatted prediction digest with anti-signals
- **`run_cycle.py`** — Runs one Oracle cycle: score → evolve → predict → report
- **`forecaster_adapter.py`** — Adapts TimesFM/AutoBNN forecasts for oracle use
- **`astrogrid_universe.py`** — Canonical [[AstroGrid]] scoring universe definitions

## Alpha Research (`alpha_research/`, 21 files)

Evolutionary factor mining and signal validation.

### Core
- **`conviction_scorer.py`** — Cross-ticker conviction scoring
- **`debate.py`** — Agent debate system for thesis evaluation
- **`ensemble.py`** — Alpha ensemble combining factor signals
- **`heartbeat.py`** — Research heartbeat monitoring

### Signals (`alpha_research/signals/`, 5 files)
- `credit_cycle.py` — Credit cycle signal
- `exposure_scaler.py` — Dynamic exposure scaling
- `macro_regime.py` — Macro regime signal
- `quanta_alpha.py` — QuantaAlpha evolutionary factor signals

### Strategies (`alpha_research/strategies/`, 2 files)
- `adaptive_rotation.py` — Adaptive sector rotation strategy

### Data (`alpha_research/data/`, 4 files)
- `panel_builder.py` — Build analysis panels from multiple features
- `shares_tracker.py` — Share count tracking for split adjustment
- `split_adjuster.py` — Price split adjustment

### Validation (`alpha_research/validation/`, 3 files)
- `gauntlet.py` — 5-test validation gauntlet for new factors

## Timeseries Forecasting (`timeseries/`, 4 files)

- **`timesfm_forecaster.py`** — TimesFM (Google) time series forecasting
- **`autobnn.py`** — AutoBNN (Bayesian Neural Network) forecasting
- **`_model_pool.py`** — Model pool management for forecasters

**Known issue**: TimesFM is essentially a coin flip for mean-reverting series (49.9% accuracy across 71 hypotheses). Use analog engine as primary, TimesFM only for comparison.

## Physics Engine (`physics/`, 8 files)

Market physics analogies for signal generation:
- **`momentum.py`** — Momentum measurement (velocity, acceleration)
- **`dealer_gamma.py`** — [[Dealer Gamma|Dealer gamma]] exposure calculation
- **`news_energy.py`** — News "energy" scoring (kinetic/potential analogy)
- **`waves.py`** — Wave analysis (interference, resonance)
- **`transforms.py`** — Signal transforms (Fourier, wavelet)
- **`conventions.py`** — Physics domain conventions
- **`verify.py`** — Physics calculation verification

## Related

- [[Data-Pipeline]] — How data reaches the ML layer
- [[Feature-Registry]] — Features that feed models
- [[Modules/TimeSeries]] — TimesFM and AutoBNN forecasting modules
- [[PIT-Store]] — Point-in-time correctness guarantee
- [[Trading-Layer]] — Where predictions become trades
- [[Agents-System]] — LLM agents that use model outputs
