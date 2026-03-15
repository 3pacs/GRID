# Grid Regime Discovery

## Project Description

Systematic study of regime-switching behavior in financial time series
using public market indicators.

## Goal

Find the minimum feature set that reliably identifies distinct market
states in historical data.

## Baseline

Random regime assignment (chance-level classification).

- **Method:** Random assignment to k states
- **Persistence Score:** 0.25 (chance level)
- **Silhouette Score:** 0.0

## Primary Metric

**Cluster persistence score** — average regime duration divided by total
periods. Higher is better (regimes should be stable, not flickering).

## Secondary Metrics

- Silhouette score (cluster separation quality)
- Transition recall (how well transitions are detected)
- Out-of-sample stability (do regimes generalise to unseen periods?)

## Important Note

This project studies regime structure only. No trading signals, no
entry/exit logic, no position sizing. All indicators used are publicly
available macroeconomic and market data.

## Contribution Guide

1. **One variable at a time.** Change one thing per experiment.
2. **Report failures.** Negative results are valuable.
3. **Clean baselines.** Every experiment must compare to random assignment.
4. **Public data only.** Use only publicly available indicators.
5. **No signal logic.** Do not include or derive trading signals.
6. **Log everything** using the standard experiment format in LEADERBOARD.md.
