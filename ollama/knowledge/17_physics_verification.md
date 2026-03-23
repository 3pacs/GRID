# Physics Verification — Market Dynamics Analysis

## Overview

GRID applies physics-inspired analysis to market data. This is not metaphorical —
these are rigorous mathematical tools from statistical mechanics and stochastic
processes applied to financial time series.

## Physics Transforms

### Energy Decomposition

| Transform | Formula | What It Measures |
|-----------|---------|-----------------|
| Kinetic Energy | ½v² (v = log returns) | Price movement speed — spikes during volatility events |
| Potential Energy | ½k(x - μ)² (spring) | Deviation from equilibrium — high PE = stretched, likely to revert |
| Total Energy | KE + PE | Overall market stress — conserved in ideal systems |
| Hamiltonian | p²/2m + V(x) | Total conserved quantity — violations signal regime changes |

### Ornstein-Uhlenbeck Parameters

Models mean-reverting processes: `dx = θ(μ - x)dt + σdW`

| Parameter | Meaning |
|-----------|---------|
| θ (theta) | Mean-reversion speed — higher = faster reversion |
| μ (mu) | Long-run equilibrium level |
| σ (sigma) | Noise/volatility of the process |
| Half-life | ln(2)/θ — time to revert halfway to equilibrium |

**Application**: When a feature's half-life is short, deviations are temporary
(mean-reverting). When long, trends persist (momentum).

### Hurst Exponent

| Value | Interpretation |
|-------|---------------|
| H < 0.5 | Mean-reverting — deviations tend to reverse |
| H = 0.5 | Random walk — no predictable behavior |
| H > 0.5 | Trending — movements tend to persist |

**Application**: Hurst exponent helps choose between momentum and mean-reversion
strategies for each feature.

### Langevin Dynamics

Decomposes price movements into:
- **Drift** — Deterministic component (trend)
- **Diffusion** — Stochastic component (noise)

When drift dominates diffusion, trends are reliable. When diffusion dominates,
it's noise.

### Fokker-Planck

Estimates the stationary probability density of a feature. Tells you where the
feature "wants to be" in the long run and how likely extreme values are.

## Market Physics Verifier

Runs verification checks on GRID's data and models:

| Check | What It Validates |
|-------|------------------|
| Conservation | Capital flows net to zero (inflows = outflows) |
| Dimensional analysis | Units are consistent across calculations |
| Limiting cases | Behavior at extremes (zero rates, infinite vol, negative yields) |
| Regime boundaries | Transitions are physically plausible |
| Stationarity | ADF tests on features — non-stationary features need differencing |
| Numerical stability | No NaN/inf propagation through the pipeline |

Returns `VerificationResult` with score (0.0–1.0) and warnings.

## Financial Conventions

14 domains with locked conventions:

| Domain | Unit | Annualization | Day Count |
|--------|------|--------------|-----------|
| Rates | percent | Yes | ACT/360 |
| Spreads | basis points | No | N/A |
| Returns | decimal | Yes | 252 trading days |
| Volatility | percent | Yes | √252 scaling |
| Momentum | z-score | No | N/A |
| Flow | USD millions | No | N/A |
| Macro | level or YoY% | No | N/A |
| FX | decimal | No | N/A |
| Commodity | USD/unit | No | N/A |

Convention locking prevents unit mismatches when combining features from different
families.

## Key Files

- `physics/verify.py` — MarketPhysicsVerifier
- `physics/transforms.py` — Energy, OU, Hurst, Langevin, Fokker-Planck
- `physics/conventions.py` — Convention registry (14 domains)
- `api/routers/physics.py` — REST endpoints
