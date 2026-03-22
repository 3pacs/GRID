# Model Governance — Lifecycle & Gate Checks

## State Machine

Every model in GRID progresses through a strict lifecycle:

```
CANDIDATE → SHADOW → STAGING → PRODUCTION → FLAGGED → RETIRED
                                    ↑           │
                                    └───────────┘  (recovery)
```

### State Definitions

| State | Meaning | Risk |
|-------|---------|------|
| CANDIDATE | Newly created from a passed hypothesis | No live exposure |
| SHADOW | Running in parallel with production, outputs logged but not acted on | No live exposure |
| STAGING | Pre-production validation, final gate checks | No live exposure |
| PRODUCTION | Active model driving recommendations | Live exposure |
| FLAGGED | Production model under review (drift, poor performance) | Exposure paused |
| RETIRED | Permanently decommissioned | No exposure |

### Constraints

- **One PRODUCTION per layer** — Enforced by database unique index
- **Three layers**: REGIME, TACTICAL, EXECUTION
- When promoting a new model to PRODUCTION, the existing PRODUCTION model is
  automatically demoted to RETIRED
- All transitions are logged with timestamp, operator_id, and reason

## Gate Checks

Before any promotion, `GateChecker` validates:

### CANDIDATE → SHADOW
- Walk-forward backtest completed with ≥5 splits
- Positive Sharpe ratio in majority of eras
- Beats buy-and-hold baseline

### SHADOW → STAGING
- Shadow period ≥ configured duration
- Shadow predictions tracked against actuals
- No significant drift detected

### STAGING → PRODUCTION
- All previous gates passed
- Simplicity comparison: new model must justify added complexity
- Baseline comparison: must beat the simpler alternative
- Manual approval (single-operator system)

## Rollback

If a PRODUCTION model is FLAGGED:
- Can be rolled back to previous PRODUCTION model
- Or recovered (FLAGGED → PRODUCTION) after investigation
- All rollback events logged

## Integration with Autoresearch

```
Autoresearch generates hypothesis
  → hypothesis PASSES backtest
  → Model created in model_registry (CANDIDATE)
  → Gate checks validate
  → Promotion through SHADOW → STAGING → PRODUCTION
```

## Key Files

- `governance/registry.py` — ModelRegistry state machine
- `validation/gates.py` — GateChecker (promotion rules)
- `api/routers/models.py` — REST endpoints (list, transition, rollback)

## Database Tables

- `model_registry` — Model versions, states, feature sets, parameters
- `validation_results` — Backtest results per hypothesis/model
- `hypothesis_registry` — Source hypothesis for each model
