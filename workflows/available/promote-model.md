---
name: promote-model
group: governance
schedule: "manual"
secrets: []
depends_on: ["validate-backtest"]
description: Evaluate and promote models through the governance state machine
---

## Steps

1. Query `model_registry` for models passing validation gates
2. For each candidate promotion:
   a. Run GateChecker.check_all_gates()
   b. Verify backtest results meet thresholds
   c. If promoting to PRODUCTION, demote current PRODUCTION to SHADOW
   d. Execute transition with operator_id and reason
3. Log all decisions to `decision_journal`

## Output

- Model state transitions recorded in `model_registry`
- Immutable journal entries in `decision_journal`

## Notes

- MANUAL schedule — requires operator initiation
- State machine: CANDIDATE → SHADOW → STAGING → PRODUCTION ↔ FLAGGED → RETIRED
- Auto-demotion ensures only one PRODUCTION model per layer
