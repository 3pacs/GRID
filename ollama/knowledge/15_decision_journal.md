# Decision Journal — Immutable Audit Log

## Purpose

The decision journal is GRID's memory. Every recommendation, regime call, and
agent decision is recorded with full context at the time of the decision. Later,
outcomes are recorded to measure whether GRID helped or harmed.

## How It Works

### Recording a Decision

Every decision captures:

| Field | Description |
|-------|-------------|
| model_version_id | Which model version made the call |
| inferred_state | Regime classification at decision time |
| state_confidence | Numerical confidence (0.0–1.0) |
| transition_probability | Likelihood of regime change |
| grid_recommendation | What GRID suggested |
| baseline_recommendation | What a simple model would suggest |
| action_taken | What actually happened |
| counterfactual | "What if we're wrong?" scenario |
| operator_confidence | Human confidence overlay |

### Recording Outcomes (Later)

After sufficient time has passed:

| Field | Description |
|-------|-------------|
| outcome_value | Actual return or P&L |
| verdict | HELPED, HARMED, NEUTRAL, or INSUFFICIENT_DATA |
| outcome_annotation | Free-text notes |

### Immutability

A database trigger (`enforce_journal_immutability`) blocks updates to core
decision fields after insertion. Only outcome fields can be updated later.

**Protected fields** (cannot be changed after insert):
- decision_timestamp, model_version_id, inferred_state, state_confidence
- transition_probability, grid_recommendation, baseline_recommendation
- action_taken, counterfactual, operator_confidence

This prevents retroactive modification of the decision record.

## Performance Analysis

`get_performance_summary()` computes:
- Total decisions, helped count, harmed count, neutral count
- Helped rate by regime state
- Helped rate by operator confidence level
- Time series of outcomes

This enables honest self-assessment: "Is GRID actually adding value, and in
which market conditions?"

## Verdict Assignment

| Verdict | Meaning |
|---------|---------|
| HELPED | GRID's recommendation outperformed baseline |
| HARMED | GRID's recommendation underperformed baseline |
| NEUTRAL | No meaningful difference |
| INSUFFICIENT_DATA | Not enough time has passed to judge |

## Key Files

- `journal/log.py` — DecisionJournal class (log_decision, record_outcome)
- `api/routers/journal.py` — REST endpoints (list, create, update outcome)
- `schema.sql` — Table definition + immutability trigger
