# Orthogonality rebuild: count independent bets, not correlated feature levels

Status: scope, 2026-11-12. Nothing in it has been built yet. Phase 2 needs a
pre-registration, in the same form as `docs/research/prune-rule-v1-preregistration.md`,
committed before anyone computes its numbers.

## What the current audit actually does (measured 2026-11-11/12)

| Piece | Runs | Reads | Who acts on it |
|---|---|---|---|
| `OrthogonalityAudit.run_full_audit` | daily, `grid-analytics-snapshots` 10:46Z | 117 of 2,715 features, as **price levels** | UI only (Discovery, Snapshots) |
| `get_orthogonal_features` | every 6h, `smart_heatmap` | pairwise `corr()` on levels; drops the higher id of each pair | Associations heatmap only |
| `signal_cooccurrence_history` lift | read live by `should_i_trade` | 490 pair rows from one bootstrap on 2026-05-17 | **conviction**, ±25% (now neutral when stale, #1138) |
| `discovery/clustering.py` | was daily, off the schedule from this PR | 19 FX/crypto features | UI only |

Why these numbers don't mean anything:

- **Levels, not returns.** Correlating price levels makes any two trending series look
  related. That explains 536 pairs flagged above 0.8 and 6,509 of 6,786 pairs flagged
  "unstable".
- **The picker keeps dead series.** "Lower id wins" means frozen `hy_spread` (id 103) is
  kept and live `hy_oas_spread` (id 18150) is dropped.
- **The lift is built on the wrong unit.** Its "signals" are per-ticker features
  (`aapl_full`, `aapl_pcr`, …). Only 6,125 deduplicated scored oracle rows exist in 365
  days, and the pair stats don't carry over to other tickers.
- **The bootstrap can't safely be re-run.** `record_joint_prediction` increments counters
  with no per-prediction ledger, so re-running it double-counts.

## The question worth answering

GRID multiplies about 12 conviction layers together, and the convergence layer boosts a
trade when several signal streams agree. Both assume the inputs are independent. The
useful output is therefore **how many independent bets a decision actually rests on**.
That question has a live consumer, and its answer can be checked against outcomes.

There is now data to answer it: `signal_source_outcomes` (`sig-eval-4-sessions`, since
2026-11-06).

- **Size:** 1,035,101 rows across 22 stream types.
- **Usable outcomes:** CORRECT 178k, WRONG 173k, NO_MOVE 91k; INELIGIBLE 593k don't count.
- **Columns:** ticker, signal_date, direction, horizon, and the cost-adjusted return.
- **Largest streams:** quiverquant:offexchange 344k, options_flow 301k, gov_contracts 232k,
  insider 44k, wsb 41k.

## Phases

### Phase 0: stop the audit misleading (small, no new data)

- `discovery/orthogonality.py`: correlate on returns or first differences (`:235`, `:576`).
- Drop any series with no change over the trailing window: a flat-line guard in
  `discovery/matrix_guard.py`, also called inside `get_orthogonal_features`.
- When two series are redundant, keep the fresher one with more coverage, not the lower
  id (`:605`).
- Measure missingness over the trailing window, not 5 years (`:193`).
- **Done when:** the picker never keeps a series that has been stale for more than 10
  days over a live one, and the count of "unstable" pairs drops below half of all pairs on
  the 11-12 matrix.

### Phase 1: how redundant are the signal streams (data exists now)

A weekly job, read-only on its inputs, over a rolling 90-day window of
`signal_source_outcomes` (eligible rows only):

1. **Agreement within a cell.** For every (ticker, signal_date, horizon) where two stream
   types both fired, how often they agreed in direction, and the correlation of their
   signed cost-adjusted returns.
2. **Effective number of streams.** Build a daily P&L series per stream (mean signed
   cost-adjusted return), take the correlation matrix, and report its participation ratio
   (Σλ)² / Σλ². Compare that with the raw stream count.
3. **Redundant groups.** Hierarchically cluster streams on 1 − |ρ|. This is where
   clustering is actually useful: grouping streams, not market days.

Output: one `analytical_snapshots` row (`signal_stream_independence`) with the matrix,
the effective N and the groups.

Consumers, which are what makes this more than a report:

- **Convergence layer** (`intelligence/signal_convergence_scanner`): count agreeing
  *groups*, not streams. Ship it behind a default-off flag, the same pattern as
  `GRID_TRUST_PRUNE_NO_EDGE`.
- **`prune_rule_v1` / `edge_report.py`:** report edge per group as well as per stream, so
  the operator can see that two "EDGE" streams are really one bet.

### Phase 2: do the conviction layers earn their place (needs a log first)

Layer multipliers are computed inside `should_i_trade` and thrown away. Nothing persists
them, and `universe_ranking_history` has 9 rows.

1. **Log every decision.** Add an append-only `conviction_decision_log`: ticker, horizon,
   direction, as_of, each layer's raw value and clamped multiplier, and the final
   conviction. It is written by `should_i_trade` and never updated, like the journal.
2. **Score each decision later** from completed closes, using the `store.observations`
   close contract that `scripts/backfill_journal_verdicts.py` already uses.
3. **Look once, after a pre-registered N** (proposal: 300 scored decisions, or 90 days):
   - layer-by-layer correlation of the multipliers;
   - per-layer ablation: the out-of-sample Brier or log-loss of the final conviction with
     and without each layer.
4. **What it decides:** layers with no marginal value go on a candidate list. As with the
   prune list, nothing changes until an operator approves it.

### Phase 3 (optional): co-occurrence lift on the right unit

- Rebuild `signal_cooccurrence_history` from stream groups (phase 1). Each run fully
  rebuilds the table over a rolling window in one transaction, so nothing is incremented
  twice.
- **Gate:** the lift may leave 1.0 only if it improves out-of-sample Brier against
  lift = 1 on a held-out month. Otherwise it stays neutral. That keeps it from becoming
  vanity again.

## What clustering is still good for

- **Not regime labels.** `scripts/auto_regime.py` owns those, and the journal grades them.
- **Not analogs.** `intelligence/historical_scenario_library.py` and
  `intelligence/pattern_library.py` already do analog matching.
- **Grouping signal streams** (phase 1, step 3): this is the useful job.
- **Concentration by co-movement** instead of sector label
  (`universe_ranker.detect_sector_concentration`). This only matters once something holds
  several names at once; today the GEX trader is SPY-only. Defer it.

## Decisions for Anik

1. Phase order: 0 → 1 → 2 recommended. Phase 2 starts collecting data now even though its
   look is months out.
2. Phase 2's look size and metric: pre-register before collecting.
3. Whether the stream-group count replaces the stream count in the convergence layer
   (flag default off until the phase 1 numbers are in).
4. Whether to delete `discovery/clustering.py`'s regime path outright once the stream
   grouping lands. This PR only takes it off the schedule.
