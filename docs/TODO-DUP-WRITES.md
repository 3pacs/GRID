# URGENT: Fix Duplicate Prediction Writes

**Status:** OPEN — high priority
**Discovered:** 2026-04-28 during [[Walk-Forward Backtesting|walk-forward]] audit
**Owner:** Anik (decisions) + agent (implementation)

## The Problem

Oracle re-writes the same logical prediction many times per actual trade event:

| prediction_type | unique_events | total_rows | dup_factor |
|---|---|---|---|
| astrogrid       | 15            | 2,709      | **180×**   |
| direction       | 1,696         | 38,944     | **23×**    |

A single QQQ CALL prediction (entry $562.58 → actual $664.23, +18.07% PnL) is
recorded as **590 separate rows** in `oracle_predictions`. The
`ON CONFLICT (id) DO NOTHING` guard in the INSERTs only catches dedup on the
random UUID primary key, not on the natural identity of the trade event.

### Why this matters for profitability

Every metric is inflated. Pre-fix [[Walk-Forward Backtesting|walk-forward]] reported:

- astrogrid: 93% hit rate, +8.6% mean PnL (FAKE — n=15, one big winner echoed 590×)
- direction: 29.3% hit rate (less inflated, but still 23× counted)

The dedup-aware report (`scripts/walk_forward_profitability.py` after the
2026-04-28 patch) collapses to unique events. But the underlying data is
still bloated, and the duplicate writes also waste DB rows + compute.

### Why dedup-by-id doesn't catch this

Each call to `store.astrogrid.AstroGridStore.save_prediction` (and the
oracle publish path) generates a **fresh UUID** for `prediction_id` /
`oracle_prediction_id`. So 180 different UUIDs → 180 distinct primary keys
→ ON CONFLICT never fires. The duplicate detection has to use a natural key.

## Plan

### Step 1 — Decide the dedup window (PRODUCT CALL — needs Anik)

Pick one:

- **A. One prediction per (ticker, direction, expiry) per day.** Re-running
  oracle_cycle within the same UTC day for an already-predicted trade is a
  no-op. New day → new prediction allowed (so you can update conviction
  daily).
- **B. One prediction per (ticker, direction, expiry) ever.** Once predicted,
  never re-predicted. Simplest, but loses the ability to update confidence
  as new signals arrive.
- **C. One prediction per (ticker, direction, expiry, model_version).**
  Allows re-prediction when the model changes (e.g., trace_evolver mutates
  weights). Cleanest semantics for the [[Postmortem|post-mortem]] feedback loop.

**Recommendation: option C** — it lines up with the trace_evolver flow.
A model change → fresh prediction; otherwise → no-op.

### Step 2 — Implement the dedup key

Pick the level:

- **DB-level (preferred):** add a UNIQUE INDEX on the dedup tuple, then
  rewrite each INSERT to `ON CONFLICT (...) DO UPDATE SET confidence = EXCLUDED.confidence`
  (or DO NOTHING if you don't want updates).
- **App-level fallback:** add a `SELECT EXISTS` probe before each insert in
  `store.astrogrid.save_prediction`, `oracle.publish.publish_astrogrid_prediction`,
  and `oracle.publisher_gate.publish_astrogrid_prediction`.

```sql
-- Step 2 migration (option C with daily window):
CREATE UNIQUE INDEX CONCURRENTLY oracle_predictions_dedup_key
ON oracle_predictions (
    ticker, direction, expiry, prediction_type, model_version,
    DATE(created_at)
);
```

Then update INSERTs:

```sql
INSERT INTO oracle_predictions (...) VALUES (...)
ON CONFLICT (ticker, direction, expiry, prediction_type, model_version, DATE(created_at))
DO UPDATE SET
    confidence = GREATEST(EXCLUDED.confidence, oracle_predictions.confidence),
    signals = EXCLUDED.signals,
    -- only bump fields when the new prediction has stronger conviction
    updated_at = NOW();
```

### Step 3 — Find every INSERT path

Audited 2026-04-28:

1. `oracle/engine.py:1992` — `OracleEngine.generate_predictions` direct INSERT
2. `oracle/publish.py:97` — `publish_astrogrid_prediction` (legacy?)
3. `oracle/publisher_gate.py:219` — `publish_astrogrid_prediction` (current)
4. `intelligence/obsidian_agent.py` — search for INSERT INTO oracle_predictions

All three of #1-#3 must be updated to the new ON CONFLICT clause. #4 needs
audit (may not write to oracle_predictions directly).

### Step 4 — Backfill / clean historical data

Two options:

- **Soft (recommended):** add a `dedup_keep` boolean column. For each
  duplicate cluster, set `dedup_keep=true` on the earliest row and
  `dedup_keep=false` on the rest. [[Walk-Forward Backtesting|Walk-forward]] + dashboards filter on
  `dedup_keep=true`. No data loss, fully reversible.
- **Hard:** `DELETE` the duplicates. Faster queries but irreversible. Don't
  do this until you're confident the dedup logic is right.

```sql
-- Soft backfill (option C window, model_version included):
WITH dedup_ranked AS (
    SELECT id,
           ROW_NUMBER() OVER (
               PARTITION BY ticker, direction, expiry, prediction_type,
                            model_version, DATE(created_at)
               ORDER BY created_at ASC
           ) AS rn
    FROM oracle_predictions
)
UPDATE oracle_predictions op
SET dedup_keep = (dr.rn = 1)
FROM dedup_ranked dr
WHERE op.id = dr.id;
```

### Step 5 — Verify

After deploy:

1. Watch new INSERTs. Count `total_rows / unique_events` for predictions
   created since the deploy timestamp. Should be ~1.0.
2. Daily [[Walk-Forward Backtesting|walk-forward]] `dup_factor` field (added 2026-04-28) should drop
   from 180/23 → ~1.0 within 7 days as old data ages out of the 90d window.
3. `walk_forward_profitability` log line warns when `dup_factor > 2.0` —
   absence of that warning = fix landed correctly.

### Step 6 — Stop the bleed in the meantime

Until Step 1-5 ships, we're flying blind on the directional model's true
performance. Two protective moves:

- **Pause production sizing on `prediction_type = direction`.** 29.3% hit
  rate (deduped) is still worse than coin-flip — don't risk capital on it.
- **Keep astrogrid at minimum sizing.** n=15 unique events is too small for
  any verdict.

## Estimate

- Step 1 (decision): 5 min, Anik
- Step 2 (migration + INSERT updates): 2-3 hours
- Step 3 (audit all paths): 30 min
- Step 4 (backfill): 30 min compute + 15 min QA
- Step 5 (verify): 15 min same day, then watch for 24-48h
- **Total: ~4 hours of focused work + a few hours of monitoring**

## Notes for the agent picking this up

- The dedup-aware [[Walk-Forward Backtesting|walk-forward]] report is already live (2026-04-28). It will
  print honest numbers as soon as it runs. So you can iterate on the
  dup-write fix WITHOUT first fixing the report — they're decoupled.
- Don't drop the existing UUID primary key. Add the natural-key UNIQUE
  INDEX as a SECOND constraint. Some downstream code ([[Postmortem|postmortem]],
  trace_evolver) joins on `id`, breaking that breaks the feedback loop.
- The astrogrid 180× duplication is so much higher than direction's 23×
  because astrogrid runs on a faster cadence (likely every cycle, ~5min vs
  every 6h for oracle). If you're option-C, the model_version inclusion
  may not be enough — astrogrid may need a tighter dedup like
  `EXTRACT(HOUR FROM created_at)` or just `(ticker, direction, expiry,
  prediction_type)` without time at all.
