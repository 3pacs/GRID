# Hypothesis Boost-Log Outcome Scoring — v1

**Author:** Claude Opus 4.7 (1M context), 2026-05-15
**Status:** active framework. Worked example below scored 20 INTC convergence rows on 2026-05-15.

## Why this doc exists

The `hypothesis_boost_log.outcome` column was 100% `'inconclusive'` across all 4,852 rows in the table as of 2026-05-15 morning, even though the linked `discovered_hypotheses` rows had ~13K invalidated + ~6K confirmed outcomes. The root cause was a direction-vocabulary mismatch: the producer side (`_hypothesis_from_convergence` at `intelligence/hypothesis_engine.py:1172`) writes the anomaly's direction verbatim — typically one of `{CALL, PUT, opposite, neutral}` — while the consumer side (`_check_ticker_move`, same file, line 1368) only matched `{bullish, up, bearish, down}`. Every check silently fell through the if-ladder and returned `'inconclusive'`, polluting downstream calibration (auto_improve STAR/NET-MISLEADING advisory, lever_pullers_density vs directional comparison, all of trust_scorer's feedback loop).

This doc is the framework for the LOCAL LLM agent to backfill outcomes on the historical 4,852 inconclusive rows, and to score new ones going forward when the mechanical normalizer in `_check_ticker_move` can't.

## Input — what the scorer reads per row

Pulled from a JOIN of `hypothesis_boost_log` + `discovered_hypotheses` + `oracle_predictions` over the eval window:

| Field | Source | Notes |
|---|---|---|
| `boost_id` | `hypothesis_boost_log.id` | row PK |
| `hypothesis_id` | `hypothesis_boost_log.hypothesis_id` | links to discovered_hypotheses |
| `is_anti` | derived: `hypothesis_id LIKE '%_anti'` | anti-hypothesis marker |
| `parent_hypothesis_id` | if `is_anti`: strip `_anti` suffix; else self | for direction inversion lookup |
| `expected_direction` | `test_criteria->>'expected_direction'` | one of {CALL, PUT, bullish, bearish, up, down, opposite, neutral, increases, decreases, …} |
| `ticker` | `test_criteria->>'ticker'` | required for convergence pattern |
| `min_move_pct` | `test_criteria->>'min_move_pct'` (default 2.0) | threshold |
| `window_days` | `test_criteria->>'window_days'` (default 14) | eval window after hypothesis creation |
| `hyp_created_at` | `discovered_hypotheses.created_at` | window start |
| `actual_avg_move` | avg of `oracle_predictions.actual_move_pct` over (ticker, hyp_created_at..+window_days) | bullish if positive |
| `actual_max_up` | max of same | best up move |
| `actual_max_down` | min of same (negative number = strongest down move) | |
| `n_preds` | count of scored predictions in window | sample size |
| `parent_thesis` | `discovered_hypotheses.thesis` of parent | for context — usually "CONVERGENCE: N sources agree X is heading {CALL,PUT}" |

## Decision rules (in this order)

### Rule 1 — Direction normalization

Map `expected_direction` to a canonical bucket:

| Raw direction | Bucket |
|---|---|
| `up`, `bullish`, `long`, `CALL`, `call`, `increase`, `increases`, `rising` | **up** |
| `down`, `bearish`, `short`, `PUT`, `put`, `decrease`, `decreases`, `falling` | **down** |
| `neutral`, `flat`, `sideways` | **neutral** → INCONCLUSIVE (no directional bet to evaluate) |
| `opposite` | **inverted** (see Rule 3) |
| `unknown`, `""`, anything else | **ambiguous** → INCONCLUSIVE |

### Rule 2 — Anti-hypothesis inversion

If `is_anti` is true AND the row's bucket is `inverted`:
- Look up the parent hypothesis (id without `_anti` suffix)
- Apply Rule 1 to the parent's `expected_direction`
- Invert the result: parent `up` → effective expectation `down`, parent `down` → effective expectation `up`
- If parent resolves to `neutral` or `ambiguous`, the anti also resolves to INCONCLUSIVE

### Rule 3 — Outcome decision

Once the effective expectation is `up` or `down`:

| Effective expectation | actual_avg_move sign + magnitude | Outcome |
|---|---|---|
| `up` | `> min_move_pct` (e.g. > +2%) | **CONFIRMED** |
| `up` | `< -min_move_pct` (e.g. < -2%) | **INVALIDATED** |
| `up` | `between -min_move_pct and +min_move_pct` | INCONCLUSIVE (no signal) |
| `down` | `< -min_move_pct` | **CONFIRMED** |
| `down` | `> +min_move_pct` | **INVALIDATED** |
| `down` | `between` | INCONCLUSIVE |

### Rule 4 — Sample-size guard

If `n_preds < 3`, return INCONCLUSIVE regardless. Not enough data to score against. Note this in the reasoning so the operator can backfill predictions later.

### Rule 5 — Reasoning template

Always emit a 1-2 sentence reasoning that names:
1. The hypothesis interpretation (`{ticker}` convergence-{direction}; anti if applicable; parent direction if inverted).
2. The observed move (`{avg_move}%` avg, `{n_preds}` preds in window).
3. The verdict relative to `min_move_pct`.

## Worked examples (the 20 INTC rows scored 2026-05-15 by Opus 4.7)

All 20 are INTC convergence hypotheses created 2026-05-01-ish, window 14 days, min_move_pct=2.0, with INTC moving +67.87% avg over 10,593–11,316 scored predictions in window. max +77.62%, min still +22.35% (the smallest move was still strongly up).

### Example A — direct CALL hypothesis (CONFIRMED)

**Input:**
- boost_id: 1061, hypothesis_id: hyp_857e6755da69fa77, is_anti: false
- ticker: INTC, expected_direction: CALL, min_move_pct: 2.0
- actual_avg_move: +67.87%, n_preds: 11316
- parent_thesis: "CONVERGENCE: 5 independent sources agree INTC is heading CALL. Sources: contagion, cross_asset, flow_momentum, fundamental, options_flow."

**Output:**
- opus_outcome: `CONFIRMED`
- opus_reasoning: "INTC convergence-CALL hypothesis: 45-source agreement on INTC heading up. Ticker moved +67.87% avg (max +77.62%) over 14-day window vs 2% threshold. Confirmed strongly."

### Example B — direct PUT hypothesis (INVALIDATED)

**Input:**
- boost_id: 989, hypothesis_id: hyp_d888ae48533a6eb2, is_anti: false
- ticker: INTC, expected_direction: PUT, min_move_pct: 2.0
- actual_avg_move: +67.87%, n_preds: 11316
- parent_thesis: "CONVERGENCE: 26 independent sources agree INTC is heading PUT."

**Output:**
- opus_outcome: `INVALIDATED`
- opus_reasoning: "INTC convergence-PUT hypothesis (26-source consensus on DOWN). Ticker moved +67.87% avg UP — direct opposite of PUT thesis. Strongly invalidated."

### Example C — anti-CALL hypothesis (INVALIDATED)

**Input:**
- boost_id: 1062, hypothesis_id: hyp_857e6755da69fa77_anti, is_anti: true
- parent_hypothesis_id: hyp_857e6755da69fa77 (direction: CALL → inverted to DOWN)
- ticker: INTC, expected_direction: opposite, effective_expectation: down
- actual_avg_move: +67.87% (ticker went UP — opposite of effective expectation)

**Output:**
- opus_outcome: `INVALIDATED`
- opus_reasoning: "INTC anti-CALL hypothesis (direction=opposite, parent=CALL). Anti inverts to DOWN expectation. Ticker moved +67.87% avg UP — opposite of anti expectation. Invalidated."

### Example D — anti-PUT hypothesis (CONFIRMED)

**Input:**
- boost_id: 990, hypothesis_id: hyp_d888ae48533a6eb2_anti, is_anti: true
- parent_hypothesis_id: hyp_d888ae48533a6eb2 (direction: PUT → inverted to UP)
- ticker: INTC, expected_direction: opposite, effective_expectation: up
- actual_avg_move: +67.87% (ticker went UP — matches effective expectation)

**Output:**
- opus_outcome: `CONFIRMED`
- opus_reasoning: "INTC anti-PUT hypothesis (direction=opposite, parent expected PUT/down). Anti inverts to UP expectation. INTC moved +67.87% avg up — matches anti expectation. Confirmed."

## Output schema

Write back to `hypothesis_boost_log`:
- `opus_outcome`: one of `CONFIRMED` | `INVALIDATED` | `INCONCLUSIVE`
- `opus_reasoning`: 1-2 sentences, see Rule 5
- `opus_scored_at`: `NOW()` at write time
- `opus_scorer_model`: e.g. `claude-opus-4-7-1m`, `qwen3.6-35b-a3b-distill`, `gpt-oss-120b`, etc.

Original `outcome` column is NOT modified (it's the truth-of-the-time value from `_evaluate_criteria`). The `opus_*` columns are the corrected/reasoned version. Downstream calibration should prefer `opus_outcome` over `outcome` for rows where it's set.

## Implementation note for local scorers

The local LLM (gridz4 / panda / ocr / koala) should be invoked via `llm.router.get_llm(Tier.REASON)` with this doc loaded as system_knowledge plus 4-8 of the worked examples above as few-shot turns. The decision is structurally regular (direction normalization → optional inversion → magnitude check) so the local model should match Opus on >95% of rows. Spot-check 1 in 50 with a frontier model and flip the boost_value calibration on the local model when accuracy drops below 95% on a rolling 200-row sample.

## See also

- `intelligence/hypothesis_engine.py:1330` — `_check_ticker_move` (the upstream mechanical path; was the bug locus)
- `scripts/score_boost_log_with_local_llm.py` — TODO, batch worker (see [[handoff-next-agent-2026-05-15]])
- `auto_improve` advisory output — currently degraded by all-INCONCLUSIVE pollution; should self-heal once enough opus_outcome rows are written
