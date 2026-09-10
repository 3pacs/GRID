# 03 — Options tapes score zero hits: fix outcome scoring for options_flow

Branch: `claude/handoff-03-options-flow-scoring`. Lane: code, then one ops-exec rescoring run.

## Evidence

`signal_sources` holds ~223K rows with `source_type='options_flow'`, ids like
`whale_<TICKER>_<STRIKE>`, `signal_type='UNUSUAL_OPTIONS'`, `signal_value`
jsonb with `direction` (CALL/PUT), `oi_ratio`, `notional`. Across ~1,900 scored
rows there are **0 CORRECT** outcomes ever, so every options tape carries trust
0.001 and `lever_pullers.get_active_lever_events` had to floor them out
(`MIN_EVENT_TRUST = 0.02`). Tapes contributed 32,378 of 37,243 lever events in
30 days before the floor. Either the scorer never recognises the direction, or
it scores the wrong window, or the whale puller writes a direction the scorer
does not read.

## Where to look

- `intelligence/trust_scorer.py`: `_infer_signal_direction` (reads
  `signal_value["direction"]`; accepts up/long/bull/bullish/buy and
  down/short/bear/bearish/sell — **CALL / PUT are not in either list**),
  `score_pending_signals`, `EVALUATION_WINDOWS["options_flow"] = 7`,
  `MOVE_THRESHOLD_PCT = 1.0`, `_extract_price` (uses `signal_value["price"]`
  as entry — for an options row that may be the option premium or the strike,
  not the underlying spot).
- The whale/options puller that writes these rows: grep
  `whale_` and `UNUSUAL_OPTIONS` under `ingestion/` and `intelligence/`
  (`ingestion/altdata/unusual_whales*.py` or the options scanner). Confirm the
  exact keys it writes (`direction`, `spot`, `strike`, `premium`, `expiry`).
- `intelligence/lever_pullers.py::assess_motivation` already maps PUT→hedging,
  CALL with `oi_ratio>=3` or `notional>=1e6`→likely_informed.

## Design

1. In `_infer_signal_direction`, map `call`→bullish and `put`→bearish (and
  `direction` values like `CALL_SWEEP`, `BULLISH_CALL`) — add to the payload
  direction lists, with a regression test in
  `tests/test_trust_scorer_direction_inference.py`.
2. Entry price for options rows must be the **underlying** price at the signal
   date, never the premium or strike. In `score_pending_signals`, when
   `source_type in ("options_flow", "whale_options")` ignore `signal_value["price"]`
   unless the payload marks it as spot (`spot`, `underlying_price`), and fall
   through to `_get_price_near_date`.
3. Evaluation window: options tapes are 7 d. Keep 7 d but score against the
   best move within the window rather than the close at day 7 only if the
   whale puller stores an expiry; otherwise leave the window logic as is and
   document why. Do not lower `MOVE_THRESHOLD_PCT` to manufacture hits.
4. Backfill: add a `rescore_source_type(engine, source_type, since_days=120)`
   helper (parameterized SQL) that resets `outcome` to PENDING for rows of that
   source type inside the window and re-runs scoring — options rows expire after
   90 d so only the recent ones can be rescued. Guard with a dry-run flag.
5. Once merged, run the rescore once on grid-svr (detached ops-exec) and report:
   scored / correct / wrong for `options_flow`, then
   `SELECT count(*) FROM lever_pullers WHERE category='options_flow' AND trust_score > 0.02`
   (or the equivalent via `build_puller_index`). If tapes now carry real trust,
   consider raising `MIN_EVENT_TRUST` only if events flood again — report, don't
   tune blind.

## Done when

`options_flow` rows show a non-zero CORRECT rate on the rescored window,
`test_lever_map_fixes.py` and the trust-scorer test files pass, and the lever
events endpoint shows options tapes without flooding.
