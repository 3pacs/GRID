# GEX P3 pre-registration: intraday GEX hypothesis family v1

Written 2026-10-01, before any outcome for any hypothesis below exists or was examined. No forward
record has been written. The UTF-8 LF body strictly between the markers is immutable once this file
is merged; its SHA-256 is pinned in `analysis/gex_intraday_prereg.py`. Any change to the body is a
new version (v2) with a new registry, a new witness file and a new start. Promotion is prohibited:
nothing here may change a weight, a signal, a briefing or an order.

Author: Claude (Opus 5.5), slice GEX-P3 (design part). Owner: Anik. Status: DESIGN REGISTERED IN GIT,
NOT ACTIVATED. Activation and the off-host witness are owner gates (section 9).

<!-- PREREG-BODY-START -->

## 0. Scope, custody and what this is not

This family is new and separate. It is not GEX-levels v1 (`docs/paper_log/gex-levels-v1-preregistration.md`,
registered 2026-09-24, code pinned at 07e1fc16, log under `/data/grid/paper_log/gex_levels_v1/`).
Nothing in this family reads, writes, re-scores, amends or replaces that log, its inputs, its code
or its 60-session evaluation. Section 6 lists the overlaps with v1 and how they are counted.

No historical outcome is used anywhere. Every hypothesis is forward only. Its first decision falls
strictly after all of: this body's registry header is witnessed off-host on vault `main`; the
owner activates the logger; and the hypothesis's own inputs and price contract are admitted
(section 4). A hypothesis whose inputs are not admitted stays `BLOCKED` and makes no prediction.

Evidence boundary: a supported verdict here is a request for independent review, not an edge,
not a promotion and not evidence of actual dealer positioning. Numerical agreement between GEX
engines (GEX-P2B) says nothing about any hypothesis in this family.

## 1. Sub-families and error control

Five sub-families, each its own multiple-testing family. They are never pooled into one test.
In particular, the price-only baseline (PO) is never tested together with a gamma-model hypothesis.

| Sub-family | Code | What it isolates | Family alpha (holdout) |
|---|---|---|---:|
| Price-only baseline | PO | What price alone already explains (the comparator) | 0.05 |
| Delayed-wall response | DW | Price response at published or recomputed delayed walls | 0.05 |
| Modeled-gamma volatility/continuation | MG | GRID's own dealer-gamma model at a pinned engine | 0.05 |
| Structural confirmation | SC | Breadth/tick confirmation of a gamma-conditioned move | 0.05 |
| Rebalancing sensitivity | RB | Hypothetical month-end allocation pressure | 0.05 |

Two-stage, forward, disjoint split per hypothesis (the forward analogue of VS1's discovery/holdout):

1. **Discovery window.** The first `n_discovery` valid observations after the hypothesis's first
   decision. One look, at exactly `n_discovery`. Selected if the statistic has the registered
   sign and one-sided p <= 0.10.
2. **Holdout window.** The next `n_holdout` valid observations, all with decisions strictly after
   the discovery verdict record. Disjoint from discovery. One look, at exactly `n_holdout`.
3. **Holdout alpha.** Frozen-selection Bonferroni within the sub-family: 0.05 divided by the number
   of hypotheses of that sub-family selected in discovery. Same sign required. Hypotheses never
   selected keep p = 1 in the family's count. Unselected, blocked or stopped hypotheses close.
4. **Across sub-families.** Nothing is pooled. By the union bound, the chance of any false holdout
   pass anywhere in this family is at most 5 x 0.05 = 0.25; every report states this bound next to
   any pass. A pass in one sub-family never lends alpha to another.
5. **Global trial count.** Exactly the ten hypotheses in section 7, registered once in the E3 trial
   ledger as ten trials. No hypothesis may be added to v1. A new hypothesis is a new version.

## 2. Stage-0 power gate (before any first decision)

Before a hypothesis's first decision, a Stage-0 synthetic power computation is run and witnessed.
It uses only the registered statistic, the registered windows and the planted effect in section 7,
with synthetic outcomes drawn from the registered noise parameter. It reads no real outcome.

- Gate: synthetic power at the registered holdout alpha (assuming every hypothesis in its
  sub-family is selected, the most conservative case) must be at least 0.50 for the registered
  `n_holdout`. Seed 20261001, 2,000 simulations.
- If the gate fails at the registered `n_holdout`, the next larger value in the hypothesis's
  `n_holdout_ladder` is tried, in order. The first rung that passes becomes binding and is
  witnessed. If no rung passes, the hypothesis is `STOP_UNDERPOWERED`: it is logged as activity
  only and can never yield a verdict. No override exists.
- Back-of-envelope (normal approximation, not binding): a daily SPY trade with a planted net edge
  of 10 bp and 80 bp noise needs about 290 trades for power 0.50 at alpha 0.0167. Trade hypotheses
  therefore carry ladders measured in years. This is stated so that nobody mistakes a short
  window for evidence.

## 3. Decision instants, inputs and the engine pin

All times are America/New_York on NYSE regular sessions (`ingestion.market_calendar`). Early-close
sessions and the session after an unscheduled closure are excluded (`early_close`, `market_closed`).

- **Pre-open decision (D0).** 09:10 for session S. The decision record must be appended to the
  family log, and its anchor must be committed to the off-host witness file on vault `main` before
  09:28. Otherwise the session is excluded `late_decision`. The entry auction is 09:30.
- **Chain.** SPY in `options_snapshots_all`, one explicit `capture_batch_id` registered in
  `options_capture_batches`: the latest complete batch for SPY with `capture_completed_at` before
  D0 and `snap_date` equal to the previous NYSE session S-1. Replayed through
  `DealerGammaEngine.compute_gex_profile("SPY", S-1, capture_batch_id=...)`. Never the latest-batch
  view, never a batch first registered after D0. A batch that is missing, incomplete or not
  registered before D0 excludes the session (`no_batch`). A later replacement batch never changes
  a logged decision.
- **Engine pin (MG and SC).** GRID `physics/dealer_gamma.py` and `physics/greeks/black_scholes.py`
  with SHA-256 `a74a4df9c9a966e38a471d05bef7a6871107d841704481780dca95fe31e5095b` and
  `7affd538a299c9ac9683d798b39df6ee1accb990e2a882457ad5155c53df317d` (content of main
  `cdf1b7f7f5a3cf2f37030a9c8164203405a23ecb`; last engine change `dafc9c06`). Native parameters:
  r = 0.05, q = 0, integer calendar DTE, DTE 0 excluded, OI > 0 and IV > 0, prior completed close
  spot from `spy_close_receipt`, engine regime thresholds (`gex_normalized` > 0.5 LONG_GAMMA,
  < -0.5 SHORT_GAMMA). The logger runs an immutable archive of exactly these files and refuses if
  their hashes differ. A later engine fix, including any GEX-P2C canonical-engine decision, does not
  change this family. It can only start a new version. The GEX-P2B arithmetic evidence for this engine
  is recorded by reference only and validates arithmetic, not any hypothesis.
- **P0.** SPY's official close of S-1 from the PIT close receipt (section 4), used for every
  distance and side. The engine's own spot is recorded but never used for P0.
- **Delayed-wall and structural inputs (DW, SC).** Each needs an append-only capture with a grid-svr
  pull receipt (`available_at` = completed grid-svr pull, never the ANIK workstation clock), held
  in GRID before D0. None exists on 2026-10-01. These hypotheses are `BLOCKED` until one is
  separately built, reviewed, owner-activated and has collected its first receipt. Missing history
  stays missing; it is never backfilled.

## 4. Executable price contracts (copied from the SPY outcome-selection rule)

Every price is the price of an order that could have been sent at the decision instant. No mid,
no theoretical fill, no intraday bar print.

- **PC-OC (open auction to close auction, session S).** Entry: SPY official opening auction price
  of S (a market-on-open order submitted after D0). Exit: SPY official closing auction price of S
  (market-on-close). Return = side * (close / open - 1) minus costs.
- **PC-CC (close auction to close auction).** Entry: official closing auction price of the entry
  session. Exit: the official closing auction price of the exit session.
- **Receipt rule.** The outcome for session S uses only a price receipt for S whose `created_at`
  is at or before D = (S + 6 calendar days) 00:00Z. Use `created_at`, not `available_at`: the receipt
  row is written later than the raw pull, the trap already documented for `spy_close_v1`. If no
  eligible receipt exists at D + 1 hour, the observation is terminally `price_unavailable`. There is
  no fallback to another session, another source or a later receipt.
- **Admitted sources.** Close: `astrogrid.price_close_receipt` (`spy_close_v1` receipts, unadjusted,
  verified). Open: none is admitted on 2026-10-01. Before any PC-OC hypothesis can start, an opening
  auction source must pass an independently reviewed admission check: at least 20 sessions in which
  the candidate open equals the NYSE Arca official opening auction print, unadjusted, with a
  grid-svr receipt. Until then every PC-OC hypothesis is `BLOCKED_PRICE_CONTRACT`.
- **Costs.** E2 cost model `e2-costs-v1`, class `us_equity_etf_large`: 3 bp per side, 6 bp round
  trip, primary. A 1 bp-per-side sensitivity (the GEX-levels v1 cost) is reported, never tested.
- **Measured quantities for forecast hypotheses.** Open-to-close absolute log return
  |ln(C_S / O_S)| uses the same PC-OC prices and receipts. A forecast hypothesis has no fill and is
  not a P&L claim.

## 5. Forward logger and E2 feed (design decision)

S10's `grid-hypothesis-forward-log.timer` does not fit. S10 admits only frozen candidates from
`scripts/run_real_panel_scan.py` scans, reads features through the latest-vintage observations
adapter and labels target series. It has no capture-batch identity, engine pin, pre-open witness
deadline or auction price contract. Bolting those on would change S10's registered behavior.

Decision: a reviewed sibling logger, `gex_intraday_v1`. Its family log is written in E2's admission
format `e2-stream-v1` (header `format`, `stream = "gex_intraday_v1"`, `prereg_sha256` = this body's
SHA-256), using the S10 chain convention (`analysis.research_forward_log.ForwardLog`: canonical JSON
lines, `prev_sha256`, chained anchor file). It adds these fields to each prediction:
`hypothesis_id`, `sub_family`, `capture_batch_id`, `engine_pin`, `decision_at`, `p0_receipt_id`
and `price_contract`.

- PC-CC direction hypotheses map onto the existing E2 rule `e2.direction.v1`.
- PC-OC trades need an E2 rule for auction-to-auction intraday entry and exit, proposed as
  `e2.auction_oc.v1`. Forecast hypotheses need a forecast rule, proposed as `e2.abs_move.v1`. Both
  are E2 releases owned by the E2 lane (EVAL-E2F4). Until they exist, E2 reports those hypotheses
  as activity only, and the family's own one-look tests in section 7 decide them.
- E2 never promotes. This family never writes a weight, signal, briefing or order.

## 6. Overlap with GEX-levels v1 and with S10

- v1 H1 (pre-open regime label -> ln range, VIX control): overlaps MG1 in spirit (gamma -> volatility).
  MG1 differs in regressor (continuous `gex_normalized`), outcome (auction open-to-close absolute
  return, not the bar range) and control (price-only persistence PO2). Both are counted. Any report
  of either cites the other, and their pass bounds add.
- v1 H2/H3 (wall hold and wall-break trade on GRID engine walls, 5-minute bars): no P3 hypothesis
  re-tests them. DW uses vendor or Cboe-recomputed delayed walls and auction prices only.
- S10: no candidate scientific pair is shared.

## 7. The ten hypotheses (machine-readable, binding)

The JSON block below is the binding list. Prose elsewhere explains it; if they disagree, the block
governs and the discrepancy is a defect to be fixed in v2.

```json
{
  "family": "gex_intraday_v1",
  "registered_on": "2026-10-01",
  "first_decision_rule": "first NYSE session strictly after witnessed registry header, owner activation and hypothesis admission",
  "stage0": {"seed": 20261001, "simulations": 2000, "min_power": 0.5},
  "discovery": {"select_one_sided_p": 0.10},
  "holdout": {"family_alpha": 0.05, "correction": "frozen_selection_bonferroni_within_sub_family", "same_sign": true},
  "sub_families": ["PO", "DW", "MG", "SC", "RB"],
  "engine_pin": {
    "physics/dealer_gamma.py": "a74a4df9c9a966e38a471d05bef7a6871107d841704481780dca95fe31e5095b",
    "physics/greeks/black_scholes.py": "7affd538a299c9ac9683d798b39df6ee1accb990e2a882457ad5155c53df317d",
    "reference_commit": "cdf1b7f7f5a3cf2f37030a9c8164203405a23ecb"
  },
  "cost": {"model": "e2-costs-v1", "class": "us_equity_etf_large", "bps_per_side": 3.0, "sensitivity_bps_per_side": 1.0},
  "hypotheses": [
    {
      "id": "PO1", "sub_family": "PO", "kind": "trade", "uses_gamma": false,
      "price_contract": "PC-OC", "decision": "D0", "e2_rule": "e2.auction_oc.v1",
      "input": "P0 and SPY official open of S",
      "rule": "if |ln(O_S/P0)| >= 0.003, side = -sign(ln(O_S/P0)) (fade the overnight gap); else no trade",
      "statistic": "mean net PC-OC return per trade; one-sided t, Newey-West 5 lags",
      "direction": "positive",
      "planted_effect": {"net_bps": 10, "noise_bps": 80},
      "n_discovery": 120, "n_holdout": 290, "n_holdout_ladder": [290, 580, 1160],
      "input_status": "BLOCKED_PRICE_CONTRACT"
    },
    {
      "id": "PO2", "sub_family": "PO", "kind": "forecast", "uses_gamma": false,
      "price_contract": "PC-OC", "decision": "D0", "e2_rule": "e2.abs_move.v1",
      "input": "|ln(C/O)| of sessions S-1..S-20 from admitted receipts",
      "rule": "forecast x = ln(mean |ln(C/O)| over S-1..S-5) - ln(median |ln(C/O)| over S-1..S-20)",
      "statistic": "OLS slope of ln|ln(C_S/O_S)| on x; one-sided, Newey-West 5 lags",
      "direction": "positive",
      "planted_effect": {"slope": 0.15, "noise_log_sd": 1.0},
      "n_discovery": 60, "n_holdout": 120, "n_holdout_ladder": [120, 250, 500],
      "input_status": "BLOCKED_PRICE_CONTRACT"
    },
    {
      "id": "DW1", "sub_family": "DW", "kind": "trade", "uses_gamma": true,
      "price_contract": "PC-OC", "decision": "D0", "e2_rule": "e2.auction_oc.v1",
      "input": "ZeroGEX delayed SPY call wall and put wall captured with a grid-svr receipt before D0",
      "rule": "if O_S is within 0.25% below the vendor call wall, side = -1; if within 0.25% above the vendor put wall, side = +1; both or neither: no trade",
      "statistic": "mean net PC-OC return per trade; one-sided t, Newey-West 5 lags",
      "direction": "positive",
      "planted_effect": {"net_bps": 10, "noise_bps": 80},
      "n_discovery": 60, "n_holdout": 290, "n_holdout_ladder": [290, 580, 1160],
      "input_status": "BLOCKED_INPUT"
    },
    {
      "id": "DW2", "sub_family": "DW", "kind": "forecast", "uses_gamma": true,
      "price_contract": "PC-OC", "decision": "D0", "e2_rule": "e2.abs_move.v1",
      "input": "Cboe delayed SPY chain captured with a grid-svr receipt before D0; walls by the GEX-P2B reference wall definitions",
      "rule": "x = min distance from P0 to the reference call wall or put wall, in percent of P0",
      "statistic": "OLS slope of ln|ln(C_S/O_S)| on ln(x), controlling for PO2's x; one-sided, Newey-West 5 lags",
      "direction": "negative",
      "planted_effect": {"slope": -0.15, "noise_log_sd": 1.0},
      "n_discovery": 60, "n_holdout": 120, "n_holdout_ladder": [120, 250, 500],
      "input_status": "BLOCKED_INPUT"
    },
    {
      "id": "MG1", "sub_family": "MG", "kind": "forecast", "uses_gamma": true,
      "price_contract": "PC-OC", "decision": "D0", "e2_rule": "e2.abs_move.v1",
      "input": "pinned engine gex_normalized from the D0 capture batch",
      "rule": "x = gex_normalized (continuous)",
      "statistic": "OLS slope of ln|ln(C_S/O_S)| on x, controlling for PO2's x; one-sided, Newey-West 5 lags",
      "direction": "negative",
      "planted_effect": {"slope": -0.10, "noise_log_sd": 1.0},
      "n_discovery": 60, "n_holdout": 120, "n_holdout_ladder": [120, 250, 500],
      "input_status": "BLOCKED_PRICE_CONTRACT"
    },
    {
      "id": "MG2", "sub_family": "MG", "kind": "trade", "uses_gamma": true,
      "price_contract": "PC-OC", "decision": "D0", "e2_rule": "e2.auction_oc.v1",
      "input": "pinned engine regime from the D0 capture batch; P0; O_S",
      "rule": "gap g = ln(O_S/P0), |g| >= 0.003: SHORT_GAMMA side = sign(g) (continue); LONG_GAMMA side = -sign(g) (fade); NEUTRAL or |g| < 0.003: no trade",
      "statistic": "mean net PC-OC return per trade; one-sided t, Newey-West 5 lags; PO1 on the same sessions reported alongside, never pooled",
      "direction": "positive",
      "planted_effect": {"net_bps": 10, "noise_bps": 80},
      "n_discovery": 120, "n_holdout": 290, "n_holdout_ladder": [290, 580, 1160],
      "input_status": "BLOCKED_PRICE_CONTRACT"
    },
    {
      "id": "MG3", "sub_family": "MG", "kind": "forecast", "uses_gamma": true,
      "price_contract": "PC-OC", "decision": "D0", "e2_rule": "e2.abs_move.v1",
      "input": "pinned engine gamma_flip and gamma_flip_crossings from the D0 capture batch; P0",
      "rule": "x = 1 if gamma_flip_crossings >= 1 and |gamma_flip - P0| / P0 <= 0.0025, else 0",
      "statistic": "difference in mean ln|ln(C_S/O_S)| (x=1 minus x=0), controlling for PO2's x by OLS; one-sided, Newey-West 5 lags",
      "direction": "positive",
      "planted_effect": {"slope": 0.20, "noise_log_sd": 1.0},
      "n_discovery": 60, "n_holdout": 120, "n_holdout_ladder": [120, 250, 500],
      "input_status": "BLOCKED_PRICE_CONTRACT"
    },
    {
      "id": "SC1", "sub_family": "SC", "kind": "trade", "uses_gamma": true,
      "price_contract": "PC-OC", "decision": "D0", "e2_rule": "e2.auction_oc.v1",
      "input": "MG2 trigger plus NYSE ADVN-DECN at the prior session close captured with a grid-svr receipt",
      "rule": "MG2's trade only when sign(ADVN-DECN at S-1 close) equals the MG2 side; else no trade",
      "statistic": "mean net PC-OC return per trade; one-sided t, Newey-West 5 lags",
      "direction": "positive",
      "planted_effect": {"net_bps": 10, "noise_bps": 80},
      "n_discovery": 60, "n_holdout": 290, "n_holdout_ladder": [290, 580, 1160],
      "input_status": "BLOCKED_INPUT"
    },
    {
      "id": "SC2", "sub_family": "SC", "kind": "forecast", "uses_gamma": true,
      "price_contract": "PC-OC", "decision": "D0", "e2_rule": "e2.abs_move.v1",
      "input": "pinned engine regime; NYSE ADVN-DECN at the prior session close with a grid-svr receipt",
      "rule": "x = 1 if regime is SHORT_GAMMA and |ADVN-DECN| at S-1 close is in its top third over S-1..S-60, else 0",
      "statistic": "difference in mean ln|ln(C_S/O_S)| (x=1 minus x=0), controlling for PO2's x by OLS; one-sided, Newey-West 5 lags",
      "direction": "positive",
      "planted_effect": {"slope": 0.20, "noise_log_sd": 1.0},
      "n_discovery": 60, "n_holdout": 120, "n_holdout_ladder": [120, 250, 500],
      "input_status": "BLOCKED_INPUT"
    },
    {
      "id": "RB1", "sub_family": "RB", "kind": "trade", "uses_gamma": false,
      "price_contract": "PC-CC", "decision": "close of the third-to-last NYSE session of each month (M-3)", "e2_rule": "e2.direction.v1",
      "input": "month-to-date total returns of SPY and AGG through M-3 from admitted unadjusted closes and declared dividends",
      "rule": "if SPY MTD minus AGG MTD >= +0.03, side = -1 from close of M-2 to close of M (rebalancing sale); if <= -0.03, side = +1; else no trade",
      "statistic": "mean net PC-CC return per trade; one-sided t",
      "direction": "positive",
      "planted_effect": {"net_bps": 40, "noise_bps": 150},
      "n_discovery": 12, "n_holdout": 36, "n_holdout_ladder": [36, 72],
      "input_status": "BLOCKED_INPUT"
    }
  ]
}
```

Notes binding the block:

- `n_discovery` and `n_holdout` count valid observations: trades for `trade`, sessions for
  `forecast`, month-ends for RB1. A session with no trigger is not a trade and not an exclusion.
- "Controlling for PO2's x" means PO2's forecast variable is computed for the same session and
  entered as a regressor. It never means PO2's verdict.
- RB1 counts month-end events. Its AGG input has no admitted source on 2026-10-01 (`BLOCKED_INPUT`).

## 8. Exclusions (reason codes, never silent)

`market_closed`, `early_close`, `late_decision`, `no_batch`, `engine_unavailable` (no measured spot
or no regime), `input_blocked`, `input_unavailable` (an admitted input has no receipt before D0),
`price_unavailable` (section 4 receipt rule) and `halted` (an SPY trading halt during S). If more
than 10% of a hypothesis's sessions are excluded (not counting `market_closed`, `early_close` or
`input_blocked`) by its 30th session, that hypothesis stops (`STOP_DATA_QUALITY`). Any fix is v2.

Until a hypothesis's look, status reports activity only: decisions, trades, exclusions by reason,
and valid observations against `n_discovery` or `n_holdout`. It never reports a return, hit rate,
slope or p-value. A look taken early is a defect and invalidates the hypothesis.

## 9. Integrity and owner gates

- Registry: `analysis/gex_intraday_prereg.py`, an append-only hash-chained JSONL using
  `ForwardLog`. It holds a header (this body's SHA-256), one `preregistration` record (this block's
  canonical SHA-256 and the ten trial ids), then, per hypothesis, `stage0_power`,
  `admitted`/`blocked`, `discovery_verdict`, `holdout_verdict` or a `stop` record.
- Witness: the anchor lines go to `05-GRID/Paper-Log/gex_intraday_v1/gex_intraday_v1_prereg.anchors.jsonl`
  on vault `main`, from a worktree off `origin/main`, LF only. This is an owner gate.
- Owner gates, none of which this PR performs: writing the registry on grid-svr; the vault witness
  commit; the Stage-0 power run; admitting an opening-auction source, a delayed-wall capture,
  breadth capture or AGG source; installing or enabling any logger timer (`GRID_ENABLE_*_JOB`,
  with backup, dry-run and GO); and the E2 releases for `e2.auction_oc.v1` and `e2.abs_move.v1`.
- The frozen GEX-levels v1 log and its pinned code are not read, imported or written by any of this.

<!-- PREREG-BODY-END -->

Body SHA-256: pinned as `PREREG_BODY_SHA256` in `analysis/gex_intraday_prereg.py` and checked by
`tests/test_gex_intraday_prereg.py`. The registration record is the git commit that adds this file.
The witnessed registry header follows the owner gates above.
