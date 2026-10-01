# GEX P3 pre-registration: intraday GEX hypothesis family v1

Written 2026-10-01, before any outcome for any hypothesis below exists or was examined. No forward
record has been written. The UTF-8 LF body strictly between the markers is immutable once this file
is merged; its SHA-256 is pinned in `analysis/gex_intraday_prereg.py`. Any change to the body is a
new version (v2) with a new registry, new witness files and a new start. Promotion is prohibited:
nothing here may change a weight, a signal, a briefing or an order.

Author: Claude (Opus 5.5), slice GEX-P3 (design part). Owner: Anik. Status: DESIGN REGISTERED IN GIT,
NOT ACTIVATED. Registry write, witnesses and activation are owner gates (section 10).

<!-- PREREG-BODY-START -->

## 0. Scope, custody and what this is not

This family is new and separate from GEX-levels v1 (`docs/paper_log/gex-levels-v1-preregistration.md`,
registered 2026-09-24, code pinned at 07e1fc16, log under `/data/grid/paper_log/gex_levels_v1/`).
Nothing here reads, writes, re-scores, amends or replaces that log, its inputs, its code or its
60-session evaluation. Section 7 states every overlap with v1 and how it is handled.

No historical outcome is used. Every hypothesis is forward only. Its first decision falls strictly
after all of: the registry header is witnessed on vault `main` (section 10); the owner activates the
logger; the hypothesis's inputs and price contract are admitted (section 4); its Stage-0 power gate
passed and was witnessed (section 2); and, for MG1, MG2, SC1, SC2 and DW1, the separation rule of
section 7. A hypothesis whose inputs are not admitted is `BLOCKED` and makes no prediction.

Evidence boundary: a supported verdict is a request for independent review, not an edge, not a
promotion and not evidence of actual dealer positioning. Numerical agreement between GEX engines
(GEX-P2B) validates arithmetic only and says nothing about any hypothesis here.

## 1. Sub-families and error control

Five sub-families, each its own multiple-testing family. They are never pooled into one test; the
price-only baseline (PO) is never tested together with a gamma-model hypothesis.

| Sub-family | Code | Registered hypotheses k | Holdout alpha per hypothesis |
|---|---|---:|---:|
| Price-only baseline | PO | 2 | 0.05 / 2 = 0.025 |
| Delayed-wall response | DW | 2 | 0.05 / 2 = 0.025 |
| Modeled-gamma volatility/continuation | MG | 3 | 0.05 / 3 = 0.0167 |
| Structural confirmation | SC | 2 | 0.05 / 2 = 0.025 |
| Rebalancing sensitivity | RB | 1 | 0.05 / 1 = 0.05 |

Two stages per hypothesis, forward and disjoint (the forward analogue of VS1's discovery/holdout):

1. **Discovery.** The first `n_discovery` valid observations after the hypothesis's first decision.
   One look, at exactly `n_discovery`. Selected if the statistic has the registered sign and
   one-sided p <= 0.10. Not selected closes the hypothesis.
2. **Gap.** Observations whose decision falls after the last discovery observation and at or before
   the discovery verdict record are dropped from both stages and logged as `between_stages`.
3. **Holdout.** The next `n_holdout` valid observations, all decided strictly after the discovery
   verdict record. One look, at exactly `n_holdout`. Pass if the registered sign holds and one-sided
   p <= the fixed per-hypothesis alpha in the table above.
4. **Fixed k.** The divisor is the registered count k of the sub-family, fixed now. It does not
   depend on how many hypotheses are selected, blocked or stopped, or on when their verdicts arrive.
5. **Across sub-families.** Nothing is pooled. By the union bound the chance of any false holdout
   pass in this family is at most 5 x 0.05 = 0.25; every report states this bound next to any pass.
   Overlapping v1 claims (section 7) add their own bounds.
6. **Global trial count.** Exactly the ten hypotheses in section 8, registered once in the E3 trial
   ledger as ten trials. No hypothesis may be added to v1.

## 2. Stage-0 power gate (before any first decision)

Before a hypothesis's first decision a Stage-0 synthetic power computation is run and witnessed. It
uses only the registered statistic, windows, alpha and the `planted_effect` of section 8, with
synthetic outcomes. It reads no real outcome.

- **Simulation.** Seed 20261001, 2,000 simulations, independent observations (optimistic: real
  returns are autocorrelated and heavy-tailed; the Newey-West statistic is still used).
  - `trade`: per-trade net return ~ Normal(`net_bps`, `noise_bps`).
  - `forecast_continuous`: standardized regressor z ~ N(0, 1); the PO2 control c ~ N(0, 1) with
    corr(z, c) = `control_corr`; outcome y = `slope_per_sd` * z + 0.3 * c + e, e ~ N(0, `noise_log_sd`).
  - `forecast_binary`: x ~ Bernoulli(`base_rate`), correlated with c through a latent Gaussian with
    correlation `control_corr`; y = `diff` * x + 0.3 * c + e.
  - `paired_trade`: per-session paired difference ~ Normal(`net_bps`, `noise_bps`).
- **Gate.** The joint power, P(selected in discovery AND pass in holdout), must be at least 0.50 at the
  registered `n_discovery` and `n_holdout`. If not, the next `n_holdout_ladder` rung is tried in order.
  The first passing rung becomes binding and is witnessed. If no rung passes, the hypothesis is
  `STOP_UNDERPOWERED`: activity is logged and no verdict can ever be issued. No override exists.
  The holdout-only power at the binding rung is reported alongside.
- **Regressions.** Every OLS includes an intercept. A hypothesis's own continuous regressor enters
  standardized (z, below). The PO2 control enters as PO2's raw x for the same session; the tested
  coefficient does not depend on the control's scale.
- **Standardization (no outcome use).** A continuous regressor x is standardized as
  z = (x - m) / s, with m and s the mean and standard deviation of x over the discovery window's
  valid observations (inputs only). m and s are frozen in the discovery verdict record and reused
  unchanged in the holdout.
- **Design check (informative, not binding).** The registered `n_discovery` values were chosen so
  that P(selected) is about 0.8 at the planted effect. A synthetic run of this model (2,000
  simulations, seed 20261001) found P(selected) between 0.79 and 0.84 for every hypothesis, and a
  ladder rung with joint power of at least 0.5 for every hypothesis. The binding gate is the
  witnessed Stage-0 run, not this check.
- **Scale.** A daily SPY trade with a 10 bp net edge and 80 bp noise needs on the order of a
  thousand trades for joint power 0.5 at alpha 0.025. The ladders are therefore measured in years.
  This is stated so that nobody mistakes a short window for evidence.

## 3. Decision instants, inputs and the engine pin

All times are America/New_York on NYSE regular sessions (`ingestion.market_calendar`). Early-close
sessions and the session after an unscheduled closure are excluded (`early_close`, `market_closed`).

- **Pre-open decision D0 = 09:10 on session S.** The decision record is appended to the decision log,
  its anchor line is appended to the decision witness file (section 10), and that commit must be on
  vault `origin/main` before 09:28, proven as in section 10. Otherwise the session is excluded
  `late_decision`. Market-on-open orders are due before the 09:30 opening auction.
- **Receipt rule for every input.** An input counts only if its GRID receipt (`created_at`, the row's
  insertion time, never the provider's or the pull's `available_at`) is at or before D0. Otherwise
  the session is excluded `input_unavailable`.
- **Chain.** SPY rows in `options_snapshots_all` for exactly one batch in `options_capture_batches`:
  ticker `SPY`, `snap_date` = S-1, `backfilled = false`, `registered_at <= D0`, the highest
  `capture_ordinal` among those, with `row_count` equal to the stored rows. It is replayed through
  `DealerGammaEngine.compute_gex_profile("SPY", S-1, capture_batch_id=...)`. The latest-batch view is
  never used. A missing or incomplete batch excludes the session (`no_batch`). A later batch never
  changes a logged decision.
- **Engine spot.** The pinned engine prices gamma at its own verified prior close: the latest
  `spy_close_receipt` available before the batch completed and at most four calendar days old. This
  is normally the S-2 close, and an earlier close across holidays or missing receipts. In winter, a
  batch completing between 13:30Z and D0 can see the S-1 close receipt and use it instead. It is
  disclosed and kept as the engine's native behavior. Every distance and side in this family uses P0
  instead.
- **P0 and PM.** P0 = SPY's official close of S-1. PM = a SPY pre-market indication (last trade or
  quote midpoint) stamped between 08:30 and 09:10. Both must come from one pre-open capture with a
  grid-svr receipt at or before D0. GRID's earliest scheduled equity pull for the S-1 close is
  13:30Z on S: 09:30 EDT (after D0) in summer and 08:30 EST in winter, and the resolver creates the
  receipt row only later. Because a `spy_close_v1` receipt created by D0 exists only in part of the
  year and is never guaranteed, it is not admitted as P0 in any season. No pre-open capture exists
  on 2026-10-01, so every
  hypothesis that uses P0 or PM is `BLOCKED_INPUT`. Admission requires an independently reviewed
  check over at least 20 sessions: the captured S-1 close equals the later `spy_close_v1` receipt
  for S-1, and the PM timestamp falls in the window.
- **Gap.** g = ln(PM / P0), known at D0.
- **Engine pin (MG and SC).** The engine and its whole spot path are pinned to main
  `cdf1b7f7f5a3cf2f37030a9c8164203405a23ecb`, git tree `8bd223aede0c91fb9b148f77767e6c2698aca9b6`.
  The logger imports `physics.dealer_gamma` and everything it reaches only from an immutable
  `git archive` of that commit, and refuses unless the archive's tree hash equals the pin. Its own
  code may be newer. For review, the SHA-256 of the LF git content of the core files is also
  pinned in section 8: `physics/dealer_gamma.py`, `physics/greeks/black_scholes.py`,
  `store/astrogrid.py` (`_verified_spy_receipt`), `store/availability.py`,
  `ingestion/market_calendar.py` and `price_close_contract.py`. Native parameters: r = 0.05, q = 0,
  integer calendar DTE, DTE 0 excluded, OI > 0 and IV > 0, engine regime thresholds
  (`gex_normalized` > 0.5 LONG_GAMMA, < -0.5 SHORT_GAMMA). A later engine or spot-path change,
  including any GEX-P2C canonical-engine decision, cannot change this family; it can only start a
  new version.
- **Delayed walls (DW).** Two inputs are named here: (a) ZeroGEX `get_gamma_levels("SPY")` call-wall
  and put-wall values; (b) a Cboe delayed SPY chain. Each needs an append-only capture with a
  grid-svr receipt at or before D0. Neither capture exists; both are `BLOCKED_INPUT`. ZeroGEX's
  methodology is not published to GRID and was not obtained at registration. DW1 therefore treats
  its levels as opaque vendor numbers. The admission record must quote the vendor's field names and
  any published definition. A later change in the vendor's fields stops DW1 (`STOP_INPUT_CHANGED`).
- **Recomputed walls (DW2), written here in full.** From the Cboe capture: standard `SPY` roots only;
  OI > 0; 0 < IV <= 5; expiry at 16:00 America/New_York; T = seconds from the capture's
  underlying `data.last_trade_time` field, read as America/New_York (a capture whose value is
  missing, malformed, offset-bearing or later than its receipt is `input_unavailable`), to expiry /
  (365 x 86400), T > 0; Black-Scholes gamma
  with r = 0.04, q = 0 and the Cboe IV; exposure per contract = gamma(P0) x OI x 100 x P0^2 x 0.01,
  sign +1 for calls and -1 for puts. Summed by strike: call wall = the strike with the largest
  positive call exposure; put wall = the strike with the most negative put exposure. Ties go to the
  strike nearer P0, then the lower strike. If neither wall exists, the session is `no_wall`.
- **DW2 direction.** The registered sign is the pinning reading: a large gamma concentration near the
  price dampens the move. Sessions whose nearest wall is farther away should then move more, so the
  slope of y on ln(distance) is positive. The opposite (acceleration) reading is not tested; this is
  one-sided.
- **Breadth (SC).** NYSE ADVN and DECN at the S-1 close, captured with a grid-svr receipt at or before
  D0. There is no capture, so `BLOCKED_INPUT`.

## 4. Executable price contracts

Every price is the price of an order that could have been sent at the decision instant. No mid, no
theoretical fill, no intraday bar print.

- **PC-OC (opening auction to closing auction, session S).** Entry: SPY's official opening auction
  price O_S (a market-on-open order submitted after D0 and before 09:28). Exit: the official closing
  auction price C_S (market-on-close). Return = side x (C_S / O_S - 1) - costs.
- **PC-CC (closing auction to closing auction).** Entry: the official closing auction price of the
  entry session (market-on-close submitted before that session's MOC cutoff, 15:50). Exit: the
  official closing auction price of the exit session.
- **Outcome receipts (copied from the SPY outcome-selection rule).** Each price session E used as an
  outcome counts only through a receipt for E whose `created_at` is at or before
  D(E) = (E + 6 calendar days) 00:00Z; `created_at`, not `available_at`, because the receipt row is
  written after the raw pull. If no eligible receipt exists at D(E) + 1 hour, the observation is
  terminally `price_unavailable`. There is no fallback to another session, source or later receipt.
  This is the only outcome-side exclusion: a missing official auction print (for any reason,
  including a halt) is `price_unavailable`; no rule may inspect intraday price behavior to exclude
  a session.
- **Admitted sources.** C_S, and every close in this family, is the value of the
  `astrogrid.price_close_receipt` receipt for that session (`spy_close_v1`, unadjusted, verified).
  It is never P0, and it is an input (PO2, RB1) only through receipts created at or before the
  decision. That receipt is the provider's daily close; it is not certified to be
  the closing auction print. Opens: none admitted on 2026-10-01. Before any hypothesis can start,
  one independently reviewed check over the same 20 or more sessions must show both that the
  candidate open equals the NYSE Arca official opening auction print and that the `spy_close_v1`
  close equals the official closing auction print. Both must be unadjusted and carry grid-svr
  receipts. Until that check passes, every PC-OC and PC-CC hypothesis is `BLOCKED_PRICE_CONTRACT`
  or `BLOCKED_INPUT`. If the close check fails, `spy_close_v1` is not admitted and the whole family
  stays `BLOCKED_PRICE_CONTRACT`; any replacement close source is a new version (v2), never a
  substitution inside v1.
- **Costs.** E2 cost model `e2-costs-v1`, class `us_equity_etf_large`: 3 bp per side, 6 bp round trip.
  This is the primary cost. A 1 bp-per-side sensitivity (GEX-levels v1's cost) is reported but never
  tested.
- **Move size for forecasts.** m_S = |ln(C_S / O_S)| from the same PC-OC prices and receipts. The
  forecast outcome is y_S = ln(max(m_S, 0.00005)); the 0.5 bp floor keeps C = O finite. A forecast
  has no fill and is not a P&L claim.

## 5. Forward logger and E2 feed (design decision)

S10's `grid-hypothesis-forward-log.timer` does not fit. S10 admits only frozen candidates from
`scripts/run_real_panel_scan.py` scans, reads features through the latest-vintage observations adapter
and labels target series. It has no capture-batch identity, engine pin, pre-open witness deadline or
auction price contract. Adding them would change S10's registered behavior.

Decision: a reviewed sibling logger, `gex_intraday_v1`. Its decision log is written in E2's admission
format `e2-stream-v1`. The header carries `format`, `stream = "gex_intraday_v1"` and `prereg_sha256` =
this body's SHA-256. It uses the S10 chain convention (`analysis.research_forward_log.ForwardLog`:
canonical JSON lines, `prev_sha256`, chained anchor file). Each prediction adds `hypothesis_id`,
`sub_family`, `capture_batch_id`, `engine_pin`, `decision_at`, `input_receipts` and `price_contract`.

- PC-CC direction hypotheses map onto the existing E2 rule `e2.direction.v1`.
- PC-OC trades need an E2 rule for auction-to-auction intraday entry and exit, proposed as
  `e2.auction_oc.v1`. Forecasts need `e2.abs_move.v1` and paired tests need `e2.paired_oc.v1`. These
  are E2 releases owned by the E2 lane (EVAL-E2F4). Until they exist, E2 reports those hypotheses as
  activity only, and this family's own one-look tests decide them.
- E2 never promotes. This family never writes a weight, signal, briefing or order.

## 6. Incremental claims (gamma and breadth are never credited by default)

- MG2's registered statistic is paired. On every session where PO1 triggers, take
  d = net(MG2) - net(PO1); net(MG2) = 0 on a session where MG2 does not trade. Only this tests
  whether the gamma regime adds anything to fading the gap.
- SC1's registered statistic is paired the same way: d = net(SC1) - net(MG2) on every session where
  MG2 trades.
- **Partners are counterfactuals.** PO1's rule is evaluated on every session MG2 evaluates, and
  MG2's rule on every session SC1 evaluates. This continues whether or not the partner was selected,
  passed, closed or stopped; a partner's verdict or stop never removes it from a pair. If the
  partner's net return is unavailable on a session while the paired hypothesis's own is available
  (for example, a partner-only input is missing), that paired observation is excluded as
  `partner_unavailable`. It is never filled with zero.
- A standalone mean for MG2 or SC1 may be reported. It may never be described as evidence for gamma
  or breadth.
- MG1, MG3, DW2 and SC2 enter PO2's regressor as a control, so their coefficients are incremental
  to price-only persistence.

## 7. Overlap with GEX-levels v1 and with S10

- MG1 and SC2 restate v1 H1 in spirit: the gamma regime conditions move size. MG2 and SC1 restate
  v1 H3's regime claim (fade under LONG_GAMMA, follow under SHORT_GAMMA) with a gap trigger instead
  of a wall. DW1 resembles H3's LONG_GAMMA leg (fade at a call or put wall) without a regime
  condition and with vendor walls.
- **Session separation.** MG1, MG2, SC1, SC2 and DW1 stay `BLOCKED` until the v1 closure artifact
  below is proven on vault `origin/main` (section 10). The proof date is the America/New_York
  calendar date of the later of the two section 10 observations (the verifier's fetch time and
  GitHub's push-event time). The first session on which any of the five may decide is the next NYSE
  session strictly after the proof date. Any report of these five cites v1's matching hypothesis,
  and their false-pass bounds add to v1's.
- **v1 closure artifact.** v1 itself writes no terminal record: `evaluate` prints to stdout and
  `status` only prints a stop advisory. Its closure is therefore defined here as one owner-committed
  file, `05-GRID/Paper-Log/gex_levels_v1/CLOSURE.json`, on vault `main`. It contains:
  - `kind`: `evaluation` or `stop`;
  - `log_records` and `log_head_sha256`: the record count and the SHA-256 of the last line of
    `/data/grid/paper_log/gex_levels_v1/gex_levels_v1.jsonl` at closure;
  - for `evaluation`: the exact stdout of `python -m paper_log.gex_levels evaluate --log-dir
    /data/grid/paper_log/gex_levels_v1` run without `--interim`, and it must be neither refused nor
    INTERIM. Because that output depends on the numerical stack (numpy's random stream, scipy and
    statsmodels), the artifact also records the Python interpreter version, the numpy, scipy and
    statsmodels versions, and the SHA-256 of the grid-svr environment's lock or `pip freeze` output;
  - for `stop`: the owner's stop decision quoting the exact `ADVISORY:` line printed by `status`.

  Verification, all of which must hold:
  - line number `log_records` of the v1 log hashes to `log_head_sha256`, using v1's own convention
    (`paper_log/gex_levels/storage.py`: SHA-256 of the exact canonical JSON line, the value each
    next record carries as `prev_sha256`), and the chain of lines 1..`log_records` verifies;
  - the verifier copies the first `log_records` lines to a scratch directory and re-runs v1's pinned
    code on that copy. For `evaluation` it runs `evaluate` in an environment matching the recorded
    interpreter, package versions and lock hash, and requires stdout byte-identical to the quoted
    stdout. For `stop` it runs `status` (pure counting) and requires its `ADVISORY:` line to be
    byte-identical to the quoted advisory line;
  - the proof of when the file reached `origin/main` follows section 10.

  **Absent this artifact, the five stay `BLOCKED` permanently; no other route unblocks them.**
- v1 H2 (wall hold against mirror placebos on 5-minute bars) is not re-tested. DW uses vendor or
  recomputed delayed walls and auction prices only.
- S10: no candidate scientific pair is shared.

## 8. The ten hypotheses (machine-readable, binding)

The JSON block below is the binding list. The prose explains it; if the two disagree, the block
governs and the discrepancy is a defect, fixed only in v2.

```json
{
  "family": "gex_intraday_v1",
  "registered_on": "2026-10-01",
  "first_decision_rule": "first NYSE session strictly after witnessed registry header, owner activation, admitted inputs and price contract, witnessed Stage-0 pass, and for MG1/MG2/SC1/SC2/DW1 the GEX-levels v1 separation rule",
  "stage0": {
    "seed": 20261001,
    "simulations": 2000,
    "min_joint_power": 0.5,
    "control_loading": 0.3
  },
  "discovery": {
    "select_one_sided_p": 0.1
  },
  "holdout": {
    "family_alpha": 0.05,
    "correction": "fixed_k_bonferroni_within_sub_family",
    "k": {
      "PO": 2,
      "DW": 2,
      "MG": 3,
      "SC": 2,
      "RB": 1
    },
    "same_sign": true
  },
  "sub_families": [
    "PO",
    "DW",
    "MG",
    "SC",
    "RB"
  ],
  "engine_pin": {
    "physics/dealer_gamma.py": "8db7ab0ac311d6564fa44a664d4685c23ff40669c845769fc842c93e63037111",
    "physics/greeks/black_scholes.py": "50d5dfd9f26d1e797ccfc913a8e127040e3908de0cfb3a8a5f6bc230c5764b99",
    "store/astrogrid.py": "3b230d7812e66a673e4501befaa3c45af3132a1184a6a5b6ff592493f5ac6ba0",
    "store/availability.py": "4cfd0acea464d5e97dfc023328d44fd275255d189bed1dcdaff0cddb1e765118",
    "ingestion/market_calendar.py": "8292a86f73f4fe5a4d28c04619b1dc5712ebcb29aae9f41e46b8cbb19f9d5147",
    "price_close_contract.py": "925cf32098abeb231e6c1f563e4193acddf4aada74c4af0152b52cedb4864693",
    "hash_basis": "sha256 of LF git content",
    "reference_commit": "cdf1b7f7f5a3cf2f37030a9c8164203405a23ecb",
    "reference_tree": "8bd223aede0c91fb9b148f77767e6c2698aca9b6"
  },
  "cost": {
    "model": "e2-costs-v1",
    "class": "us_equity_etf_large",
    "bps_per_side": 3.0,
    "sensitivity_bps_per_side": 1.0
  },
  "forecast_outcome": {
    "y": "ln(max(abs(ln(C_S/O_S)), 0.00005))"
  },
  "hypotheses": [
    {
      "id": "PO1",
      "sub_family": "PO",
      "kind": "trade",
      "uses_gamma": false,
      "price_contract": "PC-OC",
      "decision": "D0",
      "e2_rule": "e2.auction_oc.v1",
      "input": "P0 and PM from one admitted pre-open capture",
      "rule": "g = ln(PM/P0); if abs(g) >= 0.003, side = -sign(g) (fade the pre-open gap); else no trade",
      "statistic": "mean net PC-OC return per trade; one-sided t, Newey-West 5 lags",
      "direction": "positive",
      "planted_effect": {
        "model": "trade",
        "net_bps": 10,
        "noise_bps": 80
      },
      "n_discovery": 300,
      "n_holdout": 580,
      "n_holdout_ladder": [
        580,
        1160,
        2320
      ],
      "input_status": "BLOCKED_INPUT"
    },
    {
      "id": "PO2",
      "sub_family": "PO",
      "kind": "forecast",
      "uses_gamma": false,
      "price_contract": "PC-OC",
      "decision": "D0",
      "e2_rule": "e2.abs_move.v1",
      "input": "m for sessions S-2..S-21 from outcome receipts created at or before D0",
      "rule": "x = ln(mean m over S-2..S-6) - ln(median m over S-2..S-21)",
      "statistic": "OLS (with intercept) slope of y_S on z(x); one-sided, Newey-West 5 lags",
      "direction": "positive",
      "planted_effect": {
        "model": "forecast_continuous",
        "slope_per_sd": 0.15,
        "noise_log_sd": 1.0,
        "control_corr": 0.0
      },
      "n_discovery": 250,
      "n_holdout": 250,
      "n_holdout_ladder": [
        250,
        500,
        1000,
        2000
      ],
      "input_status": "BLOCKED_PRICE_CONTRACT"
    },
    {
      "id": "DW1",
      "sub_family": "DW",
      "kind": "trade",
      "uses_gamma": true,
      "price_contract": "PC-OC",
      "decision": "D0",
      "e2_rule": "e2.auction_oc.v1",
      "input": "ZeroGEX call wall and put wall captured at or before D0; PM",
      "rule": "if PM is within 0.25% below the vendor call wall, side = -1; if within 0.25% above the vendor put wall, side = +1; both or neither: no trade",
      "statistic": "mean net PC-OC return per trade; one-sided t, Newey-West 5 lags",
      "direction": "positive",
      "planted_effect": {
        "model": "trade",
        "net_bps": 10,
        "noise_bps": 80
      },
      "n_discovery": 300,
      "n_holdout": 580,
      "n_holdout_ladder": [
        580,
        1160,
        2320
      ],
      "input_status": "BLOCKED_INPUT"
    },
    {
      "id": "DW2",
      "sub_family": "DW",
      "kind": "forecast",
      "uses_gamma": true,
      "price_contract": "PC-OC",
      "decision": "D0",
      "e2_rule": "e2.abs_move.v1",
      "input": "Cboe delayed SPY chain captured at or before D0; walls by the section 3 formula; P0",
      "rule": "x = ln(min distance from P0 to the recomputed call wall or put wall, in percent of P0, floored at 0.01); neither wall: no_wall",
      "statistic": "OLS (with intercept) slope of y_S on z(x) and PO2's x; one-sided, Newey-West 5 lags",
      "direction": "positive",
      "planted_effect": {
        "model": "forecast_continuous",
        "slope_per_sd": 0.15,
        "noise_log_sd": 1.0,
        "control_corr": 0.3
      },
      "n_discovery": 250,
      "n_holdout": 250,
      "n_holdout_ladder": [
        250,
        500,
        1000,
        2000
      ],
      "input_status": "BLOCKED_INPUT"
    },
    {
      "id": "MG1",
      "sub_family": "MG",
      "kind": "forecast",
      "uses_gamma": true,
      "price_contract": "PC-OC",
      "decision": "D0",
      "e2_rule": "e2.abs_move.v1",
      "input": "pinned engine gex_normalized from the section 3 batch",
      "rule": "x = gex_normalized (continuous)",
      "statistic": "OLS (with intercept) slope of y_S on z(x) and PO2's x; one-sided, Newey-West 5 lags",
      "direction": "negative",
      "planted_effect": {
        "model": "forecast_continuous",
        "slope_per_sd": -0.15,
        "noise_log_sd": 1.0,
        "control_corr": 0.3
      },
      "n_discovery": 250,
      "n_holdout": 250,
      "n_holdout_ladder": [
        250,
        500,
        1000,
        2000
      ],
      "input_status": "BLOCKED_PRICE_CONTRACT"
    },
    {
      "id": "MG2",
      "sub_family": "MG",
      "kind": "paired_trade",
      "uses_gamma": true,
      "price_contract": "PC-OC",
      "decision": "D0",
      "e2_rule": "e2.paired_oc.v1",
      "input": "pinned engine regime from the section 3 batch; P0 and PM",
      "rule": "on abs(g) >= 0.003: SHORT_GAMMA side = sign(g) (continue); LONG_GAMMA side = -sign(g) (fade); NEUTRAL: no trade",
      "statistic": "mean of d = net(MG2) - net(PO1) over sessions where PO1 trades; one-sided t, Newey-West 5 lags",
      "direction": "positive",
      "planted_effect": {
        "model": "paired_trade",
        "net_bps": 10,
        "noise_bps": 80
      },
      "n_discovery": 300,
      "n_holdout": 580,
      "n_holdout_ladder": [
        580,
        1160,
        2320
      ],
      "input_status": "BLOCKED_INPUT"
    },
    {
      "id": "MG3",
      "sub_family": "MG",
      "kind": "forecast",
      "uses_gamma": true,
      "price_contract": "PC-OC",
      "decision": "D0",
      "e2_rule": "e2.abs_move.v1",
      "input": "pinned engine gamma_flip and gamma_flip_crossings from the section 3 batch; P0",
      "rule": "x = 1 if gamma_flip_crossings >= 1 and abs(gamma_flip - P0) / P0 <= 0.0025, else 0",
      "statistic": "OLS (with intercept) coefficient of x in y_S on x and PO2's x; one-sided, Newey-West 5 lags",
      "direction": "positive",
      "planted_effect": {
        "model": "forecast_binary",
        "diff": 0.3,
        "base_rate": 0.15,
        "noise_log_sd": 1.0,
        "control_corr": 0.2
      },
      "n_discovery": 450,
      "n_holdout": 500,
      "n_holdout_ladder": [
        500,
        1000,
        2000
      ],
      "input_status": "BLOCKED_INPUT"
    },
    {
      "id": "SC1",
      "sub_family": "SC",
      "kind": "paired_trade",
      "uses_gamma": true,
      "price_contract": "PC-OC",
      "decision": "D0",
      "e2_rule": "e2.paired_oc.v1",
      "input": "MG2 trigger plus NYSE ADVN minus DECN at the S-1 close, captured at or before D0",
      "rule": "MG2's trade only when sign(ADVN - DECN) equals the MG2 side; else no trade",
      "statistic": "mean of d = net(SC1) - net(MG2) over sessions where MG2 trades; one-sided t, Newey-West 5 lags",
      "direction": "positive",
      "planted_effect": {
        "model": "paired_trade",
        "net_bps": 10,
        "noise_bps": 80
      },
      "n_discovery": 300,
      "n_holdout": 580,
      "n_holdout_ladder": [
        580,
        1160,
        2320
      ],
      "input_status": "BLOCKED_INPUT"
    },
    {
      "id": "SC2",
      "sub_family": "SC",
      "kind": "forecast",
      "uses_gamma": true,
      "price_contract": "PC-OC",
      "decision": "D0",
      "e2_rule": "e2.abs_move.v1",
      "input": "pinned engine regime; NYSE ADVN minus DECN at the S-1 close and the prior 59 closes, captured at or before D0",
      "rule": "x = 1 if regime is SHORT_GAMMA and abs(ADVN - DECN) at the S-1 close is in its top third over the S-1..S-60 closes, else 0",
      "statistic": "OLS (with intercept) coefficient of x in y_S on x and PO2's x; one-sided, Newey-West 5 lags",
      "direction": "positive",
      "planted_effect": {
        "model": "forecast_binary",
        "diff": 0.3,
        "base_rate": 0.1,
        "noise_log_sd": 1.0,
        "control_corr": 0.2
      },
      "n_discovery": 650,
      "n_holdout": 650,
      "n_holdout_ladder": [
        650,
        1300,
        2600
      ],
      "input_status": "BLOCKED_INPUT"
    },
    {
      "id": "RB1",
      "sub_family": "RB",
      "kind": "trade",
      "uses_gamma": false,
      "price_contract": "PC-CC",
      "decision": "D_RB: 15:00 on M-2, the third-to-last NYSE session of the calendar month (M = the last NYSE session of the calendar month)",
      "e2_rule": "e2.direction.v1",
      "input": "month-to-date total returns of SPY and AGG through the M-3 close, from unadjusted close receipts and declared dividends with ex-dates in the month on or before M-3, every receipt created at or before D_RB",
      "rule": "if SPY MTD minus AGG MTD >= +0.03, side = -1 from the M-2 close to the M close (rebalancing sale); if <= -0.03, side = +1; else no trade",
      "statistic": "mean net PC-CC return per trade; one-sided t",
      "direction": "positive",
      "planted_effect": {
        "model": "trade",
        "net_bps": 40,
        "noise_bps": 150
      },
      "n_discovery": 64,
      "n_holdout": 72,
      "n_holdout_ladder": [
        72,
        144
      ],
      "input_status": "BLOCKED_INPUT"
    }
  ]
}
```

Notes binding the block:

- `n_discovery` and `n_holdout` count valid observations: trades for `trade`; paired sessions for
  `paired_trade`; sessions for `forecast`; triggered month-end trades for RB1. A session or month
  with no trigger is neither a trade nor an exclusion.
- RB1 horizon: if about half of months trigger, 64 discovery trades take about 11 years and 72 holdout
  trades about 12 more. This is stated so that nobody mistakes a short window for evidence.
- z(x) is the standardization of section 2. "PO2's x" means PO2's raw regressor for the same
  session, entered as a control. It never means PO2's verdict.
- PO2's window is exactly the NYSE sessions S-2..S-21. If any of them lacks both outcome receipts
  created at or before D0, PO2's x is unavailable and every hypothesis using it excludes the session
  (`input_unavailable`). The window is never shortened or shifted.
- Every OLS includes an intercept (section 2).
- RB1 calendar: M = the last NYSE session of the calendar month; M-1 = the second-to-last; M-2 = the
  third-to-last; M-3 = the fourth-to-last.
- RB1's decision record and its anchor must be on vault `origin/main` before 15:45 on M-2, proven as
  in section 10. Otherwise `late_decision`. Its entry is the M-2 market-on-close order. Each of its
  price sessions follows the section 4 receipt rule separately. Its AGG prices and dividend source
  are not admitted (`BLOCKED_INPUT`).

## 9. Exclusions (reason codes, never silent)

`market_closed`, `early_close`, `late_decision`, `no_batch`, `engine_unavailable` (no measured spot
or no regime), `input_blocked`, `input_unavailable` (no receipt at or before the decision),
`no_wall` (DW2: neither recomputed wall exists), `partner_unavailable` (section 6),
`between_stages` and `price_unavailable` (section 4; the only outcome-side code). A
hypothesis stops (`STOP_DATA_QUALITY`) if more than 10% of its sessions are excluded by its 30th
session. `market_closed`, `early_close`, `input_blocked`, `no_wall` and `between_stages` do not
count toward the 10%. Any fix is v2.

Until a hypothesis's look, status reports activity only: decisions, trades, exclusions by reason, and
valid observations against `n_discovery` or `n_holdout`. It never reports a return, hit rate, slope
or p-value. A look taken early is a defect and invalidates the hypothesis.

## 10. Integrity, witnesses and owner gates

- **Registry.** `analysis/gex_intraday_prereg.py`: an append-only hash-chained JSONL (`ForwardLog`). It
  starts with a header carrying this body's SHA-256 and `promotion_allowed = false`, then one
  `preregistration` record carrying this block's canonical SHA-256 and the ten trial ids. After
  that, per hypothesis: `admitted` or `blocked`, `stage0_power`, `discovery_verdict` (with the frozen
  m and s), `holdout_verdict`, or `stop`. After the real registration, the two record hashes are
  pinned in code. A different registration is a fork and is refused.
- **Registry witness.** Anchor lines go to
  `05-GRID/Paper-Log/gex_intraday_v1/gex_intraday_v1_prereg.anchors.jsonl` on vault `main`, from a
  worktree off `origin/main`, LF only.
- **Decision witness and time proof.** Every decision's anchor line is appended to
  `05-GRID/Paper-Log/gex_intraday_v1/gex_intraday_v1_decisions.anchors.jsonl` and pushed to vault
  `origin/main`. A commit's own timestamp is self-reported and proves nothing. The time proof is a
  remote observation: a separate verifier fetches `origin/main` and records its fetch time together
  with the containing commit. GitHub's server-recorded push event time is also kept. Both must be at
  or before the deadline (09:28 for D0, 15:45 for RB1). Missing either means `late_decision`.
- **Owner gates.** None of these is performed by this PR:
  - writing the registry on grid-svr;
  - the registry witness commit;
  - the Stage-0 power runs and their witnesses;
  - admitting the pre-open capture, opening-auction, ZeroGEX, Cboe-chain, breadth, AGG and dividend
    sources;
  - installing or enabling the logger timer and the daily decision-witness push automation
    (`GRID_ENABLE_*_JOB`, with backup, dry-run and GO);
  - the E2 releases for `e2.auction_oc.v1`, `e2.abs_move.v1` and `e2.paired_oc.v1`, and the E2
    `rules.json` stream registration of `gex_intraday_v1`;
  - the ten trial entries in the E3 trial ledger.
- This PR's code does not import or write the frozen GEX-levels v1 log or its pinned code. The
  section 7 verification reads the v1 log read-only, and runs v1's pinned code only on a scratch
  copy of the log's first `log_records` lines. Nothing in this family ever writes to v1.

<!-- PREREG-BODY-END -->

Body SHA-256: pinned as `PREREG_BODY_SHA256` in `analysis/gex_intraday_prereg.py` and checked by
`tests/test_gex_intraday_prereg.py`. The registration record is the git commit that adds this file.
The witnessed registry header follows the owner gates above.
