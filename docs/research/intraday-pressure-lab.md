# Intraday pressure laboratory: implementation, not validated edge

Owner request 2026-10-02: build all candidate inputs and determine whether any
improve SPYU intraday decisions, including range persistence and prominent gamma
regime changes. This isolated laboratory never imports production engines,
writes GRID databases, registers/promotes hypotheses, trades, or activates a feed.
Coordinator owns merge and activation. Paid APIs remain off.

## Existing research remains authoritative

`analysis/gex_intraday_prereg.py`, its immutable GEX intraday v1 body, GEX-levels
v1, E2/E3 releases and their witnesses are unchanged. Their forward-only,
power/admission/one-look gates remain binding. This laboratory is an exploratory
implementation candidate; its outputs cannot enter the official scoreboard or
be used as a second look at a sealed stream. Do not read sealed v1 outcomes.
Any formal admission requires an independent registered stream and E2 adapter,
an E3 trial entry, power analysis and an owner-approved activation receipt.

## Implemented candidates

| Feature | Input / interpretation |
|---|---|
| Price momentum | Five-minute SPY return; baseline only |
| ES depth | Best bid/ask size imbalance; displayed liquidity can disappear |
| ES OFI | Changes in best prices/sizes over sixty seconds; not deep-book MLOFI |
| ES aggression | Verified buy/sell aggressor volumes in sixty seconds |
| ES absorption | Opposite-flow candidate when strong aggression moves ES <=1 bp |
| Breadth | TICK and advancing/declining volume composite |
| Breadth divergence | Breadth opposes SPY's five-minute return |
| ES / NQ / constituents lead | Sixty-second return discrepancy vs SPY; a candidate, not proof of lead |
| Auction | SPX-constituent notional imbalance inside final ten minutes |
| Range compression | Five-minute return volatility contracts relative to twenty minutes |
| Failed breaks | Returns inside an earlier fixed ten-minute range after excursions |
| Gamma range | Compression conditioned on positive modeled gamma and nearby wall |

All formulas and initial thresholds are in `scripts/intraday_lab/`. Thresholds
are uncalibrated; do not tune against today's observed 769 outcome. Constituents
require an upstream documented, contemporaneous index-weighted basket; auction
input must be the matching universe, not an all-NYSE sum mislabeled SPX.
ES_BOOK is L1, not an execution feed. ES_FLOW requires verified classification;
midpoint guesses are not admitted. No broker connector is implied by these inputs.

## Packet and timing contract

Each JSONL packet supplies `decision_at` (epoch UTC trusted server time), an
exchange `session`, `provenance`, and observations with `instrument`, `source_id`,
`event_at`, `available_at`, `clock`, `status`, `values`. Only `clock=trusted_receipt`,
available records, event <= receipt <= decision and fresh complete histories
are usable. No mixtures of price providers. Ten-second cadence/gap ceilings
mean a twenty-second public fallback cannot qualify for high-frequency claims.
Unknown, null, nonfinite, future and stale data withhold features; they never vote
zero. Source IDs distinguish Yahoo unadjusted intraday from adjusted history,
vendor-delayed GEX from RTD modeled GEX and frozen-chain scenarios.

For RTD, `available_at` is grid-svr's pull receipt, never ANIK's callback clock.
An unknown exchange event clock also blocks these strict high-frequency inputs;
heartbeat freshness alone is insufficient. Capture inventories do not admit a
stream. `capture.py` preserves public raw bytes/hash and pull receipt, retains
historical bars raw, and labels its packet `capture_inventory_not_admitted`.
Its local normalized source IDs need catalog reconciliation before GRID ingestion.

## Evaluation and limitations

```
python -m scripts.intraday_lab packets.jsonl --output /scratch/report.json
python -m scripts.intraday_lab.capture --output /scratch/intraday-capture
```

Evaluator splits entire chronological sessions 60/20/20; fixed formulas do not
fit any outcome. Reserved test outcomes are not evaluated by default; the explicit
`--include-holdout` flag records `holdout_examined=true` and does not make a sealed
or official holdout. It compares baseline and candidate at identical active timestamps,
never crosses sessions, requires complete future paths, and excludes overlapping
decisions independently at 1/5/15-minute horizons. Horizons and candidates still
overlap statistically: descriptive session-block bootstrap uses a fixed 42-trial
divisor. Sixty holdout sessions are a screening floor, not a power calculation.
Formal power and E2/E3 testing remain prerequisites for a validated edge.

Returns on SPY are direction diagnostics. Economic scoring requires actual SPYU
entry ask/exit bid with positive displayed sizes: no fabricated 4x SPY P&L.
Strategy is long on positive candidate / cash otherwise; negative pressure is
not an assumed short fill. Quoted spreads are accompanied by 0/5/10 bp extra
roundtrip cost sensitivities, which are scenarios rather than observed fills.
Range hit rate is paired with the same eligible population's
unconditional range frequency and is descriptive, not a calibrated probability.
The range target is fixed at +/-10 bp at decision, assessed over the full future
path. Results always say `EXPLORATORY_NO_VALIDATED_EDGE`; missing observations
or insufficient sessions cannot produce a winner. Synthetic tests establish
calculation/integrity behavior only.

## Data integration handoff

Existing-account first: verify thinkorswim ES/NQ, breadth and SPYU quote callbacks
and timestamp lineage; separately verify market depth and execution export/API
capability. IBKR free trial is not proof of entitlement. Auction feed remains
blocked until independently verified. Raw snapshots and feature receipts need
immutable GRID custody with database identity and idempotent cursors under the
P1 design, not a parallel canonical store. No activation is included here.

For admission, validate commission/slippage assumptions, a fixed power-derived sample
size and cutoff, fixed candidate selection from discovery only, validation and
sealed holdout receipts, matched placebo/price baseline and E2/E3 adapter/witness.
Do not repeatedly inspect a held-out stream or call a bootstrap bound a live edge.
