# GEX-GRANULAR-V1: Granular Offline Exposure Decomposition and Inventory Sensitivity

## 1. Executive Summary and Architecture Boundaries

`GEX-GRANULAR-V1` provides a deterministic offline strike- and expiry-level Gamma Exposure (GEX) decomposition for the P2-A harness. It operates under strict point-in-time (PIT) accounting boundaries and treats dealer inventory as an unobserved variable evaluated exclusively through declared sensitivity envelopes.

Key architectural invariants:
1. **Reuse Existing Arithmetic Verification**: All Greek calculations reuse the verified native GRID primitive gamma values from the canonical `scripts.gex_p2a.harness:reconcile` execution on filtered admitted rows. No second Greek engine, local approximation, or scoring loop is introduced.
2. **Status Separation**: Numerical arithmetic verification (`PASS_NUMERICAL`, `FAIL_NUMERICAL`, `NOT_SUPPORTED`) is strictly decoupled from data coverage statuses (`COMPLETE_DECLARED_COVERAGE`, `PARTIAL_DECLARED_COVERAGE`, `INSUFFICIENT_DATA`) and inventory interpretation (`HYPOTHETICAL_UNOBSERVED`).
3. **Preservation of Unsupported Bounds**: If any admitted contract violates the native GRID primitive floor ($T < T_{\\min}$), the spot point remains `NOT_SUPPORTED`. It is never falsified as zero gamma.
4. **No External I/O**: Offline evaluation is completely self-contained. Output directories are create-only; inputs are preserved verbatim without mutation.

---

## 2. Executable Schema and Clock Provenance

### 2.1 Packet Input Schema (`gex-granular-input-v1`)

Packets extend `gex-p2a-v1` with a top-level `granular` object and per-row clock structures:

```json
{
  "schema": "gex-p2a-v1",
  "packet_id": "<UUID>",
  "source": "synthetic-fixture-not-market-data",
  "basis": "unadjusted",
  "calendar_version": "synthetic-explicit-UTC-expiries-v1",
  "availability_basis": "grid_svr_pull_receipt",
  "available_at": "2026-09-28T15:00:00Z",
  "valuation_at": "2026-09-28T15:00:00Z",
  "r": 0.04,
  "q": 0.012,
  "spots": [760.0, 765.0, 770.0],
  "granular": {
    "version": "gex-granular-input-v1",
    "spot_clocks": {
      "source_at": "2026-09-28T14:59:55Z",
      "received_at": "2026-09-28T15:00:00Z",
      "source": "grid_svr_pull"
    },
    "expected_contract_ids": ["SPY-20260928-765-C", "..."],
    "scenarios": [
      {
        "name": "hypothetical_scenario_name",
        "kind": "hypothetical_signed_inventory",
        "fractions": {"SPY-20260928-765-C": -0.5}
      }
    ]
  },
  "rows": [
    {
      "contract_id": "SPY-20260928-765-C",
      "underlying": "SPY",
      "expiry": "2026-09-28T20:00:00Z",
      "strike": 765.0,
      "side": "call",
      "iv": 0.20,
      "iv_origin": "direct",
      "oi": 1234,
      "oi_as_of": null,
      "multiplier": 100.0,
      "deliverable": "standard_shares",
      "bid": 1.45,
      "ask": 1.55,
      "provider_gamma": 0.0123,
      "clocks": {
        "quote": {"source_at": "2026-09-28T14:59:50Z", "received_at": "2026-09-28T15:00:00Z", "source": "grid_svr_pull"},
        "greek": {"source_at": "2026-09-28T14:59:50Z", "received_at": "2026-09-28T15:00:00Z", "source": "grid_svr_pull"},
        "oi": {"source_at": null, "received_at": "2026-09-28T09:00:00Z", "source": "grid_svr_pull"}
      }
    }
  ]
}
```

### 2.2 Point-in-Time (PIT) Clock Invariants

1. **Receipt Gate**: Top-level `available_at` and every row-level `received_at` must satisfy $\\tau_{\\text{receipt}} \\le \\tau_{\\text{valuation}}$. Any missing receipt excludes the row. Any future receipt excludes the row with reason `FUTURE_<FIELD>_RECEIPT`.
2. **Source Gate**: If `source_at` is known, it must satisfy $\\tau_{\\text{source}} \\le \\tau_{\\text{receipt}}$ and $\\tau_{\\text{source}} \\le \\tau_{\\text{valuation}}$.
3. **No Substitution**: Unknown source timestamps and `oi_as_of` remain `null`. Under no circumstances may arrival receipts be substituted for source observation times.
4. **Spot Availability**: A missing spot receipt or a future spot receipt rejects the entire packet (`INPUT_REJECTED`) because underlying exposure cannot be priced.
5. **Universe Integrity**: Every row contract ID must exist within `expected_contract_ids`. Unknown observed IDs or duplicate contract IDs reject the entire packet.
6. **Direct IV Semantics**: Greek clock timestamps reflect the observation of direct implied volatility used for recomputation, not fresh independently observed vendor gamma. Bid, ask, and provider gamma fields serve purely as provenance.

---

## 3. Mathematical Formulation and Units

### 3.1 Exposure Units

All exposures are reported in standard dimensional units: `USD_per_1pct_underlying_move`. For underlying spot $S$, option gamma $\\Gamma$, contract open interest $\\text{OI}$, multiplier $M$, and assumed dealer inventory fraction $f \\in [-1, 1]$:

$$\\text{OI Gross USD per 1\\%} = \\Gamma \\cdot \\text{OI} \\cdot M \\cdot S^2 \\cdot 0.01$$

$$\\text{Inventory Gross USD per 1\\%} = \\text{OI Gross} \\cdot |f|$$

$$\\text{Signed Exposure USD per 1\\%} = \\text{OI Gross} \\cdot f$$

$$\\text{Signed Contract Quantity} = \\text{OI} \\cdot f$$

> **Distinction Between Gross Quantities**: When $|f| < 1$, assumed inventory gross is strictly smaller than unsigned OI gross. Unsigned OI gross is the absolute gamma sensitivity associated with reported OI on the declared universe; assumed inventory gross is the magnitude of a net-position hypothesis. Neither measures observed dealer gross holdings.

### 3.2 Deterministic Aggregation

Summations across strikes within expiry, expiries, and total portfolios are computed using deterministic `math.fsum` with stable identity sorting:
- `call_oi_gross`, `put_oi_gross`, `oi_gross`
- `call_inventory_gross`, `put_inventory_gross`, `inventory_gross`
- `call_signed`, `put_signed`, `signed_net`

### 3.3 Zero Exposure vs Insufficient Data

- Legitimate $\\text{OI} = 0$ contracts admitted to the universe yield exactly $0.0$ USD exposure.
- If all contracts are excluded or the admitted universe is empty, aggregates evaluate to `null` with status `INSUFFICIENT_DATA`, never numerical zero.

### 3.4 0DTE Boundary

0DTE is defined strictly as `expiry.date() == valuation.date()` in UTC. It does not assert exchange-local trading session alignment. Expiry instants are preserved exactly without synthetic SPX contract scaling.

---
## 4. Inventory Sensitivity Scenarios

Open interest and untagged quotes alone do not identify dealer positioning. Participant-tagged execution data may support a separate reconstruction, subject to market coverage, opening inventory and availability limits. GEX-GRANULAR-V1 establishes two scenario tiers:

1. **Built-in Baseline (`oi_sign_baseline`)**:
   - Standard assumption: calls fraction $= +1.0$, puts fraction $= -1.0$.
   - Clearly disclaimed: neither baseline nor hypothetical inventory is observed truth.
2. **Hypothetical Scenarios (`scenarios`)**:
   - Up to 8 declared scenarios mapping all expected contract IDs to fractions in $[-1.0, 1.0]$.
   - Scenarios provide an arithmetic sensitivity envelope, NOT a confidence interval, and NOT a directional market signal.

---

## 5. Model Limitations and Approximations

1. **European Approximation**: SPY options are American style with potential early exercise around ex-dividend dates. The native P2-A kernel uses Black-Scholes European pricing with constant risk-free rate $r$ and continuous dividend yield $q$.
2. **UI/API Surface**: This specification proposes an offline read-only dashboard decomposition; no runtime modifications, active trading routes, or web endpoints are enabled.

---

## 6. Future Empirical Validation Protocol

Any future promotion of granular GEX metrics from sensitivity envelopes to predictive features must follow a strict validation protocol:

1. **Frozen Chronological Manifest**: Historical test packets must be locked with immutable content hashes, verified exchange receipt timestamps, and source event clocks.
2. **Disjoint Holdout Selection**: Walk-forward train/validation calibration must conclude before inspecting an embargoed out-of-time holdout dataset.
3. **Approved Validation Machinery**: Re-run existing approved testing harnesses (`analysis/offline_research_proof.py` VS1, `analysis/ledger_steered_exploration.py` S11, and the released `evals/e3` adapters) only under explicit coordinator direction. No ad-hoc parameter searches or metric gaming.
4. **Market Outcome Disaggregation**: Evaluations must isolate implied volatility surface reactions from directional delta drift, adjusting for bid-ask spread crossing costs, exchange fees, and execution slippage.
5. **Confounding Controls**: Regressions must control for intraday time-of-day volume patterns, prevailing realized volatility (RV), macro announcements, order book depth, and distance-matched strike baselines.
6. **Vendor Comparisons**: Benchmark comparisons against external vendors require verified identical raw input snapshots. Participant studies demonstrate structural utility without asserting vendor superiority.

Existing path boundary: canonical daily `physics/dealer_gamma.py` reports a legacy
`gamma * OI * 100 * S` scale. Its conversion to this payload's units is **S * .01
at each evaluated spot** under identical inputs and model parameters. Gamma Watch
`collectors/gamma_watch/broker.py` reports billions USD per 1% move, converted by
*1e9. Neither path is changed. ZeroGEX headline is provider passthrough whose
native units/universe remain unverified; direct value comparison is blocked.
This extension reuses the P2-A matching-input arithmetic boundary, not native
loaders, IV recovery, roots, walls or production pipeline validation. Gamma is
recomputed at every hypothetical spot; cumulative strike bars are not flip curves.

Available source fields (task7 source audit): RTD field receipts exist for bid,
ask, IV, gamma, OI, but exchange event time and OI vintage are unknown. Advancing
receipts do not demonstrate changed gamma/OI. Public Cboe contracts provide
strike/expiry/side, quotes/sizes, volume/OI and Greeks but lack per-contract clocks.
Do not synthesize missing clocks to admit these rows. The granular runtime
adapter and dashboard remain separate coordinated work.

Evaluation layers remain distinct: (1) Greek arithmetic against existing independent
Decimal reference, with American/dividend model discrepancy evaluated separately;
(2) signed inventory error against verified participant labels, unavailable here;
(3) future realized-volatility forecasts, direction and executable PnL scored
separately after spread/slippage/fees, including time-of-day, RV, trend, liquidity,
event and distance-matched random-level controls. Inventory accuracy cannot be
inferred from an arithmetic pass or a volatility correlation. Retain every declared
trial and negative result; bind code, parameters, data and windows before tests.
The frozen chronological dataset manifest, train-selection windows, embargo and
held-out labels are prerequisites for a later empirical benchmark, not features
implemented by this synthetic arithmetic slice. No market/participant labels,
provider performance, live holdout, E3B blocked-draft adapter or model promotion
were used here.
